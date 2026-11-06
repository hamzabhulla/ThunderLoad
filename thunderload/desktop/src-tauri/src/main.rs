use std::{collections::HashMap, path::PathBuf, process::Command, sync::{atomic::AtomicU64, Arc, Mutex, OnceLock}};
use serde_json::json;
use tauri::{Emitter, Manager};
use thunderload_engine::{default_download_dir, download_hls, download_url, download_url_resumable, inspect_video, partial_download_path, reserve_download_path, safe_filename, DownloadControl, DownloadOutcome};
use tokio::sync::{mpsc, watch};

#[tauri::command]
async fn inspect_video_url(url: String, referer: Option<String>) -> Result<Vec<thunderload_engine::VideoVariant>, String> {
    inspect_video(&url, referer.as_deref()).await.map_err(|error| error.to_string())
}


static JOB_CONTROLS: OnceLock<Mutex<HashMap<String, watch::Sender<DownloadControl>>>> = OnceLock::new();
static SPEED_LIMIT: OnceLock<Arc<AtomicU64>> = OnceLock::new();

fn job_controls() -> &'static Mutex<HashMap<String, watch::Sender<DownloadControl>>> {
    JOB_CONTROLS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn speed_limit() -> Arc<AtomicU64> {
    SPEED_LIMIT.get_or_init(|| Arc::new(AtomicU64::new(0))).clone()
}

#[tauri::command]
async fn set_speed_limit(bytes_per_second: u64) -> Result<(), String> {
    speed_limit().store(bytes_per_second.min(1_073_741_824), std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

#[tauri::command]
async fn pause_download(job_id: String) -> Result<(), String> {
    let controls = job_controls().lock().map_err(|_| "Download control lock is unavailable")?;
    let sender = controls.get(&job_id).ok_or_else(|| "This download is not active. Resume it from the queue.".to_string())?;
    sender.send(DownloadControl::Paused).map_err(|_| "The download has already stopped. Resume it from the queue.".to_string())
}

#[tauri::command]
async fn cancel_download(job_id: String, target_path: String) -> Result<(), String> {
    let sender = job_controls().lock().map_err(|_| "Download control lock is unavailable")?.remove(&job_id);
    let actively_running = sender.map(|sender| sender.send(DownloadControl::Cancelled).is_ok()).unwrap_or(false);
    if !actively_running {
        let target = PathBuf::from(target_path);
        let _ = tokio::fs::remove_file(partial_download_path(&target)).await;
        let _ = tokio::fs::remove_file(&target).await;
    }
    Ok(())
}

#[tauri::command]
async fn start_download(
    job_id: String,
    url: String,
    filename: String,
    target_path: Option<String>,
    connections: usize,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS download URLs are supported".into());
    }
    let name = safe_filename(&filename, "download.bin");
    let target = match target_path {
        Some(path) => PathBuf::from(path),
        None => reserve_download_path(&default_download_dir().join(name)).await.map_err(|error| error.to_string())?,
    };
    let _ = connections; // Resumable transfers use one stream so their checkpoint is continuous.
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    {
        let mut controls = job_controls().lock().map_err(|_| "Download control lock is unavailable")?;
        controls.insert(job_id.clone(), control_tx);
    }

    let job_for_task = job_id.clone();
    let app_for_task = app.clone();
    let limit = speed_limit();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_url_resumable(&url, &target, None, control_rx, limit, progress_tx).await;
        match result {
            Ok(DownloadOutcome::Completed) => {
                let _ = app_for_task.emit("download-complete", json!({ "jobId": job_for_task, "path": target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Ok(DownloadOutcome::Paused) => {
                let _ = app_for_task.emit("download-paused", json!({ "jobId": job_for_task, "targetPath": target.to_string_lossy() }));
            }
            Ok(DownloadOutcome::Cancelled) => {
                let _ = app_for_task.emit("download-cancelled", json!({ "jobId": job_for_task }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Err(error) => {
                let _ = app_for_task.emit("download-error", json!({ "jobId": job_for_task, "error": error.to_string(), "targetPath": target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
        }
    });
    Ok(target.to_string_lossy().to_string())
}

#[tauri::command]
async fn start_hls_download(
    job_id: String,
    url: String,
    filename: String,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS playlist URLs are supported".into());
    }
    let name = safe_filename(&filename, "video.mp4");
    let job_for_task = job_id.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let progress_app = app.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        match download_hls(&url, &name, None, progress_tx).await {
            Ok(path) => { let _ = app.emit("download-complete", json!({ "jobId": job_for_task, "path": path.to_string_lossy() })); }
            Err(error) => { let _ = app.emit("download-error", json!({ "jobId": job_for_task, "error": error.to_string() })); }
        }
    });
    Ok(job_id)
}

#[tauri::command]
async fn register_native_host(
    browser: String,
    extension_id: String,
    app: tauri::AppHandle,
) -> Result<String, String> {
    #[cfg(target_os = "windows")]
    {
        let browser = browser.trim().to_ascii_lowercase();
        let extension_id = extension_id.trim();
        let (registry_path, mut manifest) = match browser.as_str() {
            "chrome" => (r"HKCU\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host",
                json!({"name":"com.thunderload.host","description":"ThunderLoad local download engine","type":"stdio","allowed_origins":[format!("chrome-extension://{extension_id}/")]})),
            "edge" => (r"HKCU\Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host",
                json!({"name":"com.thunderload.host","description":"ThunderLoad local download engine","type":"stdio","allowed_origins":[format!("chrome-extension://{extension_id}/")]})),
            "brave" => (r"HKCU\Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host",
                json!({"name":"com.thunderload.host","description":"ThunderLoad local download engine","type":"stdio","allowed_origins":[format!("chrome-extension://{extension_id}/")]})),
            "firefox" => (r"HKCU\Software\Mozilla\NativeMessagingHosts\com.thunderload.host",
                json!({"name":"com.thunderload.host","description":"ThunderLoad local download engine","type":"stdio","allowed_extensions":[extension_id]})),
            _ => return Err("Choose Chrome, Edge, Brave or Firefox.".into()),
        };
        if browser == "firefox" {
            if extension_id.is_empty() || extension_id.len() > 128
                || !extension_id.chars().all(|ch| ch.is_ascii_alphanumeric() || "@._+-".contains(ch)) {
                return Err("Enter the exact Firefox add-on ID.".into());
            }
        } else if extension_id.len() != 32 || !extension_id.chars().all(|ch| ('a'..='p').contains(&ch)) {
            return Err("Chromium extension IDs must be exactly 32 characters (a–p).".into());
        }

        let resource_dir = app.path().resource_dir().map_err(|error| error.to_string())?;
        let source = resource_dir.join("native-host").join("thunderload-host.exe");
        if !source.is_file() {
            return Err(format!("Native host binary was not found: {}", source.display()));
        }
        let local_app_data = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from).ok_or_else(|| "LOCALAPPDATA is not available.".to_string())?;
        let install_dir = local_app_data.join("ThunderLoad").join("native-host");
        std::fs::create_dir_all(&install_dir).map_err(|error| error.to_string())?;
        let host_path = install_dir.join("thunderload-host.exe");
        std::fs::copy(&source, &host_path).map_err(|error| format!("Could not install native host: {error}"))?;
        let manifest_path = install_dir.join(format!("{browser}-native-host.json"));
        // Native messaging manifests must contain the absolute executable path.
        manifest["path"] = json!(host_path.to_string_lossy().to_string());
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
        std::fs::write(&manifest_path, bytes).map_err(|error| format!("Could not write native host manifest: {error}"))?;
        let status = Command::new("reg").args([
            "add", registry_path, "/ve", "/t", "REG_SZ", "/d",
            manifest_path.to_string_lossy().as_ref(), "/f"
        ]).status().map_err(|error| format!("Could not register native host in Windows: {error}"))?;
        if !status.success() { return Err("Windows refused to register the ThunderLoad native host.".into()); }
        Ok(format!("{browser} native host registered. Restart the browser and grant Native messaging permission in the extension popup."))
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (browser, extension_id, app);
        Err("Browser native-host setup is currently supported by the Windows installer only.".into())
    }
}

#[tauri::command]
async fn open_download_folder() -> Result<(), String> {
    let folder = default_download_dir();
    std::fs::create_dir_all(&folder).map_err(|error| error.to_string())?;
    #[cfg(target_os = "windows")]
    let result = Command::new("explorer").arg(&folder).spawn();
    #[cfg(target_os = "macos")]
    let result = Command::new("open").arg(&folder).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let result = Command::new("xdg-open").arg(&folder).spawn();
    result.map(|_| ()).map_err(|error| error.to_string())
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![inspect_video_url, start_download, pause_download, cancel_download, set_speed_limit, start_hls_download, register_native_host, open_download_folder])
        .run(tauri::generate_context!())
        .expect("error while running ThunderLoad desktop app");
}

use std::{path::PathBuf, process::Command};
use serde_json::json;
use tauri::{Emitter, Manager};
use thunderload_engine::{default_download_dir, download_hls, download_url, inspect_video, safe_filename};
use tokio::sync::mpsc;

#[tauri::command]
async fn inspect_video_url(url: String, referer: Option<String>) -> Result<Vec<thunderload_engine::VideoVariant>, String> {
    inspect_video(&url, referer.as_deref()).await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn start_download(
    job_id: String,
    url: String,
    filename: String,
    connections: usize,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS download URLs are supported".into());
    }
    let name = safe_filename(&filename, "download.bin");
    let target: PathBuf = default_download_dir().join(name);
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
        match download_url(&url, &target, connections.clamp(1, 16), None, progress_tx).await {
            Ok(path) => { let _ = app.emit("download-complete", json!({ "jobId": job_for_task, "path": path.to_string_lossy() })); }
            Err(error) => { let _ = app.emit("download-error", json!({ "jobId": job_for_task, "error": error.to_string() })); }
        }
    });
    Ok(job_id)
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
        let (registry_path, manifest) = match browser.as_str() {
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
        .invoke_handler(tauri::generate_handler![inspect_video_url, start_download, start_hls_download, register_native_host, open_download_folder])
        .run(tauri::generate_context!())
        .expect("error while running ThunderLoad desktop app");
}

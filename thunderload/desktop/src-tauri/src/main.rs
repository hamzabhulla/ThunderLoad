use std::{collections::HashMap, path::PathBuf, process::Command, sync::{Arc, Mutex, OnceLock}};
use serde_json::{json, Value};
use tauri::{Emitter, Manager};
use thunderload_engine::{default_download_dir, download_hls_resumable, download_url_resumable, hls_work_dir, inspect_video, partial_download_metadata_path, partial_download_path, reserve_download_path, safe_filename, DownloadControl, DownloadOutcome, GlobalRateLimiter};
use tokio::{process::Command as TokioCommand, sync::{mpsc, watch}};

#[tauri::command]
async fn inspect_video_url(url: String, referer: Option<String>) -> Result<Vec<thunderload_engine::VideoVariant>, String> {
    inspect_video(&url, referer.as_deref()).await.map_err(|error| error.to_string())
}


static JOB_CONTROLS: OnceLock<Mutex<HashMap<String, watch::Sender<DownloadControl>>>> = OnceLock::new();
static RATE_LIMITER: OnceLock<Arc<GlobalRateLimiter>> = OnceLock::new();

fn job_controls() -> &'static Mutex<HashMap<String, watch::Sender<DownloadControl>>> {
    JOB_CONTROLS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rate_limiter() -> Arc<GlobalRateLimiter> {
    RATE_LIMITER.get_or_init(|| Arc::new(GlobalRateLimiter::new())).clone()
}

#[tauri::command]
async fn set_speed_limit(bytes_per_second: u64, app: tauri::AppHandle) -> Result<(), String> {
    let limit = bytes_per_second.min(1_073_741_824);
    rate_limiter().set_limit(limit);
    let config_dir = app.path().app_config_dir().map_err(|error| error.to_string())?;
    tokio::fs::create_dir_all(&config_dir).await.map_err(|error| error.to_string())?;
    tokio::fs::write(config_dir.join("speed-limit.bytes"), limit.to_string()).await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn valid_scheduler_job_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64
        && value.chars().all(|character| character.is_ascii_alphanumeric() || character == '-')
}

fn valid_local_datetime_shape(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 16
        && bytes[4] == b'-' && bytes[7] == b'-' && bytes[10] == b'T' && bytes[13] == b':'
        && bytes.iter().enumerate().all(|(index, byte)| {
            [4, 7, 10, 13].contains(&index) || byte.is_ascii_digit()
        })
}

fn powershell_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[tauri::command]
async fn schedule_app_wake(
    job_id: String,
    local_date_time: String,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !valid_scheduler_job_id(&job_id) {
        return Err("Invalid scheduled job identifier.".into());
    }
    if !valid_local_datetime_shape(&local_date_time) {
        return Err("Scheduled time must use YYYY-MM-DDTHH:mm local time.".into());
    }

    #[cfg(target_os = "windows")]
    {
        let executable = std::env::current_exe().map_err(|error| error.to_string())?;
        let executable = powershell_literal(&executable.to_string_lossy());
        let task_name = format!("ThunderLoad-{job_id}");
        let script_dir = app.path().app_config_dir().map_err(|error| error.to_string())?.join("scheduled");
        tokio::fs::create_dir_all(&script_dir).await.map_err(|error| error.to_string())?;
        let script_path = script_dir.join(format!("wake-{job_id}.ps1"));
        let script = format!(
            "$exe = '{executable}'\nif (-not (Get-Process -Name 'ThunderLoad' -ErrorAction SilentlyContinue)) {{ Start-Process -FilePath $exe }}\n"
        );
        tokio::fs::write(&script_path, script).await.map_err(|error| error.to_string())?;

        let task_name_ps = powershell_literal(&task_name);
        let script_path_ps = powershell_literal(&script_path.to_string_lossy());
        let datetime_ps = powershell_literal(&local_date_time);
        let command = format!(
            "$when = [datetime]::ParseExact('{datetime_ps}', 'yyyy-MM-ddTHH:mm', [Globalization.CultureInfo]::InvariantCulture);              if ($when -le (Get-Date)) {{ throw 'The scheduled time must be in the future.' }};              $action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File \"{script_path_ps}\"';              $trigger = New-ScheduledTaskTrigger -Once -At $when;              $settings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries;              $principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited;              Register-ScheduledTask -TaskName '{task_name_ps}' -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null"
        );
        let result = Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", &command])
            .output()
            .map_err(|error| format!("Could not start Windows Task Scheduler: {error}"))?;
        if !result.status.success() {
            let _ = tokio::fs::remove_file(&script_path).await;
            let stderr = String::from_utf8_lossy(&result.stderr).trim().to_string();
            return Err(if stderr.is_empty() {
                "Windows Task Scheduler refused the wake task.".into()
            } else {
                format!("Windows Task Scheduler: {stderr}")
            });
        }
        Ok(format!("Windows wake task registered for {local_date_time}. Wake from sleep depends on your Windows power settings."))
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (local_date_time, app);
        Err("OS-level scheduled launch is currently supported on Windows only.".into())
    }
}

#[tauri::command]
async fn cancel_app_wake(job_id: String, app: tauri::AppHandle) -> Result<(), String> {
    if !valid_scheduler_job_id(&job_id) {
        return Err("Invalid scheduled job identifier.".into());
    }
    #[cfg(target_os = "windows")]
    {
        let task_name = powershell_literal(&format!("ThunderLoad-{job_id}"));
        let command = format!("Unregister-ScheduledTask -TaskName '{task_name}' -Confirm:$false -ErrorAction SilentlyContinue");
        let result = Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", &command])
            .output()
            .map_err(|error| format!("Could not update Windows Task Scheduler: {error}"))?;
        if !result.status.success() {
            return Err(String::from_utf8_lossy(&result.stderr).trim().to_string());
        }
        if let Ok(config_dir) = app.path().app_config_dir() {
            let _ = tokio::fs::remove_file(config_dir.join("scheduled").join(format!("wake-{job_id}.ps1"))).await;
        }
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = app;
        Err("OS-level scheduled launch is currently supported on Windows only.".into())
    }
}

#[tauri::command]
async fn pause_download(job_id: String) -> Result<(), String> {
    let controls = job_controls().lock().map_err(|_| "Download control lock is unavailable")?;
    let sender = controls.get(&job_id).ok_or_else(|| "This download is not active. Resume it from the queue.".to_string())?;
    sender.send(DownloadControl::Paused).map_err(|_| "The download has already stopped. Resume it from the queue.".to_string())
}

#[tauri::command]
async fn cancel_download(job_id: String, target_path: String, app: tauri::AppHandle) -> Result<(), String> {
    let sender = job_controls().lock().map_err(|_| "Download control lock is unavailable")?.remove(&job_id);
    let actively_running = sender.map(|sender| sender.send(DownloadControl::Cancelled).is_ok()).unwrap_or(false);
    if !actively_running {
        let target = PathBuf::from(target_path);
        let _ = tokio::fs::remove_file(partial_download_path(&target)).await;
        let _ = tokio::fs::remove_file(partial_download_metadata_path(&target)).await;
        let _ = tokio::fs::remove_dir_all(hls_work_dir(&target)).await;
        let _ = tokio::fs::remove_file(&target).await;
        // No task is alive to report the cancellation (e.g. the job was
        // already Paused, so its task ended), so emit it here — otherwise
        // the UI would stay on "Cancelling" forever. When the task is
        // alive it emits the event itself, so this never double-emits.
        let _ = app.emit("download-cancelled", json!({ "jobId": job_id }));
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
    let limit = rate_limiter();
    let task_target = target.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_url_resumable(&url, &task_target, None, control_rx, limit, progress_tx).await;
        match result {
            Ok(DownloadOutcome::Completed) => {
                let _ = app_for_task.emit("download-complete", json!({ "jobId": job_for_task, "path": task_target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Ok(DownloadOutcome::Paused) => {
                let _ = app_for_task.emit("download-paused", json!({ "jobId": job_for_task, "targetPath": task_target.to_string_lossy() }));
            }
            Ok(DownloadOutcome::Cancelled) => {
                let _ = app_for_task.emit("download-cancelled", json!({ "jobId": job_for_task }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Err(error) => {
                let _ = app_for_task.emit("download-error", json!({ "jobId": job_for_task, "error": error.to_string(), "targetPath": task_target.to_string_lossy() }));
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
    target_path: Option<String>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS playlist URLs are supported".into());
    }
    let name = safe_filename(&filename, "video.mp4");
    // Resume reuses the paused job's target so the on-disk HLS checkpoint
    // is found; a fresh job reserves a new target, as before. The
    // target_path argument is optional for existing callers. The target
    // path is returned (like start_download) so the frontend learns it
    // at start and can offer Cancel while the job is running.
    let target = match target_path {
        Some(path) => PathBuf::from(path),
        None => reserve_download_path(&default_download_dir().join(name)).await.map_err(|error| error.to_string())?,
    };
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    {
        let mut controls = job_controls().lock().map_err(|_| "Download control lock is unavailable")?;
        controls.insert(job_id.clone(), control_tx);
    }
    let job_for_task = job_id.clone();
    let task_target = target.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let progress_app = app.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_hls_resumable(&url, &task_target, None, control_rx, progress_tx, rate_limiter()).await;
        match result {
            Ok(DownloadOutcome::Completed) => {
                let _ = app.emit("download-complete", json!({ "jobId": job_for_task, "path": task_target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Ok(DownloadOutcome::Paused) => {
                let _ = app.emit("download-paused", json!({ "jobId": job_for_task, "targetPath": task_target.to_string_lossy() }));
            }
            Ok(DownloadOutcome::Cancelled) => {
                let _ = app.emit("download-cancelled", json!({ "jobId": job_for_task }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Err(error) => {
                let _ = app.emit("download-error", json!({ "jobId": job_for_task, "error": error.to_string(), "targetPath": task_target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
        }
    });
    Ok(target.to_string_lossy().to_string())
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
async fn save_extension_file(
    browser: String,
    destination: String,
    app: tauri::AppHandle,
) -> Result<String, String> {
    // Copies a bundled extension package to a user-picked location. The
    // frontend shows the save dialog (dialog plugin); this command only
    // reads the packaged resource and writes the bytes, following the
    // resource_dir pattern used by register_native_host.
    let browser = browser.trim().to_ascii_lowercase();
    let resource = match browser.as_str() {
        "firefox" => "extensions/ThunderLoad-Firefox.xpi",
        "chromium" | "chrome" | "edge" | "brave" | "opera" => "extensions/ThunderLoad-Chromium.zip",
        _ => return Err("Choose Chrome, Edge, Brave, Opera or Firefox.".into()),
    };
    let source = app.path().resource_dir().map_err(|error| error.to_string())?.join(resource);
    if !source.is_file() {
        return Err(format!("Extension package was not found in this install: {}", source.display()));
    }
    let destination = PathBuf::from(destination);
    if destination.file_name().is_none() {
        return Err("Choose a file name for the extension package.".into());
    }
    let bytes = tokio::fs::read(&source).await.map_err(|error| error.to_string())?;
    if let Some(parent) = destination.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| error.to_string())?;
        }
    }
    tokio::fs::write(&destination, bytes).await.map_err(|error| error.to_string())?;
    Ok(destination.to_string_lossy().to_string())
}

#[tauri::command]
async fn start_dash_download(
    job_id: String,
    url: String,
    representation_id: String,
    audio_representation_id: Option<String>,
    filename: String,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS DASH manifests are supported".into());
    }
    if representation_id.trim().is_empty() {
        return Err("A DASH representation must be selected before downloading".into());
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
        match thunderload_engine::download_dash(
            &url, &representation_id, audio_representation_id.as_deref(), &name, None, progress_tx, rate_limiter()
        ).await {
            Ok(path) => {
                let _ = app.emit("download-complete", json!({ "jobId": job_for_task, "path": path.to_string_lossy() }));
            }
            Err(error) => {
                let _ = app.emit("download-error", json!({ "jobId": job_for_task, "error": error.to_string() }));
            }
        }
    });
    Ok(job_id)
}

#[tauri::command]
async fn get_ffmpeg_path(app: tauri::AppHandle) -> Result<Option<String>, String> {
    let path = app.path().app_config_dir().map_err(|error| error.to_string())?.join("ffmpeg.path");
    match tokio::fs::read_to_string(path).await {
        Ok(value) => {
            let value = value.trim().to_string();
            Ok((!value.is_empty()).then_some(value))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

#[tauri::command]
async fn set_ffmpeg_path(path: String, app: tauri::AppHandle) -> Result<String, String> {
    let value = path.trim().to_string();
    let config_file = app.path().app_config_dir()
        .map_err(|error| error.to_string())?.join("ffmpeg.path");
    if value.is_empty() {
        match tokio::fs::remove_file(&config_file).await {
            Ok(()) => {},
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(error) => return Err(error.to_string()),
        }
        std::env::remove_var("THUNDERLOAD_FFMPEG");
        return Ok("Custom FFmpeg path cleared. ThunderLoad will look for ffmpeg on PATH.".into());
    }
    let result = TokioCommand::new(&value).arg("-version").output().await
        .map_err(|error| format!("Could not start FFmpeg at '{value}': {error}"))?;
    if !result.status.success() {
        return Err(format!("FFmpeg returned a non-zero exit code when checking '{value}'."));
    }
    let version = String::from_utf8_lossy(&result.stdout).lines().next().unwrap_or("FFmpeg verified").to_string();
    let parent = config_file.parent().ok_or_else(|| "Application config directory is unavailable".to_string())?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| error.to_string())?;
    tokio::fs::write(&config_file, &value).await.map_err(|error| error.to_string())?;
    std::env::set_var("THUNDERLOAD_FFMPEG", &value);
    Ok(format!("{version} · saved in ThunderLoad settings"))
}

#[tauri::command]
async fn get_native_downloads() -> Result<Vec<Value>, String> {
    let directory = default_download_dir().join(".native-jobs");
    if !tokio::fs::try_exists(&directory).await.unwrap_or(false) {
        return Ok(Vec::new());
    }
    let mut entries = tokio::fs::read_dir(&directory).await.map_err(|error| error.to_string())?;
    let mut jobs = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|error| error.to_string())? {
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") { continue; }
        let content = match tokio::fs::read_to_string(entry.path()).await {
            Ok(content) => content,
            Err(_) => continue,
        };
        if let Ok(mut job) = serde_json::from_str::<Value>(&content) {
            let status = job.get("status").and_then(Value::as_str).unwrap_or("");
            let updated = job.get("updatedAt").and_then(Value::as_u64).unwrap_or(0);
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default().as_millis() as u64;
            // The companion refreshes active, paused and retryable jobs every
            // ten seconds. A stale heartbeat means the native host has exited;
            // direct jobs can then hand their checkpoint to the desktop engine.
            if ["Downloading", "Paused", "Failed"].contains(&status)
                && updated > 0 && now.saturating_sub(updated) > 45_000 {
                job["status"] = json!("Interrupted");
                job["detail"] = json!("Browser companion disconnected. Resume can continue the saved direct-download checkpoint.");
                job["error"] = json!("The native host stopped before this job finished.");
                job["updatedAt"] = json!(now);
                if let Ok(bytes) = serde_json::to_vec(&job) {
                    let temporary = entry.path().with_extension("json.tmp");
                    if tokio::fs::write(&temporary, bytes).await.is_ok() {
                        let _ = tokio::fs::rename(&temporary, entry.path()).await;
                    }
                }
            }
            jobs.push(job);
        }
    }
    jobs.sort_by_key(|job| std::cmp::Reverse(job.get("createdAt").and_then(Value::as_u64).unwrap_or(0)));
    Ok(jobs)
}

#[tauri::command]
async fn control_native_download(job_id: String, action: String) -> Result<(), String> {
    if job_id.is_empty() || job_id.len() > 64
        || !job_id.chars().all(|character| character.is_ascii_alphanumeric() || character == '-') {
        return Err("Invalid browser download identifier.".into());
    }
    if !["pause", "resume", "cancel"].contains(&action.as_str()) {
        return Err("Unknown native download control action.".into());
    }
    let directory = default_download_dir().join(".native-jobs");
    let job_path = directory.join(format!("{job_id}.json"));
    let bytes = tokio::fs::read(&job_path).await
        .map_err(|_| "The browser download record is no longer available.".to_string())?;
    let job: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let status = job.get("status").and_then(Value::as_str).unwrap_or("");
    let allowed = match action.as_str() {
        "pause" => status == "Downloading",
        "resume" => status == "Paused" || status == "Failed",
        "cancel" => status == "Downloading" || status == "Paused" || status == "Failed",
        _ => false,
    };
    if !allowed {
        return Err(format!("Cannot {action} a browser download while it is {status}."));
    }

    tokio::fs::create_dir_all(&directory).await.map_err(|error| error.to_string())?;
    let command_path = directory.join(format!("{job_id}.control"));
    let temporary = directory.join(format!("{job_id}.control.tmp"));
    tokio::fs::write(&temporary, action.as_bytes()).await.map_err(|error| error.to_string())?;
    if tokio::fs::try_exists(&command_path).await.unwrap_or(false) {
        tokio::fs::remove_file(&command_path).await.map_err(|error| error.to_string())?;
    }
    tokio::fs::rename(&temporary, &command_path).await.map_err(|error| error.to_string())?;
    Ok(())
}

#[tauri::command]
async fn dismiss_native_download(job_id: String) -> Result<(), String> {
    if job_id.is_empty() || job_id.len() > 64
        || !job_id.chars().all(|character| character.is_ascii_alphanumeric() || character == '-') {
        return Err("Invalid browser download identifier.".into());
    }
    let directory = default_download_dir().join(".native-jobs");
    for path in [
        directory.join(format!("{job_id}.json")),
        directory.join(format!("{job_id}.control")),
        directory.join(format!("{job_id}.control.tmp")),
    ] {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {},
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
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
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            if let Ok(config_dir) = app.path().app_config_dir() {
                if let Ok(value) = std::fs::read_to_string(config_dir.join("ffmpeg.path")) {
                    let value = value.trim();
                    if !value.is_empty() { std::env::set_var("THUNDERLOAD_FFMPEG", value); }
                }
                if let Ok(value) = std::fs::read_to_string(config_dir.join("speed-limit.bytes")) {
                    if let Ok(limit) = value.trim().parse::<u64>() { rate_limiter().set_limit(limit); }
                }
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![inspect_video_url, start_download, pause_download, cancel_download, set_speed_limit, schedule_app_wake, cancel_app_wake, start_hls_download, start_dash_download, get_ffmpeg_path, set_ffmpeg_path, get_native_downloads, control_native_download, dismiss_native_download, register_native_host, open_download_folder, save_extension_file])
        .run(tauri::generate_context!())
        .expect("error while running ThunderLoad desktop app");
}


#[cfg(test)]
mod scheduler_tests {
    use super::{powershell_literal, valid_local_datetime_shape, valid_scheduler_job_id};

    #[test]
    fn scheduler_job_ids_allow_only_short_safe_identifiers() {
        assert!(valid_scheduler_job_id("9fa8dd1a-69a1-43d2-9fa4-10b4c2b0ad32"));
        assert!(valid_scheduler_job_id("download-42"));
        assert!(!valid_scheduler_job_id(""));
        assert!(!valid_scheduler_job_id("name'; Start-Process calc; '"));
        assert!(!valid_scheduler_job_id("../outside"));
        assert!(!valid_scheduler_job_id("a".repeat(65).as_str()));
    }

    #[test]
    fn schedule_datetime_requires_the_local_minute_format() {
        assert!(valid_local_datetime_shape("2026-11-12T09:35"));
        assert!(!valid_local_datetime_shape("2026/11/12 09:35"));
        assert!(!valid_local_datetime_shape("2026-11-12T09:3"));
        assert!(!valid_local_datetime_shape("2026-11-12T09:35Z"));
    }

    #[test]
    fn powershell_literals_escape_single_quotes() {
        assert_eq!(powershell_literal("C:\\Tools\\User's App\\ThunderLoad.exe"), "C:\\Tools\\User''s App\\ThunderLoad.exe");
    }
}

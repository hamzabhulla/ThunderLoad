use std::{path::PathBuf, process::Command};
use serde_json::json;
use tauri::Emitter;
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
        .invoke_handler(tauri::generate_handler![inspect_video_url, start_download, start_hls_download, open_download_folder])
        .run(tauri::generate_context!())
        .expect("error while running ThunderLoad desktop app");
}

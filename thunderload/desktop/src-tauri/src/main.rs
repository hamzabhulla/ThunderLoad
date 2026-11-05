use std::path::PathBuf;
use serde_json::json;
use tauri::Emitter;
use thunderload_engine::{default_download_dir, download_url, inspect_video, safe_filename};
use tokio::sync::mpsc;

#[tauri::command]
async fn inspect_video_url(url: String, referer: Option<String>) -> Result<Vec<thunderload_engine::VideoVariant>, String> {
    inspect_video(&url, referer.as_deref()).await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn start_download(
    url: String,
    filename: String,
    connections: usize,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only http and https download URLs are supported".into());
    }
    let name = safe_filename(&filename, "download.bin");
    let target: PathBuf = default_download_dir().join(name);
    let job_id = uuid::Uuid::new_v4().to_string();
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

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![inspect_video_url, start_download])
        .run(tauri::generate_context!())
        .expect("error while running ThunderLoad desktop app");
}

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::mpsc};
use thunderload_engine::{default_download_dir, download_hls, download_url, inspect_video, safe_filename};

const MAX_MESSAGE: usize = 64 * 1024 * 1024;

async fn read_message<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Value>> {
    let mut header = [0_u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {},
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_le_bytes(header) as usize;
    if length == 0 || length > MAX_MESSAGE { return Err(anyhow!("Invalid native messaging frame size")); }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).await?;
    Ok(Some(serde_json::from_slice(&body)?))
}

async fn write_message<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_MESSAGE { return Err(anyhow!("Outgoing native message is too large")); }
    writer.write_all(&(body.len() as u32).to_le_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

fn output_name(value: Option<&Value>, fallback: &str) -> String {
    safe_filename(value.and_then(Value::as_str).unwrap_or(fallback), fallback)
}

fn timestamp_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_millis() as u64
}

async fn save_native_job_state(state: &Value) -> Result<()> {
    let root = default_download_dir().join(".native-jobs");
    tokio::fs::create_dir_all(&root).await?;
    let id = safe_filename(state.get("id").and_then(Value::as_str).unwrap_or("job"), "job");
    let destination = root.join(format!("{id}.json"));
    let temporary = root.join(format!("{id}.json.tmp"));
    let bytes = serde_json::to_vec(state)?;
    tokio::fs::write(&temporary, bytes).await?;
    if tokio::fs::try_exists(&destination).await.unwrap_or(false) {
        tokio::fs::remove_file(&destination).await?;
    }
    tokio::fs::rename(&temporary, &destination).await?;
    Ok(())
}

fn native_job_state(id: &str, name: &str, url: &str, kind: &str) -> Value {
    json!({
        "id": id, "name": name, "url": url, "kind": kind,
        "status": "Downloading", "percent": 0.0, "downloaded": 0,
        "detail": "Started from browser extension", "createdAt": timestamp_ms(),
        "updatedAt": timestamp_ms()
    })
}

fn apply_native_progress(state: &mut Value, progress: &thunderload_engine::DownloadProgress) {
    state["downloaded"] = json!(progress.downloaded);
    state["total"] = progress.total.map(Value::from).unwrap_or(Value::Null);
    state["percent"] = progress.percent.map(Value::from).unwrap_or(Value::from(0));
    state["detail"] = json!(progress.phase);
    state["updatedAt"] = json!(timestamp_ms());
}

#[tokio::main]
async fn main() -> Result<()> {
    let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel::<Value>();
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Value>();

    tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        loop {
            match read_message(&mut stdin).await {
                Ok(Some(message)) => if incoming_tx.send(message).is_err() { break; },
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = outgoing_rx.recv().await {
            if write_message(&mut stdout, &message).await.is_err() { break; }
        }
    });

    while let Some(message) = incoming_rx.recv().await {
        let id = message.get("id").and_then(Value::as_str).unwrap_or("unknown").to_string();
        let action = message.get("action").and_then(Value::as_str).unwrap_or("");
        match action {
            "ping" => {
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"name": "ThunderLoad Native Host", "version": "0.1.0"}}));
            }
            "video.inspect" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let tx = outgoing_tx.clone();
                tokio::spawn(async move {
                    match inspect_video(&url, referer.as_deref()).await {
                        Ok(variants) => { let _ = tx.send(json!({"id": id, "ok": true, "data": {"variants": variants}})); }
                        Err(error) => { let _ = tx.send(json!({"id": id, "ok": false, "error": error.to_string()})); }
                    }
                });
            }
            "download.start" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let filename = output_name(message.get("filename"), "download.bin");
                let job_id = uuid::Uuid::new_v4().to_string();
                let target = default_download_dir().join(filename);
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let tx = outgoing_tx.clone();
                let mut state = native_job_state(&job_id, &filename, &url, "direct");
                let _ = save_native_job_state(&state).await;
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"accepted": true, "jobId": job_id}}));
                tokio::spawn(async move {
                    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                    let tx_progress = tx.clone();
                    let progress_job = job_id.clone();
                    let progress_state = state.clone();
                    tokio::spawn(async move {
                        let mut state = progress_state;
                        while let Some(progress) = progress_rx.recv().await {
                            apply_native_progress(&mut state, &progress);
                            let _ = save_native_job_state(&state).await;
                            let _ = tx_progress.send(json!({"event": "download.progress", "jobId": progress_job, "progress": progress}));
                        }
                    });
                    match download_url(&url, &target, 8, referer.as_deref(), progress_tx).await {
                        Ok(path) => {
                            state["status"] = json!("Completed");
                            state["percent"] = json!(100);
                            state["detail"] = json!("Download complete");
                            state["targetPath"] = json!(path.to_string_lossy().to_string());
                            state["updatedAt"] = json!(timestamp_ms());
                            let _ = save_native_job_state(&state).await;
                            let _ = tx.send(json!({"event": "download.complete", "jobId": job_id, "path": path.to_string_lossy()}));
                        }
                        Err(error) => {
                            state["status"] = json!("Failed");
                            state["error"] = json!(error.to_string());
                            state["detail"] = json!("Download failed");
                            state["updatedAt"] = json!(timestamp_ms());
                            let _ = save_native_job_state(&state).await;
                            let _ = tx.send(json!({"event": "download.error", "jobId": job_id, "error": error.to_string()}));
                        }
                    }
                });
            }
            "video.download" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let filename = output_name(message.get("filename"), "video");
                let job_id = uuid::Uuid::new_v4().to_string();
                let tx = outgoing_tx.clone();
                let mut state = native_job_state(&job_id, &filename, &url, "hls");
                let _ = save_native_job_state(&state).await;
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"accepted": true, "jobId": job_id}}));
                tokio::spawn(async move {
                    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                    let tx_progress = tx.clone();
                    let progress_job = job_id.clone();
                    let progress_state = state.clone();
                    tokio::spawn(async move {
                        let mut state = progress_state;
                        while let Some(progress) = progress_rx.recv().await {
                            apply_native_progress(&mut state, &progress);
                            let _ = save_native_job_state(&state).await;
                            let _ = tx_progress.send(json!({"event": "download.progress", "jobId": progress_job, "progress": progress}));
                        }
                    });
                    match download_hls(&url, &filename, referer.as_deref(), progress_tx).await {
                        Ok(path) => {
                            state["status"] = json!("Completed");
                            state["percent"] = json!(100);
                            state["detail"] = json!("Video download complete");
                            state["targetPath"] = json!(path.to_string_lossy().to_string());
                            state["updatedAt"] = json!(timestamp_ms());
                            let _ = save_native_job_state(&state).await;
                            let _ = tx.send(json!({"event": "download.complete", "jobId": job_id, "path": path.to_string_lossy()}));
                        }
                        Err(error) => {
                            state["status"] = json!("Failed");
                            state["error"] = json!(error.to_string());
                            state["detail"] = json!("Video download failed");
                            state["updatedAt"] = json!(timestamp_ms());
                            let _ = save_native_job_state(&state).await;
                            let _ = tx.send(json!({"event": "download.error", "jobId": job_id, "error": error.to_string()}));
                        }
                    }
                });
            }
            _ => {
                let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Unknown action"}));
            }
        }
    }
    Ok(())
}

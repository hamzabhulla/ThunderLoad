#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
// Release builds on Windows must not show a console window: Chrome spawns
// this host for native messaging, and without this attribute the host is
// built with the console subsystem, so a black console window appears
// whenever the extension connects. The native-messaging protocol talks
// over inherited stdin/stdout pipes (read_message/write_message below),
// which keep working in a windows-subsystem binary — it just gets no
// console window. Debug builds keep the console for local testing.
use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::{Arc, Mutex, OnceLock}};
use serde_json::{json, Value};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{mpsc, watch}};
use thunderload_engine::{check_ytdlp_update, dash_output_extension, dash_work_dir, default_download_dir, download_dash_resumable, download_hls_resumable, download_page, download_url_resumable, ensure_tool, error_text, hls_output_extension, hls_work_dir, inspect_page, inspect_video, partial_download_metadata_path, partial_download_path, reserve_download_path, safe_filename, tool_status, tools_dir, DownloadControl, DownloadOutcome, GlobalRateLimiter, HlsSelection, SessionHeaders, ToolDownloadProgress, ToolsMissing, YtDlpTool};

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

/// Builds the browser session for one native job from the message's
/// `session` object plus the top-level `referer` field.
///
/// SECURITY — the handoff is a bearer credential (the browser's cookies
/// for the download URL). It is validated against the download URL here
/// (cookies only ever go to their own origin; see `SessionHeaders` in
/// the engine) and the engine re-checks the origin on every request.
/// An invalid bundle is dropped and the download proceeds without it,
/// so an authenticated server fails visibly instead of silently
/// fetching the wrong thing. The session is never logged and never
/// leaves this machine: it is stored only in the job's own state file
/// (`.native-jobs/<id>.json`, read back by the desktop handover) and
/// in the job's checkpoint / jobs.json row.
fn message_session(message: &Value, url: &str, referer: Option<String>) -> Option<SessionHeaders> {
    let mut session: SessionHeaders = message.get("session")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    // The top-level referer is authoritative: it overwrites any referer
    // already in the bundle, and a non-http(s) value is ignored.
    if let Some(referer) = referer {
        if let Some(with) = SessionHeaders::with_referer(Some(referer.as_str())) {
            session.referer = with.referer;
        }
    }
    if session.validate(url).is_err() {
        return None;
    }
    if session.is_empty() {
        return None;
    }
    Some(session)
}

async fn wait_native_job_control(
    receiver: &mut watch::Receiver<DownloadControl>,
    state: &mut Value,
    consume_pending_change: bool,
) -> Option<DownloadControl> {
    if consume_pending_change {
        if receiver.changed().await.is_err() { return None; }
        match *receiver.borrow() {
            DownloadControl::Running => return Some(DownloadControl::Running),
            DownloadControl::Cancelled => return Some(DownloadControl::Cancelled),
            DownloadControl::Paused => {}
        }
    } else if *receiver.borrow() == DownloadControl::Cancelled {
        return Some(DownloadControl::Cancelled);
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                state["updatedAt"] = json!(timestamp_ms());
                let _ = save_native_job_state(state).await;
            }
            changed = receiver.changed() => {
                if changed.is_err() { return None; }
                match *receiver.borrow() {
                    DownloadControl::Running => return Some(DownloadControl::Running),
                    DownloadControl::Cancelled => return Some(DownloadControl::Cancelled),
                    DownloadControl::Paused => {}
                }
            }
        }
    }
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

async fn load_native_job_state(id: &str, fallback: Value) -> Value {
    let safe_id = safe_filename(id, "job");
    let path = default_download_dir().join(".native-jobs").join(format!("{safe_id}.json"));
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or(fallback),
        Err(_) => fallback,
    }
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

static RATE_LIMITER: OnceLock<Arc<GlobalRateLimiter>> = OnceLock::new();
static NATIVE_CONTROLS: OnceLock<Mutex<HashMap<String, watch::Sender<DownloadControl>>>> = OnceLock::new();

fn native_controls() -> &'static Mutex<HashMap<String, watch::Sender<DownloadControl>>> {
    NATIVE_CONTROLS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn cleanup_native_partial(target: &std::path::Path) {
    let _ = tokio::fs::remove_file(partial_download_path(target)).await;
    let _ = tokio::fs::remove_file(partial_download_metadata_path(target)).await;
    let _ = tokio::fs::remove_dir_all(hls_work_dir(target)).await;
    let _ = tokio::fs::remove_dir_all(dash_work_dir(target)).await;
    let _ = tokio::fs::remove_file(target).await;
}

fn start_native_job_control_watcher() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            interval.tick().await;
            let directory = default_download_dir().join(".native-jobs");
            let Ok(mut entries) = tokio::fs::read_dir(&directory).await else { continue; };
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.path().extension().and_then(|value| value.to_str()) != Some("control") {
                    continue;
                }
                let Some(job_id) = entry.path().file_stem().and_then(|value| value.to_str()).map(str::to_owned) else {
                    let _ = tokio::fs::remove_file(entry.path()).await;
                    continue;
                };
                let action = tokio::fs::read_to_string(entry.path()).await.unwrap_or_default();
                let _ = tokio::fs::remove_file(entry.path()).await;
                let control = match action.trim() {
                    "pause" => Some(DownloadControl::Paused),
                    "resume" => Some(DownloadControl::Running),
                    "cancel" => Some(DownloadControl::Cancelled),
                    _ => None,
                };
                if let Some(control) = control {
                    if let Ok(controls) = native_controls().lock() {
                        if let Some(sender) = controls.get(&job_id) {
                            let _ = sender.send(control);
                        }
                    }
                }
            }
        }
    });
}

fn rate_limiter() -> Arc<GlobalRateLimiter> {
    RATE_LIMITER.get_or_init(|| Arc::new(GlobalRateLimiter::new())).clone()
}

/// Where the desktop app keeps its configuration (ffmpeg.path,
/// speed-limit.bytes), in Tauri's `app_config_dir` layout, so the host
/// reads exactly the files the desktop writes. The pure `_from` form is
/// what the tests pin; this wrapper reads the live environment.
fn desktop_config_dir_from(
    appdata: Option<&std::path::Path>,
    xdg_config_home: Option<&std::path::Path>,
    home: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    #[cfg(target_os = "windows")]
    {
        let _ = (xdg_config_home, home);
        appdata.map(|dir| dir.join("com.thunderload.desktop"))
    }
    #[cfg(target_os = "linux")]
    {
        let _ = appdata;
        xdg_config_home
            .filter(|dir| !dir.as_os_str().is_empty())
            .map(|dir| dir.join("com.thunderload.desktop"))
            .or_else(|| home.map(|dir| dir.join(".config").join("com.thunderload.desktop")))
    }
    #[cfg(target_os = "macos")]
    {
        let _ = (appdata, xdg_config_home);
        home.map(|dir| {
            dir.join("Library")
                .join("Application Support")
                .join("com.thunderload.desktop")
        })
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        let _ = (appdata, xdg_config_home, home);
        None
    }
}

fn desktop_config_dir() -> Option<std::path::PathBuf> {
    desktop_config_dir_from(
        std::env::var_os("APPDATA")
            .as_deref()
            .map(std::path::Path::new),
        std::env::var_os("XDG_CONFIG_HOME")
            .as_deref()
            .map(std::path::Path::new),
        std::env::var_os("HOME")
            .as_deref()
            .map(std::path::Path::new),
    )
}

fn load_desktop_settings() -> bool {
    let mut has_saved_ffmpeg = false;
    if let Some(config_dir) = desktop_config_dir() {
        if let Ok(value) = std::fs::read_to_string(config_dir.join("ffmpeg.path")) {
            let value = value.trim();
            if !value.is_empty() {
                std::env::set_var("THUNDERLOAD_FFMPEG", value);
                has_saved_ffmpeg = true;
            }
        }
        if let Ok(value) = std::fs::read_to_string(config_dir.join("speed-limit.bytes")) {
            if let Ok(limit) = value.trim().parse::<u64>() {
                rate_limiter().set_limit(limit);
            }
        }
    }
    has_saved_ffmpeg
}

fn start_desktop_settings_watcher(mut has_saved_ffmpeg: bool) {
    let limiter = rate_limiter();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            let Some(config_dir) = desktop_config_dir() else {
                continue;
            };
            if let Ok(value) = std::fs::read_to_string(config_dir.join("speed-limit.bytes")) {
                if let Ok(limit) = value.trim().parse::<u64>() {
                    limiter.set_limit(limit);
                }
            }
            match std::fs::read_to_string(config_dir.join("ffmpeg.path")) {
                Ok(value) if !value.trim().is_empty() => {
                    std::env::set_var("THUNDERLOAD_FFMPEG", value.trim());
                    has_saved_ffmpeg = true;
                }
                Ok(_) => {
                    std::env::remove_var("THUNDERLOAD_FFMPEG");
                    has_saved_ffmpeg = false;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && has_saved_ffmpeg => {
                    // The user cleared the saved setting while this companion was running.
                    std::env::remove_var("THUNDERLOAD_FFMPEG");
                    has_saved_ffmpeg = false;
                }
                Err(_) => {}
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<()> {
    let has_saved_ffmpeg = load_desktop_settings();
    start_desktop_settings_watcher(has_saved_ffmpeg);
    start_native_job_control_watcher();
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
                // In-memory only: inspect never writes a state file.
                let session = message_session(&message, &url, referer);
                let tx = outgoing_tx.clone();
                tokio::spawn(async move {
                    match inspect_video(&url, session.as_ref()).await {
                        Ok(variants) => { let _ = tx.send(json!({"id": id, "ok": true, "data": {"variants": variants}})); }
                        Err(error) => { let _ = tx.send(json!({"id": id, "ok": false, "error": error.to_string()})); }
                    }
                });
            }
            "download.start" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let filename = output_name(message.get("filename"), "download.bin");
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let session = message_session(&message, &url, referer);
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Only HTTP and HTTPS download URLs are supported"}));
                    continue;
                }
                let target = match reserve_download_path(&default_download_dir().join(&filename)).await {
                    Ok(path) => path,
                    Err(error) => {
                        let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        continue;
                    }
                };
                let job_id = uuid::Uuid::new_v4().to_string();
                let (control_tx, mut control_rx) = watch::channel(DownloadControl::Running);
                if let Ok(mut controls) = native_controls().lock() {
                    controls.insert(job_id.clone(), control_tx);
                } else {
                    cleanup_native_partial(&target).await;
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Native download control registry is unavailable"}));
                    continue;
                }

                let tx = outgoing_tx.clone();
                let mut state = native_job_state(&job_id, &filename, &url, "direct");
                state["targetPath"] = json!(target.to_string_lossy().to_string());
                if let Some(session) = &session {
                    state["session"] = serde_json::to_value(session).unwrap_or(Value::Null);
                }
                let _ = save_native_job_state(&state).await;
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"accepted": true, "jobId": job_id}}));
                tokio::spawn(async move {
                    'job: loop {
                        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                        let tx_progress = tx.clone();
                        let progress_job = job_id.clone();
                        let mut progress_state = load_native_job_state(&job_id, state.clone()).await;
                        let progress_handle = tokio::spawn(async move {
                            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
                            loop {
                                tokio::select! {
                                    _ = heartbeat.tick() => {
                                        progress_state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&progress_state).await;
                                    }
                                    progress = progress_rx.recv() => match progress {
                                        Some(progress) => {
                                            apply_native_progress(&mut progress_state, &progress);
                                            let _ = save_native_job_state(&progress_state).await;
                                            let _ = tx_progress.send(json!({"event": "download.progress", "jobId": progress_job, "progress": progress}));
                                        }
                                        None => break,
                                    }
                                }
                            }
                        });

                        let result = download_url_resumable(
                            &url, &target, session.as_ref(), control_rx.clone(),
                            rate_limiter(), progress_tx
                        ).await;
                        let _ = progress_handle.await;

                        match result {
                            Ok(DownloadOutcome::Completed) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Completed");
                                state["percent"] = json!(100);
                                state["detail"] = json!("Download complete");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.complete", "jobId": job_id, "path": target.to_string_lossy()}));
                                break 'job;
                            }
                            Ok(DownloadOutcome::Cancelled) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Cancelled");
                                state["detail"] = json!("Cancelled · partial file removed");
                                state["targetPath"] = Value::Null;
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                break 'job;
                            }
                            Ok(DownloadOutcome::Paused) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Paused");
                                state["detail"] = json!("Paused · partial checkpoint saved");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.paused", "jobId": job_id}));
                                match wait_native_job_control(&mut control_rx, &mut state, true).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["detail"] = json!("Resuming saved checkpoint");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial checkpoint kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                            Err(error) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Failed");
                                state["error"] = json!(error_text(&error));
                                state["detail"] = json!("Transfer interrupted · partial checkpoint kept");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.error", "jobId": job_id, "error": error_text(&error)}));
                                // Retain the native control channel so the desktop
                                // queue can ask this browser job to Resume its checkpoint.
                                match wait_native_job_control(&mut control_rx, &mut state, false).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["error"] = Value::Null;
                                        state["detail"] = json!("Retrying from saved checkpoint");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial checkpoint kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                        }
                    }
                    if let Ok(mut controls) = native_controls().lock() {
                        controls.remove(&job_id);
                    }
                });
            }
            "video.download" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                // Slice 4 (Gemini claim 4): the extension sends the
                // selected variant's media playlist URL as `url` and the
                // master playlist URL as `masterUrl` (Slice 3's yt-dlp
                // pages have no master, so masterUrl is absent there).
                // The engine resolves the master with the selection, so
                // the audio rendition the user picked is downloaded and
                // muxed in instead of silently dropped. A bare media
                // playlist (no masterUrl, or masterUrl == url) keeps the
                // old first-variant behaviour.
                let master_url = message.get("masterUrl").and_then(Value::as_str)
                    .filter(|value| !value.is_empty()).map(str::to_string)
                    .unwrap_or_else(|| url.clone());
                let audio_representation_id = message.get("audioRepresentationId").and_then(Value::as_str)
                    .filter(|value| !value.is_empty()).map(str::to_string);
                let selection = HlsSelection {
                    media_url: if master_url != url { Some(url.clone()) } else { None },
                    audio_group: audio_representation_id.clone(),
                };
                let session = message_session(&message, &master_url, referer);
                let filename = output_name(message.get("filename"), "video");
                if !(master_url.starts_with("http://") || master_url.starts_with("https://")) {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Only HTTP and HTTPS playlist URLs are supported"}));
                    continue;
                }
                // Resolve the playlist first so the reserved target gets
                // the right extension (ts for plain TS playlists, mp4 for
                // fMP4) — the one-shot download did this, and reserving
                // from the caller's filename alone would misname a TS
                // stream as .mp4.
                let extension = match hls_output_extension(&master_url, &selection, session.as_ref()).await {
                    Ok(extension) => extension,
                    Err(error) => {
                        let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        continue;
                    }
                };
                let stem = std::path::Path::new(&filename).file_stem().and_then(|value| value.to_str()).unwrap_or("video");
                let target = match reserve_download_path(&default_download_dir().join(format!("{stem}.{extension}"))).await {
                    Ok(path) => path,
                    Err(error) => {
                        let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        continue;
                    }
                };
                let job_id = uuid::Uuid::new_v4().to_string();
                let (control_tx, mut control_rx) = watch::channel(DownloadControl::Running);
                if let Ok(mut controls) = native_controls().lock() {
                    controls.insert(job_id.clone(), control_tx);
                } else {
                    cleanup_native_partial(&target).await;
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Native download control registry is unavailable"}));
                    continue;
                }

                let tx = outgoing_tx.clone();
                // The stored URL is the MASTER playlist, and the selection
                // rides in the state: a desktop resume of this native job
                // re-resolves the master with the same variant and audio
                // instead of falling back to the first variant.
                let mut state = native_job_state(&job_id, &filename, &master_url, "hls");
                state["targetPath"] = json!(target.to_string_lossy().to_string());
                if let Some(media_url) = &selection.media_url {
                    state["representationId"] = json!(media_url);
                }
                if let Some(audio_group) = &selection.audio_group {
                    state["audioRepresentationId"] = json!(audio_group);
                }
                if let Some(session) = &session {
                    state["session"] = serde_json::to_value(session).unwrap_or(Value::Null);
                }
                let _ = save_native_job_state(&state).await;
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"accepted": true, "jobId": job_id}}));
                tokio::spawn(async move {
                    'job: loop {
                        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                        let tx_progress = tx.clone();
                        let progress_job = job_id.clone();
                        let mut progress_state = load_native_job_state(&job_id, state.clone()).await;
                        let progress_handle = tokio::spawn(async move {
                            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
                            loop {
                                tokio::select! {
                                    _ = heartbeat.tick() => {
                                        progress_state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&progress_state).await;
                                    }
                                    progress = progress_rx.recv() => match progress {
                                        Some(progress) => {
                                            apply_native_progress(&mut progress_state, &progress);
                                            let _ = save_native_job_state(&progress_state).await;
                                            let _ = tx_progress.send(json!({"event": "download.progress", "jobId": progress_job, "progress": progress}));
                                        }
                                        None => break,
                                    }
                                }
                            }
                        });

                        let result = download_hls_resumable(
                            &master_url, &selection, &target, session.as_ref(), control_rx.clone(),
                            progress_tx, rate_limiter()
                        ).await;
                        let _ = progress_handle.await;

                        match result {
                            Ok(DownloadOutcome::Completed) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Completed");
                                state["percent"] = json!(100);
                                state["detail"] = json!("Video download complete");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.complete", "jobId": job_id, "path": target.to_string_lossy()}));
                                break 'job;
                            }
                            Ok(DownloadOutcome::Cancelled) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Cancelled");
                                state["detail"] = json!("Cancelled · partial file removed");
                                state["targetPath"] = Value::Null;
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                break 'job;
                            }
                            Ok(DownloadOutcome::Paused) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Paused");
                                state["detail"] = json!("Paused · partial checkpoint saved");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.paused", "jobId": job_id}));
                                match wait_native_job_control(&mut control_rx, &mut state, true).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["detail"] = json!("Resuming saved checkpoint");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial checkpoint kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                            Err(error) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Failed");
                                state["error"] = json!(error_text(&error));
                                state["detail"] = json!("Video download interrupted · partial checkpoint kept");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.error", "jobId": job_id, "error": error_text(&error)}));
                                // Retain the native control channel so the desktop
                                // queue can ask this browser job to Resume its checkpoint.
                                match wait_native_job_control(&mut control_rx, &mut state, false).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["error"] = Value::Null;
                                        state["detail"] = json!("Retrying from saved checkpoint");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial checkpoint kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                        }
                    }
                    if let Ok(mut controls) = native_controls().lock() {
                        controls.remove(&job_id);
                    }
                });
            }
            "dash.download" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let session = message_session(&message, &url, referer);
                let filename = output_name(message.get("filename"), "video.mp4");
                let representation_id = message.get("representationId").and_then(Value::as_str).unwrap_or("").to_string();
                // The extension sends "" when there is no separate audio
                // rendition; an empty id must mean "no audio", not a
                // representation literally named "" (the old code passed
                // Some("") into the engine, so every video-only DASH
                // download from the browser failed looking for it).
                let audio_representation_id = message.get("audioRepresentationId").and_then(Value::as_str)
                    .filter(|value| !value.is_empty()).map(str::to_string);
                if representation_id.is_empty() {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Missing DASH representationId"}));
                    continue;
                }
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Only HTTP and HTTPS DASH manifests are supported"}));
                    continue;
                }
                let extension = match dash_output_extension(&url, &representation_id, audio_representation_id.as_deref(), session.as_ref()).await {
                    Ok(extension) => extension,
                    Err(error) => {
                        let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        continue;
                    }
                };
                let stem = std::path::Path::new(&filename).file_stem().and_then(|value| value.to_str()).unwrap_or("video");
                let target = match reserve_download_path(&default_download_dir().join(format!("{stem}.{extension}"))).await {
                    Ok(path) => path,
                    Err(error) => {
                        let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        continue;
                    }
                };
                let job_id = uuid::Uuid::new_v4().to_string();
                let (control_tx, mut control_rx) = watch::channel(DownloadControl::Running);
                if let Ok(mut controls) = native_controls().lock() {
                    controls.insert(job_id.clone(), control_tx);
                } else {
                    cleanup_native_partial(&target).await;
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Native download control registry is unavailable"}));
                    continue;
                }

                let tx = outgoing_tx.clone();
                let mut state = native_job_state(&job_id, &filename, &url, "dash");
                state["targetPath"] = json!(target.to_string_lossy().to_string());
                if let Some(session) = &session {
                    state["session"] = serde_json::to_value(session).unwrap_or(Value::Null);
                }
                state["representationId"] = json!(representation_id);
                state["audioRepresentationId"] = audio_representation_id.clone().map(Value::from).unwrap_or(Value::Null);
                let _ = save_native_job_state(&state).await;
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"accepted": true, "jobId": job_id}}));
                tokio::spawn(async move {
                    'job: loop {
                        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                        let tx_progress = tx.clone();
                        let progress_job = job_id.clone();
                        let mut progress_state = load_native_job_state(&job_id, state.clone()).await;
                        let progress_handle = tokio::spawn(async move {
                            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
                            loop {
                                tokio::select! {
                                    _ = heartbeat.tick() => {
                                        progress_state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&progress_state).await;
                                    }
                                    progress = progress_rx.recv() => match progress {
                                        Some(progress) => {
                                            apply_native_progress(&mut progress_state, &progress);
                                            let _ = save_native_job_state(&progress_state).await;
                                            let _ = tx_progress.send(json!({"event": "download.progress", "jobId": progress_job, "progress": progress}));
                                        }
                                        None => break,
                                    }
                                }
                            }
                        });

                        let result = download_dash_resumable(
                            &url, &representation_id, audio_representation_id.as_deref(), &target,
                            session.as_ref(), control_rx.clone(), progress_tx, rate_limiter()
                        ).await;
                        let _ = progress_handle.await;

                        match result {
                            Ok(DownloadOutcome::Completed) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Completed");
                                state["percent"] = json!(100);
                                state["detail"] = json!("DASH video download complete");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.complete", "jobId": job_id, "path": target.to_string_lossy()}));
                                break 'job;
                            }
                            Ok(DownloadOutcome::Cancelled) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Cancelled");
                                state["detail"] = json!("Cancelled · partial file removed");
                                state["targetPath"] = Value::Null;
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                break 'job;
                            }
                            Ok(DownloadOutcome::Paused) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Paused");
                                state["detail"] = json!("Paused · partial checkpoint saved");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.paused", "jobId": job_id}));
                                match wait_native_job_control(&mut control_rx, &mut state, true).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["detail"] = json!("Resuming saved checkpoint");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial checkpoint kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                            Err(error) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Failed");
                                state["error"] = json!(error_text(&error));
                                state["detail"] = json!("DASH download interrupted · partial checkpoint kept");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.error", "jobId": job_id, "error": error_text(&error)}));
                                // Retain the native control channel so the desktop
                                // queue can ask this browser job to Resume its checkpoint.
                                match wait_native_job_control(&mut control_rx, &mut state, false).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["error"] = Value::Null;
                                        state["detail"] = json!("Retrying from saved checkpoint");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial checkpoint kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                        }
                    }
                    if let Ok(mut controls) = native_controls().lock() {
                        controls.remove(&job_id);
                    }
                });
            }
            // Slice 3: yt-dlp routing for YouTube/social pages.
            // The extension sends the PAGE URL (never a sniffed media
            // URL); the engine inspects it with yt-dlp and maps the
            // formats onto the panel's VideoVariant rows. A missing
            // tool is a typed ToolsMissing, not a plain failure — the
            // extension answers it with the consent UI (named tool +
            // direct download button), not an error toast.
            "ytdlp.inspect" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let session = message_session(&message, &url, referer);
                let tx = outgoing_tx.clone();
                tokio::spawn(async move {
                    match inspect_page(&url, session.as_ref()).await {
                        Ok(inspected) => {
                            let _ = tx.send(json!({"id": id, "ok": true, "data": {"title": inspected.title, "variants": inspected.variants}}));
                        }
                        Err(error) => {
                            if let Some(missing) = error.downcast_ref::<ToolsMissing>() {
                                let tools: Vec<&str> = missing.tools.iter().map(|tool| tool.id()).collect();
                                let _ = tx.send(json!({"id": id, "ok": false, "toolsMissing": tools, "error": error.to_string()}));
                            } else {
                                let _ = tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                            }
                        }
                    }
                });
            }
            "ytdlp.tools_status" => {
                let tx = outgoing_tx.clone();
                tokio::spawn(async move {
                    let ytdlp = tool_status(YtDlpTool::YtDlp).await;
                    let deno = tool_status(YtDlpTool::Deno).await;
                    let _ = tx.send(json!({"id": id, "ok": true, "data": {"ytdlp": ytdlp, "deno": deno, "toolsDir": tools_dir().to_string_lossy()}}));
                });
            }
            // On-demand tool download (option b). The caller already
            // has explicit consent (the user tapped the download
            // button); progress is streamed as ytdlp.tool_progress
            // events so the UI can show "yt-dlp download ho raha hai
            // — 45%". The download is SHA256-verified in the engine
            // before the file becomes the tool.
            "ytdlp.ensure_tool" => {
                let tool_id = message.get("tool").and_then(Value::as_str).unwrap_or("").to_string();
                let Some(tool) = YtDlpTool::from_id(&tool_id) else {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Unknown tool"}));
                    continue;
                };
                let tx = outgoing_tx.clone();
                tokio::spawn(async move {
                    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ToolDownloadProgress>();
                    let tx_progress = tx.clone();
                    let progress_handle = tokio::spawn(async move {
                        while let Some(progress) = progress_rx.recv().await {
                            let _ = tx_progress.send(json!({"event": "ytdlp.tool_progress", "tool": progress.tool.id(), "downloaded": progress.downloaded, "total": progress.total, "percent": progress.percent}));
                        }
                    });
                    match ensure_tool(tool, Some(progress_tx)).await {
                        Ok(path) => {
                            let _ = progress_handle.await;
                            let _ = tx.send(json!({"id": id, "ok": true, "data": {"tool": tool.id(), "path": path.to_string_lossy()}}));
                        }
                        Err(error) => {
                            let _ = progress_handle.await;
                            let _ = tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        }
                    }
                });
            }
            "ytdlp.check_update" => {
                let tx = outgoing_tx.clone();
                tokio::spawn(async move {
                    match check_ytdlp_update().await {
                        Ok(message) => {
                            let _ = tx.send(json!({"id": id, "ok": true, "data": {"message": message}}));
                        }
                        Err(error) => {
                            if let Some(missing) = error.downcast_ref::<ToolsMissing>() {
                                let tools: Vec<&str> = missing.tools.iter().map(|tool| tool.id()).collect();
                                let _ = tx.send(json!({"id": id, "ok": false, "toolsMissing": tools, "error": error.to_string()}));
                            } else {
                                let _ = tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                            }
                        }
                    }
                });
            }
            "ytdlp.download" => {
                let url = message.get("url").and_then(Value::as_str).unwrap_or("").to_string();
                let referer = message.get("referer").and_then(Value::as_str).map(str::to_string);
                let session = message_session(&message, &url, referer);
                let filename = output_name(message.get("filename"), "video.mp4");
                // The panel sends the format selector directly; older
                // callers send representation ids instead.
                let format_selector = match message.get("formatSelector").and_then(Value::as_str) {
                    Some(selector) if !selector.trim().is_empty() => selector.to_string(),
                    _ => {
                        let representation = message.get("representationId").and_then(Value::as_str).unwrap_or("");
                        let audio = message.get("audioRepresentationId").and_then(Value::as_str).unwrap_or("");
                        if audio.is_empty() { representation.to_string() } else { format!("{representation}+{audio}") }
                    }
                };
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Only HTTP and HTTPS page URLs are supported"}));
                    continue;
                }
                if format_selector.trim().is_empty() {
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "No format was selected for this download"}));
                    continue;
                }
                let target = match reserve_download_path(&default_download_dir().join(&filename)).await {
                    Ok(path) => path,
                    Err(error) => {
                        let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": error.to_string()}));
                        continue;
                    }
                };
                let job_id = uuid::Uuid::new_v4().to_string();
                let (control_tx, mut control_rx) = watch::channel(DownloadControl::Running);
                if let Ok(mut controls) = native_controls().lock() {
                    controls.insert(job_id.clone(), control_tx);
                } else {
                    cleanup_native_partial(&target).await;
                    let _ = outgoing_tx.send(json!({"id": id, "ok": false, "error": "Native download control registry is unavailable"}));
                    continue;
                }
                let tx = outgoing_tx.clone();
                let mut state = native_job_state(&job_id, &filename, &url, "ytdlp");
                state["targetPath"] = json!(target.to_string_lossy().to_string());
                state["formatSelector"] = json!(format_selector);
                if let Some(session) = &session {
                    state["session"] = serde_json::to_value(session).unwrap_or(Value::Null);
                }
                let _ = save_native_job_state(&state).await;
                let _ = outgoing_tx.send(json!({"id": id, "ok": true, "data": {"accepted": true, "jobId": job_id}}));
                tokio::spawn(async move {
                    'job: loop {
                        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
                        let tx_progress = tx.clone();
                        let progress_job = job_id.clone();
                        let mut progress_state = load_native_job_state(&job_id, state.clone()).await;
                        let progress_handle = tokio::spawn(async move {
                            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(10));
                            loop {
                                tokio::select! {
                                    _ = heartbeat.tick() => {
                                        progress_state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&progress_state).await;
                                    }
                                    progress = progress_rx.recv() => match progress {
                                        Some(progress) => {
                                            apply_native_progress(&mut progress_state, &progress);
                                            let _ = save_native_job_state(&progress_state).await;
                                            let _ = tx_progress.send(json!({"event": "download.progress", "jobId": progress_job, "progress": progress}));
                                        }
                                        None => break,
                                    }
                                }
                            }
                        });

                        let result = download_page(
                            &url, &format_selector, &target, session.as_ref(), control_rx.clone(), progress_tx
                        ).await;
                        let _ = progress_handle.await;

                        match result {
                            Ok((DownloadOutcome::Completed, produced)) => {
                                let final_path = produced.unwrap_or_else(|| target.clone());
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Completed");
                                state["percent"] = json!(100);
                                state["detail"] = json!("Download complete");
                                state["targetPath"] = json!(final_path.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.complete", "jobId": job_id, "path": final_path.to_string_lossy()}));
                                break 'job;
                            }
                            Ok((DownloadOutcome::Cancelled, _)) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Cancelled");
                                state["detail"] = json!("Cancelled · partial file removed");
                                state["targetPath"] = Value::Null;
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                break 'job;
                            }
                            Ok((DownloadOutcome::Paused, _)) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Paused");
                                state["detail"] = json!("Paused · partial file kept for resume");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.paused", "jobId": job_id}));
                                match wait_native_job_control(&mut control_rx, &mut state, true).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["detail"] = json!("Resuming with yt-dlp");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial files kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                            Err(error) => {
                                state = load_native_job_state(&job_id, state).await;
                                state["status"] = json!("Failed");
                                state["error"] = json!(error_text(&error));
                                state["detail"] = json!("Download interrupted · partial files kept");
                                state["targetPath"] = json!(target.to_string_lossy().to_string());
                                state["updatedAt"] = json!(timestamp_ms());
                                let _ = save_native_job_state(&state).await;
                                let _ = tx.send(json!({"event": "download.error", "jobId": job_id, "error": error_text(&error)}));
                                match wait_native_job_control(&mut control_rx, &mut state, false).await {
                                    Some(DownloadControl::Running) => {
                                        state["status"] = json!("Downloading");
                                        state["error"] = Value::Null;
                                        state["detail"] = json!("Retrying with yt-dlp");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        continue 'job;
                                    }
                                    Some(DownloadControl::Cancelled) => {
                                        cleanup_native_partial(&target).await;
                                        state["status"] = json!("Cancelled");
                                        state["detail"] = json!("Cancelled · partial file removed");
                                        state["targetPath"] = Value::Null;
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        let _ = tx.send(json!({"event": "download.cancelled", "jobId": job_id}));
                                        break 'job;
                                    }
                                    Some(DownloadControl::Paused) => unreachable!(),
                                    None => {
                                        state["status"] = json!("Interrupted");
                                        state["detail"] = json!("Browser companion disconnected; partial files kept");
                                        state["updatedAt"] = json!(timestamp_ms());
                                        let _ = save_native_job_state(&state).await;
                                        break 'job;
                                    }
                                }
                            }
                        }
                    }
                    if let Ok(mut controls) = native_controls().lock() {
                        controls.remove(&job_id);
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

#[cfg(test)]
mod config_dir_tests {
    use super::desktop_config_dir_from;
    use anyhow::anyhow;
    use std::path::{Path, PathBuf};
    use thunderload_engine::error_text;

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_config_dir_prefers_xdg_config_home() {
        assert_eq!(
            desktop_config_dir_from(None, Some(Path::new("/xdg")), Some(Path::new("/home/u"))),
            Some(PathBuf::from("/xdg/com.thunderload.desktop"))
        );
        assert_eq!(
            desktop_config_dir_from(None, None, Some(Path::new("/home/u"))),
            Some(PathBuf::from("/home/u/.config/com.thunderload.desktop"))
        );
        // An empty XDG_CONFIG_HOME falls back to ~/.config (the dirs
        // crate treats it as unset).
        assert_eq!(
            desktop_config_dir_from(None, Some(Path::new("")), Some(Path::new("/home/u"))),
            Some(PathBuf::from("/home/u/.config/com.thunderload.desktop"))
        );
        assert_eq!(desktop_config_dir_from(None, None, None), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_config_dir_is_library_application_support() {
        assert_eq!(
            desktop_config_dir_from(None, None, Some(Path::new("/Users/u"))),
            Some(PathBuf::from(
                "/Users/u/Library/Application Support/com.thunderload.desktop"
            ))
        );
        assert_eq!(desktop_config_dir_from(None, None, None), None);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_config_dir_is_appdata() {
        assert_eq!(
            desktop_config_dir_from(Some(Path::new("C:/Users/u/AppData/Roaming")), None, None),
            Some(PathBuf::from(
                "C:/Users/u/AppData/Roaming/com.thunderload.desktop"
            ))
        );
        assert_eq!(desktop_config_dir_from(None, None, None), None);
    }

    /// Slice 5: a non-auth failure keeps its own message (the 403
    /// relabelling is covered by the engine tests of
    /// `auth_failure_label`, which this helper calls).
    #[test]
    fn error_text_keeps_non_auth_errors_unchanged() {
        assert_eq!(error_text(&anyhow!("boom")), "boom");
        assert_eq!(
            error_text(&anyhow!("Download server returned HTTP 500")),
            "Download server returned HTTP 500"
        );
    }
}

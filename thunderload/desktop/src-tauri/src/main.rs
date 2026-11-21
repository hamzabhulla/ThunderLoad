#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
// Release builds on Windows must not show a console window: without this
// attribute the desktop binary is built with the console subsystem, so
// Windows opens a black console titled with the exe path next to the app
// window — and closing that console kills the app process. Debug builds
// keep the console so eprintln! logs stay visible during development.
use std::{collections::HashMap, path::{Path, PathBuf}, process::Command, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, OnceLock}, time::{Duration, Instant}};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use serde::Serialize;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};
use thunderload_engine::{check_ytdlp_update as engine_check_ytdlp_update, dash_output_extension, dash_work_dir, default_download_dir, download_dash_resumable, download_hls_live, download_hls_resumable, download_hls_subtitles, download_page, download_url_resumable_with_connections, ensure_tool, error_text, grab_page_media as engine_grab_page_media, hls_output_extension, hls_work_dir, inspect_page, inspect_video, partial_download_metadata_path, partial_download_path, reserve_download_path, safe_filename, store_file_name, tool_status, tools_dir, with_extension, DownloadControl, DownloadOutcome, DownloadProgress, GlobalRateLimiter, HlsSelection, JobKind, JobOrigin, JobRecord, JobState, JobStore, PageMediaList, SessionHeaders, ToolDownloadProgress, ToolStatus, ToolsMissing, YtDlpTool};
use tokio::{process::Command as TokioCommand, sync::{mpsc, watch}};
use uuid::Uuid;

/// Build a child-process command that never shows a console window on
/// Windows. The app is windowless since Fix 5, so a console-subsystem
/// child (reg, powershell.exe, ffmpeg, explorer) would otherwise get
/// its own visible black window on every spawn — notably `reg query`,
/// which runs on every startup for each saved browser ID. On other
/// platforms this is just `Command::new`.
fn hidden_command<S: AsRef<std::ffi::OsStr>>(program: S) -> Command {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        let mut command = Command::new(program);
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        command
    }
    #[cfg(not(target_os = "windows"))]
    {
        Command::new(program)
    }
}

/// Same as `hidden_command`, for async spawns: tokio's Command exposes
/// no creation flag, so the flag is set on a std command and converted
/// (From<std::process::Command> for tokio::process::Command).
fn hidden_tokio_command<S: AsRef<std::ffi::OsStr>>(program: S) -> TokioCommand {
    TokioCommand::from(hidden_command(program))
}

#[tauri::command]
async fn inspect_video_url(url: String, referer: Option<String>) -> Result<Vec<thunderload_engine::VideoVariant>, String> {
    let session = SessionHeaders::with_referer(referer.as_deref());
    inspect_video(&url, session.as_ref()).await.map_err(|error| error_text(&error))
}

/// Site grabber (Slice 7): fetch one page and list its media links.
/// Best effort only — the desktop has no browser session, so pages that
/// need a login or load media with JavaScript may come back empty. The
/// extension's live-DOM scan is the full path for those pages.
#[tauri::command]
async fn grab_page_media(page_url: String) -> Result<PageMediaList, String> {
    if !(page_url.starts_with("http://") || page_url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS page URLs are supported".into());
    }
    engine_grab_page_media(&page_url).await.map_err(|error| error_text(&error))
}


static JOB_CONTROLS: OnceLock<Mutex<HashMap<String, watch::Sender<DownloadControl>>>> = OnceLock::new();
static RATE_LIMITER: OnceLock<Arc<GlobalRateLimiter>> = OnceLock::new();

fn job_controls() -> &'static Mutex<HashMap<String, watch::Sender<DownloadControl>>> {
    JOB_CONTROLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// C1 (belt-and-braces): registers a job's control channel, refusing
/// when the job already has a LIVE one. A stale sender whose task ended
/// (receivers dropped — e.g. a Paused job waiting for resume) does not
/// count as live, so a resume re-registers cleanly; a second start
/// while the first task is still running is rejected instead of
/// spawning a duplicate transfer on the same target.
fn register_job_control(job_id: &str, control_tx: watch::Sender<DownloadControl>) -> Result<(), String> {
    let mut controls = match job_controls().lock() {
        Ok(controls) => controls,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(existing) = controls.get(job_id) {
        if !existing.is_closed() {
            return Err("This download is already running.".into());
        }
    }
    controls.insert(job_id.to_string(), control_tx);
    Ok(())
}

/// Jobs that already finished successfully, with the time they completed.
/// `cancel_download` consults this before deleting anything: a cancel that
/// arrives after the download completed (a UI row that has not refreshed
/// yet) must not delete the finished file (D9). Entries are kept for the
/// whole app session — a time-based expiry would re-open the hole for a
/// late cancel — and the map is capped at 4096 entries (oldest evicted),
/// so memory stays bounded no matter how many downloads complete.
static COMPLETED_JOBS: OnceLock<Mutex<HashMap<String, (PathBuf, Instant)>>> = OnceLock::new();

fn completed_jobs() -> &'static Mutex<HashMap<String, (PathBuf, Instant)>> {
    COMPLETED_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn mark_job_completed(job_id: &str, target: &Path) {
    if let Ok(mut jobs) = completed_jobs().lock() {
        if jobs.len() >= 4096 {
            if let Some(oldest) = jobs.iter().min_by_key(|(_, (_, at))| *at).map(|(id, _)| id.clone()) {
                jobs.remove(&oldest);
            }
        }
        jobs.insert(job_id.to_string(), (target.to_path_buf(), Instant::now()));
    }
}

fn job_completed(job_id: &str) -> bool {
    completed_jobs().lock().map(|jobs| jobs.contains_key(job_id)).unwrap_or(false)
}

/// P1-23: whether the DURABLE row for this job is Completed. The
/// in-memory `COMPLETED_JOBS` map only covers this app session — a
/// restart (or a completion recorded by another path) leaves the row
/// Completed in the store while the map is empty, so `cancel_download`
/// must consult the store too before deleting anything.
fn job_store_completed(job_id: &str) -> bool {
    let Some(store) = JOB_STORE.get() else { return false };
    let Ok(store) = store.lock() else { return false };
    if store.get(job_id).map(|record| matches!(record.state, JobState::Completed)).unwrap_or(false) {
        return true;
    }
    if let Some(bare) = job_id.strip_prefix("native-") {
        return store.get(bare).map(|record| matches!(record.state, JobState::Completed)).unwrap_or(false);
    }
    false
}

/// Durable job database (Slice 5): one `jobs.json` in the app-data
/// directory, holding every desktop job (and a mirror of browser-started
/// jobs). Initialized in `setup`; if the app-data directory is
/// unavailable the store stays unset and every helper below becomes a
/// no-op — downloads still work in memory and the frontend's local
/// history still covers the UI, just without backend durability.
static JOB_STORE: OnceLock<Mutex<JobStore>> = OnceLock::new();

/// Runs `f` with the job store when it exists. Store errors are
/// swallowed here on purpose: a failed write must never break a
/// download, and the checkpoint files on disk remain the resume
/// mechanism either way.
fn with_job_store<F: FnOnce(&mut JobStore)>(f: F) {
    if let Some(store) = JOB_STORE.get() {
        if let Ok(mut store) = store.lock() {
            f(&mut store);
        }
    }
}

/// The stored browser session for one job, if it handed one off.
/// Desktop-started jobs have none; a desktop resume of a browser-started
/// job reuses the stored handoff (Slice 4 handover path). The session
/// never leaves the Rust side — `get_jobs` strips it before returning
/// rows to the frontend, and it is never logged.
fn job_session(job_id: &str) -> Option<SessionHeaders> {
    let store = JOB_STORE.get()?;
    let store = store.lock().ok()?;
    if let Some(session) = store.get(job_id).and_then(|record| record.session.clone()) {
        return Some(session);
    }
    if let Some(bare) = job_id.strip_prefix("native-") {
        return store.get(bare).and_then(|record| record.session.clone());
    }
    None
}

/// Whether the durable row for this job is a live recording, if the
/// store knows it. A live recording's partial file IS the recording:
/// it is never deleted by a cancel and never re-recorded over. Unknown
/// jobs (no row yet) report false, which is the safe VOD behaviour.
/// Browser-started rows are mirrored under the companion's own id
/// (no "native-" prefix), while the frontend calls back with the
/// prefixed id — check both, so the guard holds whichever row exists.
fn job_is_live(job_id: &str) -> bool {
    let Some(store) = JOB_STORE.get() else { return false };
    let Ok(store) = store.lock() else { return false };
    if store.get(job_id).map(|record| record.live).unwrap_or(false) {
        return true;
    }
    if let Some(bare) = job_id.strip_prefix("native-") {
        return store.get(bare).map(|record| record.live).unwrap_or(false);
    }
    false
}

/// Whether a cancel that finds no live task may delete the target's
/// files. A live recording's partial file is never deleted, and a
/// finished file is never deleted (D9) — only a paused/failed VOD
/// job's checkpoint goes.
fn cancel_may_delete_target(is_live: bool, completed: bool) -> bool {
    !is_live && !completed
}

/// The durable row's stored target for one job, if the store knows it.
/// Same id lookup as `job_session`/`job_is_live` (prefixed id, then the
/// bare companion id), so a frontend-supplied path can be checked
/// against the target the backend itself reserved — the frontend must
/// never be able to point a delete at an arbitrary path.
fn job_target_path(job_id: &str) -> Option<PathBuf> {
    let store = JOB_STORE.get()?;
    let store = store.lock().ok()?;
    if let Some(target) = store.get(job_id).and_then(|record| record.target_path.clone()) {
        return Some(PathBuf::from(target));
    }
    if let Some(bare) = job_id.strip_prefix("native-") {
        return store.get(bare).and_then(|record| record.target_path.clone()).map(PathBuf::from);
    }
    None
}

/// Whether a finish path that found a cancel waiting may delete the
/// task's own reserved target. The target here is the task's, not a
/// frontend-supplied path, but the same guards apply: never a live
/// recording, never a finished file, and never a path that escaped the
/// download directory.
fn finish_may_delete_target(job_id: &str, target: &Path) -> bool {
    cancel_may_delete_target(job_is_live(job_id), job_completed(job_id))
        && is_within_download_dir(target)
}

/// Deletes every resume file a desktop download can leave behind: the
/// partial file, the checkpoint sidecar, the HLS/DASH segment work
/// directories, and the target itself. Paths that do not exist are
/// no-ops, so this is safe to call without knowing the job kind.
async fn delete_desktop_resume_files(target: &Path) {
    let _ = tokio::fs::remove_file(partial_download_path(target)).await;
    let _ = tokio::fs::remove_file(partial_download_metadata_path(target)).await;
    let _ = tokio::fs::remove_dir_all(hls_work_dir(target)).await;
    let _ = tokio::fs::remove_dir_all(dash_work_dir(target)).await;
    let _ = tokio::fs::remove_file(target).await;
    // yt-dlp partials: <stem>.part and <stem>.<format>.<ext>.part
    // siblings of the reserved target. A paused yt-dlp job cancelled
    // from the UI has no engine checkpoint — without this its .part
    // files would survive the cancel. Harmless for other kinds: they
    // never create such files. Scoped to THIS job's fragments: the
    // name minus ".part" must end with this target's own extension,
    // so a sibling job sharing the stem ("video.webm" next to this
    // "video.mp4" — reserve uniquifies the full name, not the stem)
    // never loses its fragments to this job's cancel.
    if let (Some(parent), Some(stem)) = (target.parent(), target.file_stem().and_then(|stem| stem.to_str())) {
        let prefix = format!("{stem}.");
        let target_ext = target.extension().and_then(|ext| ext.to_str()).unwrap_or("");
        if let Ok(mut entries) = tokio::fs::read_dir(parent).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                let own_fragment = if target_ext.is_empty() {
                    name == format!("{stem}.part")
                } else {
                    name.starts_with(&prefix) && name.ends_with(&format!(".{target_ext}.part"))
                };
                if own_fragment {
                    let _ = tokio::fs::remove_file(entry.path()).await;
                }
            }
        }
    }
}

/// Reports the final outcome of a desktop download task.
/// `cancelled_while_finishing` is the control channel's value read after
/// the engine call returned: a cancel that arrived while the download was
/// finishing must not delete a completed file (D9), but it does turn a
/// Paused/Error finish into Cancelled so no checkpoint is left behind.
/// Completed always wins — the file is whole, so a racing cancel is
/// ignored and the completion event is what the UI sees.
async fn finish_desktop_download(
    app: &tauri::AppHandle,
    job_id: &str,
    target: &Path,
    result: Result<DownloadOutcome, String>,
    cancelled_while_finishing: bool,
) {
    match result {
        Ok(DownloadOutcome::Completed) => {
            mark_job_completed(job_id, target);
            let final_path = target.to_string_lossy().to_string();
            with_job_store(|store| {
                if let Some(mut record) = store.get(job_id).cloned() {
                    if record.target_path.as_deref() != Some(final_path.as_str()) {
                        record.target_path = Some(final_path.clone());
                        let _ = store.upsert(record);
                    }
                }
                let _ = store.update_state(
                    job_id,
                    JobState::Completed,
                    Some(&format!("Saved · {final_path}")),
                    None,
                );
            });
            let _ = app.emit("download-complete", json!({ "jobId": job_id, "path": final_path }));
            if let Ok(mut controls) = job_controls().lock() { controls.remove(job_id); }
        }
        Ok(DownloadOutcome::Paused) if cancelled_while_finishing => {
            // MC5: same guard as cancel_download — the target is the
            // task's own, but a live recording or a finished file must
            // still never be deleted from a finish path.
            if finish_may_delete_target(job_id, target) {
                delete_desktop_resume_files(target).await;
            }
            with_job_store(|store| {
                let _ = store.update_state(job_id, JobState::Cancelled, Some("Cancelled"), None);
            });
            let _ = app.emit("download-cancelled", json!({ "jobId": job_id }));
            if let Ok(mut controls) = job_controls().lock() { controls.remove(job_id); }
        }
        Ok(DownloadOutcome::Paused) => {
            with_job_store(|store| {
                let _ = store.update_state(job_id, JobState::Paused, Some("Paused · partial file saved"), None);
                let _ = store.refresh_segments(job_id);
            });
            let _ = app.emit("download-paused", json!({ "jobId": job_id, "targetPath": target.to_string_lossy() }));
        }
        Ok(DownloadOutcome::Cancelled) => {
            with_job_store(|store| {
                let _ = store.update_state(job_id, JobState::Cancelled, Some("Cancelled"), None);
            });
            let _ = app.emit("download-cancelled", json!({ "jobId": job_id }));
            if let Ok(mut controls) = job_controls().lock() { controls.remove(job_id); }
        }
        Err(_error) if cancelled_while_finishing => {
            // MC5: same guard as the Paused arm above.
            if finish_may_delete_target(job_id, target) {
                delete_desktop_resume_files(target).await;
            }
            with_job_store(|store| {
                let _ = store.update_state(job_id, JobState::Cancelled, Some("Cancelled"), None);
            });
            let _ = app.emit("download-cancelled", json!({ "jobId": job_id }));
            if let Ok(mut controls) = job_controls().lock() { controls.remove(job_id); }
        }
        Err(error) => {
            let error_text = error.clone();
            with_job_store(|store| {
                let _ = store.update_state(
                    job_id,
                    JobState::Failed,
                    Some("Transfer interrupted · partial data kept"),
                    Some(&error_text),
                );
                let _ = store.refresh_segments(job_id);
            });
            let _ = app.emit("download-error", json!({ "jobId": job_id, "error": error, "targetPath": target.to_string_lossy() }));
            if let Ok(mut controls) = job_controls().lock() { controls.remove(job_id); }
        }
    }
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

// P3: only the Windows scheduler path calls this — on other
// platforms it is dead code, and the unconditional unit test below
// still needs it, so the lint is silenced there instead of cfg-ing
// the function (and its test) away.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn powershell_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Scheduled-wake helpers shared by the platform implementations below.
/// `local_date_time` is always validated with `valid_local_datetime_shape`
/// ("YYYY-MM-DDTHH:mm") before these run.
#[cfg(target_os = "macos")]
fn wake_datetime_parts(local_date_time: &str) -> Option<(u32, u32, u32, u32, u32)> {
    if !valid_local_datetime_shape(local_date_time) {
        return None;
    }
    let year = local_date_time.get(0..4)?.parse().ok()?;
    let month = local_date_time.get(5..7)?.parse().ok()?;
    let day = local_date_time.get(8..10)?.parse().ok()?;
    let hour = local_date_time.get(11..13)?.parse().ok()?;
    let minute = local_date_time.get(14..16)?.parse().ok()?;
    Some((year, month, day, hour, minute))
}

/// Executable to launch at the scheduled time. An AppImage sets $APPIMAGE
/// to its own image file; `current_exe()` inside an AppImage points at a
/// path under the (ephemeral) FUSE mount, which is gone after a reboot,
/// so the image file wins when it is set.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn wake_executable() -> Result<PathBuf, String> {
    if let Some(image) = std::env::var_os("APPIMAGE") {
        let image = PathBuf::from(image);
        if !image.as_os_str().is_empty() {
            return Ok(image);
        }
    }
    std::env::current_exe().map_err(|error| error.to_string())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn home_dir() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| "HOME is not available.".to_string())
}

#[cfg(target_os = "linux")]
fn systemd_exec_arg(path: &Path) -> String {
    let text = path.to_string_lossy();
    if text
        .chars()
        .any(|c| c.is_whitespace() || c == '"' || c == '\\')
    {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        text.into_owned()
    }
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// systemd user timer + service units for a one-shot wake (Linux).
/// The timer wakes the machine from suspend when the platform allows it
/// (`WakeSystem=true`); `Persistent=true` runs a missed launch at the next
/// login/boot instead of dropping it.
#[cfg(target_os = "linux")]
fn systemd_wake_units(executable: &Path, local_date_time: &str) -> (String, String) {
    let on_calendar = format!("{} {}:00", &local_date_time[..10], &local_date_time[11..16]);
    let timer = format!(
        "[Unit]\nDescription=ThunderLoad scheduled launch\n\n[Timer]\nOnCalendar={on_calendar}\nWakeSystem=true\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n"
    );
    let service = format!(
        "[Unit]\nDescription=ThunderLoad scheduled launch\n\n[Service]\nType=oneshot\nExecStart={}\n",
        systemd_exec_arg(executable)
    );
    (timer, service)
}

#[cfg(target_os = "linux")]
fn systemd_wake_unit_names(job_id: &str) -> (String, String) {
    (
        format!("thunderload-wake-{job_id}.timer"),
        format!("thunderload-wake-{job_id}.service"),
    )
}

/// launchd agent plist for a one-shot scheduled launch (macOS).
/// launchd has no wake-from-sleep: if the Mac is asleep at the scheduled
/// time the launch is skipped, which the success message says plainly.
#[cfg(target_os = "macos")]
fn launchd_wake_plist(executable: &Path, job_id: &str, local_date_time: &str) -> Option<String> {
    let (year, month, day, hour, minute) = wake_datetime_parts(local_date_time)?;
    Some(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>com.thunderload.wake-{job_id}</string>\n  <key>ProgramArguments</key>\n  <array>\n    <string>{}</string>\n  </array>\n  <key>StartCalendarInterval</key>\n  <dict>\n    <key>Year</key>\n    <integer>{year}</integer>\n    <key>Month</key>\n    <integer>{month}</integer>\n    <key>Day</key>\n    <integer>{day}</integer>\n    <key>Hour</key>\n    <integer>{hour}</integer>\n    <key>Minute</key>\n    <integer>{minute}</integer>\n  </dict>\n</dict>\n</plist>\n",
        xml_escape(&executable.to_string_lossy())
    ))
}

/// Current local time in the validated "YYYY-MM-DDTHH:mm" shape, via the
/// `date` command (present on every Linux desktop and macOS). Used for
/// the future-time check in `schedule_app_wake`; None when the command is
/// unavailable, in which case the backends still decide (systemd fires a
/// past-due timer at once, launchd waits for the next match — both
/// visible, neither silent).
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn local_now_shaped() -> Option<String> {
    let output = Command::new("date").arg("+%Y-%m-%dT%H:%M").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let now = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if valid_local_datetime_shape(&now) {
        Some(now)
    } else {
        None
    }
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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // The validated shape makes lexicographic comparison chronological.
        // Windows rejects past times in its PowerShell script; this is the
        // same check for the systemd/launchd backends.
        if let Some(now) = local_now_shaped() {
            if local_date_time <= now {
                return Err("The scheduled time must be in the future.".into());
            }
        }
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
        let result = hidden_command("powershell.exe")
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
    #[cfg(target_os = "linux")]
    {
        let _ = app;
        let executable = wake_executable()?;
        let (timer_name, service_name) = systemd_wake_unit_names(&job_id);
        let unit_dir = home_dir()?.join(".config").join("systemd").join("user");
        tokio::fs::create_dir_all(&unit_dir)
            .await
            .map_err(|error| error.to_string())?;
        let (timer, service) = systemd_wake_units(&executable, &local_date_time);
        let timer_path = unit_dir.join(&timer_name);
        let service_path = unit_dir.join(&service_name);
        tokio::fs::write(&timer_path, timer)
            .await
            .map_err(|error| error.to_string())?;
        if let Err(error) = tokio::fs::write(&service_path, service).await {
            let _ = tokio::fs::remove_file(&timer_path).await;
            return Err(error.to_string());
        }
        let enabled = Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .output()
            .and_then(|output| {
                if output.status.success() {
                    Command::new("systemctl")
                        .args(["--user", "enable", "--now", &timer_name])
                        .output()
                } else {
                    Ok(output)
                }
            });
        match enabled {
            Ok(output) if output.status.success() => Ok(format!("Linux wake timer registered for {local_date_time}. Wake from sleep depends on systemd and your power settings.")),
            Ok(output) => {
                let _ = tokio::fs::remove_file(&timer_path).await;
                let _ = tokio::fs::remove_file(&service_path).await;
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                Err(if stderr.is_empty() { "systemd refused the wake timer.".into() } else { format!("systemctl: {stderr}") })
            }
            Err(error) => {
                let _ = tokio::fs::remove_file(&timer_path).await;
                let _ = tokio::fs::remove_file(&service_path).await;
                Err(format!("Could not run systemctl: {error}"))
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        let _ = app;
        let executable = wake_executable()?;
        let plist = launchd_wake_plist(&executable, &job_id, &local_date_time)
            .ok_or_else(|| "Scheduled time must use YYYY-MM-DDTHH:mm local time.".to_string())?;
        let agent_dir = home_dir()?.join("Library").join("LaunchAgents");
        tokio::fs::create_dir_all(&agent_dir)
            .await
            .map_err(|error| error.to_string())?;
        let plist_path = agent_dir.join(format!("com.thunderload.wake-{job_id}.plist"));
        tokio::fs::write(&plist_path, plist)
            .await
            .map_err(|error| error.to_string())?;
        // Unload a previous agent of the same name first: `load` refuses
        // an already-loaded label.
        let _ = Command::new("launchctl")
            .args(["unload", &plist_path.to_string_lossy()])
            .output();
        let loaded = Command::new("launchctl")
            .args(["load", &plist_path.to_string_lossy()])
            .output();
        match loaded {
            Ok(output) if output.status.success() => Ok(format!("macOS scheduled launch registered for {local_date_time}. Note: launchd does not wake a sleeping Mac — the app launches only if the Mac is awake at that time (use `pmset schedule wake` with admin rights to wake it).")),
            Ok(output) => {
                let _ = tokio::fs::remove_file(&plist_path).await;
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                Err(if stderr.is_empty() { "launchd refused the scheduled launch.".into() } else { format!("launchctl: {stderr}") })
            }
            Err(error) => {
                let _ = tokio::fs::remove_file(&plist_path).await;
                Err(format!("Could not run launchctl: {error}"))
            }
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        let _ = (local_date_time, app);
        Err("OS-level scheduled launch is not supported on this platform.".into())
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
        let result = hidden_command("powershell.exe")
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
    #[cfg(target_os = "linux")]
    {
        let _ = app;
        let (timer_name, service_name) = systemd_wake_unit_names(&job_id);
        // Idempotent by design: disabling a timer that was never
        // registered exits non-zero, which is not an error here.
        let _ = Command::new("systemctl")
            .args(["--user", "disable", "--now", &timer_name])
            .output();
        if let Ok(unit_dir) =
            home_dir().map(|home| home.join(".config").join("systemd").join("user"))
        {
            let _ = tokio::fs::remove_file(unit_dir.join(&timer_name)).await;
            let _ = tokio::fs::remove_file(unit_dir.join(&service_name)).await;
        }
        let _ = Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .output();
        Ok(())
    }
    #[cfg(target_os = "macos")]
    {
        let _ = app;
        if let Ok(plist_path) = home_dir().map(|home| {
            home.join("Library")
                .join("LaunchAgents")
                .join(format!("com.thunderload.wake-{job_id}.plist"))
        }) {
            let _ = Command::new("launchctl")
                .args(["unload", &plist_path.to_string_lossy()])
                .output();
            let _ = tokio::fs::remove_file(&plist_path).await;
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        let _ = app;
        Err("OS-level scheduled launch is not supported on this platform.".into())
    }
}

#[tauri::command]
async fn pause_download(job_id: String) -> Result<(), String> {
    let controls = job_controls().lock().map_err(|_| "Download control lock is unavailable")?;
    let sender = controls.get(&job_id).ok_or_else(|| "This download is not active. Resume it from the queue.".to_string())?;
    sender.send(DownloadControl::Paused).map_err(|_| "The download has already stopped. Resume it from the queue.".to_string())
}

fn normalize_lexical(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => return None,
            std::path::Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    Some(normalized)
}

fn is_within_download_dir(target: &Path) -> bool {
    if target.as_os_str().is_empty() || target.file_name().is_none() {
        return false;
    }
    if target.components().any(|component| matches!(component, std::path::Component::ParentDir)) {
        return false;
    }
    let base = default_download_dir();
    if let Ok(canon_base) = std::fs::canonicalize(&base) {
        if let Ok(canon_target) = std::fs::canonicalize(target) {
            return canon_target.starts_with(&canon_base) && canon_target != canon_base;
        }
        if let Some(parent) = target.parent() {
            if let Ok(canon_parent) = std::fs::canonicalize(parent) {
                return canon_parent.starts_with(&canon_base);
            }
        }
        return false;
    }
    match (normalize_lexical(&base), normalize_lexical(target)) {
        (Some(norm_base), Some(norm_target)) => norm_target.starts_with(&norm_base) && norm_target != norm_base,
        _ => false,
    }
}

/// P1-20: a frontend-supplied resume target is only trusted inside
/// the download directory — a path outside it (or a bare filename
/// that escapes via `..`) is refused instead of written to.
fn validated_resume_target(target_path: &str) -> Result<PathBuf, String> {
    let target = PathBuf::from(target_path);
    if !is_within_download_dir(&target) {
        return Err("The saved download location is outside the ThunderLoad folder.".into());
    }
    Ok(target)
}

#[tauri::command]
async fn cancel_download(job_id: String, target_path: String, app: tauri::AppHandle) -> Result<(), String> {
    // MC5: a poisoned control lock must not abort the cancel before the
    // state flip — recover the guard and carry on. The lock guards a
    // map, not the download itself; losing it must never strand the UI
    // on "Cancelling".
    let sender = match job_controls().lock() {
        Ok(mut controls) => controls.remove(&job_id),
        Err(poisoned) => poisoned.into_inner().remove(&job_id),
    };
    let actively_running = sender.map(|sender| sender.send(DownloadControl::Cancelled).is_ok()).unwrap_or(false);
    if !actively_running {
        // D9: the task already finished. If it completed, the target is a
        // finished file and must not be deleted — the completion event
        // already told (or will tell) the UI, so there is nothing to flip
        // and nothing to emit. This is the one early return that stays:
        // flipping a Completed row to Cancelled would corrupt it.
        // P1-23: the in-memory map only covers this app session — the
        // durable row may already be Completed after a restart, so
        // consult the store too.
        if job_completed(&job_id) || job_store_completed(&job_id) {
            return Ok(());
        }
        // Fold-in stored-target check: the frontend-supplied path is
        // only a hint. The delete target is the row's stored target —
        // the path the backend itself reserved — and a supplied path
        // that disagrees with it deletes nothing. An empty supplied
        // path falls back to the stored target, so a cancel that races
        // the start (target not yet known to the frontend) still
        // cleans up the checkpoint.
        let stored_target = job_target_path(&job_id);
        let supplied = {
            let trimmed = target_path.trim();
            if trimmed.is_empty() { None } else { Some(PathBuf::from(trimmed)) }
        };
        // Item 1: a supplied path with no stored target to check it
        // against is a mismatch — delete nothing unless a stored
        // target exists to authorize the delete. Both sides are
        // compared after normalize_lexical, so a differently-spelled
        // same path (e.g. a redundant `.` component) does not leak a
        // checkpoint delete.
        let target_matches_stored = match (&supplied, &stored_target) {
            (Some(supplied), Some(stored)) => match (normalize_lexical(supplied), normalize_lexical(stored)) {
                (Some(normalized_supplied), Some(normalized_stored)) => normalized_supplied == normalized_stored,
                _ => false,
            },
            (Some(_), None) => false,
            _ => true,
        };
        let delete_target = if target_matches_stored {
            supplied.or(stored_target)
        } else {
            None
        };
        // A live recording's partial file IS the recording: a cancel
        // that arrives after its task ended (a Paused live job has no
        // live task) must not delete it. The live task itself keeps the
        // file on every finish path, so this branch is the only place
        // a live target could still be lost. The row still flips to
        // Cancelled — the recording stopped — with the kept-file detail.
        // MC5: a guard failure (mismatched or out-of-directory target)
        // skips the delete only — the flip and the event below still
        // run, so the UI never strands on "Cancelling".
        if cancel_may_delete_target(job_is_live(&job_id), false) {
            if let Some(target) = delete_target {
                if is_within_download_dir(&target) {
                    // Item 4: the task may have completed between the
                    // job_completed check above and this delete. If it
                    // did, the target is a finished file: delete
                    // nothing, leave the row Completed, and emit no
                    // cancel for it. (P1-23: the durable row counts
                    // here too, not just the in-memory map.)
                    if job_completed(&job_id) || job_store_completed(&job_id) {
                        return Ok(());
                    }
                    delete_desktop_resume_files(&target).await;
                }
            }
        }
        let cancelled_detail = if job_is_live(&job_id) { "Recording stopped · partial file kept" } else { "Cancelled" };
        with_job_store(|store| {
            let _ = store.update_state(&job_id, JobState::Cancelled, Some(cancelled_detail), None);
        });
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
    scheduled_at: Option<u64>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS download URLs are supported".into());
    }
    let name = safe_filename(&filename, "download.bin");
    let target = match target_path {
        // P1-20: a resume target comes from the frontend — refuse it
        // when it points outside the download directory.
        Some(path) => validated_resume_target(&path)?,
        None => reserve_download_path(&default_download_dir().join(&name)).await.map_err(|error| error.to_string())?,
    };
    // Slice 7b (Gemini claim 7): the setting is real now — the queue
    // passes the user's choice in and the engine segments the file at
    // that count (1..=16 clamped inside). Before this slice the value
    // was dropped here and the engine always used 8.
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    // C1: refuse a second live task for the same job — the
    // frontend dispatcher guards this too, but the backend is the
    // last line of defence against a double start.
    register_job_control(&job_id, control_tx)?;
    // Record the job in the durable store (Slice 5) so a restart
    // restores it — Downloading rows are flipped to Interrupted on the
    // next startup and resumed from the checkpoint.
    let mut record = JobRecord::new_desktop(&job_id, &name, &url, JobKind::Direct, Some(&target));
    record.scheduled_at_ms = scheduled_at;
    with_job_store(|store| {
        let _ = store.start_job(record);
    });
    // A desktop resume of a browser-started job reuses the stored
    // handoff; a desktop-started job has none (None).
    let session = job_session(&job_id);
    // Keep a second receiver alive in the task until the outcome is
    // reported: a cancel that arrives while the download is finishing
    // still reaches a live receiver, so `cancel_download` never falls into
    // its cleanup branch for a job that just completed (D9).
    let transition_rx = control_rx.clone();

    let job_for_task = job_id.clone();
    let app_for_task = app.clone();
    let limit = rate_limiter();
    let task_target = target.clone();
    // P2: the progress task drains a buffered channel — after the
    // engine returns it could still emit stale progress events after
    // the terminal event below. The flag stops it the moment the
    // transfer is done; the receiver is dropped with the task.
    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_done_for_task = progress_done.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                if progress_done_for_task.load(Ordering::SeqCst) {
                    break;
                }
                // Persist progress (throttled inside the store) so a
                // crash keeps the last reported position; the checkpoint
                // on disk stays authoritative for resume.
                let (downloaded, total, percent) = (progress.downloaded, progress.total, progress.percent);
                let (segments_done, segments_total) = (progress.segments_done, progress.segments_total);
                with_job_store(|store| {
                    let _ = store.note_progress(&progress_id, downloaded, total, percent, segments_done, segments_total);
                });
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_url_resumable_with_connections(&url, &task_target, session.as_ref(), connections, control_rx, limit, progress_tx).await;
        progress_done.store(true, Ordering::SeqCst);
        let cancelled_while_finishing = *transition_rx.borrow() == DownloadControl::Cancelled;
        finish_desktop_download(&app_for_task, &job_for_task, &task_target, result.map_err(|error| error_text(&error)), cancelled_while_finishing).await;
    });
    Ok(target.to_string_lossy().to_string())
}

#[tauri::command]
async fn start_hls_download(
    job_id: String,
    url: String,
    representation_id: Option<String>,
    audio_representation_id: Option<String>,
    filename: String,
    target_path: Option<String>,
    connections: usize,
    scheduled_at: Option<u64>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS playlist URLs are supported".into());
    }
    // Slice 4 (Gemini claim 4): the HLS selection travels with the job.
    // `url` is the MASTER playlist URL (the frontend passes videoUrl);
    // the chosen variant's identity is its media playlist URL, which the
    // frontend stores in representationId — HLS variants carry no
    // representation id of their own. Empty strings mean "no choice".
    // Built here, before the Slice 7c extension probe below, because
    // the probe needs it (Rust locals are not hoisted — E0425 if used
    // before this definition).
    let selection = HlsSelection {
        media_url: representation_id.clone().filter(|value| !value.trim().is_empty()),
        audio_group: audio_representation_id.clone().filter(|value| !value.trim().is_empty()),
    };
    // Slice 7c (Gemini claim 6): resolve the container extension first
    // (ts for plain TS playlists, mp4 for fMP4) so the reserved target
    // gets the right name — reserving from the caller's filename alone
    // misnamed every TS stream as .mp4. Mirrors start_dash_download
    // and the native host's video.download path. The session is loaded
    // up front because the fresh-job extension probe needs it.
    //
    // P1-19: the control channel is registered and the job recorded
    // BEFORE the probe below. A Cancel during the probe then finds a
    // live control (so cancel_download routes through it instead of
    // flipping a row this start overwrites), and the start aborts
    // after the probe instead of writing to disk. The record carries
    // no target yet — the probe decides the name — and is corrected
    // below once the target is reserved.
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    register_job_control(&job_id, control_tx)?;
    let mut record = JobRecord::new_desktop(&job_id, &safe_filename(&filename, "video.mp4"), &url, JobKind::Hls, None);
    record.scheduled_at_ms = scheduled_at;
    record.representation_id = representation_id.clone();
    record.audio_representation_id = audio_representation_id.clone();
    with_job_store(|store| {
        let _ = store.start_job(record);
    });
    let session = job_session(&job_id);
    let name = match target_path {
        Some(_) => safe_filename(&filename, "video.mp4"),
        None => {
            match hls_output_extension(&url, &selection, session.as_ref()).await {
                Ok(extension) => with_extension(&safe_filename(&filename, "video.mp4"), &extension),
                Err(error) => {
                    // The probe failed: no task exists, so drop the
                    // pre-probe control and record instead of leaving
                    // a Downloading row behind.
                    let error_text = error.to_string();
                    if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_id); }
                    with_job_store(|store| {
                        let _ = store.update_state(&job_id, JobState::Failed, Some("Could not start"), Some(&error_text));
                    });
                    return Err(error_text);
                }
            }
        }
    };
    // Resume reuses the paused job's target so the on-disk HLS checkpoint
    // is found; a fresh job reserves a new target, as before. The
    // target_path argument is optional for existing callers. The target
    // path is returned (like start_download) so the frontend learns it
    // at start and can offer Cancel while the job is running.
    let target = match target_path {
        // P1-20: a resume target comes from the frontend — refuse it
        // when it points outside the download directory.
        Some(path) => validated_resume_target(&path)?,
        None => reserve_download_path(&default_download_dir().join(&name)).await.map_err(|error| error.to_string())?,
    };
    // P1-19: a Cancel that landed during the probe already flipped
    // the control to Cancelled — abort before spawning anything, so
    // no bytes are written. The row flips to Cancelled and the event
    // fires here; the frontend's dispatcher ignores this exact error
    // instead of marking the row Failed.
    if *control_rx.borrow() == DownloadControl::Cancelled {
        if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_id); }
        with_job_store(|store| {
            let _ = store.update_state(&job_id, JobState::Cancelled, Some("Cancelled"), None);
        });
        let _ = app.emit("download-cancelled", json!({ "jobId": job_id }));
        return Err("Download cancelled before it started.".into());
    }
    // Correct the pre-probe record with the reserved target and the
    // probed name, so a resume finds the checkpoint under this target.
    with_job_store(|store| {
        if let Some(mut stored) = store.get(&job_id).cloned() {
            stored.target_path = Some(target.to_string_lossy().to_string());
            stored.name = name.clone();
            let _ = store.upsert(stored);
        }
    });
    // The session was loaded up front for the extension probe above;
    // it is the stored browser handoff for a desktop resume (None for
    // desktop jobs).
    // Keep a second receiver alive in the task until the outcome is
    // reported: a cancel that arrives while the download is finishing
    // still reaches a live receiver, so `cancel_download` never falls into
    // its cleanup branch for a job that just completed (D9).
    let transition_rx = control_rx.clone();
    let job_for_task = job_id.clone();
    let app_for_task = app.clone();
    let task_target = target.clone();
    // P2: stop the progress task the moment the transfer is done, so
    // buffered progress events cannot land after the terminal event.
    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_done_for_task = progress_done.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                if progress_done_for_task.load(Ordering::SeqCst) {
                    break;
                }
                // Persist progress (throttled inside the store) so a
                // crash keeps the last reported position; the checkpoint
                // on disk stays authoritative for resume.
                let (downloaded, total, percent) = (progress.downloaded, progress.total, progress.percent);
                let (segments_done, segments_total) = (progress.segments_done, progress.segments_total);
                with_job_store(|store| {
                    let _ = store.note_progress(&progress_id, downloaded, total, percent, segments_done, segments_total);
                });
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_hls_resumable(&url, &selection, &task_target, session.as_ref(), control_rx, progress_tx, rate_limiter(), connections).await;
        progress_done.store(true, Ordering::SeqCst);
        let cancelled_while_finishing = *transition_rx.borrow() == DownloadControl::Cancelled;
        finish_desktop_download(&app_for_task, &job_for_task, &task_target, result.map_err(|error| error_text(&error)), cancelled_while_finishing).await;
    });
    Ok(target.to_string_lossy().to_string())
}

/// Slice 6: a live HLS recording. Mirrors start_hls_download, but
/// the engine records the live window as it arrives instead of
/// downloading a fixed VOD playlist — so there is no checkpoint and
/// no resume: a pause or cancel keeps the partial .ts file, because
/// the partial file IS the recording. The target is forced to .ts
/// (a live TS window cannot be muxed into .mp4 without a remuxer).
/// A restart re-records from the live edge; it never resumes.
#[tauri::command]
async fn start_hls_live_download(
    job_id: String,
    url: String,
    representation_id: Option<String>,
    audio_representation_id: Option<String>,
    filename: String,
    target_path: Option<String>,
    scheduled_at: Option<u64>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS playlist URLs are supported".into());
    }
    let raw = safe_filename(&filename, "video.ts");
    // Slice 7c (Gemini claim 9): only a known media extension is
    // replaced, so "my.video" keeps its name ("my.video.ts").
    let name = with_extension(&raw, "ts");
    let selection = HlsSelection {
        media_url: representation_id.clone().filter(|value| !value.trim().is_empty()),
        audio_group: audio_representation_id.clone().filter(|value| !value.trim().is_empty()),
    };
    let target = match target_path {
        // P1-20: a resume target comes from the frontend — refuse it
        // when it points outside the download directory.
        Some(path) => validated_resume_target(&path)?,
        None => reserve_download_path(&default_download_dir().join(&name)).await.map_err(|error| error.to_string())?,
    };
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    // C1: refuse a second live task for the same job — the
    // frontend dispatcher guards this too, but the backend is the
    // last line of defence against a double start.
    register_job_control(&job_id, control_tx)?;
    let mut record = JobRecord::new_desktop(&job_id, &name, &url, JobKind::Hls, Some(&target));
    record.scheduled_at_ms = scheduled_at;
    record.representation_id = representation_id.clone();
    record.audio_representation_id = audio_representation_id.clone();
    // Slice 6 fix (MAJOR): a live recording's durable row carries the
    // live flag, so cancel_download can tell it apart from a VOD job
    // and get_jobs can return it to the frontend.
    record.live = true;
    with_job_store(|store| {
        let _ = store.start_job(record);
    });
    let session = job_session(&job_id);
    let transition_rx = control_rx.clone();
    let job_for_task = job_id.clone();
    let app_for_task = app.clone();
    let task_target = target.clone();
    // P2: stop the progress task the moment the transfer is done, so
    // buffered progress events cannot land after the terminal event.
    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_done_for_task = progress_done.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                if progress_done_for_task.load(Ordering::SeqCst) {
                    break;
                }
                let (downloaded, total, percent) = (progress.downloaded, progress.total, progress.percent);
                let (segments_done, segments_total) = (progress.segments_done, progress.segments_total);
                with_job_store(|store| {
                    let _ = store.note_progress(&progress_id, downloaded, total, percent, segments_done, segments_total);
                });
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_hls_live(&url, &selection, &task_target, session.as_ref(), control_rx, progress_tx, rate_limiter()).await;
        progress_done.store(true, Ordering::SeqCst);
        let cancelled_while_finishing = *transition_rx.borrow() == DownloadControl::Cancelled;
        // The shared finish helper deletes the target when a cancel
        // lands while the job is finishing — for a live recording
        // that would delete the recording itself, so the live finish
        // is inline here: every terminal state keeps the partial file.
        match result.map_err(|error| error_text(&error)) {
            Ok(DownloadOutcome::Completed) => {
                mark_job_completed(&job_for_task, &task_target);
                with_job_store(|store| {
                    let _ = store.update_state(&job_for_task, JobState::Completed, Some(&format!("Saved · {}", task_target.to_string_lossy())), None);
                });
                let _ = app_for_task.emit("download-complete", json!({ "jobId": job_for_task, "path": task_target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Ok(DownloadOutcome::Paused) if cancelled_while_finishing => {
                with_job_store(|store| {
                    let _ = store.update_state(&job_for_task, JobState::Cancelled, Some("Recording stopped · partial file kept"), None);
                });
                let _ = app_for_task.emit("download-cancelled", json!({ "jobId": job_for_task }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Ok(DownloadOutcome::Paused) => {
                with_job_store(|store| {
                    let _ = store.update_state(&job_for_task, JobState::Paused, Some("Paused · partial recording kept"), None);
                });
                let _ = app_for_task.emit("download-paused", json!({ "jobId": job_for_task, "targetPath": task_target.to_string_lossy() }));
            }
            Ok(DownloadOutcome::Cancelled) => {
                with_job_store(|store| {
                    let _ = store.update_state(&job_for_task, JobState::Cancelled, Some("Recording stopped · partial file kept"), None);
                });
                let _ = app_for_task.emit("download-cancelled", json!({ "jobId": job_for_task }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
            Err(error) => {
                let error_text = error.clone();
                with_job_store(|store| {
                    let _ = store.update_state(&job_for_task, JobState::Failed, Some("Transfer interrupted · partial data kept"), Some(&error_text));
                });
                let _ = app_for_task.emit("download-error", json!({ "jobId": job_for_task, "error": error, "targetPath": task_target.to_string_lossy() }));
                if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_for_task); }
            }
        }
    });
    Ok(target.to_string_lossy().to_string())
}

/// Slice 6: a one-shot subtitle download. A direct subtitle file
/// (vtt/ttml/srt) is just a direct download and goes through
/// start_download; this command is only for an HLS subtitle playlist
/// (.m3u8), which the engine assembles into one .vtt file. No job
/// loop, no progress — the caller gets the saved path (or the
/// labelled error) directly.
#[tauri::command]
async fn download_subtitles(playlist_url: String, filename: String) -> Result<String, String> {
    if !(playlist_url.starts_with("http://") || playlist_url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS subtitle URLs are supported".into());
    }
    let raw = safe_filename(&filename, "subtitles.vtt");
    // Slice 7c (Gemini claim 9): only a known extension is replaced,
    // so "my.video" keeps its name ("my.video.vtt").
    let name = with_extension(&raw, "vtt");
    let target = reserve_download_path(&default_download_dir().join(&name)).await.map_err(|error| error.to_string())?;
    let saved = download_hls_subtitles(&playlist_url, &target, None, rate_limiter()).await.map_err(|error| error_text(&error))?;
    Ok(saved.to_string_lossy().to_string())
}

/// Chromium-family browsers vs Firefox: the native-messaging manifest
/// differs (allowed_origins vs allowed_extensions) and so does the
/// extension-ID format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeBrowserFamily {
    Chromium,
    Firefox,
}

fn native_browser_family(browser: &str) -> Option<NativeBrowserFamily> {
    match browser {
        // Slice 7c (Gemini claim 11): Opera is a Chromium browser —
        // the extension ZIP is Chromium-compatible and the install
        // script already registers its native-messaging key — so it
        // maps to the Chromium family. On Windows it shares Chrome's
        // registry key and manifest (see `windows_native_registry_path`
        // and `register_native_host`); on Linux/macOS it keeps its own
        // manifest dir below.
        "chrome" | "chromium" | "edge" | "brave" | "opera" => Some(NativeBrowserFamily::Chromium),
        "firefox" => Some(NativeBrowserFamily::Firefox),
        _ => None,
    }
}

fn validate_native_extension_id(
    family: NativeBrowserFamily,
    extension_id: &str,
) -> Result<(), String> {
    match family {
        NativeBrowserFamily::Firefox => {
            if extension_id.is_empty()
                || extension_id.len() > 128
                || !extension_id
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || "@._+-".contains(ch))
            {
                Err("Enter the exact Firefox add-on ID.".into())
            } else {
                Ok(())
            }
        }
        NativeBrowserFamily::Chromium => {
            if extension_id.len() != 32 || !extension_id.chars().all(|ch| ('a'..='p').contains(&ch))
            {
                Err("Chromium extension IDs must be exactly 32 characters (a–p).".into())
            } else {
                Ok(())
            }
        }
    }
}

/// The native-messaging manifest, identical on every platform; only the
/// install location differs (Windows registry, browser config dirs
/// elsewhere). Chrome docs:
/// https://developer.chrome.com/docs/extensions/develop/concepts/native-messaging
/// Firefox docs:
/// https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/Native_messaging
fn native_host_manifest(
    family: NativeBrowserFamily,
    extension_id: &str,
    host_path: &Path,
) -> Value {
    let mut manifest = match family {
        NativeBrowserFamily::Chromium => {
            native_host_manifest_chromium(&[chrome_extension_origin(extension_id)], host_path)
        }
        NativeBrowserFamily::Firefox => {
            json!({"name":"com.thunderload.host","description":"ThunderLoad local download engine","type":"stdio","allowed_extensions":[extension_id]})
        }
    };
    // Native messaging manifests must contain the absolute executable path.
    manifest["path"] = json!(host_path.to_string_lossy().to_string());
    manifest
}

/// One Chrome-extension origin string for a validated extension ID.
fn chrome_extension_origin(extension_id: &str) -> String {
    format!("chrome-extension://{extension_id}/")
}

/// Chromium native-messaging manifest allowlisting several extension
/// IDs. Chrome and Opera share one Windows registry key (see
/// `windows_native_registry_path`), so they share the manifest behind
/// it and the manifest must allowlist both of their IDs — a manifest
/// with a single origin is what made the last registration win.
fn native_host_manifest_chromium(origins: &[String], host_path: &Path) -> Value {
    json!({"name":"com.thunderload.host","description":"ThunderLoad local download engine","type":"stdio","allowed_origins":origins,"path":host_path.to_string_lossy().to_string()})
}

/// Whether this browser registers through the shared Chrome Windows
/// registry key (and therefore the shared manifest behind it).
/// Windows-only in production; the tests below exercise the merge.
#[cfg(any(target_os = "windows", test))]
fn is_chrome_key_browser(browser: &str) -> bool {
    matches!(browser, "chrome" | "opera")
}

/// Path of the one manifest behind the shared Chrome registry key, in
/// the Windows install dir. It keeps Chrome's name: it is the file the
/// Chrome key has always pointed at for Chrome.
#[cfg(target_os = "windows")]
fn chrome_key_manifest_path(install_dir: &Path) -> PathBuf {
    install_dir.join("chrome-native-host.json")
}

/// `allowed_origins` already recorded in a manifest file on disk, if
/// the file exists and parses; anything else reads as "no origins yet".
/// Never fails: a corrupt or missing file simply contributes nothing.
/// Windows-only in production; the tests below exercise the merge.
#[cfg(any(target_os = "windows", test))]
fn manifest_allowed_origins(manifest_path: &Path) -> Vec<String> {
    let text = match std::fs::read_to_string(manifest_path) {
        Ok(text) => text,
        Err(_) => return Vec::new(),
    };
    let manifest: Value = match serde_json::from_str(&text) {
        Ok(manifest) => manifest,
        Err(_) => return Vec::new(),
    };
    manifest["allowed_origins"]
        .as_array()
        .map(|origins| {
            origins
                .iter()
                .filter_map(|origin| origin.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Merged `allowed_origins` for the shared Chrome-key manifest: the
/// origin being registered now, plus every origin already recorded in
/// the shared manifest and in the pre-fix per-browser Opera manifest
/// (the only place a pre-fix Opera registration recorded its ID).
/// Sorted and de-duplicated, so registering the same IDs again writes
/// byte-identical content — the startup re-register is a no-op in
/// effect, and a run that only knows one browser's ID never drops the
/// other's. One wart, accepted: when a user changes one browser's
/// extension ID, the old ID's origin stays allowlisted (the manifest
/// alone cannot tell which browser an origin belongs to). A stale
/// origin is harmless — Chrome only connects an extension that is
/// actually installed under that ID — and it disappears if the
/// manifest is ever re-created from scratch.
/// Windows-only in production; the tests below exercise the merge.
#[cfg(any(target_os = "windows", test))]
fn merged_chrome_key_origins(
    shared_manifest_path: &Path,
    legacy_opera_manifest_path: &Path,
    extension_id: &str,
) -> Vec<String> {
    let mut origins = vec![chrome_extension_origin(extension_id)];
    origins.extend(manifest_allowed_origins(shared_manifest_path));
    origins.extend(manifest_allowed_origins(legacy_opera_manifest_path));
    origins.sort();
    origins.dedup();
    origins
}

/// Where a browser looks for native-messaging manifests (Linux).
#[cfg(target_os = "linux")]
fn native_manifest_dir(browser: &str, home: &Path) -> Option<PathBuf> {
    let dir = match browser {
        "chrome" => home.join(".config/google-chrome/NativeMessagingHosts"),
        "chromium" => home.join(".config/chromium/NativeMessagingHosts"),
        "edge" => home.join(".config/microsoft-edge/NativeMessagingHosts"),
        "brave" => home.join(".config/BraveSoftware/Brave-Browser/NativeMessagingHosts"),
        "opera" => home.join(".config/opera/NativeMessagingHosts"),
        "firefox" => home.join(".mozilla/native-messaging-hosts"),
        _ => return None,
    };
    Some(dir)
}

/// Where a browser looks for native-messaging manifests (macOS).
#[cfg(target_os = "macos")]
fn native_manifest_dir(browser: &str, home: &Path) -> Option<PathBuf> {
    let support = home.join("Library/Application Support");
    let dir = match browser {
        "chrome" => support.join("Google/Chrome/NativeMessagingHosts"),
        "chromium" => support.join("Chromium/NativeMessagingHosts"),
        "edge" => support.join("Microsoft Edge/NativeMessagingHosts"),
        "brave" => support.join("BraveSoftware/Brave-Browser/NativeMessagingHosts"),
        "opera" => support.join("com.operasoftware.Opera/NativeMessagingHosts"),
        "firefox" => support.join("Mozilla/NativeMessagingHosts"),
        _ => return None,
    };
    Some(dir)
}

/// Windows registry key for one browser's native-messaging host.
/// Chromium has a family but no Windows registry path here, so it maps
/// to None and callers reject it with the same message as before.
#[cfg(target_os = "windows")]
fn windows_native_registry_path(browser: &str) -> Option<&'static str> {
    match browser {
        "chrome" => Some(r"HKCU\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host"),
        "edge" => Some(r"HKCU\Software\Microsoft\Edge\NativeMessagingHosts\com.thunderload.host"),
        "brave" => Some(
            r"HKCU\Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.thunderload.host",
        ),
        // H9: Opera is Chromium-based and reads Chrome's native-messaging
        // key — there is no working "Opera Software" key. Registering
        // Opera under its own key wrote a value no Opera build ever
        // reads. It shares Chrome's key — and therefore the one manifest
        // behind it (see `register_native_host`): the shared manifest
        // allowlists BOTH browsers' extension IDs, so registering one
        // no longer overwrites the other's registration.
        "opera" => Some(r"HKCU\Software\Google\Chrome\NativeMessagingHosts\com.thunderload.host"),
        "firefox" => Some(r"HKCU\Software\Mozilla\NativeMessagingHosts\com.thunderload.host"),
        _ => None,
    }
}

/// Whether the manifest on disk actually authorises this extension ID
/// and points at a host binary that still exists. A stale manifest
/// (different ID, deleted host) does not count as registered.
fn native_host_manifest_matches(
    manifest_path: &Path,
    family: NativeBrowserFamily,
    extension_id: &str,
) -> bool {
    let text = match std::fs::read_to_string(manifest_path) {
        Ok(text) => text,
        Err(_) => return false,
    };
    let manifest: Value = match serde_json::from_str(&text) {
        Ok(manifest) => manifest,
        Err(_) => return false,
    };
    let authorised = match family {
        NativeBrowserFamily::Chromium => {
            manifest["allowed_origins"]
                .as_array()
                .map_or(false, |origins| {
                    origins.iter().any(|origin| {
                        origin.as_str() == Some(&format!("chrome-extension://{extension_id}/"))
                    })
                })
        }
        NativeBrowserFamily::Firefox => manifest["allowed_extensions"]
            .as_array()
            .map_or(false, |ids| {
                ids.iter().any(|id| id.as_str() == Some(extension_id))
            }),
    };
    if !authorised {
        return false;
    }
    manifest["path"]
        .as_str()
        .map_or(false, |host| Path::new(host).is_file())
}

/// Content hash of the bundled native-host binary (FNV-1a, 64-bit).
/// The installed copy is named after its content — `thunderload-host-<hash>.exe`
/// — so re-installing the same binary never rewrites its file, and a new
/// binary never overwrites the file a running host executes from.
///
/// A tiny hand-rolled hash is deliberate: the desktop crate has no hash
/// dependency, and this value is only a filename discriminator (the
/// byte-for-byte comparison in `install_native_host_binary` is the real
/// identity check). `DefaultHasher` must NOT be used here: it is randomly
/// seeded, so the filename would change on every launch.
fn native_host_content_hash(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// Installed file name for one host binary version.
fn native_host_binary_name(hash: &str, windows: bool) -> String {
    if windows {
        format!("thunderload-host-{hash}.exe")
    } else {
        format!("thunderload-host-{hash}")
    }
}

/// Display name for the browser in user-facing messages.
fn native_browser_display_name(browser: &str) -> &str {
    match browser {
        "chrome" => "Google Chrome",
        "chromium" => "Chromium",
        "edge" => "Microsoft Edge",
        "brave" => "Brave",
        "opera" => "Opera",
        "firefox" => "Firefox",
        _ => browser,
    }
}

/// The one user-facing message for a locked host file, or `None` when the
/// raw OS error is not a sharing violation. Pure (takes the raw code, not
/// the `io::Error`) so it is unit-testable on any platform: Windows reports
/// a locked executable as raw OS error 32 (`ERROR_SHARING_VIOLATION`);
/// the raw code is logged by the caller and never shown to the user.
fn native_host_locked_message(raw_os_error: Option<i32>, browser: &str) -> Option<String> {
    if raw_os_error != Some(32) {
        return None;
    }
    Some(format!(
        "The native host is still running. Close {} completely and try again.",
        native_browser_display_name(browser)
    ))
}

/// Maps an install failure to the message the user sees. A sharing
/// violation becomes an action ("close the browser and retry"); anything
/// else keeps the existing "Could not install native host: …" wording.
/// The raw error is also logged so the exact OS failure is not lost.
fn describe_host_install_error(error: &std::io::Error, browser: &str) -> String {
    eprintln!("[ThunderLoad] native host install failed: {error}");
    native_host_locked_message(error.raw_os_error(), browser)
        .unwrap_or_else(|| format!("Could not install native host: {error}"))
}

/// Installs the bundled native-host binary into `install_dir` and returns
/// the path the browser manifest must point at.
///
/// Windows locks a running executable's file against writes, so the old
/// code — `fs::copy` over a fixed `thunderload-host.exe` — failed with
/// "The process cannot access the file because it is being used by another
/// process. (os error 32)" whenever Chrome already had the host running
/// (the extension holds a persistent `connectNative` port, so the host
/// runs for as long as the browser is open). Re-registering the same
/// version therefore failed even though nothing needed to change.
///
/// This installs content-addressed instead:
/// - identical bytes already installed → no write at all, no lock failure;
/// - different bytes → a new `thunderload-host-<hash>` file, so the file a
///   running host executes from is never overwritten (killing the host is
///   deliberately avoided: it is the download engine for browser jobs,
///   and killing it mid-download would abort in-flight downloads; Chrome
///   spawns a fresh host on the extension's next connect anyway).
///
/// Stale installs are deliberately left in place, never deleted: another
/// browser's manifest may still point at an older version (each browser
/// re-registers on its own schedule), and deleting its file would break
/// that browser until the next app start. An orphaned install is inert —
/// one ~2.5 MB file per host-binary change — and the manifest is the only
/// thing that decides which one runs.
fn install_native_host_binary(
    source: &Path,
    install_dir: &Path,
    browser: &str,
) -> Result<PathBuf, String> {
    let bytes = std::fs::read(source)
        .map_err(|error| format!("Could not read native host binary: {error}"))?;
    std::fs::create_dir_all(install_dir).map_err(|error| error.to_string())?;
    let dest = install_dir.join(native_host_binary_name(
        &native_host_content_hash(&bytes),
        cfg!(target_os = "windows"),
    ));
    if let Ok(installed) = std::fs::read(&dest) {
        if installed == bytes {
            return Ok(dest);
        }
    }
    std::fs::write(&dest, &bytes).map_err(|error| describe_host_install_error(&error, browser))?;
    Ok(dest)
}

#[tauri::command]
async fn register_native_host(
    browser: String,
    extension_id: String,
    app: tauri::AppHandle,
) -> Result<String, String> {
    let browser = browser.trim().to_ascii_lowercase();
    let extension_id = extension_id.trim();
    #[cfg(target_os = "windows")]
    {
        let family = native_browser_family(&browser)
            .ok_or_else(|| "Choose Chrome, Edge, Brave or Firefox.".to_string())?;
        validate_native_extension_id(family, extension_id)?;
        let registry_path = windows_native_registry_path(&browser)
            .ok_or_else(|| "Choose Chrome, Edge, Brave or Firefox.".to_string())?;
        let resource_dir = app.path().resource_dir().map_err(|error| error.to_string())?;
        let source = resource_dir.join("native-host").join("thunderload-host.exe");
        if !source.is_file() {
            return Err(format!("Native host binary was not found: {}", source.display()));
        }
        let local_app_data = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from).ok_or_else(|| "LOCALAPPDATA is not available.".to_string())?;
        let install_dir = local_app_data.join("ThunderLoad").join("native-host");
        let host_path = install_native_host_binary(&source, &install_dir, &browser)?;
        // P0-4 (Gemini cross-check): Chrome and Opera share one Windows
        // registry key, so they must share the one manifest behind it.
        // Before this fix each wrote its own single-origin manifest and
        // pointed the same key at it, so the last registration won and
        // the startup re-register flipped the key on every launch. The
        // shared manifest merges both browsers' extension IDs; the
        // pre-fix per-browser Opera manifest is folded in (its origins
        // move into the shared file) and then removed, since nothing
        // points at it any more. Other browsers keep their own manifest.
        let legacy_opera_manifest = install_dir.join("opera-native-host.json");
        let (manifest, manifest_path) = if is_chrome_key_browser(&browser) {
            let shared_path = chrome_key_manifest_path(&install_dir);
            let origins =
                merged_chrome_key_origins(&shared_path, &legacy_opera_manifest, extension_id);
            (
                native_host_manifest_chromium(&origins, &host_path),
                shared_path,
            )
        } else {
            (
                native_host_manifest(family, extension_id, &host_path),
                install_dir.join(format!("{browser}-native-host.json")),
            )
        };
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
        std::fs::write(&manifest_path, bytes).map_err(|error| format!("Could not write native host manifest: {error}"))?;
        let status = hidden_command("reg").args([
            "add", registry_path, "/ve", "/t", "REG_SZ", "/d",
            manifest_path.to_string_lossy().as_ref(), "/f"
        ]).status().map_err(|error| format!("Could not register native host in Windows: {error}"))?;
        if !status.success() { return Err("Windows refused to register the ThunderLoad native host.".into()); }
        if is_chrome_key_browser(&browser) {
            // Only once the shared manifest is written AND the key
            // points at it: the pre-fix Opera manifest is dead from
            // here on, and leaving it would look like a live
            // registration to anyone (or any old build) reading the
            // install dir.
            let _ = std::fs::remove_file(&legacy_opera_manifest);
        }
        Ok(format!("{browser} native host registered. Restart the browser and grant Native messaging permission in the extension popup."))
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let family = native_browser_family(&browser)
            .ok_or_else(|| "Choose Chrome, Chromium, Edge, Brave or Firefox.".to_string())?;
        validate_native_extension_id(family, extension_id)?;
        let home = home_dir()?;
        let manifest_dir = native_manifest_dir(&browser, &home)
            .ok_or_else(|| "Choose Chrome, Chromium, Edge, Brave or Firefox.".to_string())?;
        // The bundled resource keeps the Windows file name
        // (thunderload-host.exe): on Linux/macOS the build copies the
        // platform binary to that resource name, and registration installs
        // it under its native name.
        let resource_dir = app
            .path()
            .resource_dir()
            .map_err(|error| error.to_string())?;
        let source = resource_dir
            .join("native-host")
            .join("thunderload-host.exe");
        if !source.is_file() {
            return Err(format!(
                "Native host binary was not found: {}",
                source.display()
            ));
        }
        let install_dir = app
            .path()
            .app_data_dir()
            .map_err(|error| error.to_string())?
            .join("native-host");
        let host_path = install_native_host_binary(&source, &install_dir, &browser)?;
        std::fs::set_permissions(&host_path, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| error.to_string())?;
        let manifest = native_host_manifest(family, extension_id, &host_path);
        let manifest_path = manifest_dir.join("com.thunderload.host.json");
        std::fs::create_dir_all(&manifest_dir).map_err(|error| error.to_string())?;
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
        std::fs::write(&manifest_path, bytes)
            .map_err(|error| format!("Could not write native host manifest: {error}"))?;
        Ok(format!("{browser} native host registered. Restart the browser and grant Native messaging permission in the extension popup."))
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        let _ = (browser, extension_id, app);
        Err("Browser native-host setup is not supported on this platform.".into())
    }
}

/// Whether the native host is currently registered for this browser and
/// extension ID. The desktop checks this on startup so it can
/// re-register silently when a saved ID exists but the registration
/// went missing (browser reset, registry cleaner, moved install).
/// Windows: the registry key must exist and point at the manifest file
/// on disk. Other platforms: the manifest file in the browser's
/// NativeMessagingHosts directory must authorise this ID. A manifest
/// naming a different ID never counts as registered.
#[tauri::command]
async fn check_native_host(browser: String, extension_id: String) -> Result<bool, String> {
    let browser = browser.trim().to_ascii_lowercase();
    let extension_id = extension_id.trim();
    #[cfg(target_os = "windows")]
    {
        let family = native_browser_family(&browser)
            .ok_or_else(|| "Choose Chrome, Edge, Brave or Firefox.".to_string())?;
        validate_native_extension_id(family, extension_id)?;
        let registry_path = windows_native_registry_path(&browser)
            .ok_or_else(|| "Choose Chrome, Edge, Brave or Firefox.".to_string())?;
        let local_app_data = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| "LOCALAPPDATA is not available.".to_string())?;
        // P0-4: Chrome and Opera share one registry key and one manifest
        // behind it, so both check the shared file — checking a
        // per-browser file here is what made the startup loop
        // re-register (and flip the key) on every launch.
        let manifest_path = if is_chrome_key_browser(&browser) {
            local_app_data
                .join("ThunderLoad")
                .join("native-host")
                .join("chrome-native-host.json")
        } else {
            local_app_data
                .join("ThunderLoad")
                .join("native-host")
                .join(format!("{browser}-native-host.json"))
        };
        if !native_host_manifest_matches(&manifest_path, family, extension_id) {
            return Ok(false);
        }
        let output = hidden_command("reg")
            .args(["query", registry_path, "/ve"])
            .output()
            .map_err(|error| format!("Could not read native host registration: {error}"))?;
        Ok(output.status.success()
            && String::from_utf8_lossy(&output.stdout)
                .contains(&manifest_path.to_string_lossy().into_owned()))
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let family = native_browser_family(&browser)
            .ok_or_else(|| "Choose Chrome, Chromium, Edge, Brave or Firefox.".to_string())?;
        validate_native_extension_id(family, extension_id)?;
        let home = home_dir()?;
        let manifest_dir = native_manifest_dir(&browser, &home)
            .ok_or_else(|| "Choose Chrome, Chromium, Edge, Brave or Firefox.".to_string())?;
        Ok(native_host_manifest_matches(
            &manifest_dir.join("com.thunderload.host.json"),
            family,
            extension_id,
        ))
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        let _ = (browser, extension_id);
        Err("Browser native-host setup is not supported on this platform.".into())
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
    // P1-20: the destination comes from the frontend's save dialog —
    // refuse a path outside the download directory instead of writing
    // a bundled binary anywhere on disk. Pick a location inside the
    // ThunderLoad downloads folder.
    if !is_within_download_dir(&destination) {
        return Err("Save the extension package inside the ThunderLoad downloads folder.".into());
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
    target_path: Option<String>,
    connections: usize,
    scheduled_at: Option<u64>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS DASH manifests are supported".into());
    }
    if representation_id.trim().is_empty() {
        return Err("A DASH representation must be selected before downloading".into());
    }
    let name = safe_filename(&filename, "video.mp4");
    // Stored browser handoff for a desktop resume (None for a fresh
    // desktop job). Loaded up front because the fresh-job extension
    // probe below needs it before start_job runs.
    //
    // P1-19: the control channel is registered and the job recorded
    // BEFORE the probe below — a Cancel during the probe then finds a
    // live control, and the start aborts after the probe instead of
    // writing to disk. The record carries no target yet and is
    // corrected below once the target is reserved.
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    register_job_control(&job_id, control_tx)?;
    let mut record = JobRecord::new_desktop(&job_id, &name, &url, JobKind::Dash, None);
    record.scheduled_at_ms = scheduled_at;
    record.representation_id = Some(representation_id.clone());
    record.audio_representation_id = audio_representation_id.clone();
    with_job_store(|store| {
        let _ = store.start_job(record);
    });
    let session = job_session(&job_id);
    // Resume reuses the paused job's target so the on-disk DASH checkpoint
    // is found; a fresh job resolves the manifest first so the reserved
    // target gets the right extension (webm for a video-only WebM track),
    // then reserves it. The target path is returned (like start_download)
    // so the frontend learns it at start and can offer Pause/Cancel while
    // the job is running.
    let target = match target_path {
        // P1-20: a resume target comes from the frontend — refuse it
        // when it points outside the download directory.
        Some(path) => validated_resume_target(&path)?,
        None => {
            let extension = match dash_output_extension(&url, &representation_id, audio_representation_id.as_deref(), session.as_ref()).await {
                Ok(extension) => extension,
                Err(error) => {
                    // The probe failed: no task exists, so drop the
                    // pre-probe control and record instead of leaving
                    // a Downloading row behind.
                    let error_text = error.to_string();
                    if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_id); }
                    with_job_store(|store| {
                        let _ = store.update_state(&job_id, JobState::Failed, Some("Could not start"), Some(&error_text));
                    });
                    return Err(error_text);
                }
            };
            let stem = Path::new(&name).file_stem().and_then(|value| value.to_str()).unwrap_or("video");
            reserve_download_path(&default_download_dir().join(format!("{stem}.{extension}"))).await
                .map_err(|error| error.to_string())?
        }
    };
    // P1-19: a Cancel that landed during the probe already flipped
    // the control to Cancelled — abort before spawning anything, so
    // no bytes are written. The row flips to Cancelled and the event
    // fires here; the frontend's dispatcher ignores this exact error
    // instead of marking the row Failed.
    if *control_rx.borrow() == DownloadControl::Cancelled {
        if let Ok(mut controls) = job_controls().lock() { controls.remove(&job_id); }
        with_job_store(|store| {
            let _ = store.update_state(&job_id, JobState::Cancelled, Some("Cancelled"), None);
        });
        let _ = app.emit("download-cancelled", json!({ "jobId": job_id }));
        return Err("Download cancelled before it started.".into());
    }
    // Correct the pre-probe record with the reserved target (and, for
    // a fresh job, the probed extension in the name), so a resume
    // finds the checkpoint under this target.
    with_job_store(|store| {
        if let Some(mut stored) = store.get(&job_id).cloned() {
            stored.target_path = Some(target.to_string_lossy().to_string());
            stored.name = target.file_name().and_then(|value| value.to_str()).unwrap_or(&name).to_string();
            let _ = store.upsert(stored);
        }
    });
    // Keep a second receiver alive in the task until the outcome is
    // reported: a cancel that arrives while the download is finishing
    // still reaches a live receiver, so `cancel_download` never falls into
    // its cleanup branch for a job that just completed (D9).
    let transition_rx = control_rx.clone();
    let job_for_task = job_id.clone();
    let app_for_task = app.clone();
    let task_target = target.clone();
    // P2: stop the progress task the moment the transfer is done, so
    // buffered progress events cannot land after the terminal event.
    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_done_for_task = progress_done.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                if progress_done_for_task.load(Ordering::SeqCst) {
                    break;
                }
                // Persist progress (throttled inside the store) so a
                // crash keeps the last reported position; the checkpoint
                // on disk stays authoritative for resume.
                let (downloaded, total, percent) = (progress.downloaded, progress.total, progress.percent);
                let (segments_done, segments_total) = (progress.segments_done, progress.segments_total);
                with_job_store(|store| {
                    let _ = store.note_progress(&progress_id, downloaded, total, percent, segments_done, segments_total);
                });
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_dash_resumable(
            &url, &representation_id, audio_representation_id.as_deref(), &task_target, session.as_ref(),
            control_rx, progress_tx, rate_limiter(), connections,
        ).await;
        progress_done.store(true, Ordering::SeqCst);
        let cancelled_while_finishing = *transition_rx.borrow() == DownloadControl::Cancelled;
        finish_desktop_download(&app_for_task, &job_for_task, &task_target, result.map_err(|error| error_text(&error)), cancelled_while_finishing).await;
    });
    Ok(target.to_string_lossy().to_string())
}

// --- Slice 3: yt-dlp routing for YouTube/social pages (option b) ---
// Neither tool is bundled with the installer: yt-dlp and Deno live in
// %LOCALAPPDATA%\ThunderLoad\tools and are fetched on first use, only
// after the user taps the direct download button — the tap is the
// consent, never a silent background fetch — SHA256-verified against
// the pinned sums in engine/src/ytdlp.rs. Licenses: yt-dlp is Unlicense
// (public domain), Deno is MIT. Neither binary is shipped, so there is
// no installer change and no license file to bundle; the facts are
// recorded here and where the tools are resolved.

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct YtDlpInspectView {
    title: Option<String>,
    variants: Vec<thunderload_engine::VideoVariant>,
    tools_missing: Option<String>,
}

/// Inspect a YouTube/social page with yt-dlp (desktop paste flow has no
/// browser session, so no cookies are handed over). A missing tool is
/// returned as data, not an error, so the UI can show the named
/// download button instead of a bare failure.
#[tauri::command]
async fn inspect_ytdlp_url(url: String) -> Result<YtDlpInspectView, String> {
    match inspect_page(&url, None).await {
        Ok(inspected) => Ok(YtDlpInspectView {
            title: inspected.title,
            variants: inspected.variants,
            tools_missing: None,
        }),
        Err(error) => {
            if let Some(missing) = error.downcast_ref::<ToolsMissing>() {
                // ToolsMissing carries a Vec<YtDlpTool> (there is no
                // single `.tool` field). Inspect needs yt-dlp first;
                // Deno is optional (YouTube falls back without it), so
                // the first missing tool is the one the UI offers to
                // download. The empty case cannot happen (the error is
                // only built with at least one tool) but defaults
                // defensively to yt-dlp.
                Ok(YtDlpInspectView {
                    title: None,
                    variants: Vec::new(),
                    tools_missing: Some(
                        missing
                            .tools
                            .first()
                            .map(|tool| tool.display_name())
                            .unwrap_or("yt-dlp")
                            .to_string(),
                    ),
                })
            } else {
                Err(error.to_string())
            }
        }
    }
}

/// Start a yt-dlp download. Mirrors start_hls_download: same control
/// channel, same durable record, same progress channel and finish path.
/// The format selector is stored as the record's representation_id so a
/// resume re-runs the identical yt-dlp command (yt-dlp resumes from its
/// .part files natively). The produced file may carry a different
/// extension than the reserved target (audio-only -> .m4a), so the
/// finish path is told the produced path when there is one.
#[tauri::command]
async fn start_ytdlp_download(
    job_id: String,
    url: String,
    format_selector: String,
    filename: String,
    target_path: Option<String>,
    scheduled_at: Option<u64>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS page URLs are supported".into());
    }
    if format_selector.trim().is_empty() {
        return Err("Pick a format first".into());
    }
    let name = safe_filename(&filename, "video.mp4");
    // Resume reuses the paused job's target so yt-dlp finds its .part
    // files; a fresh job reserves a new target, as before.
    let target = match target_path {
        // P1-20: a resume target comes from the frontend — refuse it
        // when it points outside the download directory.
        Some(path) => validated_resume_target(&path)?,
        None => reserve_download_path(&default_download_dir().join(&name)).await.map_err(|error| error.to_string())?,
    };
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    // C1: refuse a second live task for the same job — the
    // frontend dispatcher guards this too, but the backend is the
    // last line of defence against a double start.
    register_job_control(&job_id, control_tx)?;
    let mut record = JobRecord::new_desktop(&job_id, &name, &url, JobKind::YtDlp, Some(&target));
    record.scheduled_at_ms = scheduled_at;
    record.representation_id = Some(format_selector.clone());
    with_job_store(|store| {
        let _ = store.start_job(record);
    });
    let session = job_session(&job_id);
    let transition_rx = control_rx.clone();
    let job_for_task = job_id.clone();
    let app_for_task = app.clone();
    let task_target = target.clone();
    // P2: stop the progress task the moment the transfer is done, so
    // buffered progress events cannot land after the terminal event.
    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_done_for_task = progress_done.clone();
    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let progress_app = app_for_task.clone();
        let progress_id = job_for_task.clone();
        tauri::async_runtime::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                if progress_done_for_task.load(Ordering::SeqCst) {
                    break;
                }
                let (downloaded, total, percent) = (progress.downloaded, progress.total, progress.percent);
                let (segments_done, segments_total) = (progress.segments_done, progress.segments_total);
                with_job_store(|store| {
                    let _ = store.note_progress(&progress_id, downloaded, total, percent, segments_done, segments_total);
                });
                let _ = progress_app.emit("download-progress", json!({ "jobId": progress_id, "progress": progress }));
            }
        });
        let result = download_page(&url, &format_selector, &task_target, session.as_ref(), control_rx, progress_tx).await;
        progress_done.store(true, Ordering::SeqCst);
        let cancelled_while_finishing = *transition_rx.borrow() == DownloadControl::Cancelled;
        match result {
            Ok((outcome, produced)) => {
                // On completion report the produced path; on pause/cancel
                // there is none and the reserved target (whose stem names
                // the .part files) is the right path.
                let report_target = produced.unwrap_or_else(|| task_target.clone());
                finish_desktop_download(&app_for_task, &job_for_task, &report_target, Ok(outcome), cancelled_while_finishing).await;
            }
            Err(error) => {
                finish_desktop_download(&app_for_task, &job_for_task, &task_target, Err(error.to_string()), cancelled_while_finishing).await;
            }
        }
    });
    Ok(target.to_string_lossy().to_string())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct YtDlpToolsStatusView {
    ytdlp: ToolStatus,
    deno: ToolStatus,
    tools_dir: String,
}

#[tauri::command]
async fn get_ytdlp_tools_status() -> Result<YtDlpToolsStatusView, String> {
    Ok(YtDlpToolsStatusView {
        ytdlp: tool_status(YtDlpTool::YtDlp).await,
        deno: tool_status(YtDlpTool::Deno).await,
        tools_dir: tools_dir().to_string_lossy().to_string(),
    })
}

/// Download one tool (yt-dlp or Deno) on demand. Called ONLY from the
/// Settings download button — the tap is the consent. Progress is
/// emitted as "ytdlp-tool-progress" events carrying the tool's display
/// name so the UI can say "yt-dlp download ho raha hai — 45%".
/// Routed through ensure_tool, not download_tool: a second tap when
/// the tool is already present no-ops instead of re-downloading 23 MB
/// / 41 MB (and on Windows renaming over a running binary fails).
#[tauri::command]
async fn ensure_ytdlp_tool(tool: String, app: tauri::AppHandle) -> Result<String, String> {
    let tool = match tool.as_str() {
        "ytdlp" => YtDlpTool::YtDlp,
        "deno" => YtDlpTool::Deno,
        _ => return Err("Unknown tool".into()),
    };
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ToolDownloadProgress>();
    let progress_app = app.clone();
    let tool_name = tool.display_name().to_string();
    tauri::async_runtime::spawn(async move {
        while let Some(progress) = progress_rx.recv().await {
            let _ = progress_app.emit("ytdlp-tool-progress", json!({
                "tool": tool_name,
                "downloaded": progress.downloaded,
                "total": progress.total,
                "percent": progress.percent,
            }));
        }
    });
    let path = ensure_tool(tool, Some(progress_tx)).await.map_err(|error| error.to_string())?;
    Ok(path.to_string_lossy().to_string())
}

#[tauri::command]
async fn check_ytdlp_update() -> Result<String, String> {
    engine_check_ytdlp_update().await.map_err(|error| error.to_string())
}

/// Open the official release page for one tool in the default browser.
/// The manual alternative: the user downloads the file themselves and
/// drops it in the tools folder, and ThunderLoad picks it up.
#[tauri::command]
async fn open_ytdlp_release_page(tool: String) -> Result<(), String> {
    let url = match tool.as_str() {
        "ytdlp" => "https://github.com/yt-dlp/yt-dlp/releases/tag/2026.10.26",
        "deno" => "https://github.com/denoland/deno/releases/tag/v2.9.8",
        _ => return Err("Unknown tool".into()),
    };
    #[cfg(target_os = "windows")]
    let result = hidden_command("cmd").args(["/C", "start", "", url]).spawn();
    #[cfg(target_os = "macos")]
    let result = Command::new("open").arg(url).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let result = Command::new("xdg-open").arg(url).spawn();
    result.map(|_| ()).map_err(|error| error.to_string())
}

#[tauri::command]
async fn open_tools_folder() -> Result<(), String> {
    let folder = tools_dir();
    std::fs::create_dir_all(&folder).map_err(|error| error.to_string())?;
    #[cfg(target_os = "windows")]
    let result = hidden_command("explorer").arg(&folder).spawn();
    #[cfg(target_os = "macos")]
    let result = Command::new("open").arg(&folder).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let result = Command::new("xdg-open").arg(&folder).spawn();
    result.map(|_| ()).map_err(|error| error.to_string())
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
        thunderload_engine::set_ffmpeg_override(None);
        return Ok("Custom FFmpeg path cleared. ThunderLoad will look for ffmpeg on PATH.".into());
    }
    let result = hidden_tokio_command(&value).arg("-version").output().await
        .map_err(|error| format!("Could not start FFmpeg at '{value}': {error}"))?;
    if !result.status.success() {
        return Err(format!("FFmpeg returned a non-zero exit code when checking '{value}'."));
    }
    // P1-20: exit code 0 alone proves nothing — any binary can exit 0.
    // The output must actually identify itself as FFmpeg before the
    // path is saved and the binary is executed for real work.
    let stdout = String::from_utf8_lossy(&result.stdout).into_owned();
    if !stdout.to_lowercase().contains("ffmpeg version") {
        return Err(format!("'{value}' ran but did not identify itself as FFmpeg."));
    }
    let version = stdout.lines().next().unwrap_or("FFmpeg verified").to_string();
    let parent = config_file.parent().ok_or_else(|| "Application config directory is unavailable".to_string())?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| error.to_string())?;
    tokio::fs::write(&config_file, &value).await.map_err(|error| error.to_string())?;
    thunderload_engine::set_ffmpeg_override(Some(PathBuf::from(&value)));
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
                // P1-24: strip the browser session from the copy written
                // back to disk — the in-memory `job` keeps it, because
                // the mirror upsert below needs it for a later desktop
                // handover resume. The rewritten file is 0o600 on Unix:
                // it lives in Downloads, next to files any app can read.
                let mut disk_job = job.clone();
                if let Some(object) = disk_job.as_object_mut() {
                    object.remove("session");
                }
                if let Ok(bytes) = serde_json::to_vec(&disk_job) {
                    let temporary = entry.path().with_extension("json.tmp");
                    if tokio::fs::write(&temporary, bytes).await.is_ok() {
                        #[cfg(unix)]
                        let _ = tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await;
                        let _ = tokio::fs::rename(&temporary, entry.path()).await;
                    }
                }
            }
            jobs.push(job);
        }
    }
    jobs.sort_by_key(|job| std::cmp::Reverse(job.get("createdAt").and_then(Value::as_u64).unwrap_or(0)));
    // Slice 5: mirror browser-started jobs into the durable store, so
    // the database records every job, not just desktop ones. The native
    // host owns the state files — the store only mirrors them. Rows whose
    // state file was dismissed are pruned below (only when the file is
    // really gone; a corrupt-but-present file keeps its row).
    let native_ids: Vec<String> = jobs
        .iter()
        .filter_map(|job| job.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    with_job_store(|store| {
        for job in &jobs {
            if let Some(record) = native_value_to_record(job) {
                if store.get(&record.id).is_some_and(|existing| existing.origin == JobOrigin::Desktop) {
                    continue;
                }
                let _ = store.upsert(record);
            }
        }
    });
    let mut stale: Vec<String> = Vec::new();
    if let Some(store) = JOB_STORE.get() {
        if let Ok(store) = store.lock() {
            for row in store.all() {
                if row.origin == JobOrigin::Native && !native_ids.contains(&row.id) {
                    stale.push(row.id.clone());
                }
            }
        }
    }
    for id in stale {
        if !tokio::fs::try_exists(directory.join(format!("{id}.json"))).await.unwrap_or(true) {
            with_job_store(|store| {
                let _ = store.remove(&id);
            });
        }
    }
    // F1: the native state files carry the browser session, but the
    // frontend must never see it — strip it here. (The mirror upsert
    // above already read it into the store, and the state files on disk
    // keep it.) `get_jobs` strips it the same way.
    for job in &mut jobs {
        if let Some(object) = job.as_object_mut() {
            object.remove("session");
        }
    }
    Ok(jobs)
}

/// Converts one native-host state file (a JSON `Value`) into a job-store
/// row. The native host is the owner of these files; the store only
/// mirrors them, so unknown/missing fields fall back to safe defaults
/// instead of dropping the job.
fn native_value_to_record(job: &Value) -> Option<JobRecord> {
    let id = job.get("id")?.as_str()?.to_string();
    let kind = match job.get("kind").and_then(Value::as_str) {
        Some("hls") => JobKind::Hls,
        Some("dash") => JobKind::Dash,
        Some("ytdlp") => JobKind::YtDlp,
        _ => JobKind::Direct,
    };
    let state = match job.get("status").and_then(Value::as_str) {
        Some("Downloading") => JobState::Downloading,
        Some("Paused") => JobState::Paused,
        Some("Completed") => JobState::Completed,
        Some("Failed") => JobState::Failed,
        Some("Interrupted") => JobState::Interrupted,
        Some("Cancelled") => JobState::Cancelled,
        _ => JobState::Failed,
    };
    let target_path = job.get("targetPath").and_then(Value::as_str).map(str::to_string);
    let now = thunderload_engine::now_ms();
    // The browser session the host stored in the state file, if any.
    // Parsed and re-validated against the job URL; anything malformed
    // or for another origin is dropped (the download then proceeds
    // without it and an authenticated server fails visibly).
    let url = job.get("url").and_then(Value::as_str).unwrap_or("");
    let session = job.get("session")
        .and_then(|value| serde_json::from_value::<SessionHeaders>(value.clone()).ok())
        .filter(|session| session.validate(url).is_ok());
    Some(JobRecord {
        id,
        name: job.get("name").and_then(Value::as_str).unwrap_or("Browser download").to_string(),
        url: url.to_string(),
        kind,
        // Slice 6 fix (MAJOR): the host stores live in the state file,
        // and the mirror must carry it — otherwise a browser-started
        // live recording comes back as a VOD row and its target is not
        // protected from cancel/delete.
        live: job.get("live").and_then(Value::as_bool).unwrap_or(false),
        origin: JobOrigin::Native,
        state,
        downloaded: job.get("downloaded").and_then(Value::as_u64).unwrap_or(0),
        total: job.get("total").and_then(Value::as_u64),
        percent: job.get("percent").and_then(Value::as_f64),
        segments_done: None,
        segments_total: None,
        checkpoint_path: target_path
            .as_ref()
            .map(|target| partial_download_metadata_path(Path::new(target)).to_string_lossy().to_string()),
        target_path,
        representation_id: job.get("representationId").and_then(Value::as_str).map(str::to_string),
        audio_representation_id: job.get("audioRepresentationId").and_then(Value::as_str).map(str::to_string),
        scheduled_at_ms: None,
        session,
        detail: job.get("detail").and_then(Value::as_str).unwrap_or("Started from browser extension").to_string(),
        error: job.get("error").and_then(Value::as_str).map(str::to_string),
        created_at_ms: job.get("createdAt").and_then(Value::as_u64).unwrap_or(now),
        updated_at_ms: job.get("updatedAt").and_then(Value::as_u64).unwrap_or(now),
    })
}

/// All durable jobs (desktop + browser-started mirror), newest first.
/// The frontend seeds its queue from this on startup; browser-started
/// rows keep flowing through the `get_native_downloads` poll as well.
#[tauri::command]
async fn get_jobs() -> Vec<JobRecord> {
    // The session (browser cookies) is stripped here: it lives only in
    // jobs.json on the user's machine and in Rust memory, never in the
    // JS runtime.
    match JOB_STORE.get() {
        Some(store) => store.lock().map(|store| {
            store.all().iter().map(|row| {
                let mut row = row.clone();
                row.session = None;
                row
            }).collect()
        }).unwrap_or_default(),
        None => Vec::new(),
    }
}

/// One frontend queue item as sent by `sync_jobs` — the shape the
/// frontend already keeps in local storage. Only rows the store does
/// not know yet are inserted (migration for pre-Slice-5 history and for
/// queued jobs that never started); existing rows are never overwritten.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncJob {
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    downloaded: u64,
    #[serde(default)]
    total: Option<u64>,
    #[serde(default)]
    percent: Option<f64>,
    #[serde(default)]
    target_path: Option<String>,
    #[serde(default)]
    created_at: u64,
    #[serde(default)]
    scheduled_at: Option<u64>,
    #[serde(default)]
    representation_id: Option<String>,
    #[serde(default)]
    audio_representation_id: Option<String>,
    #[serde(default)]
    detail: String,
    #[serde(default)]
    error: Option<String>,
    /// A live HLS recording (Slice 6 fix): carried so a live row
    /// re-seeded from the frontend keeps its live routing instead of
    /// falling through to the VOD start path, which rejects live
    /// playlists. Absent for rows sent by older frontends.
    #[serde(default)]
    live: bool,
}

fn sync_job_to_record(job: SyncJob) -> JobRecord {
    let kind = match job.kind.as_str() {
        "hls" => JobKind::Hls,
        "dash" => JobKind::Dash,
        "ytdlp" => JobKind::YtDlp,
        _ => JobKind::Direct,
    };
    // Transient UI states never persist: a job that was mid-flight when
    // the app closed goes back to the queue (if it has a target to
    // resume from) or to Interrupted, mirroring the frontend's own
    // startup recovery.
    let state = match job.status.as_str() {
        "Downloading" | "Pausing" => {
            if job.target_path.is_some() { JobState::Queued } else { JobState::Interrupted }
        }
        "Cancelling" => JobState::Cancelled,
        "Paused" => JobState::Paused,
        "Completed" => JobState::Completed,
        "Failed" => JobState::Failed,
        "Interrupted" => JobState::Interrupted,
        "Cancelled" => JobState::Cancelled,
        _ => JobState::Queued,
    };
    let now = thunderload_engine::now_ms();
    JobRecord {
        id: job.id,
        name: job.name,
        url: job.url,
        kind,
        origin: JobOrigin::Desktop,
        state,
        downloaded: job.downloaded,
        total: job.total,
        percent: job.percent,
        segments_done: None,
        segments_total: None,
        checkpoint_path: job
            .target_path
            .as_ref()
            .map(|target| partial_download_metadata_path(Path::new(target)).to_string_lossy().to_string()),
        target_path: job.target_path,
        representation_id: job.representation_id,
        audio_representation_id: job.audio_representation_id,
        scheduled_at_ms: job.scheduled_at,
        session: None,
        detail: job.detail,
        error: job.error,
        live: job.live,
        created_at_ms: if job.created_at == 0 { now } else { job.created_at },
        updated_at_ms: now,
    }
}

#[tauri::command]
async fn sync_jobs(jobs: Vec<SyncJob>) -> Result<usize, String> {
    let mut added = 0;
    with_job_store(|store| {
        for job in jobs {
            if store.get(&job.id).is_some() {
                continue;
            }
            let record = sync_job_to_record(job);
            if store.upsert(record).is_ok() {
                added += 1;
            }
        }
    });
    Ok(added)
}

/// Deletes one durable row (used when the user removes a job from the
/// queue). This never touches files on disk — the caller already
/// removed the checkpoint when one had to go.
#[tauri::command]
async fn remove_job(job_id: String) -> Result<(), String> {
    if !valid_scheduler_job_id(&job_id) {
        return Err("Invalid job identifier.".into());
    }
    with_job_store(|store| {
        let _ = store.remove(&job_id);
    });
    Ok(())
}

/// Marks a queued job Cancelled in the store. Only queued rows change.
/// C2: once `start_job` has run the row is Downloading, not Queued —
/// cancelling it through here would leave a live task running while the
/// row says Cancelled, so a started job is REFUSED (Err) and must go
/// through `cancel_download` instead. A missing row is still Ok: the
/// frontend already flipped locally and there is nothing to refuse.
#[tauri::command]
async fn cancel_queued_job(job_id: String) -> Result<(), String> {
    if !valid_scheduler_job_id(&job_id) {
        return Err("Invalid job identifier.".into());
    }
    let mut refused = false;
    with_job_store(|store| {
        match store.get(&job_id).map(|job| job.state) {
            Some(JobState::Queued) => {
                let _ = store.update_state(&job_id, JobState::Cancelled, Some("Cancelled"), None);
            }
            Some(_) => refused = true,
            None => {}
        }
    });
    if refused {
        return Err("This download has already started — cancel it from the queue instead.".into());
    }
    Ok(())
}

/// Writes one control word (`pause`, `resume` or `cancel`) for a
/// browser-started job, atomically via tmp+rename, so the companion's
/// watcher picks it up within ~250 ms. Shared by `control_native_download`
/// and `dismiss_native_download`.
async fn write_native_control_file(directory: &std::path::Path, job_id: &str, action: &str) -> Result<(), String> {
    tokio::fs::create_dir_all(directory).await.map_err(|error| error.to_string())?;
    let command_path = directory.join(format!("{job_id}.control"));
    let temporary = directory.join(format!("{job_id}.{}.control.tmp", Uuid::new_v4()));
    tokio::fs::write(&temporary, action.as_bytes()).await.map_err(|error| error.to_string())?;
    if let Err(error) = tokio::fs::rename(&temporary, &command_path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.to_string());
    }
    Ok(())
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

    write_native_control_file(&directory, &job_id, &action).await?;
    Ok(())
}

#[tauri::command]
async fn dismiss_native_download(job_id: String) -> Result<(), String> {
    if job_id.is_empty() || job_id.len() > 64
        || !job_id.chars().all(|character| character.is_ascii_alphanumeric() || character == '-') {
        return Err("Invalid browser download identifier.".into());
    }
    let directory = default_download_dir().join(".native-jobs");
    let job_path = directory.join(format!("{job_id}.json"));
    // D10: if the companion may still own this job (Downloading/Paused/
    // Failed), its task is alive — possibly waiting in its pause/retry
    // loop, heartbeating the state file every ten seconds. Ask it to
    // cancel through the control file and wait until it records Cancelled
    // before deleting the state file; deleting first would let the
    // companion's next state save resurrect the job in the desktop queue.
    // A heartbeat stale for more than 45 seconds means the companion is
    // already gone (the same threshold `get_native_downloads` uses for
    // Interrupted), so no wait is needed. Interrupted/Completed/Cancelled
    // jobs have no live owner and their files are deleted immediately.
    if let Ok(bytes) = tokio::fs::read(&job_path).await {
        if let Ok(job) = serde_json::from_slice::<Value>(&bytes) {
            let status = job.get("status").and_then(Value::as_str).unwrap_or("");
            if ["Downloading", "Paused", "Failed"].contains(&status) {
                let updated = job.get("updatedAt").and_then(Value::as_u64).unwrap_or(0);
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_millis() as u64;
                if updated == 0 || now.saturating_sub(updated) <= 45_000 {
                    let _ = write_native_control_file(&directory, &job_id, "cancel").await;
                    let deadline = Instant::now() + Duration::from_secs(5);
                    loop {
                        let cancelled = tokio::fs::read(&job_path).await.ok()
                            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                            .and_then(|job| job.get("status").and_then(Value::as_str).map(str::to_string))
                            .map(|status| status == "Cancelled")
                            .unwrap_or(false);
                        if cancelled || Instant::now() >= deadline { break; }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
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

/// Atomically hands an interrupted native-host download over to the desktop
/// engine: flips its durable JOB_STORE origin to `JobOrigin::Desktop` BEFORE
/// deleting `.native-jobs/{bare_id}.json`, so a concurrent `get_native_downloads`
/// poll can never prune the row or wipe its stored browser session cookies.
#[tauri::command]
async fn handover_native_download(job_id: String) -> Result<(), String> {
    let bare_id = job_id.strip_prefix("native-").unwrap_or(&job_id);
    if bare_id.is_empty() || bare_id.len() > 64
        || !bare_id.chars().all(|character| character.is_ascii_alphanumeric() || character == '-') {
        return Err("Invalid browser download identifier.".into());
    }
    with_job_store(|store| {
        if let Some(mut record) = store.get(&job_id).or_else(|| store.get(bare_id)).cloned() {
            record.origin = JobOrigin::Desktop;
            record.state = JobState::Queued;
            record.detail = "Recovering browser checkpoint in desktop engine".to_string();
            record.error = None;
            let _ = store.upsert(record);
        }
    });
    let directory = default_download_dir().join(".native-jobs");
    for path in [
        directory.join(format!("{bare_id}.json")),
        directory.join(format!("{bare_id}.control")),
        directory.join(format!("{bare_id}.control.tmp")),
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
    let result = hidden_command("explorer").arg(&folder).spawn();
    #[cfg(target_os = "macos")]
    let result = Command::new("open").arg(&folder).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let result = Command::new("xdg-open").arg(&folder).spawn();
    result.map(|_| ()).map_err(|error| error.to_string())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        // C7: one running instance. A second launch (scheduled wake,
        // double-clicked installer shortcut, file association) hands
        // off to the instance already running instead of opening a
        // second window with its own queue — the running window is
        // shown and focused.
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }
        }))
        .setup(|app| {
            if let Ok(config_dir) = app.path().app_config_dir() {
                if let Ok(value) = std::fs::read_to_string(config_dir.join("ffmpeg.path")) {
                    let value = value.trim();
                    if !value.is_empty() { thunderload_engine::set_ffmpeg_override(Some(PathBuf::from(value))); }
                }
                if let Ok(value) = std::fs::read_to_string(config_dir.join("speed-limit.bytes")) {
                    if let Ok(limit) = value.trim().parse::<u64>() { rate_limiter().set_limit(limit); }
                }
            }
            // Slice 5: open the durable job database in the app-data
            // directory (never next to the downloads). Jobs that were
            // Downloading when the app last exited become Interrupted and
            // resumable; checkpoint files with no row are imported as
            // Interrupted jobs. A failure here only loses durability for
            // this run — the frontend's local history still covers the UI.
            if let Ok(data_dir) = app.path().app_data_dir() {
                let _ = std::fs::create_dir_all(&data_dir);
                let (mut store, note) = JobStore::load(&data_dir.join(store_file_name()), Duration::from_secs(5));
                if let Some(note) = note {
                    eprintln!("ThunderLoad job store: {note}");
                }
                match store.mark_interrupted_on_startup() {
                    Ok(0) => {}
                    Ok(count) => eprintln!("ThunderLoad job store: {count} interrupted download(s) recovered"),
                    Err(error) => eprintln!("ThunderLoad job store: could not mark interrupted jobs: {error}"),
                }
                match store.import_orphans(&default_download_dir()) {
                    Ok(imported) if !imported.is_empty() => {
                        eprintln!("ThunderLoad job store: imported {} orphaned checkpoint(s)", imported.len())
                    }
                    Ok(_) => {}
                    Err(error) => eprintln!("ThunderLoad job store: orphan import failed: {error}"),
                }
                let _ = JOB_STORE.set(Mutex::new(store));
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![inspect_video_url, grab_page_media, start_download, pause_download, cancel_download, set_speed_limit, schedule_app_wake, cancel_app_wake, start_hls_download, start_hls_live_download, download_subtitles, start_dash_download, inspect_ytdlp_url, start_ytdlp_download, get_ytdlp_tools_status, ensure_ytdlp_tool, check_ytdlp_update, open_ytdlp_release_page, open_tools_folder, get_ffmpeg_path, set_ffmpeg_path, get_native_downloads, control_native_download, dismiss_native_download, handover_native_download, register_native_host, check_native_host, open_download_folder, save_extension_file, get_jobs, sync_jobs, remove_job, cancel_queued_job])
        .run(tauri::generate_context!())
        .expect("error while running ThunderLoad desktop app");
}

#[cfg(test)]
mod scheduler_tests {
    #[cfg(target_os = "macos")]
    use super::{launchd_wake_plist, wake_datetime_parts, xml_escape};
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use super::{local_now_shaped, wake_executable};
    use super::{powershell_literal, valid_local_datetime_shape, valid_scheduler_job_id};
    #[cfg(target_os = "linux")]
    use super::{systemd_exec_arg, systemd_wake_unit_names, systemd_wake_units};
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::path::{Path, PathBuf};

    #[test]
    fn scheduler_job_ids_allow_only_short_safe_identifiers() {
        assert!(valid_scheduler_job_id(
            "9fa8dd1a-69a1-43d2-9fa4-10b4c2b0ad32"
        ));
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
        assert_eq!(
            powershell_literal("C:\\Tools\\User's App\\ThunderLoad.exe"),
            "C:\\Tools\\User''s App\\ThunderLoad.exe"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn wake_datetime_parts_split_the_local_format() {
        assert_eq!(
            wake_datetime_parts("2026-11-15T10:30"),
            Some((2026, 11, 15, 10, 30))
        );
        assert_eq!(
            wake_datetime_parts("2026-01-02T03:04"),
            Some((2026, 1, 2, 3, 4))
        );
        assert_eq!(wake_datetime_parts("2026/11/15 10:30"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_units_carry_wake_and_persistent() {
        let (timer, service) =
            systemd_wake_units(Path::new("/usr/bin/thunderload"), "2026-11-15T10:30");
        assert!(timer.contains("OnCalendar=2026-11-15 10:30:00"));
        assert!(timer.contains("WakeSystem=true"));
        assert!(timer.contains("Persistent=true"));
        assert!(timer.contains("WantedBy=timers.target"));
        assert!(service.contains("ExecStart=/usr/bin/thunderload"));
        assert!(service.contains("Type=oneshot"));
        let (timer_name, service_name) = systemd_wake_unit_names("job-1");
        assert_eq!(timer_name, "thunderload-wake-job-1.timer");
        assert_eq!(service_name, "thunderload-wake-job-1.service");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_exec_arg_quotes_paths_with_spaces() {
        assert_eq!(
            systemd_exec_arg(Path::new("/opt/Thunder Load/thunderload")),
            "\"/opt/Thunder Load/thunderload\""
        );
        assert_eq!(
            systemd_exec_arg(Path::new("/usr/bin/thunderload")),
            "/usr/bin/thunderload"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_carries_the_calendar_interval() {
        let plist = launchd_wake_plist(
            Path::new("/Applications/ThunderLoad.app/Contents/MacOS/thunderload"),
            "job-1",
            "2026-11-15T10:30",
        )
        .expect("valid datetime");
        assert!(plist.contains("<string>com.thunderload.wake-job-1</string>"));
        assert!(plist
            .contains("<string>/Applications/ThunderLoad.app/Contents/MacOS/thunderload</string>"));
        assert!(plist.contains("<key>Year</key>\n    <integer>2026</integer>"));
        assert!(plist.contains("<key>Month</key>\n    <integer>11</integer>"));
        assert!(plist.contains("<key>Day</key>\n    <integer>15</integer>"));
        assert!(plist.contains("<key>Hour</key>\n    <integer>10</integer>"));
        assert!(plist.contains("<key>Minute</key>\n    <integer>30</integer>"));
        assert!(launchd_wake_plist(Path::new("/bin/true"), "job-1", "2026/11/15 10:30").is_none());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn xml_escape_covers_ampersand_and_brackets() {
        assert_eq!(xml_escape("a & b <c> d"), "a &amp; b &lt;c&gt; d");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn local_now_shaped_matches_the_schedule_format() {
        let now = local_now_shaped().expect("date command available");
        assert!(valid_local_datetime_shape(&now));
        // The validated shape is chronological under lexicographic order,
        // which is what the future-time check relies on.
        assert!("2000-01-01T00:00" < now.as_str());
        assert!("2999-01-01T00:00" > now.as_str());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn wake_executable_prefers_appimage_env() {
        // No other test reads APPIMAGE, so save/restore here is safe.
        let previous = std::env::var_os("APPIMAGE");
        std::env::set_var("APPIMAGE", "/opt/ThunderLoad.AppImage");
        let resolved = wake_executable().expect("APPIMAGE set");
        assert_eq!(resolved, PathBuf::from("/opt/ThunderLoad.AppImage"));
        match previous {
            Some(value) => std::env::set_var("APPIMAGE", value),
            None => std::env::remove_var("APPIMAGE"),
        }
    }
}

#[cfg(test)]
mod native_host_tests {
    use super::{
        describe_host_install_error, install_native_host_binary, is_chrome_key_browser,
        manifest_allowed_origins, merged_chrome_key_origins, native_browser_family,
        native_host_binary_name, native_host_content_hash, native_host_locked_message,
        native_host_manifest, native_host_manifest_chromium, native_host_manifest_matches,
        validate_native_extension_id, NativeBrowserFamily,
    };
    use std::path::Path;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::path::PathBuf;

    #[test]
    fn browser_family_maps_chromium_and_firefox() {
        assert_eq!(
            native_browser_family("chrome"),
            Some(NativeBrowserFamily::Chromium)
        );
        assert_eq!(
            native_browser_family("chromium"),
            Some(NativeBrowserFamily::Chromium)
        );
        assert_eq!(
            native_browser_family("edge"),
            Some(NativeBrowserFamily::Chromium)
        );
        assert_eq!(
            native_browser_family("brave"),
            Some(NativeBrowserFamily::Chromium)
        );
        assert_eq!(
            native_browser_family("firefox"),
            Some(NativeBrowserFamily::Firefox)
        );
        assert_eq!(native_browser_family("safari"), None);
        // Slice 7c (Gemini claim 11): Opera is Chromium — the
        // extension ZIP and the native-messaging manifest format are
        // the same as Chrome's.
        assert_eq!(
            native_browser_family("opera"),
            Some(NativeBrowserFamily::Chromium)
        );
        assert_eq!(native_browser_family(""), None);
    }

    #[test]
    fn extension_id_rules_match_the_windows_command() {
        // Chromium IDs: exactly 32 chars a–p.
        assert!(validate_native_extension_id(
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnop"
        )
        .is_ok());
        assert!(validate_native_extension_id(
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnopq"
        )
        .is_err());
        assert!(validate_native_extension_id(
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnoq"
        )
        .is_err());
        // Firefox IDs: 1–128 chars, alphanumerics plus @._+-
        assert!(validate_native_extension_id(
            NativeBrowserFamily::Firefox,
            "thunderload@example.com"
        )
        .is_ok());
        assert!(validate_native_extension_id(NativeBrowserFamily::Firefox, "").is_err());
        assert!(validate_native_extension_id(NativeBrowserFamily::Firefox, "bad id!").is_err());
    }

    #[test]
    fn manifest_shape_is_chromium_or_firefox() {
        let chromium = native_host_manifest(
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnop",
            Path::new("/home/u/.local/share/com.thunderload.desktop/native-host/thunderload-host"),
        );
        assert_eq!(chromium["name"], "com.thunderload.host");
        assert_eq!(chromium["type"], "stdio");
        assert_eq!(
            chromium["allowed_origins"][0],
            "chrome-extension://abcdefghijklmnopabcdefghijklmnop/"
        );
        assert_eq!(
            chromium["path"],
            "/home/u/.local/share/com.thunderload.desktop/native-host/thunderload-host"
        );
        let firefox = native_host_manifest(
            NativeBrowserFamily::Firefox,
            "thunderload@example.com",
            Path::new("/opt/thunderload-host"),
        );
        assert_eq!(firefox["allowed_extensions"][0], "thunderload@example.com");
        assert_eq!(firefox["path"], "/opt/thunderload-host");
        assert!(firefox.get("allowed_origins").is_none());
    }

    #[test]
    fn manifest_match_needs_the_same_id_and_a_real_host() {
        let dir = std::env::temp_dir().join("thunderload-native-host-check-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let host = dir.join("thunderload-host");
        std::fs::write(&host, b"fake-host").unwrap();
        let manifest_path = dir.join("com.thunderload.host.json");
        let manifest = native_host_manifest(
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnop",
            &host,
        );
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        assert!(native_host_manifest_matches(
            &manifest_path,
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnop"
        ));
        // A different extension ID is not authorised by this manifest.
        assert!(!native_host_manifest_matches(
            &manifest_path,
            NativeBrowserFamily::Chromium,
            "ponmlkjihgfedcbaponmlkjihgfedcba"
        ));
        // A manifest pointing at a deleted host is not a live registration.
        std::fs::remove_file(&host).unwrap();
        assert!(!native_host_manifest_matches(
            &manifest_path,
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnop"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn chrome_key_browsers_share_the_chrome_key() {
        assert!(is_chrome_key_browser("chrome"));
        assert!(is_chrome_key_browser("opera"));
        assert!(!is_chrome_key_browser("edge"));
        assert!(!is_chrome_key_browser("brave"));
        assert!(!is_chrome_key_browser("chromium"));
        assert!(!is_chrome_key_browser("firefox"));
    }

    #[test]
    fn chrome_key_merge_keeps_both_browsers_origins() {
        // P0-4: Chrome and Opera share one Windows registry key, so the
        // manifest behind it must allowlist both extension IDs — and
        // re-registering one browser must never drop the other's.
        let dir = std::env::temp_dir().join("thunderload-chrome-key-merge-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Pre-fix state: Chrome registered its own manifest, Opera its own.
        let shared = dir.join("chrome-native-host.json");
        let legacy_opera = dir.join("opera-native-host.json");
        std::fs::write(
            &shared,
            serde_json::to_vec_pretty(&native_host_manifest_chromium(
                &["chrome-extension://abcdefghijklmnopabcdefghijklmnop/".to_string()],
                Path::new("C:\\host\\thunderload-host.exe"),
            ))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            &legacy_opera,
            serde_json::to_vec_pretty(&native_host_manifest_chromium(
                &["chrome-extension://ponmlkjihgfedcbaponmlkjihgfedcba/".to_string()],
                Path::new("C:\\host\\thunderload-host.exe"),
            ))
            .unwrap(),
        )
        .unwrap();
        // Re-registering Chrome must keep Opera's origin, not drop it.
        let merged =
            merged_chrome_key_origins(&shared, &legacy_opera, "abcdefghijklmnopabcdefghijklmnop");
        assert_eq!(
            merged,
            vec![
                "chrome-extension://abcdefghijklmnopabcdefghijklmnop/".to_string(),
                "chrome-extension://ponmlkjihgfedcbaponmlkjihgfedcba/".to_string(),
            ]
        );
        // Registering Opera (whose ID lived only in the legacy file)
        // must keep Chrome's origin too.
        let merged =
            merged_chrome_key_origins(&shared, &legacy_opera, "ponmlkjihgfedcbaponmlkjihgfedcba");
        assert_eq!(merged.len(), 2);
        // A manifest built from the merged origins authorises both IDs.
        let host = dir.join("thunderload-host");
        std::fs::write(&host, b"fake-host").unwrap();
        let manifest_path = dir.join("merged-native-host.json");
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&native_host_manifest_chromium(&merged, &host)).unwrap(),
        )
        .unwrap();
        assert!(native_host_manifest_matches(
            &manifest_path,
            NativeBrowserFamily::Chromium,
            "abcdefghijklmnopabcdefghijklmnop"
        ));
        assert!(native_host_manifest_matches(
            &manifest_path,
            NativeBrowserFamily::Chromium,
            "ponmlkjihgfedcbaponmlkjihgfedcba"
        ));
        // Merging the merged manifest again is stable — the startup
        // re-register writes the same origins, so it is a no-op.
        let merged_again = merged_chrome_key_origins(
            &manifest_path,
            &legacy_opera,
            "abcdefghijklmnopabcdefghijklmnop",
        );
        assert_eq!(merged_again, merged);
        // A missing or corrupt manifest contributes no origins.
        assert!(manifest_allowed_origins(&dir.join("does-not-exist.json")).is_empty());
        let corrupt = dir.join("corrupt.json");
        std::fs::write(&corrupt, b"{not json").unwrap();
        assert!(manifest_allowed_origins(&corrupt).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn host_content_hash_is_deterministic_and_content_sensitive() {
        assert_eq!(
            native_host_content_hash(b"thunderload-host"),
            native_host_content_hash(b"thunderload-host")
        );
        assert_ne!(
            native_host_content_hash(b"thunderload-host"),
            native_host_content_hash(b"thunderload-hosT")
        );
        // FNV-1a of empty input is the offset basis itself.
        assert_eq!(native_host_content_hash(b""), "cbf29ce484222325");
    }

    #[test]
    fn host_binary_name_keeps_the_windows_suffix() {
        assert_eq!(
            native_host_binary_name("0123456789abcdef", true),
            "thunderload-host-0123456789abcdef.exe"
        );
        assert_eq!(
            native_host_binary_name("0123456789abcdef", false),
            "thunderload-host-0123456789abcdef"
        );
    }

    #[test]
    fn locked_file_message_names_the_browser_and_the_action() {
        assert_eq!(
            native_host_locked_message(Some(32), "chrome"),
            Some(
                "The native host is still running. Close Google Chrome completely and try again."
                    .to_string()
            )
        );
        assert_eq!(
            native_host_locked_message(Some(32), "firefox"),
            Some(
                "The native host is still running. Close Firefox completely and try again."
                    .to_string()
            )
        );
        // Any other OS error is not a sharing violation: no friendly message.
        assert_eq!(native_host_locked_message(Some(2), "chrome"), None);
        assert_eq!(native_host_locked_message(None, "chrome"), None);
    }

    #[test]
    fn install_error_maps_os_error_32_to_the_close_browser_message() {
        let locked = std::io::Error::from_raw_os_error(32);
        assert_eq!(
            describe_host_install_error(&locked, "chrome"),
            "The native host is still running. Close Google Chrome completely and try again."
        );
        let other = std::io::Error::from_raw_os_error(2);
        assert!(describe_host_install_error(&other, "chrome")
            .starts_with("Could not install native host: "));
    }

    #[test]
    fn install_skips_the_write_when_the_same_bytes_are_installed() {
        // The property that fixes the reported bug: re-registering the
        // same host version must not touch the installed file at all, so
        // a running host's lock can never break it.
        let dir = std::env::temp_dir().join("thunderload-native-host-install-skip-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.join("source.exe");
        std::fs::write(&source, b"host-bytes-v1").unwrap();
        let install_dir = dir.join("install");
        let first = install_native_host_binary(&source, &install_dir, "chrome").unwrap();
        let mtime = std::fs::metadata(&first).unwrap().modified().unwrap();
        let second = install_native_host_binary(&source, &install_dir, "chrome").unwrap();
        assert_eq!(first, second);
        assert_eq!(
            std::fs::metadata(&second).unwrap().modified().unwrap(),
            mtime,
            "identical bytes must not rewrite the installed file"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_versions_new_bytes_in_a_new_file() {
        let dir = std::env::temp_dir().join("thunderload-native-host-install-version-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.join("source.exe");
        std::fs::write(&source, b"host-bytes-v1").unwrap();
        let install_dir = dir.join("install");
        let first = install_native_host_binary(&source, &install_dir, "chrome").unwrap();
        assert_eq!(std::fs::read(&first).unwrap(), b"host-bytes-v1");
        // A different binary lands in a different file; the old file is
        // never overwritten — and never deleted either: another browser's
        // manifest may still point at it, so it stays usable until that
        // browser re-registers.
        std::fs::write(&source, b"host-bytes-v2").unwrap();
        let second = install_native_host_binary(&source, &install_dir, "chrome").unwrap();
        assert_ne!(second, first);
        assert_eq!(std::fs::read(&second).unwrap(), b"host-bytes-v2");
        assert_eq!(
            std::fs::read(&first).unwrap(),
            b"host-bytes-v1",
            "the old install is left alone, bytes intact"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_leaves_the_legacy_unversioned_file_alone() {
        // v0.3.0/v0.3.1 installed a fixed `thunderload-host[.exe]`; the
        // versioned install supersedes it but must not delete it — a
        // manifest written by those versions still points at it, and
        // deleting it would break that browser until it re-registers.
        let dir = std::env::temp_dir().join("thunderload-native-host-install-legacy-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.join("source.exe");
        std::fs::write(&source, b"host-bytes-v1").unwrap();
        let install_dir = dir.join("install");
        std::fs::create_dir_all(&install_dir).unwrap();
        let legacy = install_dir.join(if cfg!(target_os = "windows") {
            "thunderload-host.exe"
        } else {
            "thunderload-host"
        });
        std::fs::write(&legacy, b"legacy-host").unwrap();
        let installed = install_native_host_binary(&source, &install_dir, "chrome").unwrap();
        assert_ne!(installed, legacy);
        assert_eq!(
            std::fs::read(&legacy).unwrap(),
            b"legacy-host",
            "the legacy install is left alone, bytes intact"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_manifest_dirs_match_the_browser_docs() {
        use super::native_manifest_dir;
        let home = Path::new("/home/u");
        assert_eq!(
            native_manifest_dir("chrome", home).unwrap(),
            PathBuf::from("/home/u/.config/google-chrome/NativeMessagingHosts")
        );
        assert_eq!(
            native_manifest_dir("chromium", home).unwrap(),
            PathBuf::from("/home/u/.config/chromium/NativeMessagingHosts")
        );
        assert_eq!(
            native_manifest_dir("edge", home).unwrap(),
            PathBuf::from("/home/u/.config/microsoft-edge/NativeMessagingHosts")
        );
        assert_eq!(
            native_manifest_dir("brave", home).unwrap(),
            PathBuf::from("/home/u/.config/BraveSoftware/Brave-Browser/NativeMessagingHosts")
        );
        assert_eq!(
            native_manifest_dir("opera", home).unwrap(),
            PathBuf::from("/home/u/.config/opera/NativeMessagingHosts")
        );
        assert_eq!(
            native_manifest_dir("firefox", home).unwrap(),
            PathBuf::from("/home/u/.mozilla/native-messaging-hosts")
        );
        assert_eq!(native_manifest_dir("safari", home), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_manifest_dirs_match_the_browser_docs() {
        use super::native_manifest_dir;
        let home = Path::new("/Users/u");
        assert_eq!(
            native_manifest_dir("chrome", home).unwrap(),
            PathBuf::from(
                "/Users/u/Library/Application Support/Google/Chrome/NativeMessagingHosts"
            )
        );
        assert_eq!(
            native_manifest_dir("firefox", home).unwrap(),
            PathBuf::from("/Users/u/Library/Application Support/Mozilla/NativeMessagingHosts")
        );
        assert_eq!(
            native_manifest_dir("opera", home).unwrap(),
            PathBuf::from("/Users/u/Library/Application Support/com.operasoftware.Opera/NativeMessagingHosts")
        );
        assert_eq!(native_manifest_dir("safari", home), None);
    }
}

/// Slice 6 fix (MAJOR): a live recording's kept partial must survive
/// the desktop paths that used to destroy it — cancel must never delete
/// a live target, and a live row must keep its flag through every
/// re-seeding path (frontend sync, native mirror). These tests pin the
/// pure decision functions; the Tauri commands around them are
/// verified by the desktop's CI run (the desktop crate does not build
/// on the dev VM).
#[cfg(test)]
mod live_job_tests {
    use super::{cancel_may_delete_target, native_value_to_record, sync_job_to_record, SyncJob};
    use serde_json::json;

    #[test]
    fn cancel_never_deletes_a_live_target() {
        // Live: the partial file IS the recording — never deleted,
        // whether or not the job also completed.
        assert!(!cancel_may_delete_target(true, false));
        assert!(!cancel_may_delete_target(true, true));
        // VOD: a finished file is never deleted (D9); only a
        // paused/failed VOD job's checkpoint goes.
        assert!(!cancel_may_delete_target(false, true));
        assert!(cancel_may_delete_target(false, false));
    }

    #[test]
    fn sync_job_carries_the_live_flag() {
        let live = SyncJob {
            id: "live-1".to_string(),
            name: "live.ts".to_string(),
            url: "https://example.test/live.m3u8".to_string(),
            kind: "hls".to_string(),
            status: "Paused".to_string(),
            downloaded: 123,
            total: None,
            percent: None,
            target_path: Some("/tmp/live.ts".to_string()),
            created_at: 1,
            scheduled_at: None,
            representation_id: None,
            audio_representation_id: None,
            detail: "Paused · partial recording kept".to_string(),
            error: None,
            live: true,
        };
        let record = sync_job_to_record(live);
        assert!(
            record.live,
            "a live row re-seeded from the frontend must stay live, or it takes the VOD start path"
        );
        let vod = SyncJob {
            id: "vod-1".to_string(),
            name: "video.mp4".to_string(),
            url: "https://example.test/vod.m3u8".to_string(),
            kind: "hls".to_string(),
            status: "Paused".to_string(),
            downloaded: 456,
            total: None,
            percent: None,
            target_path: Some("/tmp/video.mp4".to_string()),
            created_at: 1,
            scheduled_at: None,
            representation_id: None,
            audio_representation_id: None,
            detail: "Paused".to_string(),
            error: None,
            live: false,
        };
        assert!(
            !sync_job_to_record(vod).live,
            "a VOD row must not come back live"
        );
    }

    #[test]
    fn native_mirror_reads_the_live_flag() {
        let live = json!({"id": "n1", "kind": "hls", "status": "Paused", "url": "https://example.test/live.m3u8", "targetPath": "/tmp/live.ts", "live": true});
        let record = native_value_to_record(&live).expect("a live native row must convert");
        assert!(record.live, "a browser-started live recording must stay live in the mirror, or cancel deletes its target");
        let vod = json!({"id": "n2", "kind": "hls", "status": "Paused", "url": "https://example.test/vod.m3u8", "targetPath": "/tmp/video.mp4"});
        let record = native_value_to_record(&vod).expect("a VOD native row must convert");
        assert!(
            !record.live,
            "a native row without live must not come back live"
        );
    }
}


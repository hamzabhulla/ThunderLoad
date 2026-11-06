use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::mpsc;
use thunderload_engine::{
    create_transfer_control, default_download_dir, download_hls_managed, managed_download,
    reserve_download_path, ManagedOutcome, TransferControlHandle,
};

const STORE_NAME: &str = ".thunderload-jobs.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRecord {
    pub id: String,
    pub url: String,
    pub filename: String,
    pub kind: String,
    pub status: String,
    pub target_path: String,
    pub connections: usize,
    pub downloaded: u64,
    pub total: Option<u64>,
    pub percent: f64,
    pub detail: String,
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub scheduled_at: Option<u64>,
}

#[derive(Clone)]
pub struct JobState {
    jobs: Arc<Mutex<HashMap<String, JobRecord>>>,
    controls: Arc<Mutex<HashMap<String, TransferControlHandle>>>,
    store_path: PathBuf,
    speed_limit_bps: Arc<AtomicU64>,
    max_active: Arc<AtomicUsize>,
}

impl JobState {
    pub fn load() -> Self {
        let directory = default_download_dir();
        let _ = std::fs::create_dir_all(&directory);
        let store_path = directory.join(STORE_NAME);
        let mut jobs: HashMap<String, JobRecord> = std::fs::read(&store_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        for job in jobs.values_mut() {
            if job.status == "Downloading" || job.status == "Pausing" {
                job.status = "Queued".into();
                job.detail = "Resuming after app restart".into();
                job.error = None;
            }
        }
        Self {
            jobs: Arc::new(Mutex::new(jobs)),
            controls: Arc::new(Mutex::new(HashMap::new())),
            store_path,
            speed_limit_bps: Arc::new(AtomicU64::new(0)),
            max_active: Arc::new(AtomicUsize::new(2)),
        }
    }

    fn snapshot(&self) -> Vec<JobRecord> {
        self.jobs.lock().map(|jobs| {
            let mut result = jobs.values().cloned().collect::<Vec<_>>();
            result.sort_by(|a, b| b.created_at.cmp(&a.created_at));
            result
        }).unwrap_or_default()
    }

    fn persist(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(&self.snapshot()).map_err(|error| error.to_string())?;
        let temp = self.store_path.with_extension("json.tmp");
        std::fs::write(&temp, bytes).map_err(|error| error.to_string())?;
        if self.store_path.exists() {
            std::fs::remove_file(&self.store_path).map_err(|error| error.to_string())?;
        }
        std::fs::rename(&temp, &self.store_path).map_err(|error| error.to_string())
    }

    fn update<F>(&self, id: &str, update: F) -> Option<JobRecord>
    where
        F: FnOnce(&mut JobRecord),
    {
        let mut jobs = self.jobs.lock().ok()?;
        let job = jobs.get_mut(id)?;
        update(job);
        job.updated_at = now_ms();
        Some(job.clone())
    }

    fn emit_job(&self, app: &AppHandle, job: &JobRecord) {
        let _ = app.emit("job-updated", job);
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn safe_job_id(id: &str) -> String {
    id.chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || "-_".contains(*ch))
        .take(80)
        .collect()
}

pub async fn enqueue_download(
    app: AppHandle,
    state: State<'_, JobState>,
    job_id: String,
    url: String,
    filename: String,
    kind: String,
    connections: usize,
    scheduled_at: Option<u64>,
) -> Result<JobRecord, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err("Only HTTP and HTTPS download URLs are supported".into());
    }
    let id = safe_job_id(&job_id);
    if id.is_empty() {
        return Err("Invalid job ID".into());
    }
    if !matches!(kind.as_str(), "direct" | "hls") {
        return Err("Unsupported download type".into());
    }
    let name = thunderload_engine::safe_filename(&filename, if kind == "hls" { "video.ts" } else { "download.bin" });
    let directory = default_download_dir();
    let target = reserve_download_path(&directory.join(&name)).await.map_err(|error| error.to_string())?;
    let timestamp = now_ms();
    let record = JobRecord {
        id: id.clone(),
        url,
        filename: name,
        kind,
        status: if scheduled_at.is_some_and(|time| time > timestamp) { "Scheduled" } else { "Queued" }.into(),
        target_path: target.to_string_lossy().to_string(),
        connections: connections.clamp(1, 16),
        downloaded: 0,
        total: None,
        percent: 0.0,
        detail: if scheduled_at.is_some_and(|time| time > timestamp) { "Waiting for scheduled time".into() } else { "Waiting in queue".into() },
        error: None,
        created_at: timestamp,
        updated_at: timestamp,
        scheduled_at,
    };
    {
        let mut jobs = state.jobs.lock().map_err(|_| "Job store is unavailable".to_string())?;
        if jobs.contains_key(&id) {
            let _ = std::fs::remove_file(&target);
            return Err("A job with this ID already exists".into());
        }
        jobs.insert(id.clone(), record.clone());
    }
    state.persist()?;
    state.emit_job(&app, &record);
    Ok(record)
}

pub fn list_downloads(state: State<'_, JobState>) -> Vec<JobRecord> {
    state.snapshot()
}

pub fn pause_download(app: AppHandle, state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    let id = safe_job_id(&job_id);
    let control = state.controls.lock().map_err(|_| "Control store is unavailable".to_string())?.get(&id).cloned();
    if let Some(control) = control {
        control.set_paused(true);
        if let Some(job) = state.update(&id, |job| {
            job.status = "Pausing".into();
            job.detail = "Saving a resume checkpoint".into();
        }) {
            state.persist()?;
            state.emit_job(&app, &job);
        }
        Ok(())
    } else {
        let job = state.update(&id, |job| {
            if job.status == "Queued" || job.status == "Scheduled" {
                job.status = "Paused".into();
                job.detail = "Paused before transfer".into();
            }
        }).ok_or_else(|| "Download job was not found".to_string())?;
        state.persist()?;
        state.emit_job(&app, &job);
        Ok(())
    }
}

pub fn resume_download(app: AppHandle, state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    let id = safe_job_id(&job_id);
    let job = state.update(&id, |job| {
        if matches!(job.status.as_str(), "Paused" | "Interrupted" | "Failed" | "Queued") {
            job.status = "Queued".into();
            job.detail = "Waiting to resume".into();
            job.error = None;
            job.scheduled_at = None;
        }
    }).ok_or_else(|| "Download job was not found".to_string())?;
    state.persist()?;
    state.emit_job(&app, &job);
    Ok(())
}

pub fn cancel_download(app: AppHandle, state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    let id = safe_job_id(&job_id);
    let control = state.controls.lock().map_err(|_| "Control store is unavailable".to_string())?.get(&id).cloned();
    if let Some(control) = control {
        control.cancel();
        if let Some(job) = state.update(&id, |job| {
            job.status = "Cancelling".into();
            job.detail = "Cancelling transfer and removing partial data".into();
        }) {
            state.persist()?;
            state.emit_job(&app, &job);
        }
    } else if let Some(job) = state.update(&id, |job| {
        job.status = "Cancelled".into();
        job.detail = "Cancelled".into();
        job.error = None;
    }) {
        cleanup_job_files(&job);
        state.persist()?;
        state.emit_job(&app, &job);
    } else {
        return Err("Download job was not found".into());
    }
    Ok(())
}

pub fn remove_download(state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    let id = safe_job_id(&job_id);
    if state.controls.lock().map_err(|_| "Control store is unavailable".to_string())?.contains_key(&id) {
        return Err("Pause or cancel the active download before removing it".into());
    }
    let job = state.jobs.lock().map_err(|_| "Job store is unavailable".to_string())?
        .remove(&id).ok_or_else(|| "Download job was not found".to_string())?;
    if job.status != "Completed" {
        cleanup_job_files(&job);
    }
    state.persist()
}

pub fn set_speed_limit(app: AppHandle, state: State<'_, JobState>, bytes_per_second: u64) {
    state.speed_limit_bps.store(bytes_per_second, Ordering::Relaxed);
    if let Ok(controls) = state.controls.lock() {
        for control in controls.values() {
            control.set_speed_limit(bytes_per_second);
        }
    }
    let _ = app.emit("speed-limit-updated", bytes_per_second);
}

pub fn set_max_active(state: State<'_, JobState>, count: usize) {
    state.max_active.store(count.clamp(1, 8), Ordering::Relaxed);
}

fn cleanup_job_files(job: &JobRecord) {
    let target = PathBuf::from(&job.target_path);
    let safe_id = safe_job_id(&job.id);
    if let Some(parent) = target.parent() {
        let work = parent.join(format!(".thunderload-job-{safe_id}"));
        let hls_work = parent.join(format!(".thunderload-hls-job-{safe_id}"));
        if let Ok(bytes) = std::fs::read(hls_work.join("hls.json")) {
            if let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if let Some(path) = metadata.get("targetPath").and_then(serde_json::Value::as_str) {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        let _ = std::fs::remove_dir_all(work);
        let _ = std::fs::remove_dir_all(hls_work);
    }
    let _ = std::fs::remove_file(target);
}

fn spawn_worker(app: AppHandle, state: JobState, mut job: JobRecord) {
    let id = job.id.clone();
    let (handle, control) = create_transfer_control(state.speed_limit_bps.load(Ordering::Relaxed));
    if let Ok(mut controls) = state.controls.lock() {
        controls.insert(id.clone(), handle);
    }
    if let Some(updated) = state.update(&id, |record| {
        record.status = "Downloading".into();
        record.detail = "Starting transfer".into();
    }) {
        job = updated;
        let _ = state.persist();
        state.emit_job(&app, &job);
    }

    tauri::async_runtime::spawn(async move {
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<thunderload_engine::DownloadProgress>();
        let progress_app = app.clone();
        let progress_state = state.clone();
        let progress_id = id.clone();
        let progress_task = tauri::async_runtime::spawn(async move {
            let mut last_persist = std::time::Instant::now();
            while let Some(progress) = progress_rx.recv().await {
                let updated = progress_state.update(&progress_id, |record| {
                    record.downloaded = progress.downloaded;
                    record.total = progress.total;
                    record.percent = progress.percent.unwrap_or(record.percent).clamp(0.0, 100.0);
                    record.detail = match progress.phase.as_str() {
                        "resuming" => "Checking saved resume data".into(),
                        "paused" => "Paused · resume data saved".into(),
                        "complete" => "Finalizing file".into(),
                        _ => format!("{} / {}", progress.downloaded, progress.total.unwrap_or(0)),
                    };
                });
                if let Some(job) = updated {
                    let _ = progress_app.emit("download-progress", serde_json::json!({
                        "jobId": progress_id,
                        "progress": progress
                    }));
                    progress_state.emit_job(&progress_app, &job);
                    if last_persist.elapsed().as_secs_f64() >= 1.0 {
                        let _ = progress_state.persist();
                        last_persist = std::time::Instant::now();
                    }
                }
            }
        });

        let result = if job.kind == "hls" {
            download_hls_managed(
                &job.url,
                Path::new(&job.target_path),
                &job.id,
                None,
                control,
                progress_tx,
            ).await
        } else {
            managed_download(
                &job.url,
                Path::new(&job.target_path),
                &job.id,
                job.connections,
                None,
                control,
                progress_tx,
            ).await
        };
        let _ = progress_task.await;

        let final_job = match result {
            Ok(ManagedOutcome::Completed(path)) => state.update(&id, |record| {
                record.status = "Completed".into();
                record.percent = 100.0;
                record.detail = format!("Saved · {}", path.display());
                record.error = None;
            }),
            Ok(ManagedOutcome::Paused { path, downloaded, total }) => state.update(&id, |record| {
                record.target_path = path.to_string_lossy().to_string();
                record.status = "Paused".into();
                record.downloaded = downloaded;
                record.total = total;
                record.detail = "Paused · resume data saved".into();
            }),
            Err(error) => {
                let cancelled = state.jobs.lock().ok().and_then(|jobs| jobs.get(&id).map(|job| job.status == "Cancelling")).unwrap_or(false);
                if cancelled {
                    if let Ok(jobs) = state.jobs.lock() {
                        if let Some(job) = jobs.get(&id) { cleanup_job_files(job); }
                    }
                    state.update(&id, |record| {
                        record.status = "Cancelled".into();
                        record.detail = "Cancelled".into();
                        record.error = None;
                    })
                } else {
                    state.update(&id, |record| {
                        record.status = "Failed".into();
                        record.detail = "Transfer failed · Retry or resume".into();
                        record.error = Some(error.to_string());
                    })
                }
            }
        };
        if let Ok(mut controls) = state.controls.lock() {
            controls.remove(&id);
        }
        if let Some(job) = final_job {
            let _ = state.persist();
            if job.status == "Completed" {
                let _ = app.emit("download-complete", serde_json::json!({"jobId": id, "path": job.target_path}));
            } else if job.status == "Failed" {
                let _ = app.emit("download-error", serde_json::json!({"jobId": id, "error": job.error}));
            }
            state.emit_job(&app, &job);
        }
    });
}

pub fn start_scheduler(app: AppHandle, state: JobState) {
    tauri::async_runtime::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(750));
        loop {
            interval.tick().await;
            let active = state.jobs.lock().map(|jobs| {
                jobs.values().filter(|job| matches!(job.status.as_str(), "Downloading" | "Pausing" | "Cancelling")).count()
            }).unwrap_or(0);
            let capacity = state.max_active.load(Ordering::Relaxed).saturating_sub(active);
            if capacity == 0 { continue; }
            let now = now_ms();
            let next = state.jobs.lock().map(|jobs| {
                let mut queued = jobs.values().filter(|job| {
                    job.status == "Queued" || (job.status == "Scheduled" && job.scheduled_at.is_some_and(|time| time <= now))
                }).cloned().collect::<Vec<_>>();
                queued.sort_by_key(|job| job.created_at);
                queued.into_iter().take(capacity).collect::<Vec<_>>()
            }).unwrap_or_default();
            for job in next {
                spawn_worker(app.clone(), state.clone(), job);
            }
        }
    });
}

#[tauri::command]
pub async fn enqueue_download_command(
    app: AppHandle,
    state: State<'_, JobState>,
    job_id: String,
    url: String,
    filename: String,
    kind: String,
    connections: usize,
    scheduled_at: Option<u64>,
) -> Result<JobRecord, String> {
    enqueue_download(app, state, job_id, url, filename, kind, connections, scheduled_at).await
}

#[tauri::command]
pub fn list_downloads_command(state: State<'_, JobState>) -> Vec<JobRecord> {
    list_downloads(state)
}

#[tauri::command]
pub fn pause_download_command(app: AppHandle, state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    pause_download(app, state, job_id)
}

#[tauri::command]
pub fn resume_download_command(app: AppHandle, state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    resume_download(app, state, job_id)
}

#[tauri::command]
pub fn cancel_download_command(app: AppHandle, state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    cancel_download(app, state, job_id)
}

#[tauri::command]
pub fn remove_download_command(state: State<'_, JobState>, job_id: String) -> Result<(), String> {
    remove_download(state, job_id)
}

#[tauri::command]
pub fn set_speed_limit_command(app: AppHandle, state: State<'_, JobState>, bytes_per_second: u64) {
    set_speed_limit(app, state, bytes_per_second)
}

#[tauri::command]
pub fn set_max_active_command(state: State<'_, JobState>, count: usize) {
    set_max_active(state, count)
}

//! Durable job database (Slice 5).
//!
//! Every download job — desktop or browser-started — is recorded in one
//! `jobs.json` file inside the desktop app-data directory (never next to the
//! downloads themselves). The file is the durable record; the per-job
//! checkpoint files (`.thunderload.part.meta.json`, HLS/DASH segment work
//! directories) stay the resume mechanism — the store only records where
//! they are.
//!
//! Storage choice: a single JSON file written atomically (write tmp file,
//! fsync, rename), not SQLite. Rationale: the repo has no database
//! dependency anywhere, the job count is small (tens of rows, not
//! thousands) so a full-file rewrite per mutation is sub-millisecond, and
//! the atomic tmp+rename pattern is already the established crash-safety
//! mechanism in this codebase (checkpoint sidecars, native-host state
//! files, `write_native_control_file`). Adding rusqlite would pull a C
//! SQLite build into every platform's CI for no measurable benefit at
//! this scale. If the job count ever grows into the thousands, the
//! `save`/`load` boundary is the only place that needs to change.
//!
//! Crash safety: a torn write can only ever leave the *previous* complete
//! `jobs.json` (rename is atomic) or a leftover `.tmp` file, which is
//! ignored on load. A corrupt `jobs.json` is moved aside to
//! `jobs.json.corrupt-<millis>` and the store starts empty instead of
//! crashing the app — the checkpoints on disk are untouched, so the
//! orphan import can re-discover those jobs.
//!
//! The store is synchronous (`std::fs`): the files are tiny, the
//! desktop already does synchronous file work in its commands, and a
//! sync API keeps the engine unit tests free of a Tokio runtime.

use std::{
    collections::HashMap,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    download::direct_checkpoint_info, partial_download_metadata_path, partial_download_path,
    video::media_checkpoint_info,
};

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

/// File name of the job database inside the app-data directory.
pub fn store_file_name() -> &'static str {
    "jobs.json"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    #[default]
    Direct,
    Hls,
    Dash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum JobState {
    #[default]
    Queued,
    Downloading,
    Paused,
    Completed,
    Failed,
    Interrupted,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum JobOrigin {
    #[default]
    Desktop,
    Native,
}

/// One row of the durable job database. Every field has a serde default so
/// a row written by an older build — or hand-edited — still parses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct JobRecord {
    pub id: String,
    pub name: String,
    pub url: String,
    pub kind: JobKind,
    pub origin: JobOrigin,
    pub state: JobState,
    /// Bytes downloaded so far (0 when unknown).
    pub downloaded: u64,
    /// Total bytes when the server advertised a length.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    /// Last reported percent (0-100) when the engine reported one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    /// Finished media segments, where the kind has segments (HLS/DASH).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments_done: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments_total: Option<u64>,
    /// Destination file. Cleared on Cancel (the file is deleted then).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    /// The resume checkpoint for this job (`.thunderload.part.meta.json`
    /// next to the target). The checkpoint itself is the resume
    /// mechanism; the store only records where it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_representation_id: Option<String>,
    /// Scheduled start (ms since epoch) for a queued job, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_at_ms: Option<u64>,
    /// Human-readable status line shown in the queue.
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl Default for JobRecord {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            url: String::new(),
            kind: JobKind::Direct,
            origin: JobOrigin::Desktop,
            state: JobState::Queued,
            downloaded: 0,
            total: None,
            percent: None,
            segments_done: None,
            segments_total: None,
            target_path: None,
            checkpoint_path: None,
            representation_id: None,
            audio_representation_id: None,
            scheduled_at_ms: None,
            detail: String::new(),
            error: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }
}

impl JobRecord {
    /// A fresh desktop row for a job that is about to start (or be queued).
    pub fn new_desktop(id: &str, name: &str, url: &str, kind: JobKind, target: Option<&Path>) -> Self {
        let now = now_ms();
        Self {
            id: id.to_string(),
            name: name.to_string(),
            url: url.to_string(),
            kind,
            origin: JobOrigin::Desktop,
            state: JobState::Downloading,
            target_path: target.map(|path| path.to_string_lossy().to_string()),
            checkpoint_path: target.map(|path| partial_download_metadata_path(path).to_string_lossy().to_string()),
            created_at_ms: now,
            updated_at_ms: now,
            ..Default::default()
        }
    }

    /// File-stem display name for a target path (`video.mp4` -> `video.mp4`).
    pub fn name_for_target(target: &Path) -> String {
        target
            .file_name()
            .map(|value| value.to_string_lossy().to_string())
            .unwrap_or_else(|| "download".to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JobStoreFile {
    #[serde(default = "default_store_version")]
    version: u32,
    #[serde(default)]
    jobs: Vec<JobRecord>,
}

fn default_store_version() -> u32 {
    1
}

/// The durable job database: one JSON file, written atomically.
///
/// Rows are kept in queue order, newest first — the same order the
/// frontend keeps its job list in — so a restart restores the queue as
/// it was.
pub struct JobStore {
    path: PathBuf,
    jobs: Vec<JobRecord>,
    /// Minimum time between progress-driven saves for one job. State
    /// transitions always save immediately; progress reports (which can
    /// arrive many times a second) only persist when this long has
    /// passed, so a busy download does not rewrite the file constantly.
    /// A crash loses at most this much progress detail — the row's state
    /// is what matters for resume, and the checkpoint is authoritative
    /// for how much is really on disk.
    progress_throttle: Duration,
    last_progress_save: HashMap<String, Instant>,
}

impl JobStore {
    /// Loads the store from `path`. A missing file starts empty; a
    /// corrupt file is moved aside (`jobs.json.corrupt-<millis>`) and the
    /// store starts empty, so a bad write can never wedge the app. The
    /// second return value carries the corruption note, if any, for the
    /// caller to log.
    pub fn load(path: &Path, progress_throttle: Duration) -> (Self, Option<String>) {
        let mut store = Self {
            path: path.to_path_buf(),
            jobs: Vec::new(),
            progress_throttle,
            last_progress_save: HashMap::new(),
        };
        match fs::read_to_string(path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => (store, None),
            Err(error) => (
                store,
                Some(format!("Could not read job store {}: {error}", path.display())),
            ),
            Ok(text) => match serde_json::from_str::<JobStoreFile>(&text) {
                Ok(file) => {
                    store.jobs = file.jobs;
                    (store, None)
                }
                Err(error) => {
                    let backup = path.with_extension(format!("json.corrupt-{}", now_ms()));
                    let _ = fs::rename(path, &backup);
                    (
                        store,
                        Some(format!(
                            "Job store {} was corrupt ({error}); moved to {} and started empty",
                            path.display(),
                            backup.display()
                        )),
                    )
                }
            },
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// All rows in queue order (newest first).
    pub fn all(&self) -> &[JobRecord] {
        &self.jobs
    }

    pub fn get(&self, id: &str) -> Option<&JobRecord> {
        self.jobs.iter().find(|job| job.id == id)
    }

    /// Writes the whole store atomically: tmp file, fsync, rename.
    /// A crash mid-write leaves the previous complete file (or a stale
    /// `.tmp`, which load ignores).
    pub fn save(&self) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = JobStoreFile {
            version: 1,
            jobs: self.jobs.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&file)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let temporary = self.path.with_extension("json.tmp");
        fs::write(&temporary, &bytes)?;
        File::open(&temporary)?.sync_all()?;
        match fs::rename(&temporary, &self.path) {
            Ok(()) => {
                #[cfg(unix)]
                if let Some(parent) = self.path.parent() {
                    if let Ok(dir) = File::open(parent) {
                        let _ = dir.sync_all();
                    }
                }
                Ok(())
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                Err(error)
            }
        }
    }

    /// Inserts a new row at the front, or replaces the existing row with
    /// the same id in place (keeping its position and original creation
    /// time). Saves only when something actually changed — the
    /// `updated_at_ms` stamp itself does not count as a change, or every
    /// upsert would rewrite the file. Returns true when a row was
    /// inserted or changed.
    pub fn upsert(&mut self, mut record: JobRecord) -> io::Result<bool> {
        if let Some(existing) = self.jobs.iter_mut().find(|job| job.id == record.id) {
            record.created_at_ms = existing.created_at_ms;
            record.updated_at_ms = existing.updated_at_ms;
            if *existing == record {
                return Ok(false);
            }
            record.updated_at_ms = now_ms();
            *existing = record;
        } else {
            if record.created_at_ms == 0 {
                record.created_at_ms = now_ms();
            }
            record.updated_at_ms = now_ms();
            self.jobs.insert(0, record);
        }
        self.save()?;
        Ok(true)
    }

    /// Records a job (re)starting: like upsert, but when a row already
    /// exists its progress (bytes/percent/segments) carries over, so a
    /// resume does not reset the stored progress to zero before the
    /// first progress event arrives.
    pub fn start_job(&mut self, record: JobRecord) -> io::Result<bool> {
        let mut record = record;
        if let Some(existing) = self.get(&record.id) {
            record.downloaded = existing.downloaded;
            record.total = existing.total;
            record.percent = existing.percent;
            record.segments_done = existing.segments_done;
            record.segments_total = existing.segments_total;
        }
        self.upsert(record)
    }

    /// Changes one row's state (plus detail/error). Saves immediately.
    /// Returns false when the id is unknown or nothing changed.
    pub fn update_state(
        &mut self,
        id: &str,
        state: JobState,
        detail: Option<&str>,
        error: Option<&str>,
    ) -> io::Result<bool> {
        let Some(job) = self.jobs.iter_mut().find(|job| job.id == id) else {
            return Ok(false);
        };
        let changed = job.state != state
            || detail.is_some_and(|detail| detail != job.detail)
            || error != job.error.as_deref();
        if !changed {
            return Ok(false);
        }
        job.state = state;
        if let Some(detail) = detail {
            job.detail = detail.to_string();
        }
        job.error = error.map(str::to_string);
        job.updated_at_ms = now_ms();
        if state == JobState::Completed {
            if let Some(total) = job.total {
                job.downloaded = total;
            }
            job.percent = Some(100.0);
        }
        if state == JobState::Cancelled {
            // Cancel deletes the files; the row stays (so the user still
            // sees the cancelled job until they clear it) but must not
            // point at a target that no longer exists.
            job.target_path = None;
            job.checkpoint_path = None;
        }
        self.save()?;
        Ok(true)
    }

    /// Records live progress (bytes/percent). The in-memory row is always
    /// updated; the file is only rewritten when the per-job throttle has
    /// elapsed, so a busy download does not rewrite the store on every
    /// progress event. Returns false when the id is unknown.
    pub fn note_progress(
        &mut self,
        id: &str,
        downloaded: u64,
        total: Option<u64>,
        percent: Option<f64>,
    ) -> io::Result<bool> {
        let Some(job) = self.jobs.iter_mut().find(|job| job.id == id) else {
            return Ok(false);
        };
        job.downloaded = downloaded;
        job.total = total;
        job.percent = percent;
        job.updated_at_ms = now_ms();
        let due = match self.last_progress_save.get(id) {
            None => true,
            Some(last) => last.elapsed() >= self.progress_throttle,
        };
        if due {
            self.last_progress_save.insert(id.to_string(), Instant::now());
            self.save()?;
        }
        Ok(true)
    }

    /// Re-reads the segment counts from the row's checkpoint (HLS/DASH
    /// only) and persists them. Called when a media job pauses or fails,
    /// so a restarted app shows how far the segment download got.
    /// Returns false when the id is unknown or the job has no checkpoint
    /// with segment counts.
    pub fn refresh_segments(&mut self, id: &str) -> io::Result<bool> {
        let Some(job) = self.jobs.iter().find(|job| job.id == id) else {
            return Ok(false);
        };
        if !matches!(job.kind, JobKind::Hls | JobKind::Dash) {
            return Ok(false);
        }
        let Some(target) = job.target_path.clone() else {
            return Ok(false);
        };
        let Some((done, total)) = media_checkpoint_info(
            &partial_download_metadata_path(Path::new(&target)),
            Path::new(&target),
        )
        .and_then(|info| info.segments()) else {
            return Ok(false);
        };
        let job = self.jobs.iter_mut().find(|job| job.id == id).expect("row still present");
        if job.segments_done == Some(done) && job.segments_total == Some(total) {
            return Ok(false);
        }
        job.segments_done = Some(done);
        job.segments_total = Some(total);
        job.updated_at_ms = now_ms();
        self.save()?;
        Ok(true)
    }

    /// Deletes a row. Returns false when the id was unknown.
    pub fn remove(&mut self, id: &str) -> io::Result<bool> {
        let before = self.jobs.len();
        self.jobs.retain(|job| job.id != id);
        if self.jobs.len() == before {
            return Ok(false);
        }
        self.last_progress_save.remove(id);
        self.save()?;
        Ok(true)
    }

    /// Startup recovery: a row still marked Downloading means the app
    /// died (or was killed) mid-transfer — nothing is running anymore,
    /// so the job becomes Interrupted and resumable from its checkpoint.
    /// Queued, Paused, Failed, Completed and Cancelled rows are
    /// untouched. Returns how many rows were flipped.
    pub fn mark_interrupted_on_startup(&mut self) -> io::Result<usize> {
        let mut flipped = 0;
        for job in self.jobs.iter_mut() {
            if job.state == JobState::Downloading {
                job.state = JobState::Interrupted;
                job.detail = "App closed while downloading · resume from the saved checkpoint".to_string();
                job.updated_at_ms = now_ms();
                flipped += 1;
            }
        }
        if flipped > 0 {
            self.save()?;
        }
        Ok(flipped)
    }

    /// First-run migration: discovers checkpoint files in the download
    /// directory that have no row in the store (jobs from before the
    /// store existed, or jobs whose row was lost) and imports them as
    /// Interrupted jobs, so the user can resume them instead of losing
    /// them. Never deletes or modifies anything on disk. Returns the
    /// imported rows.
    pub fn import_orphans(&mut self, download_dir: &Path) -> io::Result<Vec<JobRecord>> {
        let mut imported = Vec::new();
        let entries = match fs::read_dir(download_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(imported),
            Err(error) => return Err(error),
        };
        // Collect first: meta files (real checkpoints) before bare
        // partial files, so a `.part` whose meta exists is never
        // imported twice.
        let mut metas: Vec<(PathBuf, String)> = Vec::new();
        let mut partials: Vec<(PathBuf, String)> = Vec::new();
        for entry in entries.flatten() {
            // Only regular files are checkpoints/partials; a directory
            // that happens to share the suffix is never a job.
            if !entry.file_type().map(|kind| kind.is_file()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(target) = name.strip_suffix(".thunderload.part.meta.json") {
                if !target.is_empty() {
                    metas.push((entry.path(), target.to_string()));
                }
            } else if let Some(target) = name.strip_suffix(".thunderload.part") {
                if !target.is_empty() {
                    partials.push((entry.path(), target.to_string()));
                }
            }
        }
        for (meta_path, target_name) in metas {
            let target = download_dir.join(&target_name);
            if self.has_target(&target) {
                continue;
            }
            let Some(record) = self.orphan_from_meta(&meta_path, &target) else {
                continue;
            };
            self.jobs.push(record.clone());
            imported.push(record);
        }
        for (part_path, target_name) in partials {
            let target = download_dir.join(&target_name);
            if self.has_target(&target) {
                continue;
            }
            let downloaded = fs::metadata(&part_path).map(|meta| meta.len()).unwrap_or(0);
            let now = now_ms();
            let created = fs::metadata(&part_path)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(now);
            let record = JobRecord {
                id: uuid::Uuid::new_v4().to_string(),
                name: JobRecord::name_for_target(&target),
                url: String::new(),
                kind: JobKind::Direct,
                origin: JobOrigin::Desktop,
                state: JobState::Interrupted,
                downloaded,
                total: None,
                percent: None,
                segments_done: None,
                segments_total: None,
                target_path: Some(target.to_string_lossy().to_string()),
                checkpoint_path: None,
                representation_id: None,
                audio_representation_id: None,
                scheduled_at_ms: None,
                detail: "Found a partial file without a checkpoint · resume restarts the download".to_string(),
                error: None,
                created_at_ms: created,
                updated_at_ms: now,
            };
            self.jobs.push(record.clone());
            imported.push(record);
        }
        if !imported.is_empty() {
            self.save()?;
        }
        Ok(imported)
    }

    fn has_target(&self, target: &Path) -> bool {
        let wanted = target.to_string_lossy().to_string();
        self.jobs
            .iter()
            .any(|job| job.target_path.as_deref() == Some(wanted.as_str()))
    }

    /// Builds an Interrupted row from one checkpoint sidecar. Returns
    /// None when the sidecar is not a checkpoint of any known kind.
    fn orphan_from_meta(&self, meta_path: &Path, target: &Path) -> Option<JobRecord> {
        let now = now_ms();
        let created = fs::metadata(meta_path)
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(now);
        let name = JobRecord::name_for_target(target);
        let target_string = target.to_string_lossy().to_string();
        let checkpoint_string = meta_path.to_string_lossy().to_string();
        if let Some(info) = media_checkpoint_info(meta_path, target) {
            let (segments_done, segments_total) = info.segments().unwrap_or((0, 0));
            let (kind, url) = match info.kind {
                crate::video::MediaKind::Hls => (JobKind::Hls, info.url.clone()),
                crate::video::MediaKind::Dash => (JobKind::Dash, info.url.clone()),
            };
            let detail = if info.segments().is_some() {
                format!(
                    "Found a saved {} checkpoint · {}/{} segments done",
                    if kind == JobKind::Hls { "HLS" } else { "DASH" },
                    segments_done,
                    segments_total
                )
            } else {
                format!(
                    "Found a saved {} checkpoint",
                    if kind == JobKind::Hls { "HLS" } else { "DASH" }
                )
            };
            return Some(JobRecord {
                id: uuid::Uuid::new_v4().to_string(),
                name,
                url,
                kind,
                origin: JobOrigin::Desktop,
                state: JobState::Interrupted,
                downloaded: 0,
                total: None,
                percent: None,
                segments_done: info.segments().map(|(done, _)| done),
                segments_total: info.segments().map(|(_, total)| total),
                target_path: Some(target_string),
                checkpoint_path: Some(checkpoint_string),
                representation_id: info.representation_id,
                audio_representation_id: info.audio_representation_id,
                scheduled_at_ms: None,
                detail,
                error: None,
                created_at_ms: created,
                updated_at_ms: now,
            });
        }
        if let Some((url, total)) = direct_checkpoint_info(meta_path) {
            let downloaded = fs::metadata(partial_download_path(target))
                .map(|meta| meta.len())
                .unwrap_or(0);
            return Some(JobRecord {
                id: uuid::Uuid::new_v4().to_string(),
                name,
                url,
                kind: JobKind::Direct,
                origin: JobOrigin::Desktop,
                state: JobState::Interrupted,
                downloaded,
                total,
                percent: None,
                segments_done: None,
                segments_total: None,
                target_path: Some(target_string),
                checkpoint_path: Some(checkpoint_string),
                representation_id: None,
                audio_representation_id: None,
                scheduled_at_ms: None,
                detail: "Found a saved direct-download checkpoint · resume continues from the partial file".to_string(),
                error: None,
                created_at_ms: created,
                updated_at_ms: now,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "thunderload-jobstore-test-{}-{}-{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst),
            name
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create test dir");
        dir
    }

    fn store_in(dir: &Path) -> (JobStore, PathBuf) {
        let path = dir.join("jobs.json");
        let (store, note) = JobStore::load(&path, Duration::from_secs(0));
        assert!(note.is_none(), "fresh load must not report corruption: {note:?}");
        (store, path)
    }

    fn sample_record(id: &str) -> JobRecord {
        JobRecord {
            id: id.to_string(),
            name: "archive.zip".to_string(),
            url: "https://example.com/archive.zip".to_string(),
            kind: JobKind::Direct,
            origin: JobOrigin::Desktop,
            state: JobState::Downloading,
            downloaded: 1234,
            total: Some(10_000),
            percent: Some(12.3),
            segments_done: None,
            segments_total: None,
            target_path: Some("/tmp/archive.zip".to_string()),
            checkpoint_path: Some("/tmp/archive.zip.thunderload.part.meta.json".to_string()),
            representation_id: None,
            audio_representation_id: None,
            scheduled_at_ms: None,
            detail: "Downloading".to_string(),
            error: None,
            created_at_ms: 1_700_000_000_000,
            updated_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn save_load_round_trip_preserves_every_field() {
        let dir = test_dir("round-trip");
        let (mut store, path) = store_in(&dir);
        let record = sample_record("job-1");
        assert!(store.upsert(record.clone()).unwrap());
        let (reloaded, note) = JobStore::load(&path, Duration::from_secs(0));
        assert!(note.is_none());
        assert_eq!(reloaded.all().len(), 1);
        let row = &reloaded.all()[0];
        // upsert refreshes updated_at, so compare everything but that.
        let mut expected = record.clone();
        expected.updated_at_ms = row.updated_at_ms;
        assert_eq!(row, &expected);
        assert!(row.updated_at_ms >= record.updated_at_ms);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_load_round_trip_preserves_optional_fields() {
        let dir = test_dir("optional-fields");
        let (mut store, path) = store_in(&dir);
        let mut record = sample_record("job-hls");
        record.kind = JobKind::Hls;
        record.origin = JobOrigin::Native;
        record.state = JobState::Paused;
        record.segments_done = Some(7);
        record.segments_total = Some(12);
        record.representation_id = Some("v-720p".to_string());
        record.audio_representation_id = Some("a-en".to_string());
        record.scheduled_at_ms = Some(1_700_000_100_000);
        record.error = Some("boom".to_string());
        record.total = None;
        record.percent = None;
        store.upsert(record).unwrap();
        let (reloaded, _) = JobStore::load(&path, Duration::from_secs(0));
        let row = &reloaded.all()[0];
        assert_eq!(row.kind, JobKind::Hls);
        assert_eq!(row.origin, JobOrigin::Native);
        assert_eq!(row.state, JobState::Paused);
        assert_eq!(row.segments_done, Some(7));
        assert_eq!(row.segments_total, Some(12));
        assert_eq!(row.representation_id.as_deref(), Some("v-720p"));
        assert_eq!(row.audio_representation_id.as_deref(), Some("a-en"));
        assert_eq!(row.scheduled_at_ms, Some(1_700_000_100_000));
        assert_eq!(row.error.as_deref(), Some("boom"));
        assert_eq!(row.total, None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_file_loads_empty_without_a_note() {
        let dir = test_dir("missing");
        let path = dir.join("jobs.json");
        let (store, note) = JobStore::load(&path, Duration::from_secs(0));
        assert!(note.is_none());
        assert!(store.all().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_file_is_moved_aside_and_store_starts_empty() {
        let dir = test_dir("corrupt");
        let path = dir.join("jobs.json");
        fs::write(&path, "{ this is not json").unwrap();
        let (store, note) = JobStore::load(&path, Duration::from_secs(0));
        assert!(note.is_some(), "corruption must be reported");
        assert!(store.all().is_empty());
        assert!(!path.exists(), "corrupt file must be moved away");
        let backup = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .find(|name| name.starts_with("jobs.json.corrupt-"))
            .expect("backup file must exist");
        assert!(fs::read_to_string(dir.join(backup)).unwrap().contains("not json"));
        // A second load now finds no file and starts empty, no note.
        let (store2, note2) = JobStore::load(&path, Duration::from_secs(0));
        assert!(note2.is_none());
        assert!(store2.all().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn upsert_new_inserts_at_front_and_existing_replaces_in_place() {
        let dir = test_dir("upsert");
        let (mut store, path) = store_in(&dir);
        store.upsert(sample_record("a")).unwrap();
        store.upsert(sample_record("b")).unwrap();
        assert_eq!(store.all()[0].id, "b");
        assert_eq!(store.all()[1].id, "a");
        let created_a = store.all()[1].created_at_ms;
        let mut changed = sample_record("a");
        changed.state = JobState::Completed;
        changed.downloaded = 10_000;
        assert!(store.upsert(changed).unwrap());
        assert_eq!(store.all()[0].id, "b", "replacing must keep position");
        assert_eq!(store.all()[1].id, "a");
        assert_eq!(store.all()[1].state, JobState::Completed);
        assert_eq!(store.all()[1].created_at_ms, created_a, "creation time is preserved");
        // Upserting an identical record does not rewrite the file.
        let before = fs::read_to_string(&path).unwrap();
        assert!(!store.upsert(sample_record("b")).unwrap(), "identical upsert is a no-op");
        assert_eq!(fs::read_to_string(&path).unwrap(), before);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn start_job_carries_progress_over_on_resume() {
        let dir = test_dir("start-job");
        let (mut store, _) = store_in(&dir);
        let mut paused = sample_record("r");
        paused.state = JobState::Paused;
        paused.downloaded = 6000;
        paused.total = Some(10_000);
        paused.percent = Some(60.0);
        paused.segments_done = Some(6);
        paused.segments_total = Some(10);
        store.upsert(paused).unwrap();
        let fresh = JobRecord::new_desktop(
            "r",
            "archive.zip",
            "https://example.com/archive.zip",
            JobKind::Direct,
            Some(std::path::Path::new("/tmp/archive.zip")),
        );
        assert!(store.start_job(fresh).unwrap());
        let row = store.get("r").unwrap();
        assert_eq!(row.state, JobState::Downloading);
        assert_eq!(row.downloaded, 6000);
        assert_eq!(row.total, Some(10_000));
        assert_eq!(row.percent, Some(60.0));
        assert_eq!(row.segments_done, Some(6));
        assert_eq!(row.segments_total, Some(10));
        assert_eq!(row.created_at_ms, 1_700_000_000_000, "creation time is preserved");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_leaves_no_tmp_file_behind() {
        let dir = test_dir("no-tmp");
        let (mut store, _path) = store_in(&dir);
        store.upsert(sample_record("x")).unwrap();
        assert!(!dir.join("jobs.json.tmp").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn startup_marks_only_downloading_jobs_interrupted() {
        let dir = test_dir("startup");
        let (mut store, path) = store_in(&dir);
        for (id, state) in [
            ("d", JobState::Downloading),
            ("q", JobState::Queued),
            ("p", JobState::Paused),
            ("c", JobState::Completed),
            ("f", JobState::Failed),
            ("x", JobState::Cancelled),
        ] {
            let mut record = sample_record(id);
            record.state = state;
            store.upsert(record).unwrap();
        }
        assert_eq!(store.mark_interrupted_on_startup().unwrap(), 1);
        let (reloaded, _) = JobStore::load(&path, Duration::from_secs(0));
        let state_of = |id: &str| reloaded.get(id).unwrap().state;
        assert_eq!(state_of("d"), JobState::Interrupted);
        assert_eq!(state_of("q"), JobState::Queued);
        assert_eq!(state_of("p"), JobState::Paused);
        assert_eq!(state_of("c"), JobState::Completed);
        assert_eq!(state_of("f"), JobState::Failed);
        assert_eq!(state_of("x"), JobState::Cancelled);
        assert!(reloaded
            .get("d")
            .unwrap()
            .detail
            .contains("resume from the saved checkpoint"));
        // Second call flips nothing.
        let (mut store2, _) = JobStore::load(&path, Duration::from_secs(0));
        assert_eq!(store2.mark_interrupted_on_startup().unwrap(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn progress_throttle_limits_file_rewrites_but_not_memory() {
        let dir = test_dir("throttle");
        let path = dir.join("jobs.json");
        let (mut store, _) = JobStore::load(&path, Duration::from_secs(3600));
        store.upsert(sample_record("t")).unwrap();
        // First progress report saves (no prior save for this job).
        assert!(store.note_progress("t", 100, Some(1000), Some(10.0)).unwrap());
        let saved_once = fs::read_to_string(&path).unwrap();
        assert!(saved_once.contains("\"downloaded\": 100"));
        // Second report inside the throttle updates memory only.
        assert!(store.note_progress("t", 200, Some(1000), Some(20.0)).unwrap());
        assert_eq!(store.get("t").unwrap().downloaded, 200, "memory is always current");
        assert_eq!(fs::read_to_string(&path).unwrap(), saved_once, "file is untouched inside the throttle");
        // Unknown id is a no-op.
        assert!(!store.note_progress("missing", 1, None, None).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn progress_with_zero_throttle_saves_every_report() {
        let dir = test_dir("throttle-zero");
        let (mut store, path) = store_in(&dir);
        store.upsert(sample_record("t")).unwrap();
        store.note_progress("t", 500, Some(1000), Some(50.0)).unwrap();
        assert!(fs::read_to_string(&path).unwrap().contains("\"downloaded\": 500"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_state_and_remove_behave() {
        let dir = test_dir("update-remove");
        let (mut store, path) = store_in(&dir);
        store.upsert(sample_record("u")).unwrap();
        assert!(store
            .update_state("u", JobState::Paused, Some("Paused by user"), None)
            .unwrap());
        let row = store.get("u").unwrap();
        assert_eq!(row.state, JobState::Paused);
        assert_eq!(row.detail, "Paused by user");
        // No-op when nothing changes.
        assert!(!store
            .update_state("u", JobState::Paused, Some("Paused by user"), None)
            .unwrap());
        assert!(!store.update_state("ghost", JobState::Paused, None, None).unwrap());
        // Completed fills downloaded/percent from the total.
        assert!(store
            .update_state("u", JobState::Completed, Some("Done"), None)
            .unwrap());
        let row = store.get("u").unwrap();
        assert_eq!(row.downloaded, 10_000);
        assert_eq!(row.percent, Some(100.0));
        // Cancelled clears the target/checkpoint links.
        assert!(store.update_state("u", JobState::Cancelled, None, None).unwrap());
        let row = store.get("u").unwrap();
        assert_eq!(row.target_path, None);
        assert_eq!(row.checkpoint_path, None);
        assert!(store.remove("u").unwrap());
        assert!(!store.remove("u").unwrap());
        let (reloaded, _) = JobStore::load(&path, Duration::from_secs(0));
        assert!(reloaded.all().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_direct_orphan_from_checkpoint() {
        let dir = test_dir("import-direct");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        // A direct checkpoint + partial file, as the engine writes them.
        fs::write(
            downloads.join("archive.zip.thunderload.part.meta.json"),
            r#"{"url":"https://example.com/archive.zip","etag":"abc","last_modified":null,"total":1000}"#,
        )
        .unwrap();
        fs::write(downloads.join("archive.zip.thunderload.part"), vec![0u8; 400]).unwrap();
        let (mut store, _) = store_in(&dir);
        let imported = store.import_orphans(&downloads).unwrap();
        assert_eq!(imported.len(), 1);
        let row = &imported[0];
        assert_eq!(row.kind, JobKind::Direct);
        assert_eq!(row.state, JobState::Interrupted);
        assert_eq!(row.url, "https://example.com/archive.zip");
        assert_eq!(row.total, Some(1000));
        assert_eq!(row.downloaded, 400);
        assert_eq!(row.name, "archive.zip");
        assert_eq!(
            row.target_path.as_deref(),
            Some(downloads.join("archive.zip").to_string_lossy().as_ref())
        );
        // Checkpoint files are never deleted by import.
        assert!(downloads.join("archive.zip.thunderload.part.meta.json").exists());
        assert!(downloads.join("archive.zip.thunderload.part").exists());
        // Re-import finds nothing new.
        assert!(store.import_orphans(&downloads).unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_hls_orphan_reads_segment_counts_from_checkpoint() {
        let dir = test_dir("import-hls");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        let work = downloads.join("video.mp4.thunderload.hls");
        fs::create_dir_all(&work).unwrap();
        fs::write(work.join("video-init"), b"init").unwrap();
        fs::write(work.join("video-000000"), b"s0").unwrap();
        fs::write(work.join("video-000001"), b"s1").unwrap();
        fs::write(
            downloads.join("video.mp4.thunderload.part.meta.json"),
            r#"{"version":1,"playlist_url":"https://cdn.example.com/master.m3u8","media_url":"https://cdn.example.com/media.m3u8","audio_playlist_url":null,"referer":null,"extension":"mp4","video_init":{"url":"https://cdn.example.com/init.mp4","range":null},"video_segments":[{"url":"https://cdn.example.com/s0.m4s","range":null},{"url":"https://cdn.example.com/s1.m4s","range":null},{"url":"https://cdn.example.com/s2.m4s","range":null}],"video_init_done":true,"video_done":[0,1],"audio_init":null,"audio_segments":[],"audio_init_done":false,"audio_done":[]}"#,
        )
        .unwrap();
        let (mut store, _) = store_in(&dir);
        let imported = store.import_orphans(&downloads).unwrap();
        assert_eq!(imported.len(), 1);
        let row = &imported[0];
        assert_eq!(row.kind, JobKind::Hls);
        assert_eq!(row.state, JobState::Interrupted);
        assert_eq!(row.url, "https://cdn.example.com/master.m3u8");
        assert_eq!(row.segments_done, Some(3), "init + 2 segments");
        assert_eq!(row.segments_total, Some(4), "init + 3 segments");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_dash_orphan_counts_segment_files_on_disk() {
        let dir = test_dir("import-dash");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        let work = downloads.join("video.mp4.thunderload.dash");
        fs::create_dir_all(&work).unwrap();
        fs::write(work.join("video-000000.segment"), b"init").unwrap();
        fs::write(work.join("video-000001.segment"), b"s0").unwrap();
        fs::write(work.join("video-000002.segment.tmp"), b"partial").unwrap();
        fs::write(
            downloads.join("video.mp4.thunderload.part.meta.json"),
            r#"{"version":1,"manifest_url":"https://cdn.example.com/manifest.mpd","representation_id":"v-720p","audio_representation_id":"a-en","referer":null,"video_tracks":[{"mime_type":"video/mp4","initialization_url":"https://cdn.example.com/init.mp4","initialization_range":null,"segment_urls":["https://cdn.example.com/s0.m4s","https://cdn.example.com/s1.m4s"],"segment_ranges":[null,null]}],"audio_tracks":[{"mime_type":"audio/mp4","initialization_url":"https://cdn.example.com/ainit.mp4","initialization_range":null,"segment_urls":["https://cdn.example.com/a0.m4s"],"segment_ranges":[null]}],"multi_period":false}"#,
        )
        .unwrap();
        let (mut store, _) = store_in(&dir);
        let imported = store.import_orphans(&downloads).unwrap();
        assert_eq!(imported.len(), 1);
        let row = &imported[0];
        assert_eq!(row.kind, JobKind::Dash);
        assert_eq!(row.url, "https://cdn.example.com/manifest.mpd");
        assert_eq!(row.representation_id.as_deref(), Some("v-720p"));
        assert_eq!(row.audio_representation_id.as_deref(), Some("a-en"));
        // Planned: video init + 2 media + audio init + 1 media = 5.
        assert_eq!(row.segments_total, Some(5));
        // On disk: 2 finished `.segment` files (the .tmp is in-progress, not done).
        assert_eq!(row.segments_done, Some(2));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_skips_targets_that_already_have_a_row() {
        let dir = test_dir("import-skip");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        fs::write(
            downloads.join("a.zip.thunderload.part.meta.json"),
            r#"{"url":"https://example.com/a.zip","total":10}"#,
        )
        .unwrap();
        let (mut store, _) = store_in(&dir);
        let mut existing = sample_record("existing");
        existing.target_path = Some(downloads.join("a.zip").to_string_lossy().to_string());
        store.upsert(existing).unwrap();
        assert!(store.import_orphans(&downloads).unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_partial_without_checkpoint_is_still_listed() {
        let dir = test_dir("import-partial-only");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        fs::write(downloads.join("lonely.bin.thunderload.part"), vec![0u8; 77]).unwrap();
        let (mut store, _) = store_in(&dir);
        let imported = store.import_orphans(&downloads).unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].kind, JobKind::Direct);
        assert_eq!(imported[0].state, JobState::Interrupted);
        assert_eq!(imported[0].downloaded, 77);
        assert_eq!(imported[0].checkpoint_path, None);
        assert!(imported[0].detail.contains("without a checkpoint"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_ignores_unknown_checkpoint_formats_and_missing_dirs() {
        let dir = test_dir("import-unknown");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        fs::write(
            downloads.join("weird.bin.thunderload.part.meta.json"),
            r#"{"something_else": true}"#,
        )
        .unwrap();
        let (mut store, _) = store_in(&dir);
        assert!(store.import_orphans(&downloads).unwrap().is_empty());
        assert!(store.import_orphans(&dir.join("no-such-dir")).unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn refresh_segments_reads_the_checkpoint() {
        let dir = test_dir("refresh-segments");
        let downloads = dir.join("downloads");
        fs::create_dir_all(&downloads).unwrap();
        fs::write(
            downloads.join("v.mp4.thunderload.part.meta.json"),
            r#"{"version":1,"playlist_url":"https://cdn.example.com/m.m3u8","media_url":"https://cdn.example.com/m.m3u8","audio_playlist_url":null,"referer":null,"extension":"mp4","video_init":null,"video_segments":[{"url":"https://cdn.example.com/s0.ts","range":null},{"url":"https://cdn.example.com/s1.ts","range":null}],"video_init_done":false,"video_done":[0],"audio_init":null,"audio_segments":[],"audio_init_done":false,"audio_done":[]}"#,
        )
        .unwrap();
        let (mut store, _) = store_in(&dir);
        let mut record = sample_record("h");
        record.kind = JobKind::Hls;
        record.target_path = Some(downloads.join("v.mp4").to_string_lossy().to_string());
        store.upsert(record).unwrap();
        assert!(store.refresh_segments("h").unwrap());
        let row = store.get("h").unwrap();
        assert_eq!(row.segments_done, Some(1));
        assert_eq!(row.segments_total, Some(2));
        // Second call with no change does not rewrite.
        assert!(!store.refresh_segments("h").unwrap());
        // Direct rows have no segments.
        let mut direct = sample_record("d");
        direct.kind = JobKind::Direct;
        store.upsert(direct).unwrap();
        assert!(!store.refresh_segments("d").unwrap());
        assert!(!store.refresh_segments("ghost").unwrap());
        let _ = fs::remove_dir_all(&dir);
    }
}

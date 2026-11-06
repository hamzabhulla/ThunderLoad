use anyhow::{anyhow, Context, Result};
use futures_util::{Stream, StreamExt};
use reqwest::{header, Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{mpsc, watch},
};

use crate::download::DownloadProgress;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeMetadata {
    pub url: String,
    pub total: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub connections: usize,
}

#[derive(Debug, Clone)]
pub enum ManagedOutcome {
    Completed(PathBuf),
    Paused { path: PathBuf, downloaded: u64, total: Option<u64> },
}

#[derive(Clone)]
pub struct TransferControlHandle {
    pause_tx: watch::Sender<bool>,
    cancel_tx: watch::Sender<bool>,
    speed_limit_bps: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct TransferControl {
    pause_rx: watch::Receiver<bool>,
    cancel_rx: watch::Receiver<bool>,
    limiter: Arc<RateLimiter>,
}

struct RateLimiter {
    started: Instant,
    bytes: AtomicU64,
    limit_bps: Arc<AtomicU64>,
}

impl TransferControlHandle {
    pub fn set_paused(&self, paused: bool) {
        self.pause_tx.send_replace(paused);
    }

    pub fn cancel(&self) {
        self.cancel_tx.send_replace(true);
    }

    pub fn set_speed_limit(&self, bytes_per_second: u64) {
        self.speed_limit_bps.store(bytes_per_second, Ordering::Relaxed);
    }
}

pub fn create_transfer_control(speed_limit_bps: u64) -> (TransferControlHandle, TransferControl) {
    let (pause_tx, pause_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let speed_limit_bps = Arc::new(AtomicU64::new(speed_limit_bps));
    let control = TransferControl {
        pause_rx,
        cancel_rx,
        limiter: Arc::new(RateLimiter {
            started: Instant::now(),
            bytes: AtomicU64::new(0),
            limit_bps: speed_limit_bps.clone(),
        }),
    };
    (
        TransferControlHandle {
            pause_tx,
            cancel_tx,
            speed_limit_bps,
        },
        control,
    )
}

impl TransferControl {
    fn paused(&self) -> bool {
        *self.pause_rx.borrow()
    }

    pub(crate) fn cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }

    fn check(&self) -> Result<bool> {
        if self.cancelled() {
            return Err(anyhow!("Download cancelled"));
        }
        Ok(!self.paused())
    }

    pub(crate) async fn next_chunk<S, B, E>(&self, stream: &mut S) -> Result<Option<Option<B>>>
    where
        S: Stream<Item = std::result::Result<B, E>> + Unpin,
        E: std::fmt::Display,
    {
        let mut pause = self.pause_rx.clone();
        let mut cancel = self.cancel_rx.clone();
        loop {
            if self.cancelled() { return Err(anyhow!("Download cancelled")); }
            if self.paused() { return Ok(None); }
            tokio::select! {
                item = stream.next() => {
                    return match item {
                        Some(Ok(value)) => Ok(Some(Some(value))),
                        Some(Err(error)) => Err(anyhow!(error.to_string())),
                        None => Ok(Some(None)),
                    };
                }
                _ = pause.changed() => {},
                _ = cancel.changed() => {},
            }
        }
    }

    pub(crate) async fn throttle(&self, byte_count: usize) -> Result<bool> {
        if !self.check()? {
            return Ok(false);
        }
        let limit = self.limiter.limit_bps.load(Ordering::Relaxed);
        if limit == 0 || byte_count == 0 {
            return self.check();
        }
        let total = self
            .limiter
            .bytes
            .fetch_add(byte_count as u64, Ordering::Relaxed)
            .saturating_add(byte_count as u64);
        let seconds = total as f64 / limit as f64;
        let deadline = self.limiter.started + Duration::from_secs_f64(seconds.min(86_400.0));
        let mut pause = self.pause_rx.clone();
        let mut cancel = self.cancel_rx.clone();
        loop {
            if self.cancelled() { return Err(anyhow!("Download cancelled")); }
            if self.paused() { return Ok(false); }
            tokio::select! {
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return self.check(),
                _ = pause.changed() => {},
                _ = cancel.changed() => {},
            }
        }
    }
}

fn make_client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("ThunderLoad/0.3 (download manager)")
        .connect_timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(16)
        .build()?)
}

fn content_range_total(value: &str) -> Option<u64> {
    value.rsplit_once('/')?.1.parse().ok()
}

fn range_is_valid(value: &str, start: u64, end: u64, total: u64) -> bool {
    value == format!("bytes {start}-{end}/{total}")
}

fn metadata_path(work_dir: &Path) -> PathBuf {
    work_dir.join("resume.json")
}

async fn write_metadata(work_dir: &Path, metadata: &ResumeMetadata) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(metadata)?;
    let temp = work_dir.join("resume.json.tmp");
    fs::write(&temp, bytes).await?;
    let destination = metadata_path(work_dir);
    if fs::try_exists(&destination).await.unwrap_or(false) {
        fs::remove_file(&destination).await?;
    }
    fs::rename(temp, destination).await?;
    Ok(())
}

async fn load_metadata(work_dir: &Path) -> Option<ResumeMetadata> {
    let bytes = fs::read(metadata_path(work_dir)).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

async fn emit_progress(
    tx: &mpsc::UnboundedSender<DownloadProgress>,
    downloaded: u64,
    total: Option<u64>,
    phase: &str,
) {
    let percent = total.filter(|size| *size > 0).map(|size| downloaded as f64 * 100.0 / size as f64);
    let _ = tx.send(DownloadProgress {
        downloaded,
        total,
        percent,
        phase: phase.to_string(),
    });
}

pub async fn managed_download(
    url: &str,
    target: &Path,
    job_id: &str,
    connections: usize,
    referer: Option<&str>,
    control: TransferControl,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<ManagedOutcome> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(anyhow!("Only HTTP and HTTPS URLs can be downloaded"));
    }
    let parent = target.parent().ok_or_else(|| anyhow!("Target path has no parent folder"))?;
    fs::create_dir_all(parent).await?;
    let safe_id: String = job_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || "-_".contains(*ch))
        .take(80)
        .collect();
    if safe_id.is_empty() {
        return Err(anyhow!("Invalid download job identifier"));
    }
    let work_dir = parent.join(format!(".thunderload-job-{safe_id}"));
    fs::create_dir_all(&work_dir).await?;

    let client = make_client()?;
    let mut head_request = client.head(url);
    if let Some(referrer) = referer {
        head_request = head_request.header(header::REFERER, referrer);
    }
    let head = head_request.send().await.ok();
    let mut total = head
        .as_ref()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.content_length());
    let mut etag = head.as_ref().and_then(|response| {
        response.headers().get(header::ETAG).and_then(|value| value.to_str().ok()).map(str::to_string)
    });
    let mut last_modified = head.as_ref().and_then(|response| {
        response.headers().get(header::LAST_MODIFIED).and_then(|value| value.to_str().ok()).map(str::to_string)
    });
    let mut range_probe = false;
    if let Some(response) = head.as_ref().filter(|response| response.status().is_success()) {
        range_probe = response
            .headers()
            .get(header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("bytes"));
    }
    if !range_probe {
        let mut probe = client.get(url).header(header::RANGE, "bytes=0-0");
        if let Some(referrer) = referer {
            probe = probe.header(header::REFERER, referrer);
        }
        if let Ok(response) = probe.send().await {
            if response.status() == StatusCode::PARTIAL_CONTENT {
                range_probe = true;
                if let Some(value) = response.headers().get(header::CONTENT_RANGE).and_then(|v| v.to_str().ok()) {
                    total = content_range_total(value).or(total);
                }
                etag = etag.or_else(|| response.headers().get(header::ETAG).and_then(|v| v.to_str().ok()).map(str::to_string));
                last_modified = last_modified.or_else(|| response.headers().get(header::LAST_MODIFIED).and_then(|v| v.to_str().ok()).map(str::to_string));
            }
        }
    }

    let connections = connections.clamp(1, 16);
    let current = ResumeMetadata {
        url: url.to_string(),
        total,
        etag,
        last_modified,
        connections,
    };
    let old = load_metadata(&work_dir).await;
    if old.as_ref().is_some_and(|previous| {
        previous.url != current.url
            || previous.total != current.total
            || (previous.etag.is_some() && current.etag.is_some() && previous.etag != current.etag)
            || previous.etag != current.etag
            || previous.last_modified != current.last_modified
            || previous.connections != current.connections
    }) {
        fs::remove_dir_all(&work_dir).await?;
        fs::create_dir_all(&work_dir).await?;
    }
    write_metadata(&work_dir, &current).await?;

    let outcome = if range_probe && total.is_some_and(|size| size >= 2 * 1024 * 1024) {
        download_ranges(
            &client,
            url,
            target,
            total.unwrap(),
            connections,
            referer,
            &work_dir,
            &control,
            &progress_tx,
        )
        .await
    } else {
        download_single(
            &client,
            url,
            target,
            total,
            range_probe,
            referer,
            &work_dir,
            &control,
            &progress_tx,
        )
        .await
    };

    match outcome {
        Ok(ManagedOutcome::Completed(path)) => {
            let _ = fs::remove_dir_all(&work_dir).await;
            Ok(ManagedOutcome::Completed(path))
        }
        Ok(paused @ ManagedOutcome::Paused { .. }) => Ok(paused),
        Err(error) if control.cancelled() => {
            let _ = fs::remove_dir_all(&work_dir).await;
            Err(anyhow!("Download cancelled"))
        }
        Err(error) => Err(error),
    }
}

async fn download_ranges(
    client: &Client,
    url: &str,
    target: &Path,
    total: u64,
    connections: usize,
    referer: Option<&str>,
    work_dir: &Path,
    control: &TransferControl,
    progress_tx: &mpsc::UnboundedSender<DownloadProgress>,
) -> Result<ManagedOutcome> {
    let chunk_size = total.div_ceil(connections as u64);
    let mut initial = 0_u64;
    let mut ranges = Vec::new();
    for index in 0..connections {
        let start = index as u64 * chunk_size;
        if start >= total {
            break;
        }
        let end = (start + chunk_size - 1).min(total - 1);
        let part = work_dir.join(format!("part-{index:03}.bin"));
        let len = fs::metadata(&part).await.map(|meta| meta.len()).unwrap_or(0);
        if len > end - start + 1 {
            let _ = fs::remove_file(&part).await;
            ranges.push((index, start, end, part, 0));
        } else {
            initial = initial.saturating_add(len);
            ranges.push((index, start, end, part, len));
        }
    }
    emit_progress(progress_tx, initial, Some(total), "resuming").await;

    let mut tasks = tokio::task::JoinSet::new();
    for (_index, start, end, part, existing) in ranges {
        if existing == end - start + 1 {
            continue;
        }
        let client = client.clone();
        let url = url.to_string();
        let referrer = referer.map(str::to_string);
        let control = control.clone();
        let tx = progress_tx.clone();
        tasks.spawn(async move {
            let request_start = start + existing;
            let mut request = client
                .get(&url)
                .header(header::RANGE, format!("bytes={request_start}-{end}"));
            if let Some(referrer) = referrer {
                request = request.header(header::REFERER, referrer);
            }
            let response = request.send().await?.error_for_status()?;
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(anyhow!("Server did not honor a resumed byte-range request"));
            }
            let actual = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow!("Server omitted Content-Range on a resumed request"))?;
            if !range_is_valid(actual, request_start, end, total) {
                return Err(anyhow!("Server returned an unexpected resumed byte range: {actual}"));
            }
            let mut file = fs::OpenOptions::new().create(true).append(true).open(&part).await?;
            let mut stream = response.bytes_stream();
            loop {
                let Some(next) = control.next_chunk(&mut stream).await? else {
                    file.flush().await?;
                    return Ok::<bool, anyhow::Error>(false);
                };
                let Some(chunk) = next else { break; };
                if !control.throttle(chunk.len()).await? {
                    file.flush().await?;
                    return Ok::<bool, anyhow::Error>(false);
                }
                file.write_all(&chunk).await?;
                let current = fs::metadata(&part).await?.len();
                emit_progress(&tx, current, Some(total), "downloading").await;
            }
            file.flush().await?;
            let expected = end - start + 1;
            let actual_len = fs::metadata(&part).await?.len();
            if actual_len != expected {
                return Err(anyhow!("A resumed byte range is incomplete ({actual_len}/{expected})"));
            }
            Ok(true)
        });
    }

    let mut paused = false;
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                paused = true;
                tasks.abort_all();
            }
            Ok(Err(error)) => {
                first_error = Some(error);
                tasks.abort_all();
            }
            Err(error) => {
                first_error = Some(anyhow!(error));
                tasks.abort_all();
            }
        }
        if paused || first_error.is_some() {
            while tasks.join_next().await.is_some() {}
            break;
        }
    }
    if control.cancelled() {
        return Err(anyhow!("Download cancelled"));
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if paused || control.paused() {
        let downloaded = directory_size(work_dir).await?;
        emit_progress(progress_tx, downloaded, Some(total), "paused").await;
        return Ok(ManagedOutcome::Paused { path: target.to_path_buf(), downloaded, total: Some(total) });
    }

    let temp = work_dir.join("assembled.tmp");
    let mut output = fs::File::create(&temp).await?;
    let count = total.div_ceil(chunk_size);
    for index in 0..count {
        let part = work_dir.join(format!("part-{index:03}.bin"));
        let mut input = fs::File::open(&part).await?;
        tokio::io::copy(&mut input, &mut output).await?;
    }
    output.flush().await?;
    drop(output);
    let actual = fs::metadata(&temp).await?.len();
    if actual != total {
        return Err(anyhow!("Assembled file size mismatch ({actual}/{total})"));
    }
    replace_target(&temp, target).await?;
    emit_progress(progress_tx, total, Some(total), "complete").await;
    Ok(ManagedOutcome::Completed(target.to_path_buf()))
}

async fn download_single(
    client: &Client,
    url: &str,
    target: &Path,
    total: Option<u64>,
    range_probe: bool,
    referer: Option<&str>,
    work_dir: &Path,
    control: &TransferControl,
    progress_tx: &mpsc::UnboundedSender<DownloadProgress>,
) -> Result<ManagedOutcome> {
    let part = work_dir.join("single.part");
    let mut downloaded = fs::metadata(&part).await.map(|meta| meta.len()).unwrap_or(0);
    let mut request = client.get(url);
    if downloaded > 0 && range_probe {
        request = request.header(header::RANGE, format!("bytes={downloaded}-"));
    } else {
        downloaded = 0;
    }
    if let Some(referrer) = referer {
        request = request.header(header::REFERER, referrer);
    }
    let mut response = request.send().await?;
    if downloaded > 0 {
        let valid_resume = if response.status() == StatusCode::PARTIAL_CONTENT {
            response.headers().get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with(&format!("bytes {downloaded}-"))
                    && total.is_none_or(|expected| content_range_total(value) == Some(expected)))
        } else { false };
        if !valid_resume {
            downloaded = 0;
            let _ = fs::remove_file(&part).await;
            let mut retry = client.get(url);
            if let Some(referrer) = referer {
                retry = retry.header(header::REFERER, referrer);
            }
            response = retry.send().await?;
        }
    }
    response.error_for_status_ref()?;
    let mut file = if downloaded > 0 {
        fs::OpenOptions::new().create(true).append(true).open(&part).await?
    } else {
        fs::File::create(&part).await?
    };
    emit_progress(progress_tx, downloaded, total, "resuming").await;
    let mut stream = response.bytes_stream();
    loop {
        let Some(next) = control.next_chunk(&mut stream).await? else {
            file.flush().await?;
            return Ok(ManagedOutcome::Paused { path: target.to_path_buf(), downloaded, total });
        };
        let Some(chunk) = next else { break; };
        if !control.throttle(chunk.len()).await? {
            file.flush().await?;
            return Ok(ManagedOutcome::Paused { downloaded, total });
        }
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        emit_progress(progress_tx, downloaded, total, "downloading").await;
    }
    file.flush().await?;
    drop(file);
    if total.is_some_and(|expected| expected != downloaded) {
        return Err(anyhow!("Downloaded file size mismatch ({downloaded}/{})", total.unwrap()));
    }
    replace_target(&part, target).await?;
    emit_progress(progress_tx, downloaded, total.or(Some(downloaded)), "complete").await;
    Ok(ManagedOutcome::Completed(target.to_path_buf()))
}

async fn directory_size(directory: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let mut entries = fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() && entry.file_name() != "resume.json" {
            total = total.saturating_add(entry.metadata().await?.len());
        }
    }
    Ok(total)
}

async fn replace_target(temp: &Path, target: &Path) -> Result<()> {
    if fs::try_exists(target).await.unwrap_or(false) {
        fs::remove_file(target).await?;
    }
    fs::rename(temp, target).await.with_context(|| format!("Cannot move completed file to {}", target.display()))?;
    Ok(())
}


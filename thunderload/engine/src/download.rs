use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex}, time::Instant};
use tokio::{fs, io::AsyncWriteExt, sync::{mpsc, Mutex as AsyncMutex}};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct DownloadProgress {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub percent: Option<f64>,
    pub phase: String,
}

// Reserve the destination atomically so two simultaneous jobs cannot select
// the same filename and overwrite one another.
async fn next_available_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| anyhow!("Target path has no parent folder"))?;
    fs::create_dir_all(parent).await.with_context(|| format!("Cannot create {}", parent.display()))?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("download");
    let extension = path.extension().and_then(|s| s.to_str());
    for index in 0..10_000_u32 {
        let candidate = if index == 0 {
            path.to_path_buf()
        } else {
            let name = match extension {
                Some(ext) => format!("{stem} ({index}).{ext}"),
                None => format!("{stem} ({index})"),
            };
            parent.join(name)
        };
        match fs::OpenOptions::new().write(true).create_new(true).open(&candidate).await {
            Ok(file) => {
                drop(file);
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).with_context(|| format!("Cannot reserve {}", candidate.display())),
        }
    }
    Err(anyhow!("Could not find a free filename in the download folder"))
}

fn validate_content_range(actual: &str, start: u64, end: u64, total: u64) -> Result<()> {
    let expected = format!("bytes {start}-{end}/{total}");
    if actual != expected {
        return Err(anyhow!("Server returned an unexpected range: {actual}; expected {expected}"));
    }
    Ok(())
}

/// Some servers support byte ranges but omit Accept-Ranges on HEAD. Probe a
/// single byte before falling back to one connection; only trust a strict 206.
async fn probe_range_support(client: &Client, url: &str, referer: Option<&str>) -> Option<u64> {
    let mut request = client.get(url).header(header::RANGE, "bytes=0-0");
    if let Some(referrer) = referer {
        request = request.header(header::REFERER, referrer);
    }
    let response = request.send().await.ok()?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return None;
    }
    let content_range = response.headers().get(header::CONTENT_RANGE)?.to_str().ok()?;
    let total = content_range.strip_prefix("bytes 0-0/")?.parse::<u64>().ok()?;
    if total < 1 || response.content_length().is_some_and(|length| length != 1) {
        return None;
    }
    Some(total)
}

fn client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("ThunderLoad/0.1 (download manager)")
        .connect_timeout(std::time::Duration::from_secs(15))
        .pool_max_idle_per_host(16)
        .build()?)
}

pub async fn download_url(
    url: &str,
    target: &Path,
    connections: usize,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<PathBuf> {
    let target = next_available_path(target).await?;
    let result = async {
        let connections = connections.clamp(1, 16);
        let client = client()?;
        let mut head_request = client.head(url);
        if let Some(referrer) = referer { head_request = head_request.header(header::REFERER, referrer); }
        let head = head_request.send().await.ok();
        let mut total = head.as_ref().filter(|response| response.status().is_success())
            .and_then(|response| response.content_length());
        let mut supports_ranges = head.as_ref().filter(|response| response.status().is_success()).map(|response| {
            response.headers().get(header::ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false)
        }).unwrap_or(false);

        // Probe when HEAD omits Accept-Ranges (common on CDN/object-storage
        // endpoints). A valid one-byte 206 is stronger evidence than the hint.
        if connections > 1 && !supports_ranges {
            if let Some(probed_total) = probe_range_support(&client, url, referer).await {
                total = Some(probed_total);
                supports_ranges = true;
            }
        }

        if supports_ranges && total.unwrap_or(0) >= 2 * 1024 * 1024 && connections > 1 {
            match download_segmented(&client, url, &target, total.unwrap(), connections.min(16), referer, progress_tx.clone()).await {
                Ok(path) => return Ok(path),
                Err(_) => {
                    // Some servers advertise byte ranges but return invalid ranges.
                    // Restart sequentially rather than leave a potentially corrupt file.
                    let _ = progress_tx.send(DownloadProgress {
                        downloaded: 0, total, percent: Some(0.0), phase: "fallback".into()
                    });
                }
            }
        }
        download_sequential(&client, url, &target, total, referer, progress_tx).await
    }.await;

    if result.is_err() {
        // The destination is an empty reservation until the transfer commits.
        let _ = fs::remove_file(&target).await;
    }
    result
}
async fn prepare_parent(target: &Path) -> Result<()> {
    let parent = target.parent().ok_or_else(|| anyhow!("Target path has no parent folder"))?;
    fs::create_dir_all(parent).await.with_context(|| format!("Cannot create {}", parent.display()))?;
    Ok(())
}

async fn download_sequential(
    client: &Client,
    url: &str,
    target: &Path,
    total: Option<u64>,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<PathBuf> {
    prepare_parent(target).await?;
    let temp = target.with_extension(format!("{}{}.part", target.extension().and_then(|x| x.to_str()).unwrap_or("file"), Uuid::new_v4()));
    let mut request = client.get(url);
    if let Some(referrer) = referer { request = request.header(header::REFERER, referrer); }
    let response = request.send().await?.error_for_status()?;
    let actual_total = response.content_length().or(total);

    let transfer_result = async {
        let mut stream = response.bytes_stream();
        let mut file = fs::File::create(&temp).await?;
        let mut downloaded = 0_u64;
        let mut last_report = Instant::now();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            let percent = actual_total.filter(|size| *size > 0)
                .map(|size| downloaded as f64 * 100.0 / size as f64);
            if last_report.elapsed() >= std::time::Duration::from_millis(180) {
                let _ = progress_tx.send(DownloadProgress {
                    downloaded, total: actual_total, percent, phase: "downloading".into()
                });
                last_report = Instant::now();
            }
        }
        file.flush().await?;
        Ok::<u64, anyhow::Error>(downloaded)
    }.await;

    let downloaded = match transfer_result {
        Ok(value) => value,
        Err(error) => {
            let _ = fs::remove_file(&temp).await;
            return Err(error).context("Sequential download failed; incomplete temporary file removed");
        }
    };
    if let Err(error) = replace_file(&temp, target).await {
        let _ = fs::remove_file(&temp).await;
        return Err(error).context("Could not commit completed download; temporary file removed");
    }
    let _ = progress_tx.send(DownloadProgress {
        downloaded, total: actual_total.or(Some(downloaded)), percent: Some(100.0), phase: "complete".into()
    });
    Ok(target.to_path_buf())
}
async fn download_segmented(
    client: &Client,
    url: &str,
    target: &Path,
    total: u64,
    connections: usize,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<PathBuf> {
    prepare_parent(target).await?;
    let work_dir = target.parent().unwrap().join(format!(".thunderload-{}", Uuid::new_v4()));
    fs::create_dir_all(&work_dir).await?;
    let chunk_size = (total + connections as u64 - 1) / connections as u64;
    let received = Arc::new(AtomicU64::new(0));
    let last_report = Arc::new(Mutex::new(Instant::now()));
    let mut tasks = tokio::task::JoinSet::new();

    for index in 0..connections {
        let start = index as u64 * chunk_size;
        if start >= total { break; }
        let end = (start + chunk_size - 1).min(total - 1);
        let part_path = work_dir.join(format!("part-{index:03}"));
        let client = client.clone();
        let url = url.to_owned();
        let received = received.clone();
        let last_report = last_report.clone();
        let referrer = referer.map(str::to_string);
        let progress_tx = progress_tx.clone();
        tasks.spawn(async move {
            let mut request = client.get(&url).header(header::RANGE, format!("bytes={start}-{end}"));
            if let Some(referrer) = referrer { request = request.header(header::REFERER, referrer); }
            let response = request.send().await?;
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(anyhow!("Server did not honor byte range"));
            }
            let actual_range = response.headers().get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow!("Server omitted the Content-Range header"))?;
            validate_content_range(actual_range, start, end, total)?;
            if response.content_length().is_some_and(|length| length != end - start + 1) {
                return Err(anyhow!("Server returned a range with an unexpected content length"));
            }
            let mut stream = response.bytes_stream();
            let mut file = fs::File::create(&part_path).await?;
            let mut local_bytes = 0_u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
                local_bytes += chunk.len() as u64;
                let sum = received.fetch_add(chunk.len() as u64, Ordering::Relaxed) + chunk.len() as u64;
                if let Ok(mut last) = last_report.lock() {
                    if last.elapsed() >= std::time::Duration::from_millis(180) || sum == total {
                        let _ = progress_tx.send(DownloadProgress {
                            downloaded: sum,
                            total: Some(total),
                            percent: Some(sum as f64 * 100.0 / total as f64),
                            phase: "downloading".into(),
                        });
                        *last = Instant::now();
                    }
                }
            }
            file.flush().await?;
            if local_bytes != end - start + 1 {
                return Err(anyhow!("A range returned an incomplete segment"));
            }
            Ok::<PathBuf, anyhow::Error>(part_path)
        });
    }

    let mut completed_parts = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(path)) => completed_parts.push(path),
            Ok(Err(error)) => { tasks.abort_all(); let _ = fs::remove_dir_all(&work_dir).await; return Err(error); }
            Err(error) => { tasks.abort_all(); let _ = fs::remove_dir_all(&work_dir).await; return Err(error.into()); }
        }
    }
    completed_parts.sort();

    let temp_output = work_dir.join("assembled.part");
    let assemble_result = async {
        let mut output = fs::File::create(&temp_output).await?;
        for part in completed_parts {
            let mut input = fs::File::open(part).await?;
            tokio::io::copy(&mut input, &mut output).await?;
        }
        output.flush().await?;
        drop(output);
        let metadata = fs::metadata(&temp_output).await?;
        if metadata.len() != total {
            return Err(anyhow!("Combined file size did not match the server's content length"));
        }
        replace_file(&temp_output, target).await?;
        Ok::<(), anyhow::Error>(())
    }.await;
    if let Err(error) = assemble_result {
        let _ = fs::remove_dir_all(&work_dir).await;
        return Err(error).context("Could not assemble segmented download; temporary parts removed");
    }
    let _ = fs::remove_dir_all(&work_dir).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: total, total: Some(total), percent: Some(100.0), phase: "complete".into()
    });
    Ok(target.to_path_buf())
}

async fn replace_file(temp: &Path, target: &Path) -> Result<()> {
    if fs::try_exists(target).await.unwrap_or(false) {
        fs::remove_file(target).await?;
    }
    fs::rename(temp, target).await.with_context(|| format!("Cannot move completed file to {}", target.display()))?;
    Ok(())
}

pub fn default_download_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    let base = std::env::var_os("USERPROFILE").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    #[cfg(not(target_os = "windows"))]
    let base = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    base.join("Downloads").join("ThunderLoad")
}

pub fn safe_filename(input: &str, fallback: &str) -> String {
    let cleaned: String = input.chars()
        .map(|c| if c.is_control() || "<>:\"/\\|?*".contains(c) { '_' } else { c })
        .collect();
    let cleaned = cleaned.trim().trim_matches('.');
    if cleaned.is_empty() { fallback.to_string() } else { cleaned.to_string() }
}


#[cfg(test)]
mod tests {
    use super::{next_available_path, safe_filename, validate_content_range};
    use uuid::Uuid;

    #[test]
    fn byte_range_header_must_match_exact_requested_range() {
        assert!(validate_content_range("bytes 0-99/1000", 0, 99, 1000).is_ok());
        assert!(validate_content_range("bytes 0-98/1000", 0, 99, 1000).is_err());
        assert!(validate_content_range("bytes 0-99/900", 0, 99, 1000).is_err());
    }

    #[test]
    fn one_byte_probe_requires_a_concrete_resource_length() {
        fn parse_probe(value: &str) -> Option<u64> {
            value.strip_prefix("bytes 0-0/")?.parse::<u64>().ok().filter(|total| *total >= 1)
        }
        assert_eq!(parse_probe("bytes 0-0/4096"), Some(4096));
        assert_eq!(parse_probe("bytes 0-0/*"), None);
        assert_eq!(parse_probe("bytes 1-1/4096"), None);
        assert_eq!(parse_probe("bytes 0-0/0"), None);
    }

    #[tokio::test]
    async fn concurrent_jobs_reserve_distinct_filenames() {
        let directory = std::env::temp_dir().join(format!("thunderload-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let requested = directory.join("archive.zip");
        let first = next_available_path(&requested).await.unwrap();
        let second = next_available_path(&requested).await.unwrap();
        assert_eq!(first, requested);
        assert_eq!(second, directory.join("archive (1).zip"));
        assert!(tokio::fs::try_exists(&first).await.unwrap());
        assert!(tokio::fs::try_exists(&second).await.unwrap());
        tokio::fs::remove_dir_all(&directory).await.unwrap();
    }

    #[test]
    fn checkpoint_metadata_path_is_stable_and_separate_from_partial_bytes() {
        let target = std::path::Path::new("archive.zip");
        assert_eq!(super::partial_download_metadata_path(target), std::path::PathBuf::from("archive.zip.thunderload.part.meta.json"));
        assert_ne!(super::partial_download_metadata_path(target), super::partial_download_path(target));
    }

    #[test]
    fn filename_sanitization_removes_windows_path_separators() {
        assert_eq!(safe_filename(r"..\\folder/name?.zip", "fallback"), "__folder_name_.zip");
        assert_eq!(safe_filename("   ", "fallback"), "fallback");
    }
}


/// Shared token scheduler used by every active direct transfer. This
/// enforces one aggregate cap instead of granting each job the full limit.
pub struct GlobalRateLimiter {
    limit: AtomicU64,
    state: AsyncMutex<RateLimitState>,
}

struct RateLimitState {
    limit: u64,
    epoch: Instant,
    reserved: u64,
}

impl GlobalRateLimiter {
    pub fn new() -> Self {
        Self {
            limit: AtomicU64::new(0),
            state: AsyncMutex::new(RateLimitState { limit: 0, epoch: Instant::now(), reserved: 0 }),
        }
    }

    pub fn set_limit(&self, bytes_per_second: u64) {
        self.limit.store(bytes_per_second.min(1_073_741_824), Ordering::Relaxed);
    }

    /// Reserves bytes in the aggregate transfer timeline and returns the delay
    /// required before writing/consuming them without exceeding the global cap.
    pub async fn reserve(&self, bytes: u64) -> std::time::Duration {
        let limit = self.limit.load(Ordering::Relaxed);
        let mut state = self.state.lock().await;
        if state.limit != limit {
            state.limit = limit;
            state.epoch = Instant::now();
            state.reserved = 0;
        }
        if limit == 0 || bytes == 0 { return std::time::Duration::ZERO; }
        state.reserved = state.reserved.saturating_add(bytes);
        let target = state.epoch + std::time::Duration::from_secs_f64(state.reserved as f64 / limit as f64);
        target.saturating_duration_since(Instant::now())
    }
}

impl Default for GlobalRateLimiter {
    fn default() -> Self { Self::new() }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadControl {
    Running,
    Paused,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadOutcome {
    Completed,
    Paused,
    Cancelled,
}

/// Atomically reserves a final filename. The empty file acts as a reservation
/// until the completed .part file is atomically moved into place.
pub async fn reserve_download_path(path: &Path) -> Result<PathBuf> {
    next_available_path(path).await
}

/// Stable sidecar used by the desktop queue across process restarts.
pub fn partial_download_path(target: &Path) -> PathBuf {
    let extension = target.extension().and_then(|value| value.to_str()).unwrap_or("file");
    target.with_extension(format!("{extension}.thunderload.part"))
}

pub fn partial_download_metadata_path(target: &Path) -> PathBuf {
    let extension = target.extension().and_then(|value| value.to_str()).unwrap_or("file");
    target.with_extension(format!("{extension}.thunderload.part.meta.json"))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ResumeMetadata {
    url: String,
    etag: Option<String>,
    last_modified: Option<String>,
    total: Option<u64>,
}

fn header_string(response: &reqwest::Response, name: header::HeaderName) -> Option<String> {
    response.headers().get(name).and_then(|value| value.to_str().ok()).map(str::to_owned)
}

fn resume_identity_matches(
    saved: &ResumeMetadata,
    url: &str,
    etag: Option<&str>,
    last_modified: Option<&str>,
    total: Option<u64>,
) -> bool {
    if saved.url != url { return false; }
    if let Some(old) = saved.etag.as_deref() {
        if etag != Some(old) { return false; }
    } else if let Some(old) = saved.last_modified.as_deref() {
        if last_modified != Some(old) { return false; }
    } else {
        return false;
    }
    match (saved.total, total) {
        (Some(old), Some(current)) => old == current,
        _ => true,
    }
}

async fn read_resume_metadata(path: &Path) -> Option<ResumeMetadata> {
    let bytes = fs::read(path).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

async fn write_resume_metadata(path: &Path, metadata: &ResumeMetadata) -> Result<()> {
    let temporary = path.with_extension("meta.tmp");
    let bytes = serde_json::to_vec(metadata)?;
    fs::write(&temporary, bytes).await?;
    if fs::try_exists(path).await.unwrap_or(false) {
        fs::remove_file(path).await?;
    }
    fs::rename(&temporary, path).await
        .with_context(|| format!("Could not save resume metadata at {}", path.display()))?;
    Ok(())
}

async fn remove_resume_files(part: &Path, metadata: &Path, target: &Path) {
    let _ = fs::remove_file(part).await;
    let _ = fs::remove_file(metadata).await;
    let _ = fs::remove_file(target).await;
}

fn parse_resume_content_range(value: &str, expected_start: u64) -> Result<Option<u64>> {
    let range = value.strip_prefix("bytes ")
        .ok_or_else(|| anyhow!("Server returned an invalid Content-Range header"))?;
    let (span, total) = range.split_once('/')
        .ok_or_else(|| anyhow!("Server returned an invalid Content-Range header"))?;
    let (start, end) = span.split_once('-')
        .ok_or_else(|| anyhow!("Server returned an invalid Content-Range header"))?;
    let start: u64 = start.parse().context("Invalid Content-Range start")?;
    let end: u64 = end.parse().context("Invalid Content-Range end")?;
    if start != expected_start || end < start {
        return Err(anyhow!("Server resumed at an unexpected byte offset"));
    }
    if total == "*" { return Ok(None); }
    let total: u64 = total.parse().context("Invalid Content-Range total")?;
    if end >= total { return Err(anyhow!("Content-Range exceeds the resource length")); }
    Ok(Some(total))
}

/// Sequential, checkpointed transfer used by the desktop queue. Unlike the
/// parallel fast path, this writes one stable partial file so pause/resume is
/// deterministic, including after an application restart.
pub async fn download_url_resumable(
    url: &str,
    target: &Path,
    referer: Option<&str>,
    mut control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    if !matches!(url::Url::parse(url)?.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS download URLs are supported"));
    }
    prepare_parent(target).await?;
    let part = partial_download_path(target);
    let metadata_path = partial_download_metadata_path(target);
    let mut offset = fs::metadata(&part).await.map(|meta| meta.len()).unwrap_or(0);

    if *control.borrow() == DownloadControl::Cancelled {
        remove_resume_files(&part, &metadata_path, target).await;
        return Ok(DownloadOutcome::Cancelled);
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok(DownloadOutcome::Paused);
    }

    let mut saved_metadata = if offset > 0 {
        read_resume_metadata(&metadata_path).await
    } else {
        None
    };
    // A partial file without a matching strong validator is unsafe to append:
    // the remote object may have changed since the checkpoint was written.
    if offset > 0 {
        let valid_checkpoint = saved_metadata.as_ref().is_some_and(|saved| {
            saved.url == url && (saved.etag.is_some() || saved.last_modified.is_some())
        });
        if !valid_checkpoint {
            let _ = fs::remove_file(&part).await;
            let _ = fs::remove_file(&metadata_path).await;
            offset = 0;
            saved_metadata = None;
        }
    }

    let client = client()?;
    let mut head = client.head(url);
    if let Some(referrer) = referer { head = head.header(header::REFERER, referrer); }
    let head_response = head.send().await.ok();
    let server_total = head_response.as_ref()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.content_length());
    let head_etag = head_response.as_ref()
        .and_then(|response| response.headers().get(header::ETAG))
        .and_then(|value| value.to_str().ok()).map(str::to_owned);
    let head_modified = head_response.as_ref()
        .and_then(|response| response.headers().get(header::LAST_MODIFIED))
        .and_then(|value| value.to_str().ok()).map(str::to_owned);

    let mut request = client.get(url);
    if offset > 0 {
        request = request.header(header::RANGE, format!("bytes={offset}-"));
        if let Some(saved) = &saved_metadata {
            if let Some(etag) = saved.etag.as_deref() {
                request = request.header(header::IF_RANGE, etag);
            } else if let Some(last_modified) = saved.last_modified.as_deref() {
                request = request.header(header::IF_RANGE, last_modified);
            }
        }
    }
    if let Some(referrer) = referer { request = request.header(header::REFERER, referrer); }
    let mut response = request.send().await.context("Could not connect to the download server")?;
    if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(anyhow!("Download server returned HTTP {}", response.status()));
    }

    let mut append = offset > 0 && response.status() == StatusCode::PARTIAL_CONTENT;
    if append {
        let range = response.headers().get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow!("Server omitted Content-Range while resuming"))?;
        parse_resume_content_range(range, offset)?;
        let response_etag = response.headers().get(header::ETAG)
            .and_then(|value| value.to_str().ok());
        let response_modified = response.headers().get(header::LAST_MODIFIED)
            .and_then(|value| value.to_str().ok());
        let response_total = response.headers().get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_resume_content_range(value, offset).ok().flatten());
        let still_same_resource = saved_metadata.as_ref().is_some_and(|saved| {
            resume_identity_matches(saved, url, response_etag, response_modified, response_total)
        });
        if !still_same_resource {
            // If-Range should make a changed resource return 200. Be defensive:
            // if a server nevertheless returns a mismatched 206, discard the
            // old prefix and issue a fresh full GET instead of joining versions.
            let mut restart = client.get(url);
            if let Some(referrer) = referer { restart = restart.header(header::REFERER, referrer); }
            response = restart.send().await.context("Could not restart a changed download")?.error_for_status()?;
            append = false;
            let _ = fs::remove_file(&part).await;
        }
    } else if offset > 0 {
        // Server ignored Range or If-Range failed because the object changed.
        // Restart from byte zero using this full response.
        let _ = fs::remove_file(&part).await;
    } else if response.status() == StatusCode::PARTIAL_CONTENT {
        return Err(anyhow!("Server returned partial content without a requested byte range"));
    }

    let start_offset = if append { offset } else { 0 };
    let response_etag = response.headers().get(header::ETAG)
        .and_then(|value| value.to_str().ok()).map(str::to_owned).or(head_etag);
    let response_modified = response.headers().get(header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok()).map(str::to_owned).or(head_modified);
    let total = if append {
        response.headers().get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_resume_content_range(value, offset).ok().flatten())
            .or(server_total)
    } else {
        response.content_length().or(server_total)
    };

    let current_metadata = ResumeMetadata {
        url: url.to_owned(),
        etag: response_etag,
        last_modified: response_modified,
        total,
    };
    write_resume_metadata(&metadata_path, &current_metadata).await?;
    saved_metadata = Some(current_metadata);

    let mut options = fs::OpenOptions::new();
    options.create(true).write(true);
    if append { options.append(true); } else { options.truncate(true); }
    let mut file = options.open(&part).await?;
    let mut stream = response.bytes_stream();
    let mut downloaded = start_offset;
    let mut last_report = Instant::now();

    loop {
        if *control.borrow() == DownloadControl::Paused {
            file.flush().await?;
            let _ = progress_tx.send(DownloadProgress {
                downloaded, total, percent: total.map(|size| if size == 0 { 0.0 } else { downloaded as f64 * 100.0 / size as f64 }),
                phase: "paused".into()
            });
            return Ok(DownloadOutcome::Paused);
        }
        if *control.borrow() == DownloadControl::Cancelled {
            file.flush().await?;
            drop(file);
            remove_resume_files(&part, &metadata_path, target).await;
            return Ok(DownloadOutcome::Cancelled);
        }

        let next = tokio::select! {
            value = stream.next() => value,
            changed = control.changed() => {
                if changed.is_ok() { continue; }
                None
            }
        };
        let Some(chunk_result) = next else { break; };
        let chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(error) => {
                let _ = file.flush().await;
                return Err(error).context("Network interrupted; partial file has been preserved for Resume");
            }
        };
        file.write_all(&chunk).await?;
        downloaded = downloaded.saturating_add(chunk.len() as u64);

        let wait = rate_limiter.reserve(chunk.len() as u64).await;
        if !wait.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                changed = control.changed() => { if changed.is_err() { /* host is shutting down */ } }
            }
        }

        if last_report.elapsed() >= std::time::Duration::from_millis(160) {
            let percent = total.filter(|size| *size > 0)
                .map(|size| downloaded as f64 * 100.0 / size as f64);
            let _ = progress_tx.send(DownloadProgress {
                downloaded, total, percent, phase: "downloading".into()
            });
            last_report = Instant::now();
        }
    }

    file.flush().await?;
    drop(file);
    if let Some(expected_total) = total {
        if downloaded != expected_total {
            return Err(anyhow!("Download ended at {downloaded} bytes, but the server reported {expected_total}. Partial data is preserved; Resume can retry."));
        }
    }
    replace_file(&part, target).await?;
    let _ = fs::remove_file(&metadata_path).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded, total: total.or(Some(downloaded)), percent: Some(100.0), phase: "complete".into()
    });
    let _ = saved_metadata;
    Ok(DownloadOutcome::Completed)
}

#[cfg(test)]
mod resumable_tests {
    use super::{parse_resume_content_range, partial_download_path};

    #[test]
    fn resume_content_range_requires_the_exact_checkpoint_offset() {
        assert_eq!(parse_resume_content_range("bytes 100-199/1000", 100).unwrap(), Some(1000));
        assert_eq!(parse_resume_content_range("bytes 100-199/*", 100).unwrap(), None);
        assert!(parse_resume_content_range("bytes 99-199/1000", 100).is_err());
        assert!(parse_resume_content_range("bytes 100-99/1000", 100).is_err());
        assert!(parse_resume_content_range("garbage", 100).is_err());
    }

    #[test]
    fn checkpoint_path_is_stable_for_a_final_destination() {
        let target = std::path::Path::new("movie.mp4");
        assert_eq!(partial_download_path(target), std::path::PathBuf::from("movie.mp4.thunderload.part"));
        assert_eq!(partial_download_path(target), partial_download_path(target));
    }
}



#[cfg(test)]
mod resumable_http_tests {
    use super::{
        download_url_resumable, partial_download_metadata_path, partial_download_path,
        reserve_download_path, DownloadControl, DownloadOutcome, GlobalRateLimiter,
    };
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc},
        thread,
        time::Duration,
    };
    use tokio::sync::{mpsc, watch};

    const BODY_SIZE: usize = 32 * 1024;

    fn spawn_range_server() -> (String, Arc<AtomicUsize>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let version = Arc::new(AtomicUsize::new(1));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_version = version.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(3));
                        continue;
                    }
                    Err(_) => break,
                };
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            request.extend_from_slice(&buffer[..count]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") { break; }
                        }
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let mut lines = request.lines();
                let first = lines.next().unwrap_or_default();
                let is_head = first.starts_with("HEAD ");
                let range = lines.find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("range").then(|| value.trim().to_string())
                });
                let if_range = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("if-range").then(|| value.trim().to_string())
                });
                let current_version = thread_version.load(Ordering::Relaxed);
                let etag = if current_version == 1 { "\"v1\"" } else { "\"v2\"" };
                let fill = if current_version == 1 { b'A' } else { b'B' };
                let body = vec![fill; BODY_SIZE];
                if is_head {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        BODY_SIZE, etag
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                let requested_start = range.as_deref()
                    .and_then(|value| value.strip_prefix("bytes="))
                    .and_then(|value| value.strip_suffix('-'))
                    .and_then(|value| value.parse::<usize>().ok());
                let can_resume = requested_start.is_some() && if_range.as_deref() == Some(etag);
                let start = if can_resume { requested_start.unwrap() } else { 0 };
                let payload = &body[start..];
                let (status, extra) = if can_resume {
                    ("206 Partial Content", format!("Content-Range: bytes {}-{}/{BODY_SIZE}\r\n", start, BODY_SIZE - 1))
                } else {
                    ("200 OK", String::new())
                };
                let headers = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: {}\r\nAccept-Ranges: bytes\r\n{extra}Connection: close\r\n\r\n",
                    payload.len(), etag
                );
                if stream.write_all(headers.as_bytes()).is_err() { continue; }
                for chunk in payload.chunks(1024) {
                    if stream.write_all(chunk).is_err() { break; }
                    thread::sleep(Duration::from_millis(5));
                }
            }
        });
        (format!("http://{address}/download.bin"), version, shutdown, handle)
    }

    async fn run_until_pause(url: &str, target: &std::path::Path) -> (DownloadOutcome, watch::Sender<DownloadControl>) {
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let limiter = Arc::new(GlobalRateLimiter::new());
        let url = url.to_string();
        let target = target.to_path_buf();
        let task = tokio::spawn(async move {
            download_url_resumable(&url, &target, None, control_rx, limiter, progress_tx).await
        });
        tokio::time::sleep(Duration::from_millis(45)).await;
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        (outcome, control_tx)
    }

    #[tokio::test]
    async fn pause_resume_uses_byte_range_and_preserves_original_file_bytes() {
        let (url, _version, shutdown, server) = spawn_range_server();
        let directory = std::env::temp_dir().join(format!("thunderload-resume-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let (outcome, _control) = run_until_pause(&url, &target).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        let part = partial_download_path(&target);
        let saved = tokio::fs::metadata(&part).await.unwrap().len();
        assert!(saved > 0 && saved < BODY_SIZE as u64);
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());

        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'A'; BODY_SIZE]);
        assert!(!tokio::fs::try_exists(&part).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn changed_etag_restarts_full_download_instead_of_joining_old_bytes() {
        let (url, version, shutdown, server) = spawn_range_server();
        let directory = std::env::temp_dir().join(format!("thunderload-etag-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("changed.bin")).await.unwrap();
        let (outcome, _control) = run_until_pause(&url, &target).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        version.store(2, Ordering::Relaxed);

        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'B'; BODY_SIZE]);
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

#[cfg(test)]
mod rate_limit_tests {
    use super::GlobalRateLimiter;

    #[tokio::test]
    async fn global_limiter_reserves_all_downloads_on_one_timeline() {
        let limiter = GlobalRateLimiter::new();
        limiter.set_limit(1_000_000);
        let first = limiter.reserve(500_000).await;
        let second = limiter.reserve(500_000).await;
        assert!(first.as_millis() >= 400);
        assert!(second > first);
        limiter.set_limit(0);
        assert_eq!(limiter.reserve(1_000_000).await, std::time::Duration::ZERO);
    }
}



#[cfg(test)]
mod resume_metadata_tests {
    use super::{resume_identity_matches, ResumeMetadata};

    #[test]
    fn resume_refuses_a_changed_url_or_etag() {
        let saved = ResumeMetadata {
            url: "https://example.test/file.bin".into(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            total: Some(1000),
        };
        assert!(resume_identity_matches(&saved, "https://example.test/file.bin", Some("\"v1\""), None, Some(1000)));
        assert!(!resume_identity_matches(&saved, "https://example.test/other.bin", Some("\"v1\""), None, Some(1000)));
        assert!(!resume_identity_matches(&saved, "https://example.test/file.bin", Some("\"v2\""), None, Some(1000)));
        assert!(!resume_identity_matches(&saved, "https://example.test/file.bin", Some("\"v1\""), None, Some(1200)));
    }

    #[test]
    fn last_modified_is_accepted_when_etag_is_not_available() {
        let saved = ResumeMetadata {
            url: "https://example.test/file.bin".into(),
            etag: None,
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            total: None,
        };
        assert!(resume_identity_matches(&saved, "https://example.test/file.bin", None, Some("Wed, 21 Oct 2015 07:28:00 GMT"), None));
        assert!(!resume_identity_matches(&saved, "https://example.test/file.bin", None, Some("Thu, 22 Oct 2015 07:28:00 GMT"), None));
    }
}

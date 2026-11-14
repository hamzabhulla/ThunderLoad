use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex}, time::Instant};
use tokio::{fs, io::{AsyncSeekExt, AsyncWriteExt, SeekFrom}, sync::{mpsc, Mutex as AsyncMutex}};
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

fn response_matches_validator(headers: &header::HeaderMap, validator: &str) -> bool {
    let name = if validator.trim_start().starts_with('"') {
        header::ETAG
    } else {
        header::LAST_MODIFIED
    };
    headers.get(name)
        .and_then(|value| value.to_str().ok())
        .map(|actual| actual == validator)
        .unwrap_or(true)
}

fn parse_probe_content_range(value: &str) -> Option<u64> {
    value.strip_prefix("bytes 0-0/")?.parse::<u64>().ok().filter(|total| *total >= 1)
}

#[derive(Debug, Clone)]
struct RangeProbe {
    total: u64,
    if_range: Option<String>,
}

/// Some servers support byte ranges but omit Accept-Ranges on HEAD. Probe a
/// single byte before falling back to one connection; only trust a strict 206.
async fn probe_range_support(client: &Client, url: &str, referer: Option<&str>) -> Option<RangeProbe> {
    let mut request = client.get(url).header(header::RANGE, "bytes=0-0");
    if let Some(referrer) = referer {
        request = request.header(header::REFERER, referrer);
    }
    let response = request.send().await.ok()?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return None;
    }
    let content_range = response.headers().get(header::CONTENT_RANGE)?.to_str().ok()?;
    let total = parse_probe_content_range(content_range)?;
    if response.content_length().is_some_and(|length| length != 1) {
        return None;
    }
    let etag = response.headers().get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim_start().starts_with("W/"))
        .map(str::to_owned);
    let modified = response.headers().get(header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    // Drain the single probe byte instead of dropping the response with an
    // unread body: a connection abandoned mid-body can never go idle
    // cleanly, and hyper reports buffered bytes on such a connection as
    // "received unexpected message from connection" on the next send using
    // that client. Draining also lets the default pooled client reuse the
    // probe connection. A drain failure changes nothing: the probe's
    // contract is the headers, which are already validated above.
    let _ = response.bytes().await;
    Some(RangeProbe {
        total,
        if_range: etag.or(modified),
    })
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
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    let target = next_available_path(target).await?;
    let result = async {
        let connections = connections.clamp(1, 16);
        let client = client()?;
        let mut head_request = client.head(url);
        if let Some(referrer) = referer { head_request = head_request.header(header::REFERER, referrer); }
        let head = head_request.send().await.ok();
        // Read the total from the Content-Length header, NOT from
        // response.content_length(): for a HEAD response hyper reports the
        // body size, which is always 0 (a HEAD response has no body), so
        // using content_length() here would make the segmented path below
        // unreachable and every download fall back to sequential.
        let mut total = head.as_ref().filter(|response| response.status().is_success())
            .and_then(|response| response.headers().get(header::CONTENT_LENGTH))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let mut supports_ranges = head.as_ref().filter(|response| response.status().is_success()).map(|response| {
            response.headers().get(header::ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false)
        }).unwrap_or(false);
        let mut if_range = head.as_ref().and_then(|response| {
            let etag = response.headers().get(header::ETAG)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.trim_start().starts_with("W/"));
            let modified = response.headers().get(header::LAST_MODIFIED)
                .and_then(|value| value.to_str().ok());
            etag.or(modified).map(str::to_owned)
        });

        // Probe when HEAD omits Accept-Ranges (common on CDN/object-storage
        // endpoints). A valid one-byte 206 is stronger evidence than the hint.
        if connections > 1 && (!supports_ranges || total.is_none()) {
            if let Some(probe) = probe_range_support(&client, url, referer).await {
                total = Some(probe.total);
                if if_range.is_none() { if_range = probe.if_range; }
                supports_ranges = true;
            }
        }

        if supports_ranges && total.unwrap_or(0) >= 2 * 1024 * 1024 && connections > 1 {
            match download_segmented(&client, url, &target, total.unwrap(), connections.min(16), referer, if_range.as_deref(), progress_tx.clone(), rate_limiter.clone()).await {
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
        download_sequential(&client, url, &target, total, referer, progress_tx, rate_limiter).await
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
    rate_limiter: Arc<GlobalRateLimiter>,
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
            let wait = rate_limiter.reserve(chunk.len() as u64).await;
            if !wait.is_zero() { tokio::time::sleep(wait).await; }
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
    if_range: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    prepare_parent(target).await?;
    let work_dir = target.parent().unwrap().join(format!(".thunderload-{}", Uuid::new_v4()));
    fs::create_dir_all(&work_dir).await?;
    let connection_count = connections.clamp(1, 16) as u64;
    let chunk_size = total / connection_count + u64::from(total % connection_count != 0);
    let received = Arc::new(AtomicU64::new(0));
    let last_report = Arc::new(Mutex::new(Instant::now()));
    let mut tasks = tokio::task::JoinSet::new();

    for index in 0..connections {
        let start = index as u64 * chunk_size;
        if start >= total { break; }
        let end = start.saturating_add(chunk_size - 1).min(total - 1);
        let part_path = work_dir.join(format!("part-{index:03}"));
        let client = client.clone();
        let url = url.to_owned();
        let received = received.clone();
        let last_report = last_report.clone();
        let referrer = referer.map(str::to_string);
        let validator = if_range.map(str::to_string);
        let progress_tx = progress_tx.clone();
        let rate_limiter = rate_limiter.clone();
        tasks.spawn(async move {
            let mut request = client.get(&url).header(header::RANGE, format!("bytes={start}-{end}"));
            if let Some(referrer) = referrer { request = request.header(header::REFERER, referrer); }
            if let Some(validator_value) = validator.as_deref() {
                request = request.header(header::IF_RANGE, validator_value);
            }
            let response = request.send().await?;
            if validator.as_deref().is_some_and(|expected| !response_matches_validator(response.headers(), expected)) {
                return Err(anyhow!("The media resource changed during the segmented download"));
            }
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
                let wait = rate_limiter.reserve(chunk.len() as u64).await;
                if !wait.is_zero() { tokio::time::sleep(wait).await; }
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
            Ok(Err(error)) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                let _ = fs::remove_dir_all(&work_dir).await;
                return Err(error);
            }
            Err(error) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                let _ = fs::remove_dir_all(&work_dir).await;
                return Err(error.into());
            }
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
    fn range_validator_rejects_a_changed_etag_or_last_modified() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::ETAG, reqwest::header::HeaderValue::from_static("\"v1\""));
        assert!(super::response_matches_validator(&headers, "\"v1\""));
        assert!(!super::response_matches_validator(&headers, "\"v2\""));
        headers.remove(reqwest::header::ETAG);
        headers.insert(reqwest::header::LAST_MODIFIED, reqwest::header::HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"));
        assert!(super::response_matches_validator(&headers, "Wed, 21 Oct 2015 07:28:00 GMT"));
        assert!(!super::response_matches_validator(&headers, "Thu, 22 Oct 2015 07:28:00 GMT"));
    }


    #[test]
    fn range_probe_parser_rejects_unknown_or_malformed_totals() {
        assert_eq!(super::parse_probe_content_range("bytes 0-0/4096"), Some(4096));
        assert_eq!(super::parse_probe_content_range("bytes 0-0/*"), None);
        assert_eq!(super::parse_probe_content_range("bytes 1-1/4096"), None);
        assert_eq!(super::parse_probe_content_range("bytes 0-0/0"), None);
        assert_eq!(super::parse_probe_content_range("garbage"), None);
    }

    #[test]
    fn segmented_chunk_boundaries_cover_the_file_without_overflow() {
        fn ranges(total: u64, connections: u64) -> Vec<(u64, u64)> {
            let count = connections.clamp(1, 16);
            let chunk = total / count + u64::from(total % count != 0);
            (0..count).filter_map(|index| {
                let start = index * chunk;
                if start >= total { return None; }
                Some((start, start.saturating_add(chunk - 1).min(total - 1)))
            }).collect()
        }
        assert_eq!(ranges(10, 3), vec![(0, 3), (4, 7), (8, 9)]);
        assert_eq!(ranges(2, 16), vec![(0, 0), (1, 1)]);
        assert_eq!(ranges(u64::MAX, 2), vec![(0, u64::MAX / 2), (u64::MAX / 2 + 1, u64::MAX - 1)]);
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
    // Segmented checkpoints only. Absent (None) means the checkpoint was
    // written by the sequential path — or by an older build — so resume
    // stays sequential and the old metadata still parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    connections: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segments: Option<Vec<SegmentState>>,
}

/// One byte range of a segmented checkpoint. A segment is only ever written
/// linearly from its start and is marked done after its full range is
/// written and flushed, so a partially written segment is simply
/// re-downloaded whole on resume — there is no torn-write ambiguity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SegmentState {
    start: u64,
    end: u64,
    done: bool,
}

/// Deterministic segment layout shared by the fast and the checkpointed
/// segmented paths: the same (total, connections) always yields the same
/// ranges, so a checkpoint written by one call is readable by the next.
fn segment_ranges(total: u64, connections: usize) -> Vec<(u64, u64)> {
    let count = connections.clamp(1, 16) as u64;
    let chunk_size = total / count + u64::from(total % count != 0);
    (0..count)
        .filter_map(|index| {
            let start = index * chunk_size;
            if start >= total {
                return None;
            }
            Some((start, start.saturating_add(chunk_size - 1).min(total - 1)))
        })
        .collect()
}

/// What a HEAD (plus a one-byte probe when HEAD is silent) tells us about
/// the remote object. Fetched once per resumable call and shared by the
/// segmented and sequential paths, so the sequential path no longer issues
/// its own HEAD.
struct ServerInfo {
    total: Option<u64>,
    supports_ranges: bool,
    etag: Option<String>,
    last_modified: Option<String>,
}

async fn fetch_server_info(
    client: &Client,
    url: &str,
    referer: Option<&str>,
    probe: bool,
) -> ServerInfo {
    let mut head_request = client.head(url);
    if let Some(referrer) = referer {
        head_request = head_request.header(header::REFERER, referrer);
    }
    let head = head_request.send().await.ok();
    // Read the total from the Content-Length header, NOT from
    // response.content_length(): for a HEAD response hyper reports the body
    // size, which is always 0, so content_length() would report a total of 0.
    let mut total = head
        .as_ref()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.headers().get(header::CONTENT_LENGTH))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let mut supports_ranges = head
        .as_ref()
        .filter(|response| response.status().is_success())
        .map(|response| {
            response
                .headers()
                .get(header::ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let etag = head
        .as_ref()
        .and_then(|response| response.headers().get(header::ETAG))
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim_start().starts_with("W/"))
        .map(str::to_owned);
    let last_modified = head
        .as_ref()
        .and_then(|response| response.headers().get(header::LAST_MODIFIED))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut etag = etag;
    let mut last_modified = last_modified;
    // Probe when HEAD omits Accept-Ranges or the total (common on
    // CDN/object-storage endpoints). A valid one-byte 206 is stronger
    // evidence than the hint. A validator found only by the probe is
    // classified by shape (quoted = ETag, otherwise Last-Modified) so the
    // fresh checkpoint still carries it for If-Range.
    if probe && (!supports_ranges || total.is_none()) {
        if let Some(found) = probe_range_support(client, url, referer).await {
            total = Some(found.total);
            if etag.is_none() && last_modified.is_none() {
                if let Some(value) = found.if_range {
                    if value.trim_start().starts_with('"') {
                        etag = Some(value);
                    } else {
                        last_modified = Some(value);
                    }
                }
            }
            supports_ranges = true;
        }
    }
    ServerInfo { total, supports_ranges, etag, last_modified }
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

/// Whether the stored segment layout is exactly the layout for the stored
/// (total, connections). A mismatch means the checkpoint is corrupt or was
/// written by a different layout, so it must be discarded, never trusted.
fn segmented_layout_matches(saved: &ResumeMetadata) -> bool {
    let Some(total) = saved.total else { return false };
    let Some(connections) = saved.connections else { return false };
    let Some(segments) = saved.segments.as_ref() else { return false };
    let ranges = segment_ranges(total, connections);
    ranges.len() == segments.len()
        && ranges
            .iter()
            .zip(segments.iter())
            .all(|((start, end), state)| state.start == *start && state.end == *end)
}

/// Mutable segmented checkpoint shared by the segment tasks: the metadata
/// (with per-segment completion) plus the path it is atomically written to.
/// A segment is marked done only after its full range is written and
/// flushed, so the on-disk state is always a valid resume point.
struct SegmentCheckpoint {
    metadata_path: PathBuf,
    metadata: ResumeMetadata,
}

async fn mark_segment_done(checkpoint: &Arc<AsyncMutex<SegmentCheckpoint>>, index: usize) -> Result<()> {
    let mut guard = checkpoint.lock().await;
    if let Some(segments) = guard.metadata.segments.as_mut() {
        if let Some(state) = segments.get_mut(index) {
            state.done = true;
        }
    }
    write_resume_metadata(&guard.metadata_path, &guard.metadata).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentOutcome {
    Completed,
    Paused,
    Cancelled,
    ValidatorChanged,
}

enum SegmentedRun {
    Finished(DownloadOutcome),
    ValidatorChanged,
}

/// One segment of a checkpointed segmented download: streams its byte
/// range into the shared preallocated partial file at its own offset.
/// Each task owns its own file handle and seeks to its own range, so
/// concurrent tasks never share a cursor and ranges never overlap.
#[allow(clippy::too_many_arguments)]
async fn download_segment_range(
    client: &Client,
    url: &str,
    part: &Path,
    start: u64,
    end: u64,
    total: u64,
    validator: Option<&str>,
    referer: Option<&str>,
    mut control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    received: Arc<AtomicU64>,
    last_report: Arc<Mutex<Instant>>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<SegmentOutcome> {
    let mut request = client
        .get(url)
        .header(header::RANGE, format!("bytes={start}-{end}"));
    if let Some(referrer) = referer {
        request = request.header(header::REFERER, referrer);
    }
    if let Some(validator_value) = validator {
        request = request.header(header::IF_RANGE, validator_value);
    }
    let response = request.send().await?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        // A 200 here means If-Range failed: the object changed mid-flight.
        // The coordinator discards the checkpoint and restarts cleanly
        // instead of joining two versions of the file.
        return Ok(SegmentOutcome::ValidatorChanged);
    }
    if validator.is_some_and(|expected| !response_matches_validator(response.headers(), expected)) {
        return Ok(SegmentOutcome::ValidatorChanged);
    }
    let actual_range = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow!("Server omitted the Content-Range header"))?;
    validate_content_range(actual_range, start, end, total)?;
    if response.content_length().is_some_and(|length| length != end - start + 1) {
        return Err(anyhow!("Server returned a range with an unexpected content length"));
    }
    let mut file = fs::OpenOptions::new().write(true).open(part).await?;
    file.seek(SeekFrom::Start(start)).await?;
    let mut stream = response.bytes_stream();
    let mut local_bytes = 0_u64;
    let segment_len = end - start + 1;
    loop {
        if *control.borrow() == DownloadControl::Paused {
            file.flush().await?;
            return Ok(SegmentOutcome::Paused);
        }
        if *control.borrow() == DownloadControl::Cancelled {
            return Ok(SegmentOutcome::Cancelled);
        }
        let next = tokio::select! {
            value = stream.next() => value,
            changed = control.changed() => {
                if changed.is_ok() { continue; }
                None
            }
        };
        let Some(chunk_result) = next else { break };
        let chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(error) => {
                let _ = file.flush().await;
                return Err(error)
                    .context("Network interrupted; segment checkpoint has been preserved for Resume");
            }
        };
        file.write_all(&chunk).await?;
        local_bytes += chunk.len() as u64;
        let wait = rate_limiter.reserve(chunk.len() as u64).await;
        if !wait.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                changed = control.changed() => { if changed.is_err() { /* host is shutting down */ } }
            }
        }
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
    if local_bytes != segment_len {
        return Err(anyhow!("A range returned an incomplete segment"));
    }
    Ok(SegmentOutcome::Completed)
}

/// Runs the unfinished segments of a checkpointed segmented download.
/// Completed segments are skipped; a partially written segment is
/// re-downloaded whole. Network and server errors leave the checkpoint on
/// disk so the next call resumes from the completed segments.
#[allow(clippy::too_many_arguments)]
async fn run_segmented_download(
    client: &Client,
    url: &str,
    target: &Path,
    part: &Path,
    checkpoint: &Arc<AsyncMutex<SegmentCheckpoint>>,
    total: u64,
    validator: Option<String>,
    referer: Option<&str>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<SegmentedRun> {
    let ranges: Vec<(u64, u64, bool)> = {
        let guard = checkpoint.lock().await;
        guard
            .metadata
            .segments
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|state| (state.start, state.end, state.done))
            .collect()
    };
    let downloaded: u64 = ranges
        .iter()
        .filter(|(_, _, done)| *done)
        .map(|(start, end, _)| end - start + 1)
        .sum();
    let received = Arc::new(AtomicU64::new(downloaded));
    let last_report = Arc::new(Mutex::new(Instant::now()));
    let mut tasks = tokio::task::JoinSet::new();
    for (index, (start, end, done)) in ranges.into_iter().enumerate() {
        if done {
            continue;
        }
        let client = client.clone();
        let url = url.to_owned();
        let part = part.to_path_buf();
        let referrer = referer.map(str::to_string);
        let validator = validator.clone();
        let task_control = control.clone();
        let rate_limiter = rate_limiter.clone();
        let received = received.clone();
        let last_report = last_report.clone();
        let progress_tx = progress_tx.clone();
        let checkpoint = checkpoint.clone();
        tasks.spawn(async move {
            let outcome = download_segment_range(
                &client,
                &url,
                &part,
                start,
                end,
                total,
                validator.as_deref(),
                referrer.as_deref(),
                task_control,
                rate_limiter,
                received,
                last_report,
                progress_tx,
            )
            .await?;
            if outcome == SegmentOutcome::Completed {
                mark_segment_done(&checkpoint, index).await?;
            }
            Ok::<SegmentOutcome, anyhow::Error>(outcome)
        });
    }

    let mut saw_paused = false;
    let mut saw_cancelled = false;
    let mut saw_validator_changed = false;
    let mut first_error: Option<anyhow::Error> = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(SegmentOutcome::Completed)) => {}
            Ok(Ok(SegmentOutcome::Paused)) => saw_paused = true,
            Ok(Ok(SegmentOutcome::Cancelled)) => saw_cancelled = true,
            Ok(Ok(SegmentOutcome::ValidatorChanged)) => saw_validator_changed = true,
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
        // One bad segment decides the outcome for the whole download; stop
        // the rest instead of letting them write into a doomed checkpoint.
        // Pause needs no abort: every task notices the paused control at
        // its next chunk boundary and stops on its own.
        if saw_validator_changed || saw_cancelled || first_error.is_some() {
            tasks.abort_all();
        }
    }
    // Drain aborted tasks so no segment keeps writing after we return.
    while tasks.join_next().await.is_some() {}

    if saw_validator_changed {
        return Ok(SegmentedRun::ValidatorChanged);
    }
    if saw_cancelled {
        remove_resume_files(
            part,
            &checkpoint.lock().await.metadata_path,
            target,
        )
        .await;
        return Ok(SegmentedRun::Finished(DownloadOutcome::Cancelled));
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if saw_paused {
        // Persist the final completion state (completions are already
        // written as segments finish; this covers a task that finished
        // between its last mark and the abort of its siblings).
        let guard = checkpoint.lock().await;
        write_resume_metadata(&guard.metadata_path, &guard.metadata).await?;
        let downloaded = received.load(Ordering::Relaxed);
        let _ = progress_tx.send(DownloadProgress {
            downloaded,
            total: Some(total),
            percent: Some(downloaded as f64 * 100.0 / total as f64),
            phase: "paused".into(),
        });
        return Ok(SegmentedRun::Finished(DownloadOutcome::Paused));
    }
    let size = fs::metadata(part).await.map(|meta| meta.len()).unwrap_or(0);
    if size != total {
        return Err(anyhow!(
            "Segmented download ended at {size} bytes, but the server reported {total}. Partial data is preserved; Resume can retry."
        ));
    }
    replace_file(part, target).await?;
    let metadata_path = checkpoint.lock().await.metadata_path.clone();
    let _ = fs::remove_file(&metadata_path).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: total,
        total: Some(total),
        percent: Some(100.0),
        phase: "complete".into(),
    });
    Ok(SegmentedRun::Finished(DownloadOutcome::Completed))
}

fn fresh_segmented_metadata(
    url: &str,
    total: u64,
    connections: usize,
    server: &ServerInfo,
) -> ResumeMetadata {
    let segments = segment_ranges(total, connections)
        .into_iter()
        .map(|(start, end)| SegmentState { start, end, done: false })
        .collect();
    ResumeMetadata {
        url: url.to_owned(),
        etag: server.etag.clone(),
        last_modified: server.last_modified.clone(),
        total: Some(total),
        connections: Some(connections),
        segments: Some(segments),
    }
}

/// Creates the partial file if missing and sizes it to the full object
/// without truncating existing bytes: a resume keeps the completed
/// segments already on disk, and a fresh start gets a sparse file whose
/// gaps are overwritten before completion is ever claimed.
async fn ensure_segmented_part(part: &Path, total: u64) -> Result<()> {
    let file = fs::OpenOptions::new().write(true).create(true).open(part).await?;
    file.set_len(total).await?;
    Ok(())
}

/// Runs a segmented checkpoint, allowing exactly one clean restart if a
/// segment discovers mid-flight that the object changed. A second change
/// is a real error, not a race to chase.
#[allow(clippy::too_many_arguments)]
async fn run_segmented_with_restart(
    client: &Client,
    url: &str,
    target: &Path,
    part: &Path,
    metadata_path: &Path,
    mut metadata: ResumeMetadata,
    server: &ServerInfo,
    referer: Option<&str>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    let total = metadata.total.unwrap_or(0);
    let connections = metadata
        .connections
        .unwrap_or(DEFAULT_RESUMABLE_CONNECTIONS);
    for attempt in 0..2 {
        let validator = metadata
            .etag
            .clone()
            .or(metadata.last_modified.clone());
        let checkpoint = Arc::new(AsyncMutex::new(SegmentCheckpoint {
            metadata_path: metadata_path.to_path_buf(),
            metadata: metadata.clone(),
        }));
        match run_segmented_download(
            client,
            url,
            target,
            part,
            &checkpoint,
            total,
            validator,
            referer,
            control.clone(),
            rate_limiter.clone(),
            progress_tx.clone(),
        )
        .await?
        {
            SegmentedRun::Finished(outcome) => return Ok(outcome),
            SegmentedRun::ValidatorChanged if attempt == 0 => {
                remove_resume_files(part, metadata_path, target).await;
                metadata = fresh_segmented_metadata(url, total, connections, server);
                ensure_segmented_part(part, total).await?;
                write_resume_metadata(metadata_path, &metadata).await?;
            }
            SegmentedRun::ValidatorChanged => {
                return Err(anyhow!(
                    "The download changed on the server while resuming; restart it to try again"
                ));
            }
        }
    }
    Err(anyhow!(
        "The download changed on the server while resuming; restart it to try again"
    ))
}

/// Resumes a segmented checkpoint: the part file is extended to the full
/// size if a crash truncated it, then only the unfinished segments are
/// re-requested.
#[allow(clippy::too_many_arguments)]
async fn download_segmented_resumable(
    client: &Client,
    url: &str,
    target: &Path,
    part: &Path,
    metadata_path: &Path,
    saved: ResumeMetadata,
    server: &ServerInfo,
    referer: Option<&str>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    let Some(total) = saved.total else {
        return Err(anyhow!(
            "Segmented checkpoint is missing the total size; restart the download"
        ));
    };
    ensure_segmented_part(part, total).await?;
    run_segmented_with_restart(
        client,
        url,
        target,
        part,
        metadata_path,
        saved,
        server,
        referer,
        control,
        rate_limiter,
        progress_tx,
    )
    .await
}

/// Starts a fresh segmented checkpointed download.
#[allow(clippy::too_many_arguments)]
async fn download_segmented_resumable_fresh(
    client: &Client,
    url: &str,
    target: &Path,
    part: &Path,
    metadata_path: &Path,
    total: u64,
    connections: usize,
    server: &ServerInfo,
    referer: Option<&str>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    let metadata = fresh_segmented_metadata(url, total, connections, server);
    ensure_segmented_part(part, total).await?;
    write_resume_metadata(metadata_path, &metadata).await?;
    run_segmented_with_restart(
        client,
        url,
        target,
        part,
        metadata_path,
        metadata,
        server,
        referer,
        control,
        rate_limiter,
        progress_tx,
    )
    .await
}

/// Checkpointed transfer used by the desktop queue. When the server
/// supports byte ranges and the object is at least 2 MiB, the transfer runs
/// in parallel segments against one preallocated partial file, with
/// per-segment completion persisted in the metadata sidecar — so pause,
/// resume and restart recovery all work for multi-connection downloads.
/// Otherwise it falls back to the sequential checkpointed path below, which
/// writes one continuous partial file. Either way the checkpoint is a
/// stable `.part` file plus its `.meta.json` sidecar, so a fresh call with
/// the same target picks the download up from disk.
pub async fn download_url_resumable(
    url: &str,
    target: &Path,
    referer: Option<&str>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    download_url_resumable_with_connections(
        url,
        target,
        referer,
        DEFAULT_RESUMABLE_CONNECTIONS,
        control,
        rate_limiter,
        progress_tx,
    )
    .await
}

/// Connections used by `download_url_resumable` when the caller does not
/// choose: 8 matches the segmented fast path's usual setting and keeps the
/// segment count small enough that per-segment checkpoint writes stay cheap.
const DEFAULT_RESUMABLE_CONNECTIONS: usize = 8;

/// Minimum object size for the segmented checkpointed path, mirroring the
/// fast path's threshold in `download_url`: segmenting a small object buys
/// nothing and costs one checkpoint write per segment.
const MIN_SEGMENTED_TOTAL: u64 = 2 * 1024 * 1024;

/// Same as `download_url_resumable`, with an explicit connection count.
/// The desktop queue wires its setting through here in a later slice; the
/// signature of `download_url_resumable` itself is unchanged.
pub async fn download_url_resumable_with_connections(
    url: &str,
    target: &Path,
    referer: Option<&str>,
    connections: usize,
    control: tokio::sync::watch::Receiver<DownloadControl>,
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
    // One HEAD (+ a one-byte probe when HEAD is silent) for the whole call:
    // the segmented decision, the identity check and the sequential path all
    // share it, so the sequential path no longer issues its own HEAD.
    // A sequential checkpoint never segments, so skip the probe there.
    let probe = saved_metadata
        .as_ref()
        .map(|saved| saved.segments.is_some())
        .unwrap_or(true);
    let server = fetch_server_info(&client, url, referer, probe).await;

    // A segmented checkpoint resumes in parallel — but only when the server
    // still reports the same object, still supports ranges, and the stored
    // layout matches the layout for (total, connections). Anything else
    // discards the checkpoint and falls through to a fresh decision, so a
    // changed or truncated remote object can never be joined onto old bytes.
    if let Some(saved) = saved_metadata.clone() {
        if saved.segments.is_some() {
            let still_same_resource = resume_identity_matches(
                &saved,
                url,
                server.etag.as_deref(),
                server.last_modified.as_deref(),
                server.total,
            );
            if still_same_resource && server.supports_ranges && segmented_layout_matches(&saved) {
                return download_segmented_resumable(
                    &client,
                    url,
                    target,
                    &part,
                    &metadata_path,
                    saved,
                    &server,
                    referer,
                    control,
                    rate_limiter,
                    progress_tx,
                )
                .await;
            }
            let _ = fs::remove_file(&part).await;
            let _ = fs::remove_file(&metadata_path).await;
            offset = 0;
            saved_metadata = None;
        }
    }

    // Fresh decision: parallel segments when the server supports ranges and
    // the object is large enough; otherwise the sequential path below.
    if offset == 0 && saved_metadata.is_none() {
        if let Some(total) = server.total {
            if server.supports_ranges && total >= MIN_SEGMENTED_TOTAL {
                return download_segmented_resumable_fresh(
                    &client,
                    url,
                    target,
                    &part,
                    &metadata_path,
                    total,
                    connections,
                    &server,
                    referer,
                    control,
                    rate_limiter,
                    progress_tx,
                )
                .await;
            }
        }
    }

    download_sequential_resumable(
        url,
        target,
        referer,
        control,
        rate_limiter,
        progress_tx,
        part,
        metadata_path,
        offset,
        saved_metadata,
        &server,
    )
    .await
}

/// Sequential, checkpointed transfer: one continuous partial file, used when
/// the server does not support byte ranges, the object is small, or a
/// sequential checkpoint already exists. Pause/resume is deterministic,
/// including after an application restart.
#[allow(clippy::too_many_arguments)]
async fn download_sequential_resumable(
    url: &str,
    target: &Path,
    referer: Option<&str>,
    mut control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    part: PathBuf,
    metadata_path: PathBuf,
    offset: u64,
    mut saved_metadata: Option<ResumeMetadata>,
    server: &ServerInfo,
) -> Result<DownloadOutcome> {
    let client = client()?;
    // The total and validators come from the shared ServerInfo fetched by
    // the dispatcher (Content-Length header, not content_length(): a HEAD
    // response reports a body size of 0). The GET below still supplies its
    // own Content-Length when present.
    let server_total = server.total;
    let head_etag = server.etag.clone();
    let head_modified = server.last_modified.clone();

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
        // Sequential checkpoint: no segment layout.
        connections: None,
        segments: None,
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

    const BODY_SIZE: usize = 512 * 1024;

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
                    thread::sleep(Duration::from_millis(12));
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
        let task_target = target.clone();
        let task = tokio::spawn(async move {
            download_url_resumable(&url, &task_target, None, control_rx, limiter, progress_tx).await
        });
        let part = partial_download_path(&target);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let written = tokio::fs::metadata(&part).await.map(|metadata| metadata.len()).unwrap_or(0);
            if written > 0 { break; }
            assert!(tokio::time::Instant::now() < deadline, "download never wrote its first checkpoint bytes");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Leave enough time for a partial body, but never depend on an exact CI timing.
        tokio::time::sleep(Duration::from_millis(25)).await;
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
            connections: None,
            segments: None,
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
            connections: None,
            segments: None,
        };
        assert!(resume_identity_matches(&saved, "https://example.test/file.bin", None, Some("Wed, 21 Oct 2015 07:28:00 GMT"), None));
        assert!(!resume_identity_matches(&saved, "https://example.test/file.bin", None, Some("Thu, 22 Oct 2015 07:28:00 GMT"), None));
    }
}

#[cfg(test)]
mod segmented_http_tests {
    use super::{
        client, download_segmented, download_url, probe_range_support, DownloadProgress,
        GlobalRateLimiter,
    };
    use reqwest::Client;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc},
        thread,
        time::Duration,
    };
    use tokio::sync::mpsc;

    const BODY_SIZE: usize = 2 * 1024 * 1024 + 37;

    fn spawn_segment_server() -> (String, Arc<AtomicUsize>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let range_requests = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let requests = range_requests.clone();
        let stop = shutdown.clone();
        let body = Arc::new((0..BODY_SIZE).map(|index| (index % 251) as u8).collect::<Vec<u8>>());
        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                // Accepted sockets inherit the listener's non-blocking mode
                // on Windows: serve this connection blocking, so a request
                // that has not arrived yet is waited for, not misread as
                // an empty request.
                let _ = stream.set_nonblocking(false);
                let request_body = body.clone();
                let request_counter = requests.clone();
                thread::spawn(move || {
                    // Serve sequential requests on this connection (HTTP/1.1
                    // keep-alive) until the client closes it. The old
                    // fixture answered after a single read and closed the
                    // socket; on Windows that read could return WouldBlock
                    // and be misread as an empty request, so the server
                    // spoke before the client had sent anything — hyper
                    // reports such unsolicited bytes as "received unexpected
                    // message from connection", the exact Windows CI error.
                    loop {
                        let mut raw = Vec::new();
                        let mut buffer = [0_u8; 1024];
                        let head_complete = loop {
                            match stream.read(&mut buffer) {
                                Ok(0) => break false,
                                Ok(count) => {
                                    raw.extend_from_slice(&buffer[..count]);
                                    if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                                        break true;
                                    }
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    thread::sleep(Duration::from_millis(1));
                                }
                                Err(_) => break false,
                            }
                        };
                        if !head_complete {
                            return;
                        }
                        let request = String::from_utf8_lossy(&raw);
                        let method = request.split_whitespace().next().unwrap_or("");
                        let range = request.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("range").then(|| value.trim().to_string())
                        });
                        let validator = request.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("if-range").then(|| value.trim().to_string())
                        });
                        if method == "HEAD" {
                            let headers = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nAccept-Ranges: bytes\r\nETag: \"v1\"\r\n\r\n"
                            );
                            if stream.write_all(headers.as_bytes()).is_err() {
                                return;
                            }
                            continue;
                        }
                        // Count each GET range request so the fixture can assert segmented requests reliably.
                        if method != "HEAD" && range.is_some() {
                            request_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        let bounds = range.as_deref()
                            .and_then(|value| value.strip_prefix("bytes="))
                            .and_then(|value| value.split_once('-'))
                            .and_then(|(start, end)| Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?)));
                        let mut response = Vec::new();
                        if let Some((start, end)) = bounds.filter(|(start, end)| {
                            *start <= *end && *end < BODY_SIZE
                                && (validator.as_deref() == Some("\"v1\"")
                                    || (*start == 0 && *end == 0 && validator.is_none()))
                        }) {
                            let payload = &request_body[start..=end];
                            response.extend_from_slice(
                                format!(
                                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{BODY_SIZE}\r\nETag: \"v1\"\r\n\r\n",
                                    payload.len()
                                )
                                .as_bytes(),
                            );
                            response.extend_from_slice(payload);
                        } else {
                            response.extend_from_slice(
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nETag: \"v1\"\r\n\r\n"
                                )
                                .as_bytes(),
                            );
                            response.extend_from_slice(&request_body);
                        }
                        // One write for head + body: a split write followed
                        // by an immediate close can truncate the body.
                        if stream.write_all(&response).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://{address}/file.bin"), range_requests, shutdown, handle)
    }

    #[tokio::test]
    async fn multi_connection_download_probes_ranges_and_assembles_exact_bytes() {
        let (url, range_requests, shutdown, server) = spawn_segment_server();
        let directory = std::env::temp_dir().join(format!("thunderload-segment-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        // Disable idle pooling here so the test validates independent
        // concurrent ranges: each of the 8 chunk requests must open its
        // own connection instead of sharing one through the pool.
        let client = Client::builder()
            .user_agent("ThunderLoad-test")
            .http1_only()
            .pool_max_idle_per_host(0)
            .connect_timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let probe = probe_range_support(&client, &url, None).await.unwrap();
        assert_eq!(probe.total, BODY_SIZE as u64);
        assert_eq!(probe.if_range.as_deref(), Some("\"v1\""));
        let path = directory.join("payload.bin");
        let result = download_segmented(
            &client, &url, &path, probe.total, 8, None, probe.if_range.as_deref(), progress_tx,
            Arc::new(GlobalRateLimiter::new())
        ).await;
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        result.unwrap();
        // One probe request plus eight independent chunk requests.
        assert!(range_requests.load(Ordering::Relaxed) >= 9, "expected probe + 8 chunk Range requests; observed {}", range_requests.load(Ordering::Relaxed));
        let bytes = tokio::fs::read(&path).await.unwrap();
        let expected: Vec<u8> = (0..BODY_SIZE).map(|index| (index % 251) as u8).collect();
        assert_eq!(bytes, expected);
        let mut saw_complete = false;
        while let Ok(progress) = progress_rx.try_recv() {
            if progress.phase == "complete" && progress.percent == Some(100.0) { saw_complete = true; }
        }
        assert!(saw_complete);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn download_url_end_to_end_uses_head_and_segmented_ranges() {
        // Regression cover for the production path: the default pooled
        // client against the keep-alive fixture (HEAD, then 8 concurrent
        // ranges). The probe is skipped here because HEAD already
        // advertises ranges; probe_range_support itself is covered by the
        // test above.
        let (url, range_requests, shutdown, server) = spawn_segment_server();
        let directory = std::env::temp_dir().join(format!("thunderload-download-url-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let target = directory.join("payload.bin");
        let result = download_url(
            &url,
            &target,
            8,
            None,
            progress_tx,
            Arc::new(GlobalRateLimiter::new()),
        )
        .await;
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        let path = result.unwrap();
        assert_eq!(path, target);
        assert!(range_requests.load(Ordering::Relaxed) >= 8, "expected 8 chunk Range requests; observed {}", range_requests.load(Ordering::Relaxed));
        let bytes = tokio::fs::read(&path).await.unwrap();
        let expected: Vec<u8> = (0..BODY_SIZE).map(|index| (index % 251) as u8).collect();
        assert_eq!(bytes, expected);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn probe_then_segmented_reuses_pooled_connection() {
        // Guards the probe drain in probe_range_support on the pooled
        // path: the probe's connection must go idle cleanly so the
        // segmented phase can reuse this client (and its pooled
        // connection) without hyper reporting unexpected bytes.
        let (url, range_requests, shutdown, server) = spawn_segment_server();
        let directory = std::env::temp_dir().join(format!("thunderload-probe-pooled-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let pooled = client().unwrap();
        let probe = probe_range_support(&pooled, &url, None).await.unwrap();
        assert_eq!(probe.total, BODY_SIZE as u64);
        assert_eq!(probe.if_range.as_deref(), Some("\"v1\""));
        let path = directory.join("payload.bin");
        let result = download_segmented(
            &pooled, &url, &path, probe.total, 8, None, probe.if_range.as_deref(), progress_tx,
            Arc::new(GlobalRateLimiter::new()),
        )
        .await;
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        result.unwrap();
        // One probe request plus eight independent chunk requests.
        assert!(range_requests.load(Ordering::Relaxed) >= 9, "expected probe + 8 chunk Range requests; observed {}", range_requests.load(Ordering::Relaxed));
        let bytes = tokio::fs::read(&path).await.unwrap();
        let expected: Vec<u8> = (0..BODY_SIZE).map(|index| (index % 251) as u8).collect();
        assert_eq!(bytes, expected);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}


#[cfg(test)]
mod segmented_resumable_tests {
    use super::{
        download_url_resumable_with_connections, partial_download_metadata_path,
        partial_download_path, read_resume_metadata, reserve_download_path,
        write_resume_metadata, DownloadControl, DownloadOutcome, GlobalRateLimiter,
        ResumeMetadata, SegmentState,
    };
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc},
        thread,
        time::Duration,
    };
    use tokio::sync::{mpsc, watch};

    const SMALL_BODY: usize = 2 * 1024 * 1024 + 37;
    const LARGE_BODY: usize = 8 * 1024 * 1024;

    fn body_for(version: usize, size: usize) -> Vec<u8> {
        (0..size)
            .map(|index| {
                if version == 1 {
                    (index % 251) as u8
                } else {
                    ((index + 101) % 251) as u8
                }
            })
            .collect()
    }

    struct CheckpointFixture {
        url: String,
        version: Arc<AtomicUsize>,
        range_support: Arc<AtomicBool>,
        head_advertises_ranges: Arc<AtomicBool>,
        slow: Arc<AtomicBool>,
        range_requests: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
        handle: thread::JoinHandle<()>,
    }

    impl CheckpointFixture {
        fn stop(self) {
            self.shutdown.store(true, Ordering::Relaxed);
            self.handle.join().unwrap();
        }
    }

    fn spawn_checkpoint_server(body_size: usize) -> CheckpointFixture {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let version = Arc::new(AtomicUsize::new(1));
        let range_support = Arc::new(AtomicBool::new(true));
        let head_advertises_ranges = Arc::new(AtomicBool::new(true));
        let slow = Arc::new(AtomicBool::new(false));
        let range_requests = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let body_v1 = Arc::new(body_for(1, body_size));
        let body_v2 = Arc::new(body_for(2, body_size));
        let thread_version = version.clone();
        let thread_range_support = range_support.clone();
        let thread_head_advertises = head_advertises_ranges.clone();
        let thread_slow = slow.clone();
        let thread_requests = range_requests.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                // Accepted sockets inherit the listener's non-blocking mode
                // on Windows: serve this connection blocking.
                let _ = stream.set_nonblocking(false);
                let body_v1 = body_v1.clone();
                let body_v2 = body_v2.clone();
                let version = thread_version.clone();
                let range_support = thread_range_support.clone();
                let head_advertises = thread_head_advertises.clone();
                let slow = thread_slow.clone();
                let requests = thread_requests.clone();
                thread::spawn(move || {
                    loop {
                        let mut raw = Vec::new();
                        let mut buffer = [0_u8; 1024];
                        let head_complete = loop {
                            match stream.read(&mut buffer) {
                                Ok(0) => break false,
                                Ok(count) => {
                                    raw.extend_from_slice(&buffer[..count]);
                                    if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                                        break true;
                                    }
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    thread::sleep(Duration::from_millis(1));
                                }
                                Err(_) => break false,
                            }
                        };
                        if !head_complete {
                            return;
                        }
                        let request = String::from_utf8_lossy(&raw);
                        let method = request.split_whitespace().next().unwrap_or("");
                        let header = |name: &str| {
                            request.lines().find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case(name)
                                    .then(|| value.trim().to_string())
                            })
                        };
                        let range = header("range");
                        let if_range = header("if-range");
                        let current_version = version.load(Ordering::Relaxed);
                        let etag = if current_version == 1 { "\"v1\"" } else { "\"v2\"" };
                        let body = if current_version == 1 { &body_v1 } else { &body_v2 };
                        let ranges_on = range_support.load(Ordering::Relaxed);
                        if method == "HEAD" {
                            // HEAD may stay silent about ranges even when
                            // GET honors them (common on CDN/object-storage
                            // endpoints): the client probes in that case.
                            // It never advertises ranges the server does
                            // not honor.
                            let accept = if ranges_on && head_advertises.load(Ordering::Relaxed) { "Accept-Ranges: bytes\r\n" } else { "" };
                            let headers = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {body_size}\r\nETag: {etag}\r\n{accept}\r\n"
                            );
                            if stream.write_all(headers.as_bytes()).is_err() {
                                return;
                            }
                            continue;
                        }
                        if range.is_some() {
                            requests.fetch_add(1, Ordering::Relaxed);
                        }
                        let bounds = range.as_deref()
                            .and_then(|value| value.strip_prefix("bytes="))
                            .and_then(|value| value.split_once('-'))
                            .and_then(|(start, end)| {
                                Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                            });
                        // A mismatched If-Range means the object changed:
                        // answer the full body with 200, as a real server
                        // does, so the client must detect it and restart.
                        let honored = ranges_on
                            && bounds.is_some_and(|(start, end)| {
                                start <= end
                                    && end < body_size
                                    && (if_range.as_deref() == Some(etag) || if_range.is_none())
                            });
                        let (status, payload_range) = if honored {
                            let (start, end) = bounds.unwrap();
                            (
                                "206 Partial Content",
                                Some((start, end, format!("Content-Range: bytes {start}-{end}/{body_size}\r\n"))),
                            )
                        } else {
                            ("200 OK", None)
                        };
                        let (start, end) = payload_range
                            .as_ref()
                            .map(|(start, end, _)| (*start, *end))
                            .unwrap_or((0, body_size - 1));
                        let extra = payload_range
                            .as_ref()
                            .map(|(_, _, range)| range.clone())
                            .unwrap_or_default();
                        let payload_len = end - start + 1;
                        let headers = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {payload_len}\r\nETag: {etag}\r\n{extra}\r\n"
                        );
                        if stream.write_all(headers.as_bytes()).is_err() {
                            return;
                        }
                        let payload = &body[start..=end];
                        if slow.load(Ordering::Relaxed) {
                            for chunk in payload.chunks(16 * 1024) {
                                if stream.write_all(chunk).is_err() {
                                    return;
                                }
                                thread::sleep(Duration::from_millis(3));
                            }
                        } else if stream.write_all(payload).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        CheckpointFixture {
            url: format!("http://{address}/file.bin"),
            version,
            range_support,
            head_advertises_ranges,
            slow,
            range_requests,
            shutdown,
            handle,
        }
    }

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("thunderload-{prefix}-{}", uuid::Uuid::new_v4()))
    }

    /// Starts a segmented resumable download, pauses it on the first
    /// progress report that shows partial bytes, and returns the outcome.
    /// The slow-drip server guarantees the download is still in flight
    /// when the first progress report arrives (progress is reported every
    /// 180 ms; a segment takes ~400 ms to drip).
    async fn run_until_pause(
        url: &str,
        target: &std::path::Path,
        connections: usize,
    ) -> DownloadOutcome {
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let limiter = Arc::new(GlobalRateLimiter::new());
        let url = url.to_string();
        let target = target.to_path_buf();
        let task_target = target.clone();
        let task = tokio::spawn(async move {
            download_url_resumable_with_connections(
                &url,
                &task_target,
                None,
                connections,
                control_rx,
                limiter,
                progress_tx,
            )
            .await
        });
        let paused = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(progress) = progress_rx.recv().await {
                if progress.downloaded > 0 && progress.percent.is_some_and(|p| p < 100.0) {
                    break;
                }
            }
        })
        .await
        .expect("no partial progress report within 10s; download may have finished before pause");
        let _ = paused;
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        drop(control_tx);
        outcome
    }

    async fn resume(
        url: &str,
        target: &std::path::Path,
        connections: usize,
    ) -> DownloadOutcome {
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable_with_connections(
            url,
            target,
            None,
            connections,
            control_rx,
            Arc::new(GlobalRateLimiter::new()),
            progress_tx,
        )
        .await
        .unwrap();
        drop(control_tx);
        outcome
    }

    #[tokio::test]
    async fn segmented_resumable_download_completes_with_exact_bytes() {
        let fixture = spawn_checkpoint_server(SMALL_BODY);
        let directory = temp_dir("seg-resume-complete");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let outcome = resume(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert_eq!(
            fixture.range_requests.load(Ordering::Relaxed),
            4,
            "expected exactly 4 segment Range requests"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body_for(1, SMALL_BODY));
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn segmented_resumable_pause_then_resume_completes_with_exact_bytes() {
        let fixture = spawn_checkpoint_server(LARGE_BODY);
        fixture.slow.store(true, Ordering::Relaxed);
        let directory = temp_dir("seg-resume-pause");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let outcome = run_until_pause(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        let part = partial_download_path(&target);
        let part_len = tokio::fs::metadata(&part).await.unwrap().len();
        assert_eq!(part_len, LARGE_BODY as u64, "segmented part is preallocated to the full size");
        let metadata_path = partial_download_metadata_path(&target);
        let metadata = read_resume_metadata(&metadata_path).await.expect("checkpoint metadata missing");
        let segments = metadata.segments.expect("segmented checkpoint must list segments");
        assert_eq!(segments.len(), 4);
        assert_eq!(metadata.total, Some(LARGE_BODY as u64));

        fixture.slow.store(false, Ordering::Relaxed);
        let before = fixture.range_requests.load(Ordering::Relaxed);
        let outcome = resume(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert!(
            fixture.range_requests.load(Ordering::Relaxed) - before >= 1,
            "resume must re-request unfinished segments"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body_for(1, LARGE_BODY));
        assert!(!tokio::fs::try_exists(&part).await.unwrap());
        assert!(!tokio::fs::try_exists(&metadata_path).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn segmented_resumable_restart_recovers_from_disk_checkpoint() {
        // Simulated restart: no in-memory state at all — the checkpoint is
        // built by hand on disk, then a fresh call must resume from it,
        // requesting only the unfinished segments.
        let fixture = spawn_checkpoint_server(SMALL_BODY);
        let directory = temp_dir("seg-resume-restart");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let ranges = super::segment_ranges(SMALL_BODY as u64, 4);
        assert_eq!(ranges.len(), 4);
        let expected = body_for(1, SMALL_BODY);
        let part = partial_download_path(&target);
        {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&part)
                .await
                .unwrap();
            file.set_len(SMALL_BODY as u64).await.unwrap();
            for (index, (start, end)) in ranges.iter().enumerate() {
                if index == 0 || index == 2 {
                    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
                    let mut file = file.try_clone().await.unwrap();
                    file.seek(SeekFrom::Start(*start)).await.unwrap();
                    file.write_all(&expected[*start as usize..=*end as usize]).await.unwrap();
                    file.flush().await.unwrap();
                }
            }
        }
        let metadata = ResumeMetadata {
            url: fixture.url.clone(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            total: Some(SMALL_BODY as u64),
            connections: Some(4),
            segments: Some(
                ranges
                    .iter()
                    .enumerate()
                    .map(|(index, (start, end))| SegmentState {
                        start: *start,
                        end: *end,
                        done: index == 0 || index == 2,
                    })
                    .collect(),
            ),
        };
        let metadata_path = partial_download_metadata_path(&target);
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();

        let before = fixture.range_requests.load(Ordering::Relaxed);
        let outcome = resume(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert_eq!(
            fixture.range_requests.load(Ordering::Relaxed) - before,
            2,
            "resume must request only the 2 unfinished segments"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        assert!(!tokio::fs::try_exists(&part).await.unwrap());
        assert!(!tokio::fs::try_exists(&metadata_path).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn segmented_resumable_resumes_segmented_checkpoint_when_head_silent() {
        // Regression test for the inverted probe flag: HEAD omits
        // Accept-Ranges (common on CDN/object-storage endpoints) while
        // GET still honors ranges. Resume must probe, discover ranges,
        // and resume the segmented checkpoint — not discard it and
        // restart sequentially from byte zero.
        let fixture = spawn_checkpoint_server(SMALL_BODY);
        fixture.head_advertises_ranges.store(false, Ordering::Relaxed);
        let directory = temp_dir("seg-resume-head-silent");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let ranges = super::segment_ranges(SMALL_BODY as u64, 4);
        assert_eq!(ranges.len(), 4);
        let expected = body_for(1, SMALL_BODY);
        let part = partial_download_path(&target);
        {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&part)
                .await
                .unwrap();
            file.set_len(SMALL_BODY as u64).await.unwrap();
            for (index, (start, end)) in ranges.iter().enumerate() {
                if index == 0 || index == 2 {
                    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
                    let mut file = file.try_clone().await.unwrap();
                    file.seek(SeekFrom::Start(*start)).await.unwrap();
                    file.write_all(&expected[*start as usize..=*end as usize]).await.unwrap();
                    file.flush().await.unwrap();
                }
            }
        }
        let metadata = ResumeMetadata {
            url: fixture.url.clone(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            total: Some(SMALL_BODY as u64),
            connections: Some(4),
            segments: Some(
                ranges
                    .iter()
                    .enumerate()
                    .map(|(index, (start, end))| SegmentState {
                        start: *start,
                        end: *end,
                        done: index == 0 || index == 2,
                    })
                    .collect(),
            ),
        };
        let metadata_path = partial_download_metadata_path(&target);
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();

        let before = fixture.range_requests.load(Ordering::Relaxed);
        let outcome = resume(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert_eq!(
            fixture.range_requests.load(Ordering::Relaxed) - before,
            3,
            "resume must probe once, then request only the 2 unfinished segments"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        assert!(!tokio::fs::try_exists(&part).await.unwrap());
        assert!(!tokio::fs::try_exists(&metadata_path).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn segmented_resumable_falls_back_without_range_support() {
        let fixture = spawn_checkpoint_server(SMALL_BODY);
        fixture.range_support.store(false, Ordering::Relaxed);
        fixture.slow.store(true, Ordering::Relaxed);
        let directory = temp_dir("seg-resume-norange");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let outcome = run_until_pause(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        // The fallback used the sequential path: no segment layout.
        let metadata_path = partial_download_metadata_path(&target);
        let metadata = read_resume_metadata(&metadata_path).await.expect("checkpoint metadata missing");
        assert!(metadata.segments.is_none(), "no-range server must fall back to the sequential checkpoint");
        // Exactly one Range request: the one-byte probe that discovered
        // ranges are unsupported. The sequential download itself sent none
        // (offset was 0 on the first call).
        assert_eq!(fixture.range_requests.load(Ordering::Relaxed), 1);

        // Resume on a no-range server restarts from byte zero (the
        // sequential path cannot append without ranges) but still
        // completes with exact bytes.
        let outcome = resume(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body_for(1, SMALL_BODY));
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(&metadata_path).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn segmented_resumable_changed_etag_discards_checkpoint() {
        let fixture = spawn_checkpoint_server(LARGE_BODY);
        fixture.slow.store(true, Ordering::Relaxed);
        let directory = temp_dir("seg-resume-etag");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let outcome = run_until_pause(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        let part = partial_download_path(&target);
        assert!(tokio::fs::try_exists(&part).await.unwrap());

        // The object changed while paused: version 2 has a new ETag and
        // different bytes. Resume must discard the v1 checkpoint and
        // download the new version cleanly, never joining the two.
        fixture.version.store(2, Ordering::Relaxed);
        fixture.slow.store(false, Ordering::Relaxed);
        let before = fixture.range_requests.load(Ordering::Relaxed);
        let outcome = resume(&fixture.url, &target, 4).await;
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert_eq!(
            fixture.range_requests.load(Ordering::Relaxed) - before,
            4,
            "changed ETag must restart with a fresh set of 4 segment requests"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body_for(2, LARGE_BODY));
        assert!(!tokio::fs::try_exists(&part).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

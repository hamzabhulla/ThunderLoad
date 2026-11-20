use anyhow::{anyhow, Context, Result};
use crate::session::{apply_session, map_auth_error, SessionHeaders, AUTH_REQUIRED_MESSAGE};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, path::{Path, PathBuf}, sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex}, time::{Duration, Instant}};
use tokio::{fs, io::{AsyncSeekExt, AsyncWriteExt, SeekFrom}, sync::{mpsc, Mutex as AsyncMutex}, task::{AbortHandle, JoinHandle}};
use uuid::Uuid;

/// Returned when a URL turns out to be a webpage, not a direct media file.
/// Single source of truth for this wording: `inspect_video` in video.rs
/// reports the same message, so the desktop shows one consistent error
/// whether the URL was inspected first or pasted straight into a download.
pub(crate) const WEBPAGE_NOT_MEDIA_MESSAGE: &str = "This URL is a webpage, not a direct media file. Paste an MP4/audio URL or an HLS playlist. Page extraction is not available for this source.";

/// Whether a response is a webpage: `text/html`, ignoring any `; charset=…`
/// suffix (and case). Checked on the GET response before any bytes are
/// written, so a pasted watch-page URL fails fast instead of saving HTML.
fn response_is_webpage(headers: &header::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or("").trim().eq_ignore_ascii_case("text/html"))
        .unwrap_or(false)
}

/// Progress on the live channel, emitted by the engine and forwarded to
/// the UI. `segments_done`/`segments_total` (Slice 7b, Gemini claim 14)
/// report per-segment progress for segmented transfers — direct
/// multi-connection files, HLS and DASH — and are `None` when the
/// transfer does not report segments (sequential downloads, live
/// recording totals). The UI shows real segment data when present and
/// an honest "not reported" only when both are `None`.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadProgress {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub percent: Option<f64>,
    pub phase: String,
    pub segments_done: Option<u64>,
    pub segments_total: Option<u64>,
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
async fn probe_range_support(client: &Client, url: &str, session: Option<&SessionHeaders>) -> Option<RangeProbe> {
    let request = apply_session(session, client.get(url).header(header::RANGE, "bytes=0-0"), url);
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
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(READ_TIMEOUT)
        .redirect(crate::session::validated_redirect_policy())
        .dns_resolver(std::sync::Arc::new(
            crate::session::ValidatingResolver::from_env(),
        ))
        .pool_max_idle_per_host(16)
        .build()?)
}

pub async fn download_url(
    url: &str,
    target: &Path,
    connections: usize,
    session: Option<&SessionHeaders>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
) -> Result<PathBuf> {
    crate::session::validate_public_http_url(url)?;
    let target = next_available_path(target).await?;
    let result = async {
        let connections = connections.clamp(1, 16);
        let client = client()?;
        let head_request = client.head(url);
        let head_request = apply_session(session, head_request, url);
        let head = head_request.send().await.ok();
        // F1: fail fast on a webpage HEAD, before the segmented path can
        // save the page as a file. A HEAD that omits Content-Type is
        // still caught: the sequential fallback checks the GET response,
        // and the segmented tasks check each segment response.
        if head
            .as_ref()
            .filter(|response| response.status().is_success())
            .is_some_and(|response| response_is_webpage(response.headers()))
        {
            return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
        }
        // Read the total from the Content-Length header, NOT from
        // response.content_length(): for a HEAD response hyper reports the
        // body size, which is always 0 (a HEAD response has no body), so
        // using content_length() here would make the segmented path below
        // unreachable and every download fall back to sequential.
        // A Content-Length of 0 means "unknown", not a 0-byte object:
        // chunked servers (notably YouTube watch pages) report 0 on HEAD
        // while streaming a body on GET. Treating it as Some(0) would make
        // the sequential completion check fail a finished transfer, and
        // would claim a 0-byte total in progress reports.
        let mut total = head.as_ref().filter(|response| response.status().is_success())
            .and_then(|response| response.headers().get(header::CONTENT_LENGTH))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|total| *total != 0);
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
            if let Some(probe) = probe_range_support(&client, url, session).await {
                total = Some(probe.total);
                if if_range.is_none() { if_range = probe.if_range; }
                supports_ranges = true;
            }
        }

        if supports_ranges && total.unwrap_or(0) >= 2 * 1024 * 1024 && connections > 1 {
            match download_segmented(&client, url, &target, total.unwrap(), connections.min(16), session, if_range.as_deref(), progress_tx.clone(), rate_limiter.clone()).await {
                Ok(path) => return Ok(path),
                Err(error) => {
                    // Auth and webpage failures are terminal: re-running
                    // them through a full sequential GET only repeats a
                    // request the server already refused (P1-7).
                    let message = error.to_string();
                    if message == AUTH_REQUIRED_MESSAGE || message == WEBPAGE_NOT_MEDIA_MESSAGE {
                        return Err(error);
                    }
                    // Some servers advertise byte ranges but return invalid ranges.
                    // Restart sequentially rather than leave a potentially corrupt file.
                    let _ = progress_tx.send(DownloadProgress {
                        downloaded: 0, total, percent: Some(0.0), phase: "fallback".into(),
                        segments_done: None, segments_total: None,
                    });
                }
            }
        }
        download_sequential(&client, url, &target, total, session, progress_tx, rate_limiter, control).await
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
    session: Option<&SessionHeaders>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    mut control: tokio::sync::watch::Receiver<DownloadControl>,
) -> Result<PathBuf> {
    prepare_parent(target).await?;
    let temp = target.with_extension(format!("{}{}.part", target.extension().and_then(|x| x.to_str()).unwrap_or("file"), Uuid::new_v4()));
    let request = apply_session(session, client.get(url), url);
    // P1-4: the opening request can block up to the read timeout, so
    // the control channel is watched while it is in flight — a cancel
    // must interrupt the send promptly, not after 30 s. The send future
    // is re-polled across loop iterations (never re-issued). No bytes
    // exist yet at this point, so Pause/Cancel just end the call.
    let mut send_fut = request.send();
    let response = loop {
        tokio::select! {
            result = &mut send_fut => match result {
                Ok(response) => break response,
                Err(error) => return Err(map_auth_error(error)),
            },
            changed = control.changed() => {
                if changed.is_err() {
                    return Err(anyhow!(CONTROL_CLOSED_ERROR));
                }
                match *control.borrow() {
                    DownloadControl::Paused => return Err(anyhow!("Download paused")),
                    DownloadControl::Cancelled => return Err(anyhow!("Download cancelled")),
                    DownloadControl::Running => {}
                }
            }
        }
    };
    let response = response.error_for_status().map_err(map_auth_error)?;
    if response_is_webpage(response.headers()) {
        return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
    }
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
                    downloaded, total: actual_total, percent, phase: "downloading".into(),
                    segments_done: None, segments_total: None,
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
    if let Some(expected) = actual_total {
        if downloaded != expected {
            let _ = fs::remove_file(&temp).await;
            return Err(anyhow!("Download ended early ({downloaded} of {expected} bytes)"));
        }
    }
    if let Err(error) = replace_file(&temp, target).await {
        let _ = fs::remove_file(&temp).await;
        return Err(error).context("Could not commit completed download; temporary file removed");
    }
    let _ = progress_tx.send(DownloadProgress {
        downloaded, total: actual_total.or(Some(downloaded)), percent: Some(100.0), phase: "complete".into(),
        segments_done: None, segments_total: None,
    });
    Ok(target.to_path_buf())
}
async fn download_segmented(
    client: &Client,
    url: &str,
    target: &Path,
    total: u64,
    connections: usize,
    session: Option<&SessionHeaders>,
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
    // Slice 7b (Gemini claim 14): per-part progress for the live
    // channel. The part count is fixed by the layout above, so the
    // total is known before the first worker starts.
    let parts_total = ((total + chunk_size - 1) / chunk_size).min(connection_count);
    let parts_done = Arc::new(AtomicU64::new(0));
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
        let parts_done = parts_done.clone();
        let session = session.cloned();
        let validator = if_range.map(str::to_string);
        let progress_tx = progress_tx.clone();
        let rate_limiter = rate_limiter.clone();
        tasks.spawn(async move {
            let mut request = client.get(&url).header(header::RANGE, format!("bytes={start}-{end}"));
            request = apply_session(session.as_ref(), request, &url);
            if let Some(validator_value) = validator.as_deref() {
                request = request.header(header::IF_RANGE, validator_value);
            }
            let response = request.send().await?;
            if validator.as_deref().is_some_and(|expected| !response_matches_validator(response.headers(), expected)) {
                return Err(anyhow!("The media resource changed during the segmented download"));
            }
            // Slice 5 fix: a 401/403 on a segment fetch (the session
            // expired after a successful first response) is an auth
            // failure, not a range failure. Check it before the 206
            // check — the old code discarded the status, so the
            // error_text funnel could not recover it and the user
            // saw "Server did not honor byte range" instead of the
            // login label.
            if response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::FORBIDDEN {
                return Err(anyhow!(AUTH_REQUIRED_MESSAGE));
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
            // F1 backstop: a server whose HEAD omits Content-Type reaches
            // here. Abort before this segment's bytes touch a part file.
            if response_is_webpage(response.headers()) {
                return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
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
                            segments_done: Some(parts_done.load(Ordering::Relaxed)),
                            segments_total: Some(parts_total),
                        });
                        *last = Instant::now();
                    }
                }
            }
            file.flush().await?;
            if local_bytes != end - start + 1 {
                return Err(anyhow!("A range returned an incomplete segment"));
            }
            parts_done.fetch_add(1, Ordering::Relaxed);
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
        downloaded: total, total: Some(total), percent: Some(100.0), phase: "complete".into(),
        segments_done: Some(parts_total), segments_total: Some(parts_total),
    });
    Ok(target.to_path_buf())
}

async fn replace_file(temp: &Path, target: &Path) -> Result<()> {
    // P0-4: rename(2) replaces the destination atomically on Unix, so
    // the target is never deleted first — a crash mid-commit leaves
    // either the old or the new file, never none. Windows keeps the
    // pre-delete for now: a true atomic replace there needs
    // ReplaceFileW via the `windows-sys` crate, which is not a
    // dependency (adding it would touch Cargo.lock); the gap is
    // recorded, not silently claimed fixed.
    #[cfg(not(unix))]
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
    const MAX_FILENAME_BYTES: usize = 200;
    let cleaned: String = input
        .chars()
        .filter(|&c| !matches!(c, '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}'))
        .map(|c| if c.is_control() || "<>:\"/\\|?*".contains(c) { '_' } else { c })
        .collect();
    let cleaned = cleaned.trim_matches(|c: char| c.is_whitespace() || c == '.');
    if cleaned.is_empty() {
        return fallback.to_string();
    }
    let mut name = guard_windows_reserved_name(cleaned.to_string());
    if name.len() > MAX_FILENAME_BYTES {
        let split_ext = name.rfind('.').and_then(|dot| {
            let ext = &name[dot..];
            if dot > 0 && ext.len() > 1 && ext.len() <= MAX_FILENAME_BYTES - 5 {
                Some((&name[..dot], ext))
            } else {
                None
            }
        });
        name = match split_ext {
            Some((base, ext)) => {
                let max_base = MAX_FILENAME_BYTES - ext.len();
                let mut end = max_base.min(base.len());
                while end > 0 && !base.is_char_boundary(end) {
                    end -= 1;
                }
                let truncated_base = base[..end].trim_end_matches(|c: char| c.is_whitespace() || c == '.');
                if truncated_base.is_empty() {
                    format!("file{ext}")
                } else {
                    format!("{truncated_base}{ext}")
                }
            }
            None => {
                let mut end = MAX_FILENAME_BYTES.min(name.len());
                while end > 0 && !name.is_char_boundary(end) {
                    end -= 1;
                }
                let truncated = name[..end].trim_end_matches(|c: char| c.is_whitespace() || c == '.');
                if truncated.is_empty() {
                    fallback.to_string()
                } else {
                    truncated.to_string()
                }
            }
        };
    }
    guard_windows_reserved_name(name)
}

fn guard_windows_reserved_name(name: String) -> String {
    let stem = name.split('.').next().unwrap_or("").trim_end();
    let is_reserved = matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    );
    if is_reserved {
        format!("_{name}")
    } else {
        name
    }
}

/// Extensions that count as a real file extension for naming purposes
/// (lowercased, without the dot). Playlist extensions count too: a
/// playlist stem is named after the assembled file ("master.m3u8" ->
/// "master.mp4"). Anything else after the last dot is part of the
/// name, not an extension.
const KNOWN_FILE_EXTENSIONS: &[&str] = &[
    "mp4", "mkv", "webm", "mov", "avi", "m4v", "ts", "m2ts", "mp3", "m4a", "aac", "opus", "flac",
    "wav", "ogg", "oga", "wmv", "flv", "3gp", "m3u8", "mpd", "vtt", "srt", "ass", "ssa",
];

/// The filename `name` with its extension replaced by `extension` — but
/// only when the name already ends in a known media/subtitle extension.
/// A name with no extension, or one ending in something that is not a
/// media extension, keeps its full name and gains the extension:
/// "dash" -> "dash.mp4", "my.video" -> "my.video.mp4",
/// "clip.mp4" -> "clip.mp4", "a.b.c.mkv" -> "a.b.c.mp4".
/// (Gemini claim 9: the old code replaced whatever followed the last
/// dot, so "my.video" became "my.mp4".)
pub fn with_extension(name: &str, extension: &str) -> String {
    let trimmed = name.trim_end_matches('.');
    match trimmed.rfind('.') {
        Some(dot) if dot > 0
            && KNOWN_FILE_EXTENSIONS.contains(&trimmed[dot + 1..].to_ascii_lowercase().as_str()) =>
        {
            format!("{}.{}", &trimmed[..dot], extension)
        }
        _ => format!("{trimmed}.{extension}"),
    }
}


#[cfg(test)]
mod tests {
    use super::{next_available_path, safe_filename, validate_content_range};
    use uuid::Uuid;

    #[test]
    fn with_extension_replaces_only_known_extensions() {
        // Gemini claim 9: only a known media/subtitle extension is
        // replaced; anything else after the last dot is part of the
        // name and is kept.
        assert_eq!(super::with_extension("dash", "mp4"), "dash.mp4");
        assert_eq!(super::with_extension("my.video", "mp4"), "my.video.mp4");
        assert_eq!(super::with_extension("clip.mp4", "mp4"), "clip.mp4");
        assert_eq!(super::with_extension("a.b.c.mkv", "mp4"), "a.b.c.mp4");
        assert_eq!(super::with_extension("CLIP.MKV", "mp4"), "CLIP.mp4");
        assert_eq!(super::with_extension("master.m3u8", "mp4"), "master.mp4");
        assert_eq!(super::with_extension("manifest.mpd", "mp4"), "manifest.mp4");
        assert_eq!(super::with_extension("subtitles.srt", "vtt"), "subtitles.vtt");
        assert_eq!(super::with_extension("recording", "ts"), "recording.ts");
        assert_eq!(super::with_extension("my.video", "ts"), "my.video.ts");
    }

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
        // Calls the production layout, not a local copy of it.
        assert_eq!(super::segment_ranges(10, 3), vec![(0, 3), (4, 7), (8, 9)]);
        assert_eq!(super::segment_ranges(2, 16), vec![(0, 0), (1, 1)]);
        assert_eq!(super::segment_ranges(u64::MAX, 2), vec![(0, u64::MAX / 2), (u64::MAX / 2 + 1, u64::MAX - 1)]);
    }

    #[test]
    fn one_byte_probe_requires_a_concrete_resource_length() {
        // Calls the production parser, not a local copy of it.
        assert_eq!(super::parse_probe_content_range("bytes 0-0/4096"), Some(4096));
        assert_eq!(super::parse_probe_content_range("bytes 0-0/*"), None);
        assert_eq!(super::parse_probe_content_range("bytes 1-1/4096"), None);
        assert_eq!(super::parse_probe_content_range("bytes 0-0/0"), None);
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

    #[tokio::test]
    async fn over_delivering_chunked_206_is_rejected_before_overwrite() {
        // P0-3: a chunked 206 with no Content-Length that sends more
        // than the requested range must fail before the excess bytes
        // reach the shared .part file — the bytes past the segment end
        // are sentinel and must survive untouched.
        use std::sync::{atomic::AtomicU64, Arc};
        use tokio::sync::{mpsc, watch};
        let directory = std::env::temp_dir()
            .join(format!("thunderload-overdeliver-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let part = directory.join("file.thunderload.part");
        tokio::fs::write(&part, vec![0xAA_u8; 1024]).await.unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let serve = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => return,
                    Ok(count) => {
                        request.extend_from_slice(&buffer[..count]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }
            let mut response = b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-99/1024\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
            for _ in 0..4 {
                response.extend_from_slice(b"32\r\n");
                response.extend_from_slice(&[0xBB_u8; 50]);
                response.extend_from_slice(b"\r\n");
            }
            response.extend_from_slice(b"0\r\n\r\n");
            let _ = stream.write_all(&response);
        });
        let client = super::client().unwrap();
        let (control_tx, control_rx) = watch::channel(super::DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = super::download_segment_range(
            &client,
            &format!("http://{addr}/file.bin"),
            &part,
            0,
            99,
            1024,
            None,
            None,
            control_rx,
            Arc::new(super::GlobalRateLimiter::new()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
            progress_tx,
            Arc::new(AtomicU64::new(0)),
            1,
            Arc::new(AtomicU64::new(0)),
            Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
            Arc::new(AtomicU64::new(0)),
        )
        .await;
        drop(control_tx);
        serve.join().unwrap();
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("more bytes"),
            "over-delivery must fail with the range error, got: {error}"
        );
        let bytes = tokio::fs::read(&part).await.unwrap();
        assert_eq!(bytes.len(), 1024);
        assert!(bytes[..100].iter().all(|byte| *byte == 0xBB));
        assert!(
            bytes[100..].iter().all(|byte| *byte == 0xAA),
            "over-delivered bytes overwrote the adjacent range"
        );
        tokio::fs::remove_dir_all(&directory).await.unwrap();
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
    ///
    /// Idle time cannot bank burst allowance (Slice 7a, Gemini 15a): bytes
    /// already reserved are "spent" by `epoch + reserved/limit`, so when
    /// that instant is in the past — the downloader was idle — the epoch
    /// is clamped forward to end now. Without the clamp, 60 s idle at a
    /// 1 MB/s limit left ~50 MB unthrottled on the next reserve.
    pub async fn reserve(&self, bytes: u64) -> std::time::Duration {
        let limit = self.limit.load(Ordering::Relaxed);
        let mut state = self.state.lock().await;
        if state.limit != limit {
            state.limit = limit;
            state.epoch = Instant::now();
            state.reserved = 0;
        }
        if limit == 0 || bytes == 0 { return std::time::Duration::ZERO; }
        let now = Instant::now();
        let spent_until = state.epoch + std::time::Duration::from_secs_f64(state.reserved as f64 / limit as f64);
        if spent_until < now {
            state.epoch = now;
            state.reserved = 0;
        }
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
/// The suffix is appended to the target's full name (`OsString` push),
/// never built with `with_extension`: the old `"file"` fallback made
/// `README` and `README.file` share one part path (P1-6).
pub fn partial_download_path(target: &Path) -> PathBuf {
    let mut path = target.as_os_str().to_owned();
    path.push(".thunderload.part");
    PathBuf::from(path)
}

pub fn partial_download_metadata_path(target: &Path) -> PathBuf {
    let mut path = target.as_os_str().to_owned();
    path.push(".thunderload.part.meta.json");
    PathBuf::from(path)
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
    /// Dynamic-segmentation marker (Slice 8). `Some(true)` means
    /// `segments` is a chunk plan (see `chunk_ranges`), validated by
    /// coverage, not by exact equality with `segment_ranges`. Absent
    /// (None) means the checkpoint was written by Slice 1-7 code — or
    /// by an older build — so the fixed equal-segment layout rule
    /// applies and the old checkpoint still parses and resumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dynamic: Option<bool>,
    /// Browser session handed off with this job, if any — stored here
    /// so a resume reuses it even when the caller has none. A desktop
    /// resume of a browser-started job mints a fresh job id, so the
    /// stored JobRecord session is unreachable; the checkpoint is the
    /// only copy left (F2). An explicit caller session always wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session: Option<SessionHeaders>,
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

/// Dynamic segmentation (Slice 8): the chunk plan for a fresh segmented
/// download. The file is split into near-equal chunks — each between
/// `MIN_CHUNK_SIZE` and `2 * MIN_CHUNK_SIZE` bytes (no silly 1 KB
/// chunks) and never more than `MAX_CHUNKS` chunks in total (so
/// per-chunk checkpoint rewrites stay cheap even on a 100 GiB file).
/// The count is `floor(total / MIN_CHUNK_SIZE)` — not the ceiling — and
/// the bytes are split evenly (the first `total % count` chunks get one
/// byte more), so every chunk is `floor(total / count)` or
/// `ceil(total / count)`, both ≥ the minimum because
/// `count ≤ total / MIN`. (A ceiling count would shrink every chunk
/// toward half the minimum whenever the total is just over a multiple
/// of MIN — F3.)
/// Chunks are the checkpoint unit, exactly like segments were:
/// `ResumeMetadata.segments` holds the chunk list and a chunk is marked
/// done only after its full range is written and flushed, so a pause
/// mid-chunk re-downloads at most one chunk.
///
/// Why chunks instead of re-splitting segment boundaries mid-flight:
/// the remaining work is re-split *at plan time* into chunks, and the
/// *assignment* adapts continuously — workers pull the next unfinished
/// chunk when free, so each connection's share of the remaining work
/// ends up proportional to its measured throughput, and a stalled
/// connection holds at most one chunk while the others drain the rest.
/// No byte range that is downloaded or in flight is ever moved, so a
/// pause/resume mid-adaptation cannot lose or re-download committed
/// bytes (only a partially written chunk is re-fetched whole, exactly
/// like a partially written segment was before).
const MIN_CHUNK_SIZE: u64 = 256 * 1024;
const MAX_CHUNKS: usize = 1024;

/// How long a worker may go without receiving a byte before the
/// coordinator treats its current chunk as stalled and re-assigns it
/// (see `run_segmented_download_with_stats`).
///
/// INVARIANT (MC4): the stall watchdog MUST stay clearly above the
/// client's read timeout (`READ_TIMEOUT`, 30 s) — 90 s here, 3x.
/// The two must never fire together: on a dead connection the read
/// timeout fires first and surfaces as a retryable timeout error,
/// which the chunk worker re-queues as a fresh attempt (see
/// `is_transient_segment_error`); the watchdog is only the backstop
/// for a connection that keeps trickling bytes — trickle resets the
/// read timeout, so only the watchdog can catch it. If the watchdog
/// were equal to (or below) the read timeout, a dead connection
/// would race between a timeout error and a watchdog abort, and the
/// abort path could discard a chunk the timeout path was already
/// re-queueing. Keep this gap when either value changes.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(90);
/// A chunk that stalls this many times in one run is a real failure, not
/// a race to chase: the run errors out (checkpoint preserved for Resume)
/// instead of aborting and re-queueing forever.
const MAX_STALL_REQUEUES: u32 = 3;

fn chunk_ranges(total: u64) -> Vec<(u64, u64)> {
    if total == 0 {
        return Vec::new();
    }
    // Floor, not ceiling: with count = total / MIN (rounded down),
    // total / count >= MIN, so an even split keeps every chunk at or
    // above the minimum. (A ceiling count would shrink every chunk
    // toward MIN/2 whenever the total is just over a multiple of MIN.)
    let count = ((total / MIN_CHUNK_SIZE) as usize).min(MAX_CHUNKS).max(1) as u64;
    let base = total / count;
    let extra = total % count;
    let mut start = 0_u64;
    (0..count)
        .map(|index| {
            let size = base + u64::from(index < extra);
            let end = start + size - 1;
            let range = (start, end);
            start = end + 1;
            range
        })
        .collect()
}

/// Whether a dynamic (chunked) checkpoint layout is safe to trust: the
/// ranges must be sorted, contiguous, and cover exactly `[0, total)`,
/// with a bounded chunk count. Corrupt-but-covering layouts are
/// harmless — they just partition the file — so the rule checks shape,
/// not exact equality with today's chunk plan (a chunk plan written by
/// a future chunk size still validates).
fn dynamic_layout_matches(saved: &ResumeMetadata) -> bool {
    let Some(total) = saved.total else { return false };
    let Some(segments) = saved.segments.as_ref() else { return false };
    if segments.is_empty() || segments.len() > MAX_CHUNKS {
        return false;
    }
    let mut expected_start = 0_u64;
    for state in segments {
        if state.start != expected_start || state.end < state.start || state.end >= total {
            return false;
        }
        expected_start = state.end + 1;
    }
    expected_start == total
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
    // Whether the HEAD response advertised a webpage (text/html). The
    // dispatchers fail fast on this (F1): without it, a page with
    // Content-Length >= 2 MiB and Accept-Ranges: bytes was segmented and
    // saved as a "Completed" file.
    is_webpage: bool,
}

async fn fetch_server_info(
    client: &Client,
    url: &str,
    session: Option<&SessionHeaders>,
    probe: bool,
) -> ServerInfo {
    let head_request = apply_session(session, client.head(url), url);
    let head = head_request.send().await.ok();
    // Read the total from the Content-Length header, NOT from
    // response.content_length(): for a HEAD response hyper reports the body
    // size, which is always 0, so content_length() would report a total of 0.
    // A Content-Length of 0 likewise means "unknown", not a 0-byte object:
    // chunked servers (notably YouTube watch pages) report 0 on HEAD while
    // streaming a body on GET. Keeping Some(0) would fail a finished
    // transfer in the sequential completion check ("server reported 0"),
    // and the segmented path is only entered on a known total, so None
    // routes such downloads to the sequential path, which skips the check
    // when the total is unknown. A genuinely empty file still completes:
    // 0 bytes delivered, no check to fail.
    let mut total = head
        .as_ref()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.headers().get(header::CONTENT_LENGTH))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|total| *total != 0);
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
    // F1: a webpage HEAD means the URL is a page, not media. Recorded
    // here so the dispatchers can fail before any segmented or
    // sequential work creates part files or writes bytes.
    let is_webpage = head
        .as_ref()
        .filter(|response| response.status().is_success())
        .map(|response| response_is_webpage(response.headers()))
        .unwrap_or(false);
    // Probe when HEAD omits Accept-Ranges or the total (common on
    // CDN/object-storage endpoints). A valid one-byte 206 is stronger
    // evidence than the hint. A validator found only by the probe is
    // classified by shape (quoted = ETag, otherwise Last-Modified) so the
    // fresh checkpoint still carries it for If-Range.
    if probe && (!supports_ranges || total.is_none()) {
        if let Some(found) = probe_range_support(client, url, session).await {
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
    ServerInfo {
        total,
        supports_ranges,
        etag,
        last_modified,
        is_webpage,
    }
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

/// Synchronous read of a direct-download checkpoint sidecar (Slice 5:
/// the job store imports orphaned checkpoints without a Tokio runtime).
/// Returns the download URL and the advertised total, if any.
/// Reads a direct-download checkpoint sidecar: the URL, the total,
/// and the browser session stored with it, if any (F3: a direct orphan
/// keeps its handoff so a desktop resume reuses it).
pub(crate) fn direct_checkpoint_info(path: &Path) -> Option<(String, Option<u64>, Option<SessionHeaders>)> {
    let bytes = std::fs::read(path).ok()?;
    let metadata: ResumeMetadata = serde_json::from_slice(&bytes).ok()?;
    Some((metadata.url, metadata.total, metadata.session))
}

async fn write_resume_metadata(path: &Path, metadata: &ResumeMetadata) -> Result<()> {
    // Unique tmp name (pid + uuid, as the job store does): two writers
    // to the same checkpoint must never share one `meta.tmp`.
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(format!(".{}.{}.tmp", std::process::id(), Uuid::new_v4()));
    let temporary = PathBuf::from(tmp_name);
    let bytes = serde_json::to_vec(metadata)?;
    let mut options = fs::OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut tmp_file = options.open(&temporary).await?;
    tmp_file.write_all(&bytes).await?;
    tmp_file.flush().await?;
    // P0-4: the checkpoint must be durable before the rename — a power
    // loss after the rename must never expose a torn checkpoint.
    tmp_file.sync_all().await?;
    drop(tmp_file);
    // Unix rename(2) replaces the destination atomically, so the
    // metadata file is never absent: a crash mid-write leaves either
    // the old or the new checkpoint, never none. Windows rename fails
    // when the destination exists, so there the old file is removed
    // first (a crash in that gap loses the checkpoint — the part file
    // survives and a fresh download can restart — but Slice 8's
    // per-chunk marks make the gap far more likely to be hit, so the
    // Unix path must not pay it). F5.
    #[cfg(not(unix))]
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
/// Dynamic checkpoints (Slice 8, `dynamic: true`) are validated by
/// coverage instead — see `dynamic_layout_matches` — so a chunk plan
/// written by any chunk size still resumes.
fn segmented_layout_matches(saved: &ResumeMetadata) -> bool {
    if saved.dynamic == Some(true) {
        return dynamic_layout_matches(saved);
    }
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
    /// Instant of the last on-disk checkpoint write (F6).
    last_write: Instant,
}

/// Defense-in-depth helper (F1): every chunk must be marked done before
/// the part file is committed. The preallocated file's size can never
/// prove coverage, so the checkpoint is the authority. An empty segment
/// list is not coverage — a checkpoint with no segments must never
/// commit.
fn all_segments_done(metadata: &ResumeMetadata) -> bool {
    metadata
        .segments
        .as_ref()
        .is_some_and(|segments| !segments.is_empty() && segments.iter().all(|state| state.done))
}

/// How often a chunk completion may rewrite the checkpoint file at
/// most (F6): a full JSON rewrite per chunk is up to ~47 MB of metadata
/// writes for a 1024-chunk file. Marks update the in-memory state
/// immediately and flush to disk at most this often — or immediately
/// when the last unfinished chunk completes, so the on-disk state is
/// complete before the run can report success. A crash between flushes
/// only loses completion markers (safe: those chunks are re-downloaded
/// on resume, never joined wrongly); the Paused tail writes the
/// metadata explicitly before returning, and the Completed tail deletes
/// the file, so no final state ever depends on the throttle.
const MARK_COALESCE: Duration = Duration::from_millis(500);

async fn mark_segment_done(checkpoint: &Arc<AsyncMutex<SegmentCheckpoint>>, index: usize) -> Result<()> {
    let mut guard = checkpoint.lock().await;
    if let Some(segments) = guard.metadata.segments.as_mut() {
        if let Some(state) = segments.get_mut(index) {
            state.done = true;
        }
    }
    let all_done = all_segments_done(&guard.metadata);
    if all_done || guard.last_write.elapsed() >= MARK_COALESCE {
        write_resume_metadata(&guard.metadata_path, &guard.metadata).await?;
        guard.last_write = Instant::now();
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentOutcome {
    Completed,
    Paused,
    Cancelled,
    ValidatorChanged,
    /// The control channel closed (host disconnected) while this
    /// segment was in flight. This is NOT a user Cancel: the
    /// coordinator turns it into an error with the checkpoint kept,
    /// so the host's disconnect path can mark the job Interrupted
    /// ("partial checkpoint kept") instead of deleting the resume
    /// state (N1). An explicit Cancel still wins over it.
    Interrupted,
}

/// Attempts for one chunk, initial try included: a chunk that hits a
/// transient failure is retried this many times in total before the
/// run fails with the checkpoint preserved.
const MAX_SEGMENT_ATTEMPTS: u32 = 4;

/// Whether an HTTP status is worth retrying with backoff: 429 (rate
/// limited) and 5xx (server fault). Everything else fails fast —
/// notably 401/403 (login wall) and 404/416, which a retry cannot fix.
fn is_transient_http_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Whether a failed chunk attempt is worth retrying (MC3): a 429/5xx
/// status carried on a reqwest error, or a transport failure
/// (connect / timeout — including the client's read timeout firing on
/// a dead connection, MC4: a read timeout re-queues the chunk, it
/// never fails the run outright). Anything else — auth, a bad range,
/// a webpage, a validator change — is terminal.
fn is_transient_segment_error(error: &anyhow::Error) -> bool {
    for cause in error.chain() {
        if let Some(request_error) = cause.downcast_ref::<reqwest::Error>() {
            if let Some(status) = request_error.status() {
                if is_transient_http_status(status) {
                    return true;
                }
                // A status is present but not transient: fail fast.
                return false;
            }
            if request_error.is_connect() || request_error.is_timeout() || request_error.is_body() {
                return true;
            }
        }
    }
    false
}

/// Backoff between chunk attempts: 1 s, 2 s, 4 s. Bounded by
/// `MAX_SEGMENT_ATTEMPTS`, so the worst case is ~7 s of waiting
/// before the run fails with the checkpoint preserved.
fn transient_backoff(attempt: u32) -> Duration {
    Duration::from_secs(1 << attempt.min(2))
}

/// Error text when the control channel closes mid-transfer (N1):
/// the host disconnected, which is NOT a user Cancel — the partial
/// file and checkpoint stay on disk for Resume, and the host marks
/// the job Interrupted from this error.
const CONTROL_CLOSED_ERROR: &str =
    "Download control channel closed; partial checkpoint kept for Resume";

/// Backoff interruption in the sequential path: translate the
/// control outcome from `sleep_or_control` into the return value of
/// `download_sequential_resumable`. Returns `None` to keep retrying.
/// Paused keeps the checkpoint, Cancelled deletes it (explicit user
/// action), Interrupted errors with the checkpoint kept (N1).
async fn sequential_backoff(
    control: &mut tokio::sync::watch::Receiver<DownloadControl>,
    delay: Duration,
    part: &Path,
    metadata_path: &Path,
    target: &Path,
) -> Option<Result<DownloadOutcome>> {
    match sleep_or_control(control, delay).await {
        None => None,
        Some(SegmentOutcome::Paused) => Some(Ok(DownloadOutcome::Paused)),
        Some(SegmentOutcome::Cancelled) => {
            remove_resume_files(part, metadata_path, target).await;
            Some(Ok(DownloadOutcome::Cancelled))
        }
        Some(_) => Some(Err(anyhow!(CONTROL_CLOSED_ERROR))),
    }
}
/// Sleep `delay` unless the control channel changes state first.
/// Returns the outcome the caller must return when control spoke:
/// Paused / Cancelled on an explicit state change, Interrupted when
/// the channel closed (host disconnected — checkpoint kept, N1).
/// Returns `None` when the full delay elapsed with control still
/// Running. A re-sent Running mark just restarts the wait.
async fn sleep_or_control(
    control: &mut tokio::sync::watch::Receiver<DownloadControl>,
    delay: Duration,
) -> Option<SegmentOutcome> {
    if delay.is_zero() {
        return None;
    }
    let deadline = Instant::now() + delay;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        tokio::select! {
            _ = tokio::time::sleep(remaining) => return None,
            changed = control.changed() => {
                if changed.is_err() {
                    return Some(SegmentOutcome::Interrupted);
                }
                match *control.borrow() {
                    DownloadControl::Paused => return Some(SegmentOutcome::Paused),
                    DownloadControl::Cancelled => return Some(SegmentOutcome::Cancelled),
                    // A re-sent Running mark: keep waiting out the remainder.
                    DownloadControl::Running => {}
                }
            }
        }
    }
}

#[derive(Debug)]
enum SegmentedRun {
    Finished(DownloadOutcome),
    ValidatorChanged,
}

/// One segment of a checkpointed segmented download: streams its byte
/// range into the shared preallocated partial file at its own offset.
/// Each task owns its own file handle and seeks to its own range, so
/// concurrent tasks never share a cursor and ranges never overlap.
///
/// Slice 8 additions: `chunk_received` counts this range's bytes alone
/// (the coordinator subtracts them if the chunk is re-queued after a
/// stall, so the shared `received` counter stays the true unique-byte
/// count); `activity` is the owning worker's last-received-byte instant
/// (the stall watchdog reads it); `reported` is the running maximum of
/// bytes ever reported, so progress never goes backwards even across a
/// re-queue subtraction.
///
/// Slice 7b additions: `segments_done`/`segments_total` report the
/// finished-chunk count on the live progress channel.
#[allow(clippy::too_many_arguments)]
async fn download_segment_range(
    client: &Client,
    url: &str,
    part: &Path,
    start: u64,
    end: u64,
    total: u64,
    validator: Option<&str>,
    session: Option<&SessionHeaders>,
    mut control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    received: Arc<AtomicU64>,
    last_report: Arc<Mutex<Instant>>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    segments_done: Arc<AtomicU64>,
    segments_total: u64,
    chunk_received: Arc<AtomicU64>,
    activity: Arc<Mutex<Instant>>,
    reported: Arc<AtomicU64>,
) -> Result<SegmentOutcome> {
    let mut request = apply_session(
        session,
        client.get(url).header(header::RANGE, format!("bytes={start}-{end}")),
        url,
    );
    if let Some(validator_value) = validator {
        request = request.header(header::IF_RANGE, validator_value);
    }
    // N1: the opening request can block up to the read timeout, so
    // the control channel is watched while it is in flight — a
    // disconnect must interrupt the segment promptly, not after 30 s.
    // The send future is re-polled across loop iterations (never
    // re-issued), so a re-sent Running mark just keeps waiting.
    let mut send_fut = request.send();
    let response = loop {
        tokio::select! {
            result = &mut send_fut => break result?,
            changed = control.changed() => {
                if changed.is_err() {
                    return Ok(SegmentOutcome::Interrupted);
                }
                // Copy the value out first: watch::Ref is !Send.
                let state = *control.borrow();
                match state {
                    DownloadControl::Paused => return Ok(SegmentOutcome::Paused),
                    DownloadControl::Cancelled => return Ok(SegmentOutcome::Cancelled),
                    DownloadControl::Running => {}
                }
            }
        }
    };
    if response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::FORBIDDEN {
        return Err(anyhow!(AUTH_REQUIRED_MESSAGE));
    }
    if response.status() == StatusCode::OK {
        // A 200 here means If-Range failed: the object changed mid-flight.
        // The coordinator discards the checkpoint and restarts cleanly
        // instead of joining two versions of the file.
        return Ok(SegmentOutcome::ValidatorChanged);
    }
    if response.status() != StatusCode::PARTIAL_CONTENT {
        // Carry the status on the reqwest error (instead of a bare
        // "HTTP 503" string) so the chunk worker can classify it:
        // 429/5xx is transient and retried with backoff, anything
        // else fails fast (MC3). 3xx cannot carry a reqwest status
        // error — error_for_status only fires on 4xx/5xx — so those
        // keep the plain message.
        if response.status().is_client_error() || response.status().is_server_error() {
            return Err(anyhow::Error::new(
                response.error_for_status().unwrap_err(),
            ));
        }
        return Err(anyhow!("Segment request returned HTTP {}", response.status()));
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
    // F1 backstop: a server whose HEAD omits Content-Type reaches here,
    // as does a stale pre-fix checkpoint resumed from disk. Abort before
    // this chunk's bytes are written into the partial file.
    if response_is_webpage(response.headers()) {
        return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
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
                // Control channel closed (host disconnected): NOT a
                // user Cancel — flush and report Interrupted so the
                // coordinator keeps the checkpoint (N1).
                let _ = file.flush().await;
                return Ok(SegmentOutcome::Interrupted);
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
        // P0-3: a chunked 206 with no Content-Length can over-deliver
        // past this segment's end and overwrite the adjacent segment
        // in the shared .part file. Abort before writing, not after.
        if chunk.len() as u64 > segment_len.saturating_sub(local_bytes) {
            return Err(anyhow!("Server sent more bytes than the requested range"));
        }
        file.write_all(&chunk).await?;
        local_bytes += chunk.len() as u64;
        // R1: both counters are incremented together, before any await.
        // The rate-limiter wait below can suspend the task, and the
        // watchdog can abort it there; if `received` were incremented
        // after the wait, the reap could subtract bytes that were never
        // added to it (fetch_sub underflow — the progress counter would
        // wrap to a huge value in both debug and release builds).
        chunk_received.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        let sum = received.fetch_add(chunk.len() as u64, Ordering::Relaxed) + chunk.len() as u64;
        if let Ok(mut last) = activity.lock() {
            *last = Instant::now();
        }
        let wait = rate_limiter.reserve(chunk.len() as u64).await;
        let mut remaining = wait;
        while remaining > Duration::ZERO {
            let step = remaining.min(Duration::from_secs(5));
            tokio::select! {
                _ = tokio::time::sleep(step) => {
                    remaining = remaining.saturating_sub(step);
                    if let Ok(mut last) = activity.lock() {
                        *last = Instant::now();
                    }
                }
                changed = control.changed() => {
                    if changed.is_err() {
                        // Control channel closed (host disconnected):
                        // NOT a user Cancel — keep the checkpoint (N1).
                        let _ = file.flush().await;
                        return Ok(SegmentOutcome::Interrupted);
                    }
                    let state = *control.borrow();
                    match state {
                        DownloadControl::Paused => {
                            file.flush().await?;
                            return Ok(SegmentOutcome::Paused);
                        }
                        DownloadControl::Cancelled => {
                            return Ok(SegmentOutcome::Cancelled);
                        }
                        DownloadControl::Running => {}
                    }
                }
            }
        }
        if let Ok(mut last) = last_report.lock() {
            if last.elapsed() >= std::time::Duration::from_millis(180) || sum == total {
                // Clamp the reported value to the running maximum: a
                // stall re-queue subtracts this chunk's partial bytes
                // from `received`, and progress must never go backwards.
                let shown = reported.fetch_max(sum, Ordering::Relaxed).max(sum);
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: shown,
                    total: Some(total),
                    percent: Some(shown as f64 * 100.0 / total as f64),
                    phase: "downloading".into(),
                    segments_done: Some(segments_done.load(Ordering::Relaxed)),
                    segments_total: Some(segments_total),
                });
                *last = Instant::now();
            }
        }
    }
    file.flush().await?;
    if local_bytes != segment_len {
        return Err(anyhow!("A range returned an incomplete segment"));
    }
    // P0-4: the segment bytes must be durable before the checkpoint can
    // mark this chunk done — otherwise a power loss leaves the metadata
    // claiming bytes that never reached disk.
    file.sync_data().await?;
    Ok(SegmentOutcome::Completed)
}

/// Per-connection throughput measurement (Slice 8). One `WorkerStats`
/// is produced per worker slot. Throughput is an exponentially weighted
/// moving average over completed chunks (weight 1/2 per sample), so a
/// measurement older than a couple of chunks contributes under a quarter
/// of the estimate — stale measurements age out instead of steering the
/// run forever. The average is recorded for diagnostics and tests; the
/// adaptation itself is the pull-based queue (a fast connection simply
/// claims more chunks), so no decision ever depends on a stale estimate.
#[derive(Debug, Clone, Default)]
struct WorkerStats {
    chunks_completed: usize,
    bytes_downloaded: u64,
    ewma_bytes_per_sec: f64,
}

/// What one segmented run did: per-slot stats plus how often the
/// watchdog had to re-assign a stalled chunk.
#[derive(Debug, Clone, Default)]
struct SegmentRunStats {
    workers: Vec<WorkerStats>,
    stall_requeues: u32,
    /// The shared received-bytes counter at the end of the run:
    /// unique bytes actually streamed. A retried or re-queued
    /// chunk's partial bytes are subtracted back out, so this stays
    /// the true byte count rather than the sum of every attempt's
    /// bytes — tests assert it against the real total instead of
    /// re-deriving bytes from the chunk plan (per-worker
    /// `bytes_downloaded` only counts completed chunks, so it cannot
    /// prove the rollback).
    received: u64,
}

/// Shared work-stealing state for one segmented run. `pending` holds the
/// indices of unfinished chunks in file order; a worker claims the front
/// entry when free, so the remaining work is always handed to whoever is
/// ready — faster connections end up with the larger share. `current` is
/// the chunk each slot is working on right now (if any); `stalls` counts,
/// per chunk, how many times the watchdog re-queued it. The mutex is
/// only ever held for a pop/push or a flag read — never across an await
/// — so claiming never blocks downloading.
struct DynamicQueue {
    pending: VecDeque<usize>,
    stalls: Vec<u32>,
    current: Vec<Option<usize>>,
}

/// Everything a chunk worker needs that is shared across slots.
struct SegmentWorkers {
    client: Client,
    url: String,
    part: PathBuf,
    total: u64,
    ranges: Vec<(u64, u64)>,
    validator: Option<String>,
    session: Option<SessionHeaders>,
    rate_limiter: Arc<GlobalRateLimiter>,
    received: Arc<AtomicU64>,
    reported: Arc<AtomicU64>,
    last_report: Arc<Mutex<Instant>>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    checkpoint: Arc<AsyncMutex<SegmentCheckpoint>>,
    queue: Arc<Mutex<DynamicQueue>>,
    chunk_received: Vec<Arc<AtomicU64>>,
    activities: Vec<Arc<Mutex<Instant>>>,
    // Slice 7b (Gemini claim 14): finished-chunk count for the live
    // progress channel, seeded from the checkpoint's pre-done chunks so
    // a resume reports the true count, not zero.
    segments_done: Arc<AtomicU64>,
    segments_total: u64,
}

struct WorkerResult {
    terminal: Option<SegmentOutcome>,
    error: Option<anyhow::Error>,
    stats: WorkerStats,
}

/// One worker slot: claim unfinished chunks from the shared queue until
/// it is empty. A chunk is claimed only when the worker is free, so a
/// slow connection never holds more than one chunk at a time while the
/// others drain the rest — that is the work stealing. A claimed chunk is
/// downloaded by `download_segment_range` and marked done in the
/// checkpoint on completion; a partially written chunk is simply
/// re-downloaded whole on the next run, exactly like a partially
/// written segment was before.
async fn download_chunk_worker(
    shared: Arc<SegmentWorkers>,
    slot: usize,
    control: tokio::sync::watch::Receiver<DownloadControl>,
) -> WorkerResult {
    let mut stats = WorkerStats::default();
    loop {
        let claimed = {
            let mut guard = shared.queue.lock().unwrap();
            let next = guard.pending.pop_front();
            if next.is_some() {
                guard.current[slot] = next;
            }
            next
        };
        let Some(index) = claimed else {
            return WorkerResult { terminal: None, error: None, stats };
        };
        // Transient retry (MC3): a chunk that fails on a 429/5xx
        // status or a transport failure (including the read timeout
        // firing on a dead connection, MC4) is re-downloaded whole
        // on a fresh connection with backoff, instead of failing the
        // run outright. The attempt's counted bytes are rolled back
        // first — the same swap/subtract pairing the stall re-queue
        // uses — so progress never over-counts across attempts.
        // Non-transient failures (auth, bad range, webpage) and
        // ValidatorChanged still end the chunk immediately.
        let (start, end) = shared.ranges[index];
        // Set at the top of every attempt below, before any break
        // out of the retry loop — always the successful attempt's
        // start by the time the Completed arm reads it.
        let mut started: Instant;
        let outcome: Result<SegmentOutcome> = {
            let mut attempt: u32 = 0;
            loop {
                attempt += 1;
                started = Instant::now();
                if let Ok(mut last) = shared.activities[slot].lock() {
                    *last = Instant::now();
                }
                match download_segment_range(
                    &shared.client,
                    &shared.url,
                    &shared.part,
                    start,
                    end,
                    shared.total,
                    shared.validator.as_deref(),
                    shared.session.as_ref(),
                    control.clone(),
                    shared.rate_limiter.clone(),
                    shared.received.clone(),
                    shared.last_report.clone(),
                    shared.progress_tx.clone(),
                    shared.segments_done.clone(),
                    shared.segments_total,
                    shared.chunk_received[index].clone(),
                    shared.activities[slot].clone(),
                    shared.reported.clone(),
                )
                .await
                {
                    Ok(outcome) => break Ok(outcome),
                    Err(error) if is_transient_segment_error(&error)
                        && attempt < MAX_SEGMENT_ATTEMPTS =>
                    {
                        let partial =
                            shared.chunk_received[index].swap(0, Ordering::Relaxed);
                        debug_assert!(partial <= shared.received.load(Ordering::Relaxed));
                        shared.received.fetch_sub(partial, Ordering::Relaxed);
                        let mut backoff_control = control.clone();
                        match sleep_or_control(
                            &mut backoff_control,
                            transient_backoff(attempt - 1),
                        )
                        .await
                        {
                            Some(outcome) => break Ok(outcome),
                            None => continue,
                        }
                    }
                    Err(error) => break Err(error),
                }
            }
        };
        {
            let mut guard = shared.queue.lock().unwrap();
            guard.current[slot] = None;
        }
        match outcome {
            Ok(SegmentOutcome::Completed) => {
                if let Err(error) = mark_segment_done(&shared.checkpoint, index).await {
                    return WorkerResult { terminal: None, error: Some(error), stats };
                }
                // Slice 7b: only after the checkpoint marks the chunk
                // done — the live count never claims a chunk the resume
                // state does not have.
                shared.segments_done.fetch_add(1, Ordering::Relaxed);
                let chunk_len = end - start + 1;
                let rate = chunk_len as f64 / started.elapsed().as_secs_f64().max(1e-6);
                stats.chunks_completed += 1;
                stats.bytes_downloaded += chunk_len;
                stats.ewma_bytes_per_sec = if stats.chunks_completed == 1 {
                    rate
                } else {
                    0.5 * stats.ewma_bytes_per_sec + 0.5 * rate
                };
            }
            Ok(other) => return WorkerResult { terminal: Some(other), error: None, stats },
            Err(error) => return WorkerResult { terminal: None, error: Some(error), stats },
        }
    }
}

/// One slot of the coordinator: its task handle, the handle used to
/// abort it, and the chunk it was aborted on (if the watchdog fired and
/// the task has not finished dying yet).
struct Slot {
    handle: Option<JoinHandle<WorkerResult>>,
    abort: Option<AbortHandle>,
    aborting_chunk: Option<usize>,
}

fn spawn_chunk_worker(
    shared: &Arc<SegmentWorkers>,
    control: &tokio::sync::watch::Receiver<DownloadControl>,
    slot: usize,
    state: &mut Slot,
) {
    let shared = shared.clone();
    let task_control = control.clone();
    let handle = tokio::spawn(async move { download_chunk_worker(shared, slot, task_control).await });
    state.abort = Some(handle.abort_handle());
    state.handle = Some(handle);
}

/// Runs the unfinished segments of a checkpointed segmented download.
/// Completed segments are skipped; a partially written segment is
/// re-downloaded whole. Network and server errors leave the checkpoint on
/// disk so the next call resumes from the completed segments.
///
/// Slice 8: the segments are the chunk plan (fresh downloads) or the
/// stored fixed layout (old checkpoints). `connections` worker slots pull
/// unfinished chunks from a shared queue, so the remaining work is
/// continuously re-split by availability instead of being fixed up
/// front; a worker that receives nothing for `stall_timeout` has its
/// chunk re-assigned to a fresh worker. Progress never goes backwards:
/// the reported byte count is a running maximum across re-queues.
async fn run_segmented_download(
    client: &Client,
    url: &str,
    target: &Path,
    part: &Path,
    checkpoint: &Arc<AsyncMutex<SegmentCheckpoint>>,
    total: u64,
    validator: Option<String>,
    session: Option<&SessionHeaders>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<SegmentedRun> {
    let (run, _stats) = run_segmented_download_with_stats(
        client, url, target, part, checkpoint, total, validator, session,
        control, rate_limiter, progress_tx, DEFAULT_STALL_TIMEOUT,
    )
    .await?;
    Ok(run)
}

#[allow(clippy::too_many_arguments)]
async fn run_segmented_download_with_stats(
    client: &Client,
    url: &str,
    target: &Path,
    part: &Path,
    checkpoint: &Arc<AsyncMutex<SegmentCheckpoint>>,
    total: u64,
    validator: Option<String>,
    session: Option<&SessionHeaders>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    stall_timeout: Duration,
) -> Result<(SegmentedRun, SegmentRunStats)> {
    let (ranges, worker_count) = {
        let guard = checkpoint.lock().await;
        let ranges: Vec<(u64, u64, bool)> = guard
            .metadata
            .segments
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|state| (state.start, state.end, state.done))
            .collect();
        let worker_count = guard
            .metadata
            .connections
            .unwrap_or(DEFAULT_RESUMABLE_CONNECTIONS)
            .clamp(1, 16);
        (ranges, worker_count)
    };
    let downloaded: u64 = ranges
        .iter()
        .filter(|(_, _, done)| *done)
        .map(|(start, end, _)| end - start + 1)
        .sum();
    let received = Arc::new(AtomicU64::new(downloaded));
    let reported = Arc::new(AtomicU64::new(downloaded));
    let last_report = Arc::new(Mutex::new(Instant::now()));
    let queue = Arc::new(Mutex::new(DynamicQueue {
        pending: ranges
            .iter()
            .enumerate()
            .filter(|(_, (_, _, done))| !*done)
            .map(|(index, _)| index)
            .collect(),
        stalls: vec![0; ranges.len()],
        current: vec![None; worker_count],
    }));
    let shared = Arc::new(SegmentWorkers {
        client: client.clone(),
        url: url.to_owned(),
        part: part.to_path_buf(),
        total,
        ranges: ranges.iter().map(|(start, end, _)| (*start, *end)).collect(),
        validator,
        session: session.cloned(),
        rate_limiter: rate_limiter.clone(),
        received: received.clone(),
        reported: reported.clone(),
        last_report,
        progress_tx: progress_tx.clone(),
        checkpoint: checkpoint.clone(),
        queue,
        chunk_received: ranges.iter().map(|_| Arc::new(AtomicU64::new(0))).collect(),
        activities: (0..worker_count).map(|_| Arc::new(Mutex::new(Instant::now()))).collect(),
        segments_done: Arc::new(AtomicU64::new(
            ranges.iter().filter(|(_, _, done)| *done).count() as u64,
        )),
        segments_total: ranges.len() as u64,
    });
    // Explicit JoinHandles instead of a JoinSet: the stall watchdog must
    // abort ONE worker without touching the others, and JoinSet can only
    // abort all. The coordinator polls every 100 ms — one sleeping task,
    // negligible cost for a download.
    let mut slots: Vec<Slot> = (0..worker_count)
        .map(|_| Slot { handle: None, abort: None, aborting_chunk: None })
        .collect();
    for slot in 0..worker_count {
        spawn_chunk_worker(&shared, &control, slot, &mut slots[slot]);
    }

    let mut run_stats = SegmentRunStats::default();
    let mut saw_paused = false;
    let mut saw_cancelled = false;
    let mut saw_validator_changed = false;
    let mut saw_interrupted = false;
    let mut first_error: Option<anyhow::Error> = None;
    loop {
        // Reap finished slots.
        for slot_index in 0..slots.len() {
            let finished = slots[slot_index]
                .handle
                .as_ref()
                .is_some_and(|handle| handle.is_finished());
            if !finished {
                continue;
            }
            let handle = slots[slot_index].handle.take().unwrap();
            slots[slot_index].abort = None;
            let was_aborting = slots[slot_index].aborting_chunk.take();
            match handle.await {
                Ok(result) => {
                    run_stats.workers.push(result.stats);
                    if let Some(error) = result.error {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    match result.terminal {
                        Some(SegmentOutcome::Paused) => saw_paused = true,
                        Some(SegmentOutcome::Cancelled) => saw_cancelled = true,
                        Some(SegmentOutcome::ValidatorChanged) => saw_validator_changed = true,
                        Some(SegmentOutcome::Interrupted) => saw_interrupted = true,
                        Some(SegmentOutcome::Completed) | None => {}
                    }
                }
                Err(join_error) => {
                    if join_error.is_cancelled() {
                        // A watchdog-aborted worker: its task is dead
                        // (the handle finished), so whatever it was
                        // doing can be safely re-queued — no writer owns
                        // it anymore. An abort from the terminal path
                        // below carries no chunk and needs no re-queue.
                        // F1: re-queue the slot's LIVE current chunk,
                        // taken at reap time — never the id captured at
                        // detection time. The worker may have completed
                        // that chunk and claimed the next one since the
                        // watchdog noticed the stall; re-queueing the
                        // stale id would drop the live chunk from both
                        // pending and current, losing it silently (the
                        // preallocated file's size check cannot detect
                        // a hole). If the worker died between chunks,
                        // current is None and nothing is re-queued.
                        if was_aborting.is_some() {
                            let live = {
                                let mut guard = shared.queue.lock().unwrap();
                                guard.current[slot_index].take()
                            };
                            if let Some(chunk) = live {
                                let partial =
                                    shared.chunk_received[chunk].swap(0, Ordering::Relaxed);
                                // fetch_sub is a single atomic RMW: it
                                // cannot fail spuriously. The swap(0)
                                // above guarantees each chunk's bytes are
                                // subtracted at most once, and only bytes
                                // previously added to `received` (both
                                // counters are incremented together, with
                                // no await between them — see R1). The
                                // debug_assert pins that ordering for
                                // future refactors: if it ever trips in a
                                // debug run, the pairing has been broken.
                                debug_assert!(partial <= received.load(Ordering::Relaxed));
                                received.fetch_sub(partial, Ordering::Relaxed);
                                run_stats.stall_requeues += 1;
                                let mut guard = shared.queue.lock().unwrap();
                                guard.pending.push_front(chunk);
                            }
                        }
                    } else if first_error.is_none() {
                        first_error = Some(join_error.into());
                    }
                }
            }
        }

        let terminating =
            saw_validator_changed || saw_cancelled || saw_interrupted || first_error.is_some()
                || *control.borrow() == DownloadControl::Cancelled;
        // F4: once the user asked for Pause, the watchdog stands down —
        // no aborts, no stall-limit errors, no replacement workers
        // issuing fresh Range requests while the run is draining. The
        // pausing workers exit on their own at the next chunk boundary.
        let paused = saw_paused || *control.borrow() == DownloadControl::Paused;
        if terminating {
            // One bad chunk decides the outcome for the whole download;
            // stop the rest instead of letting them write into a doomed
            // checkpoint. Pause needs no abort: every worker notices the
            // paused control at its next chunk boundary and stops on its
            // own.
            for slot in slots.iter_mut() {
                if let Some(abort) = slot.abort.take() {
                    abort.abort();
                }
            }
        } else if !paused {
            // Stall watchdog: a slot whose current chunk received
            // nothing for `stall_timeout` is stuck on a dead connection
            // (in addition to the client's 30s read timeout, this catches
            // stalled or trickling slots at `stall_timeout`). Abort that one worker,
            // re-queue its chunk for a fresh connection, and spawn a
            // replacement. A chunk that stalls too often is a real
            // failure: error out with the checkpoint preserved.
            let now = Instant::now();
            for slot_index in 0..slots.len() {
                if slots[slot_index].handle.is_none()
                    || slots[slot_index].aborting_chunk.is_some()
                {
                    continue;
                }
                let stalled_chunk = {
                    let guard = shared.queue.lock().unwrap();
                    guard.current[slot_index].filter(|_| {
                        shared.activities[slot_index]
                            .lock()
                            .map(|last| now.duration_since(*last) > stall_timeout)
                            .unwrap_or(false)
                    })
                };
                let Some(chunk) = stalled_chunk else { continue };
                // F1: re-validate immediately before aborting. The
                // worker may have completed this chunk and claimed the
                // next one since detection; aborting it now would kill
                // the next chunk for the old chunk's stall. (The reap
                // path re-queues the live current chunk, so even a
                // residual race cannot lose a chunk — this check only
                // avoids spurious aborts.)
                let still_stalled = {
                    let guard = shared.queue.lock().unwrap();
                    guard.current[slot_index] == Some(chunk)
                        && shared.activities[slot_index]
                            .lock()
                            .map(|last| now.duration_since(*last) > stall_timeout)
                            .unwrap_or(false)
                };
                if !still_stalled {
                    continue;
                }
                let stalls = {
                    let guard = shared.queue.lock().unwrap();
                    guard.stalls[chunk]
                };
                if stalls >= MAX_STALL_REQUEUES {
                    if first_error.is_none() {
                        first_error = Some(anyhow!(
                            "Download stalled repeatedly on one range; the connection is not delivering data. Partial data is preserved; Resume can retry."
                        ));
                    }
                    break;
                }
                {
                    let mut guard = shared.queue.lock().unwrap();
                    guard.stalls[chunk] += 1;
                }
                if let Some(abort) = slots[slot_index].abort.take() {
                    abort.abort();
                }
                slots[slot_index].aborting_chunk = Some(chunk);
            }
            // Replace slots emptied by the watchdog while work remains.
            // (A slot whose worker drained the queue exits with nothing
            // pending, so no replacement is needed there.)
            for slot_index in 0..slots.len() {
                if slots[slot_index].handle.is_some()
                    || slots[slot_index].aborting_chunk.is_some()
                {
                    continue;
                }
                let has_pending = {
                    let guard = shared.queue.lock().unwrap();
                    !guard.pending.is_empty()
                };
                if has_pending {
                    spawn_chunk_worker(&shared, &control, slot_index, &mut slots[slot_index]);
                }
            }
        }

        let all_idle = slots
            .iter()
            .all(|slot| slot.handle.is_none() && slot.aborting_chunk.is_none());
        let queue_empty = {
            let guard = shared.queue.lock().unwrap();
            guard.pending.is_empty()
        };
        // A terminating run ends once every slot is idle even if chunks
        // are still pending (they stay in the checkpoint for Resume).
        // P0-2: the break must use the live `paused` state, not only
        // `saw_paused` — when the watchdog aborts the last worker and
        // Pause arrives before the reap, no worker ever reports Paused
        // and the old condition looped forever.
        if all_idle && (queue_empty || paused || terminating) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Snapshot the shared received counter once, for every outcome,
    // so tests can assert it against the true total.
    run_stats.received = received.load(Ordering::Relaxed);

    // P0-2: the outcome reads the live control state, not only what a
    // worker happened to report — a watchdog-aborted worker never
    // reports Paused/Cancelled, so the reaped flags alone would fall
    // through to the size check and fail a run the user paused.
    let paused = saw_paused || *control.borrow() == DownloadControl::Paused;
    let cancelled = saw_cancelled || *control.borrow() == DownloadControl::Cancelled;

    if saw_validator_changed {
        return Ok((SegmentedRun::ValidatorChanged, run_stats));
    }
    if cancelled {
        remove_resume_files(
            part,
            &checkpoint.lock().await.metadata_path,
            target,
        )
        .await;
        return Ok((SegmentedRun::Finished(DownloadOutcome::Cancelled), run_stats));
    }
    if saw_interrupted {
        // N1: the control channel closed (host disconnected) — this is
        // NOT a user Cancel, so nothing is deleted. The error keeps
        // the part file and checkpoint on disk, and the host's own
        // disconnect path turns the failed run into Interrupted
        // ("partial checkpoint kept") instead of Cancelled. An
        // explicit Cancel above still wins over a racing disconnect.
        // Persist the final completion state first, as the Paused
        // tail does, so a chunk that finished while siblings were
        // being aborted is not lost.
        let guard = checkpoint.lock().await;
        let _ = write_resume_metadata(&guard.metadata_path, &guard.metadata).await;
        return Err(anyhow!(CONTROL_CLOSED_ERROR));
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if paused {
        // Persist the final completion state (completions are already
        // written as chunks finish; this covers a worker that finished
        // between its last mark and the abort of its siblings).
        let guard = checkpoint.lock().await;
        write_resume_metadata(&guard.metadata_path, &guard.metadata).await?;
        // Report the running maximum, not the post-subtraction count:
        // progress must never go backwards within a run.
        let downloaded = reported.load(Ordering::Relaxed);
        let _ = progress_tx.send(DownloadProgress {
            downloaded,
            total: Some(total),
            percent: Some(downloaded as f64 * 100.0 / total as f64),
            phase: "paused".into(),
            segments_done: Some(shared.segments_done.load(Ordering::Relaxed)),
            segments_total: Some(shared.segments_total),
        });
        return Ok((SegmentedRun::Finished(DownloadOutcome::Paused), run_stats));
    }
    let size = fs::metadata(part).await.map(|meta| meta.len()).unwrap_or(0);
    if size != total {
        return Err(anyhow!(
            "Segmented download ended at {size} bytes, but the server reported {total}. Partial data is preserved; Resume can retry."
        ));
    }
    // Defense in depth (F1): the part file is preallocated, so its size
    // can never prove coverage — confirm every chunk is marked done in
    // the checkpoint before committing. A lost chunk fails loudly here
    // instead of delivering wrong bytes as Completed; Resume re-fetches
    // it from the preserved checkpoint.
    {
        let guard = checkpoint.lock().await;
        if !all_segments_done(&guard.metadata) {
            return Err(anyhow!(
                "Segmented download ended with unfinished chunks; partial data is preserved, Resume can retry."
            ));
        }
    }
    replace_file(part, target).await?;
    let metadata_path = checkpoint.lock().await.metadata_path.clone();
    let _ = fs::remove_file(&metadata_path).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: total,
        total: Some(total),
        percent: Some(100.0),
        phase: "complete".into(),
        segments_done: Some(shared.segments_total),
        segments_total: Some(shared.segments_total),
    });
    Ok((SegmentedRun::Finished(DownloadOutcome::Completed), run_stats))
}

fn fresh_segmented_metadata(
    url: &str,
    total: u64,
    connections: usize,
    server: &ServerInfo,
    session: Option<&SessionHeaders>,
) -> ResumeMetadata {
    // Fresh segmented downloads use the dynamic chunk plan (Slice 8):
    // near-equal chunks, one per checkpoint entry, pulled work-stealing
    // style by the connection workers. The connection count is still
    // recorded — it is the worker count, not the chunk count.
    let segments = chunk_ranges(total)
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
        dynamic: Some(true),
        session: session.cloned(),
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
    _server: &ServerInfo,
    session: Option<&SessionHeaders>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    let mut total = metadata.total.unwrap_or(0);
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
            last_write: Instant::now(),
        }));
        match run_segmented_download(
            client,
            url,
            target,
            part,
            &checkpoint,
            total,
            validator,
            session,
            control.clone(),
            rate_limiter.clone(),
            progress_tx.clone(),
        )
        .await?
        {
            SegmentedRun::Finished(outcome) => return Ok(outcome),
            SegmentedRun::ValidatorChanged if attempt == 0 => {
                // M1: probe FIRST, delete only after the probe proves
                // the new object is usable (fresh total + range
                // support). The old code deleted the checkpoint before
                // re-probing, so a silent probe hard-errored with the
                // completed segments already gone. Now a silent probe
                // errors with the old checkpoint still on disk, and
                // Resume can retry later. The P0-03 restart itself is
                // unchanged: it still re-calls fetch_server_info and
                // still requires a fresh total and range support.
                let fresh_server = fetch_server_info(client, url, session, true).await;
                if fresh_server.is_webpage {
                    return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
                }
                let Some(fresh_total) = fresh_server.total else {
                    return Err(anyhow!(
                        "The download changed on the server and its new size is unknown; restart it to try again"
                    ));
                };
                if !fresh_server.supports_ranges {
                    return Err(anyhow!(
                        "The download changed on the server and no longer supports segmented resume; restart it to try again"
                    ));
                }
                let _ = fs::remove_file(part).await;
                let _ = fs::remove_file(metadata_path).await;
                total = fresh_total;
                metadata = fresh_segmented_metadata(url, total, connections, &fresh_server, session);
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
    mut saved: ResumeMetadata,
    server: &ServerInfo,
    session: Option<&SessionHeaders>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    let Some(total) = saved.total else {
        return Err(anyhow!(
            "Segmented checkpoint is missing the total size; restart the download"
        ));
    };
    let existing_len = tokio::fs::metadata(part).await.map(|m| m.len()).unwrap_or(0);
    if existing_len < total {
        let mut invalidated = false;
        if let Some(segments) = saved.segments.as_mut() {
            for seg in segments.iter_mut() {
                if seg.end >= existing_len && seg.done {
                    seg.done = false;
                    invalidated = true;
                }
            }
        }
        if invalidated {
            write_resume_metadata(metadata_path, &saved).await?;
        }
    }
    ensure_segmented_part(part, total).await?;
    run_segmented_with_restart(
        client,
        url,
        target,
        part,
        metadata_path,
        saved,
        server,
        session,
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
    session: Option<&SessionHeaders>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    let metadata = fresh_segmented_metadata(url, total, connections, server, session);
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
        session,
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
    session: Option<&SessionHeaders>,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    download_url_resumable_with_connections(
        url,
        target,
        session,
        DEFAULT_RESUMABLE_CONNECTIONS,
        control,
        rate_limiter,
        progress_tx,
    )
    .await
}

/// Connections used when the caller does not choose: 8 matches the
/// segmented fast path's usual setting and keeps the segment count small
/// enough that per-segment checkpoint writes stay cheap.
///
/// This is the engine's default connections value: the HLS and DASH
/// segment fetchers use it too (Slice 4), so direct files and media
/// segments all run at the same concurrency unless the caller passes an
/// explicit count. The desktop Settings value (Slice 7b) overrides it
/// for desktop downloads; the native host passes this default (it has
/// no settings channel). Do not add a second setting here.
pub const DEFAULT_RESUMABLE_CONNECTIONS: usize = 8;

/// Minimum object size for the segmented checkpointed path, mirroring the
/// fast path's threshold in `download_url`: segmenting a small object buys
/// nothing and costs one checkpoint write per segment.
const MIN_SEGMENTED_TOTAL: u64 = 2 * 1024 * 1024;

/// Same as `download_url_resumable`, with an explicit connection count.
/// Slice 7b: the desktop queue wires its setting through here, so the
/// value the Settings UI shows is the value the engine uses.
pub async fn download_url_resumable_with_connections(
    url: &str,
    target: &Path,
    session: Option<&SessionHeaders>,
    connections: usize,
    control: tokio::sync::watch::Receiver<DownloadControl>,
    rate_limiter: Arc<GlobalRateLimiter>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    crate::session::validate_public_http_url(url)?;
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

    // A resume reuses the handoff stored with the checkpoint when the
    // caller has none (F2). An explicit caller session always wins.
    let owned_session = session.is_none()
        .then(|| saved_metadata.as_ref().and_then(|saved| saved.session.clone()))
        .flatten();
    let session = session.or(owned_session.as_ref());

    let client = client()?;
    // One HEAD (+ a one-byte probe when HEAD is silent) for the whole call:
    // the segmented decision, the identity check and the sequential path all
    // share it, so the sequential path no longer issues its own HEAD.
    // A sequential checkpoint never segments, so skip the probe there.
    let probe = saved_metadata
        .as_ref()
        .map(|saved| saved.segments.is_some())
        .unwrap_or(true);
    let server = fetch_server_info(&client, url, session, probe).await;

    // F1: a webpage is never a download. Fail here — before the resume
    // branch, the fresh segmented branch, or the sequential path can
    // create part files or write bytes — so a large ranged page errors
    // with no file left behind. The segment fetchers carry the same
    // check as a backstop for servers whose HEAD omits Content-Type,
    // and for stale pre-fix checkpoints resumed below.
    if server.is_webpage {
        return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
    }

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
                    session,
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
                    session,
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
        session,
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
    session: Option<&SessionHeaders>,
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

    // Transient retry (MC3) for the opening request: a 429/5xx
    // answer — or a transport failure sending the request — is
    // retried with backoff instead of failing the run outright.
    // 401/403 fail fast in the status gate below (login wall), as
    // does any other non-transient status. The request is rebuilt
    // per attempt: a sent RequestBuilder cannot be reused.
    let mut response = {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
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
            let request = apply_session(session, request, url);
            // N1: the opening request can block up to the read
            // timeout, so the control channel is watched while it is
            // in flight — a disconnect must interrupt the run
            // promptly, not after 30 s. The send future is re-polled
            // across loop iterations (never re-issued).
            let mut send_fut = request.send();
            let send_result = loop {
                tokio::select! {
                    result = &mut send_fut => break result,
                    changed = control.changed() => {
                        if changed.is_err() {
                            return Err(anyhow!(CONTROL_CLOSED_ERROR));
                        }
                        // Copy the value out first: watch::Ref is
                        // !Send and must not live across the await
                        // in the Cancelled arm below.
                        let state = *control.borrow();
                        match state {
                            DownloadControl::Paused => {
                                return Ok(DownloadOutcome::Paused);
                            }
                            DownloadControl::Cancelled => {
                                remove_resume_files(&part, &metadata_path, target).await;
                                return Ok(DownloadOutcome::Cancelled);
                            }
                            DownloadControl::Running => {}
                        }
                    }
                }
            };
            match send_result {
                Ok(response)
                    if is_transient_http_status(response.status())
                        && attempt < MAX_SEGMENT_ATTEMPTS =>
                {
                    if let Some(outcome) = sequential_backoff(
                        &mut control,
                        transient_backoff(attempt - 1),
                        &part,
                        &metadata_path,
                        target,
                    )
                    .await
                    {
                        return outcome;
                    }
                    continue;
                }
                Ok(response) => break response,
                Err(error) => {
                    let wrapped = anyhow::Error::new(error);
                    if is_transient_segment_error(&wrapped)
                        && attempt < MAX_SEGMENT_ATTEMPTS
                    {
                        if let Some(outcome) = sequential_backoff(
                            &mut control,
                            transient_backoff(attempt - 1),
                            &part,
                            &metadata_path,
                            target,
                        )
                        .await
                        {
                            return outcome;
                        }
                        continue;
                    }
                    return Err(wrapped)
                        .context("Could not connect to the download server");
                }
            }
        }
    };
    if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
        if offset > 0 && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            let server_416_total = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().strip_prefix("bytes */"))
                .and_then(|n| n.trim().parse::<u64>().ok());
            // P1-1: a 416 commits the part file only when the server's
            // total is explicitly the resume offset AND the 416
            // response still identifies the same resource — a missing
            // Content-Range, or a changed ETag/Last-Modified, must
            // never mark stale bytes Completed.
            let total_matches = server_416_total == Some(offset);
            let response_etag = response
                .headers()
                .get(header::ETAG)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.trim_start().starts_with("W/"));
            let response_modified = response
                .headers()
                .get(header::LAST_MODIFIED)
                .and_then(|value| value.to_str().ok());
            let identity_matches = saved_metadata.as_ref().is_some_and(|saved| {
                resume_identity_matches(saved, url, response_etag, response_modified, Some(offset))
            });
            if total_matches && identity_matches {
                replace_file(&part, target).await?;
                let _ = fs::remove_file(&metadata_path).await;
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: offset,
                    total: Some(offset),
                    percent: Some(100.0),
                    phase: "complete".into(),
                    segments_done: None,
                    segments_total: None,
                });
                return Ok(DownloadOutcome::Completed);
            }
            let _ = fs::remove_file(&part).await;
            let _ = fs::remove_file(&metadata_path).await;
            return Err(anyhow!(
                "Resume offset ({offset} bytes) was rejected by the server (HTTP 416); cleared stale partial data so the next attempt starts fresh."
            ));
        }
        // A 401/403 is the login wall, not a generic server error —
        // say so instead of the bare status (Slice 5).
        if response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::FORBIDDEN {
            return Err(anyhow!(AUTH_REQUIRED_MESSAGE));
        }
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
            let restart = apply_session(session, client.get(url), url);
            response = restart.send().await.context("Could not restart a changed download")?.error_for_status().map_err(map_auth_error)?;
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

    // Fail fast on webpages: the GET response is a page, not media. This
    // runs after any If-Range restart above, so it sees the final response,
    // and before the checkpoint metadata is written or the part file is
    // opened, so no file is created. HLS/DASH downloads never reach here:
    // they go through inspect_video in video.rs, which already rejects
    // webpages with this same message.
    if response_is_webpage(response.headers()) {
        return Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE));
    }

    let start_offset = if append { offset } else { 0 };
    let response_etag = response.headers().get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim_start().starts_with("W/"))
        .map(str::to_owned)
        .or(head_etag);
    let response_modified = response.headers().get(header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok()).map(str::to_owned).or(head_modified);
    let mut total = if append {
        response.headers().get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_resume_content_range(value, offset).ok().flatten())
            .or(server_total)
    } else {
        // A Content-Length of 0 on the GET means "unknown" (same
        // chunked-server case as the HEAD probe); a genuinely empty file
        // still completes because an unknown total skips the check below.
        response.content_length().filter(|length| *length != 0).or(server_total)
    };

    let current_metadata = ResumeMetadata {
        url: url.to_owned(),
        etag: response_etag,
        last_modified: response_modified,
        total,
        // Sequential checkpoint: no segment layout.
        connections: None,
        segments: None,
        dynamic: None,
        session: session.cloned(),
    };
    write_resume_metadata(&metadata_path, &current_metadata).await?;
    saved_metadata = Some(current_metadata);

    let mut options = fs::OpenOptions::new();
    options.create(true).write(true);
    if append { options.append(true); } else { options.truncate(true); }
    let mut file = options.open(&part).await?;
    let mut stream = response.bytes_stream();
    let mut downloaded = start_offset;
    // Mid-stream retry budget: the opening request above retries on
    // its own counter; this counts stream attempts (initial stream
    // included) so a transient failure mid-body gets the same
    // MAX_SEGMENT_ATTEMPTS total as one segmented chunk.
    let mut stream_attempt: u32 = 1;
    let mut last_report = Instant::now();

    loop {
        if *control.borrow() == DownloadControl::Paused {
            file.flush().await?;
            let _ = progress_tx.send(DownloadProgress {
                downloaded, total, percent: total.map(|size| if size == 0 { 0.0 } else { downloaded as f64 * 100.0 / size as f64 }),
                phase: "paused".into(), segments_done: None, segments_total: None,
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
                // Control channel closed (host disconnected): NOT a
                // user Cancel — flush and error with the checkpoint
                // kept (N1).
                let _ = file.flush().await;
                drop(file);
                return Err(anyhow!(CONTROL_CLOSED_ERROR));
            }
        };
        let Some(chunk_result) = next else { break; };
        let chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(error) => {
                // Mid-stream transient retry (MC3, sequential path):
                // the opening request above retries, but until now a
                // failure after the first byte was terminal. A
                // transient stream error — a 429/5xx carried on the
                // error, or a transport failure such as a read timeout
                // — is retried with the same is_transient_segment_error
                // check and backoff as the segmented path, re-issuing
                // the GET from the preserved offset so bytes already
                // written are neither re-downloaded nor truncated: the
                // file stays open in append mode across the retry and
                // only `downloaded` moves. Non-transient errors still
                // fail fast with the checkpoint kept.
                let _ = file.flush().await;
                let wrapped = anyhow::Error::new(error);
                if !is_transient_segment_error(&wrapped)
                    || stream_attempt >= MAX_SEGMENT_ATTEMPTS
                {
                    return Err(wrapped)
                        .context("Network interrupted; partial file has been preserved for Resume");
                }
                if let Some(outcome) = sequential_backoff(
                    &mut control,
                    transient_backoff(stream_attempt - 1),
                    &part,
                    &metadata_path,
                    target,
                )
                .await
                {
                    return outcome;
                }
                stream_attempt += 1;
                // Re-issue the GET from the preserved offset. A
                // transient answer to the re-issue itself (a 429/5xx
                // status, or a transport failure sending it) consumes
                // another attempt and backs off again instead of
                // failing the run.
                loop {
                    let mut request = client.get(url);
                    request =
                        request.header(header::RANGE, format!("bytes={downloaded}-"));
                    if let Some(saved) = &saved_metadata {
                        if let Some(etag) = saved.etag.as_deref() {
                            request = request.header(header::IF_RANGE, etag);
                        } else if let Some(last_modified) = saved.last_modified.as_deref() {
                            request = request.header(header::IF_RANGE, last_modified);
                        }
                    }
                    let request = apply_session(session, request, url);
                    match request.send().await {
                        Ok(retry_response)
                            if is_transient_http_status(retry_response.status()) =>
                        {
                            if stream_attempt >= MAX_SEGMENT_ATTEMPTS {
                                return Err(anyhow!(
                                    "Download server returned HTTP {}",
                                    retry_response.status()
                                ));
                            }
                            if let Some(outcome) = sequential_backoff(
                                &mut control,
                                transient_backoff(stream_attempt - 1),
                                &part,
                                &metadata_path,
                                target,
                            )
                            .await
                            {
                                return outcome;
                            }
                            stream_attempt += 1;
                            continue;
                        }
                        Ok(retry_response) => {
                            match retry_response.status() {
                                StatusCode::PARTIAL_CONTENT => {
                                    let range = retry_response
                                        .headers()
                                        .get(header::CONTENT_RANGE)
                                        .and_then(|value| value.to_str().ok())
                                        .ok_or_else(|| {
                                            anyhow!("Server omitted Content-Range while resuming")
                                        })?;
                                    // The server may report a new total in
                                    // Content-Range: refresh it (and the
                                    // saved metadata) so the completion
                                    // check below compares against the
                                    // live version, not the opening one.
                                    if let Some(new_total) =
                                        parse_resume_content_range(range, downloaded)?
                                    {
                                        total = Some(new_total);
                                        if let Some(saved) = saved_metadata.as_mut() {
                                            saved.total = total;
                                            write_resume_metadata(&metadata_path, saved)
                                                .await?;
                                        }
                                    }
                                    stream = retry_response.bytes_stream();
                                }
                                StatusCode::OK => {
                                    // Server ignored Range (or If-Range
                                    // failed because the object changed):
                                    // restart from byte zero on this full
                                    // response instead of joining a new
                                    // version onto the old prefix. This is
                                    // a NEW version: refresh the total
                                    // from its Content-Length and the
                                    // saved identity/total metadata (plus
                                    // the metadata file) so the completion
                                    // check below compares against the new
                                    // version instead of the stale opening
                                    // total.
                                    total = retry_response
                                        .content_length()
                                        .filter(|length| *length != 0);
                                    let retry_etag = retry_response
                                        .headers()
                                        .get(header::ETAG)
                                        .and_then(|value| value.to_str().ok())
                                        .filter(|value| {
                                            !value.trim_start().starts_with("W/")
                                        })
                                        .map(str::to_owned);
                                    let retry_modified = retry_response
                                        .headers()
                                        .get(header::LAST_MODIFIED)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned);
                                    if let Some(saved) = saved_metadata.as_mut() {
                                        saved.etag = retry_etag;
                                        saved.last_modified = retry_modified;
                                        saved.total = total;
                                        write_resume_metadata(&metadata_path, saved).await?;
                                    }
                                    // `set_len(0)` truncates the file but
                                    // does NOT move the write offset: without
                                    // the seek below, the new version would
                                    // be written after a zero-filled hole
                                    // the size of the old prefix.
                                    file.set_len(0).await?;
                                    file.seek(SeekFrom::Start(0)).await?;
                                    downloaded = 0;
                                    stream = retry_response.bytes_stream();
                                }
                                StatusCode::RANGE_NOT_SATISFIABLE
                                    if total == Some(downloaded) =>
                                {
                                    // Nothing left to fetch: the part
                                    // file is already complete; the empty
                                    // body ends the stream and the
                                    // completion check below passes.
                                    stream = retry_response.bytes_stream();
                                }
                                status => {
                                    if status == StatusCode::UNAUTHORIZED
                                        || status == StatusCode::FORBIDDEN
                                    {
                                        return Err(anyhow!(AUTH_REQUIRED_MESSAGE));
                                    }
                                    return Err(anyhow!(
                                        "Download server returned HTTP {status}"
                                    ));
                                }
                            }
                            break;
                        }
                        Err(error) => {
                            let wrapped = anyhow::Error::new(error);
                            if !is_transient_segment_error(&wrapped)
                                || stream_attempt >= MAX_SEGMENT_ATTEMPTS
                            {
                                return Err(wrapped)
                                    .context("Could not connect to the download server");
                            }
                            if let Some(outcome) = sequential_backoff(
                                &mut control,
                                transient_backoff(stream_attempt - 1),
                                &part,
                                &metadata_path,
                                target,
                            )
                            .await
                            {
                                return outcome;
                            }
                            stream_attempt += 1;
                            continue;
                        }
                    }
                }
                continue;
            }
        };
        file.write_all(&chunk).await?;
        downloaded = downloaded.saturating_add(chunk.len() as u64);

        let wait = rate_limiter.reserve(chunk.len() as u64).await;
        if !wait.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                changed = control.changed() => {
                    if changed.is_err() {
                        // Host disconnected mid-wait: NOT a Cancel —
                        // keep the checkpoint (N1).
                        let _ = file.flush().await;
                        drop(file);
                        return Err(anyhow!(CONTROL_CLOSED_ERROR));
                    }
                }
            }
        }

        if last_report.elapsed() >= std::time::Duration::from_millis(160) {
            let percent = total.filter(|size| *size > 0)
                .map(|size| downloaded as f64 * 100.0 / size as f64);
            let _ = progress_tx.send(DownloadProgress {
                downloaded, total, percent, phase: "downloading".into(),
                segments_done: None, segments_total: None,
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
        downloaded, total: total.or(Some(downloaded)), percent: Some(100.0), phase: "complete".into(),
        segments_done: None, segments_total: None,
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
        download_segmented, download_url_resumable, partial_download_metadata_path, partial_download_path,
        reserve_download_path, ResumeMetadata, write_resume_metadata, DownloadControl, DownloadOutcome,
        GlobalRateLimiter, SessionHeaders, WEBPAGE_NOT_MEDIA_MESSAGE,
    };
    use crate::AUTH_REQUIRED_MESSAGE;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, Mutex},
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
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
        run_until_pause_with_session(url, target, None).await
    }

    async fn run_until_pause_with_session(url: &str, target: &std::path::Path, session: Option<SessionHeaders>) -> (DownloadOutcome, watch::Sender<DownloadControl>) {
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let limiter = Arc::new(GlobalRateLimiter::new());
        let url = url.to_string();
        let target = target.to_path_buf();
        let task_target = target.clone();
        let task = tokio::spawn(async move {
            download_url_resumable(&url, &task_target, session.as_ref(), control_rx, limiter, progress_tx).await
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
    async fn control_channel_close_keeps_checkpoint_instead_of_cancelling() {
        // N1: dropping the control sender mid-download (host
        // disconnected) is NOT a user Cancel — the run must error
        // with the checkpoint kept, and a later resume must still
        // complete with exact bytes.
        let (url, _version, shutdown, server) = spawn_range_server();
        let directory = std::env::temp_dir().join(format!("thunderload-control-close-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let url_for_run = url.clone();
        let target_for_run = target.clone();
        let task = tokio::spawn(async move {
            download_url_resumable(&url_for_run, &target_for_run, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx).await
        });
        let part = partial_download_path(&target);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let written = tokio::fs::metadata(&part).await.map(|metadata| metadata.len()).unwrap_or(0);
            if written > 0 { break; }
            assert!(tokio::time::Instant::now() < deadline, "download never wrote its first checkpoint bytes");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        drop(control_tx);
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("run hung after the control channel closed")
            .unwrap();
        assert!(result.is_err(), "a dropped control channel must error, not cancel");
        assert!(
            result.unwrap_err().to_string().contains("partial checkpoint kept"),
            "the error must say the checkpoint was kept"
        );
        assert!(tokio::fs::try_exists(&part).await.unwrap(), "part file must be kept");
        assert!(
            tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap(),
            "checkpoint metadata must be kept"
        );
        // Resume after the disconnect completes with exact bytes.
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'A'; BODY_SIZE]);
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
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

    /// A server whose object changes mid-download: the opening HEAD
    /// probe and the first GET see v1 (256 KiB, ETag "v1"), but the
    /// first GET is cut off halfway through the body — a mid-stream
    /// transport failure. Every later GET serves the whole of v2
    /// (384 KiB, ETag "v2") as a 200, because the retry's If-Range
    /// ("v1") no longer matches. Models a mid-stream transient
    /// failure followed by an object change.
    fn spawn_changing_midstream_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
        const V1_SIZE: usize = 256 * 1024;
        const V2_SIZE: usize = 384 * 1024;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let gets = Arc::new(AtomicUsize::new(0));
        let thread_gets = gets.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(3));
                        continue;
                    }
                    Err(_) => break,
                };
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            request.extend_from_slice(&buffer[..count]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let is_head = request
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .starts_with("HEAD ");
                if is_head {
                    // The opening probe always sees v1.
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {V1_SIZE}\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                let get_no = thread_gets.fetch_add(1, Ordering::Relaxed);
                if get_no == 0 {
                    // First GET: v1 headers, half the body, then drop
                    // the socket — a mid-stream transport failure.
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {V1_SIZE}\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    );
                    if stream.write_all(headers.as_bytes()).is_err() {
                        continue;
                    }
                    let _ = stream.write_all(&vec![b'A'; V1_SIZE / 2]);
                    drop(stream);
                    continue;
                }
                // Every later GET: the object changed, so the retry's
                // If-Range ("v1") no longer matches and the whole new
                // version is served as a 200 with a different total.
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {V2_SIZE}\r\nETag: \"v2\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                );
                if stream.write_all(headers.as_bytes()).is_err() {
                    continue;
                }
                let _ = stream.write_all(&vec![b'B'; V2_SIZE]);
            }
        });
        (format!("http://{address}/download.bin"), shutdown, handle)
    }

    #[tokio::test]
    async fn sequential_midstream_retry_restarting_on_changed_object_completes_with_new_total() {
        // MUST-FIX regression test: a mid-stream transient failure
        // whose retry GET returns 200 with a DIFFERENT total (the
        // object changed, so If-Range failed) must complete against
        // the NEW total. Before the fix the run downloaded the new
        // version byte-exact, then failed the end-of-function check
        // against the stale opening total — and Resume would have
        // re-downloaded everything from scratch.
        let (url, shutdown, server) = spawn_changing_midstream_server();
        let directory = std::env::temp_dir().join(format!(
            "thunderload-midstream-200-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("changed.bin"))
            .await
            .unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = tokio::time::timeout(
            Duration::from_secs(30),
            download_url_resumable(
                &url,
                &target,
                None,
                control_rx,
                Arc::new(GlobalRateLimiter::new()),
                progress_tx,
            ),
        )
        .await
        .expect("run hung after the mid-stream failure")
        .unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(
            bytes.len(),
            384 * 1024,
            "final file length must equal the new version's total"
        );
        assert!(
            bytes.iter().all(|byte| *byte == b'B'),
            "final file must be entirely the new version's bytes"
        );
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// A server that only serves its file to a request carrying the exact
    /// session cookie. Records every request's Cookie / Referer /
    /// User-Agent headers so the test can prove what the engine sent.
    fn spawn_cookie_gated_server() -> (String, Arc<Mutex<Vec<String>>>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_seen = seen.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let header = |name: &str| request.lines().find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
                }).unwrap_or_default();
                let cookie = header("cookie");
                let referer = header("referer");
                let user_agent = header("user-agent");
                thread_seen.lock().unwrap().push(format!("cookie={cookie}|referer={referer}|user-agent={user_agent}"));
                if cookie != "sessionid=abc123" {
                    let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                }
                let body = vec![b'C'; BODY_SIZE];
                let first = request.lines().next().unwrap_or_default();
                if first.starts_with("HEAD ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"gated-v1\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        BODY_SIZE
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                // Honor byte ranges (with If-Range) so a resume is a real
                // resume from the saved offset, not a restart.
                let range = header("range");
                let if_range = header("if-range");
                let requested_start = range
                    .strip_prefix("bytes=")
                    .and_then(|value| value.strip_suffix('-'))
                    .and_then(|value| value.parse::<usize>().ok());
                let can_resume = requested_start.is_some() && if_range.as_str() == "\"gated-v1\"";
                let start = if can_resume { requested_start.unwrap() } else { 0 };
                let payload = &body[start..];
                let (status, extra) = if can_resume {
                    ("206 Partial Content", format!("Content-Range: bytes {}-{}/{BODY_SIZE}\r\n", start, BODY_SIZE - 1))
                } else {
                    ("200 OK", String::new())
                };
                let headers = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: \"gated-v1\"\r\nAccept-Ranges: bytes\r\n{extra}Connection: close\r\n\r\n",
                    payload.len()
                );
                if stream.write_all(headers.as_bytes()).is_err() { continue; }
                for chunk in payload.chunks(1024) {
                    if stream.write_all(chunk).is_err() { break; }
                    thread::sleep(Duration::from_millis(3));
                }
            }
        });
        (format!("http://{address}/file.bin"), seen, shutdown, handle)
    }

    fn gated_session_for(url: &str) -> SessionHeaders {
        // The referer must be genuinely same-origin with the download
        // URL (same scheme/host/port): a bare "http://127.0.0.1/page"
        // is cross-origin with the server's ephemeral port, so the
        // same-origin Referer rule correctly withholds it.
        let referer = SessionHeaders::origin_of(url).map(|origin| format!("{origin}/page"));
        let session = SessionHeaders {
            cookies: Some("sessionid=abc123".to_string()),
            referer,
            user_agent: Some("ThunderLoad-Test/1.0".to_string()),
            origin: SessionHeaders::origin_of(url),
        };
        assert!(session.validate(url).is_ok(), "test session must validate for {url}");
        session
    }

    #[tokio::test]
    async fn session_cookies_and_headers_are_sent_for_a_gated_download() {
        let (url, seen, shutdown, server) = spawn_cookie_gated_server();
        let directory = std::env::temp_dir().join(format!("thunderload-session-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let session = gated_session_for(&url);
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, Some(&session), control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'C'; BODY_SIZE]);
        // Every request the server saw carried the cookie, the referer
        // and the browser user-agent (HEAD probe + GET).
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "server must have seen at least one request");
        for entry in seen.iter() {
            assert!(entry.contains("cookie=sessionid=abc123"), "request without the session cookie: {entry}");
            let expected_referer = format!("referer={}/page", SessionHeaders::origin_of(&url).unwrap());
            assert!(entry.contains(&expected_referer), "request without the referer: {entry}");
            assert!(entry.contains("user-agent=ThunderLoad-Test/1.0"), "request without the browser UA: {entry}");
        }
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// P1-2 (Referer): a cross-origin referer must be withheld even
    /// when the rest of the session applies — the cookie is still sent
    /// (same-origin) and the browser UA is still sent, but no request
    /// may carry the referer at all.
    #[tokio::test]
    async fn cross_origin_referer_is_withheld_for_a_gated_download() {
        let (url, seen, shutdown, server) = spawn_cookie_gated_server();
        let directory = std::env::temp_dir().join(format!("thunderload-session-xorigin-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let session = SessionHeaders {
            cookies: Some("sessionid=abc123".to_string()),
            referer: Some("http://example.com/page".to_string()),
            user_agent: Some("ThunderLoad-Test/1.0".to_string()),
            origin: SessionHeaders::origin_of(&url),
        };
        assert!(session.validate(&url).is_ok(), "test session must validate for {url}");
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, Some(&session), control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'C'; BODY_SIZE]);
        // The cookie and UA still went out (same-origin / unconditional),
        // but no request carried the cross-origin referer.
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "server must have seen at least one request");
        for entry in seen.iter() {
            assert!(entry.contains("cookie=sessionid=abc123"), "request without the session cookie: {entry}");
            assert!(entry.contains("referer=|"), "cross-origin referer leaked: {entry}");
            assert!(!entry.contains("example.com"), "cross-origin referer leaked: {entry}");
            assert!(entry.contains("user-agent=ThunderLoad-Test/1.0"), "request without the browser UA: {entry}");
        }
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn gated_download_fails_without_a_session() {
        let (url, _seen, shutdown, server) = spawn_cookie_gated_server();
        let directory = std::env::temp_dir().join(format!("thunderload-session-noauth-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await;
        assert!(result.is_err(), "a cookie-gated download without a session must fail, got {result:?}");
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Slice 5: the same gated failure must carry the login label, not
    /// a bare "Download server returned HTTP 403" — the user can act on
    /// the label (open the page logged in, retry), not on a status code.
    #[tokio::test]
    async fn gated_download_failure_carries_the_login_label() {
        let (url, _seen, shutdown, server) = spawn_cookie_gated_server();
        let directory = std::env::temp_dir().join(format!("thunderload-session-label-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let error = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap_err();
        assert_eq!(error.to_string(), AUTH_REQUIRED_MESSAGE);
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Slice 5 fix: a 403 on a parallel segment fetch (the session
    /// expired after the first response) must carry the login label,
    /// not "Server did not honor byte range" — the old error
    /// discarded the status, so the error_text funnel could not
    /// recover it. The server answers every request with 403; the
    /// segmented path is driven directly so the test proves the
    /// segment fetch itself, not the HEAD/probe path.
    fn spawn_forbidden_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            }
        });
        (format!("http://{address}/download.bin"), shutdown, handle)
    }

    #[tokio::test]
    async fn segmented_download_403_carries_the_login_label() {
        let (url, shutdown, server) = spawn_forbidden_server();
        let directory = std::env::temp_dir().join(format!("thunderload-segment-403-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let client = reqwest::Client::new();
        let error = download_segmented(
            &client, &url, &target, BODY_SIZE as u64, 2, None, None, progress_tx, Arc::new(GlobalRateLimiter::new()),
        ).await.unwrap_err();
        assert_eq!(error.to_string(), AUTH_REQUIRED_MESSAGE);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn resume_reuses_checkpoint_session_when_caller_has_none() {
        let (url, seen, shutdown, server) = spawn_cookie_gated_server();
        let directory = std::env::temp_dir().join(format!("thunderload-session-resume-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let session = gated_session_for(&url);
        // Phase 1: start with the session, pause mid-flight — the
        // checkpoint on disk now holds the handoff.
        let (outcome, _control) = run_until_pause_with_session(&url, &target, Some(session)).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        let part = partial_download_path(&target);
        let saved = tokio::fs::metadata(&part).await.unwrap().len();
        assert!(saved > 0 && saved < BODY_SIZE as u64);
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let requests_before_resume = seen.lock().unwrap().len();
        // Phase 2: resume with NO session — a desktop resume of a
        // browser-started job mints a fresh job id, so no stored session
        // is available. The checkpoint must supply it (F2): without the
        // fix the playlist/HEAD request goes out without cookies and the
        // server answers 403.
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'C'; BODY_SIZE]);
        // The resume requests carried the cookie too.
        let seen = seen.lock().unwrap();
        assert!(seen.len() > requests_before_resume, "resume must have issued requests");
        for entry in seen.iter().skip(requests_before_resume) {
            assert!(entry.contains("cookie=sessionid=abc123"), "resume request without the session cookie: {entry}");
        }
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Serves a complete object on HEAD, then answers every Range GET
    /// with 416. `include_content_range` controls whether the 416
    /// carries `Content-Range: bytes */{BODY_SIZE}` — the P1-1 commit
    /// gate needs the total stated explicitly.
    fn spawn_416_server(include_content_range: bool) -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let first = request.lines().next().unwrap_or_default().to_string();
                if first.starts_with("HEAD ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nContent-Type: application/octet-stream\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                let content_range = if include_content_range {
                    format!("Content-Range: bytes */{BODY_SIZE}\r\n")
                } else {
                    String::new()
                };
                let headers = format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nContent-Type: application/octet-stream\r\nETag: \"v1\"\r\n{content_range}Connection: close\r\n\r\n"
                );
                let _ = stream.write_all(headers.as_bytes());
            }
        });
        (format!("http://{address}/file.bin"), shutdown, handle)
    }

    /// Seeds a complete sequential checkpoint: a part file holding all
    /// BODY_SIZE bytes plus metadata naming the same object (ETag "v1",
    /// total BODY_SIZE). A resume of this checkpoint sends
    /// `Range: bytes={BODY_SIZE}-`, which a correct server answers 416.
    async fn seed_complete_checkpoint(target: &std::path::Path, url: &str) {
        let part = partial_download_path(target);
        let metadata_path = partial_download_metadata_path(target);
        tokio::fs::write(&part, vec![b'C'; BODY_SIZE]).await.unwrap();
        let metadata = ResumeMetadata {
            url: url.to_string(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            total: Some(BODY_SIZE as u64),
            connections: None,
            segments: None,
            dynamic: None,
            session: None,
        };
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();
    }

    /// P1-1: a 416 with no Content-Range must NOT commit the part —
    /// the server never stated its total, so the bytes may be stale.
    #[tokio::test]
    async fn resume_416_without_content_range_does_not_commit() {
        let (url, shutdown, server) = spawn_416_server(false);
        let directory = std::env::temp_dir().join(format!("thunderload-416-norange-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = directory.join("file.bin");
        seed_complete_checkpoint(&target, &url).await;
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("416"), "expected a 416 rejection, got {error}");
        assert!(!tokio::fs::try_exists(&target).await.unwrap(), "target must not be committed");
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap(), "stale part must be cleared");
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap(), "stale metadata must be cleared");
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// P1-1: a 416 that states `bytes */{BODY_SIZE}` with the same
    /// ETag commits the already-complete part file.
    #[tokio::test]
    async fn resume_416_with_matching_total_and_identity_commits() {
        let (url, shutdown, server) = spawn_416_server(true);
        let directory = std::env::temp_dir().join(format!("thunderload-416-commit-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = directory.join("file.bin");
        seed_complete_checkpoint(&target, &url).await;
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'C'; BODY_SIZE]);
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap(), "part must be consumed by the commit");
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap(), "metadata must be consumed by the commit");
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Serves a 302 to `target` for any GET (HEAD is answered directly),
    /// and records whether a Cookie header arrived. The target is a
    /// different origin (different port), so the Cookie header must not
    /// follow the redirect.
    fn spawn_redirect_server(target: String) -> (String, Arc<AtomicBool>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let saw_cookie = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_saw_cookie = saw_cookie.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let has_cookie = request.lines().any(|line| {
                    line.split_once(':').is_some_and(|(key, _)| key.eq_ignore_ascii_case("cookie"))
                });
                if has_cookie {
                    thread_saw_cookie.store(true, Ordering::Relaxed);
                }
                let first = request.lines().next().unwrap_or_default();
                if first.starts_with("HEAD ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        BODY_SIZE
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                let headers = format!("HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                let _ = stream.write_all(headers.as_bytes());
            }
        });
        (format!("http://{address}/file.bin"), saw_cookie, shutdown, handle)
    }

    /// Serves its file to any GET (HEAD answered directly) and records
    /// whether a Cookie header arrived.
    fn spawn_plain_file_server() -> (String, Arc<AtomicBool>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let saw_cookie = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_saw_cookie = saw_cookie.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let has_cookie = request.lines().any(|line| {
                    line.split_once(':').is_some_and(|(key, _)| key.eq_ignore_ascii_case("cookie"))
                });
                if has_cookie {
                    thread_saw_cookie.store(true, Ordering::Relaxed);
                }
                let first = request.lines().next().unwrap_or_default();
                if first.starts_with("HEAD ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        BODY_SIZE
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                let body = vec![b'R'; BODY_SIZE];
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    BODY_SIZE
                );
                if stream.write_all(headers.as_bytes()).is_err() { continue; }
                for chunk in body.chunks(1024) {
                    if stream.write_all(chunk).is_err() { break; }
                }
            }
        });
        (format!("http://{address}/file.bin"), saw_cookie, shutdown, handle)
    }

    #[tokio::test]
    async fn session_cookies_are_not_sent_on_cross_origin_redirect() {
        let (file_url, file_saw_cookie, file_shutdown, file_server) = spawn_plain_file_server();
        let (url, origin_saw_cookie, redirect_shutdown, redirect_server) = spawn_redirect_server(file_url);
        let directory = std::env::temp_dir().join(format!("thunderload-session-redirect-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let session = gated_session_for(&url);
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, Some(&session), control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, vec![b'R'; BODY_SIZE]);
        assert!(origin_saw_cookie.load(Ordering::Relaxed), "origin server must have seen the session cookie");
        assert!(!file_saw_cookie.load(Ordering::Relaxed), "redirect target (different origin) must NOT see the session cookie");
        drop(control_tx);
        redirect_shutdown.store(true, Ordering::Relaxed);
        file_shutdown.store(true, Ordering::Relaxed);
        redirect_server.join().unwrap();
        file_server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Mimics a chunked watch page (the YouTube case): HEAD reports
    /// Content-Length: 0 with no Accept-Ranges, and GET streams the body
    /// with Transfer-Encoding: chunked and no Content-Length. A Range
    /// probe GET is answered with the same 200 chunked body (no 206), so
    /// the probe yields no total either — exactly the live behaviour that
    /// used to leave the download believing the server reported 0.
    fn spawn_head_zero_chunked_server() -> (String, Vec<u8>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let body: Vec<u8> = (0..100_003_usize).map(|i| (i % 256) as u8).collect();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_body = body.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let first = request.lines().next().unwrap_or_default();
                if first.starts_with("HEAD ") {
                    let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                }
                let headers = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n";
                if stream.write_all(headers).is_err() { continue; }
                for chunk in thread_body.chunks(8192) {
                    if stream.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).is_err() { break; }
                    if stream.write_all(chunk).is_err() { break; }
                    if stream.write_all(b"\r\n").is_err() { break; }
                }
                let _ = stream.write_all(b"0\r\n\r\n");
            }
        });
        (format!("http://{address}/watch"), body, shutdown, handle)
    }

    /// Serves a webpage: `text/html; charset=utf-8` on GET.
    fn spawn_html_page_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let html = b"<!doctype html><html><head><title>Watch</title></head><body>not media</body></html>";
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let first = request.lines().next().unwrap_or_default();
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n",
                    html.len()
                );
                if stream.write_all(headers.as_bytes()).is_err() { continue; }
                if first.starts_with("HEAD ") { continue; }
                let _ = stream.write_all(html);
            }
        });
        (format!("http://{address}/watch"), shutdown, handle)
    }

    /// A webpage large enough to segment (Content-Length >= 2 MiB) that
    /// also advertises byte ranges — the exact shape F1 closed: without
    /// the HEAD webpage check this page was downloaded in parallel
    /// segments and saved as a "Completed" file.
    fn spawn_large_ranged_html_page_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
        const PAGE_SIZE: usize = 2 * 1024 * 1024 + 37;
        let mut page = b"<html><body>".to_vec();
        page.resize(PAGE_SIZE, b'a');
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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
                let first = request.lines().next().unwrap_or_default();
                let range = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("range").then(|| value.trim().to_string())
                });
                let bounds = range.as_deref()
                    .and_then(|value| value.strip_prefix("bytes="))
                    .and_then(|value| value.split_once('-'))
                    .and_then(|(start, end)| Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?)))
                    .filter(|(start, end)| *start <= *end && *end < PAGE_SIZE);
                let headers = if first.starts_with("HEAD ") {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {PAGE_SIZE}\r\nContent-Type: text/html; charset=utf-8\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    )
                } else if let Some((start, end)) = bounds {
                    format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{PAGE_SIZE}\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n",
                        end - start + 1
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {PAGE_SIZE}\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n"
                    )
                };
                if stream.write_all(headers.as_bytes()).is_err() { continue; }
                if first.starts_with("HEAD ") { continue; }
                let payload: &[u8] = match bounds {
                    Some((start, end)) => &page[start..=end],
                    None => &page,
                };
                let _ = stream.write_all(payload);
            }
        });
        (format!("http://{address}/watch"), shutdown, handle)
    }

    #[tokio::test]
    async fn head_content_length_zero_with_chunked_body_still_completes() {
        let (url, expected, shutdown, server) = spawn_head_zero_chunked_server();
        let directory = std::env::temp_dir().join(format!("thunderload-head-zero-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("page.bin")).await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn webpage_content_type_fails_fast_without_writing_a_file() {
        let (url, shutdown, server) = spawn_html_page_server();
        let directory = std::env::temp_dir().join(format!("thunderload-webpage-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("video.mp4")).await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let error = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap_err();
        assert_eq!(error.to_string(), WEBPAGE_NOT_MEDIA_MESSAGE);
        // Nothing was written: the target is still only the empty
        // reservation the test itself created, and neither the part file
        // nor the checkpoint sidecar exists.
        assert_eq!(tokio::fs::metadata(&target).await.unwrap().len(), 0);
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        drop(control_tx);
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn large_webpage_with_ranges_fails_fast_without_writing_a_file() {
        let (url, shutdown, server) = spawn_large_ranged_html_page_server();
        let directory = std::env::temp_dir().join(format!("thunderload-webpage-large-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("video.mp4")).await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let error = download_url_resumable(
            &url, &target, None, control_rx, Arc::new(GlobalRateLimiter::new()), progress_tx,
        ).await.unwrap_err();
        assert_eq!(error.to_string(), WEBPAGE_NOT_MEDIA_MESSAGE);
        // Nothing was written: the target is still only the empty
        // reservation the test itself created, and neither the part file
        // nor the checkpoint sidecar exists — the HEAD check fires before
        // the fresh segmented branch can create them.
        assert_eq!(tokio::fs::metadata(&target).await.unwrap().len(), 0);
        assert!(!tokio::fs::try_exists(partial_download_path(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
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

    /// Idle time must not bank burst allowance (Slice 7a, Gemini 15a):
    /// after 2 s idle at a 1 MB/s limit, reserving 1 MB must still cost
    /// ~1 s, not ~0 s. Before the fix the 2 s of idle banked 2 MB, so
    /// this reserve returned zero delay.
    #[tokio::test]
    async fn global_limiter_idle_time_cannot_bank_burst() {
        let limiter = GlobalRateLimiter::new();
        limiter.set_limit(1_000_000);
        // One byte starts the timeline; its own delay is ~0 either way.
        let _ = limiter.reserve(1).await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let wait = limiter.reserve(1_000_000).await;
        assert!(
            wait.as_millis() >= 700,
            "1 MB at a 1 MB/s limit after 2 s idle must cost ~1 s, got {wait:?}"
        );
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
            dynamic: None,
            session: None,
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
            dynamic: None,
            session: None,
        };
        assert!(resume_identity_matches(&saved, "https://example.test/file.bin", None, Some("Wed, 21 Oct 2015 07:28:00 GMT"), None));
        assert!(!resume_identity_matches(&saved, "https://example.test/file.bin", None, Some("Thu, 22 Oct 2015 07:28:00 GMT"), None));
    }
}

#[cfg(test)]
mod segmented_http_tests {
    use super::{
        client, download_segmented, download_url, probe_range_support, DownloadControl,
        DownloadProgress, GlobalRateLimiter, WEBPAGE_NOT_MEDIA_MESSAGE,
    };
    use crate::AUTH_REQUIRED_MESSAGE;
    use reqwest::Client;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc},
        thread,
        time::Duration,
    };
    use tokio::sync::{mpsc, watch};

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
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
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

    /// Same body as `spawn_segment_server`, but the HEAD response claims
    /// a binary Content-Type while ranged GETs arrive as text/html — the
    /// shape that reaches the segmented tasks with a silent HEAD. The
    /// segment-response check must still abort before any bytes are
    /// written.
    fn spawn_html_segment_server() -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let body = Arc::new((0..BODY_SIZE).map(|index| (index % 251) as u8).collect::<Vec<u8>>());
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
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
                let first = request.lines().next().unwrap_or_default();
                let range = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("range").then(|| value.trim().to_string())
                });
                let bounds = range.as_deref()
                    .and_then(|value| value.strip_prefix("bytes="))
                    .and_then(|value| value.split_once('-'))
                    .and_then(|(start, end)| Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?)))
                    .filter(|(start, end)| *start <= *end && *end < BODY_SIZE);
                if first.starts_with("HEAD ") {
                    // HEAD claims a binary file: the dispatcher's HEAD
                    // check cannot fire, so the request reaches the
                    // segmented tasks.
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n"
                    );
                    if stream.write_all(headers.as_bytes()).is_err() { continue; }
                    continue;
                }
                if let Some((start, end)) = bounds {
                    let payload = &body[start..=end];
                    let headers = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{BODY_SIZE}\r\nContent-Type: text/html; charset=utf-8\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                        payload.len()
                    );
                    if stream.write_all(headers.as_bytes()).is_err() { continue; }
                    let _ = stream.write_all(payload);
                } else {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nContent-Type: text/html; charset=utf-8\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n"
                    );
                    if stream.write_all(headers.as_bytes()).is_err() { continue; }
                    let _ = stream.write_all(&body);
                }
            }
        });
        (format!("http://{address}/file.bin"), shutdown, handle)
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
    async fn segmented_download_rejects_webpage_when_head_omits_content_type() {
        // F1 backstop: the HEAD response claims a binary file, so only
        // the segment-response check can catch this page. The one-shot
        // entry removes its reservation on error, so no file is left.
        let (url, shutdown, server) = spawn_html_segment_server();
        let directory = std::env::temp_dir().join(format!("thunderload-segment-html-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = directory.join("video.mp4");
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let error = download_url(
            &url, &target, 4, None, progress_tx, Arc::new(GlobalRateLimiter::new()),
            watch::channel(DownloadControl::Running).1,
        ).await.unwrap_err();
        assert_eq!(error.to_string(), WEBPAGE_NOT_MEDIA_MESSAGE);
        assert!(!tokio::fs::try_exists(&target).await.unwrap());
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
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
            watch::channel(DownloadControl::Running).1,
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

    // Slice 7b (Gemini claim 7): the connections value the caller passes
    // is the value the engine uses — 2 connections means exactly two
    // Range requests, not the old hardcoded 8. (Gemini claim 14) the
    // same download reports real per-part progress on the live channel.
    #[tokio::test]
    async fn download_url_with_two_connections_uses_two_ranges_and_reports_segments() {
        let (url, range_requests, shutdown, server) = spawn_segment_server();
        let directory = std::env::temp_dir().join(format!("thunderload-connections-two-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(progress) = progress_rx.recv().await {
                events.push(progress);
            }
            events
        });
        let path = directory.join("payload.bin");
        let result = download_url(&url, &path, 2, None, progress_tx, Arc::new(GlobalRateLimiter::new()), watch::channel(DownloadControl::Running).1).await;
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        result.unwrap();
        let events = collector.await.unwrap();
        assert_eq!(range_requests.load(Ordering::Relaxed), 2, "expected exactly two Range requests for connections=2");
        let bytes = tokio::fs::read(&path).await.unwrap();
        let expected: Vec<u8> = (0..BODY_SIZE).map(|index| (index % 251) as u8).collect();
        assert_eq!(bytes, expected);
        assert!(
            events.iter().any(|event| event.phase == "downloading" && event.segments_done.is_some() && event.segments_total == Some(2)),
            "a downloading event must report segments 0..2 of 2; events: {events:?}"
        );
        let last = events.last().expect("at least a complete event");
        assert_eq!(last.phase, "complete");
        assert_eq!((last.segments_done, last.segments_total), (Some(2), Some(2)), "the complete event must report all parts done");
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // Slice 7b (Gemini claim 14): a one-connection download is
    // sequential, so it honestly reports no segments — None, not a
    // fabricated 0 of 1.
    #[tokio::test]
    async fn download_url_with_one_connection_reports_no_segments() {
        let (url, range_requests, shutdown, server) = spawn_segment_server();
        let directory = std::env::temp_dir().join(format!("thunderload-connections-one-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(progress) = progress_rx.recv().await {
                events.push(progress);
            }
            events
        });
        let path = directory.join("payload.bin");
        let result = download_url(&url, &path, 1, None, progress_tx, Arc::new(GlobalRateLimiter::new()), watch::channel(DownloadControl::Running).1).await;
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
        result.unwrap();
        let events = collector.await.unwrap();
        assert_eq!(range_requests.load(Ordering::Relaxed), 0, "one connection must not issue Range requests");
        assert!(!events.is_empty(), "a sequential download still reports progress");
        assert!(
            events.iter().all(|event| event.segments_done.is_none() && event.segments_total.is_none()),
            "a sequential download must not claim segment progress; events: {events:?}"
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Serves HEAD as a large ranged object, 403s every Range GET, but
    /// answers a plain GET with the full body — the P1-7 guard must
    /// surface the login label instead of falling back to that plain
    /// GET, so the test also counts plain GETs and requires zero.
    fn spawn_range_forbidden_server() -> (String, Arc<AtomicUsize>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let plain_gets = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_plain_gets = plain_gets.clone();
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            let body: Vec<u8> = (0..BODY_SIZE).map(|index| (index % 251) as u8).collect();
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
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
                let header = |name: &str| request.lines().find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
                }).unwrap_or_default();
                let first = request.lines().next().unwrap_or_default().to_string();
                if first.starts_with("HEAD ") {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nContent-Type: application/octet-stream\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    );
                    let _ = stream.write_all(headers.as_bytes());
                    continue;
                }
                if !header("range").is_empty() {
                    let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                }
                thread_plain_gets.fetch_add(1, Ordering::Relaxed);
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {BODY_SIZE}\r\nContent-Type: application/octet-stream\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                );
                if stream.write_all(headers.as_bytes()).is_err() { continue; }
                for chunk in body.chunks(8192) {
                    if stream.write_all(chunk).is_err() { break; }
                }
            }
        });
        (format!("http://{address}/file.bin"), plain_gets, shutdown, handle)
    }

    /// P1-7: when the segmented path 403s but a plain GET would 200,
    /// `download_url` must surface the login label — not silently fall
    /// back to the plain GET, and not report a bare HTTP 403.
    #[tokio::test]
    async fn download_url_propagates_login_label_when_ranges_403() {
        let (url, plain_gets, shutdown, server) = spawn_range_forbidden_server();
        let directory = std::env::temp_dir().join(format!("thunderload-url-403-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let path = directory.join("file.bin");
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel::<DownloadProgress>();
        let error = download_url(
            &url, &path, 2, None, progress_tx, Arc::new(GlobalRateLimiter::new()),
            watch::channel(DownloadControl::Running).1,
        ).await.unwrap_err();
        assert_eq!(error.to_string(), AUTH_REQUIRED_MESSAGE);
        assert_eq!(plain_gets.load(Ordering::Relaxed), 0, "the sequential fallback must never issue a plain GET after a segmented auth failure");
        assert!(!tokio::fs::try_exists(&path).await.unwrap(), "no file must be left behind on a terminal auth failure");
        shutdown.store(true, Ordering::Relaxed);
        server.join().unwrap();
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
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                // Accepted sockets inherit the listener's non-blocking mode
                // on Windows: serve this connection blocking.
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
            super::chunk_ranges(SMALL_BODY as u64).len(),
            "expected exactly one Range request per chunk of the dynamic plan (Slice 8)"
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
        assert_eq!(segments.len(), super::chunk_ranges(LARGE_BODY as u64).len());
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

    // Slice 7b (Gemini claim 7): the connections value the caller passes
    // is stored in the checkpoint, so a resume reuses the count the
    // download started with instead of the caller's new value.
    #[tokio::test]
    async fn with_connections_two_is_stored_in_checkpoint() {
        let fixture = spawn_checkpoint_server(LARGE_BODY);
        fixture.slow.store(true, Ordering::Relaxed);
        let directory = temp_dir("seg-connections-stored");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let outcome = run_until_pause(&fixture.url, &target, 2).await;
        assert_eq!(outcome, DownloadOutcome::Paused);
        let metadata = read_resume_metadata(&partial_download_metadata_path(&target))
            .await
            .expect("checkpoint metadata missing");
        assert_eq!(metadata.connections, Some(2), "the checkpoint must store the connections the caller passed");
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
            dynamic: None,
            session: None,
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
            dynamic: None,
            session: None,
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
            super::chunk_ranges(LARGE_BODY as u64).len(),
            "changed ETag must restart with a fresh set of chunk requests (Slice 8)"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body_for(2, LARGE_BODY));
        assert!(!tokio::fs::try_exists(&part).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

#[cfg(test)]
mod dynamic_segmentation_tests {
    use super::{
        chunk_ranges, download_url_resumable_with_connections, dynamic_layout_matches,
        ensure_segmented_part, fresh_segmented_metadata, partial_download_metadata_path,
        partial_download_path, read_resume_metadata, reserve_download_path,
        run_segmented_download_with_stats, segment_ranges, write_resume_metadata,
        DownloadControl, DownloadOutcome, GlobalRateLimiter, ResumeMetadata, SegmentCheckpoint,
        SegmentState, SegmentedRun, ServerInfo,
    };
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::{Duration, Instant},
    };
    use tokio::sync::{mpsc, watch};

    const BODY_SIZE: usize = 8 * 1024 * 1024;

    fn body() -> Vec<u8> {
        body_sized(BODY_SIZE)
    }

    fn body_sized(size: usize) -> Vec<u8> {
        (0..size).map(|index| (index % 251) as u8).collect()
    }

    const SMALL_BODY: usize = 2 * 1024 * 1024 + 37;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("thunderload-dynamic-{name}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn chunk_plan_is_contiguous_near_equal_and_bounded() {
        for total in [0_u64, 1, 100, 256 * 1024, 2 * 1024 * 1024, 2 * 1024 * 1024 + 37, 8 * 1024 * 1024, 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024] {
            let ranges = chunk_ranges(total);
            if total == 0 {
                assert!(ranges.is_empty());
                continue;
            }
            assert!(!ranges.is_empty() && ranges.len() <= super::MAX_CHUNKS, "total {total}: {} chunks", ranges.len());
            let mut expected_start = 0_u64;
            let mut sizes = Vec::new();
            for (start, end) in &ranges {
                assert_eq!(*start, expected_start, "total {total}: gap or overlap at {start}");
                assert!(*end >= *start && *end < total, "total {total}: range {start}-{end} out of bounds");
                sizes.push(end - start + 1);
                expected_start = end + 1;
            }
            assert_eq!(expected_start, total, "total {total}: plan does not cover the file");
            let (min, max) = (*sizes.iter().min().unwrap(), *sizes.iter().max().unwrap());
            assert!(max - min <= 1, "total {total}: chunks are not near-equal ({min}..{max})");
            if total >= super::MIN_CHUNK_SIZE {
                assert!(min >= super::MIN_CHUNK_SIZE, "total {total}: a chunk is below the minimum ({min})");
            }
            assert_eq!(ranges.len(), ((total / super::MIN_CHUNK_SIZE) as usize).min(super::MAX_CHUNKS).max(1));
        }
        // The sizes the production path actually uses.
        assert_eq!(chunk_ranges(2 * 1024 * 1024 + 37).len(), 8);
        assert_eq!(chunk_ranges(8 * 1024 * 1024).len(), 32);
        // Every chunk of a just-over-a-multiple total stays at or above
        // the minimum (F3: the old ceiling count shrank them to 233,021 B).
        assert!(chunk_ranges(2 * 1024 * 1024 + 37).iter().all(|(start, end)| end - start + 1 >= super::MIN_CHUNK_SIZE));
    }

    fn metadata_with(segments: Vec<SegmentState>, total: u64, dynamic: Option<bool>) -> ResumeMetadata {
        ResumeMetadata {
            url: "https://example.test/file.bin".into(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            total: Some(total),
            connections: Some(8),
            segments: Some(segments),
            dynamic,
            session: None,
        }
    }

    #[test]
    fn dynamic_layout_matches_requires_full_coverage() {
        let total = 8 * 1024 * 1024_u64;
        let good: Vec<SegmentState> = chunk_ranges(total)
            .into_iter()
            .map(|(start, end)| SegmentState { start, end, done: false })
            .collect();
        assert!(dynamic_layout_matches(&metadata_with(good.clone(), total, Some(true))));
        // A gap: drop a middle chunk.
        let mut gapped = good.clone();
        gapped.remove(5);
        assert!(!dynamic_layout_matches(&metadata_with(gapped, total, Some(true))));
        // An overlap: extend one chunk into the next.
        let mut overlapped = good.clone();
        overlapped[3].end = overlapped[4].end;
        assert!(!dynamic_layout_matches(&metadata_with(overlapped, total, Some(true))));
        // Wrong total: the plan no longer covers the file.
        assert!(!dynamic_layout_matches(&metadata_with(good.clone(), total + 1, Some(true))));
        // Empty plan covers nothing.
        assert!(!dynamic_layout_matches(&metadata_with(Vec::new(), total, Some(true))));
        // The old fixed layout still validates through the shared entry
        // point when `dynamic` is absent — and a wrong connection count
        // still rejects it.
        let fixed: Vec<SegmentState> = segment_ranges(total, 8)
            .into_iter()
            .map(|(start, end)| SegmentState { start, end, done: false })
            .collect();
        assert!(super::segmented_layout_matches(&metadata_with(fixed.clone(), total, None)));
        let mut wrong_connections = metadata_with(fixed, total, None);
        wrong_connections.connections = Some(4);
        assert!(!super::segmented_layout_matches(&wrong_connections));
    }

    #[test]
    fn all_segments_done_is_the_commit_gate() {
        // F1 safety net: the predicate behind the pre-commit coverage
        // check. All done → true; one unfinished → false; empty list or
        // no list → false (an empty checkpoint must never commit).
        let done = |flags: &[bool]| {
            metadata_with(
                flags
                    .iter()
                    .enumerate()
                    .map(|(index, done)| SegmentState {
                        start: index as u64,
                        end: index as u64,
                        done: *done,
                    })
                    .collect(),
                3,
                Some(true),
            )
        };
        assert!(super::all_segments_done(&done(&[true, true, true])));
        assert!(!super::all_segments_done(&done(&[true, false, true])));
        assert!(!super::all_segments_done(&done(&[false, false, false])));
        assert!(!super::all_segments_done(&metadata_with(Vec::new(), 3, Some(true))));
        let mut no_segments = done(&[true]);
        no_segments.segments = None;
        assert!(!super::all_segments_done(&no_segments));
    }

    /// A keep-alive fixture server. `SlowFirstConnection` mode makes
    /// the first connection to issue a ranged GET drip its body (16 KiB
    /// per 50 ms ≈ 327 KB/s); every other connection serves at full
    /// localhost speed. That models one slow route among fast ones — the
    /// case fixed segmentation cannot adapt to. `StallOnce` mode hangs
    /// the first request for one range (until shutdown) and serves every
    /// later request fast: a connection that hangs once, then recovers.

    enum UnevenMode {
        SlowFirstConnection,
        StallOnce { start: usize },
        /// Answer the first N ranged GETs with 503, then serve
        /// normally — the transient-retry path must recover.
        FailFirstRangeGets { remaining: Arc<AtomicUsize> },
    }

    struct RangedFixture {
        url: String,
        range_requests: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
        handle: thread::JoinHandle<()>,
    }

    impl RangedFixture {
        fn stop(self) {
            self.shutdown.store(true, Ordering::Relaxed);
            self.handle.join().unwrap();
        }
    }

    fn spawn_uneven_server() -> RangedFixture {
        spawn_ranged_server(BODY_SIZE, UnevenMode::SlowFirstConnection)
    }

    fn spawn_stall_once_server(stall_start: usize) -> RangedFixture {
        spawn_ranged_server(BODY_SIZE, UnevenMode::StallOnce { start: stall_start })
    }

    fn spawn_flaky_server(failures: usize) -> RangedFixture {
        spawn_ranged_server(
            BODY_SIZE,
            UnevenMode::FailFirstRangeGets {
                remaining: Arc::new(AtomicUsize::new(failures)),
            },
        )
    }

    fn spawn_ranged_server(body_size: usize, mode: UnevenMode) -> RangedFixture {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let slow_taken = Arc::new(AtomicBool::new(false));
        let stalled_once = Arc::new(AtomicBool::new(false));
        let range_requests = Arc::new(AtomicUsize::new(0));
        let body = Arc::new(body_sized(body_size));
        let thread_shutdown = shutdown.clone();
        let thread_requests = range_requests.clone();
        let fail_remaining = match &mode {
            UnevenMode::FailFirstRangeGets { remaining } => Some(remaining.clone()),
            _ => None,
        };
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let body = body.clone();
                let slow_taken = slow_taken.clone();
                let stalled_once = stalled_once.clone();
                let shutdown = thread_shutdown.clone();
                let requests = thread_requests.clone();
                let fail_remaining = fail_remaining.clone();
                let mode_slow = matches!(mode, UnevenMode::SlowFirstConnection);
                let stall_start = match mode {
                    UnevenMode::StallOnce { start } => Some(start),
                    _ => None,
                };
                thread::spawn(move || {
                    // This connection is slow iff it is the first one to
                    // issue a ranged GET (SlowFirstConnection mode).
                    let mut slow_connection = false;
                    loop {
                        let mut raw = Vec::new();
                        let mut buffer = [0_u8; 1024];
                        let head_complete = loop {
                            match stream.read(&mut buffer) {
                                Ok(0) => return,
                                Ok(count) => {
                                    raw.extend_from_slice(&buffer[..count]);
                                    if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                                        break true;
                                    }
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    thread::sleep(Duration::from_millis(1));
                                }
                                Err(_) => return,
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
                                key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
                            })
                        };
                        let range = header("range");
                        let if_range = header("if-range");
                        if range.is_some() {
                            requests.fetch_add(1, Ordering::Relaxed);
                        }
                        if method == "HEAD" {
                            let headers = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {body_size}\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\n\r\n"
                            );
                            if stream.write_all(headers.as_bytes()).is_err() {
                                return;
                            }
                            continue;
                        }
                        let bounds = range.as_deref()
                            .and_then(|value| value.strip_prefix("bytes="))
                            .and_then(|value| value.split_once('-'))
                            .and_then(|(start, end)| {
                                Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                            });
                        let honored = bounds.is_some_and(|(start, end)| {
                            start <= end
                                && end < body_size
                                && (if_range.as_deref() == Some("\"v1\"") || if_range.is_none())
                        });
                        let (start, end) = if honored {
                            bounds.unwrap()
                        } else {
                            (0, body_size - 1)
                        };
                        // First ranged GET on this connection claims the
                        // slow slot (SlowFirstConnection mode only).
                        if range.is_some() && mode_slow && !slow_taken.swap(true, Ordering::Relaxed) {
                            slow_connection = true;
                        }
                        // StallOnce mode: hang the first request for the
                        // chosen range until the test shuts down.
                        if let Some(stall) = stall_start {
                            if start == stall && !stalled_once.swap(true, Ordering::Relaxed) {
                                while !shutdown.load(Ordering::Relaxed) {
                                    thread::sleep(Duration::from_millis(100));
                                }
                                return;
                            }
                        }
                        // FailFirstRangeGets mode: the first N ranged
                        // GETs answer 503, then the server recovers.
                        // HEADs (no Range) are never failed.
                        if range.is_some() {
                            if let Some(remaining) = &fail_remaining {
                                let mut current = remaining.load(Ordering::Relaxed);
                                while current > 0 {
                                    match remaining.compare_exchange_weak(
                                        current,
                                        current - 1,
                                        Ordering::Relaxed,
                                        Ordering::Relaxed,
                                    ) {
                                        Ok(_) => {
                                            let _ = stream.write_all(
                                                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                            );
                                            return;
                                        }
                                        Err(next) => current = next,
                                    }
                                }
                            }
                        }
                        let payload_len = end - start + 1;
                        let headers = if honored {
                            format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Length: {payload_len}\r\nETag: \"v1\"\r\nContent-Range: bytes {start}-{end}/{body_size}\r\n\r\n"
                            )
                        } else {
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {payload_len}\r\nETag: \"v1\"\r\n\r\n"
                            )
                        };
                        if stream.write_all(headers.as_bytes()).is_err() {
                            return;
                        }
                        let payload = &body[start..=end];
                        if slow_connection {
                            for chunk in payload.chunks(16 * 1024) {
                                if stream.write_all(chunk).is_err() {
                                    return;
                                }
                                thread::sleep(Duration::from_millis(50));
                            }
                        } else if stream.write_all(payload).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        RangedFixture { url: format!("http://{address}/file.bin"), range_requests, shutdown, handle }
    }

    async fn run_dynamic(
        url: &str,
        target: &std::path::Path,
        connections: usize,
        stall_timeout: Duration,
    ) -> (SegmentedRun, super::SegmentRunStats, Vec<super::DownloadProgress>) {
        let server = ServerInfo {
            total: Some(BODY_SIZE as u64),
            supports_ranges: true,
            etag: Some("\"v1\"".into()),
            last_modified: None,
            is_webpage: false,
        };
        let part = partial_download_path(target);
        let metadata_path = partial_download_metadata_path(target);
        let metadata = fresh_segmented_metadata(url, BODY_SIZE as u64, connections, &server, None);
        ensure_segmented_part(&part, BODY_SIZE as u64).await.unwrap();
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();
        let checkpoint = Arc::new(tokio::sync::Mutex::new(SegmentCheckpoint {
            metadata_path: metadata_path.clone(),
            metadata,
            last_write: Instant::now(),
        }));
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let client = super::client().unwrap();
        let (run, stats) = run_segmented_download_with_stats(
            &client,
            url,
            target,
            &part,
            &checkpoint,
            BODY_SIZE as u64,
            Some("\"v1\"".into()),
            None,
            control_rx,
            Arc::new(GlobalRateLimiter::new()),
            progress_tx,
            stall_timeout,
        )
        .await
        .unwrap();
        drop(control_tx);
        let mut events = Vec::new();
        while let Ok(event) = progress_rx.try_recv() {
            events.push(event);
        }
        (run, stats, events)
    }

    #[tokio::test]
    async fn uneven_connection_speeds_share_work() {
        let fixture = spawn_uneven_server();
        let directory = temp_dir("uneven");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let started = Instant::now();
        let (run, stats, _events) =
            run_dynamic(&fixture.url, &target, 8, Duration::from_secs(30)).await;
        let elapsed = started.elapsed();
        assert!(matches!(run, SegmentedRun::Finished(DownloadOutcome::Completed)), "run: {run:?}");
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body());
        // 32 chunks shared across 8 workers: the slow connection's worker
        // finishes ~1 chunk while the fast workers drain the rest, so the
        // per-worker counts must skew. Fixed segmentation (one 1 MiB
        // segment on the slow connection at 327 KB/s) cannot finish
        // faster than ~3.2 s; the dynamic run must beat that bound.
        let total_chunks: usize = stats.workers.iter().map(|worker| worker.chunks_completed).sum();
        assert_eq!(total_chunks, 32, "every chunk must be completed exactly once");
        let min_chunks = stats.workers.iter().map(|worker| worker.chunks_completed).min().unwrap();
        let max_chunks = stats.workers.iter().map(|worker| worker.chunks_completed).max().unwrap();
        assert!(min_chunks <= 2, "slow worker must not be stuck with a large share; min was {min_chunks}");
        assert!(max_chunks >= 4, "fast workers must pick up the slack; max was {max_chunks}");
        for worker in &stats.workers {
            if worker.chunks_completed > 0 {
                assert!(worker.ewma_bytes_per_sec > 0.0, "throughput must be measured per worker");
            }
        }
        // The speed claim is relative, not an absolute wall-clock bound:
        // a fixed 1 MiB segment on the slow connection needs
        // 1 MiB / (that connection's measured rate), and the dynamic run
        // must beat it. The old absolute 900 ms bound was calibrated on
        // a fast VM and fails on slow CI runners — the macOS runner took
        // ~2 s for the same run while the whole suite ran ~5x slower —
        // even though work-sharing itself was correct (skew assertions
        // above all passed there).
        let slow_rate = stats
            .workers
            .iter()
            .filter(|worker| worker.chunks_completed > 0)
            .map(|worker| worker.ewma_bytes_per_sec)
            .fold(f64::INFINITY, f64::min);
        assert!(
            slow_rate.is_finite() && slow_rate > 0.0,
            "slow connection's measured rate must be positive"
        );
        let fixed_floor = Duration::from_secs_f64(1_048_576.0 / slow_rate);
        assert!(
            elapsed < fixed_floor,
            "dynamic run took {elapsed:?}; a fixed 1 MiB segment on the slow connection at {slow_rate:.0} B/s needs {fixed_floor:?}"
        );
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn stall_watchdog_reassigns_hung_chunk_and_completes() {
        // Chunk index 3's start, from the plan itself; all 8 workers
        // claim a chunk at start, so this range is in flight from the
        // beginning.
        let stall_start = chunk_ranges(BODY_SIZE as u64)[3].0 as usize;
        let fixture = spawn_stall_once_server(stall_start);
        let directory = temp_dir("stall");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let started = Instant::now();
        let (run, stats, events) =
            run_dynamic(&fixture.url, &target, 8, Duration::from_secs(1)).await;
        let elapsed = started.elapsed();
        assert!(matches!(run, SegmentedRun::Finished(DownloadOutcome::Completed)), "run: {run:?}");
        assert!(stats.stall_requeues >= 1, "watchdog must have re-queued the hung chunk");
        assert!(elapsed < Duration::from_secs(10), "run took {elapsed:?}");
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body());
        // Progress never goes backwards, even across the re-queue
        // subtraction, and never exceeds the total.
        let mut previous = 0_u64;
        for event in &events {
            assert!(
                event.downloaded >= previous,
                "progress went backwards: {} after {previous}",
                event.downloaded
            );
            assert!(event.downloaded <= BODY_SIZE as u64, "progress exceeded the total");
            previous = event.downloaded;
        }
        assert_eq!(previous, BODY_SIZE as u64, "final progress must reach the total");
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn pause_racing_watchdog_abort_returns_paused_not_a_hang() {
        // P0-2: one worker, one chunk stalled forever (until shutdown).
        // The watchdog aborts the worker at the 1 s stall timeout and
        // Pause arrives just after — before the reap, or just after it.
        // Either ordering must end Paused: on the old coordinator the
        // break condition only saw `saw_paused`, which a
        // watchdog-aborted worker never sets, so the loop ran forever.
        // The timeout turns a regression into a test failure instead of
        // hanging the suite.
        let stall_start = chunk_ranges(BODY_SIZE as u64)[0].0 as usize;
        let fixture = spawn_stall_once_server(stall_start);
        let directory = temp_dir("pause-watchdog-race");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let server = ServerInfo {
            total: Some(BODY_SIZE as u64),
            supports_ranges: true,
            etag: Some("\"v1\"".into()),
            last_modified: None,
            is_webpage: false,
        };
        let part = partial_download_path(&target);
        let metadata_path = partial_download_metadata_path(&target);
        let metadata =
            fresh_segmented_metadata(&fixture.url, BODY_SIZE as u64, 1, &server, None);
        ensure_segmented_part(&part, BODY_SIZE as u64).await.unwrap();
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();
        let checkpoint = Arc::new(tokio::sync::Mutex::new(SegmentCheckpoint {
            metadata_path: metadata_path.clone(),
            metadata,
            last_write: Instant::now(),
        }));
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let client = super::client().unwrap();
        let pauser = {
            let control_tx = control_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1100)).await;
                let _ = control_tx.send(DownloadControl::Paused);
            })
        };
        let run = tokio::time::timeout(
            Duration::from_secs(15),
            run_segmented_download_with_stats(
                &client,
                &fixture.url,
                &target,
                &part,
                &checkpoint,
                BODY_SIZE as u64,
                Some("\"v1\"".into()),
                None,
                control_rx,
                Arc::new(GlobalRateLimiter::new()),
                progress_tx,
                Duration::from_secs(1),
            ),
        )
        .await;
        pauser.abort();
        drop(control_tx);
        let (run, _stats) = run
            .expect("coordinator must return instead of looping forever")
            .unwrap();
        assert!(
            matches!(run, SegmentedRun::Finished(DownloadOutcome::Paused)),
            "run: {run:?}"
        );
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn transient_503_chunks_are_retried_and_complete() {
        // MC3: the first two ranged GETs answer 503. Before the fix
        // that was a terminal error; now the affected chunks retry
        // with backoff on fresh connections and the run completes
        // with exact bytes.
        let fixture = spawn_flaky_server(2);
        let directory = temp_dir("flaky-503");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let before = fixture.range_requests.load(Ordering::Relaxed);
        let (run, stats, _events) =
            run_dynamic(&fixture.url, &target, 8, Duration::from_secs(30)).await;
        assert!(matches!(run, SegmentedRun::Finished(DownloadOutcome::Completed)), "run: {run:?}");
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, body());
        // Rollback proof on the real counter, not the plan: the
        // shared received counter must equal the true byte count
        // after the retries. Bytes counted twice across a retried
        // attempt would leave it above the total even though the
        // file itself is byte-exact.
        assert_eq!(
            stats.received,
            BODY_SIZE as u64,
            "shared received counter must equal the true byte count after the 503 retries"
        );
        assert!(
            fixture.range_requests.load(Ordering::Relaxed) - before
                > chunk_ranges(BODY_SIZE as u64).len(),
            "the 503s must have cost extra (retried) range requests"
        );
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn control_channel_close_keeps_segmented_checkpoint() {
        // N1: dropping the control sender mid-run (host disconnected)
        // is NOT a user Cancel — the run must error with the
        // checkpoint kept, never delete the resume state. One chunk
        // is stalled for the whole run so the download is guaranteed
        // still in flight when the channel closes.
        let stall_start = chunk_ranges(BODY_SIZE as u64)[3].0 as usize;
        let fixture = spawn_stall_once_server(stall_start);
        let directory = temp_dir("control-close-seg");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let server = ServerInfo {
            total: Some(BODY_SIZE as u64),
            supports_ranges: true,
            etag: Some("\"v1\"".into()),
            last_modified: None,
            is_webpage: false,
        };
        let part = partial_download_path(&target);
        let metadata_path = partial_download_metadata_path(&target);
        let metadata =
            fresh_segmented_metadata(&fixture.url, BODY_SIZE as u64, 8, &server, None);
        ensure_segmented_part(&part, BODY_SIZE as u64).await.unwrap();
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();
        let checkpoint = Arc::new(tokio::sync::Mutex::new(SegmentCheckpoint {
            metadata_path: metadata_path.clone(),
            metadata,
            last_write: Instant::now(),
        }));
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let client = super::client().unwrap();
        let url = fixture.url.clone();
        let part_for_run = part.clone();
        let run = tokio::spawn(async move {
            run_segmented_download_with_stats(
                &client,
                &url,
                &target,
                &part_for_run,
                &checkpoint,
                BODY_SIZE as u64,
                Some("\"v1\"".into()),
                None,
                control_rx,
                Arc::new(GlobalRateLimiter::new()),
                progress_tx,
                Duration::from_secs(30),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(control_tx);
        let result = tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("run hung after the control channel closed")
            .unwrap();
        assert!(result.is_err(), "a dropped control channel must error, not cancel");
        assert!(
            result.unwrap_err().to_string().contains("partial checkpoint kept"),
            "the error must say the checkpoint was kept"
        );
        assert!(
            tokio::fs::try_exists(&part).await.unwrap(),
            "part file must be kept"
        );
        assert!(
            tokio::fs::try_exists(&metadata_path).await.unwrap(),
            "checkpoint metadata must be kept"
        );
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn old_checkpoint_json_without_dynamic_field_resumes_with_exact_bytes() {
        // The old wire format: serialize a checkpoint with `dynamic:
        // None` — `skip_serializing_if` omits the key entirely, exactly
        // like a Slice 1-7 build wrote it — then resume through the real
        // dispatcher and prove the fixed layout still completes.
        let fixture = spawn_ranged_server(SMALL_BODY, UnevenMode::SlowFirstConnection);
        let directory = temp_dir("old-json");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let ranges = segment_ranges(SMALL_BODY as u64, 4);
        let expected = body_sized(SMALL_BODY);
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
            dynamic: None,
            session: None,
        };
        let json = serde_json::to_string(&metadata).unwrap();
        assert!(!json.contains("dynamic"), "old wire format must not carry the new key: {json}");
        let metadata_path = partial_download_metadata_path(&target);
        tokio::fs::write(&metadata_path, json.as_bytes()).await.unwrap();
        // The file on disk parses back with `dynamic: None`.
        let reparsed = read_resume_metadata(&metadata_path).await.unwrap();
        assert_eq!(reparsed.dynamic, None);

        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable_with_connections(
            &fixture.url,
            &target,
            None,
            4,
            control_rx,
            Arc::new(GlobalRateLimiter::new()),
            progress_tx,
        )
        .await
        .unwrap();
        drop(control_tx);
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn dynamic_checkpoint_resume_requests_only_unfinished_chunks() {
        // A paused dynamic download resumes at chunk granularity: done
        // chunks are never re-requested.
        let fixture = spawn_ranged_server(SMALL_BODY, UnevenMode::SlowFirstConnection);
        let directory = temp_dir("dynamic-resume");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let ranges = chunk_ranges(SMALL_BODY as u64);
        assert_eq!(ranges.len(), 8);
        let expected = body_sized(SMALL_BODY);
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
            dynamic: Some(true),
            session: None,
        };
        let metadata_path = partial_download_metadata_path(&target);
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();

        let before = fixture.range_requests.load(Ordering::Relaxed);
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_url_resumable_with_connections(
            &fixture.url,
            &target,
            None,
            4,
            control_rx,
            Arc::new(GlobalRateLimiter::new()),
            progress_tx,
        )
        .await
        .unwrap();
        drop(control_tx);
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert_eq!(
            fixture.range_requests.load(Ordering::Relaxed) - before,
            6,
            "resume must request only the 6 unfinished chunks"
        );
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        fixture.stop();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

#[cfg(test)]
mod mark_coalesce_tests {
    use super::{
        mark_segment_done, partial_download_metadata_path, read_resume_metadata,
        reserve_download_path, write_resume_metadata, ResumeMetadata, SegmentCheckpoint,
        SegmentState,
    };
    use std::time::Instant;

    #[tokio::test]
    async fn checkpoint_marks_coalesce_until_all_done() {
        // F6: a non-final mark inside the throttle window must not touch
        // the file; the mark that completes the last chunk must flush.
        // No timing dependence: the checkpoint was just constructed, so
        // every mark here is inside the 500 ms window.
        let directory = std::env::temp_dir().join(format!("thunderload-coalesce-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let metadata_path = partial_download_metadata_path(&target);
        let metadata = ResumeMetadata {
            url: "https://example.test/file.bin".into(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            total: Some(3),
            connections: Some(1),
            segments: Some(vec![
                SegmentState { start: 0, end: 0, done: false },
                SegmentState { start: 1, end: 1, done: false },
                SegmentState { start: 2, end: 2, done: false },
            ]),
            dynamic: Some(true),
            session: None,
        };
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();
        let checkpoint = std::sync::Arc::new(tokio::sync::Mutex::new(SegmentCheckpoint {
            metadata_path: metadata_path.clone(),
            metadata,
            last_write: Instant::now(),
        }));
        let before = tokio::fs::read(&metadata_path).await.unwrap();
        mark_segment_done(&checkpoint, 0).await.unwrap();
        assert_eq!(tokio::fs::read(&metadata_path).await.unwrap(), before, "non-final mark must not rewrite the file");
        mark_segment_done(&checkpoint, 1).await.unwrap();
        assert_eq!(tokio::fs::read(&metadata_path).await.unwrap(), before, "non-final mark must not rewrite the file");
        mark_segment_done(&checkpoint, 2).await.unwrap();
        let after = read_resume_metadata(&metadata_path).await.unwrap();
        assert!(after.segments.unwrap().iter().all(|state| state.done), "final mark must flush all done flags");
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

#[cfg(test)]
mod mark_coalesce_expiry_tests {
    use super::{
        mark_segment_done, partial_download_metadata_path, read_resume_metadata,
        reserve_download_path, write_resume_metadata, ResumeMetadata, SegmentCheckpoint,
        SegmentState,
    };
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn expired_throttle_window_flushes_non_final_mark() {
        // R4: the 500 ms throttle-expiry branch — a non-final mark must
        // still reach the file once the window has expired. No timing
        // dependence: the checkpoint is constructed with last_write
        // already 600 ms in the past.
        let directory = std::env::temp_dir().join(format!("thunderload-coalesce-exp-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("file.bin")).await.unwrap();
        let metadata_path = partial_download_metadata_path(&target);
        let metadata = ResumeMetadata {
            url: "https://example.test/file.bin".into(),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            total: Some(3),
            connections: Some(1),
            segments: Some(vec![
                SegmentState { start: 0, end: 0, done: false },
                SegmentState { start: 1, end: 1, done: false },
                SegmentState { start: 2, end: 2, done: false },
            ]),
            dynamic: Some(true),
            session: None,
        };
        write_resume_metadata(&metadata_path, &metadata).await.unwrap();
        let checkpoint = std::sync::Arc::new(tokio::sync::Mutex::new(SegmentCheckpoint {
            metadata_path: metadata_path.clone(),
            metadata,
            last_write: Instant::now() - Duration::from_millis(600),
        }));
        mark_segment_done(&checkpoint, 0).await.unwrap();
        let flushed = read_resume_metadata(&metadata_path).await.unwrap();
        let segments = flushed.segments.unwrap();
        assert!(segments[0].done, "expired window must flush the mark");
        assert!(!segments[1].done && !segments[2].done, "only the marked chunk is done");
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

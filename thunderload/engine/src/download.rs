use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use serde::Serialize;
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex}, time::Instant};
use tokio::{fs, io::AsyncWriteExt, sync::mpsc};
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
        let total = head.as_ref().filter(|response| response.status().is_success())
            .and_then(|response| response.content_length());
        let supports_ranges = head.as_ref().filter(|response| response.status().is_success()).map(|response| {
            response.headers().get(header::ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false)
        }).unwrap_or(false);

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
    fn filename_sanitization_removes_windows_path_separators() {
        assert_eq!(safe_filename(r"..\\folder/name?.zip", "fallback"), "__folder_name_.zip");
        assert_eq!(safe_filename("   ", "fallback"), "fallback");
    }
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
    speed_limit: Arc<AtomicU64>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadOutcome> {
    if !matches!(url::Url::parse(url)?.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS download URLs are supported"));
    }
    prepare_parent(target).await?;
    let part = partial_download_path(target);
    let offset = fs::metadata(&part).await.map(|meta| meta.len()).unwrap_or(0);

    if *control.borrow() == DownloadControl::Cancelled {
        let _ = fs::remove_file(&part).await;
        let _ = fs::remove_file(target).await;
        return Ok(DownloadOutcome::Cancelled);
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok(DownloadOutcome::Paused);
    }

    let client = client()?;
    let mut head = client.head(url);
    if let Some(referrer) = referer { head = head.header(header::REFERER, referrer); }
    let head_response = head.send().await.ok();
    let server_total = head_response.as_ref()
        .filter(|response| response.status().is_success())
        .and_then(|response| response.content_length());

    let mut request = client.get(url);
    if offset > 0 { request = request.header(header::RANGE, format!("bytes={offset}-")); }
    if let Some(referrer) = referer { request = request.header(header::REFERER, referrer); }
    let response = request.send().await.context("Could not connect to the download server")?;
    if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(anyhow!("Download server returned HTTP {}", response.status()));
    }

    let append = if offset > 0 && response.status() == StatusCode::PARTIAL_CONTENT {
        let range = response.headers().get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow!("Server omitted Content-Range while resuming"))?;
        parse_resume_content_range(range, offset)?;
        true
    } else {
        // The server ignored Range or no checkpoint exists. Restart from byte 0
        // using this same response rather than appending a full copy to the part.
        false
    };

    let start_offset = if append { offset } else { 0 };
    if !append && offset > 0 {
        let _ = fs::remove_file(&part).await;
    }
    let total = if append {
        response.headers().get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_resume_content_range(value, offset).ok().flatten())
            .or(server_total)
    } else {
        response.content_length().or(server_total)
    };

    let mut options = fs::OpenOptions::new();
    options.create(true).write(true);
    if append { options.append(true); } else { options.truncate(true); }
    let mut file = options.open(&part).await?;
    let mut stream = response.bytes_stream();
    let started = Instant::now();
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
            let _ = fs::remove_file(&part).await;
            let _ = fs::remove_file(target).await;
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

        let limit = speed_limit.load(Ordering::Relaxed);
        if limit > 0 {
            let expected = std::time::Duration::from_secs_f64(downloaded.saturating_sub(start_offset) as f64 / limit as f64);
            let wait = expected.saturating_sub(started.elapsed());
            if !wait.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {},
                    changed = control.changed() => { if changed.is_err() { /* host is shutting down */ } }
                }
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
    let _ = progress_tx.send(DownloadProgress {
        downloaded, total: total.or(Some(downloaded)), percent: Some(100.0), phase: "complete".into()
    });
    Ok(DownloadOutcome::Completed)
}

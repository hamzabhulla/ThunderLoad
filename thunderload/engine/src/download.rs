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

async fn next_available_path(path: &Path) -> Result<PathBuf> {
    if !fs::try_exists(path).await.unwrap_or(false) { return Ok(path.to_path_buf()); }
    let parent = path.parent().ok_or_else(|| anyhow!("Target path has no parent folder"))?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("download");
    let extension = path.extension().and_then(|s| s.to_str());
    for index in 1..10_000_u32 {
        let name = match extension {
            Some(ext) => format!("{stem} ({index}).{ext}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = parent.join(name);
        if !fs::try_exists(&candidate).await.unwrap_or(true) { return Ok(candidate); }
    }
    Err(anyhow!("Could not find a free filename in the download folder"))
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
    let connections = connections.clamp(1, 16);
    let client = client()?;
    let mut head_request = client.head(url);
    if let Some(referrer) = referer { head_request = head_request.header(header::REFERER, referrer); }
    let head = head_request.send().await.ok();
    let total = head.as_ref().and_then(|r| r.content_length());
    let supports_ranges = head.as_ref().map(|r| {
        r.headers().get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false)
    }).unwrap_or(false);

    if supports_ranges && total.unwrap_or(0) >= 2 * 1024 * 1024 && connections > 1 {
        match download_segmented(&client, url, &target, total.unwrap(), connections.min(16), referer, progress_tx.clone()).await {
            Ok(path) => return Ok(path),
            Err(_) => {
                // Some servers advertise ranges but reject or rewrite them. Restart cleanly.
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: 0, total, percent: Some(0.0), phase: "fallback".into()
                });
            }
        }
    }
    download_sequential(&client, url, &target, total, referer, progress_tx).await
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
    let mut stream = response.bytes_stream();
    let mut file = fs::File::create(&temp).await?;
    let mut downloaded = 0_u64;
    let mut last_report = Instant::now();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        let percent = actual_total.filter(|n| *n > 0).map(|n| downloaded as f64 * 100.0 / n as f64);
        if last_report.elapsed() >= std::time::Duration::from_millis(180) {
            let _ = progress_tx.send(DownloadProgress {
                downloaded, total: actual_total, percent, phase: "downloading".into()
            });
            last_report = Instant::now();
        }
    }
    file.flush().await?;
    drop(file);
    replace_file(&temp, target).await?;
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
    let mut output = fs::File::create(&temp_output).await?;
    for part in completed_parts {
        let mut input = fs::File::open(part).await?;
        tokio::io::copy(&mut input, &mut output).await?;
    }
    output.flush().await?;
    drop(output);
    let metadata = fs::metadata(&temp_output).await?;
    if metadata.len() != total {
        let _ = fs::remove_dir_all(&work_dir).await;
        return Err(anyhow!("Combined file size did not match the server's content length"));
    }
    replace_file(&temp_output, target).await?;
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

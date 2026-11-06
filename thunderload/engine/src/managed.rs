use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, time::{Duration, Instant}};
use tokio::{fs, io::AsyncWriteExt, sync::{mpsc, watch}};
use crate::download::{default_download_dir, safe_filename, DownloadProgress};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TransferControl {
    pub paused: bool,
    pub cancelled: bool,
    pub speed_limit_bps: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResumeMetadata {
    url: String,
    referer: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    total: Option<u64>,
}

pub fn resumable_temp_path(target: &Path) -> PathBuf {
    PathBuf::from(format!("{}.thunderload-part", target.to_string_lossy()))
}

fn metadata_path(target: &Path) -> PathBuf {
    PathBuf::from(format!("{}.thunderload.json", target.to_string_lossy()))
}

pub async fn reserve_download_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| anyhow!("Target path has no parent folder"))?;
    fs::create_dir_all(parent).await?;
    let stem = path.file_stem().and_then(|value| value.to_str()).unwrap_or("download");
    let ext = path.extension().and_then(|value| value.to_str());
    for index in 0..10_000_u32 {
        let candidate = if index == 0 {
            path.to_path_buf()
        } else {
            let name = match ext {
                Some(extension) => format!("{stem} ({index}).{extension}"),
                None => format!("{stem} ({index})"),
            };
            parent.join(name)
        };
        match fs::OpenOptions::new().write(true).create_new(true).open(&candidate).await {
            Ok(file) => { drop(file); return Ok(candidate); }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).with_context(|| format!("Cannot reserve {}", candidate.display())),
        }
    }
    Err(anyhow!("Could not reserve a unique output filename"))
}

async fn wait_until_running(control: &mut watch::Receiver<TransferControl>) -> Result<TransferControl> {
    loop {
        let value = control.borrow_and_update().clone();
        if value.cancelled { return Err(anyhow!("Download cancelled")); }
        if !value.paused { return Ok(value); }
        control.changed().await.map_err(|_| anyhow!("Download controller was closed"))?;
    }
}

fn request_client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("ThunderLoad/0.3 download manager")
        .connect_timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(16)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?)
}

fn parse_range_start(value: &str) -> Option<u64> {
    let value = value.strip_prefix("bytes ")?;
    let (range, _) = value.split_once('/')?;
    let (start, _) = range.split_once('-')?;
    start.parse().ok()
}

async fn save_metadata(path: &Path, metadata: &ResumeMetadata) -> Result<()> {
    let bytes = serde_json::to_vec(metadata)?;
    let temporary = path.with_extension("thunderload.json.tmp");
    fs::write(&temporary, bytes).await?;
    fs::rename(&temporary, path).await?;
    Ok(())
}

async fn load_metadata(path: &Path, url: &str, referer: Option<&str>) -> Option<ResumeMetadata> {
    let bytes = fs::read(path).await.ok()?;
    let metadata: ResumeMetadata = serde_json::from_slice(&bytes).ok()?;
    if metadata.url != url || metadata.referer.as_deref() != referer { return None; }
    Some(metadata)
}

pub async fn download_url_managed(
    url: &str,
    target: &Path,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    mut control: watch::Receiver<TransferControl>,
) -> Result<PathBuf> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(anyhow!("Only HTTP and HTTPS download URLs are supported"));
    }
    let client = request_client()?;
    let part = resumable_temp_path(target);
    let sidecar = metadata_path(target);
    let mut metadata = load_metadata(&sidecar, url, referer).await;
    let mut downloaded = fs::metadata(&part).await.map(|value| value.len()).unwrap_or(0);
    let mut started = Instant::now();
    let mut reported = Instant::now();

    loop {
        let settings = wait_until_running(&mut control).await?;
        if downloaded == 0 {
            metadata = None;
        }
        let mut request = client.get(url);
        if let Some(value) = referer { request = request.header(header::REFERER, value); }
        if downloaded > 0 {
            request = request.header(header::RANGE, format!("bytes={downloaded}-"));
            if let Some(etag) = metadata.as_ref().and_then(|m| m.etag.as_deref()) {
                request = request.header(header::IF_RANGE, etag);
            } else if let Some(last_modified) = metadata.as_ref().and_then(|m| m.last_modified.as_deref()) {
                request = request.header(header::IF_RANGE, last_modified);
            }
        }
        let response = request.send().await?.error_for_status()?;
        let append = if downloaded > 0 && response.status() == StatusCode::PARTIAL_CONTENT {
            let range = response.headers().get(header::CONTENT_RANGE).and_then(|v| v.to_str().ok());
            if range.and_then(parse_range_start) != Some(downloaded) {
                return Err(anyhow!("Server returned an invalid resume range; partial data has been retained"));
            }
            true
        } else {
            false
        };
        if !append { downloaded = 0; }
        let total = if append {
            response.headers().get(header::CONTENT_RANGE).and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit('/').next()).and_then(|v| v.parse().ok())
        } else {
            response.content_length()
        };
        let current = ResumeMetadata {
            url: url.to_string(),
            referer: referer.map(str::to_string),
            etag: response.headers().get(header::ETAG).and_then(|v| v.to_str().ok()).map(str::to_string)
                .or_else(|| metadata.as_ref().and_then(|m| m.etag.clone())),
            last_modified: response.headers().get(header::LAST_MODIFIED).and_then(|v| v.to_str().ok()).map(str::to_string)
                .or_else(|| metadata.as_ref().and_then(|m| m.last_modified.clone())),
            total,
        };
        save_metadata(&sidecar, &current).await?;
        metadata = Some(current);

        let mut file = if append {
            fs::OpenOptions::new().create(true).append(true).open(&part).await?
        } else {
            fs::File::create(&part).await?
        };
        started = Instant::now();
        let mut stream = response.bytes_stream();
        let mut paused = false;

        loop {
            tokio::select! {
                next = stream.next() => {
                    match next {
                        Some(Ok(chunk)) => {
                            file.write_all(&chunk).await?;
                            downloaded += chunk.len() as u64;
                            let percent = total.filter(|v| *v > 0)
                                .map(|v| downloaded as f64 * 100.0 / v as f64);
                            if reported.elapsed() >= Duration::from_millis(150) {
                                let _ = progress_tx.send(DownloadProgress {
                                    downloaded, total, percent, phase: "downloading".into()
                                });
                                reported = Instant::now();
                            }
                            let latest = control.borrow().clone();
                            if let Some(limit) = latest.speed_limit_bps.filter(|v| *v > 0) {
                                let expected = Duration::from_secs_f64(downloaded as f64 / limit as f64);
                                let delay = expected.saturating_sub(started.elapsed());
                                if !delay.is_zero() {
                                    tokio::select! {
                                        _ = tokio::time::sleep(delay) => {},
                                        changed = control.changed() => {
                                            changed.map_err(|_| anyhow!("Download controller was closed"))?;
                                        }
                                    }
                                }
                            }
                        }
                        Some(Err(error)) => {
                            file.flush().await?;
                            return Err(error.into()).context("Network interrupted; partial data retained for resume");
                        }
                        None => break,
                    }
                }
                changed = control.changed() => {
                    changed.map_err(|_| anyhow!("Download controller was closed"))?;
                    let state = control.borrow().clone();
                    if state.cancelled {
                        file.flush().await?;
                        drop(file);
                        let _ = fs::remove_file(&part).await;
                        let _ = fs::remove_file(&sidecar).await;
                        return Err(anyhow!("Download cancelled"));
                    }
                    if state.paused {
                        file.flush().await?;
                        paused = true;
                        break;
                    }
                }
            }
            let latest = control.borrow().clone();
            if latest.cancelled {
                file.flush().await?;
                drop(file);
                let _ = fs::remove_file(&part).await;
                let _ = fs::remove_file(&sidecar).await;
                return Err(anyhow!("Download cancelled"));
            }
            if latest.paused {
                file.flush().await?;
                paused = true;
                break;
            }
        }
        file.flush().await?;
        drop(file);

        if paused {
            let _ = progress_tx.send(DownloadProgress {
                downloaded, total: metadata.as_ref().and_then(|m| m.total),
                percent: metadata.as_ref().and_then(|m| m.total).filter(|v| *v > 0)
                    .map(|v| downloaded as f64 * 100.0 / v as f64),
                phase: "paused".into(),
            });
            continue;
        }

        let expected = metadata.as_ref().and_then(|m| m.total);
        if expected.is_some_and(|size| downloaded != size) {
            return Err(anyhow!("Transfer ended at {downloaded} bytes; server declared {} bytes. Partial data retained for resume", expected.unwrap()));
        }
        fs::remove_file(target).await.ok();
        fs::rename(&part, target).await.with_context(|| format!("Cannot commit completed download to {}", target.display()))?;
        fs::remove_file(&sidecar).await.ok();
        let _ = progress_tx.send(DownloadProgress {
            downloaded, total: expected.or(Some(downloaded)), percent: Some(100.0), phase: "complete".into()
        });
        return Ok(target.to_path_buf());
    }
}

pub fn suggested_target(filename: &str) -> PathBuf {
    default_download_dir().join(safe_filename(filename, "download.bin"))
}

#[cfg(test)]
mod tests {
    use super::{metadata_path, parse_range_start, resumable_temp_path};
    use std::path::Path;

    #[test]
    fn parses_resume_content_range_start_only() {
        assert_eq!(parse_range_start("bytes 512-1023/2048"), Some(512));
        assert_eq!(parse_range_start("bytes 0-0/*"), Some(0));
        assert_eq!(parse_range_start("bytes */2048"), None);
        assert_eq!(parse_range_start("items 0-1/2"), None);
    }

    #[test]
    fn partial_and_sidecar_names_are_stable() {
        let target = Path::new("C:/Downloads/demo.zip");
        assert!(resumable_temp_path(target).to_string_lossy().ends_with("demo.zip.thunderload-part"));
        assert!(metadata_path(target).to_string_lossy().ends_with("demo.zip.thunderload.json"));
    }
}

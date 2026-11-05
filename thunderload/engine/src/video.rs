use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicUsize, Ordering}, Arc}};
use tokio::{fs, io::AsyncWriteExt, sync::mpsc};
use url::Url;
use uuid::Uuid;
use crate::download::{default_download_dir, safe_filename, DownloadProgress};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoVariant {
    pub kind: String,
    pub quality: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bandwidth: Option<u64>,
    pub url: String,
    pub downloadable: bool,
    pub note: Option<String>,
}

fn client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("Mozilla/5.0 ThunderLoad/0.1")
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?)
}

pub async fn inspect_video(url: &str, referer: Option<&str>) -> Result<Vec<VideoVariant>> {
    let parsed = Url::parse(url).context("Invalid video URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS media URLs can be inspected"));
    }
    let lower = parsed.path().to_ascii_lowercase();
    if lower.ends_with(".m3u8") {
        let client = client()?;
        let mut request = client.get(url);
        if let Some(referrer) = referer { request = request.header(reqwest::header::REFERER, referrer); }
        let body = request.send().await?.error_for_status()?.text().await?;
        let variants = parse_hls_master(&body, &parsed)?;
        if !variants.is_empty() { return Ok(variants); }
        return Ok(vec![VideoVariant {
            kind: "hls".into(), quality: "Playlist / adaptive".into(), width: None, height: None,
            bandwidth: None, url: url.to_string(), downloadable: true,
            note: Some("ThunderLoad can download unencrypted HLS playlists".into()),
        }]);
    }
    if lower.ends_with(".mpd") {
        let client = client()?;
        let mut request = client.get(url);
        if let Some(referrer) = referer { request = request.header(reqwest::header::REFERER, referrer); }
        let body = request.send().await?.error_for_status()?.text().await?;
        let reps = parse_dash_representations(&body, url);
        if reps.is_empty() {
            return Ok(vec![VideoVariant { kind: "dash".into(), quality: "Adaptive DASH".into(), width: None,
                height: None, bandwidth: None, url: url.into(), downloadable: false,
                note: Some("DASH manifest found; segmented DASH download is not enabled in this starter".into()) }]);
        }
        return Ok(reps);
    }
    Ok(vec![VideoVariant {
        kind: "direct".into(), quality: "Original file".into(), width: None, height: None,
        bandwidth: None, url: url.into(), downloadable: true,
        note: Some("Direct media file".into()),
    }])
}

fn parse_hls_master(body: &str, base: &Url) -> Result<Vec<VideoVariant>> {
    let mut out = Vec::new();
    let mut pending: Option<(Option<u32>, Option<u32>, Option<u64>)> = None;
    for line in body.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(attrs) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            let resolution = attr(attrs, "RESOLUTION").and_then(|s| {
                let (w, h) = s.split_once('x')?;
                Some((w.parse().ok()?, h.parse().ok()?))
            });
            let bandwidth = attr(attrs, "AVERAGE-BANDWIDTH").or_else(|| attr(attrs, "BANDWIDTH"))
                .and_then(|s| s.parse().ok());
            pending = Some((resolution.map(|r| r.0), resolution.map(|r| r.1), bandwidth));
        } else if !line.starts_with('#') {
            if let Some((width, height, bandwidth)) = pending.take() {
                let resolved = base.join(line)?.to_string();
                let quality = height.map(|h| format!("{h}p")).unwrap_or_else(|| "Adaptive".into());
                out.push(VideoVariant { kind: "hls".into(), quality, width, height, bandwidth,
                    url: resolved, downloadable: true, note: Some("HLS variant".into()) });
            }
        }
    }
    out.sort_by(|a, b| b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)));
    Ok(out)
}

fn attr(input: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    input.split(',').find_map(|part| {
        let part = part.trim();
        let value = part.strip_prefix(&prefix)?;
        Some(value.trim_matches('"').to_string())
    })
}

fn parse_dash_representations(body: &str, manifest_url: &str) -> Vec<VideoVariant> {
    let mut out = Vec::new();
    // Small, dependency-free starter parser for common Representation attributes.
    for tag in body.split('<').filter(|s| s.starts_with("Representation ") || s.starts_with("Representation>")) {
        let attrs = tag.split('>').next().unwrap_or("");
        let id = xml_attr(attrs, "id").unwrap_or_else(|| "DASH".into());
        let width = xml_attr(attrs, "width").and_then(|v| v.parse().ok());
        let height = xml_attr(attrs, "height").and_then(|v| v.parse().ok());
        let bandwidth = xml_attr(attrs, "bandwidth").and_then(|v| v.parse().ok());
        let quality = height.map(|h| format!("{h}p")).unwrap_or_else(|| id.clone());
        out.push(VideoVariant { kind: "dash".into(), quality, width, height, bandwidth,
            url: manifest_url.to_string(), downloadable: false,
            note: Some("Quality detected; DASH segment assembly is not enabled yet".into()) });
    }
    out.sort_by(|a, b| b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)));
    out
}

fn xml_attr(input: &str, name: &str) -> Option<String> {
    let marker = format!("{name}=\"");
    let rest = input.split_once(&marker)?.1;
    Some(rest.split('"').next()?.to_string())
}

async fn unique_video_path(directory: &Path, stem: &str, extension: &str) -> Result<PathBuf> {
    let first = directory.join(format!("{stem}.{extension}"));
    if !fs::try_exists(&first).await.unwrap_or(false) { return Ok(first); }
    for index in 1..10_000_u32 {
        let candidate = directory.join(format!("{stem} ({index}).{extension}"));
        if !fs::try_exists(&candidate).await.unwrap_or(true) { return Ok(candidate); }
    }
    Err(anyhow!("Could not find a free filename for this video"))
}

pub async fn download_hls(
    playlist_url: &str,
    filename: &str,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<PathBuf> {
    let base = Url::parse(playlist_url)?;
    let client = client()?;
    let mut req = client.get(playlist_url);
    if let Some(referrer) = referer { req = req.header(reqwest::header::REFERER, referrer); }
    let body = req.send().await?.error_for_status()?.text().await?;

    // If a master playlist is supplied, choose the first (highest listed) variant.
    let variants = parse_hls_master(&body, &base)?;
    let media_url = variants.first().map(|v| v.url.clone()).unwrap_or_else(|| playlist_url.to_string());
    let media_base = Url::parse(&media_url)?;
    let media = if media_url == playlist_url { body } else {
        let mut r = client.get(&media_url);
        if let Some(referrer) = referer { r = r.header(reqwest::header::REFERER, referrer); }
        r.send().await?.error_for_status()?.text().await?
    };

    let mut init_segment: Option<String> = None;
    let mut segments = Vec::new();
    for line in media.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(v) = line.strip_prefix("#EXT-X-MAP:") {
            init_segment = attr(v, "URI").and_then(|u| media_base.join(&u).ok()).map(|u| u.to_string());
        } else if let Some(key) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attr(key, "METHOD").unwrap_or_default();
            if method != "NONE" {
                return Err(anyhow!("This HLS playlist is encrypted. ThunderLoad does not decrypt protected streams."));
            }
        } else if line.starts_with('#') {
            continue;
        } else {
            segments.push(media_base.join(line)?.to_string());
        }
    }
    if segments.is_empty() { return Err(anyhow!("No media segments were found in this HLS playlist")); }

    let out_dir = default_download_dir();
    fs::create_dir_all(&out_dir).await?;
    let extension = if init_segment.is_some() { "mp4" } else { "ts" };
    let safe = safe_filename(filename, "video");
    let stem = Path::new(&safe).file_stem().and_then(|s| s.to_str()).unwrap_or("video");
    let target = unique_video_path(&out_dir, stem, extension).await?;
    let work_dir = out_dir.join(format!(".hls-{}", Uuid::new_v4()));
    fs::create_dir_all(&work_dir).await?;

    let mut all_urls = Vec::new();
    if let Some(init) = init_segment { all_urls.push(init); }
    all_urls.extend(segments);
    let total_segments = all_urls.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for (index, segment_url) in all_urls.into_iter().enumerate() {
        let client = client.clone();
        let referrer = referer.map(str::to_string);
        let path = work_dir.join(format!("segment-{index:06}"));
        let completed = completed.clone();
        let tx = progress_tx.clone();
        tasks.spawn(async move {
            let mut request = client.get(segment_url);
            if let Some(referrer) = referrer { request = request.header(reqwest::header::REFERER, referrer); }
            let mut response = request.send().await?.error_for_status()?;
            let mut file = fs::File::create(&path).await?;
            while let Some(chunk) = response.chunk().await? { file.write_all(&chunk).await?; }
            file.flush().await?;
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = tx.send(DownloadProgress { downloaded: done as u64, total: Some(total_segments as u64),
                percent: Some(done as f64 * 100.0 / total_segments as f64), phase: "video-segments".into() });
            Ok::<PathBuf, anyhow::Error>(path)
        });
        // Limit concurrent segment requests. Drain in batches to cap memory and sockets.
        if tasks.len() >= 6 {
            if let Some(result) = tasks.join_next().await { result??; }
        }
    }
    while let Some(result) = tasks.join_next().await { result??; }

    let temp = work_dir.join("assembled.media");
    let mut output = fs::File::create(&temp).await?;
    for index in 0..total_segments {
        let mut input = fs::File::open(work_dir.join(format!("segment-{index:06}"))).await?;
        tokio::io::copy(&mut input, &mut output).await?;
    }
    output.flush().await?;
    drop(output);
    if fs::try_exists(&target).await.unwrap_or(false) { fs::remove_file(&target).await?; }
    fs::rename(&temp, &target).await?;
    let _ = fs::remove_dir_all(&work_dir).await;
    let _ = progress_tx.send(DownloadProgress { downloaded: total_segments as u64, total: Some(total_segments as u64),
        percent: Some(100.0), phase: "complete".into() });
    Ok(target)
}

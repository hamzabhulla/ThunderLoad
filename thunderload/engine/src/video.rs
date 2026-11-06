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
    let parsed = Url::parse(url).context("Invalid media URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS media URLs can be inspected"));
    }
    let lower = parsed.path().to_ascii_lowercase();

    if lower.ends_with(".m3u8") {
        let body = fetch_text(url, referer).await?;
        let mut variants = parse_hls_master(&body, &parsed)?;
        if !variants.is_empty() {
            annotate_hls_variants(&mut variants, referer).await;
            if let Some(reason) = unsupported_hls_master_reason(&body) {
                for variant in &mut variants {
                    variant.downloadable = false;
                    variant.note = Some(reason.clone());
                }
            }
            return Ok(variants);
        }
        let reason = unsupported_hls_reason(&body);
        return Ok(vec![VideoVariant {
            kind: "hls".into(), quality: "Adaptive playlist".into(), width: None, height: None,
            bandwidth: None, url: url.to_string(), downloadable: reason.is_none(),
            note: reason.or_else(|| Some("Unencrypted HLS playlist".into())),
        }]);
    }
    if lower.ends_with(".mpd") {
        let body = fetch_text(url, referer).await?;
        let reps = parse_dash_representations(&body, url);
        if reps.is_empty() {
            return Ok(vec![VideoVariant {
                kind: "dash".into(), quality: "Adaptive DASH".into(), width: None,
                height: None, bandwidth: None, url: url.into(), downloadable: false,
                note: Some("DASH manifest detected; DASH segment assembly is not available".into())
            }]);
        }
        return Ok(reps);
    }

    // Never label an arbitrary webpage (for example a YouTube watch page) as a file.
    // Known media extensions are accepted; extensionless URLs must identify as media
    // or an attachment from the server's response headers.
    if is_direct_media_path(&lower) {
        return Ok(vec![direct_variant(url)]);
    }

    let client = client()?;
    let mut request = client.head(url);
    if let Some(referrer) = referer { request = request.header(reqwest::header::REFERER, referrer); }
    // A number of CDNs block HEAD even when normal downloads are allowed.
    // Retry the metadata probe with a one-byte range GET before rejecting the URL.
    let response = match request.send().await {
        Ok(response) if response.status().is_success() => response,
        _ => {
            let mut probe = client.get(url).header(reqwest::header::RANGE, "bytes=0-0");
            if let Some(referrer) = referer { probe = probe.header(reqwest::header::REFERER, referrer); }
            probe.send().await
                .context("Could not inspect the URL response headers with HEAD or a one-byte GET")?
                .error_for_status()
                .context("The server rejected both media inspection requests")?
        }
    };
    let content_type = response.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';').next().unwrap_or("").trim().to_ascii_lowercase();

    if content_type.contains("mpegurl") || content_type.contains("vnd.apple.mpegurl") {
        let body = fetch_text(url, referer).await?;
        let mut variants = parse_hls_master(&body, &parsed)?;
        if !variants.is_empty() {
            annotate_hls_variants(&mut variants, referer).await;
            if let Some(reason) = unsupported_hls_master_reason(&body) {
                for variant in &mut variants {
                    variant.downloadable = false;
                    variant.note = Some(reason.clone());
                }
            }
            return Ok(variants);
        }
        let reason = unsupported_hls_reason(&body);
        return Ok(vec![VideoVariant {
            kind: "hls".into(), quality: "Adaptive playlist".into(), width: None, height: None,
            bandwidth: None, url: url.to_string(), downloadable: reason.is_none(),
            note: reason.or_else(|| Some("Unencrypted HLS playlist".into())),
        }]);
    }
    if content_type.contains("dash+xml") {
        let body = fetch_text(url, referer).await?;
        return Ok(parse_dash_representations(&body, url));
    }
    if content_type.starts_with("video/") || content_type.starts_with("audio/")
        || content_type == "application/octet-stream" || content_type == "application/zip"
        || content_type == "application/pdf" {
        return Ok(vec![direct_variant(url)]);
    }

    Err(anyhow!(
        "This URL is a webpage, not a direct media file. Paste an MP4/audio URL or an HLS playlist. Page extraction is not available for this source."
    ))
}

async fn fetch_text(url: &str, referer: Option<&str>) -> Result<String> {
    let client = client()?;
    let mut request = client.get(url);
    if let Some(referrer) = referer { request = request.header(reqwest::header::REFERER, referrer); }
    Ok(request.send().await?.error_for_status()?.text().await?)
}

fn is_complete_vod_playlist(body: &str) -> bool {
    body.lines().any(|line| line.trim() == "#EXT-X-ENDLIST")
}

fn unsupported_hls_master_reason(body: &str) -> Option<String> {
    for line in body.lines().map(str::trim) {
        if let Some(attributes) = line.strip_prefix("#EXT-X-SESSION-KEY:") {
            let method = attr(attributes, "METHOD").unwrap_or_default();
            if !method.eq_ignore_ascii_case("NONE") {
                return Some("This HLS master uses an encryption session key; protected streams are not supported".into());
            }
        }
        if let Some(attributes) = line.strip_prefix("#EXT-X-MEDIA:") {
            if attr(attributes, "TYPE").is_some_and(|kind| kind.eq_ignore_ascii_case("AUDIO")) {
                return Some("This HLS source has a separate audio rendition. Audio/video muxing is not available in this build".into());
            }
        }
    }
    None
}

fn unsupported_hls_reason(body: &str) -> Option<String> {
    for line in body.lines().map(str::trim) {
        if let Some(attributes) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attr(attributes, "METHOD").unwrap_or_default();
            if !method.eq_ignore_ascii_case("NONE") {
                return Some("Encrypted HLS stream is not supported".into());
            }
        }
        if line.starts_with("#EXT-X-BYTERANGE:") {
            return Some("HLS byte-range segments are not supported".into());
        }
        if line.starts_with("#EXT-X-MAP:") && line.contains("BYTERANGE=") {
            return Some("HLS initialization byte-range segments are not supported".into());
        }
    }
    if !is_complete_vod_playlist(body) {
        return Some("This is a live or unfinished HLS playlist. Only completed VOD playlists can be downloaded".into());
    }
    None
}

async fn annotate_hls_variants(variants: &mut [VideoVariant], referer: Option<&str>) {
    for variant in variants.iter_mut().filter(|item| item.kind == "hls") {
        match fetch_text(&variant.url, referer).await {
            Ok(body) => {
                if let Some(reason) = unsupported_hls_reason(&body) {
                    variant.downloadable = false;
                    variant.note = Some(reason);
                }
            }
            Err(_) => {
                variant.downloadable = false;
                variant.note = Some("Could not read this quality playlist".into());
            }
        }
    }
}

fn direct_variant(url: &str) -> VideoVariant {
    VideoVariant {
        kind: "direct".into(), quality: "Original file".into(), width: None, height: None,
        bandwidth: None, url: url.into(), downloadable: true,
        note: Some("Direct media or file URL".into()),
    }
}

fn is_direct_media_path(path: &str) -> bool {
    [
        ".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi", ".mp3", ".m4a",
        ".aac", ".wav", ".ogg", ".flac", ".zip", ".pdf", ".7z", ".rar"
    ].iter().any(|extension| path.ends_with(extension))
}

#[cfg(test)]
mod inspect_tests {
    use super::is_direct_media_path;

    #[test]
    fn recognizes_common_file_extensions() {
        assert!(is_direct_media_path("/movie.mp4"));
        assert!(is_direct_media_path("/audio.m4a"));
        assert!(is_direct_media_path("/archive.zip"));
    }

    #[test]
    fn does_not_treat_web_pages_as_media_files() {
        assert!(!is_direct_media_path("/watch"));
        assert!(!is_direct_media_path("/video/12345"));
        assert!(!is_direct_media_path("/index.html"));
    }
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
    fs::create_dir_all(directory).await?;
    for index in 0..10_000_u32 {
        let filename = if index == 0 {
            format!("{stem}.{extension}")
        } else {
            format!("{stem} ({index}).{extension}")
        };
        let candidate = directory.join(filename);
        match fs::OpenOptions::new().write(true).create_new(true).open(&candidate).await {
            Ok(file) => {
                drop(file);
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).with_context(|| format!("Cannot reserve {}", candidate.display())),
        }
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
    if !variants.is_empty() {
        if let Some(reason) = unsupported_hls_master_reason(&body) {
            return Err(anyhow!(reason));
        }
    }
    let media_url = variants.first().map(|v| v.url.clone()).unwrap_or_else(|| playlist_url.to_string());
    let media_base = Url::parse(&media_url)?;
    let media = if media_url == playlist_url { body } else {
        let mut r = client.get(&media_url);
        if let Some(referrer) = referer { r = r.header(reqwest::header::REFERER, referrer); }
        r.send().await?.error_for_status()?.text().await?
    };
    // The current implementation downloads finite VOD playlists. A live playlist
    // without ENDLIST would otherwise keep the job running indefinitely.
    if !is_complete_vod_playlist(&media) {
        return Err(anyhow!("This is a live or unfinished HLS playlist. Only completed VOD playlists can be downloaded in this build."));
    }

    let mut init_segment: Option<String> = None;
    let mut segments = Vec::new();
    for line in media.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(v) = line.strip_prefix("#EXT-X-MAP:") {
            init_segment = attr(v, "URI").and_then(|u| media_base.join(&u).ok()).map(|u| u.to_string());
        } else if let Some(key) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attr(key, "METHOD").unwrap_or_default();
            if !method.eq_ignore_ascii_case("NONE") {
                return Err(anyhow!("This HLS playlist is encrypted. ThunderLoad does not decrypt protected streams."));
            }
        } else if line.starts_with("#EXT-X-BYTERANGE:") {
            return Err(anyhow!("HLS byte-range segments are not supported by this build"));
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
    let download_result = async {
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
            // Bound segment concurrency to keep file descriptors and sockets predictable.
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
        // The target was reserved atomically before downloading. Replace only our
        // empty reservation after every segment has been written successfully.
        fs::remove_file(&target).await?;
        fs::rename(&temp, &target).await?;
        let _ = progress_tx.send(DownloadProgress { downloaded: total_segments as u64, total: Some(total_segments as u64),
            percent: Some(100.0), phase: "complete".into() });
        Ok::<PathBuf, anyhow::Error>(target.clone())
    }.await;

    if download_result.is_err() {
        let _ = fs::remove_dir_all(&work_dir).await;
        let _ = fs::remove_file(&target).await;
    } else {
        let _ = fs::remove_dir_all(&work_dir).await;
    }
    download_result
}

#[cfg(test)]
mod tests {
    use super::{is_complete_vod_playlist, parse_dash_representations, parse_hls_master, unsupported_hls_master_reason, unsupported_hls_reason};
    use url::Url;

    #[test]
    fn hls_master_exposes_quality_options_highest_first() {
        let playlist = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720\nhigh.m3u8\n";
        let base = Url::parse("https://cdn.example.test/master.m3u8").unwrap();
        let variants = parse_hls_master(playlist, &base).unwrap();
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].quality, "720p");
        assert_eq!(variants[1].quality, "360p");
        assert!(variants[0].url.ends_with("/high.m3u8"));
    }

    #[test]
    fn dash_representations_are_detected_but_not_marked_downloadable() {
        let manifest = r#"<MPD><Period><AdaptationSet><Representation id="low" bandwidth="300000" width="640" height="360"></Representation><Representation id="high" bandwidth="1200000" width="1280" height="720"></Representation></AdaptationSet></Period></MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/video.mpd");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].quality, "720p");
        assert!(!variants[0].downloadable);
    }

    #[test]
    fn live_hls_playlist_is_not_misreported_as_a_finished_download() {
        assert!(is_complete_vod_playlist("#EXTM3U\n#EXTINF:5,\nsegment.ts\n#EXT-X-ENDLIST"));
        assert!(!is_complete_vod_playlist("#EXTM3U\n#EXTINF:5,\nsegment.ts"));
        assert!(unsupported_hls_reason("#EXTM3U\n#EXTINF:5,\nsegment.ts").is_some());
    }

    #[test]
    fn hls_session_encryption_and_separate_audio_are_not_offered_as_downloadable() {
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI=\"key.bin\"").is_some());
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"audio.m3u8\"").is_some());
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"100@0\"\n#EXT-X-ENDLIST").is_some());
    }
}

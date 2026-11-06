use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use reqwest::{header, Client};
use roxmltree::{Document, Node};
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, time::Duration};
use tokio::{fs, io::AsyncWriteExt, process::Command, sync::mpsc};
use url::Url;

use crate::{
    download::{DownloadProgress},
    managed::{ManagedOutcome, TransferControl},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashRepresentation {
    pub id: String,
    pub quality: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bandwidth: Option<u64>,
    pub downloadable: bool,
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
struct DashTrack {
    id: String,
    mime_type: String,
    bandwidth: Option<u64>,
    width: Option<u32>,
    height: Option<u32>,
    initialization: Option<String>,
    segments: Vec<String>,
}

#[derive(Debug, Clone)]
struct DashPlan {
    videos: Vec<DashTrack>,
    audios: Vec<DashTrack>,
    representations: Vec<DashRepresentation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DashCheckpoint {
    mpd_url: String,
    video_id: String,
    video_urls: Vec<String>,
    audio_id: Option<String>,
    audio_urls: Vec<String>,
    target_path: String,
}

pub async fn inspect_dash(url: &str, referer: Option<&str>) -> Result<Vec<DashRepresentation>> {
    let body = fetch_text(url, referer).await?;
    Ok(parse_dash(&body, url)?.representations)
}

pub async fn download_dash_managed(
    mpd_url: &str,
    target: &Path,
    job_id: &str,
    representation_id: Option<&str>,
    referer: Option<&str>,
    control: TransferControl,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<ManagedOutcome> {
    if !(mpd_url.starts_with("http://") || mpd_url.starts_with("https://")) {
        return Err(anyhow!("Only HTTP and HTTPS DASH URLs are supported"));
    }
    let body = fetch_text(mpd_url, referer).await?;
    let mut plan = parse_dash(&body, mpd_url)?;
    if let Some(id) = representation_id {
        if let Some(index) = plan.videos.iter().position(|track| track.id == id) {
            let chosen = plan.videos.remove(index);
            plan.videos.insert(0, chosen);
        }
    }
    let video = plan.videos.first().cloned().ok_or_else(|| anyhow!("No supported video Representation was found"))?;
    let audio = plan.audios.first().cloned();
    let mut video_urls = Vec::new();
    if let Some(init) = &video.initialization { video_urls.push(init.clone()); }
    video_urls.extend(video.segments.clone());
    let mut audio_urls = Vec::new();
    if let Some(track) = &audio {
        if let Some(init) = &track.initialization { audio_urls.push(init.clone()); }
        audio_urls.extend(track.segments.clone());
    }
    if video_urls.is_empty() {
        return Err(anyhow!("The selected DASH video Representation has no media segments"));
    }

    let safe_id: String = job_id.chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || "-_".contains(*ch))
        .take(80)
        .collect();
    if safe_id.is_empty() { return Err(anyhow!("Invalid DASH job identifier")); }
    let parent = target.parent().ok_or_else(|| anyhow!("Target path has no parent directory"))?;
    fs::create_dir_all(parent).await?;
    let work_dir = parent.join(format!(".thunderload-dash-job-{safe_id}"));
    let existed = fs::try_exists(&work_dir).await.unwrap_or(false);
    fs::create_dir_all(&work_dir).await?;
    let checkpoint_path = work_dir.join("dash.json");
    let old: Option<DashCheckpoint> = fs::read(&checkpoint_path).await.ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let matches = old.as_ref().is_some_and(|saved| {
        saved.mpd_url == mpd_url
            && saved.video_id == video.id
            && saved.video_urls == video_urls
            && saved.audio_id == audio.as_ref().map(|track| track.id.clone())
            && saved.audio_urls == audio_urls
    });
    if existed && !matches {
        fs::remove_dir_all(&work_dir).await?;
        fs::create_dir_all(&work_dir).await?;
    }
    let final_target = if matches {
        PathBuf::from(old.as_ref().unwrap().target_path.clone())
    } else {
        target.to_path_buf()
    };
    let checkpoint = DashCheckpoint {
        mpd_url: mpd_url.to_string(),
        video_id: video.id.clone(),
        video_urls: video_urls.clone(),
        audio_id: audio.as_ref().map(|track| track.id.clone()),
        audio_urls: audio_urls.clone(),
        target_path: final_target.to_string_lossy().to_string(),
    };
    write_checkpoint(&work_dir, &checkpoint_path, &checkpoint).await?;

    let total_segments = (video_urls.len() + audio_urls.len()) as u64;
    let client = client()?;
    let video_complete = download_track(
        &client, &video_urls, &work_dir.join("video"), "video",
        referer, &control, &progress_tx, total_segments,
    ).await?;
    if !video_complete {
        return Ok(ManagedOutcome::Paused {
            path: final_target,
            downloaded: count_completed(&work_dir.join("video"), "video", video_urls.len()).await as u64,
            total: Some(total_segments),
        });
    }

    if !audio_urls.is_empty() {
        let audio_complete = download_track(
            &client, &audio_urls, &work_dir.join("audio"), "audio",
            referer, &control, &progress_tx, total_segments,
        ).await?;
        if !audio_complete {
            let done = count_completed(&work_dir.join("video"), "video", video_urls.len()).await
                + count_completed(&work_dir.join("audio"), "audio", audio_urls.len()).await;
            return Ok(ManagedOutcome::Paused { path: final_target, downloaded: done as u64, total: Some(total_segments) });
        }
    }

    if control.cancelled() {
        let _ = fs::remove_dir_all(&work_dir).await;
        let _ = fs::remove_file(&final_target).await;
        return Err(anyhow!("Download cancelled"));
    }
    let video_file = assemble_track(&work_dir.join("video"), "video-track.mp4", "video", video_urls.len()).await?;
    let muxed = work_dir.join("output.mp4");
    if !audio_urls.is_empty() {
        let audio_file = assemble_track(&work_dir.join("audio"), "audio-track.m4a", "audio", audio_urls.len()).await?;
        let output = Command::new("ffmpeg")
            .arg("-nostdin").arg("-hide_banner").arg("-loglevel").arg("error").arg("-y")
            .arg("-i").arg(&video_file).arg("-i").arg(&audio_file)
            .arg("-map").arg("0:v:0").arg("-map").arg("1:a:0")
            .arg("-c").arg("copy").arg("-movflags").arg("+faststart")
            .arg(&muxed)
            .output().await
            .context("DASH needs FFmpeg to mux separate audio and video tracks. Install FFmpeg and add it to PATH, then resume this job.")?;
        if !output.status.success() {
            return Err(anyhow!("FFmpeg could not mux DASH tracks: {}", String::from_utf8_lossy(&output.stderr)));
        }
    } else {
        fs::copy(&video_file, &muxed).await?;
    }
    if fs::metadata(&muxed).await?.len() == 0 {
        return Err(anyhow!("The assembled DASH output is empty"));
    }
    if fs::try_exists(&final_target).await.unwrap_or(false) {
        fs::remove_file(&final_target).await?;
    }
    fs::rename(&muxed, &final_target).await?;
    let _ = fs::remove_dir_all(&work_dir).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: total_segments, total: Some(total_segments), percent: Some(100.0), phase: "complete".into()
    });
    Ok(ManagedOutcome::Completed(final_target))
}

fn client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("ThunderLoad/0.3 DASH downloader")
        .connect_timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(12)
        .build()?)
}

async fn fetch_text(url: &str, referer: Option<&str>) -> Result<String> {
    let client = client()?;
    let mut request = client.get(url);
    if let Some(value) = referer { request = request.header(header::REFERER, value); }
    Ok(request.send().await?.error_for_status()?.text().await?)
}

fn child<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Option<Node<'a, 'input>> {
    node.children().find(|candidate| candidate.is_element() && candidate.tag_name().name() == name)
}

fn children<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Vec<Node<'a, 'input>> {
    node.children().filter(|candidate| candidate.is_element() && candidate.tag_name().name() == name).collect()
}

fn nearest_child<'a, 'input>(nodes: &[Node<'a, 'input>], name: &str) -> Option<Node<'a, 'input>> {
    nodes.iter().rev().find_map(|node| child(*node, name))
}

fn resolve_base(node: Node<'_, '_>, parent: &Url) -> Result<Url> {
    match child(node, "BaseURL").and_then(|element| element.text()) {
        Some(value) if !value.trim().is_empty() => Ok(parent.join(value.trim())?),
        _ => Ok(parent.clone()),
    }
}

fn parse_iso_duration(value: &str) -> Option<f64> {
    let time = value.strip_prefix("PT")?;
    let mut number = String::new();
    let mut seconds = 0.0;
    for ch in time.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            number.push(ch);
            continue;
        }
        let amount: f64 = number.parse().ok()?;
        number.clear();
        seconds += match ch {
            'H' => amount * 3600.0,
            'M' => amount * 60.0,
            'S' => amount,
            _ => return None,
        };
    }
    if number.is_empty() { Some(seconds) } else { None }
}

fn template_substitute(value: &str, representation_id: &str, bandwidth: u64, number: u64, time: u64) -> Result<String> {
    let escaped = "\u{1}";
    let input = value.replace("$$", escaped);
    let mut output = String::new();
    let mut rest = input.as_str();
    while let Some(start) = rest.find('$') {
        output.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let end = after.find('$').ok_or_else(|| anyhow!("Unclosed DASH template token"))?;
        let token = &after[..end];
        let (name, format) = token.split_once('%').unwrap_or((token, ""));
        let rendered = match name {
            "RepresentationID" => representation_id.to_string(),
            "Bandwidth" => bandwidth.to_string(),
            "Number" => format_number(number, format)?,
            "Time" => time.to_string(),
            _ => return Err(anyhow!("Unsupported DASH template variable")),
        };
        output.push_str(&rendered);
        rest = &after[end + 1..];
    }
    output.push_str(rest);
    Ok(output.replace(escaped, "$"))
}

fn format_number(number: u64, format: &str) -> Result<String> {
    if format.is_empty() { return Ok(number.to_string()); }
    let width_text = format.strip_prefix('0').and_then(|value| value.strip_suffix('d'))
        .ok_or_else(|| anyhow!("Unsupported DASH number format"))?;
    let width: usize = width_text.parse()?;
    if width > 16 { return Err(anyhow!("DASH number format width is too large")); }
    Ok(format!("{number:0width$}"))
}

fn expand_template<'a, 'input>(
    template: Node<'a, 'input>,
    representation_id: &str,
    bandwidth: u64,
    base: &Url,
    period_duration: Option<f64>,
) -> Result<(Option<String>, Vec<String>)> {
    let media = template.attribute("media").ok_or_else(|| anyhow!("DASH SegmentTemplate has no media attribute"))?;
    let timescale = template.attribute("timescale").unwrap_or("1").parse::<u64>()?.max(1);
    let start_number = template.attribute("startNumber").unwrap_or("1").parse::<u64>()?;
    let initialization = template.attribute("initialization")
        .map(|value| template_substitute(value, representation_id, bandwidth, start_number, 0))
        .transpose()?
        .map(|value| base.join(&value).map(|url| url.to_string()))
        .transpose()?;
    let mut entries: Vec<(u64, u64)> = Vec::new();
    if let Some(timeline) = child(template, "SegmentTimeline") {
        let timeline_items = children(timeline, "S");
        let total_units = period_duration.map(|duration| (duration * timescale as f64).ceil() as u64);
        let mut current = 0_u64;
        for (index, item) in timeline_items.iter().enumerate() {
            let duration = item.attribute("d").ok_or_else(|| anyhow!("DASH SegmentTimeline S has no d attribute"))?.parse::<u64>()?;
            if duration == 0 { return Err(anyhow!("DASH SegmentTimeline has a zero-duration segment")); }
            let start = item.attribute("t").map(str::parse::<u64>).transpose()?.unwrap_or(current);
            let repeat = item.attribute("r").unwrap_or("0").parse::<i64>()?;
            let count = if repeat >= 0 {
                repeat as u64 + 1
            } else {
                let end = timeline_items.get(index + 1).and_then(|next| next.attribute("t")).and_then(|v| v.parse().ok())
                    .or(total_units).ok_or_else(|| anyhow!("DASH repeat=-1 needs a finite period duration or a following timeline entry"))?;
                if end <= start { return Err(anyhow!("Invalid DASH timeline repeat boundary")); }
                (end - start).div_ceil(duration)
            };
            if count > 20_000 || entries.len() as u64 + count > 20_000 {
                return Err(anyhow!("DASH timeline expands beyond 20,000 segments"));
            }
            for item_index in 0..count {
                entries.push((start_number + entries.len() as u64, start.saturating_add(item_index * duration)));
            }
            current = start.saturating_add(count.saturating_mul(duration));
        }
    } else {
        let duration = template.attribute("duration").ok_or_else(|| anyhow!("DASH SegmentTemplate requires SegmentTimeline or duration"))?.parse::<u64>()?;
        if duration == 0 { return Err(anyhow!("DASH SegmentTemplate duration cannot be zero")); }
        let period = period_duration.ok_or_else(|| anyhow!("DASH duration templates need a finite period duration"))?;
        let count = ((period * timescale as f64) / duration as f64).ceil() as u64;
        if count == 0 || count > 20_000 { return Err(anyhow!("DASH segment count is outside the supported range")); }
        for index in 0..count {
            entries.push((start_number + index, index * duration));
        }
    }
    let mut urls = Vec::with_capacity(entries.len());
    for (number, time) in entries {
        let value = template_substitute(media, representation_id, bandwidth, number, time)?;
        urls.push(base.join(&value)?.to_string());
    }
    Ok((initialization, urls))
}

fn expand_segment_list<'a, 'input>(list: Node<'a, 'input>, base: &Url) -> Result<(Option<String>, Vec<String>)> {
    let initialization = child(list, "Initialization")
        .and_then(|node| node.attribute("sourceURL"))
        .map(|value| base.join(value).map(|url| url.to_string()))
        .transpose()?;
    let urls = children(list, "SegmentURL").into_iter()
        .filter_map(|node| node.attribute("media"))
        .map(|value| base.join(value).map(|url| url.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    if urls.is_empty() { return Err(anyhow!("DASH SegmentList has no SegmentURL entries")); }
    Ok((initialization, urls))
}

fn parse_representation<'a, 'input>(
    representation: Node<'a, 'input>, adaptation: Node<'a, 'input>,
    period: Node<'a, 'input>, root: Node<'a, 'input>, mpd_base: &Url, period_duration: Option<f64>,
) -> Result<(DashTrack, String)> {
    let id = representation.attribute("id").unwrap_or("representation").to_string();
    let bandwidth = representation.attribute("bandwidth").or_else(|| adaptation.attribute("bandwidth"))
        .unwrap_or("0").parse::<u64>()?;
    let width = representation.attribute("width").and_then(|value| value.parse::<u32>().ok());
    let height = representation.attribute("height").and_then(|value| value.parse::<u32>().ok());
    let mut base = resolve_base(root, mpd_base)?;
    base = resolve_base(period, &base)?;
    base = resolve_base(adaptation, &base)?;
    base = resolve_base(representation, &base)?;
    let mime = representation.attribute("mimeType").or_else(|| adaptation.attribute("mimeType"))
        .unwrap_or(if height.is_some() { "video/mp4" } else { "application/octet-stream" }).to_string();
    let inheritance = [root, period, adaptation, representation];
    if nearest_child(&inheritance, "SegmentBase").is_some() {
        return Err(anyhow!("DASH SegmentBase/index-range manifests are not supported yet"));
    }
    let (initialization, segments) = if let Some(template) = nearest_child(&inheritance, "SegmentTemplate") {
        expand_template(template, &id, bandwidth, &base, period_duration)?
    } else if let Some(list) = nearest_child(&inheritance, "SegmentList") {
        expand_segment_list(list, &base)?
    } else {
        return Err(anyhow!("DASH Representation does not expose a supported SegmentTemplate or SegmentList"));
    };
    if segments.is_empty() { return Err(anyhow!("DASH Representation has no media segments")); }
    Ok((DashTrack { id, mime_type: mime.clone(), bandwidth: Some(bandwidth), width, height, initialization, segments }, mime))
}

fn parse_dash(body: &str, mpd_url: &str) -> Result<DashPlan> {
    let document = Document::parse(body).context("Invalid DASH MPD XML")?;
    let root = document.root_element();
    if root.attribute("type").is_some_and(|kind| kind.eq_ignore_ascii_case("dynamic")) {
        return Err(anyhow!("Live/dynamic DASH MPDs are not supported; use a static VOD MPD"));
    }
    if root.descendants().any(|node| node.is_element() && node.tag_name().name() == "ContentProtection") {
        return Err(anyhow!("This DASH MPD declares protected content. DRM streams are not decrypted"));
    }
    let periods = children(root, "Period");
    if periods.is_empty() { return Err(anyhow!("DASH MPD contains no Period")); }
    if periods.len() > 1 { return Err(anyhow!("Multi-period DASH MPDs are not supported by this build")); }
    let period = periods[0];
    let duration = period.attribute("duration").and_then(parse_iso_duration)
        .or_else(|| root.attribute("mediaPresentationDuration").and_then(parse_iso_duration));
    let base = Url::parse(mpd_url)?;
    let mut videos = Vec::new();
    let mut audios = Vec::new();
    for adaptation in children(period, "AdaptationSet") {
        for representation in children(adaptation, "Representation") {
            let (track, mime) = parse_representation(representation, adaptation, period, root, &base, duration)?;
            let content_type = representation.attribute("contentType").or_else(|| adaptation.attribute("contentType")).unwrap_or("");
            let is_audio = content_type.eq_ignore_ascii_case("audio") || mime.to_ascii_lowercase().starts_with("audio/");
            let is_video = content_type.eq_ignore_ascii_case("video") || mime.to_ascii_lowercase().starts_with("video/") || track.height.is_some();
            if is_audio { audios.push(track); } else if is_video { videos.push(track); }
        }
    }
    if videos.is_empty() { return Err(anyhow!("No supported video Representation was found")); }
    videos.sort_by(|a, b| b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)).then(b.bandwidth.cmp(&a.bandwidth)));
    audios.sort_by(|a, b| b.bandwidth.cmp(&a.bandwidth));
    let representations = videos.iter().map(|track| DashRepresentation {
        id: track.id.clone(),
        quality: track.height.map(|height| format!("{height}p")).unwrap_or_else(|| track.id.clone()),
        width: None, height: track.height, bandwidth: track.bandwidth,
        downloadable: true,
        note: Some("Static DASH VOD · FFmpeg required when audio and video are separate".into()),
    }).collect();
    Ok(DashPlan { videos, audios, representations })
}

async fn write_checkpoint(work_dir: &Path, path: &Path, data: &DashCheckpoint) -> Result<()> {
    let temp = work_dir.join("dash.json.tmp");
    fs::write(&temp, serde_json::to_vec_pretty(data)?).await?;
    if fs::try_exists(path).await.unwrap_or(false) { fs::remove_file(path).await?; }
    fs::rename(temp, path).await?;
    Ok(())
}

async fn download_track(
    client: &Client, urls: &[String], directory: &Path, track: &str, referer: Option<&str>,
    control: &TransferControl, progress: &mpsc::UnboundedSender<DownloadProgress>, total: u64,
) -> Result<bool> {
    fs::create_dir_all(directory).await?;
    for (index, url) in urls.iter().enumerate() {
        if control.cancelled() { return Err(anyhow!("Download cancelled")); }
        let complete = directory.join(format!("{track}-{index:06}.bin"));
        if fs::metadata(&complete).await.is_ok() { continue; }
        let part = directory.join(format!("{track}-{index:06}.part"));
        let _ = fs::remove_file(&part).await;
        let mut request = client.get(url);
        if let Some(value) = referer { request = request.header(header::REFERER, value); }
        let response = request.send().await?.error_for_status()?;
        let mut stream = response.bytes_stream();
        let mut file = fs::File::create(&part).await?;
        loop {
            let Some(next) = control.next_chunk(&mut stream).await? else {
                file.flush().await?; drop(file); let _ = fs::remove_file(&part).await;
                return Ok(false);
            };
            let Some(chunk) = next else { break; };
            if !control.throttle(chunk.len()).await? {
                file.flush().await?; drop(file); let _ = fs::remove_file(&part).await;
                return Ok(false);
            }
            file.write_all(&chunk).await?;
        }
        file.flush().await?; drop(file);
        fs::rename(&part, &complete).await?;
        let done = count_completed(directory, track, urls.len()).await as u64;
        let _ = progress.send(DownloadProgress {
            downloaded: done, total: Some(total), percent: Some(done as f64 * 100.0 / total as f64),
            phase: "dash-segments".into(),
        });
    }
    Ok(true)
}

async fn count_completed(directory: &Path, prefix: &str, count: usize) -> usize {
    let mut completed = 0;
    for index in 0..count {
        if fs::metadata(directory.join(format!("{prefix}-{index:06}.bin"))).await.is_ok() { completed += 1; }
    }
    completed
}

async fn assemble_track(directory: &Path, filename: &str, prefix: &str, count: usize) -> Result<PathBuf> {
    let target = directory.parent().unwrap().join(filename);
    let mut output = fs::File::create(&target).await?;
    for index in 0..count {
        let mut input = fs::File::open(directory.join(format!("{prefix}-{index:06}.bin"))).await?;
        tokio::io::copy(&mut input, &mut output).await?;
    }
    output.flush().await?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::{parse_dash, template_substitute};

    #[test]
    fn dash_parser_expands_timeline_and_selects_audio_video() {
        let mpd = r#"<MPD type="static" mediaPresentationDuration="PT10S"><Period><AdaptationSet contentType="video" mimeType="video/mp4"><SegmentTemplate timescale="1" initialization="init-$RepresentationID$.mp4" media="v-$Number%03d$.m4s"><SegmentTimeline><S t="0" d="5" r="1"/></SegmentTimeline></SegmentTemplate><Representation id="v1" bandwidth="900000" width="1280" height="720"/></AdaptationSet><AdaptationSet contentType="audio" mimeType="audio/mp4"><SegmentTemplate initialization="a-init.mp4" media="a-$Number$.m4s" duration="5"/><Representation id="a1" bandwidth="128000"/></AdaptationSet></Period></MPD>"#;
        let parsed = parse_dash(mpd, "https://cdn.example.test/path/manifest.mpd").unwrap();
        assert_eq!(parsed.representations[0].id, "v1");
        assert_eq!(parsed.representations[0].quality, "720p");
        assert_eq!(parsed.representations[0].width, Some(1280));
        assert_eq!(parsed.videos[0].segments.len(), 2);
        assert_eq!(parsed.audios[0].segments.len(), 2);
        assert_eq!(parsed.videos[0].initialization.as_deref(), Some("https://cdn.example.test/path/init-v1.mp4"));
    }

    #[test]
    fn dash_template_expands_padding_and_variables() {
        let result = template_substitute("segment-$Number%05d$-$Time$-$Bandwidth$-$RepresentationID$.m4s", "v", 900000, 7, 30).unwrap();
        assert_eq!(result, "segment-00007-30-900000-v.m4s");
    }

    #[test]
    fn dynamic_protected_and_segment_base_dash_are_rejected() {
        assert!(parse_dash(r#"<MPD type="dynamic"><Period/></MPD>"#, "https://cdn.example.test/a.mpd").is_err());
        assert!(parse_dash(r#"<MPD><Period><ContentProtection schemeIdUri="urn:test"/></Period></MPD>"#, "https://cdn.example.test/a.mpd").is_err());
        let base_only = r#"<MPD mediaPresentationDuration="PT10S"><Period><AdaptationSet mimeType="video/mp4"><Representation id="v1" bandwidth="1000" width="640" height="360"><SegmentBase indexRange="0-100"/></Representation></AdaptationSet></Period></MPD>"#;
        assert!(parse_dash(base_only, "https://cdn.example.test/a.mpd").is_err());
    }
}

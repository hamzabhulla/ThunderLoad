use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc}};
use tokio::{fs, io::AsyncWriteExt, process::Command as TokioCommand, sync::{mpsc, watch}};
use quick_xml::{events::Event, Reader};
use futures_util::StreamExt;
use std::collections::HashMap;
use url::Url;
use uuid::Uuid;
use crate::download::{default_download_dir, partial_download_metadata_path, safe_filename, DownloadControl, DownloadOutcome, DownloadProgress, GlobalRateLimiter};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub representation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_representation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_ffmpeg: Option<bool>,
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
            annotate_hls_audio(&mut variants).await;
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
            representation_id: None, audio_representation_id: None, audio_only: None, requires_ffmpeg: None,
        }]);
    }
    if lower.ends_with(".mpd") {
        let body = fetch_text(url, referer).await?;
        let mut reps = parse_dash_representations(&body, url);
        annotate_dash_variants(&mut reps).await;
        if reps.is_empty() {
            return Ok(vec![VideoVariant {
                kind: "dash".into(), quality: "Adaptive DASH".into(), width: None,
                height: None, bandwidth: None, url: url.into(), downloadable: false,
                note: Some("DASH manifest detected, but no supported static segment layout was found".into()),
                representation_id: None, audio_representation_id: None, audio_only: None, requires_ffmpeg: None,
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
            annotate_hls_audio(&mut variants).await;
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
            representation_id: None, audio_representation_id: None, audio_only: None, requires_ffmpeg: None,
        }]);
    }
    if content_type.contains("dash+xml") {
        let body = fetch_text(url, referer).await?;
        let mut variants = parse_dash_representations(&body, url);
        annotate_dash_variants(&mut variants).await;
        return Ok(variants);
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
    fetch_text_with(&client()?, url, referer).await
}

/// A variant with a separate audio rendition is downloadable only when
/// FFmpeg is available to mux the tracks, mirroring the DASH annotation.
async fn annotate_hls_audio(variants: &mut [VideoVariant]) {
    if variants.iter().all(|variant| variant.audio_representation_id.is_none()) { return; }
    if ffmpeg_available().await { return; }
    for variant in variants.iter_mut().filter(|variant| variant.audio_representation_id.is_some()) {
        variant.downloadable = false;
        variant.note = Some("This HLS playlist has separate audio and video tracks. Install FFmpeg or set THUNDERLOAD_FFMPEG before downloading.".into());
    }
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
        representation_id: None, audio_representation_id: None, audio_only: None, requires_ffmpeg: None,
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
    let mut pending: Option<(Option<u32>, Option<u32>, Option<u64>, Option<String>)> = None;
    for line in body.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(attrs) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            let resolution = attr(attrs, "RESOLUTION").and_then(|s| {
                let (w, h) = s.split_once('x')?;
                Some((w.parse().ok()?, h.parse().ok()?))
            });
            let bandwidth = attr(attrs, "AVERAGE-BANDWIDTH").or_else(|| attr(attrs, "BANDWIDTH"))
                .and_then(|s| s.parse().ok());
            pending = Some((resolution.map(|r| r.0), resolution.map(|r| r.1), bandwidth, attr(attrs, "AUDIO")));
        } else if !line.starts_with('#') {
            if let Some((width, height, bandwidth, audio_group)) = pending.take() {
                let resolved = base.join(line)?.to_string();
                let quality = height.map(|h| format!("{h}p")).unwrap_or_else(|| "Adaptive".into());
                out.push(VideoVariant { kind: "hls".into(), quality, width, height, bandwidth,
                    url: resolved, downloadable: true, note: Some("HLS variant".into()),
                    representation_id: None, audio_representation_id: audio_group, audio_only: None, requires_ffmpeg: None });
            }
        }
    }
    out.sort_by(|a, b| b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)));
    Ok(out)
}

/// An `#EXT-X-MEDIA:TYPE=AUDIO` rendition from a master playlist. Only the
/// rendition whose GROUP-ID matches the chosen variant's `AUDIO` attribute
/// is downloaded; `DEFAULT=YES` is never guessed across groups.
#[derive(Debug, Clone)]
struct HlsAudioRendition {
    group_id: String,
    uri: String,
    is_default: bool,
}

fn parse_hls_audio_renditions(body: &str, base: &Url) -> Vec<HlsAudioRendition> {
    let mut out = Vec::new();
    for line in body.lines().map(str::trim) {
        let Some(attributes) = line.strip_prefix("#EXT-X-MEDIA:") else { continue; };
        if !attr(attributes, "TYPE").is_some_and(|kind| kind.eq_ignore_ascii_case("AUDIO")) { continue; }
        let Some(group_id) = attr(attributes, "GROUP-ID") else { continue; };
        let Some(uri) = attr(attributes, "URI") else { continue; };
        let Ok(resolved) = base.join(&uri) else { continue; };
        out.push(HlsAudioRendition {
            group_id,
            uri: resolved.to_string(),
            is_default: attr(attributes, "DEFAULT").is_some_and(|value| value.eq_ignore_ascii_case("YES")),
        });
    }
    out
}

fn attr(input: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    input.split(',').find_map(|part| {
        let part = part.trim();
        let value = part.strip_prefix(&prefix)?;
        Some(value.trim_matches('"').to_string())
    })
}

#[derive(Debug, Clone, Default)]
struct DashTimelinePoint {
    time: Option<u64>,
    duration: u64,
    repeat: i64,
}

#[derive(Debug, Clone, Default)]
struct DashTemplate {
    media: Option<String>,
    initialization: Option<String>,
    timescale: u64,
    duration: Option<u64>,
    start_number: u64,
    timeline: Vec<DashTimelinePoint>,
}

#[derive(Debug, Clone, Default)]
struct DashSegmentListEntry {
    media: Option<String>,
    media_range: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Default)]
struct DashSegmentList {
    init_source_url: Option<String>,
    init_range: Option<(u64, u64)>,
    segments: Vec<DashSegmentListEntry>,
}

#[derive(Debug, Clone, Default)]
struct DashSegmentBase {
    index_range: Option<(u64, u64)>,
    init_source_url: Option<String>,
    init_range: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Default)]
struct DashRepresentation {
    id: String,
    width: Option<u32>,
    height: Option<u32>,
    bandwidth: Option<u64>,
    mime_type: String,
    codecs: Option<String>,
    frame_rate: Option<f64>,
    audio_sampling_rate: Option<u32>,
    audio_channels: Option<u32>,
    base_url: Option<String>,
    template: Option<DashTemplate>,
    segment_list: Option<DashSegmentList>,
    segment_base: Option<DashSegmentBase>,
}

#[derive(Debug, Clone, Default)]
struct DashAdaptation {
    content_type: String,
    mime_type: String,
    frame_rate: Option<f64>,
    audio_sampling_rate: Option<u32>,
    audio_channels: Option<u32>,
    base_url: Option<String>,
    template: Option<DashTemplate>,
    segment_list: Option<DashSegmentList>,
    segment_base: Option<DashSegmentBase>,
    representations: Vec<DashRepresentation>,
}

#[derive(Debug, Clone, Default)]
struct DashPeriod {
    duration_seconds: Option<f64>,
    base_url: Option<String>,
    template: Option<DashTemplate>,
    segment_list: Option<DashSegmentList>,
    segment_base: Option<DashSegmentBase>,
    adaptations: Vec<DashAdaptation>,
}

#[derive(Debug, Clone, Default)]
struct DashDocument {
    is_static: bool,
    protected: bool,
    duration_seconds: Option<f64>,
    base_url: Option<String>,
    template: Option<DashTemplate>,
    segment_list: Option<DashSegmentList>,
    segment_base: Option<DashSegmentBase>,
    periods: Vec<DashPeriod>,
}

#[derive(Debug, Clone, Copy)]
enum DashScope {
    Mpd,
    Period,
    Adaptation,
    Representation,
}

#[derive(Default)]
struct DashParserState {
    active_period: Option<usize>,
    active_adaptation: Option<usize>,
    active_representation: Option<(usize, usize)>,
    active_template_scope: Option<DashScope>,
    active_segment_list_scope: Option<DashScope>,
    active_segment_base_scope: Option<DashScope>,
    base_scope: Option<DashScope>,
    base_text: String,
}

#[derive(Debug, Clone)]
struct DashSegmentBasePlan {
    base_url: String,
    index_range: (u64, u64),
    init_url: Option<String>,
    init_range: Option<(u64, u64)>,
}

#[derive(Debug, Clone)]
struct DashResolvedTrack {
    mime_type: String,
    codecs: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    frame_rate: Option<f64>,
    audio_sampling_rate: Option<u32>,
    audio_channels: Option<u32>,
    initialization_url: Option<String>,
    initialization_range: Option<(u64, u64)>,
    segment_urls: Vec<String>,
    segment_ranges: Vec<Option<(u64, u64)>>,
    segment_base: Option<DashSegmentBasePlan>,
}

fn local_xml_name(raw: &[u8]) -> String {
    let raw = String::from_utf8_lossy(raw);
    raw.rsplit(':').next().unwrap_or(&raw).to_string()
}

fn xml_attributes(element: &quick_xml::events::BytesStart<'_>) -> Result<HashMap<String, String>> {
    let mut values = HashMap::new();
    for attribute in element.attributes().with_checks(false) {
        let attribute = attribute?;
        values.insert(
            local_xml_name(attribute.key.as_ref()),
            attribute.unescape_value()?.into_owned(),
        );
    }
    Ok(values)
}

fn parse_iso_duration(value: &str) -> Option<f64> {
    let time = value.strip_prefix("PT")?;
    let mut number = String::new();
    let mut seconds = 0.0;
    for character in time.chars() {
        if character.is_ascii_digit() || character == '.' {
            number.push(character);
            continue;
        }
        let amount: f64 = number.parse().ok()?;
        number.clear();
        seconds += match character {
            'H' => amount * 3600.0,
            'M' => amount * 60.0,
            'S' => amount,
            _ => return None,
        };
    }
    if !number.is_empty() || !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Some(seconds)
}

fn dash_scope(state: &DashParserState) -> DashScope {
    if state.active_representation.is_some() {
        DashScope::Representation
    } else if state.active_adaptation.is_some() {
        DashScope::Adaptation
    } else if state.active_period.is_some() {
        DashScope::Period
    } else {
        DashScope::Mpd
    }
}

fn parse_dash_byte_range(value: &str) -> Option<(u64, u64)> {
    let (start, end) = value.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: u64 = end.trim().parse().ok()?;
    if end < start { return None; }
    Some((start, end))
}

/// Parses an MPD `frameRate` value ("30", "29.97" or a rational
/// "30000/1001") to frames per second. Returns None for anything
/// unparseable or non-positive, so a weird value never blocks a download.
fn parse_dash_frame_rate(value: &str) -> Option<f64> {
    let value = value.trim();
    let rate = if let Some((numerator, denominator)) = value.split_once('/') {
        let numerator: f64 = numerator.trim().parse().ok()?;
        let denominator: f64 = denominator.trim().parse().ok()?;
        if denominator == 0.0 { return None; }
        numerator / denominator
    } else {
        value.parse::<f64>().ok()?
    };
    if rate.is_finite() && rate > 0.0 { Some(rate) } else { None }
}

/// Short display for a frame rate in a compat error ("30", "29.97").
fn format_dash_frame_rate(rate: f64) -> String {
    if rate.fract() == 0.0 { format!("{rate:.0}") } else { format!("{rate:.2}") }
}

fn inherited_dash_template(
    document: &DashDocument,
    period: Option<&DashPeriod>,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
) -> DashTemplate {
    if let (Some(period), Some(adaptation_index), Some(representation_index)) = (period, adaptation_index, representation_index) {
        if let Some(template) = period.adaptations.get(adaptation_index)
            .and_then(|adaptation| adaptation.representations.get(representation_index))
            .and_then(|representation| representation.template.clone()) {
            return template;
        }
    }
    if let Some(template) = period
        .and_then(|period| adaptation_index.and_then(|index| period.adaptations.get(index)))
        .and_then(|adaptation| adaptation.template.clone()) {
        return template;
    }
    period.and_then(|period| period.template.clone())
        .or_else(|| document.template.clone())
        .unwrap_or_else(|| DashTemplate { timescale: 1, start_number: 1, ..DashTemplate::default() })
}

fn apply_dash_template_attributes(
    mut template: DashTemplate,
    attributes: &HashMap<String, String>,
) -> DashTemplate {
    if let Some(value) = attributes.get("media") { template.media = Some(value.clone()); }
    if let Some(value) = attributes.get("initialization") { template.initialization = Some(value.clone()); }
    if let Some(value) = attributes.get("timescale").and_then(|value| value.parse::<u64>().ok()) {
        template.timescale = value.max(1);
    }
    if let Some(value) = attributes.get("duration").and_then(|value| value.parse::<u64>().ok()) {
        template.duration = Some(value);
    }
    if let Some(value) = attributes.get("startNumber").and_then(|value| value.parse::<u64>().ok()) {
        template.start_number = value;
    }
    if template.timescale == 0 { template.timescale = 1; }
    if template.start_number == 0 { template.start_number = 1; }
    template
}

fn dash_period_mut<'a>(
    document: &'a mut DashDocument,
    period_index: Option<usize>,
) -> Option<&'a mut DashPeriod> {
    document.periods.get_mut(period_index?)
}

fn dash_template_mut<'a>(
    document: &'a mut DashDocument,
    scope: DashScope,
    period_index: Option<usize>,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
) -> Option<&'a mut DashTemplate> {
    match scope {
        DashScope::Mpd => document.template.as_mut(),
        DashScope::Period => dash_period_mut(document, period_index)?.template.as_mut(),
        DashScope::Adaptation => dash_period_mut(document, period_index)?
            .adaptations.get_mut(adaptation_index?)?.template.as_mut(),
        DashScope::Representation => dash_period_mut(document, period_index)?
            .adaptations.get_mut(adaptation_index?)?
            .representations.get_mut(representation_index?)?.template.as_mut(),
    }
}

fn dash_segment_list_mut<'a>(
    document: &'a mut DashDocument,
    scope: DashScope,
    period_index: Option<usize>,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
) -> Option<&'a mut DashSegmentList> {
    match scope {
        DashScope::Mpd => document.segment_list.as_mut(),
        DashScope::Period => dash_period_mut(document, period_index)?.segment_list.as_mut(),
        DashScope::Adaptation => dash_period_mut(document, period_index)?
            .adaptations.get_mut(adaptation_index?)?.segment_list.as_mut(),
        DashScope::Representation => dash_period_mut(document, period_index)?
            .adaptations.get_mut(adaptation_index?)?
            .representations.get_mut(representation_index?)?.segment_list.as_mut(),
    }
}

fn dash_segment_base_mut<'a>(
    document: &'a mut DashDocument,
    scope: DashScope,
    period_index: Option<usize>,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
) -> Option<&'a mut DashSegmentBase> {
    match scope {
        DashScope::Mpd => document.segment_base.as_mut(),
        DashScope::Period => dash_period_mut(document, period_index)?.segment_base.as_mut(),
        DashScope::Adaptation => dash_period_mut(document, period_index)?
            .adaptations.get_mut(adaptation_index?)?.segment_base.as_mut(),
        DashScope::Representation => dash_period_mut(document, period_index)?
            .adaptations.get_mut(adaptation_index?)?
            .representations.get_mut(representation_index?)?.segment_base.as_mut(),
    }
}

fn set_dash_base_url(
    document: &mut DashDocument,
    scope: DashScope,
    period_index: Option<usize>,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
    value: String,
) {
    if value.trim().is_empty() { return; }
    match scope {
        DashScope::Mpd => { if document.base_url.is_none() { document.base_url = Some(value); } }
        DashScope::Period => {
            if let Some(period) = dash_period_mut(document, period_index) {
                if period.base_url.is_none() { period.base_url = Some(value); }
            }
        }
        DashScope::Adaptation => {
            if let Some(adaptation) = dash_period_mut(document, period_index)
                .and_then(|period| adaptation_index.and_then(|index| period.adaptations.get_mut(index))) {
                if adaptation.base_url.is_none() { adaptation.base_url = Some(value); }
            }
        }
        DashScope::Representation => {
            if let Some(representation) = dash_period_mut(document, period_index)
                .and_then(|period| adaptation_index.and_then(|index| period.adaptations.get_mut(index)))
                .and_then(|adaptation| representation_index.and_then(|index| adaptation.representations.get_mut(index))) {
                if representation.base_url.is_none() { representation.base_url = Some(value); }
            }
        }
    }
}

fn dash_start_element(
    document: &mut DashDocument,
    state: &mut DashParserState,
    name: &str,
    attributes: HashMap<String, String>,
) {
    let scope_before = dash_scope(state);
    let period_index = state.active_period;
    let (adaptation_index, representation_index) = state.active_representation
        .map(|(adaptation, representation)| (Some(adaptation), Some(representation)))
        .unwrap_or((state.active_adaptation, None));
    match name {
        "MPD" => {
            document.is_static = attributes.get("type").map(|value| value.eq_ignore_ascii_case("static")).unwrap_or(true);
            document.duration_seconds = attributes.get("mediaPresentationDuration").and_then(|value| parse_iso_duration(value));
        }
        "Period" => {
            document.periods.push(DashPeriod {
                duration_seconds: attributes.get("duration").and_then(|value| parse_iso_duration(value)),
                ..DashPeriod::default()
            });
            state.active_period = Some(document.periods.len() - 1);
        }
        "AdaptationSet" => {
            if state.active_period.is_none() {
                document.periods.push(DashPeriod::default());
                state.active_period = Some(document.periods.len() - 1);
            }
            let period_index = state.active_period.unwrap();
            document.periods[period_index].adaptations.push(DashAdaptation {
                content_type: attributes.get("contentType").cloned().unwrap_or_default(),
                mime_type: attributes.get("mimeType").cloned().unwrap_or_default(),
                frame_rate: attributes.get("frameRate").and_then(|value| parse_dash_frame_rate(value)),
                audio_sampling_rate: attributes.get("audioSamplingRate").and_then(|value| value.trim().parse().ok()),
                ..DashAdaptation::default()
            });
            state.active_adaptation = Some(document.periods[period_index].adaptations.len() - 1);
        }
        "Representation" => {
            if let (Some(period_index), Some(adaptation_index)) = (state.active_period, state.active_adaptation) {
                let representation_index = document.periods[period_index].adaptations[adaptation_index].representations.len();
                document.periods[period_index].adaptations[adaptation_index].representations.push(DashRepresentation {
                    id: attributes.get("id").cloned().unwrap_or_else(|| format!("representation-{representation_index}")),
                    width: attributes.get("width").and_then(|value| value.parse().ok()),
                    height: attributes.get("height").and_then(|value| value.parse().ok()),
                    bandwidth: attributes.get("bandwidth").and_then(|value| value.parse().ok()),
                    mime_type: attributes.get("mimeType").cloned().unwrap_or_default(),
                    codecs: attributes.get("codecs").cloned(),
                    frame_rate: attributes.get("frameRate").and_then(|value| parse_dash_frame_rate(value)),
                    audio_sampling_rate: attributes.get("audioSamplingRate").and_then(|value| value.trim().parse().ok()),
                    ..DashRepresentation::default()
                });
                state.active_representation = Some((adaptation_index, representation_index));
            }
        }
        "SegmentTemplate" => {
            let period = period_index.and_then(|index| document.periods.get(index));
            let inherited = inherited_dash_template(document, period, adaptation_index, representation_index);
            let template = apply_dash_template_attributes(inherited, &attributes);
            match scope_before {
                DashScope::Mpd => document.template = Some(template),
                DashScope::Period => {
                    if let Some(period) = dash_period_mut(document, period_index) { period.template = Some(template); }
                }
                DashScope::Adaptation => {
                    if let Some(index) = state.active_adaptation {
                        if let Some(period) = dash_period_mut(document, period_index) {
                            period.adaptations[index].template = Some(template);
                        }
                    }
                }
                DashScope::Representation => {
                    if let Some((adaptation, representation)) = state.active_representation {
                        if let Some(period) = dash_period_mut(document, period_index) {
                            period.adaptations[adaptation].representations[representation].template = Some(template);
                        }
                    }
                }
            }
            state.active_template_scope = Some(scope_before);
        }
        "S" => {
            if let Some(scope) = state.active_template_scope {
                let point = DashTimelinePoint {
                    time: attributes.get("t").and_then(|value| value.parse().ok()),
                    duration: attributes.get("d").and_then(|value| value.parse::<u64>().ok()).unwrap_or(0),
                    repeat: attributes.get("r").and_then(|value| value.parse::<i64>().ok()).unwrap_or(0),
                };
                if point.duration > 0 {
                    if let Some(template) = dash_template_mut(document, scope, period_index, adaptation_index, representation_index) {
                        template.timeline.push(point);
                    }
                }
            }
        }
        "SegmentList" => {
            let list = DashSegmentList::default();
            match scope_before {
                DashScope::Mpd => document.segment_list = Some(list),
                DashScope::Period => {
                    if let Some(period) = dash_period_mut(document, period_index) { period.segment_list = Some(list); }
                }
                DashScope::Adaptation => {
                    if let Some(index) = state.active_adaptation {
                        if let Some(period) = dash_period_mut(document, period_index) {
                            period.adaptations[index].segment_list = Some(list);
                        }
                    }
                }
                DashScope::Representation => {
                    if let Some((adaptation, representation)) = state.active_representation {
                        if let Some(period) = dash_period_mut(document, period_index) {
                            period.adaptations[adaptation].representations[representation].segment_list = Some(list);
                        }
                    }
                }
            }
            state.active_segment_list_scope = Some(scope_before);
        }
        "SegmentURL" => {
            if let Some(scope) = state.active_segment_list_scope {
                let entry = DashSegmentListEntry {
                    media: attributes.get("media").cloned(),
                    media_range: attributes.get("mediaRange").and_then(|value| parse_dash_byte_range(value)),
                };
                if let Some(list) = dash_segment_list_mut(document, scope, period_index, adaptation_index, representation_index) {
                    list.segments.push(entry);
                }
            }
        }
        "SegmentBase" => {
            let base = DashSegmentBase {
                index_range: attributes.get("indexRange").and_then(|value| parse_dash_byte_range(value)),
                ..DashSegmentBase::default()
            };
            match scope_before {
                DashScope::Mpd => document.segment_base = Some(base),
                DashScope::Period => {
                    if let Some(period) = dash_period_mut(document, period_index) { period.segment_base = Some(base); }
                }
                DashScope::Adaptation => {
                    if let Some(index) = state.active_adaptation {
                        if let Some(period) = dash_period_mut(document, period_index) {
                            period.adaptations[index].segment_base = Some(base);
                        }
                    }
                }
                DashScope::Representation => {
                    if let Some((adaptation, representation)) = state.active_representation {
                        if let Some(period) = dash_period_mut(document, period_index) {
                            period.adaptations[adaptation].representations[representation].segment_base = Some(base);
                        }
                    }
                }
            }
            state.active_segment_base_scope = Some(scope_before);
        }
        "Initialization" => {
            let source_url = attributes.get("sourceURL").cloned();
            let range = attributes.get("range").and_then(|value| parse_dash_byte_range(value));
            if let Some(scope) = state.active_segment_base_scope {
                if let Some(base) = dash_segment_base_mut(document, scope, period_index, adaptation_index, representation_index) {
                    base.init_source_url = source_url;
                    base.init_range = range;
                }
            } else if let Some(scope) = state.active_segment_list_scope {
                if let Some(list) = dash_segment_list_mut(document, scope, period_index, adaptation_index, representation_index) {
                    list.init_source_url = source_url;
                    list.init_range = range;
                }
            }
        }
        "AudioChannelConfiguration" => {
            // Only the audio_channel_configuration schemes carry a plain
            // channel count in `value` (MPEG 23003:3 and Dolby); other
            // schemes use different semantics, so their values are ignored
            // rather than compared across periods.
            let scheme = attributes.get("schemeIdUri").cloned().unwrap_or_default();
            if scheme.ends_with("audio_channel_configuration:2011") {
                if let Some(channels) = attributes.get("value").and_then(|value| value.trim().parse::<u32>().ok()) {
                    match scope_before {
                        DashScope::Representation => {
                            if let Some((adaptation, representation)) = state.active_representation {
                                if let Some(period) = dash_period_mut(document, period_index) {
                                    if let Some(rep) = period.adaptations.get_mut(adaptation)
                                        .and_then(|adaptation| adaptation.representations.get_mut(representation)) {
                                        rep.audio_channels = Some(channels);
                                    }
                                }
                            }
                        }
                        _ => {
                            if let Some(index) = state.active_adaptation {
                                if let Some(period) = dash_period_mut(document, period_index) {
                                    if let Some(adaptation) = period.adaptations.get_mut(index) {
                                        adaptation.audio_channels = Some(channels);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "BaseURL" => {
            state.base_scope = Some(scope_before);
            state.base_text.clear();
        }
        "ContentProtection" => document.protected = true,
        _ => {}
    }
}

fn dash_end_element(document: &mut DashDocument, state: &mut DashParserState, name: &str) {
    match name {
        "BaseURL" => {
            if let Some(scope) = state.base_scope.take() {
                let period_index = state.active_period;
                let (adaptation_index, representation_index) = state.active_representation
                    .map(|(adaptation, representation)| (Some(adaptation), Some(representation)))
                    .unwrap_or((state.active_adaptation, None));
                set_dash_base_url(document, scope, period_index, adaptation_index, representation_index, state.base_text.trim().to_string());
            }
            state.base_text.clear();
        }
        "SegmentTemplate" => state.active_template_scope = None,
        "SegmentList" => state.active_segment_list_scope = None,
        "SegmentBase" => state.active_segment_base_scope = None,
        "Representation" => state.active_representation = None,
        "AdaptationSet" => state.active_adaptation = None,
        "Period" => state.active_period = None,
        _ => {}
    }
}

fn parse_dash_document(body: &str) -> Result<DashDocument> {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);
    let mut document = DashDocument { is_static: true, ..DashDocument::default() };
    let mut state = DashParserState::default();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_xml_name(element.name().as_ref());
                let attributes = xml_attributes(&element)?;
                dash_start_element(&mut document, &mut state, &name, attributes);
            }
            Ok(Event::Empty(element)) => {
                let name = local_xml_name(element.name().as_ref());
                let attributes = xml_attributes(&element)?;
                dash_start_element(&mut document, &mut state, &name, attributes);
                dash_end_element(&mut document, &mut state, &name);
            }
            Ok(Event::Text(text)) => {
                if state.base_scope.is_some() {
                    state.base_text.push_str(&text.decode()?.into_owned());
                }
            }
            Ok(Event::GeneralRef(reference)) => {
                if state.base_scope.is_some() {
                    if let Some(character) = reference.resolve_char_ref()? {
                        state.base_text.push(character);
                    } else {
                        let resolved = match reference.as_ref() {
                            b"amp" => Some("&"), b"lt" => Some("<"), b"gt" => Some(">"),
                            b"apos" => Some("'"), b"quot" => Some("\""), _ => None,
                        };
                        if let Some(value) = resolved { state.base_text.push_str(value); }
                    }
                }
            }
            Ok(Event::CData(text)) => {
                if state.base_scope.is_some() {
                    state.base_text.push_str(&String::from_utf8_lossy(text.as_ref()));
                }
            }
            Ok(Event::End(element)) => {
                let name = local_xml_name(element.name().as_ref());
                dash_end_element(&mut document, &mut state, &name);
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(anyhow!("Invalid DASH XML: {error}")),
        }
    }
    Ok(document)
}

fn merge_dash_base_url(
    manifest_url: &str,
    document: &DashDocument,
    period: &DashPeriod,
    adaptation: &DashAdaptation,
    representation: &DashRepresentation,
) -> Result<Url> {
    let mut base = Url::parse(manifest_url)?;
    for component in [
        document.base_url.as_deref(),
        period.base_url.as_deref(),
        adaptation.base_url.as_deref(),
        representation.base_url.as_deref(),
    ].into_iter().flatten() {
        base = base.join(component)?;
    }
    Ok(base)
}

fn effective_dash_segment_list(
    document: &DashDocument,
    period: &DashPeriod,
    adaptation: &DashAdaptation,
    representation: &DashRepresentation,
) -> Option<DashSegmentList> {
    representation.segment_list.clone()
        .or_else(|| adaptation.segment_list.clone())
        .or_else(|| period.segment_list.clone())
        .or_else(|| document.segment_list.clone())
}

fn effective_dash_segment_base(
    document: &DashDocument,
    period: &DashPeriod,
    adaptation: &DashAdaptation,
    representation: &DashRepresentation,
) -> Option<DashSegmentBase> {
    representation.segment_base.clone()
        .or_else(|| adaptation.segment_base.clone())
        .or_else(|| period.segment_base.clone())
        .or_else(|| document.segment_base.clone())
}

fn effective_dash_mime_type(adaptation: &DashAdaptation, representation: &DashRepresentation) -> String {
    if representation.mime_type.is_empty() { adaptation.mime_type.clone() } else { representation.mime_type.clone() }
}

fn expand_dash_template(
    template: &str,
    representation: &DashRepresentation,
    number: u64,
    time: u64,
) -> Result<String> {
    let mut output = String::new();
    let mut cursor = 0;
    while let Some(relative_start) = template[cursor..].find('$') {
        let start = cursor + relative_start;
        output.push_str(&template[cursor..start]);
        if template.as_bytes().get(start + 1) == Some(&b'$') {
            output.push('$');
            cursor = start + 2;
            continue;
        }
        let remainder = &template[start + 1..];
        let Some(close) = remainder.find('$') else {
            // A lone dollar is literal text; only a paired "$...$" starts a token.
            output.push('$');
            cursor = start + 1;
            continue;
        };
        let token = &remainder[..close];
        let value = if token == "RepresentationID" {
            representation.id.clone()
        } else if token == "Bandwidth" {
            representation.bandwidth.unwrap_or(0).to_string()
        } else if token == "Number" {
            number.to_string()
        } else if token == "Time" {
            time.to_string()
        } else if let Some(format_spec) = token.strip_prefix("Number%0").and_then(|value| value.strip_suffix('d')) {
            let width: usize = format_spec.parse().context("Invalid DASH Number format width")?;
            format!("{number:0width$}")
        } else {
            return Err(anyhow!("Unsupported DASH template token ${token}$"));
        };
        output.push_str(&value);
        cursor = start + close + 2;
    }
    output.push_str(&template[cursor..]);
    Ok(output)
}

fn join_dash_resource(base: &Url, reference: &str) -> Result<Url> {
    let mut resolved = base.join(reference)?;
    // Relative SegmentTemplate URLs commonly inherit auth tokens carried by
    // a BaseURL query string. RFC-style path joining drops the base query, so
    // propagate it only when the child resource did not provide its own query.
    if reference.split('#').next().unwrap_or(reference).contains('?') == false {
        if resolved.query().is_none() {
            if let Some(query) = base.query() {
                resolved.set_query(Some(query));
            }
        }
    }
    Ok(resolved)
}

fn resolve_dash_track(
    document: &DashDocument,
    period: &DashPeriod,
    adaptation: &DashAdaptation,
    representation: &DashRepresentation,
    manifest_url: &str,
    _is_audio: bool,
) -> Result<DashResolvedTrack> {
    let mime_type = effective_dash_mime_type(adaptation, representation);
    let base = merge_dash_base_url(manifest_url, document, period, adaptation, representation)?;
    // Encoding parameters are declared on the Representation or inherited
    // from the AdaptationSet; the multi-period compat check compares them
    // (only when both periods declare a value, so an undeclared parameter
    // never blocks a download).
    let frame_rate = representation.frame_rate.or(adaptation.frame_rate);
    let audio_sampling_rate = representation.audio_sampling_rate.or(adaptation.audio_sampling_rate);
    let audio_channels = representation.audio_channels.or(adaptation.audio_channels);

    // SegmentList: every segment is listed explicitly in the manifest, so
    // resolution is fully synchronous. An entry without a media URL is a
    // byte range of the representation's own resource (its BaseURL).
    if let Some(list) = effective_dash_segment_list(document, period, adaptation, representation) {
        if list.segments.is_empty() {
            return Err(anyhow!("DASH SegmentList for representation '{}' contains no segments", representation.id));
        }
        // A range-only init is a byte range of the representation's own
        // resource (the standard single-file form), exactly like SegmentBase.
        let initialization_url = match (&list.init_source_url, list.init_range) {
            (Some(source), _) => Some(join_dash_resource(&base, source)?.to_string()),
            (None, Some(_)) => Some(base.to_string()),
            (None, None) => None,
        };
        let mut segment_urls = Vec::with_capacity(list.segments.len());
        let mut segment_ranges = Vec::with_capacity(list.segments.len());
        for entry in &list.segments {
            match (&entry.media, entry.media_range) {
                (Some(media), range) => {
                    segment_urls.push(join_dash_resource(&base, media)?.to_string());
                    segment_ranges.push(range);
                }
                (None, Some(range)) => {
                    segment_urls.push(base.to_string());
                    segment_ranges.push(Some(range));
                }
                (None, None) => {
                    return Err(anyhow!("DASH SegmentList entry for representation '{}' has neither a media URL nor a mediaRange", representation.id));
                }
            }
        }
        return Ok(DashResolvedTrack {
            mime_type,
            codecs: representation.codecs.clone(),
            width: representation.width,
            height: representation.height,
            frame_rate,
            audio_sampling_rate,
            audio_channels,
            initialization_url,
            initialization_range: list.init_range,
            segment_urls,
            segment_ranges,
            segment_base: None,
        });
    }

    // SegmentBase: the media segments are byte ranges of a single file and
    // only the index (sidx box) knows their boundaries, so resolution stops
    // here with a plan; the index is fetched and expanded at download time.
    if let Some(list) = effective_dash_segment_base(document, period, adaptation, representation) {
        let index_range = list.index_range.ok_or_else(|| anyhow!(
            "DASH SegmentBase for representation '{}' has no indexRange; the segment index cannot be located",
            representation.id
        ))?;
        let (init_url, init_range) = match (&list.init_source_url, list.init_range) {
            (Some(source), range) => (Some(join_dash_resource(&base, source)?.to_string()), range),
            (None, Some(range)) => (Some(base.to_string()), Some(range)),
            (None, None) => {
                return Err(anyhow!(
                    "DASH SegmentBase for representation '{}' has no usable initialization segment (no Initialization sourceURL or range)",
                    representation.id
                ));
            }
        };
        return Ok(DashResolvedTrack {
            mime_type,
            codecs: representation.codecs.clone(),
            width: representation.width,
            height: representation.height,
            frame_rate,
            audio_sampling_rate,
            audio_channels,
            initialization_url: None,
            initialization_range: None,
            segment_urls: Vec::new(),
            segment_ranges: Vec::new(),
            segment_base: Some(DashSegmentBasePlan {
                base_url: base.to_string(),
                index_range,
                init_url,
                init_range,
            }),
        });
    }

    let template = representation.template.clone()
        .or_else(|| adaptation.template.clone())
        .or_else(|| period.template.clone())
        .or_else(|| document.template.clone())
        .ok_or_else(|| anyhow!("DASH representation has no inherited SegmentTemplate"))?;
    let media_template = template.media.as_deref().ok_or_else(|| anyhow!("DASH SegmentTemplate has no media attribute"))?;
    if template.timescale == 0 { return Err(anyhow!("DASH SegmentTemplate timescale is zero")); }
    let timescale = template.timescale.max(1);
    let period_duration = period.duration_seconds.or(document.duration_seconds)
        .ok_or_else(|| anyhow!("DASH static VOD MPD does not declare a period duration"))?;
    let mut sequence = Vec::<(u64, u64)>::new();
    let mut current_time = 0_u64;
    let mut current_number = template.start_number.max(1);
    const MAX_DASH_SEGMENTS: u64 = 100_000;

    if !template.timeline.is_empty() {
        for (index, point) in template.timeline.iter().enumerate() {
            if let Some(time) = point.time { current_time = time; }
            let count = if point.repeat >= 0 {
                point.repeat as u64 + 1
            } else {
                let end_time = template.timeline.get(index + 1).and_then(|next| next.time)
                    .unwrap_or_else(|| (period_duration * timescale as f64).ceil() as u64);
                end_time.saturating_sub(current_time).saturating_add(point.duration - 1) / point.duration
            };
            if sequence.len() as u64 + count > MAX_DASH_SEGMENTS {
                return Err(anyhow!("DASH manifest expands to too many media segments"));
            }
            for _ in 0..count {
                sequence.push((current_number, current_time));
                current_number = current_number.saturating_add(1);
                current_time = current_time.saturating_add(point.duration);
            }
        }
    } else if let Some(duration) = template.duration.filter(|duration| *duration > 0) {
        let total_units = (period_duration * timescale as f64).ceil() as u64;
        let count = total_units.saturating_add(duration - 1) / duration;
        if count == 0 || count > MAX_DASH_SEGMENTS {
            return Err(anyhow!("DASH SegmentTemplate duration produced an invalid segment count"));
        }
        for _ in 0..count {
            sequence.push((current_number, current_time));
            current_number = current_number.saturating_add(1);
            current_time = current_time.saturating_add(duration);
        }
    } else {
        return Err(anyhow!("DASH SegmentTemplate needs SegmentTimeline or duration"));
    }
    if sequence.is_empty() { return Err(anyhow!("DASH representation contains no media segments")); }

    let initialization_url = template.initialization.as_deref()
        .map(|value| expand_dash_template(value, representation, template.start_number.max(1), 0))
        .transpose()?
        .map(|value| join_dash_resource(&base, &value).map(|url| url.to_string()))
        .transpose()?;
    let mut segment_urls = Vec::with_capacity(sequence.len());
    for (number, time) in sequence {
        let resolved = expand_dash_template(media_template, representation, number, time)?;
        segment_urls.push(join_dash_resource(&base, &resolved)?.to_string());
    }
    let segment_count = segment_urls.len();
    Ok(DashResolvedTrack {
        mime_type,
        codecs: representation.codecs.clone(),
        width: representation.width,
        height: representation.height,
        frame_rate,
        audio_sampling_rate,
        audio_channels,
        initialization_url,
        initialization_range: None,
        segment_urls,
        segment_ranges: vec![None; segment_count],
        segment_base: None,
    })
}

fn is_audio_adaptation(adaptation: &DashAdaptation) -> bool {
    adaptation.content_type.eq_ignore_ascii_case("audio")
        || adaptation.mime_type.to_ascii_lowercase().starts_with("audio/")
        || adaptation.representations.iter().any(|rep| rep.mime_type.to_ascii_lowercase().starts_with("audio/"))
}

fn is_video_representation(adaptation: &DashAdaptation, representation: &DashRepresentation) -> bool {
    adaptation.content_type.eq_ignore_ascii_case("video")
        || adaptation.mime_type.to_ascii_lowercase().starts_with("video/")
        || representation.mime_type.to_ascii_lowercase().starts_with("video/")
        || representation.height.is_some()
}

fn dash_codec_label(codecs: &Option<String>) -> String {
    codecs.clone().unwrap_or_else(|| "unspecified codec".into())
}

fn check_dash_period_compat(video_tracks: &[DashResolvedTrack], audio_tracks: &[DashResolvedTrack]) -> Result<()> {
    for (index, track) in video_tracks.iter().enumerate().skip(1) {
        let first = &video_tracks[0];
        if track.mime_type != first.mime_type
            || track.codecs != first.codecs
            || track.width != first.width
            || track.height != first.height
        {
            return Err(anyhow!(
                "DASH Period {} video uses different parameters ({}; {}; {}x{}) than period 1 ({}; {}; {}x{}); multi-period concatenation would produce an unplayable file",
                index + 1,
                track.mime_type, dash_codec_label(&track.codecs), track.width.unwrap_or(0), track.height.unwrap_or(0),
                first.mime_type, dash_codec_label(&first.codecs), first.width.unwrap_or(0), first.height.unwrap_or(0)
            ));
        }
        // Frame rates are compared only when both periods declare one, and
        // with a 0.1% tolerance so the same rate written two ways
        // ("30000/1001" vs "29.97") does not false-positive.
        if let (Some(rate), Some(first_rate)) = (track.frame_rate, first.frame_rate) {
            let difference = (rate - first_rate).abs() / first_rate.max(rate);
            if difference > 0.001 {
                return Err(anyhow!(
                    "DASH Period {} video uses a different frame rate ({} fps) than period 1 ({} fps); multi-period concatenation would produce an unplayable file",
                    index + 1,
                    format_dash_frame_rate(rate), format_dash_frame_rate(first_rate)
                ));
            }
        }
    }
    for (index, track) in audio_tracks.iter().enumerate().skip(1) {
        let first = &audio_tracks[0];
        if track.mime_type != first.mime_type || track.codecs != first.codecs {
            return Err(anyhow!(
                "DASH Period {} audio uses different parameters ({}; {}) than period 1 ({}; {}); multi-period concatenation would produce an unplayable file",
                index + 1,
                track.mime_type, dash_codec_label(&track.codecs),
                first.mime_type, dash_codec_label(&first.codecs)
            ));
        }
        if let (Some(rate), Some(first_rate)) = (track.audio_sampling_rate, first.audio_sampling_rate) {
            if rate != first_rate {
                return Err(anyhow!(
                    "DASH Period {} audio uses a different sample rate ({} Hz) than period 1 ({} Hz); multi-period concatenation would produce an unplayable file",
                    index + 1, rate, first_rate
                ));
            }
        }
        if let (Some(channels), Some(first_channels)) = (track.audio_channels, first.audio_channels) {
            if channels != first_channels {
                return Err(anyhow!(
                    "DASH Period {} audio uses a different channel count ({}) than period 1 ({}); multi-period concatenation would produce an unplayable file",
                    index + 1, channels, first_channels
                ));
            }
        }
    }
    Ok(())
}

fn parse_dash_representations(body: &str, manifest_url: &str) -> Vec<VideoVariant> {
    let document = match parse_dash_document(body) {
        Ok(document) => document,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    if !document.is_static || document.protected || document.periods.is_empty() {
        let note = if !document.is_static {
            "Dynamic/live DASH MPDs are not supported"
        } else if document.protected {
            "ContentProtection/DRM DASH streams are not supported"
        } else {
            "This DASH MPD contains no Periods"
        };
        for period in &document.periods {
            for adaptation in &period.adaptations {
                for representation in &adaptation.representations {
                    if is_video_representation(adaptation, representation) {
                        out.push(VideoVariant {
                            kind: "dash".into(),
                            quality: representation.height.map(|height| format!("{height}p")).unwrap_or_else(|| representation.id.clone()),
                            width: representation.width,
                            height: representation.height,
                            bandwidth: representation.bandwidth,
                            url: manifest_url.to_string(),
                            downloadable: false,
                            note: Some(note.to_string()),
                            representation_id: Some(representation.id.clone()),
                            audio_representation_id: None,
                            audio_only: Some(false),
                            requires_ffmpeg: None,
                        });
                    }
                }
            }
        }
        return out;
    }

    let first_period = &document.periods[0];
    let multi_period = document.periods.len() > 1;
    let audio_representation = first_period.adaptations.iter().enumerate()
        .filter(|(_, adaptation)| is_audio_adaptation(adaptation))
        .flat_map(|(adaptation_index, adaptation)| adaptation.representations.iter().enumerate()
            .map(move |(representation_index, representation)| (adaptation_index, representation_index, representation)))
        .max_by_key(|(_, _, representation)| representation.bandwidth.unwrap_or(0));

    for adaptation in &first_period.adaptations {
        for representation in adaptation.representations.iter().filter(|representation| is_video_representation(adaptation, representation)) {
            // Resolve this representation in every period; the resolved tracks
            // feed the cross-period compatibility check below.
            let mut video_tracks = Vec::new();
            let mut failure: Option<String> = None;
            for (period_index, period) in document.periods.iter().enumerate() {
                match find_dash_representation(period, &representation.id, false) {
                    None => {
                        failure = Some(format!("Period {} has no DASH video representation '{}'", period_index + 1, representation.id));
                        break;
                    }
                    Some((found_adaptation, found_representation)) => {
                        match resolve_dash_track(&document, period, found_adaptation, found_representation, manifest_url, false) {
                            Ok(track) => video_tracks.push(track),
                            Err(error) => { failure = Some(error.to_string()); break; }
                        }
                    }
                }
            }
            let mut audio_tracks = Vec::new();
            if failure.is_none() {
                if let Some((_, _, audio)) = audio_representation {
                    for (period_index, period) in document.periods.iter().enumerate() {
                        match find_dash_representation(period, &audio.id, true) {
                            None => {
                                failure = Some(format!("Period {} has no DASH audio representation '{}'", period_index + 1, audio.id));
                                break;
                            }
                            Some((found_adaptation, found_representation)) => {
                                match resolve_dash_track(&document, period, found_adaptation, found_representation, manifest_url, true) {
                                    Ok(track) => audio_tracks.push(track),
                                    Err(error) => { failure = Some(format!("Audio representation is unsupported: {error}")); break; }
                                }
                            }
                        }
                    }
                }
            }
            if failure.is_none() {
                if let Err(error) = check_dash_period_compat(&video_tracks, &audio_tracks) {
                    failure = Some(error.to_string());
                }
            }
            let (downloadable, note) = match failure {
                Some(error) => (false, Some(error)),
                None => {
                    let note = if multi_period {
                        format!("Multi-period DASH ({} periods); periods are concatenated with FFmpeg", document.periods.len())
                    } else if audio_representation.is_some() {
                        "Static DASH segments; separate audio will be muxed with FFmpeg".to_string()
                    } else {
                        "Static DASH VOD segments".to_string()
                    };
                    (true, Some(note))
                }
            };
            out.push(VideoVariant {
                kind: "dash".into(),
                quality: representation.height.map(|height| format!("{height}p")).unwrap_or_else(|| representation.id.clone()),
                width: representation.width,
                height: representation.height,
                bandwidth: representation.bandwidth,
                url: manifest_url.to_string(),
                downloadable,
                note,
                representation_id: Some(representation.id.clone()),
                audio_representation_id: audio_representation.map(|(_, _, audio)| audio.id.clone()),
                audio_only: Some(false),
                requires_ffmpeg: if multi_period { Some(true) } else { None },
            });
        }
    }
    out.sort_by(|left, right| right.height.unwrap_or(0).cmp(&left.height.unwrap_or(0)));
    out
}

async fn ffmpeg_available() -> bool {
    let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    TokioCommand::new(executable).arg("-version").output().await
        .map(|output| output.status.success()).unwrap_or(false)
}

async fn annotate_dash_variants(variants: &mut [VideoVariant]) {
    let available = ffmpeg_available().await;
    if available { return; }
    for variant in variants.iter_mut().filter(|variant| variant.kind == "dash") {
        if variant.audio_representation_id.is_some() {
            variant.downloadable = false;
            variant.note = Some("This DASH MPD has separate audio and video tracks. Install FFmpeg or set THUNDERLOAD_FFMPEG before downloading.".into());
        } else if variant.requires_ffmpeg == Some(true) {
            variant.downloadable = false;
            variant.note = Some("This multi-period DASH MPD needs FFmpeg to concatenate the periods. Install FFmpeg or set THUNDERLOAD_FFMPEG before downloading.".into());
        }
    }
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

/// One addressable piece of an HLS playlist: a whole segment file, or a
/// byte range (`EXT-X-BYTERANGE`) of a larger resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct HlsSegmentRef {
    url: String,
    /// (start offset, length) when the segment is an EXT-X-BYTERANGE slice.
    range: Option<(u64, u64)>,
}

/// Everything needed to (re)download one HLS job, persisted as the
/// checkpoint sidecar next to the target. Segment files live in a stable
/// work directory derived from the target, so a fresh call — including from
/// a fresh process — resumes purely from disk: no in-memory state is used.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HlsJobState {
    version: u32,
    playlist_url: String,
    media_url: String,
    audio_playlist_url: Option<String>,
    referer: Option<String>,
    extension: String,
    video_init: Option<HlsSegmentRef>,
    video_segments: Vec<HlsSegmentRef>,
    video_init_done: bool,
    video_done: Vec<usize>,
    audio_init: Option<HlsSegmentRef>,
    audio_segments: Vec<HlsSegmentRef>,
    audio_init_done: bool,
    audio_done: Vec<usize>,
}

impl HlsJobState {
    fn total_segments(&self) -> usize {
        usize::from(self.video_init.is_some()) + self.video_segments.len()
            + usize::from(self.audio_init.is_some()) + self.audio_segments.len()
    }

    fn done_segments(&self) -> usize {
        usize::from(self.video_init_done) + self.video_done.len()
            + usize::from(self.audio_init_done) + self.audio_done.len()
    }

    /// A checkpoint is reusable only when it describes exactly the same
    /// job: same playlist, same variant, same audio rendition, same
    /// segment list in the same order. Anything else is discarded and the
    /// job restarts from scratch instead of joining two different files.
    fn matches_plan(&self, plan: &HlsJobState) -> bool {
        self.version == plan.version
            && self.playlist_url == plan.playlist_url
            && self.media_url == plan.media_url
            && self.audio_playlist_url == plan.audio_playlist_url
            && self.referer == plan.referer
            && self.extension == plan.extension
            && self.video_init == plan.video_init
            && self.video_segments == plan.video_segments
            && self.audio_init == plan.audio_init
            && self.audio_segments == plan.audio_segments
    }
}

fn parse_hls_byterange(value: &str) -> Result<(u64, Option<u64>)> {
    let (length, offset) = match value.split_once('@') {
        Some((length, offset)) => (
            length.parse::<u64>().context("Invalid EXT-X-BYTERANGE length")?,
            Some(offset.parse::<u64>().context("Invalid EXT-X-BYTERANGE offset")?),
        ),
        None => (value.parse::<u64>().context("Invalid EXT-X-BYTERANGE length")?, None),
    };
    if length == 0 {
        return Err(anyhow!("EXT-X-BYTERANGE length is zero"));
    }
    Ok((length, offset))
}

struct HlsMediaPlaylist {
    init: Option<HlsSegmentRef>,
    segments: Vec<HlsSegmentRef>,
}

/// Parses one HLS media playlist: segment URIs, the EXT-X-MAP init segment
/// and EXT-X-BYTERANGE slices. Encrypted segments are rejected; a playlist
/// without EXT-X-ENDLIST is a live/unfinished playlist and is rejected too.
fn parse_hls_media_playlist(body: &str, base: &Url) -> Result<HlsMediaPlaylist> {
    if !is_complete_vod_playlist(body) {
        return Err(anyhow!("This is a live or unfinished HLS playlist. Only completed VOD playlists can be downloaded in this build."));
    }
    let mut init = None;
    let mut segments = Vec::new();
    // An EXT-X-BYTERANGE without an explicit offset continues at the end
    // of the previous range for the same URI (RFC 8216 §4.3.2.2).
    let mut last_end_by_uri: HashMap<String, u64> = HashMap::new();
    let mut pending_range: Option<(u64, Option<u64>)> = None;
    for line in body.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(attributes) = line.strip_prefix("#EXT-X-MAP:") {
            let uri = attr(attributes, "URI").and_then(|u| base.join(&u).ok()).map(|u| u.to_string());
            let range = attr(attributes, "BYTERANGE")
                .map(|value| parse_hls_byterange(&value).map(|(length, offset)| (offset.unwrap_or(0), length)))
                .transpose()?;
            // The MAP range is a sub-range of its URI, so it seeds offset
            // continuation for later segments on the same URI (RFC 8216
            // §4.3.2.2): a segment BYTERANGE without an offset continues
            // after the init range instead of restarting at byte 0.
            if let (Some(url), Some((map_start, map_length))) = (uri.as_ref(), range) {
                last_end_by_uri.insert(url.clone(), map_start.saturating_add(map_length));
            }
            init = uri.map(|url| HlsSegmentRef { url, range });
        } else if let Some(key) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attr(key, "METHOD").unwrap_or_default();
            if !method.eq_ignore_ascii_case("NONE") {
                return Err(anyhow!("This HLS playlist is encrypted. ThunderLoad does not decrypt protected streams."));
            }
        } else if let Some(value) = line.strip_prefix("#EXT-X-BYTERANGE:") {
            pending_range = Some(parse_hls_byterange(value.trim())?);
        } else if line.starts_with('#') {
            continue;
        } else {
            let url = base.join(line)?.to_string();
            let range = pending_range.take().map(|(length, offset)| {
                let start = offset.unwrap_or_else(|| last_end_by_uri.get(&url).copied().unwrap_or(0));
                last_end_by_uri.insert(url.clone(), start.saturating_add(length));
                (start, length)
            });
            segments.push(HlsSegmentRef { url, range });
        }
    }
    Ok(HlsMediaPlaylist { init, segments })
}

async fn fetch_text_with(client: &Client, url: &str, referer: Option<&str>) -> Result<String> {
    let mut request = client.get(url);
    if let Some(referrer) = referer { request = request.header(reqwest::header::REFERER, referrer); }
    Ok(request.send().await?.error_for_status()?.text().await?)
}

/// Fetches the playlist (master first, then the chosen media playlist and
/// its audio rendition, if any) and turns it into a download plan. The
/// first (highest listed) variant is chosen, matching the old behavior;
/// the audio rendition is the one whose GROUP-ID matches that variant's
/// `AUDIO` attribute. Live playlists and encrypted streams are rejected
/// here, before any segment is fetched.
async fn resolve_hls_plan(playlist_url: &str, referer: Option<&str>) -> Result<HlsJobState> {
    let base = Url::parse(playlist_url)?;
    let client = client()?;
    let body = fetch_text_with(&client, playlist_url, referer).await?;
    let variants = parse_hls_master(&body, &base)?;
    let (media_url, audio_playlist_url) = if !variants.is_empty() {
        if let Some(reason) = unsupported_hls_master_reason(&body) {
            return Err(anyhow!(reason));
        }
        let variant = variants.first().expect("non-empty variant list");
        let media_url = variant.url.clone();
        // The rendition whose GROUP-ID matches the variant's AUDIO
        // attribute; a variant that names a missing group falls back to
        // the DEFAULT audio rendition instead of failing the download.
        let audio = variant.audio_representation_id.as_deref().and_then(|group| {
            let renditions = parse_hls_audio_renditions(&body, &base);
            renditions.iter()
                .find(|rendition| rendition.group_id == group)
                .or_else(|| renditions.iter().find(|rendition| rendition.is_default))
                .map(|rendition| rendition.uri.clone())
        });
        (media_url, audio)
    } else {
        (playlist_url.to_string(), None)
    };
    let media_base = Url::parse(&media_url)?;
    let media_body = if media_url == playlist_url {
        body
    } else {
        fetch_text_with(&client, &media_url, referer).await?
    };
    let media = parse_hls_media_playlist(&media_body, &media_base)?;
    if media.segments.is_empty() {
        return Err(anyhow!("No media segments were found in this HLS playlist"));
    }
    let (audio_init, audio_segments) = match &audio_playlist_url {
        Some(url) => {
            let audio_base = Url::parse(url)?;
            let audio_body = fetch_text_with(&client, url, referer).await?;
            let parsed = parse_hls_media_playlist(&audio_body, &audio_base)?;
            if parsed.segments.is_empty() {
                return Err(anyhow!("No media segments were found in this HLS audio playlist"));
            }
            (parsed.init, parsed.segments)
        }
        None => (None, Vec::new()),
    };
    Ok(HlsJobState {
        version: 1,
        playlist_url: playlist_url.to_string(),
        media_url,
        audio_playlist_url,
        referer: referer.map(str::to_string),
        extension: if media.init.is_some() { "mp4".into() } else { "ts".into() },
        video_init: media.init,
        video_segments: media.segments,
        video_init_done: false,
        video_done: Vec::new(),
        audio_init,
        audio_segments,
        audio_init_done: false,
        audio_done: Vec::new(),
    })
}

/// Stable work directory for one HLS job, derived from its target so a
/// fresh process finds the same segment files. `video.mp4` becomes the
/// directory `video.mp4.thunderload.hls` next to the target.
pub fn hls_work_dir(target: &Path) -> PathBuf {
    let extension = target.extension().and_then(|value| value.to_str()).unwrap_or("file");
    target.with_extension(format!("{extension}.thunderload.hls"))
}

fn hls_metadata_path(target: &Path) -> PathBuf {
    partial_download_metadata_path(target)
}

async fn write_hls_checkpoint(path: &Path, state: &HlsJobState) -> Result<()> {
    let temporary = path.with_extension("meta.tmp");
    let bytes = serde_json::to_vec(state)?;
    fs::write(&temporary, bytes).await?;
    if fs::try_exists(path).await.unwrap_or(false) {
        fs::remove_file(path).await?;
    }
    fs::rename(&temporary, path).await
        .with_context(|| format!("Could not save HLS checkpoint at {}", path.display()))?;
    Ok(())
}

async fn read_hls_checkpoint(path: &Path) -> Option<HlsJobState> {
    let bytes = fs::read(path).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

async fn remove_hls_resume_files(target: &Path) {
    let _ = fs::remove_dir_all(hls_work_dir(target)).await;
    let _ = fs::remove_file(hls_metadata_path(target)).await;
    let _ = fs::remove_file(target).await;
}

/// Validates a `Content-Range: bytes A-B/T` header against the requested
/// byte range. The total may be `*` (unknown), so only the returned span
/// is checked — a server answering 206 with a different (same-length)
/// range would otherwise silently corrupt the assembled file. A missing
/// header is tolerated (the length check in the caller still applies).
fn validate_hls_content_range(actual: &str, start: u64, end: u64) -> Result<()> {
    let range = actual.strip_prefix("bytes ").ok_or_else(|| anyhow!("Server returned an unexpected Content-Range: {actual}"))?;
    let (span, _) = range.split_once('/').ok_or_else(|| anyhow!("Server returned an unexpected Content-Range: {actual}"))?;
    let (first, last) = span.split_once('-').ok_or_else(|| anyhow!("Server returned an unexpected Content-Range: {actual}"))?;
    let first: u64 = first.parse().map_err(|_| anyhow!("Server returned an unexpected Content-Range: {actual}"))?;
    let last: u64 = last.parse().map_err(|_| anyhow!("Server returned an unexpected Content-Range: {actual}"))?;
    if first != start || last != end {
        return Err(anyhow!("Server returned range bytes {first}-{last} for a requested HLS byte range bytes {start}-{end}"));
    }
    Ok(())
}

/// Fetches one segment (whole file, or one EXT-X-BYTERANGE window of a
/// larger resource) into `path`. A server that ignores Range and answers
/// 200 is handled by slicing the requested window out of the full body;
/// a 206 answer must contain exactly the requested length.
async fn fetch_hls_segment(
    client: &Client,
    segment: &HlsSegmentRef,
    referer: Option<&str>,
    path: &Path,
    rate_limiter: &Arc<GlobalRateLimiter>,
) -> Result<u64> {
    let mut request = client.get(&segment.url);
    if let Some(referrer) = referer { request = request.header(reqwest::header::REFERER, referrer); }
    if let Some((start, length)) = segment.range {
        request = request.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", start, start.saturating_add(length).saturating_sub(1)),
        );
    }
    let response = request.send().await?.error_for_status()?;
    let mut file = fs::File::create(path).await?;
    let mut bytes = 0_u64;
    if let Some((start, length)) = segment.range {
        if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            if let Some(content_range) = response.headers().get(reqwest::header::CONTENT_RANGE).and_then(|value| value.to_str().ok()) {
                validate_hls_content_range(content_range, start, start.saturating_add(length).saturating_sub(1))?;
            }
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                bytes = bytes.saturating_add(chunk.len() as u64);
                file.write_all(&chunk).await?;
                let wait = rate_limiter.reserve(chunk.len() as u64).await;
                if !wait.is_zero() { tokio::time::sleep(wait).await; }
            }
            if bytes != length {
                return Err(anyhow!("Server returned {bytes} bytes for a {length}-byte HLS byte range"));
            }
        } else {
            // The server ignored Range: stream the whole resource but
            // keep only the requested window, without buffering it in
            // memory, and apply the rate limiter to every byte read off
            // the wire. Reading stops as soon as the window is complete.
            let end = start.saturating_add(length);
            let mut stream = response.bytes_stream();
            let mut position = 0_u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                let wait = rate_limiter.reserve(chunk.len() as u64).await;
                if !wait.is_zero() { tokio::time::sleep(wait).await; }
                let chunk_start = position;
                let chunk_end = position.saturating_add(chunk.len() as u64);
                position = chunk_end;
                let keep_start = chunk_start.max(start);
                let keep_end = chunk_end.min(end);
                if keep_end > keep_start {
                    let from = usize::try_from(keep_start - chunk_start).context("HLS byte-range offset is too large for this platform")?;
                    let to = usize::try_from(keep_end - chunk_start).context("HLS byte-range offset is too large for this platform")?;
                    file.write_all(&chunk[from..to]).await?;
                    bytes = bytes.saturating_add(keep_end - keep_start);
                }
                if chunk_end >= end { break; }
            }
            if bytes != length {
                return Err(anyhow!("Server returned a {position}-byte resource for an HLS byte range ending at {end}"));
            }
        }
    } else {
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            bytes = bytes.saturating_add(chunk.len() as u64);
            file.write_all(&chunk).await?;
            let wait = rate_limiter.reserve(chunk.len() as u64).await;
            if !wait.is_zero() { tokio::time::sleep(wait).await; }
        }
    }
    file.flush().await?;
    Ok(bytes)
}

/// Concatenates one finished track (init segment first, then media
/// segments in playlist order) into `output`. For fMP4 this is exactly the
/// playable file: the EXT-X-MAP init segment carries ftyp+moov and each
/// media segment is a moof/mdat fragment, so ordered concatenation is the
/// container. No FFmpeg remux is needed for a single track.
async fn assemble_hls_track(work_dir: &Path, state: &HlsJobState, audio: bool, output: &Path) -> Result<()> {
    let (has_init, init_name, segments) = if audio {
        (state.audio_init.is_some(), "audio-init", &state.audio_segments)
    } else {
        (state.video_init.is_some(), "video-init", &state.video_segments)
    };
    let prefix = if audio { "audio" } else { "video" };
    let mut out = fs::File::create(output).await?;
    if has_init {
        let mut input = fs::File::open(work_dir.join(init_name)).await
            .with_context(|| format!("HLS init segment is missing: {init_name}"))?;
        tokio::io::copy(&mut input, &mut out).await?;
    }
    for index in 0..segments.len() {
        let name = format!("{prefix}-{index:06}");
        let mut input = fs::File::open(work_dir.join(&name)).await
            .with_context(|| format!("HLS segment is missing: {name}"))?;
        tokio::io::copy(&mut input, &mut out).await?;
    }
    out.flush().await?;
    Ok(())
}

/// Stream-copies the assembled video and audio tracks into one MP4, the
/// same approach `download_dash` uses. FFmpeg is never bundled: it is the
/// externally installed executable (THUNDERLOAD_FFMPEG or `ffmpeg` on
/// PATH). When it is absent the download fails here — after the segments
/// were fetched — with an error that names the missing executable, and the
/// checkpoint is kept so installing FFmpeg and retrying resumes straight
/// at the mux step instead of re-downloading.
async fn mux_hls_tracks(video_track: &Path, audio_track: &Path, output: &Path) -> Result<()> {
    let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    let result = TokioCommand::new(&executable)
        .arg("-hide_banner").arg("-loglevel").arg("error").arg("-y")
        .arg("-i").arg(video_track)
        .arg("-i").arg(audio_track)
        .arg("-map").arg("0:v:0").arg("-map").arg("1:a:0")
        .arg("-c").arg("copy")
        .arg("-movflags").arg("+faststart")
        .arg("-shortest")
        .arg(output)
        .output().await;
    match result {
        Err(error) => Err(anyhow!(
            "Could not launch FFmpeg ({}) to combine HLS audio and video: {}. Install FFmpeg or set THUNDERLOAD_FFMPEG to its executable path, then retry — the downloaded segments were kept for resume.",
            executable.to_string_lossy(), error
        )),
        Ok(output) if !output.status.success() => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(anyhow!("FFmpeg could not mux the HLS tracks: {}. The downloaded segments were kept for resume.", stderr.trim()))
        }
        Ok(_) => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HlsSegmentMark {
    VideoInit,
    Video(usize),
    AudioInit,
    Audio(usize),
}

fn hls_segment_path(work_dir: &Path, mark: HlsSegmentMark) -> PathBuf {
    match mark {
        HlsSegmentMark::VideoInit => work_dir.join("video-init"),
        HlsSegmentMark::Video(index) => work_dir.join(format!("video-{index:06}")),
        HlsSegmentMark::AudioInit => work_dir.join("audio-init"),
        HlsSegmentMark::Audio(index) => work_dir.join(format!("audio-{index:06}")),
    }
}

/// HLS download with pause/resume/cancel and an on-disk checkpoint,
/// mirroring the direct-download pattern in
/// `download::download_url_resumable`: the playlist URL, chosen variant,
/// completed segment indexes and target are persisted next to the target,
/// so a fresh call — including from a fresh process — resumes from the
/// next unfinished segment. Cancel removes the partial files. A network
/// error keeps the checkpoint and the finished segments, so a retry
/// resumes instead of restarting.
pub async fn download_hls_resumable(
    playlist_url: &str,
    target: &Path,
    referer: Option<&str>,
    control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<DownloadOutcome> {
    if *control.borrow() == DownloadControl::Cancelled {
        remove_hls_resume_files(target).await;
        return Ok(DownloadOutcome::Cancelled);
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok(DownloadOutcome::Paused);
    }
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    let work_dir = hls_work_dir(target);
    let meta_path = hls_metadata_path(target);
    let mut state = resolve_hls_plan(playlist_url, referer).await?;
    // Reuse a checkpoint only when it describes exactly this job; a
    // playlist whose segment list changed restarts from scratch.
    let mut reused_checkpoint = false;
    if let Some(saved) = read_hls_checkpoint(&meta_path).await {
        if saved.matches_plan(&state) {
            // A segment counts as done only when its file is actually on
            // disk; a checkpoint entry without a file is re-downloaded.
            let mut pruned = saved;
            if pruned.video_init_done && !fs::try_exists(hls_segment_path(&work_dir, HlsSegmentMark::VideoInit)).await.unwrap_or(false) {
                pruned.video_init_done = false;
            }
            let mut kept_video_done = Vec::new();
            for index in pruned.video_done {
                if fs::try_exists(hls_segment_path(&work_dir, HlsSegmentMark::Video(index))).await.unwrap_or(false) {
                    kept_video_done.push(index);
                }
            }
            pruned.video_done = kept_video_done;
            if pruned.audio_init_done && !fs::try_exists(hls_segment_path(&work_dir, HlsSegmentMark::AudioInit)).await.unwrap_or(false) {
                pruned.audio_init_done = false;
            }
            let mut kept_audio_done = Vec::new();
            for index in pruned.audio_done {
                if fs::try_exists(hls_segment_path(&work_dir, HlsSegmentMark::Audio(index))).await.unwrap_or(false) {
                    kept_audio_done.push(index);
                }
            }
            pruned.audio_done = kept_audio_done;
            state = pruned;
            reused_checkpoint = true;
        }
    }
    if !reused_checkpoint {
        // No usable checkpoint — fresh job, changed playlist, or corrupt
        // metadata: drop any stale segment files so they can never be
        // mistaken for this job's output.
        let _ = fs::remove_dir_all(&work_dir).await;
        let _ = fs::remove_file(&meta_path).await;
    }
    fs::create_dir_all(&work_dir).await?;
    write_hls_checkpoint(&meta_path, &state).await?;

    let client = client()?;
    let total = state.total_segments();
    let completed = Arc::new(AtomicUsize::new(state.done_segments()));
    // Progress restarts from the bytes already on disk, not from zero.
    let mut already_downloaded = 0_u64;
    if state.video_init_done {
        if let Ok(metadata) = fs::metadata(hls_segment_path(&work_dir, HlsSegmentMark::VideoInit)).await {
            already_downloaded = already_downloaded.saturating_add(metadata.len());
        }
    }
    for index in &state.video_done {
        if let Ok(metadata) = fs::metadata(hls_segment_path(&work_dir, HlsSegmentMark::Video(*index))).await {
            already_downloaded = already_downloaded.saturating_add(metadata.len());
        }
    }
    if state.audio_init_done {
        if let Ok(metadata) = fs::metadata(hls_segment_path(&work_dir, HlsSegmentMark::AudioInit)).await {
            already_downloaded = already_downloaded.saturating_add(metadata.len());
        }
    }
    for index in &state.audio_done {
        if let Ok(metadata) = fs::metadata(hls_segment_path(&work_dir, HlsSegmentMark::Audio(*index))).await {
            already_downloaded = already_downloaded.saturating_add(metadata.len());
        }
    }
    let downloaded_bytes = Arc::new(AtomicU64::new(already_downloaded));

    let mut work: std::collections::VecDeque<(HlsSegmentMark, HlsSegmentRef)> = std::collections::VecDeque::new();
    if state.video_init.is_some() && !state.video_init_done {
        work.push_back((HlsSegmentMark::VideoInit, state.video_init.clone().expect("checked above")));
    }
    for (index, segment) in state.video_segments.iter().enumerate() {
        if !state.video_done.contains(&index) {
            work.push_back((HlsSegmentMark::Video(index), segment.clone()));
        }
    }
    if state.audio_init.is_some() && !state.audio_init_done {
        work.push_back((HlsSegmentMark::AudioInit, state.audio_init.clone().expect("checked above")));
    }
    for (index, segment) in state.audio_segments.iter().enumerate() {
        if !state.audio_done.contains(&index) {
            work.push_back((HlsSegmentMark::Audio(index), segment.clone()));
        }
    }

    let mut tasks = tokio::task::JoinSet::new();
    let mut stopped: Option<DownloadControl> = None;
    while !work.is_empty() || !tasks.is_empty() {
        if matches!(*control.borrow(), DownloadControl::Paused | DownloadControl::Cancelled) {
            stopped = Some(*control.borrow());
            break;
        }
        while tasks.len() < 6 {
            let Some((mark, segment)) = work.pop_front() else { break; };
            let client = client.clone();
            let referrer = referer.map(str::to_string);
            let path = hls_segment_path(&work_dir, mark);
            let rate_limiter = rate_limiter.clone();
            tasks.spawn(async move {
                let bytes = fetch_hls_segment(&client, &segment, referrer.as_deref(), &path, &rate_limiter).await?;
                Ok::<(HlsSegmentMark, u64), anyhow::Error>((mark, bytes))
            });
        }
        let Some(result) = tasks.join_next().await else { break; };
        match result {
            Ok(Ok((mark, bytes))) => {
                match mark {
                    HlsSegmentMark::VideoInit => state.video_init_done = true,
                    HlsSegmentMark::Video(index) => {
                        if !state.video_done.contains(&index) { state.video_done.push(index); }
                    }
                    HlsSegmentMark::AudioInit => state.audio_init_done = true,
                    HlsSegmentMark::Audio(index) => {
                        if !state.audio_done.contains(&index) { state.audio_done.push(index); }
                    }
                }
                // Only the coordinator writes the checkpoint, so the
                // on-disk state can never race with a segment task.
                write_hls_checkpoint(&meta_path, &state).await?;
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                let transferred = downloaded_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
                let phase = match mark {
                    HlsSegmentMark::VideoInit | HlsSegmentMark::Video(_) => "video-segments",
                    HlsSegmentMark::AudioInit | HlsSegmentMark::Audio(_) => "audio-segments",
                };
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: transferred, total: None,
                    percent: Some(done as f64 * 100.0 / total.max(1) as f64), phase: phase.into(),
                });
            }
            Ok(Err(error)) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(error);
            }
            Err(error) => {
                if error.is_cancelled() { continue; }
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(anyhow!("HLS segment task failed: {error}"));
            }
        }
    }
    if let Some(stopped) = stopped {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        write_hls_checkpoint(&meta_path, &state).await?;
        return match stopped {
            DownloadControl::Paused => {
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
                    percent: Some(state.done_segments() as f64 * 100.0 / total.max(1) as f64),
                    phase: "paused".into(),
                });
                Ok(DownloadOutcome::Paused)
            }
            DownloadControl::Cancelled => {
                remove_hls_resume_files(target).await;
                Ok(DownloadOutcome::Cancelled)
            }
            DownloadControl::Running => unreachable!("loop only stops on pause or cancel"),
        };
    }

    let _ = progress_tx.send(DownloadProgress {
        downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
        percent: Some(100.0), phase: "assembling".into(),
    });
    let video_track = work_dir.join("video.assembled");
    assemble_hls_track(&work_dir, &state, false, &video_track).await?;
    let final_file = if state.audio_playlist_url.is_some() {
        let audio_track = work_dir.join("audio.assembled");
        assemble_hls_track(&work_dir, &state, true, &audio_track).await?;
        let _ = progress_tx.send(DownloadProgress {
            downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
            percent: Some(100.0), phase: "muxing".into(),
        });
        let muxed = work_dir.join("muxed.mp4");
        mux_hls_tracks(&video_track, &audio_track, &muxed).await?;
        muxed
    } else {
        video_track
    };
    // The target was reserved atomically before downloading. Replace only
    // our empty reservation after the file is fully assembled.
    let _ = fs::remove_file(target).await;
    fs::rename(&final_file, target).await
        .with_context(|| format!("Could not move the assembled HLS file to {}", target.display()))?;
    let _ = fs::remove_dir_all(&work_dir).await;
    let _ = fs::remove_file(&meta_path).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
        percent: Some(100.0), phase: "complete".into(),
    });
    Ok(DownloadOutcome::Completed)
}

pub async fn download_hls(
    playlist_url: &str,
    filename: &str,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    // Resolve the plan first so the reserved target gets the right
    // extension (mp4 when an EXT-X-MAP init segment is present, ts
    // otherwise); the resumable call re-resolves the same small playlist.
    let plan = resolve_hls_plan(playlist_url, referer).await?;
    let out_dir = default_download_dir();
    fs::create_dir_all(&out_dir).await?;
    let safe = safe_filename(filename, "video");
    let stem = Path::new(&safe).file_stem().and_then(|s| s.to_str()).unwrap_or("video");
    let target = unique_video_path(&out_dir, stem, &plan.extension).await?;
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    let outcome = download_hls_resumable(playlist_url, &target, referer, control_rx, progress_tx, rate_limiter).await;
    drop(control_tx);
    match outcome? {
        DownloadOutcome::Completed => Ok(target),
        DownloadOutcome::Paused => Err(anyhow!("HLS download paused before completion")),
        DownloadOutcome::Cancelled => Err(anyhow!("HLS download cancelled")),
    }
}

fn find_dash_representation<'a>(
    period: &'a DashPeriod,
    representation_id: &str,
    audio: bool,
) -> Option<(&'a DashAdaptation, &'a DashRepresentation)> {
    period.adaptations.iter()
        .filter(|adaptation| is_audio_adaptation(adaptation) == audio)
        .find_map(|adaptation| adaptation.representations.iter()
            .find(|representation| representation.id == representation_id)
            .map(|representation| (adaptation, representation)))
}

fn validate_dash_content_range(actual: &str, start: u64, end: u64) -> Result<()> {
    let expected = format!("bytes {start}-{end}/");
    if !actual.starts_with(&expected) {
        return Err(anyhow!("DASH server returned Content-Range '{actual}'; expected bytes {start}-{end}/<total>"));
    }
    Ok(())
}

/// Parses an ISO BMFF Segment Index (sidx) box and returns the media
/// segment byte ranges (inclusive) it describes. `index_start` is the
/// absolute file offset of the first byte of `bytes` (the indexRange
/// start), because `first_offset` is measured from the end of the sidx
/// box. Only version 0 and 1 are supported; a reference to another index
/// (reference_type 1) is rejected instead of being followed.
fn parse_dash_sidx(bytes: &[u8], index_start: u64) -> Result<Vec<(u64, u64)>> {
    if bytes.len() < 32 {
        return Err(anyhow!("DASH segment index is too short to contain an sidx box"));
    }
    if &bytes[4..8] != b"sidx" {
        return Err(anyhow!("DASH segment index does not start with an sidx box"));
    }
    let size = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if size < 32 || size > bytes.len() {
        return Err(anyhow!("DASH sidx box has an invalid size"));
    }
    let version = bytes[8];
    if version > 1 {
        return Err(anyhow!("DASH sidx box version {version} is not supported"));
    }
    // The v1 header is 8 bytes longer than v0 (64-bit earliest presentation
    // time and first offset); a v1 box shorter than 40 bytes cannot hold
    // its header, so reject it here instead of panicking on an
    // out-of-bounds slice read below.
    let min_header = if version == 0 { 32 } else { 40 };
    if size < min_header {
        return Err(anyhow!("DASH sidx box is too short for a version {version} header"));
    }
    let mut cursor = 12;
    cursor += 8; // reference_ID (4) + timescale (4)
    let first_offset = if version == 0 {
        let value = u32::from_be_bytes(bytes[cursor + 4..cursor + 8].try_into().unwrap()) as u64;
        cursor += 8; // earliest_presentation_time (4) + first_offset (4)
        value
    } else {
        let value = u64::from_be_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap());
        cursor += 16; // earliest_presentation_time (8) + first_offset (8)
        value
    };
    let reference_count = u16::from_be_bytes(bytes[cursor + 2..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4; // reserved (2) + reference_count (2)
    if reference_count == 0 {
        return Err(anyhow!("DASH sidx box contains no segment references"));
    }
    if size < cursor + reference_count * 12 {
        return Err(anyhow!("DASH sidx box is truncated"));
    }
    let mut ranges = Vec::with_capacity(reference_count);
    let mut position = index_start + size as u64 + first_offset;
    for index in 0..reference_count {
        let entry = &bytes[cursor + index * 12..cursor + (index + 1) * 12];
        let referenced_size = u32::from_be_bytes(entry[0..4].try_into().unwrap());
        if referenced_size & 0x8000_0000 != 0 {
            return Err(anyhow!("DASH sidx entry {index} references another index, not a media segment"));
        }
        let referenced_size = (referenced_size & 0x7fff_ffff) as u64;
        if referenced_size == 0 {
            return Err(anyhow!("DASH sidx entry {index} has a zero segment size"));
        }
        ranges.push((position, position + referenced_size - 1));
        position += referenced_size;
    }
    Ok(ranges)
}

/// Fetches exactly the bytes of `range` from `url`. A 206 must carry
/// exactly the requested span (Content-Range and Content-Length validated
/// when present); a 200 that ignores the Range is streamed as a bounded
/// window so a whole media file is never buffered. SegmentBase addressing
/// is byte ranges throughout, so a server that will not serve ranges
/// cannot serve SegmentBase at all.
async fn fetch_dash_range_bytes(
    client: &Client,
    url: &str,
    range: (u64, u64),
    referer: Option<&str>,
) -> Result<Vec<u8>> {
    let (start, end) = range;
    let expected = end - start + 1;
    let mut request = client.get(url);
    if let Some(referrer) = referer {
        request = request.header(reqwest::header::REFERER, referrer);
    }
    let response = request
        .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
        .send().await?;
    if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
        if let Some(content_range) = response.headers().get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()) {
            validate_dash_content_range(content_range, start, end)?;
        }
        if let Some(length) = response.headers().get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok()) {
            if length != expected {
                return Err(anyhow!("DASH server returned {length} bytes for a {expected}-byte range request"));
            }
        }
        let bytes = response.bytes().await?;
        if bytes.len() as u64 != expected {
            return Err(anyhow!("DASH server returned {} bytes for a {expected}-byte range request", bytes.len()));
        }
        return Ok(bytes.to_vec());
    }
    let response = response.error_for_status()?;
    let mut stream = response.bytes_stream();
    let mut kept = Vec::with_capacity(expected.min(1 << 20) as usize);
    let mut position = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let chunk_start = position;
        let chunk_end = position + chunk.len() as u64;
        position = chunk_end;
        if chunk_end <= start || chunk_start > end { continue; }
        let keep_start = start.saturating_sub(chunk_start) as usize;
        let keep_end = (end + 1 - chunk_start).min(chunk.len() as u64) as usize;
        kept.extend_from_slice(&chunk[keep_start..keep_end]);
        if position > end { break; }
    }
    if kept.len() as u64 != expected {
        return Err(anyhow!("DASH server returned {} bytes for a {expected}-byte range request", kept.len()));
    }
    Ok(kept)
}

/// Fetches a SegmentBase index (sidx) and expands it into the concrete
/// segment byte ranges of the representation's media file. Returns the
/// initialization reference plus the segment URLs (all the media file)
/// and their ranges.
async fn expand_dash_segment_base(
    client: &Client,
    plan: &DashSegmentBasePlan,
    referer: Option<&str>,
) -> Result<(Option<String>, Option<(u64, u64)>, Vec<String>, Vec<Option<(u64, u64)>>)> {
    let index_bytes = fetch_dash_range_bytes(client, &plan.base_url, plan.index_range, referer).await
        .with_context(|| format!(
            "Could not fetch the DASH segment index (bytes {}-{}) from {}",
            plan.index_range.0, plan.index_range.1, plan.base_url
        ))?;
    let ranges = parse_dash_sidx(&index_bytes, plan.index_range.0)?;
    let segment_urls = vec![plan.base_url.clone(); ranges.len()];
    let segment_ranges = ranges.into_iter().map(Some).collect();
    Ok((plan.init_url.clone(), plan.init_range, segment_urls, segment_ranges))
}

async fn download_dash_track(
    client: &Client,
    track: &DashResolvedTrack,
    work_dir: &Path,
    prefix: &str,
    referer: Option<&str>,
    progress_tx: &mpsc::UnboundedSender<DownloadProgress>,
    completed: Arc<AtomicUsize>,
    downloaded_bytes: Arc<AtomicU64>,
    total_segments: usize,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    let mut refs = Vec::with_capacity(track.segment_urls.len() + usize::from(track.initialization_url.is_some()));
    if let Some(initialization) = &track.initialization_url {
        refs.push((initialization.clone(), track.initialization_range));
    } else if track.mime_type.to_ascii_lowercase().contains("mp4") {
        return Err(anyhow!("DASH MP4 representation is missing its initialization segment"));
    }
    if track.segment_urls.len() != track.segment_ranges.len() {
        return Err(anyhow!("DASH representation has mismatched segment URLs and ranges"));
    }
    for (url, range) in track.segment_urls.iter().zip(track.segment_ranges.iter()) {
        refs.push((url.clone(), *range));
    }
    if refs.is_empty() { return Err(anyhow!("DASH representation contains no media URLs")); }

    let mut tasks = tokio::task::JoinSet::new();
    for (index, (segment_url, range)) in refs.into_iter().enumerate() {
        let client = client.clone();
        let referrer = referer.map(str::to_string);
        let path = work_dir.join(format!("{prefix}-{index:06}.segment"));
        let tx = progress_tx.clone();
        let completed = completed.clone();
        let downloaded_bytes = downloaded_bytes.clone();
        let rate_limiter = rate_limiter.clone();
        tasks.spawn(async move {
            let mut request = client.get(segment_url);
            if let Some(referrer) = referrer {
                request = request.header(reqwest::header::REFERER, referrer);
            }
            if let Some((start, end)) = range {
                request = request.header(reqwest::header::RANGE, format!("bytes={start}-{end}"));
            }
            let response = request.send().await?;
            // A ranged request answered with 206 is the range itself; a 200
            // means the server ignored Range, so the body is windowed below.
            let is_partial = range.is_some() && response.status() == reqwest::StatusCode::PARTIAL_CONTENT;
            let response = if is_partial {
                if let Some((start, end)) = range {
                    if let Some(content_range) = response.headers().get(reqwest::header::CONTENT_RANGE)
                        .and_then(|value| value.to_str().ok()) {
                        validate_dash_content_range(content_range, start, end)?;
                    }
                    if let Some(length) = response.headers().get(reqwest::header::CONTENT_LENGTH)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok()) {
                        let expected = end - start + 1;
                        if length != expected {
                            return Err(anyhow!("DASH server returned {length} bytes for a {expected}-byte segment range"));
                        }
                    }
                }
                response
            } else {
                response.error_for_status()?
            };
            let mut stream = response.bytes_stream();
            let mut file = fs::File::create(&path).await?;
            let mut bytes = 0_u64;
            let mut position = 0_u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if is_partial || range.is_none() {
                    file.write_all(&chunk).await?;
                    bytes = bytes.saturating_add(chunk.len() as u64);
                    let wait = rate_limiter.reserve(chunk.len() as u64).await;
                    if !wait.is_zero() { tokio::time::sleep(wait).await; }
                    continue;
                }
                // 200 response to a ranged request: stream a bounded window.
                let (start, end) = range.unwrap();
                let chunk_start = position;
                let chunk_end = position + chunk.len() as u64;
                position = chunk_end;
                if chunk_end <= start || chunk_start > end { continue; }
                let keep_start = start.saturating_sub(chunk_start) as usize;
                let keep_end = (end + 1 - chunk_start).min(chunk.len() as u64) as usize;
                let keep = &chunk[keep_start..keep_end];
                file.write_all(keep).await?;
                bytes = bytes.saturating_add(keep.len() as u64);
                let wait = rate_limiter.reserve(keep.len() as u64).await;
                if !wait.is_zero() { tokio::time::sleep(wait).await; }
                if position > end { break; }
            }
            file.flush().await?;
            if bytes == 0 { return Err(anyhow!("DASH server returned an empty initialization/segment file")); }
            if let Some((start, end)) = range {
                let expected = end - start + 1;
                if bytes != expected {
                    return Err(anyhow!("DASH server returned {bytes} bytes for a {expected}-byte segment range"));
                }
            }
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let transferred = downloaded_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
            let _ = tx.send(DownloadProgress {
                downloaded: transferred,
                total: None,
                percent: Some(done as f64 * 100.0 / total_segments.max(1) as f64),
                phase: "dash-segments".into(),
            });
            Ok::<PathBuf, anyhow::Error>(path)
        });
        if tasks.len() >= 6 {
            if let Some(result) = tasks.join_next().await {
                if let Err(error) = result? { tasks.abort_all(); return Err(error); }
            }
        }
    }

    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => failures.push(error),
            Err(error) => failures.push(anyhow!(error)),
        }
    }
    if let Some(error) = failures.into_iter().next() { return Err(error); }

    let extension = if track.mime_type.to_ascii_lowercase().contains("webm") { "webm" } else { "mp4" };
    let assembled = work_dir.join(format!("{prefix}-track.{extension}"));
    let mut output = fs::File::create(&assembled).await?;
    // The numeric filenames preserve the manifest's initialization + timeline order.
    let expected_files: Vec<PathBuf> = (0..(track.segment_urls.len() + usize::from(track.initialization_url.is_some())))
        .map(|index| work_dir.join(format!("{prefix}-{index:06}.segment")))
        .collect();
    if expected_files.is_empty() {
        return Err(anyhow!("No DASH media segments were saved"));
    }
    for path in expected_files {
        let mut input = fs::File::open(&path).await
            .with_context(|| format!("DASH segment is missing: {}", path.display()))?;
        tokio::io::copy(&mut input, &mut output).await?;
    }
    output.flush().await?;
    drop(output);
    Ok(assembled)
}

async fn ffmpeg_mux_dash_tracks(
    video_file: &Path,
    audio_file: &Path,
    output: &Path,
) -> Result<()> {
    let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    let result = TokioCommand::new(executable)
        .arg("-hide_banner").arg("-loglevel").arg("error").arg("-y")
        .arg("-i").arg(video_file)
        .arg("-i").arg(audio_file)
        .arg("-map").arg("0:v:0").arg("-map").arg("1:a:0")
        .arg("-c").arg("copy")
        .arg("-movflags").arg("+faststart")
        .arg("-shortest")
        .arg(output)
        .output().await.context("Could not launch FFmpeg to combine DASH audio and video")?;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(anyhow!("FFmpeg could not mux the DASH tracks: {}", stderr.trim()));
    }
    Ok(())
}

/// Concatenates per-period track files into one valid container with the
/// FFmpeg concat demuxer. Raw byte concatenation would produce a file
/// with several moov boxes; the demuxer re-times the fragments instead.
/// All files must share codec and dimensions (checked before download).
async fn ffmpeg_concat_dash_tracks(files: &[PathBuf], output: &Path) -> Result<()> {
    let list_path = output.with_extension("concat.txt");
    let mut list = String::new();
    for file in files {
        let escaped = file.to_string_lossy().replace('\'', "'\\''");
        list.push_str(&format!("file '{escaped}'\n"));
    }
    fs::write(&list_path, list).await?;
    let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    let result = TokioCommand::new(executable)
        .arg("-hide_banner").arg("-loglevel").arg("error").arg("-y")
        .arg("-f").arg("concat").arg("-safe").arg("0")
        .arg("-i").arg(&list_path)
        .arg("-c").arg("copy")
        .arg("-movflags").arg("+faststart")
        .arg(output)
        .output().await.context("Could not launch FFmpeg to concatenate DASH periods")?;
    let _ = fs::remove_file(&list_path).await;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(anyhow!("FFmpeg could not concatenate the DASH periods: {}", stderr.trim()));
    }
    Ok(())
}

/// Downloads one static DASH representation. When the MPD defines separate
/// audio/video AdaptationSets, both tracks are fetched and stream-copied into a
/// playable container using FFmpeg. Multi-period MPDs are downloaded period
/// by period and concatenated with FFmpeg; periods must agree on codec and
/// dimensions. Dynamic MPDs and DRM are rejected instead of producing a
/// broken file.
pub async fn download_dash(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    filename: &str,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    let out_dir = default_download_dir();
    fs::create_dir_all(&out_dir).await?;
    download_dash_into(
        manifest_url, representation_id, audio_representation_id, filename,
        referer, progress_tx, rate_limiter, &out_dir,
    ).await
}

async fn download_dash_into(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    filename: &str,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    out_dir: &Path,
) -> Result<PathBuf> {
    let body = fetch_text(manifest_url, referer).await?;
    let document = parse_dash_document(&body)?;
    if !document.is_static { return Err(anyhow!("Dynamic/live DASH MPDs are not supported")); }
    if document.protected { return Err(anyhow!("DASH ContentProtection/DRM is not supported")); }
    if document.periods.is_empty() { return Err(anyhow!("DASH MPD contains no Periods")); }
    let multi_period = document.periods.len() > 1;

    let client = client()?;
    let mut video_tracks = Vec::new();
    let mut audio_tracks = Vec::new();
    for (period_index, period) in document.periods.iter().enumerate() {
        let (video_adaptation, video_representation) = find_dash_representation(period, representation_id, false)
            .ok_or_else(|| anyhow!("Period {} has no DASH video representation '{representation_id}'", period_index + 1))?;
        let mut video = resolve_dash_track(&document, period, video_adaptation, video_representation, manifest_url, false)?;
        if let Some(plan) = video.segment_base.clone() {
            let (init_url, init_range, urls, ranges) = expand_dash_segment_base(&client, &plan, referer).await?;
            video.initialization_url = init_url;
            video.initialization_range = init_range;
            video.segment_urls = urls;
            video.segment_ranges = ranges;
            video.segment_base = None;
        }
        video_tracks.push(video);
        if let Some(audio_id) = audio_representation_id {
            let (audio_adaptation, audio_representation) = find_dash_representation(period, audio_id, true)
                .ok_or_else(|| anyhow!("Period {} has no DASH audio representation '{audio_id}'", period_index + 1))?;
            let mut audio = resolve_dash_track(&document, period, audio_adaptation, audio_representation, manifest_url, true)?;
            if let Some(plan) = audio.segment_base.clone() {
                let (init_url, init_range, urls, ranges) = expand_dash_segment_base(&client, &plan, referer).await?;
                audio.initialization_url = init_url;
                audio.initialization_range = init_range;
                audio.segment_urls = urls;
                audio.segment_ranges = ranges;
                audio.segment_base = None;
            }
            audio_tracks.push(audio);
        }
    }
    check_dash_period_compat(&video_tracks, &audio_tracks)?;

    if (audio_representation_id.is_some() || multi_period) && !ffmpeg_available().await {
        return Err(anyhow!("This DASH download needs FFmpeg (separate audio tracks or multiple periods to concatenate). Install FFmpeg or set THUNDERLOAD_FFMPEG to its executable path, then retry."));
    }

    let total_segments = video_tracks.iter()
        .map(|track| track.segment_urls.len() + usize::from(track.initialization_url.is_some())).sum::<usize>()
        + audio_tracks.iter()
        .map(|track| track.segment_urls.len() + usize::from(track.initialization_url.is_some())).sum::<usize>();
    if total_segments == 0 { return Err(anyhow!("DASH MPD did not produce media segments")); }

    let track_extension = |mime_type: &str| if mime_type.to_ascii_lowercase().contains("webm") { "webm" } else { "mp4" };
    let extension = if audio_representation_id.is_some() { "mp4" } else { track_extension(&video_tracks[0].mime_type) };
    let safe = safe_filename(filename, "video");
    let stem = Path::new(&safe).file_stem().and_then(|value| value.to_str()).unwrap_or("video");
    let target = unique_video_path(out_dir, stem, extension).await?;
    let work_dir = out_dir.join(format!(".dash-{}", Uuid::new_v4()));
    let download_result = async {
        fs::create_dir_all(&work_dir).await?;
        let completed = Arc::new(AtomicUsize::new(0));
        let downloaded_bytes = Arc::new(AtomicU64::new(0));
        let mut video_files = Vec::new();
        for (index, track) in video_tracks.iter().enumerate() {
            video_files.push(download_dash_track(
                &client, track, &work_dir, &format!("video-p{index}"), referer, &progress_tx,
                completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone()
            ).await?);
        }
        let mut audio_files = Vec::new();
        for (index, track) in audio_tracks.iter().enumerate() {
            audio_files.push(download_dash_track(
                &client, track, &work_dir, &format!("audio-p{index}"), referer, &progress_tx,
                completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone()
            ).await?);
        }

        let video_file = if multi_period {
            let concatenated = work_dir.join(format!("video-concat.{}", track_extension(&video_tracks[0].mime_type)));
            ffmpeg_concat_dash_tracks(&video_files, &concatenated).await?;
            concatenated
        } else {
            video_files.into_iter().next().unwrap()
        };
        let output_file = if !audio_files.is_empty() {
            let audio_file = if multi_period {
                let concatenated = work_dir.join(format!("audio-concat.{}", track_extension(&audio_tracks[0].mime_type)));
                ffmpeg_concat_dash_tracks(&audio_files, &concatenated).await?;
                concatenated
            } else {
                audio_files.into_iter().next().unwrap()
            };
            let muxed = work_dir.join("muxed.mp4");
            ffmpeg_mux_dash_tracks(&video_file, &audio_file, &muxed).await?;
            muxed
        } else {
            video_file
        };

        let metadata = fs::metadata(&output_file).await?;
        if metadata.len() == 0 { return Err(anyhow!("DASH assembly produced an empty output file")); }
        fs::remove_file(&target).await?;
        fs::rename(&output_file, &target).await
            .with_context(|| format!("Could not move the assembled DASH file to {}", target.display()))?;
        let _ = progress_tx.send(DownloadProgress {
            downloaded: downloaded_bytes.load(Ordering::Relaxed),
            total: None,
            percent: Some(100.0),
            phase: "complete".into(),
        });
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
    use super::{
        download_dash_into, expand_dash_template, is_complete_vod_playlist, parse_dash_document,
        parse_dash_representations, parse_dash_sidx, parse_hls_master, resolve_dash_track,
        unsupported_hls_master_reason, unsupported_hls_reason,
    };
    use crate::download::GlobalRateLimiter;
    use std::collections::{HashMap, HashSet};
    use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock};
    use tokio::sync::mpsc;
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
    fn dash_template_expands_all_supported_tokens() {
        let representation = super::DashRepresentation {
            id: "video-main".into(), bandwidth: Some(1_800_000), ..Default::default()
        };
        let expanded = expand_dash_template(
            "video/$RepresentationID$/$Bandwidth$/$Number%05d$-$Time$-$.m4s",
            &representation, 7, 12000
        ).unwrap();
        assert_eq!(expanded, "video/video-main/1800000/00007-12000-$.m4s");
    }

    #[test]
    fn static_dash_segment_template_builds_init_and_timeline_urls() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT6S">
          <BaseURL>https://cdn.example.test/root/</BaseURL>
          <Period duration="PT6S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate timescale="1" startNumber="1" initialization="$RepresentationID$/init.mp4" media="$RepresentationID$/seg-$Number%03d$.m4s">
                <SegmentTimeline><S t="0" d="2" r="2"/></SegmentTimeline>
              </SegmentTemplate>
              <Representation id="v720" bandwidth="1800000" width="1280" height="720"/>
              <Representation id="v360" bandwidth="700000" width="640" height="360"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let manifest_url = "https://cdn.example.test/manifest/video.mpd";
        let document = parse_dash_document(manifest).unwrap();
        assert!(document.is_static);
        assert_eq!(document.periods.len(), 1);
        let variants = parse_dash_representations(manifest, manifest_url);
        assert_eq!(variants.len(), 2);
        assert!(variants[0].downloadable);
        assert_eq!(variants[0].representation_id.as_deref(), Some("v720"));
        let period = &document.periods[0];
        let (adaptation, representation) = period.adaptations[0].representations.iter()
            .find(|representation| representation.id == "v720")
            .map(|representation| (&period.adaptations[0], representation)).unwrap();
        let track = resolve_dash_track(&document, period, adaptation, representation, manifest_url, false).unwrap();
        assert_eq!(track.segment_urls.len(), 3);
        assert_eq!(track.initialization_url.as_deref(), Some("https://cdn.example.test/root/v720/init.mp4"));
        assert_eq!(track.segment_urls[0], "https://cdn.example.test/root/v720/seg-001.m4s");
        assert_eq!(track.segment_urls[2], "https://cdn.example.test/root/v720/seg-003.m4s");
    }

    #[test]
    fn dash_inherits_segment_template_and_resolves_baseurl_entities() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT4S">
          <BaseURL>https://cdn.example.test/vod/?token=a&amp;quality=1/</BaseURL>
          <Period duration="PT4S">
            <SegmentTemplate timescale="1" duration="2" startNumber="5" initialization="init-$RepresentationID$.mp4" media="chunk-$Number$.m4s"/>
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <Representation id="main" bandwidth="900000" width="960" height="540"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let manifest_url = "https://cdn.example.test/index.mpd";
        let document = parse_dash_document(manifest).unwrap();
        let period = &document.periods[0];
        let adaptation = &period.adaptations[0];
        let representation = &adaptation.representations[0];
        let track = resolve_dash_track(&document, period, adaptation, representation, manifest_url, false).unwrap();
        assert_eq!(track.segment_urls.len(), 2);
        assert!(track.segment_urls[0].contains("/vod/chunk-5.m4s?token=a&quality=1"));
        let initialization_url = track.initialization_url.unwrap();
        assert!(initialization_url.contains("/vod/init-main.mp4?token=a&quality=1"));
    }

    #[test]
    fn dash_separate_audio_is_selected_and_attached_to_each_video_quality() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT4S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="v-init.mp4" media="v-$Number$.m4s"/>
              <Representation id="video-720" bandwidth="1600000" width="1280" height="720"/>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4">
              <SegmentTemplate duration="2" initialization="a-init.mp4" media="a-$Number$.m4s"/>
              <Representation id="audio-en" bandwidth="128000"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 1);
        assert!(variants[0].downloadable);
        assert_eq!(variants[0].representation_id.as_deref(), Some("video-720"));
        assert_eq!(variants[0].audio_representation_id.as_deref(), Some("audio-en"));
    }

    #[test]
    fn dynamic_drm_and_unsupported_dash_layouts_are_not_downloadable() {
        let dynamic = r#"<MPD type="dynamic"><Period><AdaptationSet contentType="video"><Representation id="v1" height="720"/></AdaptationSet></Period></MPD>"#;
        let drm = r#"<MPD type="static" mediaPresentationDuration="PT4S"><Period duration="PT4S"><AdaptationSet contentType="video"><ContentProtection schemeIdUri="urn:uuid:0000"/><Representation id="v1" height="720"/></AdaptationSet></Period></MPD>"#;
        let segment_base = r#"<MPD type="static"><Period><AdaptationSet contentType="video"><SegmentBase indexRange="1-10"/><Representation id="v1" height="720"/></AdaptationSet></Period></MPD>"#;
        for manifest in [dynamic, drm, segment_base] {
            let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
            assert!(!variants.is_empty());
            assert!(variants.iter().all(|variant| !variant.downloadable));
        }
    }

    #[test]
    fn hls_session_encryption_is_not_offered_but_audio_and_byterange_are() {
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI=\"key.bin\"").is_some());
        // A separate audio rendition is supported now (muxed with FFmpeg),
        // so it is no longer an unsupported-master reason.
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"audio.m3u8\"").is_none());
        // Byte-range segments (including a ranged init segment) are
        // supported now, so they are no longer an unsupported reason.
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"100@0\"\n#EXT-X-ENDLIST").is_none());
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-BYTERANGE:10@0\nseg.ts\n#EXT-X-ENDLIST").is_none());
    }

    #[test]
    fn hls_byterange_segments_parse_with_explicit_and_implicit_offsets() {
        let playlist = "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXT-X-BYTERANGE:10@0\npart.bin\n#EXT-X-BYTERANGE:10\npart.bin\n#EXT-X-BYTERANGE:5@100\npart.bin\n#EXT-X-ENDLIST\n";
        let base = Url::parse("https://cdn.example.test/hls/media.m3u8").unwrap();
        let parsed = super::parse_hls_media_playlist(playlist, &base).unwrap();
        assert_eq!(parsed.segments.len(), 3);
        assert_eq!(parsed.segments[0].range, Some((0, 10)));
        // No offset: continues at the end of the previous range (10).
        assert_eq!(parsed.segments[1].range, Some((10, 10)));
        assert_eq!(parsed.segments[2].range, Some((100, 5)));
        assert!(parsed.segments.iter().all(|segment| segment.url.ends_with("/part.bin")));
    }

    #[test]
    fn hls_master_audio_group_is_attached_to_its_variant() {
        let master = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio-en\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"audio.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720,AUDIO=\"audio-en\"\nhigh.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\nlow.m3u8\n";
        let base = Url::parse("https://cdn.example.test/master.m3u8").unwrap();
        let variants = parse_hls_master(master, &base).unwrap();
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].audio_representation_id.as_deref(), Some("audio-en"));
        assert_eq!(variants[1].audio_representation_id, None);
        let renditions = super::parse_hls_audio_renditions(master, &base);
        assert_eq!(renditions.len(), 1);
        assert_eq!(renditions[0].group_id, "audio-en");
        assert!(renditions[0].is_default);
        assert!(renditions[0].uri.ends_with("/audio.m3u8"));
    }

    #[test]
    fn dash_segment_list_resolves_media_urls_and_ranges() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT6S">
          <Period duration="PT6S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <Representation id="v1" bandwidth="900000" width="640" height="360">
                <SegmentList>
                  <Initialization sourceURL="init.mp4"/>
                  <SegmentURL media="seg-1.m4s"/>
                  <SegmentURL media="seg-2.m4s" mediaRange="100-199"/>
                  <SegmentURL mediaRange="0-49"/>
                </SegmentList>
              </Representation>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let manifest_url = "https://cdn.example.test/manifest/video.mpd";
        let document = parse_dash_document(manifest).unwrap();
        let period = &document.periods[0];
        let (adaptation, representation) = super::find_dash_representation(period, "v1", false).unwrap();
        let track = resolve_dash_track(&document, period, adaptation, representation, manifest_url, false).unwrap();
        assert_eq!(track.initialization_url.as_deref(), Some("https://cdn.example.test/manifest/init.mp4"));
        assert_eq!(track.segment_urls.len(), 3);
        assert_eq!(track.segment_urls[0], "https://cdn.example.test/manifest/seg-1.m4s");
        assert_eq!(track.segment_ranges[0], None);
        assert_eq!(track.segment_urls[1], "https://cdn.example.test/manifest/seg-2.m4s");
        assert_eq!(track.segment_ranges[1], Some((100, 199)));
        // An entry without a media URL is a byte range of the BaseURL resource.
        assert_eq!(track.segment_urls[2], "https://cdn.example.test/manifest/video.mpd");
        assert_eq!(track.segment_ranges[2], Some((0, 49)));
        let variants = parse_dash_representations(manifest, manifest_url);
        assert_eq!(variants.len(), 1);
        assert!(variants[0].downloadable);
    }

    #[test]
    fn dash_segment_base_plan_requires_index_range_and_init() {
        let with_index = r#"<MPD type="static" mediaPresentationDuration="PT4S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <BaseURL>https://cdn.example.test/media.mp4</BaseURL>
              <Representation id="v1" bandwidth="900000" width="640" height="360">
                <SegmentBase indexRange="100-167">
                  <Initialization sourceURL="init.mp4"/>
                </SegmentBase>
              </Representation>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let manifest_url = "https://cdn.example.test/manifest/video.mpd";
        let document = parse_dash_document(with_index).unwrap();
        let period = &document.periods[0];
        let (adaptation, representation) = super::find_dash_representation(period, "v1", false).unwrap();
        let track = resolve_dash_track(&document, period, adaptation, representation, manifest_url, false).unwrap();
        let plan = track.segment_base.as_ref().unwrap();
        assert_eq!(plan.base_url, "https://cdn.example.test/media.mp4");
        assert_eq!(plan.index_range, (100, 167));
        assert_eq!(plan.init_url.as_deref(), Some("https://cdn.example.test/init.mp4"));
        assert!(parse_dash_representations(with_index, manifest_url)[0].downloadable);

        let no_index = with_index.replace(" indexRange=\"100-167\"", "");
        let document = parse_dash_document(&no_index).unwrap();
        let period = &document.periods[0];
        let (adaptation, representation) = super::find_dash_representation(period, "v1", false).unwrap();
        assert!(resolve_dash_track(&document, period, adaptation, representation, manifest_url, false).is_err());
        assert!(!parse_dash_representations(&no_index, manifest_url)[0].downloadable);

        let no_init = with_index.replace("<Initialization sourceURL=\"init.mp4\"/>", "");
        let document = parse_dash_document(&no_init).unwrap();
        let period = &document.periods[0];
        let (adaptation, representation) = super::find_dash_representation(period, "v1", false).unwrap();
        assert!(resolve_dash_track(&document, period, adaptation, representation, manifest_url, false).is_err());
        assert!(!parse_dash_representations(&no_init, manifest_url)[0].downloadable);
    }

    #[test]
    fn dash_multi_period_variants_require_the_representation_in_every_period() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT8S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v720" bandwidth="1600000" width="1280" height="720"/>
              <Representation id="v360" bandwidth="700000" width="640" height="360"/>
            </AdaptationSet>
          </Period>
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v720" bandwidth="1600000" width="1280" height="720"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 2);
        let v720 = variants.iter().find(|variant| variant.representation_id.as_deref() == Some("v720")).unwrap();
        assert!(v720.downloadable);
        assert_eq!(v720.requires_ffmpeg, Some(true));
        let v360 = variants.iter().find(|variant| variant.representation_id.as_deref() == Some("v360")).unwrap();
        assert!(!v360.downloadable);
        assert!(v360.note.as_deref().unwrap().contains("Period 2"));
        assert!(v360.note.as_deref().unwrap().contains("v360"));
    }

    #[test]
    fn dash_multi_period_codec_mismatch_is_not_downloadable() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT8S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028"/>
            </AdaptationSet>
          </Period>
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.4d401f"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 1);
        assert!(!variants[0].downloadable);
        assert!(variants[0].note.as_deref().unwrap().contains("Period 2"));
        assert!(variants[0].note.as_deref().unwrap().contains("avc1.4d401f"));
    }

    #[test]
    fn dash_multi_period_audio_sample_rate_mismatch_is_not_downloadable() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT8S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028"/>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4" audioSamplingRate="44100">
              <SegmentTemplate duration="2" initialization="a-init.mp4" media="a-seg-$Number$.m4s"/>
              <Representation id="a1" bandwidth="128000" codecs="mp4a.40.2"/>
            </AdaptationSet>
          </Period>
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028"/>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4" audioSamplingRate="48000">
              <SegmentTemplate duration="2" initialization="a-init.mp4" media="a-seg-$Number$.m4s"/>
              <Representation id="a1" bandwidth="128000" codecs="mp4a.40.2"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 1);
        assert!(!variants[0].downloadable);
        assert!(variants[0].note.as_deref().unwrap().contains("Period 2"));
        assert!(variants[0].note.as_deref().unwrap().contains("48000"));
    }

    #[test]
    fn dash_multi_period_frame_rate_mismatch_is_not_downloadable() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT8S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028" frameRate="30"/>
            </AdaptationSet>
          </Period>
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028" frameRate="25"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 1);
        assert!(!variants[0].downloadable);
        assert!(variants[0].note.as_deref().unwrap().contains("Period 2"));
        assert!(variants[0].note.as_deref().unwrap().contains("25 fps"));
    }

    #[test]
    fn dash_multi_period_equivalent_frame_rate_forms_are_compatible() {
        // "30000/1001" and "29.97" are the same rate written two ways; the
        // compat check must not false-positive on them.
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT8S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028" frameRate="30000/1001"/>
            </AdaptationSet>
          </Period>
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028" frameRate="29.97"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 1);
        assert!(variants[0].downloadable);
    }

    #[test]
    fn dash_multi_period_audio_channel_mismatch_is_not_downloadable() {
        let manifest = r#"<MPD type="static" mediaPresentationDuration="PT8S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028"/>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4">
              <AudioChannelConfiguration schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011" value="2"/>
              <SegmentTemplate duration="2" initialization="a-init.mp4" media="a-seg-$Number$.m4s"/>
              <Representation id="a1" bandwidth="128000" codecs="mp4a.40.2"/>
            </AdaptationSet>
          </Period>
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
              <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028"/>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4">
              <AudioChannelConfiguration schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011" value="6"/>
              <SegmentTemplate duration="2" initialization="a-init.mp4" media="a-seg-$Number$.m4s"/>
              <Representation id="a1" bandwidth="128000" codecs="mp4a.40.2"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/manifest.mpd");
        assert_eq!(variants.len(), 1);
        assert!(!variants[0].downloadable);
        assert!(variants[0].note.as_deref().unwrap().contains("Period 2"));
        assert!(variants[0].note.as_deref().unwrap().contains("channel count (6)"));
    }

    fn build_dash_sidx(version: u8, first_offset: u64, sizes: &[u32]) -> Vec<u8> {
        let header = if version == 0 { 32 } else { 40 };
        let size = header + sizes.len() as u32 * 12;
        let mut out = Vec::new();
        out.extend_from_slice(&size.to_be_bytes());
        out.extend_from_slice(b"sidx");
        out.extend_from_slice(&[version, 0, 0, 0]);
        out.extend_from_slice(&1u32.to_be_bytes());
        out.extend_from_slice(&1000u32.to_be_bytes());
        if version == 0 {
            out.extend_from_slice(&0u32.to_be_bytes());
            out.extend_from_slice(&(first_offset as u32).to_be_bytes());
        } else {
            out.extend_from_slice(&0u64.to_be_bytes());
            out.extend_from_slice(&first_offset.to_be_bytes());
        }
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&(sizes.len() as u16).to_be_bytes());
        for size in sizes {
            out.extend_from_slice(&size.to_be_bytes());
            out.extend_from_slice(&1000u32.to_be_bytes());
            out.extend_from_slice(&0u32.to_be_bytes());
        }
        out
    }

    #[test]
    fn dash_sidx_parses_first_offset_and_reference_sizes() {
        let sidx = build_dash_sidx(0, 0, &[10, 20, 30]);
        assert_eq!(parse_dash_sidx(&sidx, 100).unwrap(), vec![(168, 177), (178, 197), (198, 227)]);
        let sidx = build_dash_sidx(0, 5, &[10]);
        assert_eq!(parse_dash_sidx(&sidx, 100).unwrap(), vec![(149, 158)]);
        let sidx = build_dash_sidx(1, 0, &[7, 8]);
        assert_eq!(parse_dash_sidx(&sidx, 0).unwrap(), vec![(64, 70), (71, 78)]);
        assert!(parse_dash_sidx(b"not an sidx box at all, definitely not", 0).is_err());
        assert!(parse_dash_sidx(&build_dash_sidx(0, 0, &[]), 0).is_err());
    }

    #[test]
    fn dash_sidx_truncated_v1_header_is_an_error_not_a_panic() {
        // A v1 sidx whose declared size is 32..=39 cannot hold its 40-byte
        // header; the parser must return an error, never panic on the
        // reference_count read (a 36-byte v1 box used to panic there).
        for size in [32_u32, 36] {
            let mut short = build_dash_sidx(1, 0, &[7, 8]);
            short.truncate(size as usize);
            short[0..4].copy_from_slice(&size.to_be_bytes());
            assert!(parse_dash_sidx(&short, 0).is_err(), "v1 sidx with size {size} must error, not panic");
        }
        // A truncated v0 header errors too.
        let mut short_v0 = build_dash_sidx(0, 0, &[10]);
        short_v0.truncate(20);
        short_v0[0..4].copy_from_slice(&20u32.to_be_bytes());
        assert!(parse_dash_sidx(&short_v0, 0).is_err());
    }

    /// Minimal static-file HTTP fixture for DASH tests. Serves the bytes in
    /// `routes` (shared so the test can add the MPD after learning the base
    /// URL), honors `Range: bytes=a-b` with 206 + Content-Range unless the
    /// path is in `no_range` (then it always answers 200 with the whole
    /// body), and counts GETs (total and ranged) per path.
    fn spawn_dash_server(
        routes: Arc<RwLock<HashMap<String, Vec<u8>>>>,
        no_range: Arc<HashSet<String>>,
    ) -> (String, Arc<Mutex<HashMap<String, usize>>>, Arc<Mutex<HashMap<String, usize>>>, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::Duration;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let counts = Arc::new(Mutex::new(HashMap::new()));
        let range_counts = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let stop = shutdown.clone();
        let counts_out = counts.clone();
        let range_counts_out = range_counts.clone();
        let handle = std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let _ = stream.set_nonblocking(false);
                let routes = routes.clone();
                let no_range = no_range.clone();
                let counts = counts.clone();
                let range_counts = range_counts.clone();
                std::thread::spawn(move || loop {
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
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            Err(_) => break false,
                        }
                    };
                    if !head_complete { return; }
                    let request = String::from_utf8_lossy(&raw);
                    let mut parts = request.split_whitespace();
                    let method = parts.next().unwrap_or("");
                    let target = parts.next().unwrap_or("");
                    let path = target.split('?').next().unwrap_or(target).to_string();
                    let range = request.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("range").then(|| value.trim().to_string())
                    });
                    let body = routes.read().unwrap().get(&path).cloned();
                    let Some(body) = body else {
                        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
                        return;
                    };
                    if method == "HEAD" {
                        let headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n", body.len());
                        if stream.write_all(headers.as_bytes()).is_err() { return; }
                        continue;
                    }
                    if method != "GET" {
                        let _ = stream.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n");
                        return;
                    }
                    *counts.lock().unwrap().entry(path.clone()).or_insert(0) += 1;
                    let bounds = range.as_deref()
                        .filter(|_| !no_range.contains(&path))
                        .and_then(|value| value.strip_prefix("bytes="))
                        .and_then(|value| value.split_once('-'))
                        .and_then(|(start, end)| Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?)));
                    let mut response = Vec::new();
                    if let Some((start, end)) = bounds.filter(|(start, end)| *start <= *end && *end < body.len()) {
                        *range_counts.lock().unwrap().entry(path.clone()).or_insert(0) += 1;
                        let payload = &body[start..=end];
                        response.extend_from_slice(
                            format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\n\r\n", payload.len(), body.len()).as_bytes(),
                        );
                        response.extend_from_slice(payload);
                    } else if bounds.is_some() {
                        response.extend_from_slice(b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\n\r\n");
                    } else {
                        response.extend_from_slice(
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n", body.len()).as_bytes(),
                        );
                        response.extend_from_slice(&body);
                    }
                    if stream.write_all(&response).is_err() { return; }
                });
            }
        });
        (format!("http://{address}"), counts_out, range_counts_out, shutdown, handle)
    }

    fn stop_dash_server(shutdown: Arc<AtomicBool>, handle: std::thread::JoinHandle<()>) {
        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }

    fn dash_test_dir(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("thunderload-dash-{prefix}-{}", uuid::Uuid::new_v4()))
    }

    async fn dash_out_dir_is_empty(out_dir: &std::path::Path) -> bool {
        match tokio::fs::read_dir(out_dir).await {
            Ok(mut entries) => entries.next_entry().await.unwrap().is_none(),
            Err(_) => true,
        }
    }

    /// Generates a real fragmented-MP4 track with ffmpeg (testsrc video or
    /// sine audio) and returns (init bytes, segment bytes in order).
    /// Returns None when ffmpeg cannot run — the caller then asserts the
    /// FFmpeg-missing error path instead of the real-media path.
    async fn generate_dash_fmp4_track(dir: &std::path::Path, video: bool) -> Option<(Vec<u8>, Vec<Vec<u8>>)> {
        tokio::fs::create_dir_all(dir).await.ok()?;
        let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
        let mut command = tokio::process::Command::new(&executable);
        command.arg("-y");
        if video {
            command.arg("-f").arg("lavfi").arg("-i").arg("testsrc=duration=2:size=128x72:rate=10")
                .arg("-an").arg("-c:v").arg("libx264").arg("-pix_fmt").arg("yuv420p");
        } else {
            command.arg("-f").arg("lavfi").arg("-i").arg("sine=frequency=440:duration=2")
                .arg("-vn").arg("-c:a").arg("aac");
        }
        let output = command
            .arg("-hls_time").arg("1")
            .arg("-hls_segment_type").arg("fmp4")
            .arg("-hls_playlist_type").arg("vod")
            .arg("-hls_fmp4_init_filename").arg("init.mp4")
            .arg(dir.join("track.m3u8"))
            .output().await.ok()?;
        if !output.status.success() { return None; }
        let init = tokio::fs::read(dir.join("init.mp4")).await.ok()?;
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(dir).await.ok()?;
        while let Some(entry) = entries.next_entry().await.ok()? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".m4s") { names.push(name); }
        }
        names.sort();
        let mut segments = Vec::new();
        for name in names {
            segments.push(tokio::fs::read(dir.join(name)).await.ok()?);
        }
        if segments.is_empty() { return None; }
        Some((init, segments))
    }

    /// Lists the stream types of a media file with ffprobe plus its
    /// duration in seconds, or None when ffprobe is not available.
    async fn probe_dash_media(path: &std::path::Path) -> Option<(Vec<String>, f64)> {
        let output = tokio::process::Command::new("ffprobe")
            .arg("-v").arg("error")
            .arg("-show_entries").arg("stream=codec_type")
            .arg("-show_entries").arg("format=duration")
            .arg("-of").arg("csv=p=0")
            .arg(path)
            .output().await.ok()?;
        if !output.status.success() { return None; }
        let mut streams = Vec::new();
        let mut duration = 0.0;
        for line in String::from_utf8_lossy(&output.stdout).lines().map(str::trim).filter(|line| !line.is_empty()) {
            if line == "video" || line == "audio" {
                streams.push(line.to_string());
            } else if let Ok(value) = line.parse::<f64>() {
                duration = value;
            }
        }
        Some((streams, duration))
    }

    fn segment_list_av_mpd(video_segments: usize, audio_segments: usize) -> String {
        let mut mpd = String::from(r#"<MPD type="static" mediaPresentationDuration="PT4S">
          <Period duration="PT4S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <Representation id="v1" bandwidth="1600000" width="1280" height="720">
                <SegmentList>
                  <Initialization sourceURL="v-init.mp4"/>"#);
        for index in 1..=video_segments {
            mpd.push_str(&format!("\n                  <SegmentURL media=\"v-{index}.m4s\"/>"));
        }
        mpd.push_str(r#"
                </SegmentList>
              </Representation>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4">
              <Representation id="a1" bandwidth="128000">
                <SegmentList>
                  <Initialization sourceURL="a-init.mp4"/>"#);
        for index in 1..=audio_segments {
            mpd.push_str(&format!("\n                  <SegmentURL media=\"a-{index}.m4s\"/>"));
        }
        mpd.push_str(r#"
                </SegmentList>
              </Representation>
            </AdaptationSet>
          </Period>
        </MPD>"#);
        mpd
    }

    #[tokio::test]
    async fn dash_segment_list_download_completes_with_exact_bytes() {
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let init = b"FAKEINIT".to_vec();
        let segments = vec![b"SEG-ONE".to_vec(), b"SEG-TWO".to_vec(), b"SEG-THREE".to_vec()];
        {
            let mut map = routes.write().unwrap();
            map.insert("/init.mp4".into(), init.clone());
            for (index, segment) in segments.iter().enumerate() {
                map.insert(format!("/seg-{}.m4s", index + 1), segment.clone());
            }
            map.insert("/video.mpd".into(), r#"<MPD type="static" mediaPresentationDuration="PT6S">
              <Period duration="PT6S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="900000" width="640" height="360">
                    <SegmentList>
                      <Initialization sourceURL="init.mp4"/>
                      <SegmentURL media="seg-1.m4s"/>
                      <SegmentURL media="seg-2.m4s"/>
                      <SegmentURL media="seg-3.m4s"/>
                    </SegmentList>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#.to_string().into_bytes());
        }
        let out_dir = dash_test_dir("segment-list");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let target = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "list-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = init.clone();
        for segment in &segments { expected.extend_from_slice(segment); }
        assert_eq!(bytes, expected);
        let counts = counts.lock().unwrap();
        assert_eq!(counts.get("/init.mp4"), Some(&1));
        assert_eq!(counts.get("/seg-1.m4s"), Some(&1));
        assert_eq!(counts.get("/seg-2.m4s"), Some(&1));
        assert_eq!(counts.get("/seg-3.m4s"), Some(&1));
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_segment_list_range_only_init_downloads_with_exact_bytes() {
        // Single-file SegmentList: the init is a byte range of the
        // representation's own resource (<Initialization range="..."/> with
        // no sourceURL), as are the segments.
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let init = b"FAKEINIT".to_vec();
        let segments = vec![b"SEG-ONE".to_vec(), b"SEG-TWO".to_vec(), b"SEG-THREE".to_vec()];
        let mut media = init.clone();
        for segment in &segments { media.extend_from_slice(segment); }
        {
            let mut map = routes.write().unwrap();
            map.insert("/media.mp4".into(), media);
            map.insert("/video.mpd".into(), r#"<MPD type="static" mediaPresentationDuration="PT6S">
              <Period duration="PT6S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="900000" width="640" height="360">
                    <BaseURL>media.mp4</BaseURL>
                    <SegmentList>
                      <Initialization range="0-7"/>
                      <SegmentURL mediaRange="8-14"/>
                      <SegmentURL mediaRange="15-21"/>
                      <SegmentURL mediaRange="22-30"/>
                    </SegmentList>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#.to_string().into_bytes());
        }
        let out_dir = dash_test_dir("segment-list-range-init");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let target = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "list-range-init", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = init.clone();
        for segment in &segments { expected.extend_from_slice(segment); }
        assert_eq!(bytes, expected);
        let counts = counts.lock().unwrap();
        assert_eq!(counts.get("/media.mp4"), Some(&4));
        let range_counts = range_counts.lock().unwrap();
        assert_eq!(range_counts.get("/media.mp4"), Some(&4));
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_segment_list_range_only_init_included_for_webm() {
        // Non-mp4 mime: a dropped range-only init used to be silently
        // skipped (no error, broken output); it must be fetched like any
        // other init.
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let init = b"WEBMINIT".to_vec();
        let segments = vec![b"WEBM-ONE".to_vec(), b"WEBM-TWO".to_vec()];
        let mut media = init.clone();
        for segment in &segments { media.extend_from_slice(segment); }
        {
            let mut map = routes.write().unwrap();
            map.insert("/media.webm".into(), media);
            map.insert("/video.mpd".into(), r#"<MPD type="static" mediaPresentationDuration="PT4S">
              <Period duration="PT4S">
                <AdaptationSet contentType="video" mimeType="video/webm">
                  <Representation id="v1" bandwidth="900000" width="640" height="360">
                    <BaseURL>media.webm</BaseURL>
                    <SegmentList>
                      <Initialization range="0-7"/>
                      <SegmentURL mediaRange="8-15"/>
                      <SegmentURL mediaRange="16-23"/>
                    </SegmentList>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#.to_string().into_bytes());
        }
        let out_dir = dash_test_dir("segment-list-range-init-webm");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let target = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "list-range-init-webm", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = init.clone();
        for segment in &segments { expected.extend_from_slice(segment); }
        assert_eq!(bytes, expected);
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_segment_list_separate_audio_muxes_with_ffmpeg() {
        let media_dir = dash_test_dir("segment-list-audio-media");
        let video = generate_dash_fmp4_track(&media_dir.join("video"), true).await;
        let audio = generate_dash_fmp4_track(&media_dir.join("audio"), false).await;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let (video_segments, audio_segments) = match (&video, &audio) {
            (Some((_, video_segments)), Some((_, audio_segments))) => (video_segments.len(), audio_segments.len()),
            _ => (2, 2),
        };
        {
            let mut map = routes.write().unwrap();
            if let (Some((video_init, video_segments)), Some((audio_init, audio_segments))) = (&video, &audio) {
                map.insert("/v-init.mp4".into(), video_init.clone());
                for (index, segment) in video_segments.iter().enumerate() {
                    map.insert(format!("/v-{}.m4s", index + 1), segment.clone());
                }
                map.insert("/a-init.mp4".into(), audio_init.clone());
                for (index, segment) in audio_segments.iter().enumerate() {
                    map.insert(format!("/a-{}.m4s", index + 1), segment.clone());
                }
            }
            map.insert("/video.mpd".into(), segment_list_av_mpd(video_segments, audio_segments).into_bytes());
        }
        let out_dir = dash_test_dir("segment-list-audio");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_dash_into(
            &format!("{base}/video.mpd"), "v1", Some("a1"), "list-av", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await;
        if video.is_some() && audio.is_some() {
            let target = result.unwrap();
            let (streams, _duration) = probe_dash_media(&target).await
                .expect("ffprobe must read the muxed DASH output");
            assert_eq!(streams, vec!["video".to_string(), "audio".to_string()]);
        } else {
            let message = result.unwrap_err().to_string();
            assert!(message.contains("FFmpeg"), "expected an FFmpeg error, got: {message}");
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(media_dir).await.unwrap();
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_segment_base_sidx_download_completes_with_exact_bytes() {
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let init = b"FAKEINIT-BASE".to_vec();
        let seg1 = b"SEGMENT-ONE-DATA".to_vec();
        let seg2 = b"SEGMENT-TWO-DATA!".to_vec();
        let seg3 = b"SEGMENT-THREE-DAT".to_vec();
        let sidx = build_dash_sidx(0, 0, &[seg1.len() as u32, seg2.len() as u32, seg3.len() as u32]);
        let index_end = sidx.len() - 1;
        let mut media = sidx.clone();
        media.extend_from_slice(&seg1);
        media.extend_from_slice(&seg2);
        media.extend_from_slice(&seg3);
        {
            let mut map = routes.write().unwrap();
            map.insert("/init.mp4".into(), init.clone());
            map.insert("/media.mp4".into(), media);
            map.insert("/video.mpd".into(), format!(r#"<MPD type="static" mediaPresentationDuration="PT6S">
              <Period duration="PT6S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="900000" width="640" height="360">
                    <BaseURL>media.mp4</BaseURL>
                    <SegmentBase indexRange="0-{index_end}">
                      <Initialization sourceURL="init.mp4"/>
                    </SegmentBase>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#).into_bytes());
        }
        let out_dir = dash_test_dir("segment-base");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let target = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "base-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = init.clone();
        expected.extend_from_slice(&seg1);
        expected.extend_from_slice(&seg2);
        expected.extend_from_slice(&seg3);
        assert_eq!(bytes, expected);
        let counts = counts.lock().unwrap();
        assert_eq!(counts.get("/init.mp4"), Some(&1));
        // One ranged fetch for the sidx index plus one per media segment.
        assert_eq!(range_counts.lock().unwrap().get("/media.mp4"), Some(&4));
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_multi_period_same_codec_concatenates_in_order() {
        let media_dir = dash_test_dir("multi-period-media");
        let period1 = generate_dash_fmp4_track(&media_dir.join("p1"), true).await;
        let period2 = generate_dash_fmp4_track(&media_dir.join("p2"), true).await;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let (p1_segments, p2_segments) = match (&period1, &period2) {
            (Some((_, video_segments)), Some((_, audio_segments))) => (video_segments.len(), audio_segments.len()),
            _ => (2, 2),
        };
        {
            let mut map = routes.write().unwrap();
            if let (Some((p1_init, p1_segments)), Some((p2_init, p2_segments))) = (&period1, &period2) {
                map.insert("/p1-init.mp4".into(), p1_init.clone());
                for (index, segment) in p1_segments.iter().enumerate() {
                    map.insert(format!("/p1-{}.m4s", index + 1), segment.clone());
                }
                map.insert("/p2-init.mp4".into(), p2_init.clone());
                for (index, segment) in p2_segments.iter().enumerate() {
                    map.insert(format!("/p2-{}.m4s", index + 1), segment.clone());
                }
            }
            let mut mpd = String::from(r#"<MPD type="static" mediaPresentationDuration="PT8S">
              <Period duration="PT4S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028">
                    <SegmentList>
                      <Initialization sourceURL="p1-init.mp4"/>"#);
            for index in 1..=p1_segments {
                mpd.push_str(&format!("\n                      <SegmentURL media=\"p1-{index}.m4s\"/>"));
            }
            mpd.push_str(r#"
                    </SegmentList>
                  </Representation>
                </AdaptationSet>
              </Period>
              <Period duration="PT4S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028">
                    <SegmentList>
                      <Initialization sourceURL="p2-init.mp4"/>"#);
            for index in 1..=p2_segments {
                mpd.push_str(&format!("\n                      <SegmentURL media=\"p2-{index}.m4s\"/>"));
            }
            mpd.push_str(r#"
                    </SegmentList>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#);
            map.insert("/video.mpd".into(), mpd.into_bytes());
        }
        let out_dir = dash_test_dir("multi-period");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "multi-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await;
        if period1.is_some() && period2.is_some() {
            let target = result.unwrap();
            let (streams, duration) = probe_dash_media(&target).await
                .expect("ffprobe must read the concatenated multi-period output");
            assert_eq!(streams, vec!["video".to_string()]);
            assert!((3.5..=4.5).contains(&duration), "expected ~4s of video, got {duration}s");
            let counts = counts.lock().unwrap();
            assert_eq!(counts.get("/p1-init.mp4"), Some(&1));
            assert_eq!(counts.get("/p2-init.mp4"), Some(&1));
        } else {
            let message = result.unwrap_err().to_string();
            assert!(message.contains("FFmpeg"), "expected an FFmpeg error, got: {message}");
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(media_dir).await.unwrap();
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_multi_period_codec_mismatch_fails_without_output() {
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        {
            let mut map = routes.write().unwrap();
            map.insert("/video.mpd".into(), r#"<MPD type="static" mediaPresentationDuration="PT8S">
              <Period duration="PT4S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
                  <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.640028"/>
                </AdaptationSet>
              </Period>
              <Period duration="PT4S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <SegmentTemplate duration="2" initialization="init.mp4" media="seg-$Number$.m4s"/>
                  <Representation id="v1" bandwidth="1600000" width="1280" height="720" codecs="avc1.4d401f"/>
                </AdaptationSet>
              </Period>
            </MPD>"#.to_string().into_bytes());
        }
        let out_dir = dash_test_dir("multi-period-mismatch");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "mismatch-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await;
        let message = result.unwrap_err().to_string();
        assert!(message.contains("Period 2"), "expected the period-2 mismatch, got: {message}");
        assert!(message.contains("avc1.4d401f"), "expected the mismatching codec, got: {message}");
        assert!(dash_out_dir_is_empty(&out_dir).await, "a failed download must not leave an output file behind");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    #[tokio::test]
    async fn dash_segment_base_without_usable_index_fails_without_output() {
        // Case 1: the indexRange points at bytes that are not an sidx box.
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        {
            let mut map = routes.write().unwrap();
            map.insert("/init.mp4".into(), b"FAKEINIT".to_vec());
            map.insert("/media.mp4".into(), b"GARBAGE-NOT-AN-SIDX-BOX-AT-ALL".to_vec());
            map.insert("/video.mpd".into(), r#"<MPD type="static" mediaPresentationDuration="PT6S">
              <Period duration="PT6S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="900000" width="640" height="360">
                    <BaseURL>media.mp4</BaseURL>
                    <SegmentBase indexRange="0-15">
                      <Initialization sourceURL="init.mp4"/>
                    </SegmentBase>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#.to_string().into_bytes());
        }
        let out_dir = dash_test_dir("segment-base-garbage");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "garbage-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await;
        let message = result.unwrap_err().to_string();
        assert!(message.contains("sidx"), "expected an sidx error, got: {message}");
        assert!(dash_out_dir_is_empty(&out_dir).await, "a failed download must not leave an output file behind");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();

        // Case 2: SegmentBase with no indexRange at all.
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        {
            let mut map = routes.write().unwrap();
            map.insert("/video.mpd".into(), r#"<MPD type="static" mediaPresentationDuration="PT6S">
              <Period duration="PT6S">
                <AdaptationSet contentType="video" mimeType="video/mp4">
                  <Representation id="v1" bandwidth="900000" width="640" height="360">
                    <SegmentBase>
                      <Initialization sourceURL="init.mp4"/>
                    </SegmentBase>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>"#.to_string().into_bytes());
        }
        let out_dir = dash_test_dir("segment-base-no-index");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "noindex-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await;
        let message = result.unwrap_err().to_string();
        assert!(message.contains("indexRange"), "expected an indexRange error, got: {message}");
        assert!(dash_out_dir_is_empty(&out_dir).await, "a failed download must not leave an output file behind");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }
}


#[cfg(test)]
mod hls_download_tests {
    use super::{
        download_hls_resumable, ffmpeg_available, hls_work_dir,
        partial_download_metadata_path, validate_hls_content_range,
        DownloadControl, DownloadOutcome, GlobalRateLimiter,
    };
    use crate::download::reserve_download_path;
    use std::{
        collections::{HashMap, HashSet},
        io::{Read, Write},
        net::TcpListener,
        sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, Mutex},
        thread,
        time::Duration,
    };
    use tokio::sync::{mpsc, watch};

    /// Fixture knobs for the HLS test server. Defaults reproduce the old
    /// behavior (no failures, Range honored); tests opt into deterministic
    /// failures or Range-ignoring responses per path. Failure by segment
    /// index is decided per request, so it cannot race with the test
    /// thread the way a flipped flag can.
    #[derive(Default)]
    struct HlsServerOptions {
        /// Fail segment requests whose index is >= this value.
        fail_from_index: Option<Arc<AtomicUsize>>,
        /// Paths for which the server ignores Range and always answers
        /// 200 with the full body (exercises the 200-fallback path).
        ignore_range_paths: Arc<Mutex<HashSet<String>>>,
        /// Paths for which the server answers with this Content-Range
        /// header value instead of the correct one.
        content_range_overrides: Arc<Mutex<HashMap<String, String>>>,
    }

    /// Static-route HTTP/1.1 fixture mirroring the keep-alive style of the
    /// segmented-download tests: accepted sockets are served blocking, each
    /// connection handles sequential requests, Range is honored with 206,
    /// and bodies trickle in 1 KiB chunks so pause/cancel tests can stop
    /// a download mid-flight.
    fn spawn_hls_server(
        routes: HashMap<String, Vec<u8>>,
        chunk_delay: Duration,
        options: HlsServerOptions,
    ) -> (String, Arc<Mutex<HashMap<String, usize>>>, Arc<AtomicUsize>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let counts = Arc::new(Mutex::new(HashMap::new()));
        let range_206 = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let routes = Arc::new(routes);
        let HlsServerOptions { fail_from_index, ignore_range_paths, content_range_overrides } = options;
        let routes = Arc::new(routes);
        let thread_counts = counts.clone();
        let thread_range = range_206.clone();
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
                let _ = stream.set_nonblocking(false);
                let routes = routes.clone();
                let counts = thread_counts.clone();
                let range_206 = thread_range.clone();
                let fail_from_index = fail_from_index.clone();
                let ignore_range_paths = ignore_range_paths.clone();
                let content_range_overrides = content_range_overrides.clone();
                thread::spawn(move || loop {
                    let mut raw = Vec::new();
                    let mut buffer = [0_u8; 1024];
                    let head_complete = loop {
                        match stream.read(&mut buffer) {
                            Ok(0) => break false,
                            Ok(count) => {
                                raw.extend_from_slice(&buffer[..count]);
                                if raw.windows(4).any(|window| window == b"\r\n\r\n") { break true; }
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(1));
                            }
                            Err(_) => break false,
                        }
                    };
                    if !head_complete { return; }
                    let request = String::from_utf8_lossy(&raw);
                    let mut lines = request.lines();
                    let first = lines.next().unwrap_or_default();
                    let path = first.split_whitespace().nth(1).unwrap_or("/").to_string();
                    let path_only = path.split('?').next().unwrap_or(&path).to_string();
                    let range = request.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("range").then(|| value.trim().to_string())
                    });
                    {
                        let mut counts = counts.lock().unwrap();
                        *counts.entry(path_only.clone()).or_insert(0) += 1;
                    }
                    if let Some(from) = &fail_from_index {
                        let segment_index = path_only.strip_prefix("/seg")
                            .and_then(|rest| rest.strip_suffix(".ts"))
                            .and_then(|number| number.parse::<usize>().ok());
                        if segment_index.map_or(false, |index| index >= from.load(Ordering::Relaxed)) {
                            let _ = stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            return;
                        }
                    }
                    let Some(body) = routes.get(&path_only) else {
                        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                        return;
                    };
                    let ignore_range = ignore_range_paths.lock().unwrap().contains(&path_only);
                    let (status, payload, is_range) = if ignore_range {
                        ("200 OK", &body[..], false)
                    } else {
                        match range.as_deref()
                            .and_then(|value| value.strip_prefix("bytes="))
                            .and_then(|value| value.split_once('-'))
                            .and_then(|(start, end)| Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?)))
                        {
                            Some((start, end)) if start <= end && end < body.len() => {
                                ("206 Partial Content", &body[start..=end], true)
                            }
                            _ => ("200 OK", &body[..], false),
                        }
                    };
                    if is_range { range_206.fetch_add(1, Ordering::Relaxed); }
                    let content_range = content_range_overrides.lock().unwrap().get(&path_only).cloned();
                    let headers = match content_range {
                        Some(value) => format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nContent-Range: {value}\r\nConnection: keep-alive\r\n\r\n",
                            payload.len()
                        ),
                        None => format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: keep-alive\r\n\r\n",
                            payload.len()
                        ),
                    };
                    if stream.write_all(headers.as_bytes()).is_err() { return; }
                    for chunk in payload.chunks(1024) {
                        if stream.write_all(chunk).is_err() { return; }
                        if !chunk_delay.is_zero() { thread::sleep(chunk_delay); }
                    }
                });
            }
        });
        (format!("http://{address}"), counts, range_206, shutdown, handle)
    }

    fn stop_server(shutdown: Arc<AtomicBool>, handle: thread::JoinHandle<()>) {
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    async fn run_resumable(
        url: &str,
        target: &std::path::Path,
        control_rx: watch::Receiver<DownloadControl>,
    ) -> anyhow::Result<DownloadOutcome> {
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        download_hls_resumable(url, target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new())).await
    }

    fn fresh_control() -> (watch::Sender<DownloadControl>, watch::Receiver<DownloadControl>) {
        watch::channel(DownloadControl::Running)
    }

    /// Reads the on-disk checkpoint and returns the completed video
    /// segment indexes. Waits up to the deadline for at least one.
    async fn wait_for_checkpoint_progress(target: &std::path::Path, minimum_done: usize) -> Vec<usize> {
        let meta = partial_download_metadata_path(target);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(bytes) = tokio::fs::read(&meta).await {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    let done = value.get("video_done").and_then(|v| v.as_array())
                        .map(|items| items.iter().filter_map(|item| item.as_u64().map(|n| n as usize)).collect::<Vec<_>>())
                        .unwrap_or_default();
                    if done.len() >= minimum_done { return done; }
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "checkpoint never recorded progress for {}", target.display());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn segment_routes(count: usize, size: usize) -> (HashMap<String, Vec<u8>>, Vec<Vec<u8>>) {
        let mut routes = HashMap::new();
        let mut bodies = Vec::new();
        for index in 0..count {
            let body = vec![(index % 251) as u8; size];
            routes.insert(format!("/seg{index}.ts"), body.clone());
            bodies.push(body);
        }
        (routes, bodies)
    }

    fn media_playlist(segment_count: usize) -> String {
        let mut playlist = String::from("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:0\n");
        for index in 0..segment_count {
            playlist.push_str("#EXTINF:10.0,\n");
            playlist.push_str(&format!("seg{index}.ts\n"));
        }
        playlist.push_str("#EXT-X-ENDLIST\n");
        playlist
    }

    fn fmp4_media_playlist(init: &str, segments: &[String]) -> String {
        let mut playlist = String::from("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:10\n");
        playlist.push_str(&format!("#EXT-X-MAP:URI=\"{init}\"\n"));
        for segment in segments {
            playlist.push_str("#EXTINF:10.0,\n");
            playlist.push_str(segment);
            playlist.push('\n');
        }
        playlist.push_str("#EXT-X-ENDLIST\n");
        playlist
    }

    /// Generates a real fragmented-MP4 HLS track with ffmpeg (testsrc
    /// video or sine audio) and returns (init bytes, segment bytes in
    /// order). Returns None when ffmpeg cannot run — the caller then
    /// skips the real-media assertion. The playlist text is always
    /// hand-written by the test, so no ffmpeg playlist dialect is
    /// depended on.
    async fn generate_fmp4_track(dir: &std::path::Path, video: bool) -> Option<(Vec<u8>, Vec<Vec<u8>>)> {
        tokio::fs::create_dir_all(dir).await.ok()?;
        let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
        let mut command = tokio::process::Command::new(&executable);
        command.arg("-y");
        if video {
            command.arg("-f").arg("lavfi").arg("-i").arg("testsrc=duration=2:size=128x72:rate=10")
                .arg("-an").arg("-c:v").arg("libx264").arg("-pix_fmt").arg("yuv420p");
        } else {
            command.arg("-f").arg("lavfi").arg("-i").arg("sine=frequency=440:duration=2")
                .arg("-vn").arg("-c:a").arg("aac");
        }
        let output = command
            .arg("-hls_time").arg("1")
            .arg("-hls_segment_type").arg("fmp4")
            .arg("-hls_playlist_type").arg("vod")
            .arg("-hls_fmp4_init_filename").arg("init.mp4")
            .arg(dir.join("track.m3u8"))
            .output().await.ok()?;
        if !output.status.success() { return None; }
        let init = tokio::fs::read(dir.join("init.mp4")).await.ok()?;
        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(dir).await.ok()?;
        while let Some(entry) = entries.next_entry().await.ok()? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".m4s") { names.push(name); }
        }
        names.sort();
        let mut segments = Vec::new();
        for name in names {
            segments.push(tokio::fs::read(dir.join(name)).await.ok()?);
        }
        if segments.is_empty() { return None; }
        Some((init, segments))
    }

    /// Lists the stream types of a media file with ffprobe, or None when
    /// ffprobe is not available (the caller falls back to an ftyp check).
    async fn probe_stream_types(path: &std::path::Path) -> Option<Vec<String>> {
        let output = tokio::process::Command::new("ffprobe")
            .arg("-v").arg("error")
            .arg("-show_entries").arg("stream=codec_type")
            .arg("-of").arg("csv=p=0")
            .arg(path)
            .output().await.ok()?;
        if !output.status.success() { return None; }
        Some(String::from_utf8_lossy(&output.stdout).lines()
            .map(str::trim).filter(|line| !line.is_empty()).map(str::to_string).collect())
    }

    #[tokio::test]
    async fn hls_vod_download_completes_with_exact_bytes() {
        let (mut routes, bodies) = segment_routes(6, 4096);
        routes.insert("/media.m3u8".into(), media_playlist(6).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-vod-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        let expected: Vec<u8> = bodies.concat();
        assert_eq!(bytes, expected);
        assert!(!tokio::fs::try_exists(hls_work_dir(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_pause_mid_download_then_resume_completes_without_re_requesting_finished_segments() {
        let (mut routes, bodies) = segment_routes(24, 8192);
        routes.insert("/media.m3u8".into(), media_playlist(24).into_bytes());
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-pause-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");

        let (control_tx, control_rx) = fresh_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_resumable(&task_url, &task_target, control_rx).await });
        let done_before_pause = wait_for_checkpoint_progress(&target, 1).await;
        assert!(!done_before_pause.is_empty() && done_before_pause.len() < 24);
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Paused);
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let counts_after_pause = counts.lock().unwrap().clone();
        drop(control_tx);

        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, bodies.concat());
        // Segments finished before the pause were not requested again.
        let counts_after_resume = counts.lock().unwrap().clone();
        for index in done_before_pause {
            let key = format!("/seg{index}.ts");
            assert_eq!(counts_after_resume.get(&key), counts_after_pause.get(&key),
                "segment {index} was re-requested after resume");
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_error_interrupted_download_resumes_from_disk_checkpoint() {
        let (mut routes, bodies) = segment_routes(12, 8192);
        routes.insert("/media.m3u8".into(), media_playlist(12).into_bytes());
        // Deterministic failure: segments 0-5 always succeed, segments
        // 6+ always fail with 500. The failure is decided per request by
        // the fixture, so it cannot race with the test thread the way a
        // flipped flag could (a flag flipped after the checkpoint wait
        // regularly landed too late — the remaining requests had already
        // been read by the server — and the download completed instead
        // of erroring).
        let fail_from_index = Arc::new(AtomicUsize::new(6));
        let options = HlsServerOptions {
            fail_from_index: Some(fail_from_index.clone()),
            ..Default::default()
        };
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-error-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");

        let (control_tx, control_rx) = fresh_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_resumable(&task_url, &task_target, control_rx).await });
        let done_before_error = wait_for_checkpoint_progress(&target, 1).await;
        assert!(!done_before_error.is_empty() && done_before_error.len() < 12);
        // Segments 6+ fail deterministically, so the download must error
        // while keeping its checkpoint on disk.
        let result = task.await.unwrap();
        assert!(result.is_err(), "expected the failing server to abort the download");
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let counts_after_error = counts.lock().unwrap().clone();
        drop(control_tx);

        // Server recovers; a fresh call (fresh process equivalent) resumes.
        fail_from_index.store(usize::MAX, Ordering::Relaxed);
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, bodies.concat());
        let counts_after_resume = counts.lock().unwrap().clone();
        for index in done_before_error {
            let key = format!("/seg{index}.ts");
            assert_eq!(counts_after_resume.get(&key), counts_after_error.get(&key),
                "segment {index} was re-requested after the error-interrupted resume");
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_byterange_playlist_completes_with_exact_bytes() {
        let resource: Vec<u8> = (0..300).map(|index| (index % 251) as u8).collect();
        let mut routes = HashMap::new();
        routes.insert("/part.bin".into(), resource.clone());
        let playlist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-BYTERANGE:100@0\npart.bin\n#EXT-X-BYTERANGE:100\npart.bin\n#EXT-X-BYTERANGE:50@200\npart.bin\n#EXT-X-BYTERANGE:50\npart.bin\n#EXT-X-ENDLIST\n";
        routes.insert("/media.m3u8".into(), playlist.as_bytes().to_vec());
        let (base, counts, range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-range-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, resource);
        // The fixture answered the slice requests with 206, proving the
        // Range path was used (a 200 fallback would still produce exact
        // bytes by slicing, so the count is the real assertion here).
        assert!(range_206.load(Ordering::Relaxed) >= 4, "expected Range requests answered with 206");
        assert_eq!(*counts.lock().unwrap().get("/part.bin").unwrap_or(&0), 4);
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_byterange_server_ignoring_range_streams_window_without_buffering() {
        // D3 regression: the server ignores Range and answers 200 with
        // the whole resource. The download must still produce exact bytes
        // — and must not answer any request with 206, proving the
        // 200-fallback path was taken (the old code buffered the whole
        // body in RAM and skipped the rate limiter on this path).
        let resource: Vec<u8> = (0..300).map(|index| (index % 251) as u8).collect();
        let mut routes = HashMap::new();
        routes.insert("/part.bin".into(), resource.clone());
        let playlist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-BYTERANGE:100@0\npart.bin\n#EXT-X-BYTERANGE:100\npart.bin\n#EXT-X-BYTERANGE:50@200\npart.bin\n#EXT-X-BYTERANGE:50\npart.bin\n#EXT-X-ENDLIST\n";
        routes.insert("/media.m3u8".into(), playlist.as_bytes().to_vec());
        let options = HlsServerOptions {
            ignore_range_paths: Arc::new(Mutex::new(HashSet::from(["/part.bin".to_string()]))),
            ..Default::default()
        };
        let (base, counts, range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-range200-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, resource);
        assert_eq!(range_206.load(Ordering::Relaxed), 0, "expected no 206 answers on the Range-ignoring server");
        assert_eq!(*counts.lock().unwrap().get("/part.bin").unwrap_or(&0), 4);
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_map_byterange_seeds_offset_continuation_for_same_uri_segments() {
        // D4 regression: the MAP range (bytes 0-9 of part.bin) and the
        // first segment range (no offset) share one URI, so the segment
        // must continue after the init range (bytes 10-19), not restart
        // at byte 0. The second segment has an explicit offset (20).
        let resource: Vec<u8> = (0..30).map(|index| (index % 251) as u8).collect();
        let mut routes = HashMap::new();
        routes.insert("/part.bin".into(), resource.clone());
        let playlist = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:10\n#EXT-X-MAP:URI=\"part.bin\",BYTERANGE=\"10@0\"\n#EXTINF:10.0,\n#EXT-X-BYTERANGE:10\npart.bin\n#EXTINF:10.0,\n#EXT-X-BYTERANGE:10@20\npart.bin\n#EXT-X-ENDLIST\n";
        routes.insert("/media.m3u8".into(), playlist.as_bytes().to_vec());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-mapcont-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, resource);
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_wrong_content_range_on_206_is_rejected() {
        // D5 regression: the server answers 206 with a Content-Range
        // for a different span than requested. The download must error
        // (keeping its checkpoint) instead of silently assembling the
        // wrong bytes.
        let resource: Vec<u8> = (0..300).map(|index| (index % 251) as u8).collect();
        let mut routes = HashMap::new();
        routes.insert("/part.bin".into(), resource.clone());
        let playlist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-BYTERANGE:100@0\npart.bin\n#EXT-X-ENDLIST\n";
        routes.insert("/media.m3u8".into(), playlist.as_bytes().to_vec());
        let options = HlsServerOptions {
            content_range_overrides: Arc::new(Mutex::new(HashMap::from([
                ("/part.bin".to_string(), "bytes 100-199/300".to_string()),
            ]))),
            ..Default::default()
        };
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-badrange-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await;
        assert!(outcome.is_err(), "expected a 206 with a mismatched Content-Range to be rejected");
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn hls_content_range_validation_accepts_matching_spans_only() {
        assert!(validate_hls_content_range("bytes 0-99/300", 0, 99).is_ok());
        assert!(validate_hls_content_range("bytes 100-199/*", 100, 199).is_ok());
        assert!(validate_hls_content_range("bytes 100-199/300", 0, 99).is_err());
        assert!(validate_hls_content_range("bytes 0-98/300", 0, 99).is_err());
        assert!(validate_hls_content_range("nonsense", 0, 99).is_err());
        assert!(validate_hls_content_range("bytes 0-99", 0, 99).is_err());
    }

    #[tokio::test]
    async fn hls_fmp4_playlist_produces_init_first_mp4_file() {
        // Minimal fake ftyp box: 24-byte size, "ftyp", "isom", "iso2".
        let mut init = Vec::new();
        init.extend_from_slice(&24_u32.to_be_bytes());
        init.extend_from_slice(b"ftypisomiso2");
        init.extend_from_slice(&[0_u8; 8]);
        let mut routes = HashMap::new();
        routes.insert("/init.mp4".into(), init.clone());
        let mut segments = Vec::new();
        for index in 0..3 {
            let body = format!("moof-mdat-segment-{index}").into_bytes();
            routes.insert(format!("/seg{index}.m4s"), body.clone());
            segments.push(body);
        }
        let playlist = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:10\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:10.0,\nseg0.m4s\n#EXTINF:10.0,\nseg1.m4s\n#EXTINF:10.0,\nseg2.m4s\n#EXT-X-ENDLIST\n";
        routes.insert("/media.m3u8".into(), playlist.as_bytes().to_vec());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-fmp4-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        assert_eq!(target.extension().and_then(|value| value.to_str()), Some("mp4"));
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = init.clone();
        for segment in &segments { expected.extend_from_slice(segment); }
        assert_eq!(bytes, expected);
        assert_eq!(&bytes[4..8], b"ftyp");
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_fmp4_real_fragments_produce_valid_mp4() {
        // Without FFmpeg in this environment the fake-fragment test above
        // still proves init-first ordered assembly; only the real
        // playability check is skipped.
        if !ffmpeg_available().await { return; }
        let directory = std::env::temp_dir().join(format!("thunderload-hls-fmp4-real-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let gen_dir = directory.join("gen");
        let Some((init, segments)) = generate_fmp4_track(&gen_dir, true).await else {
            panic!("ffmpeg is available but could not generate the video test track");
        };
        let mut routes = HashMap::new();
        routes.insert("/init.mp4".into(), init);
        let segment_names: Vec<String> = segments.iter().enumerate()
            .map(|(index, _)| format!("seg{index}.m4s")).collect();
        for (index, body) in segments.iter().enumerate() {
            routes.insert(format!("/seg{index}.m4s"), body.clone());
        }
        routes.insert("/media.m3u8".into(), fmp4_media_playlist("init.mp4", &segment_names).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        match probe_stream_types(&target).await {
            Some(streams) => {
                assert!(streams.iter().any(|stream| stream == "video"), "assembled fMP4 has no video stream: {streams:?}");
            }
            None => {
                let bytes = tokio::fs::read(&target).await.unwrap();
                assert_eq!(&bytes[4..8], b"ftyp");
            }
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_fmp4_init_byterange_is_fetched_as_range() {
        let mut resource = Vec::new();
        resource.extend_from_slice(&24_u32.to_be_bytes());
        resource.extend_from_slice(b"ftypisomiso2");
        resource.extend_from_slice(&[0_u8; 8]);
        resource.extend_from_slice(b"trailing-media-bytes-not-part-of-init");
        let mut routes = HashMap::new();
        routes.insert("/media.bin".into(), resource.clone());
        routes.insert("/seg0.m4s".into(), b"moof-mdat-0".to_vec());
        let playlist = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:10\n#EXT-X-MAP:URI=\"media.bin\",BYTERANGE=\"24@0\"\n#EXTINF:10.0,\nseg0.m4s\n#EXT-X-ENDLIST\n";
        routes.insert("/media.m3u8".into(), playlist.as_bytes().to_vec());
        let (base, _counts, range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-maprange-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&format!("{base}/media.m3u8"), &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = resource[..24].to_vec();
        expected.extend_from_slice(b"moof-mdat-0");
        assert_eq!(bytes, expected);
        assert!(range_206.load(Ordering::Relaxed) >= 1, "expected the init range request to be answered with 206");
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_separate_audio_downloads_and_muxes() {
        // With FFmpeg present the tracks are real fMP4 fragments generated
        // by ffmpeg, so the mux result is a genuinely playable file
        // (verified with ffprobe below). Without FFmpeg the fragments are
        // fake bytes: the download still fetches both tracks and then
        // fails at the mux step with a clear FFmpeg error.
        let ffmpeg = ffmpeg_available().await;
        let directory = std::env::temp_dir().join(format!("thunderload-hls-audio-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let mut routes = HashMap::new();
        let (video_segments, audio_segments): (Vec<String>, Vec<String>);
        if ffmpeg {
            let video_dir = directory.join("gen-video");
            let audio_dir = directory.join("gen-audio");
            let (video_init, video_parts) = generate_fmp4_track(&video_dir, true).await
                .expect("ffmpeg is available but could not generate the video test track");
            let (audio_init, audio_parts) = generate_fmp4_track(&audio_dir, false).await
                .expect("ffmpeg is available but could not generate the audio test track");
            routes.insert("/v-init.mp4".into(), video_init);
            routes.insert("/a-init.mp4".into(), audio_init);
            video_segments = video_parts.iter().enumerate().map(|(index, _)| format!("v{index}.m4s")).collect();
            audio_segments = audio_parts.iter().enumerate().map(|(index, _)| format!("a{index}.m4s")).collect();
            for (index, body) in video_parts.iter().enumerate() {
                routes.insert(format!("/v{index}.m4s"), body.clone());
            }
            for (index, body) in audio_parts.iter().enumerate() {
                routes.insert(format!("/a{index}.m4s"), body.clone());
            }
        } else {
            let mut init = Vec::new();
            init.extend_from_slice(&24_u32.to_be_bytes());
            init.extend_from_slice(b"ftypisomiso2");
            init.extend_from_slice(&[0_u8; 8]);
            routes.insert("/v-init.mp4".into(), init.clone());
            routes.insert("/a-init.mp4".into(), init);
            for index in 0..2 {
                routes.insert(format!("/v{index}.m4s"), format!("video-fragment-{index}").into_bytes());
                routes.insert(format!("/a{index}.m4s"), format!("audio-fragment-{index}").into_bytes());
            }
            video_segments = vec!["v0.m4s".into(), "v1.m4s".into()];
            audio_segments = vec!["a0.m4s".into(), "a1.m4s".into()];
        }
        let master = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio-en\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"audio.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720,AUDIO=\"audio-en\"\nvideo.m3u8\n";
        routes.insert("/master.m3u8".into(), master.as_bytes().to_vec());
        routes.insert("/video.m3u8".into(), fmp4_media_playlist("v-init.mp4", &video_segments).into_bytes());
        routes.insert("/audio.m3u8".into(), fmp4_media_playlist("a-init.mp4", &audio_segments).into_bytes());
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let (_control_tx, control_rx) = fresh_control();
        let result = run_resumable(&format!("{base}/master.m3u8"), &target, control_rx).await;
        // Both tracks must have been fetched regardless of FFmpeg.
        let counts = counts.lock().unwrap().clone();
        let mut expected_paths = vec!["/v-init.mp4".to_string(), "/a-init.mp4".to_string()];
        expected_paths.extend(video_segments.iter().map(|name| format!("/{name}")));
        expected_paths.extend(audio_segments.iter().map(|name| format!("/{name}")));
        for path in expected_paths {
            assert_eq!(counts.get(&path), Some(&1), "expected exactly one request for {path}");
        }
        if ffmpeg {
            let outcome = result.unwrap();
            assert_eq!(outcome, DownloadOutcome::Completed);
            let bytes = tokio::fs::read(&target).await.unwrap();
            assert!(!bytes.is_empty());
            match probe_stream_types(&target).await {
                Some(streams) => {
                    assert!(streams.iter().any(|stream| stream == "video"), "muxed output has no video stream: {streams:?}");
                    assert!(streams.iter().any(|stream| stream == "audio"), "muxed output has no audio stream: {streams:?}");
                }
                None => {
                    assert_eq!(&bytes[4..8], b"ftyp", "muxed output should be an MP4 starting with an ftyp box");
                }
            }
        } else {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("FFmpeg"), "expected a clear FFmpeg error, got: {error}");
            // The checkpoint is kept so installing FFmpeg and retrying
            // resumes straight at the mux step.
            assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_cancel_removes_partial_files() {
        let (mut routes, _bodies) = segment_routes(24, 8192);
        routes.insert("/media.m3u8".into(), media_playlist(24).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-cancel-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (control_tx, control_rx) = fresh_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_resumable(&task_url, &task_target, control_rx).await });
        let done = wait_for_checkpoint_progress(&target, 1).await;
        assert!(!done.is_empty());
        control_tx.send(DownloadControl::Cancelled).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Cancelled);
        assert!(!tokio::fs::try_exists(&target).await.unwrap());
        assert!(!tokio::fs::try_exists(hls_work_dir(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

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
            representation_id: None, audio_representation_id: None, audio_only: None,
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
                representation_id: None, audio_representation_id: None, audio_only: None,
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
            representation_id: None, audio_representation_id: None, audio_only: None,
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
        representation_id: None, audio_representation_id: None, audio_only: None,
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
                    representation_id: None, audio_representation_id: audio_group, audio_only: None });
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
struct DashRepresentation {
    id: String,
    width: Option<u32>,
    height: Option<u32>,
    bandwidth: Option<u64>,
    mime_type: String,
    base_url: Option<String>,
    template: Option<DashTemplate>,
}

#[derive(Debug, Clone, Default)]
struct DashAdaptation {
    content_type: String,
    mime_type: String,
    base_url: Option<String>,
    template: Option<DashTemplate>,
    representations: Vec<DashRepresentation>,
}

#[derive(Debug, Clone, Default)]
struct DashDocument {
    is_static: bool,
    protected: bool,
    unsupported_segment_mode: bool,
    period_count: usize,
    duration_seconds: Option<f64>,
    period_duration_seconds: Option<f64>,
    base_url: Option<String>,
    period_base_url: Option<String>,
    template: Option<DashTemplate>,
    period_template: Option<DashTemplate>,
    adaptations: Vec<DashAdaptation>,
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
    stack: Vec<String>,
    active_adaptation: Option<usize>,
    active_representation: Option<(usize, usize)>,
    active_template_scope: Option<DashScope>,
    base_scope: Option<DashScope>,
    base_text: String,
}

#[derive(Debug, Clone)]
struct DashResolvedTrack {
    mime_type: String,
    initialization_url: Option<String>,
    segment_urls: Vec<String>,
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
    } else if state.stack.iter().any(|item| item == "Period") {
        DashScope::Period
    } else {
        DashScope::Mpd
    }
}

fn inherited_dash_template(
    document: &DashDocument,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
) -> DashTemplate {
    if let (Some(adaptation_index), Some(representation_index)) = (adaptation_index, representation_index) {
        if let Some(template) = document.adaptations.get(adaptation_index)
            .and_then(|adaptation| adaptation.representations.get(representation_index))
            .and_then(|representation| representation.template.clone()) {
            return template;
        }
    }
    if let Some(template) = adaptation_index.and_then(|index| document.adaptations.get(index))
        .and_then(|adaptation| adaptation.template.clone()) {
        return template;
    }
    document.period_template.clone()
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

fn dash_template_mut<'a>(
    document: &'a mut DashDocument,
    scope: DashScope,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
) -> Option<&'a mut DashTemplate> {
    match scope {
        DashScope::Mpd => document.template.as_mut(),
        DashScope::Period => document.period_template.as_mut(),
        DashScope::Adaptation => document.adaptations.get_mut(adaptation_index?)?.template.as_mut(),
        DashScope::Representation => document.adaptations.get_mut(adaptation_index?)?
            .representations.get_mut(representation_index?)?.template.as_mut(),
    }
}

fn set_dash_base_url(
    document: &mut DashDocument,
    scope: DashScope,
    adaptation_index: Option<usize>,
    representation_index: Option<usize>,
    value: String,
) {
    if value.trim().is_empty() { return; }
    match scope {
        DashScope::Mpd => { if document.base_url.is_none() { document.base_url = Some(value); } }
        DashScope::Period => { if document.period_base_url.is_none() { document.period_base_url = Some(value); } }
        DashScope::Adaptation => {
            if let Some(adaptation) = adaptation_index.and_then(|index| document.adaptations.get_mut(index)) {
                if adaptation.base_url.is_none() { adaptation.base_url = Some(value); }
            }
        }
        DashScope::Representation => {
            if let Some(representation) = adaptation_index
                .and_then(|index| document.adaptations.get_mut(index))
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
    match name {
        "MPD" => {
            document.is_static = attributes.get("type").map(|value| value.eq_ignore_ascii_case("static")).unwrap_or(true);
            document.duration_seconds = attributes.get("mediaPresentationDuration").and_then(|value| parse_iso_duration(value));
        }
        "Period" => {
            document.period_count += 1;
            document.period_duration_seconds = attributes.get("duration").and_then(|value| parse_iso_duration(value));
        }
        "AdaptationSet" => {
            document.adaptations.push(DashAdaptation {
                content_type: attributes.get("contentType").cloned().unwrap_or_default(),
                mime_type: attributes.get("mimeType").cloned().unwrap_or_default(),
                ..DashAdaptation::default()
            });
            state.active_adaptation = Some(document.adaptations.len() - 1);
        }
        "Representation" => {
            if let Some(adaptation_index) = state.active_adaptation {
                let representation_index = document.adaptations[adaptation_index].representations.len();
                document.adaptations[adaptation_index].representations.push(DashRepresentation {
                    id: attributes.get("id").cloned().unwrap_or_else(|| format!("representation-{representation_index}")),
                    width: attributes.get("width").and_then(|value| value.parse().ok()),
                    height: attributes.get("height").and_then(|value| value.parse().ok()),
                    bandwidth: attributes.get("bandwidth").and_then(|value| value.parse().ok()),
                    mime_type: attributes.get("mimeType").cloned().unwrap_or_default(),
                    ..DashRepresentation::default()
                });
                state.active_representation = Some((adaptation_index, representation_index));
            }
        }
        "SegmentTemplate" => {
            let (adaptation_index, representation_index) = state.active_representation
                .map(|(adaptation, representation)| (Some(adaptation), Some(representation)))
                .unwrap_or((state.active_adaptation, None));
            let inherited = inherited_dash_template(document, adaptation_index, representation_index);
            let template = apply_dash_template_attributes(inherited, &attributes);
            match scope_before {
                DashScope::Mpd => document.template = Some(template),
                DashScope::Period => document.period_template = Some(template),
                DashScope::Adaptation => {
                    if let Some(index) = state.active_adaptation { document.adaptations[index].template = Some(template); }
                }
                DashScope::Representation => {
                    if let Some((adaptation, representation)) = state.active_representation {
                        document.adaptations[adaptation].representations[representation].template = Some(template);
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
                    let (adaptation_index, representation_index) = state.active_representation
                        .map(|(adaptation, representation)| (Some(adaptation), Some(representation)))
                        .unwrap_or((state.active_adaptation, None));
                    if let Some(template) = dash_template_mut(document, scope, adaptation_index, representation_index) {
                        template.timeline.push(point);
                    }
                }
            }
        }
        "BaseURL" => {
            state.base_scope = Some(scope_before);
            state.base_text.clear();
        }
        "ContentProtection" => document.protected = true,
        "SegmentList" | "SegmentBase" => document.unsupported_segment_mode = true,
        _ => {}
    }
}

fn dash_end_element(document: &mut DashDocument, state: &mut DashParserState, name: &str) {
    match name {
        "BaseURL" => {
            if let Some(scope) = state.base_scope.take() {
                let (adaptation_index, representation_index) = state.active_representation
                    .map(|(adaptation, representation)| (Some(adaptation), Some(representation)))
                    .unwrap_or((state.active_adaptation, None));
                set_dash_base_url(document, scope, adaptation_index, representation_index, state.base_text.trim().to_string());
            }
            state.base_text.clear();
        }
        "SegmentTemplate" => state.active_template_scope = None,
        "Representation" => state.active_representation = None,
        "AdaptationSet" => state.active_adaptation = None,
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
                state.stack.push(name);
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
                let _ = state.stack.pop();
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
    adaptation: &DashAdaptation,
    representation: &DashRepresentation,
) -> Result<Url> {
    let mut base = Url::parse(manifest_url)?;
    for component in [
        document.base_url.as_deref(),
        document.period_base_url.as_deref(),
        adaptation.base_url.as_deref(),
        representation.base_url.as_deref(),
    ].into_iter().flatten() {
        base = base.join(component)?;
    }
    Ok(base)
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
    adaptation: &DashAdaptation,
    representation: &DashRepresentation,
    manifest_url: &str,
    _is_audio: bool,
) -> Result<DashResolvedTrack> {
    let template = representation.template.clone()
        .or_else(|| adaptation.template.clone())
        .or_else(|| document.period_template.clone())
        .or_else(|| document.template.clone())
        .ok_or_else(|| anyhow!("DASH representation has no inherited SegmentTemplate"))?;
    let media_template = template.media.as_deref().ok_or_else(|| anyhow!("DASH SegmentTemplate has no media attribute"))?;
    if template.timescale == 0 { return Err(anyhow!("DASH SegmentTemplate timescale is zero")); }
    let timescale = template.timescale.max(1);
    let period_duration = document.period_duration_seconds.or(document.duration_seconds)
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

    let base = merge_dash_base_url(manifest_url, document, adaptation, representation)?;
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
    Ok(DashResolvedTrack {
        mime_type: if representation.mime_type.is_empty() { adaptation.mime_type.clone() } else { representation.mime_type.clone() },
        initialization_url,
        segment_urls,
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

fn parse_dash_representations(body: &str, manifest_url: &str) -> Vec<VideoVariant> {
    let document = match parse_dash_document(body) {
        Ok(document) => document,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    if !document.is_static || document.protected || document.unsupported_segment_mode || document.period_count > 1 {
        let note = if !document.is_static {
            "Dynamic/live DASH MPDs are not supported"
        } else if document.protected {
            "ContentProtection/DRM DASH streams are not supported"
        } else if document.period_count > 1 {
            "Multi-period DASH MPDs are not supported in this build"
        } else {
            "This DASH SegmentBase/SegmentList layout is not supported"
        };
        for adaptation in &document.adaptations {
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
                    });
                }
            }
        }
        return out;
    }

    let audio_representation = document.adaptations.iter().enumerate()
        .filter(|(_, adaptation)| is_audio_adaptation(adaptation))
        .flat_map(|(adaptation_index, adaptation)| adaptation.representations.iter().enumerate()
            .map(move |(representation_index, representation)| (adaptation_index, representation_index, representation)))
        .max_by_key(|(_, _, representation)| representation.bandwidth.unwrap_or(0));

    for adaptation in &document.adaptations {
        for representation in adaptation.representations.iter().filter(|representation| is_video_representation(adaptation, representation)) {
            let result = resolve_dash_track(&document, adaptation, representation, manifest_url, false);
            let (downloadable, note) = match result {
                Ok(_) => {
                    if let Some((audio_adaptation_index, _, audio)) = audio_representation {
                        match resolve_dash_track(&document, &document.adaptations[audio_adaptation_index], audio, manifest_url, true) {
                            Ok(_) => (true, Some("Static DASH segments; separate audio will be muxed with FFmpeg".to_string())),
                            Err(error) => (false, Some(format!("Audio representation is unsupported: {error}"))),
                        }
                    } else {
                        (true, Some("Static DASH VOD segments".to_string()))
                    }
                }
                Err(error) => (false, Some(error.to_string())),
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
    for variant in variants.iter_mut().filter(|variant| variant.kind == "dash" && variant.audio_representation_id.is_some()) {
        if !available {
            variant.downloadable = false;
            variant.note = Some("This DASH MPD has separate audio and video tracks. Install FFmpeg or set THUNDERLOAD_FFMPEG before downloading.".into());
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
    document: &'a DashDocument,
    representation_id: &str,
    audio: bool,
) -> Option<(&'a DashAdaptation, &'a DashRepresentation)> {
    document.adaptations.iter()
        .filter(|adaptation| is_audio_adaptation(adaptation) == audio)
        .find_map(|adaptation| adaptation.representations.iter()
            .find(|representation| representation.id == representation_id)
            .map(|representation| (adaptation, representation)))
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
    let mut urls = Vec::with_capacity(track.segment_urls.len() + usize::from(track.initialization_url.is_some()));
    if let Some(initialization) = &track.initialization_url {
        urls.push(initialization.clone());
    } else if track.mime_type.to_ascii_lowercase().contains("mp4") {
        return Err(anyhow!("DASH MP4 representation is missing its initialization segment"));
    }
    urls.extend(track.segment_urls.iter().cloned());
    if urls.is_empty() { return Err(anyhow!("DASH representation contains no media URLs")); }

    let mut tasks = tokio::task::JoinSet::new();
    for (index, segment_url) in urls.into_iter().enumerate() {
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
            let response = request.send().await?.error_for_status()?;
            let mut stream = response.bytes_stream();
            let mut file = fs::File::create(&path).await?;
            let mut bytes = 0_u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
                bytes = bytes.saturating_add(chunk.len() as u64);
                let wait = rate_limiter.reserve(chunk.len() as u64).await;
                if !wait.is_zero() { tokio::time::sleep(wait).await; }
            }
            file.flush().await?;
            if bytes == 0 { return Err(anyhow!("DASH server returned an empty initialization/segment file")); }
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

/// Downloads one static DASH representation. When the MPD defines separate
/// audio/video AdaptationSets, both tracks are fetched and stream-copied into a
/// playable container using FFmpeg. Dynamic MPDs, DRM and unsupported segment
/// addressing are rejected during inspection instead of producing a broken file.
pub async fn download_dash(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    filename: &str,
    referer: Option<&str>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    let body = fetch_text(manifest_url, referer).await?;
    let document = parse_dash_document(&body)?;
    if !document.is_static { return Err(anyhow!("Dynamic/live DASH MPDs are not supported")); }
    if document.protected { return Err(anyhow!("DASH ContentProtection/DRM is not supported")); }
    if document.unsupported_segment_mode { return Err(anyhow!("DASH SegmentBase/SegmentList is not supported by this build")); }
    if document.period_count > 1 { return Err(anyhow!("Multi-period DASH MPDs are not supported by this build")); }

    let (video_adaptation, video_representation) = find_dash_representation(&document, representation_id, false)
        .ok_or_else(|| anyhow!("The selected DASH video representation is not present in the MPD"))?;
    let video = resolve_dash_track(&document, video_adaptation, video_representation, manifest_url, false)?;
    let audio = audio_representation_id.map(|id| {
        let (adaptation, representation) = find_dash_representation(&document, id, true)
            .ok_or_else(|| anyhow!("The selected DASH audio representation is not present in the MPD"))?;
        resolve_dash_track(&document, adaptation, representation, manifest_url, true)
    }).transpose()?;

    if audio.is_some() && !ffmpeg_available().await {
        return Err(anyhow!("This DASH MPD has separate audio and video tracks. Install FFmpeg or set THUNDERLOAD_FFMPEG to its executable path, then retry."));
    }

    let total_segments = video.segment_urls.len() + usize::from(video.initialization_url.is_some())
        + audio.as_ref().map(|track| track.segment_urls.len() + usize::from(track.initialization_url.is_some())).unwrap_or(0);
    if total_segments == 0 { return Err(anyhow!("DASH MPD did not produce media segments")); }

    let out_dir = default_download_dir();
    fs::create_dir_all(&out_dir).await?;
    let extension = if audio.is_some() { "mp4" }
        else if video.mime_type.to_ascii_lowercase().contains("webm") { "webm" }
        else { "mp4" };
    let safe = safe_filename(filename, "video");
    let stem = Path::new(&safe).file_stem().and_then(|value| value.to_str()).unwrap_or("video");
    let target = unique_video_path(&out_dir, stem, extension).await?;
    let work_dir = out_dir.join(format!(".dash-{}", Uuid::new_v4()));
    let download_result = async {
        fs::create_dir_all(&work_dir).await?;
        let client = client()?;
        let completed = Arc::new(AtomicUsize::new(0));
        let downloaded_bytes = Arc::new(AtomicU64::new(0));
        let video_file = download_dash_track(
            &client, &video, &work_dir, "video", referer, &progress_tx,
            completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone()
        ).await?;

        let output_file = if let Some(audio) = &audio {
            let audio_file = download_dash_track(
                &client, audio, &work_dir, "audio", referer, &progress_tx,
                completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone()
            ).await?;
            let muxed = work_dir.join("muxed.mp4");
            let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
            let result = TokioCommand::new(executable)
                .arg("-hide_banner").arg("-loglevel").arg("error").arg("-y")
                .arg("-i").arg(&video_file)
                .arg("-i").arg(&audio_file)
                .arg("-map").arg("0:v:0").arg("-map").arg("1:a:0")
                .arg("-c").arg("copy")
                .arg("-movflags").arg("+faststart")
                .arg("-shortest")
                .arg(&muxed)
                .output().await.context("Could not launch FFmpeg to combine DASH audio and video")?;
            if !result.status.success() {
                let stderr = String::from_utf8_lossy(&result.stderr);
                return Err(anyhow!("FFmpeg could not mux the DASH tracks: {}", stderr.trim()));
            }
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
        expand_dash_template, is_complete_vod_playlist, parse_dash_document,
        parse_dash_representations, parse_hls_master, resolve_dash_track,
        unsupported_hls_master_reason, unsupported_hls_reason,
    };
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
        assert_eq!(document.period_count, 1);
        let variants = parse_dash_representations(manifest, manifest_url);
        assert_eq!(variants.len(), 2);
        assert!(variants[0].downloadable);
        assert_eq!(variants[0].representation_id.as_deref(), Some("v720"));
        let (adaptation, representation) = document.adaptations[0].representations.iter()
            .find(|representation| representation.id == "v720")
            .map(|representation| (&document.adaptations[0], representation)).unwrap();
        let track = resolve_dash_track(&document, adaptation, representation, manifest_url, false).unwrap();
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
        let adaptation = &document.adaptations[0];
        let representation = &adaptation.representations[0];
        let track = resolve_dash_track(&document, adaptation, representation, manifest_url, false).unwrap();
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

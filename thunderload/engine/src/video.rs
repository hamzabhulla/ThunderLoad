use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicUsize, Ordering}, Arc}};
use tokio::{fs, io::AsyncWriteExt, process::Command as TokioCommand, sync::mpsc};
use quick_xml::{events::Event, Reader};
use futures_util::StreamExt;
use std::collections::HashMap;
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
                    url: resolved, downloadable: true, note: Some("HLS variant".into()),
                    representation_id: None, audio_representation_id: None, audio_only: None });
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
            return Err(anyhow!("Unclosed DASH template token"));
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
            return Err(anyhow!("Unsupported DASH template token $"+token+"$"));
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
    total_segments: usize,
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
            }
            file.flush().await?;
            if bytes == 0 { return Err(anyhow!("DASH server returned an empty initialization/segment file")); }
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = tx.send(DownloadProgress {
                downloaded: done as u64,
                total: Some(total_segments as u64),
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
        let video_file = download_dash_track(
            &client, &video, &work_dir, "video", referer, &progress_tx,
            completed.clone(), total_segments
        ).await?;

        let output_file = if let Some(audio) = &audio {
            let audio_file = download_dash_track(
                &client, audio, &work_dir, "audio", referer, &progress_tx,
                completed.clone(), total_segments
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
            downloaded: total_segments as u64,
            total: Some(total_segments as u64),
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
    fn hls_session_encryption_and_separate_audio_are_not_offered_as_downloadable() {
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI=\"key.bin\"").is_some());
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"audio.m3u8\"").is_some());
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"100@0\"\n#EXT-X-ENDLIST").is_some());
    }
}
).map(|index| start + 1 + index) else {
            // A lone dollar is literal text; only a paired "$...$" starts a token.
            output.push('
        let token = &template[start + 1..close];
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
            return Err(anyhow!("Unsupported DASH template token $" .to_string() + token + "$"));
        };
        output.push_str(&value);
        cursor = close + 1;
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
    total_segments: usize,
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
            }
            file.flush().await?;
            if bytes == 0 { return Err(anyhow!("DASH server returned an empty initialization/segment file")); }
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = tx.send(DownloadProgress {
                downloaded: done as u64,
                total: Some(total_segments as u64),
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
        let video_file = download_dash_track(
            &client, &video, &work_dir, "video", referer, &progress_tx,
            completed.clone(), total_segments
        ).await?;

        let output_file = if let Some(audio) = &audio {
            let audio_file = download_dash_track(
                &client, audio, &work_dir, "audio", referer, &progress_tx,
                completed.clone(), total_segments
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
            downloaded: total_segments as u64,
            total: Some(total_segments as u64),
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
        assert!(track.initialization_url.unwrap().ends_with("init-main.mp4"));
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
    fn hls_session_encryption_and_separate_audio_are_not_offered_as_downloadable() {
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI=\"key.bin\"").is_some());
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"audio.m3u8\"").is_some());
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"100@0\"\n#EXT-X-ENDLIST").is_some());
    }
}
);
            cursor = start + 1;
            continue;
        };
        let token = &template[start + 1..close];
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
            return Err(anyhow!("Unsupported DASH template token $" .to_string() + token + "$"));
        };
        output.push_str(&value);
        cursor = close + 1;
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
    total_segments: usize,
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
            }
            file.flush().await?;
            if bytes == 0 { return Err(anyhow!("DASH server returned an empty initialization/segment file")); }
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = tx.send(DownloadProgress {
                downloaded: done as u64,
                total: Some(total_segments as u64),
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
        let video_file = download_dash_track(
            &client, &video, &work_dir, "video", referer, &progress_tx,
            completed.clone(), total_segments
        ).await?;

        let output_file = if let Some(audio) = &audio {
            let audio_file = download_dash_track(
                &client, audio, &work_dir, "audio", referer, &progress_tx,
                completed.clone(), total_segments
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
            downloaded: total_segments as u64,
            total: Some(total_segments as u64),
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
        assert!(track.initialization_url.unwrap().ends_with("init-main.mp4"));
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
    fn hls_session_encryption_and_separate_audio_are_not_offered_as_downloadable() {
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI=\"key.bin\"").is_some());
        assert!(unsupported_hls_master_reason("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"audio.m3u8\"").is_some());
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"100@0\"\n#EXT-X-ENDLIST").is_some());
    }
}

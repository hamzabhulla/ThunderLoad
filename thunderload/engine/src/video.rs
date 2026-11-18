use anyhow::{anyhow, Context, Result};
use crate::session::{apply_session, auth_failure_label, map_auth_error, SessionHeaders};
use aes::Aes128;
use cbc::{cipher::{generic_array::{typenum::U16, GenericArray}, BlockDecryptMut, KeyIvInit}, Decryptor};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{path::{Path, PathBuf}, sync::{atomic::{AtomicU64, AtomicUsize, Ordering}, Arc}, time::Duration};
use tokio::{fs, io::AsyncWriteExt, process::Command as TokioCommand, sync::{mpsc, watch}};
use quick_xml::{events::Event, Reader};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use url::Url;
use crate::download::{default_download_dir, partial_download_metadata_path, safe_filename, DownloadControl, DownloadOutcome, DownloadProgress, GlobalRateLimiter, DEFAULT_RESUMABLE_CONNECTIONS, WEBPAGE_NOT_MEDIA_MESSAGE};

/// How many times one download re-resolves its playlist/manifest to
/// refresh expired signed URLs before giving up. Two refreshes cover a
/// playlist that expired mid-download and one unlucky retry; more would
/// just hammer a server that will never serve the URLs again.
const MAX_SEGMENT_URL_REFRESHES: usize = 2;

/// True when `error` is an HTTP 401/403 — the signature of an expired
/// signed URL (or a session the server no longer accepts). Segment
/// fetchers use it to decide whether re-resolving the playlist is worth
/// trying; every other failure keeps the checkpoint and fails as today.
fn is_auth_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<reqwest::Error>()
        .and_then(|error| error.status())
        .is_some_and(|status| status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN)
}

/// The identity of a media URL for plan comparison: the URL with its
/// query string and fragment stripped. Signed segment URLs rotate their
/// query (the signature) on every manifest fetch while the media itself
/// stays the same file — comparing full URLs would treat every refresh
/// as a different playlist and restart the download from scratch. The
/// path (plus the byte range, compared alongside) is what identifies a
/// segment; a path change is a genuinely different plan.
fn url_identity(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => url.to_string(),
    }
}

/// One subtitle track as the source carries it: the language tag, the
/// display name, the URL, and the container — "vtt" for a direct
/// WebVTT file, "m3u8" for an HLS subtitle media playlist whose
/// segments are assembled into one .vtt, "ttml" for a DASH TTML file.
/// Never invented: a track appears here only when the playlist or the
/// yt-dlp info dict actually lists it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubtitleVariant {
    pub language: String,
    pub name: String,
    pub url: String,
    pub format: String,
}

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
    /// True when the playlist is a live/event stream (HLS without
    /// #EXT-X-ENDLIST, dynamic DASH, yt-dlp is_live). The UI shows a
    /// "Live" label from this flag, never from a guessed quality.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_live: Option<bool>,
    /// Frame rate as the source carries it (HLS FRAME-RATE, DASH
    /// @frameRate, yt-dlp fps). Shown only when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fps: Option<f64>,
    /// Subtitle tracks listed by this stream's playlist (HLS
    /// #EXT-X-MEDIA TYPE=SUBTITLES, DASH text AdaptationSets, yt-dlp
    /// subtitles dict). Absent means the source lists none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitles: Option<Vec<SubtitleVariant>>,
}

fn client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent("Mozilla/5.0 ThunderLoad/0.1")
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?)
}

pub async fn inspect_video(url: &str, session: Option<&SessionHeaders>) -> Result<Vec<VideoVariant>> {
    let parsed = Url::parse(url).context("Invalid media URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS media URLs can be inspected"));
    }
    let lower = parsed.path().to_ascii_lowercase();

    if lower.ends_with(".m3u8") {
        let body = fetch_text(url, session).await?;
        let variants = parse_hls_master(&body, &parsed)?;
        if !variants.is_empty() {
            return Ok(finish_hls_master(variants, &body, &parsed, session).await);
        }
        return Ok(vec![hls_single_variant(url, &body)]);
    }
    if lower.ends_with(".mpd") {
        let body = fetch_text(url, session).await?;
        let mut reps = parse_dash_representations(&body, url);
        annotate_dash_variants(&mut reps).await;
        if reps.is_empty() {
            return Ok(vec![VideoVariant {
                kind: "dash".into(), quality: "Adaptive DASH".into(), width: None,
                height: None, bandwidth: None, url: url.into(), downloadable: false,
                note: Some("DASH manifest detected, but no supported static segment layout was found".into()),
                representation_id: None, audio_representation_id: None, audio_only: None, requires_ffmpeg: None,
                is_live: None, fps: None, subtitles: None,
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
    let request = apply_session(session, client.head(url), url);
    // A number of CDNs block HEAD even when normal downloads are allowed.
    // Retry the metadata probe with a one-byte range GET before rejecting the URL.
    let response = match request.send().await {
        Ok(response) if response.status().is_success() => response,
        _ => {
            let probe = apply_session(session, client.get(url).header(reqwest::header::RANGE, "bytes=0-0"), url);
            let probed = probe.send().await
                .context("Could not inspect the URL response headers with HEAD or a one-byte GET")?
                .error_for_status()
                .context("The server rejected both media inspection requests");
            // A 401/403 from the probe is the login wall, not a
            // generic inspection failure — show the label alone
            // (Slice 5); anything else keeps its context.
            match probed {
                Ok(response) => response,
                Err(error) => match auth_failure_label(&error) {
                    Some(label) => return Err(anyhow::Error::msg(label)),
                    None => return Err(error),
                },
            }
        }
    };
    let content_type = response.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';').next().unwrap_or("").trim().to_ascii_lowercase();

    if content_type.contains("mpegurl") || content_type.contains("vnd.apple.mpegurl") {
        let body = fetch_text(url, session).await?;
        let variants = parse_hls_master(&body, &parsed)?;
        if !variants.is_empty() {
            return Ok(finish_hls_master(variants, &body, &parsed, session).await);
        }
        return Ok(vec![hls_single_variant(url, &body)]);
    }
    if content_type.contains("dash+xml") {
        let body = fetch_text(url, session).await?;
        let mut variants = parse_dash_representations(&body, url);
        annotate_dash_variants(&mut variants).await;
        return Ok(variants);
    }
    if content_type.starts_with("video/") || content_type.starts_with("audio/")
        || content_type == "application/octet-stream" || content_type == "application/zip"
        || content_type == "application/pdf" {
        return Ok(vec![direct_variant(url)]);
    }

    Err(anyhow!(WEBPAGE_NOT_MEDIA_MESSAGE))
}

async fn fetch_text(url: &str, session: Option<&SessionHeaders>) -> Result<String> {
    fetch_text_with(&client()?, url, session).await
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

/// The encryption half of unsupported_hls_reason, split out so live
/// handling can run first: a live playlist is not an error, it is a
/// different kind of stream — but an encrypted live playlist is still
/// protected content and stays refused.
fn hls_encryption_reason(body: &str) -> Option<String> {
    for line in body.lines().map(str::trim) {
        if let Some(attributes) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attr(attributes, "METHOD").unwrap_or_default();
            // AES-128 is downloadable (Slice 4: decrypted in-engine, key
            // fetched with the same session as the playlist). Anything
            // else — SAMPLE-AES, or a method this code has never seen —
            // is protected content and stays refused, never attempted.
            if !method.eq_ignore_ascii_case("NONE") && !method.eq_ignore_ascii_case("AES-128") {
                return Some("Encrypted HLS stream is not supported".into());
            }
        }
    }
    None
}

fn unsupported_hls_reason(body: &str) -> Option<String> {
    if let Some(reason) = hls_encryption_reason(body) {
        return Some(reason);
    }
    if !is_complete_vod_playlist(body) {
        return Some("This is a live or unfinished HLS playlist. Only completed VOD playlists can be downloaded".into());
    }
    None
}

/// Shared tail of both HLS master inspect paths (by extension and by
/// Content-Type): per-variant annotation (live + encryption), the
/// separate-audio FFmpeg gate, then the audio-only row. Subtitles ride
/// on the variants already (parse_hls_master attaches them).
async fn finish_hls_master(
    variants: Vec<VideoVariant>,
    body: &str,
    base: &Url,
    session: Option<&SessionHeaders>,
) -> Vec<VideoVariant> {
    let mut variants = variants;
    // The FFmpeg gate runs first so a VOD variant with separate audio
    // stays gated; annotate_hls_variants then overrides it for LIVE
    // variants — live recording appends the video track's raw TS
    // segments and never muxes, so no FFmpeg is needed there, and the
    // audio-only row below is still offered separately.
    annotate_hls_audio(&mut variants).await;
    annotate_hls_variants(&mut variants, session).await;
    if let Some(reason) = unsupported_hls_master_reason(body) {
        for variant in &mut variants {
            variant.downloadable = false;
            variant.note = Some(reason.clone());
        }
    }
    // The audio-only row sorts last (no height) and never disturbs
    // the best-first order of the video rows.
    if let Some(audio_row) = hls_audio_only_variant(body, base) {
        variants.push(audio_row);
    }
    variants
}

/// The one row for a bare HLS media playlist (no master): the playlist
/// itself. Live playlists are labelled Live — MPEG-TS records as it
/// streams, fMP4 (EXT-X-MAP present) refuses honestly — and anything
/// else keeps the old unsupported reason.
fn hls_single_variant(url: &str, body: &str) -> VideoVariant {
    let mut variant = VideoVariant {
        kind: "hls".into(), quality: "Adaptive playlist".into(), width: None, height: None,
        bandwidth: None, url: url.to_string(), downloadable: true,
        note: Some("Unencrypted HLS playlist".into()),
        representation_id: None, audio_representation_id: None, audio_only: None, requires_ffmpeg: None,
        is_live: None, fps: None, subtitles: None,
    };
    if let Some(reason) = hls_encryption_reason(body) {
        variant.downloadable = false;
        variant.note = Some(reason);
        return variant;
    }
    if !is_complete_vod_playlist(body) {
        variant.is_live = Some(true);
        variant.quality = "Live".into();
        if body.contains("#EXT-X-MAP:") {
            variant.downloadable = false;
            variant.note = Some("Live stream in this format can't be recorded yet.".into());
        } else {
            variant.note = Some("Live — records the stream as it arrives (.ts file). Stop the download to keep what was recorded.".into());
        }
    }
    variant
}

async fn annotate_hls_variants(variants: &mut [VideoVariant], session: Option<&SessionHeaders>) {
    for variant in variants.iter_mut().filter(|item| item.kind == "hls") {
        match fetch_text(&variant.url, session).await {
            Ok(body) => {
                // Encryption first: an encrypted live playlist is still
                // protected content, never a recording candidate.
                if let Some(reason) = hls_encryption_reason(&body) {
                    variant.downloadable = false;
                    variant.note = Some(reason);
                    continue;
                }
                // Live second: a live media playlist is not an error.
                // MPEG-TS records as it streams; fMP4 (EXT-X-MAP)
                // cannot be appended to a playable file, so it refuses
                // honestly instead of faking a download.
                if !is_complete_vod_playlist(&body) {
                    variant.is_live = Some(true);
                    if body.contains("#EXT-X-MAP:") {
                        variant.downloadable = false;
                        variant.note = Some("Live stream in this format can't be recorded yet.".into());
                    } else {
                        variant.downloadable = true;
                        variant.note = Some("Live — records the stream as it arrives (.ts file). Stop the download to keep what was recorded.".into());
                    }
                    continue;
                }
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
        is_live: None, fps: None, subtitles: None,
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
    let mut pending: Option<(Option<u32>, Option<u32>, Option<u64>, Option<String>, Option<f64>)> = None;
    for line in body.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(attrs) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            let resolution = attr(attrs, "RESOLUTION").and_then(|s| {
                let (w, h) = s.split_once('x')?;
                Some((w.parse().ok()?, h.parse().ok()?))
            });
            let bandwidth = attr(attrs, "AVERAGE-BANDWIDTH").or_else(|| attr(attrs, "BANDWIDTH"))
                .and_then(|s| s.parse().ok());
            // FRAME-RATE is the source's own frame rate — carried as
            // fps, shown only when present, never guessed.
            let fps = attr(attrs, "FRAME-RATE").and_then(|s| s.parse().ok());
            pending = Some((resolution.map(|r| r.0), resolution.map(|r| r.1), bandwidth, attr(attrs, "AUDIO"), fps));
        } else if !line.starts_with('#') {
            if let Some((width, height, bandwidth, audio_group, fps)) = pending.take() {
                let resolved = base.join(line)?.to_string();
                let quality = height.map(|h| format!("{h}p")).unwrap_or_else(|| "Adaptive".into());
                out.push(VideoVariant { kind: "hls".into(), quality, width, height, bandwidth, fps,
                    url: resolved, downloadable: true, note: Some("HLS variant".into()),
                    representation_id: None, audio_representation_id: audio_group, audio_only: None, requires_ffmpeg: None,
                    is_live: None, subtitles: None });
            }
        }
    }
    out.sort_by(|a, b| b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)));
    // Subtitle renditions are master-level, not per-variant: the same
    // list rides on every variant so the panel can read it from the
    // first row. Absent when the master lists none.
    let subtitles = parse_hls_subtitle_renditions(body, base);
    if !subtitles.is_empty() {
        for variant in &mut out {
            variant.subtitles = Some(subtitles.clone());
        }
    }
    Ok(out)
}

fn parse_hls_subtitle_renditions(body: &str, base: &Url) -> Vec<SubtitleVariant> {
    let mut out = Vec::new();
    for line in body.lines().map(str::trim) {
        let Some(attributes) = line.strip_prefix("#EXT-X-MEDIA:") else { continue; };
        if !attr(attributes, "TYPE").is_some_and(|kind| kind.eq_ignore_ascii_case("SUBTITLES")) { continue; }
        let Some(uri) = attr(attributes, "URI") else { continue; };
        let Ok(resolved) = base.join(&uri) else { continue; };
        let language = attr(attributes, "LANGUAGE").unwrap_or_else(|| "und".into());
        let name = attr(attributes, "NAME").unwrap_or_else(|| language.clone());
        out.push(SubtitleVariant {
            language,
            name,
            url: resolved.to_string(),
            // An HLS subtitle rendition is a media playlist of WebVTT
            // segments; the downloader assembles it into one .vtt.
            format: "m3u8".into(),
        });
    }
    out
}

/// The audio-only row for an HLS master: the DEFAULT audio rendition
/// (else the first), as its own downloadable variant. Its URL is the
/// audio media playlist itself, so the normal HLS download path treats
/// it as a bare media playlist — no video track is ever mixed in.
/// The GROUP-ID rides as representation_id for identification only;
/// the download path keys off the URL.
fn hls_audio_only_variant(body: &str, base: &Url) -> Option<VideoVariant> {
    let renditions = parse_hls_audio_renditions(body, base);
    let rendition = renditions.iter().find(|rendition| rendition.is_default)
        .or_else(|| renditions.first())?;
    Some(VideoVariant {
        kind: "hls".into(), quality: "Audio".into(), width: None, height: None,
        bandwidth: None, url: rendition.uri.clone(), downloadable: true,
        note: Some("Audio-only track".into()),
        representation_id: Some(rendition.group_id.clone()),
        audio_representation_id: None, audio_only: Some(true), requires_ffmpeg: None,
        is_live: None, fps: None, subtitles: None,
    })
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
    split_attr_list(input).into_iter().find_map(|part| {
        let part = part.trim();
        let value = part.strip_prefix(&prefix)?;
        Some(value.trim_matches('"').to_string())
    })
}

/// Splits an HLS attribute list on commas that sit outside double
/// quotes, so `NAME="English, US"` stays one attribute (Slice 7a,
/// Gemini 15c: the old `input.split(',')` cut it in half and the name
/// came back as just `English`). A comma inside a quoted value is data,
/// not a separator.
fn split_attr_list(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (index, character) in input.char_indices() {
        match character {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                parts.push(&input[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&input[start..]);
    parts
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

/// Parses an ISO 8601 duration (`PT5M`, `PT1H30M`, `P1D`, `P0DT0H5M0S`,
/// `P1DT2H`) into seconds. The day component was rejected before (Slice
/// 7a, Gemini 15b: `strip_prefix("PT")` dropped any `P[n]D...` value),
/// which silently dropped the duration of any MPD that uses it. Garbage
/// is still rejected: no `P` prefix, no component at all, an unknown
/// designator, a time component without `T`, or a non-positive total.
fn parse_iso_duration(value: &str) -> Option<f64> {
    let rest = value.strip_prefix('P')?;
    let (days_part, time_part) = match rest.split_once('T') {
        Some((days, time)) => (days, Some(time)),
        None => (rest, None),
    };
    let mut seconds = 0.0;
    if !days_part.is_empty() {
        let days: f64 = days_part.strip_suffix('D')?.parse().ok()?;
        seconds += days * 86400.0;
    }
    if let Some(time) = time_part {
        let mut number = String::new();
        let mut saw_component = false;
        for character in time.chars() {
            if character.is_ascii_digit() || character == '.' {
                number.push(character);
                continue;
            }
            let amount: f64 = number.parse().ok()?;
            number.clear();
            saw_component = true;
            seconds += match character {
                'H' => amount * 3600.0,
                'M' => amount * 60.0,
                'S' => amount,
                _ => return None,
            };
        }
        if !number.is_empty() || !saw_component {
            return None;
        }
    } else if days_part.is_empty() {
        // Bare "P" — no day component and no time part.
        return None;
    }
    if !seconds.is_finite() || seconds <= 0.0 {
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

fn is_text_adaptation(adaptation: &DashAdaptation) -> bool {
    adaptation.content_type.eq_ignore_ascii_case("text")
        || adaptation.mime_type.to_ascii_lowercase().starts_with("text/")
        || adaptation.mime_type.eq_ignore_ascii_case("application/ttml+xml")
        || adaptation.representations.iter().any(|rep| {
            rep.mime_type.to_ascii_lowercase().starts_with("text/")
                || rep.mime_type.eq_ignore_ascii_case("application/ttml+xml")
        })
}

/// Subtitle variants from a static DASH manifest's text AdaptationSets.
/// Only single-file subtitles are offered: a representation whose URL
/// resolves to one file (BaseURL, no SegmentTemplate/SegmentList/
/// SegmentBase anywhere in scope). Template-based subtitle segments are
/// skipped — no row is faked for them. The format comes from the mime
/// type or the file extension; a representation whose format cannot be
/// identified is skipped. The language is the representation id — the
/// parser carries no lang attribute — so it is what the manifest
/// itself calls the track.
fn dash_subtitle_variants(document: &DashDocument, manifest_url: &str) -> Vec<SubtitleVariant> {
    let Some(first_period) = document.periods.first() else { return Vec::new(); };
    let mut out = Vec::new();
    for adaptation in &first_period.adaptations {
        if !is_text_adaptation(adaptation) { continue; }
        for representation in &adaptation.representations {
            let templated = representation.template.is_some() || adaptation.template.is_some()
                || first_period.template.is_some() || document.template.is_some()
                || representation.segment_list.is_some() || adaptation.segment_list.is_some()
                || first_period.segment_list.is_some() || document.segment_list.is_some()
                || representation.segment_base.is_some() || adaptation.segment_base.is_some()
                || first_period.segment_base.is_some() || document.segment_base.is_some();
            if templated { continue; }
            // merge_dash_base_url falls back to the manifest URL itself
            // when no BaseURL exists anywhere — a text representation
            // with no BaseURL has no file to download, so skip it
            // rather than offering the .mpd as a subtitle.
            let has_base = representation.base_url.is_some() || adaptation.base_url.is_some()
                || first_period.base_url.is_some() || document.base_url.is_some();
            if !has_base { continue; }
            let Ok(base) = merge_dash_base_url(manifest_url, document, first_period, adaptation, representation) else { continue; };
            let url = base.to_string();
            let mime = effective_dash_mime_type(adaptation, representation).to_ascii_lowercase();
            let format = if mime.contains("vtt") || url.to_ascii_lowercase().ends_with(".vtt") {
                "vtt"
            } else if mime.contains("ttml") || url.to_ascii_lowercase().ends_with(".ttml") {
                "ttml"
            } else {
                continue;
            };
            out.push(SubtitleVariant {
                language: representation.id.clone(),
                name: representation.id.clone(),
                url,
                format: format.into(),
            });
        }
    }
    out
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
            // Live DASH is labelled Live and honestly refused: the
            // engine has no live DASH recorder in this slice (HLS live
            // recording exists; DASH would need a segment-timeline
            // follower that does not exist yet). The row is a label,
            // not a fake download.
            "Live DASH streams can't be recorded yet."
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
                            quality: if !document.is_static { "Live".into() } else { representation.height.map(|height| format!("{height}p")).unwrap_or_else(|| representation.id.clone()) },
                            width: representation.width,
                            height: representation.height,
                            bandwidth: representation.bandwidth,
                            fps: representation.frame_rate.or(adaptation.frame_rate),
                            url: manifest_url.to_string(),
                            downloadable: false,
                            note: Some(note.to_string()),
                            representation_id: Some(representation.id.clone()),
                            audio_representation_id: None,
                            audio_only: Some(false),
                            requires_ffmpeg: None,
                            is_live: if !document.is_static { Some(true) } else { None },
                            subtitles: None,
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
                fps: representation.frame_rate.or(adaptation.frame_rate),
                url: manifest_url.to_string(),
                downloadable,
                note,
                representation_id: Some(representation.id.clone()),
                audio_representation_id: audio_representation.map(|(_, _, audio)| audio.id.clone()),
                audio_only: Some(false),
                requires_ffmpeg: if multi_period { Some(true) } else { None },
                is_live: None,
                subtitles: {
                    let subtitles = dash_subtitle_variants(&document, manifest_url);
                    if subtitles.is_empty() { None } else { Some(subtitles) }
                },
            });
        }
    }
    out.sort_by(|left, right| right.height.unwrap_or(0).cmp(&left.height.unwrap_or(0)));
    out
}

/// Build a child-process command that never shows a console window on
/// Windows (CREATE_NO_WINDOW, 0x08000000). The engine runs inside the
/// windowless desktop app and native host, so a console-subsystem child
/// (ffmpeg/ffprobe) would otherwise flash its own black window on every
/// spawn — including every DASH/HLS mux. On other platforms this is just
/// `TokioCommand::new`. tokio's Command exposes no creation flag, so the
/// flag is set on a std command and converted
/// (From<std::process::Command> for tokio::process::Command).
fn hidden_command<S: AsRef<std::ffi::OsStr>>(program: S) -> TokioCommand {
    #[cfg(target_os = "windows")]
    let std_command = {
        use std::os::windows::process::CommandExt;
        let mut std_command = std::process::Command::new(program);
        std_command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        std_command
    };
    #[cfg(not(target_os = "windows"))]
    let std_command = std::process::Command::new(program);
    TokioCommand::from(std_command)
}

pub(crate) async fn ffmpeg_available() -> bool {
    let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    hidden_command(executable).arg("-version").output().await
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
///
/// Equality is the segment's IDENTITY — its URL with the query stripped
/// plus its byte range — not its encryption key. The key is fetched fresh
/// per segment at download time, and a finished segment is already
/// plaintext on disk, so a key-URI rotation (or a re-key) must not make
/// a checkpoint look like a different job: the media is the same, the
/// old plaintext is still correct, and only the query (the signature)
/// changed. See `url_identity`.
#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
struct HlsSegmentRef {
    url: String,
    /// (start offset, length) when the segment is an EXT-X-BYTERANGE slice.
    range: Option<(u64, u64)>,
    /// AES-128 encryption for this segment, if any (Slice 4). The key is
    /// fetched at download time and never persisted — only its URI and
    /// IV are stored, so a resume re-fetches the key fresh.
    #[serde(default)]
    key: Option<HlsKeyRef>,
}

impl PartialEq for HlsSegmentRef {
    fn eq(&self, other: &Self) -> bool {
        url_identity(&self.url) == url_identity(&other.url) && self.range == other.range
    }
}

/// The AES-128 key for one HLS segment, as declared by `#EXT-X-KEY`:
/// the key URI (resolved against the playlist) and the IV. `iv` is None
/// when the playlist carries no explicit IV — then the IV is that
/// segment's media sequence number as a 16-byte big-endian integer
/// (RFC 8216 §4.3.2.4), resolved per segment at parse time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct HlsKeyRef {
    uri: String,
    iv: Option<[u8; 16]>,
}

/// What the user picked from an HLS master playlist (Slice 4, Gemini
/// claim 4). HLS variants carry no representation id of their own, so
/// the variant's identity is its media playlist URL, and the audio
/// rendition's identity is its GROUP-ID — the same two values the
/// `VideoVariant` already carries as `url` and `audio_representation_id`.
/// Both None means "the first (highest) variant", the old behaviour:
/// callers that only know a bare media playlist pass the default.
#[derive(Debug, Clone, Default)]
pub struct HlsSelection {
    /// Media playlist URL of the chosen variant, if the caller picked one
    /// from a master playlist.
    pub media_url: Option<String>,
    /// GROUP-ID of the chosen audio rendition, if the variant has one.
    pub audio_group: Option<String>,
}

impl HlsSelection {
    /// No explicit choice: first variant, its own audio group if any.
    pub fn none() -> Self {
        Self::default()
    }
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
    #[serde(default)]
    session: Option<SessionHeaders>,
    extension: String,
    video_init: Option<HlsSegmentRef>,
    video_segments: Vec<HlsSegmentRef>,
    video_init_done: bool,
    video_done: Vec<usize>,
    audio_init: Option<HlsSegmentRef>,
    audio_segments: Vec<HlsSegmentRef>,
    audio_init_done: bool,
    audio_done: Vec<usize>,
    /// The user's selection from the master playlist (Slice 4): the
    /// chosen variant's media playlist URL and audio GROUP-ID. Both None
    /// on checkpoints written before Slice 4 (`#[serde(default)]`) and
    /// on bare media playlists, which have no master to select from.
    /// A different selection is a different job: its segment list and
    /// output file differ, so it must not reuse this checkpoint.
    #[serde(default)]
    selected_media_url: Option<String>,
    #[serde(default)]
    selected_audio_group: Option<String>,
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
    /// Segment identity is URL-minus-query plus byte range (see
    /// `url_identity`): a signed-URL rotation must not look like a
    /// different job, or a resume after a refresh would restart.
    fn matches_plan(&self, plan: &HlsJobState) -> bool {
        self.version == plan.version
            && self.playlist_url == plan.playlist_url
            && self.media_url == plan.media_url
            && self.audio_playlist_url == plan.audio_playlist_url
            && self.selected_media_url == plan.selected_media_url
            && self.selected_audio_group == plan.selected_audio_group
            && self.session.as_ref().map(|session| session.credential_identity())
                == plan.session.as_ref().map(|session| session.credential_identity())
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
    /// The playlist's #EXT-X-MEDIA-SEQUENCE (default 0): the sequence
    /// number of segments[0]. The live recorder (Slice 6) uses it to
    /// skip segments it has already recorded; the VOD path ignores it.
    media_sequence: u64,
}

/// Parses one HLS media playlist: segment URIs, the EXT-X-MAP init segment
/// and EXT-X-BYTERANGE slices, and `#EXT-X-KEY` AES-128 encryption (Slice
/// 4). The key URI and IV ride on each segment as an `HlsKeyRef`; the
/// key itself is fetched at download time, never here. A playlist without
/// EXT-X-ENDLIST is a live/unfinished playlist and is rejected; a
/// `#EXT-X-KEY` with METHOD=SAMPLE-AES (or any other method that is not
/// NONE or AES-128) is protected content and is rejected, never attempted.
/// (Slice 6) `parse_hls_live_media_playlist` is the same parser with the
/// ENDLIST requirement lifted, for the live recorder.
fn parse_hls_media_playlist(body: &str, base: &Url) -> Result<HlsMediaPlaylist> {
    parse_hls_media_playlist_inner(body, base, false)
}

fn parse_hls_live_media_playlist(body: &str, base: &Url) -> Result<HlsMediaPlaylist> {
    parse_hls_media_playlist_inner(body, base, true)
}

fn parse_hls_media_playlist_inner(body: &str, base: &Url, live: bool) -> Result<HlsMediaPlaylist> {
    if !live && !is_complete_vod_playlist(body) {
        return Err(anyhow!("This is a live or unfinished HLS playlist. Only completed VOD playlists can be downloaded as a normal download; live streams are recorded instead."));
    }
    let mut init = None;
    let mut segments = Vec::new();
    // An EXT-X-BYTERANGE without an explicit offset continues at the end
    // of the previous range for the same URI (RFC 8216 §4.3.2.2).
    let mut last_end_by_uri: HashMap<String, u64> = HashMap::new();
    let mut pending_range: Option<(u64, Option<u64>)> = None;
    // `#EXT-X-MEDIA-SEQUENCE` (default 0): the first segment's sequence
    // number. With no explicit IV, the IV of a segment IS its media
    // sequence number as a 16-byte big-endian integer (RFC 8216
    // §4.3.2.4), so the parser — the only place that knows the numbering
    // — resolves the default IV here, per segment.
    let mut media_sequence: u64 = 0;
    // The `#EXT-X-KEY` in force for the next segment, if any. A KEY with
    // METHOD=NONE clears it; a KEY with METHOD=AES-128 sets it.
    let mut current_key: Option<HlsKeyRef> = None;
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
            // The init segment is container metadata, never encrypted:
            // the KEY in force applies to media segments only.
            init = uri.map(|url| HlsSegmentRef { url, range, key: None });
        } else if let Some(attributes) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            media_sequence = attributes.trim().parse::<u64>()
                .context("Invalid EXT-X-MEDIA-SEQUENCE")?;
        } else if let Some(key) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attr(key, "METHOD").unwrap_or_default();
            if method.eq_ignore_ascii_case("NONE") {
                current_key = None;
            } else if method.eq_ignore_ascii_case("AES-128") {
                let uri = attr(key, "URI").context("EXT-X-KEY with METHOD=AES-128 has no URI")?;
                let uri = base.join(&uri).context("EXT-X-KEY URI is not a valid URL")?.to_string();
                // An explicit IV rides along; without one, the IV is the
                // media sequence number of the segment the key applies
                // to, resolved per segment below (None here means that).
                let iv = match attr(key, "IV") {
                    Some(value) => Some(parse_hls_iv(&value)?),
                    None => None,
                };
                current_key = Some(HlsKeyRef { uri, iv });
            } else {
                return Err(anyhow!("This HLS playlist uses {method} encryption, which is protected content and is not supported."));
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
            // A KEY without an explicit IV uses this segment's media
            // sequence number as its IV (RFC 8216 §4.3.2.4): the number
            // is the playlist's MEDIA-SEQUENCE plus this segment's
            // position in the list, as a 16-byte big-endian integer.
            let key = current_key.clone().map(|key_ref| {
                let iv = key_ref.iv.unwrap_or_else(|| {
                    let sequence = media_sequence + segments.len() as u64;
                    let mut iv = [0u8; 16];
                    iv[8..].copy_from_slice(&sequence.to_be_bytes());
                    iv
                });
                HlsKeyRef { uri: key_ref.uri, iv: Some(iv) }
            });
            segments.push(HlsSegmentRef { url, range, key });
        }
    }
    Ok(HlsMediaPlaylist { init, segments, media_sequence })
}

/// Parses the IV attribute of an `#EXT-X-KEY` line: `0x` followed by 32
/// hex digits (16 bytes), per RFC 8216 §4.3.2.4.
fn parse_hls_iv(value: &str) -> Result<[u8; 16]> {
    let hex = value.strip_prefix("0x").or_else(|| value.strip_prefix("0X")).unwrap_or(value);
    if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("EXT-X-KEY IV must be 0x followed by 32 hex digits, got {value}"));
    }
    let mut iv = [0u8; 16];
    for index in 0..16 {
        iv[index] = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .context("EXT-X-KEY IV is not valid hex")?;
    }
    Ok(iv)
}

async fn fetch_text_with(client: &Client, url: &str, session: Option<&SessionHeaders>) -> Result<String> {
    let request = apply_session(session, client.get(url), url);
    // A 401/403 here is an auth failure, not a fetch failure: the
    // manifest fetch is the first request that meets the login wall,
    // so relabel it before it reaches the panel (Slice 5).
    let response = request.send().await.map_err(map_auth_error)?;
    let response = response.error_for_status().map_err(map_auth_error)?;
    Ok(response.text().await?)
}

/// Fetches the playlist (master first, then the chosen media playlist and
/// its audio rendition, if any) and turns it into a download plan.
///
/// `selection` (Slice 4, Gemini claim 4) is what the user picked from
/// the master playlist: the variant's media playlist URL and its audio
/// GROUP-ID. With no selection the first (highest listed) variant is
/// chosen, matching the old behaviour. A selection that names a variant
/// or audio group the master no longer lists is an error, not a silent
/// fall back to a different quality — the user asked for that quality.
/// (Slice 6) A selection that names an audio rendition's playlist
/// instead is the audio-only row: the plan is that playlist alone.
/// When `playlist_url` is a bare media playlist (no master), there is
/// nothing to select from and the selection is ignored. Live playlists,
/// SESSION-KEY encryption and non-AES-128 media encryption are rejected
/// here, before any segment is fetched.
async fn resolve_hls_plan(playlist_url: &str, selection: &HlsSelection, session: Option<&SessionHeaders>) -> Result<HlsJobState> {
    let base = Url::parse(playlist_url)?;
    let client = client()?;
    let body = fetch_text_with(&client, playlist_url, session).await?;
    let variants = parse_hls_master(&body, &base)?;
    let (media_url, audio_playlist_url) = if !variants.is_empty() {
        if let Some(reason) = unsupported_hls_master_reason(&body) {
            return Err(anyhow!(reason));
        }
        let renditions = parse_hls_audio_renditions(&body, &base);
        // The audio-only row selects an audio rendition's media
        // playlist, not a video variant: when the selection names one,
        // the plan is that playlist alone, with no video track and no
        // second audio track. This is how the "Audio" row downloads.
        if let Some(wanted) = selection.media_url.as_deref() {
            if let Some(rendition) = renditions.iter().find(|rendition| rendition.uri == wanted) {
                (rendition.uri.clone(), None)
            } else {
                // The selected variant, or the first (highest) when the caller
                // made no choice. A selection that no longer matches the master
                // is an error: the playlist changed under the user.
                let variant = variants.iter().find(|variant| variant.url == wanted)
                    .ok_or_else(|| anyhow!("The selected HLS quality is no longer listed in the master playlist"))?;
                let media_url = variant.url.clone();
                // The audio rendition for the variant: the explicitly selected
                // GROUP-ID, or the variant's own AUDIO group, or the DEFAULT
                // rendition when the variant names no group. An explicitly
                // selected group that does not exist is an error, like the
                // variant itself; the DEFAULT fall back applies only to the
                // unselected case, matching the old behaviour.
                let audio = match selection.audio_group.as_deref() {
                    Some(wanted) => Some(renditions.iter().find(|rendition| rendition.group_id == wanted)
                        .map(|rendition| rendition.uri.clone())
                        .ok_or_else(|| anyhow!("The selected HLS audio track is no longer listed in the master playlist"))?),
                    None => variant.audio_representation_id.as_deref().and_then(|group| {
                        renditions.iter()
                            .find(|rendition| rendition.group_id == group)
                            .or_else(|| renditions.iter().find(|rendition| rendition.is_default))
                            .map(|rendition| rendition.uri.clone())
                    }),
                };
                (media_url, audio)
            }
        } else {
            let variant = variants.first().expect("non-empty variant list");
            let media_url = variant.url.clone();
            let audio = variant.audio_representation_id.as_deref().and_then(|group| {
                renditions.iter()
                    .find(|rendition| rendition.group_id == group)
                    .or_else(|| renditions.iter().find(|rendition| rendition.is_default))
                    .map(|rendition| rendition.uri.clone())
            });
            (media_url, audio)
        }
    } else {
        (playlist_url.to_string(), None)
    };
    let media_base = Url::parse(&media_url)?;
    let media_body = if media_url == playlist_url {
        body
    } else {
        fetch_text_with(&client, &media_url, session).await?
    };
    let media = parse_hls_media_playlist(&media_body, &media_base)?;
    if media.segments.is_empty() {
        return Err(anyhow!("No media segments were found in this HLS playlist"));
    }
    let (audio_init, audio_segments) = match &audio_playlist_url {
        Some(url) => {
            let audio_base = Url::parse(url)?;
            let audio_body = fetch_text_with(&client, url, session).await?;
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
        session: session.cloned(),
        extension: if media.init.is_some() { "mp4".into() } else { "ts".into() },
        video_init: media.init,
        video_segments: media.segments,
        video_init_done: false,
        video_done: Vec::new(),
        audio_init,
        audio_segments,
        audio_init_done: false,
        audio_done: Vec::new(),
        selected_media_url: selection.media_url.clone(),
        selected_audio_group: selection.audio_group.clone(),
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
    // Unix rename(2) replaces atomically, so the checkpoint file is
    // never absent; Windows rename fails when the destination exists.
    // (Same gap as the direct-download writer, fixed the same way.)
    #[cfg(not(unix))]
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

/// Windows routinely holds a freshly written file open for a moment
/// (Defender/AV scan, just-closed handle), so a single remove_dir_all
/// can fail with a sharing violation right after a download stops.
/// Retry briefly instead of silently leaving partial files behind.
async fn remove_dir_all_retry(path: &Path) {
    for attempt in 0..5 {
        match fs::remove_dir_all(path).await {
            Ok(()) => return,
            Err(_) if attempt < 4 => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(_) => return,
        }
    }
}

async fn remove_hls_resume_files(target: &Path) {
    remove_dir_all_retry(&hls_work_dir(target)).await;
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

/// Cache of fetched AES-128 keys for one download call, keyed by key
/// URI. Several segments usually share one key; fetching it once per URI
/// keeps parallel tasks from hammering the key endpoint (and from racing
/// a signed key URL's expiry). The lock is held across the fetch itself
/// (see `fetch_hls_key`), so parallel tasks for the same key serialize
/// and only the first one goes to the network. A key stays cached only
/// for this one download call — a resume re-fetches it fresh.
#[derive(Clone, Default)]
struct HlsKeyCache(Arc<tokio::sync::Mutex<HashMap<String, Arc<[u8; 16]>>>>);

impl HlsKeyCache {
    /// Fast path: returns the cached key without waiting, when no fetch
    /// is in flight. Never blocks — `blocking_lock` would panic inside
    /// the async runtime, so a contended lock just falls through to the
    /// async lock in `fetch_hls_key`.
    fn try_get(&self, uri: &str) -> Option<Arc<[u8; 16]>> {
        self.0.try_lock().ok()?.get(uri).cloned()
    }
}

/// Fetches the 16-byte AES-128 key for one segment, with the same session
/// (cookies, Referer, User-Agent) as the playlist — a key URL behind
/// auth fails exactly like a protected segment would. The key is never
/// written to disk and never stored in the checkpoint.
async fn fetch_hls_key(client: &Client, uri: &str, session: Option<&SessionHeaders>, cache: &HlsKeyCache) -> Result<Arc<[u8; 16]>> {
    // Fast path: the key is already cached.
    if let Some(key) = cache.try_get(uri) {
        return Ok(key);
    }
    // The lock is held across the fetch itself, so parallel tasks for the
    // same key serialize on it and only the first one goes to the network.
    // A second check inside the lock covers the task that waited.
    let mut locked = cache.0.lock().await;
    if let Some(key) = locked.get(uri) {
        return Ok(key.clone());
    }
    let bytes = apply_session(session, client.get(uri), uri)
        .send().await
        .with_context(|| format!("Could not fetch the HLS AES-128 key at {uri}"))?
        .error_for_status()
        .with_context(|| format!("The HLS AES-128 key at {uri} was refused"))?
        .bytes().await?;
    if bytes.len() != 16 {
        return Err(anyhow!("The HLS AES-128 key at {uri} is {} bytes, not 16", bytes.len()));
    }
    let mut key = [0u8; 16];
    key.copy_from_slice(&bytes);
    let key = Arc::new(key);
    locked.insert(uri.to_string(), key.clone());
    Ok(key)
}

/// Decrypts one AES-128-CBC encrypted segment file (`raw`, as downloaded)
/// into `out`, streaming so a large segment never sits whole in memory.
/// Every 16-byte block but the last is decrypted as it arrives; the last
/// block is held back, decrypted at EOF, and its PKCS#7 padding is
/// validated and stripped. Returns the plaintext byte count. A file whose
/// length is not a multiple of 16, or whose padding is invalid, is an
/// error — a corrupt segment must fail the download, not silently join
/// garbage into the output.
async fn decrypt_aes128_segment(raw: &Path, out: &Path, key: &[u8; 16], iv: &[u8; 16]) -> Result<u64> {
    use tokio::io::AsyncReadExt;
    let mut decryptor = Decryptor::<Aes128>::new_from_slices(key, iv)
        .map_err(|_| anyhow!("The HLS AES-128 key or IV is not 16 bytes"))?;
    let mut source = fs::File::open(raw).await
        .with_context(|| format!("Could not open the encrypted segment at {}", raw.display()))?;
    let mut destination = fs::File::create(out).await
        .with_context(|| format!("Could not create the decrypted segment at {}", out.display()))?;
    // One block held back at all times: the last block carries the
    // padding, and it can only be recognised once the file ends.
    let mut pending: Vec<u8> = Vec::with_capacity(16);
    let mut plaintext_bytes: u64 = 0;
    let mut chunk = [0u8; 65536];
    loop {
        let read = source.read(&mut chunk).await
            .with_context(|| format!("Could not read the encrypted segment at {}", raw.display()))?;
        if read == 0 {
            break;
        }
        pending.extend_from_slice(&chunk[..read]);
        // Decrypt every complete block except the last 16 bytes.
        let decryptable = pending.len().saturating_sub(16) / 16 * 16;
        for block in pending[..decryptable].chunks_exact_mut(16) {
            let mut generic = GenericArray::<u8, U16>::from_mut_slice(block);
            decryptor.decrypt_block_mut(&mut generic);
        }
        destination.write_all(&pending[..decryptable]).await
            .with_context(|| format!("Could not write the decrypted segment at {}", out.display()))?;
        plaintext_bytes += decryptable as u64;
        pending.drain(..decryptable);
    }
    if pending.len() != 16 {
        return Err(anyhow!("The encrypted HLS segment at {} is {} bytes, not a multiple of 16", raw.display(), pending.len()));
    }
    let mut last = GenericArray::<u8, U16>::from_mut_slice(&mut pending);
    decryptor.decrypt_block_mut(&mut last);
    let pad_length = *last.last().unwrap_or(&0) as usize;
    if pad_length == 0 || pad_length > 16 || !last[16 - pad_length..].iter().all(|byte| *byte as usize == pad_length) {
        return Err(anyhow!("The encrypted HLS segment at {} has invalid PKCS#7 padding", raw.display()));
    }
    let plaintext_len = 16 - pad_length;
    destination.write_all(&last[..plaintext_len]).await
        .with_context(|| format!("Could not write the decrypted segment at {}", out.display()))?;
    plaintext_bytes += plaintext_len as u64;
    destination.flush().await
        .with_context(|| format!("Could not flush the decrypted segment at {}", out.display()))?;
    Ok(plaintext_bytes)
}

/// Fetches one segment (whole file, or one EXT-X-BYTERANGE window of a
/// larger resource) into `path`. A server that ignores Range and answers
/// 200 is handled by slicing the requested window out of the full body;
/// a 206 answer must contain exactly the requested length.
async fn fetch_hls_segment(
    client: &Client,
    segment: &HlsSegmentRef,
    session: Option<&SessionHeaders>,
    path: &Path,
    rate_limiter: &Arc<GlobalRateLimiter>,
    key_cache: &HlsKeyCache,
) -> Result<u64> {
    // The key is fetched BEFORE the segment: a refused key aborts before
    // bandwidth is spent, and the key fetch itself can trigger the
    // signed-URL refresh (its 403 is classified exactly like a segment
    // 403). The cache makes the fetch free for segments sharing a key.
    let key = match &segment.key {
        Some(key_ref) => Some((
            fetch_hls_key(client, &key_ref.uri, session, key_cache).await?,
            key_ref.iv.expect("HlsKeyRef always carries a resolved IV after parsing"),
        )),
        None => None,
    };
    // An encrypted segment is downloaded as ciphertext to a temp file
    // and decrypted into `path` only when whole and valid: a partial or
    // corrupt ciphertext must never be mistaken for a finished segment.
    // Unencrypted segments write straight to `path`, as before.
    let raw_path = if key.is_some() { path.with_extension("tmp") } else { path.to_path_buf() };
    let mut request = apply_session(session, client.get(&segment.url), &segment.url);
    if let Some((start, length)) = segment.range {
        request = request.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", start, start.saturating_add(length).saturating_sub(1)),
        );
    }
    let response = request.send().await?.error_for_status()?;
    let mut file = fs::File::create(&raw_path).await?;
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
    if let Some((key, iv)) = key {
        // The raw file is closed before decrypting it (Windows keeps an
        // open handle from blocking the read), and the ciphertext temp
        // file is removed either way: a failed decrypt must not leave a
        // stale partial behind for the next attempt to mistake for one.
        drop(file);
        let plaintext = decrypt_aes128_segment(&raw_path, path, &key, &iv).await;
        let _ = fs::remove_file(&raw_path).await;
        return plaintext;
    }
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
    let result = hidden_command(&executable)
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

/// Fetches every unfinished segment of an HLS plan, in parallel up to
/// `connections` (Slice 7b — the caller's value, already clamped),
/// writing each finished segment's index into the checkpoint as it
/// lands. Returns Completed when every segment is on disk, or the
/// Paused/Cancelled outcome — with the checkpoint written (paused) or
/// the resume files removed (cancelled) — when the control changes. A
/// segment error aborts the in-flight tasks and returns the error; the
/// checkpoint keeps the finished segments, so the caller can resume or,
/// for a 401/403, refresh the playlist and retry.
///
/// Pause/cancel are prompt (Slice 4, Gemini claim 5): the loop waits on
/// `tasks.join_next()` AND `control.changed()` together, so a stop during
/// a slow segment returns as soon as the control flips instead of after
/// the in-flight segment finishes. The watch guard is only ever borrowed
/// at loop tops — never held across an await — so the `!Send`
/// `watch::Ref` never escapes the loop.
async fn run_hls_segments(
    client: &Client,
    state: &mut HlsJobState,
    work_dir: &Path,
    meta_path: &Path,
    session: Option<&SessionHeaders>,
    control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    key_cache: &HlsKeyCache,
    completed: Arc<AtomicUsize>,
    downloaded_bytes: Arc<AtomicU64>,
    total: usize,
    target: &Path,
    connections: usize,
) -> Result<DownloadOutcome> {
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
    // A second receiver watches for control changes: `changed()` needs
    // &mut, and the coordinator's own receiver is borrowed at loop tops.
    let mut control_watch = control.clone();
    let mut watch_alive = true;
    let mut stopped: Option<DownloadControl> = None;
    while !work.is_empty() || !tasks.is_empty() {
        if matches!(*control.borrow(), DownloadControl::Paused | DownloadControl::Cancelled) {
            stopped = Some(*control.borrow());
            break;
        }
        while tasks.len() < connections {
            let Some((mark, segment)) = work.pop_front() else { break; };
            let client = client.clone();
            let session = session.cloned();
            let path = hls_segment_path(work_dir, mark);
            let rate_limiter = rate_limiter.clone();
            let key_cache = key_cache.clone();
            tasks.spawn(async move {
                let bytes = fetch_hls_segment(&client, &segment, session.as_ref(), &path, &rate_limiter, &key_cache).await?;
                Ok::<(HlsSegmentMark, u64), anyhow::Error>((mark, bytes))
            });
        }
        // Wait for the next finished segment, or for the control to
        // change — whichever comes first. When the sender is gone the
        // watch arm is dropped and the loop waits on tasks alone.
        // `join_next()` is cancel-safe: dropping its future on the
        // control arm leaves the tasks running.
        let result = if watch_alive {
            tokio::select! {
                result = tasks.join_next() => result,
                changed = control_watch.changed() => {
                    if changed.is_err() {
                        watch_alive = false;
                    }
                    continue;
                }
            }
        } else {
            tasks.join_next().await
        };
        let Some(result) = result else { break; };
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
                write_hls_checkpoint(meta_path, state).await?;
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                let transferred = downloaded_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
                let phase = match mark {
                    HlsSegmentMark::VideoInit | HlsSegmentMark::Video(_) => "video-segments",
                    HlsSegmentMark::AudioInit | HlsSegmentMark::Audio(_) => "audio-segments",
                };
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: transferred, total: None,
                    percent: Some(done as f64 * 100.0 / total.max(1) as f64), phase: phase.into(),
                    segments_done: Some(done as u64), segments_total: Some(total as u64),
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
        write_hls_checkpoint(meta_path, state).await?;
        return match stopped {
            DownloadControl::Paused => {
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
                    percent: Some(state.done_segments() as f64 * 100.0 / total.max(1) as f64),
                    phase: "paused".into(),
                    segments_done: Some(state.done_segments() as u64), segments_total: Some(total as u64),
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
    Ok(DownloadOutcome::Completed)
}

/// HLS download with pause/resume/cancel and an on-disk checkpoint,
/// mirroring the direct-download pattern in
/// `download::download_url_resumable`: the playlist URL, chosen variant,
/// completed segment indexes and target are persisted next to the target,
/// so a fresh call — including from a fresh process — resumes from the
/// next unfinished segment. Cancel removes the partial files. A network
/// error keeps the checkpoint and the finished segments, so a retry
/// resumes instead of restarting. A 401/403 from a segment or key fetch
/// re-resolves the playlist (refreshing expired signed URLs) and retries
/// the missing segments, bounded by `MAX_SEGMENT_URL_REFRESHES`.
/// `selection` is what the user picked from the master playlist (Slice 4,
/// Gemini claim 4); `HlsSelection::none()` keeps the old first-variant
/// behaviour. `connections` is the segment parallelism (Slice 7b — the
/// desktop Settings value, clamped 1..=16).
pub async fn download_hls_resumable(
    playlist_url: &str,
    selection: &HlsSelection,
    target: &Path,
    session: Option<&SessionHeaders>,
    control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    connections: usize,
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
    // A resume reuses the handoff stored with the checkpoint when the
    // caller has none (F2): a desktop resume of a browser-started job
    // mints a fresh job id, so the stored JobRecord session is
    // unreachable — the checkpoint is the only copy left. Read it before
    // resolving the plan so the playlist fetch itself is authenticated.
    // An explicit caller session always wins.
    let saved = read_hls_checkpoint(&meta_path).await;
    let owned_session = session.is_none()
        .then(|| saved.as_ref().and_then(|saved| saved.session.clone()))
        .flatten();
    let session = session.or(owned_session.as_ref());
    let mut state = resolve_hls_plan(playlist_url, selection, session).await?;
    // Reuse a checkpoint only when it describes exactly this job; a
    // playlist whose segment list changed restarts from scratch.
    let mut reused_checkpoint = false;
    if let Some(saved) = saved {
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
            // Swap the fresh segment URLs into the reused checkpoint: the
            // saved state's URLs may have expired (rotated signed URLs),
            // and reusing them would 403 the first missing segment and
            // burn one of the two refreshes. The done-sets stay as
            // pruned; the plan match already proved the segments are the
            // same files (identity is URL-minus-query), so the fresh refs
            // can only differ in the signature query — and the AES-128
            // key URIs ride along with them.
            pruned.video_init = state.video_init;
            pruned.video_segments = state.video_segments;
            pruned.audio_init = state.audio_init;
            pruned.audio_segments = state.audio_segments;
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

    // Signed-URL refresh (Slice 4): a 401/403 from a segment (or key)
    // fetch usually means the signed URLs expired mid-download. Re-resolve
    // the playlist with the same session and selection, swap the fresh
    // URLs into the plan, and retry the segments that are still missing.
    // The checkpoint and the finished segments are untouched, so the
    // retry resumes instead of restarting. After
    // `MAX_SEGMENT_URL_REFRESHES` the last 401/403 is the error. A
    // re-resolve whose segment list no longer matches the started plan is
    // a changed playlist, not an expiry, and fails outright.
    let key_cache = HlsKeyCache::default();
    let mut refreshes = 0;
    let outcome = loop {
        match run_hls_segments(
            &client, &mut state, &work_dir, &meta_path, session, control.clone(),
            progress_tx.clone(), rate_limiter.clone(), &key_cache,
            completed.clone(), downloaded_bytes.clone(), total, target,
            connections.clamp(1, 16),
        ).await {
            Ok(outcome) => break outcome,
            Err(error) if is_auth_error(&error) && refreshes < MAX_SEGMENT_URL_REFRESHES => {
                refreshes += 1;
                let fresh = resolve_hls_plan(playlist_url, selection, session).await
                    .with_context(|| "Could not refresh the HLS playlist after the signed segment URLs expired")?;
                if !state.matches_plan(&fresh) {
                    return Err(anyhow!("The HLS playlist changed while downloading (its segment list no longer matches the started download); restart the download"));
                }
                state.video_init = fresh.video_init;
                state.video_segments = fresh.video_segments;
                state.audio_init = fresh.audio_init;
                state.audio_segments = fresh.audio_segments;
                write_hls_checkpoint(&meta_path, &state).await?;
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
                    percent: Some(state.done_segments() as f64 * 100.0 / total.max(1) as f64),
                    phase: "refreshing".into(),
                    segments_done: Some(state.done_segments() as u64), segments_total: Some(total as u64),
                });
            }
            Err(error) if is_auth_error(&error) => {
                return Err(error).context("The signed HLS segment URLs expired and refreshing the playlist did not help; retry the download");
            }
            Err(error) => return Err(error),
        }
    };
    // The segment phase ended paused or cancelled: the checkpoint is
    // already written (paused) or the resume files removed (cancelled)
    // by run_hls_segments — there is nothing to assemble.
    if outcome != DownloadOutcome::Completed {
        return Ok(outcome);
    }

    let _ = progress_tx.send(DownloadProgress {
        downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
        percent: Some(100.0), phase: "assembling".into(),
        segments_done: Some(state.done_segments() as u64), segments_total: Some(total as u64),
    });
    let video_track = work_dir.join("video.assembled");
    assemble_hls_track(&work_dir, &state, false, &video_track).await?;
    let final_file = if state.audio_playlist_url.is_some() {
        let audio_track = work_dir.join("audio.assembled");
        assemble_hls_track(&work_dir, &state, true, &audio_track).await?;
        let _ = progress_tx.send(DownloadProgress {
            downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
            percent: Some(100.0), phase: "muxing".into(),
            segments_done: Some(state.done_segments() as u64), segments_total: Some(total as u64),
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
    remove_dir_all_retry(&work_dir).await;
    let _ = fs::remove_file(&meta_path).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
        percent: Some(100.0), phase: "complete".into(),
        segments_done: Some(state.done_segments() as u64), segments_total: Some(total as u64),
    });
    Ok(DownloadOutcome::Completed)
}

/// The file extension an HLS download of this playlist produces ("mp4"
/// when an EXT-X-MAP init segment is present, "ts" otherwise). Callers use
/// it to reserve the target before starting the resumable download,
/// mirroring `dash_output_extension` — reserving from the caller's filename
/// alone would misname a TS stream as .mp4.
pub async fn hls_output_extension(
    playlist_url: &str,
    selection: &HlsSelection,
    session: Option<&SessionHeaders>,
) -> Result<String> {
    let plan = resolve_hls_plan(playlist_url, selection, session).await?;
    Ok(plan.extension)
}

pub async fn download_hls(
    playlist_url: &str,
    selection: &HlsSelection,
    filename: &str,
    session: Option<&SessionHeaders>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    // Resolve the plan first so the reserved target gets the right
    // extension (mp4 when an EXT-X-MAP init segment is present, ts
    // otherwise); the resumable call re-resolves the same small playlist.
    let plan = resolve_hls_plan(playlist_url, selection, session).await?;
    let out_dir = default_download_dir();
    fs::create_dir_all(&out_dir).await?;
    let safe = safe_filename(filename, "video");
    let stem = Path::new(&safe).file_stem().and_then(|s| s.to_str()).unwrap_or("video");
    let target = unique_video_path(&out_dir, stem, &plan.extension).await?;
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    let outcome = download_hls_resumable(playlist_url, selection, &target, session, control_rx, progress_tx, rate_limiter, DEFAULT_RESUMABLE_CONNECTIONS).await;
    drop(control_tx);
    match outcome? {
        DownloadOutcome::Completed => Ok(target),
        DownloadOutcome::Paused => Err(anyhow!("HLS download paused before completion")),
        DownloadOutcome::Cancelled => Err(anyhow!("HLS download cancelled")),
    }
}

/// The media playlist URL a live recording should follow: the same
/// master-selection rules as `resolve_hls_plan` (chosen variant, or an
/// audio rendition's playlist for the audio-only row, or the first
/// variant), but without building a VOD plan — a live playlist has no
/// fixed segment list to plan against. A bare media playlist is its own
/// answer.
async fn resolve_hls_live_media_url(playlist_url: &str, selection: &HlsSelection, session: Option<&SessionHeaders>) -> Result<String> {
    let base = Url::parse(playlist_url)?;
    let client = client()?;
    let body = fetch_text_with(&client, playlist_url, session).await?;
    let variants = parse_hls_master(&body, &base)?;
    if variants.is_empty() {
        return Ok(playlist_url.to_string());
    }
    if let Some(reason) = unsupported_hls_master_reason(&body) {
        return Err(anyhow!(reason));
    }
    let renditions = parse_hls_audio_renditions(&body, &base);
    if let Some(wanted) = selection.media_url.as_deref() {
        if let Some(rendition) = renditions.iter().find(|rendition| rendition.uri == wanted) {
            return Ok(rendition.uri.clone());
        }
        if let Some(variant) = variants.iter().find(|variant| variant.url == wanted) {
            return Ok(variant.url.clone());
        }
        return Err(anyhow!("The selected HLS quality is no longer listed in the master playlist"));
    }
    Ok(variants.first().expect("non-empty variant list").url.clone())
}

/// How many consecutive polls with no new segment stop a live
/// recording as stalled. Ten silent windows is 20 s at the shortest
/// target duration and 5 min at the longest — long enough that a brief
/// encoder hiccup never kills a recording, short enough that a dead
/// stream does not poll forever.
const LIVE_STALL_LIMIT: u64 = 10;

/// Records a live HLS stream as it arrives (Slice 6). The media playlist
/// is polled every TARGETDURATION (default 6 s, clamped 2–30 s); each
/// poll downloads only the segments whose sequence numbers have not been
/// recorded yet, in order, appending them to a single `.ts` file with the
/// same segment fetcher the VOD path uses (AES-128 decryption, byte
/// ranges, rate limiting). The first poll records the whole live window
/// as it stands — there is no earlier window to recover.
///
/// Stops when the playlist gains EXT-X-ENDLIST (Completed), when the
/// user cancels (Cancelled — the partial file is kept, it is a valid
/// recording of what arrived), when it pauses (Paused — also keeps the
/// partial; a later resume re-records from the live edge, it does not
/// continue the old recording), or when the playlist stalls with no new
/// segment for LIVE_STALL_LIMIT consecutive polls (a stall error — the
/// partial is kept, and when nothing was recorded at all no empty file
/// is left behind). A segment that slid out of the window
/// between the playlist fetch and its download (404) is skipped, not
/// fatal — but if the very first poll's segments are all gone, the
/// recording fails instead of producing an empty file. fMP4 live
/// (EXT-X-MAP present) is refused upfront: its segments cannot be
/// appended into a playable file without a remuxer, and the refusal
/// leaves no empty target file behind. No checkpoint is
/// written — a live recording has no fixed segment list to resume from.
pub async fn download_hls_live(
    playlist_url: &str,
    selection: &HlsSelection,
    target: &Path,
    session: Option<&SessionHeaders>,
    mut control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<DownloadOutcome> {
    let client = client()?;
    let key_cache = HlsKeyCache::default();
    let media_url = resolve_hls_live_media_url(playlist_url, selection, session).await?;
    let media_base = Url::parse(&media_url)?;
    // The target is the recording itself: create it once, append every
    // segment in order. The desktop reserves a unique path beforehand.
    let mut output = fs::OpenOptions::new().create(true).write(true).truncate(true).open(target).await?;
    let segment_temp = target.with_extension("live-seg");
    let mut next_sequence: Option<u64> = None;
    let mut downloaded: u64 = 0;
    let mut recorded: u64 = 0;
    let mut stalled_polls: u64 = 0;
    loop {
        // Copy the control value out first: matching on
        // `*control.borrow()` directly would hold the watch Ref across
        // the awaits inside the arms, and the Ref is not Send.
        let current = *control.borrow();
        match current {
            DownloadControl::Cancelled => {
                let _ = fs::remove_file(&segment_temp).await;
                return Ok(DownloadOutcome::Cancelled);
            }
            DownloadControl::Paused => {
                let _ = fs::remove_file(&segment_temp).await;
                return Ok(DownloadOutcome::Paused);
            }
            DownloadControl::Running => {}
        }
        let body = fetch_text_with(&client, &media_url, session).await?;
        let parsed = parse_hls_live_media_playlist(&body, &media_base)?;
        // fMP4 live cannot be appended into a playable file: refuse
        // honestly instead of writing fragments that never play. The
        // refusal happens before any segment is appended (on the first
        // poll), so the target is still the empty reservation — remove
        // it instead of leaving a 0-byte file behind. If a later poll
        // somehow gained an init map after segments were recorded, the
        // partial is kept: it is a real recording.
        if parsed.init.is_some() {
            let _ = fs::remove_file(&segment_temp).await;
            if recorded == 0 {
                let _ = fs::remove_file(target).await;
            }
            return Err(anyhow!("Live stream in this format can't be recorded yet."));
        }
        let skip = match next_sequence {
            None => 0,
            Some(next) => next.saturating_sub(parsed.media_sequence) as usize,
        };
        let mut recorded_this_poll = 0_u64;
        let mut attempted_this_poll = 0_u64;
        for (index, segment) in parsed.segments.iter().enumerate().skip(skip) {
            attempted_this_poll += 1;
            let sequence = parsed.media_sequence + index as u64;
            match fetch_hls_segment(&client, segment, session, &segment_temp, &rate_limiter, &key_cache).await {
                Ok(bytes) => {
                    let mut input = fs::File::open(&segment_temp).await?;
                    let copied = tokio::io::copy(&mut input, &mut output).await?;
                    debug_assert_eq!(copied, bytes);
                    let _ = fs::remove_file(&segment_temp).await;
                    downloaded = downloaded.saturating_add(bytes);
                    recorded += 1;
                    recorded_this_poll += 1;
                    next_sequence = Some(sequence + 1);
                    let _ = progress_tx.send(DownloadProgress {
                        downloaded,
                        total: None,
                        percent: None,
                        phase: "recording".into(),
                        // A live recording has no total segment count —
                        // the window keeps growing — so only the done
                        // count is reported (Slice 7b).
                        segments_done: Some(recorded),
                        segments_total: None,
                    });
                }
                Err(error) => {
                    // A segment that slid out of the live window while
                    // this poll was running is gone for good: skip it
                    // and keep recording the rest. Anything else
                    // (auth, network, corrupt segment) fails the
                    // recording — the error_text funnel labels it.
                    let gone = error.downcast_ref::<reqwest::Error>()
                        .and_then(|error| error.status())
                        .is_some_and(|status| status == reqwest::StatusCode::NOT_FOUND);
                    if gone {
                        next_sequence = Some(sequence + 1);
                        continue;
                    }
                    let _ = fs::remove_file(&segment_temp).await;
                    return Err(error);
                }
            }
        }
        // The first poll's window was entirely gone before any segment
        // could be fetched: fail instead of returning an empty file —
        // and remove the empty target, which nothing was recorded into.
        if recorded == 0 && attempted_this_poll > 0 && recorded_this_poll == 0 {
            let _ = fs::remove_file(&segment_temp).await;
            let _ = fs::remove_file(target).await;
            return Err(anyhow!("The live stream's segments are no longer available."));
        }
        if is_complete_vod_playlist(&body) {
            let _ = fs::remove_file(&segment_temp).await;
            output.flush().await?;
            return Ok(DownloadOutcome::Completed);
        }
        // A playlist that stops advancing is a dead stream, not a slow
        // one: after LIVE_STALL_LIMIT consecutive polls with no new
        // segment, stop with a stall error instead of polling forever.
        // The partial recording is kept; when nothing was recorded at
        // all the empty target goes with it (same rule as the fMP4
        // refusal above).
        if recorded_this_poll == 0 {
            stalled_polls += 1;
            if stalled_polls >= LIVE_STALL_LIMIT {
                let _ = fs::remove_file(&segment_temp).await;
                if recorded == 0 {
                    let _ = fs::remove_file(target).await;
                }
                // Slice 6 fix round 2 (nit 1): when nothing was
                // recorded the target is gone with it, so claiming the
                // partial was kept lies — the message says so only
                // when a partial actually survives.
                let message = if recorded == 0 {
                    "The live stream stalled — no new segments arrived."
                } else {
                    "The live stream stalled — no new segments arrived. The partial recording was kept."
                };
                return Err(anyhow!(message));
            }
        } else {
            stalled_polls = 0;
        }
        // Wait for the next window: TARGETDURATION from the playlist
        // itself (default 6 s, clamped 2–30 s), woken early when the
        // user pauses or cancels.
        let poll = parse_hls_target_duration(&body).unwrap_or(6).clamp(2, 30);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(poll)) => {}
            _ = control.changed() => {}
        }
    }
}

/// The playlist's `#EXT-X-TARGETDURATION`, in seconds, when present.
fn parse_hls_target_duration(body: &str) -> Option<u64> {
    body.lines().map(str::trim)
        .find_map(|line| line.strip_prefix("#EXT-X-TARGETDURATION:"))
        .and_then(|value| value.trim().parse().ok())
}

/// Downloads an HLS subtitle media playlist (format "m3u8") into one
/// `.vtt` file: each segment is a WebVTT fragment, so the fragments are
/// concatenated with the duplicate `WEBVTT` headers stripped after the
/// first. Single-file subtitles (format "vtt"/"ttml") are plain direct
/// downloads and never reach this function. A live subtitle playlist
/// (no ENDLIST) is refused — live subtitles are not recorded in this
/// slice. The caller reserves the target path (a `.vtt` file).
pub async fn download_hls_subtitles(
    playlist_url: &str,
    target: &Path,
    session: Option<&SessionHeaders>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    let client = client()?;
    let base = Url::parse(playlist_url)?;
    let body = fetch_text_with(&client, playlist_url, session).await?;
    if !is_complete_vod_playlist(&body) {
        return Err(anyhow!("Live subtitle playlists can't be downloaded yet."));
    }
    let parsed = parse_hls_media_playlist(&body, &base)?;
    if parsed.segments.is_empty() {
        return Err(anyhow!("No subtitle segments were found in this playlist"));
    }
    let mut out = fs::File::create(target).await?;
    let mut first = true;
    for segment in &parsed.segments {
        // Subtitle segments are small text; fetched whole (with the
        // session, so a login-walled track labels as auth, not 403).
        let text = fetch_text_with(&client, &segment.url, session).await?;
        let _ = rate_limiter.reserve(text.len() as u64).await;
        let mut text = text.as_str();
        // Every fragment repeats the WEBVTT header; keep only the
        // first, so the joined file is one valid WebVTT document.
        if !first {
            if let Some(rest) = text.strip_prefix("WEBVTT") {
                text = rest.trim_start_matches(['\r', '\n']);
            }
        }
        if !first && !text.is_empty() {
            out.write_all(b"\n").await?;
        }
        out.write_all(text.as_bytes()).await?;
        first = false;
    }
    out.flush().await?;
    Ok(target.to_path_buf())
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
    session: Option<&SessionHeaders>,
) -> Result<Vec<u8>> {
    let (start, end) = range;
    let expected = end - start + 1;
    let request = apply_session(session, client.get(url), url);
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
    session: Option<&SessionHeaders>,
) -> Result<(Option<String>, Option<(u64, u64)>, Vec<String>, Vec<Option<(u64, u64)>>)> {
    let index_bytes = fetch_dash_range_bytes(client, &plan.base_url, plan.index_range, session).await
        .with_context(|| format!(
            "Could not fetch the DASH segment index (bytes {}-{}) from {}",
            plan.index_range.0, plan.index_range.1, plan.base_url
        ))?;
    let ranges = parse_dash_sidx(&index_bytes, plan.index_range.0)?;
    let segment_urls = vec![plan.base_url.clone(); ranges.len()];
    let segment_ranges = ranges.into_iter().map(Some).collect();
    Ok((plan.init_url.clone(), plan.init_range, segment_urls, segment_ranges))
}

/// What one DASH track download finished as: the assembled track file, or
/// a stop (pause/cancel) observed mid-track — the finished segments stay on
/// disk for the next call, which skips them via its done-set.
enum DashTrackOutcome {
    Assembled(PathBuf),
    Stopped(DownloadControl),
}

/// Awaits the next finished task in a segment `JoinSet`, or a control
/// change, whichever comes first (Slice 4, Gemini claim 5). Returns `None`
/// when the control changed — the caller re-checks `control.borrow()` at
/// its loop top and stops, or carries on when the change was a no-op
/// (e.g. a Running send) — or when the task set is empty. `watch_alive`
/// tracks whether the control sender is still alive; once it is gone the
/// watch arm is dropped and the loop waits on tasks alone. `join_next()`
/// is cancel-safe, so dropping its future on the control arm leaves the
/// tasks running.
async fn join_next_or_control<T: Send + 'static>(
    tasks: &mut tokio::task::JoinSet<T>,
    control: &mut watch::Receiver<DownloadControl>,
    watch_alive: &mut bool,
) -> Option<Result<T, tokio::task::JoinError>> {
    if *watch_alive {
        tokio::select! {
            result = tasks.join_next() => result,
            changed = control.changed() => {
                if changed.is_err() {
                    *watch_alive = false;
                }
                None
            }
        }
    } else {
        tasks.join_next().await
    }
}

/// Removes one track's in-progress segment temp files from the work
/// dir (Slice 7a, claim 5 nit). A segment is written to
/// `{prefix}-{index:06}.segment.tmp` and renamed into place only on
/// success, so any `.segment.tmp` left when the track stops or fails is
/// garbage: an aborted task can never clean up after itself, and a
/// failed task returns before the rename. Only temp files are removed —
/// finished `.segment` files are the resume record and are never
/// touched. Call only after the task set is fully drained, so no task
/// can still be writing.
async fn remove_dash_segment_temps(work_dir: &Path, prefix: &str) {
    let Ok(mut entries) = fs::read_dir(work_dir).await else { return };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(prefix) && name.ends_with(".segment.tmp") {
            let _ = fs::remove_file(entry.path()).await;
        }
    }
}

async fn download_dash_track(
    client: &Client,
    track: &DashResolvedTrack,
    work_dir: &Path,
    prefix: &str,
    session: Option<&SessionHeaders>,
    progress_tx: &mpsc::UnboundedSender<DownloadProgress>,
    completed: Arc<AtomicUsize>,
    downloaded_bytes: Arc<AtomicU64>,
    total_segments: usize,
    rate_limiter: Arc<GlobalRateLimiter>,
    done: &HashSet<usize>,
    control: &watch::Receiver<DownloadControl>,
    connections: usize,
) -> Result<DashTrackOutcome> {
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
    // A second receiver watches for control changes (Slice 4, Gemini
    // claim 5): `changed()` needs &mut, and the coordinator's own
    // receiver is borrowed at loop tops only.
    let mut control_watch = control.clone();
    let mut watch_alive = true;
    let mut stopped: Option<DownloadControl> = None;
    for (index, (segment_url, range)) in refs.into_iter().enumerate() {
        // Already on disk from an earlier call: the segment file is the
        // progress record, so it is never re-requested.
        if done.contains(&index) { continue; }
        // Copy the value out first: the borrow guard must not be held
        // across the awaits below (watch::Ref is not Send).
        let current = *control.borrow();
        if let control @ (DownloadControl::Paused | DownloadControl::Cancelled) = current {
            stopped = Some(control);
            break;
        }
        let client = client.clone();
        let session = session.cloned();
        let path = work_dir.join(format!("{prefix}-{index:06}.segment"));
        // A segment is written to a temporary file and renamed into place
        // only after its bytes are fully written and validated, so a
        // partial file can never be mistaken for a finished segment on
        // resume (the rename is the commit point).
        let tmp_path = path.with_extension("segment.tmp");
        let tx = progress_tx.clone();
        let completed = completed.clone();
        let downloaded_bytes = downloaded_bytes.clone();
        let rate_limiter = rate_limiter.clone();
        tasks.spawn(async move {
            let mut request = apply_session(session.as_ref(), client.get(segment_url.as_str()), &segment_url);
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
            let mut file = fs::File::create(&tmp_path).await?;
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
            // Commit the segment: drop the handle first (Windows cannot
            // rename an open file), then rename into place.
            drop(file);
            fs::rename(&tmp_path, &path).await?;
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let transferred = downloaded_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
            let _ = tx.send(DownloadProgress {
                downloaded: transferred,
                total: None,
                percent: Some(done as f64 * 100.0 / total_segments.max(1) as f64),
                phase: "dash-segments".into(),
                segments_done: Some(done as u64), segments_total: Some(total_segments as u64),
            });
            Ok::<PathBuf, anyhow::Error>(path)
        });
        // Segments run concurrently up to `connections` (Slice 7b —
        // the caller's value, already clamped; was the hardcoded
        // DEFAULT_RESUMABLE_CONNECTIONS before Slice 4, then the
        // constant itself).
        // The drain waits on the next finished segment OR a control
        // change, so a pause/cancel during a slow segment returns
        // promptly instead of after the in-flight segment finishes.
        if tasks.len() >= connections {
            if let Some(result) = join_next_or_control(&mut tasks, &mut control_watch, &mut watch_alive).await {
                if let Err(error) = result? {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    remove_dash_segment_temps(work_dir, prefix).await;
                    return Err(error);
                }
            }
        }
    }
    if let Some(stopped) = stopped {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        remove_dash_segment_temps(work_dir, prefix).await;
        return Ok(DashTrackOutcome::Stopped(stopped));
    }

    let mut failures = Vec::new();
    loop {
        let Some(result) = join_next_or_control(&mut tasks, &mut control_watch, &mut watch_alive).await else {
            // The task set is empty, or the control changed. A stop
            // aborts the in-flight tasks; a no-op change (e.g. a Running
            // send) just resumes the drain; an empty set ends it.
            if matches!(*control.borrow(), DownloadControl::Paused | DownloadControl::Cancelled) {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                remove_dash_segment_temps(work_dir, prefix).await;
                return Ok(DashTrackOutcome::Stopped(*control.borrow()));
            }
            if tasks.is_empty() {
                break;
            }
            continue;
        };
        // Copy the value out first: the borrow guard must not be held
        // across the awaits below (watch::Ref is not Send).
        let current = *control.borrow();
        if let control @ (DownloadControl::Paused | DownloadControl::Cancelled) = current {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            remove_dash_segment_temps(work_dir, prefix).await;
            return Ok(DashTrackOutcome::Stopped(control));
        }
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => failures.push(error),
            Err(error) => failures.push(anyhow!(error)),
        }
    }
    if let Some(error) = failures.into_iter().next() {
        remove_dash_segment_temps(work_dir, prefix).await;
        return Err(error);
    }

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
    Ok(DashTrackOutcome::Assembled(assembled))
}

async fn ffmpeg_mux_dash_tracks(
    video_file: &Path,
    audio_file: &Path,
    output: &Path,
) -> Result<()> {
    let executable = std::env::var_os("THUNDERLOAD_FFMPEG").unwrap_or_else(|| "ffmpeg".into());
    let result = hidden_command(executable)
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
    let result = hidden_command(executable)
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

/// A fully resolved static DASH job: every period's video track (and audio
/// track, when one was requested) with concrete segment URLs, plus whether
/// more than one period must be concatenated. Self-contained — the parsed
/// MPD is dropped after resolution — so a checkpoint can be built from it
/// and a resume can compare a freshly resolved plan against the saved one.
struct DashPlan {
    video_tracks: Vec<DashResolvedTrack>,
    audio_tracks: Vec<DashResolvedTrack>,
    multi_period: bool,
}

/// Fetches the MPD, rejects dynamic/live and DRM manifests, resolves every
/// period's requested representations (expanding SegmentBase sidx indexes),
/// and checks that the periods agree on codec and dimensions. Shared by the
/// one-shot download and the resumable download so both see the same plan.
async fn resolve_dash_plan(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    session: Option<&SessionHeaders>,
) -> Result<DashPlan> {
    let body = fetch_text(manifest_url, session).await?;
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
            let (init_url, init_range, urls, ranges) = expand_dash_segment_base(&client, &plan, session).await?;
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
                let (init_url, init_range, urls, ranges) = expand_dash_segment_base(&client, &plan, session).await?;
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
    Ok(DashPlan { video_tracks, audio_tracks, multi_period })
}

/// The file extension a DASH download of these representations produces
/// ("webm" for a video-only WebM track, "mp4" otherwise — separate audio is
/// always muxed into MP4). Callers use it to reserve the target before
/// starting the resumable download, mirroring how `download_hls` resolves
/// the playlist first so the reserved target gets the right extension.
pub async fn dash_output_extension(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    session: Option<&SessionHeaders>,
) -> Result<String> {
    let plan = resolve_dash_plan(manifest_url, representation_id, audio_representation_id, session).await?;
    let extension = if audio_representation_id.is_some() {
        "mp4"
    } else if plan.video_tracks.first().map(|track| track.mime_type.to_ascii_lowercase().contains("webm")).unwrap_or(false) {
        "webm"
    } else {
        "mp4"
    };
    Ok(extension.to_string())
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
    session: Option<&SessionHeaders>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
) -> Result<PathBuf> {
    let out_dir = default_download_dir();
    fs::create_dir_all(&out_dir).await?;
    download_dash_into(
        manifest_url, representation_id, audio_representation_id, filename,
        session, progress_tx, rate_limiter, &out_dir,
    ).await
}

async fn download_dash_into(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    filename: &str,
    session: Option<&SessionHeaders>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    out_dir: &Path,
) -> Result<PathBuf> {
    // One-shot download: a fresh target, a fresh control channel that is
    // never paused or cancelled, and the old contract — on any failure the
    // work directory and the reserved target are removed, so a failed call
    // leaves nothing behind. The resumable engine does the work; a caller
    // that wants pause/resume uses `download_dash_resumable` directly.
    let plan = resolve_dash_plan(manifest_url, representation_id, audio_representation_id, session).await?;
    let track_extension = |mime_type: &str| if mime_type.to_ascii_lowercase().contains("webm") { "webm" } else { "mp4" };
    let extension = if audio_representation_id.is_some() { "mp4" } else { track_extension(&plan.video_tracks[0].mime_type) };
    let safe = safe_filename(filename, "video");
    let stem = Path::new(&safe).file_stem().and_then(|value| value.to_str()).unwrap_or("video");
    let target = unique_video_path(out_dir, stem, extension).await?;
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    let outcome = download_dash_resumable_with_plan(
        manifest_url, representation_id, audio_representation_id, &target, session,
        control_rx, progress_tx, rate_limiter, plan, DEFAULT_RESUMABLE_CONNECTIONS,
    ).await;
    drop(control_tx);
    match outcome {
        Ok(DownloadOutcome::Completed) => Ok(target),
        Ok(outcome) => {
            remove_dash_resume_files(&target).await;
            Err(anyhow!("DASH download stopped with {outcome:?} before completion"))
        }
        Err(error) => {
            remove_dash_resume_files(&target).await;
            Err(error)
        }
    }
}

/// Stable work directory for one DASH target, mirroring `hls_work_dir`:
/// segment files live here, so a fresh call — including from a fresh
/// process — resumes purely from disk.
pub fn dash_work_dir(target: &Path) -> PathBuf {
    let extension = target.extension().and_then(|value| value.to_str()).unwrap_or("file");
    target.with_extension(format!("{extension}.thunderload.dash"))
}

async fn remove_dash_resume_files(target: &Path) {
    remove_dir_all_retry(&dash_work_dir(target)).await;
    let _ = fs::remove_file(partial_download_metadata_path(target)).await;
    let _ = fs::remove_file(target).await;
}

/// Which kind of media checkpoint a sidecar describes (Slice 5: the job
/// store reads checkpoints to import orphaned jobs and to report segment
/// progress, without re-running the downloaders).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MediaKind {
    Hls,
    Dash,
}

/// The resumable identity of one HLS/DASH checkpoint sidecar, read
/// synchronously from the JSON file on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MediaCheckpointInfo {
    pub kind: MediaKind,
    /// Playlist URL (HLS) or manifest URL (DASH) — the URL a resume call
    /// needs.
    pub url: String,
    pub representation_id: Option<String>,
    pub audio_representation_id: Option<String>,
    /// Browser session handed off with the job, if any — carried so an
    /// orphaned browser-started job keeps its handoff for a desktop resume.
    pub session: Option<SessionHeaders>,
    segments_done: Option<u64>,
    segments_total: Option<u64>,
}

impl MediaCheckpointInfo {
    /// (finished segments, planned segments) when the checkpoint carries
    /// a segment plan. None for a checkpoint with no segments.
    pub(crate) fn segments(&self) -> Option<(u64, u64)> {
        match (self.segments_done, self.segments_total) {
            (Some(done), Some(total)) => Some((done, total)),
            _ => None,
        }
    }
}

/// Reads one HLS/DASH checkpoint sidecar (`<target>.thunderload.part.
/// meta.json`) and returns what the job store needs: the kind, the URL to
/// resume from, the chosen representations, and the segment progress.
/// Returns None when the file is missing, unreadable, or not a media
/// checkpoint of a known kind. Never touches the files on disk.
pub(crate) fn media_checkpoint_info(meta_path: &Path, target: &Path) -> Option<MediaCheckpointInfo> {
    let bytes = std::fs::read(meta_path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    if value.get("playlist_url").is_some() {
        let state: HlsJobState = serde_json::from_value(value).ok()?;
        let (done, total) = (state.done_segments(), state.total_segments());
        // The selection is the job's identity for a resume: an orphaned
        // HLS job must re-resolve the master with the same variant and
        // audio it started with (Slice 4), not fall back to the first
        // variant. Old checkpoints carry no selection (None = first).
        return Some(MediaCheckpointInfo {
            kind: MediaKind::Hls,
            url: state.playlist_url,
            representation_id: state.selected_media_url,
            audio_representation_id: state.selected_audio_group,
            session: state.session,
            segments_done: Some(done as u64),
            segments_total: Some(total as u64),
        });
    }
    if value.get("manifest_url").is_some() {
        let state: DashJobState = serde_json::from_value(value).ok()?;
        let total: usize = state
            .video_tracks
            .iter()
            .chain(state.audio_tracks.iter())
            .map(|track| track.segment_urls.len() + usize::from(track.initialization_url.is_some()))
            .sum();
        // Done = finished `.segment` files on disk (in-progress downloads
        // live in `.tmp` files until renamed into place, exactly like the
        // downloader's own done-set derivation).
        let work_dir = dash_work_dir(target);
        let done = std::fs::read_dir(&work_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| {
                        entry.file_name().to_string_lossy().ends_with(".segment")
                            && entry.file_type().map(|kind| kind.is_file()).unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0);
        return Some(MediaCheckpointInfo {
            kind: MediaKind::Dash,
            url: state.manifest_url,
            representation_id: Some(state.representation_id),
            audio_representation_id: state.audio_representation_id,
            session: state.session,
            segments_done: Some(done as u64),
            segments_total: Some(total as u64),
        });
    }
    None
}

/// One resolved DASH track as persisted in the checkpoint: the concrete
/// segment plan (initialization + media URLs with their byte ranges).
/// The done-set is NOT stored — a segment counts as done only when its
/// file is on disk, and segment files are renamed into place only after
/// their bytes are fully written and validated, so a partial file can
/// never be mistaken for a finished segment.
///
/// Equality is the track's IDENTITY — mime type plus the initialization
/// and segment URLs with their queries stripped, plus the byte ranges —
/// not the full signed URLs. Signed segment URLs rotate their query
/// (the signature) on every manifest fetch while the media stays the
/// same; comparing full URLs would treat every refresh as a different
/// plan and restart the download from scratch. See `url_identity`.
#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
struct DashCheckpointTrack {
    mime_type: String,
    initialization_url: Option<String>,
    initialization_range: Option<(u64, u64)>,
    segment_urls: Vec<String>,
    segment_ranges: Vec<Option<(u64, u64)>>,
}

impl PartialEq for DashCheckpointTrack {
    fn eq(&self, other: &Self) -> bool {
        self.mime_type == other.mime_type
            && self.initialization_range == other.initialization_range
            && self.segment_ranges == other.segment_ranges
            && self.initialization_url.as_deref().map(url_identity) == other.initialization_url.as_deref().map(url_identity)
            && self.segment_urls.len() == other.segment_urls.len()
            && self.segment_urls.iter().zip(other.segment_urls.iter())
                .all(|(a, b)| url_identity(a) == url_identity(b))
    }
}

impl From<&DashResolvedTrack> for DashCheckpointTrack {
    fn from(track: &DashResolvedTrack) -> Self {
        Self {
            mime_type: track.mime_type.clone(),
            initialization_url: track.initialization_url.clone(),
            initialization_range: track.initialization_range,
            segment_urls: track.segment_urls.clone(),
            segment_ranges: track.segment_ranges.clone(),
        }
    }
}

/// Everything needed to (re)download one DASH job, persisted as the
/// checkpoint sidecar next to the target — the same sidecar path HLS uses
/// (`<target>.thunderload.part.meta.json`); the two never share a target.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DashJobState {
    version: u32,
    manifest_url: String,
    representation_id: String,
    audio_representation_id: Option<String>,
    #[serde(default)]
    session: Option<SessionHeaders>,
    video_tracks: Vec<DashCheckpointTrack>,
    audio_tracks: Vec<DashCheckpointTrack>,
    multi_period: bool,
}

impl DashJobState {
    fn from_plan(
        manifest_url: &str,
        representation_id: &str,
        audio_representation_id: Option<&str>,
        session: Option<&SessionHeaders>,
        plan: &DashPlan,
    ) -> Self {
        Self {
            version: 1,
            manifest_url: manifest_url.to_string(),
            representation_id: representation_id.to_string(),
            audio_representation_id: audio_representation_id.map(str::to_string),
            session: session.cloned(),
            video_tracks: plan.video_tracks.iter().map(DashCheckpointTrack::from).collect(),
            audio_tracks: plan.audio_tracks.iter().map(DashCheckpointTrack::from).collect(),
            multi_period: plan.multi_period,
        }
    }

    fn total_segments(&self) -> usize {
        self.video_tracks.iter()
            .map(|track| track.segment_urls.len() + usize::from(track.initialization_url.is_some()))
            .sum::<usize>()
            + self.audio_tracks.iter()
            .map(|track| track.segment_urls.len() + usize::from(track.initialization_url.is_some()))
            .sum::<usize>()
    }

    /// A checkpoint is reusable only when it describes exactly the same
    /// job: same manifest, same representations, same segment plan in the
    /// same order. Anything else is discarded and the job restarts from
    /// scratch instead of joining two different files.
    fn matches_plan(&self, plan: &DashJobState) -> bool {
        self.version == plan.version
            && self.manifest_url == plan.manifest_url
            && self.representation_id == plan.representation_id
            && self.audio_representation_id == plan.audio_representation_id
            && self.session.as_ref().map(|session| session.credential_identity())
                == plan.session.as_ref().map(|session| session.credential_identity())
            && self.video_tracks == plan.video_tracks
            && self.audio_tracks == plan.audio_tracks
            && self.multi_period == plan.multi_period
    }
}

async fn write_dash_checkpoint(path: &Path, state: &DashJobState) -> Result<()> {
    let temporary = path.with_extension("meta.tmp");
    let bytes = serde_json::to_vec(state)?;
    fs::write(&temporary, bytes).await?;
    // Unix rename(2) replaces atomically, so the checkpoint file is
    // never absent; Windows rename fails when the destination exists.
    // (Same gap as the direct-download writer, fixed the same way.)
    #[cfg(not(unix))]
    if fs::try_exists(path).await.unwrap_or(false) {
        fs::remove_file(path).await?;
    }
    fs::rename(&temporary, path).await
        .with_context(|| format!("Could not save DASH checkpoint at {}", path.display()))?;
    Ok(())
}

async fn read_dash_checkpoint(path: &Path) -> Option<DashJobState> {
    let bytes = fs::read(path).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Segment file for one reference of a track: index 0 is the initialization
/// segment when the track has one, then the media segments in order.
fn dash_segment_path(work_dir: &Path, prefix: &str, index: usize) -> PathBuf {
    work_dir.join(format!("{prefix}-{index:06}.segment"))
}

/// The references of one track that are already complete on disk: a segment
/// counts only when its final file exists (in-progress downloads live in a
/// `.tmp` file until they are renamed into place).
async fn dash_done_refs(work_dir: &Path, prefix: &str, track: &DashResolvedTrack) -> HashSet<usize> {
    let count = track.segment_urls.len() + usize::from(track.initialization_url.is_some());
    let mut done = HashSet::new();
    for index in 0..count {
        if fs::try_exists(dash_segment_path(work_dir, prefix, index)).await.unwrap_or(false) {
            done.insert(index);
        }
    }
    done
}

/// DASH download with pause/resume/cancel and an on-disk checkpoint,
/// mirroring `download_hls_resumable`: the manifest URL, chosen
/// representations and segment plan are persisted next to the target, so a
/// fresh call — including from a fresh process — resumes from the next
/// unfinished segment. Cancel removes the partial files. A network error
/// keeps the checkpoint and the finished segments, so a retry resumes
/// instead of restarting.
pub async fn download_dash_resumable(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    target: &Path,
    session: Option<&SessionHeaders>,
    control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    connections: usize,
) -> Result<DownloadOutcome> {
    // A resume reuses the handoff stored with the checkpoint when the
    // caller has none (F2) — read it before resolving the plan so the
    // manifest fetch itself is authenticated. An explicit caller
    // session always wins.
    let meta_path = partial_download_metadata_path(target);
    let saved = read_dash_checkpoint(&meta_path).await;
    let owned_session = session.is_none()
        .then(|| saved.as_ref().and_then(|saved| saved.session.clone()))
        .flatten();
    let session = session.or(owned_session.as_ref());
    let plan = resolve_dash_plan(manifest_url, representation_id, audio_representation_id, session).await?;
    download_dash_resumable_with_plan(
        manifest_url, representation_id, audio_representation_id, target, session,
        control, progress_tx, rate_limiter, plan, connections.clamp(1, 16),
    ).await
}

/// What one pass over the DASH tracks produced: every track assembled,
/// or the control that stopped the pass (pause/cancel).
enum DashTracksOutcome {
    Finished { video_files: Vec<PathBuf>, audio_files: Vec<PathBuf> },
    Stopped(DownloadControl),
}

/// Runs every video track then every audio track of one DASH plan, in
/// period order, assembling each track into a single file. Done-ness is
/// re-derived from the segment files on disk at the start of each track,
/// so a refresh pass skips the segments the previous pass finished.
/// A segment error aborts the in-flight tasks and returns the error —
/// the checkpoint and the finished segments survive, so the caller can
/// resume or, for a 401/403, refresh the manifest and retry.
async fn run_dash_tracks(
    client: &Client,
    plan: &DashPlan,
    work_dir: &Path,
    session: Option<&SessionHeaders>,
    progress_tx: &mpsc::UnboundedSender<DownloadProgress>,
    completed: Arc<AtomicUsize>,
    downloaded_bytes: Arc<AtomicU64>,
    total_segments: usize,
    rate_limiter: Arc<GlobalRateLimiter>,
    control: &watch::Receiver<DownloadControl>,
    connections: usize,
) -> Result<DashTracksOutcome> {
    let mut video_files = Vec::new();
    for (period_index, track) in plan.video_tracks.iter().enumerate() {
        let prefix = format!("video-p{period_index}");
        let done = dash_done_refs(work_dir, &prefix, track).await;
        match download_dash_track(
            client, track, work_dir, &prefix, session, progress_tx,
            completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone(),
            &done, control, connections,
        ).await? {
            DashTrackOutcome::Assembled(path) => video_files.push(path),
            DashTrackOutcome::Stopped(stopped) => return Ok(DashTracksOutcome::Stopped(stopped)),
        }
    }
    let mut audio_files = Vec::new();
    for (period_index, track) in plan.audio_tracks.iter().enumerate() {
        let prefix = format!("audio-p{period_index}");
        let done = dash_done_refs(work_dir, &prefix, track).await;
        match download_dash_track(
            client, track, work_dir, &prefix, session, progress_tx,
            completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone(),
            &done, control, connections,
        ).await? {
            DashTrackOutcome::Assembled(path) => audio_files.push(path),
            DashTrackOutcome::Stopped(stopped) => return Ok(DashTracksOutcome::Stopped(stopped)),
        }
    }
    Ok(DashTracksOutcome::Finished { video_files, audio_files })
}

async fn download_dash_resumable_with_plan(
    manifest_url: &str,
    representation_id: &str,
    audio_representation_id: Option<&str>,
    target: &Path,
    session: Option<&SessionHeaders>,
    control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
    rate_limiter: Arc<GlobalRateLimiter>,
    plan: DashPlan,
    connections: usize,
) -> Result<DownloadOutcome> {
    if *control.borrow() == DownloadControl::Cancelled {
        remove_dash_resume_files(target).await;
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
    let work_dir = dash_work_dir(target);
    let meta_path = partial_download_metadata_path(target);
    let state = DashJobState::from_plan(manifest_url, representation_id, audio_representation_id, session, &plan);
    // Reuse a checkpoint only when it describes exactly this job; a
    // manifest whose segment plan changed restarts from scratch.
    let reused = matches!(read_dash_checkpoint(&meta_path).await, Some(saved) if saved.matches_plan(&state));
    if !reused {
        // No usable checkpoint — fresh job, changed plan, or corrupt
        // metadata: drop any stale segment files so they can never be
        // mistaken for this job's output.
        let _ = fs::remove_dir_all(&work_dir).await;
        let _ = fs::remove_file(&meta_path).await;
    }
    fs::create_dir_all(&work_dir).await?;
    write_dash_checkpoint(&meta_path, &state).await?;

    if (audio_representation_id.is_some() || plan.multi_period) && !ffmpeg_available().await {
        return Err(anyhow!("This DASH download needs FFmpeg (separate audio tracks or multiple periods to concatenate). Install FFmpeg or set THUNDERLOAD_FFMPEG to its executable path, then retry."));
    }

    let client = client()?;
    let total_segments = state.total_segments();
    if total_segments == 0 { return Err(anyhow!("DASH MPD did not produce media segments")); }
    let track_extension = |mime_type: &str| if mime_type.to_ascii_lowercase().contains("webm") { "webm" } else { "mp4" };

    // Progress restarts from the segment files already on disk, not from
    // zero: count them and their bytes before any track runs.
    let mut already_completed = 0_usize;
    let mut already_downloaded = 0_u64;
    for (kind, tracks) in [("video", &plan.video_tracks), ("audio", &plan.audio_tracks)] {
        for (period_index, track) in tracks.iter().enumerate() {
            let prefix = format!("{kind}-p{period_index}");
            for index in dash_done_refs(&work_dir, &prefix, track).await {
                already_completed += 1;
                if let Ok(metadata) = fs::metadata(dash_segment_path(&work_dir, &prefix, index)).await {
                    already_downloaded = already_downloaded.saturating_add(metadata.len());
                }
            }
        }
    }
    let completed = Arc::new(AtomicUsize::new(already_completed));
    let downloaded_bytes = Arc::new(AtomicU64::new(already_downloaded));

    // Signed-URL refresh (Slice 4): a 401/403 from a segment fetch
    // usually means the signed URLs expired mid-download. Re-resolve the
    // manifest with the same session and representation ids, swap the
    // fresh URLs into the plan, and retry the tracks that are still
    // missing segments. Done-ness is derived from the segment files on
    // disk, so nothing finished is re-downloaded and the work dir is
    // never wiped on a refresh. After `MAX_SEGMENT_URL_REFRESHES` the
    // last 401/403 is the error. A re-resolve whose segment plan no
    // longer matches the started job is a changed manifest, not an
    // expiry, and fails outright.
    let mut refreshes = 0;
    let mut plan = plan;
    let (video_files, audio_files) = loop {
        match run_dash_tracks(
            &client, &plan, &work_dir, session, &progress_tx,
            completed.clone(), downloaded_bytes.clone(), total_segments, rate_limiter.clone(),
            &control, connections,
        ).await {
            Ok(DashTracksOutcome::Finished { video_files, audio_files }) => break (video_files, audio_files),
            Ok(DashTracksOutcome::Stopped(stopped)) => return Ok(dash_stopped_outcome(stopped, target).await),
            Err(error) if is_auth_error(&error) && refreshes < MAX_SEGMENT_URL_REFRESHES => {
                refreshes += 1;
                let fresh_plan = resolve_dash_plan(manifest_url, representation_id, audio_representation_id, session).await
                    .with_context(|| "Could not refresh the DASH manifest after the signed segment URLs expired")?;
                let fresh_state = DashJobState::from_plan(manifest_url, representation_id, audio_representation_id, session, &fresh_plan);
                if !state.matches_plan(&fresh_state) {
                    return Err(anyhow!("The DASH manifest changed while downloading (its segment list no longer matches the started download); restart the download"));
                }
                plan = fresh_plan;
                write_dash_checkpoint(&meta_path, &fresh_state).await?;
                let _ = progress_tx.send(DownloadProgress {
                    downloaded: downloaded_bytes.load(Ordering::Relaxed), total: None,
                    percent: Some(completed.load(Ordering::Relaxed) as f64 * 100.0 / total_segments.max(1) as f64),
                    phase: "refreshing".into(),
                    segments_done: Some(completed.load(Ordering::Relaxed) as u64), segments_total: Some(total_segments as u64),
                });
            }
            Err(error) if is_auth_error(&error) => {
                return Err(error).context("The signed DASH segment URLs expired and refreshing the manifest did not help; retry the download");
            }
            Err(error) => return Err(error),
        }
    };

    // The segment loops are the only place the control is polled while
    // downloading; check it again around the FFmpeg steps, which can take
    // seconds. A pause here keeps every segment and the checkpoint, so a
    // resume redoes only the cheap assembly and mux.
    if *control.borrow() == DownloadControl::Cancelled {
        remove_dash_resume_files(target).await;
        return Ok(DownloadOutcome::Cancelled);
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok(DownloadOutcome::Paused);
    }
    let video_file = if plan.multi_period {
        let concatenated = work_dir.join(format!("video-concat.{}", track_extension(&plan.video_tracks[0].mime_type)));
        ffmpeg_concat_dash_tracks(&video_files, &concatenated).await?;
        concatenated
    } else {
        video_files.into_iter().next().unwrap()
    };
    if *control.borrow() == DownloadControl::Cancelled {
        remove_dash_resume_files(target).await;
        return Ok(DownloadOutcome::Cancelled);
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok(DownloadOutcome::Paused);
    }
    let output_file = if !audio_files.is_empty() {
        let audio_file = if plan.multi_period {
            let concatenated = work_dir.join(format!("audio-concat.{}", track_extension(&plan.audio_tracks[0].mime_type)));
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
    if *control.borrow() == DownloadControl::Cancelled {
        remove_dash_resume_files(target).await;
        return Ok(DownloadOutcome::Cancelled);
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok(DownloadOutcome::Paused);
    }

    let metadata = fs::metadata(&output_file).await?;
    if metadata.len() == 0 { return Err(anyhow!("DASH assembly produced an empty output file")); }
    // The target was reserved atomically before downloading. Replace only
    // our empty reservation after the file is fully assembled.
    let _ = fs::remove_file(target).await;
    fs::rename(&output_file, target).await
        .with_context(|| format!("Could not move the assembled DASH file to {}", target.display()))?;
    remove_dir_all_retry(&work_dir).await;
    let _ = fs::remove_file(&meta_path).await;
    let _ = progress_tx.send(DownloadProgress {
        downloaded: downloaded_bytes.load(Ordering::Relaxed),
        total: None,
        percent: Some(100.0),
        phase: "complete".into(),
        segments_done: Some(completed.load(Ordering::Relaxed) as u64), segments_total: Some(total_segments as u64),
    });
    Ok(DownloadOutcome::Completed)
}

async fn dash_stopped_outcome(stopped: DownloadControl, target: &Path) -> DownloadOutcome {
    match stopped {
        DownloadControl::Paused => DownloadOutcome::Paused,
        DownloadControl::Cancelled => {
            remove_dash_resume_files(target).await;
            DownloadOutcome::Cancelled
        }
        DownloadControl::Running => unreachable!("a DASH track only stops on pause or cancel"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        dash_output_extension, dash_work_dir, download_dash_into, download_dash_resumable,
        expand_dash_template, hidden_command,
        hls_audio_only_variant, hls_single_variant, is_complete_vod_playlist, parse_dash_document,
        parse_dash_representations, parse_dash_sidx, parse_hls_live_media_playlist,
        parse_hls_master, parse_hls_media_playlist, parse_iso_duration, resolve_dash_track,
        unsupported_hls_master_reason, unsupported_hls_reason, attr, DashCheckpointTrack,
        DashJobState, DownloadControl, DownloadOutcome, partial_download_metadata_path,
        SessionHeaders, DEFAULT_RESUMABLE_CONNECTIONS,
    };
    use crate::download::{reserve_download_path, GlobalRateLimiter};
    use std::collections::{HashMap, HashSet};
    use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, Mutex, RwLock};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};
    use url::Url;
    // The concurrency tracker/barrier are generic test-server helpers
    // defined in hls_download_tests; the DASH fixture reuses them.
    use super::hls_download_tests::{ConcurrencyBarrier, InFlightTracker};

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

    /// Day components are real ISO 8601 durations (Slice 7a, Gemini
    /// 15b): `P1D` is a day, `P0DT0H5M0S` is five minutes. Before the
    /// fix both parsed as None and the MPD duration was silently
    /// dropped. The existing time-only cases and the garbage cases
    /// keep their old answers.
    #[test]
    fn parse_iso_duration_accepts_day_components() {
        assert_eq!(parse_iso_duration("P1D"), Some(86400.0));
        assert_eq!(parse_iso_duration("P0DT0H5M0S"), Some(300.0));
        assert_eq!(parse_iso_duration("P1DT2H"), Some(93600.0));
        assert_eq!(parse_iso_duration("P2DT3H4M5S"), Some(2.0 * 86400.0 + 3.0 * 3600.0 + 4.0 * 60.0 + 5.0));
        assert_eq!(parse_iso_duration("PT5M"), Some(300.0));
        assert_eq!(parse_iso_duration("PT1H30M"), Some(5400.0));
        assert_eq!(parse_iso_duration("PT0.5S"), Some(0.5));
        assert_eq!(parse_iso_duration(""), None);
        assert_eq!(parse_iso_duration("P"), None);
        assert_eq!(parse_iso_duration("PT"), None);
        assert_eq!(parse_iso_duration("PT0S"), None);
        assert_eq!(parse_iso_duration("P1X"), None);
        assert_eq!(parse_iso_duration("PT5X"), None);
        assert_eq!(parse_iso_duration("5M"), None);
        assert_eq!(parse_iso_duration("P1D2H"), None);
    }

    /// A comma inside a quoted attribute value is data, not a
    /// separator (Slice 7a, Gemini 15c): `NAME="English, US"` is one
    /// attribute. Before the fix the split cut it in half and the name
    /// came back as just `English`. Unquoted values are unaffected.
    #[test]
    fn attr_keeps_commas_inside_quoted_values() {
        let attributes = "TYPE=AUDIO,GROUP-ID=\"audio\",LANGUAGE=\"en\",NAME=\"English, US\",DEFAULT=YES,URI=\"audio.m3u8\"";
        assert_eq!(attr(attributes, "NAME").as_deref(), Some("English, US"));
        assert_eq!(attr(attributes, "GROUP-ID").as_deref(), Some("audio"));
        assert_eq!(attr(attributes, "LANGUAGE").as_deref(), Some("en"));
        assert_eq!(attr(attributes, "URI").as_deref(), Some("audio.m3u8"));
        assert_eq!(attr(attributes, "DEFAULT").as_deref(), Some("YES"));
        assert_eq!(attr(attributes, "MISSING"), None);
        let with_query = "BANDWIDTH=800000,URI=\"seg.m3u8?x=1,2\"";
        assert_eq!(attr(with_query, "URI").as_deref(), Some("seg.m3u8?x=1,2"));
        assert_eq!(attr(with_query, "BANDWIDTH").as_deref(), Some("800000"));
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

    // --- Slice 4: AES-128 ---

    #[test]
    fn hls_aes128_media_playlist_parses_key_and_default_iv() {
        let base = Url::parse("https://cdn.example.test/hls/media.m3u8").unwrap();
        // Explicit IV rides on the key; the second segment's key has no
        // IV, so its IV is its media sequence number (MEDIA-SEQUENCE 7 +
        // index 1 = 8) as a 16-byte big-endian integer.
        let playlist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:7\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x000102030405060708090a0b0c0d0e0f\nseg0.ts\n#EXT-X-KEY:METHOD=AES-128,URI=\"key2.bin\"\nseg1.ts\n#EXT-X-KEY:METHOD=NONE\nseg2.ts\n#EXT-X-ENDLIST\n";
        let parsed = super::parse_hls_media_playlist(playlist, &base).unwrap();
        assert_eq!(parsed.segments.len(), 3);
        let key0 = parsed.segments[0].key.as_ref().expect("segment 0 has a key");
        assert_eq!(key0.uri, "https://cdn.example.test/hls/key.bin");
        assert_eq!(key0.iv, Some([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]));
        let key1 = parsed.segments[1].key.as_ref().expect("segment 1 has a key");
        assert_eq!(key1.uri, "https://cdn.example.test/hls/key2.bin");
        let mut expected_iv = [0u8; 16];
        expected_iv[15] = 8;
        assert_eq!(key1.iv, Some(expected_iv));
        // METHOD=NONE clears the key for the segments after it.
        assert!(parsed.segments[2].key.is_none());
    }

    #[test]
    fn hls_aes128_media_playlist_rejects_protected_methods_and_bad_keys() {
        let base = Url::parse("https://cdn.example.test/hls/media.m3u8").unwrap();
        // SAMPLE-AES is protected content: refused, never attempted.
        let sample_aes = "#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key.bin\"\nseg0.ts\n#EXT-X-ENDLIST\n";
        assert!(super::parse_hls_media_playlist(sample_aes, &base).is_err());
        // AES-128 without a URI has no key to fetch.
        let no_uri = "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128\nseg0.ts\n#EXT-X-ENDLIST\n";
        assert!(super::parse_hls_media_playlist(no_uri, &base).is_err());
        // A malformed IV is a playlist error, not a guess.
        let bad_iv = "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x1234\nseg0.ts\n#EXT-X-ENDLIST\n";
        assert!(super::parse_hls_media_playlist(bad_iv, &base).is_err());
    }

    #[test]
    fn hls_aes128_reason_no_longer_flags_aes128_but_flags_sample_aes() {
        // AES-128 is downloadable in-engine now, so it is no longer an
        // unsupported reason; SAMPLE-AES still is.
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\nseg0.ts\n#EXT-X-ENDLIST").is_none());
        assert!(unsupported_hls_reason("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key.bin\"\nseg0.ts\n#EXT-X-ENDLIST").is_some());
    }

    #[test]
    fn hls_segment_identity_ignores_signed_url_query() {
        // The same segment under a rotated signature is the same segment
        // (resume must not restart); a different path is a different one.
        let segment = |url: &str| super::HlsSegmentRef { url: url.to_string(), range: None, key: None };
        assert_eq!(segment("https://cdn.example.test/seg0.ts?token=aaa"), segment("https://cdn.example.test/seg0.ts?token=bbb"));
        assert_ne!(segment("https://cdn.example.test/seg0.ts?token=aaa"), segment("https://cdn.example.test/seg1.ts?token=aaa"));
        // A fragment is never sent to the server, so it is not part of
        // the identity either.
        assert_eq!(segment("https://cdn.example.test/seg0.ts"), segment("https://cdn.example.test/seg0.ts#frag"));
        assert_eq!(segment("https://cdn.example.test/seg0.ts"), segment("https://cdn.example.test/seg0.ts"));
        assert_ne!(
            super::HlsSegmentRef { url: "https://cdn.example.test/seg0.ts".into(), range: Some((0, 100)), key: None },
            super::HlsSegmentRef { url: "https://cdn.example.test/seg0.ts".into(), range: Some((100, 100)), key: None },
        );
    }

    #[tokio::test]
    async fn hls_aes128_segment_decrypts_to_exact_bytes() {
        use aes::Aes128;
        use cbc::{cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit}, Encryptor};
        let key = [0x2b_u8; 16];
        let iv = [0x10_u8; 16];
        // Plaintext spanning several blocks, plus a short tail so the
        // padding path is exercised (1000 bytes = 62 blocks + 8 bytes).
        let plaintext: Vec<u8> = (0..1000).map(|index| (index % 251) as u8).collect();
        let encryptor = Encryptor::<Aes128>::new_from_slices(&key, &iv).unwrap();
        let ciphertext = encryptor.encrypt_padded_vec_mut::<Pkcs7>(&plaintext);
        assert_eq!(ciphertext.len() % 16, 0);
        assert_ne!(ciphertext, plaintext);

        let directory = std::env::temp_dir().join(format!("thunderload-aes128-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let raw = directory.join("seg.raw");
        let out = directory.join("seg.ts");
        tokio::fs::write(&raw, &ciphertext).await.unwrap();
        let decrypted = super::decrypt_aes128_segment(&raw, &out, &key, &iv).await.unwrap();
        assert_eq!(decrypted, plaintext.len() as u64);
        assert_eq!(tokio::fs::read(&out).await.unwrap(), plaintext);

        // Corrupt padding: the last byte lies about the pad length.
        let mut bad_padding = ciphertext.clone();
        let last = bad_padding.len() - 1;
        bad_padding[last] = bad_padding[last].wrapping_add(1);
        let bad_raw = directory.join("bad.raw");
        let bad_out = directory.join("bad.ts");
        tokio::fs::write(&bad_raw, &bad_padding).await.unwrap();
        assert!(super::decrypt_aes128_segment(&bad_raw, &bad_out, &key, &iv).await.is_err());

        // Truncated ciphertext (not a multiple of 16) is an error.
        let truncated = directory.join("trunc.raw");
        tokio::fs::write(&truncated, &ciphertext[..ciphertext.len() - 1]).await.unwrap();
        assert!(super::decrypt_aes128_segment(&truncated, &out, &key, &iv).await.is_err());
        tokio::fs::remove_dir_all(directory).await.unwrap();
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
        spawn_dash_server_with_delay(routes, no_range, Duration::ZERO)
    }

    /// Options for the DASH test fixture, mirroring HlsServerOptions: an
    /// in-flight tracker and a concurrency barrier. Both default to
    /// None (no tracking); only the concurrency tests set them.
    #[derive(Default)]
    struct DashServerOptions {
        in_flight: Option<Arc<InFlightTracker>>,
        barrier: Option<Arc<ConcurrencyBarrier>>,
    }

    /// Same fixture with a fixed delay before every GET response, so a
    /// test can observe progress and pause mid-download instead of racing
    /// a download that finishes in milliseconds on loopback.
    fn spawn_dash_server_with_delay(
        routes: Arc<RwLock<HashMap<String, Vec<u8>>>>,
        no_range: Arc<HashSet<String>>,
        response_delay: Duration,
    ) -> (String, Arc<Mutex<HashMap<String, usize>>>, Arc<Mutex<HashMap<String, usize>>>, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        spawn_dash_server_with_options(routes, no_range, response_delay, DashServerOptions::default())
    }

    /// The delay fixture with tracking options (see DashServerOptions).
    fn spawn_dash_server_with_options(
        routes: Arc<RwLock<HashMap<String, Vec<u8>>>>,
        no_range: Arc<HashSet<String>>,
        response_delay: Duration,
        options: DashServerOptions,
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
        let DashServerOptions { in_flight, barrier } = options;
        let handle = std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let routes = routes.clone();
                let no_range = no_range.clone();
                let counts = counts.clone();
                let range_counts = range_counts.clone();
                let in_flight = in_flight.clone();
                let barrier = barrier.clone();
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
                    // The request is being served from here until its body
                    // is fully written: the guard covers the response delay
                    // and the write. Only media segment fetches are
                    // tracked — the init segment is fetched as part of the
                    // same track and would otherwise pollute peak_paths
                    // (the same fixture artifact the HLS 8-connection
                    // test documents for the playlist).
                    let is_segment = path.contains("/seg-") && path.ends_with(".m4s");
                    let _in_flight = if is_segment {
                        in_flight.as_ref().map(|tracker| tracker.enter(&path))
                    } else {
                        None
                    };
                    // Segment requests wait at the concurrency barrier;
                    // the MPD and init fetches are never gated.
                    if is_segment {
                        if let Some(barrier) = &barrier {
                            barrier.wait_for_all(Duration::from_secs(10));
                        }
                    }
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
                    if !response_delay.is_zero() {
                        std::thread::sleep(response_delay);
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
        let mut command = hidden_command(&executable);
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
        let output = hidden_command("ffprobe")
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

    /// SegmentTemplate MPD for `segment_count` segments: init
    /// `$RepresentationID$/init.mp4`, media
    /// `$RepresentationID$/seg-$Number%03d$.m4s`, timeline `r = count - 1`.
    /// No BaseURL: segment URLs resolve against the manifest URL, so the
    /// test server serves them.
    fn dash_segment_template_mpd(segment_count: usize) -> String {
        format!(r#"<MPD type="static" mediaPresentationDuration="PT{segment_count}S">
          <Period duration="PT{segment_count}S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate timescale="1" startNumber="1" initialization="$RepresentationID$/init.mp4" media="$RepresentationID$/seg-$Number%03d$.m4s">
                <SegmentTimeline><S t="0" d="1" r="{}"/></SegmentTimeline>
              </SegmentTemplate>
              <Representation id="v1" bandwidth="900000" width="640" height="360"/>
            </AdaptationSet>
          </Period>
        </MPD>"#, segment_count - 1)
    }

    async fn run_dash_resumable(
        url: &str,
        target: &std::path::Path,
        control_rx: watch::Receiver<DownloadControl>,
    ) -> anyhow::Result<DownloadOutcome> {
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        download_dash_resumable(url, "v1", None, target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new()), DEFAULT_RESUMABLE_CONNECTIONS).await
    }

    fn fresh_dash_control() -> (watch::Sender<DownloadControl>, watch::Receiver<DownloadControl>) {
        watch::channel(DownloadControl::Running)
    }

    /// Segment file names currently on disk in the DASH work dir (final
    /// names only — in-progress `.tmp` files do not count). Waits until at
    /// least `minimum` exist but fewer than `total`, i.e. mid-download.
    async fn wait_for_dash_segment_files(target: &std::path::Path, minimum: usize, total: usize) -> Vec<String> {
        let work_dir = dash_work_dir(target);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let mut files = Vec::new();
            if let Ok(mut entries) = tokio::fs::read_dir(&work_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.ends_with(".segment") { files.push(name); }
                }
            }
            files.sort();
            if files.len() >= minimum && files.len() < total { return files; }
            assert!(tokio::time::Instant::now() < deadline, "DASH segment files never reached {minimum} of {total} for {}", target.display());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Maps a DASH work-dir segment file back to the request path it was
    /// fetched from, for the SegmentTemplate fixture above (ref 0 is the
    /// init, ref N is seg-N).
    fn dash_segment_file_to_request_path(name: &str) -> String {
        let index: usize = name.trim_start_matches("video-p0-").trim_end_matches(".segment").parse().unwrap();
        if index == 0 { "/v1/init.mp4".to_string() } else { format!("/v1/seg-{index:03}.m4s") }
    }

    #[tokio::test]
    async fn dash_pause_mid_download_then_resume_completes_without_re_requesting_finished_segments() {
        let segment_count = 48;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, _range_counts, shutdown, server) =
            spawn_dash_server_with_delay(routes.clone(), Arc::new(HashSet::new()), Duration::from_millis(10));
        let init = b"FAKEINIT".to_vec();
        let mut expected = init.clone();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), init.clone());
            for index in 1..=segment_count {
                let body = format!("SEGMENT-{index:03}-DATA").into_bytes();
                expected.extend_from_slice(&body);
                map.insert(format!("/v1/seg-{index:03}.m4s"), body);
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-pause-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");

        let (control_tx, control_rx) = fresh_dash_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_dash_resumable(&task_url, &task_target, control_rx).await });
        let done_before_pause = wait_for_dash_segment_files(&target, 1, segment_count + 1).await;
        assert!(!done_before_pause.is_empty() && done_before_pause.len() < segment_count + 1);
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Paused);
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let counts_after_pause = counts.lock().unwrap().clone();
        drop(control_tx);

        let (_control_tx, control_rx) = fresh_dash_control();
        let outcome = run_dash_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        // Segments finished before the pause were not requested again.
        let counts_after_resume = counts.lock().unwrap().clone();
        for name in done_before_pause {
            let key = dash_segment_file_to_request_path(&name);
            assert_eq!(counts_after_resume.get(&key), counts_after_pause.get(&key),
                "segment {key} was re-requested after resume");
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // Slice 7b (Gemini claims 7 + 14): a DASH download at connections=1
    // completes with exact bytes and reports real per-segment counts on
    // the live channel — init + media segments against the plan total.
    #[tokio::test]
    async fn dash_progress_reports_segment_counts_at_one_connection() {
        let segment_count = 6;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let init = b"FAKEINIT".to_vec();
        let mut expected = init.clone();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), init.clone());
            for index in 1..=segment_count {
                let body = format!("SEGMENT-{index:03}-DATA").into_bytes();
                expected.extend_from_slice(&body);
                map.insert(format!("/v1/seg-{index:03}.m4s"), body);
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-segments-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (_control_tx, control_rx) = fresh_dash_control();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(progress) = progress_rx.recv().await {
                events.push(progress);
            }
            events
        });
        let outcome = download_dash_resumable(&url, "v1", None, &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new()), 1).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let events = collector.await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        // Total segments = init + media segments.
        let total = (segment_count + 1) as u64;
        assert!(
            events.iter().any(|event| event.phase == "dash-segments" && event.segments_total == Some(total)),
            "a segment event must report the total segment count; events: {events:?}"
        );
        let last = events.last().expect("at least a complete event");
        assert_eq!(last.phase, "complete");
        assert_eq!((last.segments_done, last.segments_total), (Some(total), Some(total)), "the complete event must report all segments done");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // Slice 7b (Gemini claim 7): the same concurrency proof for DASH
    // at connections=2 — the engine fills exactly 2 segment fetches,
    // not the default 8. The barrier makes the peak deterministic: if
    // the engine opened 8, the peak would read 7+ and fail this test.
    #[tokio::test]
    async fn dash_parallel_fetch_with_two_connections_uses_two_segments() {
        let segment_count = 12;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let tracker = Arc::new(InFlightTracker { current: AtomicUsize::new(0), max: AtomicUsize::new(0), paths: Mutex::new(std::collections::HashSet::new()), peak_paths: Mutex::new(Vec::new()) });
        let barrier = Arc::new(ConcurrencyBarrier { target: 2, arrived: AtomicUsize::new(0) });
        let options = DashServerOptions {
            in_flight: Some(tracker.clone()),
            barrier: Some(barrier),
        };
        let (base, counts, _range_counts, shutdown, server) =
            spawn_dash_server_with_options(routes.clone(), Arc::new(HashSet::new()), Duration::from_millis(10), options);
        let init = b"FAKEINIT".to_vec();
        let mut expected = init.clone();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), init.clone());
            for index in 1..=segment_count {
                let body = format!("SEGMENT-{index:03}-DATA").into_bytes();
                expected.extend_from_slice(&body);
                map.insert(format!("/v1/seg-{index:03}.m4s"), body);
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-concurrency-two-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (_control_tx, control_rx) = fresh_dash_control();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(progress) = progress_rx.recv().await {
                events.push(progress);
            }
            events
        });
        let outcome = download_dash_resumable(&url, "v1", None, &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new()), 2).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let _events = collector.await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        let peak_segments = tracker.peak_paths().iter()
            .filter(|path| path.contains("/seg-") && path.ends_with(".m4s"))
            .count();
        assert_eq!(peak_segments, 2, "expected the engine to fill exactly 2 concurrent segment fetches; in flight at the peak: {:?}", tracker.peak_paths());
        let counts = counts.lock().unwrap();
        for index in 1..=segment_count {
            assert_eq!(counts.get(&format!("/v1/seg-{index:03}.m4s")), Some(&1));
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn dash_error_interrupted_download_resumes_from_disk_checkpoint() {
        // The last four segments are absent (404) on the first call, so the
        // download errors after the first batch — keeping its checkpoint
        // and the finished segments. Which segments finished is read from
        // disk afterwards, never assumed. The first failing segment (9)
        // sits past the first full batch of concurrent segments (8,
        // DEFAULT_RESUMABLE_CONNECTIONS — init + seg 1-7), so at least
        // one segment completes — and its file is written — before the
        // failure aborts the download: with the old concurrency of 6 the
        // fixture used 6 for the same reason.
        let segment_count = 12;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let init = b"FAKEINIT".to_vec();
        let mut expected = init.clone();
        let mut missing = Vec::new();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), init.clone());
            for index in 1..=segment_count {
                let body = format!("SEGMENT-{index:03}-DATA").into_bytes();
                expected.extend_from_slice(&body);
                if index <= 8 {
                    map.insert(format!("/v1/seg-{index:03}.m4s"), body);
                } else {
                    missing.push((format!("/v1/seg-{index:03}.m4s"), body));
                }
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-error-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");

        let (control_tx, control_rx) = fresh_dash_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_dash_resumable(&task_url, &task_target, control_rx).await });
        let result = task.await.unwrap();
        assert!(result.is_err(), "expected the missing segments to abort the download");
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let done_before_error = wait_for_dash_segment_files(&target, 1, segment_count + 1).await;
        assert!(!done_before_error.is_empty(), "at least the first batch must have finished before the error");
        let counts_after_error = counts.lock().unwrap().clone();
        drop(control_tx);

        // Server recovers; a fresh call (fresh process equivalent) resumes.
        {
            let mut map = routes.write().unwrap();
            for (path, body) in missing { map.insert(path, body); }
        }
        let (_control_tx, control_rx) = fresh_dash_control();
        let outcome = run_dash_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected);
        let counts_after_resume = counts.lock().unwrap().clone();
        for name in done_before_error {
            let key = dash_segment_file_to_request_path(&name);
            assert_eq!(counts_after_resume.get(&key), counts_after_error.get(&key),
                "segment {key} was re-requested after the error-interrupted resume");
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// A segment that fails after its temp file is created must not
    /// leave the temp behind (Slice 7a, claim 5 nit). Segment 2 is
    /// served with an empty body, so its task creates
    /// `video-p0-000002.segment.tmp` and then errors on the empty
    /// file; the download aborts, but the finished segments and the
    /// checkpoint stay for resume. Before the fix the empty segment's
    /// `.segment.tmp` survived in the work dir.
    #[tokio::test]
    async fn dash_failed_segment_leaves_no_temp_files() {
        let segment_count = 4;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), b"FAKEINIT".to_vec());
            for index in 1..=segment_count {
                // Segment 2 is empty on purpose: its task errors only
                // after creating its temp file.
                let body = if index == 2 { Vec::new() } else { format!("SEGMENT-{index:03}-DATA").into_bytes() };
                map.insert(format!("/v1/seg-{index:03}.m4s"), body);
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");

        let (_control_tx, control_rx) = fresh_dash_control();
        let result = run_dash_resumable(&url, &target, control_rx).await;
        assert!(result.is_err(), "expected the empty segment to abort the download");
        // The checkpoint and the finished segments survive for resume.
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let work_dir = dash_work_dir(&target);
        let mut temps = Vec::new();
        let mut finished = Vec::new();
        let mut entries = tokio::fs::read_dir(&work_dir).await.unwrap();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".segment.tmp") {
                temps.push(name);
            } else if name.ends_with(".segment") {
                finished.push(name);
            }
        }
        assert!(temps.is_empty(), "temp files left behind in the work dir: {temps:?}");
        assert_eq!(finished.len(), 4, "init + segments 1, 3, 4 must still be on disk: {finished:?}");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // --- Slice 4: signed-URL refresh ---

    /// A test server that simulates signed-URL expiry for DASH: the MPD
    /// it serves embeds segment URLs with a `?token=N` query. A segment
    /// request succeeds only when its token is the latest one the MPD has
    /// issued AND that token is not the first one (token 0 is always
    /// expired — the URLs the first resolve returns are already stale).
    /// The first resolve therefore 403s every segment; the refresh
    /// re-resolves, gets token 1, and completes. When `never_valid` is
    /// set, no token is ever accepted, so the refreshes are exhausted
    /// and the download fails. `mpd_fetches` counts MPD GETs;
    /// `segment_counts` counts segment GETs per path.
    fn spawn_signed_dash_server(
        segment_count: usize,
        never_valid: bool,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>, Arc<Mutex<HashMap<String, usize>>>, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let epoch = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mpd_fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let segment_counts = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_epoch = epoch.clone();
        let thread_fetches = mpd_fetches.clone();
        let thread_counts = segment_counts.clone();
        let thread_shutdown = shutdown.clone();
        let handle = std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let epoch = thread_epoch.clone();
                let fetches = thread_fetches.clone();
                let counts = thread_counts.clone();
                std::thread::spawn(move || loop {
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
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            Err(_) => break false,
                        }
                    };
                    if !head_complete { return; }
                    let request = String::from_utf8_lossy(&raw);
                    let target = request.split_whitespace().nth(1).unwrap_or("/").to_string();
                    let path = target.split('?').next().unwrap_or(&target).to_string();
                    let token = target.split("token=").nth(1)
                        .and_then(|value| value.split('&').next())
                        .and_then(|value| value.parse::<usize>().ok());
                    if path == "/video.mpd" {
                        let issued = epoch.fetch_add(1, Ordering::Relaxed);
                        fetches.fetch_add(1, Ordering::Relaxed);
                        let mpd = format!(r#"<MPD type="static" mediaPresentationDuration="PT{segment_count}S">
                          <Period duration="PT{segment_count}S">
                            <AdaptationSet contentType="video" mimeType="video/mp4">
                              <SegmentTemplate timescale="1" startNumber="1" initialization="init.mp4" media="seg-$Number$.m4s?token={issued}">
                                <SegmentTimeline><S t="0" d="1" r="{}"/></SegmentTimeline>
                              </SegmentTemplate>
                              <Representation id="v1" bandwidth="900000" width="640" height="360"/>
                            </AdaptationSet>
                          </Period>
                        </MPD>"#, segment_count - 1);
                        let body = mpd.into_bytes();
                        let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n", body.len()).as_bytes());
                        let _ = stream.write_all(&body);
                        continue;
                    }
                    if path == "/init.mp4" {
                        let body = b"FAKEINIT";
                        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: keep-alive\r\n\r\n");
                        let _ = stream.write_all(body);
                        continue;
                    }
                    if let Some(number) = path.strip_prefix("/seg-").and_then(|rest| rest.strip_suffix(".m4s")).and_then(|value| value.parse::<usize>().ok()) {
                        {
                            let mut counts = counts.lock().unwrap();
                            *counts.entry(path.clone()).or_insert(0) += 1;
                        }
                        let latest = epoch.load(Ordering::Relaxed).saturating_sub(1);
                        let valid = !never_valid && token == Some(latest) && latest >= 1;
                        if valid {
                            let body = format!("SEGMENT-{number:03}-DATA").into_bytes();
                            let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n", body.len()).as_bytes());
                            let _ = stream.write_all(&body);
                        } else {
                            let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n");
                        }
                        continue;
                    }
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    return;
                });
            }
        });
        (format!("http://{address}"), mpd_fetches, segment_counts, shutdown, handle)
    }

    #[tokio::test]
    async fn dash_segment_url_refresh_on_403_recovers() {
        let segment_count = 4;
        let (base, mpd_fetches, segment_counts, shutdown, server) =
            spawn_signed_dash_server(segment_count, false);
        let directory = dash_test_dir("refresh");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (_control_tx, control_rx) = fresh_dash_control();
        let outcome = run_dash_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = b"FAKEINIT".to_vec();
        for index in 1..=segment_count {
            expected.extend_from_slice(format!("SEGMENT-{index:03}-DATA").as_bytes());
        }
        assert_eq!(bytes, expected);
        // Exactly one refresh: the first MPD's token-0 URLs were all
        // refused, the second MPD's token-1 URLs served every segment.
        assert_eq!(mpd_fetches.load(Ordering::Relaxed), 2, "expected exactly one playlist refresh");
        let counts = segment_counts.lock().unwrap();
        for index in 1..=segment_count {
            assert_eq!(counts.get(&format!("/seg-{index}.m4s")), Some(&2),
                "segment {index} should be fetched twice: once refused, once served");
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn dash_refresh_exhaustion_fails_after_two_refreshes() {
        let segment_count = 4;
        let (base, mpd_fetches, _segment_counts, shutdown, server) =
            spawn_signed_dash_server(segment_count, true);
        let directory = dash_test_dir("refresh-exhaust");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (_control_tx, control_rx) = fresh_dash_control();
        let result = run_dash_resumable(&url, &target, control_rx).await;
        assert!(result.is_err(), "a manifest whose URLs never become valid must fail");
        let message = result.unwrap_err().to_string();
        assert!(message.contains("expired"), "expected the expiry error, got: {message}");
        // Initial resolve + exactly two refreshes, then give up — never
        // a loop.
        assert_eq!(mpd_fetches.load(Ordering::Relaxed), 3, "expected initial resolve + 2 refreshes");
        // The checkpoint is kept, so a retry resumes instead of restarting.
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // --- Slice 4: prompt pause/cancel (Gemini claim 5) ---

    #[tokio::test]
    async fn dash_pause_is_prompt_during_slow_segment() {
        // Every response takes 3 s (response_delay). With the old bare
        // `join_next()` wait, a pause sent 300 ms in would return only
        // after the first in-flight segment finished (~3 s). With the
        // select! on control.changed() it returns in well under a second.
        let segment_count = 4;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server_with_delay(routes.clone(), Arc::new(HashSet::new()), Duration::from_secs(3));
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), b"FAKEINIT".to_vec());
            for index in 1..=segment_count {
                map.insert(format!("/v1/seg-{index:03}.m4s"), format!("SEGMENT-{index:03}-DATA").into_bytes());
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = dash_test_dir("prompt-pause");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (control_tx, control_rx) = fresh_dash_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_dash_resumable(&task_url, &task_target, control_rx).await });
        // The response delay applies to the MPD fetch too, so the pause
        // must wait until the manifest is resolved and the track loop has
        // started: the checkpoint sidecar is written right before the
        // first segment is fetched, so its appearance means the slow
        // segment fetches are in flight.
        let meta_path = partial_download_metadata_path(&target);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if tokio::fs::try_exists(&meta_path).await.unwrap_or(false) { break; }
            assert!(tokio::time::Instant::now() < deadline, "checkpoint never appeared");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let started = tokio::time::Instant::now();
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Paused);
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(2),
            "pause took {elapsed:?} — it must return promptly during a slow segment, not after it finishes");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn dash_cancel_removes_partial_files() {
        let segment_count = 48;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server_with_delay(routes.clone(), Arc::new(HashSet::new()), Duration::from_millis(10));
        let init = b"FAKEINIT".to_vec();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), init.clone());
            for index in 1..=segment_count {
                map.insert(format!("/v1/seg-{index:03}.m4s"), format!("SEGMENT-{index:03}-DATA").into_bytes());
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-cancel-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (control_tx, control_rx) = fresh_dash_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_dash_resumable(&task_url, &task_target, control_rx).await });
        let done = wait_for_dash_segment_files(&target, 1, segment_count + 1).await;
        assert!(!done.is_empty());
        control_tx.send(DownloadControl::Cancelled).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Cancelled);
        assert!(!tokio::fs::try_exists(&target).await.unwrap());
        assert!(!tokio::fs::try_exists(dash_work_dir(&target)).await.unwrap());
        assert!(!tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn dash_changed_plan_restarts_from_scratch() {
        // Pause mid-download, then change the MPD to a different segment
        // plan (different URLs and bytes). The resume must discard the
        // stale checkpoint and its segment files and download the new plan
        // from scratch — never join two different files.
        let segment_count = 12;
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, counts, _range_counts, shutdown, server) =
            spawn_dash_server_with_delay(routes.clone(), Arc::new(HashSet::new()), Duration::from_millis(10));
        let init = b"FAKEINIT".to_vec();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), init.clone());
            for index in 1..=segment_count {
                map.insert(format!("/v1/seg-{index:03}.m4s"), format!("SEGMENT-{index:03}-DATA").into_bytes());
            }
            map.insert("/video.mpd".into(), dash_segment_template_mpd(segment_count).into_bytes());
        }
        let directory = std::env::temp_dir().join(format!("thunderload-dash-plan-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let url = format!("{base}/video.mpd");
        let (control_tx, control_rx) = fresh_dash_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_dash_resumable(&task_url, &task_target, control_rx).await });
        let done_before_pause = wait_for_dash_segment_files(&target, 1, segment_count + 1).await;
        assert!(!done_before_pause.is_empty());
        control_tx.send(DownloadControl::Paused).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), DownloadOutcome::Paused);
        let counts_after_pause = counts.lock().unwrap().clone();
        drop(control_tx);

        let new_init = b"NEW-INIT!".to_vec();
        let mut new_expected = new_init.clone();
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/n-init.mp4".into(), new_init.clone());
            for index in 1..=segment_count {
                let body = format!("NEW-SEGMENT-{index:03}").into_bytes();
                new_expected.extend_from_slice(&body);
                map.insert(format!("/v1/n-{index:03}.m4s"), body);
            }
            let mpd = dash_segment_template_mpd(segment_count)
                .replace("$RepresentationID$/init.mp4", "$RepresentationID$/n-init.mp4")
                .replace("$RepresentationID$/seg-$Number%03d$.m4s", "$RepresentationID$/n-$Number%03d$.m4s");
            map.insert("/video.mpd".into(), mpd.into_bytes());
        }
        let (_control_tx, control_rx) = fresh_dash_control();
        let outcome = run_dash_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, new_expected);
        // The new plan was fetched from scratch: every new segment was
        // requested exactly once, and no stale segment was reused.
        let counts_after_resume = counts.lock().unwrap().clone();
        for index in 1..=segment_count {
            assert_eq!(counts_after_resume.get(&format!("/v1/n-{index:03}.m4s")), Some(&1));
        }
        assert_eq!(counts_after_resume.get("/v1/n-init.mp4"), Some(&1));
        for name in done_before_pause {
            let key = dash_segment_file_to_request_path(&name);
            assert_eq!(counts_after_resume.get(&key), counts_after_pause.get(&key),
                "stale segment {key} was re-requested after the plan change");
        }
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[test]
    fn dash_checkpoint_matches_only_the_same_plan() {
        let track = DashCheckpointTrack {
            mime_type: "video/mp4".into(),
            initialization_url: Some("https://cdn.example.test/init.mp4".into()),
            initialization_range: None,
            segment_urls: vec!["https://cdn.example.test/seg-1.m4s".into()],
            segment_ranges: vec![None],
        };
        let state = DashJobState {
            version: 1,
            manifest_url: "https://cdn.example.test/video.mpd".into(),
            representation_id: "v1".into(),
            audio_representation_id: None,
            session: None,
            video_tracks: vec![track.clone()],
            audio_tracks: vec![],
            multi_period: false,
        };
        assert!(state.matches_plan(&state));
        let mut changed = state.clone();
        changed.video_tracks[0].segment_urls = vec!["https://cdn.example.test/seg-9.m4s".into()];
        assert!(!state.matches_plan(&changed));
        let mut changed = state.clone();
        changed.representation_id = "v2".into();
        assert!(!state.matches_plan(&changed));
        let mut changed = state.clone();
        changed.manifest_url = "https://cdn.example.test/other.mpd".into();
        assert!(!state.matches_plan(&changed));
    }

    #[test]
    fn dash_checkpoint_matches_ignores_referer_and_user_agent_changes() {
        let track = DashCheckpointTrack {
            mime_type: "video/mp4".into(),
            initialization_url: Some("https://cdn.example.test/init.mp4".into()),
            initialization_range: None,
            segment_urls: vec!["https://cdn.example.test/seg-1.m4s".into()],
            segment_ranges: vec![None],
        };
        let state = DashJobState {
            version: 1,
            manifest_url: "https://cdn.example.test/video.mpd".into(),
            representation_id: "v1".into(),
            audio_representation_id: None,
            session: Some(SessionHeaders {
                cookies: Some("sessionid=abc123".to_string()),
                referer: Some("https://cdn.example.test/watch".to_string()),
                user_agent: Some("TestAgent/1.0".to_string()),
                origin: Some("https://cdn.example.test".to_string()),
            }),
            video_tracks: vec![track],
            audio_tracks: vec![],
            multi_period: false,
        };
        assert!(state.matches_plan(&state));
        // A referer/UA-only change still matches (F4): the checkpoint is
        // reused instead of restarting and deleting finished segments.
        let mut changed = state.clone();
        let session = changed.session.as_mut().unwrap();
        session.referer = Some("https://cdn.example.test/other-page".to_string());
        session.user_agent = Some("OtherAgent/2.0".to_string());
        assert!(state.matches_plan(&changed));
        // A credential change does not match: the job restarts.
        let mut changed = state.clone();
        changed.session.as_mut().unwrap().cookies = Some("sessionid=zzz".to_string());
        assert!(!state.matches_plan(&changed));
        let mut changed = state.clone();
        changed.session.as_mut().unwrap().origin = Some("https://other.example.test".to_string());
        assert!(!state.matches_plan(&changed));
        // A session appearing or disappearing does not match either.
        let mut changed = state.clone();
        changed.session = None;
        assert!(!state.matches_plan(&changed));
    }

    #[tokio::test]
    async fn dash_output_extension_matches_track_container() {
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        let webm_mpd = dash_segment_template_mpd(1).replace("video/mp4", "video/webm");
        let mp4_mpd = dash_segment_template_mpd(1);
        let av_mpd = r#"<MPD type="static" mediaPresentationDuration="PT1S">
          <Period duration="PT1S">
            <AdaptationSet contentType="video" mimeType="video/webm">
              <SegmentTemplate timescale="1" startNumber="1" initialization="v1/init.mp4" media="v1/seg-$Number%03d$.m4s">
                <SegmentTimeline><S t="0" d="1" r="0"/></SegmentTimeline>
              </SegmentTemplate>
              <Representation id="v1" bandwidth="900000" width="640" height="360"/>
            </AdaptationSet>
            <AdaptationSet contentType="audio" mimeType="audio/mp4">
              <SegmentTemplate timescale="1" startNumber="1" initialization="a1/init.mp4" media="a1/seg-$Number%03d$.m4s">
                <SegmentTimeline><S t="0" d="1" r="0"/></SegmentTimeline>
              </SegmentTemplate>
              <Representation id="a1" bandwidth="128000"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        {
            let mut map = routes.write().unwrap();
            map.insert("/video.mpd".into(), webm_mpd.into_bytes());
        }
        assert_eq!(dash_output_extension(&format!("{base}/video.mpd"), "v1", None, None).await.unwrap(), "webm");
        {
            let mut map = routes.write().unwrap();
            map.insert("/video.mpd".into(), mp4_mpd.into_bytes());
        }
        assert_eq!(dash_output_extension(&format!("{base}/video.mpd"), "v1", None, None).await.unwrap(), "mp4");
        {
            let mut map = routes.write().unwrap();
            map.insert("/video.mpd".into(), av_mpd.as_bytes().to_vec());
        }
        assert_eq!(dash_output_extension(&format!("{base}/video.mpd"), "v1", Some("a1"), None).await.unwrap(), "mp4");
        stop_dash_server(shutdown, server);
    }

    #[tokio::test]
    async fn dash_one_shot_download_removes_partial_files_on_error() {
        // The one-shot wrapper keeps the old contract: a failed call
        // leaves no target, work directory, or checkpoint behind.
        let routes: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
        let (base, _counts, _range_counts, shutdown, server) =
            spawn_dash_server(routes.clone(), Arc::new(HashSet::new()));
        {
            let mut map = routes.write().unwrap();
            map.insert("/v1/init.mp4".into(), b"FAKEINIT".to_vec());
            map.insert("/v1/seg-001.m4s".into(), b"SEGMENT-001-DATA".to_vec());
            map.insert("/video.mpd".into(), dash_segment_template_mpd(2).into_bytes());
        }
        let out_dir = dash_test_dir("one-shot-error");
        tokio::fs::create_dir_all(&out_dir).await.unwrap();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_dash_into(
            &format!("{base}/video.mpd"), "v1", None, "cleanup-video", None,
            progress_tx, Arc::new(GlobalRateLimiter::new()), &out_dir,
        ).await;
        assert!(result.is_err(), "expected the missing second segment to fail the download");
        assert!(dash_out_dir_is_empty(&out_dir).await, "a failed one-shot download must not leave files behind");
        stop_dash_server(shutdown, server);
        tokio::fs::remove_dir_all(out_dir).await.unwrap();
    }

    // --- Slice 6: live / subtitles / audio-only / fps ---

    #[test]
    fn hls_master_subtitles_and_frame_rate_surface_on_variants() {
        let playlist = "#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"subs-en.m3u8\"\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",LANGUAGE=\"es\",NAME=\"Spanish\",URI=\"subs-es.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720,FRAME-RATE=30.000\n720.m3u8\n";
        let base = Url::parse("https://cdn.example.test/master.m3u8").unwrap();
        let variants = parse_hls_master(playlist, &base).unwrap();
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].quality, "720p");
        assert_eq!(variants[0].fps, Some(30.0));
        let subtitles = variants[0].subtitles.as_ref().expect("subtitle renditions must ride on the variant");
        assert_eq!(subtitles.len(), 2);
        assert_eq!(subtitles[0].language, "en");
        assert_eq!(subtitles[0].name, "English");
        assert_eq!(subtitles[0].format, "m3u8");
        assert!(subtitles[0].url.ends_with("/subs-en.m3u8"));
        assert_eq!(subtitles[1].language, "es");
    }

    #[test]
    fn hls_master_audio_only_row_uses_default_rendition() {
        let playlist = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"audio-en.m3u8\"\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",LANGUAGE=\"es\",NAME=\"Spanish\",URI=\"audio-es.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720,AUDIO=\"audio\"\n720.m3u8\n";
        let base = Url::parse("https://cdn.example.test/master.m3u8").unwrap();
        let row = hls_audio_only_variant(playlist, &base).expect("an audio row must exist");
        assert_eq!(row.quality, "Audio");
        assert_eq!(row.audio_only, Some(true));
        // The DEFAULT rendition wins over the merely-first one.
        assert!(row.url.ends_with("/audio-en.m3u8"));
        assert_eq!(row.representation_id.as_deref(), Some("audio"));
    }

    #[test]
    fn hls_live_media_playlist_parses_but_vod_parser_rejects_it() {
        let live = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:42\n#EXTINF:6.0,\nseg42.ts\n#EXTINF:6.0,\nseg43.ts\n";
        let base = Url::parse("https://cdn.example.test/live.m3u8").unwrap();
        let parsed = parse_hls_live_media_playlist(live, &base).unwrap();
        assert_eq!(parsed.segments.len(), 2);
        assert_eq!(parsed.media_sequence, 42);
        assert!(parsed.init.is_none());
        assert!(parse_hls_media_playlist(live, &base).is_err(), "the VOD parser must still refuse a live playlist");
        let vod = format!("{live}#EXT-X-ENDLIST\n");
        assert!(parse_hls_media_playlist(&vod, &base).is_ok(), "the same playlist with ENDLIST is VOD");
    }

    #[test]
    fn hls_single_variant_labels_live_ts_and_refuses_live_fmp4() {
        let live_ts = "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:6.0,\nseg0.ts\n";
        let variant = hls_single_variant("https://cdn.example.test/live.m3u8", live_ts);
        assert_eq!(variant.is_live, Some(true));
        assert_eq!(variant.quality, "Live");
        assert!(variant.downloadable, "live TS records as it streams");
        assert!(variant.note.as_deref().unwrap().contains("records the stream"));
        let live_fmp4 = "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:6.0,\nseg0.m4s\n";
        let variant = hls_single_variant("https://cdn.example.test/live.m3u8", live_fmp4);
        assert_eq!(variant.is_live, Some(true));
        assert!(!variant.downloadable, "live fMP4 cannot be appended into a playable file");
        assert!(variant.note.as_deref().unwrap().contains("can't be recorded yet"));
    }

    #[test]
    fn dash_live_mpd_is_labelled_live_and_not_downloadable() {
        let dynamic = r#"<MPD type="dynamic"><Period><AdaptationSet contentType="video"><Representation id="v1" height="720"/></AdaptationSet></Period></MPD>"#;
        let variants = parse_dash_representations(dynamic, "https://cdn.example.test/video.mpd");
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].is_live, Some(true));
        assert_eq!(variants[0].quality, "Live");
        assert!(!variants[0].downloadable);
        assert!(variants[0].note.as_deref().unwrap().contains("can't be recorded yet"));
    }

    #[test]
    fn dash_static_mpd_surfaces_subtitles_and_frame_rate() {
        let manifest = r#"<MPD type="static"><Period>
            <AdaptationSet contentType="video" mimeType="video/mp4"><Representation id="v1" height="720" frameRate="30"/></AdaptationSet>
            <AdaptationSet contentType="text" mimeType="text/vtt"><Representation id="sub-en" mimeType="text/vtt"><BaseURL>subs-en.vtt</BaseURL></Representation></AdaptationSet>
            <AdaptationSet contentType="text" mimeType="application/ttml+xml"><Representation id="sub-de"><BaseURL>subs-de.ttml</BaseURL></Representation></AdaptationSet>
        </Period></MPD>"#;
        let variants = parse_dash_representations(manifest, "https://cdn.example.test/video.mpd");
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].fps, Some(30.0));
        let subtitles = variants[0].subtitles.as_ref().expect("text adaptations must surface as subtitles");
        assert_eq!(subtitles.len(), 2);
        assert_eq!(subtitles[0].language, "sub-en");
        assert_eq!(subtitles[0].format, "vtt");
        assert!(subtitles[0].url.ends_with("/subs-en.vtt"));
        assert_eq!(subtitles[1].format, "ttml");
    }
}


#[cfg(test)]
mod hls_download_tests {
    use super::{
        download_hls_live, download_hls_resumable, download_hls_subtitles, ffmpeg_available, hidden_command, hls_output_extension, hls_work_dir,
        inspect_video, partial_download_metadata_path, validate_hls_content_range,
        DownloadControl, DownloadOutcome, GlobalRateLimiter, HlsSelection, SessionHeaders,
        DEFAULT_RESUMABLE_CONNECTIONS,
    };
    use crate::session::AUTH_REQUIRED_MESSAGE;
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
        /// When set, every request must carry exactly this Cookie
        /// header value or the server answers 403 (exercises the
        /// session handoff: playlist + segment fetches all need it).
        require_cookie: Option<String>,
        /// When set, tracks how many requests are being served
        /// concurrently and the peak (proves the segment-fetch
        /// concurrency: the client must fill all of the engine's
        /// connections, no more).
        in_flight: Option<Arc<InFlightTracker>>,
        /// When set, segment requests wait at this barrier until
        /// `target` of them have arrived (or the timeout passes) before
        /// being served. Without it the peak-concurrency measurement is a
        /// race against scheduling jitter: on a loaded machine the eighth
        /// request can arrive after the first segment finished, so the
        /// peak reads 7 for a correct engine. With the barrier the peak
        /// is a synchronization proof instead — if the engine only ever
        /// opens 6 connections, the barrier times out and the peak stays
        /// 6. The barrier applies to segment paths only, never to the
        /// playlist fetch.
        barrier: Option<Arc<ConcurrencyBarrier>>,
    }

    /// Barrier for the concurrency proof: segment handlers wait until
    /// `target` of them have arrived before serving, so the peak
    /// in-flight count is deterministic instead of a scheduling race.
    /// A target that is never reached times out (no deadlock) and the
    /// handlers serve anyway, so the peak honestly reports how many
    /// connections the client actually opened.
    ///
    /// Shared with the DASH fixture in `super::tests` (generic
    /// test-server helpers, not HLS-specific), hence `pub(super)`.
    pub(super) struct ConcurrencyBarrier {
        pub(super) target: usize,
        pub(super) arrived: AtomicUsize,
    }

    impl ConcurrencyBarrier {
        pub(super) fn wait_for_all(&self, timeout: Duration) {
            let arrived = self.arrived.fetch_add(1, Ordering::Relaxed) + 1;
            if arrived >= self.target {
                return;
            }
            let deadline = std::time::Instant::now() + timeout;
            while self.arrived.load(Ordering::Relaxed) < self.target {
                if std::time::Instant::now() >= deadline {
                    return;
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
    }

    /// Concurrent-request tracker for the test servers: `current` is the
    /// number of requests being served right now, `max` the peak seen.
    /// `peak_paths` is the set of request paths that were in flight when
    /// the peak was recorded, so a surprising peak can be diagnosed
    /// instead of guessed at. Shared with the DASH fixture in
    /// `super::tests`, hence `pub(super)`.
    pub(super) struct InFlightTracker {
        pub(super) current: AtomicUsize,
        pub(super) max: AtomicUsize,
        pub(super) paths: Mutex<std::collections::HashSet<String>>,
        pub(super) peak_paths: Mutex<Vec<String>>,
    }

    impl InFlightTracker {
        pub(super) fn enter(&self, path: &str) -> InFlightGuard<'_> {
            let current = self.current.fetch_add(1, Ordering::Relaxed) + 1;
            {
                let mut paths = self.paths.lock().unwrap();
                paths.insert(path.to_string());
                if current > self.max.load(Ordering::Relaxed) {
                    self.max.store(current, Ordering::Relaxed);
                    *self.peak_paths.lock().unwrap() = paths.iter().cloned().collect();
                }
            }
            InFlightGuard { tracker: self, path: path.to_string() }
        }

        fn max(&self) -> usize {
            self.max.load(Ordering::Relaxed)
        }

        pub(super) fn peak_paths(&self) -> Vec<String> {
            let mut paths = self.peak_paths.lock().unwrap().clone();
            paths.sort();
            paths
        }
    }

    pub(super) struct InFlightGuard<'a> {
        tracker: &'a InFlightTracker,
        path: String,
    }

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.tracker.current.fetch_sub(1, Ordering::Relaxed);
            self.tracker.paths.lock().unwrap().remove(&self.path);
        }
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
        let HlsServerOptions { fail_from_index, ignore_range_paths, content_range_overrides, require_cookie, in_flight, barrier } = options;
        let routes = Arc::new(routes);
        let thread_counts = counts.clone();
        let thread_range = range_206.clone();
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
                let routes = routes.clone();
                let counts = thread_counts.clone();
                let range_206 = thread_range.clone();
                let fail_from_index = fail_from_index.clone();
                let ignore_range_paths = ignore_range_paths.clone();
                let content_range_overrides = content_range_overrides.clone();
                let require_cookie = require_cookie.clone();
                let in_flight = in_flight.clone();
                let barrier = barrier.clone();
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
                    // The request is being served from here until its
                    // body is fully written (or the handler returns
                    // early): the guard covers every response path.
                    let _in_flight = in_flight.as_ref().map(|tracker| tracker.enter(&path_only));
                    // Segment requests wait at the concurrency barrier
                    // (see the option's doc comment); the playlist fetch
                    // is never gated.
                    if let Some(barrier) = &barrier {
                        if path_only.starts_with("/seg") && path_only.ends_with(".ts") {
                            barrier.wait_for_all(Duration::from_secs(10));
                        }
                    }
                    if let Some(required) = &require_cookie {
                        let cookie = request.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("cookie").then(|| value.trim().to_string())
                        }).unwrap_or_default();
                        if cookie != *required {
                            let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            return;
                        }
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
        run_resumable_with_session(url, target, control_rx, None).await
    }

    async fn run_resumable_with_session(
        url: &str,
        target: &std::path::Path,
        control_rx: watch::Receiver<DownloadControl>,
        session: Option<SessionHeaders>,
    ) -> anyhow::Result<DownloadOutcome> {
        run_resumable_with_selection_and_session(url, target, control_rx, HlsSelection::none(), session).await
    }

    async fn run_resumable_with_selection_and_session(
        url: &str,
        target: &std::path::Path,
        control_rx: watch::Receiver<DownloadControl>,
        selection: HlsSelection,
        session: Option<SessionHeaders>,
    ) -> anyhow::Result<DownloadOutcome> {
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        download_hls_resumable(url, &selection, target, session.as_ref(), control_rx, progress_tx, Arc::new(GlobalRateLimiter::new()), DEFAULT_RESUMABLE_CONNECTIONS).await
    }

    // Slice 7b (Gemini claim 7): the same resumable HLS path, but the
    // caller chooses the connection count instead of the default.
    async fn run_resumable_with_connections(
        url: &str,
        target: &std::path::Path,
        control_rx: watch::Receiver<DownloadControl>,
        connections: usize,
    ) -> anyhow::Result<DownloadOutcome> {
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        download_hls_resumable(url, &HlsSelection::none(), target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new()), connections).await
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
        let mut command = hidden_command(&executable);
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
        let output = hidden_command("ffprobe")
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
    async fn hls_output_extension_matches_playlist_container() {
        // Plain TS playlist (no EXT-X-MAP) -> "ts"; fMP4 playlist
        // (EXT-X-MAP init) -> "mp4". The native host reserves the target
        // from this, so a TS stream is not misnamed .mp4.
        let (mut routes, _bodies) = segment_routes(2, 64);
        routes.insert("/ts.m3u8".into(), media_playlist(2).into_bytes());
        routes.insert("/init.mp4".into(), b"fake-init".to_vec());
        routes.insert("/seg0.m4s".into(), b"fake-fragment-0".to_vec());
        routes.insert("/seg1.m4s".into(), b"fake-fragment-1".to_vec());
        routes.insert("/fmp4.m3u8".into(), fmp4_media_playlist("init.mp4", &["seg0.m4s".to_string(), "seg1.m4s".to_string()]).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        assert_eq!(hls_output_extension(&format!("{base}/ts.m3u8"), &HlsSelection::none(), None).await.unwrap(), "ts");
        assert_eq!(hls_output_extension(&format!("{base}/fmp4.m3u8"), &HlsSelection::none(), None).await.unwrap(), "mp4");
        stop_server(shutdown, server);
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
    async fn hls_resume_reuses_checkpoint_session_when_caller_has_none() {
        let (mut routes, bodies) = segment_routes(24, 8192);
        routes.insert("/media.m3u8".into(), media_playlist(24).into_bytes());
        let options = HlsServerOptions {
            require_cookie: Some("sessionid=abc123".to_string()),
            ..Default::default()
        };
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-session-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let session = SessionHeaders {
            cookies: Some("sessionid=abc123".to_string()),
            referer: None,
            user_agent: None,
            origin: SessionHeaders::origin_of(&url),
        };
        assert!(session.validate(&url).is_ok(), "test session must validate");

        // Phase 1: start with the session, pause mid-flight — the
        // checkpoint now holds the handoff.
        let (control_tx, control_rx) = fresh_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_resumable_with_session(&task_url, &task_target, control_rx, Some(session)).await });
        let done_before_pause = wait_for_checkpoint_progress(&target, 1).await;
        assert!(!done_before_pause.is_empty() && done_before_pause.len() < 24);
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Paused);
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        let counts_after_pause = counts.lock().unwrap().clone();
        drop(control_tx);

        // Phase 2: resume with NO session — a desktop resume of a
        // browser-started job mints a fresh job id, so no stored session
        // is available. The checkpoint must supply it (F2): without the
        // fix the playlist fetch goes out without cookies and the server
        // answers 403.
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

    /// Slice 5: a 401/403 from inspect is the login wall, not a bare
    /// status — the panel must show the login label. Covers both
    /// inspect paths: the manifest fetch (fetch_text_with) and the
    /// HEAD/one-byte-GET probe for extensionless URLs.
    #[tokio::test]
    async fn inspect_video_labels_a_403_as_a_login_problem() {
        let (mut routes, _bodies) = segment_routes(2, 64);
        routes.insert("/media.m3u8".into(), media_playlist(2).into_bytes());
        routes.insert("/clip".into(), b"fake-media-bytes".to_vec());
        let options = HlsServerOptions {
            require_cookie: Some("sessionid=abc123".to_string()),
            ..Default::default()
        };
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, options);
        let session = SessionHeaders {
            cookies: Some("sessionid=abc123".to_string()),
            referer: None,
            user_agent: None,
            origin: SessionHeaders::origin_of(&format!("{base}/media.m3u8")),
        };
        // Manifest fetch without the session: the login label, not "HTTP 403".
        let error = inspect_video(&format!("{base}/media.m3u8"), None).await.unwrap_err();
        assert_eq!(error.to_string(), AUTH_REQUIRED_MESSAGE);
        // Extensionless probe without the session: the same label.
        let error = inspect_video(&format!("{base}/clip"), None).await.unwrap_err();
        assert_eq!(error.to_string(), AUTH_REQUIRED_MESSAGE);
        // With the session the manifest path works; the extensionless
        // probe gets past auth too (it then fails only because the
        // fixture sends no media Content-Type).
        let variants = inspect_video(&format!("{base}/media.m3u8"), Some(&session)).await.unwrap();
        assert!(!variants.is_empty());
        let error = inspect_video(&format!("{base}/clip"), Some(&session)).await.unwrap_err();
        assert!(error.to_string().contains("webpage"), "probe passed auth, failed on content type: {error}");
        stop_server(shutdown, server);
    }

    #[tokio::test]
    async fn hls_error_interrupted_download_resumes_from_disk_checkpoint() {
        let (mut routes, bodies) = segment_routes(12, 8192);
        routes.insert("/media.m3u8".into(), media_playlist(12).into_bytes());
        // Deterministic failure: segments 0-9 always succeed, segments
        // 10+ always fail with 500. The failure is decided per request by
        // the fixture, so it cannot race with the test thread the way a
        // flipped flag could (a flag flipped after the checkpoint wait
        // regularly landed too late — the remaining requests had already
        // been read by the server — and the download completed instead
        // of erroring). The failing index sits past the first full batch
        // of concurrent segments (8, DEFAULT_RESUMABLE_CONNECTIONS), so
        // at least one segment completes — and its checkpoint entry is
        // written — before the failure aborts the download: with the old
        // concurrency of 6 the fixture used 6 for the same reason.
        let fail_from_index = Arc::new(AtomicUsize::new(10));
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
        // Segments 10+ fail deterministically, so the download must error
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

    // --- Slice 4: AES-128 end-to-end, selection, refresh, concurrency ---

    #[tokio::test]
    async fn hls_aes128_download_completes_with_exact_bytes() {
        // Four TS segments encrypted with one AES-128 key (fixed key/IV,
        // no explicit IV in the playlist — the default IV is the media
        // sequence number). The key is served only to requests carrying
        // the session cookie, so the test also proves the key fetch uses
        // the same session as the playlist and segment fetches.
        use aes::Aes128;
        use cbc::{cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit}, Encryptor};
        let key = [0x2b_u8; 16];
        let segment_count = 4;
        let mut routes = HashMap::new();
        let mut expected = Vec::new();
        let mut playlist = String::from("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n");
        for index in 0..segment_count {
            let plaintext: Vec<u8> = (0..512).map(|byte| ((index * 37 + byte) % 251) as u8).collect();
            expected.extend_from_slice(&plaintext);
            // Default IV for segment `index` is its media sequence
            // number (index) as a 16-byte big-endian integer.
            let mut iv = [0u8; 16];
            iv[15] = index as u8;
            let encryptor = Encryptor::<Aes128>::new_from_slices(&key, &iv).unwrap();
            let ciphertext = encryptor.encrypt_padded_vec_mut::<Pkcs7>(&plaintext);
            routes.insert(format!("/seg{index}.ts"), ciphertext);
            playlist.push_str("#EXTINF:10.0,\n");
            playlist.push_str(&format!("seg{index}.ts\n"));
        }
        playlist.push_str("#EXT-X-ENDLIST\n");
        routes.insert("/media.m3u8".into(), playlist.into_bytes());
        routes.insert("/key.bin".into(), key.to_vec());
        let options = HlsServerOptions {
            require_cookie: Some("sessionid=abc123".to_string()),
            ..Default::default()
        };
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-aes128-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let session = SessionHeaders {
            cookies: Some("sessionid=abc123".to_string()),
            referer: None,
            user_agent: None,
            origin: SessionHeaders::origin_of(&url),
        };
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable_with_session(&url, &target, control_rx, Some(session)).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, expected, "decrypted output must be the exact plaintext");
        // The key was fetched exactly once (cached per URI), not once
        // per segment.
        assert_eq!(counts.lock().unwrap().get("/key.bin"), Some(&1));
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_selected_variant_downloads_with_audio() {
        // Master with two variants sharing one audio rendition. The user
        // picks the 360p variant: the download must fetch ONLY the 360p
        // segments + the audio, never the 720p segments (the old code
        // always took the first variant and silently dropped the audio
        // when given a child playlist — Gemini claim 4).
        let ffmpeg = ffmpeg_available().await;
        let directory = std::env::temp_dir().join(format!("thunderload-hls-select-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let mut routes = HashMap::new();
        let (high_segments, low_segments, audio_segments): (Vec<String>, Vec<String>, Vec<String>);
        if ffmpeg {
            let high_dir = directory.join("gen-high");
            let low_dir = directory.join("gen-low");
            let audio_dir = directory.join("gen-audio");
            let (high_init, high_parts) = generate_fmp4_track(&high_dir, true).await
                .expect("ffmpeg is available but could not generate the high test track");
            let (low_init, low_parts) = generate_fmp4_track(&low_dir, true).await
                .expect("ffmpeg is available but could not generate the low test track");
            let (audio_init, audio_parts) = generate_fmp4_track(&audio_dir, false).await
                .expect("ffmpeg is available but could not generate the audio test track");
            routes.insert("/h-init.mp4".into(), high_init);
            routes.insert("/l-init.mp4".into(), low_init);
            routes.insert("/a-init.mp4".into(), audio_init);
            high_segments = high_parts.iter().enumerate().map(|(index, _)| format!("h{index}.m4s")).collect();
            low_segments = low_parts.iter().enumerate().map(|(index, _)| format!("l{index}.m4s")).collect();
            audio_segments = audio_parts.iter().enumerate().map(|(index, _)| format!("a{index}.m4s")).collect();
            for (index, body) in high_parts.iter().enumerate() {
                routes.insert(format!("/h{index}.m4s"), body.clone());
            }
            for (index, body) in low_parts.iter().enumerate() {
                routes.insert(format!("/l{index}.m4s"), body.clone());
            }
            for (index, body) in audio_parts.iter().enumerate() {
                routes.insert(format!("/a{index}.m4s"), body.clone());
            }
        } else {
            let mut init = Vec::new();
            init.extend_from_slice(&24_u32.to_be_bytes());
            init.extend_from_slice(b"ftypisomiso2");
            init.extend_from_slice(&[0_u8; 8]);
            routes.insert("/h-init.mp4".into(), init.clone());
            routes.insert("/l-init.mp4".into(), init.clone());
            routes.insert("/a-init.mp4".into(), init);
            high_segments = vec!["h0.m4s".into(), "h1.m4s".into()];
            low_segments = vec!["l0.m4s".into(), "l1.m4s".into()];
            audio_segments = vec!["a0.m4s".into(), "a1.m4s".into()];
            for index in 0..2 {
                routes.insert(format!("/h{index}.m4s"), format!("high-fragment-{index}").into_bytes());
                routes.insert(format!("/l{index}.m4s"), format!("low-fragment-{index}").into_bytes());
                routes.insert(format!("/a{index}.m4s"), format!("audio-fragment-{index}").into_bytes());
            }
        }
        let master = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio-en\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"audio.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720,AUDIO=\"audio-en\"\nhigh.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360,AUDIO=\"audio-en\"\nlow.m3u8\n";
        routes.insert("/master.m3u8".into(), master.as_bytes().to_vec());
        routes.insert("/high.m3u8".into(), fmp4_media_playlist("h-init.mp4", &high_segments).into_bytes());
        routes.insert("/low.m3u8".into(), fmp4_media_playlist("l-init.mp4", &low_segments).into_bytes());
        routes.insert("/audio.m3u8".into(), fmp4_media_playlist("a-init.mp4", &audio_segments).into_bytes());
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let target = reserve_download_path(&directory.join("movie.mp4")).await.unwrap();
        let selection = HlsSelection {
            media_url: Some(format!("{base}/low.m3u8")),
            audio_group: Some("audio-en".to_string()),
        };
        let (_control_tx, control_rx) = fresh_control();
        let result = run_resumable_with_selection_and_session(&format!("{base}/master.m3u8"), &target, control_rx, selection, None).await;
        // The 360p segments and the audio were fetched exactly once; the
        // 720p segments were never requested at all.
        let counts = counts.lock().unwrap().clone();
        for name in low_segments.iter().chain(audio_segments.iter()) {
            assert_eq!(counts.get(&format!("/{name}")), Some(&1), "expected exactly one request for {name}");
        }
        for name in &high_segments {
            assert_eq!(counts.get(&format!("/{name}")), None, "the unselected 720p segment {name} must never be fetched");
        }
        if ffmpeg {
            let outcome = result.unwrap();
            assert_eq!(outcome, DownloadOutcome::Completed);
            match probe_stream_types(&target).await {
                Some(streams) => {
                    assert!(streams.iter().any(|stream| stream == "video"), "muxed output has no video stream: {streams:?}");
                    assert!(streams.iter().any(|stream| stream == "audio"), "muxed output has no audio stream: {streams:?}");
                }
                None => {
                    let bytes = tokio::fs::read(&target).await.unwrap();
                    assert_eq!(&bytes[4..8], b"ftyp", "muxed output should be an MP4 starting with an ftyp box");
                }
            }
        } else {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("FFmpeg"), "expected a clear FFmpeg error, got: {error}");
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_selected_variant_missing_is_an_error() {
        let master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720\nhigh.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\nlow.m3u8\n";
        let mut routes = HashMap::new();
        routes.insert("/master.m3u8".into(), master.as_bytes().to_vec());
        routes.insert("/high.m3u8".into(), fmp4_media_playlist("h-init.mp4", &["h0.m4s".to_string()]).into_bytes());
        routes.insert("/low.m3u8".into(), fmp4_media_playlist("l-init.mp4", &["l0.m4s".to_string()]).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::ZERO, HlsServerOptions::default());
        let selection = HlsSelection {
            media_url: Some(format!("{base}/gone.m3u8")),
            audio_group: None,
        };
        let error = super::resolve_hls_plan(&format!("{base}/master.m3u8"), &selection, None).await.unwrap_err();
        assert!(error.to_string().contains("no longer listed"), "expected the stale-selection error, got: {error}");
        // A stale audio group is the same kind of error, not a silent
        // fall back to a different track.
        let selection = HlsSelection {
            media_url: Some(format!("{base}/low.m3u8")),
            audio_group: Some("audio-missing".to_string()),
        };
        let error = super::resolve_hls_plan(&format!("{base}/master.m3u8"), &selection, None).await.unwrap_err();
        assert!(error.to_string().contains("no longer listed"), "expected the stale-audio error, got: {error}");
        stop_server(shutdown, server);
    }

    /// A test server that simulates signed-URL expiry for HLS: the media
    /// playlist it serves embeds segment URLs with a `?token=N` query. A
    /// segment request succeeds only when its token is the latest one the
    /// playlist has issued AND that token is not the first one (token 0
    /// is always expired — the URLs the first resolve returns are already
    /// stale). The first resolve therefore 403s every segment; the
    /// refresh re-resolves, gets token 1, and completes. When
    /// `never_valid` is set, no token is ever accepted, so the refreshes
    /// are exhausted and the download fails. `playlist_fetches` counts
    /// playlist GETs; `segment_counts` counts segment GETs per path.
    /// `chunk_delay` paces each 1 KiB of a segment body (zero for an
    /// instant server): a slow server lets a test pause mid-download
    /// deterministically instead of racing the last batch.
    fn spawn_signed_hls_server(
        segment_count: usize,
        never_valid: bool,
        chunk_delay: Duration,
    ) -> (String, Arc<AtomicUsize>, Arc<Mutex<HashMap<String, usize>>>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let epoch = Arc::new(AtomicUsize::new(0));
        let playlist_fetches = Arc::new(AtomicUsize::new(0));
        let segment_counts = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_epoch = epoch.clone();
        let thread_fetches = playlist_fetches.clone();
        let thread_counts = segment_counts.clone();
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
                let epoch = thread_epoch.clone();
                let fetches = thread_fetches.clone();
                let counts = thread_counts.clone();
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
                    let target = request.split_whitespace().nth(1).unwrap_or("/").to_string();
                    let path = target.split('?').next().unwrap_or(&target).to_string();
                    let token = target.split("token=").nth(1)
                        .and_then(|value| value.split('&').next())
                        .and_then(|value| value.parse::<usize>().ok());
                    if path == "/media.m3u8" {
                        let issued = epoch.fetch_add(1, Ordering::Relaxed);
                        fetches.fetch_add(1, Ordering::Relaxed);
                        let mut playlist = String::from("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-ENDLIST\n");
                        for index in 0..segment_count {
                            playlist.push_str("#EXTINF:10.0,\n");
                            playlist.push_str(&format!("/seg{index}.ts?token={issued}\n"));
                        }
                        let body = playlist.into_bytes();
                        let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n", body.len()).as_bytes());
                        let _ = stream.write_all(&body);
                        continue;
                    }
                    if let Some(index) = path.strip_prefix("/seg").and_then(|rest| rest.strip_suffix(".ts")).and_then(|value| value.parse::<usize>().ok()) {
                        {
                            let mut counts = counts.lock().unwrap();
                            *counts.entry(path.clone()).or_insert(0) += 1;
                        }
                        let latest = epoch.load(Ordering::Relaxed).saturating_sub(1);
                        let valid = !never_valid && token == Some(latest) && latest >= 1;
                        if valid {
                            let body = vec![(index % 251) as u8; 8192];
                            let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n", body.len()).as_bytes());
                            for chunk in body.chunks(1024) {
                                if stream.write_all(chunk).is_err() { return; }
                                if !chunk_delay.is_zero() { thread::sleep(chunk_delay); }
                            }
                        } else {
                            let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n");
                        }
                        continue;
                    }
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    return;
                });
            }
        });
        (format!("http://{address}"), playlist_fetches, segment_counts, shutdown, handle)
    }

    #[tokio::test]
    async fn hls_segment_url_refresh_on_403_recovers() {
        let segment_count = 4;
        let (base, playlist_fetches, segment_counts, shutdown, server) =
            spawn_signed_hls_server(segment_count, false, Duration::ZERO);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-refresh-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = Vec::new();
        for index in 0..segment_count {
            expected.extend_from_slice(&vec![(index % 251) as u8; 8192]);
        }
        assert_eq!(bytes, expected);
        // Exactly one refresh: the first playlist's token-0 URLs were all
        // refused, the second playlist's token-1 URLs served every segment.
        assert_eq!(playlist_fetches.load(Ordering::Relaxed), 2, "expected exactly one playlist refresh");
        let counts = segment_counts.lock().unwrap();
        for index in 0..segment_count {
            assert_eq!(counts.get(&format!("/seg{index}.ts")), Some(&2),
                "segment {index} should be fetched twice: once refused, once served");
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_refresh_exhaustion_fails_after_two_refreshes() {
        let segment_count = 4;
        let (base, playlist_fetches, _segment_counts, shutdown, server) =
            spawn_signed_hls_server(segment_count, true, Duration::ZERO);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-refresh-exhaust-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let result = run_resumable(&url, &target, control_rx).await;
        assert!(result.is_err(), "a playlist whose URLs never become valid must fail");
        let message = result.unwrap_err().to_string();
        assert!(message.contains("expired"), "expected the expiry error, got: {message}");
        // Initial resolve + exactly two refreshes, then give up — never
        // a loop.
        assert_eq!(playlist_fetches.load(Ordering::Relaxed), 3, "expected initial resolve + 2 refreshes");
        // The checkpoint is kept, so a retry resumes instead of restarting.
        assert!(tokio::fs::try_exists(partial_download_metadata_path(&target)).await.unwrap());
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_resume_after_rotation_uses_fresh_urls_without_refresh() {
        // Regression test for the Slice 4 re-audit MINOR: on resume the
        // saved checkpoint's stale segment URLs were kept and the fresh
        // URLs from the resume-time resolve discarded, so the first
        // missing segment 403'd and burned one of the two refreshes.
        // The fix swaps the fresh URLs into the reused checkpoint state
        // (done-sets unchanged). Proof: the resume costs exactly one
        // playlist fetch (its own resolve) and no refresh — 3 fetches
        // total (initial resolve + first refresh + resume resolve). With
        // the stale URLs, the resume would 403 and refresh once more: 4.
        // Segments trickle (8 ms per KiB) so the pause lands
        // deterministically mid-download instead of racing the last batch.
        let segment_count = 12;
        let (base, playlist_fetches, _segment_counts, shutdown, server) =
            spawn_signed_hls_server(segment_count, false, Duration::from_millis(8));
        let directory = std::env::temp_dir().join(format!("thunderload-hls-resume-rotate-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");

        // First call: token-0 URLs 403, one refresh gets token 1, the
        // download proceeds. Pause after at least one segment completes.
        let (control_tx, control_rx) = fresh_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_resumable(&task_url, &task_target, control_rx).await });
        let done_before_pause = wait_for_checkpoint_progress(&target, 1).await;
        assert!(!done_before_pause.is_empty() && done_before_pause.len() < segment_count,
            "expected a partial download before the pause, got {} of {}", done_before_pause.len(), segment_count);
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Paused);
        assert_eq!(playlist_fetches.load(Ordering::Relaxed), 2, "first call: initial resolve + one refresh");
        drop(control_tx);

        // Resume (fresh process equivalent): its own resolve issues
        // token 2. The resumed download must use those fresh URLs
        // directly — no 403, no refresh — and complete with the segments
        // it already had.
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        let mut expected = Vec::new();
        for index in 0..segment_count {
            expected.extend_from_slice(&vec![(index % 251) as u8; 8192]);
        }
        assert_eq!(bytes, expected);
        assert_eq!(playlist_fetches.load(Ordering::Relaxed), 3,
            "resume must cost exactly its own resolve — a 4th fetch means it 403'd on stale URLs and refreshed");
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_pause_is_prompt_during_slow_segment() {
        // Four 512 KiB segments, each taking ~4 s to trickle in (1 KiB
        // chunks at 8 ms). With the old bare `join_next()` wait, a pause
        // sent mid-segment would return only after the first in-flight
        // segment finished (~4 s). With the select! on control.changed()
        // it returns in well under two seconds.
        let (mut routes, _bodies) = segment_routes(4, 512 * 1024);
        routes.insert("/media.m3u8".into(), media_playlist(4).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), HlsServerOptions::default());
        let directory = std::env::temp_dir().join(format!("thunderload-hls-prompt-pause-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (control_tx, control_rx) = fresh_control();
        let task_target = target.clone();
        let task_url = url.clone();
        let task = tokio::spawn(async move { run_resumable(&task_url, &task_target, control_rx).await });
        // The playlist fetch is instant (tiny body); the segments take
        // seconds, so a short sleep puts the pause squarely mid-segment.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = tokio::time::Instant::now();
        control_tx.send(DownloadControl::Paused).unwrap();
        let outcome = task.await.unwrap().unwrap();
        assert_eq!(outcome, DownloadOutcome::Paused);
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(2),
            "pause took {elapsed:?} — it must return promptly during a slow segment, not after it finishes");
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_parallel_fetch_uses_eight_concurrent_segments() {
        // 24 segments of 128 KiB, each taking ~1 s to trickle in (1 KiB
        // chunks at 8 ms). The engine must fill all of its connections —
        // 8, DEFAULT_RESUMABLE_CONNECTIONS — and no more: the peak
        // in-flight count on the server is exactly 8. The segments are
        // deliberately slow (~1 s each) so the overlap window dwarfs any
        // scheduling jitter on a loaded machine: with 32 KiB segments the
        // peak sometimes read 7 under full-suite parallel load.
        let segment_count = 24;
        let (mut routes, bodies) = segment_routes(segment_count, 128 * 1024);
        routes.insert("/media.m3u8".into(), media_playlist(segment_count).into_bytes());
        let tracker = Arc::new(InFlightTracker { current: AtomicUsize::new(0), max: AtomicUsize::new(0), paths: Mutex::new(std::collections::HashSet::new()), peak_paths: Mutex::new(Vec::new()) });
        let barrier = Arc::new(ConcurrencyBarrier { target: 8, arrived: AtomicUsize::new(0) });
        let options = HlsServerOptions {
            in_flight: Some(tracker.clone()),
            barrier: Some(barrier),
            ..Default::default()
        };
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-concurrency-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable(&url, &target, control_rx).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, bodies.concat());
        // The peak is counted in SEGMENT requests, not total requests:
        // the server sleeps `chunk_delay` after writing even the last
        // chunk of a response, so the playlist handler's guard can still
        // be held when the first segment batch arrives (total peak 9).
        // That overlap is a fixture artifact, not a ninth connection —
        // the engine opens exactly 8 segment fetches, which the barrier
        // proves by holding all 8 in flight at once.
        assert!(tracker.max() >= 8, "total peak should be at least the 8 segment fetches");
        let peak_segments = tracker.peak_paths().iter()
            .filter(|path| path.starts_with("/seg") && path.ends_with(".ts"))
            .count();
        assert_eq!(peak_segments, 8, "expected the engine to fill exactly 8 concurrent segment fetches; in flight at the peak: {:?}", tracker.peak_paths());
        let counts = counts.lock().unwrap();
        for index in 0..segment_count {
            assert_eq!(counts.get(&format!("/seg{index}.ts")), Some(&1));
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // Slice 7b (Gemini claim 7): the same concurrency proof at
    // connections=2 — the engine fills exactly 2 segment fetches, not
    // the default 8. The barrier makes the peak deterministic: if the
    // engine opened 8, the peak would read 8 and fail this test.
    #[tokio::test]
    async fn hls_parallel_fetch_with_two_connections_uses_two_segments() {
        let segment_count = 24;
        let (mut routes, bodies) = segment_routes(segment_count, 128 * 1024);
        routes.insert("/media.m3u8".into(), media_playlist(segment_count).into_bytes());
        let tracker = Arc::new(InFlightTracker { current: AtomicUsize::new(0), max: AtomicUsize::new(0), paths: Mutex::new(std::collections::HashSet::new()), peak_paths: Mutex::new(Vec::new()) });
        let barrier = Arc::new(ConcurrencyBarrier { target: 2, arrived: AtomicUsize::new(0) });
        let options = HlsServerOptions {
            in_flight: Some(tracker.clone()),
            barrier: Some(barrier),
            ..Default::default()
        };
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), options);
        let directory = std::env::temp_dir().join(format!("thunderload-hls-concurrency-two-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable_with_connections(&url, &target, control_rx, 2).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, bodies.concat());
        let peak_segments = tracker.peak_paths().iter()
            .filter(|path| path.starts_with("/seg") && path.ends_with(".ts"))
            .count();
        assert_eq!(peak_segments, 2, "expected the engine to fill exactly 2 concurrent segment fetches; in flight at the peak: {:?}", tracker.peak_paths());
        let counts = counts.lock().unwrap();
        for index in 0..segment_count {
            assert_eq!(counts.get(&format!("/seg{index}.ts")), Some(&1));
        }
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // Slice 7b (Gemini claim 14): the live progress channel reports
    // real per-segment counts for an HLS download — every segment
    // event carries done/total, and the complete event reports all
    // segments done.
    #[tokio::test]
    async fn hls_progress_reports_segment_counts() {
        let segment_count = 6;
        let (mut routes, bodies) = segment_routes(segment_count, 1024);
        routes.insert("/media.m3u8".into(), media_playlist(segment_count).into_bytes());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(1), HlsServerOptions { ..Default::default() });
        let directory = std::env::temp_dir().join(format!("thunderload-hls-segments-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("movie.ts")).await.unwrap();
        let url = format!("{base}/media.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(progress) = progress_rx.recv().await {
                events.push(progress);
            }
            events
        });
        let outcome = download_hls_resumable(&url, &HlsSelection::none(), &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new()), 2).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let events = collector.await.unwrap();
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, bodies.concat());
        assert!(
            events.iter().any(|event| event.phase == "video-segments" && event.segments_total == Some(segment_count as u64)),
            "a segment event must report the total segment count; events: {events:?}"
        );
        let last = events.last().expect("at least a complete event");
        assert_eq!(last.phase, "complete");
        assert_eq!((last.segments_done, last.segments_total), (Some(segment_count as u64), Some(segment_count as u64)), "the complete event must report all segments done");
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    // --- Slice 6: audio-only selection, live recording, subtitles ---

    #[tokio::test]
    async fn hls_audio_only_selection_downloads_only_the_audio_track() {
        let mut routes = HashMap::new();
        routes.insert("/master.m3u8".into(), "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"audio.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720,AUDIO=\"audio\"\nvideo.m3u8\n".as_bytes().to_vec());
        routes.insert("/video.m3u8".into(), "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXTINF:10.0,\nv0.ts\n#EXTINF:10.0,\nv1.ts\n#EXT-X-ENDLIST\n".as_bytes().to_vec());
        routes.insert("/audio.m3u8".into(), "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXTINF:10.0,\na0.ts\n#EXTINF:10.0,\na1.ts\n#EXT-X-ENDLIST\n".as_bytes().to_vec());
        routes.insert("/v0.ts".into(), b"VIDEO0".to_vec());
        routes.insert("/v1.ts".into(), b"VIDEO1".to_vec());
        routes.insert("/a0.ts".into(), b"AUDIO0".to_vec());
        routes.insert("/a1.ts".into(), b"AUDIO1".to_vec());
        let (base, counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), HlsServerOptions { ..Default::default() });
        let directory = std::env::temp_dir().join(format!("thunderload-hls-audio-only-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("audio.ts")).await.unwrap();
        let url = format!("{base}/master.m3u8");
        // The audio-only row selects the audio rendition's playlist, not
        // a video variant: the plan must be that playlist alone.
        let selection = HlsSelection { media_url: Some(format!("{base}/audio.m3u8")), audio_group: None };
        let (_control_tx, control_rx) = fresh_control();
        let outcome = run_resumable_with_selection_and_session(&url, &target, control_rx, selection, None).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed);
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, b"AUDIO0AUDIO1");
        let counts = counts.lock().unwrap();
        assert_eq!(counts.get("/v0.ts"), None, "no video segment may be fetched for the audio-only row");
        assert_eq!(counts.get("/v1.ts"), None);
        assert_eq!(counts.get("/a0.ts"), Some(&1));
        assert_eq!(counts.get("/a1.ts"), Some(&1));
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// A live HLS server: the playlist window slides on every fetch and
    /// gains EXT-X-ENDLIST on the third fetch, so the recorder must
    /// capture seg0..seg4 exactly once, in order, then stop. Sequential
    /// accept loop — the recorder fetches one thing at a time.
    fn spawn_live_hls_server() -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let fetches = Arc::new(AtomicUsize::new(0));
        let thread_shutdown = shutdown.clone();
        let thread_fetches = fetches.clone();
        let handle = std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                use std::io::{Read, Write};
                let mut raw = Vec::new();
                let mut buffer = [0_u8; 1024];
                let head_complete = loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break false,
                        Ok(count) => {
                            raw.extend_from_slice(&buffer[..count]);
                            if raw.windows(4).any(|window| window == b"\r\n\r\n") { break true; }
                        }
                        Err(_) => break false,
                    }
                };
                if !head_complete { continue; }
                let request = String::from_utf8_lossy(&raw);
                let path = request.lines().next().unwrap_or_default()
                    .split_whitespace().nth(1).unwrap_or("/").to_string();
                let path_only = path.split('?').next().unwrap_or(&path).to_string();
                let body: Vec<u8> = if path_only == "/live.m3u8" {
                    let fetch = thread_fetches.fetch_add(1, Ordering::SeqCst) + 1;
                    // The recorder resolves the media URL with one
                    // playlist fetch before its first poll, so fetches 1
                    // and 2 return the same window — the recording
                    // starts from the window the first poll sees.
                    let playlist = match fetch {
                        1 | 2 => "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\nseg0.ts\n#EXTINF:2.0,\nseg1.ts\n#EXTINF:2.0,\nseg2.ts\n",
                        3 => "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:2.0,\nseg1.ts\n#EXTINF:2.0,\nseg2.ts\n#EXTINF:2.0,\nseg3.ts\n",
                        _ => "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:2\n#EXTINF:2.0,\nseg2.ts\n#EXTINF:2.0,\nseg3.ts\n#EXTINF:2.0,\nseg4.ts\n#EXT-X-ENDLIST\n",
                    };
                    playlist.as_bytes().to_vec()
                } else if let Some(index) = path_only.strip_prefix("/seg").and_then(|rest| rest.strip_suffix(".ts")) {
                    format!("SEG{index}").into_bytes()
                } else {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                };
                let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        (format!("http://{address}"), shutdown, handle)
    }

    /// Slice 6 fix round 2 (nit 1): a live playlist that never lists
    /// a segment — same shape as the stalled server, but the window is
    /// always empty, so the recorder stalls with nothing recorded.
    fn spawn_empty_live_hls_server() -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                use std::io::{Read, Write};
                let mut raw = Vec::new();
                let mut buffer = [0_u8; 1024];
                let head_complete = loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break false,
                        Ok(count) => {
                            raw.extend_from_slice(&buffer[..count]);
                            if raw.windows(4).any(|window| window == b"\r\n\r\n") { break true; }
                        }
                        Err(_) => break false,
                    }
                };
                if !head_complete { continue; }
                let request = String::from_utf8_lossy(&raw);
                let path = request.lines().next().unwrap_or_default()
                    .split_whitespace().nth(1).unwrap_or("/").to_string();
                let path_only = path.split('?').next().unwrap_or(&path).to_string();
                let body: Vec<u8> = if path_only == "/live.m3u8" {
                    b"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n".to_vec()
                } else {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                };
                let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        (format!("http://{address}"), shutdown, handle)
    }

    #[tokio::test]
    async fn hls_live_recording_captures_the_window_until_endlist() {
        let (base, shutdown, server) = spawn_live_hls_server();
        let directory = std::env::temp_dir().join(format!("thunderload-hls-live-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("live.ts")).await.unwrap();
        let url = format!("{base}/live.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let outcome = download_hls_live(&url, &HlsSelection::none(), &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new())).await.unwrap();
        assert_eq!(outcome, DownloadOutcome::Completed, "the recording must stop when the playlist gains ENDLIST");
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, b"SEG0SEG1SEG2SEG3SEG4", "each live segment exactly once, in order");
        shutdown.store(true, Ordering::Relaxed);
        let _ = server.join();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Serves a fixed live fMP4 playlist (init map + m4s segments, no
    /// ENDLIST) on every fetch — the recorder must refuse it on its
    /// first poll.
    fn spawn_live_fmp4_server() -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                use std::io::{Read, Write};
                let mut raw = Vec::new();
                let mut buffer = [0_u8; 1024];
                let head_complete = loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break false,
                        Ok(count) => {
                            raw.extend_from_slice(&buffer[..count]);
                            if raw.windows(4).any(|window| window == b"\r\n\r\n") { break true; }
                        }
                        Err(_) => break false,
                    }
                };
                if !head_complete { continue; }
                let request = String::from_utf8_lossy(&raw);
                let path = request.lines().next().unwrap_or_default()
                    .split_whitespace().nth(1).unwrap_or("/").to_string();
                let path_only = path.split('?').next().unwrap_or(&path).to_string();
                let body: Vec<u8> = if path_only == "/live.m3u8" {
                    b"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\nseg0.m4s\n#EXTINF:2.0,\nseg1.m4s\n".to_vec()
                } else if path_only == "/init.mp4" {
                    b"FAKEINIT".to_vec()
                } else if let Some(index) = path_only.strip_prefix("/seg").and_then(|rest| rest.strip_suffix(".m4s")) {
                    format!("SEG{index}").into_bytes()
                } else {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                };
                let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        (format!("http://{address}"), shutdown, handle)
    }

    /// Slice 6 fix (MINOR 2b): a refused live fMP4 recording must leave
    /// no empty target file behind. The target is created before the
    /// first playlist fetch; the refusal used to return without
    /// removing it, so a failed recording left a 0-byte file on disk.
    #[tokio::test]
    async fn live_fmp4_refusal_leaves_no_target_file() {
        let (base, shutdown, server) = spawn_live_fmp4_server();
        let directory = std::env::temp_dir().join(format!("thunderload-hls-live-fmp4-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("live.ts")).await.unwrap();
        let url = format!("{base}/live.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let result = download_hls_live(&url, &HlsSelection::none(), &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new())).await;
        assert!(result.is_err(), "live fMP4 must be refused, got {result:?}");
        assert!(!target.exists(), "a refused live fMP4 recording must not leave an empty target file behind");
        shutdown.store(true, Ordering::Relaxed);
        let _ = server.join();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Serves the same live window (SEG0–SEG2, TARGETDURATION 2, no
    /// ENDLIST) on every fetch — the stream never advances, so the
    /// recorder sees no new segment on any poll after the first.
    fn spawn_stalled_live_hls_server() -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                use std::io::{Read, Write};
                let mut raw = Vec::new();
                let mut buffer = [0_u8; 1024];
                let head_complete = loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break false,
                        Ok(count) => {
                            raw.extend_from_slice(&buffer[..count]);
                            if raw.windows(4).any(|window| window == b"\r\n\r\n") { break true; }
                        }
                        Err(_) => break false,
                    }
                };
                if !head_complete { continue; }
                let request = String::from_utf8_lossy(&raw);
                let path = request.lines().next().unwrap_or_default()
                    .split_whitespace().nth(1).unwrap_or("/").to_string();
                let path_only = path.split('?').next().unwrap_or(&path).to_string();
                let body: Vec<u8> = if path_only == "/live.m3u8" {
                    b"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\nseg0.ts\n#EXTINF:2.0,\nseg1.ts\n#EXTINF:2.0,\nseg2.ts\n".to_vec()
                } else if let Some(index) = path_only.strip_prefix("/seg").and_then(|rest| rest.strip_suffix(".ts")) {
                    format!("SEG{index}").into_bytes()
                } else {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    continue;
                };
                let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes());
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        (format!("http://{address}"), shutdown, handle)
    }

    /// Slice 6 fix (MINOR 2a): a stalled live playlist — no new
    /// segments for LIVE_STALL_LIMIT consecutive polls — must stop with
    /// a stall error instead of polling forever. The partial recording
    /// of what arrived before the stall is kept.
    #[tokio::test]
    async fn live_stalled_playlist_stops_and_keeps_partial() {
        let (base, shutdown, server) = spawn_stalled_live_hls_server();
        let directory = std::env::temp_dir().join(format!("thunderload-hls-live-stall-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("live.ts")).await.unwrap();
        let url = format!("{base}/live.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        // The 60 s timeout is the failing-before guard: without the
        // stall limit this call never returns.
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            download_hls_live(&url, &HlsSelection::none(), &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new())),
        ).await;
        let result = result.expect("the stalled recorder must stop on its own instead of polling forever");
        let error = result.expect_err("a stalled live playlist must fail, not complete");
        assert!(error.to_string().contains("stall"), "the error must label the stall, got: {error}");
        // Slice 6 fix round 2 (nit 1): segments arrived before the
        // stall, so the message must say the partial was kept.
        assert!(error.to_string().contains("The partial recording was kept."), "a stall after segments arrived must say the partial was kept, got: {error}");
        let bytes = tokio::fs::read(&target).await.unwrap();
        assert_eq!(bytes, b"SEG0SEG1SEG2", "the partial recording of the stalled stream is kept");
        shutdown.store(true, Ordering::Relaxed);
        let _ = server.join();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    /// Slice 6 fix round 2 (nit 1): a live playlist that never lists
    /// a segment stalls with nothing recorded — the empty target is
    /// removed, so the error must NOT claim a partial was kept.
    #[tokio::test]
    async fn live_stalled_empty_playlist_stops_without_a_kept_partial() {
        let (base, shutdown, server) = spawn_empty_live_hls_server();
        let directory = std::env::temp_dir().join(format!("thunderload-hls-live-stall-empty-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("live.ts")).await.unwrap();
        let url = format!("{base}/live.m3u8");
        let (_control_tx, control_rx) = fresh_control();
        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        // The 60 s timeout is the failing-before guard: without the
        // stall limit this call never returns.
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            download_hls_live(&url, &HlsSelection::none(), &target, None, control_rx, progress_tx, Arc::new(GlobalRateLimiter::new())),
        ).await;
        let result = result.expect("the stalled recorder must stop on its own instead of polling forever");
        let error = result.expect_err("a stalled empty playlist must fail, not complete");
        assert!(error.to_string().contains("stall"), "the error must label the stall, got: {error}");
        assert!(!error.to_string().contains("kept"), "with nothing recorded the error must not claim a partial was kept, got: {error}");
        assert!(!target.exists(), "an empty recording leaves no target file behind");
        shutdown.store(true, Ordering::Relaxed);
        let _ = server.join();
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn hls_subtitle_download_joins_vtt_fragments() {
        let mut routes = HashMap::new();
        routes.insert("/subs.m3u8".into(), "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXTINF:10.0,\nsub0.vtt\n#EXTINF:10.0,\nsub1.vtt\n#EXT-X-ENDLIST\n".as_bytes().to_vec());
        routes.insert("/sub0.vtt".into(), b"WEBVTT\n\n00:00.000 --> 00:01.000\nHello\n".to_vec());
        routes.insert("/sub1.vtt".into(), b"WEBVTT\n\n00:01.000 --> 00:02.000\nWorld\n".to_vec());
        let (base, _counts, _range_206, shutdown, server) =
            spawn_hls_server(routes, Duration::from_millis(8), HlsServerOptions { ..Default::default() });
        let directory = std::env::temp_dir().join(format!("thunderload-hls-subs-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let target = reserve_download_path(&directory.join("subs.vtt")).await.unwrap();
        let url = format!("{base}/subs.m3u8");
        let saved = download_hls_subtitles(&url, &target, None, Arc::new(GlobalRateLimiter::new())).await.unwrap();
        let text = tokio::fs::read_to_string(&saved).await.unwrap();
        assert_eq!(text.matches("WEBVTT").count(), 1, "the joined file must have exactly one WEBVTT header");
        assert!(text.contains("Hello"));
        assert!(text.contains("World"));
        stop_server(shutdown, server);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }
}

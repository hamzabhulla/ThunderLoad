//! Site grabber (Slice 7): collect every downloadable media URL from one
//! page.
//!
//! Two entry points share one classifier so the desktop and the extension
//! agree on what counts as media:
//! - `extract_page_media` — pure function over page HTML. The desktop
//!   `grab_page_media` command fetches the page and calls it. This path
//!   sees only the static HTML the server returns: pages that need a login
//!   or load media with JavaScript may show nothing. The browser
//!   extension's content script is the full path — it reads the live DOM.
//! - The extension content script (`extension/content.js`) runs the same
//!   rules over the live DOM (`<video>`/`<audio>` sources, `<a href>`
//!   links, resource timings) and reports into the popup.
//!
//! Limits, kept honest on purpose:
//! - Same page only — no crawling to other pages.
//! - At most 100 items per page (`MAX_ITEMS`); `truncated` says when the
//!   page held more. `data:`/`blob:`/`javascript:` URLs are skipped — the
//!   engine cannot fetch them — as are non-HTTP(S) URLs and URLs whose
//!   path has no recognizable media extension. A page that hides its
//!   streams behind MSE/blob URLs therefore shows nothing downloadable,
//!   rather than a broken entry.

use anyhow::{anyhow, Context, Result};
use crate::session::SessionHeaders;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::download::{safe_filename, with_extension};

/// Maximum media items reported for one page. The extension popup uses
/// the same cap, so both surfaces agree.
pub const MAX_ITEMS: usize = 100;

/// Largest page body the desktop grabber will read (R3). Real link
/// pages are kilobytes; 8 MiB is far above any of them.
pub const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// The media extensions the grabber recognizes, in both the engine and
/// the extension content script. Playlists first: a `.m3u8`/`.mpd` is a
/// stream to inspect, not a file to save as-is.
const MEDIA_EXTENSIONS: &[&str] = &[
    "m3u8", "mpd", "mp4", "m4v", "mov", "webm", "mkv", "avi", "mp3", "m4a", "aac", "wav", "ogg",
    "opus", "flac", "pdf", "zip",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrabKind {
    Direct,
    Hls,
    Dash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageMedia {
    pub url: String,
    pub kind: GrabKind,
    /// File name derived from the URL path (percent-decoded, sanitized).
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageMediaList {
    pub items: Vec<PageMedia>,
    /// True when the page held more than `MAX_ITEMS` media URLs.
    pub truncated: bool,
}

fn classify(path: &str) -> Option<GrabKind> {
    // Decode first: a path like `/file%2Emp4` has no literal dot, so
    // classify the decoded form (F6). The extension itself is what
    // matters, and a decoded `%2F` cannot change that.
    let decoded = percent_decode(path);
    let lower = decoded.to_ascii_lowercase();
    let ext = lower.rsplit('.').next()?;
    if !lower.contains('.') || ext.contains('/') {
        return None;
    }
    match ext {
        "m3u8" => Some(GrabKind::Hls),
        "mpd" => Some(GrabKind::Dash),
        _ if MEDIA_EXTENSIONS.contains(&ext) => Some(GrabKind::Direct),
        _ => None,
    }
}

/// Percent-decode one URL path segment (`%20` -> space, `+` stays a
/// plus). Invalid sequences are left as-is rather than failing the name.
fn percent_decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |b: u8| (b as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn name_for(url: &Url, kind: GrabKind) -> String {
    let last = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .unwrap_or("");
    let decoded = percent_decode(last);
    match kind {
        GrabKind::Direct => safe_filename(&decoded, "download.bin"),
        // A playlist assembles to one MP4 file, so the item is named
        // after the playlist stem with an .mp4 extension — the same
        // name the extension popup derives for the same stream.
        // Slice 7c (Gemini claim 9): only a known media extension is
        // replaced, so "my.video" keeps its name ("my.video.mp4")
        // instead of losing ".video" to "my.mp4".
        GrabKind::Hls | GrabKind::Dash => with_extension(&safe_filename(&decoded, "video"), "mp4"),
    }
}

/// Read one attribute value starting at `pos` (just past the `=`).
/// Returns the value and the position just after it, or `None` when the
/// attribute has no readable value.
fn read_attr_value(html: &str, mut pos: usize) -> Option<(String, usize)> {
    let bytes = html.as_bytes();
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos >= bytes.len() {
        return None;
    }
    let quote = bytes[pos];
    if quote == b'"' || quote == b'\'' {
        let start = pos + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end] != quote {
            end += 1;
        }
        if end >= bytes.len() {
            return None;
        }
        Some((html[start..end].to_string(), end + 1))
    } else {
        let start = pos;
        let mut end = start;
        while end < bytes.len()
            && !bytes[end].is_ascii_whitespace()
            && bytes[end] != b'>'
            && bytes[end] != b'"'
            && bytes[end] != b'\''
        {
            end += 1;
        }
        if end == start {
            return None;
        }
        Some((html[start..end].to_string(), end))
    }
}

/// Decode the one HTML entity that routinely appears inside attribute
/// URLs (`&amp;` for `&`). Other entities are left untouched — a URL
/// carrying one is still fetched verbatim rather than mangled.
fn decode_entities(value: &str) -> String {
    value.replace("&amp;", "&").replace("&#38;", "&")
}

fn is_skippable_scheme(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    lower.starts_with("data:")
        || lower.starts_with("blob:")
        || lower.starts_with("javascript:")
        || lower.starts_with("mailto:")
        || lower.starts_with("tel:")
        || lower.starts_with('#')
}

fn find_bytes(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    let mut i = from;
    while i + needle.len() <= haystack.len() {
        if &haystack[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Blank out spans of the lowercased HTML that are not markup — inline
/// `<script>`/`<style>` bodies and `<!-- -->` comments — so a `src=`
/// string inside JavaScript or a comment is not mistaken for a real
/// attribute (F5). Blanking replaces chars with spaces, so byte
/// positions stay aligned with the original HTML the values are read
/// from.
fn blank_non_markup(lower: &str) -> String {
    let mut bytes = lower.as_bytes().to_vec();
    for (start_marker, end_marker) in [
        ("<!--", "-->"),
        ("<script", "</script>"),
        ("<style", "</style>"),
    ] {
        let mut from = 0;
        while let Some(start) = find_bytes(&bytes, start_marker.as_bytes(), from) {
            let end = match find_bytes(&bytes, end_marker.as_bytes(), start + start_marker.len()) {
                Some(e) => e + end_marker.len(),
                // Unterminated span (e.g. unclosed script): by HTML
                // rules the rest of the document is script/comment
                // content, so blank to the end.
                None => bytes.len(),
            };
            for b in &mut bytes[start..end] {
                *b = b' ';
            }
            from = end;
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Collect media URLs from raw page HTML.
///
/// Scans `src`/`href` attribute values (any element — `<video>`,
/// `<audio>`, `<source>`, `<a>`, `<img>`, `<link>` — the media
/// extension filter decides what counts), resolves relative URLs
/// against `base_url`, deduplicates, and caps the list at `MAX_ITEMS`.
/// Attribute names must stand alone (`data-src` and `srcset` do not
/// match), and values are read quoted or unquoted. `src`/`href` text
/// inside `<script>`, `<style>` and HTML comments is ignored.
pub fn extract_page_media(html: &str, base_url: &str) -> PageMediaList {
    let base = match Url::parse(base_url) {
        Ok(url) => url,
        Err(_) => {
            return PageMediaList {
                items: Vec::new(),
                truncated: false,
            };
        }
    };
    let lower = blank_non_markup(&html.to_ascii_lowercase());
    let bytes = lower.as_bytes();
    let mut items: Vec<PageMedia> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut truncated = false;
    let mut pos = 0;
    while pos < bytes.len() {
        // Find the next `src` or `href` attribute name.
        let rest = &lower[pos..];
        let src_at = rest.find("src");
        let href_at = rest.find("href");
        let (at, len) = match (src_at, href_at) {
            (Some(s), Some(h)) => {
                if s < h {
                    (s, 3)
                } else {
                    (h, 4)
                }
            }
            (Some(s), None) => (s, 3),
            (None, Some(h)) => (h, 4),
            (None, None) => break,
        };
        let abs = pos + at;
        // The name must stand alone: the previous char must not be a
        // name character (so `data-src` and `srcset` do not match), and
        // the next non-space char must be `=`.
        let prev_ok = abs == 0
            || !(bytes[abs - 1].is_ascii_alphanumeric()
                || bytes[abs - 1] == b'_'
                || bytes[abs - 1] == b'-');
        let mut eq = abs + len;
        while eq < bytes.len() && bytes[eq].is_ascii_whitespace() {
            eq += 1;
        }
        let next_ok = eq < bytes.len() && bytes[eq] == b'=';
        if !prev_ok || !next_ok {
            pos = abs + len;
            continue;
        }
        let Some((raw, after)) = read_attr_value(html, eq + 1) else {
            pos = abs + len;
            continue;
        };
        pos = after;
        let value = decode_entities(raw.trim());
        if value.is_empty() || is_skippable_scheme(&value) {
            continue;
        }
        let Ok(url) = base.join(&value) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https") {
            continue;
        }
        let Some(kind) = classify(url.path()) else {
            continue;
        };
        let normalized = url.to_string();
        if !seen.insert(normalized.clone()) {
            continue;
        }
        if items.len() >= MAX_ITEMS {
            truncated = true;
            continue;
        }
        let name = name_for(&url, kind);
        items.push(PageMedia {
            url: normalized,
            kind,
            name,
        });
    }
    PageMediaList { items, truncated }
}

/// Fetch a page and extract its media links (desktop grabber path).
/// No browser session is attached by default — the desktop has none —
/// so pages that need a login or load media with JavaScript may come
/// back empty. The extension's live-DOM scan is the full path for
/// those pages. Callers that DO hold a session use
/// `grab_page_media_with_session` (Slice 5): the session's cookies,
/// Referer and User-Agent are applied to the page fetch, so a
/// login-walled page answers instead of 403ing.
pub async fn grab_page_media(page_url: &str) -> Result<PageMediaList> {
    grab_page_media_with_session(page_url, None).await
}

/// Same as `grab_page_media`, with a browser session applied to the
/// page fetch. A 401/403 from the page is relabelled as the login
/// message instead of a bare status.
pub async fn grab_page_media_with_session(
    page_url: &str,
    session: Option<&SessionHeaders>,
) -> Result<PageMediaList> {
    let parsed = Url::parse(page_url).context("Invalid page URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS page URLs can be grabbed"));
    }
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 ThunderLoad/0.1")
        .redirect(reqwest::redirect::Policy::limited(10))
        // A hung server must not hang the Tauri command forever (F4):
        // 15 s to connect, 30 s for the whole fetch including the body.
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("Could not build HTTP client")?;
    let request = crate::session::apply_session(session, client.get(page_url), page_url);
    let response = request
        .send()
        .await
        .context("Could not fetch the page")?;
    // A 401/403 is the login wall — show the label alone (Slice 5);
    // anything else keeps its context.
    let mut response = response.error_for_status().context("The page request failed").map_err(
        |error| match crate::session::auth_failure_label(&error) {
            Some(label) => anyhow::Error::msg(label),
            None => error,
        },
    )?;
    // Read the body in chunks with a size cap (R3): an unbounded
    // `response.text()` lets a huge or streaming page eat memory for
    // the full 30 s timeout. 8 MiB is far above any real link page.
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await.context("Could not read the page")? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_PAGE_BYTES {
            return Err(anyhow!(
                "Page is larger than {MAX_PAGE_BYTES} bytes; refusing to parse it as a grab source"
            ));
        }
    }
    let html = String::from_utf8_lossy(&body).into_owned();
    Ok(extract_page_media(&html, page_url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn html(body: &str) -> String {
        format!("<html><head><title>t</title></head><body>{body}</body></html>")
    }

    #[test]
    fn extracts_video_audio_and_link_sources() {
        let list = extract_page_media(
            &html(
                r#"<video src="/v/clip.mp4" controls></video>
                <video><source src="https://cdn.example.com/a.webm"><source src="https://cdn.example.com/a.mp4"></video>
                <audio src="/a/song.mp3"></audio>
                <audio><source src="/a/song.ogg"></audio>
                <a href="https://files.example.com/doc.pdf">PDF</a>
                <a href="/files/archive.zip">ZIP</a>"#,
            ),
            "https://example.com/page",
        );
        let urls: Vec<&str> = list.items.iter().map(|item| item.url.as_str()).collect();
        assert_eq!(
            urls,
            [
                "https://example.com/v/clip.mp4",
                "https://cdn.example.com/a.webm",
                "https://cdn.example.com/a.mp4",
                "https://example.com/a/song.mp3",
                "https://example.com/a/song.ogg",
                "https://files.example.com/doc.pdf",
                "https://example.com/files/archive.zip",
            ]
        );
        assert!(list.items.iter().all(|item| item.kind == GrabKind::Direct));
        assert_eq!(list.items[0].name, "clip.mp4");
        assert!(!list.truncated);
    }

    #[test]
    fn classifies_playlists_and_resolves_relative_urls() {
        let list = extract_page_media(
            &html(
                r#"<video src="streams/master.m3u8?token=abc"></video>
                <a href="../other/manifest.mpd">DASH</a>
                <a href="/abs/trailer.mov">MOV</a>"#,
            ),
            "https://example.com/dir/page.html",
        );
        assert_eq!(list.items.len(), 3);
        assert_eq!(list.items[0].kind, GrabKind::Hls);
        assert_eq!(list.items[0].url, "https://example.com/dir/streams/master.m3u8?token=abc");
        assert_eq!(list.items[1].kind, GrabKind::Dash);
        assert_eq!(list.items[1].url, "https://example.com/other/manifest.mpd");
        assert_eq!(list.items[2].kind, GrabKind::Direct);
    }

    #[test]
    fn skips_non_media_data_blob_and_javascript_urls() {
        let list = extract_page_media(
            &html(
                r##"<video src="blob:https://example.com/1234"></video>
                <a href="data:video/mp4;base64,AAAA">data</a>
                <a href="javascript:void(0)">js</a>
                <a href="mailto:a@example.com">mail</a>
                <a href="#section">anchor</a>
                <img src="/pic.png">
                <link href="/style.css">
                <a href="https://example.com/real.mp4">real</a>"##,
            ),
            "https://example.com/page",
        );
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].url, "https://example.com/real.mp4");
    }

    #[test]
    fn deduplicates_and_keeps_first_occurrence_order() {
        let list = extract_page_media(
            &html(
                r#"<a href="/x.mp4">one</a><video src="/x.mp4"></video><a href="/y.mp4">two</a><a href="/x.mp4">again</a>"#,
            ),
            "https://example.com/",
        );
        let urls: Vec<&str> = list.items.iter().map(|item| item.url.as_str()).collect();
        assert_eq!(urls, ["https://example.com/x.mp4", "https://example.com/y.mp4"]);
    }

    #[test]
    fn does_not_match_data_src_or_srcset() {
        let list = extract_page_media(
            &html(
                r#"<img data-src="/lazy.mp4" srcset="/s1.mp4 1x, /s2.mp4 2x"><video src="/real.mp4"></video>"#,
            ),
            "https://example.com/",
        );
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].url, "https://example.com/real.mp4");
    }

    #[test]
    fn reads_unquoted_values_and_decodes_amp_entities() {
        let list = extract_page_media(
            &html(r#"<video src=/v/a%20b.mp4></video><a href="/f.mp3?x=1&amp;y=2">m</a>"#),
            "https://example.com/",
        );
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].name, "a b.mp4");
        assert_eq!(list.items[1].url, "https://example.com/f.mp3?x=1&y=2");
    }

    #[test]
    fn attribute_names_are_case_insensitive() {
        let list = extract_page_media(
            &html(r#"<VIDEO SRC="/UP.MP4"></VIDEO><A HREF="/doc.PDF">d</A>"#),
            "https://example.com/",
        );
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].name, "UP.MP4");
    }

    #[test]
    fn caps_at_max_items_and_reports_truncated() {
        let mut body = String::new();
        for i in 0..105 {
            body.push_str(&format!(r#"<a href="/f{i}.mp4">x</a>"#));
        }
        let list = extract_page_media(&html(&body), "https://example.com/");
        assert_eq!(list.items.len(), MAX_ITEMS);
        assert!(list.truncated);
        assert_eq!(list.items[99].url, "https://example.com/f99.mp4");
    }

    #[test]
    fn invalid_base_url_yields_empty_list() {
        let list = extract_page_media(&html(r#"<a href="/x.mp4">x</a>"#), "not a url");
        assert!(list.items.is_empty());
        assert!(!list.truncated);
    }

    #[test]
    fn playlist_names_use_the_playlist_stem_with_mp4() {
        let list = extract_page_media(
            &html(r#"<video src="/streams/master.m3u8"></video><a href="/m/manifest.mpd">d</a>"#),
            "https://example.com/",
        );
        assert_eq!(list.items[0].name, "master.mp4");
        assert_eq!(list.items[1].name, "manifest.mp4");
    }

    #[tokio::test]
    async fn grab_page_media_fetches_and_extracts() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let page = html(
            r#"<video src="/v/clip.mp4"></video><a href="https://cdn.example.com/doc.pdf">pdf</a>"#,
        );
        let body = page.clone();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0_u8; 4096];
                let _ = stream.read(&mut buffer);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        let list = grab_page_media(&format!("http://127.0.0.1:{}/page.html", address.port()))
            .await
            .expect("grab must succeed");
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].url, format!("http://127.0.0.1:{}/v/clip.mp4", address.port()));
        assert_eq!(list.items[1].url, "https://cdn.example.com/doc.pdf");
        assert!(!list.truncated);
    }

    #[test]
    fn ignores_src_inside_script_style_and_comments() {
        let list = extract_page_media(
            &html(
                r#"<script>var u = 'src="https://example.com/phantom.mp4"';</script>
                <style>.x { background: url("href='https://example.com/ghost.mp4'"); }</style>
                <!-- <a href="https://example.com/hidden.mp4">hidden</a> -->
                <a href="https://example.com/real.mp4">real</a>"#,
            ),
            "https://example.com/",
        );
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].url, "https://example.com/real.mp4");
    }

    #[test]
    fn classifies_percent_encoded_dot() {
        let list = extract_page_media(
            &html(r#"<a href="/file%2Emp4">encoded</a><a href="/clip%2Em3u8">list</a>"#),
            "https://example.com/",
        );
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].kind, GrabKind::Direct);
        assert_eq!(list.items[1].kind, GrabKind::Hls);
    }

    /// A server that accepts but never answers must fail within the
    /// 30 s total timeout instead of hanging the Tauri command (F4).
    /// Ignored in the normal suite: it deliberately waits out the
    /// timeout. Run with `cargo test -p thunderload-engine -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn grab_page_media_times_out_on_silent_server() {
        use std::net::TcpListener;
        use std::thread;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            // Accept one connection and then never answer it.
            if let Ok((stream, _)) = listener.accept() {
                thread::sleep(std::time::Duration::from_secs(35));
                drop(stream);
            }
        });
        let started = std::time::Instant::now();
        let result = grab_page_media(&format!("http://127.0.0.1:{port}/page.html")).await;
        assert!(result.is_err(), "silent server should fail, got {result:?}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(35),
            "fetch took {:?}, expected the 30 s timeout to fire first",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn grab_page_media_rejects_oversized_page() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buffer = [0_u8; 4096];
                let _ = stream.read(&mut buffer);
                // One byte over the cap: the fetch must fail fast
                // instead of reading the whole body.
                let oversized = "a".repeat(MAX_PAGE_BYTES + 1);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    oversized.len(),
                    oversized
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        let result =
            grab_page_media(&format!("http://127.0.0.1:{}/page.html", address.port())).await;
        assert!(result.is_err(), "oversized page should fail, got {result:?}");
        assert!(result.unwrap_err().to_string().contains("larger than"));
    }

    #[tokio::test]
    async fn grab_page_media_rejects_non_http_and_failed_pages() {
        assert!(grab_page_media("ftp://example.com/page.html").await.is_err());
        assert!(grab_page_media("not a url").await.is_err());
    }

    /// Slice 5: the grabber with a session can fetch a login-walled
    /// page (cookies + Referer + UA on the page fetch); without one
    /// the 403 carries the login label instead of a bare status.
    #[tokio::test]
    async fn grab_page_media_with_session_passes_a_cookie_gated_page() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc,
            },
            thread,
            time::Duration,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => {
                        let _ = connection.0.set_nonblocking(false);
                        connection
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(3));
                        continue;
                    }
                    Err(_) => break,
                };
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            request.extend_from_slice(&buffer[..count]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let cookie = request
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("cookie")
                            .then(|| value.trim().to_string())
                    })
                    .unwrap_or_default();
                if cookie != "sessionid=abc123" {
                    let _ = stream.write_all(
                        b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                    continue;
                }
                let body = "<html><head><title>t</title></head><body><video src=\"/v/clip.mp4\" controls></video></body></html>";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        let page_url = format!("http://{address}/page.html");
        let session = SessionHeaders {
            cookies: Some("sessionid=abc123".to_string()),
            referer: Some(page_url.clone()),
            user_agent: Some("ThunderLoad-Test/1.0".to_string()),
            origin: SessionHeaders::origin_of(&page_url),
        };
        assert!(session.validate(&page_url).is_ok(), "test session must validate");
        // Without the session the page 403s — and the error is the label.
        let error = grab_page_media(&page_url).await.unwrap_err();
        assert_eq!(error.to_string(), crate::session::AUTH_REQUIRED_MESSAGE);
        // With the session the page answers and the media link is found.
        let list = grab_page_media_with_session(&page_url, Some(&session)).await.unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].url, format!("http://{address}/v/clip.mp4"));
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}

//! Browser session handoff (Slice 6).
//!
//! A browser-started download carries the session it needs to fetch
//! cookie-bound / signed / authenticated URLs: the cookies for the
//! download's own origin, the page Referer, and the browser User-Agent.
//! The extension captures these for the download URL only and hands
//! them to the native host, which validates them and passes them to the
//! engine. The engine applies them to every request of the job —
//! including resume and retry requests — and never to another origin.
//!
//! Threat model:
//! - The handoff is a bearer credential: anyone holding the Cookie
//!   header can act as the user's browser session on that origin until
//!   the cookies expire. It is therefore treated like a password: it is
//!   never logged (no engine/host code path prints it), never sent to
//!   any origin other than the one it was captured for, and stored only
//!   in the job's own durable record (the `JobRecord` / native state
//!   file) on the user's own machine — never in a separate file, never
//!   in a shared log, and never over the network except as an HTTP
//!   header to its own origin.
//! - The extension is trusted to capture cookies only for the URL the
//!   user asked to download (`chrome.cookies.getAll({url})`); the host
//!   re-checks that the declared origin matches the request URL's
//!   origin, and the engine re-checks per request, so a compromised or
//!   buggy extension cannot make the engine send cookies to an
//!   unrelated host.
//! - Redirects: reqwest strips the Cookie header when following a
//!   redirect to a different origin (see the
//!   `session_cookies_are_not_sent_on_cross_origin_redirect` test), so
//!   a same-origin URL that redirects elsewhere cannot leak the
//!   handoff either.

use anyhow::{anyhow, Context, Result};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{self, HeaderValue};
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use url::Url;

/// The browser session for one download job.
///
/// `None` (the default) means "no browser session": desktop-started
/// jobs, and any job whose handoff was absent or failed validation.
//
// No derived `Debug`: the cookies are a bearer credential and must
// never appear in a log line via `{:?}` — see the manual impl below.
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionHeaders {
    /// Raw `Cookie` header value (`"name=value; name2=value2"`) for
    /// `origin`. Only ever sent to requests whose origin equals
    /// `origin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookies: Option<String>,
    /// Page the download was started from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub referer: Option<String>,
    /// Browser User-Agent, used instead of the engine default when set.
    /// The extension sends camelCase `userAgent`; the alias keeps that
    /// handoff from silently dropping the browser's User-Agent (P1-26).
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "userAgent")]
    pub user_agent: Option<String>,
    /// Origin the cookies belong to, e.g. `"https://example.com"`.
    /// Cookies are only sent to requests whose origin equals this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

impl std::fmt::Debug for SessionHeaders {
    /// Cookies are a bearer credential: show only that one is present,
    /// never its value. Everything else debugs normally.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHeaders")
            .field("cookies", &self.cookies.as_ref().map(|_| "[redacted]"))
            .field("referer", &self.referer)
            .field("user_agent", &self.user_agent)
            .field("origin", &self.origin)
            .finish()
    }
}

impl SessionHeaders {
    /// Build from a referer only (desktop call sites, which have no
    /// browser session). Returns `None` when there is no referer, or
    /// when it is not an HTTP(S) URL: a non-HTTP referer (e.g. a
    /// `chrome://` page) must not poison a handoff that also carries
    /// cookies — `validate` rejects non-HTTP referers, so accepting one
    /// here used to make the host drop the whole session, valid cookies
    /// included (F5).
    pub fn with_referer(referer: Option<&str>) -> Option<Self> {
        referer
            .filter(|value| !value.is_empty())
            .filter(|value| {
                Url::parse(value).is_ok_and(|parsed| matches!(parsed.scheme(), "http" | "https"))
            })
            .map(|referer| Self {
                referer: Some(referer.to_string()),
                ..Default::default()
            })
    }

    /// The credential identity of this handoff — the cookies and the
    /// origin they belong to. Referer and User-Agent are not part of
    /// it: they do not change which bytes a fixed manifest resolves
    /// to, and the segment-plan comparison already catches any manifest
    /// change they cause. Checkpoint identity uses this, so a
    /// referer/UA-only change resumes instead of restarting and
    /// deleting finished segments (F4).
    pub fn credential_identity(&self) -> (&Option<String>, &Option<String>) {
        (&self.cookies, &self.origin)
    }

    /// True when this handoff carries nothing at all.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_none() && self.referer.is_none() && self.user_agent.is_none()
    }

    /// The serialized origin of `url` (`"https://example.com"`), for
    /// comparing request URLs against the cookie scope.
    pub fn origin_of(url: &str) -> Option<String> {
        Url::parse(url).ok().map(|parsed| parsed.origin().ascii_serialization())
    }

    /// Validate a handoff received from the native host for `url`.
    /// Fail-closed: any inconsistency is an error and the caller must
    /// drop the handoff (the download then proceeds without it, so an
    /// authenticated URL fails visibly at the server instead of sending
    /// credentials somewhere unexpected).
    pub fn validate(&self, url: &str) -> Result<()> {
        let expected = Self::origin_of(url).context("Invalid download URL")?;
        if let Some(origin) = &self.origin {
            let parsed = Url::parse(origin).context("Invalid session origin")?;
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                return Err(anyhow!("Session origin is not an HTTP(S) origin"));
            }
            if parsed.origin().ascii_serialization() != expected {
                return Err(anyhow!("Session origin does not match the download URL"));
            }
        }
        if self.cookies.is_some() && self.origin.is_none() {
            return Err(anyhow!("Session cookies without an origin are rejected"));
        }
        if let Some(cookies) = &self.cookies {
            if cookies.trim().is_empty() {
                return Err(anyhow!("Session cookies are empty"));
            }
            if cookies.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
                return Err(anyhow!("Session cookies contain control characters"));
            }
        }
        if let Some(referer) = &self.referer {
            if referer.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
                return Err(anyhow!("Session referer contains control characters"));
            }
            let parsed = Url::parse(referer).context("Invalid session referer")?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(anyhow!("Session referer is not an HTTP(S) URL"));
            }
        }
        if let Some(user_agent) = &self.user_agent {
            if user_agent.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
                return Err(anyhow!("Session user agent contains control characters"));
            }
        }
        Ok(())
    }

    /// True when the cookies in this handoff may be sent to `url`:
    /// cookies are present and the request origin equals the origin
    /// they were captured for.
    pub fn cookies_apply_to(&self, url: &str) -> bool {
        match (&self.cookies, &self.origin) {
            (Some(cookies), Some(origin)) if !cookies.trim().is_empty() => {
                let normalized_origin = Url::parse(origin)
                    .ok()
                    .filter(|u| matches!(u.scheme(), "http" | "https"))
                    .map(|u| u.origin().ascii_serialization());
                normalized_origin.is_some() && Self::origin_of(url) == normalized_origin
            }
            _ => false,
        }
    }

    /// Apply this session to one request: Cookie only when the request
    /// URL is in scope, User-Agent whenever set, Referer only when the
    /// request URL is same-origin with the page it came from — a
    /// cross-origin Referer would leak the user's browsing to a host
    /// that never served the page. Never panics on a malformed header
    /// value — that header is skipped.
    pub fn apply(&self, request: RequestBuilder, url: &str) -> RequestBuilder {
        let mut request = request;
        if self.cookies_apply_to(url) {
            if let Some(cookies) = &self.cookies {
                if let Ok(value) = HeaderValue::from_str(cookies) {
                    request = request.header(header::COOKIE, value);
                }
            }
        }
        if let Some(referer) = &self.referer {
            let referer_origin = Self::origin_of(referer);
            if referer_origin.is_some() && referer_origin == Self::origin_of(url) {
                if let Ok(value) = HeaderValue::from_str(referer) {
                    request = request.header(header::REFERER, value);
                }
            }
        }
        if let Some(user_agent) = &self.user_agent {
            if let Ok(value) = HeaderValue::from_str(user_agent) {
                request = request.header(header::USER_AGENT, value);
            }
        }
        request
    }
}

/// Apply an optional session to a request; `None` is a no-op, so
/// desktop jobs (which have no browser session) behave exactly as
/// before.
pub fn apply_session(
    session: Option<&SessionHeaders>,
    request: RequestBuilder,
    url: &str,
) -> RequestBuilder {
    match session {
        Some(session) => session.apply(request, url),
        None => request,
    }
}

/// What the user sees when a server answers 401/403 to a download
/// request (Slice 5): a bare "HTTP 403" tells them nothing they can
/// act on. The fix is almost always on their side — open the page in
/// the browser while logged in, then try again — so say that instead
/// of the status code. This is a label, not a diagnosis: a 403 can
/// also mean an expired signed URL or a geo-block, and the retry
/// paths (manifest re-fetch, signed-URL refresh) still run before
/// this label ever surfaces.
pub const AUTH_REQUIRED_MESSAGE: &str =
    "This video needs you to be logged in. Open it in the browser while logged in, then try again.";

/// Map a request error to the login label when the server answered
/// 401/403, else `None`. Walks the anyhow chain for the reqwest error
/// (preserved by `?` and `.context()`), so callers that already
/// wrapped the error still get the label. Errors that carry the
/// status only as formatted text (e.g. `anyhow!("... HTTP 403")`)
/// are NOT matched here — those sites map the status directly where
/// they know it.
pub fn auth_failure_label(error: &anyhow::Error) -> Option<&'static str> {
    for cause in error.chain() {
        if let Some(request_error) = cause.downcast_ref::<reqwest::Error>() {
            if let Some(status) = request_error.status() {
                if status == reqwest::StatusCode::UNAUTHORIZED
                    || status == reqwest::StatusCode::FORBIDDEN
                {
                    return Some(AUTH_REQUIRED_MESSAGE);
                }
            }
        }
    }
    None
}

/// Map a reqwest error to an anyhow error, relabelling 401/403 as the
/// login message. For the `send().await` / `error_for_status()` sites
/// in the fetch helpers, where the error has not been wrapped yet.
pub fn map_auth_error(error: reqwest::Error) -> anyhow::Error {
    let wrapped = anyhow::Error::new(error);
    match auth_failure_label(&wrapped) {
        Some(label) => anyhow::Error::msg(label),
        None => wrapped,
    }
}

/// User-facing text for a failed engine call: a 401/403 anywhere in
/// the error chain becomes the login label, anything else keeps its
/// own message — unless it is one of the known failure shapes below,
/// which get a plain-language label of their own (Slice 6). The
/// desktop and the native host both funnel their job failures through
/// this one function, so a gated download reads the same in the app
/// whether it started in the browser or on the desktop.
///
/// Label precedence (open question, Slice 5/6 — preserved as-is, Hamza
/// to decide): auth wins over everything, including Slice 4's
/// refresh-exhaustion expiry message, because a 401/403 in the chain
/// is the login label by design. If Hamza decides expiry should win,
/// the change is one branch here.
pub fn error_text(error: &anyhow::Error) -> String {
    if let Some(label) = auth_failure_label(error) {
        return label.to_string();
    }
    if let Some(label) = download_failure_label(error) {
        return label;
    }
    error.to_string()
}

/// Plain-language labels for the failure shapes whose cause is known
/// (Slice 6), checked after auth. A 404 is a gone file, a 416 is a
/// resume the server will not honour, a 5xx is the server's fault, and
/// a transport failure is a connection problem — none of those is a
/// login problem, and none of them deserves a bare status code or a
/// raw reqwest message. Anything unrecognised stays raw: inventing a
/// label for an unknown error would be worse than showing it.
pub fn download_failure_label(error: &anyhow::Error) -> Option<String> {
    for cause in error.chain() {
        if let Some(request_error) = cause.downcast_ref::<reqwest::Error>() {
            if let Some(status) = request_error.status() {
                return Some(match status {
                    reqwest::StatusCode::NOT_FOUND => {
                        "This file is no longer available (404).".to_string()
                    }
                    reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
                        "This download can't be resumed from where it stopped.".to_string()
                    }
                    status if status.is_server_error() => {
                        format!("The server reported an error (HTTP {}). Try again later.", status.as_u16())
                    }
                    _ => return None,
                });
            }
            if request_error.is_connect() {
                return Some("Could not reach the server. Check your connection and try again.".to_string());
            }
            if request_error.is_timeout() {
                return Some("The server took too long to respond. Try again.".to_string());
            }
        }
    }
    None
}

/// True when the bench-only localhost opt-in is set
/// (`THUNDERLOAD_ALLOW_LOCALHOST=1`). The bench binary sets it for
/// itself at startup; nothing in production ever sets it. It exempts
/// loopback only — every other blocked range still applies.
pub fn localhost_allowed() -> bool {
    std::env::var("THUNDERLOAD_ALLOW_LOCALHOST").is_ok_and(|value| value == "1")
}

/// True when an IPv4 address is not a public address: loopback,
/// RFC1918, link-local, unspecified, CGNAT (`100.64.0.0/10`),
/// multicast (`224.0.0.0/4`), broadcast, benchmarking (`198.18.0.0/15`)
/// and reserved (`240.0.0.0/4`) space. Kept as a plain predicate so it
/// can be unit-tested without a live server.
pub fn is_blocked_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (224..=239).contains(&octets[0])
        || octets == [255, 255, 255, 255]
        || (octets[0] == 198 && (18..=19).contains(&octets[1]))
        || octets[0] >= 240
}

/// True when an IPv6 address is not a public address: loopback,
/// unspecified, link-local (`fe80::/10`), unique-local (`fc00::/7`),
/// multicast (`ff00::/8`), NAT64 (`64:ff9b::/96`), plus anything that
/// maps to a blocked IPv4 address. Kept as a plain predicate so it can
/// be unit-tested without a live server.
pub fn is_blocked_ipv6(ip: Ipv6Addr) -> bool {
    // Loopback/unspecified first: `to_ipv4()` maps `::1` to the
    // IPv4-compatible `0.0.0.1`, which none of the IPv4 checks catch,
    // so the mapped path below must never see them.
    if ip.is_loopback() || ip.is_unspecified() {
        return true;
    }
    if let Some(mapped) = ip.to_ipv4() {
        return is_blocked_ipv4(mapped);
    }
    let segments = ip.segments();
    (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xff00) == 0xff00
        || (segments[0] == 0x0064
            && segments[1] == 0xff9b
            && segments[2] == 0
            && segments[3] == 0
            && segments[4] == 0
            && segments[5] == 0)
}

/// True when an IP address (v4 or v6) is not a public address.
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_blocked_ipv4(ip),
        IpAddr::V6(ip) => is_blocked_ipv6(ip),
    }
}

/// True when a URL host is blocked: a `localhost` name, or a blocked
/// IP. With `allow_loopback` (unit tests, and the bench binary's
/// opt-in) loopback is exempt; every other range still blocks.
/// Only called from the `#[cfg(not(test))]` arm of
/// `validate_public_http_url`, so test builds would warn it is dead.
#[cfg_attr(test, allow(dead_code))]
fn host_is_blocked(host: &url::Host<&str>, allow_loopback: bool) -> bool {
    match host {
        url::Host::Domain(domain) => {
            let normalized = domain.trim_end_matches('.').to_ascii_lowercase();
            let is_localhost =
                normalized == "localhost" || normalized.ends_with(".localhost");
            is_localhost && !allow_loopback
        }
        url::Host::Ipv4(ip) => {
            if allow_loopback && ip.is_loopback() {
                false
            } else {
                is_blocked_ipv4(*ip)
            }
        }
        url::Host::Ipv6(ip) => {
            if allow_loopback && ip.is_loopback() {
                false
            } else {
                is_blocked_ipv6(*ip)
            }
        }
    }
}

/// A DNS resolver that drops every resolved address the SSRF blocklist
/// forbids, before reqwest ever connects to it. The URL-string check in
/// `validate_public_http_url` is the first layer; this is the
/// enforcement at connect time, so a hostname that resolves to a
/// private address (DNS rebinding, or a record that changes between
/// the check and the connect) is caught here — the addresses handed to
/// `connect` are exactly the addresses that were checked.
///
/// Residual gap, stated honestly: DNS is re-resolved per request, so a
/// record that flips public-to-private between two requests is
/// re-checked on the next request, not the in-flight one — but no
/// in-flight connection ever goes to an unchecked address.
#[derive(Debug, Clone, Copy)]
pub struct ValidatingResolver {
    allow_loopback: bool,
}

impl ValidatingResolver {
    pub fn new(allow_loopback: bool) -> Self {
        Self { allow_loopback }
    }

    /// The resolver the engine clients use: loopback is allowed in unit
    /// tests (local fixtures) and in the bench binary, which opts in via
    /// `THUNDERLOAD_ALLOW_LOCALHOST=1`; everywhere else it is blocked.
    pub fn from_env() -> Self {
        Self::new(cfg!(test) || localhost_allowed())
    }
}

impl Resolve for ValidatingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let allow_loopback = self.allow_loopback;
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((name.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            let kept: Vec<std::net::SocketAddr> = addrs
                .filter(|addr| {
                    (allow_loopback && addr.ip().is_loopback()) || !is_blocked_ip(addr.ip())
                })
                .collect();
            if kept.is_empty() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "DNS resolved only to blocked (local or private) addresses",
                )) as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(kept.into_iter()) as Addrs)
        })
    }
}

/// Validate that `url` is an HTTP(S) URL targeting a public host.
/// Rejects non-HTTP(S) schemes unconditionally, and (outside unit tests,
/// which bind local `127.0.0.1` mock servers) rejects localhost,
/// unspecified, loopback, link-local, and private addresses — including
/// the audit ranges (CGNAT, multicast, broadcast, benchmarking,
/// reserved, NAT64) — to guard against SSRF. The bench binary opts
/// into a loopback exemption via `THUNDERLOAD_ALLOW_LOCALHOST=1`.
///
/// The URL-string check here is the first layer only: the
/// `ValidatingResolver` wired into the engine clients re-checks every
/// resolved address before connecting, which is what closes the
/// DNS-rebinding gap.
pub fn validate_public_http_url(url: &str) -> Result<Url> {
    let parsed = Url::parse(url).context("Invalid URL")?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("Only HTTP and HTTPS URLs are supported"));
    }
    let host = parsed
        .host()
        .ok_or_else(|| anyhow!("URL must include a host"))?;
    #[cfg(not(test))]
    {
        if host_is_blocked(&host, localhost_allowed()) {
            return Err(anyhow!(
                "Refusing to connect to a local or private network address"
            ));
        }
    }
    #[cfg(test)]
    let _ = host;
    Ok(parsed)
}

/// Redirect policy for every engine HTTP client: at most 10 hops
/// (the same limit `Policy::limited(10)` enforced before), and every
/// hop's target must pass `validate_public_http_url`. A hop to a
/// non-HTTP(S) URL or a local/private address fails the whole request
/// with a clear error instead of being followed silently — so a
/// public URL that redirects at a private host can no longer turn the
/// engine into an SSRF probe. Each hop is also resolved through the
/// client's `ValidatingResolver`, so a redirect target that resolves
/// to a private address is blocked at connect time, not just by URL
/// string.
pub fn validated_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() > 10 {
            return attempt.error("Too many redirects (limit 10)");
        }
        match validate_public_http_url(attempt.url().as_str()) {
            Ok(_) => attempt.follow(),
            Err(error) => attempt.error(format!(
                "Redirect target failed validation: {error}"
            )),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionHeaders {
        SessionHeaders {
            cookies: Some("session=abc; theme=dark".to_string()),
            referer: Some("https://example.com/page".to_string()),
            user_agent: Some("Mozilla/5.0 (test)".to_string()),
            origin: Some("https://example.com".to_string()),
        }
    }

    #[test]
    fn origin_of_serializes_scheme_host_and_port() {
        assert_eq!(
            SessionHeaders::origin_of("https://example.com:8443/a/b?x=1"),
            Some("https://example.com:8443".to_string())
        );
        assert_eq!(
            SessionHeaders::origin_of("http://127.0.0.1:8080/file"),
            Some("http://127.0.0.1:8080".to_string())
        );
        assert_eq!(SessionHeaders::origin_of("not a url"), None);
    }

    #[test]
    fn validate_accepts_a_matching_handoff() {
        assert!(session().validate("https://example.com/video.mp4").is_ok());
    }

    #[test]
    fn validate_rejects_origin_mismatch() {
        let result = session().validate("https://attacker.example/video.mp4");
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_cookies_without_origin() {
        let session = SessionHeaders {
            cookies: Some("session=abc".to_string()),
            ..Default::default()
        };
        assert!(session.validate("https://example.com/video.mp4").is_err());
    }

    #[test]
    fn validate_rejects_control_characters_in_cookies() {
        let session = SessionHeaders {
            cookies: Some("session=abc\r\nX-Injected: 1".to_string()),
            origin: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert!(session.validate("https://example.com/video.mp4").is_err());
    }

    #[test]
    fn validate_rejects_non_http_referer() {
        let session = SessionHeaders {
            referer: Some("file:///etc/passwd".to_string()),
            ..Default::default()
        };
        assert!(session.validate("https://example.com/video.mp4").is_err());
    }

    #[test]
    fn debug_hides_cookie_values() {
        let rendered = format!("{:?}", session());
        assert!(
            !rendered.contains("session=abc"),
            "cookies must never appear in Debug output: {rendered}"
        );
        assert!(rendered.contains("[redacted]"), "Debug must still show a cookie is present: {rendered}");
        assert!(rendered.contains("https://example.com/page"));
    }

    #[test]
    fn user_agent_deserializes_from_camel_case_alias() {
        // P1-26: the extension sends `userAgent`; without the alias the
        // browser's User-Agent was silently dropped on every request.
        let parsed: SessionHeaders =
            serde_json::from_str(r#"{"userAgent":"Mozilla/5.0","origin":"https://example.com"}"#)
                .unwrap();
        assert_eq!(parsed.user_agent.as_deref(), Some("Mozilla/5.0"));
        // snake_case still works, and serialization keeps snake_case.
        let snake: SessionHeaders =
            serde_json::from_str(r#"{"user_agent":"Other/1.0"}"#).unwrap();
        assert_eq!(snake.user_agent.as_deref(), Some("Other/1.0"));
        assert!(serde_json::to_string(&parsed)
            .unwrap()
            .contains("\"user_agent\":\"Mozilla/5.0\""));
    }

    #[test]
    fn ssrf_blocklist_covers_the_audit_ranges() {
        // Previously blocked (kept): loopback, RFC1918, link-local, ULA.
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "::1",
            "fe80::1",
            "fc00::1",
        ] {
            let parsed: std::net::IpAddr = ip.parse().unwrap();
            assert!(is_blocked_ip(parsed), "{ip} must be blocked");
        }
        // New ranges (P0-1 remainder): CGNAT, multicast, broadcast,
        // benchmarking, reserved, IPv6 multicast, NAT64.
        for ip in [
            "100.64.0.1",
            "100.127.255.255",
            "224.0.0.1",
            "239.255.255.255",
            "255.255.255.255",
            "198.18.0.1",
            "198.19.255.255",
            "240.0.0.1",
            "255.0.0.1",
            "ff02::1",
            "64:ff9b::8.8.8.8",
            "64:ff9b::1",
            "::ffff:127.0.0.1",
        ] {
            let parsed: std::net::IpAddr = ip.parse().unwrap();
            assert!(is_blocked_ip(parsed), "{ip} must be blocked");
        }
        // Public addresses stay reachable.
        for ip in ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2001:4860:4860::8888"] {
            let parsed: std::net::IpAddr = ip.parse().unwrap();
            assert!(!is_blocked_ip(parsed), "{ip} must not be blocked");
        }
    }

    #[tokio::test]
    async fn validating_resolver_rejects_private_addresses() {
        use reqwest::dns::Resolve as _;
        // No live server or DNS involved: literal IPs resolve locally.
        let strict = ValidatingResolver::new(false);
        for name in ["127.0.0.1", "100.64.0.1", "10.0.0.1", "::1"] {
            let parsed: reqwest::dns::Name = name.parse().unwrap();
            assert!(
                strict.resolve(parsed).await.is_err(),
                "{name} must not resolve through the validating resolver"
            );
        }
        let public: reqwest::dns::Name = "8.8.8.8".parse().unwrap();
        let addrs = strict.resolve(public).await.unwrap();
        assert_eq!(addrs.count(), 1);
        // The loopback exemption (tests / bench opt-in) allows 127.0.0.1
        // and nothing else extra: CGNAT still blocks.
        let loopback_ok = ValidatingResolver::new(true);
        let local: reqwest::dns::Name = "127.0.0.1".parse().unwrap();
        assert!(loopback_ok.resolve(local).await.is_ok());
        let cgnat: reqwest::dns::Name = "100.64.0.1".parse().unwrap();
        assert!(loopback_ok.resolve(cgnat).await.is_err());
    }

    #[test]
    fn with_referer_is_none_without_a_referer() {
        assert_eq!(SessionHeaders::with_referer(None), None);
        assert_eq!(SessionHeaders::with_referer(Some("")), None);
        // A non-HTTP referer must not poison a handoff: it is dropped
        // here instead of failing validation downstream (F5).
        assert_eq!(SessionHeaders::with_referer(Some("chrome://extensions/")), None);
        assert_eq!(SessionHeaders::with_referer(Some("file:///etc/passwd")), None);
        let session = SessionHeaders::with_referer(Some("https://example.com/")).unwrap();
        assert_eq!(session.referer.as_deref(), Some("https://example.com/"));
        assert!(session.cookies.is_none());
    }

    #[test]
    fn credential_identity_ignores_referer_and_user_agent() {
        let base = session();
        let mut changed = base.clone();
        changed.referer = Some("https://example.com/other-page".to_string());
        changed.user_agent = Some("AnotherAgent/2.0".to_string());
        assert_eq!(base.credential_identity(), changed.credential_identity());
        let mut new_cookies = base.clone();
        new_cookies.cookies = Some("session=zzz".to_string());
        assert_ne!(base.credential_identity(), new_cookies.credential_identity());
        let mut new_origin = base.clone();
        new_origin.origin = Some("https://cdn.example.com".to_string());
        assert_ne!(base.credential_identity(), new_origin.credential_identity());
    }

    #[test]
    fn cookies_apply_only_to_their_origin() {
        let session = session();
        assert!(session.cookies_apply_to("https://example.com/video.mp4"));
        assert!(session.cookies_apply_to("https://example.com:443/other"));
        assert!(!session.cookies_apply_to("https://cdn.example.net/seg.ts"));
        assert!(!session.cookies_apply_to("https://example.com.evil.com/x"));
        assert!(!session.cookies_apply_to("http://example.com/x"));
        let no_cookies = SessionHeaders {
            origin: Some("https://example.com".to_string()),
            ..Default::default()
        };
        assert!(!no_cookies.cookies_apply_to("https://example.com/x"));
    }

    /// One-shot local server answering a fixed status, so the tests
    /// can hold a real reqwest error with that status.
    async fn status_error(status: u16) -> reqwest::Error {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serve = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Read the request head first: answering before the client
            // has sent its request races with the socket close and
            // hyper reports the response as cancelled.
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(count) => {
                        request.extend_from_slice(&buffer[..count]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }
            let _ = stream
                .write_all(
                    format!(
                        "HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await;
        });
        let error = reqwest::Client::new()
            .get(format!("http://{addr}/"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap_err();
        let _ = serve.await;
        error
    }

    #[tokio::test]
    async fn auth_failure_label_maps_401_and_403_only() {
        let forbidden = anyhow::Error::new(status_error(403).await);
        assert_eq!(auth_failure_label(&forbidden), Some(AUTH_REQUIRED_MESSAGE));
        let unauthorized = anyhow::Error::new(status_error(401).await);
        assert_eq!(auth_failure_label(&unauthorized), Some(AUTH_REQUIRED_MESSAGE));
        let not_found = anyhow::Error::new(status_error(404).await);
        assert_eq!(auth_failure_label(&not_found), None);
        let plain = anyhow::anyhow!("Download server returned HTTP 403");
        assert_eq!(auth_failure_label(&plain), None);
    }

    #[tokio::test]
    async fn auth_failure_label_survives_context_wrapping() {
        let wrapped = anyhow::Error::new(status_error(403).await)
            .context("The server rejected both media inspection requests");
        assert_eq!(auth_failure_label(&wrapped), Some(AUTH_REQUIRED_MESSAGE));
    }

    #[tokio::test]
    async fn map_auth_error_relabels_403_and_keeps_other_statuses() {
        let relabelled = map_auth_error(status_error(403).await);
        assert_eq!(relabelled.to_string(), AUTH_REQUIRED_MESSAGE);
        let kept = map_auth_error(status_error(500).await);
        assert!(kept.to_string().contains("500"));
    }

    // Slice 6: known failure shapes get a plain-language label; auth
    // still wins over all of them.
    #[tokio::test]
    async fn download_failure_label_maps_known_shapes() {
        let not_found = anyhow::Error::new(status_error(404).await);
        assert_eq!(download_failure_label(&not_found).as_deref(), Some("This file is no longer available (404)."));
        let range = anyhow::Error::new(status_error(416).await);
        assert_eq!(download_failure_label(&range).as_deref(), Some("This download can't be resumed from where it stopped."));
        let server = anyhow::Error::new(status_error(503).await);
        assert!(download_failure_label(&server).unwrap().contains("503"));
        // Auth wins: a 403 is a login label, never a generic failure label.
        let forbidden = anyhow::Error::new(status_error(403).await);
        assert_eq!(download_failure_label(&forbidden), None);
        assert_eq!(error_text(&forbidden), AUTH_REQUIRED_MESSAGE);
        assert_eq!(error_text(&not_found), "This file is no longer available (404).");
        // An unknown error stays raw — no invented label.
        let plain = anyhow::anyhow!("something odd happened");
        assert_eq!(download_failure_label(&plain), None);
        assert_eq!(error_text(&plain), "something odd happened");
    }

    #[tokio::test]
    async fn download_failure_label_maps_connect_and_timeout() {
        // Unreachable localhost port: a real connect failure.
        let error = reqwest::Client::new()
            .get("http://127.0.0.1:1/")
            .send()
            .await
            .unwrap_err();
        let wrapped = anyhow::Error::new(error);
        assert_eq!(
            download_failure_label(&wrapped).as_deref(),
            Some("Could not reach the server. Check your connection and try again.")
        );
    }

    #[tokio::test]
    async fn validated_redirect_policy_revalidates_each_hop() {
        // N2: a redirect hop to a non-HTTP(S) URL must fail the
        // request, while a hop to another public-URL-shaped target
        // is still followed. (In test builds the validator allows
        // localhost, so the "public" hop here is a second local
        // path; the scheme check is what this test can prove. A
        // `file:` hop is not used: tower-http cannot resolve it to
        // a URI at all, so it returns the 302 un-followed without
        // ever consulting the policy — still never fetched, but not
        // a policy test. `ftp:` resolves and reaches the policy.)
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let thread_requests = requests.clone();
        let serve = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(connection) => connection,
                    Err(_) => return,
                };
                thread_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    match stream.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            request.extend_from_slice(&buffer[..count]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let path = text.split_whitespace().nth(1).unwrap_or("");
                let reply = match path {
                    "/hop" => "HTTP/1.1 302 Found\r\nLocation: ftp://example.com/evil\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
                    "/ok" => format!("HTTP/1.1 302 Found\r\nLocation: http://{addr}/final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
                    "/final" => "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone".to_string(),
                    _ => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
                };
                if stream.write_all(reply.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
        let client = reqwest::Client::builder()
            .redirect(validated_redirect_policy())
            .build()
            .unwrap();
        let blocked = client
            .get(format!("http://{addr}/hop"))
            .send()
            .await;
        assert!(blocked.is_err(), "a redirect to ftp:// must fail, not be followed");
        // The validation message rides on the error's source chain
        // ("error following redirect for url (...)" wraps it).
        let blocked_err = blocked.unwrap_err();
        let mut chain = blocked_err.to_string();
        {
            use std::error::Error as _;
            let mut source = blocked_err.source();
            while let Some(err) = source {
                chain.push_str(&format!(" <- {err}"));
                source = err.source();
            }
        }
        assert!(
            chain.contains("Redirect target failed validation"),
            "the error must name the failed redirect validation: {chain}"
        );
        let followed = client
            .get(format!("http://{addr}/ok"))
            .send()
            .await
            .unwrap();
        assert_eq!(followed.status(), reqwest::StatusCode::OK);
        assert_eq!(followed.text().await.unwrap(), "done");
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "exactly /hop once and the /ok -> /final chain: the blocked hop was never followed"
        );
        serve.abort();
    }

    #[tokio::test]
    async fn validated_redirect_policy_stops_after_ten_redirects() {
        // The cap at `validated_redirect_policy`: a chain of 11
        // redirects must fail with the limit named, and the final
        // URL must never be fetched. Paths /0 .. /10 each redirect
        // to the next hop; /11 is the final 200. `attempt.previous()`
        // includes the initial URL, so the `> 10` check fires on
        // the 11th redirect decision — 10 hops followed, 11 requests
        // served — and the chain never reaches /11.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let thread_requests = requests.clone();
        let serve = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(connection) => connection,
                    Err(_) => return,
                };
                thread_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    match stream.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            request.extend_from_slice(&buffer[..count]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let path = text.split_whitespace().nth(1).unwrap_or("");
                let hop: Option<u32> =
                    path.strip_prefix('/').and_then(|n| n.parse().ok());
                let reply = match hop {
                    Some(n) if n <= 10 => format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://{addr}/{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        n + 1
                    ),
                    Some(11) => "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone"
                        .to_string(),
                    _ => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string(),
                };
                if stream.write_all(reply.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
        let client = reqwest::Client::builder()
            .redirect(validated_redirect_policy())
            .build()
            .unwrap();
        let result = client.get(format!("http://{addr}/0")).send().await;
        assert!(
            result.is_err(),
            "an 11-redirect chain must fail at the cap, not be followed to the end"
        );
        let err = result.unwrap_err();
        let mut chain = err.to_string();
        {
            use std::error::Error as _;
            let mut source = err.source();
            while let Some(e) = source {
                chain.push_str(&format!(" <- {e}"));
                source = e.source();
            }
        }
        assert!(
            chain.contains("Too many redirects (limit 10)"),
            "the error must name the redirect limit: {chain}"
        );
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::Relaxed),
            11,
            "exactly the initial request plus the 10 hops the cap allows: /11 must never be fetched"
        );
        serve.abort();
    }
}

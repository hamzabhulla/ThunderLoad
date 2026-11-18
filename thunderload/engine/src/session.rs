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
use reqwest::header::{self, HeaderValue};
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};
use url::Url;

/// The browser session for one download job.
///
/// `None` (the default) means "no browser session": desktop-started
/// jobs, and any job whose handoff was absent or failed validation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// Origin the cookies belong to, e.g. `"https://example.com"`.
    /// Cookies are only sent to requests whose origin equals this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
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
                Self::origin_of(url).as_deref() == Some(origin.as_str())
            }
            _ => false,
        }
    }

    /// Apply this session to one request: Cookie only when the request
    /// URL is in scope, Referer and User-Agent whenever set. Never
    /// panics on a malformed header value — that header is skipped.
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
            if let Ok(value) = HeaderValue::from_str(referer) {
                request = request.header(header::REFERER, value);
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
}

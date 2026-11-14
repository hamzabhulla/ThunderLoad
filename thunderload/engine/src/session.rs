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
}

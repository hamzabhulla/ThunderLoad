// ThunderLoad MAIN-world media hook (Slice 2).
//
// Runs in the page's own JavaScript world (manifest content_scripts
// "world": "MAIN", document_start) so it can see what the isolated
// content script cannot: MediaSource objects, blob: URLs, and the
// original fetch/XHR URLs before a player rewrites them into an
// opaque/relay URL.
//
// It has NO chrome.* access: findings cross to the isolated content
// script via window.postMessage only, guarded by a per-page random
// token announced in a HELLO handshake. The token is NOT a secret
// from the page (any page script can read postMessage traffic) —
// it stops accidental collisions with the page's own messages and
// casual spoofing. The isolated side re-validates every finding
// (source + token + payload shape + its own classifier), and
// downloads still go through the native host's inspect, so a
// spoofed finding can only plant a fake list entry, never a
// download. Segments are never sent as findings: per-MediaSource
// segment counts are evidence only (1st + every 20th announced).
//
// The hook never breaks the page: every wrap calls the original
// through with its original arguments and return value, and every
// hook step is guarded so a failure degrades to the other detection
// paths (DOM scan + webRequest) instead of throwing.
(() => {
  if (window.__thunderLoadHookInstalled) return;
  window.__thunderLoadHookInstalled = true;

  const TOKEN = (window.crypto && typeof window.crypto.randomUUID === "function")
    ? window.crypto.randomUUID()
    : "tl-" + Math.random().toString(36).slice(2) + Date.now().toString(36);

  // URL classifier (mirror of content.js classify(), URL patterns
  // only — no Content-Type is visible here). Segments and YouTube
  // service URLs never classify: they are not downloadable media.
  const MEDIA_EXT = /\.(?:m3u8|mpd|mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav|ogg|opus|flac|pdf|zip)(?:$|[?#])/i;
  const SEGMENT_EXT = /\.(?:ts|m4s|cmfv|cmfa)(?:$|[?#])/i;
  const YOUTUBE_DASH_MANIFEST = /(?:^|[\/.])youtube\.com\/api\/manifest\/dash(?:[\/?#]|$)/i;
  const YOUTUBE_SERVICE_PATH = /^\/(?:s|api|youtubei)\//i;
  function isYouTubeServiceUrl(url) {
    try {
      const parsed = new URL(url);
      const host = parsed.hostname.toLowerCase();
      return (host === "youtube.com" || host.endsWith(".youtube.com") || host === "youtubei.googleapis.com")
        && YOUTUBE_SERVICE_PATH.test(parsed.pathname);
    } catch (_) { return false; }
  }
  function classify(url) {
    if (!url || !/^https?:\/\//i.test(url)) return null;
    if (/\.m3u8(?:$|[?#])/i.test(url)) return "HLS playlist";
    if (/\.mpd(?:$|[?#])/i.test(url)) return "DASH manifest";
    if (YOUTUBE_DASH_MANIFEST.test(url)) return "DASH manifest";
    if (SEGMENT_EXT.test(url)) return null;
    if (isYouTubeServiceUrl(url)) return null;
    if (MEDIA_EXT.test(url)) return "Direct media";
    return null;
  }

  function post(type, payload) {
    try {
      window.postMessage({ __thunderLoadHook: true, type, token: TOKEN, ...(payload || {}) }, "*");
    } catch (_) { /* The page may block postMessage; other paths still work. */ }
  }

  // Deduped URL findings (Flux keeps at most 500 segment URLs; the
  // same bound applies here so a chatty player cannot flood the
  // bridge — a URL already announced is never announced again).
  const MAX_SENT_URLS = 500;
  const sentUrls = new Set();
  function postUrlFinding(url) {
    if (!url || typeof url !== "string") return;
    let absolute = url;
    try { absolute = new URL(url, window.location.href).href; } catch (_) { return; }
    const kind = classify(absolute);
    if (!kind) return;
    if (sentUrls.has(absolute)) return;
    if (sentUrls.size >= MAX_SENT_URLS) return;
    sentUrls.add(absolute);
    post("THUNDERLOAD_HOOK_FINDING", { url: absolute, kind });
  }
  function recordFetchUrl(input) {
    try {
      if (typeof input === "string") postUrlFinding(input);
      else if (input instanceof URL) postUrlFinding(input.href);
      else if (input && typeof input.url === "string") postUrlFinding(input.url);
    } catch (_) { /* A weird fetch argument must never break fetch. */ }
  }

  // Handshake: the isolated content script pings for the token; the
  // hook answers with HELLO, echoing the PING's nonce (Slice 3
  // MINOR (a)) so the content script can tell a fresh HELLO from a
  // replayed one. The hook also announces itself once at startup
  // (in case a listener is already attached) — that HELLO carries
  // no nonce and is ignored until a PING is answered.
  window.addEventListener("message", (event) => {
    try {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || data.__thunderLoadHook !== true || data.type !== "THUNDERLOAD_HOOK_PING") return;
      post("THUNDERLOAD_HOOK_HELLO", { nonce: data.nonce });
    } catch (_) {}
  });
  post("THUNDERLOAD_HOOK_HELLO", {});

  // fetch: observe the URL only, never the response (no clone, no
  // body read — avoids CORS/opaque-response issues and keeps the
  // player's promise untouched).
  try {
    const origFetch = window.fetch;
    if (typeof origFetch === "function") {
      window.fetch = function (input, init) {
        try { recordFetchUrl(input); } catch (_) {}
        return origFetch.apply(this, arguments);
      };
    }
  } catch (_) {}

  // XMLHttpRequest: observe the URL at open() time, before the
  // player can rewrite it. The shared prototype is wrapped (the
  // only way to observe all XHR); behavior is preserved exactly.
  try {
    const origOpen = window.XMLHttpRequest && window.XMLHttpRequest.prototype
      ? window.XMLHttpRequest.prototype.open
      : null;
    if (typeof origOpen === "function") {
      window.XMLHttpRequest.prototype.open = function (method, url) {
        try { recordFetchUrl(url); } catch (_) {}
        return origOpen.apply(this, arguments);
      };
    }
  } catch (_) {}

  // MediaSource + blob: URLs. Per-instance wrapping only — the shared
  // MediaSource prototype is left untouched except for the
  // constructor itself, which must tag instances at creation.
  try {
    const OrigMediaSource = window.MediaSource;
    const origCreateObjectURL = window.URL && window.URL.createObjectURL
      ? window.URL.createObjectURL.bind(window.URL)
      : null;
    if (typeof OrigMediaSource === "function" && typeof origCreateObjectURL === "function") {
      const mediaSources = new WeakMap();
      let nextMediaSourceId = 1;
      function tagMediaSource(ms) {
        if (mediaSources.has(ms)) return mediaSources.get(ms);
        const state = { id: nextMediaSourceId++, mimes: [], segments: 0, bytes: 0 };
        mediaSources.set(ms, state);
        try {
          const origAddSourceBuffer = ms.addSourceBuffer;
          if (typeof origAddSourceBuffer === "function") {
            ms.addSourceBuffer = function (mimeType) {
              const sb = origAddSourceBuffer.call(this, mimeType);
              try {
                if (typeof mimeType === "string" && mimeType && !state.mimes.includes(mimeType)) {
                  state.mimes.push(mimeType);
                }
                const origAppend = sb.appendBuffer;
                if (typeof origAppend === "function") {
                  sb.appendBuffer = function (data) {
                    try {
                      state.segments += 1;
                      state.bytes += (data && data.byteLength) || 0;
                      // Announce the 1st segment and every 20th:
                      // proof the blob player is really streaming,
                      // without flooding the bridge per segment.
                      if (state.segments === 1 || state.segments % 20 === 0) {
                        post("THUNDERLOAD_HOOK_MSE", {
                          mediaSourceId: state.id,
                          mimeTypes: state.mimes.slice(),
                          segments: state.segments,
                          bytes: state.bytes,
                          duration: Number.isFinite(ms.duration) ? ms.duration : null
                        });
                      }
                    } catch (_) {}
                    return origAppend.call(this, data);
                  };
                }
              } catch (_) {}
              return sb;
            };
          }
        } catch (_) {}
        return state;
      }
      // Instances are real MediaSources (created by the original
      // constructor); instanceof checks keep working because the
      // wrapper's prototype is the original's.
      function MediaSourceWrapper() {
        const ms = new OrigMediaSource(...arguments);
        tagMediaSource(ms);
        return ms;
      }
      MediaSourceWrapper.prototype = OrigMediaSource.prototype;
      Object.setPrototypeOf(MediaSourceWrapper, OrigMediaSource);
      try { MediaSourceWrapper.isTypeSupported = OrigMediaSource.isTypeSupported.bind(OrigMediaSource); } catch (_) {}
      window.MediaSource = MediaSourceWrapper;
      window.URL.createObjectURL = function (obj) {
        const url = origCreateObjectURL(obj);
        try {
          if (obj instanceof OrigMediaSource) {
            const state = tagMediaSource(obj);
            post("THUNDERLOAD_HOOK_BLOB", {
              blobUrl: url,
              mediaSourceId: state.id,
              mimeTypes: state.mimes.slice(),
              duration: Number.isFinite(obj.duration) ? obj.duration : null
            });
          }
        } catch (_) {}
        return url;
      };
    }
  } catch (_) {}
})();

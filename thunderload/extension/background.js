const api = globalThis.browser ?? chrome;
const HOST_NAME = "com.thunderload.host";
let nativePort = null;
let nextRequestId = 1;
const pendingRequests = new Map();
const sourcesByTab = new Map();
const truncatedByTab = new Map();
const MAX_SOURCES_PER_TAB = 100;

// Fix 3: the ThunderLoad app / native host is required for Direct
// media. A googlevideo segment downloaded by the browser is one
// audio/video chunk, not the video, so there is no silent browser
// fallback any more: a missing permission or a disconnected host
// surfaces this error instead, pointing at the desktop app's
// one-time Browser setup (the setup steps themselves live in the
// desktop app, not here).
const NATIVE_NOT_CONNECTED_ERROR = "ThunderLoad app is not connected. Open the ThunderLoad desktop app and complete the one-time Browser setup, then try again.";
function isNativeConnectionError(message) {
  return /permission has not been granted|disconnected|timed out|native messaging host not found/i.test(String(message || ""));
}
function nativeErrorMessage(message) {
  const text = String(message || "");
  return isNativeConnectionError(text) ? NATIVE_NOT_CONNECTED_ERROR : text;
}

async function browserDownload(url, filename) {
  const options = { url, filename: filename || undefined, saveAs: false, conflictAction: "uniquify" };
  try {
    if (globalThis.browser && typeof globalThis.browser.downloads?.download === "function") {
      const downloadId = await globalThis.browser.downloads.download(options);
      return { ok: true, browserDownloadId: downloadId };
    }
    const downloadId = await new Promise((resolve, reject) => {
      api.downloads.download(options, (id) => {
        const error = api.runtime.lastError;
        if (error) reject(new Error(error.message));
        else resolve(id);
      });
    });
    return { ok: true, browserDownloadId: downloadId };
  } catch (error) {
    return { ok: false, error: error?.message || String(error) };
  }
}
async function hasNativePermission() {
  try { return await api.permissions.contains({ permissions: ["nativeMessaging"] }); }
  catch (_) { return false; }
}
function ensureNativePort() {
  if (nativePort) return nativePort;
  nativePort = api.runtime.connectNative(HOST_NAME);
  nativePort.onMessage.addListener((message) => {
    if (message && message.id && pendingRequests.has(message.id)) {
      const resolve = pendingRequests.get(message.id);
      pendingRequests.delete(message.id);
      resolve(message);
      return;
    }
    api.runtime.sendMessage({ type: "NATIVE_EVENT", event: message }).catch(() => {});
  });
  nativePort.onDisconnect.addListener(() => {
    nativePort = null;
    for (const [id, resolve] of pendingRequests) resolve({ id, ok: false, error: api.runtime.lastError?.message || "Native host disconnected" });
    pendingRequests.clear();
    api.runtime.sendMessage({ type: "NATIVE_DISCONNECTED" }).catch(() => {});
  });
  return nativePort;
}
async function requestNative(action, payload = {}) {
  if (!(await hasNativePermission())) return { ok: false, error: "Optional desktop-engine permission has not been granted" };
  // Cookie/header handoff (Slice 6): a browser-started download carries
  // the session its own URL needs — the browser's cookies for that URL
  // only, plus the browser user-agent. The referer stays a top-level
  // payload field (the native host merges it into the bundle). The host
  // validates the bundle against the download URL and the engine
  // re-checks the origin on every request, so the session never leaves
  // the user's machine and is never sent to any other origin. Only the
  // four download/inspect actions get a session.
  if (["download.start", "video.download", "dash.download", "video.inspect"].includes(action) && payload?.url) {
    payload = { ...payload, session: await sessionForUrl(payload.url) };
  }
  return new Promise((resolve) => {
    const id = "ext-" + Date.now() + "-" + nextRequestId++;
    try {
      const port = ensureNativePort();
      pendingRequests.set(id, resolve);
      port.postMessage({ id, action, ...payload });
      setTimeout(() => {
        if (pendingRequests.has(id)) { pendingRequests.delete(id); resolve({ id, ok: false, error: "Native host timed out" }); }
      }, 30000);
    } catch (error) {
      resolve({ id, ok: false, error: error?.message || String(error) });
    }
  });
}
// Builds the session handoff for one download URL: the browser's
// cookies for that URL only — `cookies.getAll({ url })` is URL-scoped,
// never a broad cookie read — plus the browser user-agent. A cookie
// whose name or value cannot appear in a Cookie header (control chars,
// `; , " \` or whitespace, per RFC 6265) is skipped, matching the
// engine's validation: a malformed cookie is dropped, never sent.
// Returns null when there is nothing to hand off (or the cookies
// permission is unavailable), so the download proceeds without a session.
async function sessionForUrl(url) {
  try {
    const cookies = await api.cookies.getAll({ url });
    const parts = [];
    for (const cookie of cookies || []) {
      if (!cookie?.name || !cookie.value) continue;
      if (/[\x00-\x1f\x7f;,\"\\\s]/.test(cookie.name) || /[\x00-\x1f\x7f;,\"\\\s]/.test(cookie.value)) continue;
      parts.push(cookie.name + "=" + cookie.value);
    }
    const userAgent = typeof navigator !== "undefined" && navigator.userAgent ? navigator.userAgent : null;
    if (!parts.length && !userAgent) return null;
    // The origin the cookies were captured for — the engine refuses
    // cookies without it (fail-closed), and only ever sends them to
    // requests on this exact origin.
    return { cookies: parts.length ? parts.join("; ") : null, userAgent, origin: new URL(url).origin };
  } catch (_) {
    return null;
  }
}
function safeFilenameFromUrl(url, fallback = "download.bin") {
  try {
    const raw = decodeURIComponent(new URL(url).pathname.split("/").pop() || fallback);
    return raw.replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_").slice(0, 180) || fallback;
  } catch (_) { return fallback; }
}

async function quickDownloadMedia({ url, kind, pageUrl, filename, selectedVariant }) {
  const name = filename || safeFilenameFromUrl(url, kind === "HLS playlist" ? "video.mp4" : "download.bin");

  // Fix 3: no silent browser fallback. A "Direct media" entry is
  // often one googlevideo DASH segment — a single audio or video
  // chunk, not the video — so downloading it in the browser produced
  // the audio-only bug with no error at all. Every path below either
  // starts the download in the ThunderLoad engine or returns a clear
  // error telling the user the desktop app / native host is not
  // connected and to complete the one-time Browser setup in the
  // desktop app. There is no explicit "Download in browser" action
  // in the overlay, so no browser-download path remains here.
  if (!(await hasNativePermission())) {
    return { ok: false, error: NATIVE_NOT_CONNECTED_ERROR };
  }
  const inspected = await requestNative("video.inspect", { url, referer: pageUrl || "" });
  if (!inspected?.ok) {
    return { ok: false, error: nativeErrorMessage(inspected?.error || "Could not inspect this media source.") };
  }
  const variants = Array.isArray(inspected.data?.variants) ? inspected.data.variants : [];
  const downloadable = variants.filter((item) => item?.downloadable && item?.url);
  downloadable.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
  const selected = selectedVariant && selectedVariant.downloadable && selectedVariant.url
    ? selectedVariant
    : downloadable[0];
  if (!selected) {
    return { ok: false, error: variants[0]?.note || "No downloadable stream was exposed by this page." };
  }
  const action = selected.kind === "hls" ? "video.download" : selected.kind === "dash" ? "dash.download" : "download.start";
  const result = await requestNative(action, {
    url: selected.url,
    filename: selected.kind === "hls" || selected.kind === "dash" ? (name.replace(/\.[^.]+$/, "") + ".mp4") : name,
    referer: pageUrl || "",
    representationId: selected.representationId,
    audioRepresentationId: selected.audioRepresentationId || ""
  });
  if (result?.ok) {
    return { ok: true, message: selected.quality && variants.length > 1
      ? "Downloading " + selected.quality + " in ThunderLoad."
      : "Download started in ThunderLoad." };
  }
  return { ok: false, error: nativeErrorMessage(result?.error || "ThunderLoad could not start this download.") };
}

// Download interception: every browser download goes through
// ThunderLoad instead of the browser. onCreated fires when the
// download starts, before any bytes land, so cancel + erase + a
// native start swaps the engine in with no partial file left behind.
// Fail-open: if the engine cannot start it, the browser download is
// restarted so the user still gets the file. Own downloads (started
// by this extension, e.g. the context-menu fallback) are never
// intercepted — that would loop forever. Incognito and non-http(s)
// downloads stay with the browser.
const INTERCEPT_SETTING_KEY = "interceptBrowserDownloads";
let interceptEnabled = true;
api.storage?.local?.get({ [INTERCEPT_SETTING_KEY]: true })
  .then((stored) => { interceptEnabled = stored?.[INTERCEPT_SETTING_KEY] !== false; })
  .catch(() => {});
api.storage?.onChanged?.addListener((changes, area) => {
  if (area === "local" && changes && Object.prototype.hasOwnProperty.call(changes, INTERCEPT_SETTING_KEY)) {
    interceptEnabled = changes[INTERCEPT_SETTING_KEY].newValue !== false;
  }
});
function downloadsCancel(id) {
  if (globalThis.browser && typeof globalThis.browser.downloads?.cancel === "function") return globalThis.browser.downloads.cancel(id);
  return new Promise((resolve) => { api.downloads.cancel(id, () => resolve()); });
}
function downloadsErase(id) {
  if (globalThis.browser && typeof globalThis.browser.downloads?.erase === "function") return globalThis.browser.downloads.erase(id);
  return new Promise((resolve) => { api.downloads.erase(id, () => resolve()); });
}
function cleanDownloadFilename(name, fallback) {
  const cleaned = String(name || "").replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_").slice(0, 180);
  return cleaned || fallback;
}
async function interceptDownload(downloadItem) {
  if (!interceptEnabled) return;
  if (!downloadItem || typeof downloadItem.id !== "number") return;
  if (downloadItem.byExtensionId && downloadItem.byExtensionId === api.runtime.id) return;
  if (!/^https?:\/\//i.test(downloadItem.url || "")) return;
  if (downloadItem.state === "complete") return;
  if (downloadItem.incognito) return;
  const fromItem = String(downloadItem.filename || "").split(/[\\/]/).pop() || "";
  const filename = cleanDownloadFilename(fromItem, safeFilenameFromUrl(downloadItem.url, "download.bin"));
  try { await downloadsCancel(downloadItem.id); } catch (_) { /* Already finished or gone. */ }
  try { await downloadsErase(downloadItem.id); } catch (_) { /* History entry already gone. */ }
  const native = await requestNative("download.start", { url: downloadItem.url, filename, referer: downloadItem.referrer || "" });
  if (!native?.ok) await browserDownload(downloadItem.url, filename);
}
if (api.downloads?.onCreated) {
  api.downloads.onCreated.addListener((downloadItem) => { interceptDownload(downloadItem).catch(() => {}); });
}

api.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === "VIDEO_SOURCES_FOUND") {
    const tabId = sender.tab?.id;
    if (typeof tabId === "number") {
      const map = new Map((sourcesByTab.get(tabId) || []).map((item) => [item.url, item]));
      for (const item of (message.sources || [])) {
        if (!item?.url || !/^https?:\/\//i.test(item.url)) continue;
        // Defense in depth: the content script filters service URLs
        // too, but DOM-sourced junk must never reach the popup even
        // if content.js is bypassed or stale.
        if (isYouTubeServiceUrl(item.url)) continue;
        map.set(item.url, enrichSource(item));
      }
      const all = sortSources(Array.from(map.values()));
      if (all.length > MAX_SOURCES_PER_TAB || message.truncated) truncatedByTab.set(tabId, true);
      sourcesByTab.set(tabId, all.slice(0, MAX_SOURCES_PER_TAB));
    }
    sendResponse({ ok: true });
    return false;
  }
  if (message?.type === "GET_ACTIVE_SOURCES") {
    (async () => {
      try {
        const [tab] = await api.tabs.query({ active: true, currentWindow: true });
        if (!tab?.id) return sendResponse({ ok: false, error: "No active tab" });
        // Recompute truncation from this scan (R1): clear the flag
        // before the fresh scan, so a page that shrank below the caps
        // stops reporting truncated. It is set again below from the
        // SCAN_NOW response and from any VIDEO_SOURCES_FOUND messages
        // that arrive during the settle wait.
        truncatedByTab.delete(tab.id);
        await api.scripting.executeScript({ target: { tabId: tab.id }, files: ["content.js"] });
        const scanResult = await api.tabs.sendMessage(tab.id, { type: "SCAN_NOW" }).catch(() => null);
        await new Promise((resolve) => setTimeout(resolve, 250));
        // The SCAN_NOW result is authoritative for this scan (R1
        // follow-up): it overwrites the flag after the settle wait,
        // so a mid-scan notify from add() that disagreed with the
        // scan's own recompute cannot leave a stale true behind.
        // Messages from the same scan agree with it; a null result
        // (content script blocked) clears the flag as well.
        if (scanResult?.truncated) truncatedByTab.set(tab.id, true);
        else truncatedByTab.delete(tab.id);
        sendResponse({ ok: true, tabId: tab.id, sources: sortSources(sourcesByTab.get(tab.id) || []), pageUrl: tab.url || "", truncated: !!truncatedByTab.get(tab.id) });
      } catch (error) {
        sendResponse({ ok: false, error: error?.message || "This page does not allow extension scanning" });
      }
    })();
    return true;
  }
  if (message?.type === "NATIVE_REQUEST") {
    requestNative(message.action, message.payload || {}).then(sendResponse);
    return true;
  }
  if (message?.type === "INSPECT_MEDIA") {
    requestNative("video.inspect", { url: message.url, referer: message.pageUrl || "" }).then((result) => {
      sendResponse(result?.ok
        ? { ok: true, variants: Array.isArray(result.data?.variants) ? result.data.variants : [] }
        : { ok: false, error: result?.error || "The desktop engine could not inspect this stream." });
    });
    return true;
  }
  if (message?.type === "QUICK_DOWNLOAD_MEDIA") {
    quickDownloadMedia(message).then(sendResponse).catch((error) => sendResponse({ ok: false, error: error?.message || String(error) }));
    return true;
  }
  if (message?.type === "BROWSER_DOWNLOAD") {
    browserDownload(message.url, message.filename).then(sendResponse).catch((error) => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  return false;
});

api.tabs.onRemoved.addListener((tabId) => { sourcesByTab.delete(tabId); truncatedByTab.delete(tabId); });

// IDM-style network backstop: catch media the DOM scan cannot see
// (fetch/XHR-loaded streams, cross-origin iframes). Observation only —
// nothing is blocked, modified or redirected. The classifier mirrors the
// extension used by content.js so both paths report the same kinds.
const NETWORK_MEDIA_EXT = /\.(?:m3u8|mpd|mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav|ogg|opus|flac|pdf|zip)(?:$|[?#])/i;
const NETWORK_SEGMENT_EXT = /\.(?:ts|m4s|cmfv|cmfa)(?:$|[?#])/i;
// Fix 3 — YouTube segment labels + DASH-manifest-first ordering.
// A googlevideo segment URL carries its media type in query params
// (mime=audio/mp4 vs mime=video/mp4, itag=140 for audio-only AAC),
// so the popup can tell an audio chunk from a video chunk instead
// of listing every segment as a blind "Direct media" card.
const YOUTUBE_DASH_MANIFEST = /(?:^|[\/.])youtube\.com\/api\/manifest\/dash(?:[\/?#]|$)/i;
function parseGoogleVideoSegment(url) {
  try {
    const parsed = new URL(url);
    if (!/\.googlevideo\.com$/i.test(parsed.hostname)) return null;
    const mime = parsed.searchParams.get("mime") || "";
    const itag = parsed.searchParams.get("itag") || "";
    if (!mime && !itag) return null;
    const mediaType = /^audio\//i.test(mime) ? "audio" : /^video\//i.test(mime) ? "video" : null;
    if (!mediaType && !/^\d+$/.test(itag)) return null;
    return { mediaType, itag: itag || null };
  } catch (_) { return null; }
}
function enrichSource(item) {
  if (!item?.url) return item;
  const url = item.url;
  if (YOUTUBE_DASH_MANIFEST.test(url)) {
    return { ...item, kind: "DASH manifest", label: "YouTube video (DASH)", isYouTubeDash: true };
  }
  if (item.kind === "DASH manifest") {
    return { ...item, label: "DASH manifest" };
  }
  if (item.kind === "Direct media") {
    const segment = parseGoogleVideoSegment(url);
    if (segment) {
      const type = segment.mediaType === "audio" ? "Audio" : segment.mediaType === "video" ? "Video" : "Media";
      return { ...item, label: segment.itag ? type + " (itag " + segment.itag + ")" : type, mediaType: segment.mediaType, itag: segment.itag };
    }
  }
  return item;
}
function sourceRank(item) {
  if (item.kind === "DASH manifest") return item.isYouTubeDash ? 0 : 1;
  if (item.kind === "HLS playlist") return 2;
  if (item.kind === "Direct media") return item.mediaType === "video" ? 3 : item.mediaType === "audio" ? 4 : 5;
  return 6;
}
function sortSources(list) {
  return [...list].sort((a, b) => sourceRank(a) - sourceRank(b) || (a.discoveredAt || 0) - (b.discoveredAt || 0));
}
// YouTube service endpoints serve JSON/search data with an audio/video
// Content-Type (e.g. www.youtube.com/s/search/audio) — not media.
// They are not downloadable files, so they must never appear as
// "Direct media". The DASH-manifest check above runs first, so
// /api/manifest/dash is unaffected by this exclusion.
const YOUTUBE_SERVICE_PATH = /^\/(?:s|api|youtubei)\//i;
function isYouTubeServiceUrl(url) {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    return (host === "youtube.com" || host.endsWith(".youtube.com") || host === "youtubei.googleapis.com")
      && YOUTUBE_SERVICE_PATH.test(parsed.pathname);
  } catch (_) { return false; }
}
function classifyNetworkResponse(url, headers) {
  if (!url || !/^https?:\/\//i.test(url)) return null;
  const contentType = (headers || []).find((header) => header.name?.toLowerCase() === "content-type")?.value?.toLowerCase() || "";
  if (/\.m3u8(?:$|[?#])/i.test(url) || contentType.includes("application/x-mpegurl") || contentType.includes("application/vnd.apple.mpegurl")) return "HLS playlist";
  // YouTube's DASH manifest has no .mpd suffix, and its Content-Type
  // is not relied on here: match the manifest URL pattern itself, so
  // detection works whichever Content-Type YouTube serves it with.
  // (The application/dash+xml check below stays as a second trigger
  // for manifests served with that type.) The manifest is fetched
  // via XHR, which the onHeadersReceived listener below already
  // observes, so no new webRequest filter was needed.
  if (YOUTUBE_DASH_MANIFEST.test(url)) return "DASH manifest";
  if (/\.mpd(?:$|[?#])/i.test(url) || contentType.includes("application/dash+xml")) return "DASH manifest";
  if (NETWORK_SEGMENT_EXT.test(url)) return null;
  if (isYouTubeServiceUrl(url)) return null;
  if (contentType.startsWith("video/") || contentType.startsWith("audio/")) return "Direct media";
  if (NETWORK_MEDIA_EXT.test(url)) return "Direct media";
  return null;
}
function recordNetworkSource(tabId, url, kind) {
  if (typeof tabId !== "number" || tabId < 0) return;
  const map = new Map((sourcesByTab.get(tabId) || []).map((item) => [item.url, item]));
  // Re-enrich an existing entry too: a segment first seen by the DOM
  // scan arrives as a bare "Direct media" label, and only the network
  // observer knows its mime/itag.
  const previous = map.get(url);
  map.set(url, enrichSource({ url, kind, label: previous?.label || kind, discoveredAt: previous?.discoveredAt || Date.now() }));
  const all = sortSources(Array.from(map.values()));
  if (all.length > MAX_SOURCES_PER_TAB) truncatedByTab.set(tabId, true);
  sourcesByTab.set(tabId, all.slice(0, MAX_SOURCES_PER_TAB));
}
if (api.webRequest?.onHeadersReceived) {
  api.webRequest.onHeadersReceived.addListener((details) => {
    const kind = classifyNetworkResponse(details.url, details.responseHeaders || []);
    if (kind) recordNetworkSource(details.tabId, details.url, kind);
  }, { urls: ["<all_urls>"], types: ["xmlhttprequest", "media", "other"] }, ["responseHeaders"]);
}
api.tabs.onUpdated.addListener((tabId, changeInfo) => { if (changeInfo.status === "loading") { sourcesByTab.delete(tabId); truncatedByTab.delete(tabId); } });
api.commands?.onCommand.addListener(async (command) => {
  if (command !== "scan-media") return;
  const [tab] = await api.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id) return;
  try {
    await api.scripting.executeScript({ target: { tabId: tab.id }, files: ["content.js"] });
    await api.tabs.sendMessage(tab.id, { type: "SCAN_NOW" });
  } catch (_) { /* Browser-internal pages cannot be inspected. */ }
});
api.runtime.onInstalled?.addListener(() => {
  try { api.contextMenus.create({ id: "thunderload-download-link", title: "Download link with ThunderLoad", contexts: ["link"] }); }
  catch (_) { /* Already registered. */ }
});
api.contextMenus?.onClicked.addListener(async (info) => {
  if (info.menuItemId !== "thunderload-download-link" || !info.linkUrl) return;
  const filename = safeFilenameFromUrl(info.linkUrl);
  const native = await requestNative("download.start", { url: info.linkUrl, filename, referer: info.pageUrl || "" });
  if (native?.ok) return;
  try {
    const result = await browserDownload(info.linkUrl, filename);
    api.runtime.sendMessage({ type: "CONTEXT_DOWNLOAD_RESULT", result }).catch(() => {});
  } catch (error) {
    api.runtime.sendMessage({ type: "CONTEXT_DOWNLOAD_RESULT", result: { ok: false, error: error.message } }).catch(() => {});
  }
});
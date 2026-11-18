const api = globalThis.browser ?? chrome;
const HOST_NAME = "com.thunderload.host";
let nativePort = null;
let nextRequestId = 1;
const pendingRequests = new Map();
const sourcesByTab = new Map();
const truncatedByTab = new Map();
const MAX_SOURCES_PER_TAB = 100;

// Slice 3: yt-dlp routing for YouTube/social pages (option b —
// neither tool is bundled; both download on first use from Settings).
// The extension never downloads YouTube itself: it sends the PAGE URL
// to the native host, which runs yt-dlp and returns formats. The
// per-page cache stops a panel re-open from re-spawning yt-dlp
// (inspect takes 2–10 s); ytdlpByTab remembers which tab an inspect
// belongs to so GET_ACTIVE_SOURCES can suppress the raw googlevideo
// rows once yt-dlp formats exist.
const YTDLP_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "tiktok.com", "twitter.com", "x.com", "facebook.com", "fb.watch", "reddit.com", "twitch.tv", "vimeo.com"];
const YTDLP_CACHE_TTL_MS = 5 * 60 * 1000;
const YTDLP_CACHE_MAX = 50;
const ytdlpCache = new Map();
const ytdlpByTab = new Map();
// The cache is bounded: stale entries are dropped and the oldest
// entries are evicted past the cap, on every write. A hit-only check
// would let dead entries pile up forever in a long-lived worker.
function pruneYtdlpCache() {
  const now = Date.now();
  for (const [key, entry] of ytdlpCache) {
    if (now - entry.at >= YTDLP_CACHE_TTL_MS) ytdlpCache.delete(key);
  }
  while (ytdlpCache.size > YTDLP_CACHE_MAX) {
    const oldest = ytdlpCache.keys().next().value;
    if (oldest === undefined) break;
    ytdlpCache.delete(oldest);
  }
}
function isYtDlpPageUrl(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return YTDLP_HOSTS.some((listed) => host === listed || host.endsWith("." + listed));
  } catch (_) { return false; }
}
function isGoogleVideoRow(item) {
  try { return /\.googlevideo\.com$/i.test(new URL(item?.url || "").hostname); } catch (_) { return false; }
}
const YTDLP_RELEASE_PAGES = {
  ytdlp: "https://github.com/yt-dlp/yt-dlp/releases/tag/2026.10.26",
  deno: "https://github.com/denoland/deno/releases/tag/v2.9.8"
};

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
  // Chrome only exposes runtime.connectNative while the optional
  // nativeMessaging permission is granted; without it the function
  // is undefined and calling it throws a raw TypeError ("... is not
  // a function") that would reach the user as a toast. Check first
  // and fail with the clean permission error instead — requestNative's
  // catch turns it into { ok: false, error }, and quickDownloadMedia
  // maps it to the "app is not connected" message.
  if (typeof api.runtime.connectNative !== "function") {
    throw new Error("Optional desktop-engine permission has not been granted");
  }
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
async function requestNative(action, payload = {}, timeoutMs = 30000) {
  if (!(await hasNativePermission())) return { ok: false, error: "Optional desktop-engine permission has not been granted" };
  // Cookie/header handoff (Slice 6): a browser-started download carries
  // the session its own URL needs — the browser's cookies for that URL
  // only, plus the browser user-agent. The referer stays a top-level
  // payload field (the native host merges it into the bundle). The host
  // validates the bundle against the download URL and the engine
  // re-checks the origin on every request, so the session never leaves
  // the user's machine and is never sent to any other origin. Only the
  // six download/inspect actions get a session.
  if (["download.start", "video.download", "dash.download", "video.inspect", "ytdlp.inspect", "ytdlp.download"].includes(action) && payload?.url) {
    payload = { ...payload, session: await sessionForUrl(payload.url) };
  }
  return new Promise((resolve) => {
    const id = "ext-" + Date.now() + "-" + nextRequestId++;
    try {
      const port = ensureNativePort();
      pendingRequests.set(id, resolve);
      port.postMessage({ id, action, ...payload });
      // Slice 3: tool downloads (22 MB yt-dlp + 41 MB Deno) outlast
      // the 30 s default, so ensure_tool passes a 10-minute timeout.
      setTimeout(() => {
        if (pendingRequests.has(id)) { pendingRequests.delete(id); resolve({ id, ok: false, error: "Native host timed out" }); }
      }, timeoutMs);
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
    // Slice 4: the master playlist URL, so the engine can re-resolve it
    // with the selected variant + audio instead of downloading the child
    // media playlist alone (which drops the audio track).
    masterUrl: url,
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
      mergeTabSources(tabId, message.sources || [], message.truncated);
      persistTabSources(tabId, sender.tab?.url);
    }
    sendResponse({ ok: true });
    return false;
  }
  if (message?.type === "GET_ACTIVE_SOURCES") {
    (async () => {
      try {
        const [tab] = await api.tabs.query({ active: true, currentWindow: true });
        if (!tab?.id) return sendResponse({ ok: false, error: "No active tab" });
        // Rehydrate persisted detections for this exact page first,
        // so a restarted worker still shows sources even if the live
        // scan below finds nothing new. The return value carries the
        // persisted truncation signal: truncatedByTab is cleared
        // below before the live scan, so it must be captured here
        // and OR-ed into the final flag.
        const rehydratedTruncated = await rehydrateTabSources(tab.id, tab.url || "");
        // Recompute truncation from this scan (R1): clear the flag
        // before the fresh scan, so a page that shrank below the caps
        // stops reporting truncated. It is set again below from the
        // SCAN_NOW response, the scanned-media pull, and any
        // VIDEO_SOURCES_FOUND messages that arrive during the settle
        // wait.
        truncatedByTab.delete(tab.id);
        await api.scripting.executeScript({ target: { tabId: tab.id }, files: ["content.js"] });
        const scanResult = await api.tabs.sendMessage(tab.id, { type: "SCAN_NOW" }).catch(() => null);
        await new Promise((resolve) => setTimeout(resolve, 250));
        // Pull the content script's own scanned media (audit F1/F6):
        // add() only ever reports NEW items, so SCAN_NOW's count and
        // the VIDEO_SOURCES_FOUND messages from the settle window
        // cannot repopulate a sourcesByTab wiped by a worker restart.
        // GET_SCANNED_MEDIA returns the content script's full `found`
        // map, which survives worker restarts (it lives in the page).
        // Fail-open with a timeout: a tab still running a content
        // script from an older extension version has no such handler
        // and would never respond, so never await it unboundedly.
        const scanned = await Promise.race([
          api.tabs.sendMessage(tab.id, { type: "GET_SCANNED_MEDIA" }).catch(() => null),
          new Promise((resolve) => setTimeout(() => resolve(null), 250))
        ]);
        let mergedTruncated = false;
        if (scanned && Array.isArray(scanned.sources)) {
          mergedTruncated = mergeTabSources(tab.id, scanned.sources, scanned.truncated);
        }
        // Slice 2: merge service-worker (tabId === -1) sources that
        // match this tab's page — exact Referer first, initiator
        // origin as fallback. Runs after the live scan so the flag
        // below can include the merge-overflow signal.
        const unassignedTruncated = await mergeUnassignedForTab(tab.id, tab.url || "");
        // The scan results are authoritative for this scan (R1
        // follow-up): they overwrite the flag after the settle wait,
        // so a mid-scan notify from add() that disagreed with the
        // scan's own recompute cannot leave a stale true behind.
        // The merge overflow above is a third signal: network +
        // DOM sources combined can exceed the cap even when neither
        // scan flag is set. The persisted truncation signal from the
        // rehydrate above is a fourth: a stored truncated:true must
        // survive a live scan that reports truncated:false. The
        // unassigned merge is a fifth: it can push the list over
        // the cap after the live scan.
        if (scanResult?.truncated || mergedTruncated || rehydratedTruncated || unassignedTruncated) truncatedByTab.set(tab.id, true);
        else truncatedByTab.delete(tab.id);
        await persistTabSources(tab.id, tab.url || "");
        // Slice 3: yt-dlp formats replace the raw sniffed rows. Once
        // they exist for this tab's page, the googlevideo segments and
        // the DASH manifest are suppressed — they are fragments and a
        // manifest for the same video, not extra downloads. When yt-dlp
        // returned nothing, the sniffed manifest stays as the fallback
        // row (it is only suppressed when formats exist).
        const ytdlpEntry = ytdlpByTab.get(tab.id);
        const ytdlpForPage = ytdlpEntry && ytdlpEntry.pageUrl === (tab.url || "") ? ytdlpEntry : null;
        const hasYtDlpFormats = !!(ytdlpForPage && Array.isArray(ytdlpForPage.variants) && ytdlpForPage.variants.length);
        let sources = sortSources(sourcesByTab.get(tab.id) || []);
        if (hasYtDlpFormats) {
          sources = sources.filter((item) => !isGoogleVideoRow(item) && !item.isYouTubeDash);
        }
        sendResponse({ ok: true, tabId: tab.id, sources, pageUrl: tab.url || "", truncated: !!truncatedByTab.get(tab.id),
          ytdlp: ytdlpForPage ? { variants: ytdlpForPage.variants, title: ytdlpForPage.title } : null });
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
  // Slice 1b: the in-page panel's "Connect desktop app" button.
  // The permission prompt is unreachable from the popup when no
  // media is detected (no Download button to click) and the in-page
  // Download never asks — so the panel asks explicitly. The request
  // runs here: content scripts cannot call permissions.request
  // themselves. MV3 may not carry the click's user gesture across
  // the message round-trip, so a false/failed request is NOT an
  // error — the panel shows the manual steps instead. The download
  // path (requestNative) is untouched: it keeps failing clean,
  // never prompting mid-download.
  if (message?.type === "REQUEST_NATIVE_PERMISSION") {
    (async () => {
      try {
        if (typeof api.permissions?.request !== "function") return sendResponse({ ok: true, granted: false });
        const granted = await api.permissions.request({ permissions: ["nativeMessaging"] });
        sendResponse({ ok: true, granted: !!granted });
      } catch (error) {
        sendResponse({ ok: true, granted: false, error: error?.message || String(error) });
      }
    })();
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
  // Slice 3: yt-dlp routing. The page URL (never a sniffed media
  // URL) goes to the native host, which runs `yt-dlp -j` with the
  // session handoff and returns formats mapped onto VideoVariant.
  if (message?.type === "YTDLP_INSPECT") {
    (async () => {
      const pageUrl = message.pageUrl || "";
      const tabId = sender.tab?.id;
      const cached = ytdlpCache.get(pageUrl);
      if (cached && Date.now() - cached.at < YTDLP_CACHE_TTL_MS) {
        if (typeof tabId === "number") ytdlpByTab.set(tabId, { pageUrl, variants: cached.variants, title: cached.title });
        sendResponse({ ok: true, variants: cached.variants, title: cached.title, cached: true });
        return;
      }
      // 65 s: the engine allows 60 s for a slow inspect; the default
      // 30 s requestNative timeout would cut a 30–60 s inspect off in
      // the extension even though the engine would succeed.
      const result = await requestNative("ytdlp.inspect", { url: pageUrl }, 65000);
      if (!result?.ok) {
        sendResponse({ ok: false, error: result?.error || "yt-dlp could not inspect this page.", toolsMissing: result?.toolsMissing || null });
        return;
      }
      const variants = Array.isArray(result.data?.variants) ? result.data.variants : [];
      const title = result.data?.title || "";
      ytdlpCache.set(pageUrl, { variants, title, at: Date.now() });
      pruneYtdlpCache();
      if (typeof tabId === "number") ytdlpByTab.set(tabId, { pageUrl, variants, title });
      sendResponse({ ok: true, variants, title, cached: false });
    })();
    return true;
  }
  if (message?.type === "YTDLP_DOWNLOAD") {
    requestNative("ytdlp.download", { url: message.pageUrl, formatSelector: message.formatSelector, filename: message.filename }).then((result) => {
      sendResponse(result?.ok
        ? { ok: true, message: result.message || "Download started in ThunderLoad." }
        : { ok: false, error: nativeErrorMessage(result?.error || "ThunderLoad could not start this download.") });
    });
    return true;
  }
  // Slice 3: on-demand tool download. Called ONLY from the direct
  // download button in the panel/popup/Settings — the tap is the
  // consent; nothing here ever fetches a tool silently. The 10-minute
  // timeout covers both the 22 MB yt-dlp and the 41 MB Deno zip.
  // Progress arrives as NATIVE_EVENTs (ytdlp.tool_progress) forwarded
  // by ensureNativePort, not through this response.
  if (message?.type === "YTDLP_ENSURE_TOOL") {
    requestNative("ytdlp.ensure_tool", { tool: message.tool }, 600000).then((result) => {
      sendResponse(result?.ok
        ? { ok: true, path: result.path || "" }
        : { ok: false, error: result?.error || "The tool could not be downloaded." });
    });
    return true;
  }
  if (message?.type === "YTDLP_TOOLS_STATUS") {
    requestNative("ytdlp.tools_status", {}).then((result) => {
      sendResponse(result?.ok ? { ok: true, ...(result.data || {}) } : { ok: false, error: result?.error || "Could not read the tool status." });
    });
    return true;
  }
  if (message?.type === "YTDLP_CHECK_UPDATE") {
    requestNative("ytdlp.check_update", {}).then((result) => {
      sendResponse(result?.ok ? { ok: true, message: result.message || "" } : { ok: false, error: result?.error || "Could not check for updates." });
    });
    return true;
  }
  // Slice 3: manual alternative (backup only) — open the official
  // release page so the user can download the tool themselves; the
  // app picks it up from the tools dir.
  if (message?.type === "YTDLP_OPEN_MANUAL_PAGE") {
    const url = YTDLP_RELEASE_PAGES[message.tool] || YTDLP_RELEASE_PAGES.ytdlp;
    if (typeof api.tabs?.create === "function") api.tabs.create({ url }).catch(() => {});
    sendResponse({ ok: true });
    return false;
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

api.tabs.onRemoved.addListener((tabId) => { sourcesByTab.delete(tabId); truncatedByTab.delete(tabId); ytdlpByTab.delete(tabId); removePersistedTabSources(tabId); });

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
// Shared merge for every detection path (DOM messages, network
// observer, storage rehydrate, scanned-media pull): dedupe by URL,
// drop non-http(s) and YouTube service URLs, re-enrich, sort, cap.
// Returns true when the merged list is truncated (cap overflow or the
// caller's truncated flag), so the caller can set the flag exactly.
function mergeTabSources(tabId, items, truncatedFlag) {
  if (typeof tabId !== "number") return false;
  const map = new Map((sourcesByTab.get(tabId) || []).map((item) => [item.url, item]));
  for (const item of (items || [])) {
    if (!item?.url || !/^https?:\/\//i.test(item.url)) continue;
    // Defense in depth: the content script filters service URLs
    // too, but DOM-sourced junk must never reach the popup even
    // if content.js is bypassed or stale.
    // The DASH-manifest check runs first, mirroring classify() and
    // classifyNetworkResponse(): /api/manifest/dash matches the
    // service-path pattern (/api/...) and would otherwise be
    // excluded as a service URL — the manifest must reach the popup.
    if (!YOUTUBE_DASH_MANIFEST.test(item.url) && isYouTubeServiceUrl(item.url)) continue;
    map.set(item.url, enrichSource(item));
  }
  const all = sortSources(Array.from(map.values()));
  const truncated = all.length > MAX_SOURCES_PER_TAB || !!truncatedFlag;
  if (truncated) truncatedByTab.set(tabId, true);
  sourcesByTab.set(tabId, all.slice(0, MAX_SOURCES_PER_TAB));
  return truncated;
}

// Persisted detections (audit F3): sourcesByTab lives only in worker
// memory and is wiped on every service-worker restart. Mirror it to
// chrome.storage.local per tab, together with the tab's page URL, so a
// restarted worker can rehydrate — but only for the same page (a
// navigation makes stored sources stale).
//
// Storage schema (one key per tab, minimal, namespaced):
//   key:   "thunderload.tabSources.v1.<tabId>"
//   value: { pageUrl: string, sources: Array<{url, kind, label,
//            discoveredAt, isYouTubeDash?, mediaType?, itag?,
//            referer?}>, truncated: boolean, updatedAt: number }
// The intercept toggle ("interceptBrowserDownloads") is a separate
// key and is untouched by this code.
//
// Storage is best-effort: every call below is guarded, so a missing
// storage API or a quota error can only lose the mirror, never
// detection. The "storage" permission is declared in both manifests
// (no install warning), so chrome.storage is available in the real
// browser; the guards only cover a missing API on older installs.
const TAB_SOURCES_KEY_PREFIX = "thunderload.tabSources.v1.";
function tabSourcesKey(tabId) { return TAB_SOURCES_KEY_PREFIX + tabId; }
// JSON of the last value written per tab (pageUrl + sources +
// truncated, without updatedAt), so re-recording an unchanged list
// does not hammer storage.
const lastPersistedJsonByTab = new Map();
function storageLocal() { return api.storage?.local ?? null; }
async function persistTabSources(tabId, pageUrl) {
  const local = storageLocal();
  if (!local || !pageUrl) return;
  try {
    const sources = sourcesByTab.get(tabId) || [];
    const key = tabSourcesKey(tabId);
    if (!sources.length) {
      await local.remove(key);
      lastPersistedJsonByTab.delete(tabId);
      return;
    }
    const value = { pageUrl, sources, truncated: !!truncatedByTab.get(tabId), updatedAt: Date.now() };
    const dedupeJson = JSON.stringify({ pageUrl: value.pageUrl, sources: value.sources, truncated: value.truncated });
    if (lastPersistedJsonByTab.get(tabId) === dedupeJson) return;
    lastPersistedJsonByTab.set(tabId, dedupeJson);
    await local.set({ [key]: value });
  } catch (_) { /* Storage is a best-effort mirror. */ }
}
async function removePersistedTabSources(tabId) {
  lastPersistedJsonByTab.delete(tabId);
  try { await storageLocal()?.remove(tabSourcesKey(tabId)); } catch (_) { /* Best-effort. */ }
}
// Returns mergeTabSources' truncation result (true when the restored
// list is truncated), so GET_ACTIVE_SOURCES can OR the persisted
// signal into the final flag — the flag is cleared and recomputed
// from the live scan, so a persisted truncated:true would otherwise
// be lost when the live scan reports truncated:false.
async function rehydrateTabSources(tabId, pageUrl) {
  const local = storageLocal();
  if (!local || !pageUrl) return false;
  try {
    const entry = (await local.get(tabSourcesKey(tabId)))?.[tabSourcesKey(tabId)];
    if (!entry || entry.pageUrl !== pageUrl || !Array.isArray(entry.sources)) return false;
    return mergeTabSources(tabId, entry.sources, entry.truncated);
  } catch (_) { /* A stale or unreadable entry is simply ignored. */ return false; }
}

// Worker startup: rehydrate persisted detections for tabs that still
// show the same page; drop entries for closed tabs or navigated pages.
(async () => {
  const local = storageLocal();
  if (!local) return;
  try {
    const all = (await local.get(null)) || {};
    for (const [key, entry] of Object.entries(all)) {
      if (!key.startsWith(TAB_SOURCES_KEY_PREFIX)) continue;
      const tabId = Number(key.slice(TAB_SOURCES_KEY_PREFIX.length));
      if (!Number.isInteger(tabId)) continue;
      try {
        const tab = await api.tabs.get(tabId);
        if (tab?.url && entry?.pageUrl === tab.url && Array.isArray(entry.sources)) {
          mergeTabSources(tabId, entry.sources, entry.truncated);
        } else {
          await local.remove(key).catch(() => {});
        }
      } catch (_) {
        // Tab is gone; its stored entry is stale.
        await local.remove(key).catch(() => {});
      }
    }
  } catch (_) { /* Rehydrate is best-effort. */ }
})();

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
  // URL-pattern checks first (parity with content.js classify()):
  // an .m3u8/.mpd suffix or the YouTube DASH-manifest URL classifies
  // regardless of Content-Type.
  if (/\.m3u8(?:$|[?#])/i.test(url)) return "HLS playlist";
  // YouTube's DASH manifest has no .mpd suffix, and its Content-Type
  // is not relied on here: match the manifest URL pattern itself, so
  // detection works whichever Content-Type YouTube serves it with.
  if (YOUTUBE_DASH_MANIFEST.test(url)) return "DASH manifest";
  if (/\.mpd(?:$|[?#])/i.test(url)) return "DASH manifest";
  // Slice 2: the service-URL exclusion runs BEFORE any Content-Type
  // check. A YouTube service endpoint serving JSON with a video/* or
  // mpegurl Content-Type is not media — previously the m3u8
  // Content-Type check above it could classify such a response as
  // an HLS playlist (mergeTabSources dropped it as defense in depth,
  // but the classifier itself was wrong).
  if (isYouTubeServiceUrl(url)) return null;
  // Content-Type checks (extension-less manifests/segments): HLS
  // playlist types per the media-sniffing standard, DASH, then
  // generic video/audio. A Content-Type alone never overrides the
  // junk filter above.
  if (contentType.includes("application/x-mpegurl") || contentType.includes("application/vnd.apple.mpegurl")
    || contentType.includes("audio/mpegurl") || contentType.includes("audio/x-mpegurl")) return "HLS playlist";
  if (contentType.includes("application/dash+xml")) return "DASH manifest";
  if (NETWORK_SEGMENT_EXT.test(url)) return null;
  if (contentType.startsWith("video/") || contentType.startsWith("audio/")) return "Direct media";
  if (NETWORK_MEDIA_EXT.test(url)) return "Direct media";
  return null;
}
function recordNetworkSource(tabId, url, kind, extra = {}) {
  if (typeof tabId === "number" && tabId < 0) {
    // Slice 2: the site's service worker fetched this — no tab to
    // attribute it to. Dropping it loses real detections;
    // attributing it to a random tab is worse. Keep it unassigned;
    // GET_ACTIVE_SOURCES merges only the ones matching the active
    // tab's page (exact Referer match, initiator-origin fallback).
    recordUnassignedSource(url, kind, extra.referer, extra.initiator);
    return;
  }
  if (typeof tabId !== "number") return;
  // Re-enrich an existing entry too: a segment first seen by the DOM
  // scan arrives as a bare "Direct media" label, and only the network
  // observer knows its mime/itag.
  const previous = (sourcesByTab.get(tabId) || []).find((item) => item.url === url);
  mergeTabSources(tabId, [{ url, kind, label: previous?.label || kind, discoveredAt: previous?.discoveredAt || Date.now(), ...(extra.referer ? { referer: extra.referer } : {}) }], false);
  // Persist with the tab's current page URL (the manifest is observed
  // here, often before the popup ever opens, so the URL is looked up
  // rather than taken from a map that may not know this tab yet).
  try { api.tabs.get(tabId).then((tab) => persistTabSources(tabId, tab?.url)).catch(() => {}); }
  catch (_) { /* Tabs lookup is best-effort. */ }
}

// Slice 2: Referer capture. onSendHeaders records the Referer per
// requestId so the response handler can attach it to the source —
// many CDNs 403 without it, so later authenticated requests (Slice
// 5/6) need it. The map is bounded (FIFO eviction) and entries are
// dropped when their response is observed or the request errors,
// so it cannot grow without bound. No new permission: requestHeaders
// is an extraInfoSpec, not a permission (webRequest is declared).
const refererByRequestId = new Map();
const REFERER_MAP_MAX = 1000;
if (api.webRequest?.onSendHeaders) {
  api.webRequest.onSendHeaders.addListener((details) => {
    try {
      const referer = (details.requestHeaders || []).find((header) => header.name?.toLowerCase() === "referer")?.value || "";
      if (!referer) return;
      if (refererByRequestId.size >= REFERER_MAP_MAX) {
        const first = refererByRequestId.keys().next().value;
        if (first !== undefined) refererByRequestId.delete(first);
      }
      refererByRequestId.set(details.requestId, referer);
    } catch (_) { /* Referer capture is best-effort. */ }
  }, { urls: ["<all_urls>"], types: ["xmlhttprequest", "media", "other"] }, ["requestHeaders"]);
}
function takeReferer(requestId) {
  const referer = refererByRequestId.get(requestId) || "";
  refererByRequestId.delete(requestId);
  return referer;
}
if (api.webRequest?.onErrorOccurred) {
  api.webRequest.onErrorOccurred.addListener((details) => {
    refererByRequestId.delete(details.requestId);
  }, { urls: ["<all_urls>"] });
}

// Slice 2: unassigned (tabId === -1) sources. A service-worker
// fetch carries no tab, but it does carry an initiator origin and
// usually a Referer naming the page that triggered it. Entries are
// kept here (never in a random tab's list), expire after 5 minutes,
// and are merged into a tab only by GET_ACTIVE_SOURCES when they
// match that tab's page. Unassigned sources are NOT persisted:
// without a tab there is no page URL to rehydrate against.
const unassignedSources = [];
const UNASSIGNED_MAX = 100;
const UNASSIGNED_TTL_MS = 5 * 60 * 1000;
function recordUnassignedSource(url, kind, referer, initiator) {
  try {
    const now = Date.now();
    for (let i = unassignedSources.length - 1; i >= 0; i--) {
      if (now - unassignedSources[i].discoveredAt > UNASSIGNED_TTL_MS) unassignedSources.splice(i, 1);
    }
    if (unassignedSources.some((item) => item.url === url)) return;
    unassignedSources.push({ url, kind, label: kind, discoveredAt: now, referer: referer || "", initiator: initiator || "" });
    if (unassignedSources.length > UNASSIGNED_MAX) {
      unassignedSources.splice(0, unassignedSources.length - UNASSIGNED_MAX);
    }
  } catch (_) { /* Unassigned storage is best-effort. */ }
}
// Returns mergeTabSources' truncation result (true when the merged
// list is truncated), or false when nothing matched.
// Slice 3 MINOR (c): the 5-minute TTL is checked HERE too, not only
// when a new entry is recorded — a stale entry no longer merges on a
// later popup open in a quiet system; it is dropped instead.
// Slice 3 MINOR (b): the origin-only fallback applies only when
// exactly one tab of that origin is open. With two same-origin tabs
// the entry could belong to either, so it stays unassigned (the exact
// Referer path is unaffected — it is always correct).
async function mergeUnassignedForTab(tabId, pageUrl) {
  if (typeof tabId !== "number" || !pageUrl) return false;
  let pageOrigin = "";
  try { pageOrigin = new URL(pageUrl).origin; } catch (_) { return false; }
  const now = Date.now();
  const live = unassignedSources.filter((item) => now - item.discoveredAt <= UNASSIGNED_TTL_MS);
  unassignedSources.length = 0;
  unassignedSources.push(...live);
  let sameOriginTabCount = 0;
  try {
    const tabs = await api.tabs.query({});
    for (const tab of tabs || []) {
      try { if (tab?.url && new URL(tab.url).origin === pageOrigin) sameOriginTabCount++; } catch (_) {}
    }
  } catch (_) {
    // Tabs lookup failed: fail closed — origin-only matching is off,
    // exact-Referer matching still works.
    sameOriginTabCount = 2;
  }
  const originFallbackAllowed = sameOriginTabCount === 1;
  const matched = [];
  const remaining = [];
  for (const item of unassignedSources) {
    let refererUrl = "";
    try { refererUrl = item.referer ? new URL(item.referer).href : ""; } catch (_) {}
    const exact = !!refererUrl && refererUrl === pageUrl;
    let origin = "";
    try { origin = item.initiator ? new URL(item.initiator).origin : ""; } catch (_) {}
    const originMatch = originFallbackAllowed && origin && origin === pageOrigin;
    if (exact || originMatch) {
      matched.push(item);
      // Origin-only matches stay: another same-origin tab may own
      // them. Exact-Referer matches belong to exactly this page.
      if (!exact) remaining.push(item);
    } else {
      remaining.push(item);
    }
  }
  if (!matched.length) return false;
  unassignedSources.length = 0;
  unassignedSources.push(...remaining);
  return mergeTabSources(tabId, matched.map((item) => {
    const merged = { url: item.url, kind: item.kind, label: item.label, discoveredAt: item.discoveredAt };
    if (item.referer) merged.referer = item.referer;
    return merged;
  }), false);
}
if (api.webRequest?.onHeadersReceived) {
  api.webRequest.onHeadersReceived.addListener((details) => {
    const kind = classifyNetworkResponse(details.url, details.responseHeaders || []);
    if (kind) {
      recordNetworkSource(details.tabId, details.url, kind, {
        referer: takeReferer(details.requestId),
        initiator: details.initiator || ""
      });
    } else {
      // No media: still drop the Referer entry so the map does not
      // hold entries for requests we will never attribute.
      takeReferer(details.requestId);
    }
  }, { urls: ["<all_urls>"], types: ["xmlhttprequest", "media", "other"] }, ["responseHeaders"]);
}
api.tabs.onUpdated.addListener((tabId, changeInfo) => { if (changeInfo.status === "loading") { sourcesByTab.delete(tabId); truncatedByTab.delete(tabId); ytdlpByTab.delete(tabId); removePersistedTabSources(tabId); } });
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
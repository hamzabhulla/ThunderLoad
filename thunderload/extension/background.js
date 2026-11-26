const api = globalThis.browser ?? chrome;
const HOST_NAME = "com.thunderload.host";
let nativePort = null;
let nextRequestId = 1;
const pendingRequests = new Map();
// Timeout handle per pending requestNative call, so a response (or a
// port disconnect) can cancel the timer instead of letting it fire
// later against an already-resolved request.
const pendingRequestTimers = new Map();
const jobTabById = new Map();
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
function normalizeMissingTool(missing) {
  if (Array.isArray(missing)) return typeof missing[0] === "string" && missing[0] ? missing[0] : null;
  return typeof missing === "string" && missing ? missing : null;
}
async function getActiveTab() {
  try {
    const [tab] = await api.tabs.query({ active: true, currentWindow: true });
    return tab || null;
  } catch (_) {
    return null;
  }
}

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
// H7: Firefox MV3 <all_urls> host access is granted at install (FF
// >=127) but the user can revoke it with one click in about:addons,
// after which content-script injection and webRequest observation
// silently stop — no error surfaces anywhere, and there is no
// permissions event the popup could rely on. So the background
// carries the hostPermissionGranted field on GET_ACTIVE_SOURCES,
// which the popup reads on every open, and also answers the
// standalone GET_HOST_PERMISSION_STATUS check for any caller that
// needs the status without a source scan; REQUEST_HOST_PERMISSION
// re-prompts when revoked. A missing permissions API or a thrown check fails OPEN
// (assumes granted) so a browser quirk can never cry wolf; only an
// explicit `false` from contains() counts as revoked.
const HOST_ORIGINS = ["<all_urls>"];
async function hasHostPermission() {
  try {
    if (typeof api.permissions?.contains !== "function") return true;
    return await api.permissions.contains({ origins: HOST_ORIGINS });
  } catch (_) { return true; }
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
      clearTimeout(pendingRequestTimers.get(message.id));
      pendingRequestTimers.delete(message.id);
      resolve(message);
      return;
    }
    api.runtime.sendMessage({ type: "NATIVE_EVENT", event: message }).catch(() => {});
    if (message?.jobId && jobTabById.has(message.jobId)) {
      const tabId = jobTabById.get(message.jobId);
      api.tabs?.sendMessage?.(tabId, { type: "NATIVE_EVENT", event: message })?.catch?.(() => {});
      if (message.event === "download.complete" || message.event === "download.error" || message.event === "download.cancelled") {
        jobTabById.delete(message.jobId);
      }
    }
    if (message?.event === "ytdlp.tool_progress" && typeof api.tabs?.query === "function") {
      api.tabs.query({ active: true }).then((tabs) => {
        for (const tab of tabs || []) {
          if (typeof tab?.id === "number") {
            api.tabs?.sendMessage?.(tab.id, { type: "NATIVE_EVENT", event: message })?.catch?.(() => {});
          }
        }
      }).catch(() => {});
    }
  });
  nativePort.onDisconnect.addListener(() => {
    nativePort = null;
    jobTabById.clear();
    for (const [id, resolve] of pendingRequests) resolve({ id, ok: false, error: api.runtime.lastError?.message || "Native host disconnected" });
    pendingRequests.clear();
    for (const timer of pendingRequestTimers.values()) clearTimeout(timer);
    pendingRequestTimers.clear();
    api.runtime.sendMessage({ type: "NATIVE_DISCONNECTED" }).catch(() => {});
  });
  // Slice 6: the port is back — retry any intercepted download that
  // was waiting for the desktop app.
  setTimeout(() => { flushInterceptQueue(); }, 0);
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
  if (["download.start", "video.download", "dash.download", "video.inspect", "ytdlp.inspect", "ytdlp.download", "hls.subtitles.download"].includes(action) && payload?.url) {
    payload = { ...payload, session: await sessionForUrl(payload.url) };
  }
  return new Promise((resolve) => {
    const id = "ext-" + Date.now() + "-" + nextRequestId++;
    try {
      const port = ensureNativePort();
      pendingRequests.set(id, resolve);
      port.postMessage({ ...payload, id, action });
      // Slice 3: tool downloads (22 MB yt-dlp + 41 MB Deno) outlast
      // the 30 s default, so ensure_tool passes a 10-minute timeout.
      // The handle is stored so a response or disconnect clears it.
      const timer = setTimeout(() => {
        if (pendingRequests.has(id)) { pendingRequests.delete(id); pendingRequestTimers.delete(id); resolve({ id, ok: false, error: "Native host timed out" }); }
      }, timeoutMs);
      pendingRequestTimers.set(id, timer);
    } catch (error) {
      pendingRequests.delete(id);
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

// Slice 7c (Gemini claim 9): only a KNOWN media/subtitle extension is
// replaced — the old `name.replace(/\.[^.]+$/, "")` stripped whatever
// followed the last dot, so "my.video" became "my.mp4". A name with no
// extension, or one ending in something that is not a media extension,
// keeps its full name and gains the extension ("my.video.mp4").
// Mirrored in popup.js (same helper, same cases).
const KNOWN_FILE_EXTENSIONS = new Set(["mp4", "mkv", "webm", "mov", "avi", "m4v", "ts", "m2ts", "mp3", "m4a", "aac", "opus", "flac", "wav", "ogg", "oga", "wmv", "flv", "3gp", "m3u8", "mpd", "vtt", "srt", "ass", "ssa"]);
function withExtension(name, extension) {
  const trimmed = name.replace(/\.+$/, "");
  const dot = trimmed.lastIndexOf(".");
  if (dot > 0 && KNOWN_FILE_EXTENSIONS.has(trimmed.slice(dot + 1).toLowerCase())) {
    return trimmed.slice(0, dot) + "." + extension;
  }
  return trimmed + "." + extension;
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
    filename: selected.kind === "hls" || selected.kind === "dash" ? withExtension(name, "mp4") : name,
    referer: pageUrl || "",
    representationId: selected.representationId,
    audioRepresentationId: selected.audioRepresentationId || "",
    // Slice 6: a live row records the stream as it arrives instead of
    // downloading a finished file — the host routes it to the live
    // recorder, which keeps the partial as the recording.
    live: !!(selected.isLive || selected.is_live)
  });
  if (result?.ok) {
    // jobId lets the panel show live progress for this download
    // (Slice 5): the native host reports download.progress events
    // with the same jobId, and the panel matches them to this start.
    return { ok: true, jobId: result.data?.jobId || null, message: selected.quality && variants.length > 1
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
  // Slice 6: the handoff is queued, not fire-and-forget — if the
  // native port is down the download waits for the desktop app
  // instead of being silently dropped.
  queueIntercept(downloadItem.url, filename, downloadItem.referrer || "");
}
if (api.downloads?.onCreated) {
  api.downloads.onCreated.addListener((downloadItem) => { interceptDownload(downloadItem).catch(() => {}); });
}

// Slice 6: interception transport. An intercepted download that
// cannot reach the native host is QUEUED and retried (5s, 15s, 30s)
// — never silently dropped, never double-downloaded (dedupe by URL,
// in-flight guard, entry removed before any fallback). When the
// retries are exhausted the browser download runs exactly once as
// the last resort; interceptDownload's byExtensionId check skips
// extension-started downloads, so that fallback cannot loop back
// into the queue. The queue is flushed when the native port comes
// back (ensureNativePort) and every 30 s, so a download intercepted
// while the desktop app was closed starts once the app opens.
const INTERCEPT_RETRY_DELAYS_MS = [5000, 15000, 30000];
const interceptQueue = new Map();
// Slice 6: the queue survives a service-worker restart. A plain
// in-memory Map would silently drop a download intercepted while the
// desktop app was down the moment the worker was killed — the exact
// loss this queue exists to prevent. Only the resumable fields are
// stored (url, filename, referer, attempts); timers and the in-flight
// flag are rebuilt on rehydrate, and a rehydrated entry retries at
// once instead of waiting for its next delay.
const INTERCEPT_QUEUE_KEY = "thunderload.interceptQueue.v1";
function persistInterceptQueue() {
  const saved = Array.from(interceptQueue.values()).map((entry) => ({ url: entry.url, filename: entry.filename, referer: entry.referer, attempts: entry.attempts }));
  api.storage?.local?.set({ [INTERCEPT_QUEUE_KEY]: saved }).catch(() => {});
}
function pendingIntercepts() {
  return Array.from(interceptQueue.values()).map((entry) => ({ url: entry.url, filename: entry.filename, attempts: entry.attempts }));
}
function flushInterceptQueue() {
  for (const entry of Array.from(interceptQueue.values())) tryIntercept(entry).catch(() => {});
}
async function tryIntercept(entry) {
  if (entry.inFlight) return;
  entry.inFlight = true;
  const native = await requestNative("download.start", { url: entry.url, filename: entry.filename, referer: entry.referer });
  entry.inFlight = false;
  if (native?.ok) {
    clearTimeout(entry.timer);
    interceptQueue.delete(entry.url);
    persistInterceptQueue();
    return;
  }
  entry.attempts += 1;
  if (entry.attempts > INTERCEPT_RETRY_DELAYS_MS.length) {
    interceptQueue.delete(entry.url);
    persistInterceptQueue();
    await browserDownload(entry.url, entry.filename);
    return;
  }
  persistInterceptQueue();
  entry.timer = setTimeout(() => { tryIntercept(entry).catch(() => {}); }, INTERCEPT_RETRY_DELAYS_MS[entry.attempts - 1]);
}
function queueIntercept(url, filename, referer) {
  if (interceptQueue.has(url)) return;
  const entry = { url, filename, referer, attempts: 0, timer: null, inFlight: false };
  interceptQueue.set(url, entry);
  persistInterceptQueue();
  tryIntercept(entry).catch(() => {});
}
// MV3 service workers are suspended, so a plain setInterval is not
// a reliable 30 s ticker — use chrome.alarms when the API is
// available (it needs the "alarms" manifest permission; without it
// api.alarms is undefined and the setInterval fallback below keeps
// the old behaviour). The alarm name is namespaced so a future
// alarm cannot collide with it.
const INTERCEPT_FLUSH_ALARM = "thunderload-intercept-flush";
if (typeof api.alarms?.create === "function" && typeof api.alarms?.onAlarm?.addListener === "function") {
  api.alarms.create(INTERCEPT_FLUSH_ALARM, { periodInMinutes: 0.5 });
  api.alarms.onAlarm.addListener((alarm) => { if (alarm?.name === INTERCEPT_FLUSH_ALARM) flushInterceptQueue(); });
} else {
  setInterval(() => { flushInterceptQueue(); }, 30000);
}
// Rehydrate the queue after a worker restart (see above): entries
// recorded before the restart retry at once — the desktop app may
// already be back by now, so waiting for the next delay would waste
// the restart.
api.storage?.local?.get(INTERCEPT_QUEUE_KEY)
  .then((stored) => {
    const saved = Array.isArray(stored?.[INTERCEPT_QUEUE_KEY]) ? stored[INTERCEPT_QUEUE_KEY] : [];
    for (const item of saved) {
      if (!item?.url || interceptQueue.has(item.url)) continue;
      interceptQueue.set(item.url, { url: item.url, filename: item.filename || safeFilenameFromUrl(item.url, "download.bin"), referer: item.referer || "", attempts: typeof item.attempts === "number" ? item.attempts : 0, timer: null, inFlight: false });
    }
    if (interceptQueue.size) flushInterceptQueue();
  })
  .catch(() => {});

// Slice 6: Firefox blocking build. Firefox supports blocking
// webRequest; Chrome MV3 does not. On Firefox only, a NAVIGATION to
// a direct media file (link click, address bar, open in new tab) is
// cancelled and handed to the intercept queue, so the desktop engine
// takes the download over. Playlists (.m3u8/.mpd), segments and media
// fetches are never blocked — cancelling those would break playback.
// Guarded by the browser namespace, which only Firefox exposes, so
// Chrome never runs this path (the Chrome manifest also does not
// request webRequestBlocking — the validator asserts both).
const IS_FIREFOX = typeof globalThis.browser !== "undefined";
const MEDIA_FILE_NAV_EXT = /\.(?:mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav|ogg|opus|flac|pdf|zip)(?:$|[?#])/i;
if (IS_FIREFOX && api.webRequest?.onBeforeRequest) {
  api.webRequest.onBeforeRequest.addListener((details) => {
    if (!interceptEnabled) return {};
    if (!/^https?:\/\//i.test(details.url || "")) return {};
    if (!MEDIA_FILE_NAV_EXT.test(details.url)) return {};
    queueIntercept(details.url, safeFilenameFromUrl(details.url, "download.bin"), details.initiator || details.originUrl || details.documentUrl || "");
    return { cancel: true };
  }, { urls: ["<all_urls>"], types: ["main_frame", "sub_frame"] }, ["blocking"]);
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
          // H7: host access may have been revoked in about:addons
          // since install — the popup shows a re-enable notice when
          // this is false instead of a bare "No media found".
          hostPermissionGranted: await hasHostPermission(),
          ytdlp: ytdlpForPage ? { variants: ytdlpForPage.variants, title: ytdlpForPage.title } : null,
          // Slice 6: downloads intercepted while the desktop app was
          // down, still waiting for the handoff — the popup shows them
          // so the user knows the file is not lost.
          pendingIntercepts: pendingIntercepts() });
      } catch (error) {
        sendResponse({ ok: false, error: error?.message || "This page does not allow extension scanning" });
      }
    })();
    return true;
  }
  if (message?.type === "NATIVE_REQUEST") {
    const senderTabId = sender.tab?.id;
    requestNative(message.action, message.payload || {}).then((result) => {
      if (result?.ok && result.data?.jobId && typeof senderTabId === "number") {
        jobTabById.set(result.data.jobId, senderTabId);
      }
      sendResponse(result);
    });
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
  // H7: host-permission status check and re-grant prompt. The
  // popup reads hostPermissionGranted from GET_ACTIVE_SOURCES on
  // every open; GET_HOST_PERMISSION_STATUS below is the standalone
  // check for callers that need the status without a source scan.
  // When revoked the popup shows a notice with a re-enable button. The button calls
  // permissions.request directly from the popup when it can (the
  // click's user gesture survives there); this background handler
  // is the fallback for callers without a gesture, e.g. the in-page
  // panel. A failed request is NOT an error — the notice stays and
  // points at the manual steps in about:addons / chrome://extensions.
  if (message?.type === "GET_HOST_PERMISSION_STATUS") {
    hasHostPermission().then((granted) => sendResponse({ ok: true, granted }));
    return true;
  }
  if (message?.type === "REQUEST_HOST_PERMISSION") {
    (async () => {
      try {
        if (typeof api.permissions?.request !== "function") return sendResponse({ ok: true, granted: false });
        const granted = await api.permissions.request({ origins: HOST_ORIGINS });
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
      let tabId = typeof sender.tab?.id === "number"
        ? sender.tab.id
        : (typeof message.tabId === "number" ? message.tabId : undefined);
      if (typeof tabId !== "number") {
        const activeTab = await getActiveTab();
        if (typeof activeTab?.id === "number") tabId = activeTab.id;
      }
      const cached = ytdlpCache.get(pageUrl);
      if (cached && Date.now() - cached.at < YTDLP_CACHE_TTL_MS) {
        if (typeof tabId === "number") ytdlpByTab.set(tabId, { pageUrl, variants: cached.variants, title: cached.title });
        sendResponse({ ok: true, variants: cached.variants, title: cached.title, cached: true });
        return;
      }
      // 90 s: the engine allows 60 s for a slow inspect, and the
      // extension must wait LONGER than that — dispatch, permission
      // verify, spawn, kill and response overhead on a cold Windows
      // machine (Defender scan, first launch) eats a 5 s margin, so
      // the old 65 s timer fired first, the pending request was
      // deleted, and the engine's on-time answer was dropped as
      // unknown. 30 s of margin covers that overhead; the default
      // 30 s requestNative timeout would cut a 30–60 s inspect off in
      // the extension even though the engine would succeed.
      // Slice 5: the page URL is also the referer — yt-dlp passes it
      // to the site, so a login-walled video inspects with the session
      // instead of 403ing. (Cookies + UA were already attached by
      // requestNative; only the referer was missing here.)
      const result = await requestNative("ytdlp.inspect", { url: pageUrl, referer: pageUrl }, 90000);
      if (!result?.ok) {
        sendResponse({ ok: false, error: result?.error || "yt-dlp could not inspect this page.", toolsMissing: normalizeMissingTool(result?.toolsMissing) });
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
  // Slice 6: GenericIE fallback. When sniffing and the normal
  // yt-dlp inspect found nothing usable for a page, the panel/popup
  // ask for the generic extractor instead (`--force-generic-extractor`):
  // yt-dlp reads whatever formats the page itself exposes. The result
  // lives under a SEPARATE cache key (`generic:<url>`) and is never
  // written to ytdlpByTab — a generic result must never overwrite a
  // real inspect, and must never suppress the sniffed rows the way a
  // real yt-dlp result does. The rows carry `generic: true` from the
  // host and are rendered like any other quality rows — the quality
  // shown is what the page carries, never a guessed label.
  if (message?.type === "YTDLP_INSPECT_GENERIC") {
    (async () => {
      const pageUrl = message.pageUrl || "";
      const cacheKey = "generic:" + pageUrl;
      const cached = ytdlpCache.get(cacheKey);
      if (cached && Date.now() - cached.at < YTDLP_CACHE_TTL_MS) {
        sendResponse({ ok: true, variants: cached.variants, title: cached.title, cached: true, generic: true });
        return;
      }
      const result = await requestNative("ytdlp.inspect", { url: pageUrl, referer: pageUrl, generic: true }, 90000);
      if (!result?.ok) {
        sendResponse({ ok: false, error: result?.error || "yt-dlp could not inspect this page.", toolsMissing: normalizeMissingTool(result?.toolsMissing) });
        return;
      }
      const variants = Array.isArray(result.data?.variants) ? result.data.variants : [];
      const title = result.data?.title || "";
      ytdlpCache.set(cacheKey, { variants, title, at: Date.now() });
      pruneYtdlpCache();
      sendResponse({ ok: true, variants, title, cached: false, generic: true });
    })();
    return true;
  }
  // Slice 6: the panel's live view of the intercept queue — how many
  // downloads are waiting for the desktop app right now.
  if (message?.type === "GET_INTERCEPT_STATUS") {
    sendResponse({ ok: true, pending: pendingIntercepts() });
    return false;
  }
  if (message?.type === "YTDLP_DOWNLOAD") {
    // Slice 5: same referer as the inspect — the download must carry
    // the same session, or a gated video fails after the inspect
    // succeeded. jobId is passed through so the panel can show live
    // progress for this download.
    const senderTabId = sender.tab?.id;
    requestNative("ytdlp.download", { url: message.pageUrl, formatSelector: message.formatSelector, filename: message.filename, referer: message.pageUrl }).then((result) => {
      const jobId = result?.ok ? (result.data?.jobId || null) : null;
      if (jobId && typeof senderTabId === "number") jobTabById.set(jobId, senderTabId);
      sendResponse(result?.ok
        ? { ok: true, jobId, message: result.message || "Download started in ThunderLoad." }
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
    const tool = Array.isArray(message.tool) ? (message.tool[0] || "ytdlp") : message.tool;
    requestNative("ytdlp.ensure_tool", { tool }, 600000).then((result) => {
      sendResponse(result?.ok
        ? { ok: true, path: result.data?.path || result.path || "" }
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
      sendResponse(result?.ok ? { ok: true, message: result.data?.message || result.message || "" } : { ok: false, error: result?.error || "Could not check for updates." });
    });
    return true;
  }
  // Slice 3: manual alternative (backup only) — open the official
  // release page so the user can download the tool themselves; the
  // app picks it up from the tools dir.
  if (message?.type === "YTDLP_OPEN_MANUAL_PAGE") {
    const tool = Array.isArray(message.tool) ? (message.tool[0] || "ytdlp") : message.tool;
    const url = YTDLP_RELEASE_PAGES[tool] || YTDLP_RELEASE_PAGES.ytdlp;
    if (typeof api.tabs?.create === "function") api.tabs.create({ url }).catch(() => {});
    sendResponse({ ok: true });
    return false;
  }
  if (message?.type === "QUICK_DOWNLOAD_MEDIA") {
    const senderTabId = sender.tab?.id;
    quickDownloadMedia(message).then((result) => {
      if (result?.ok && result.jobId && typeof senderTabId === "number") {
        jobTabById.set(result.jobId, senderTabId);
      }
      sendResponse(result);
    }).catch((error) => sendResponse({ ok: false, error: error?.message || String(error) }));
    return true;
  }
  if (message?.type === "BROWSER_DOWNLOAD") {
    let safeFilename = message.filename;
    if (typeof safeFilename === "string" && safeFilename) {
      const cleaned = safeFilename.replace(/[\u0000-\u001f\\/:*?"<>|]+/g, "_");
      const dot = cleaned.lastIndexOf(".");
      safeFilename = dot > 0
        ? (cleaned.slice(0, dot).slice(0, 180) || "download") + cleaned.slice(dot)
        : cleaned.slice(0, 180);
    }
    browserDownload(message.url, safeFilename).then(sendResponse).catch((error) => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  return false;
});

api.tabs.onRemoved.addListener((tabId) => { sourcesByTab.delete(tabId); truncatedByTab.delete(tabId); ytdlpByTab.delete(tabId); for (const [jobId, tid] of jobTabById) { if (tid === tabId) jobTabById.delete(jobId); } removePersistedTabSources(tabId); });

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
        initiator: details.initiator || details.originUrl || details.documentUrl || ""
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
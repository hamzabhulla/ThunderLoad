const api = globalThis.browser ?? chrome;
const HOST_NAME = "com.thunderload.host";
let nativePort = null;
let nextRequestId = 1;
const pendingRequests = new Map();
const sourcesByTab = new Map();

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
function safeFilenameFromUrl(url, fallback = "download.bin") {
  try {
    const raw = decodeURIComponent(new URL(url).pathname.split("/").pop() || fallback);
    return raw.replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_").slice(0, 180) || fallback;
  } catch (_) { return fallback; }
}

async function quickDownloadMedia({ url, kind, pageUrl, filename, selectedVariant }) {
  const name = filename || safeFilenameFromUrl(url, kind === "HLS playlist" ? "video.mp4" : "download.bin");
  const nativePermission = await hasNativePermission();

  // Prefer the ThunderLoad engine when the optional native bridge is installed.
  // A failed inspection/start must never block an ordinary direct-file download.
  if (nativePermission) {
    const inspected = await requestNative("video.inspect", { url, referer: pageUrl || "" });
    if (inspected?.ok) {
      const variants = Array.isArray(inspected.data?.variants) ? inspected.data.variants : [];
      const downloadable = variants.filter((item) => item?.downloadable && item?.url);
      downloadable.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
      const selected = selectedVariant && selectedVariant.downloadable && selectedVariant.url
        ? selectedVariant
        : downloadable[0];
      if (selected) {
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
        if (kind !== "Direct media") {
          return { ok: false, error: result?.error || "ThunderLoad could not start this download." };
        }
      } else if (kind !== "Direct media") {
        return { ok: false, error: variants[0]?.note || "No downloadable stream was exposed by this page." };
      }
    } else if (kind !== "Direct media") {
      return { ok: false, error: inspected?.error || "Could not inspect this media source." };
    }
  }

  // Direct files always have a browser-download fallback, whether the native
  // permission is absent, the host is disconnected, or the desktop engine fails.
  if (kind === "Direct media") {
    const result = await browserDownload(url, name);
    if (result?.ok) return { ok: true, message: "Download started in your browser." };
    return { ok: false, error: result?.error || "Your browser could not start this download." };
  }

  return {
    ok: false,
    error: nativePermission
      ? "Adaptive video could not be started by the ThunderLoad engine."
      : "Adaptive video needs the desktop companion. Open the extension popup and choose Download to grant its connection permission."
  };
}

api.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === "VIDEO_SOURCES_FOUND") {
    const tabId = sender.tab?.id;
    if (typeof tabId === "number") {
      const map = new Map((sourcesByTab.get(tabId) || []).map((item) => [item.url, item]));
      for (const item of (message.sources || [])) if (item?.url && /^https?:\/\//i.test(item.url)) map.set(item.url, item);
      sourcesByTab.set(tabId, Array.from(map.values()).slice(0, 80));
    }
    sendResponse({ ok: true });
    return false;
  }
  if (message?.type === "GET_ACTIVE_SOURCES") {
    (async () => {
      try {
        const [tab] = await api.tabs.query({ active: true, currentWindow: true });
        if (!tab?.id) return sendResponse({ ok: false, error: "No active tab" });
        await api.scripting.executeScript({ target: { tabId: tab.id }, files: ["content.js"] });
        await api.tabs.sendMessage(tab.id, { type: "SCAN_NOW" }).catch(() => {});
        await new Promise((resolve) => setTimeout(resolve, 250));
        sendResponse({ ok: true, tabId: tab.id, sources: sourcesByTab.get(tab.id) || [], pageUrl: tab.url || "" });
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

api.tabs.onRemoved.addListener((tabId) => sourcesByTab.delete(tabId));

// IDM-style network backstop: catch media the DOM scan cannot see
// (fetch/XHR-loaded streams, cross-origin iframes). Observation only —
// nothing is blocked, modified or redirected. The classifier mirrors the
// extension used by content.js so both paths report the same kinds.
const NETWORK_MEDIA_EXT = /\.(?:m3u8|mpd|mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav)(?:$|[?#])/i;
const NETWORK_SEGMENT_EXT = /\.(?:ts|m4s|cmfv|cmfa)(?:$|[?#])/i;
function classifyNetworkResponse(url, headers) {
  if (!url || !/^https?:\/\//i.test(url)) return null;
  const contentType = (headers || []).find((header) => header.name?.toLowerCase() === "content-type")?.value?.toLowerCase() || "";
  if (/\.m3u8(?:$|[?#])/i.test(url) || contentType.includes("application/x-mpegurl") || contentType.includes("application/vnd.apple.mpegurl")) return "HLS playlist";
  if (/\.mpd(?:$|[?#])/i.test(url) || contentType.includes("application/dash+xml")) return "DASH manifest";
  if (NETWORK_SEGMENT_EXT.test(url)) return null;
  if (contentType.startsWith("video/") || contentType.startsWith("audio/")) return "Direct media";
  if (NETWORK_MEDIA_EXT.test(url)) return "Direct media";
  return null;
}
function recordNetworkSource(tabId, url, kind) {
  if (typeof tabId !== "number" || tabId < 0) return;
  const map = new Map((sourcesByTab.get(tabId) || []).map((item) => [item.url, item]));
  if (!map.has(url)) map.set(url, { url, kind, label: kind, discoveredAt: Date.now() });
  sourcesByTab.set(tabId, Array.from(map.values()).slice(0, 80));
}
if (api.webRequest?.onHeadersReceived) {
  api.webRequest.onHeadersReceived.addListener((details) => {
    const kind = classifyNetworkResponse(details.url, details.responseHeaders || []);
    if (kind) recordNetworkSource(details.tabId, details.url, kind);
  }, { urls: ["<all_urls>"], types: ["xmlhttprequest", "media", "other"] }, ["responseHeaders"]);
}
api.tabs.onUpdated.addListener((tabId, changeInfo) => { if (changeInfo.status === "loading") sourcesByTab.delete(tabId); });
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
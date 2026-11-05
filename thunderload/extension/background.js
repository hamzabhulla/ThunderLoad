const api = globalThis.browser ?? chrome;
const HOST_NAME = "com.thunderload.host";
let nativePort = null;
let nextRequestId = 1;
const pendingRequests = new Map();
const sourcesByTab = new Map();

function browserDownload(url, filename) {
  return new Promise((resolve, reject) => {
    api.downloads.download({ url, filename: filename || undefined, saveAs: false, conflictAction: "uniquify" }, (downloadId) => {
      const error = api.runtime.lastError;
      if (error) reject(new Error(error.message));
      else resolve({ ok: true, browserDownloadId: downloadId });
    });
  });
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
        await new Promise((resolve) => setTimeout(resolve, 350));
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
  if (message?.type === "BROWSER_DOWNLOAD") {
    browserDownload(message.url, message.filename).then(sendResponse).catch((error) => sendResponse({ ok: false, error: error.message }));
    return true;
  }
  return false;
});

api.tabs.onRemoved.addListener((tabId) => sourcesByTab.delete(tabId));
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

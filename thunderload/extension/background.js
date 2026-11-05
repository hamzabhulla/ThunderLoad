const HOST_NAME = "com.thunderload.host";
let nativePort = null;
let nextRequestId = 1;
const pendingRequests = new Map();
const sourcesByTab = new Map();

function ensureNativePort() {
  if (nativePort) return nativePort;
  try {
    nativePort = chrome.runtime.connectNative(HOST_NAME);
    nativePort.onMessage.addListener((message) => {
      if (message && message.id && pendingRequests.has(message.id)) {
        const resolve = pendingRequests.get(message.id);
        pendingRequests.delete(message.id);
        resolve(message);
        return;
      }
      chrome.runtime.sendMessage({ type: "NATIVE_EVENT", event: message }).catch(() => {});
    });
    nativePort.onDisconnect.addListener(() => {
      const error = chrome.runtime.lastError?.message || "ThunderLoad desktop engine disconnected";
      nativePort = null;
      for (const [id, resolve] of pendingRequests) resolve({ id, ok: false, error });
      pendingRequests.clear();
      chrome.runtime.sendMessage({ type: "NATIVE_DISCONNECTED", error }).catch(() => {});
    });
    return nativePort;
  } catch (error) {
    nativePort = null;
    throw error;
  }
}

function requestNative(action, payload = {}) {
  return new Promise((resolve) => {
    const id = "ext-" + Date.now() + "-" + nextRequestId++;
    try {
      const port = ensureNativePort();
      pendingRequests.set(id, resolve);
      port.postMessage({ id, action, ...payload });
      setTimeout(() => {
        if (pendingRequests.has(id)) {
          pendingRequests.delete(id);
          resolve({ id, ok: false, error: "Native host did not respond in time" });
        }
      }, 30000);
    } catch (error) {
      resolve({ id, ok: false, error: error?.message || String(error) });
    }
  });
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === "VIDEO_SOURCES_FOUND") {
    const tabId = sender.tab?.id;
    if (typeof tabId === "number") {
      const existing = sourcesByTab.get(tabId) || [];
      const map = new Map(existing.map((item) => [item.url, item]));
      for (const item of (message.sources || [])) {
        if (item?.url && /^https?:\/\//i.test(item.url)) map.set(item.url, item);
      }
      sourcesByTab.set(tabId, Array.from(map.values()).slice(0, 80));
    }
    sendResponse({ ok: true });
    return false;
  }

  if (message?.type === "GET_ACTIVE_SOURCES") {
    (async () => {
      try {
        const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
        if (!tab?.id) return sendResponse({ ok: false, error: "No active tab" });
        await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ["content.js"] });
        await chrome.tabs.sendMessage(tab.id, { type: "SCAN_NOW" }).catch(() => {});
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
  if (message?.type === "GET_NATIVE_STATUS") {
    sendResponse({ connected: Boolean(nativePort) });
    return false;
  }
  return false;
});

chrome.tabs.onRemoved.addListener((tabId) => sourcesByTab.delete(tabId));
chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
  if (changeInfo.status === "loading") sourcesByTab.delete(tabId);
});

chrome.commands?.onCommand.addListener(async (command) => {
  if (command !== "scan-media") return;
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id) return;
  try {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ["content.js"] });
    await chrome.tabs.sendMessage(tab.id, { type: "SCAN_NOW" });
  } catch (_) {
    // Restricted browser pages cannot be inspected.
  }
});

chrome.runtime.onInstalled?.addListener(() => {
  try {
    chrome.contextMenus.create({ id: "thunderload-download-link", title: "Download link with ThunderLoad", contexts: ["link"] });
  } catch (_) { /* Menu may already exist after an extension update. */ }
});

chrome.contextMenus?.onClicked.addListener((info) => {
  if (info.menuItemId !== "thunderload-download-link" || !info.linkUrl) return;
  const url = info.linkUrl;
  let filename = "download.bin";
  try { filename = decodeURIComponent(new URL(url).pathname.split("/").pop() || filename); } catch (_) {}
  requestNative("download.start", { url, filename, referer: info.pageUrl || "" }).then((result) => {
    chrome.runtime.sendMessage({ type: "CONTEXT_DOWNLOAD_RESULT", result }).catch(() => {});
  });
});

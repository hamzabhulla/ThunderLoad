const sourceList = document.getElementById("sourceList");
const scanMeta = document.getElementById("scanMeta");
const sourceCount = document.getElementById("sourceCount");
const engineStatus = document.getElementById("engineStatus");
const toast = document.getElementById("toast");
let pageUrl = "";
let sources = [];

function showToast(text) {
  toast.textContent = text;
  toast.classList.add("show");
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => toast.classList.remove("show"), 4200);
}
function requestNative(action, payload) {
  return chrome.runtime.sendMessage({ type: "NATIVE_REQUEST", action, payload });
}
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function render() {
  sourceCount.textContent = String(sources.length);
  sourceList.replaceChildren();
  if (!sources.length) {
    sourceList.append(el("div", "empty", "No supported media URL found yet. Start playback, then choose Rescan."));
    return;
  }
  for (const source of sources) {
    const card = el("article", "source-card");
    const head = el("div", "source-head");
    const info = el("div");
    info.append(el("div", "source-title", source.label || "Media source"));
    info.append(el("div", "kind", source.kind || "Media"));
    head.append(info);
    card.append(head, el("div", "url", source.url));
    const actions = el("div", "source-actions");
    const inspect = el("button", "primary", "Inspect qualities");
    inspect.addEventListener("click", () => inspectSource(source, card, inspect));
    actions.append(inspect);
    if (source.kind === "Direct media") {
      const download = el("button", "download-btn", "Download file");
      download.addEventListener("click", () => startDirectDownload(source));
      actions.append(download);
    }
    card.append(actions);
    sourceList.append(card);
  }
}
async function inspectSource(source, card, button) {
  button.disabled = true;
  button.textContent = "Inspecting…";
  try {
    const result = await requestNative("video.inspect", { url: source.url, referer: pageUrl });
    if (!result?.ok) throw new Error(result?.error || "Could not inspect this source");
    const qualityList = el("div", "quality-list");
    for (const variant of result.data?.variants || []) {
      const item = el("div", "quality");
      const description = el("div");
      description.append(el("strong", "", variant.quality || "Available stream"));
      const details = [
        variant.width && variant.height ? String(variant.width) + "×" + String(variant.height) : null,
        variant.bandwidth ? String(Math.round(variant.bandwidth / 1000)) + " kbps" : null,
        variant.kind?.toUpperCase()
      ].filter(Boolean).join(" · ");
      description.append(el("small", "", details || variant.note || "Detected quality"));
      item.append(description);
      const download = el("button", "download-btn", variant.downloadable ? "Download" : "Not ready");
      download.disabled = !variant.downloadable;
      download.addEventListener("click", () => startVideoDownload(variant, source));
      item.append(download);
      qualityList.append(item);
    }
    card.querySelector(".quality-list")?.remove();
    card.append(qualityList);
    if (!qualityList.children.length) throw new Error("No quality variants were returned");
  } catch (error) {
    showToast(error.message || String(error));
  } finally {
    button.disabled = false;
    button.textContent = "Inspect qualities";
  }
}
async function startDirectDownload(source) {
  const result = await requestNative("download.start", {
    url: source.url, filename: filenameFromUrl(source.url, "download.bin"), referer: pageUrl
  });
  if (result?.ok) showToast("Download started in ThunderLoad. Check the desktop app for progress.");
  else showToast(result?.error || "Desktop engine is not available. Install the native host first.");
}
async function startVideoDownload(variant, source) {
  if (!variant.downloadable) return;
  const action = variant.kind === "hls" ? "video.download" : "download.start";
  const result = await requestNative(action, {
    url: variant.url, referer: pageUrl, filename: filenameFromUrl(source.url, "video")
  });
  if (result?.ok) showToast("Video download started. The engine is working in the background.");
  else showToast(result?.error || "Could not start this download");
}
function filenameFromUrl(url, fallback) {
  try {
    const name = new URL(url).pathname.split("/").pop();
    return decodeURIComponent(name || fallback).slice(0, 120) || fallback;
  } catch (_) { return fallback; }
}
async function scanActiveTab() {
  sourceList.replaceChildren(el("div", "empty", "Scanning this tab…"));
  scanMeta.textContent = "Scanning active tab…";
  try {
    const result = await chrome.runtime.sendMessage({ type: "GET_ACTIVE_SOURCES" });
    if (!result?.ok) throw new Error(result?.error || "Cannot scan this page");
    pageUrl = result.pageUrl || "";
    sources = result.sources || [];
    scanMeta.textContent = sources.length ? "Page scanned locally" : "No media detected yet";
    render();
  } catch (error) {
    sources = [];
    scanMeta.textContent = "Scan unavailable";
    render();
    showToast(error.message || String(error));
  }
}
async function checkEngine() {
  try {
    const result = await requestNative("ping", {});
    if (result?.ok) {
      engineStatus.textContent = "Engine connected";
      engineStatus.classList.add("ready");
    } else engineStatus.textContent = "Engine not installed";
  } catch (_) { engineStatus.textContent = "Engine not installed"; }
}
document.getElementById("rescanBtn").addEventListener("click", scanActiveTab);
chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "NATIVE_EVENT") {
    const event = message.event;
    if (event?.event === "download.complete") showToast("Download complete: " + event.path);
    if (event?.event === "download.error") showToast("Download failed: " + event.error);
  }
  if (message?.type === "NATIVE_DISCONNECTED") {
    engineStatus.textContent = "Engine disconnected";
    engineStatus.classList.remove("ready");
  }
});
checkEngine();
scanActiveTab();

const api = globalThis.browser ?? chrome;
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
async function requestNative(action, payload, askPermission = false) {
  if (askPermission) {
    const has = await api.permissions.contains({ permissions: ["nativeMessaging"] });
    if (!has) {
      const granted = await api.permissions.request({ permissions: ["nativeMessaging"] });
      if (!granted) throw new Error("Desktop engine permission was not granted.");
    }
  }
  return api.runtime.sendMessage({ type: "NATIVE_REQUEST", action, payload });
}
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function filenameFromUrl(url, fallback) {
  try {
    const name = decodeURIComponent(new URL(url).pathname.split("/").pop() || fallback);
    return name.replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_").slice(0, 180) || fallback;
  } catch (_) { return fallback; }
}
function render() {
  sourceCount.textContent = String(sources.length);
  sourceList.replaceChildren();
  if (!sources.length) {
    sourceList.append(el("div", "empty", "No media URL is exposed yet. Start playback, then rescan this page."));
    return;
  }
  for (const source of sources) {
    const card = el("article", "source-card");
    const head = el("div", "source-head");
    const info = el("div", "source-info");
    info.append(el("div", "source-title", source.label || "Media source"), el("div", "kind", source.kind || "Media"));
    head.append(info);
    card.append(head);
    const actions = el("div", "source-actions");
    const download = el("button", "primary", "Download");
    download.addEventListener("click", () => quickDownload(source, card, download));
    actions.append(download);
    if (source.kind === "HLS playlist" || source.kind === "DASH manifest") {
      const more = el("button", "secondary", "Other qualities");
      more.addEventListener("click", () => {
        const current = card.querySelector(".quality-list");
        if (current) { current.remove(); more.textContent = "Other qualities"; return; }
        inspectSource(source, card, more);
      });
      actions.append(more);
    }
    card.append(actions);
    sourceList.append(card);
  }
}
async function quickDownload(source, card, button) {
  button.disabled = true;
  button.textContent = "Preparing…";
  try {
    const permission = await api.permissions.contains({ permissions: ["nativeMessaging"] });
    if (source.kind === "Direct media" && !permission) {
      const filename = filenameFromUrl(source.url, "download.bin");
      const result = await api.runtime.sendMessage({ type: "BROWSER_DOWNLOAD", url: source.url, filename });
      if (!result?.ok) throw new Error(result?.error || "Could not start this download.");
      showToast("Download started in your browser.");
      return;
    }
    if (source.kind === "HLS playlist" || source.kind === "DASH manifest") {
      const inspected = await requestNative("video.inspect", { url: source.url, referer: pageUrl }, true);
      if (!inspected?.ok) throw new Error(inspected?.error || "Could not inspect this media source.");
      const variants = (inspected.data?.variants || []).filter((item) => item?.downloadable && item?.url);
      variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
      const selected = variants[0];
      if (!selected) throw new Error(inspected.data?.variants?.[0]?.note || "No downloadable quality is exposed.");
      const action = selected.kind === "hls" ? "video.download" : "download.start";
      await startVariant(selected, source, action);
      return;
    }
    const filename = filenameFromUrl(source.url, "download.bin");
    const result = await requestNative("download.start", { url: source.url, filename, referer: pageUrl }, true);
    if (result?.ok) { showToast("Download started in ThunderLoad."); return; }
    const fallback = await api.runtime.sendMessage({ type: "BROWSER_DOWNLOAD", url: source.url, filename });
    if (fallback?.ok) showToast("Download started in your browser.");
    else throw new Error(result?.error || fallback?.error || "Could not start this download.");
  } catch (error) {
    if (source.kind === "Direct media") {
      try {
        const fallback = await api.runtime.sendMessage({ type: "BROWSER_DOWNLOAD", url: source.url, filename: filenameFromUrl(source.url, "download.bin") });
        if (fallback?.ok) { showToast("Download started in your browser."); return; }
      } catch (_) { /* Report the original issue below. */ }
    }
    showToast(error?.message || String(error));
  } finally {
    button.disabled = false;
    button.textContent = "Download";
  }
}
async function startVariant(variant, source, action) {
  const filename = filenameFromUrl(source.url, "video");
  const result = await requestNative(action, {
    url: variant.url,
    referer: pageUrl,
    filename: variant.kind === "hls" ? (filename.replace(/\.[^.]+$/, "") + ".mp4") : filename
  });
  if (!result?.ok) throw new Error(result?.error || "Could not start the selected quality.");
  showToast("Downloading " + (variant.quality || "media") + " in ThunderLoad.");
}
async function inspectSource(source, card, button) {
  button.disabled = true;
  button.textContent = "Checking…";
  try {
    const result = await requestNative("video.inspect", { url: source.url, referer: pageUrl }, true);
    if (!result?.ok) throw new Error(result?.error || "Install the ThunderLoad desktop companion to inspect quality options.");
    const qualityList = el("div", "quality-list");
    const variants = result.data?.variants || [];
    variants.forEach((variant) => {
      const item = el("div", "quality");
      const description = el("div", "quality-info");
      description.append(el("strong", "", variant.quality || "Available stream"));
      const details = [
        variant.width && variant.height ? String(variant.width) + " × " + String(variant.height) : null,
        variant.bandwidth ? String(Math.round(variant.bandwidth / 1000)) + " kbps" : null,
        variant.kind?.toUpperCase()
      ].filter(Boolean).join(" · ");
      description.append(el("small", "", details || variant.note || "Detected quality"));
      item.append(description);
      const download = el("button", "download-btn", variant.downloadable ? "Download" : "Unavailable");
      download.disabled = !variant.downloadable;
      download.addEventListener("click", async () => {
        download.disabled = true;
        try {
          await startVariant(variant, source, variant.kind === "hls" ? "video.download" : "download.start");
        } catch (error) { showToast(error?.message || String(error)); }
        finally { download.disabled = false; }
      });
      item.append(download);
      qualityList.append(item);
    });
    card.querySelector(".quality-list")?.remove();
    if (!qualityList.children.length) {
      qualityList.append(el("div", "quality-empty", "No alternative downloadable qualities are exposed."));
    }
    card.append(qualityList);
    button.textContent = "Hide qualities";
  } catch (error) {
    showToast(error?.message || String(error));
    button.textContent = "Other qualities";
  } finally { button.disabled = false; }
}
async function scanActiveTab() {
  sourceList.replaceChildren(el("div", "empty", "Scanning this tab…"));
  scanMeta.textContent = "Scanning active tab…";
  try {
    const result = await api.runtime.sendMessage({ type: "GET_ACTIVE_SOURCES" });
    if (!result?.ok) throw new Error(result?.error || "Cannot scan this page");
    pageUrl = result.pageUrl || "";
    sources = result.sources || [];
    scanMeta.textContent = sources.length ? "Page scanned locally" : "No media detected yet";
    render();
  } catch (error) {
    sources = []; scanMeta.textContent = "Scan unavailable"; render();
    showToast(error.message || String(error));
  }
}
async function checkEngine() {
  try {
    const hasPermission = await api.permissions.contains({ permissions: ["nativeMessaging"] });
    if (!hasPermission) { engineStatus.textContent = "Browser mode"; return; }
    const result = await requestNative("ping", {});
    if (result?.ok) { engineStatus.textContent = "Engine connected"; engineStatus.classList.add("ready"); }
    else engineStatus.textContent = "Browser mode";
  } catch (_) { engineStatus.textContent = "Browser mode"; }
}
document.getElementById("rescanBtn").addEventListener("click", scanActiveTab);
api.runtime.onMessage.addListener((message) => {
  if (message?.type === "NATIVE_EVENT") {
    const event = message.event;
    if (event?.event === "download.complete") showToast("Download complete: " + event.path);
    if (event?.event === "download.error") showToast("Download failed: " + event.error);
  }
  if (message?.type === "NATIVE_DISCONNECTED") {
    engineStatus.textContent = "Engine disconnected";
    engineStatus.classList.remove("ready");
  }
  if (message?.type === "CONTEXT_DOWNLOAD_RESULT" && !message.result?.ok) showToast(message.result?.error || "Download failed");
});
checkEngine();
scanActiveTab();
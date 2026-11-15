const api = globalThis.browser ?? chrome;
const sourceList = document.getElementById("sourceList");
const scanMeta = document.getElementById("scanMeta");
const sourceCount = document.getElementById("sourceCount");
const engineStatus = document.getElementById("engineStatus");
const toast = document.getElementById("toast");
const grabBar = document.getElementById("grabBar");
const selectAllBtn = document.getElementById("selectAllBtn");
const selectNoneBtn = document.getElementById("selectNoneBtn");
const downloadSelectedBtn = document.getElementById("downloadSelectedBtn");
const truncatedNote = document.getElementById("truncatedNote");
let pageUrl = "";
let sources = [];
// Site grabber selection: every detected item starts checked, so
// "Download selected" is one click. The set is keyed by URL — the
// popup de-duplicates sources by URL, so keys are unique.
let selected = new Set();
let truncated = false;

// Fix 3: the ThunderLoad app / native host is required for Direct
// media. A googlevideo segment downloaded by the browser is one
// audio/video chunk, not the video, so there is no silent browser
// fallback any more: a missing permission or a disconnected host
// surfaces this error instead, pointing at the desktop app's
// one-time Browser setup (the setup steps themselves live in the
// desktop app, not here).
const NATIVE_NOT_CONNECTED_ERROR = "ThunderLoad app is not connected. Open the ThunderLoad desktop app and complete the one-time Browser setup, then try again.";
function isNativeConnectionError(message) {
  return /permission has not been granted|permission was not granted|disconnected|timed out|native messaging host not found/i.test(String(message || ""));
}
function nativeErrorMessage(error) {
  const text = error?.message || String(error || "");
  return isNativeConnectionError(text) ? NATIVE_NOT_CONNECTED_ERROR : text;
}

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
  grabBar.hidden = sources.length === 0;
  truncatedNote.hidden = !truncated;
  downloadSelectedBtn.textContent = "Download selected (" + selected.size + ")";
  downloadSelectedBtn.disabled = selected.size === 0;
  sourceList.replaceChildren();
  if (!sources.length) {
    sourceList.append(el("div", "empty", "No media URL is exposed yet. Start playback, then rescan this page."));
    return;
  }
  for (const source of sources) {
    const card = el("article", "source-card");
    const head = el("div", "source-head");
    const check = el("input", "source-check");
    check.type = "checkbox";
    check.checked = selected.has(source.url);
    check.setAttribute("aria-label", "Select " + (source.label || source.url));
    check.addEventListener("change", () => {
      if (check.checked) selected.add(source.url);
      else selected.delete(source.url);
      downloadSelectedBtn.textContent = "Download selected (" + selected.size + ")";
      downloadSelectedBtn.disabled = selected.size === 0;
    });
    const info = el("div", "source-info");
    info.append(el("div", "source-title", source.label || "Media source"), el("div", "kind", source.kind || "Media"));
    // The DASH manifest card carries the full-video note from the
    // background (ranked first); segment cards carry their
    // Audio/Video + itag label instead of a blind "Direct media".
    if (source.note) info.append(el("div", "source-note", source.note));
    head.append(check, info);
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
function selectAll(none) {
  selected = new Set(none ? [] : sources.map((source) => source.url));
  render();
}
async function downloadSelected() {
  const chosen = sources.filter((source) => selected.has(source.url));
  if (!chosen.length) return;
  downloadSelectedBtn.disabled = true;
  downloadSelectedBtn.textContent = "Downloading…";
  // Freeze every button on the cards, not just the primary Download
  // ones (R2): "Other qualities" and the quality-list Download buttons
  // could otherwise start the same item twice mid-batch. Each
  // button's prior disabled state is restored afterwards, so an
  // "Unavailable" quality button stays unavailable.
  const frozenButtons = Array.from(sourceList.querySelectorAll(".source-card button"))
    .map((button) => { const wasDisabled = button.disabled; button.disabled = true; return { button, wasDisabled }; });
  let started = 0;
  let firstError = "";
  // One permission prompt per batch (F7): a denial stops prompting
  // for the rest of the items instead of asking once per playlist.
  let promptAllowed = true;
  // Sequential on purpose: each item inspects (HLS/DASH) and starts
  // through the same single-item path, so ordering and the native
  // host's per-job flow stay identical to one-click downloads.
  for (const source of chosen) {
    const result = await downloadOne(source, promptAllowed);
    if (result?.ok) started += 1;
    else {
      if (!firstError) firstError = result?.error || "Could not start this download.";
      if (result?.error === NATIVE_NOT_CONNECTED_ERROR) promptAllowed = false;
    }
  }
  // Restore only the buttons the freeze itself disabled (S1): a
  // button that was already disabled when the batch started belongs
  // to an operation already in flight, and that operation's own
  // completion re-enables it — restoring the captured `true` would
  // leave it stuck disabled until the next rescan.
  frozenButtons.forEach(({ button, wasDisabled }) => { if (!wasDisabled) button.disabled = false; });
  downloadSelectedBtn.disabled = selected.size === 0;
  downloadSelectedBtn.textContent = "Download selected (" + selected.size + ")";
  showToast(started === chosen.length
    ? "Started " + started + " download" + (started === 1 ? "" : "s") + "."
    : "Started " + started + " of " + chosen.length + " downloads." + (firstError ? " First failure: " + firstError : ""));
}
async function downloadOne(source, askPermission = true) {
  // Single-item download core, shared by the per-card button and the
  // grabber's "Download selected" batch loop. Returns a result instead
  // of touching the UI, so the batch caller can summarize.
  //
  // Fix 3: no silent browser fallback for Direct media. Previously a
  // missing nativeMessaging permission — or any native failure — fell
  // through to BROWSER_DOWNLOAD, and a googlevideo segment downloaded
  // that way is a single audio/video chunk (the audio-only bug), with
  // no error shown. Now every kind goes through the ThunderLoad
  // engine: requestNative() prompts for the optional nativeMessaging
  // permission on demand (askPermission, the existing
  // optional-permission pattern), and a missing permission, a denied
  // prompt, or a disconnected/erroring host returns the clear
  // "app not connected" error pointing at the desktop app's one-time
  // Browser setup. There is no explicit "Download in browser" action
  // in the popup, so no browser-download path remains in this
  // function. `askPermission` lets the batch loop prompt once and
  // then stop asking after a denial (F7).
  try {
    if (source.kind === "HLS playlist" || source.kind === "DASH manifest") {
      const inspected = await requestNative("video.inspect", { url: source.url, referer: pageUrl }, askPermission);
      if (!inspected?.ok) throw new Error(nativeErrorMessage(inspected?.error || "Could not inspect this media source."));
      const variants = (inspected.data?.variants || []).filter((item) => item?.downloadable && item?.url);
      variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
      const selected = variants[0];
      if (!selected) throw new Error(inspected.data?.variants?.[0]?.note || "No downloadable quality is exposed.");
      const action = selected.kind === "hls" ? "video.download" : selected.kind === "dash" ? "dash.download" : "download.start";
      return await startVariant(selected, source, action, askPermission);
    }
    const filename = filenameFromUrl(source.url, "download.bin");
    const result = await requestNative("download.start", { url: source.url, filename, referer: pageUrl }, askPermission);
    if (result?.ok) return { ok: true, message: "Download started in ThunderLoad." };
    throw new Error(nativeErrorMessage(result?.error || "Could not start this download."));
  } catch (error) {
    return { ok: false, error: nativeErrorMessage(error) };
  }
}
async function quickDownload(source, card, button) {
  button.disabled = true;
  button.textContent = "Preparing…";
  const result = await downloadOne(source);
  if (result?.ok) showToast(result.message || "Download started.");
  else showToast(result?.error || "Could not start this download.");
  button.disabled = false;
  button.textContent = "Download";
}
async function startVariant(variant, source, action, askPermission = false) {
  const filename = filenameFromUrl(source.url, "video");
  const result = await requestNative(action, {
    url: variant.url,
    referer: pageUrl,
    filename: variant.kind === "hls" || variant.kind === "dash" ? (filename.replace(/\.[^.]+$/, "") + ".mp4") : filename,
    representationId: variant.representationId,
    audioRepresentationId: variant.audioRepresentationId || ""
  }, askPermission);
  if (!result?.ok) throw new Error(nativeErrorMessage(result?.error || "Could not start the selected quality."));
  return { ok: true, message: "Downloading " + (variant.quality || "media") + " in ThunderLoad." };
}
async function inspectSource(source, card, button) {
  button.disabled = true;
  button.textContent = "Checking…";
  try {
    const result = await requestNative("video.inspect", { url: source.url, referer: pageUrl }, true);
    if (!result?.ok) throw new Error(nativeErrorMessage(result?.error || "Install the ThunderLoad desktop companion to inspect quality options."));
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
          const result = await startVariant(variant, source, variant.kind === "hls" ? "video.download" : variant.kind === "dash" ? "dash.download" : "download.start");
          showToast(result.message || "Download started in ThunderLoad.");
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
    showToast(nativeErrorMessage(error));
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
    truncated = !!result.truncated;
    // A fresh scan checks everything, so "Download selected" is one click.
    selected = new Set(sources.map((source) => source.url));
    scanMeta.textContent = sources.length ? "Page scanned locally" : "No media detected yet";
    render();
  } catch (error) {
    sources = []; truncated = false; scanMeta.textContent = "Scan unavailable"; render();
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
selectAllBtn.addEventListener("click", () => selectAll(false));
selectNoneBtn.addEventListener("click", () => selectAll(true));
downloadSelectedBtn.addEventListener("click", downloadSelected);
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
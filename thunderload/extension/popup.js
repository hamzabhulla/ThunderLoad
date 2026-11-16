const api = globalThis.browser ?? chrome;
const sourceList = document.getElementById("sourceList");
const toast = document.getElementById("toast");
const rescanBtn = document.getElementById("rescanBtn");
const interceptToggle = document.getElementById("interceptToggle");
let pageUrl = "";
let sources = [];
// The first DASH/HLS source on the page (or the first source of any
// kind when the page has no manifest). Its qualities are inspected
// automatically on scan — no "Other qualities" click needed.
let primarySource = null;
let primaryVariants = null;

const INTERCEPT_SETTING_KEY = "interceptBrowserDownloads";

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
// Quality label from a real variant only: "{height}p" (+ " HD" at
// 720p and up), "Audio" for audio-only streams, otherwise the
// engine's own quality string ("Adaptive" for an HLS master).
// No fps and no subtitles are shown — the engine exposes neither.
function qualityLabel(variant) {
  const height = variant?.height || 0;
  if (height > 0) return height + "p" + (height >= 720 ? " HD" : "");
  if (variant?.audioOnly || variant?.audio_only) return "Audio";
  return variant?.quality || "Stream";
}
function otherLabel(source) {
  const name = filenameFromUrl(source.url, "");
  if (name) return name;
  try { return new URL(source.url).hostname; } catch (_) { return source.kind || "Media"; }
}
function renderQualityRows(list, variants, source) {
  list.replaceChildren();
  if (!variants.length) {
    list.append(el("div", "quality-empty", "No downloadable qualities are exposed."));
    return;
  }
  for (const variant of variants) {
    const row = el("div", "quality");
    row.append(el("strong", "", qualityLabel(variant)));
    const download = el("button", "download-btn", "Download");
    download.addEventListener("click", async () => {
      download.disabled = true;
      try {
        const result = await startVariant(variant, source, variant.kind === "hls" ? "video.download" : variant.kind === "dash" ? "dash.download" : "download.start");
        showToast(result.message || "Download started in ThunderLoad.");
      } catch (error) { showToast(nativeErrorMessage(error)); }
      finally { download.disabled = false; }
    });
    row.append(download);
    list.append(row);
  }
}
function renderOtherMedia(others) {
  if (!others.length) return null;
  const details = el("details", "other-media");
  details.append(el("summary", "", "Other media on this page (" + others.length + ")"));
  const list = el("div", "other-list");
  for (const source of others) {
    const row = el("div", "other-row");
    row.append(el("span", "other-label", otherLabel(source)));
    const download = el("button", "download-btn", "Download");
    download.addEventListener("click", async () => {
      download.disabled = true;
      const result = await downloadOne(source, true);
      showToast(result?.ok ? (result.message || "Download started.") : (result?.error || "Could not start this download."));
      download.disabled = false;
    });
    row.append(download);
    list.append(row);
  }
  details.append(list);
  return details;
}
function render() {
  sourceList.replaceChildren();
  primarySource = null;
  primaryVariants = null;
  if (!sources.length) {
    sourceList.append(el("div", "empty", "No media found on this page."));
    return;
  }
  primarySource = sources.find((source) => source.kind === "DASH manifest" || source.kind === "HLS playlist") || sources[0];
  const others = sources.filter((source) => source !== primarySource);
  const card = el("section", "primary-card");
  const primaryBtn = el("button", "primary primary-btn", "Download this video");
  primaryBtn.addEventListener("click", () => downloadPrimary(primaryBtn));
  card.append(primaryBtn);
  if (primarySource.kind === "DASH manifest" || primarySource.kind === "HLS playlist") {
    const list = el("div", "quality-list");
    list.append(el("div", "quality-empty", "Checking qualities…"));
    card.append(list);
  }
  sourceList.append(card);
  const otherSection = renderOtherMedia(others);
  if (otherSection) sourceList.append(otherSection);
}
function updatePrimaryCard() {
  const card = sourceList.querySelector(".primary-card");
  if (!card || !primarySource) return;
  const button = card.querySelector(".primary-btn");
  const list = card.querySelector(".quality-list");
  if (button && primaryVariants?.length) button.textContent = "Download this video · " + qualityLabel(primaryVariants[0]);
  if (list) renderQualityRows(list, primaryVariants || [], primarySource);
}
async function downloadPrimary(button) {
  button.disabled = true;
  try {
    let result;
    if (primaryVariants?.length) {
      const best = primaryVariants[0];
      const action = best.kind === "hls" ? "video.download" : best.kind === "dash" ? "dash.download" : "download.start";
      result = await startVariant(best, primarySource, action, true);
    } else {
      // No inspected variants (inspect failed, or a Direct media
      // primary): downloadOne inspects again and picks the best, and
      // prompts for the nativeMessaging permission if needed.
      result = await downloadOne(primarySource, true);
    }
    showToast(result?.ok ? (result.message || "Download started.") : (result?.error || "Could not start this download."));
  } catch (error) { showToast(nativeErrorMessage(error)); }
  finally { button.disabled = false; }
}
async function downloadOne(source, askPermission = true) {
  // Single-item download core, shared by the primary button and the
  // other-media rows. Returns a result instead of touching the UI,
  // so the caller can summarize.
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
  // function. `askPermission` lets the caller prompt on click.
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
async function autoInspectPrimary() {
  if (!primarySource) return;
  if (primarySource.kind !== "DASH manifest" && primarySource.kind !== "HLS playlist") return;
  const target = primarySource;
  try {
    // No permission prompt here: if the permission is missing, the
    // inspect fails quietly and the primary button prompts on click.
    const inspected = await requestNative("video.inspect", { url: target.url, referer: pageUrl }, false);
    if (primarySource !== target) return;
    if (!inspected?.ok) return;
    const variants = (inspected.data?.variants || []).filter((item) => item?.downloadable && item?.url);
    variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
    primaryVariants = variants;
    updatePrimaryCard();
  } catch (_) { /* Primary button falls back to downloadOne. */ }
}
async function scanActiveTab() {
  sourceList.replaceChildren(el("div", "empty", "Scanning this tab…"));
  try {
    const result = await api.runtime.sendMessage({ type: "GET_ACTIVE_SOURCES" });
    if (!result?.ok) throw new Error(result?.error || "Cannot scan this page");
    pageUrl = result.pageUrl || "";
    sources = result.sources || [];
    render();
    autoInspectPrimary();
  } catch (error) {
    sources = []; render();
    showToast(error.message || String(error));
  }
}
async function loadInterceptToggle() {
  try {
    const stored = await api.storage.local.get({ [INTERCEPT_SETTING_KEY]: true });
    interceptToggle.checked = stored?.[INTERCEPT_SETTING_KEY] !== false;
  } catch (_) { interceptToggle.checked = true; }
}
rescanBtn.addEventListener("click", scanActiveTab);
interceptToggle.addEventListener("change", () => {
  // Mirror loadInterceptToggle's guard (:248): evaluating
  // api.storage.local throws a synchronous TypeError when the
  // storage API is unavailable, and .catch only handles the
  // promise — so the access itself must be guarded.
  try { api.storage?.local?.set({ [INTERCEPT_SETTING_KEY]: interceptToggle.checked })?.catch(() => {}); }
  catch (_) { /* Storage unavailable; the toggle still applies to this session. */ }
});
api.runtime.onMessage.addListener((message) => {
  if (message?.type === "NATIVE_EVENT") {
    const event = message.event;
    if (event?.event === "download.complete") showToast("Download complete: " + event.path);
    if (event?.event === "download.error") showToast("Download failed: " + event.error);
  }
  if (message?.type === "NATIVE_DISCONNECTED") showToast("Engine disconnected");
  if (message?.type === "CONTEXT_DOWNLOAD_RESULT" && !message.result?.ok) showToast(message.result?.error || "Download failed");
});
loadInterceptToggle();
scanActiveTab();

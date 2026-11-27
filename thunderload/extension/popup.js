const api = globalThis.browser ?? chrome;
const sourceList = document.getElementById("sourceList");
const toast = document.getElementById("toast");
const rescanBtn = document.getElementById("rescanBtn");
const interceptToggle = document.getElementById("interceptToggle");
const interceptRow = document.getElementById("interceptRow");
let pageUrl = "";
let activeTabId = null;
let sources = [];
// H7: false when the browser reports <all_urls> host access revoked
// (about:addons toggle) — content scripts and webRequest silently
// stop, so the popup must say so instead of "No media found".
let hostPermissionGranted = true;
let scanRun = 0;
// Slice 7c (Gemini claim 8): the background caps the list at
// MAX_SOURCES_PER_TAB and reports it as `truncated` — the popup must
// say so instead of silently showing a partial list.
let sourcesTruncated = false;
// The first DASH/HLS source on the page (or the first source of any
// kind when the page has no manifest). Its qualities are inspected
// automatically on scan — no "Other qualities" click needed.
let primarySource = null;
let primaryVariants = null;
// Slice 3: yt-dlp formats for this tab's page. null = no inspect has
// run (or it returned nothing usable yet); an array (possibly empty)
// means an inspect ran. The popup auto-inspects a YouTube/social
// page; the background cache stops a re-open from re-spawning yt-dlp.
let ytdlpVariants = null;
let ytdlpTitle = "";
let ytdlpInspecting = false;
let ytdlpToolsMissing = null;
let ytdlpDenoMissing = false;
let ytdlpToolProgressText = "";
const YTDLP_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "tiktok.com", "twitter.com", "x.com", "facebook.com", "fb.watch", "reddit.com", "twitch.tv", "vimeo.com"];
function isYtDlpPageUrl(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return YTDLP_HOSTS.some((listed) => host === listed || host.endsWith("." + listed));
  } catch (_) { return false; }
}
function normalizeMissingTool(missing) {
  if (Array.isArray(missing)) return typeof missing[0] === "string" && missing[0] ? missing[0] : null;
  return typeof missing === "string" && missing ? missing : null;
}
function sanitizeSubtitleFilename(name) {
  const cleaned = String(name || "subtitles.vtt").replace(/[\u0000-\u001f\\/:*?"<>|]+/g, "_");
  const dot = cleaned.lastIndexOf(".");
  if (dot > 0) {
    return (cleaned.slice(0, dot).slice(0, 180) || "subtitles") + cleaned.slice(dot);
  }
  return cleaned.slice(0, 180) || "subtitles.vtt";
}

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

// Slice 7c (Gemini claim 9): only a KNOWN media/subtitle extension is
// replaced — the old `filename.replace(/\.[^.]+$/, "")` stripped
// whatever followed the last dot, so "my.video" became "my.mp4". A name
// with no extension, or one ending in something that is not a media
// extension, keeps its full name and gains the extension
// ("my.video.mp4"). Mirrored in background.js (same helper, same cases).
const KNOWN_FILE_EXTENSIONS = new Set(["mp4", "mkv", "webm", "mov", "avi", "m4v", "ts", "m2ts", "mp3", "m4a", "aac", "opus", "flac", "wav", "ogg", "oga", "wmv", "flv", "3gp", "m3u8", "mpd", "vtt", "srt", "ass", "ssa"]);
function withExtension(name, extension) {
  const trimmed = name.replace(/\.+$/, "");
  const dot = trimmed.lastIndexOf(".");
  if (dot > 0 && KNOWN_FILE_EXTENSIONS.has(trimmed.slice(dot + 1).toLowerCase())) {
    return trimmed.slice(0, dot) + "." + extension;
  }
  return trimmed + "." + extension;
}
// Quality label from a real variant only: "{height}p" (+ " HD" at
// 720p and up), "Audio" for audio-only streams, otherwise the
// engine's own quality string ("Adaptive" for an HLS master).
// Slice 6: a live row is prefixed "Live · ", and the frame rate rides
// next to the quality when the source carries it — never guessed.
function qualityLabel(variant) {
  const height = variant?.height || 0;
  let label;
  if (height > 0) label = height + "p" + (height >= 720 ? " HD" : "");
  else if (variant?.audioOnly || variant?.audio_only) label = "Audio";
  else label = variant?.quality || "Stream";
  if (variant?.isLive || variant?.is_live) label = "Live · " + label;
  if (variant?.fps) label += " · " + Math.round(variant.fps) + " fps";
  return label;
}

// Slice 6: subtitle tracks the engine surfaced for the first variant
// (they ride on every variant). One row per language with its own
// Download button — the format is what the source carries.
function renderSubtitleRows(list, variants) {
  const subtitles = variants?.[0]?.subtitles || [];
  if (!subtitles.length) return;
  list.append(el("div", "quality-sub-head", "Subtitles"));
  for (const subtitle of subtitles) {
    const row = el("div", "quality");
    row.append(el("strong", "", subtitle.name || subtitle.language || "Subtitles"));
    const download = el("button", "download-btn", "Download");
    download.addEventListener("click", async () => {
      download.disabled = true;
      await downloadSubtitle(subtitle);
      download.disabled = false;
    });
    row.append(download);
    list.append(row);
  }
}
async function downloadSubtitle(subtitle) {
  try {
    if (subtitle.format === "m3u8") {
      const result = await api.runtime.sendMessage({
        type: "NATIVE_REQUEST",
        action: "hls.subtitles.download",
        payload: { playlistUrl: subtitle.url, url: subtitle.url, filename: sanitizeSubtitleFilename((subtitle.language || "subtitles") + ".vtt"), referer: pageUrl }
      });
      if (!result?.ok) throw new Error(result?.error || "Could not download subtitles.");
    } else {
      const result = await api.runtime.sendMessage({
        type: "BROWSER_DOWNLOAD",
        url: subtitle.url,
        filename: sanitizeSubtitleFilename((subtitle.language || "subtitles") + "." + (subtitle.format || "vtt"))
      });
      if (!result?.ok) throw new Error(result?.error || "Could not download subtitles.");
    }
    showToast("Subtitles saved.");
  } catch (error) {
    showToast(nativeErrorMessage(error));
  }
}

// Slice 6: downloads intercepted while the desktop app was down are
// still waiting for the handoff — the popup says so instead of
// leaving the user wondering where the file went.
let pendingIntercepts = [];
function appendInterceptNote() {
  if (!pendingIntercepts.length) return;
  sourceList.append(el("div", "intercept-note",
    pendingIntercepts.length === 1
      ? "1 download is waiting for the desktop app."
      : pendingIntercepts.length + " downloads are waiting for the desktop app."));
}
// H7: when host access was revoked, detection is silently dead —
// the notice names the cause and offers a re-enable tap. The request
// runs directly from this click handler first, so the user gesture
// survives; the background REQUEST_HOST_PERMISSION message is the
// fallback when the permissions API is unavailable here. Either way
// a denied/failed prompt leaves the notice up (with the manual path
// in the text) instead of erroring.
function appendHostPermissionNote() {
  if (hostPermissionGranted) return;
  const note = el("div", "intercept-note", "Site access is off — ThunderLoad can't see media on this page. Turn it back on in the browser's extension settings, or tap below.");
  const button = el("button", "secondary", "Enable site access");
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      if (typeof api.permissions?.request === "function") {
        const granted = await api.permissions.request({ origins: ["<all_urls>"] });
        if (granted) { hostPermissionGranted = true; render(); return; }
      }
    } catch (_) { /* Fall through to the background path. */ }
    try {
      const result = await api.runtime.sendMessage({ type: "REQUEST_HOST_PERMISSION" });
      hostPermissionGranted = result?.granted === true;
    } catch (_) { /* Stays revoked; the notice remains. */ }
    button.disabled = false;
    render();
  });
  note.append(button);
  sourceList.append(note);
}
function appendTruncatedNote() {
  if (!sourcesTruncated || !sources.length) return;
  sourceList.append(el("div", "truncated-note",
    "Showing the first " + sources.length + " of more media on this page."));
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
  renderSubtitleRows(list, variants);
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
  appendHostPermissionNote();
  primarySource = null;
  primaryVariants = null;
  // Slice 3: a yt-dlp page gets the yt-dlp card as its primary —
  // loading, consent, or formats — with the sniffed sources below
  // it (already suppressed by the background when formats exist;
  // the sniffed manifest stays as the fallback when they do not).
  if (ytdlpInspecting || ytdlpVariants !== null || ytdlpToolsMissing) {
    renderYtDlpCard();
    const others = sources.filter((source) => source !== primarySource);
    const otherSection = renderOtherMedia(others);
    if (otherSection) sourceList.append(otherSection);
    appendInterceptNote();
    appendTruncatedNote();
    return;
  }
  if (!sources.length) {
    sourceList.append(el("div", "empty", "No media found on this page."));
    appendInterceptNote();
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
  appendInterceptNote();
  appendTruncatedNote();
}
function renderYtDlpCard() {
  const card = el("section", "primary-card");
  if (ytdlpInspecting) {
    card.append(el("div", "quality-empty", "Getting formats…"));
    sourceList.append(card);
    return;
  }
  if (ytdlpToolsMissing) {
    const name = ytdlpToolsMissing === "deno" ? "Deno" : "yt-dlp";
    card.append(el("div", "ytdlp-note", name + " is not installed yet. Download it to get formats from this page."));
    card.append(ytdlpToolDownloadButton(ytdlpToolsMissing, () => { void inspectYtDlpPage(); }));
    sourceList.append(card);
    return;
  }
  if (!ytdlpVariants?.length) {
    card.append(el("div", "quality-empty", "No downloadable formats found for this page."));
    sourceList.append(card);
    return;
  }
  const primaryBtn = el("button", "primary primary-btn", "Download this video · " + qualityLabel(ytdlpVariants[0]));
  primaryBtn.addEventListener("click", async () => {
    primaryBtn.disabled = true;
    try {
      const result = await startYtDlpVariant(ytdlpVariants[0]);
      showToast(result.message || "Download started in ThunderLoad.");
    } catch (error) { showToast(nativeErrorMessage(error)); }
    finally { primaryBtn.disabled = false; }
  });
  card.append(primaryBtn);
  const list = el("div", "quality-list");
  renderYtDlpQualityRows(list, ytdlpVariants);
  card.append(list);
  if (ytdlpDenoMissing) {
    card.append(el("div", "ytdlp-note", "Deno is not installed — YouTube shows fewer formats without it."));
    card.append(ytdlpToolDownloadButton("deno", () => { ytdlpDenoMissing = false; render(); }));
  }
  sourceList.append(card);
}
function renderYtDlpQualityRows(list, variants) {
  list.replaceChildren();
  for (const variant of variants) {
    const row = el("div", "quality");
    row.append(el("strong", "", qualityLabel(variant)));
    const download = el("button", "download-btn", "Download");
    download.addEventListener("click", async () => {
      download.disabled = true;
      try {
        const result = await startYtDlpVariant(variant);
        showToast(result.message || "Download started in ThunderLoad.");
      } catch (error) { showToast(nativeErrorMessage(error)); }
      finally { download.disabled = false; }
    });
    row.append(download);
    list.append(row);
  }
  renderSubtitleRows(list, variants);
}
// One named tool-download button (consent + progress, FINAL
// requirement): the button names the tool ("Download yt-dlp" /
// "Download Deno"), the progress line names it too ("yt-dlp download
// ho raha hai — 45%") — never a generic label
// anywhere. The tap itself is the consent: nothing is fetched until
// the user presses this button. The manual link is only the backup.
function ytdlpToolDownloadButton(tool, onDone) {
  const toolId = normalizeMissingTool(tool) || "ytdlp";
  const wrap = el("div", "ytdlp-consent");
  const name = toolId === "deno" ? "Deno" : "yt-dlp";
  const button = el("button", "secondary-btn", "Download " + name);
  const progress = el("div", "ytdlp-progress" + (ytdlpToolProgressText ? "" : " hidden"), ytdlpToolProgressText);
  const manual = el("button", "ytdlp-manual", "or download it yourself");
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      const result = await api.runtime.sendMessage({ type: "YTDLP_ENSURE_TOOL", tool: toolId });
      if (!result?.ok) throw new Error(result?.error || "Download failed.");
      if (typeof onDone === "function") onDone();
    } catch (error) {
      showToast(nativeErrorMessage(error));
      button.disabled = false;
    }
  });
  manual.addEventListener("click", () => {
    api.runtime.sendMessage({ type: "YTDLP_OPEN_MANUAL_PAGE", tool: toolId }).catch(() => {});
  });
  wrap.append(button, progress, manual);
  return wrap;
}
async function startYtDlpVariant(variant) {
  const formatSelector = variant.audioRepresentationId
    ? variant.representationId + "+" + variant.audioRepresentationId
    : variant.representationId;
  const filename = ytdlpTitle ? ytdlpTitle + ".mp4" : "video.mp4";
  const result = await api.runtime.sendMessage({ type: "YTDLP_DOWNLOAD", pageUrl, formatSelector, filename });
  if (!result?.ok) throw new Error(nativeErrorMessage(result?.error || "Could not start this download."));
  return { ok: true, message: result.message || "Download started in ThunderLoad." };
}
async function inspectYtDlpPage() {
  ytdlpInspecting = true;
  try {
    const result = await api.runtime.sendMessage({ type: "YTDLP_INSPECT", pageUrl, tabId: activeTabId });
    if (!result?.ok) {
      ytdlpToolsMissing = normalizeMissingTool(result?.toolsMissing);
      if (!ytdlpToolsMissing) showToast(result?.error || "Could not inspect this page.");
      ytdlpVariants = null;
      // Slice 6 fix (MINOR 1): the normal inspect found nothing
      // usable — try the GenericIE fallback. toolsMissing is the
      // exception: generic needs the same yt-dlp binary, so it
      // cannot run either.
      if (!ytdlpToolsMissing) void inspectGenericPage(++genericInspectRun);
    } else {
      const variants = (result.variants || []).filter((item) => item?.downloadable && item?.url);
      variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
      ytdlpVariants = variants;
      ytdlpTitle = result.title || "";
      ytdlpToolsMissing = null;
      if (!variants.length) void inspectGenericPage(++genericInspectRun);
      else void checkYtDlpDeno();
    }
  } catch (error) {
    showToast(nativeErrorMessage(error));
    ytdlpVariants = null;
    // A failed inspect is not the end — try the GenericIE fallback.
    void inspectGenericPage(++genericInspectRun);
  } finally {
    ytdlpInspecting = false;
    render();
  }
}
async function checkYtDlpDeno() {
  try {
    const status = await api.runtime.sendMessage({ type: "YTDLP_TOOLS_STATUS" });
    if (status?.ok && status.deno && status.deno.present === false) {
      ytdlpDenoMissing = true;
      render();
    }
  } catch (_) { /* Tools status is best-effort here. */ }
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
    // Slice 4: the master playlist URL, so the engine can re-resolve it
    // with the selected variant + audio instead of downloading the child
    // media playlist alone (which drops the audio track).
    masterUrl: source.url,
    referer: pageUrl,
    filename: variant.kind === "hls" || variant.kind === "dash" ? withExtension(filename, "mp4") : filename,
    representationId: variant.representationId,
    audioRepresentationId: variant.audioRepresentationId || "",
    // Slice 6: a live row records the stream as it arrives.
    live: !!(variant.isLive || variant.is_live)
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
  const run = ++scanRun;
  rescanBtn.disabled = true;
  sourceList.replaceChildren(el("div", "empty", "Scanning this tab…"));
  try {
    const result = await api.runtime.sendMessage({ type: "GET_ACTIVE_SOURCES" });
    if (run !== scanRun) return;
    if (!result?.ok) throw new Error(result?.error || "Cannot scan this page");
    pageUrl = result.pageUrl || "";
    activeTabId = result.tabId ?? null;
    sources = result.sources || [];
    sourcesTruncated = result.truncated === true;
    // H7: undefined (an older background without the field) means
    // granted — only an explicit false counts as revoked.
    hostPermissionGranted = result.hostPermissionGranted !== false;
    // Slice 6: downloads intercepted while the desktop app was down.
    pendingIntercepts = Array.isArray(result.pendingIntercepts) ? result.pendingIntercepts : [];
    if (result.ytdlp) {
      // An inspect already ran for this tab (this popup session or
      // the cached background entry): show its formats, no re-spawn.
      ytdlpVariants = result.ytdlp.variants || [];
      ytdlpTitle = result.ytdlp.title || "";
      ytdlpInspecting = false;
      ytdlpToolsMissing = null;
      render();
      // Slice 6 fix (MINOR 1): a cached inspect that found nothing
      // usable is not the end — try the GenericIE fallback.
      if (!ytdlpVariants.length) void inspectGenericPage(++genericInspectRun);
      else void checkYtDlpDeno();
    } else if (isYtDlpPageUrl(pageUrl)) {
      // First open on a YouTube/social page: inspect it (loading
      // state first — inspect takes 2–10 s; the background cache
      // stops a re-open from re-spawning yt-dlp).
      ytdlpInspecting = true;
      ytdlpVariants = null;
      render();
      await inspectYtDlpPage();
    } else {
      ytdlpVariants = null;
      ytdlpInspecting = false;
      ytdlpToolsMissing = null;
      render();
      autoInspectPrimary();
      // Slice 6: GenericIE fallback — sniffing found nothing usable
      // for this page, so ask yt-dlp's generic extractor what the
      // page itself exposes. A stale result (user rescanned while
      // the inspect ran) is dropped by the run guard.
      if (!sources.length) void inspectGenericPage(++genericInspectRun);
    }
  } catch (error) {
    if (run !== scanRun) return;
    sources = []; sourcesTruncated = false; render();
    showToast(error.message || String(error));
  } finally {
    if (run === scanRun) rescanBtn.disabled = false;
  }
}
// Slice 6: the GenericIE fallback inspect for the popup. Runs only
// when the scan found nothing; the run guard drops results from a
// superseded scan.
let genericInspectRun = 0;
async function inspectGenericPage(run) {
  try {
    const result = await api.runtime.sendMessage({ type: "YTDLP_INSPECT_GENERIC", pageUrl });
    if (run !== genericInspectRun) return;
    if (!result?.ok) return;
    const variants = (result.variants || []).filter((item) => item?.downloadable && item?.url);
    if (!variants.length) return;
    variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
    ytdlpVariants = variants;
    ytdlpTitle = result.title || "";
    ytdlpToolsMissing = null;
    render();
  } catch (_) { /* A failed generic inspect leaves the empty state. */ }
}
async function loadInterceptToggle() {
  // M12: no flicker. The toggle ships hidden + disabled with NO
  // hardcoded checked state (popup.html) and is only revealed once
  // the stored value is applied below — the user never sees it flip
  // from the hardcoded `checked` to the stored false. An inline
  // script in popup.html could not do this instead: MV3 extension
  // pages block inline scripts under the default CSP, so
  // hide-until-loaded from the external script is the correct fix.
  // The unhide runs on every path (including the catch), so a
  // storage failure can never leave the toggle hidden forever.
  try {
    const stored = await api.storage.local.get({ [INTERCEPT_SETTING_KEY]: true });
    interceptToggle.checked = stored?.[INTERCEPT_SETTING_KEY] !== false;
  } catch (_) { interceptToggle.checked = true; }
  interceptToggle.disabled = false;
  if (interceptRow) interceptRow.hidden = false;
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
    // Slice 3: tool-download progress — the line names the tool.
    if (event?.event === "ytdlp.tool_progress") {
      const tool = event.tool === "deno" ? "Deno" : "yt-dlp";
      ytdlpToolProgressText = tool + " download ho raha hai"
        + (typeof event.percent === "number" ? " — " + Math.round(event.percent) + "%" : "…");
      for (const node of sourceList.querySelectorAll(".ytdlp-progress")) {
        node.textContent = ytdlpToolProgressText;
        node.classList.remove("hidden");
      }
    }
  }
  if (message?.type === "NATIVE_DISCONNECTED") showToast("Engine disconnected");
  if (message?.type === "CONTEXT_DOWNLOAD_RESULT" && !message.result?.ok) showToast(message.result?.error || "Download failed");
});
loadInterceptToggle();
scanActiveTab();

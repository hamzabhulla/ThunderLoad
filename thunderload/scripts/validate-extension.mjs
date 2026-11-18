import { readFile, access } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ext = resolve(root, "extension");
for (const file of ["background.js","content.js","hook.js","popup.html","popup.css","popup.js","icons/16.png","icons/32.png","icons/48.png","icons/128.png"]) await access(resolve(ext,file));
const chromium = JSON.parse(await readFile(resolve(ext,"manifest.json"),"utf8"));
const firefox = JSON.parse(await readFile(resolve(ext,"manifest.firefox.json"),"utf8"));
// Why the extension requests broad host access: IDM-style automatic video
// detection cannot work with activeTab-only access. The content script must
// run on every page (content_scripts, <all_urls>) so the hover button appears
// over any video without a click, and the background must observe media
// traffic (webRequest + host_permissions) to catch streams the DOM scan
// cannot see (fetch/XHR-loaded media, cross-origin iframes). The earlier
// minimal-permission stance is intentionally given up for this feature;
// nothing is blocked, modified or exfiltrated — observation feeds the local
// download list only.
for (const [name,manifest] of [["Chromium",chromium],["Firefox",firefox]]) {
  if (manifest.manifest_version !== 3 || !manifest.version || !manifest.name || !manifest.action?.default_popup) throw new Error(name+" manifest metadata is invalid");
  if (!manifest.permissions?.includes("activeTab") || !manifest.permissions?.includes("downloads")) throw new Error(name+" manifest lacks activeTab or downloads");
  if (!manifest.permissions?.includes("webRequest")) throw new Error(name+" manifest lacks webRequest for media-traffic observation");
  if (!manifest.permissions?.includes("storage")) throw new Error(name+" manifest lacks storage for the intercept toggle and persisted detections");
  if (!manifest.optional_permissions?.includes("nativeMessaging")) throw new Error(name+" must make nativeMessaging optional");
  if (!manifest.host_permissions?.includes("<all_urls>")) throw new Error(name+" must request <all_urls> host access for automatic detection");
  const script = manifest.content_scripts?.[0];
  if (!script || !script.matches?.includes("<all_urls>") || !script.js?.includes("content.js") || script.run_at !== "document_idle" || script.all_frames !== true) throw new Error(name+" must auto-inject content.js on every page (all frames, document_idle)");
  // Slice 2: the MAIN-world hook (hook.js) runs in the page's own
  // JavaScript world at document_start so it can wrap MediaSource /
  // fetch / XHR before page scripts run. Chrome supports world MAIN
  // since 111 (manifest requires 116); Firefox since 128 (manifest
  // requires 128.0) — older Firefox ignores the key and runs the
  // script isolated, where it observes nothing (harmless no-op).
  const hook = manifest.content_scripts?.find((entry) => entry.world === "MAIN");
  if (!hook || !hook.js?.includes("hook.js") || hook.run_at !== "document_start" || hook.all_frames !== true) throw new Error(name+" must inject hook.js in the MAIN world at document_start (all frames)");
}
if (!firefox.browser_specific_settings?.gecko?.id) throw new Error("Firefox manifest needs a stable Gecko add-on ID");
// Slice 6: Firefox blocking build + GenericIE + interception transport
// + live/subtitles. Firefox — and only Firefox — requests
// webRequestBlocking so the background can cancel a navigation to a
// direct media file and hand it to the desktop engine; Chrome MV3
// cannot block, so its manifest must NOT carry the permission.
if (!firefox.permissions?.includes("webRequestBlocking")) throw new Error("Firefox manifest must request webRequestBlocking for the blocking download handoff");
if (chromium.permissions?.includes("webRequestBlocking")) throw new Error("Chromium manifest must NOT request webRequestBlocking (MV3 cannot block)");
// Slice 3: yt-dlp routing contract. The background must handle the
// new YTDLP message types; the panel and popup must show the tool by
// name on the consent + progress steps ("yt-dlp download ho raha
// hai" / "Deno download ho raha hai") and must never show a generic
// "downloading tool" label. No manifest permission was added for
// this slice (the tools run in the native host, not the extension).
const background = await readFile(resolve(ext, "background.js"), "utf8");
for (const type of ["YTDLP_INSPECT", "YTDLP_DOWNLOAD", "YTDLP_ENSURE_TOOL", "YTDLP_TOOLS_STATUS", "YTDLP_CHECK_UPDATE", "YTDLP_OPEN_MANUAL_PAGE"]) {
  if (!background.includes('"' + type + '"')) throw new Error("background.js must handle " + type);
}
if (!background.includes("ytdlp.inspect") || !background.includes("ytdlp.download")) throw new Error("background.js must forward yt-dlp actions to the native host");
const content = await readFile(resolve(ext, "content.js"), "utf8");
const popup = await readFile(resolve(ext, "popup.js"), "utf8");
for (const [name, text] of [["content.js", content], ["popup.js", popup]]) {
  if (!text.includes("download ho raha hai")) throw new Error(name + " must name the tool in the download progress text");
  if (/downloading tool/i.test(text)) throw new Error(name + " must not use a generic downloading-tool label");
  if (!text.includes("or download it yourself")) throw new Error(name + " must offer the manual download alternative");
}
if (!content.includes("YTDLP_INSPECT") || !content.includes("YTDLP_DOWNLOAD")) throw new Error("content.js must run the yt-dlp panel flow");
if (!popup.includes("YTDLP_INSPECT") || !popup.includes("YTDLP_DOWNLOAD")) throw new Error("popup.js must run the yt-dlp inspect flow");
// Slice 5: auth forwarding + IDM-style panel contract. yt-dlp
// inspect and download must carry the page URL as the referer, so a
// login-walled video inspects and downloads with the session instead
// of 403ing (cookies + UA were already attached by requestNative).
// The panel must show live progress for a download started from it:
// the background passes the native jobId through, and content.js
// renders the progress section and tracks download.* events.
if (!background.includes("referer: pageUrl") || !background.includes("referer: message.pageUrl")) throw new Error("background.js must forward the page URL as the referer for yt-dlp inspect and download");
if (!background.includes("jobId")) throw new Error("background.js must pass the native jobId through to the panel");
if (!content.includes("p-progress") || !content.includes("download.progress")) throw new Error("content.js must show live download progress in the panel");
if (!content.includes("p-title-text")) throw new Error("content.js must show the video/page title at the top of the panel");
// Slice 6: Firefox blocking build + GenericIE + interception transport
// + live/subtitles. The generic result lives on a separate cache key
// and never overwrites a real inspect; an intercepted download is
// queued and retried instead of dropped; the panel/popup show live
// rows, subtitle tracks, and the waiting-intercept note.
if (!background.includes('"YTDLP_INSPECT_GENERIC"')) throw new Error("background.js must handle the GenericIE fallback inspect");
if (!background.includes("generic: true") || !background.includes("generic:")) throw new Error("background.js must keep generic results on a separate cache key and never let them overwrite a real inspect");
if (!background.includes("queueIntercept") || !background.includes("INTERCEPT_RETRY_DELAYS_MS")) throw new Error("background.js must queue an intercepted download and retry the handoff instead of dropping it");
if (!background.includes("thunderload.interceptQueue.v1")) throw new Error("background.js must persist the intercept queue so a worker restart cannot drop a waiting download");
if (!background.includes('"GET_INTERCEPT_STATUS"')) throw new Error("background.js must report the waiting intercept queue to the panel/popup");
if (!background.includes("onBeforeRequest") || !background.includes('"blocking"')) throw new Error("background.js must cancel direct-media navigations on Firefox only");
if (!background.includes("hls.subtitles.download")) throw new Error("background.js must forward subtitle downloads to the native host");
for (const [name, text] of [["content.js", content], ["popup.js", popup]]) {
  if (!text.includes("Live · ")) throw new Error(name + " must prefix live rows with Live");
  if (!text.includes("subtitles")) throw new Error(name + " must surface subtitle tracks");
}
// The panel asks the background directly (GET_INTERCEPT_STATUS); the
// popup reads the same queue from GET_ACTIVE_SOURCES' response
// (pendingIntercepts) — both must show the waiting-intercept note.
if (!content.includes("GET_INTERCEPT_STATUS")) throw new Error("content.js must show the waiting-intercept note");
if (!popup.includes("pendingIntercepts")) throw new Error("popup.js must show the waiting-intercept note");
if (!content.includes("YTDLP_INSPECT_GENERIC") || !popup.includes("YTDLP_INSPECT_GENERIC")) throw new Error("content.js and popup.js must trigger the GenericIE fallback when nothing was found");
console.log("Extension manifests and required assets are valid.");

import { readFile, access } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ext = resolve(root, "extension");
for (const file of ["background.js","content.js","popup.html","popup.css","popup.js","icons/16.png","icons/32.png","icons/48.png","icons/128.png"]) await access(resolve(ext,file));
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
  if (!manifest.optional_permissions?.includes("nativeMessaging")) throw new Error(name+" must make nativeMessaging optional");
  if (!manifest.host_permissions?.includes("<all_urls>")) throw new Error(name+" must request <all_urls> host access for automatic detection");
  const script = manifest.content_scripts?.[0];
  if (!script || !script.matches?.includes("<all_urls>") || !script.js?.includes("content.js") || script.run_at !== "document_idle" || script.all_frames !== true) throw new Error(name+" must auto-inject content.js on every page (all frames, document_idle)");
}
if (!firefox.browser_specific_settings?.gecko?.id) throw new Error("Firefox manifest needs a stable Gecko add-on ID");
console.log("Extension manifests and required assets are valid.");

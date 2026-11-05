import { readFile, access } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ext = resolve(root, "extension");
for (const file of ["background.js","content.js","popup.html","popup.css","popup.js","icons/16.png","icons/32.png","icons/48.png","icons/128.png"]) await access(resolve(ext,file));
const chromium = JSON.parse(await readFile(resolve(ext,"manifest.json"),"utf8"));
const firefox = JSON.parse(await readFile(resolve(ext,"manifest.firefox.json"),"utf8"));
for (const [name,manifest] of [["Chromium",chromium],["Firefox",firefox]]) {
  if (manifest.manifest_version !== 3 || !manifest.version || !manifest.name || !manifest.action?.default_popup) throw new Error(name+" manifest metadata is invalid");
  if (!manifest.permissions?.includes("activeTab") || !manifest.permissions?.includes("downloads")) throw new Error(name+" manifest lacks activeTab or downloads");
  if (!manifest.optional_permissions?.includes("nativeMessaging")) throw new Error(name+" must make nativeMessaging optional");
  if (manifest.host_permissions?.length) throw new Error(name+" must not request broad persistent host access");
}
if (!firefox.browser_specific_settings?.gecko?.id) throw new Error("Firefox manifest needs a stable Gecko add-on ID");
console.log("Extension manifests and required assets are valid.");

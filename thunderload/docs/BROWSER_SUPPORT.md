# Browser support and distribution

ThunderLoad ships browser-specific packages, not one universal extension file.

## Chromium family
The Chromium Manifest V3 package is the starting point for Chrome, Microsoft Edge, Brave, Opera and Vivaldi. Upload it to each applicable browser's store or unpack it for manual testing. Each store may require its own listing, screenshots, privacy disclosures, review and signing. Test direct downloads, media detection and native messaging in each browser before advertising support.

### Developer-mode install (no store needed)
Download `ThunderLoad-Chromium.zip` from desktop ThunderLoad (**Settings → Browser extension files**), or build it with `node scripts/package-extensions.mjs`. Extract the ZIP, open `chrome://extensions` (`edge://extensions`, `brave://extensions`, `opera://extensions`), turn Developer mode on, click **Load unpacked** and select the extracted folder. The extension then detects videos on every page automatically and shows its hover Download button over each video. No `.crx` is offered: Chrome refuses to install a `.crx` from outside the Web Store.

## Firefox
Firefox uses the separate Manifest V3 manifest (`manifest.firefox.json` becomes `manifest.json` when packaged). Submit the Firefox ZIP to addons.mozilla.org (AMO). Release and Beta Firefox require Mozilla-signed add-ons; Chrome Web Store approval does not approve the Firefox add-on.

### Extension ID — do not change it
The Firefox add-on ID is `thunderload@example.com` (`browser_specific_settings.gecko.id` in `manifest.firefox.json`). It is intentional and must not change: it is the add-on's stable AMO identity, and it must match the ID entered in Settings → Browser connection when the native host is registered — the browser only talks to a native host whose manifest allowlists exactly this ID. Changing it would break the browser connection for every existing install and require a new AMO identity.

### Developer-mode install (no store needed)
Download `ThunderLoad-Firefox.xpi` from desktop ThunderLoad (**Settings → Browser extension files**), or build it with `node scripts/package-extensions.mjs`. Open `about:addons`, click the gear icon, choose **Install Add-on From File** and select the `.xpi`. Release Firefox only accepts Mozilla-signed add-ons, so until the AMO-signed build exists this file loads on Firefox Developer Edition or Nightly (with `xpinstall.signatures.required` switched off). It is not promised to work on release Firefox today.

### If host permission is revoked
Firefox lets you revoke an extension's access to sites from `about:addons` → ThunderLoad → Permissions (site access). If host access is switched off, automatic detection silently stops: the content scripts no longer run on pages, so no videos are found, no in-player Download button appears, and the background no longer sees media traffic. The popup checks host permission every time it opens and offers to re-grant it on the spot; the popup's own Rescan can still inject the scanner into the active tab on demand (it uses `activeTab`/`scripting`, not host access), so an empty auto-detected list with a working Rescan is the tell-tale sign. If the in-popup re-grant prompt does not appear, re-grant manually in `about:addons` → ThunderLoad → Permissions by switching site access back on, then reload the affected tabs.

## Safari
Safari requires a WebExtension wrapped as a macOS app with Xcode tooling, followed by Apple signing/notarization and distribution. The macOS wrapper is not included in this source release. Do not advertise Safari as available until it is built and tested on macOS.

## Native messaging
Native messaging is optional for single-file subtitle tracks and for the last-resort fallback below. Direct links are intercepted and handed to the desktop engine; the browser's downloads manager is only the last-resort fallback when the engine is unreachable (or from the link context menu). Adaptive HLS and static DASH inspection/download require the ThunderLoad local native host. Static DASH with separate audio/video also requires FFmpeg; install FFmpeg and verify its executable path in desktop Settings → DASH video support. The native host reads the saved FFmpeg path and speed cap from the local desktop configuration and refreshes them while connected.

On Windows, install the desktop app, then open **Settings → Browser connection**. Select Chrome, Edge, Brave or Firefox, enter the exact extension ID, and choose **Connect browser**. Restart that browser and grant Native messaging when the extension asks. Chromium IDs must be 32 characters in the range a–p. The Firefox add-on ID in this package is `thunderload@example.com`.

The desktop app copies the native host into the user's local application data directory, writes a per-browser manifest, and registers a per-user Windows registry entry. The manifest allowlists only the supplied extension ID.

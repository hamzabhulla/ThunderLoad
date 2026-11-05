# ThunderLoad — starter codebase

ThunderLoad is a Windows-first, privacy-conscious download-manager starter. It contains a React/Tauri desktop UI, a Rust transfer engine, a Chromium extension, and a Firefox manifest variant.

> This is an early starter build, not a finished IDM replacement. It has not been compiled or release-tested in this environment because Rust/Cargo and the Tauri build toolchain are not installed here.

## What is implemented in this starter

- Rust HTTP/HTTPS downloader with up to 16 parallel byte-range connections when the server supports them; it falls back to a normal streaming download when range requests are not accepted.
- HLS master-playlist inspection and quality enumeration (resolution and bandwidth when exposed by the playlist).
- Basic HLS media-playlist downloading with concurrent segment fetching for unencrypted streams. Output is `.mp4` when an HLS initialization segment is present, otherwise `.ts`.
- Basic DASH Representation metadata detection (quality enumeration only; DASH segment assembly is not implemented yet).
- Browser extension scan is user-triggered from the extension popup or shortcut. It inspects visible `<video>`/`<source>` elements, page resource entries, and direct media links.
- No in-page banners, injected download buttons, pop-under windows, or unsolicited notifications.
- Right-click a media/file link and choose **Download link with ThunderLoad**.
- Desktop React/Tauri starter UI with a downloads list, URL input, video inspection, and quick-setting controls.

## Honest limitations

1. “10x faster” cannot be guaranteed. Parallel downloading can improve throughput only when the remote server permits range requests and has enough available bandwidth. It cannot exceed the user's connection or the server's rate limits.
2. HLS qualities are available only when the site's master playlist exposes them. This starter chooses the highest listed HLS variant when downloading a master playlist directly; use the extension's quality list to choose a specific variant.
3. DASH qualities are detected but DASH segment downloading/remuxing is not implemented in v0.1.0.
4. Direct media URLs and unencrypted HLS are supported. DRM-protected streams, encrypted HLS keys, blob-only/MSE streams whose source URL is not exposed, and access-control bypass are not supported.
5. Some sites require browser cookies, custom authorization headers, or anti-bot/session context. The starter host does not yet transfer browser cookies to the downloader, so those streams may fail.
6. Pause/resume with persistent byte checkpoints, download scheduling, speed limiting, queue persistence, browser auto-capture on every site, and automatic updates are planned, not complete. The desktop UI labels these as starter settings and does not pretend they are active. Browser-extension downloads are currently tracked in the extension/native-host session; they are not yet merged into the desktop app's in-app queue.
7. The extension asks for `activeTab` and scans only after the user opens it or invokes the scan shortcut. It does not request permanent access to every website. Firefox support is a compatibility starting point and still needs testing on current Firefox releases.

## Build requirements

- Windows 10/11 (recommended for the first build)
- Rust stable and Cargo
- Node.js 20+ and npm
- Tauri v2 prerequisites for Windows (Microsoft C++ Build Tools and WebView2 Runtime)

## Build the native host

From the project root:

```powershell
cargo build --release --manifest-path native-host/Cargo.toml
```

The executable is expected at:

```text
native-host/target/release/thunderload-host.exe
```

## Install the browser extension (Chrome / Edge / Brave)

1. Open `chrome://extensions` (or `edge://extensions` / `brave://extensions`).
2. Enable **Developer mode**.
3. Select **Load unpacked** and choose the `extension` folder.
4. Copy the extension ID shown by the browser.
5. From the project root, run PowerShell:

```powershell
.\scripts-install-native-host.ps1 -ExtensionId "YOUR_EXTENSION_ID"
```

6. Reload the extension and restart the browser.

The installer registers the native host for Chrome, Edge, Brave and Firefox. For Firefox, copy `extension/manifest.firefox.json` over a temporary copy's `manifest.json`, load that folder as a temporary add-on, and use its fixed ID `thunderload@example.com`.

## Run the desktop app

```powershell
cd desktop
npm install
npm run tauri dev
```

To build the frontend separately:

```powershell
cd desktop
npm run build
```

## How video detection behaves

- Open the extension on a page to scan it. Press **Rescan** after playback starts if the site only reveals its source later.
- The scanner is local and quiet: it does not modify the page UI, inject overlays, or show a banner while the user watches a video.
- Select **Inspect qualities** to ask the local Rust engine to inspect an HLS/DASH manifest.
- HLS quality entries can be downloaded if they are unencrypted. DASH entries currently show detected qualities but are disabled for download.
- Protected or DRM-controlled streams are not unlocked or decrypted.

## Suggested next milestones

1. Add a real persisted job database and per-download pause/resume/cancel using range checkpoints.
2. Finish DASH segment-template/list assembly and MP4 remuxing with a bundled, properly licensed media toolchain.
3. Add secure, explicit per-site cookie/header handoff for authenticated sessions.
4. Wire speed limiter, queue, scheduler, category rules, and settings persistence to the engine.
5. Add Chromium/Firefox automated extension tests and Windows installer/update signing.

## Project layout

```text
engine/                 Rust download + media inspection library
native-host/            Native Messaging host for browser extensions
extension/              Chromium extension and Firefox manifest variant
desktop/                React + Tauri v2 desktop starter
scripts-install-native-host.ps1
```

## Privacy

The extension's scanner processes URLs in the current tab locally. There is no analytics endpoint or remote account service in this starter. Downloads are saved under the current user's `Downloads/ThunderLoad` folder.

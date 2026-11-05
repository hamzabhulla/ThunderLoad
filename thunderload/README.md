# ThunderLoad 0.2.0 — Windows desktop + browser extension

**Status: release candidate / not production-ready yet.** This repository contains packaging pipelines, but the tagged Windows build must pass CI and be tested on real machines/browsers before general release. Store approval is decided by each browser vendor.

## Current source features
- Rust HTTP/HTTPS downloader with parallel byte-range transfers (up to 16 connections) when the server explicitly supports ranges, with sequential fallback.
- Direct browser downloads through the browser downloads API.
- On-demand scan of media URLs visible in the active tab. It starts only when the user opens the popup or uses its shortcut.
- No injected page banners, floating video buttons, or automatic scans of every visited site.
- HLS master-playlist quality discovery and unencrypted HLS segment download through the optional desktop native host.
- DASH Representation quality detection only; DASH segment download/remux is not implemented.
- React + Tauri desktop starter with a Windows NSIS installer configuration.
- Native messaging is optional; ordinary direct downloads do not need the desktop app.

## Browser package matrix

| Browser | Package | Status |
|---|---|---|
| Chrome | Chromium ZIP → Chrome Web Store | Buildable; store review pending |
| Edge | Chromium ZIP → Edge Add-ons / supported store route | Buildable; store review pending |
| Brave | Chromium ZIP | Chromium-compatible; test required |
| Opera / Vivaldi | Chromium ZIP | Chromium-compatible; test required |
| Firefox | Separate Firefox manifest ZIP → AMO signing | Buildable; AMO review/signing required |
| Safari | Separate macOS WebExtension app + Xcode signing | Not included |

There is no single file that truthfully installs into every browser. Firefox uses a separate manifest; Safari requires a macOS/Xcode wrapper.

## Build Windows installer

Requirements: Windows 10/11 x64, Rust stable + Cargo, Node.js 20+, npm, Microsoft C++ Build Tools and WebView2.

From this directory in PowerShell:

```powershell
cargo build --release -p thunderload-host
npm install --prefix desktop
npm run build --prefix desktop
npm run tauri --prefix desktop -- build
```

NSIS setup output: `target/release/bundle/nsis/`. GitHub Actions can build the installer on a Windows runner. The installer is unsigned until a trusted code-signing certificate is configured.

## Browser packages

Run `.scriptspackage-extensions.ps1` in PowerShell. Outputs go to `dist/extensions/`:
- `ThunderLoad-Chromium.zip`
- `ThunderLoad-Firefox.zip`

Firefox release/beta requires Mozilla signing. Chrome Web Store and Edge Add-ons each have their own listing and review process.

## Optional native host

Direct links work through the browser downloads API. HLS inspection/download uses the local native host.

1. Install desktop ThunderLoad.
2. Install the extension and copy that browser's extension ID.
3. Run the included `register-native-host.ps1` with the relevant IDs. It copies the native executable to `%LOCALAPPDATA%\ThunderLoad\native-host` and registers selected browsers.
4. Restart those browsers.

Example:
```powershell
.\register-native-host.ps1 -ChromeExtensionId "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" -FirefoxExtensionId "thunderload@example.com"
```
Use separate IDs for Edge, Brave, Opera and Vivaldi if installed there.

## Video limits
HLS qualities are listed only when exposed by the site. Unencrypted HLS segments are supported; encrypted/DRM streams are not decrypted or bypassed. DASH qualities can be detected but DASH downloading/remux is future work. Blob/MSE-only, cookie-bound and signed URLs may not be exposed or may fail. Parallel connections cannot guarantee a fixed speed multiplier; results depend on server support and bandwidth.

## Production work still required
- Persistent job database; reliable pause/resume checkpoints, cancellation and crash recovery.
- DASH segment/template/list assembly and tested MP4 remuxing.
- Explicit, secure cookie/header handoff for user-authorized sessions.
- Speed limiter, queue, scheduler and settings persistence wired to engine.
- Desktop queue integration for extension-started jobs.
- Code signing, update strategy, uninstall cleanup, Windows VM testing and accessibility QA.
- Current-version Firefox/Chromium testing, AMO/Chrome/Edge store review; separate Safari wrapper if Safari is advertised.

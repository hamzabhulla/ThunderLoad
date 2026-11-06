# ThunderLoad 0.2.0 — Windows desktop + browser extension

**Status: release candidate / not production-ready yet.** This repository contains packaging pipelines, but the tagged Windows build must pass CI and be tested on real machines/browsers before general release. Store approval is decided by each browser vendor.

## Current source features
- Desktop direct HTTP/HTTPS transfers use a persistent sidecar checkpoint (`.thunderload.part`). Pause flushes the checkpoint, Resume requests the remaining byte range when the server supports it, and startup recovers direct jobs that were in progress when the app closed.
- Direct desktop transfers support Pause, Resume, Cancel and an aggregate speed cap shared across active desktop direct downloads.
- Queue entries and scheduled start times are stored in desktop local storage. Scheduled entries start at their local selected time while the desktop app is running; no Windows Task Scheduler/background wake-up is installed.
- Desktop HLS downloads support completed, unencrypted VOD playlists. HLS does not yet support Pause/Resume/Cancel, separate audio/video muxing, byte-range playlists or live playlists.
- Browser-started direct and HLS transfers write local status files into `Downloads/ThunderLoad/.native-jobs`. The desktop queue polls these files to show browser job progress and completion. Desktop Pause/Cancel commands do not yet control native-host jobs.
- The Rust engine retains a parallel byte-range downloader (up to 16 connections), but the checkpointed desktop transfer path currently uses one connection. Multi-connection checkpointing is not implemented.
- On-demand scan of media URLs visible in the active tab. It starts only when the user opens the popup or uses its shortcut.
- A compact, hover-only in-player Download control for supported video elements; no banners or automatic scans of every visited site.
- HLS master-playlist quality discovery through the optional desktop native host.
- DASH Representation quality detection only; DASH segment download/remux is not implemented.
- Minimal React + Tauri desktop workspace with Downloads, Queue, Video Grabber and Settings pages, plus a Windows NSIS installer configuration.
- Native messaging is optional; ordinary direct browser downloads use the browser's own downloads API.

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

Run `scripts/package-extensions.ps1` in PowerShell. Outputs go to `dist/extensions/`:
- `ThunderLoad-Chromium.zip`
- `ThunderLoad-Firefox.zip`

Firefox release/beta requires Mozilla signing. Chrome Web Store and Edge Add-ons each have their own listing and review process.

## Optional native host

Direct file links use the browser downloads API and do not need a companion connection. HLS inspection/download uses the local native host.

On Windows, install desktop ThunderLoad, install the browser extension, then open **Settings → Browser connection**. Choose Chrome, Edge, Brave or Firefox, paste the exact extension ID and select **Connect browser**. Restart the browser and grant Native messaging in the extension popup when prompted.

The extension ID is required because the browser native-messaging allowlist must be restricted to the exact installed extension. Firefox's packaged ID is `thunderload@example.com`. Chromium IDs are shown on the browser's extensions developer page and contain 32 characters from `a` through `p`.

## Video limits
HLS qualities are listed only when exposed by the site. Unencrypted HLS segments are supported; encrypted/DRM streams are not decrypted or bypassed. DASH qualities can be detected but DASH downloading/remux is future work. Blob/MSE-only, cookie-bound and signed URLs may not be exposed or may fail. Parallel connections cannot guarantee a fixed speed multiplier; results depend on server support and bandwidth.

## Production work still required
- Persistent job database; reliable pause/resume checkpoints, cancellation and crash recovery.
- DASH segment/template/list assembly and tested MP4 remuxing.
- Explicit, secure cookie/header handoff for user-authorized sessions.
- Transfer cancellation, speed limiting and scheduled downloads; current queue concurrency and local settings are implemented in the desktop UI.
- Desktop queue integration for extension-started jobs.
- Code signing, update strategy, uninstall cleanup, Windows VM testing and accessibility QA.
- Current-version Firefox/Chromium testing, AMO/Chrome/Edge store review; separate Safari wrapper if Safari is advertised.

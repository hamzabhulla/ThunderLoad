# ThunderLoad 0.2.0 — Windows desktop + browser extension

**Status: release candidate / not production-ready yet.** This repository contains packaging pipelines, but the tagged Windows build must pass CI and be tested on real machines/browsers before general release. Store approval is decided by each browser vendor.

## Current source features
- Desktop direct HTTP/HTTPS transfers use a persistent sidecar checkpoint (`.thunderload.part`). Pause flushes the checkpoint, Resume re-requests only the unfinished segments when the server supports byte ranges (falling back to the remaining byte range on the sequential path), and startup recovers direct jobs that were in progress when the app closed.
- Direct desktop transfers support Pause, Resume, Cancel and an aggregate speed cap shared across active direct, HLS and DASH transfers within each ThunderLoad process.
- The same saved speed cap is applied to direct segmented/streaming, HLS and DASH segment transfers in the desktop and browser-native processes independently.
- Queue entries and scheduled start times are recorded in a durable job database (`jobs.json` in the desktop app-data directory, written atomically), so the queue survives an app restart or crash: jobs that were downloading become Interrupted and resumable from their checkpoints, paused jobs stay paused, and scheduled entries keep their start times. Checkpoint files on disk with no database row are imported as Interrupted jobs on startup. Browser-started jobs are mirrored into the same database from their `.native-jobs` status files. On Windows, each scheduled job registers a one-time Task Scheduler wake task (`WakeToRun` / `StartWhenAvailable`) that launches ThunderLoad when due; it is cancelled when the job starts or is removed. The user must be logged in, and actual wake behavior depends on Windows power settings. Other platforms start scheduled entries while the app is running only.
- Desktop HLS downloads support completed, unencrypted VOD playlists, with Pause/Resume/Cancel backed by an on-disk segment checkpoint, EXT-X-BYTERANGE slices, fMP4 (EXT-X-MAP init + fragments) and separate audio renditions muxed into MP4 with an externally installed FFmpeg. Live playlists and encrypted/DRM streams are still not supported.
- Browser-started direct, HLS and DASH transfers write local status files into `Downloads/ThunderLoad/.native-jobs`. The desktop queue polls these files to show browser job progress and completion, and Pause/Resume/Cancel on a browser job are sent to the native host through a per-job control file, so the desktop controls browser-started jobs of every kind. A browser job whose companion has exited (Interrupted) can have its checkpoint handed to the desktop engine and resumed there.
- The checkpointed desktop transfer path downloads in parallel segments (up to 8 connections by default) against one preallocated partial file, with per-segment completion persisted in the metadata sidecar — so pause, resume and restart recovery all work for multi-connection downloads. Servers without byte-range support fall back to the single-connection checkpointed path.
- Every job is recorded in the durable job database (see the queue entry above): a restart restores the full queue with progress, and a corrupt database file is moved aside instead of crashing the app.
- Automatic scan of media URLs visible on every visited page. The extension watches for video elements and media traffic, and shows a hover-only in-player Download control over each video; the popup lists everything found on the active tab, and Alt+Shift+D still forces a re-scan.
- A compact, hover-only in-player Download control for supported video elements; no banners. Automatic detection needs broad host access (`<all_urls>` + `webRequest`): the content script must run on every page and the background must observe media traffic to catch streams the DOM scan cannot see. The earlier minimal-permission stance is intentionally given up for this feature.
- HLS master-playlist quality discovery through the optional desktop native host.
- Static DASH VOD downloads for MPDs using SegmentTemplate/SegmentTimeline, SegmentList and SegmentBase (sidx index), including multi-period MPDs whose periods agree on codec and dimensions, inherited templates, BaseURL resolution and RepresentationID/Bandwidth/Number/Time substitutions.
- Desktop and browser-started DASH downloads support Pause/Resume/Cancel backed by an on-disk checkpoint: finished segments are detected from the segment files on disk, so a resume re-requests only the unfinished segments of each period, and Cancel removes the target, the work directory and the checkpoint. Separate DASH audio/video AdaptationSets are stream-copy muxed into MP4 using an externally installed FFmpeg executable; set/verify its path in Settings → DASH video support (FFmpeg is not bundled).
- Dynamic/live MPDs and DRM/ContentProtection are intentionally not offered.
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

Run `scripts/package-extensions.ps1` in PowerShell, or `node scripts/package-extensions.mjs` on any platform. Outputs go to `dist/extensions/`:
- `ThunderLoad-Chromium.zip`
- `ThunderLoad-Firefox.zip`
- `ThunderLoad-Firefox.xpi` (byte-identical copy of the Firefox ZIP)

No `.crx` is produced: Chrome refuses to install a `.crx` from outside the Web Store, so the developer-mode path is ZIP + Load unpacked.

Developer-mode install (no store listing needed): in desktop ThunderLoad, open **Settings → Browser extension files** and download the package for your browser. Chromium (Chrome/Edge/Brave/Opera): extract the ZIP, open `chrome://extensions` (`edge://extensions`, `brave://extensions`, `opera://extensions`), turn Developer mode on, click **Load unpacked** and select the extracted folder. Firefox: open `about:addons`, click the gear icon, choose **Install Add-on From File** and select the `.xpi`.

Firefox release/beta requires Mozilla signing. Chrome Web Store and Edge Add-ons each have their own listing and review process.

## Optional native host

Direct file links use the browser downloads API and do not need a companion connection. HLS and DASH inspection/download use the local native host. DASH qualities with separate audio/video tracks require FFmpeg; the desktop Settings path is persisted locally and read by the native companion on Windows.

On Windows, install desktop ThunderLoad, install the browser extension, then open **Settings → Browser connection**. Choose Chrome, Edge, Brave or Firefox, paste the exact extension ID and select **Connect browser**. Restart the browser and grant Native messaging in the extension popup when prompted.

The extension ID is required because the browser native-messaging allowlist must be restricted to the exact installed extension. Firefox's packaged ID is `thunderload@example.com`. Chromium IDs are shown on the browser's extensions developer page and contain 32 characters from `a` through `p`.

## Video limits
HLS qualities are listed only when exposed by the site. Completed unencrypted HLS VOD playlists can be downloaded, including EXT-X-BYTERANGE slices, fMP4 (EXT-X-MAP init + fragments) and separate audio groups muxed into MP4 with FFmpeg; encrypted/DRM streams and live playlists are not supported. Static DASH VOD with SegmentTemplate/SegmentTimeline, SegmentList, SegmentBase and multi-period MPDs can be downloaded, with Pause/Resume/Cancel. Separate audio/video tracks need an external FFmpeg executable for stream-copy muxing. Dynamic/live MPDs and ContentProtection/DRM are not supported. Blob/MSE-only, cookie-bound and signed URLs may not be exposed or may fail. Parallel connections cannot guarantee a fixed speed multiplier; results depend on server support and bandwidth.

## Production work still required
- HLS live playlists are still not supported.
- Dynamic/live DASH MPDs are still not supported; DRM remains unsupported by design.
- Explicit, secure cookie/header handoff for user-authorized sessions.
- Validate Windows Task Scheduler wake tasks on clean Windows 10/11 VMs, including sleep/wake, missed start time, logged-out state, task cancellation and duplicate-instance races.
- Code signing, update strategy, uninstall cleanup, Windows VM testing and accessibility QA.
- Current-version Firefox/Chromium testing, AMO/Chrome/Edge store review; separate Safari wrapper if Safari is advertised.

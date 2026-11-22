# ThunderLoad 0.3.5 — Cross-platform desktop + browser extension

**Status: release candidate / not production-ready yet.** This repository contains packaging pipelines, but the tagged builds must pass CI on Windows, Linux and macOS and be tested on real machines/browsers before general release. Store approval is decided by each browser vendor.

## Current source features
- Desktop direct HTTP/HTTPS transfers use a persistent sidecar checkpoint (`.thunderload.part`). Pause flushes the checkpoint, Resume re-requests only the unfinished segments when the server supports byte ranges (falling back to the remaining byte range on the sequential path), and startup recovers direct jobs that were in progress when the app closed.
- Direct desktop transfers support Pause, Resume, Cancel and an aggregate speed cap shared across active direct, HLS and DASH transfers within each ThunderLoad process.
- The same saved speed cap is applied to direct segmented/streaming, HLS and DASH segment transfers in the desktop and browser-native processes independently.
- Queue entries and scheduled start times are recorded in a durable job database (`jobs.json` in the desktop app-data directory, written atomically), so the queue survives an app restart or crash: jobs that were downloading come back Queued and resume from their checkpoints, a job that closed before a resumable checkpoint was created comes back Interrupted, paused jobs stay paused, and scheduled entries keep their start times. Checkpoint files on disk with no database row are imported as Interrupted jobs on startup. Browser-started jobs are mirrored into the same database from their `.native-jobs` status files. Each scheduled job registers a one-time OS wake task that launches ThunderLoad when due — Task Scheduler (`WakeToRun` / `StartWhenAvailable`) on Windows, a systemd user timer (`WakeSystem=true`, `Persistent=true`) on Linux, a launchd agent (`StartCalendarInterval`) on macOS — and it is cancelled when the job starts or is removed. On Windows the user must be logged in, and actual wake behavior depends on Windows power settings; on Linux wake depends on systemd and firmware settings; on macOS launchd does not wake a sleeping Mac, so the launch happens only if the Mac is awake at that time (use `pmset schedule wake` with admin rights to wake it). Other platforms start scheduled entries while the app is running only.
- Desktop HLS downloads support completed VOD playlists, with Pause/Resume/Cancel backed by an on-disk segment checkpoint, EXT-X-BYTERANGE slices, fMP4 (EXT-X-MAP init + fragments), AES-128-encrypted segments decrypted in-engine, and separate audio renditions muxed into MP4 with an externally installed FFmpeg. Live HLS playlists are recorded as they arrive instead: stopping (Pause) keeps the partial recording, and a live recording cannot be resumed. SAMPLE-AES and other non-AES-128 encryption, encrypted live HLS, and DRM remain unsupported.
- Browser-started direct, HLS and DASH transfers write local status files into `Downloads/ThunderLoad/.native-jobs`. The desktop queue polls these files to show browser job progress and completion, and Pause/Resume/Cancel on a browser job are sent to the native host through a per-job control file, so the desktop controls browser-started jobs of every kind. A browser job whose companion has exited (Interrupted) can have its checkpoint handed to the desktop engine and resumed there.
- Browser-started downloads carry the browser session they need: the extension hands the download URL's cookies (that URL only, never a broad cookie read), the page referer and the browser user-agent to the native host, which validates them against the download URL and passes them to the engine. The session is applied to every request of the job — direct, HLS and DASH, including resume and retry requests — and is stored with the job's checkpoint/job record so a desktop resume of a browser-started job reuses it. Cookies are only ever sent to their own origin (and never follow a cross-origin redirect); the session is never logged and never leaves the machine. The extension now requests the `cookies` permission for this; updating the extension may prompt for the new permission once.
- The checkpointed desktop transfer path downloads in parallel segments (up to 8 connections by default) against one preallocated partial file, with per-segment completion persisted in the metadata sidecar — so pause, resume and restart recovery all work for multi-connection downloads. Servers without byte-range support fall back to the single-connection checkpointed path.
- Segment sizes are dynamic, not fixed: a fresh multi-connection download splits the file into near-equal chunks (at least 256 KiB each, at most 1024 chunks) and the connections pull chunks work-stealing style — a fast connection ends up with the larger share of the remaining work, and a connection that stops delivering data for 30 s has its chunk re-assigned to a fresh connection (at most 3 re-assignments per chunk, then the download errors out with the checkpoint preserved for Resume). Per-connection throughput is measured as a short-window moving average. Progress never goes backwards across a re-assignment. Old checkpoints (fixed equal segments) still resume exactly as before; this applies to the checkpointed desktop/native path only — the one-shot fast path keeps fixed segments.
- Every job is recorded in the durable job database (see the queue entry above): a restart restores the full queue with progress, and a corrupt database file is moved aside instead of crashing the app.
- Automatic scan of media URLs visible on every visited page. The extension watches for video elements and media traffic, and shows a hover-only in-player Download control over each video; the popup lists everything found on the active tab, and Alt+Shift+D still forces a re-scan.
- Site grabber: the extension popup lists video, audio, playlists and media links found on the active tab, each row with its own Download button — the popup has no checkboxes and no select-all. Multi-select checkboxes live only in the desktop app's Video grabber dialog ("Grab a page", first 100 items, all selected by default). The desktop grabber reads a pasted page URL's static HTML; pages that need a login or load media with JavaScript need the extension's live-page scan instead. `data:` and `blob:` URLs are listed nowhere — the engine cannot fetch them.
- YouTube and social video pages (YouTube, Instagram, TikTok, X, Facebook, Reddit, Twitch, Vimeo) are inspected and downloaded with yt-dlp; Deno runs the JavaScript yt-dlp needs for YouTube's full quality. Neither tool is bundled with the installer — each downloads once from its GitHub Releases page (yt-dlp 2026.10.26, Deno 2.9.8) when you click its Download button in Settings, with visible progress, and is SHA256-verified against a pinned hash before use (the yt-dlp binary's hash; the Deno release ZIP's hash, verified before extraction). Without Deno, YouTube falls back to fewer formats. yt-dlp downloads the video itself; its progress, pause and cancel work like any other job.
- A compact, hover-only in-player Download control for supported video elements; no banners. Automatic detection needs broad host access (`<all_urls>` + `webRequest`): the content script must run on every page and the background must observe media traffic to catch streams the DOM scan cannot see. The earlier minimal-permission stance is intentionally given up for this feature.
- HLS master-playlist quality discovery through the optional desktop native host.
- Static DASH VOD downloads for MPDs using SegmentTemplate/SegmentTimeline, SegmentList and SegmentBase (sidx index), including multi-period MPDs whose periods agree on codec and dimensions, inherited templates, BaseURL resolution and RepresentationID/Bandwidth/Number/Time substitutions.
- Desktop and browser-started DASH downloads support Pause/Resume/Cancel backed by an on-disk checkpoint: finished segments are detected from the segment files on disk, so a resume re-requests only the unfinished segments of each period, and Cancel removes the target, the work directory and the checkpoint. Separate DASH audio/video AdaptationSets are stream-copy muxed into MP4 using an externally installed FFmpeg executable; set/verify its path in Settings → DASH video support (FFmpeg is not bundled).
- Dynamic/live MPDs and DRM/ContentProtection are intentionally not offered.
- Minimal React + Tauri desktop workspace with Downloads, Queues, Scheduler and Settings pages, plus NSIS (Windows), deb/AppImage (Linux) and DMG (macOS) installer configurations. The Video Grabber is a dialog (reachable from the Queues page and the Add form), not a page.
- Native messaging is optional for single-file subtitle tracks and for the last-resort fallback below; ordinary direct browser downloads are intercepted and handed to the desktop engine, and only fall back to the browser's own downloads API when the engine is unreachable (or from the link context menu).

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

## Build the desktop app

Windows requirements: Windows 10/11 x64, Rust stable + Cargo, Node.js 20+, npm, Microsoft C++ Build Tools and WebView2. Linux requirements: Rust stable + Cargo, Node.js 20+, npm, and the Tauri system libraries (`libwebkit2gtk-4.1-dev build-essential curl wget file libxdo-dev libssl-dev libayatana-appindicator3-dev librsvg2-dev patchelf` on Debian/Ubuntu). macOS requirements: Rust stable + Cargo, Node.js 20+, npm, Xcode Command Line Tools.

From this directory, package the browser extensions **first** — `tauri.conf.json` bundles `dist/extensions/ThunderLoad-Chromium.zip` and `dist/extensions/ThunderLoad-Firefox.xpi` as resources, and the Tauri build script rejects the build when they are missing (this also applies to `cargo test` on the desktop crate, so the packaging step comes before any `cargo` step that touches it):

```sh
node scripts/package-extensions.mjs
cargo build --release -p thunderload-host
npm install --prefix desktop
npm run build --prefix desktop
npm run tauri --prefix desktop -- build
```

On Linux and macOS, copy the host binary to the resource name the Tauri config expects before building, otherwise the build script rejects the missing resource (the desktop copies it back out under its native name at registration time):

```sh
cp target/release/thunderload-host target/release/thunderload-host.exe
```

Outputs: NSIS setup `target/release/bundle/nsis/` (Windows), `.deb` / `.AppImage` under `target/release/bundle/deb/` and `target/release/bundle/appimage/` (Linux), `.dmg` under `target/release/bundle/dmg/` (macOS). GitHub Actions builds all three on their runners. The installers are unsigned until a trusted code-signing certificate is configured.

## Browser packages

Run `scripts/package-extensions.ps1` in PowerShell, or `node scripts/package-extensions.mjs` on any platform. Outputs go to `dist/extensions/`:
- `ThunderLoad-Chromium.zip`
- `ThunderLoad-Firefox.zip`
- `ThunderLoad-Firefox.xpi` (byte-identical copy of the Firefox ZIP)

No `.crx` is produced: Chrome refuses to install a `.crx` from outside the Web Store, so the developer-mode path is ZIP + Load unpacked.

Developer-mode install (no store listing needed): in desktop ThunderLoad, open **Settings → Browser extension files** and download the package for your browser. Chromium (Chrome/Edge/Brave/Opera): extract the ZIP, open `chrome://extensions` (`edge://extensions`, `brave://extensions`, `opera://extensions`), turn Developer mode on, click **Load unpacked** and select the extracted folder. Firefox: open `about:addons`, click the gear icon, choose **Install Add-on From File** and select the `.xpi`.

Firefox release/beta requires Mozilla signing. Chrome Web Store and Edge Add-ons each have their own listing and review process.

## Optional native host

Direct file links are intercepted and handed to the desktop engine; the browser downloads API is only the last-resort fallback when the engine is unreachable (or from the link context menu), and the path for single-file subtitle tracks, which do not need a companion connection. HLS and DASH inspection/download use the local native host. DASH qualities with separate audio/video tracks require FFmpeg; the desktop Settings path is persisted locally and read by the native companion on every platform.

Install desktop ThunderLoad (Windows NSIS setup, Linux `.deb`/AppImage, or macOS `.dmg`), install the browser extension, then open **Settings → Browser connection**. Choose Chrome, Chromium, Edge, Brave or Firefox, paste the exact extension ID and select **Connect browser**. Restart the browser and grant Native messaging in the extension popup when prompted. On Linux the browser manifest is written to the browser's `NativeMessagingHosts` directory under `~/.config` (Firefox: `~/.mozilla/native-messaging-hosts`); on macOS under `~/Library/Application Support`.

The extension ID is required because the browser native-messaging allowlist must be restricted to the exact installed extension. Firefox's packaged ID is `thunderload@example.com`. Chromium IDs are shown on the browser's extensions developer page and contain 32 characters from `a` through `p`.

## Video limits
HLS qualities are listed only when exposed by the site. Completed HLS VOD playlists can be downloaded, including EXT-X-BYTERANGE slices, fMP4 (EXT-X-MAP init + fragments), AES-128-encrypted segments (decrypted in-engine) and separate audio groups muxed into MP4 with FFmpeg; live HLS playlists are recorded as they arrive, and stopping keeps the partial recording. SAMPLE-AES and other non-AES-128 encryption, encrypted live HLS and DRM are not supported. Static DASH VOD with SegmentTemplate/SegmentTimeline, SegmentList, SegmentBase and multi-period MPDs can be downloaded, with Pause/Resume/Cancel. Separate audio/video tracks need an external FFmpeg executable for stream-copy muxing. Dynamic/live MPDs and ContentProtection/DRM are not supported. Blob/MSE-only URLs may not be exposed or may fail; cookie-bound and signed URLs work when the download is started from the browser (the session handoff above). Parallel connections cannot guarantee a fixed speed multiplier; results depend on server support and bandwidth.

## Benchmarks

Engine throughput is measured by a built-in bench binary against localhost fixture servers — see [`BENCHMARKS.md`](BENCHMARKS.md) for methodology and full results. Headline numbers (Linux VM, 4 CPUs, 2026-11-14): a 64 MiB direct download scales from 70 MiB/s at 1 connection to 311 MiB/s at 8 and 615 MiB/s at 16; the checkpointed 8-connection path is ~5.5x the one-shot sequential path (56.7 MiB/s); HLS/DASH media run ~170–180 MiB/s. These are engine-parallelism numbers under a per-chunk server-latency model, not real-internet throughput, and no ThunderLoad-vs-IDM comparison is claimed — that needs a real Windows machine and an IDM install.

## Production work still required
- Dynamic/live DASH MPDs are still not supported; DRM remains unsupported by design.
- Validate scheduled-wake tasks on clean machines: Windows Task Scheduler wake tasks on Windows 10/11 VMs (sleep/wake, missed start time, logged-out state, task cancellation, duplicate-instance races), systemd user timers on a Linux desktop, launchd agents on macOS.
- Code signing, update strategy, uninstall cleanup, Linux/macOS VM testing and accessibility QA.
- Current-version Firefox/Chromium testing, AMO/Chrome/Edge store review; separate Safari wrapper if Safari is advertised.
- The release workflow (`.github/workflows/release.yml`) now builds Windows, Linux, and macOS; no tagged release has run the new Linux/macOS jobs yet, so those assets are unproven until the first tag.

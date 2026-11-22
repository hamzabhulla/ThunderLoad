# ThunderLoad

[![CI](https://github.com/hamzabhulla/ThunderLoad/actions/workflows/ci.yml/badge.svg)](https://github.com/hamzabhulla/ThunderLoad/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![Version](https://img.shields.io/badge/version-0.3.5-blue)

**A cross-platform download manager that treats an interrupted download as a pause, not a failure — a Rust engine, a Tauri desktop app, and a browser extension that work as one queue.**

ThunderLoad is an IDM-style download manager for Windows, Linux, and macOS. It downloads files, HLS streams, and DASH videos with multi-connection checkpoint resume, a persistent job queue, and desktop controls for jobs started in the browser — so a closed laptop, a crashed app, or a dead connection costs you the remaining bytes, not the whole file.

> **Status:** v0.3.5, active development. CI is green on Windows, Linux, and macOS. 247 engine tests passing, 0 warnings (engine + host). See [Current limitations](#current-limitations) for what is not done yet — no code signing, no real-machine smoke test, no store listings.

## Quick start

Prerequisites: Rust stable + Cargo, Node.js 20+, npm. Windows also needs Microsoft C++ Build Tools and WebView2; Linux needs the Tauri system libraries (see `thunderload/README.md`); macOS needs Xcode Command Line Tools.

```sh
git clone https://github.com/hamzabhulla/ThunderLoad.git
cd ThunderLoad/thunderload
node scripts/package-extensions.mjs
cargo build --release -p thunderload-host
npm install --prefix desktop
npm run build --prefix desktop
npm run tauri --prefix desktop -- build
```

Package the browser extensions **first**, before any `cargo` or `tauri` step that touches the desktop crate: `tauri.conf.json` bundles `dist/extensions/ThunderLoad-Chromium.zip` and `dist/extensions/ThunderLoad-Firefox.xpi` as resources, and the Tauri build script rejects the build when they are missing. (On Windows, `.\scripts\package-extensions.ps1` does the same thing.)

On Linux and macOS, copy the host binary first, or the build rejects the missing resource:

```sh
cp target/release/thunderload-host target/release/thunderload-host.exe
```

Outputs: NSIS setup under `target/release/bundle/nsis/` (Windows), `.deb` / `.AppImage` under `target/release/bundle/deb/` and `target/release/bundle/appimage/` (Linux), `.dmg` under `target/release/bundle/dmg/` (macOS). GitHub Actions builds all three on pushes to main and on pull requests.

Verify the engine yourself:

```sh
cargo test -p thunderload-engine
cargo run --release -p thunderload-engine --bin bench
```

### One-time browser setup

Browser downloads need one manual setup per browser — after that, ThunderLoad re-registers the connection automatically on later startups if it ever goes missing:

1. Install the desktop app and the browser extension (in the app: Settings → Browser extension files).
2. Copy the extension ID: open `chrome://extensions` (`edge://extensions`, `brave://extensions`), turn Developer mode on — the ID is shown under the ThunderLoad extension. Firefox uses the fixed ID `thunderload@example.com`.
3. In the app, open Settings → Browser connection, paste the ID and click Connect browser, then restart the browser and allow Native messaging when prompted.
4. Done — direct file downloads never need this setup.

<!-- Screenshots: pending real screenshots from Hamza's machine (docs/screenshots/desktop-queue.png, extension-popup.png, demo.gif). No AI-generated imagery — add the section back once the files exist. -->

## Features

**Downloads that survive interruptions**
- Multi-connection checkpoint resume: up to 8 parallel connections against one preallocated file, per-segment completion persisted in a metadata sidecar. Pause, resume, and restart recovery all re-fetch only the unfinished bytes.
- Dynamic segmentation: fresh downloads split into near-equal chunks (≥256 KiB, ≤1024 chunks) and connections pull work-stealing style, so a fast connection takes the larger share. A connection silent for 30 s has its chunk re-assigned (max 3 re-assignments, then the download errors with its checkpoint preserved).
- A durable job database (`jobs.json`, atomic writes): the queue survives restarts and crashes. Interrupted jobs come back resumable, paused jobs stay paused, orphaned checkpoints on disk are imported as Interrupted jobs.
- Scheduled starts with a one-time OS wake task — Task Scheduler on Windows, systemd user timer on Linux, launchd agent on macOS.

**Video: HLS and DASH**
- HLS VOD with pause/resume/cancel, byte-range slices, fMP4 (init + fragments), AES-128-encrypted segments decrypted in-engine, and separate audio renditions muxed with FFmpeg.
- Live HLS streams can be recorded as they arrive — stopping keeps the partial recording (Pause is terminal for a live recording; there is no resume from a partial live file).
- DASH VOD with SegmentTemplate, SegmentList, SegmentBase (sidx), and multi-period MPDs; pause/resume/cancel backed by an on-disk checkpoint; separate audio/video muxed with FFmpeg.
- Dynamic/live DASH MPDs, SAMPLE-AES and other non-AES-128 HLS encryption, encrypted live HLS, and DRM are not supported — DRM never, by design.

**YouTube and social video (yt-dlp)**
- YouTube, Instagram, TikTok, X, Facebook, Reddit, Twitch, and Vimeo pages are inspected and downloaded with yt-dlp; Deno runs the JavaScript yt-dlp needs for YouTube's full quality. Neither tool is bundled with the installer — each downloads once from its GitHub Releases page (yt-dlp 2026.10.26, Deno 2.9.8) when you click its Download button in Settings, with visible progress, and is SHA256-verified against a pinned hash before use. Without Deno, YouTube falls back to fewer formats.

**Browser integration**
- Chrome/Firefox extension (Chromium ZIP + Firefox XPI) with automatic media detection, an in-player Download control, and a site grabber: the popup lists what it found on the active tab, each row with its own Download button. Multi-select checkboxes exist only in the desktop app's Video grabber dialog ("Grab a page", first 100 items).
- The desktop app controls browser-started jobs of every kind — pause, resume, and cancel from one queue — via the local native-messaging host.
- Cookie/header handoff: the extension passes the download URL's cookies (that URL only), referer, and user-agent to the engine, so cookie-bound and signed URLs work. Cookies are sent only to their own origin, never follow a cross-origin redirect, and are never logged.

**Cross-platform**
- Windows 10/11, Linux, macOS. Same engine, same queue, same controls everywhere.

## Benchmarks

Measured with the built-in bench binary (`cargo run --release -p thunderload-engine --bin bench`) against localhost fixture servers, Linux VM, 4 CPUs, 2026-11-14. Full methodology in [`thunderload/BENCHMARKS.md`](thunderload/BENCHMARKS.md).

| Bench | Throughput |
|---|---|
| Direct, 1 connection | 70 MiB/s |
| Direct, 2 connections | 126 MiB/s |
| Direct, 4 connections | 209 MiB/s |
| Direct, 8 connections | 311 MiB/s |
| Direct, 16 connections | 615 MiB/s |
| Direct, sequential (one-shot) | 57 MiB/s |
| HLS (32 × 1 MiB segments) | 175 MiB/s |
| DASH (32 × 1 MiB segments) | 173 MiB/s |

The checkpointed 8-connection path is ~5.5× the one-shot sequential path on the same 64 MiB file. These are engine-parallelism numbers under a per-chunk server-latency model — not real-internet throughput, and **no ThunderLoad-vs-IDM comparison has been run or is claimed**.

## Architecture

One Rust engine does all the transferring; thin clients drive it. The desktop app (Tauri 2 + React) owns the queue and the job database, the browser extension finds media and starts jobs, and a small Rust native-messaging host bridges the two on the user's machine. Checkpoints and job state live on disk, so nothing is lost when a process dies.

```
thunderload/
├── engine/      Rust transfer engine: HTTP ranges, HLS, DASH, job store, session handoff
├── desktop/     Tauri 2 + React client (Downloads, Queues, Scheduler, Settings pages; Video Grabber is a dialog, not a page)
├── extension/   Chromium + Firefox extension (media detection, grabber, popup)
├── native-host/ Rust native-messaging bridge between extension and desktop
└── docs/        Browser distribution, store submission, and privacy notes
```

## Contributing

Issues and pull requests are welcome. For anything large, open an issue first so the design can be agreed before code is written. A `CONTRIBUTING.md` with setup and review notes is on the way.

## Current limitations

Stated plainly, because a README that hides them is worse than useless:

- **No code signing yet.** Installers will trigger SmartScreen / Gatekeeper warnings until a certificate is configured.
- **No real-machine smoke test yet.** CI is green on all three platforms, but no one has run the built installers on a real Windows, Linux, or macOS machine.
- **Store listings not submitted.** Chrome Web Store, Edge Add-ons, and AMO review are all pending.
- **FFmpeg is required** for HLS/DASH audio muxing and is not bundled — set its path in Settings.
- **No live DASH or DRM content.** Live/dynamic DASH MPDs, SAMPLE-AES and other non-AES-128 HLS encryption, encrypted live HLS, and DRM are not supported, by design. Live HLS recording and AES-128 HLS VOD are supported (see Features).
- **Release packaging now covers Windows, Linux, and macOS.** Tagged releases build the Windows NSIS installer, Linux and macOS bundles, and extension packages — but no tagged release has run the new Linux/macOS jobs yet, so those assets are unproven until the first tag.

## License

Licensed under the MIT License — see [LICENSE](LICENSE).

## Links

- Repository: https://github.com/hamzabhulla/ThunderLoad
- Issues: https://github.com/hamzabhulla/ThunderLoad/issues

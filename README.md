# ThunderLoad

ThunderLoad is a Windows-first download manager project with a Rust transfer engine, a Tauri desktop client, and browser-specific extensions.

## Project layout
- thunderload/engine — Rust HTTP range downloader and HLS/DASH inspection.
- thunderload/desktop — Tauri + React desktop client and Windows NSIS configuration.
- thunderload/extension — Chromium and Firefox extension source.
- thunderload/native-host — optional local Native Messaging bridge.
- thunderload/docs — browser distribution, store submission and privacy notes.

## Status
This is an active release-candidate starter, not a production-ready IDM replacement. HLS is limited to exposed, unencrypted playlists. DASH quality detection exists, but DASH downloading/remuxing is not implemented. Pause/resume persistence, queue persistence, code signing and live browser testing remain release blockers.

## Build and release
See thunderload/README.md. GitHub Actions checks source on pushes and pull requests; tags beginning with v build a Windows NSIS setup executable and Chromium/Firefox extension packages.

## Browser distribution
- Chromium package: Chrome, Edge, Brave, Opera and Vivaldi starting point.
- Firefox: separate manifest build; Mozilla AMO signing is required for release/beta Firefox.
- Safari: separate macOS/Xcode wrapper not included.

## Privacy
See the draft privacy policy at thunderload/docs/PRIVACY.md. Before store submission, publish it at a public URL and replace its contact placeholder with a monitored support address.

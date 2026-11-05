# ThunderLoad

A Windows-first, lightweight download manager starter with a Rust download engine, Tauri desktop UI, and browser-extension integration.

## Starter source

The complete v0.1.0 source package is provided in [`thunderload/ThunderLoad_Starter_v0.1.0.zip`](thunderload/ThunderLoad_Starter_v0.1.0.zip). Extract it to get the `thunderload/` project directory.

## Current scope

- Rust HTTP downloader with parallel range requests where supported
- HLS playlist quality discovery and segment download support
- DASH representation/quality detection (DASH downloading is not yet implemented)
- Browser extension starter for Chromium-based browsers
- Tauri + React desktop UI starter

## Important

This is an early starter, not a production-ready IDM replacement. Real-world compatibility, Firefox packaging, pause/resume, DASH downloading/remuxing, and native-host integration still need implementation and end-to-end testing. It does not bypass DRM or access controls.

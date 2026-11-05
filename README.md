# ThunderLoad

A Windows-first, lightweight download manager starter with a Rust download engine, Tauri desktop UI, and browser-extension integration.

## Source code

The starter project lives in the thunderload/ directory.

- Project setup and build instructions: thunderload/README.md
- Rust download engine: thunderload/engine/
- Browser extension: thunderload/extension/
- Native messaging host: thunderload/native-host/
- Desktop application: thunderload/desktop/

## Current scope

- Rust HTTP downloader with parallel range requests where supported
- HLS playlist quality discovery and unencrypted segment download support
- DASH representation/quality detection (DASH downloading is not yet implemented)
- User-triggered browser media scan without page overlays or playback interruptions
- Tauri + React desktop UI starter

## Important

This is an early starter, not a production-ready IDM replacement. Real-world compatibility, Firefox packaging, pause/resume, DASH downloading/remuxing, and full native-host/desktop queue integration still need implementation and end-to-end testing. It does not bypass DRM or access controls. Parallel downloads cannot guarantee a fixed speed multiplier; results depend on server support and available bandwidth.

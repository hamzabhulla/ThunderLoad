# Changelog

All notable changes to ThunderLoad are recorded here. Dates are in
`YYYY-MM-DD`.

## Unreleased

### Fixed

- Startup split-brain race (PR #32, 2026-11-21): the queue dispatcher is
  now gated on a `seeded` flag so it cannot auto-start jobs before the
  durable job database has been merged; queue history is flushed on
  page hide / before unload; rows skipped during the merge are no longer
  echoed back into `jobs.json`.

## 0.3.5 — 2026-11-20

Test release, not production-ready. Published for testing on Windows;
installers are unsigned, no real-machine smoke test has been run, and
release packaging is Windows-only.

### Added

- Real HLS/DASH assembly with parallel segments, AES-128 decryption, and
  signed-URL refresh (PR #20).
- YouTube and social video downloads routed through yt-dlp, with yt-dlp
  and Deno downloaded on demand and SHA256-verified (PR #18).
- GenericIE fallback, clipboard LinkGrabber, interception queue, Firefox
  blocking, live HLS recording, and subtitles (Slice 6, PR #22).
- Authenticated session forwarding from the extension to the desktop
  engine (PR #21).
- Extension MAIN-world MSE/fetch hook and webRequest sniffer hardening
  (PR #17).
- In-player Download pill with auto-hide, "Other media" rows, and
  in-page permission request (PR #16).
- Extension detection persistence, including the `storage` permission
  (PR #15).

### Fixed

- Engine edge cases: rate-limiter burst, ISO-8601 duration days,
  quote-aware attribute parsing, orphan import skip, DASH temp sweep
  (Slice 7a, PR #23).
- Desktop grabber multi-select, connections wired end-to-end, native
  speed/ETA, and live segments (Slice 7b, PR #24).
- Cross-check minors: Firefox manifest, docs, and extension fixes
  (Slice 7c, PR #25).
- Mega-audit fixes: 9 P0, 25 P1, and P2/P3 items covering SSRF hardening,
  coordinator deadlock, chunked 206 responses, DASH/HLS, extension,
  native host, and docs (PR #28).
- Desktop `download_url_resumable_with_connections` argument order
  (PR #26).
- `check_ytdlp_update` name collision breaking CI (PR #19).
- Windows native-host test deriving its home directory from
  `default_download_dir` (PR #27).
- Windows CI timing flake in the uneven-segmentation drip test
  (PRs #29 and #30).

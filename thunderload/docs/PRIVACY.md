# ThunderLoad Privacy Policy

**Last updated:** 19 November 2026

ThunderLoad processes all browsing, media detection, session handoff, and download data **100% locally on your device**. ThunderLoad operates no remote backend, telemetry, analytics, or advertising servers.

## Browser Extension

### Permissions & What They Access
- **Host access (`<all_urls>`) & automatic scripts (`content.js`, `hook.js`):** To provide automatic IDM-style media detection without requiring manual per-site activation, the extension injects `content.js` (isolated world, `document_idle`) and `hook.js` (`MAIN` world, `document_start`) across frames to observe video/audio elements, MediaSource/fetch/XHR media URLs, and resource timing entries. Observation is read-only and feeds your local download list only.
- **Network observation (`webRequest`, and `webRequestBlocking` on Firefox):** The background worker observes media network requests to detect streams (HLS, DASH, direct media) that DOM scanning alone cannot see. On Firefox only, `webRequestBlocking` is used to optionally intercept direct-media navigations and hand them to the local desktop engine. Nothing is redirected to external servers.
- **Local storage (`storage` / `chrome.storage.local`):** Used strictly on-device to persist per-tab detected media state across service-worker restarts, your extension preferences (such as the download-interception toggle), and the local pending-intercept retry queue (`thunderload.interceptQueue.v1`). Per-tab entries are cleared when tabs close.
- **Cookies (`cookies`):** Used exclusively when you initiate a media inspection or download that requires your browser's authenticated session. Cookies matching the target media origin are read locally and handed off solely to your locally installed ThunderLoad native host over the browser's native messaging pipe so authenticated media can be fetched on your behalf. Cookies are origin-validated, never logged, and never transmitted to any third-party or ThunderLoad server.
- **Downloads (`downloads`):** Used for direct browser-handled downloads (such as single-file subtitle tracks) and optional browser-download interception.
- **Optional Native Messaging (`nativeMessaging`):** Requested on demand to communicate with the locally installed ThunderLoad native host (`com.thunderload.host`, binary `thunderload-host` / `thunderload-host.exe`) for media inspection, HLS/DASH/yt-dlp transfers, and desktop queue handoff. The desktop app or the extension can also download the yt-dlp and Deno executables on demand from GitHub Releases (yt-dlp 2026.10.26, Deno 2.9.8) — only when you click the Download button for that tool. A first install is SHA256-verified against a pinned hash before it is used. Updating yt-dlp downloads and installs the latest release unpinned, via `yt-dlp -U`, after asking for confirmation; the new binary's own hash is then recorded and used as tamper-evidence on later launches, not as a pinned-release guarantee.

### Local Native-Host & Job State
- Browser-started native-host transfers write per-job status files under `Downloads/ThunderLoad/.native-jobs/<id>.json` so the local desktop application can synchronize its queue display.
- On Unix systems, job state files are created with owner-only (`0o600`) permissions.
- Any temporary session/cookie data stored for an active resumable transfer is kept in the job state file while the job is `Interrupted`, `Failed`, or `Paused`, so it can still be resumed or retried, and is **automatically scrubbed** from the job state file only when the job reaches `Completed` or `Cancelled`, or when the job is successfully handed over to the desktop engine (the native state file is then deleted and the session lives only in the desktop job database).
- Local desktop settings (`ffmpeg.path`, `speed-limit.bytes`) are stored in the desktop application's local configuration directory and read locally by the native host.
- Scheduled downloads on Windows create a per-job Task Scheduler entry (`ThunderLoad-<job-id>`) and a small local launcher script in the application configuration directory, both removed when the job starts or is cancelled.

## Desktop Application
The desktop application downloads URLs you submit and saves files to your local ThunderLoad download folder. Resumable HTTP jobs store partial checkpoints beside the destination using a `.thunderload.part` suffix; checkpoints are deleted on completion or cancellation and retained on pause or network interruption to support resume. Queue history is stored locally on your device. No account, cloud sync, or telemetry is included.

## Third-Party Websites
When you inspect or download a URL, your browser or the local ThunderLoad application makes ordinary network requests directly to the destination website hosting that media. That website's own terms and privacy policy govern those requests.

## Data Retention
- Per-tab extension detections are removed from `chrome.storage.local` when the tab is closed.
- Temporary HLS/DASH segment work directories are removed after assembly or cancellation.
- Session credentials in `.native-jobs/<id>.json` are scrubbed only when the job reaches `Completed` or `Cancelled`, or when the job is handed over to the desktop engine.
- Native-host job status records remain in `Downloads/ThunderLoad/.native-jobs` until dismissed from the desktop queue or deleted by you.
- Downloaded files remain on your device until you delete them.

## Contact & Support
For privacy questions, support requests, or data-handling inquiries, open an issue or review the project documentation in the official ThunderLoad repository.

# ThunderLoad privacy policy

**Last updated:** 5 November 2026

ThunderLoad processes download and media information locally on the user's device.

## Browser extension
- The extension scans the active tab only after the user opens its popup or invokes the scan shortcut.
- It reads media URLs visible in video/source elements and current-page resource timing entries.
- Results remain in extension memory for the active tab and are not sent to a ThunderLoad server.
- There is no analytics, advertising, account, telemetry or remote-code service in this release.
- Direct downloads use the browser's built-in downloads API.
- If the user grants optional native-messaging permission, selected URLs and the referring page URL are passed to the locally installed native host for inspection/download. They are processed on-device and are not uploaded to a ThunderLoad-operated server.
- Browser-started native-host transfers write local per-job status files under `Downloads/ThunderLoad/.native-jobs`. The desktop application reads these files to synchronize its queue display. These records include the media URL, filename, transfer status, byte counts, error (when present), DASH representation IDs (for DASH jobs) and final saved path. They remain on the local device until dismissed from the desktop queue or deleted by the user.
- The optional FFmpeg executable path is stored in the desktop application's local config directory as `ffmpeg.path`; the speed cap is stored as `speed-limit.bytes`. The browser native host reads both local settings on Windows and refreshes them while connected.
- Scheduled downloads create a per-job Windows Task Scheduler entry named `ThunderLoad-<job-id>` and a small PowerShell launcher script under the same local application config directory. The task is removed when the queued job starts or is cancelled/removed. The script only launches the installed ThunderLoad executable and does not upload job data.

## Desktop application
The desktop application downloads URLs submitted by the user and saves files in the local ThunderLoad download folder. Direct desktop HTTP jobs keep partial checkpoints beside the selected destination using a `.thunderload.part` suffix; these files are removed after completion or cancellation and retained after Pause/network interruption for Resume. Queue history is stored locally in the desktop WebView's local storage. Account syncing and telemetry are not included.

## Third parties
When a URL is downloaded, the destination website receives ordinary network requests from the browser or local application. That site's own terms and privacy policy apply.

## Retention
Temporary HLS segment files are removed after successful assembly. Native-host job status records remain in `Downloads/ThunderLoad/.native-jobs` until dismissed. Partial direct-download checkpoints remain after Pause or network interruption and are removed on Cancel or successful completion. Downloaded files remain on the user's device until deleted.

## Contact
Before public store submission, replace this section with a monitored publisher support email and public support URL. Do not submit placeholder support details.

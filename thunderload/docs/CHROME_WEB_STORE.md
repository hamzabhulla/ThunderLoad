# Chrome Web Store submission notes

This is preparation guidance, not a claim of store approval.

## Package
1. Run the extension packaging script.
2. Upload `dist/extensions/ThunderLoad-Chromium.zip` in the Chrome Web Store Developer Dashboard.
3. Keep all executable JavaScript inside the submitted package. Do not load executable code from remote servers.
4. Use the packaged icons. Add genuine product screenshots and a 440×280 promotional tile in the dashboard.

## Listing copy

**Name:** ThunderLoad Download Manager

**Short description:** A quiet, lightweight download companion with automatic media detection and optional desktop acceleration.

**Detailed description:**

ThunderLoad hands direct links to the optional ThunderLoad desktop engine for multi-connection downloads, supported HLS and DASH media, live HLS recording, and yt-dlp downloads from video pages. When the desktop engine is unreachable, an intercepted direct link falls back to your browser's own download manager as a last resort.

- Start a download from a link's right-click menu.
- Media on the active tab is detected automatically — the extension watches video elements and media traffic on every page and shows a compact in-player Download action while hovering a supported video. No banners or automatic popups.
- View HLS qualities when the page exposes a readable HLS master playlist and the optional desktop companion is installed. Static DASH videos download with pause/resume/cancel through the same companion.
- Single-file subtitle tracks download through the browser's own download manager without the desktop app. Ordinary direct links go to the desktop engine; the browser's download manager is only a last-resort fallback when the engine is unreachable.

Limits: transfer speed depends on the server and network; DRM-protected or access-controlled streams are not decrypted or bypassed; adaptive HLS inspection/download and DASH downloading need the desktop native host.

## Permission justifications

The extension requests broad access because automatic, IDM-style media detection cannot work per-site or on demand only — it has to observe every page and the media traffic on it. The actual manifest permissions are:
- `<all_urls>` (host permission): content scripts must run on every page, and the background must observe media requests on every site, to catch streams the DOM scan alone cannot see.
- `webRequest` (plus `webRequestBlocking` on Firefox only): observe media network requests (HLS, DASH, direct media) and, on Firefox only, optionally intercept a direct-media navigation and hand it to the local desktop engine. Nothing is redirected to an external server.
- `activeTab` + `scripting`: the popup's Rescan action (Alt+Shift+D) and user-invoked scans on the active tab.
- `downloads`: start direct downloads through Chrome's downloads manager (e.g. single-file subtitle tracks).
- `contextMenus`: add the user-invoked "Download link with ThunderLoad" menu item.
- `cookies`: read only the cookies matching the URL being inspected or downloaded, and hand them solely to the locally installed native host over the native-messaging pipe so authenticated media can be fetched. Cookies are origin-validated, never logged, and never sent to any third party.
- `storage`: persist per-tab detected media, extension preferences, and the pending-intercept retry queue locally in `chrome.storage.local` across service-worker restarts.
- Optional `nativeMessaging`: connect to the locally installed desktop companion only when the user chooses adaptive media inspection or accelerated downloading. The content scripts themselves are `content.js` (isolated world, `document_idle`) and `hook.js` (`MAIN` world, `document_start`), both injected on `<all_urls>` in all frames.

No browsing history is sent to a ThunderLoad server. There is no analytics. The extension loads no remote code — all of its JavaScript ships inside the submitted package. (Separately, the desktop app can download the yt-dlp and Deno executables on demand from GitHub Releases, only when the user clicks Download for that tool in Settings, SHA256-verified against a pinned hash; disclose that in the dashboard if the listing mentions YouTube/social downloads.)

## Before submission
Publish a public privacy-policy URL and make dashboard data disclosures match the actual code. Add real screenshots, support contact details and a public support page. Test installation, context menu, popup scan, permission prompt, native host, HLS quality inspection, error paths and uninstall. Google decides approval after review.

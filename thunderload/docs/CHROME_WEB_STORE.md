# Chrome Web Store submission notes

This is preparation guidance, not a claim of store approval.

## Package
1. Run the extension packaging script.
2. Upload `dist/extensions/ThunderLoad-Chromium.zip` in the Chrome Web Store Developer Dashboard.
3. Keep all executable JavaScript inside the submitted package. Do not load executable code from remote servers.
4. Use the packaged icons. Add genuine product screenshots and a 440×280 promotional tile in the dashboard.

## Listing copy

**Name:** ThunderLoad Download Manager

**Short description:** A quiet, lightweight download companion with on-demand media detection and optional desktop acceleration.

**Detailed description:**

ThunderLoad lets you save direct links using your browser's own download manager and optionally connect to the ThunderLoad desktop engine for multi-connection downloads and supported HLS media.

- Start a download from a link's right-click menu.
- Scan the active tab for media sources only when you open the extension or use its shortcut.
- View HLS qualities when the page exposes a readable HLS master playlist and the optional desktop companion is installed.
- Keep video playback clean: no injected buttons, overlays, banners or automatic popups.
- Use browser-native downloading without installing the desktop app for ordinary direct links.

Limits: transfer speed depends on the server and network; DASH quality detection is available but DASH downloading/remux is not implemented; DRM-protected or access-controlled streams are not decrypted or bypassed; adaptive HLS inspection/download needs the desktop native host.

## Permission justifications
- `activeTab` + `scripting`: inject a short-lived scanner only after the user opens the extension or invokes the shortcut; no persistent all-site access.
- `downloads`: start direct downloads through Chrome's downloads manager.
- `contextMenus`: add the user-invoked “Download link with ThunderLoad” menu item.
- Optional `nativeMessaging`: connect to the locally installed desktop companion only when the user chooses adaptive media inspection or accelerated downloading.

No browsing history is sent to a ThunderLoad server. There is no analytics or remote executable code.

## Before submission
Publish a public privacy-policy URL and make dashboard data disclosures match the actual code. Add real screenshots, support contact details and a public support page. Test installation, context menu, popup scan, permission prompt, native host, HLS quality inspection, error paths and uninstall. Google decides approval after review.

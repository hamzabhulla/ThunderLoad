# Browser support and distribution

ThunderLoad ships browser-specific packages, not one universal extension file.

## Chromium family
The Chromium Manifest V3 package is the starting point for Chrome, Microsoft Edge, Brave, Opera and Vivaldi. Upload it to each applicable browser's store or unpack it for manual testing. Each store may require its own listing, screenshots, privacy disclosures, review and signing. Test direct downloads, media detection and native messaging in each browser before advertising support.

## Firefox
Firefox uses the separate Manifest V3 manifest (`manifest.firefox.json` becomes `manifest.json` when packaged). Submit the Firefox ZIP to addons.mozilla.org (AMO). Release and Beta Firefox require Mozilla-signed add-ons; Chrome Web Store approval does not approve the Firefox add-on.

## Safari
Safari requires a WebExtension wrapped as a macOS app with Xcode tooling, followed by Apple signing/notarization and distribution. The macOS wrapper is not included in this source release. Do not advertise Safari as available until it is built and tested on macOS.

## Native messaging
Native messaging is optional. Direct links use the browser's downloads manager without a companion app. Adaptive HLS and static DASH inspection/download require the ThunderLoad local native host. Static DASH with separate audio/video also requires FFmpeg; install FFmpeg and verify its executable path in desktop Settings → DASH video support. The native host reads the saved FFmpeg path and speed cap from the local desktop configuration and refreshes them while connected.

On Windows, install the desktop app, then open **Settings → Browser connection**. Select Chrome, Edge, Brave or Firefox, enter the exact extension ID, and choose **Connect browser**. Restart that browser and grant Native messaging when the extension asks. Chromium IDs must be 32 characters in the range a–p. The Firefox add-on ID in this package is `thunderload@example.com`.

The desktop app copies the native host into the user's local application data directory, writes a per-browser manifest, and registers a per-user Windows registry entry. The manifest allowlists only the supplied extension ID.

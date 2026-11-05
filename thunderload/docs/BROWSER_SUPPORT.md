# Browser support and distribution

ThunderLoad ships browser-specific packages, not one universal extension file.

## Chromium family
The Chromium Manifest V3 package is the starting point for Chrome, Microsoft Edge, Brave, Opera and Vivaldi. Upload it to each applicable browser's store or unpack it for manual testing. Each store may require its own listing, screenshots, privacy disclosures, review and signing. Test direct downloads, media detection and native messaging in each browser before advertising support.

## Firefox
Firefox uses the separate Manifest V3 manifest (`manifest.firefox.json` becomes `manifest.json` when packaged). Submit the Firefox ZIP to addons.mozilla.org (AMO). Release and Beta Firefox require Mozilla-signed add-ons; Chrome Web Store approval does not approve the Firefox add-on.

## Safari
Safari requires a WebExtension wrapped as a macOS app with Xcode tooling, followed by Apple signing/notarization and distribution. The macOS wrapper is not included in this source release. Do not advertise Safari as available until it is built and tested on macOS.

## Native messaging
Native messaging is optional. Direct links use the browser's downloads manager without a companion app. Adaptive HLS inspection/download requires the ThunderLoad local native host. The native host's allowlist must contain the exact ID issued to each browser/store install.

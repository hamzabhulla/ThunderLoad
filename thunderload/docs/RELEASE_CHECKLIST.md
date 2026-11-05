# Production release checklist

Do not label a build production-ready until every applicable item is complete.

## Windows desktop
- [ ] Windows x64 release workflow passes.
- [ ] NSIS installer installs, launches, upgrades and uninstalls on clean Windows 10/11 VMs.
- [ ] Native host is included and registration tested in Chrome, Edge, Brave, Opera, Vivaldi and Firefox.
- [ ] Installer/native host are signed with a trusted code-signing certificate.
- [ ] WebView2 bootstrapper tested online/offline.
- [ ] Uninstaller safely removes registry entries and local host files.
- [ ] Queue persistence, crash recovery, pause/resume and cancel implemented and tested.
- [ ] Update channel, rollback, checksums and release provenance documented.

## Browser extension
- [ ] Chrome Web Store package validated and reviewed.
- [ ] Edge Add-ons listing and package reviewed.
- [ ] Brave/Opera/Vivaldi current stable builds tested.
- [ ] Firefox passes AMO validation, signing and review.
- [ ] Safari wrapper built and signed on macOS if advertised.
- [ ] Permissions and store privacy disclosures match the implementation.
- [ ] Store-issued IDs are used in native host allowlists.
- [ ] Popup, context menu, downloads, scanner, no-overlay behavior and shortcut tested.
- [ ] Permission denial and no-desktop-app paths tested.

## Download engine
- [ ] Range requests validated against test servers and malformed Content-Range rejected.
- [ ] Retry, timeout, cancel, partial cleanup and disk-full behavior tested.
- [ ] HLS byte ranges, discontinuities, live playlists, fMP4, encryption rejection and quality selection tested.
- [ ] DASH templates/lists, audio/video tracks, timelines and remux completed before claiming DASH downloads.
- [ ] Cookie/custom-header handoff is explicit and local.
- [ ] No DRM decryption, paywall bypass or unauthorized access.

## Store submission
- [ ] Genuine screenshots, icons, description, support contact, public privacy URL and reviewer notes ready.
- [ ] Chrome Web Store and AMO submissions approved.
- [ ] Store URLs and support process published.

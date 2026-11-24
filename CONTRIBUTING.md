# Contributing to ThunderLoad

Thanks for wanting to help. ThunderLoad is a cross-platform download manager:
a Rust transfer engine, a Tauri 2 + React desktop app, and a Chromium/Firefox
browser extension, all in this repo under `thunderload/`.

## Setup

You need:

- **Rust stable + Cargo** — install via [rustup](https://rustup.rs/).
- **Node.js 20+ and npm** — for the desktop frontend, the extension
  packaging scripts, and the extension validator.
- **Windows:** Microsoft C++ Build Tools and WebView2.
- **Linux:** the Tauri system libraries (`libwebkit2gtk-4.1-dev
  build-essential curl wget file libxdo-dev libssl-dev
  libayatana-appindicator3-dev librsvg2-dev patchelf` on Debian/Ubuntu).
- **macOS:** Xcode Command Line Tools.

All commands below run from the `thunderload/` directory unless stated
otherwise.

## Layout

```
thunderload/
├── engine/      Rust transfer engine: HTTP ranges, HLS, DASH, job store, session handoff
├── desktop/     Tauri 2 + React client (Downloads, Queues, Scheduler, Settings)
├── extension/   Chromium + Firefox extension (media detection, grabber, popup)
├── native-host/ Rust native-messaging bridge between extension and desktop
├── scripts/     Extension packaging and validation
└── docs/        Browser distribution, store submission, and privacy notes
```

## Building

Package the browser extensions **first** — `tauri.conf.json` bundles
`dist/extensions/ThunderLoad-Chromium.zip` and
`dist/extensions/ThunderLoad-Firefox.xpi` as resources, and the Tauri build
script rejects the build when they are missing (this also applies to
`cargo test` on the desktop crate):

```sh
node scripts/package-extensions.mjs
cargo build --release -p thunderload-host
npm install --prefix desktop
npm run build --prefix desktop
npm run tauri --prefix desktop -- build
```

On Linux and macOS, copy the host binary to the resource name the Tauri
config expects before building:

```sh
cp target/release/thunderload-host target/release/thunderload-host.exe
```

Outputs: NSIS setup under `target/release/bundle/nsis/` (Windows),
`.deb` / `.AppImage` under `target/release/bundle/deb/` and
`target/release/bundle/appimage/` (Linux), `.dmg` under
`target/release/bundle/dmg/` (macOS).

## Testing

Run these before opening a pull request:

```sh
cargo check --workspace
cargo test --workspace
node scripts/validate-extension.mjs
npm run build --prefix desktop   # runs tsc && vite build
```

On a memory-constrained machine, run the engine tests single-threaded:

```sh
CARGO_BUILD_JOBS=1 cargo test --workspace -- --test-threads=1
```

GitHub Actions runs the same checks on Windows, Linux, and macOS; CI must
be green on all three platforms.

## Pull requests

- **For anything large, open an issue first** so the design can be agreed
  before code is written.
- Keep pull requests small and focused on one change.
- Describe what changed and how you tested it (see
  `.github/pull_request_template.md`).
- Update the docs (`thunderload/README.md`, `thunderload/docs/`, or
  `CHANGELOG.md`) when behaviour changes.

## Review rule: independent audit before delivery

Every change gets an independent audit before it is delivered: the original
issue is re-tested, the legitimate flows around it are regression-checked,
neighbouring code is checked for the same mistake, and the full build is
run. A change advances only on a pass. Do not merge your own work without
that check, and do not skip it because a fix looks small.

## Things that are still true (do not paper over them)

- **No code signing yet.** Installers trigger SmartScreen / Gatekeeper
  warnings until a certificate is configured.
- **No real-machine smoke test yet.** CI is green on all three platforms,
  but the built installers have not been run on a real Windows, Linux, or
  macOS machine. If you run one on real hardware, report the result —
  including failures — in the pull request or issue.
- **Store listings not submitted.** Chrome Web Store, Edge Add-ons, and AMO
  review are all pending.
- **Release packaging is Windows-only for now.** Tagged releases build the
  Windows NSIS installer and extension packages; Linux/macOS release
  packaging is a follow-up.
- **FFmpeg is required** for HLS/DASH audio muxing and is not bundled — set
  its path in Settings.

## Do not change

- **The Firefox add-on ID `thunderload@example.com`** in
  `extension/manifest.firefox.json` is the project's stable identity.
  Changing it breaks native-host matching for existing installs.

## Security issues

Report security issues privately through a GitHub Security Advisory in this
repository; do not publish exploitable details in public issues. See
`thunderload/SECURITY.md`.

## License

ThunderLoad is MIT licensed — see [LICENSE](LICENSE).

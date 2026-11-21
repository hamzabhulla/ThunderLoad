## What changed

Describe the change in a few sentences. Link the issue it fixes, if there
is one.

## How tested

- [ ] `cargo check --workspace`
- [ ] `cargo test --workspace` (or `CARGO_BUILD_JOBS=1 cargo test --workspace -- --test-threads=1` on a memory-constrained machine)
- [ ] `node thunderload/scripts/validate-extension.mjs`
- [ ] `npm run build --prefix thunderload/desktop`
- [ ] Ran the built installer on a real machine (state which OS — CI green is not a real-machine test)

## Checklist

- [ ] CI is green on Windows, Linux, and macOS.
- [ ] An independent audit was done before delivery: the original issue
      was re-tested, legitimate flows around it were regression-checked,
      neighbouring code was checked for the same mistake, and the full
      build was run. (See `CONTRIBUTING.md` — a change advances only on a
      pass.)
- [ ] Docs updated where behaviour changed (`thunderload/README.md`,
      `thunderload/docs/`, or `CHANGELOG.md`).
- [ ] No unrelated changes, no lockfiles, no workflow changes.
- [ ] The Firefox add-on ID `thunderload@example.com` was not changed.

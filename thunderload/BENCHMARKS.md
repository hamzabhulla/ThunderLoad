# ThunderLoad engine benchmarks

Reproducible throughput numbers for the download engine, measured
against localhost fixture servers. These are **engine numbers, not
real-world internet numbers** — read the methodology before quoting
them.

## How to run

```sh
cargo run --release -p thunderload-engine --bin bench [--runs N] [--bench NAME] [--list]
```

The suite lives in `engine/src/bin/bench.rs` and uses no extra
dependencies: the servers are plain `std::net::TcpListener` threads in
the style of the engine's own test fixtures. It is a binary, not part
of `cargo test` — the full suite takes ~20 s in release and is not run
by the test suite. Every download is verified byte-for-byte after each
run; a fast but wrong download fails loudly instead of reporting a
throughput number.

## Methodology

- **Servers:** one static-route HTTP/1.1 server per bench on
  `127.0.0.1`, keep-alive, HEAD + byte-range (`206`) support, ETag /
  `Last-Modified` validators so the resumable path accepts
  checkpoints.
- **Trickle model:** the server writes every body in 64 KiB chunks
  with a 1 ms sleep *between* chunks — after every chunk except the
  last of a response, never after the last one. That models a server
  that drips data with per-chunk latency, which is what makes parallel
  connections pay off; on raw loopback without the trickle the files
  would transfer in milliseconds and the numbers would be noise. The
  measured quantity is therefore **engine parallelism under a latency
  model**, not internet throughput. The checkpointed path fetches each
  256 KiB engine segment with its own Range request, so a 1-connection
  64 MiB download is 256 responses of 4 trickle chunks each: 256 × 3 =
  768 sleeps, a ≈768 ms floor on `direct/1-conn` (measured 911.2 ms) —
  not the 1024 ms a sleep-after-every-chunk reading would imply.
- **Runs:** each bench runs 3 times by default; the median is
  reported (min/max shown by the binary).
- **Files:** direct benches use a 64 MiB file; HLS/DASH use 32 x 1 MiB
  segments (32 MiB of media; DASH adds a 1 KiB init segment).
- **Resume bench:** pauses a 256 MiB download at ~50% and times the
  resume call to completion. It uses a **4 ms trickle** (not 1 ms) and
  a bigger file because progress is reported at most every 180 ms, so
  pausing a ~200 ms download at 50% is a race the download usually
  wins. Its MiB/s is therefore **not comparable** to the direct
  benches — what it proves is that a resume re-fetches only the
  unfinished bytes (the checkpoint's done-set) and still completes
  byte-identically.

## Results

Measured 2026-11-14 15:36 UTC on a Linux VM (4 CPUs) with
`rustc`/`cargo` 1.99.1, from `thunderload/`:

```sh
cargo run --release -p thunderload-engine --bin bench -- --runs 3
```

| Bench | Bytes | Median | Min | Max | Throughput |
|---|---|---|---|---|---|
| direct/1-conn | 64 MiB | 911.2 ms | 910.8 ms | 1.013 s | 70.2 MiB/s |
| direct/2-conn | 64 MiB | 506.9 ms | 506.7 ms | 507.6 ms | 126.3 MiB/s |
| direct/4-conn | 64 MiB | 305.7 ms | 304.8 ms | 305.9 ms | 209.3 MiB/s |
| direct/8-conn | 64 MiB | 205.8 ms | 204.9 ms | 205.9 ms | 310.9 MiB/s |
| direct/16-conn | 64 MiB | 104.1 ms | 103.5 ms | 105.1 ms | 614.8 MiB/s |
| direct/sequential | 64 MiB | 1.128 s | 1.122 s | 1.142 s | 56.7 MiB/s |
| hls | 32 MiB | 183.4 ms | 178.2 ms | 185.8 ms | 174.5 MiB/s |
| dash | 32 MiB | 184.6 ms | 178.6 ms | 207.5 ms | 173.4 MiB/s |
| direct/resume | ~114 MiB remaining | 811.9 ms | 810.2 ms | 813.5 ms | 140.4 MiB/s |

Three full runs of the suite agreed within ~5% on every bench
except `direct/1-conn`, whose median moved between 911 ms and
1.013 s across runs (the noisiest bench; single-connection runs are
most exposed to scheduling jitter on this 4-CPU VM). `direct/16-conn`
showed one slow run (204 ms vs a ~104 ms median) in an earlier run.

What the numbers show, under this model:

- Throughput scales near-linearly with connection count up to 8
  (70 → 126 → 209 → 311 MiB/s), and 16 connections roughly doubles
  8 again (615 MiB/s). That is the dynamic-segmentation path doing
  its job: more workers pull more chunks in parallel.
- The checkpointed 8-connection path is ~5.5x the one-shot
  sequential path on the same file (311 vs 56.7 MiB/s).
- HLS and DASH media throughput is ~170–180 MiB/s for 32 MiB of
  segments through the resumable paths.
- Resume re-fetches only the unfinished bytes and completes
  byte-identically; the ~114 MiB remainder of a paused 256 MiB
  download resumed in ~811 ms at its (slower-trickle) pace.

## What is NOT measured

- **No ThunderLoad-vs-IDM comparison.** That needs a real Windows
  machine and an IDM install; it is not doable from this VM and no
  such claim is made anywhere in this document.
- **No real-internet numbers.** Everything above is loopback against
  a fixture server with an artificial per-chunk delay. A real CDN,
  a congested link, or a server that throttles per-connection will
  produce different numbers — usually lower, and with different
  scaling.
- **No Windows/macOS numbers.** The suite runs on Linux here; the
  engine code is the same, but OS scheduling and loopback behaviour
  differ.

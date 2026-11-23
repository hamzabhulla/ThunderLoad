//! Engine benchmarks: throughput of the ThunderLoad download engine
//! against local fixture servers on 127.0.0.1.
//!
//! Run it with:
//!
//! ```sh
//! cargo run --release -p thunderload-engine --bin bench
//! ```
//!
//! Options: `--runs N` (default 3, median is reported), `--bench NAME`
//! (run only benches whose name contains NAME), `--list`.
//!
//! What is measured (all downloads verified byte-for-byte after each run):
//!   * `direct/N-conn` — the checkpointed multi-connection path
//!     (`download_url_resumable_with_connections`) on a 64 MiB file, for
//!     N in 1/2/4/8/16.
//!   * `direct/sequential` — the one-shot sequential path (`download_url`
//!     with 1 connection) on the same 64 MiB file.
//!   * `hls` — 32 x 1 MiB TS segments through `download_hls_resumable`.
//!   * `dash` — 32 x 1 MiB segments through `download_dash_resumable`
//!     (SegmentTemplate manifest, single video track, no FFmpeg needed).
//!   * `direct/resume` — pause a 256 MiB download at ~50%, then time the
//!     resume call to completion; the reported throughput is over the
//!     bytes the resume actually had to fetch. (Bigger file and slower
//!     trickle than the other direct benches — see RESUME_SIZE — so the
//!     pause lands mid-flight instead of racing a ~200 ms download.)
//!
//! What the numbers mean — read this before quoting them:
//!   * The fixture server trickles every body in 64 KiB chunks with a
//!     1 ms sleep *between* chunks — after every chunk except the last
//!     of a response, never after the last one. That models a server
//!     that drips data with per-chunk latency, which is what makes
//!     parallel connections pay off; on raw loopback without the trickle
//!     the file would transfer in milliseconds and the numbers would be
//!     noise. These are **engine-parallelism numbers under a latency
//!     model, not real-internet throughput**. The checkpointed path
//!     fetches each 256 KiB engine segment with its own Range request,
//!     so a 1-connection 64 MiB download is 256 responses of 4 trickle
//!     chunks each: 256 × 3 = 768 sleeps, a ~768 ms floor on
//!     direct/1-conn — not the 1024 ms a sleep-after-every-chunk
//!     reading would imply.
//!   * Everything runs on localhost against a fixture server in this
//!     process. No real server, no real network, no IDM is involved:
//!     a ThunderLoad-vs-IDM side-by-side needs a real Windows machine
//!     and an IDM install, and is explicitly NOT claimed here.
//!   * The suite is a binary, not part of `cargo test`: it takes
//!     ~20 s in release and is not run by the test suite.
//!
//! This binary adds no dependencies: the servers are plain
//! `std::net::TcpListener` threads in the same style as the engine's
//! own test fixtures, and timing is `std::time::Instant`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use thunderload_engine::{
    download_dash_resumable, download_hls_resumable, download_url,
    download_url_resumable_with_connections, partial_download_metadata_path,
    DownloadControl, DownloadOutcome, GlobalRateLimiter, HlsSelection,
    DEFAULT_RESUMABLE_CONNECTIONS,
};
use tokio::sync::{mpsc, watch};

/// Size of the file served for the direct-download benches.
const DIRECT_SIZE: usize = 64 * 1024 * 1024;
/// The resume bench uses a bigger file and a slower trickle than the
/// other direct benches: progress is reported at most every 180 ms,
/// so pausing a ~200 ms download at 50% is a race the download usually
/// wins. At 256 MiB / 4 ms the first half takes ~1 s and the pause
/// lands mid-flight with a wide margin.
const RESUME_SIZE: usize = 256 * 1024 * 1024;
const RESUME_TRICKLE: Duration = Duration::from_millis(4);
/// Bytes per media segment for the HLS/DASH benches.
const SEGMENT_SIZE: usize = 1024 * 1024;
/// Segment count for the HLS/DASH benches (32 MiB of media each).
const MEDIA_SEGMENTS: usize = 32;
/// The fixture server writes bodies in chunks of this size...
const TRICKLE_CHUNK: usize = 64 * 1024;
/// ...sleeping this long between chunks (models per-chunk server latency).
const TRICKLE_DELAY: Duration = Duration::from_millis(1);
/// Validator headers, so the resumable path accepts checkpoints.
const ETAG: &str = "\"bench-v1\"";
const LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

// ---------------------------------------------------------------------------
// Fixture server
// ---------------------------------------------------------------------------

/// A static-route HTTP/1.1 server on 127.0.0.1, in the style of the
/// engine's own test fixtures: one thread per accepted connection,
/// keep-alive, HEAD + Range supported, bodies trickled.
struct BenchServer {
    base: String,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl BenchServer {
    fn new(routes: HashMap<String, Vec<u8>>) -> Self {
        Self::with_trickle(routes, TRICKLE_DELAY)
    }

    /// Same server with a different per-chunk trickle delay. The resume
    /// bench uses a slower trickle so the pause lands mid-flight with a
    /// wide margin (progress is reported at most every 180 ms, so a
    /// ~200 ms download can finish before the first progress update
    /// that crosses 50%).
    fn with_trickle(routes: HashMap<String, Vec<u8>>, trickle: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bench server bind");
        listener.set_nonblocking(true).expect("bench server nonblocking");
        let address = listener.local_addr().expect("bench server addr");
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let routes = Arc::new(routes);
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let _ = stream.set_nonblocking(false);
                let routes = routes.clone();
                // Detached: a keep-alive connection can idle on read long
                // after the bench ends; the process exit reaps it.
                thread::spawn(move || serve_connection(stream, &routes, trickle));
            }
        });
        Self {
            base: format!("http://{address}"),
            shutdown,
            handle: Some(handle),
        }
    }

    fn stop(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Serves sequential requests on one keep-alive connection until the
/// client closes it or a write fails.
fn serve_connection(
    mut stream: TcpStream,
    routes: &Arc<HashMap<String, Vec<u8>>>,
    trickle: Duration,
) {
    loop {
        let mut raw = Vec::new();
        let mut buffer = [0_u8; 1024];
        let head_complete = loop {
            match stream.read(&mut buffer) {
                Ok(0) => break false,
                Ok(count) => {
                    raw.extend_from_slice(&buffer[..count]);
                    if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                        break true;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break false,
            }
        };
        if !head_complete {
            return;
        }
        let request = String::from_utf8_lossy(&raw);
        let mut parts = request.split_whitespace();
        let method = parts.next().unwrap_or("");
        let target = parts.next().unwrap_or("/");
        let path = target.split('?').next().unwrap_or(target).to_string();
        let range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("range")
                .then(|| value.trim().to_string())
        });
        let Some(body) = routes.get(&path) else {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        };
        if method == "HEAD" {
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: {ETAG}\r\nLast-Modified: {LAST_MODIFIED}\r\nAccept-Ranges: bytes\r\nConnection: keep-alive\r\n\r\n",
                body.len()
            );
            if stream.write_all(headers.as_bytes()).is_err() {
                return;
            }
            continue;
        }
        if method != "GET" {
            let _ = stream.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        }
        // Range: `bytes=start-` (open-ended, what the engine sends) or
        // `bytes=start-end` (closed, what the HLS/DASH byte-range fetches
        // send). Anything unparsable falls back to the whole body.
        let (status, content_range, slice): (&str, String, &[u8]) =
            match range.as_deref().and_then(parse_range) {
                Some((start, end)) if start < body.len() && start <= end => {
                    let end = end.min(body.len() - 1);
                    (
                        "206 Partial Content",
                        format!(
                            "Content-Range: bytes {start}-{end}/{}\r\n",
                            body.len()
                        ),
                        &body[start..=end],
                    )
                }
                Some(_) => {
                    let _ = stream.write_all(b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    return;
                }
                None => ("200 OK", String::new(), &body[..]),
            };
        let headers = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: {ETAG}\r\nLast-Modified: {LAST_MODIFIED}\r\nAccept-Ranges: bytes\r\n{content_range}Connection: keep-alive\r\n\r\n",
            slice.len()
        );
        if stream.write_all(headers.as_bytes()).is_err() {
            return;
        }
        for (index, chunk) in slice.chunks(TRICKLE_CHUNK).enumerate() {
            if stream.write_all(chunk).is_err() {
                return;
            }
            // Sleep between chunks, not after the last one.
            if index + 1 < slice.len().div_ceil(TRICKLE_CHUNK) {
                thread::sleep(trickle);
            }
        }
    }
}

/// Parses `bytes=start-` or `bytes=start-end` into (start, end-inclusive).
/// Returns None when the header is absent or malformed.
fn parse_range(header: &str) -> Option<(usize, usize)> {
    let rest = header.strip_prefix("bytes=")?;
    let (start, end) = rest.split_once('-')?;
    let start: usize = start.parse().ok()?;
    let end: usize = if end.is_empty() {
        usize::MAX
    } else {
        end.parse().ok()?
    };
    Some((start, end))
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Median of a non-empty list, averaging the two middle values for
/// even counts instead of taking the slower one (M4).
fn median_duration(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        values[middle]
    } else {
        (values[middle - 1] + values[middle]) / 2
    }
}

fn median_f64(mut values: Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        values[middle]
    } else {
        (values[middle - 1] + values[middle]) / 2.0
    }
}

fn median_u64(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        values[middle]
    } else {
        (values[middle - 1] + values[middle]) / 2
    }
}

/// One bench's result: per-run wall times and the byte count each run
/// covered. Throughput is the median of the per-run throughputs, so a
/// run's time is always paired with that same run's bytes (M4) — the
/// resume bench's remaining bytes vary run to run, and pairing the
/// median time with the median bytes would mix two different runs.
struct BenchResult {
    name: String,
    /// Median bytes per run (for the table's bytes column; constant
    /// except for direct/resume, where the pause point varies).
    bytes: u64,
    runs: Vec<Duration>,
    throughputs: Vec<f64>,
}

impl BenchResult {
    fn new(name: String, bytes_per_run: Vec<u64>, runs: Vec<Duration>) -> Self {
        assert_eq!(bytes_per_run.len(), runs.len(), "bytes/runs mismatch");
        let throughputs = bytes_per_run
            .iter()
            .zip(&runs)
            .map(|(bytes, time)| {
                let secs = time.as_secs_f64();
                if secs == 0.0 {
                    f64::INFINITY
                } else {
                    *bytes as f64 / secs / (1024.0 * 1024.0)
                }
            })
            .collect();
        let bytes = median_u64(bytes_per_run);
        Self {
            name,
            bytes,
            runs,
            throughputs,
        }
    }

    fn median(&self) -> Duration {
        median_duration(self.runs.clone())
    }

    /// Median MiB/s across runs (per-run throughput, not
    /// bytes-over-median-time).
    fn throughput_mib_s(&self) -> f64 {
        median_f64(self.throughputs.clone())
    }
}

/// Removes the run's temp dir when it goes out of scope, so a failed
/// run (bail, verify failure, outcome mismatch) cleans up like a
/// successful one instead of leaking up to a 256 MiB preallocated
/// `.part` file in /tmp (M1). Removal is best-effort: a failure to
/// delete must not mask the bench's own result.
struct TempDirGuard(PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fresh_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "thunderload-bench-{prefix}-{}",
        uuid::Uuid::new_v4()
    ))
}

fn new_control() -> (watch::Sender<DownloadControl>, watch::Receiver<DownloadControl>) {
    watch::channel(DownloadControl::Running)
}

fn new_progress() -> (
    mpsc::UnboundedSender<thunderload_engine::DownloadProgress>,
    mpsc::UnboundedReceiver<thunderload_engine::DownloadProgress>,
) {
    mpsc::unbounded_channel()
}

fn new_limiter() -> Arc<GlobalRateLimiter> {
    Arc::new(GlobalRateLimiter::new())
}

/// Reads the finished file and checks it byte-for-byte against the
/// expected content. A fast but wrong download must fail loudly, never
/// report a throughput number.
async fn verify_file(path: &Path, expected: &[u8]) -> anyhow::Result<()> {
    let bytes = tokio::fs::read(path).await?;
    anyhow::ensure!(
        bytes.len() == expected.len(),
        "{}: wrong size ({} bytes, expected {})",
        path.display(),
        bytes.len(),
        expected.len()
    );
    anyhow::ensure!(
        bytes == expected,
        "{}: content mismatch",
        path.display()
    );
    Ok(())
}

fn media_playlist(segment_count: usize) -> String {
    let mut playlist = String::from(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:0\n",
    );
    for index in 0..segment_count {
        playlist.push_str("#EXTINF:10.0,\n");
        playlist.push_str(&format!("seg{index}.ts\n"));
    }
    playlist.push_str("#EXT-X-ENDLIST\n");
    playlist
}

fn dash_segment_template_mpd(segment_count: usize) -> String {
    format!(
        r#"<MPD type="static" mediaPresentationDuration="PT{segment_count}S">
          <Period duration="PT{segment_count}S">
            <AdaptationSet contentType="video" mimeType="video/mp4">
              <SegmentTemplate timescale="1" startNumber="1" initialization="$RepresentationID$/init.mp4" media="$RepresentationID$/seg-$Number%03d$.m4s">
                <SegmentTimeline><S t="0" d="1" r="{}"/></SegmentTimeline>
              </SegmentTemplate>
              <Representation id="v1" bandwidth="900000" width="640" height="360"/>
            </AdaptationSet>
          </Period>
        </MPD>"#,
        segment_count - 1
    )
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

async fn bench_direct(connections: usize, runs: usize) -> anyhow::Result<BenchResult> {
    let routes = HashMap::from([(
        "/file.bin".to_string(),
        distinct_payload(DIRECT_SIZE),
    )]);
    let server = BenchServer::new(routes);
    let url = format!("{}/file.bin", server.base);
    let expected = distinct_payload(DIRECT_SIZE);
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let dir = fresh_dir("direct");
        tokio::fs::create_dir_all(&dir).await?;
        // Cleans up on success AND on failure (M1).
        let _guard = TempDirGuard(dir.clone());
        let target = dir.join("file.bin");
        let (control_tx, control_rx) = new_control();
        let (progress_tx, _progress_rx) = new_progress();
        let start = Instant::now();
        let outcome = download_url_resumable_with_connections(
            &url,
            &target,
            None,
            connections,
            control_rx,
            new_limiter(),
            progress_tx,
        )
        .await?;
        let elapsed = start.elapsed();
        anyhow::ensure!(
            outcome == DownloadOutcome::Completed,
            "direct/{connections}-conn: unexpected outcome {outcome:?}"
        );
        verify_file(&target, &expected).await?;
        times.push(elapsed);
        drop(control_tx);
    }
    server.stop();
    Ok(BenchResult::new(
        format!("direct/{connections}-conn"),
        vec![DIRECT_SIZE as u64; runs],
        times,
    ))
}

async fn bench_sequential(runs: usize) -> anyhow::Result<BenchResult> {
    let routes = HashMap::from([(
        "/file.bin".to_string(),
        distinct_payload(DIRECT_SIZE),
    )]);
    let server = BenchServer::new(routes);
    let url = format!("{}/file.bin", server.base);
    let expected = distinct_payload(DIRECT_SIZE);
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let dir = fresh_dir("sequential");
        tokio::fs::create_dir_all(&dir).await?;
        // Cleans up on success AND on failure (M1).
        let _guard = TempDirGuard(dir.clone());
        let target = dir.join("file.bin");
        let (control_tx, control_rx) = new_control();
        let (progress_tx, _progress_rx) = new_progress();
        let start = Instant::now();
        let out = download_url(&url, &target, 1, None, progress_tx, new_limiter(), control_rx).await?;
        drop(control_tx);
        let elapsed = start.elapsed();
        anyhow::ensure!(
            out == target,
            "direct/sequential: engine wrote to {} instead of {}",
            out.display(),
            target.display()
        );
        verify_file(&target, &expected).await?;
        times.push(elapsed);
    }
    server.stop();
    Ok(BenchResult::new(
        "direct/sequential".to_string(),
        vec![DIRECT_SIZE as u64; runs],
        times,
    ))
}

async fn bench_hls(runs: usize) -> anyhow::Result<BenchResult> {
    let mut routes = HashMap::new();
    let mut expected = Vec::new();
    for index in 0..MEDIA_SEGMENTS {
        let body = vec![(index % 251) as u8; SEGMENT_SIZE];
        routes.insert(format!("/seg{index}.ts"), body.clone());
        expected.extend_from_slice(&body);
    }
    routes.insert(
        "/media.m3u8".to_string(),
        media_playlist(MEDIA_SEGMENTS).into_bytes(),
    );
    let server = BenchServer::new(routes);
    let url = format!("{}/media.m3u8", server.base);
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let dir = fresh_dir("hls");
        tokio::fs::create_dir_all(&dir).await?;
        // Cleans up on success AND on failure (M1).
        let _guard = TempDirGuard(dir.clone());
        let target = dir.join("video.mp4");
        let (control_tx, control_rx) = new_control();
        let (progress_tx, _progress_rx) = new_progress();
        let start = Instant::now();
        let outcome = download_hls_resumable(
            &url,
            &HlsSelection::none(),
            &target,
            None,
            control_rx,
            progress_tx,
            new_limiter(),
            DEFAULT_RESUMABLE_CONNECTIONS,
        )
        .await?;
        let elapsed = start.elapsed();
        anyhow::ensure!(
            outcome == DownloadOutcome::Completed,
            "hls: unexpected outcome {outcome:?}"
        );
        verify_file(&target, &expected).await?;
        times.push(elapsed);
        drop(control_tx);
    }
    server.stop();
    Ok(BenchResult::new(
        "hls".to_string(),
        vec![expected.len() as u64; runs],
        times,
    ))
}

async fn bench_dash(runs: usize) -> anyhow::Result<BenchResult> {
    let mut routes = HashMap::new();
    let init = vec![b'I'; 1024];
    let mut expected = init.clone();
    routes.insert("/v1/init.mp4".to_string(), init);
    for index in 1..=MEDIA_SEGMENTS {
        let body = vec![(index % 251) as u8; SEGMENT_SIZE];
        routes.insert(format!("/v1/seg-{index:03}.m4s"), body.clone());
        expected.extend_from_slice(&body);
    }
    routes.insert(
        "/video.mpd".to_string(),
        dash_segment_template_mpd(MEDIA_SEGMENTS).into_bytes(),
    );
    let server = BenchServer::new(routes);
    let url = format!("{}/video.mpd", server.base);
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let dir = fresh_dir("dash");
        tokio::fs::create_dir_all(&dir).await?;
        // Cleans up on success AND on failure (M1).
        let _guard = TempDirGuard(dir.clone());
        let target = dir.join("movie.mp4");
        let (control_tx, control_rx) = new_control();
        let (progress_tx, _progress_rx) = new_progress();
        let start = Instant::now();
        let outcome = download_dash_resumable(
            &url,
            "v1",
            None,
            &target,
            None,
            control_rx,
            progress_tx,
            new_limiter(),
            DEFAULT_RESUMABLE_CONNECTIONS,
        )
        .await?;
        let elapsed = start.elapsed();
        anyhow::ensure!(
            outcome == DownloadOutcome::Completed,
            "dash: unexpected outcome {outcome:?}"
        );
        verify_file(&target, &expected).await?;
        times.push(elapsed);
        drop(control_tx);
    }
    server.stop();
    Ok(BenchResult::new(
        "dash".to_string(),
        vec![expected.len() as u64; runs],
        times,
    ))
}

async fn bench_resume(runs: usize) -> anyhow::Result<BenchResult> {
    let routes = HashMap::from([(
        "/file.bin".to_string(),
        distinct_payload(RESUME_SIZE),
    )]);
    let server = BenchServer::with_trickle(routes, RESUME_TRICKLE);
    let url = format!("{}/file.bin", server.base);
    let expected = distinct_payload(RESUME_SIZE);
    let mut times = Vec::with_capacity(runs);
    let mut remaining_per_run = Vec::with_capacity(runs);
    for _ in 0..runs {
        let dir = fresh_dir("resume");
        tokio::fs::create_dir_all(&dir).await?;
        // Cleans up on success AND on failure (M1).
        let _guard = TempDirGuard(dir.clone());
        let target = dir.join("file.bin");
        // Phase 1: download with 8 connections, pause at ~50%.
        //
        // The progress channel is the signal: the part file is
        // preallocated to the full size on creation, so its length
        // cannot tell us how much was actually written.
        let (control_tx, control_rx) = new_control();
        let (progress_tx, mut progress_rx) = new_progress();
        let seen = Arc::new(AtomicU64::new(0));
        let seen_writer = seen.clone();
        let progress_task = tokio::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                seen_writer.store(progress.downloaded, Ordering::Relaxed);
            }
        });
        let task_url = url.clone();
        let task_target = target.clone();
        let task = tokio::spawn(async move {
            download_url_resumable_with_connections(
                &task_url,
                &task_target,
                None,
                8,
                control_rx,
                new_limiter(),
                progress_tx,
            )
            .await
        });
        let half = RESUME_SIZE as u64 / 2;
        loop {
            if seen.load(Ordering::Relaxed) >= half {
                break;
            }
            // If the task finished before we saw 50%, the bench is
            // meaningless — but first find out WHY it finished (M2): if
            // the engine errored, that error is the diagnosis, not the
            // race. Only a genuine completion means the download won
            // the race against the pause.
            if task.is_finished() {
                progress_task.abort();
                let joined = task
                    .await
                    .map_err(|error| anyhow::anyhow!("task failed: {error}"))?;
                let outcome = joined?;
                anyhow::bail!(
                    "direct/resume: download finished ({outcome:?}) before the pause landed"
                );
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        control_tx.send(DownloadControl::Paused)?;
        let joined = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .map_err(|_| anyhow::anyhow!("direct/resume: pause phase timed out"))?;
        let outcome = joined.map_err(|error| anyhow::anyhow!("task failed: {error}"))??;
        anyhow::ensure!(
            outcome == DownloadOutcome::Paused,
            "direct/resume: phase 1 outcome was {outcome:?}, expected Paused"
        );
        progress_task.abort();
        drop(control_tx);
        // Phase 2: time the resume to completion. The remaining bytes
        // come from the checkpoint the pause flushed: only fully-done
        // segments count, so a chunk that was in flight at the pause is
        // counted as remaining, exactly as the resume will re-fetch it.
        // Segment ranges are inclusive (start, end), so a done
        // segment covers end - start + 1 bytes.
        let meta_path = partial_download_metadata_path(&target);
        let meta_bytes = tokio::fs::read(&meta_path).await?;
        let meta: serde_json::Value = serde_json::from_slice(&meta_bytes)?;
        let done_bytes: u64 = meta
            .get("segments")
            .and_then(|value| value.as_array())
            .map(|segments| {
                segments
                    .iter()
                    .map(|segment| {
                        let done = segment
                            .get("done")
                            .and_then(|value| value.as_bool())
                            .unwrap_or(false);
                        if !done {
                            return 0;
                        }
                        let start = segment
                            .get("start")
                            .and_then(|value| value.as_u64())
                            .unwrap_or(0);
                        let end = segment
                            .get("end")
                            .and_then(|value| value.as_u64())
                            .unwrap_or(0);
                        end.saturating_sub(start) + 1
                    })
                    .sum()
            })
            .unwrap_or(0);
        let remaining = RESUME_SIZE as u64 - done_bytes;
        anyhow::ensure!(
            remaining > 0 && remaining < RESUME_SIZE as u64,
            "direct/resume: checkpoint shows {done_bytes} of {RESUME_SIZE} bytes done, nothing left to resume"
        );
        let (control_tx, control_rx) = new_control();
        let (progress_tx, _progress_rx) = new_progress();
        let start = Instant::now();
        let outcome = download_url_resumable_with_connections(
            &url,
            &target,
            None,
            8,
            control_rx,
            new_limiter(),
            progress_tx,
        )
        .await?;
        let elapsed = start.elapsed();
        anyhow::ensure!(
            outcome == DownloadOutcome::Completed,
            "direct/resume: phase 2 outcome was {outcome:?}, expected Completed"
        );
        verify_file(&target, &expected).await?;
        times.push(elapsed);
        remaining_per_run.push(remaining);
        drop(control_tx);
    }
    server.stop();
    Ok(BenchResult::new(
        "direct/resume".to_string(),
        remaining_per_run,
        times,
    ))
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const ALL_BENCHES: &[&str] = &[
    "direct/1-conn",
    "direct/2-conn",
    "direct/4-conn",
    "direct/8-conn",
    "direct/16-conn",
    "direct/sequential",
    "hls",
    "dash",
    "direct/resume",
];

struct Args {
    runs: usize,
    filter: Option<String>,
    list: bool,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args {
        runs: 3,
        filter: None,
        list: false,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        // `--flag=value` form (I1).
        if let Some(value) = arg.strip_prefix("--runs=") {
            args.runs = value.parse()?;
            anyhow::ensure!(args.runs >= 1, "--runs must be at least 1");
            continue;
        }
        if let Some(value) = arg.strip_prefix("--bench=") {
            args.filter = Some(value.to_string());
            continue;
        }
        match arg.as_str() {
            "--list" => args.list = true,
            "--help" | "-h" => {
                println!("ThunderLoad engine benchmarks");
                println!("  cargo run --release -p thunderload-engine --bin bench [--runs N] [--bench NAME] [--list]");
                println!("Runs each bench N times (default 3) against a localhost fixture");
                println!("server and reports the median. See the header comment in");
                println!("engine/src/bin/bench.rs for what the numbers do and do not mean.");
                std::process::exit(0);
            }
            "--runs" => {
                let value = argv
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--runs needs a number"))?;
                args.runs = value.parse()?;
                anyhow::ensure!(args.runs >= 1, "--runs must be at least 1");
            }
            "--bench" => {
                args.filter = Some(
                    argv.next()
                        .ok_or_else(|| anyhow::anyhow!("--bench needs a name"))?,
                );
            }
            other => anyhow::bail!("unknown argument: {other} (try --help)"),
        }
    }
    Ok(args)
}

async fn run_bench(name: &str, runs: usize) -> anyhow::Result<BenchResult> {
    match name {
        "direct/1-conn" => bench_direct(1, runs).await,
        "direct/2-conn" => bench_direct(2, runs).await,
        "direct/4-conn" => bench_direct(4, runs).await,
        "direct/8-conn" => bench_direct(8, runs).await,
        "direct/16-conn" => bench_direct(16, runs).await,
        "direct/sequential" => bench_sequential(runs).await,
        "hls" => bench_hls(runs).await,
        "dash" => bench_dash(runs).await,
        "direct/resume" => bench_resume(runs).await,
        _ => anyhow::bail!("unknown bench: {name}"),
    }
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs >= 1.0 {
        format!("{secs:.3} s")
    } else {
        format!("{:.1} ms", secs * 1000.0)
    }
}

/// Payload whose byte at offset `i` is `i % 251`: no two 64 KiB
/// regions are identical, so a segment written to the wrong offset
/// (or duplicated) fails the byte-for-byte check instead of passing
/// silently the way a uniform `b'A'` payload would (P3).
fn distinct_payload(size: usize) -> Vec<u8> {
    (0..size).map(|index| (index % 251) as u8).collect()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Localhost exemption (P1-3): the engine's SSRF validator and
    // validating resolver block loopback by default; this binary runs
    // its fixtures on 127.0.0.1, so it opts in via the env-gated
    // allowlist instead of the old `cfg(test)` exemption.
    std::env::set_var("THUNDERLOAD_ALLOW_LOCALHOST", "1");
    let args = parse_args()?;
    if args.list {
        for name in ALL_BENCHES {
            println!("{name}");
        }
        return Ok(());
    }
    let selected: Vec<&str> = ALL_BENCHES
        .iter()
        .copied()
        .filter(|name| {
            args.filter
                .as_deref()
                .is_none_or(|needle| name.contains(needle))
        })
        .collect();
    anyhow::ensure!(!selected.is_empty(), "no bench matches the filter");
    println!("ThunderLoad engine benchmarks (localhost fixture server)");
    println!(
        "runs: {} (median reported) | direct file: 64 MiB (resume: 256 MiB) | media: 32 x 1 MiB segments | trickle: 1 ms per 64 KiB chunk (resume: 4 ms)",
        args.runs
    );
    println!(
        "os: {} | cpus: {}",
        std::env::consts::OS,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    println!();
    println!(
        "{:<20} {:>10} {:>12} {:>12} {:>12} {:>14}",
        "bench", "bytes", "median", "min", "max", "throughput"
    );
    let mut results = Vec::new();
    for name in selected {
        let result = run_bench(name, args.runs).await?;
        let min = result.runs.iter().min().copied().unwrap_or_default();
        let max = result.runs.iter().max().copied().unwrap_or_default();
        println!(
            "{:<20} {:>10} {:>12} {:>12} {:>12} {:>13.1} MiB/s",
            result.name,
            result.bytes,
            format_duration(result.median()),
            format_duration(min),
            format_duration(max),
            result.throughput_mib_s(),
        );
        results.push(result);
    }
    println!();
    println!("Notes: localhost fixture numbers under a 1 ms / 64 KiB server-trickle model —");
    println!("they measure engine parallelism, not real-internet throughput. No IDM");
    println!("comparison is made or implied; that needs a real Windows machine + IDM.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the fixture's Range parsing (M3): the closed form every
    /// bench sends, the open-ended form the sequential-resume path
    /// sends, and the malformed inputs that fall back to a full-body
    /// 200. The reversed range parses here and is rejected with 416
    /// by the server's `start <= end` guard.
    #[test]
    fn parse_range_forms() {
        // Closed ranges, including the one-byte `bytes=0-0` probe edge.
        assert_eq!(parse_range("bytes=0-0"), Some((0, 0)));
        assert_eq!(parse_range("bytes=100-199"), Some((100, 199)));
        // Open-ended: an empty end means "to the end of the object".
        assert_eq!(parse_range("bytes=100-"), Some((100, usize::MAX)));
        assert_eq!(parse_range("bytes=0-"), Some((0, usize::MAX)));
        // Malformed: no prefix, no dash, non-numeric, suffix-only.
        assert_eq!(parse_range("100-199"), None);
        assert_eq!(parse_range("bytes=100"), None);
        assert_eq!(parse_range("bytes=abc-def"), None);
        assert_eq!(parse_range("bytes=-100"), None);
        assert_eq!(parse_range("bytes="), None);
        assert_eq!(parse_range(""), None);
        // Reversed: parses, the server answers 416.
        assert_eq!(parse_range("bytes=200-100"), Some((200, 100)));
    }

    /// The median helpers average the two middle values for even
    /// counts instead of taking the slower one (M4).
    #[test]
    fn median_helpers() {
        assert_eq!(
            median_duration(vec![
                Duration::from_millis(100),
                Duration::from_millis(300),
                Duration::from_millis(200),
            ]),
            Duration::from_millis(200)
        );
        assert_eq!(
            median_duration(vec![
                Duration::from_millis(100),
                Duration::from_millis(300),
            ]),
            Duration::from_millis(200)
        );
        assert_eq!(median_f64(vec![1.0, 3.0, 2.0]), 2.0);
        assert_eq!(median_f64(vec![1.0, 3.0]), 2.0);
        assert_eq!(median_u64(vec![10, 30, 20]), 20);
        assert_eq!(median_u64(vec![10, 30]), 20);
    }

    /// Throughput pairs each run's bytes with that same run's time
    /// (M4): two runs with different bytes must not be mixed.
    #[test]
    fn bench_result_pairs_per_run_bytes_with_per_run_time() {
        let result = BenchResult::new(
            "test".to_string(),
            vec![100 * 1024 * 1024, 50 * 1024 * 1024],
            vec![Duration::from_secs(1), Duration::from_secs(1)],
        );
        // Per-run throughputs are 100 and 50 MiB/s; the median is 75.
        assert_eq!(result.throughput_mib_s(), 75.0);
        assert_eq!(result.bytes, 75 * 1024 * 1024);
    }
}

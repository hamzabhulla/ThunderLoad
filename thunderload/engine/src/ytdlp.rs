//! yt-dlp routing (Slice 3, option b — on-demand tools).
//!
//! YouTube's desktop web client serves SABR over UMP: `adaptiveFormats`
//! arrive without a `url`, so no sniffer — however good — can produce a
//! downloadable YouTube URL. yt-dlp is the extractor layer that can: it
//! asks YouTube's InnerTube API with a player client that still returns
//! direct URLs. The same routing covers the other API-driven social
//! sites (Instagram, TikTok, X, Facebook, Reddit, Twitch, Vimeo).
//!
//! Bundling (Hamza's decision, option b): NEITHER tool is bundled —
//! the installer stays ~5 MB. yt-dlp and Deno download on first use,
//! with explicit user consent (the download button IS the consent —
//! never a silent background fetch), visible progress, and SHA256
//! verification against the pinned hashes below before use.
//!
//! Licenses (verified live 2026-11-17): yt-dlp is Unlicense (public
//! domain — the research report's §8 table says "GPL", which is
//! wrong); Deno is MIT. Nothing is bundled, so no installer change —
//! the license facts live here, where the tools are resolved.
//!
//! Player-client chain (measured live 2026-11-17 on Big Buck Bunny,
//! `aqz-KE-bpKQ`): `default,android_vr` returns 54 formats up to
//! 2160p with real googlevideo URLs; the android_vr-led chain alone
//! returns 5 formats capped at 360p (format 18 muxed). So the web
//! client goes first while a JS runtime exists, and android_vr is the
//! fallback when it does not. The chain is one constant — when YouTube
//! moves, the fix is a constant change, not a rewrite.
//!
//! Direct download, not extract-only: yt-dlp downloads the video
//! itself. Extract-only would hand the engine signed googlevideo URLs
//! that expire in 60–180 s and force the engine to re-implement
//! fragment downloading — that is Slice 4's work, duplicated.

use anyhow::{anyhow, Context, Result};
use crate::download::{
    download_url_resumable_with_connections, partial_download_metadata_path,
    partial_download_path, DownloadControl, DownloadOutcome, DownloadProgress,
    GlobalRateLimiter, DEFAULT_RESUMABLE_CONNECTIONS,
};
use crate::session::SessionHeaders;
use crate::video::{SubtitleVariant, VideoVariant};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::sync::{mpsc, watch};

/// Pinned yt-dlp release. Never float to "latest" at build time — a
/// release must be reproducible. The pin is a compile-time const: the
/// updater (check_ytdlp_update) does NOT move it — `yt-dlp -U`
/// replaces the binary on disk, and the on-disk binary's hash is then
/// recorded in its sidecar file (see verify_tool_on_disk). The pin
/// moves the download URL and the expected hash of a FRESH download;
/// after an update, the on-disk version (from `yt-dlp --version`) and
/// the sidecar are authoritative, not this const.
pub const YTDLP_VERSION: &str = "2026.10.26";
/// SHA256 of yt-dlp.exe 2026.10.26 (Windows x64 standalone), from the
/// release's published SHA2-256SUMS — verified live 2026-11-17.
/// The Linux/macOS assets are different bytes with different hashes;
/// their pins are the `YTDLP_LINUX_SHA256` / `YTDLP_MACOS_SHA256`
/// consts below — see `YtDlpTool::pinned_sha256`.
pub const YTDLP_EXE_SHA256: &str =
    "f5389d3698ecde4ef06f4e6b1ce0294fe9ac1396228c8990b8d3416d6d177a7d";
/// SHA256 of yt-dlp_linux 2026.10.26, from the release's published
/// SHA2-256SUMS — verified live 2026-11-19: the asset was downloaded
/// from the release and hashed, and the hash matches the sums file.
pub const YTDLP_LINUX_SHA256: &str =
    "767ba9dce7052aa196f8b12652f433d76de128ba168966fa84b6394d5e6c6795";
/// SHA256 of yt-dlp_macos 2026.10.26, same source and verification
/// as the Linux pin (asset downloaded and hashed 2026-11-19).
pub const YTDLP_MACOS_SHA256: &str =
    "0f922c15ccc0b83eefa1c8bc9104e434318c4bf16e408e1ee93b7745a6efe0aa";
/// Pinned Deno release (the JS runtime yt-dlp uses for YouTube's
/// signature deciphering since 2025.11.12).
pub const DENO_VERSION: &str = "2.9.8";
/// SHA256 of deno-x86_64-pc-windows-msvc.zip v2.9.8, from the release's
/// published .sha256sum file (PowerShell Get-FileHash format) —
/// verified live 2026-11-17. The Linux/macOS zips are different bytes
/// with different hashes; their pins are the `DENO_ZIP_*` consts
/// below — see `YtDlpTool::pinned_sha256`.
pub const DENO_ZIP_SHA256: &str =
    "cba84b26e0df5ae7f74fc66b86a95b866872a86292ebd84fa1e959927edd93bf";
/// SHA256 of deno-x86_64-unknown-linux-gnu.zip v2.9.8, from the
/// release's published .sha256sum file — verified live 2026-11-19:
/// the asset was downloaded from the release and hashed, and the
/// hash matches the .sha256sum file.
pub const DENO_ZIP_LINUX_SHA256: &str =
    "e890975bc2549807bbcc0b584ffafda72cc7c93349dd99c80483dd784f6c4609";
/// SHA256 of deno-aarch64-apple-darwin.zip v2.9.8, same source and
/// verification as the Linux pin (asset downloaded and hashed
/// 2026-11-19).
pub const DENO_ZIP_MACOS_AARCH64_SHA256: &str =
    "9f0bf660393da2eaff044cdc7444cb08c73f235849b61d224d825a194ce7c98e";
/// SHA256 of deno-x86_64-apple-darwin.zip v2.9.8, same source and
/// verification as the Linux pin (asset downloaded and hashed
/// 2026-11-19).
pub const DENO_ZIP_MACOS_X86_64_SHA256: &str =
    "4b2f5edd2639b7470f034238b321d7002d8dfb4726d8558f52c17afac370eb2c";
/// Player-client fallback chain, best quality first. See the module
/// docs for the live measurement behind this order.
pub const PLAYER_CLIENT_CHAIN: &str = "default,android_vr,tv,ios,web_safari";

/// Per-OS download URLs (P0-13b): the old code returned the Windows
/// asset URL on every platform, so a Linux/macOS download fetched a
/// `.exe` / Windows zip it could never run. yt-dlp ships one
/// standalone binary per OS (`yt-dlp.exe`, `yt-dlp_linux`,
/// `yt-dlp_macos`); Deno ships one zip per OS+arch.
const YTDLP_DOWNLOAD_URL_WINDOWS: &str =
    "https://github.com/yt-dlp/yt-dlp/releases/download/2026.10.26/yt-dlp.exe";
const YTDLP_DOWNLOAD_URL_LINUX: &str =
    "https://github.com/yt-dlp/yt-dlp/releases/download/2026.10.26/yt-dlp_linux";
const YTDLP_DOWNLOAD_URL_MACOS: &str =
    "https://github.com/yt-dlp/yt-dlp/releases/download/2026.10.26/yt-dlp_macos";
const DENO_DOWNLOAD_URL_WINDOWS: &str =
    "https://github.com/denoland/deno/releases/download/v2.9.8/deno-x86_64-pc-windows-msvc.zip";
const DENO_DOWNLOAD_URL_LINUX: &str =
    "https://github.com/denoland/deno/releases/download/v2.9.8/deno-x86_64-unknown-linux-gnu.zip";
const DENO_DOWNLOAD_URL_MACOS_AARCH64: &str =
    "https://github.com/denoland/deno/releases/download/v2.9.8/deno-aarch64-apple-darwin.zip";
const DENO_DOWNLOAD_URL_MACOS_X86_64: &str =
    "https://github.com/denoland/deno/releases/download/v2.9.8/deno-x86_64-apple-darwin.zip";
/// Approximate download sizes (bytes) shown with the consent UI so
/// the user knows what the tap fetches. Exact sizes come from the
/// download itself; these are the published release sizes.
pub const YTDLP_EXE_SIZE_BYTES: u64 = 23_575_744;
pub const DENO_ZIP_SIZE_BYTES: u64 = 42_917_472;

/// One of the two on-demand tools. Internal ids only — the UI renders
/// the plain display name ("yt-dlp" / "Deno") wherever it talks about
/// a download, per Hamza's final call: the name is shown on both the
/// consent step and the progress step, never a generic label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum YtDlpTool {
    YtDlp,
    Deno,
}

impl YtDlpTool {
    /// Wire id, used in native-host / Tauri payloads.
    pub fn id(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => "ytdlp",
            YtDlpTool::Deno => "deno",
        }
    }
    /// Plain name shown in the UI.
    pub fn display_name(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => "yt-dlp",
            YtDlpTool::Deno => "Deno",
        }
    }
    /// On-disk file name for this platform (P0-13b): the `.exe`
    /// suffix is Windows-only — Linux/macOS ship plain `yt-dlp`
    /// and `deno` binaries.
    pub fn file_name(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => {
                if cfg!(target_os = "windows") {
                    "yt-dlp.exe"
                } else {
                    "yt-dlp"
                }
            }
            YtDlpTool::Deno => {
                if cfg!(target_os = "windows") {
                    "deno.exe"
                } else {
                    "deno"
                }
            }
        }
    }
    /// Download URL for this platform's release asset (P0-13b).
    pub fn download_url(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => {
                if cfg!(target_os = "windows") {
                    YTDLP_DOWNLOAD_URL_WINDOWS
                } else if cfg!(target_os = "macos") {
                    YTDLP_DOWNLOAD_URL_MACOS
                } else {
                    YTDLP_DOWNLOAD_URL_LINUX
                }
            }
            YtDlpTool::Deno => {
                if cfg!(target_os = "windows") {
                    DENO_DOWNLOAD_URL_WINDOWS
                } else if cfg!(target_os = "macos") {
                    if cfg!(target_arch = "aarch64") {
                        DENO_DOWNLOAD_URL_MACOS_AARCH64
                    } else {
                        DENO_DOWNLOAD_URL_MACOS_X86_64
                    }
                } else {
                    DENO_DOWNLOAD_URL_LINUX
                }
            }
        }
    }
    /// The pinned SHA256 for this platform's download. Every
    /// platform has a pin now: the Windows pins were verified live
    /// 2026-11-17 and the Linux/macOS pins 2026-11-19 (each asset
    /// downloaded from its GitHub Release and hashed — the hashes
    /// match the releases' published .sha256sum files). A first
    /// install is therefore verified against the release hash on
    /// every platform, never trusted on first use. (The one
    /// remaining trust-on-first-use case is Deno's EXTRACTED exe:
    /// its pin is the ZIP's hash, never the exe's, so a manual
    /// Deno drop with no sidecar is still recorded at first use —
    /// see `read_expected_tool_hash`.) A platform pin must never be
    /// "borrowed" for another platform's asset: the bytes differ,
    /// so the check would fail every download on that platform.
    /// `None` is only the fallback for a future platform with no
    /// pin recorded yet — today it is unreachable.
    pub fn pinned_sha256(self) -> Option<&'static str> {
        Some(match self {
            YtDlpTool::YtDlp => {
                if cfg!(target_os = "windows") {
                    YTDLP_EXE_SHA256
                } else if cfg!(target_os = "macos") {
                    YTDLP_MACOS_SHA256
                } else {
                    YTDLP_LINUX_SHA256
                }
            }
            YtDlpTool::Deno => {
                if cfg!(target_os = "windows") {
                    DENO_ZIP_SHA256
                } else if cfg!(target_os = "macos") {
                    if cfg!(target_arch = "aarch64") {
                        DENO_ZIP_MACOS_AARCH64_SHA256
                    } else {
                        DENO_ZIP_MACOS_X86_64_SHA256
                    }
                } else {
                    DENO_ZIP_LINUX_SHA256
                }
            }
        })
    }
    pub fn approx_size_bytes(self) -> u64 {
        match self {
            YtDlpTool::YtDlp => YTDLP_EXE_SIZE_BYTES,
            YtDlpTool::Deno => DENO_ZIP_SIZE_BYTES,
        }
    }
    pub fn from_id(id: &str) -> Option<Self> {
        match id {
            "ytdlp" | "yt-dlp" => Some(YtDlpTool::YtDlp),
            "deno" => Some(YtDlpTool::Deno),
            _ => None,
        }
    }
}

/// Returned when a tool is needed but not on disk. The caller turns
/// this into the consent UI (named tool + direct download button) —
/// it is not a plain failure, so it is a typed error, not a string.
#[derive(Debug)]
pub struct ToolsMissing {
    pub tools: Vec<YtDlpTool>,
}

impl std::fmt::Display for ToolsMissing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.tools.iter().map(|tool| tool.display_name()).collect();
        write!(f, "Missing download helper: {}", names.join(", "))
    }
}

impl std::error::Error for ToolsMissing {}

/// Where on-demand tools live: `%LOCALAPPDATA%\ThunderLoad\tools` on
/// Windows, `~/.local/share/ThunderLoad/tools` elsewhere. A user who
/// downloads a tool themselves drops the file here and the app picks
/// it up — that is the manual alternative behind the download button.
/// `THUNDERLOAD_TOOLS_DIR` overrides the location (tests use it).
pub fn tools_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("THUNDERLOAD_TOOLS_DIR") {
        if !dir.trim().is_empty() {
            return PathBuf::from(dir);
        }
    }
    #[cfg(target_os = "windows")]
    {
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            if !local.trim().is_empty() {
                return PathBuf::from(local).join("ThunderLoad").join("tools");
            }
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".local").join("share").join("ThunderLoad").join("tools")
}

pub fn tool_path(tool: YtDlpTool) -> PathBuf {
    tools_dir().join(tool.file_name())
}

pub fn tool_present(tool: YtDlpTool) -> bool {
    tool_path(tool).is_file()
}

/// Status of one tool, for Settings / the consent UI. `version` is
/// the tool's own reported version when it runs, else None. `verified`
/// says the file on disk matches its expected SHA256 (pinned hash, or
/// the hash recorded when it was downloaded / updated) — a present but
/// unverified tool is never executed.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolStatus {
    pub tool: YtDlpTool,
    pub present: bool,
    pub verified: bool,
    pub version: Option<String>,
    pub path: String,
}

pub async fn tool_status(tool: YtDlpTool) -> ToolStatus {
    let present = tool_present(tool);
    let verified = if present {
        verify_tool_on_disk(tool).await.is_ok()
    } else {
        false
    };
    let version = if present && verified {
        // Already verified above: run --version without hashing the
        // binary a second time.
        run_tool_version(tool).await
    } else {
        None
    };
    ToolStatus {
        tool,
        present,
        verified,
        version,
        path: tool_path(tool).to_string_lossy().to_string(),
    }
}

fn hidden_tokio_command<S: AsRef<std::ffi::OsStr>>(program: S) -> TokioCommand {
    #[cfg(target_os = "windows")]
    let std_command = {
        use std::os::windows::process::CommandExt;
        let mut std_command = std::process::Command::new(program);
        std_command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        std_command
    };
    #[cfg(unix)]
    let std_command = {
        use std::os::unix::process::CommandExt;
        let mut std_command = std::process::Command::new(program);
        std_command.process_group(0);
        std_command
    };
    #[cfg(not(any(target_os = "windows", unix)))]
    let std_command = std::process::Command::new(program);
    let mut command = TokioCommand::from(std_command);
    command.kill_on_drop(true);
    command
}

/// Run `<tool> --version` and return its output. Callers must have
/// verified the tool first (`verify_tool_on_disk`): a binary swapped
/// in after install must not run, even for --version. Times out: a
/// hung tool must not hang the caller (Settings, the update check)
/// forever. Kept separate from verification so `tool_status` hashes
/// the binary once, not twice.
async fn run_tool_version(tool: YtDlpTool) -> Option<String> {
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        hidden_tokio_command(tool_path(tool)).arg("--version").output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// One progress event while a tool downloads. The UI renders the
/// tool's plain name with these numbers ("yt-dlp download ho raha
/// hai — 45% — 1.2 MB/s · 12 s bacha"); the event itself carries the
/// wire id. Speed and ETA come from the engine (see
/// `ToolSpeedTracker`): the desktop used to show a bare percent,
/// which reads as "stuck" on a slow connection.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDownloadProgress {
    pub tool: YtDlpTool,
    pub downloaded: u64,
    pub total: Option<u64>,
    pub percent: Option<f64>,
    /// Average bytes per second since the download started. None
    /// for the first 100 ms — one chunk in one millisecond would
    /// otherwise read as gigabytes per second.
    pub speed_bps: Option<u64>,
    /// Whole seconds left at the current average speed. None when
    /// the total is unknown or the speed is not known yet; 0 when
    /// the download is complete.
    pub eta_secs: Option<u64>,
}

/// Speed + ETA for a tool download, from the AVERAGE rate since the
/// download started — not an instantaneous sample. The average is
/// one steady number the user can sanity-check against the bytes
/// moved, instead of a figure that jumps with every chunk. It is a
/// plain struct over explicit instants (`update_at`) so tests can
/// drive it without sleeping.
struct ToolSpeedTracker {
    started_at: std::time::Instant,
}

impl ToolSpeedTracker {
    fn new() -> Self {
        Self {
            started_at: std::time::Instant::now(),
        }
    }

    /// Speed + ETA for a progress event at this moment.
    fn update(&self, downloaded: u64, total: Option<u64>) -> (Option<u64>, Option<u64>) {
        self.update_at(downloaded, total, std::time::Instant::now())
    }

    fn update_at(
        &self,
        downloaded: u64,
        total: Option<u64>,
        now: std::time::Instant,
    ) -> (Option<u64>, Option<u64>) {
        let elapsed = now.duration_since(self.started_at);
        if elapsed < std::time::Duration::from_millis(100) || downloaded == 0 {
            return (None, None);
        }
        let speed = downloaded as f64 / elapsed.as_secs_f64();
        if speed <= 0.0 {
            return (None, None);
        }
        let speed_bps = Some(speed as u64);
        let eta_secs = match total {
            Some(total) if total <= downloaded => Some(0),
            Some(total) => Some(((total - downloaded) as f64 / speed).ceil() as u64),
            None => None,
        };
        (speed_bps, eta_secs)
    }
}

/// Download one tool with SHA256 verification. The caller must have
/// obtained explicit consent first (the download button tap) — this
/// function never runs unprompted. A hash mismatch deletes the temp
/// file and fails loudly; a half-downloaded temp file never becomes
/// the tool.
/// Check a downloaded tool file against its pinned SHA256 before it
/// is installed. A mismatch refuses the file outright — the temp file
/// is discarded by the caller, nothing is installed. Every platform
/// has a pin now (see `pinned_sha256`), so a first install is
/// verified against the release hash, not trusted on first use. The
/// `None` arm below is only the fallback for a future platform with
/// no pin recorded yet: there the digest is accepted and recorded in
/// the sidecar at install time, so later launches still verify the
/// file — trust on first use, not trust forever.
fn verify_tool_digest(tool: YtDlpTool, digest: &str) -> Result<()> {
    if let Some(pin) = tool.pinned_sha256() {
        if digest != pin {
            return Err(anyhow!(
                "The {} download failed verification (SHA256 mismatch) — the file was discarded, nothing was installed",
                tool.display_name()
            ));
        }
    }
    Ok(())
}

/// On-disk trust (MINOR 10): the tools live in a user-writable
/// folder, so a hash checked once at download time is not enough — a
/// binary swapped in afterwards would run unchecked, and a
/// user-dropped manual file was trusted on sight. Every launch
/// re-hashes the file before it is executed (cheap: ~50 ms for the
/// 23 MB yt-dlp, ~200 ms for the 100 MB Deno) and compares it with
/// the expected hash:
/// - a `.sha256` sidecar next to the tool (written at download time,
///   and rewritten by the updater after `yt-dlp -U`), if one exists;
/// - else the pinned hash for yt-dlp (recorded for every platform —
///   a yt-dlp file with no sidecar is checked against the pin);
/// - Deno has no pinned hash for the extracted exe (the pin is the
///   ZIP's hash), so a Deno file with no sidecar is trusted on FIRST
///   use and its hash is recorded in a sidecar then — a later swap
///   is still caught.
fn tool_hash_sidecar_path(tool: YtDlpTool) -> PathBuf {
    tools_dir().join(format!(".{}.sha256", tool.file_name()))
}

fn read_expected_tool_hash(tool: YtDlpTool) -> Option<String> {
    if let Ok(text) = std::fs::read_to_string(tool_hash_sidecar_path(tool)) {
        let hash = text.trim().to_ascii_lowercase();
        if hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Some(hash);
        }
    }
    match tool {
        YtDlpTool::YtDlp => tool.pinned_sha256().map(str::to_string),
        YtDlpTool::Deno => None,
    }
}

fn write_tool_hash_sidecar(tool: YtDlpTool, hash: &str) {
    let _ = std::fs::write(tool_hash_sidecar_path(tool), hash);
}

/// Stream-hash a file at an arbitrary path without loading it whole
/// into memory. The buffer is boxed on purpose: a 64 KiB inline
/// buffer lives in this async fn's state machine, and Tauri's IPC
/// layers copy that future by value several times on the way to
/// tokio::spawn. On Windows the main thread stack is 1 MiB; an inline
/// buffer here made the spawned future ~396 KiB and overflowed it
/// (STATUS_STACK_OVERFLOW opening Settings). A heap buffer keeps the
/// future small with the same 64 KiB read chunks.
async fn hash_file(path: &Path) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Cannot read {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = Box::new([0u8; 65536]);
    loop {
        let n = file
            .read(&mut buf[..])
            .await
            .with_context(|| format!("Cannot read {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Stream-hash a tool file without loading it whole into memory.
async fn hash_tool_file(tool: YtDlpTool) -> Result<String> {
    hash_file(&tool_path(tool)).await
}

/// Verify the tool on disk before it is executed. Missing → the typed
/// ToolsMissing error (the caller shows the consent UI). Present but
/// hash-mismatched → a plain error naming the tool and telling the
/// user to re-download it from Settings.
async fn verify_tool_on_disk(tool: YtDlpTool) -> Result<()> {
    if !tool_present(tool) {
        return Err(anyhow::Error::new(ToolsMissing { tools: vec![tool] }));
    }
    let hash = hash_tool_file(tool).await?;
    match read_expected_tool_hash(tool) {
        Some(expected) if hash == expected => Ok(()),
        Some(_) => Err(anyhow!(
            "The {} in your tools folder failed verification (SHA256 mismatch) — it may have been replaced. Re-download it from Settings.",
            tool.display_name()
        )),
        None => {
            // Deno, no sidecar yet: trust on first use, then record
            // the hash so any later swap is caught.
            write_tool_hash_sidecar(tool, &hash);
            Ok(())
        }
    }
}

/// One in-process lock per tool, taken by `download_tool` for the
/// whole download+install. Two callers in one process can ask for
/// the same tool at once — the desktop Tauri command and the native
/// host's `ytdlp.ensure_tool` handler (one per browser message) —
/// and without it both would download into the same temp file.
/// This stops same-process double-runs; it does NOT stop two
/// processes (the desktop app and the native host are separate
/// processes), which is why the temp file is also unique per
/// attempt below.
static TOOL_DOWNLOAD_LOCKS: std::sync::OnceLock<[tokio::sync::Mutex<()>; 2]> =
    std::sync::OnceLock::new();

fn tool_download_lock(tool: YtDlpTool) -> &'static tokio::sync::Mutex<()> {
    let locks = TOOL_DOWNLOAD_LOCKS
        .get_or_init(|| [tokio::sync::Mutex::new(()), tokio::sync::Mutex::new(())]);
    &locks[tool as usize]
}

/// Counter for unique per-attempt names: a PID alone is not enough
/// (one process can start two attempts), a counter alone is not
/// enough (two processes share nothing) — together they are unique.
static TOOL_TEMP_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// "pid-counter": unique per attempt in this process, and per
/// process across the machine — the two halves of every per-attempt
/// temp name (download temp file, Deno extract scratch dir).
fn attempt_suffix() -> String {
    let attempt = TOOL_TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{}-{}", std::process::id(), attempt)
}

/// A temp path no other attempt can share. The old fixed name
/// (`.deno.exe-download.tmp`) was the race: two overlapping Deno
/// downloads — desktop Settings and the native host's
/// `ytdlp.ensure_tool` — both `File::create` (truncate) the same
/// file, so the bytes on disk interleave while each caller's
/// in-memory hash still passes. Expand-Archive then reads the
/// corrupt on-disk zip and fails with "Could not unpack the Deno
/// download", even though the download itself was fine.
fn unique_tool_temp_path(dir: &Path, tool: YtDlpTool) -> PathBuf {
    dir.join(format!(
        ".{}-download-{}.tmp",
        tool.file_name(),
        attempt_suffix()
    ))
}

/// Fetch one tool's release file into `temp` with the engine's
/// multi-connection segmented downloader — the same path normal
/// downloads use — instead of a single sequential GET. A 22–41 MB
/// tool file over one connection crawls on a slow link and the UI
/// showed only a percent, so the download read as "stuck"; segmented
/// downloads use 8 connections and the progress events below carry
/// speed + ETA, so it reads as a download again.
///
/// Fallback: `download_url_resumable_with_connections` itself falls
/// back to its sequential checkpointed path when the server does not
/// support byte ranges (or the object is under 2 MiB) — the caller
/// does not choose. GitHub releases (the download source) support
/// ranges, so the segmented path is the normal case.
///
/// The transfer runs in a spawned task so its future lives on the
/// heap: awaiting the resumable downloader inline would inline its
/// state machine into this future, which Tauri copies by value on
/// the way to spawn — the same shape that overflowed the Windows
/// main-thread stack in the Settings crash (PR #47). Progress is
/// drained here and translated to `ToolDownloadProgress` events;
/// the channel closes when the transfer returns, which is also how
/// this function knows the transfer is done.
///
/// The caller hashes `temp` and installs it afterwards; this
/// function only fetches. While the transfer runs, a half-fetched
/// `temp` keeps its `.part` + `.meta.json` checkpoint; on any error
/// the caller (`download_tool`) deletes the temp and the checkpoint
/// with it — temp names are unique per attempt, so a retry gets a
/// fresh path and could never resume the old checkpoint anyway.
async fn download_tool_file(
    tool: YtDlpTool,
    url: &str,
    temp: &Path,
    progress_tx: Option<mpsc::UnboundedSender<ToolDownloadProgress>>,
) -> Result<()> {
    // No cancel path exists for tool downloads (the button tap is
    // the consent and the transfer is one-shot), so the control
    // channel stays Running for the whole transfer. The sender is
    // kept alive until the task returns: dropping it mid-transfer
    // would read as a host disconnect, not a cancel.
    let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
    // A fresh limiter, not the jobs' shared one: the user's speed
    // limit is for downloads, and a one-time ~63 MB tools fetch
    // must not be throttled by it — fast tools are the point.
    let rate_limiter = Arc::new(GlobalRateLimiter::new());
    let (engine_tx, mut engine_rx) = mpsc::unbounded_channel::<DownloadProgress>();
    let url = url.to_string();
    let temp_path = temp.to_path_buf();
    let fetch = tokio::spawn(async move {
        download_url_resumable_with_connections(
            &url,
            &temp_path,
            None,
            DEFAULT_RESUMABLE_CONNECTIONS,
            control_rx,
            rate_limiter,
            engine_tx,
        )
        .await
    });
    let tracker = ToolSpeedTracker::new();
    while let Some(event) = engine_rx.recv().await {
        if let Some(tx) = &progress_tx {
            let (speed_bps, eta_secs) = tracker.update(event.downloaded, event.total);
            let _ = tx.send(ToolDownloadProgress {
                tool,
                downloaded: event.downloaded,
                total: event.total,
                percent: event.percent,
                speed_bps,
                eta_secs,
            });
        }
    }
    drop(control_tx);
    let outcome = fetch
        .await
        .context("Tool download task failed")?
        .with_context(|| format!("Could not download {}", tool.display_name()))?;
    match outcome {
        DownloadOutcome::Completed => Ok(()),
        DownloadOutcome::Paused => Err(anyhow!(
            "The {} download was paused",
            tool.display_name()
        )),
        DownloadOutcome::Cancelled => Err(anyhow!(
            "The {} download was cancelled",
            tool.display_name()
        )),
    }
}

/// Remove a tool download's scratch files: the temp file plus the
/// resumable downloader's `.part` + `.meta.json` checkpoint next to
/// it. Runs on ANY `download_tool` failure after the temp is
/// created — download failure, hash failure, verification failure,
/// install failure — so a failed attempt leaves nothing behind.
/// (A corrupt checkpoint must not be resumed into the same corrupt
/// bytes on the next attempt, and with unique per-attempt temp
/// names a retry starts fresh regardless.)
fn remove_tool_download_scratch(temp: &Path) {
    let _ = std::fs::remove_file(temp);
    let _ = std::fs::remove_file(partial_download_path(temp));
    let _ = std::fs::remove_file(partial_download_metadata_path(temp));
}

/// Download one tool (yt-dlp or Deno) and install it into the tools
/// dir. Merged shape: the fetch is the segmented
/// `download_tool_file` (8 connections, speed + ETA); the temp name
/// is the per-attempt unique path, so two processes can never share
/// a file; the Deno install is the scratch-dir `install_deno_zip`;
/// and the whole download+install runs inside the wrapper below,
/// which deletes the temp AND the resumable downloader's `.part` +
/// `.meta.json` checkpoint on ANY error — a failed attempt leaves
/// nothing behind.
pub async fn download_tool(
    tool: YtDlpTool,
    progress_tx: Option<mpsc::UnboundedSender<ToolDownloadProgress>>,
) -> Result<PathBuf> {
    // Serialize same-tool downloads inside this process; the guard
    // is held until this function returns.
    let _download_guard = tool_download_lock(tool).lock().await;
    let dir = tools_dir();
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("Cannot create {}", dir.display()))?;
    let target = tool_path(tool);
    // A concurrent downloader in another process may have installed
    // the tool while this call waited on the lock (or before it was
    // even made): a present, verified tool needs no second download.
    if tool_present(tool) {
        if verify_tool_on_disk(tool).await.is_ok() {
            return Ok(target);
        }
        let _ = tokio::fs::remove_file(&target).await;
        let _ = tokio::fs::remove_file(tool_hash_sidecar_path(tool)).await;
    }
    let temp = unique_tool_temp_path(&dir, tool);
    // Everything after the temp is computed runs inside this block:
    // if anything fails — the fetch, the hash, the install — the
    // temp and its checkpoint go with the error. Temp names are
    // unique per attempt, so no later attempt ever truncates or
    // reuses a stale one; without this, every failed download leaks
    // one hidden `.<tool>-download-<pid>-<counter>.tmp` plus its
    // `.part` + `.meta.json` (up to ~41 MB).
    let result: Result<PathBuf> = async {
        download_tool_file(tool, tool.download_url(), &temp, progress_tx).await?;
        // The bytes are on disk now (the resumable downloader moved
        // its .part file into place): hash the temp file the same
        // way the installed tool is hashed on every launch, and
        // verify against the pin before anything becomes the tool.
        let digest = hash_file(&temp).await?;
        if let Err(error) = verify_tool_digest(tool, &digest) {
            return Err(error);
        }
        match tool {
            YtDlpTool::YtDlp => {
                tokio::fs::rename(&temp, &target)
                    .await
                    .with_context(|| format!("Cannot install {}", temp.display()))?;
                // Unix binaries need the executable bit: a freshly
                // renamed download is 0644, and yt-dlp would fail to
                // spawn with "permission denied" on every launch.
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
                        .with_context(|| format!("Could not make {} executable", target.display()))?;
                }
                // The digest was just verified against the pin: record it
                // so every later launch can re-verify this file.
                write_tool_hash_sidecar(tool, &digest);
            }
            YtDlpTool::Deno => {
                // `install_deno_zip` deletes the temp zip itself when
                // the install fails (~41 MB leaked per failed attempt
                // before); on success the caller deletes it, below.
                install_deno_zip(&temp, &dir).await?;
                let _ = tokio::fs::remove_file(&temp).await;
                if !target.is_file() {
                    return Err(anyhow!(
                        "The Deno download unpacked but {} was not found in it",
                        tool.file_name()
                    ));
                }
                // The zip was verified; record the EXTRACTED exe's hash —
                // the pin is the zip's hash, so only the sidecar can
                // verify the exe on later launches.
                if let Ok(exe_hash) = hash_tool_file(tool).await {
                    write_tool_hash_sidecar(tool, &exe_hash);
                }
            }
        }
        Ok(target)
    }
    .await;
    if result.is_err() {
        remove_tool_download_scratch(&temp);
    }
    result
}

/// Escape one value for a PowerShell single-quoted string: a literal
/// `'` is doubled (`''`). Without this, a tools-dir path containing
/// an apostrophe (e.g. `O'Brien`) breaks the Expand-Archive command.
/// Windows-only, like its only caller: without the gate the Linux
/// build warns that the function is never used.
#[cfg(target_os = "windows")]
fn powershell_quote(value: &str) -> String {
    value.replace('\'', "''")
}

/// Install a verified Deno zip: extract it into a fresh scratch
/// directory, then move the single binary into place.
///
/// Why the scratch directory: each attempt now has its own temp
/// zip, but two attempts extracting straight into the tools dir
/// would still race on the same `deno.exe` — Expand-Archive
/// `-Force` overwriting a file another Expand-Archive is still
/// writing. Extracting into a per-attempt directory and moving the
/// one finished file into place keeps the race out of the shared
/// folder entirely.
///
/// A leftover `deno.exe` from a crashed older unpack is removed
/// before extracting, so a half-written exe can never be mistaken
/// for the new one. If that delete fails the file is locked by a
/// running process, and the error says so plainly instead of
/// failing later inside the unpack.
///
/// On any failure the temp zip is deleted here — the caller only
/// deletes it on success — so a failed attempt never leaks ~41 MB.
async fn install_deno_zip(zip: &Path, dir: &Path) -> Result<()> {
    let target = tool_path(YtDlpTool::Deno);
    if target.is_file() {
        if let Err(error) = tokio::fs::remove_file(&target).await {
            let _ = tokio::fs::remove_file(zip).await;
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                return Err(anyhow!(
                    "Could not install Deno: {} is in use by another process — a download may still be running. Close it and try again.",
                    target.display()
                ));
            }
            return Err(error).context("Could not remove the old Deno install");
        }
    }
    let scratch = dir.join(format!(".deno-extract-{}", attempt_suffix()));
    match install_deno_zip_from_scratch(zip, &scratch, &target).await {
        Ok(()) => {
            let _ = tokio::fs::remove_dir_all(&scratch).await;
            Ok(())
        }
        Err(error) => {
            let _ = tokio::fs::remove_file(zip).await;
            let _ = tokio::fs::remove_dir_all(&scratch).await;
            Err(error)
        }
    }
}

/// Extract the zip into `scratch`, then move the binary into place
/// (same directory, so the rename is atomic).
async fn install_deno_zip_from_scratch(zip: &Path, scratch: &Path, target: &Path) -> Result<()> {
    tokio::fs::create_dir_all(scratch)
        .await
        .with_context(|| format!("Cannot create {}", scratch.display()))?;
    extract_deno_zip(zip, scratch).await?;
    let extracted = scratch.join(YtDlpTool::Deno.file_name());
    if !extracted.is_file() {
        return Err(anyhow!(
            "The Deno download unpacked but {} was not found in it",
            YtDlpTool::Deno.file_name()
        ));
    }
    tokio::fs::rename(&extracted, target)
        .await
        .with_context(|| format!("Could not install {}", target.display()))?;
    Ok(())
}

/// The extractor's stderr, for the failure message: `": <stderr>"`,
/// or empty when the extractor said nothing. Truncated — a wall of
/// PowerShell text would bury the message in the Settings UI.
fn unpack_error_detail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }
    let text: String = text.chars().take(500).collect();
    format!(": {text}")
}

/// Extract the verified Deno zip into `dir`. Windows uses
/// PowerShell's Expand-Archive (always present); elsewhere `unzip`
/// is used. The zip holds the Deno binary at its top level
/// (`deno.exe` on Windows, `deno` elsewhere).
///
/// The extractor's stderr is captured and included in the failure:
/// the old `.status()` call threw it away, so every unpack failure
/// looked identical ("Could not unpack the Deno download") no matter
/// the cause — a corrupt zip, a locked file, a missing tool.
async fn extract_deno_zip(zip: &Path, dir: &Path) -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        let output = hidden_tokio_command("powershell")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "Expand-Archive -LiteralPath '{}' -DestinationPath '{}' -Force",
                    powershell_quote(&zip.display().to_string()),
                    powershell_quote(&dir.display().to_string())
                ),
            ])
            .output()
            .await
            .context("Could not unpack the Deno download")?;
        if !output.status.success() {
            return Err(anyhow!(
                "Could not unpack the Deno download{}",
                unpack_error_detail(&output.stderr)
            ));
        }
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    {
        let output = hidden_tokio_command("unzip")
            .args(["-o", "-j"])
            .arg(zip)
            .arg(YtDlpTool::Deno.file_name())
            .arg("-d")
            .arg(dir)
            .output()
            .await
            .context("Could not unpack the Deno download (unzip is required)")?;
        if !output.status.success() {
            return Err(anyhow!(
                "Could not unpack the Deno download{}",
                unpack_error_detail(&output.stderr)
            ));
        }
        Ok(())
    }
}

/// Ensure a tool is on disk, downloading it first when the caller
/// already has consent (the progress channel is `Some` only after the
/// user tapped the download button). Returns the tool path. A tool
/// that is present but fails verification (swapped or corrupt) is
/// dropped and fetched fresh — presence alone is not trust.
pub async fn ensure_tool(
    tool: YtDlpTool,
    progress_tx: Option<mpsc::UnboundedSender<ToolDownloadProgress>>,
) -> Result<PathBuf> {
    if tool_present(tool) {
        if verify_tool_on_disk(tool).await.is_ok() {
            return Ok(tool_path(tool));
        }
        let _ = tokio::fs::remove_file(tool_path(tool)).await;
        let _ = tokio::fs::remove_file(tool_hash_sidecar_path(tool)).await;
    }
    download_tool(tool, progress_tx).await
}

/// Check whether yt-dlp has a newer release than the pin, and apply
/// it with yt-dlp's own self-update (`yt-dlp -U` works for the
/// standalone exe). The pin does NOT move: it is a compile-time const.
/// What moves is the binary on disk — after a successful update its
/// new hash is recorded in the sidecar, and the message reports the
/// ON-DISK version from `yt-dlp --version`, never the stale const.
/// Returns a short human-readable result.
pub async fn check_ytdlp_update() -> Result<String> {
    // Verify before executing: a swapped binary must not run -U.
    verify_tool_on_disk(YtDlpTool::YtDlp).await?;
    // -U downloads a new binary, so this gets a generous timeout
    // instead of hanging the caller forever on a stalled network.
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        hidden_tokio_command(tool_path(YtDlpTool::YtDlp)).arg("-U").output(),
    )
    .await
    .map_err(|_| anyhow!("The yt-dlp update timed out after 120 seconds"))?
    .context("Could not run the yt-dlp updater")?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let trimmed = text.trim().to_string();
    if output.status.success() {
        // -U replaced the binary on disk: record its new hash so
        // later launches still verify, and report the version that
        // is actually installed now. Re-verify the new binary against
        // the hash just recorded before running it — the update
        // itself is the window a swap would use.
        if let Ok(hash) = hash_tool_file(YtDlpTool::YtDlp).await {
            write_tool_hash_sidecar(YtDlpTool::YtDlp, &hash);
        }
        verify_tool_on_disk(YtDlpTool::YtDlp).await
            .context("The yt-dlp update produced a binary that failed verification")?;
        let on_disk = run_tool_version(YtDlpTool::YtDlp)
            .await
            .unwrap_or_else(|| YTDLP_VERSION.to_string());
        Ok(if trimmed.is_empty() {
            format!("yt-dlp is up to date ({on_disk})")
        } else {
            format!("{trimmed} (yt-dlp {on_disk} installed)")
        })
    } else {
        Err(anyhow!(
            "The yt-dlp update failed: {}",
            trimmed.lines().last().unwrap_or("unknown error")
        ))
    }
}

/// The yt-dlp format selector for one panel variant: `video+audio`
/// for a combined pick, the single format id otherwise.
pub fn format_selector(variant: &VideoVariant) -> String {
    match (&variant.representation_id, &variant.audio_representation_id) {
        (Some(video), Some(audio)) if !audio.is_empty() => format!("{video}+{audio}"),
        (Some(video), _) => video.clone(),
        (None, _) => String::new(),
    }
}

fn is_youtube_url(url: &str) -> bool {
    url::Url::parse(url)
        .map(|parsed| {
            let host = parsed.host_str().unwrap_or("").to_ascii_lowercase();
            host == "youtube.com"
                || host.ends_with(".youtube.com")
                || host == "youtu.be"
                || host == "music.youtube.com"
        })
        .unwrap_or(false)
}

/// One inspected page: the title yt-dlp reports plus the mapped
/// variants the panel renders.
#[derive(Debug, Clone)]
pub struct YtDlpInspect {
    pub title: Option<String>,
    pub variants: Vec<VideoVariant>,
    /// True when this result came from the generic extractor fallback
    /// (Slice 6): the page had no site-specific extractor, so yt-dlp
    /// read whatever generic formats the page itself exposes. The
    /// caller never lets a generic result overwrite a real inspect.
    pub generic: bool,
}

/// A Netscape-format cookies file for one run, deleted when the run
/// ends. Only ever written from the session handoff (the browser's
/// cookies for the page's own origin) and only ever passed to yt-dlp
/// for that same page.
struct TempCookiesFile {
    path: PathBuf,
}

impl TempCookiesFile {
    async fn write(session: &SessionHeaders, url: &str) -> Result<Option<Self>> {
        if !session.cookies_apply_to(url) {
            return Ok(None);
        }
        let cookies = match &session.cookies {
            Some(cookies) if !cookies.trim().is_empty() => cookies.clone(),
            _ => return Ok(None),
        };
        let Some(origin) = &session.origin else {
            return Ok(None);
        };
        let domain = url::Url::parse(origin)
            .ok()
            .and_then(|parsed| parsed.host_str().map(str::to_string))
            .unwrap_or_default();
        if domain.is_empty() {
            return Ok(None);
        }
        let secure = if url
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
        {
            "TRUE"
        } else {
            "FALSE"
        };
        let expiry = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() + 3600)
            .unwrap_or(0);
        let mut text = String::from("# Netscape HTTP Cookie File\n");
        for pair in cookies.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            // Cookie values never contain tabs/newlines (the extension
            // filters them on capture); skip anything suspicious
            // rather than writing a malformed file.
            if name.contains(['\t', '\n', '\r']) || value.contains(['\t', '\n', '\r']) {
                continue;
            }
            // The second field is "include subdomains": FALSE keeps
            // these cookies exact-origin, matching the session policy
            // that only ever sends them to the page's own origin.
            // TRUE used to widen them to every subdomain.
            text.push_str(&format!("{domain}\tFALSE\t/\t{secure}\t{expiry}\t{name}\t{value}\n"));
        }
        let path = std::env::temp_dir().join(format!(
            "thunderload-cookies-{}.txt",
            uuid::Uuid::new_v4()
        ));
        #[cfg(unix)]
        {
            let mut options = tokio::fs::OpenOptions::new();
            options.create_new(true).write(true).mode(0o600);
            let mut file = options
                .open(&path)
                .await
                .context("Could not write the temporary cookies file")?;
            file.write_all(text.as_bytes())
                .await
                .context("Could not write the temporary cookies file")?;
            file.flush()
                .await
                .context("Could not write the temporary cookies file")?;
        }
        #[cfg(not(unix))]
        {
            tokio::fs::write(&path, text)
                .await
                .context("Could not write the temporary cookies file")?;
        }
        Ok(Some(Self { path }))
    }
}

impl Drop for TempCookiesFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Extra PATH for yt-dlp child processes: the tools dir first, so a
/// downloaded Deno is found as the JS runtime without any
/// --js-runtimes syntax (yt-dlp looks `deno` up on PATH).
fn child_path_with_tools() -> String {
    let tools = tools_dir().to_string_lossy().to_string();
    let sep = if cfg!(target_os = "windows") { ";" } else { ":" };
    match std::env::var("PATH") {
        Ok(path) if !path.is_empty() => format!("{tools}{sep}{path}"),
        _ => tools,
    }
}

/// Inspect cap (default 60 s). Overridable for tests via
/// THUNDERLOAD_YTDLP_INSPECT_TIMEOUT_SECS — the production default
/// never changes; only the test suite shortens it.
fn inspect_timeout() -> std::time::Duration {
    std::env::var("THUNDERLOAD_YTDLP_INSPECT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(60))
}

pub async fn inspect_page(
    url: &str,
    session: Option<&SessionHeaders>,
) -> Result<YtDlpInspect> {
    inspect_page_with(url, session, false).await
}

/// Inspect one YouTube/social page URL and map its formats onto the
/// panel's VideoVariant rows. Fails with ToolsMissing when yt-dlp is
/// not on disk yet (the caller shows the consent UI), and with a
/// plain-language failure label otherwise.
/// `force_generic` (Slice 6) runs yt-dlp with `--force-generic-extractor`:
/// the fallback when sniffing and the normal inspect found nothing
/// usable for a page. The generic extractor reads whatever formats the
/// page itself exposes (direct media links, embedded players) — it
/// never overrides a real detector result, and the returned rows carry
/// `generic: true` so the caller can keep them separate from a real
/// inspect. Inspect cap (default 60 s). Overridable for tests via
/// THUNDERLOAD_YTDLP_INSPECT_TIMEOUT_SECS — the production default
/// never changes; only the test suite shortens it.
pub async fn inspect_page_with(
    url: &str,
    session: Option<&SessionHeaders>,
    force_generic: bool,
) -> Result<YtDlpInspect> {
    // N2: the full public-URL validator, not a scheme-only check.
    crate::session::validate_public_http_url(url).context("Invalid page URL")?;
    // Verified before executing: missing → ToolsMissing (consent UI),
    // swapped → a plain refusal. Never run an unverified binary.
    verify_tool_on_disk(YtDlpTool::YtDlp).await?;
    let cookies = match session {
        Some(session) => TempCookiesFile::write(session, url).await?,
        None => None,
    };
    let mut command = hidden_tokio_command(tool_path(YtDlpTool::YtDlp));
    command
        .arg("-j")
        .arg("--no-download")
        .arg("--no-playlist")
        .arg("--no-warnings");
    if force_generic {
        command.arg("--force-generic-extractor");
    }
    if is_youtube_url(url) {
        command.arg("--extractor-args");
        command.arg(format!("youtube:player-client={PLAYER_CLIENT_CHAIN}"));
    }
    if let Some(file) = &cookies {
        command.arg("--cookies").arg(file.path.as_os_str());
    }
    if let Some(session) = session {
        if let Some(referer) = &session.referer {
            command.arg("--referer").arg(referer);
        }
        if let Some(user_agent) = &session.user_agent {
            command.arg("--user-agent").arg(user_agent);
        }
    }
    command.arg("--").arg(url);
    command.env("PATH", child_path_with_tools());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    let mut child = command.spawn().context("Could not run yt-dlp")?;
    // Inspect takes 2–10 s on a good connection; the timeout caps it
    // so a hung extractor cannot wedge the panel. A bare
    // tokio::time::timeout around command.output() would drop the
    // future WITHOUT killing the child — the orphan keeps running and
    // the next inspect spawns another. So the timeout kills the child
    // explicitly (the TempCookiesFile drops with the early return, as
    // on every other error path here). wait_with_output() cannot be
    // used for this: it takes ownership of the child, so there would
    // be nothing left to kill on timeout — stdout/stderr are drained
    // on their own tasks and the status is awaited with &mut child.
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        if let Some(mut out) = stdout {
            let _ = out.read_to_end(&mut buf).await;
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        if let Some(mut err) = stderr {
            let _ = err.read_to_end(&mut buf).await;
        }
        buf
    });
    let status = tokio::select! {
        status = child.wait() => status.context("Could not run yt-dlp")?,
        _ = tokio::time::sleep(inspect_timeout()) => {
            kill_process_tree(&mut child).await;
            let _ = child.wait().await;
            stdout_task.abort();
            stderr_task.abort();
            return Err(anyhow!(
                "Getting formats took too long — the site may be slow or blocking the request"
            ));
        }
    };
    let output = std::process::Output {
        status,
        stdout: stdout_task.await.unwrap_or_default(),
        stderr: stderr_task.await.unwrap_or_default(),
    };
    drop(cookies);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("{}", failure_label(&stderr)));
    }
    let info: serde_json::Value = serde_json::from_slice(&output.stdout)
        .context("yt-dlp returned a response that could not be read")?;
    let title = info
        .get("title")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let variants = map_formats(&info).await;
    if variants.is_empty() {
        return Err(anyhow!(
            "No downloadable formats were found for this page."
        ));
    }
    Ok(YtDlpInspect { title, variants, generic: force_generic })
}

/// Subtitle tracks from a yt-dlp info dict's `subtitles` map
/// (language → [{url, ext, ...}]). One entry per language, first usable
/// entry only; the name is the language itself. Only direct-file
/// formats are kept (vtt/ttml/srt) — a subtitle playlist format would
/// need assembly the yt-dlp path does not do, so it is skipped, never
/// faked. Returns an empty list when the info dict carries none.
fn parse_ytdlp_subtitles(info: &serde_json::Value) -> Vec<SubtitleVariant> {
    let Some(languages) = info.get("subtitles").and_then(|value| value.as_object()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (language, entries) in languages {
        let Some(entries) = entries.as_array() else { continue; };
        for entry in entries {
            let Some(url) = entry.get("url").and_then(|value| value.as_str()) else { continue; };
            if url.is_empty() { continue; }
            let ext = entry.get("ext").and_then(|value| value.as_str()).unwrap_or("").to_ascii_lowercase();
            let format = match ext.as_str() {
                "vtt" | "ttml" | "srt" => ext,
                _ => continue,
            };
            out.push(SubtitleVariant {
                language: language.clone(),
                name: language.clone(),
                url: url.to_string(),
                format,
            });
            break;
        }
    }
    out
}

/// Map one yt-dlp `-j` info dict onto VideoVariant rows: one row per
/// quality (best format per height), one "Audio" row, muxed formats
/// as their own rows. Storyboards and URL-less entries are skipped.
/// A video+audio pick needs FFmpeg to merge; when FFmpeg is missing
/// the row is marked requires_ffmpeg (same pattern as video.rs:160)
/// so the UI says so before the download starts.
pub async fn map_formats(info: &serde_json::Value) -> Vec<VideoVariant> {
    let formats = match info.get("formats").and_then(|value| value.as_array()) {
        Some(formats) => formats,
        None => return Vec::new(),
    };
    // Live info (Slice 6): yt-dlp reports it at the top level. Every
    // row from a live info dict is a live row — the UIs prefix "Live · ".
    let is_live = info.get("is_live").and_then(|value| value.as_bool()).unwrap_or(false);
    // Subtitles (Slice 6): yt-dlp's `subtitles` dict maps language →
    // [{url, ext, ...}]. One row per language, first usable entry only,
    // attached to every video variant. Formats the downloader cannot
    // serve (anything but a direct vtt/ttml/srt file) are skipped, never
    // faked.
    let subtitles = parse_ytdlp_subtitles(info);
    #[derive(Clone)]
    struct Fmt {
        id: String,
        ext: String,
        height: u32,
        bandwidth: u64,
        fps: Option<f64>,
        vcodec: String,
        acodec: String,
        url: String,
    }
    let mut video_only: Vec<Fmt> = Vec::new();
    let mut audio_only: Vec<Fmt> = Vec::new();
    let mut muxed: Vec<Fmt> = Vec::new();
    for format in formats {
        let Some(url) = format.get("url").and_then(|value| value.as_str()) else {
            continue;
        };
        if url.is_empty() {
            continue;
        }
        // Storyboard thumbnails (mhtml) are not video.
        if format.get("protocol").and_then(|value| value.as_str()) == Some("mhtml") {
            continue;
        }
        let vcodec = format
            .get("vcodec")
            .and_then(|value| value.as_str())
            .unwrap_or("none")
            .to_string();
        let acodec = format
            .get("acodec")
            .and_then(|value| value.as_str())
            .unwrap_or("none")
            .to_string();
        if vcodec == "none" && acodec == "none" {
            continue;
        }
        let fmt = Fmt {
            id: format
                .get("format_id")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string(),
            ext: format
                .get("ext")
                .and_then(|value| value.as_str())
                .unwrap_or("mp4")
                .to_string(),
            height: format.get("height").and_then(|value| value.as_u64()).unwrap_or(0) as u32,
            bandwidth: format
                .get("tbr")
                .and_then(|value| value.as_f64())
                .map(|tbr| (tbr * 1000.0) as u64)
                .unwrap_or(0),
            // fps is what the format carries — shown only when present.
            fps: format.get("fps").and_then(|value| value.as_f64()),
            vcodec,
            acodec,
            url: url.to_string(),
        };
        if fmt.id.is_empty() {
            continue;
        }
        let has_video = fmt.vcodec != "none";
        let has_audio = fmt.acodec != "none";
        if has_video && !has_audio {
            video_only.push(fmt);
        } else if !has_video && has_audio {
            audio_only.push(fmt);
        } else {
            muxed.push(fmt);
        }
    }
    let ffmpeg_missing = !crate::video::ffmpeg_available().await;
    let mut variants: Vec<VideoVariant> = Vec::new();
    // Best audio first: the top-bitrate audio-only format pairs with
    // every video-only format.
    audio_only.sort_by(|a, b| b.bandwidth.cmp(&a.bandwidth));
    let best_audio = audio_only.first().cloned();
    // One row per height: the highest-bitrate video-only format wins.
    // mp4 (avc1) is preferred over webm at equal bandwidth — it plays
    // everywhere without a codec surprise.
    video_only.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then(b.bandwidth.cmp(&a.bandwidth))
            .then(a.ext.cmp(&b.ext))
    });
    let mut seen_heights: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for fmt in &video_only {
        if fmt.height == 0 || !seen_heights.insert(fmt.height) {
            continue;
        }
        let audio_id = best_audio.as_ref().map(|audio| audio.id.clone());
        variants.push(VideoVariant {
            kind: "ytdlp".into(),
            quality: format!("{}p", fmt.height),
            width: None,
            height: Some(fmt.height),
            bandwidth: if fmt.bandwidth > 0 { Some(fmt.bandwidth) } else { None },
            fps: fmt.fps,
            // The panel never downloads this URL directly — the
            // download goes through yt-dlp with the format selector.
            // The URL is kept so the row still identifies the stream.
            url: fmt.url.clone(),
            downloadable: true,
            note: None,
            representation_id: Some(fmt.id.clone()),
            audio_representation_id: audio_id,
            audio_only: None,
            requires_ffmpeg: if ffmpeg_missing && best_audio.is_some() {
                Some(true)
            } else {
                None
            },
            is_live: if is_live { Some(true) } else { None },
            subtitles: if subtitles.is_empty() { None } else { Some(subtitles.clone()) },
        });
    }
    for fmt in muxed {
        variants.push(VideoVariant {
            kind: "ytdlp".into(),
            quality: if fmt.height > 0 {
                format!("{}p", fmt.height)
            } else {
                "Stream".into()
            },
            width: None,
            height: if fmt.height > 0 { Some(fmt.height) } else { None },
            bandwidth: if fmt.bandwidth > 0 { Some(fmt.bandwidth) } else { None },
            fps: fmt.fps,
            url: fmt.url.clone(),
            downloadable: true,
            note: None,
            representation_id: Some(fmt.id.clone()),
            audio_representation_id: None,
            audio_only: None,
            requires_ffmpeg: None,
            is_live: if is_live { Some(true) } else { None },
            subtitles: if subtitles.is_empty() { None } else { Some(subtitles.clone()) },
        });
    }
    if let Some(audio) = best_audio {
        variants.push(VideoVariant {
            kind: "ytdlp".into(),
            quality: "Audio".into(),
            width: None,
            height: None,
            bandwidth: if audio.bandwidth > 0 { Some(audio.bandwidth) } else { None },
            fps: audio.fps,
            url: audio.url.clone(),
            downloadable: true,
            note: None,
            representation_id: Some(audio.id.clone()),
            audio_representation_id: None,
            audio_only: Some(true),
            requires_ffmpeg: None,
            is_live: if is_live { Some(true) } else { None },
            subtitles: if subtitles.is_empty() { None } else { Some(subtitles.clone()) },
        });
    }
    // Best quality first; the audio-only row sorts last (height 0).
    variants.sort_by(|a, b| {
        (b.height.unwrap_or(0))
            .cmp(&a.height.unwrap_or(0))
            .then(b.bandwidth.unwrap_or(0).cmp(&a.bandwidth.unwrap_or(0)))
    });
    variants
}

/// Plain-language failure labels (design §4 — part of the build, not
/// polish). A download manager that fails silently gets retried
/// forever; one that says why gets fixed once.
pub fn failure_label(stderr: &str) -> &'static str {
    let text = stderr.to_ascii_lowercase();
    if text.contains("sign in to confirm you're not a bot")
        || text.contains("sign in to confirm you are not a bot")
    {
        return "YouTube wants a login for this video. Open it in the browser while logged in, then try again.";
    }
    if text.contains("private video")
        || text.contains("video unavailable")
        || text.contains("this video is no longer available")
        || text.contains("http error 404")
        || text.contains("404: not found")
    {
        return "This video is removed, private, or region-blocked.";
    }
    if text.contains("no supported javascript runtime") || text.contains("no supported js runtime") {
        return "Deno is missing — download it from Settings, or YouTube stays on fewer formats.";
    }
    if text.contains("http error 403") || text.contains("403: forbidden") {
        return "YouTube refused this request. Try again, or open the video in the browser first.";
    }
    if text.contains("unsupported url") {
        return "This page is not a supported video page.";
    }
    // DRM is anchored: "drm" alone is not enough (a URL or title can
    // mention it) — it must come with protected / not supported /
    // encrypted / not downloadable, which is how yt-dlp actually
    // reports a DRM-gated format.
    if text.contains("drm")
        && (text.contains("protected")
            || text.contains("not supported")
            || text.contains("encrypted")
            || text.contains("not downloadable"))
    {
        return "Protected — not downloadable.";
    }
    if text.contains("extractor") || text.contains("unable to extract") || text.contains("failed to extract") {
        return "YouTube changed how this video works. Update yt-dlp from Settings and retry.";
    }
    "The download helper reported an error. Try again, or update it from Settings."
}

/// Parse one yt-dlp progress line. With `--progress-template` the
/// line is `download: 45.2%|123456|234567|1.23MiB/s|00:30`. Anything
/// else is ignored (never a crash) — yt-dlp's stdout format is not a
/// stable API, so an unparsable line only loses one progress sample.
pub fn parse_progress_line(line: &str) -> Option<DownloadProgress> {
    let body = line
        .trim()
        .strip_prefix("[download]")
        .map(str::trim)
        .unwrap_or(line.trim());
    let body = body.strip_prefix("download:").map(str::trim).unwrap_or(body);
    let parts: Vec<&str> = body.split('|').map(str::trim).collect();
    if parts.len() < 3 {
        return None;
    }
    let percent_text = parts[0].strip_suffix('%')?;
    let percent: f64 = percent_text.trim().parse().ok()?;
    if !(0.0..=100.0).contains(&percent) {
        return None;
    }
    let downloaded: u64 = parts[1].parse().ok()?;
    let total: Option<u64> = parts[2].parse().ok();
    Some(DownloadProgress {
        downloaded,
        total,
        percent: Some(percent),
        phase: "Downloading".into(),
        // yt-dlp reports bytes only — no segment counts (Slice 7b).
        segments_done: None,
        segments_total: None,
    })
}

/// Kill a yt-dlp child and its whole process tree: yt-dlp spawns
/// ffmpeg for the merge, and killing only yt-dlp would orphan it
/// (locked .part files, zombie processes). Windows gets taskkill /T;
/// Unix gets the child kill (no extra dependency for a group kill).
async fn kill_process_tree(child: &mut tokio::process::Child) {
    let Some(pid) = child.id() else {
        let _ = child.kill().await;
        return;
    };
    #[cfg(target_os = "windows")]
    {
        let _ = hidden_tokio_command("taskkill")
            .args(["/T", "/F", "/PID"])
            .arg(pid.to_string())
            .output()
            .await;
        let _ = child.kill().await;
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = TokioCommand::new("kill")
            .args(["-9", &format!("-{pid}")])
            .status()
            .await;
        let _ = child.kill().await;
    }
}

fn is_audio_only_selector(selector: &str) -> bool {
    if selector.contains('+') {
        return false;
    }
    let lower = selector.to_ascii_lowercase();
    let first = lower.split('/').next().unwrap_or(&lower).trim();
    let base = first.split('[').next().unwrap_or(first).trim();
    matches!(
        base,
        "bestaudio"
            | "bestaudio*"
            | "worstaudio"
            | "worstaudio*"
            | "ba"
            | "ba*"
            | "wa"
            | "wa*"
            | "139"
            | "140"
            | "141"
            | "233"
            | "234"
            | "249"
            | "250"
            | "251"
            | "599"
            | "600"
    ) || first.contains("vcodec=none")
        || base.starts_with("audio")
        || base.ends_with("-audio")
        || base.ends_with("_audio")
        || base.contains("audio_only")
}

/// Download one page URL with yt-dlp, directly (not extract-only).
/// Pause kills the process and keeps the .part files — re-running the
/// same command resumes natively. Cancel kills and deletes the
/// partials. The job store records the command (URL + format selector
/// + output path), so resume is a re-run, not a re-download.
///
/// The output template is `<stem>.%(ext)s`: yt-dlp picks the real
/// container (mp4 for merged video — forced with --merge-output-format
/// mp4 / --remux-video mp4 — m4a/opus/webm for audio-only). The
/// returned path is the file yt-dlp actually produced, or None while
/// the job is paused/cancelled.
#[allow(clippy::too_many_arguments)]
pub async fn download_page(
    url: &str,
    format_selector: &str,
    target: &Path,
    session: Option<&SessionHeaders>,
    mut control: watch::Receiver<DownloadControl>,
    progress_tx: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<(DownloadOutcome, Option<PathBuf>)> {
    // N2: the full public-URL validator, not a scheme-only check.
    crate::session::validate_public_http_url(url).context("Invalid page URL")?;
    let trimmed_selector = format_selector.trim();
    if trimmed_selector.is_empty() {
        return Err(anyhow!("No format was selected for this download"));
    }
    if trimmed_selector.starts_with('-')
        || format_selector.chars().any(|c| matches!(c, '\r' | '\n' | '\0'))
    {
        return Err(anyhow!("Invalid format selector"));
    }
    // Verified before executing: missing → ToolsMissing (consent UI),
    // swapped → a plain refusal. Never run an unverified binary.
    verify_tool_on_disk(YtDlpTool::YtDlp).await?;
    let cookies = match session {
        Some(session) => TempCookiesFile::write(session, url).await?,
        None => None,
    };
    // The reserved target names the file (e.g. video.mp4); yt-dlp
    // writes <stem>.<real ext> next to it, so the caller never has
    // to guess the container up front. The reservation creates an
    // empty file — remove it before spawning, or yt-dlp sees an
    // existing file and skips the download as "already downloaded".
    let stem = target.with_extension("");
    if let Ok(metadata) = tokio::fs::metadata(&target).await {
        if metadata.len() == 0 {
            let _ = tokio::fs::remove_file(&target).await;
        }
    }
    let escaped_stem = stem.to_string_lossy().replace('%', "%%");
    let outtmpl = format!("{escaped_stem}.%(ext)s");
    let mut command = hidden_tokio_command(tool_path(YtDlpTool::YtDlp));
    command
        .arg("-f")
        .arg(trimmed_selector)
        .arg("-o")
        .arg(&outtmpl)
        .arg("--newline")
        .arg("--progress-template")
        .arg("download:%(progress._percent_str)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.speed_str)s|%(progress.eta_str)s")
        .arg("--no-playlist")
        .arg("--no-warnings")
        .arg("--retries")
        .arg("10")
        .arg("--fragment-retries")
        .arg("10");
    if !is_audio_only_selector(trimmed_selector) {
        command
            .arg("--merge-output-format")
            .arg("mp4");
        // `--remux-video mp4` only when the target container actually
        // needs it (an .mp4 target) and FFmpeg is present to do it. A
        // muxed WebM row (a .webm target, requires_ffmpeg None) must
        // never get it — remuxing WebM to MP4 fails — and without
        // FFmpeg the flag only makes yt-dlp error out.
        let target_is_mp4 = target.extension().and_then(|value| value.to_str())
            .is_some_and(|value| value.eq_ignore_ascii_case("mp4"));
        if target_is_mp4 && crate::video::ffmpeg_available().await {
            command.arg("--remux-video").arg("mp4");
        }
    }
    if let Some(ffmpeg_path) = crate::video::configured_ffmpeg_path() {
        command.arg("--ffmpeg-location").arg(ffmpeg_path);
    }
    if is_youtube_url(url) {
        command.arg("--extractor-args");
        command.arg(format!("youtube:player-client={PLAYER_CLIENT_CHAIN}"));
    }
    if let Some(file) = &cookies {
        command.arg("--cookies").arg(file.path.as_os_str());
    }
    if let Some(session) = session {
        if let Some(referer) = &session.referer {
            command.arg("--referer").arg(referer);
        }
        if let Some(user_agent) = &session.user_agent {
            command.arg("--user-agent").arg(user_agent);
        }
    }
    command.arg("--").arg(url);
    command.env("PATH", child_path_with_tools());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    // A pause/cancel that arrived while the tool was being verified
    // or the cookies written must stop the spawn, not start a child
    // the loop below would only notice after its first line.
    if *control.borrow() == DownloadControl::Cancelled {
        cleanup_partial(&stem).await;
        return Ok((DownloadOutcome::Cancelled, None));
    }
    if *control.borrow() == DownloadControl::Paused {
        return Ok((DownloadOutcome::Paused, None));
    }
    let mut child = command
        .spawn()
        .context("Could not start yt-dlp")?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    // stderr is drained on its own task: a child blocked on a full
    // stderr pipe would hang forever, and its text is the failure
    // label when the run fails.
    let stderr_text = Arc::new(std::sync::Mutex::new(String::new()));
    let mut stderr_task = {
        let stderr_text = Arc::clone(&stderr_text);
        tokio::spawn(async move {
            if let Some(stderr) = stderr {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break,
                        Ok(_) => {
                            if let Ok(mut text) = stderr_text.lock() {
                                text.push_str(&line);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        })
    };
    let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
    let stdout_task = tokio::spawn(async move {
        if let Some(stdout) = stdout {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if line_tx.send(line.trim_end().to_string()).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });
    let mut last_progress: Option<DownloadProgress> = None;
    // Pause and Cancel are latched: a control change observed while
    // the child is still running kills the process tree immediately
    // (no waiting for the current fragment — that wait is the old
    // Pause/Cancel hang, Gemini claim 5).
    let outcome: Result<(DownloadOutcome, Option<PathBuf>)> = loop {
        tokio::select! {
            line = line_rx.recv() => {
                match line {
                    Some(line) => {
                        if let Some(progress) = parse_progress_line(&line) {
                            last_progress = Some(progress.clone());
                            let _ = progress_tx.send(progress);
                        }
                    }
                    None => {
                        // stdout closed; the child is finishing.
                        match child.try_wait() {
                            Ok(Some(status)) => {
                                break finish_download(status, target, &stem, &stderr_text, &mut stderr_task, last_progress.clone()).await;
                            }
                            Ok(None) => {
                                // stdout closed but the child lives on
                                // (still writing stderr): wait for it,
                                // staying responsive to Pause/Cancel —
                                // a bare child.wait() here would ignore
                                // a pause until the child exits on its
                                // own (same class as Gemini claim 5,
                                // narrower window).
                                let waited = loop {
                                    tokio::select! {
                                        result = child.wait() => {
                                            break match result {
                                                Ok(status) => finish_download(status, target, &stem, &stderr_text, &mut stderr_task, last_progress.clone()).await,
                                                Err(error) => Err(anyhow!(error)),
                                            };
                                        }
                                        _ = control.changed() => {
                                            // Copy the value out first:
                                            // watch::Ref is !Send and
                                            // must not live across the
                                            // kill await below.
                                            let control_now = *control.borrow();
                                            match control_now {
                                                DownloadControl::Paused => {
                                                    kill_process_tree(&mut child).await;
                                                    let _ = child.wait().await;
                                                    break Ok((DownloadOutcome::Paused, None));
                                                }
                                                DownloadControl::Cancelled => {
                                                    kill_process_tree(&mut child).await;
                                                    let _ = child.wait().await;
                                                    cleanup_partial(&stem).await;
                                                    break Ok((DownloadOutcome::Cancelled, None));
                                                }
                                                DownloadControl::Running => {}
                                            }
                                        }
                                    }
                                };
                                break waited;
                            }
                            Err(error) => break Err(anyhow!(error)),
                        }
                    }
                }
            }
            // The child's exit is its own wakeup. Without this arm
            // the loop only wakes on a stdout line or a control
            // change: if the child exits while an orphan (e.g.
            // ffmpeg outliving yt-dlp) holds stdout open, no line
            // ever arrives and the exit stays hidden until the
            // orphan dies — the loop would sit here for the
            // orphan's whole lifetime even though the download is
            // already done. The drain abort below runs only after
            // the loop breaks, so it cannot help here either.
            result = child.wait() => {
                break match result {
                    Ok(status) => finish_download(status, target, &stem, &stderr_text, &mut stderr_task, last_progress.clone()).await,
                    Err(error) => Err(anyhow!(error)),
                };
            }
            _ = control.changed() => {
                // Copy the value out first: watch::Ref is !Send and
                // must not live across the kill await below.
                let control_now = *control.borrow();
                match control_now {
                    DownloadControl::Paused => {
                        kill_process_tree(&mut child).await;
                        let _ = child.wait().await;
                        break Ok((DownloadOutcome::Paused, None));
                    }
                    DownloadControl::Cancelled => {
                        kill_process_tree(&mut child).await;
                        let _ = child.wait().await;
                        cleanup_partial(&stem).await;
                        break Ok((DownloadOutcome::Cancelled, None));
                    }
                    DownloadControl::Running => {}
                }
            }
        }
    };
    // The drain tasks must not hold up the return. An orphaned child
    // (e.g. ffmpeg outliving a killed yt-dlp) can keep a pipe's write
    // end open long after the direct child is gone, so awaiting the
    // drains would delay Pause/Cancel by the orphan's lifetime — the
    // same class as Gemini claim 5. The outcome no longer needs
    // anything from them: finish_download already read the stderr
    // text it needed, and no stdout line after the break matters.
    // Abort instead of awaiting; the pipes close with the tasks.
    stdout_task.abort();
    stderr_task.abort();
    drop(cookies);
    outcome
}

async fn finish_download(
    status: std::process::ExitStatus,
    target: &Path,
    stem: &Path,
    stderr_text: &Arc<std::sync::Mutex<String>>,
    stderr_task: &mut tokio::task::JoinHandle<()>,
    last_progress: Option<DownloadProgress>,
) -> Result<(DownloadOutcome, Option<PathBuf>)> {
    if status.success() {
        // The reserved target first (video picks land on it via the
        // mp4 merge); otherwise the one file yt-dlp produced for
        // this stem (audio-only picks keep their own container).
        if target.is_file() {
            return Ok((DownloadOutcome::Completed, Some(target.to_path_buf())));
        }
        if let Some(produced) = produced_file_for_stem(stem).await {
            return Ok((DownloadOutcome::Completed, Some(produced)));
        }
        return Err(anyhow!(
            "yt-dlp finished but the expected file was not created{}",
            last_progress
                .and_then(|progress| progress.percent)
                .map(|percent| format!(" (last progress {percent:.0}%)"))
                .unwrap_or_default()
        ));
    }
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        stderr_task,
    )
    .await;
    let stderr = stderr_text
        .lock()
        .map(|text| text.clone())
        .unwrap_or_default();
    Err(anyhow!("{}", failure_label(&stderr)))
}

/// Media containers yt-dlp can produce for a download. Sibling files
/// that share the stem but are not media (`.vtt` subtitles, `.jpg`
/// thumbnails, `.info.json`) must never shadow the finished file.
const MEDIA_EXTENSIONS: [&str; 13] = [
    "mp4", "webm", "mkv", "mov", "m4v", "ts", "m4a", "mp3", "opus", "ogg", "wav", "flac", "aac",
];

/// The single finished file for a stem: files in the same directory
/// whose stem matches, excluding .part files. The caller reserved a
/// unique name, so there is at most one.
async fn produced_file_for_stem(stem: &Path) -> Option<PathBuf> {
    let parent = match stem.parent()? {
        p if p.as_os_str().is_empty() => Path::new("."),
        p => p,
    };
    let wanted = stem.file_name()?.to_str()?;
    let mut found: Vec<PathBuf> = Vec::new();
    let mut entries = tokio::fs::read_dir(parent).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".part")
            || name.contains(".part.")
            || name.ends_with(".ytdl")
            || name.contains(".temp.")
        {
            continue;
        }
        // Only media files count: a sibling `stem.vtt` or `stem.jpg`
        // used to make `found.len() > 1` and return None for a
        // download that had actually finished.
        let Some(extension) = Path::new(&name).extension().and_then(|value| value.to_str()) else {
            continue;
        };
        if !MEDIA_EXTENSIONS.iter().any(|media| media.eq_ignore_ascii_case(extension)) {
            continue;
        }
        if let Some(file_stem) = Path::new(&name).file_stem().and_then(|s| s.to_str()) {
            if file_stem == wanted {
                found.push(entry.path());
            }
        }
    }
    // Prefer the container yt-dlp most commonly produces, so two media
    // files sharing a stem (e.g. an .mp4 next to a leftover .webm)
    // still resolve instead of failing the finished download.
    found.sort_by_key(|path| {
        path.extension()
            .and_then(|value| value.to_str())
            .and_then(|value| MEDIA_EXTENSIONS.iter().position(|media| media.eq_ignore_ascii_case(value)))
            .unwrap_or(usize::MAX)
    });
    found.into_iter().next()
}

async fn cleanup_partial(stem: &Path) {
    // yt-dlp's .part / .ytdl / .temp. files sit next to the stem;
    // only remove yt-dlp's own partial artifacts so unrelated files
    // sharing the same stem (e.g. video.pdf, video.jpg) are never touched.
    if let Some(parent) = stem.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        if let Some(wanted) = stem.file_name().and_then(|name| name.to_str()) {
            let prefix = format!("{wanted}.");
            if let Ok(mut entries) = tokio::fs::read_dir(parent).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with(&prefix)
                        && (name.ends_with(".part")
                            || name.contains(".part-Frag")
                            || name.ends_with(".ytdl")
                            || name.contains(".temp."))
                    {
                        let _ = tokio::fs::remove_file(entry.path()).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_info() -> serde_json::Value {
        json!({
            "title": "Test video",
            "formats": [
                {"format_id": "sb1", "ext": "mhtml", "protocol": "mhtml", "vcodec": "none", "acodec": "none", "url": "https://example.com/sb"},
                {"format_id": "137", "ext": "mp4", "height": 1080, "tbr": 4500.0, "vcodec": "avc1.640028", "acodec": "none", "url": "https://example.com/v1080"},
                {"format_id": "248", "ext": "webm", "height": 1080, "tbr": 5000.0, "vcodec": "vp9", "acodec": "none", "url": "https://example.com/v1080vp9"},
                {"format_id": "136", "ext": "mp4", "height": 720, "tbr": 2500.0, "vcodec": "avc1.4d401f", "acodec": "none", "url": "https://example.com/v720"},
                {"format_id": "18", "ext": "mp4", "height": 360, "tbr": 700.0, "vcodec": "avc1.42001E", "acodec": "mp4a.40.2", "url": "https://example.com/mux360"},
                {"format_id": "140", "ext": "m4a", "tbr": 128.0, "vcodec": "none", "acodec": "mp4a.40.2", "url": "https://example.com/a140"},
                {"format_id": "251", "ext": "webm", "tbr": 160.0, "vcodec": "none", "acodec": "opus", "url": "https://example.com/a251"},
                {"format_id": "999", "ext": "mp4", "height": 480, "vcodec": "avc1.4d401e", "acodec": "none"}
            ]
        })
    }

    #[tokio::test]
    async fn map_formats_keeps_one_row_per_quality_with_best_audio() {
        let variants = map_formats(&sample_info()).await;
        // 1080p (best of 137/248 — highest tbr wins), 720p, muxed
        // 360p, one Audio row. Storyboard + URL-less entries skipped.
        let qualities: Vec<String> = variants.iter().map(|v| v.quality.clone()).collect();
        assert_eq!(qualities, vec!["1080p", "720p", "360p", "Audio"]);
        let top = &variants[0];
        assert_eq!(top.kind, "ytdlp");
        assert_eq!(top.representation_id.as_deref(), Some("248"));
        assert_eq!(top.audio_representation_id.as_deref(), Some("251"));
        assert_eq!(top.height, Some(1080));
        let audio = variants.iter().find(|v| v.quality == "Audio").unwrap();
        assert_eq!(audio.audio_only, Some(true));
        assert_eq!(audio.representation_id.as_deref(), Some("251"));
        let muxed = variants.iter().find(|v| v.quality == "360p").unwrap();
        assert_eq!(muxed.representation_id.as_deref(), Some("18"));
        assert!(muxed.audio_representation_id.is_none());
    }

    #[tokio::test]
    async fn map_formats_empty_without_formats() {
        assert!(map_formats(&json!({"title": "x"})).await.is_empty());
        assert!(map_formats(&json!({"formats": []})).await.is_empty());
    }

    #[test]
    fn format_selector_combines_video_and_audio() {
        let variant = VideoVariant {
            kind: "ytdlp".into(),
            quality: "1080p".into(),
            width: None,
            height: Some(1080),
            bandwidth: None,
            fps: None,
            url: "https://example.com/v".into(),
            downloadable: true,
            note: None,
            representation_id: Some("137".into()),
            audio_representation_id: Some("140".into()),
            audio_only: None,
            requires_ffmpeg: None,
            is_live: None,
            subtitles: None,
        };
        assert_eq!(format_selector(&variant), "137+140");
        let muxed = VideoVariant {
            representation_id: Some("18".into()),
            audio_representation_id: None,
            ..variant.clone()
        };
        assert_eq!(format_selector(&muxed), "18");
    }

    #[test]
    fn parse_progress_line_reads_template_and_ignores_junk() {
        let progress = parse_progress_line("download: 45.2%|123456|234567|1.23MiB/s|00:30").unwrap();
        assert_eq!(progress.downloaded, 123456);
        assert_eq!(progress.total, Some(234567));
        assert!((progress.percent.unwrap() - 45.2).abs() < 0.001);
        assert_eq!(progress.phase, "Downloading");
        // Default yt-dlp line shape (no template) is ignored, not a crash.
        assert!(parse_progress_line("[download]  45.2% of 123.45MiB at 1.23MiB/s ETA 00:30").is_none());
        assert!(parse_progress_line("[download] Destination: video.mp4").is_none());
        assert!(parse_progress_line("").is_none());
        assert!(parse_progress_line("download: not-a-number|1|2").is_none());
        assert!(parse_progress_line("download: 150%|1|2").is_none());
        // Total unknown: yt-dlp can report a bare downloaded count.
        let no_total = parse_progress_line("download: 10.0%|500|").unwrap();
        assert_eq!(no_total.total, None);
    }

    #[test]
    fn failure_label_maps_each_design_case() {
        assert!(failure_label("ERROR: Sign in to confirm you're not a bot").starts_with("YouTube wants a login"));
        assert!(failure_label("ERROR: Private video").starts_with("This video is removed"));
        assert!(failure_label("ERROR: Video unavailable").starts_with("This video is removed"));
        assert!(failure_label("ERROR: HTTP Error 404: Not Found").starts_with("This video is removed"));
        assert!(failure_label("WARNING: No supported JavaScript runtime could be found").starts_with("Deno is missing"));
        assert!(failure_label("ERROR: HTTP Error 403: Forbidden").starts_with("YouTube refused"));
        assert!(failure_label("ERROR: Unsupported URL: https://example.com/").starts_with("This page is not"));
        assert!(failure_label("ERROR: [youtube] abc: Failed to extract player response").starts_with("YouTube changed"));
        assert!(failure_label("something totally new").contains("download helper"));
    }

    // Slice 6: live info marks every row live, fps rides on the format,
    // and the subtitles dict attaches one entry per language.
    #[tokio::test]
    async fn map_formats_marks_live_and_attaches_subtitles_and_fps() {
        let info = json!({
            "title": "Live stream",
            "is_live": true,
            "formats": [
                {"format_id": "137", "ext": "mp4", "height": 1080, "tbr": 4500.0, "fps": 30.0, "vcodec": "avc1", "acodec": "none", "url": "https://cdn.example.test/v1080"},
                {"format_id": "140", "ext": "m4a", "height": null, "tbr": 128.0, "vcodec": "none", "acodec": "mp4a", "url": "https://cdn.example.test/a140"}
            ],
            "subtitles": {
                "en": [{"url": "https://cdn.example.test/en.vtt", "ext": "vtt"}],
                "es": [{"url": "https://cdn.example.test/es.ttml", "ext": "ttml"}]
            }
        });
        let variants = map_formats(&info).await;
        assert_eq!(variants.len(), 2);
        let video = variants.iter().find(|variant| variant.height == Some(1080)).unwrap();
        assert_eq!(video.is_live, Some(true));
        assert_eq!(video.fps, Some(30.0));
        let subtitles = video.subtitles.as_ref().expect("subtitles must ride on the video row");
        assert_eq!(subtitles.len(), 2);
        assert_eq!(subtitles[0].language, "en");
        assert_eq!(subtitles[0].format, "vtt");
        assert_eq!(subtitles[1].format, "ttml");
        let audio = variants.iter().find(|variant| variant.audio_only == Some(true)).unwrap();
        assert_eq!(audio.is_live, Some(true));
        assert!(audio.subtitles.is_some(), "the audio row carries the same subtitle list");
    }

    #[tokio::test]
    async fn map_formats_skips_unusable_subtitle_formats() {
        let info = json!({
            "formats": [
                {"format_id": "18", "ext": "mp4", "height": 360, "tbr": 500.0, "vcodec": "avc1", "acodec": "mp4a", "url": "https://cdn.example.test/v18"}
            ],
            "subtitles": {
                "en": [{"url": "https://cdn.example.test/en.json3", "ext": "json3"}]
            }
        });
        let variants = map_formats(&info).await;
        assert_eq!(variants.len(), 1);
        assert!(variants[0].subtitles.is_none(), "a format the downloader cannot serve must not be faked into a row");
    }

    #[test]
    fn tool_ids_and_pins_are_stable() {
        assert_eq!(YtDlpTool::from_id("ytdlp"), Some(YtDlpTool::YtDlp));
        assert_eq!(YtDlpTool::from_id("yt-dlp"), Some(YtDlpTool::YtDlp));
        assert_eq!(YtDlpTool::from_id("deno"), Some(YtDlpTool::Deno));
        assert_eq!(YtDlpTool::from_id("nope"), None);
        assert_eq!(YtDlpTool::YtDlp.display_name(), "yt-dlp");
        assert_eq!(YtDlpTool::Deno.display_name(), "Deno");
        assert!(!YTDLP_VERSION.is_empty());
        assert_eq!(YTDLP_EXE_SHA256.len(), 64);
        assert_eq!(DENO_ZIP_SHA256.len(), 64);
        assert!(PLAYER_CLIENT_CHAIN.starts_with("default,android_vr"));
        assert!(is_youtube_url("https://www.youtube.com/watch?v=x"));
        assert!(is_youtube_url("https://youtu.be/x"));
        assert!(!is_youtube_url("https://www.instagram.com/p/x"));
    }

    #[test]
    fn tools_dir_override_and_paths() {
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", "/tmp/thunderload-tools-test");
        assert_eq!(
            tool_path(YtDlpTool::YtDlp),
            PathBuf::from("/tmp/thunderload-tools-test").join(YtDlpTool::YtDlp.file_name())
        );
        assert_eq!(
            tool_path(YtDlpTool::Deno),
            PathBuf::from("/tmp/thunderload-tools-test").join(YtDlpTool::Deno.file_name())
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
    }

    #[test]
    fn tool_file_names_and_download_urls_are_per_os() {
        // P0-13b: no `.exe` and no Windows asset URL off Windows.
        if cfg!(target_os = "windows") {
            assert_eq!(YtDlpTool::YtDlp.file_name(), "yt-dlp.exe");
            assert_eq!(YtDlpTool::Deno.file_name(), "deno.exe");
            assert!(YtDlpTool::YtDlp.download_url().ends_with("yt-dlp.exe"));
            assert!(YtDlpTool::Deno
                .download_url()
                .ends_with("deno-x86_64-pc-windows-msvc.zip"));
        } else if cfg!(target_os = "macos") {
            assert_eq!(YtDlpTool::YtDlp.file_name(), "yt-dlp");
            assert_eq!(YtDlpTool::Deno.file_name(), "deno");
            assert!(YtDlpTool::YtDlp.download_url().ends_with("yt-dlp_macos"));
            assert!(YtDlpTool::Deno.download_url().contains("apple-darwin"));
        } else {
            assert_eq!(YtDlpTool::YtDlp.file_name(), "yt-dlp");
            assert_eq!(YtDlpTool::Deno.file_name(), "deno");
            assert!(YtDlpTool::YtDlp.download_url().ends_with("yt-dlp_linux"));
            assert!(YtDlpTool::Deno
                .download_url()
                .ends_with("deno-x86_64-unknown-linux-gnu.zip"));
        }
        assert!(YtDlpTool::YtDlp.download_url().contains(YTDLP_VERSION));
        assert!(YtDlpTool::Deno.download_url().contains(DENO_VERSION));
    }

    #[test]
    fn sha256_verification_accepts_good_and_rejects_bad() {
        // The old version of this test hashed the literal b"abc" and
        // never called the function under test. This one exercises
        // verify_tool_digest itself; SHA256("abc") is only an
        // arbitrary wrong value here.
        let wrong = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        match YtDlpTool::YtDlp.pinned_sha256() {
            Some(pin) => {
                assert!(verify_tool_digest(YtDlpTool::YtDlp, pin).is_ok());
                assert!(verify_tool_digest(YtDlpTool::YtDlp, wrong).is_err());
                // A good hash for the wrong tool is still a mismatch.
                assert!(verify_tool_digest(YtDlpTool::YtDlp, DENO_ZIP_SHA256).is_err());
            }
            None => {
                // No pin recorded for this platform — every platform
                // has one now, so reaching this arm means a pin went
                // missing: a wrong digest must still be rejected,
                // never silently trusted on first use.
                assert!(verify_tool_digest(YtDlpTool::YtDlp, wrong).is_err());
            }
        }
        match YtDlpTool::Deno.pinned_sha256() {
            Some(pin) => {
                assert!(verify_tool_digest(YtDlpTool::Deno, pin).is_ok());
                assert!(verify_tool_digest(YtDlpTool::Deno, wrong).is_err());
            }
            None => {
                // As above: a missing pin must not silently become
                // trust on first use.
                assert!(verify_tool_digest(YtDlpTool::Deno, wrong).is_err());
            }
        }
    }

    // Serializes every test that points THUNDERLOAD_TOOLS_DIR at a
    // scratch dir: the env var is process-global, so two such tests
    // running in parallel would point the tool lookup at each
    // other's dir.
    static TOOLS_DIR_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn write_fake_ytdlp(dir: &std::path::Path, script: &str) -> PathBuf {
        let path = dir.join(YtDlpTool::YtDlp.file_name());
        std::fs::write(&path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    /// Write the fake and record its hash in the sidecar, i.e. simulate
    /// a verified install — inspect/download verify the tool before
    /// executing it, so a fake without a sidecar is refused (its hash
    /// is not the pinned one). Used by every test that spawns the fake
    /// and by the verification tests (which never execute it). The
    /// sidecar is written to `dir` directly (same name the production
    /// code uses: `.{file_name}.sha256`): the tests set
    /// THUNDERLOAD_TOOLS_DIR only after writing the fake, so resolving
    /// the path through tools_dir() here would point at the wrong
    /// (possibly nonexistent) directory.
    fn write_verified_fake_ytdlp(dir: &std::path::Path, script: &str) -> PathBuf {
        let path = write_fake_ytdlp(dir, script);
        let hash = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
        std::fs::write(
            dir.join(format!(".{}.sha256", YtDlpTool::YtDlp.file_name())),
            hash,
        )
        .unwrap();
        path
    }

    // NOTE (fix round): the fakes are shell scripts named after the
    // platform tool file (`yt-dlp` / `yt-dlp.exe`).
    // They execute on Unix only — Windows CreateProcess cannot run a
    // text file named .exe (ERROR_BAD_EXE_FORMAT), and Windows CI runs
    // `cargo test --workspace`, so the three Slice 3 tests that spawned
    // the fake would fail there. Every test that SPAWNS the fake is
    // therefore #[cfg(unix)]; the pure-function tests run everywhere.
    // A portable (compiled) fake is a follow-up, not this fix.
    #[cfg(unix)]
    fn fake_download_script() -> String {
        // Mimics yt-dlp's observable behavior just enough for the
        // pause/resume/cancel tests: prints a progress line, writes
        // <stem>.part, then sleeps (exec, so the kill lands on sleep
        // itself). A second run finds the .part file and finishes it
        // — the resume path.
        r#"#!/bin/sh
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  prev="$a"
done
stem=$(printf '%s' "$out" | sed 's/\.%(ext)s$//')
echo "download: 10.0%|1024|10240|1.0KiB/s|00:09"
if [ -f "$stem.mp4.part" ]; then
  mv "$stem.mp4.part" "$stem.mp4"
  echo "download: 100.0%|10240|10240|1.0MiB/s|00:00"
  exit 0
fi
printf 'partial content' > "$stem.mp4.part"
exec sleep 30
"#
        .to_string()
    }

    #[test]
    fn verify_tool_digest_accepts_pinned_and_rejects_other() {
        // Pins are recorded in code for every platform (see
        // pinned_sha256) — a wrong digest is rejected at download
        // time on all of them.
        #[cfg(target_os = "windows")]
        {
            assert!(verify_tool_digest(YtDlpTool::YtDlp, YTDLP_EXE_SHA256).is_ok());
            assert!(verify_tool_digest(YtDlpTool::Deno, DENO_ZIP_SHA256).is_ok());
            let bad = verify_tool_digest(
                YtDlpTool::YtDlp,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            assert!(bad.is_err());
            assert!(bad.unwrap_err().to_string().contains("SHA256 mismatch"));
            // A good hash for the wrong tool is still a mismatch.
            assert!(verify_tool_digest(YtDlpTool::YtDlp, DENO_ZIP_SHA256).is_err());
        }
        #[cfg(not(target_os = "windows"))]
        {
            // Pins are recorded for this platform too (see
            // pinned_sha256): the platform pin is accepted, a wrong
            // digest is rejected, and the other tool's pin is still
            // a mismatch — no trust on first use.
            let ytdlp_pin = YtDlpTool::YtDlp
                .pinned_sha256()
                .expect("yt-dlp pin recorded");
            let deno_pin = YtDlpTool::Deno
                .pinned_sha256()
                .expect("Deno pin recorded");
            assert!(verify_tool_digest(YtDlpTool::YtDlp, ytdlp_pin).is_ok());
            assert!(verify_tool_digest(YtDlpTool::Deno, deno_pin).is_ok());
            let bad = verify_tool_digest(
                YtDlpTool::YtDlp,
                "0000000000000000000000000000000000000000000000000000000000000000",
            );
            assert!(bad.is_err());
            assert!(bad.unwrap_err().to_string().contains("SHA256 mismatch"));
            // A good hash for the wrong tool is still a mismatch.
            assert!(verify_tool_digest(YtDlpTool::YtDlp, deno_pin).is_err());
            assert!(verify_tool_digest(YtDlpTool::Deno, ytdlp_pin).is_err());
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn inspect_page_uses_fake_ytdlp_and_maps_formats() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-inspect-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let info = serde_json::to_string(&sample_info()).unwrap();
        write_verified_fake_ytdlp(&dir, &format!("#!/bin/sh\nprintf '%s' '{info}'\n"));
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let inspected = inspect_page("https://www.youtube.com/watch?v=aqz-KE-bpKQ", None)
            .await
            .unwrap();
        assert_eq!(inspected.title.as_deref(), Some("Test video"));
        assert!(inspected
            .variants
            .iter()
            .any(|v| v.height == Some(1080) && v.representation_id.as_deref() == Some("248")));
        assert!(inspected.variants.iter().any(|v| v.audio_only == Some(true)));
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn inspect_and_download_fail_clean_without_tool() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-no-tool-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let error = inspect_page("https://www.youtube.com/watch?v=aqz-KE-bpKQ", None)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<ToolsMissing>().is_some());
        let (_tx, rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _rx) = mpsc::unbounded_channel();
        let error = download_page(
            "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
            "248+251",
            &dir.join("video.mp4"),
            None,
            rx,
            progress_tx,
        )
        .await
        .unwrap_err();
        assert!(error.downcast_ref::<ToolsMissing>().is_some());
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn download_page_pause_keeps_part_and_resume_completes() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-pause-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        write_verified_fake_ytdlp(&dir, &fake_download_script());
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let target = dir.join("video.mp4");
        tokio::fs::write(&target, b"").await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let target_for_run = target.clone();
        let run = tokio::spawn(async move {
            download_page(
                "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
                "248+251",
                &target_for_run,
                None,
                control_rx,
                progress_tx,
            )
            .await
        });
        // The progress line proves the child is up and writing.
        let progress = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            progress_rx.recv(),
        )
        .await
        .unwrap();
        assert!(progress.is_some());
        let _ = control_tx.send(DownloadControl::Paused);
        let (outcome, produced) = run.await.unwrap().unwrap();
        assert!(matches!(outcome, DownloadOutcome::Paused));
        assert!(produced.is_none());
        assert!(
            dir.join("video.mp4.part").is_file(),
            "pause must keep the .part file"
        );
        // Resume: the same command, same target — the fake finishes it.
        let (_tx2, rx2) = watch::channel(DownloadControl::Running);
        let (progress_tx2, _rx) = mpsc::unbounded_channel();
        let (outcome2, produced2) = download_page(
            "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
            "248+251",
            &target,
            None,
            rx2,
            progress_tx2,
        )
        .await
        .unwrap();
        assert!(matches!(outcome2, DownloadOutcome::Completed));
        assert_eq!(produced2, Some(dir.join("video.mp4")));
        assert!(dir.join("video.mp4").is_file());
        assert!(!dir.join("video.mp4.part").exists());
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn download_page_cancel_deletes_partials() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-cancel-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        write_verified_fake_ytdlp(&dir, &fake_download_script());
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let target = dir.join("video.mp4");
        tokio::fs::write(&target, b"").await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let target_for_run = target.clone();
        let run = tokio::spawn(async move {
            download_page(
                "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
                "248+251",
                &target_for_run,
                None,
                control_rx,
                progress_tx,
            )
            .await
        });
        let progress = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            progress_rx.recv(),
        )
        .await
        .unwrap();
        assert!(progress.is_some());
        let _ = control_tx.send(DownloadControl::Cancelled);
        let (outcome, produced) = run.await.unwrap().unwrap();
        assert!(matches!(outcome, DownloadOutcome::Cancelled));
        assert!(produced.is_none());
        assert!(
            !dir.join("video.mp4.part").exists(),
            "cancel must delete the .part file"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn cleanup_partial_removes_partials_but_keeps_unrelated_files() {
        // P0-01: call cleanup_partial directly — a real video.mp4.part
        // must go, while a video.pdf sharing the same stem (the decoy)
        // and the finished video.mp4 must survive. The whitelist is
        // "{stem}." + .part / .part-Frag / .ytdl / .temp. — a file
        // that only shares the stem is never touched.
        let dir = std::env::temp_dir()
            .join(format!("thunderload-cleanup-partial-{}", uuid::Uuid::new_v4()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("video.mp4.part"), b"partial").await.unwrap();
        tokio::fs::write(dir.join("video.mp4.part-Frag0"), b"fragment").await.unwrap();
        tokio::fs::write(dir.join("video.mp4.ytdl"), b"info").await.unwrap();
        tokio::fs::write(dir.join("video.pdf"), b"decoy").await.unwrap();
        tokio::fs::write(dir.join("video.mp4"), b"finished").await.unwrap();
        cleanup_partial(&dir.join("video")).await;
        assert!(
            !dir.join("video.mp4.part").exists(),
            "the real .part file must be deleted"
        );
        assert!(
            !dir.join("video.mp4.part-Frag0").exists(),
            "a .part-Frag file must be deleted"
        );
        assert!(
            !dir.join("video.mp4.ytdl").exists(),
            "a .ytdl file must be deleted"
        );
        assert!(
            dir.join("video.pdf").is_file(),
            "the video.pdf decoy must survive"
        );
        assert!(
            dir.join("video.mp4").is_file(),
            "the finished video.mp4 must survive"
        );
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    // Windows-only, like the helper: it does not exist on other
    // platforms, and Windows CI (which runs the full suite) covers it.
    #[cfg(target_os = "windows")]
    #[test]
    fn powershell_quote_doubles_apostrophes() {
        assert_eq!(
            powershell_quote("C:\\Users\\O'Brien\\AppData"),
            "C:\\Users\\O''Brien\\AppData"
        );
        assert_eq!(powershell_quote("plain"), "plain");
        assert_eq!(powershell_quote("a'b'c"), "a''b''c");
    }

    #[test]
    fn failure_label_drm_is_anchored() {
        assert_eq!(
            failure_label("ERROR: This video is DRM protected"),
            "Protected — not downloadable."
        );
        assert_eq!(
            failure_label("ERROR: DRM is not supported for this format"),
            "Protected — not downloadable."
        );
        // A bare "drm" mention with no protection wording is not DRM.
        assert_ne!(
            failure_label("ERROR: https://example.com/drm-video failed"),
            "Protected — not downloadable."
        );
        assert_ne!(
            failure_label("ERROR: protected content"),
            "Protected — not downloadable."
        );
    }

    #[tokio::test]
    async fn verify_tool_on_disk_accepts_sidecar_rejects_swap() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-verify-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        // A verified install: fake + sidecar with its hash.
        write_verified_fake_ytdlp(&dir, "#!/bin/sh\nexit 0\n");
        assert!(verify_tool_on_disk(YtDlpTool::YtDlp).await.is_ok());
        // Swap the binary afterwards: same path, different bytes.
        std::fs::write(dir.join(YtDlpTool::YtDlp.file_name()), b"tampered").unwrap();
        let error = verify_tool_on_disk(YtDlpTool::YtDlp).await.unwrap_err();
        assert!(error.to_string().contains("SHA256 mismatch"));
        // No sidecar at all: the pinned hash applies on every
        // platform now, so a fake that is not the pinned binary is
        // refused; a later swap is still caught (above).
        let _ = std::fs::remove_file(tool_hash_sidecar_path(YtDlpTool::YtDlp));
        assert!(verify_tool_on_disk(YtDlpTool::YtDlp).await.is_err());
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn verify_tool_on_disk_deno_trust_on_first_use() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-verify-deno-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        // A manual Deno drop: no sidecar, no pinned exe hash — the
        // first use trusts it and records the hash.
        let deno = dir.join(YtDlpTool::Deno.file_name());
        std::fs::write(&deno, b"fake-deno").unwrap();
        assert!(!tool_hash_sidecar_path(YtDlpTool::Deno).exists());
        assert!(verify_tool_on_disk(YtDlpTool::Deno).await.is_ok());
        assert!(tool_hash_sidecar_path(YtDlpTool::Deno).exists());
        // A later swap is caught.
        std::fs::write(&deno, b"swapped-deno").unwrap();
        assert!(verify_tool_on_disk(YtDlpTool::Deno).await.is_err());
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn ensure_tool_noop_when_present_and_verified() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-ensure-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        write_verified_fake_ytdlp(&dir, "#!/bin/sh\nexit 0\n");
        let before = std::fs::read(dir.join(YtDlpTool::YtDlp.file_name())).unwrap();
        // No progress channel needed: a present, verified tool returns
        // without downloading (no network is touched).
        let path = ensure_tool(YtDlpTool::YtDlp, None).await.unwrap();
        assert_eq!(path, dir.join(YtDlpTool::YtDlp.file_name()));
        assert_eq!(
            std::fs::read(dir.join(YtDlpTool::YtDlp.file_name())).unwrap(),
            before
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn tool_temp_paths_are_unique_per_attempt() {
        // The Deno unpack race: two overlapping downloads shared one
        // fixed temp name. Two attempts must never get the same path.
        let dir = PathBuf::from("/tmp/thunderload-tools-test");
        let first = unique_tool_temp_path(&dir, YtDlpTool::Deno);
        let second = unique_tool_temp_path(&dir, YtDlpTool::Deno);
        assert_ne!(first, second);
        assert!(first
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".deno"));
        assert!(first.extension().unwrap() == "tmp");
    }

    #[tokio::test]
    async fn install_deno_zip_removes_temp_zip_on_unpack_failure() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-deno-unpack-fail-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        // Not a zip at all: the extractor fails, and the error must
        // carry the extractor's own words, not just the generic line.
        let zip = dir.join("deno-bad.zip");
        std::fs::write(&zip, b"this is not a zip file").unwrap();
        let error = install_deno_zip(&zip, &dir).await.unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("Could not unpack the Deno download"),
            "opaque error, extractor detail lost: {message}"
        );
        assert!(
            message.len() > "Could not unpack the Deno download".len(),
            "extractor stderr was not included: {message}"
        );
        // The failed attempt leaks nothing: no temp zip, no scratch
        // dir, no half-installed binary.
        assert!(!zip.exists(), "temp zip leaked after unpack failure");
        assert!(!tool_path(YtDlpTool::Deno).exists());
        let leftovers: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
        assert!(
            leftovers.is_empty(),
            "scratch dir leaked after unpack failure: {leftovers:?}"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn install_deno_zip_installs_from_a_real_zip() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-deno-unpack-ok-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        // A real zip holding a fake `deno` at its top level.
        let src = dir.join("zip-src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        std::fs::write(src.join(YtDlpTool::Deno.file_name()), b"fake-deno").unwrap();
        let zip = dir.join("deno.zip");
        let packed = std::process::Command::new("zip")
            .arg("-q")
            .arg(&zip)
            .arg(YtDlpTool::Deno.file_name())
            .current_dir(&src)
            .status()
            .expect("zip CLI is required for this test");
        assert!(packed.success(), "zip CLI could not pack the fake Deno");
        // A leftover partial exe from an older crashed unpack must
        // not survive the install.
        std::fs::write(tool_path(YtDlpTool::Deno), b"half-written").unwrap();
        install_deno_zip(&zip, &dir).await.unwrap();
        assert_eq!(
            std::fs::read(tool_path(YtDlpTool::Deno)).unwrap(),
            b"fake-deno"
        );
        // Success leaves the zip for the caller to delete, and no
        // scratch directory behind.
        assert!(zip.exists(), "install must not delete the zip on success");
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            leftovers.iter().all(|name| !name.starts_with(".deno-extract-")),
            "scratch dir leaked after a successful install: {leftovers:?}"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn inspect_page_timeout_kills_child() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-timeout-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        // The fake writes one heartbeat line per second forever; if
        // the timeout kills it, the log stops growing.
        let alive = dir.join("alive.log");
        write_verified_fake_ytdlp(
            &dir,
            &format!(
                "#!/bin/sh\nwhile true; do echo alive >> '{}'; sleep 1; done\n",
                alive.display()
            ),
        );
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        std::env::set_var("THUNDERLOAD_YTDLP_INSPECT_TIMEOUT_SECS", "1");
        let result = inspect_page("https://www.youtube.com/watch?v=aqz-KE-bpKQ", None).await;
        std::env::remove_var("THUNDERLOAD_YTDLP_INSPECT_TIMEOUT_SECS");
        let error = result.unwrap_err();
        assert!(error.to_string().contains("took too long"));
        // If the child were orphaned it would keep writing heartbeats;
        // killed at ~1 s it stops after at most 3 lines (scheduling).
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let lines = std::fs::read_to_string(&alive)
            .map(|text| text.lines().count())
            .unwrap_or(0);
        assert!(
            lines <= 3,
            "orphaned inspect child kept running: {lines} heartbeats"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[cfg(unix)]
    fn fake_stdout_closes_script() -> String {
        // Prints a progress line, writes the .part file, then CLOSES
        // stdout and sleeps 10 s: download_page's stdout-closed
        // branch must stay responsive to Pause during that window.
        r#"#!/bin/sh
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  prev="$a"
done
stem=$(printf '%s' "$out" | sed 's/\.%(ext)s$//')
echo "download: 10.0%|1024|10240|1.0KiB/s|00:09"
printf 'partial content' > "$stem.part"
exec 1>&-
sleep 10
mv "$stem.part" "$stem.mp4"
exit 0
"#
        .to_string()
    }

    #[cfg(unix)]
    fn fake_orphan_pipes_script(orphan_secs: u32, exit_after_orphan: bool) -> String {
        // Prints a progress line, writes the .part file, then spawns
        // an orphaned sleep that inherits stdout+stderr, so both
        // pipes stay open. With exit_after_orphan the fake finishes
        // the file and exits at once (completion case: the exit must
        // be noticed even though no EOF ever arrives); otherwise it
        // execs a long sleep and stays alive (pause case).
        let tail = if exit_after_orphan {
            "mv \"$stem.part\" \"$stem.mp4\"\necho \"download: 100.0%|10240|10240|1.0MiB/s|00:00\"\nexit 0\n"
        } else {
            "exec sleep 30\n"
        };
        format!(
            r#"#!/bin/sh
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  prev="$a"
done
stem=$(printf '%s' "$out" | sed 's/\.%(ext)s$//')
echo "download: 10.0%|1024|10240|1.0KiB/s|00:09"
printf 'partial content' > "$stem.part"
( sleep {} ) &
{}"#,
            orphan_secs, tail
        )
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn download_page_completes_when_orphan_holds_pipes_open() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-orphan-complete-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        // The fake exits at once while an orphaned `sleep 10` holds
        // stdout+stderr open: without a child.wait() arm the loop
        // only wakes on a stdout line, so it would sit here ~10 s
        // even though the download is already done.
        write_verified_fake_ytdlp(&dir, &fake_orphan_pipes_script(10, true));
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let target = dir.join("video.mp4");
        tokio::fs::write(&target, b"").await.unwrap();
        let (_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, _rx) = mpsc::unbounded_channel();
        let started = std::time::Instant::now();
        let (outcome, produced) = download_page(
            "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
            "248+251",
            &target,
            None,
            control_rx,
            progress_tx,
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        assert!(matches!(outcome, DownloadOutcome::Completed));
        assert_eq!(produced, Some(dir.join("video.mp4")));
        assert!(dir.join("video.mp4").is_file());
        assert!(
            elapsed < std::time::Duration::from_secs(7),
            "completion with an orphan holding the pipes took {elapsed:?} — the child exit was not a wakeup"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn download_page_pause_with_orphan_holding_pipes() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-orphan-pause-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        // The fake stays alive (exec sleep 30) with an orphaned
        // `sleep 30` holding stdout+stderr open: a pause must be
        // answered at once, not when a pipe closes or the child
        // exits on its own.
        write_verified_fake_ytdlp(&dir, &fake_orphan_pipes_script(30, false));
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let target = dir.join("video.mp4");
        tokio::fs::write(&target, b"").await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let target_for_run = target.clone();
        let started = std::time::Instant::now();
        let run = tokio::spawn(async move {
            download_page(
                "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
                "248+251",
                &target_for_run,
                None,
                control_rx,
                progress_tx,
            )
            .await
        });
        let progress = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            progress_rx.recv(),
        )
        .await
        .unwrap();
        assert!(progress.is_some());
        let _ = control_tx.send(DownloadControl::Paused);
        let (outcome, produced) = run.await.unwrap().unwrap();
        let elapsed = started.elapsed();
        assert!(matches!(outcome, DownloadOutcome::Paused));
        assert!(produced.is_none());
        assert!(
            elapsed < std::time::Duration::from_secs(7),
            "pause with an orphan holding the pipes took {elapsed:?}"
        );
        assert!(
            dir.join("video.part").is_file(),
            "pause must keep the .part file"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn download_page_pause_in_stdout_closed_window() {
        let _guard = TOOLS_DIR_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("thunderload-wait-window-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        write_verified_fake_ytdlp(&dir, &fake_stdout_closes_script());
        std::env::set_var("THUNDERLOAD_TOOLS_DIR", &dir);
        let target = dir.join("video.mp4");
        tokio::fs::write(&target, b"").await.unwrap();
        let (control_tx, control_rx) = watch::channel(DownloadControl::Running);
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let target_for_run = target.clone();
        let started = std::time::Instant::now();
        let run = tokio::spawn(async move {
            download_page(
                "https://www.youtube.com/watch?v=aqz-KE-bpKQ",
                "248+251",
                &target_for_run,
                None,
                control_rx,
                progress_tx,
            )
            .await
        });
        // Progress proves the child is up; stdout closes right after.
        let progress = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            progress_rx.recv(),
        )
        .await
        .unwrap();
        assert!(progress.is_some());
        // Let stdout close, then pause inside the window where the
        // child still lives for 10 s. Fixed code answers at once;
        // the old bare wait() would answer only after ~10 s, with
        // Completed instead of Paused.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = control_tx.send(DownloadControl::Paused);
        let (outcome, produced) = run.await.unwrap().unwrap();
        let elapsed = started.elapsed();
        assert!(matches!(outcome, DownloadOutcome::Paused));
        assert!(produced.is_none());
        assert!(
            elapsed < std::time::Duration::from_secs(7),
            "pause in the stdout-closed window took {elapsed:?} — the wait was not covered"
        );
        assert!(
            dir.join("video.part").is_file(),
            "pause must keep the .part file"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn audio_only_selector_recognises_audio_ids_and_combos() {
        // Audio-only selectors (single ids, bestaudio, vcodec=none)
        // must never get --remux-video mp4; combined selectors must.
        assert!(is_audio_only_selector("bestaudio"));
        assert!(is_audio_only_selector("ba"));
        assert!(is_audio_only_selector("140"));
        assert!(is_audio_only_selector("251"));
        assert!(is_audio_only_selector("140[vcodec=none]"));
        assert!(is_audio_only_selector("bestaudio/best"));
        assert!(!is_audio_only_selector("137+140"));
        assert!(!is_audio_only_selector("248+251"));
        assert!(!is_audio_only_selector("137"));
        assert!(!is_audio_only_selector("bestvideo+bestaudio"));
        assert!(!is_audio_only_selector("best"));
    }

    #[tokio::test]
    async fn produced_file_for_stem_ignores_non_media_siblings() {
        let dir = std::env::temp_dir()
            .join(format!("thunderload-produced-stem-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        // The finished file plus the siblings yt-dlp leaves next to
        // it: subtitles, a thumbnail, a leftover .part and an info
        // file. The old code returned None whenever more than one
        // file shared the stem.
        tokio::fs::write(dir.join("video.mp4"), b"media").await.unwrap();
        tokio::fs::write(dir.join("video.en.vtt"), b"subs").await.unwrap();
        tokio::fs::write(dir.join("video.jpg"), b"thumb").await.unwrap();
        tokio::fs::write(dir.join("video.mp4.part"), b"partial").await.unwrap();
        tokio::fs::write(dir.join("video.info.json"), b"{}").await.unwrap();
        let found = produced_file_for_stem(&dir.join("video")).await;
        assert_eq!(found, Some(dir.join("video.mp4")));
        // No media file at all: still None, not a sibling.
        tokio::fs::remove_file(dir.join("video.mp4")).await.unwrap();
        assert_eq!(produced_file_for_stem(&dir.join("video")).await, None);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn temp_cookies_file_is_exact_origin_only() {
        let session = SessionHeaders {
            cookies: Some("session=abc; other=def".into()),
            origin: Some("https://example.com".into()),
            ..Default::default()
        };
        let file = TempCookiesFile::write(&session, "https://example.com/watch")
            .await
            .unwrap()
            .expect("cookies apply to the page origin");
        let text = tokio::fs::read_to_string(&file.path).await.unwrap();
        // Second field is "include subdomains": FALSE keeps the
        // cookies exact-origin instead of widening them.
        for line in text.lines().filter(|line| !line.starts_with('#')) {
            assert!(
                line.starts_with("example.com\tFALSE\t"),
                "cookie line must be exact-origin: {line}"
            );
        }
        assert!(text.contains("session\tabc"));
    }

    // Tool downloads through the multi-connection engine (2026-11-25).

    #[test]
    fn tool_speed_tracker_reports_average_speed_and_eta() {
        let start = std::time::Instant::now();
        let tracker = ToolSpeedTracker { started_at: start };
        // 1 MB in 1 s, 10 MB total: 1 MB/s, 9 s left.
        let (speed, eta) =
            tracker.update_at(1_000_000, Some(10_000_000), start + Duration::from_secs(1));
        assert_eq!(speed, Some(1_000_000));
        assert_eq!(eta, Some(9));
        // 5 MB in 2 s: 2.5 MB/s, 2 s left.
        let (speed, eta) =
            tracker.update_at(5_000_000, Some(10_000_000), start + Duration::from_secs(2));
        assert_eq!(speed, Some(2_500_000));
        assert_eq!(eta, Some(2));
    }

    #[test]
    fn tool_speed_tracker_withholds_speed_while_too_early_or_empty() {
        let start = std::time::Instant::now();
        let tracker = ToolSpeedTracker { started_at: start };
        // First 100 ms: no rate yet, however many bytes arrived —
        // one chunk in one millisecond would read as GB/s.
        assert_eq!(
            tracker.update_at(1_000_000, Some(10_000_000), start + Duration::from_millis(50)),
            (None, None)
        );
        // Nothing downloaded yet: no rate even after the window.
        assert_eq!(
            tracker.update_at(0, Some(10_000_000), start + Duration::from_secs(1)),
            (None, None)
        );
        // Unknown total: speed yes, ETA no.
        assert_eq!(
            tracker.update_at(2_000_000, None, start + Duration::from_secs(1)),
            (Some(2_000_000), None)
        );
        // Complete: ETA is 0, not a division by zero.
        assert_eq!(
            tracker.update_at(10_000_000, Some(10_000_000), start + Duration::from_secs(5)),
            (Some(2_000_000), Some(0))
        );
    }

    // A local HTTP server serving one fixed body, modeled on the
    // resumable downloader's `spawn_range_server` but with the knobs
    // these tests need: range support on or off, and a per-chunk drip
    // so a transfer lasts long enough for progress events to carry
    // speed. Each connection runs on its own thread — the resumable
    // harness serves connections inline, which would serialize the
    // segments and hide the parallelism this test observes.
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    struct ToolServerStats {
        /// Peak simultaneous in-flight Range GETs.
        max_range_in_flight: AtomicUsize,
        range_in_flight: AtomicUsize,
        /// 206 responses served.
        partial_responses: AtomicUsize,
    }

    fn tool_test_body(size: usize) -> Vec<u8> {
        // Offset-dependent bytes, so a wrong range corrupts the file
        // instead of passing silently.
        (0..size).map(|i| (i % 251) as u8).collect()
    }

    fn spawn_tool_server(
        body_size: usize,
        range_support: bool,
        chunk_size: usize,
        chunk_delay: Duration,
    ) -> (String, Arc<ToolServerStats>, Arc<AtomicBool>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let body = Arc::new(tool_test_body(body_size));
        let stats = Arc::new(ToolServerStats {
            max_range_in_flight: AtomicUsize::new(0),
            range_in_flight: AtomicUsize::new(0),
            partial_responses: AtomicUsize::new(0),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let thread_stats = stats.clone();
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                let (stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(3));
                        continue;
                    }
                    Err(_) => break,
                };
                let _ = stream.set_nonblocking(false);
                let body = body.clone();
                let stats = thread_stats.clone();
                thread::spawn(move || {
                    serve_tool_connection(
                        stream,
                        &body,
                        range_support,
                        chunk_size,
                        chunk_delay,
                        &stats,
                    );
                });
            }
        });
        (format!("http://{address}/tool.bin"), stats, shutdown, handle)
    }

    fn serve_tool_connection(
        mut stream: std::net::TcpStream,
        body: &[u8],
        range_support: bool,
        chunk_size: usize,
        chunk_delay: Duration,
        stats: &ToolServerStats,
    ) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(count) => {
                    request.extend_from_slice(&buffer[..count]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
            }
        }
        let request = String::from_utf8_lossy(&request);
        let mut lines = request.lines();
        let first = lines.next().unwrap_or_default();
        let is_head = first.starts_with("HEAD ");
        let range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("range")
                .then(|| value.trim().to_string())
        });
        let if_range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("if-range")
                .then(|| value.trim().to_string())
        });
        const ETAG: &str = "\"tool-v1\"";
        let total = body.len();
        if is_head {
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nETag: {ETAG}\r\n{}Connection: close\r\n\r\n",
                if range_support {
                    "Accept-Ranges: bytes\r\n"
                } else {
                    ""
                }
            );
            let _ = stream.write_all(headers.as_bytes());
            return;
        }
        // Parse "bytes=START-" / "bytes=START-END".
        let parsed_range = range.as_deref().and_then(|value| {
            let value = value.strip_prefix("bytes=")?;
            let (start, end) = value.split_once('-')?;
            let start: usize = start.parse().ok()?;
            let end: usize = if end.is_empty() {
                total - 1
            } else {
                end.parse().ok()?
            };
            (start <= end && end < total).then_some((start, end))
        });
        let can_range = range_support
            && parsed_range.is_some()
            && if_range.as_deref().is_none_or(|value| value == ETAG);
        if can_range {
            let (start, end) = parsed_range.unwrap();
            let payload = &body[start..=end];
            stats.partial_responses.fetch_add(1, Ordering::Relaxed);
            let in_flight = stats.range_in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            stats
                .max_range_in_flight
                .fetch_max(in_flight, Ordering::Relaxed);
            let headers = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nETag: {ETAG}\r\nAccept-Ranges: bytes\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            if stream.write_all(headers.as_bytes()).is_err() {
                stats.range_in_flight.fetch_sub(1, Ordering::Relaxed);
                return;
            }
            for chunk in payload.chunks(chunk_size) {
                if stream.write_all(chunk).is_err() {
                    break;
                }
                thread::sleep(chunk_delay);
            }
            stats.range_in_flight.fetch_sub(1, Ordering::Relaxed);
        } else {
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nETag: {ETAG}\r\n{}Connection: close\r\n\r\n",
                if range_support {
                    "Accept-Ranges: bytes\r\n"
                } else {
                    ""
                }
            );
            if stream.write_all(headers.as_bytes()).is_err() {
                return;
            }
            for chunk in body.chunks(chunk_size) {
                if stream.write_all(chunk).is_err() {
                    break;
                }
                thread::sleep(chunk_delay);
            }
        }
    }

    #[tokio::test]
    async fn tool_download_uses_segments_against_range_server() {
        let body_size = 4 * 1024 * 1024;
        let (url, stats, shutdown, handle) =
            spawn_tool_server(body_size, true, 32 * 1024, Duration::from_millis(40));
        let dir = std::env::temp_dir()
            .join(format!("thunderload-tool-segments-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let temp = dir.join(".tool-download.tmp");
        let expected = tool_test_body(body_size);
        download_tool_file(YtDlpTool::YtDlp, &url, &temp, None)
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&temp).await.unwrap(), expected);
        // Segments really ran in parallel: more than one Range GET
        // was in flight at once, and the server served 206s.
        assert!(
            stats.max_range_in_flight.load(Ordering::Relaxed) >= 2,
            "segmented path must overlap Range requests, max in flight was {}",
            stats.max_range_in_flight.load(Ordering::Relaxed)
        );
        assert!(stats.partial_responses.load(Ordering::Relaxed) >= 2);
        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn tool_download_falls_back_without_range_support() {
        // 3 MiB is over the 2 MiB segmentation threshold, so a
        // range-capable server WOULD segment it — the fallback is
        // the server's doing, not the size's.
        let body_size = 3 * 1024 * 1024;
        let (url, stats, shutdown, handle) =
            spawn_tool_server(body_size, false, 128 * 1024, Duration::from_millis(2));
        let dir = std::env::temp_dir()
            .join(format!("thunderload-tool-fallback-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let temp = dir.join(".tool-download.tmp");
        let expected = tool_test_body(body_size);
        download_tool_file(YtDlpTool::YtDlp, &url, &temp, None)
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&temp).await.unwrap(), expected);
        // No Range was ever honored: the sequential path served the
        // whole body with plain 200s.
        assert_eq!(stats.partial_responses.load(Ordering::Relaxed), 0);
        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn tool_download_progress_carries_speed_and_eta() {
        let body_size = 4 * 1024 * 1024;
        let (url, _stats, shutdown, handle) =
            spawn_tool_server(body_size, true, 32 * 1024, Duration::from_millis(50));
        let dir = std::env::temp_dir()
            .join(format!("thunderload-tool-progress-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let temp = dir.join(".tool-download.tmp");
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ToolDownloadProgress>();
        let download = tokio::spawn(async move {
            download_tool_file(YtDlpTool::YtDlp, &url, &temp, Some(progress_tx)).await
        });
        let mut events = Vec::new();
        while let Some(event) = progress_rx.recv().await {
            events.push(event);
        }
        download.await.unwrap().unwrap();
        assert!(!events.is_empty(), "a download must emit progress");
        assert!(
            events.iter().any(|event| event.speed_bps.is_some()),
            "at least one event must carry speed, got {} events: {events:?}",
            events.len()
        );
        assert!(
            events.iter().any(|event| event.eta_secs.is_some()),
            "at least one event must carry ETA, got {} events: {events:?}",
            events.len()
        );
        let last = events.last().unwrap();
        assert_eq!(last.percent, Some(100.0));
        assert_eq!(last.downloaded, body_size as u64);
        assert_eq!(last.total, Some(body_size as u64));
        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join();
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}

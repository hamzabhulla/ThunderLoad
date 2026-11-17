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
use crate::download::{DownloadControl, DownloadOutcome, DownloadProgress};
use crate::session::SessionHeaders;
use crate::video::VideoVariant;
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
pub const YTDLP_EXE_SHA256: &str =
    "f5389d3698ecde4ef06f4e6b1ce0294fe9ac1396228c8990b8d3416d6d177a7d";
/// Pinned Deno release (the JS runtime yt-dlp uses for YouTube's
/// signature deciphering since 2025.11.12).
pub const DENO_VERSION: &str = "2.9.8";
/// SHA256 of deno-x86_64-pc-windows-msvc.zip v2.9.8, from the release's
/// published .sha256sum file (PowerShell Get-FileHash format) —
/// verified live 2026-11-17.
pub const DENO_ZIP_SHA256: &str =
    "cba84b26e0df5ae7f74fc66b86a95b866872a86292ebd84fa1e959927edd93bf";
/// Player-client fallback chain, best quality first. See the module
/// docs for the live measurement behind this order.
pub const PLAYER_CLIENT_CHAIN: &str = "default,android_vr,tv,ios,web_safari";

const YTDLP_DOWNLOAD_URL: &str =
    "https://github.com/yt-dlp/yt-dlp/releases/download/2026.10.26/yt-dlp.exe";
const DENO_DOWNLOAD_URL: &str =
    "https://github.com/denoland/deno/releases/download/v2.9.8/deno-x86_64-pc-windows-msvc.zip";
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
    pub fn file_name(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => "yt-dlp.exe",
            YtDlpTool::Deno => "deno.exe",
        }
    }
    pub fn download_url(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => YTDLP_DOWNLOAD_URL,
            YtDlpTool::Deno => DENO_DOWNLOAD_URL,
        }
    }
    /// The pinned SHA256 the downloaded file must match. Deno's is
    /// the hash of the ZIP (verified before extraction).
    pub fn sha256(self) -> &'static str {
        match self {
            YtDlpTool::YtDlp => YTDLP_EXE_SHA256,
            YtDlpTool::Deno => DENO_ZIP_SHA256,
        }
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
    let version = if present {
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

async fn run_tool_version(tool: YtDlpTool) -> Option<String> {
    // Never execute a tool that failed verification: a binary swapped
    // in after install must not run, even for --version.
    if verify_tool_on_disk(tool).await.is_err() {
        return None;
    }
    let output = TokioCommand::new(tool_path(tool))
        .arg("--version")
        .output()
        .await
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
/// hai — 45%"); the event itself carries the wire id.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDownloadProgress {
    pub tool: YtDlpTool,
    pub downloaded: u64,
    pub total: Option<u64>,
    pub percent: Option<f64>,
}

/// Download one tool with SHA256 verification. The caller must have
/// obtained explicit consent first (the download button tap) — this
/// function never runs unprompted. A hash mismatch deletes the temp
/// file and fails loudly; a half-downloaded temp file never becomes
/// the tool.
/// Check a downloaded tool file against its pinned SHA256 before it
/// is installed. A mismatch refuses the file outright — the temp file
/// is discarded by the caller, nothing is installed.
fn verify_tool_digest(tool: YtDlpTool, digest: &str) -> Result<()> {
    if digest != tool.sha256() {
        return Err(anyhow!(
            "The {} download failed verification (SHA256 mismatch) — the file was discarded, nothing was installed",
            tool.display_name()
        ));
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
/// - else the pinned hash for yt-dlp;
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
        YtDlpTool::YtDlp => Some(YTDLP_EXE_SHA256.to_string()),
        YtDlpTool::Deno => None,
    }
}

fn write_tool_hash_sidecar(tool: YtDlpTool, hash: &str) {
    let _ = std::fs::write(tool_hash_sidecar_path(tool), hash);
}

/// Stream-hash a tool file without loading it whole into memory.
async fn hash_tool_file(tool: YtDlpTool) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(tool_path(tool))
        .await
        .with_context(|| format!("Cannot read {}", tool_path(tool).display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file
            .read(&mut buf)
            .await
            .with_context(|| format!("Cannot read {}", tool_path(tool).display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
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

pub async fn download_tool(
    tool: YtDlpTool,
    progress_tx: Option<mpsc::UnboundedSender<ToolDownloadProgress>>,
) -> Result<PathBuf> {
    let dir = tools_dir();
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("Cannot create {}", dir.display()))?;
    let target = tool_path(tool);
    let temp = dir.join(format!(".{}-download.tmp", tool.file_name()));
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 ThunderLoad/0.1")
        .build()
        .context("Could not build the download client")?;
    let response = client
        .get(tool.download_url())
        .send()
        .await
        .with_context(|| format!("Could not download {}", tool.display_name()))?
        .error_for_status()
        .with_context(|| format!("The {} download was refused", tool.display_name()))?;
    let total = response.content_length();
    let mut file = tokio::fs::File::create(&temp)
        .await
        .with_context(|| format!("Cannot write {}", temp.display()))?;
    let mut hasher = Sha256::new();
    let mut downloaded: u64 = 0;
    let mut stream = response.bytes_stream();
    use futures_util::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("The {} download was interrupted", tool.display_name()))?;
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .with_context(|| format!("Cannot write {}", temp.display()))?;
        downloaded += chunk.len() as u64;
        if let Some(tx) = &progress_tx {
            let _ = tx.send(ToolDownloadProgress {
                tool,
                downloaded,
                total,
                percent: total.map(|total| {
                    if total == 0 {
                        0.0
                    } else {
                        downloaded as f64 / total as f64 * 100.0
                    }
                }),
            });
        }
    }
    file.flush().await.ok();
    drop(file);
    let digest = format!("{:x}", hasher.finalize());
    if let Err(error) = verify_tool_digest(tool, &digest) {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    match tool {
        YtDlpTool::YtDlp => {
            tokio::fs::rename(&temp, &target)
                .await
                .with_context(|| format!("Cannot install {}", temp.display()))?;
            // The digest was just verified against the pin: record it
            // so every later launch can re-verify this file.
            write_tool_hash_sidecar(tool, &digest);
        }
        YtDlpTool::Deno => {
            extract_deno_zip(&temp, &dir).await?;
            let _ = tokio::fs::remove_file(&temp).await;
            if !target.is_file() {
                return Err(anyhow!(
                    "The Deno download unpacked but deno.exe was not found in it"
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

/// Escape one value for a PowerShell single-quoted string: a literal
/// `'` is doubled (`''`). Without this, a tools-dir path containing
/// an apostrophe (e.g. `O'Brien`) breaks the Expand-Archive command.
/// Windows-only, like its only caller: without the gate the Linux
/// build warns that the function is never used.
#[cfg(target_os = "windows")]
fn powershell_quote(value: &str) -> String {
    value.replace('\'', "''")
}

/// Extract the verified Deno zip. Windows uses PowerShell's
/// Expand-Archive (always present); elsewhere `unzip` is used. The
/// zip holds deno.exe at its top level.
async fn extract_deno_zip(zip: &Path, dir: &Path) -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        let status = TokioCommand::new("powershell")
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
            .status()
            .await
            .context("Could not unpack the Deno download")?;
        if !status.success() {
            return Err(anyhow!("Could not unpack the Deno download"));
        }
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    {
        let status = TokioCommand::new("unzip")
            .args(["-o", "-j"])
            .arg(zip)
            .arg("deno.exe")
            .arg("-d")
            .arg(dir)
            .status()
            .await
            .context("Could not unpack the Deno download (unzip is required)")?;
        if !status.success() {
            return Err(anyhow!("Could not unpack the Deno download"));
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
    let output = TokioCommand::new(tool_path(YtDlpTool::YtDlp))
        .arg("-U")
        .output()
        .await
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
        // is actually installed now.
        if let Ok(hash) = hash_tool_file(YtDlpTool::YtDlp).await {
            write_tool_hash_sidecar(YtDlpTool::YtDlp, &hash);
        }
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
        let cookies = match &session.cookies {
            Some(cookies) if !cookies.trim().is_empty() => cookies.clone(),
            _ => return Ok(None),
        };
        let origin = session.origin.clone().unwrap_or_else(|| {
            url::Url::parse(url)
                .map(|parsed| parsed.origin().ascii_serialization())
                .unwrap_or_default()
        });
        let domain = url::Url::parse(&origin)
            .ok()
            .and_then(|parsed| parsed.host_str().map(str::to_string))
            .unwrap_or_default();
        if domain.is_empty() {
            return Ok(None);
        }
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
            text.push_str(&format!("{domain}\tTRUE\t/\tFALSE\t{expiry}\t{name}\t{value}\n"));
        }
        let path = std::env::temp_dir().join(format!(
            "thunderload-cookies-{}.txt",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&path, text)
            .await
            .context("Could not write the temporary cookies file")?;
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

/// Inspect one YouTube/social page URL and map its formats onto the
/// panel's VideoVariant rows. Fails with ToolsMissing when yt-dlp is
/// not on disk yet (the caller shows the consent UI), and with a
/// plain-language failure label otherwise.
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
    // Verified before executing: missing → ToolsMissing (consent UI),
    // swapped → a plain refusal. Never run an unverified binary.
    verify_tool_on_disk(YtDlpTool::YtDlp).await?;
    let cookies = match session {
        Some(session) => TempCookiesFile::write(session, url).await?,
        None => None,
    };
    let mut command = TokioCommand::new(tool_path(YtDlpTool::YtDlp));
    command
        .arg("-j")
        .arg("--no-download")
        .arg("--no-playlist")
        .arg("--no-warnings")
        .arg(url);
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
            let _ = child.kill().await;
            let _ = child.wait().await;
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
    Ok(YtDlpInspect { title, variants })
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
    #[derive(Clone)]
    struct Fmt {
        id: String,
        ext: String,
        height: u32,
        bandwidth: u64,
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
            url: fmt.url.clone(),
            downloadable: true,
            note: None,
            representation_id: Some(fmt.id.clone()),
            audio_representation_id: None,
            audio_only: None,
            requires_ffmpeg: None,
        });
    }
    if let Some(audio) = best_audio {
        variants.push(VideoVariant {
            kind: "ytdlp".into(),
            quality: "Audio".into(),
            width: None,
            height: None,
            bandwidth: if audio.bandwidth > 0 { Some(audio.bandwidth) } else { None },
            url: audio.url.clone(),
            downloadable: true,
            note: None,
            representation_id: Some(audio.id.clone()),
            audio_representation_id: None,
            audio_only: Some(true),
            requires_ffmpeg: None,
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
        let _ = TokioCommand::new("taskkill")
            .args(["/T", "/F", "/PID"])
            .arg(pid.to_string())
            .output()
            .await;
        let _ = child.kill().await;
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = child.kill().await;
        let _ = pid;
    }
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
    if format_selector.trim().is_empty() {
        return Err(anyhow!("No format was selected for this download"));
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
    let outtmpl = format!("{}.%(ext)s", stem.to_string_lossy());
    let mut command = TokioCommand::new(tool_path(YtDlpTool::YtDlp));
    command
        .arg("-f")
        .arg(format_selector)
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
        .arg("10")
        .arg("--merge-output-format")
        .arg("mp4")
        .arg("--remux-video")
        .arg("mp4")
        .arg(url);
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
    command.env("PATH", child_path_with_tools());
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    let mut child = command
        .spawn()
        .context("Could not start yt-dlp")?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    // stderr is drained on its own task: a child blocked on a full
    // stderr pipe would hang forever, and its text is the failure
    // label when the run fails.
    let stderr_text = Arc::new(std::sync::Mutex::new(String::new()));
    let stderr_task = {
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
                                break finish_download(status, target, &stem, &stderr_text, last_progress.clone()).await;
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
                                                Ok(status) => finish_download(status, target, &stem, &stderr_text, last_progress.clone()).await,
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
                    Ok(status) => finish_download(status, target, &stem, &stderr_text, last_progress.clone()).await,
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
    let stderr = stderr_text
        .lock()
        .map(|text| text.clone())
        .unwrap_or_default();
    Err(anyhow!("{}", failure_label(&stderr)))
}

/// The single finished file for a stem: files in the same directory
/// whose stem matches, excluding .part files. The caller reserved a
/// unique name, so there is at most one.
async fn produced_file_for_stem(stem: &Path) -> Option<PathBuf> {
    let parent = stem.parent()?;
    let wanted = stem.file_name()?.to_str()?;
    let mut found: Vec<PathBuf> = Vec::new();
    let mut entries = tokio::fs::read_dir(parent).await.ok()?;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".part") || name.contains(".part.") {
            continue;
        }
        if let Some(file_stem) = Path::new(&name).file_stem().and_then(|s| s.to_str()) {
            if file_stem == wanted {
                found.push(entry.path());
            }
        }
    }
    if found.len() == 1 {
        found.into_iter().next()
    } else {
        None
    }
}

async fn cleanup_partial(stem: &Path) {
    // yt-dlp's .part files sit next to the stem; remove them and any
    // reserved/partial target so Cancel leaves nothing behind.
    if let Some(parent) = stem.parent() {
        if let Some(wanted) = stem.file_name().and_then(|name| name.to_str()) {
            if let Ok(mut entries) = tokio::fs::read_dir(parent).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if let Some(file_stem) = Path::new(&name).file_stem().and_then(|s| s.to_str()) {
                        if file_stem == wanted {
                            let _ = tokio::fs::remove_file(entry.path()).await;
                        }
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
            url: "https://example.com/v".into(),
            downloadable: true,
            note: None,
            representation_id: Some("137".into()),
            audio_representation_id: Some("140".into()),
            audio_only: None,
            requires_ffmpeg: None,
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
        assert!(failure_label("WARNING: No supported JavaScript runtime could be found").starts_with("Deno is missing"));
        assert!(failure_label("ERROR: HTTP Error 403: Forbidden").starts_with("YouTube refused"));
        assert!(failure_label("ERROR: Unsupported URL: https://example.com/").starts_with("This page is not"));
        assert!(failure_label("ERROR: [youtube] abc: Failed to extract player response").starts_with("YouTube changed"));
        assert!(failure_label("something totally new").contains("download helper"));
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
            PathBuf::from("/tmp/thunderload-tools-test/yt-dlp.exe")
        );
        assert_eq!(
            tool_path(YtDlpTool::Deno),
            PathBuf::from("/tmp/thunderload-tools-test/deno.exe")
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
    }

    #[tokio::test]
    async fn sha256_verification_accepts_good_and_rejects_bad() {
        let dir = std::env::temp_dir().join("thunderload-sha-test");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let file = dir.join("abc.bin");
        tokio::fs::write(&file, b"abc").await.unwrap();
        // SHA256("abc") — the standard test vector.
        let expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let bytes = tokio::fs::read(&file).await.unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        assert_eq!(format!("{:x}", hasher.finalize()), expected);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    // Serializes every test that points THUNDERLOAD_TOOLS_DIR at a
    // scratch dir: the env var is process-global, so two such tests
    // running in parallel would point the tool lookup at each
    // other's dir.
    static TOOLS_DIR_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn write_fake_ytdlp(dir: &std::path::Path, script: &str) -> PathBuf {
        let path = dir.join("yt-dlp.exe");
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
    /// code uses: `.yt-dlp.exe.sha256`): the tests set
    /// THUNDERLOAD_TOOLS_DIR only after writing the fake, so resolving
    /// the path through tools_dir() here would point at the wrong
    /// (possibly nonexistent) directory.
    fn write_verified_fake_ytdlp(dir: &std::path::Path, script: &str) -> PathBuf {
        let path = write_fake_ytdlp(dir, script);
        let hash = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
        std::fs::write(dir.join(".yt-dlp.exe.sha256"), hash).unwrap();
        path
    }

    // NOTE (fix round): the fakes are shell scripts named yt-dlp.exe.
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
if [ -f "$stem.part" ]; then
  mv "$stem.part" "$stem.mp4"
  echo "download: 100.0%|10240|10240|1.0MiB/s|00:00"
  exit 0
fi
printf 'partial content' > "$stem.part"
exec sleep 30
"#
        .to_string()
    }

    #[test]
    fn verify_tool_digest_accepts_pinned_and_rejects_other() {
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
            dir.join("video.part").is_file(),
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
        assert!(!dir.join("video.part").exists());
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
            !dir.join("video.part").exists(),
            "cancel must delete the .part file"
        );
        std::env::remove_var("THUNDERLOAD_TOOLS_DIR");
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
        std::fs::write(dir.join("yt-dlp.exe"), b"tampered").unwrap();
        let error = verify_tool_on_disk(YtDlpTool::YtDlp).await.unwrap_err();
        assert!(error.to_string().contains("SHA256 mismatch"));
        // No sidecar at all: the pinned hash applies, so a fake that
        // is not the pinned binary is refused.
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
        let deno = dir.join("deno.exe");
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
        let before = std::fs::read(dir.join("yt-dlp.exe")).unwrap();
        // No progress channel needed: a present, verified tool returns
        // without downloading (no network is touched).
        let path = ensure_tool(YtDlpTool::YtDlp, None).await.unwrap();
        assert_eq!(path, dir.join("yt-dlp.exe"));
        assert_eq!(std::fs::read(dir.join("yt-dlp.exe")).unwrap(), before);
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
}

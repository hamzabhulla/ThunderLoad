import { useEffect, useMemo, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { confirm, save } from "@tauri-apps/plugin-dialog";

type Page = "downloads" | "queues" | "scheduler" | "settings";
type StatusTab = "all" | "active" | "completed" | "failed";
type InspectorTab = "overview" | "files" | "segments" | "activity";
type Kind = "direct" | "hls" | "dash" | "ytdlp";
type Status = "Queued" | "Downloading" | "Pausing" | "Paused" | "Cancelling" | "Completed" | "Failed" | "Interrupted" | "Cancelled";
type SubtitleVariant = { language: string; name?: string; url: string; format: string };
type Variant = { kind: string; quality: string; width?: number; height?: number; bandwidth?: number; url: string; downloadable: boolean; note?: string; representationId?: string; audioRepresentationId?: string; audioOnly?: boolean; isLive?: boolean; fps?: number; subtitles?: SubtitleVariant[] };
type GrabItem = { url: string; kind: string; name: string };
type GrabResult = { items: GrabItem[]; truncated: boolean };
// P1-10: no `phase` field — the backend emits one, but it only
// duplicates the row status and the download-complete /
// download-paused events this handler already uses, so nothing
// reads it. (It was declared and never read.)
type Progress = { downloaded: number; total?: number; percent?: number; segments_done?: number; segments_total?: number };
type Item = { id: string; name: string; url: string; kind: Kind; status: Status; percent: number; downloaded: number; total?: number; detail: string; error?: string; createdAt: number; targetPath?: string; scheduledAt?: number; source?: "native"; representationId?: string; audioRepresentationId?: string; segmentsDone?: number; segmentsTotal?: number; live?: boolean; connections?: number };
type StoredJob = { id: string; name: string; url: string; kind: string; origin: string; state: string; downloaded: number; total?: number; percent?: number; segmentsDone?: number; segmentsTotal?: number; targetPath?: string; checkpointPath?: string; representationId?: string; audioRepresentationId?: string; scheduledAtMs?: number; detail: string; error?: string; createdAtMs: number; updatedAtMs: number; live?: boolean };

const HISTORY_KEY = "thunderload.history.v2";
// The v2 history key was never a user choice: the v1 control was
// disabled, and v1 stored 1, which was never the engine's real value.
// v2 starts fresh at the engine default, 8.
const CONNECTIONS_KEY = "thunderload.connections.v2";
const PARALLEL_KEY = "thunderload.parallel.v1";
const SPEED_LIMIT_KEY = "thunderload.speed-limit.v1";
const NATIVE_IDS_KEY = "thunderload.native-extension-ids.v1";
const CLIPBOARD_WATCH_KEY = "thunderload.clipboard-watch.v1";
const NATIVE_BANNER_DISMISSED_KEY = "thunderload.native-banner-dismissed.v1";

// Extension IDs the user entered once in Settings → Browser connection,
// one per browser. The startup check re-registers from these, so the
// user never has to paste an ID twice.
function readNativeIds(): Record<string, string> {
  try {
    const saved = JSON.parse(localStorage.getItem(NATIVE_IDS_KEY) || "{}");
    if (!saved || typeof saved !== "object") return {};
    const ids: Record<string, string> = {};
    for (const [browser, id] of Object.entries(saved)) {
      if (typeof id === "string" && id.trim()) ids[browser] = id.trim();
    }
    return ids;
  } catch { return {}; }
}

// Browsers the desktop can connect, in the order the Settings form
// offers them. The connected list below renders in this order too, so
// Chrome always comes first (it seeds the shared Chrome/Opera manifest
// deterministically — see the startup check).
const NATIVE_BROWSERS: Array<{ id: string; name: string }> = [
  { id: "chrome", name: "Google Chrome" },
  { id: "edge", name: "Microsoft Edge" },
  { id: "brave", name: "Brave" },
  { id: "opera", name: "Opera" },
  { id: "vivaldi", name: "Vivaldi" },
  { id: "firefox", name: "Firefox" },
];
function nativeBrowserName(browser: string): string {
  return NATIVE_BROWSERS.find((item) => item.id === browser)?.name || browser;
}

// Connected browsers, in NATIVE_BROWSERS order, from the saved IDs.
function listNativeBrowsers(): Array<{ browser: string; extensionId: string }> {
  const ids = readNativeIds();
  return NATIVE_BROWSERS.filter((item) => ids[item.id]).map((item) => ({ browser: item.id, extensionId: ids[item.id] }));
}

// Refusal shown when a pasted URL inspects as a webpage instead of a
// direct media file — the same wording as the Format support note on the
// Video page. (YouTube/social pages never reach this: they route to the
// yt-dlp inspect flow in add() below.)
const YOUTUBE_PAGE_MESSAGE = "That page is not a direct media URL. Paste a YouTube or social video page to inspect it with yt-dlp, or a direct media link.";

// Pages yt-dlp knows how to extract — same host list as the
// extension's YTDLP_HOSTS. A pasted page URL here routes to the yt-dlp
// inspect flow instead of the direct-media probe.
const YTDLP_PAGE_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "tiktok.com", "twitter.com", "x.com", "facebook.com", "fb.watch", "reddit.com", "twitch.tv", "vimeo.com"];
function isYtDlpPageUrl(value: string): boolean {
  try {
    const host = new URL(value).hostname.toLowerCase();
    return YTDLP_PAGE_HOSTS.some((listed) => host === listed || host.endsWith("." + listed));
  } catch { return false; }
}
// A URL the clipboard watcher treats as downloadable — a direct media
// file, or a YouTube/social page (which the yt-dlp inspect flow
// handles once the user picks Download).
const CLIPBOARD_MEDIA_EXT = /\.(mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav|ogg|opus|flac|m3u8|mpd|zip|pdf)$/i;
function isClipboardMediaUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return false;
    return CLIPBOARD_MEDIA_EXT.test(parsed.pathname) || isYtDlpPageUrl(value);
  } catch { return false; }
}

function bytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return (value / Math.pow(1024, unit)).toFixed(unit ? 1 : 0) + " " + units[unit];
}

// One speed sample per job — the rate comes from the bytes moved since
// the previous sample, never from a single absolute value (the Motrix
// rule). `rate: null` means only one sample exists so far and the rate
// is still unknown.
type SpeedSample = { downloaded: number; at: number; rate: number | null };
function recordSpeedSample(samples: Map<string, SpeedSample>, jobId: string, downloaded: number, now: number): void {
  const prev = samples.get(jobId);
  if (prev) {
    const dt = (now - prev.at) / 1000;
    const rate = dt > 0 ? Math.max(0, (downloaded - prev.downloaded) / dt) : prev.rate;
    samples.set(jobId, { downloaded, at: now, rate });
  } else {
    samples.set(jobId, { downloaded, at: now, rate: null });
  }
}
function readItems(): Item[] {
  try {
    const saved = JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
    if (!Array.isArray(saved)) return [];
    return saved.map((item: Item) => forgetStaleSchedule(item.source === "native"
      ? item
      : item.live && item.targetPath && (item.status === "Downloading" || item.status === "Pausing" || item.status === "Queued")
        // A live recording has no checkpoint to recover from —
        // re-queuing it would re-record over the kept partial. It comes
        // back Paused, file kept; Pause is terminal. (Queued with a
        // target is a resume-queued recording from an older version; a
        // live job that never started has no target and stays Queued.)
        ? { ...item, status: "Paused", detail: "Paused · partial recording kept" }
        : item.status === "Downloading" || item.status === "Pausing"
          ? item.targetPath
            ? { ...item, status: "Queued", detail: "Recovering saved partial download…" }
            : { ...item, status: "Interrupted", detail: "App closed before a resumable checkpoint was created" }
          : item))
      // P1 slice 1 audit follow-up: rows saved before the P1 #10 fix
      // carried a NaN percent in memory, which JSON stores as null —
      // without this guard the table renders "null%". A null or
      // non-finite stored percent comes back as 0.
      .map((item) => ({ ...item, percent: Number.isFinite(item.percent) ? Math.max(0, Math.min(100, item.percent)) : 0 }));
  } catch { return []; }
}
function readSetting(key: string, fallback: number, allowed: number[]): number {
  try {
    const value = Number(localStorage.getItem(key));
    return allowed.includes(value) ? value : fallback;
  } catch { return fallback; }
}
function nameFromUrl(value: string): string {
  try {
    const raw = new URL(value).pathname.split("/").filter(Boolean).pop();
    return raw ? decodeURIComponent(raw) : "download.bin";
  } catch { return "download.bin"; }
}
function storedJobToItem(row: StoredJob): Item {
  const kind: Kind = row.kind === "hls" ? "hls" : row.kind === "dash" ? "dash" : row.kind === "ytdlp" ? "ytdlp" : "direct";
  const rawStatus: Status = (["Queued", "Downloading", "Pausing", "Paused", "Completed", "Failed", "Interrupted", "Cancelled"].includes(row.state) ? row.state : "Failed") as Status;
  // readItems' recovery mapping is dead code unless stored rows get the
  // same treatment — the seeding effect lets stored rows win by id, so a
  // live recording closed mid-flight came back Downloading (wedged:
  // no Pause target, no Resume, no Cancel, no Remove, and one
  // parallel slot eaten forever), and a VOD job came back Downloading
  // instead of Queued "Recovering saved partial download…". This
  // mirrors readItems' mapping exactly, including the Queued case:
  // a live Queued row WITH a target is a resume-queued recording
  // from an older version; a live Queued row WITHOUT one is a schedule
  // that never started, and stays Queued. "Pausing" is in the list
  // even though the durable JobState enum has no such variant
  // (sync maps it to Queued/Interrupted) — a row carrying it must
  // still recover like Downloading, never fall through to "Failed".
  let status = rawStatus;
  let detail = row.detail || "";
  if (row.live && row.targetPath && (rawStatus === "Downloading" || rawStatus === "Pausing" || rawStatus === "Queued")) {
    status = "Paused";
    detail = "Paused · partial recording kept";
  } else if (rawStatus === "Downloading" || rawStatus === "Pausing") {
    if (row.targetPath) {
      status = "Queued";
      detail = "Recovering saved partial download…";
    } else {
      status = "Interrupted";
      detail = "App closed before a resumable checkpoint was created";
    }
  }
  // Same guard as readItems (P1 slice 1 audit follow-up): a NaN
  // percent saved before the P1 #10 fix round-trips through JSON as
  // null — treat a null/non-finite stored percent as unknown and fall
  // back to the downloaded/total ratio, or 0.
  const storedPercent = row.percent;
  const percent = typeof storedPercent === "number" && Number.isFinite(storedPercent)
    ? Math.max(0, Math.min(100, storedPercent))
    : row.total ? Math.max(0, Math.min(100, Math.round(row.downloaded / row.total * 100))) : 0;
  return {
    id: row.id, name: row.name || nameFromUrl(row.url), url: row.url, kind, status, percent,
    downloaded: row.downloaded, total: row.total, detail,
    error: row.error, createdAt: row.createdAtMs, targetPath: row.targetPath,
    scheduledAt: row.scheduledAtMs, representationId: row.representationId,
    audioRepresentationId: row.audioRepresentationId,
    segmentsDone: row.segmentsDone, segmentsTotal: row.segmentsTotal,
    // Restore the live flag, or a live row re-seeded from the durable
    // store loses live routing and takes the VOD start path, which
    // rejects live playlists.
    live: row.live ? true : undefined
  };
}
function itemToSyncJob(item: Item) {
  return {
    id: item.id, name: item.name, url: item.url, kind: item.kind, status: item.status,
    downloaded: item.downloaded, total: item.total ?? null, percent: item.percent,
    targetPath: item.targetPath ?? null, createdAt: item.createdAt,
    scheduledAt: item.scheduledAt ?? null, representationId: item.representationId ?? null,
    audioRepresentationId: item.audioRepresentationId ?? null, detail: item.detail,
    error: item.error ?? null,
    // Carry the live flag when syncing to the durable store, or a live
    // row loses it and comes back as VOD.
    live: item.live ?? false
  };
}
function pageInfo(page: Page) {
  const values = {
    downloads: ["Downloads", "A clear view of what is moving and what is finished."],
    queues: ["Queues", "Transfers waiting, running, or needing attention."],
    scheduler: ["Scheduler", "Downloads waiting for their start time."],
    settings: ["Settings", "A few useful controls. No decorative switches."]
  };
  return values[page];
}
function Status({ value }: { value: Status }) {
  return <span className={"status status-" + value.toLowerCase()}><i />{value}</span>;
}

// Statuses for which Cancel is offered — the same set the row actions
// use, so the Scheduler page can never offer a Cancel the backend
// silently ignores (a completed job has no live task to cancel).
// Live recordings are never cancellable: a live partial file IS the
// recording, and Pause — not Cancel — is how a live recording stops
const CANCELLABLE_STATUSES: Status[] = ["Downloading", "Pausing", "Paused", "Failed", "Interrupted", "Queued"];
function canCancel(item: Item): boolean {
  return !item.live && CANCELLABLE_STATUSES.includes(item.status);
}
// The row × button never shows while a transfer is live or
// cancelling; browser jobs additionally need a state the companion can
// dismiss. Mirrors the old inline condition exactly.
function canRemove(item: Item): boolean {
  if (item.status === "Downloading" || item.status === "Pausing" || item.status === "Cancelling") return false;
  return item.source !== "native" || ["Completed", "Cancelled", "Failed", "Interrupted", "Paused"].includes(item.status);
}

// File-type classification for the Downloads filter chips (IDM
// categories), derived from the filename extension — the transfer
// kind (direct/hls/dash) says how it downloads, this says what it is.
type FileType = "video" | "music" | "documents" | "compressed" | "programs" | "other";
type KindFilter = "all" | "video" | "music" | "documents" | "compressed" | "programs" | "other";
function fileTypeOf(item: Item): FileType {
  const ext = item.name.split(".").pop()?.toLowerCase() || "";
  if (["mp4", "webm", "mkv", "mov", "avi", "m4v", "m3u8", "mpd", "ts"].includes(ext)) return "video";
  if (["mp3", "wav", "flac", "ogg", "oga", "m4a", "opus", "aac"].includes(ext)) return "music";
  if (["pdf", "doc", "docx", "txt", "epub", "md", "xls", "xlsx", "ppt", "pptx", "csv", "rtf"].includes(ext)) return "documents";
  if (["zip", "rar", "7z", "tar", "gz", "bz2", "xz"].includes(ext)) return "compressed";
  if (["exe", "msi", "dmg", "pkg", "apk", "appimage", "deb", "rpm"].includes(ext)) return "programs";
  return "other";
}

// A schedule only means something while the job has not started yet: a
// job with a checkpoint (targetPath) or any status other than Queued
// already fired its schedule, so a stale scheduledAt — written before
// this fix — must not bring it back onto the Scheduler page.
function forgetStaleSchedule(item: Item): Item {
  if (item.scheduledAt === undefined) return item;
  if (item.status === "Queued" && !item.targetPath) return item;
  return { ...item, scheduledAt: undefined };
}

// True when keyboard focus sits in an editable field: global shortcuts
// (Ctrl/Cmd+N, Delete, P) must not fire while the user is typing.
function isTypingTarget(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el || typeof el.tagName !== "string") return false;
  return el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.tagName === "SELECT" || el.isContentEditable;
}

export default function App() {
  const [page, setPage] = useState<Page>("downloads");
  const [url, setUrl] = useState("");
  const [filename, setFilename] = useState("");
  const [scheduleAt, setScheduleAt] = useState("");
  const [videoUrl, setVideoUrl] = useState("");
  // The URL the variants were actually inspected from. The input above
  // stays editable, so the Download button must not read the live input
  // — a yt-dlp format needs the inspected page URL, and an edited input
  // would send yt-dlp a wrong URL.
  const [inspectedUrl, setInspectedUrl] = useState("");
  const [items, setItems] = useState<Item[]>(readItems);
  const [variants, setVariants] = useState<Variant[]>([]);
  const [inspecting, setInspecting] = useState(false);
  const [notice, setNotice] = useState("Ready.");
  const [videoNotice, setVideoNotice] = useState("");
  // Site grabber: paste-a-page-URL path. The desktop reads the page's
  // static HTML only, so `grabbed` may miss media that a login or
  // JavaScript would reveal; the extension sees the live page.
  const [grabUrl, setGrabUrl] = useState("");
  const [grabbed, setGrabbed] = useState<GrabItem[]>([]);
  const [grabChecked, setGrabChecked] = useState<Record<string, boolean>>({});
  const [grabTruncated, setGrabTruncated] = useState(false);
  const [grabbing, setGrabbing] = useState(false);
  const [addingUrl, setAddingUrl] = useState(false);
  const [downloadingGrabbed, setDownloadingGrabbed] = useState(false);
  // P1-21: the Video Grabber variant Download button in flight
  // (variant key) — the button shows its own busy state and the
  // add() result lands in the modal's videoNotice, not only in the
  // global notice behind the dialog.
  const [grabberVariantBusy, setGrabberVariantBusy] = useState<string | null>(null);
  const [grabNotice, setGrabNotice] = useState("");
  const [grabScheduleAt, setGrabScheduleAt] = useState("");
  const [connections, setConnections] = useState(() => readSetting(CONNECTIONS_KEY, 8, [1, 2, 4, 8, 16]));
  const [parallel, setParallel] = useState(() => readSetting(PARALLEL_KEY, 2, [1, 2, 3, 4]));
  const [speedLimit, setSpeedLimit] = useState(() => readSetting(SPEED_LIMIT_KEY, 0, [0, 262144, 524288, 1048576, 2097152, 5242880, 10485760]));
  const [clock, setClock] = useState(Date.now());
  const [nativeBrowser, setNativeBrowser] = useState("chrome");
  const [nativeExtensionId, setNativeExtensionId] = useState(() => readNativeIds()["chrome"] || "");
  const [nativeSetupStatus, setNativeSetupStatus] = useState("");
  const [nativeSettingUp, setNativeSettingUp] = useState(false);
  // Connected browsers, with an Add form. Several browsers can stay
  // connected at once — each one registers its own native host and its
  // extension talks to its own host process, so downloads from any of
  // them work at the same time. The form starts open when nothing is
  // connected yet (the old first-run experience) and stays shut once
  // at least one browser is in the list.
  const [connectedBrowsers, setConnectedBrowsers] = useState(listNativeBrowsers);
  // The browser a removal is in flight for, or null. While one removal
  // runs, every Remove button (and Add/Connect) is disabled: two rapid
  // Removes would interleave read-modify-write on the saved IDs and
  // resurrect the first one.
  const [removingNativeBrowser, setRemovingNativeBrowser] = useState<string | null>(null);
  const [showAddBrowser, setShowAddBrowser] = useState(() => Object.keys(readNativeIds()).length === 0);
  // Non-blocking startup prompt: "missing-id" when no extension ID was
  // ever saved, "register-failed" when a saved ID could not be
  // re-registered. Dismissal is persisted per kind (see
  // dismissedBannerKind below), so it survives a restart.
  const [nativeBanner, setNativeBanner] = useState<"missing-id" | "register-failed" | null>(null);
  // P1-23: a dismissed banner stays dismissed across restarts — the
  // dismissed kind is persisted in localStorage, like the other UI
  // prefs in this file. Stored per kind, so dismissing "missing-id"
  // never silences a later "register-failed" (or vice versa); a
  // successful registration clears it (see registerNativeHost).
  const [dismissedBannerKind, setDismissedBannerKind] = useState(() => {
    try { return localStorage.getItem(NATIVE_BANNER_DISMISSED_KEY) || ""; } catch { return ""; }
  });
  const [extensionFileStatus, setExtensionFileStatus] = useState("");
  // Clipboard watcher + LinkGrabber. Off by default — the user turns it
  // on in Settings. The seen set lives in a ref so a URL is never
  // offered twice in one app session.
  const [clipboardWatch, setClipboardWatch] = useState(() => localStorage.getItem(CLIPBOARD_WATCH_KEY) === "1");
  const [grabbedLinks, setGrabbedLinks] = useState<string[]>([]);
  const [linkGrabberOpen, setLinkGrabberOpen] = useState(false);
  // P2: the LinkGrabber Download button in flight (its URL), and the
  // dialog's own error line — a failed add must say so in the dialog,
  // not only in the Add form's notice behind it.
  const [linkGrabberBusy, setLinkGrabberBusy] = useState<string | null>(null);
  const [linkGrabberNotice, setLinkGrabberNotice] = useState("");
  const seenClipboardUrls = useRef(new Set<string>());
  const [extensionFileSaving, setExtensionFileSaving] = useState(false);
  const [ffmpegPath, setFfmpegPath] = useState("ffmpeg");
  const [ffmpegStatus, setFfmpegStatus] = useState("");
  const [ffmpegSaving, setFfmpegSaving] = useState(false);
  // On-demand yt-dlp/Deno tools — nothing is bundled. The title from a
  // yt-dlp inspect becomes the download filename.
  const [videoTitle, setVideoTitle] = useState("");
  const [ytdlpTools, setYtdlpTools] = useState<{ ytdlp: { present: boolean; version?: string }; deno: { present: boolean; version?: string }; toolsDir: string } | null>(null);
  const [ytdlpToolProgress, setYtdlpToolProgress] = useState<{ tool: string; percent?: number; speedBps?: number; etaSecs?: number } | null>(null);
  const [ytdlpDownloading, setYtdlpDownloading] = useState<string | null>(null);
  const [ytdlpUpdateStatus, setYtdlpUpdateStatus] = useState("");
  const [ytdlpCheckingUpdate, setYtdlpCheckingUpdate] = useState(false);
  // Direction A ("The Task List") layout state: the status tab + search
  // filter the Downloads table; a row click opens the right inspector;
  // the Add form and the video grabber live in dialogs, not pages.
  const [statusTab, setStatusTab] = useState<StatusTab>("all");
  const [search, setSearch] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>("overview");
  const [addOpen, setAddOpen] = useState(false);
  const [grabberOpen, setGrabberOpen] = useState(false);
  const [kindFilter, setKindFilter] = useState<KindFilter>("all");
  const [showKind, setShowKind] = useState(false);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; id: string } | null>(null);
  // Mirror of the job list for one-shot startup effects (the durable
  // store seed below needs the current list without re-running).
  const itemsRef = useRef(items);
  useEffect(() => { itemsRef.current = items; }, [items]);
  // Motrix speed rule, computed from progress events: each
  // download-progress event records bytes + time; the table derives a
  // live rate from the last two samples. A rate of exactly 0 shows
  // "0 B/s"; a stale or missing sample shows muted "–"; anything that
  // is not actively downloading shows a blank cell. `rate: null` means
  // only one sample exists so far — the rate is still unknown.
  const speedSamples = useRef(new Map<string, SpeedSample>());
  // C1/C2: ids with a start_* call in flight — set before the await,
  // cleared when the target path lands (or the start fails). The
  // dispatcher skips them so a job can never start twice, and cancel()
  // routes them through cancel_download even though the frontend has
  // no target path yet.
  const startingIds = useRef(new Set<string>());
  // P1-25: ids with a resume() call in flight — a double-click (or a
  // keyboard P plus a button tap) must not hand the same checkpoint
  // over twice.
  const resumingIds = useRef(new Set<string>());
  // P2: subtitle URLs with a downloadSubtitle() call in flight.
  const downloadingSubtitles = useRef(new Set<string>());
  // P2: pending throttled history write (see the HISTORY_KEY effect).
  const historyWriteTimer = useRef<number | null>(null);
  // Ids with a cancel()/remove() call in flight — the row ×, the
  // context-menu Remove and keyboard Delete stay disabled until the
  // call settles, so a second Remove cannot race the first.
  const [busyIds, setBusyIds] = useState<string[]>([]);
  // Schedule wake-ups tick every 15 s, not every second — the 1 s clock
  // below exists only for speed/ETA staleness.
  const [scheduleTick, setScheduleTick] = useState(() => Date.now());

  // Live transfer rate for one job: a number when a fresh rate is
  // known, null when the rate is unknown, undefined when the cell does
  // not apply (the job is not downloading).
  function jobSpeed(item: Item): number | null | undefined {
    if (item.status !== "Downloading") return undefined;
    const sample = speedSamples.current.get(item.id);
    if (!sample || sample.rate === null || clock - sample.at > 2500) return null;
    return sample.rate;
  }
  function speedCell(item: Item): string {
    const speed = jobSpeed(item);
    if (speed === undefined) return "";
    if (speed === null) return "–";
    return speed === 0 ? "0 B/s" : bytes(speed) + "/s";
  }
  function etaText(seconds: number): string {
    if (!Number.isFinite(seconds) || seconds < 0) return "–";
    if (seconds < 60) return Math.ceil(seconds) + " s";
    if (seconds < 3600) return Math.ceil(seconds / 60) + " min";
    return Math.floor(seconds / 3600) + " h " + Math.ceil(seconds % 3600 / 60) + " min";
  }
  function etaCell(item: Item): string {
    if (item.status !== "Downloading") return "";
    const speed = jobSpeed(item);
    if (speed === null || speed === undefined || speed <= 0 || !item.total) return "–";
    return etaText(Math.max(0, item.total - item.downloaded) / speed);
  }
  // Speed + ETA suffix for the tool-download notice (" — 1.2 MB/s ·
  // 12 s bacha"). Empty while the engine has no rate yet — the first
  // 100 ms of a download carry neither, and a bare percent is what
  // the notice showed before.
  function toolSpeedEta(): string {
    if (!ytdlpToolProgress) return "";
    const speed = ytdlpToolProgress.speedBps !== undefined ? " — " + bytes(ytdlpToolProgress.speedBps) + "/s" : "";
    const eta = ytdlpToolProgress.etaSecs !== undefined ? " · " + etaText(ytdlpToolProgress.etaSecs) + " bacha" : "";
    return speed + eta;
  }
  // Closed status vocabulary for the tabs: terminal-but-cancelled jobs
  // sit with Completed, Failed/Interrupted sit with Failed, everything
  // else is Active — so no job is hidden from every tab.
  function tabOf(item: Item): "active" | "completed" | "failed" {
    if (item.status === "Completed" || item.status === "Cancelled") return "completed";
    if (item.status === "Failed" || item.status === "Interrupted") return "failed";
    return "active";
  }

  const active = items.filter((item) => item.status === "Downloading" || item.status === "Pausing" || item.status === "Cancelling").length;
  // Native jobs run in the companion and never consume a desktop slot —
  // only desktop actives count against the parallel limit, or browser
  // downloads would starve the desktop queue.
  const activeDesktop = items.filter((item) => item.source !== "native" && (item.status === "Downloading" || item.status === "Pausing" || item.status === "Cancelling")).length;
  const queued = items.filter((item) => item.status === "Queued").length;
  const done = items.filter((item) => item.status === "Completed").length;
  const attention = items.filter((item) => item.status === "Failed" || item.status === "Interrupted").length;
  const ordered = useMemo(() => items.slice().sort((a, b) => b.createdAt - a.createdAt), [items]);
  // Direction A filtered views, derived from the same ordered list.
  const tabCounts = useMemo(() => ({
    all: ordered.length,
    active: ordered.filter((item) => tabOf(item) === "active").length,
    completed: ordered.filter((item) => tabOf(item) === "completed").length,
    failed: ordered.filter((item) => tabOf(item) === "failed").length
  }), [ordered]);
  const searched = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) return ordered;
    return ordered.filter((item) => (item.name + " " + item.url).toLowerCase().includes(query));
  }, [ordered, search]);
  const tabbed = useMemo(() => statusTab === "all" ? searched : searched.filter((item) => tabOf(item) === statusTab), [searched, statusTab]);
  const kindFiltered = useMemo(() => kindFilter === "all" ? tabbed : tabbed.filter((item) => fileTypeOf(item) === kindFilter), [tabbed, kindFilter]);
  const kindCounts = useMemo(() => {
    const counts: Record<KindFilter, number> = { all: tabbed.length, video: 0, music: 0, documents: 0, compressed: 0, programs: 0, other: 0 };
    for (const item of tabbed) {
      const type = fileTypeOf(item);
      counts[type] += 1;
    }
    return counts;
  }, [tabbed]);
  // The Queues page promises "waiting, running, or needing attention":
  // Failed/Interrupted jobs belong here too, with their Retry action.
  const queueItems = useMemo(() => ordered.filter((item) => tabOf(item) === "active" || tabOf(item) === "failed"), [ordered]);
  // The schedule input's minimum is derived from the 15 s schedule
  // tick, not the 1 s clock — it must not recompute every second.
  const scheduleMin = useMemo(() => new Date(scheduleTick - new Date(scheduleTick).getTimezoneOffset() * 60000).toISOString().slice(0, 16), [scheduleTick]);
  const scheduledItems = useMemo(() => ordered.filter((item) => item.scheduledAt !== undefined).sort((a, b) => (a.scheduledAt || 0) - (b.scheduledAt || 0)), [ordered]);
  const aggregateSpeed = items.reduce((sum, item) => {
    const speed = jobSpeed(item);
    return sum + (typeof speed === "number" ? speed : 0);
  }, 0);
  // Motrix rule for the aggregate too: a real 0 is "0 B/s", but when
  // downloads are running and no rate is known yet the bar shows "–",
  // matching the per-row cells instead of claiming 0 B/s.
  const downloadingCount = items.filter((item) => item.status === "Downloading").length;
  const hasKnownRate = items.some((item) => typeof jobSpeed(item) === "number");
  const aggregateLabel = downloadingCount > 0 && !hasKnownRate ? "–" : aggregateSpeed === 0 ? "0 B/s" : bytes(aggregateSpeed) + "/s";
  const [title, subtitle] = pageInfo(page);

  // P2: history writes are throttled (trailing, 500 ms) and guarded —
  // every progress event re-renders items, and an unthrottled
  // JSON.stringify + setItem on each one stalls the UI; a full
  // localStorage (or a denied write) must never throw out of a render
  // effect. itemsRef always holds the latest list, so the trailing
  // write never persists a stale snapshot.
  useEffect(() => {
    if (historyWriteTimer.current !== null) return;
    historyWriteTimer.current = window.setTimeout(() => {
      historyWriteTimer.current = null;
      try { localStorage.setItem(HISTORY_KEY, JSON.stringify(itemsRef.current)); } catch { /* history is best-effort */ }
    }, 500);
  }, [items]);
  // The 500 ms trailing write above can lose the last state when the app
  // closes or the window hides before the timer fires — the next startup
  // would then re-seed stale statuses. Flush the pending write
  // synchronously on beforeunload and when the page becomes hidden, so
  // the history on disk is never older than the last committed list.
  useEffect(() => {
    const flushHistory = () => {
      if (historyWriteTimer.current !== null) {
        window.clearTimeout(historyWriteTimer.current);
        historyWriteTimer.current = null;
      }
      try { localStorage.setItem(HISTORY_KEY, JSON.stringify(itemsRef.current)); } catch { /* history is best-effort */ }
    };
    const onVisibilityChange = () => { if (document.visibilityState === "hidden") flushHistory(); };
    window.addEventListener("beforeunload", flushHistory);
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      window.removeEventListener("beforeunload", flushHistory);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, []);
  useEffect(() => { localStorage.setItem(CONNECTIONS_KEY, String(connections)); }, [connections]);
  useEffect(() => { localStorage.setItem(PARALLEL_KEY, String(parallel)); }, [parallel]);
  useEffect(() => { localStorage.setItem(CLIPBOARD_WATCH_KEY, clipboardWatch ? "1" : "0"); }, [clipboardWatch]);
  useEffect(() => {
    try { localStorage.setItem(NATIVE_BANNER_DISMISSED_KEY, dismissedBannerKind); } catch { /* dismissal is best-effort */ }
  }, [dismissedBannerKind]);
  // The Tauri clipboard-manager plugin, not the webview's
  // navigator.clipboard — polling the web clipboard every 2 s with
  // only a focus guard is what the audit flagged. The plugin read is
  // an explicit, user-opted-in capability
  // (clipboard-manager:allow-read-text) and returns "" when the
  // clipboard holds no text. No npm package is needed: this is the
  // same invoke the plugin's readText() wraps
  // ("plugin:clipboard-manager|read_text").
  useEffect(() => {
    if (!clipboardWatch) return;
    const timer = window.setInterval(async () => {
      if (!document.hasFocus()) return;
      let text: string;
      try { text = await invoke<string>("plugin:clipboard-manager|read_text"); } catch { return; }
      if (!text) return;
      const found = text.match(/https?:\/\/[^\s"'<>]+/gi) || [];
      const fresh: string[] = [];
      for (const raw of found) {
        const url = raw.replace(/[),.;]+$/, "");
        if (seenClipboardUrls.current.has(url)) continue;
        seenClipboardUrls.current.add(url);
        if (itemsRef.current.some((item) => item.url === url)) continue;
        if (isClipboardMediaUrl(url)) fresh.push(url);
      }
      if (fresh.length) {
        setGrabbedLinks((current) => [...current, ...fresh.filter((url) => !current.includes(url))]);
        // P1-11: a notice from the last visit ("That link could not
        // be queued") must not still be showing when the dialog
        // reopens — nothing about it was cleared on close.
        setLinkGrabberNotice("");
        setLinkGrabberOpen(true);
      }
    }, 2000);
    return () => window.clearInterval(timer);
  }, [clipboardWatch]);
  useEffect(() => {
    localStorage.setItem(SPEED_LIMIT_KEY, String(speedLimit));
    void invoke("set_speed_limit", { bytesPerSecond: speedLimit }).catch((error) => setNotice("Could not apply speed limit: " + String(error)));
  }, [speedLimit]);
  // The 1 s clock drives speed/ETA staleness only. Scheduled starts use
  // the separate 15 s schedule tick (see the dispatcher), so the
  // dispatcher and the schedule input's minimum do not re-run every
  // second.
  // P1-22: a stale rate only ever shows on a Downloading row
  // (jobSpeed returns early otherwise), so when nothing is
  // downloading the tick has nothing to update — skip the setClock
  // instead of re-rendering the whole app every second while idle.
  // While any transfer runs, the tick behaves exactly as before.
  useEffect(() => {
    const timer = window.setInterval(() => {
      if (itemsRef.current.some((item) => item.status === "Downloading")) setClock(Date.now());
    }, 1000);
    const scheduleTimer = window.setInterval(() => setScheduleTick(Date.now()), 15000);
    return () => { window.clearInterval(timer); window.clearInterval(scheduleTimer); };
  }, []);
  // Direction A shortcuts: Ctrl/Cmd+N opens Add (never while typing);
  // Escape closes the topmost layer (context menu, dialog, inspector);
  // Delete removes and P pauses/resumes the selected row.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "n") {
        if (isTypingTarget(event.target)) return;
        event.preventDefault();
        // A dialog must never open under an open context menu: dismiss
        // the menu first (it is the only dialog opener reachable while
        // the menu is up — buttons sit behind the menu overlay).
        setContextMenu(null);
        setAddOpen(true);
      } else if (event.key === "Escape") {
        if (contextMenu) setContextMenu(null);
        else if (linkGrabberOpen) setLinkGrabberOpen(false);
        else if (grabberOpen) setGrabberOpen(false);
        else if (addOpen) { setAddOpen(false); setScheduleAt(""); setUrl(""); setFilename(""); }
        else setSelectedId(null);
      } else if (!addOpen && !grabberOpen && !linkGrabberOpen && !contextMenu && selectedId && !isTypingTarget(event.target) && !event.ctrlKey && !event.metaKey && !event.altKey) {
        const item = items.find((entry) => entry.id === selectedId);
        if (!item) return;
        if (event.key === "Delete" || event.key === "Backspace") {
          event.preventDefault();
          // Same gate as the row × button and context-menu Remove: a
          // live transfer must not be killed (and its partial file
          // deleted) from the keyboard. Also blocked while a
          // cancel/remove is already in flight for this row.
          if (canRemove(item) && !busyIds.includes(selectedId)) void remove(selectedId);
        } else if (event.key.toLowerCase() === "p") {
          if (item.status === "Downloading") void pause(item);
          else if (item.status === "Paused") void resume(item);
        }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [addOpen, grabberOpen, linkGrabberOpen, selectedId, items, contextMenu, busyIds]);

  // Seed the queue from the durable job database on startup, then send
  // the merged desktop list back so jobs the store does not know yet
  // (older history, queued jobs that never started) enter the database.
  // Stored rows win by id; local history fills the rest.
  // Browser-started rows keep coming through the native poll. The
  // dispatcher must not auto-start anything until this initial merge has
  // run — a stale local row re-queued from history would otherwise start
  // before the authoritative jobs.json state lands, and that start would
  // durably overwrite the stored row via start_job. Seeded also flips
  // true when get_jobs fails, so a dead backend cannot wedge the queue
  // forever.
  const [seeded, setSeeded] = useState(false);
  const seededRef = useRef(false);
  useEffect(() => {
    if (seededRef.current) return;
    seededRef.current = true;
    void invoke<Array<StoredJob>>("get_jobs").then((rows) => {
      const stored = rows.filter((row) => row.origin !== "native").map(storedJobToItem);
      const byId = new Map(itemsRef.current.map((item) => [item.id, item]));
      // Ids whose stored row the C1 skip below dropped. The local row
      // wins in memory, but the stored row is authoritative — skipped ids
      // stay out of the sync_jobs payload below as belt-and-braces (the
      // backend already skips ids it knows, so the echo could not
      // overwrite them; this guards a future sync_jobs change).
      const skipped = new Set<string>();
      for (const item of stored) {
        // C1: never overwrite a row the dispatcher/engine already
        // moved into an active or transitional state — the seed
        // resolves once at startup and can land after the dispatcher
        // flipped a local row to Downloading, Pausing or Cancelling;
        // the stored row must not put it back in the queue (which
        // would start the same job a second time) or resurrect a
        // cancel in progress, whatever the stored status is.
        const local = byId.get(item.id);
        if (local && (local.status === "Downloading" || local.status === "Pausing" || local.status === "Cancelling")) { skipped.add(item.id); continue; }
        byId.set(item.id, item);
      }
      // Drop stale schedules from stored rows too (a started/completed
      // job whose row still carries its old start time), or the next
      // startup would put it back on the Scheduler page.
      const merged = Array.from(byId.values()).sort((a, b) => b.createdAt - a.createdAt).map(forgetStaleSchedule);
      setItems(merged);
      const desktop = merged.filter((item) => item.source !== "native" && !skipped.has(item.id));
      if (desktop.length) void invoke("sync_jobs", { jobs: desktop.map(itemToSyncJob) }).catch(() => {});
    }).catch(() => {}).finally(() => setSeeded(true));
  }, []);

  // Fix 4: on startup, check the native-host registration for every
  // browser whose extension ID was saved once before, and re-register
  // silently when it went missing (browser reset, registry cleaner,
  // moved install). When no ID was ever saved, show a prominent
  // non-blocking banner instead — never a modal, never a silent no-op.
  // P0-4: Chrome and Opera share one Windows registry key and one
  // manifest behind it whose allowed_origins merges both IDs, so
  // re-registering either browser merges the other's ID instead of
  // flipping the key. Check Chrome first so the shared manifest is
  // seeded from Chrome deterministically, whatever order the IDs were
  // saved in; Opera's check then sees the merged manifest and stays a
  // no-op, and a second launch registers nothing at all.
  const nativeCheckedRef = useRef(false);
  useEffect(() => {
    if (nativeCheckedRef.current) return;
    nativeCheckedRef.current = true;
    const entries = Object.entries(readNativeIds());
    if (!entries.length) { setNativeBanner("missing-id"); return; }
    const chromeKeyRank = (browser: string) => (browser === "chrome" ? 0 : browser === "opera" ? 1 : 2);
    const ordered = [...entries].sort((a, b) => chromeKeyRank(a[0]) - chromeKeyRank(b[0]));
    void (async () => {
      let failed = false;
      for (const [browser, extensionId] of ordered) {
        try {
          const registered = await invoke<boolean>("check_native_host", { browser, extensionId });
          if (registered) continue;
          await invoke<string>("register_native_host", { browser, extensionId });
        } catch { failed = true; }
      }
      if (failed) setNativeBanner("register-failed");
    })();
  }, []);

  useEffect(() => {
    let disposed = false;
    let inFlight = false;
    const refreshNativeJobs = async () => {
      if (inFlight || disposed) return;
      inFlight = true;
      try {
        const jobs = await invoke<Array<{
          id: string; name?: string; url?: string; kind?: string; status?: string;
          percent?: number; downloaded?: number; total?: number | null; detail?: string;
          error?: string; createdAt?: number; targetPath?: string;
          representationId?: string; audioRepresentationId?: string;
          segmentsDone?: number; segmentsTotal?: number;
          live?: boolean;
        }>>("get_native_downloads");
        if (disposed) return;
        const external: Item[] = jobs.map((job) => ({
          id: "native-" + job.id,
          name: job.name || "Browser download",
          url: job.url || "",
          kind: job.kind === "hls" ? "hls" : job.kind === "dash" ? "dash" : job.kind === "ytdlp" ? "ytdlp" : "direct",
          // Pausing/Cancelling/Queued are real states, not errors — the
          // old whitelist mapped them to "Failed".
          status: (["Downloading", "Pausing", "Paused", "Cancelling", "Queued", "Completed", "Failed", "Interrupted", "Cancelled"].includes(job.status || "") ? job.status : "Failed") as Status,
          percent: Math.max(0, Math.min(100, job.percent || 0)),
          downloaded: job.downloaded || 0,
          total: job.total ?? undefined,
          detail: job.detail || "Started from browser extension",
          error: job.error,
          createdAt: job.createdAt || Date.now(),
          targetPath: job.targetPath,
          representationId: job.representationId,
          audioRepresentationId: job.audioRepresentationId,
          source: "native",
          segmentsDone: job.segmentsDone,
          segmentsTotal: job.segmentsTotal,
          // Carry the companion's live flag, or a browser-started live
          // recording shows Resume/Cancel buttons that destroy it.
          live: job.live ? true : undefined
        }));
        // Native jobs are polled, never pushed, so the poll is where
        // their speed samples come from — the same helper the local
        // download-progress listener uses. Samples for jobs that stopped
        // downloading (or vanished) are dropped, so a finished job never
        // shows a stale rate.
        const now = Date.now();
        for (const item of external) {
          if (item.status === "Downloading") recordSpeedSample(speedSamples.current, item.id, item.downloaded, now);
        }
        for (const id of Array.from(speedSamples.current.keys())) {
          if (id.startsWith("native-") && !external.some((item) => item.id === id && item.status === "Downloading")) speedSamples.current.delete(id);
        }
        setItems((current) => {
          const merged = new Map(current.map((item) => [item.id, item]));
          for (const item of external) {
            const prev = merged.get(item.id);
            // The poll must not clobber an optimistic Pausing/Cancelling
            // row — the companion still reports Downloading until its
            // control loop acts, and the row would flicker back to
            // Downloading on every poll.
            if (prev && (prev.status === "Pausing" || prev.status === "Cancelling") && item.status === "Downloading") {
              merged.set(item.id, { ...item, status: prev.status, detail: prev.detail });
            } else {
              merged.set(item.id, item);
            }
          }
          // Native rows that vanished from the poll are gone.
          for (const item of current) {
            if (item.source === "native" && !external.some((entry) => entry.id === item.id)) merged.delete(item.id);
          }
          return Array.from(merged.values()).sort((a, b) => b.createdAt - a.createdAt);
        });
      } catch (error) {
        // The companion is optional; local desktop transfers remain usable without it.
      } finally { inFlight = false; }
    };
    void refreshNativeJobs();
    const timer = window.setInterval(refreshNativeJobs, 1500);
    return () => { disposed = true; window.clearInterval(timer); };
  }, []);

  useEffect(() => {
    let alive = true;
    void invoke<string | null>("get_ffmpeg_path").then((path) => {
      if (alive && path) setFfmpegPath(path);
    }).catch(() => {});
    return () => { alive = false; };
  }, []);

  useEffect(() => { if (page === "settings") void loadYtDlpTools(); }, [page]);

  useEffect(() => {
    let alive = true;
    let cleanup: Array<() => void> = [];
    (async () => {
      const listeners = await Promise.all([
        listen<{ tool: string; downloaded: number; total?: number; percent?: number; speedBps?: number; etaSecs?: number }>("ytdlp-tool-progress", ({ payload }) => {
          setYtdlpToolProgress({ tool: payload.tool, percent: payload.percent, speedBps: payload.speedBps, etaSecs: payload.etaSecs });
        }),
        listen<{ jobId: string; progress: Progress }>("download-progress", ({ payload }) => {
          // The shared sampler — the native poll feeds the same helper,
          // so local and native jobs measure speed the same way. Segment
          // counts ride the same event and are kept when the transfer
          // reports them.
          recordSpeedSample(speedSamples.current, payload.jobId, payload.progress.downloaded, Date.now());
          setItems((current) => current.map((item) => {
            if (item.id !== payload.jobId) return item;
            // P1-10: a tick may omit total, and may carry a non-finite
            // percent (NaN from a 0/0 division upstream) — keep the
            // row's total and percent in those cases, and store the
            // percent rounded, so the table and inspector never render
            // 37.4829…% or NaN%.
            const total = payload.progress.total ?? item.total;
            const incoming = payload.progress.percent;
            const percent = typeof incoming === "number" && Number.isFinite(incoming)
              ? Math.round(Math.max(0, Math.min(100, incoming)))
              : item.percent;
            return {
              ...item,
              downloaded: payload.progress.downloaded,
              total,
              percent,
              segmentsDone: payload.progress.segments_done ?? item.segmentsDone,
              segmentsTotal: payload.progress.segments_total ?? item.segmentsTotal,
              // A progress event must not overwrite the detail of a
              // Pausing/Cancelling row or a terminal row (e.g. a late
              // event arriving after the user paused) — the status is
              // never overwritten here, and neither is its detail.
              detail: ["Pausing", "Cancelling", "Paused", "Completed", "Failed", "Interrupted", "Cancelled"].includes(item.status)
                ? item.detail
                : item.kind === "hls"
                  ? "Downloading media segments · " + percent + "%"
                  : bytes(payload.progress.downloaded) + (total ? " of " + bytes(total) : " downloaded")
            };
          }));
        }),
        listen<{ jobId: string; path: string }>("download-complete", ({ payload }) => {
          // Terminal state: the rate sample is done, and a schedule
          // that fired long ago must not linger on the Scheduler page.
          speedSamples.current.delete(payload.jobId);
          // C1/C2: the start settled — the id leaves the in-flight and
          // busy sets so a later cancel routes by status again.
          startingIds.current.delete(payload.jobId);
          setBusyIds((current) => current.filter((id) => id !== payload.jobId));
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Completed", percent: 100, targetPath: payload.path || item.targetPath, detail: "Saved · " + payload.path, error: undefined, scheduledAt: undefined }
            : item));
          setNotice("Download completed.");
        }),
        listen<{ jobId: string; targetPath: string }>("download-paused", ({ payload }) => {
          // A paused job's rate sample is done, like complete, cancelled
          // or error — without this a paused row kept a stale sample that
          // skewed the status-bar aggregate.
          speedSamples.current.delete(payload.jobId);
          startingIds.current.delete(payload.jobId);
          setItems((current) => current.map((item) => item.id === payload.jobId
            // A live recording has no checkpoint, so "partial file saved"
            // is the wrong words for it — the partial IS the recording.
            ? { ...item, status: "Paused", targetPath: payload.targetPath, detail: item.live ? "Paused · partial recording kept" : "Paused · partial file saved", error: undefined }
            : item));
        }),
        listen<{ jobId: string }>("download-cancelled", ({ payload }) => {
          speedSamples.current.delete(payload.jobId);
          startingIds.current.delete(payload.jobId);
          setBusyIds((current) => current.filter((id) => id !== payload.jobId));
          setItems((current) => current.map((item) => item.id === payload.jobId
            // The live finish arms emit this event too, and a live partial
            // file IS the recording — never wipe its target or claim it
            // was removed. No UI path cancels a live job today (every
            // cancel call site is live-gated), so this is the finishing
            // race only, but the row must stay honest if it ever fires:
            // the recording stays on disk, with its size.
            ? item.live
              ? { ...item, status: "Cancelled", detail: "Recording stopped · partial file kept", scheduledAt: undefined }
              : { ...item, status: "Cancelled", detail: "Cancelled · partial file removed", targetPath: undefined, downloaded: 0, percent: 0, scheduledAt: undefined }
            : item));
        }),
listen<{ jobId: string; error: string; targetPath?: string }>("download-error", ({ payload }) => {
          speedSamples.current.delete(payload.jobId);
          startingIds.current.delete(payload.jobId);
          setBusyIds((current) => current.filter((id) => id !== payload.jobId));
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Failed", detail: "Transfer interrupted · partial data kept", targetPath: payload.targetPath || item.targetPath, error: payload.error, scheduledAt: undefined }
            : item));
          setNotice(payload.error);
        })
      ]);
      if (!alive) listeners.forEach((fn) => fn());
      else cleanup = listeners;
    })().catch((error) => setNotice(String(error)));
    return () => { alive = false; cleanup.forEach((fn) => fn()); };
  }, []);

  useEffect(() => {
    // Never auto-start before the initial get_jobs merge — a stale local
    // row must not start (and get echoed back as Downloading) before the
    // authoritative jobs.json state lands.
    if (!seeded) return;
    // Slots count desktop actives only (see activeDesktop) — a browser
    // download must not eat a desktop queue slot.
    const slots = Math.max(0, parallel - activeDesktop);
    // C1: skip ids with a start already in flight. The optimistic flip
    // below is not enough on its own: the effect can re-run before the
    // flip commits (StrictMode double effects, a schedule re-run), and
    // the Set — stable across renders — closes that window. Due times
    // are checked against the 15 s schedule tick, not the 1 s clock, so
    // this effect does not re-run every second.
    const next = items.filter((item) => item.source !== "native" && item.status === "Queued" && !startingIds.current.has(item.id) && (!item.scheduledAt || item.scheduledAt <= scheduleTick)).slice(0, slots);
    if (!next.length) return;
    const ids = new Set(next.map((item) => item.id));
    next.forEach((item) => startingIds.current.add(item.id));
    setItems((current) => current.map((item) => ids.has(item.id) ? { ...item, status: "Downloading", detail: "Starting transfer…" } : item));
    next.forEach(async (item) => {
      try {
        if (item.scheduledAt) {
          void invoke("cancel_app_wake", { jobId: item.id }).catch(() => {});
          // The schedule has fired: forget it so the job leaves the
          // Scheduler page and badge now, and pass no schedule to the
          // start call below — the backend records the value it is
          // given in the durable row, and a stale one would bring the
          // job back onto the Scheduler page after a restart.
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, scheduledAt: undefined } : entry));
        }
        if (item.kind === "ytdlp") {
          // The format selector is representationId (+ "+" +
          // audioRepresentationId for a merged pick) — the backend
          // re-runs the identical yt-dlp command on resume, and yt-dlp
          // resumes from its .part files natively.
          const formatSelector = item.audioRepresentationId
            ? (item.representationId || "") + "+" + item.audioRepresentationId
            : (item.representationId || "");
          const targetPath = await invoke<string>("start_ytdlp_download", {
            jobId: item.id, url: item.url, formatSelector, filename: item.name,
            targetPath: item.targetPath || null, scheduledAt: null
          });
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath } : entry));
        } else if (item.kind === "hls") {
          // A live row records the stream as it arrives — the live
          // command keeps the partial .ts file on pause, because the
          // partial file IS the recording. A live row only ever reaches
          // this command as a brand-new recording: there is NO checkpoint
          // to reuse, and the engine re-records from the live edge and
          // truncates the target — so no live row is ever re-queued on an
          // existing target (Pause is terminal; see resume()/readItems).
          // The VOD half of this branch reuses its on-disk checkpoint
          // instead of reserving a fresh target and restarting from
          // scratch. The returned path is stored so Cancel works while
          // the job is running (VOD only — Cancel is never offered for
          // live).
          const targetPath = item.live
            ? await invoke<string>("start_hls_live_download", { jobId: item.id, url: item.url, representationId: item.representationId || null, audioRepresentationId: item.audioRepresentationId || null, filename: item.name, targetPath: item.targetPath || null, scheduledAt: null })
            : await invoke<string>("start_hls_download", { jobId: item.id, url: item.url, representationId: item.representationId || null, audioRepresentationId: item.audioRepresentationId || null, filename: item.name, targetPath: item.targetPath || null, connections, scheduledAt: null });
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath, ...(item.live ? {} : { connections }) } : entry));
        } else if (item.kind === "dash") {
          // Pass the paused/failed job's target back so the backend
          // reuses its on-disk checkpoint instead of reserving a fresh
          // target and restarting from scratch. The returned path is
          // stored so Pause/Cancel work while the job is running.
          const targetPath = await invoke<string>("start_dash_download", {
            jobId: item.id, url: item.url, representationId: item.representationId,
            audioRepresentationId: item.audioRepresentationId || null, filename: item.name,
            targetPath: item.targetPath || null, connections, scheduledAt: null
          });
          // Remember the connections this job started with, so the
          // inspector shows the job's own value, not the live setting.
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath, connections } : entry));
        } else {
          const targetPath = await invoke<string>("start_download", {
            jobId: item.id, url: item.url, filename: item.name, targetPath: item.targetPath || null, connections, scheduledAt: null
          });
          // Remember the connections this job started with, so the
          // inspector shows the job's own value, not the live setting.
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath, connections } : entry));
        }
      } catch (error) {
        // P1-19: the backend aborted a start whose Cancel landed
        // during the extension probe — it already flipped the row to
        // Cancelled and emitted download-cancelled, so the row must
        // not flip to Failed here.
        if (String(error) === "Download cancelled before it started.") return;
        setItems((current) => current.map((entry) => entry.id === item.id
          ? { ...entry, status: "Failed", detail: "Could not start", error: String(error) }
          : entry));
      } finally {
        // C1/C2: the start settled (target landed or failed) — the id
        // leaves the in-flight set so cancel() routes by status again.
        startingIds.current.delete(item.id);
      }
    });
  }, [items, parallel, connections, scheduleTick, seeded]);

  // Returns true when a job was queued OR the request was handed to
  // the Video Grabber for the user to pick a format, false when
  // nothing was queued or opened (invalid URL, past schedule,
  // webpage verdict, live DASH) — downloadGrabbed and the
  // LinkGrabber count on this.
  async function add(value: string, saveAs?: string, requestedKind?: Kind, representationId?: string, audioRepresentationId?: string, live?: boolean, scheduledFor?: string, clearForm = false): Promise<boolean> {
    const clean = value.trim();
    if (!/^https?:\/\//i.test(clean)) { setNotice("Enter a valid HTTP or HTTPS URL."); return false; }
    let kind: Kind = requestedKind || "direct";
    try {
      if (!requestedKind) {
        const pathname = new URL(clean).pathname.toLowerCase();
        if (pathname.endsWith(".m3u8")) kind = "hls";
        else if (pathname.endsWith(".mpd")) kind = "dash";
      }
    } catch { setNotice("That URL is not valid."); return false; }
    const scheduledAt = scheduledFor ? new Date(scheduledFor).getTime() : undefined;
    if (scheduledAt !== undefined && (!Number.isFinite(scheduledAt) || scheduledAt <= Date.now())) {
      setNotice("Choose a future start time, or clear the schedule field.");
      return false;
    }
    let resolvedRepId = representationId;
    let resolvedAudioRepId = audioRepresentationId;
    // P1-9: the paste path derives live from the inspected variants
    // below (a live playlist has no URL-suffix tell); callers that
    // pass live already classified it.
    let resolvedLive = live;
    const openInVideoGrabber = (ytdlp: boolean) => {
      setVideoUrl(clean);
      setAddOpen(false);
      setScheduleAt("");
      setLinkGrabberOpen(false);
      // P1-11: the LinkGrabber notice must not outlive the dialog it
      // was shown in — it resurfaces stale on the next reopen.
      setLinkGrabberNotice("");
      setGrabbedLinks((current) => current.filter((entry) => entry !== clean && entry.trim() !== clean));
      setGrabberOpen(true);
      if (ytdlp) void inspectYtDlp(clean);
      else void inspect(clean);
    };
    // Paste path only (no requestedKind): a YouTube/social page routes
    // to the yt-dlp inspect flow — the grabber opens and the formats are
    // listed for the user to pick. Everything else asks the engine to
    // classify the URL before any job exists. Callers that pass
    // requestedKind already classified it — the Video page inspected the
    // URL, the grabber only yields media links, and a retry re-queues a
    // job that was already accepted — so they skip this probe.
    // inspect_video rejects a webpage (its error says the URL "is a
    // webpage"); that verdict refuses the add with the Format-support
    // wording and creates no job. Any other probe failure (server refused
    // the metadata probe, network down) is not a webpage verdict: the
    // URL queues exactly as before and start_download still validates it.
    if (!requestedKind) {
      if (isYtDlpPageUrl(clean)) {
        openInVideoGrabber(true);
        // P1-11: the grabber opened — the request was handled, not
        // failed. Returning false here made the LinkGrabber show
        // "That link could not be queued" for a YouTube URL.
        return true;
      }
      setNotice("Checking link…");
      try {
        // The pasted URL as its own referer: hotlink-protected hosts
        // accept a same-origin referer, and the desktop has no better
        // one. The engine validates it before use.
        const inspected = await invoke<Variant[]>("inspect_video_url", { url: clean, referer: clean });
        // P1-9: kind + live come from the inspected variants, not the
        // URL suffix. An extensionless HLS/DASH URL has no suffix to
        // read (it used to queue as "direct" and download raw
        // playlist text), and a live playlist looks identical to a
        // VOD one until its media playlist is read (no ENDLIST) — it
        // used to queue as VOD and fail at start, because the engine
        // rejects live playlists on the VOD path. inspect_video_url
        // classifies by suffix first, then by Content-Type, so its
        // kind is the engine's own verdict; isLive rides on the
        // variants (HLS without ENDLIST, dynamic DASH).
        const inspectedKind = inspected[0]?.kind;
        if (inspectedKind === "hls" || inspectedKind === "dash" || inspectedKind === "direct") kind = inspectedKind;
        const inspectedLive = inspected.some((variant) => variant.isLive === true);
        if (kind === "dash" && inspectedLive) {
          // The engine has no live DASH recorder (dynamic MPDs are
          // refused at start), and a dash row never carries the live
          // flag — refuse honestly instead of queueing a VOD job
          // that fails at start.
          setNotice("This is a live DASH stream, which ThunderLoad can't record yet.");
          return false;
        }
        if (kind === "hls" && inspectedLive) resolvedLive = true;
        if (kind === "dash" && !resolvedRepId) {
          const best = inspected.find((variant) => variant.downloadable && variant.representationId);
          if (best?.representationId) {
            resolvedRepId = best.representationId;
            if (!resolvedAudioRepId && best.audioRepresentationId) {
              resolvedAudioRepId = best.audioRepresentationId;
            }
          } else {
            openInVideoGrabber(false);
            return true;
          }
        }
      } catch (error) {
        if (String(error).includes("is a webpage")) {
          setNotice(YOUTUBE_PAGE_MESSAGE);
          return false;
        }
        if (kind === "dash" && !resolvedRepId) {
          openInVideoGrabber(false);
          return true;
        }
      }
    }
    if (kind === "dash" && !resolvedRepId) {
      openInVideoGrabber(false);
      return true;
    }
    const suggested = nameFromUrl(clean);
    const item: Item = {
      id: crypto.randomUUID(), name: (saveAs || "").trim() || (kind === "hls" || kind === "dash" ? "video.mp4" : suggested),
      url: clean, kind, status: "Queued", percent: 0, downloaded: 0,
      representationId: resolvedRepId, audioRepresentationId: resolvedAudioRepId,
      // Only an HLS row can be live — the queue effect
      // routes it to the live recorder; yt-dlp live and DASH live
      // never take this path (yt-dlp handles its own, DASH live is
      // refused by the engine).
      live: resolvedLive === true && kind === "hls" ? true : undefined,
      detail: scheduledAt ? "Scheduled for " + new Date(scheduledAt).toLocaleString() : "Waiting in queue",
      createdAt: Date.now(),
      scheduledAt
    };
    // Append, not prepend. The table sorts newest-first for display
    // (`ordered`), but the dispatcher takes queued jobs in list order —
    // prepending made the newest job jump the queue and starved older
    // ones. The functional update stays: the render closure's `items`
    // goes stale when several adds land before a re-render, and a stale
    // closure would keep only the last item.
    setItems((current) => [...current, item]);
    // Record the queued job in the durable store immediately, so it
    // survives a restart even if it never gets a download slot before
    // the app closes.
    void invoke("sync_jobs", { jobs: [itemToSyncJob(item)] }).catch(() => {});
    setNotice(scheduledAt ? "Added to queue. Registering scheduled wake…" : "Added to queue.");
    if (scheduledAt && scheduledFor) {
      void invoke<string>("schedule_app_wake", { jobId: item.id, localDateTime: scheduledFor })
        .then((status) => setNotice(status))
        .catch((error) => setNotice("Queued locally, but a scheduled wake task could not be registered: " + String(error)));
    }
    // The Add-dialog inputs are cleared only when this add came from the
    // dialog's own submit (clearForm). Retry, grabber, subtitle and
    // LinkGrabber adds must not wipe what the user typed in the dialog.
    if (clearForm) { setUrl(""); setFilename(""); setScheduleAt(""); }
    return true;
  }
  async function pause(item: Item) {
    // The row flips to "Pausing" BEFORE the invoke — the old code set it
    // after the await, and a pause event landing in between (or a slow
    // backend) left the row wedged. Terminal rows never flip. If the
    // invoke fails, the row goes back to the status it had, instead of
    // stranding on "Pausing".
    const prevStatus = item.status;
    const prevDetail = item.detail;
    setItems((current) => current.map((entry) => entry.id === item.id
      && !["Completed", "Failed", "Interrupted", "Cancelled"].includes(entry.status)
      // A live recording has no checkpoint to save — the pause keeps the
      // partial recording.
      ? { ...entry, status: "Pausing", detail: item.live ? "Pausing · keeping partial recording…" : "Pausing · saving checkpoint…" }
      : entry));
    try {
      if (item.source === "native") {
        await invoke("control_native_download", { jobId: item.id.replace(/^native-/, ""), action: "pause" });
      } else {
        await invoke("pause_download", { jobId: item.id });
      }
    } catch (error) {
      setItems((current) => current.map((entry) => entry.id === item.id && entry.status === "Pausing"
        ? { ...entry, status: prevStatus, detail: prevDetail }
        : entry));
      setNotice(String(error));
    }
  }
  // P1-25: in-flight guard — a double Resume (double-click, keyboard
  // P plus a button tap) must not hand the same checkpoint over twice.
  async function resume(item: Item) {
    if (resumingIds.current.has(item.id)) return;
    resumingIds.current.add(item.id);
    try {
      await resumeInner(item);
    } finally {
      resumingIds.current.delete(item.id);
    }
  }
  async function resumeInner(item: Item) {
    // Pause is terminal for a live recording — there is no checkpoint to
    // resume from, and re-queuing the same target would re-record over
    // the kept partial. A live job never reaches this function through
    // the UI (Resume is not offered for live jobs); this guard is the
    // last line of defence if a caller bypasses those gates — it routes
    // to a fresh recording instead.
    if (item.live) {
      retry(item);
      return;
    }
    if (item.source === "native") {
      if (item.status === "Interrupted" && item.targetPath) {
        // The browser companion has exited. Hand its persistent checkpoint
        // to the desktop engine so the user can still continue the download
        // — direct, HLS and DASH checkpoints all live next to the target.
        // Keep the companion's bare job ID (matching the JOB_STORE record
        // left intact by dismiss_native_download) so the desktop engine
        // reuses the stored browser session cookies on resume.
        const bareId = item.id.replace(/^native-/, "");
        // P1-25: a failed handover must surface — the old
        // `.catch(() => {})` queued a recovery row for a checkpoint
        // the desktop never received, and the row then failed again
        // on start with no explanation.
        try {
          await invoke("handover_native_download", { jobId: bareId });
        } catch (error) {
          setNotice("Could not move the browser checkpoint to the desktop: " + String(error));
          return;
        }
        const recovered: Item = {
          ...item, id: bareId, source: undefined, status: "Queued",
          detail: "Recovering browser checkpoint in desktop engine", error: undefined
        };
        setItems((current) => [recovered, ...current.filter((entry) => entry.id !== item.id && entry.id !== bareId)]);
        // P1-26: the handover renames the row (native-<id> -> <id>),
        // so a selection pointing at the old id would find nothing
        // and the inspector would close — carry it to the new id.
        setSelectedId((current) => current === item.id ? bareId : current);
        void invoke("sync_jobs", { jobs: [itemToSyncJob(recovered)] }).catch(() => {});
        setNotice("Browser checkpoint moved to the desktop queue.");
        return;
      }
      if (!item.targetPath) { retry(item); return; }
      try {
        await invoke("control_native_download", { jobId: item.id.replace(/^native-/, ""), action: "resume" });
        setItems((current) => current.map((entry) => entry.id === item.id
          ? { ...entry, status: "Downloading", error: undefined, detail: "Resume requested · continuing saved checkpoint" }
          : entry));
      } catch (error) { setNotice(String(error)); }
      return;
    }
    // Desktop direct/HLS/DASH jobs resume the SAME item so the queue
    // effect passes its targetPath back and the backend reuses the
    // on-disk checkpoint instead of reserving a fresh target and
    // restarting from scratch.
    if (item.targetPath) {
      setItems((current) => current.map((entry) => entry.id === item.id
        ? { ...entry, status: "Queued", error: undefined, detail: "Resume queued from saved checkpoint" }
        : entry));
      return;
    }
    retry(item);
  }
  async function cancel(item: Item) {
    if (item.scheduledAt) {
      await invoke("cancel_app_wake", { jobId: item.id }).catch(() => {});
      // A cancelled schedule is no longer waiting: forget it so the job
      // leaves the Scheduler page instead of lingering there as Cancelled.
      setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, scheduledAt: undefined } : entry));
    }
    if (item.source === "native") {
      // The row flips to "Cancelling" BEFORE the invoke — the old code
      // set it after the await, overwriting the "Cancelled" the backend
      // event had already delivered. If the invoke fails, the row goes
      // back to the status it had.
      const prevStatus = item.status;
      const prevDetail = item.detail;
      try {
        if (item.status === "Interrupted") {
          // No native process owns this job anymore: delete its orphaned
          // checkpoint locally (if any) and drop its record. A live
          // recording has no checkpoint — the backend keeps its partial
          // file, so the row keeps its target and says so. Cancel is
          // never offered for live jobs, so this is only reachable if a
          // caller bypasses the button gates.
          if (item.targetPath) {
            await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath });
          }
          await invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {});
          setItems((current) => current.map((entry) => entry.id === item.id
            ? item.live
              ? { ...entry, status: "Cancelled", detail: "Recording stopped · partial file kept" }
              : { ...entry, status: "Cancelled", targetPath: undefined, downloaded: 0, percent: 0, detail: "Cancelled · checkpoint removed" }
            : entry));
        } else {
          // Direct, HLS and DASH browser jobs alike: the companion owns
          // the checkpoint, so cancel goes through its control channel
          // and the companion deletes its own partial files.
          setItems((current) => current.map((entry) => entry.id === item.id
            && !["Completed", "Failed", "Interrupted", "Cancelled"].includes(entry.status)
            ? { ...entry, status: "Cancelling", detail: "Cancelling browser download…" }
            : entry));
          await invoke("control_native_download", { jobId: item.id.replace(/^native-/, ""), action: "cancel" });
        }
      } catch (error) {
        setItems((current) => current.map((entry) => entry.id === item.id && entry.status === "Cancelling"
          ? { ...entry, status: prevStatus, detail: prevDetail }
          : entry));
        setNotice(String(error));
      }
      return;
    }
    // C2: route by state, not by targetPath. A job that started — or
    // is starting, its target not yet landed in the frontend — cancels
    // through the backend, which owns the live task and the checkpoint.
    // Only a job that never started cancels locally. The row is marked
    // busy until the call settles so ×/Delete cannot race it. The row
    // flips to "Cancelling" BEFORE the invoke below — setting it after
    // the await overwrote the "Cancelled" the backend event had already
    // delivered, wedging the row on "Cancelling". If the invoke fails,
    // the row goes back to the status it had.
    const prevStatus = item.status;
    const prevDetail = item.detail;
    setBusyIds((current) => current.includes(item.id) ? current : [...current, item.id]);
    try {
      if (!startingIds.current.has(item.id) && !item.targetPath && item.status === "Queued") {
        // Never started: the durable row must flip too, or the job
        // would come back as Queued on the next startup. Awaited, not
        // fire-and-forget — cancel_queued_job REFUSES once start_job
        // has run, and the row must not flip locally when it does.
        await invoke("cancel_queued_job", { jobId: item.id });
        setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "Cancelled", detail: "Cancelled" } : entry));
        return;
      }
      setItems((current) => current.map((entry) => entry.id === item.id
        && !["Completed", "Failed", "Interrupted", "Cancelled"].includes(entry.status)
        // Cancel is never offered for live jobs, so this is only
        // reachable if a caller bypasses the button gates — the backend
        // keeps a live target either way, so the detail must not claim
        // the partial was deleted.
        ? { ...entry, status: "Cancelling", detail: item.live ? "Cancelling…" : "Cancelling and deleting partial file…" }
        : entry));
      await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath || "" });
    } catch (error) {
      setItems((current) => current.map((entry) => entry.id === item.id && entry.status === "Cancelling"
        ? { ...entry, status: prevStatus, detail: prevDetail }
        : entry));
      setNotice(String(error));
    }
    finally { setBusyIds((current) => current.filter((id) => id !== item.id)); }
  }
  async function remove(id: string) {
    const item = items.find((entry) => entry.id === id);
    if (!item) return;
    // Removing a row with a saved checkpoint deletes the partial file
    // (via cancel_download below) — destructive and irreversible, so
    // Paused/Failed/Interrupted/Queued rows with a target ask first.
    // Completed rows have no checkpoint to lose, and a live recording's
    // file is never deleted, so neither asks.
    if (item.targetPath && !item.live && ["Paused", "Failed", "Interrupted", "Queued"].includes(item.status)) {
      let ok: boolean;
      try {
        ok = await confirm("Remove \"" + item.name + "\"?\n\nThis deletes the saved partial file — the download cannot be resumed afterwards.");
      } catch {
        ok = window.confirm("Remove \"" + item.name + "\"? This deletes the saved partial file and the download cannot be resumed.");
      }
      if (!ok) return;
    }
    // Mark the row busy until remove settles, so a second × / Delete /
    // context-menu Remove cannot race this one.
    setBusyIds((current) => current.includes(id) ? current : [...current, id]);
    if (item?.scheduledAt) {
      await invoke("cancel_app_wake", { jobId: item.id }).catch(() => {});
    }
    // A live browser job is cancelled, not removed: the × button on an
    // active native job acts as Cancel (all kinds alike). A live
    // recording is the exception — its companion's job already ended
    // when it paused or failed, so there is no live task to cancel;
    // fall through to the dismiss below, which drops the stale record
    // and keeps the recording file.
    if (item?.source === "native" && !item.live
        && ["Downloading", "Pausing", "Paused", "Failed"].includes(item.status)) {
      await cancel(item);
      setBusyIds((current) => current.filter((entry) => entry !== id));
      return;
    }
    if (item?.source === "native") {
      // D10: the desktop never deletes a companion-owned checkpoint with
      // its own cancel_download — only an Interrupted job (no companion
      // left) has its orphaned files removed locally. Dismissing asks a
      // live companion to cancel first and waits for it, so the companion
      // cannot resurrect the record afterwards. A live Interrupted job
      // is the exception: its partial file IS the recording, so the
      // backend must not delete it — cancel_download skips target
      // deletion for live rows, prefixed id included.
      if (item.status === "Interrupted" && item.targetPath && !item.live) {
        await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath }).catch(() => {});
      }
      await invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {});
    } else if (item?.targetPath && item.status !== "Completed" && !item.live) {
      // Never call cancel_download for a live recording — removing the
      // row is enough; the file is kept.
      await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath }).catch(() => {});
    }
    // Drop the durable row too, or a removed job would be re-seeded from
    // the database on the next startup.
    if (item?.source !== "native") {
      void invoke("remove_job", { jobId: id }).catch(() => {});
    }
    // The job is gone: its rate sample must not linger and skew the
    // status-bar aggregate.
    speedSamples.current.delete(id);
    // The filter keeps a Cancelling row, not just Downloading/Pausing —
    // removing the row while the backend is still cancelling would leave
    // the cancel event with no row to land on.
    setItems((current) => current.filter((entry) => entry.id !== id || entry.status === "Downloading" || entry.status === "Pausing" || entry.status === "Cancelling"));
    setBusyIds((current) => current.filter((entry) => entry !== id));
  }
  function retry(item: Item) {
    // A live recording has no checkpoint — a retry is a FRESH recording
    // on a fresh target, never the old one. Re-queueing the same target
    // would re-record over the kept partial; the old partial stays on
    // disk.
    if (item.live) {
      add(item.url, item.name, item.kind, item.representationId, item.audioRepresentationId, true);
      return;
    }
    // A job with a checkpoint on disk resumes from it — a browser job
    // through its companion, a desktop job through the desktop engine.
    // Without a checkpoint there is nothing to resume, so a fresh
    // desktop job is queued instead.
    if (item.targetPath) { void resume(item); return; }
    add(item.url, item.name, item.kind, item.representationId, item.audioRepresentationId);
  }
  async function clearFinished() {
    const finished = items.filter((item) => item.status === "Completed" || item.status === "Cancelled");
    await Promise.all(finished.filter((item) => item.scheduledAt)
      .map((item) => invoke("cancel_app_wake", { jobId: item.id }).catch(() => {})));
    await Promise.all(finished.filter((item) => item.source === "native")
      .map((item) => invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {})));
    // Drop the durable rows too, or cleared jobs would be re-seeded from
    // the database on the next startup.
    await Promise.all(finished.filter((item) => item.source !== "native")
      .map((item) => invoke("remove_job", { jobId: item.id }).catch(() => {})));
    finished.forEach((item) => speedSamples.current.delete(item.id));
    setItems((current) => current.filter((item) => item.status !== "Completed" && item.status !== "Cancelled"));
  }
  // Inspect a YouTube/social page with yt-dlp and list its formats as
  // variants (kind "ytdlp", representationId = format id). A missing
  // tool is data, not an error — the notice names the tool and points
  // at Settings, where the direct download button lives.
  async function inspectYtDlp(pageUrl: string) {
    setInspecting(true); setVariants([]); setVideoTitle(""); setInspectedUrl("");
    setVideoNotice("Getting formats…");
    try {
      const result = await invoke<{ title?: string; variants: Variant[]; toolsMissing?: string }>("inspect_ytdlp_url", { url: pageUrl });
      if (result.toolsMissing) {
        setVideoNotice(result.toolsMissing + " is not installed yet — download it from Settings, then inspect again.");
        return;
      }
      setVariants(result.variants);
      setVideoTitle(result.title || "");
      setInspectedUrl(pageUrl);
      setVideoNotice(result.variants.length ? result.variants.length + " format option(s) found." : "No downloadable formats found.");
    } catch (error) { setVideoNotice(String(error)); }
    finally { setInspecting(false); }
  }
  async function loadYtDlpTools() {
    try {
      const status = await invoke<{ ytdlp: { present: boolean; version?: string }; deno: { present: boolean; version?: string }; toolsDir: string }>("get_ytdlp_tools_status");
      setYtdlpTools(status);
    } catch { /* Settings shows the loading state again on next visit. */ }
  }
  async function downloadYtDlpTool(tool: "ytdlp" | "deno", displayName: string) {
    // The tap itself is the consent: nothing downloads until the user
    // presses this button, and progress names the tool being fetched.
    setYtdlpDownloading(tool);
    setYtdlpToolProgress({ tool: displayName, percent: 0 });
    try {
      await invoke<string>("ensure_ytdlp_tool", { tool });
      setYtdlpToolProgress(null);
      setYtdlpUpdateStatus(displayName + " downloaded and verified.");
      await loadYtDlpTools();
    } catch (error) {
      setYtdlpToolProgress(null);
      setYtdlpUpdateStatus("Could not download " + displayName + ": " + String(error));
    } finally {
      setYtdlpDownloading(null);
    }
  }
  async function checkYtDlpUpdate() {
    // This button APPLIES an update, it does not just check: yt-dlp -U
    // downloads and installs the latest release, so ask first.
    let ok: boolean;
    try {
      ok = await confirm("Update yt-dlp?\n\nThis downloads and installs the latest yt-dlp release — it does not just check for one. The new version replaces the installed binary.");
    } catch {
      ok = window.confirm("Update yt-dlp? This downloads and installs the latest yt-dlp release — it does not just check for one. The new version replaces the installed binary.");
    }
    if (!ok) return;
    setYtdlpCheckingUpdate(true);
    setYtdlpUpdateStatus("Updating yt-dlp…");
    try {
      setYtdlpUpdateStatus(await invoke<string>("check_ytdlp_update"));
      await loadYtDlpTools();
    } catch (error) { setYtdlpUpdateStatus(String(error)); }
    finally { setYtdlpCheckingUpdate(false); }
  }
  async function inspect(overrideUrl?: string) {
    const clean = (overrideUrl ?? videoUrl).trim();
    if (!/^https?:\/\//i.test(clean)) { setVideoNotice("Paste a valid HTTP or HTTPS media URL."); return; }
    if (isYtDlpPageUrl(clean)) { await inspectYtDlp(clean); return; }
    setInspecting(true); setVariants([]); setInspectedUrl(""); setVideoNotice("Inspecting media URL…");
    try {
      const result = await invoke<Variant[]>("inspect_video_url", { url: clean, referer: clean });
      setVariants(result);
      setInspectedUrl(clean);
      setVideoNotice(result.length ? result.length + " stream option(s) found." : "No downloadable streams found.");
    } catch (error) { setVideoNotice(String(error)); }
    finally { setInspecting(false); }
  }
  function hostOf(value: string) {
    try { return new URL(value).hostname; } catch { return value; }
  }
  async function grabPage() {
    const clean = grabUrl.trim();
    if (!/^https?:\/\//i.test(clean)) { setGrabNotice("Paste a valid HTTP or HTTPS page URL."); return; }
    setGrabbing(true);
    setGrabNotice("");
    try {
      const result = await invoke<GrabResult>("grab_page_media", { pageUrl: clean });
      setGrabbed(result.items);
      setGrabTruncated(result.truncated);
      setGrabChecked(Object.fromEntries(result.items.map((item) => [item.url, true])));
      setGrabNotice(result.items.length
        ? ""
        : "No media links were found in this page's HTML. Pages that need a login or load media with JavaScript need the browser extension instead.");
    } catch (error) {
      setGrabbed([]);
      setGrabTruncated(false);
      setGrabChecked({});
      setGrabNotice(String(error));
    } finally {
      setGrabbing(false);
    }
  }
  // A subtitle track from the inspected variants. An HLS subtitle
  // playlist (.m3u8) is assembled into one .vtt file by the backend; a
  // direct subtitle file (vtt/ttml/srt) is just a direct download and
  // queues like any other file.
  async function downloadSubtitle(subtitle: SubtitleVariant) {
    // P2: in-flight guard — a double-click must not save the same
    // track twice.
    if (downloadingSubtitles.current.has(subtitle.url)) return;
    downloadingSubtitles.current.add(subtitle.url);
    try {
    if (subtitle.format === "m3u8") {
      try {
        const path = await invoke<string>("download_subtitles", { playlistUrl: subtitle.url, filename: (subtitle.name || subtitle.language || "subtitles") + ".vtt" });
        setVideoNotice("Saved · " + path);
      } catch (error) { setVideoNotice(String(error)); }
    } else {
      // A direct subtitle file queues as a direct download with an
      // explicit kind and a filename from the track name/language, so the
      // saved file is named after the track instead of the URL.
      const base = subtitle.name || subtitle.language || "subtitles";
      const ext = subtitle.url.match(/\.(vtt|srt|ttml|ass|ssa)(\?|#|$)/i)?.[1]?.toLowerCase() || "vtt";
      const filename = base.toLowerCase().endsWith("." + ext) ? base : base + "." + ext;
      await add(subtitle.url, filename, "direct");
    }
    } finally {
      downloadingSubtitles.current.delete(subtitle.url);
    }
  }
  // P1-21 + P1-24: one Video Grabber variant's Download button. The
  // add() result is awaited and surfaced in the modal: success shows
  // confirmation, failure shows the error — never a success for an
  // add() that returned false. The filename comes from the variant
  // itself: a direct file keeps its real name from the URL (a .zip
  // or .pdf is saved under its own name, not video.mp4); HLS/DASH
  // pass a bare stem — the backend probes the real container
  // extension and appends it on disk — audio-only variants use the
  // "audio" stem; yt-dlp keeps title + ".mp4" because the backend
  // strips the extension and substitutes the real one
  // ({stem}.%(ext)s), and a bare title would truncate dotted titles
  // via Path::with_extension("").
  async function downloadVariant(variant: Variant, key: string) {
    if (grabberVariantBusy !== null) return;
    setGrabberVariantBusy(key);
    setVideoNotice("");
    try {
      const kind: Kind = variant.kind === "hls" ? "hls" : variant.kind === "dash" ? "dash" : variant.kind === "ytdlp" ? "ytdlp" : "direct";
      const filename = kind === "direct"
        ? nameFromUrl(variant.url)
        : kind === "ytdlp"
          ? (videoTitle ? videoTitle + ".mp4" : variant.audioOnly ? "audio.mp4" : "video.mp4")
          : variant.audioOnly ? "audio" : "video";
      const queued = await add(
        kind === "dash" || kind === "hls" || kind === "ytdlp" ? (inspectedUrl || videoUrl) : variant.url,
        filename, kind,
        kind === "hls" ? variant.url : variant.representationId,
        variant.audioRepresentationId, variant.isLive
      );
      setVideoNotice(queued ? "Added to queue." : "That link could not be queued.");
    } catch (error) {
      setVideoNotice(String(error));
    } finally {
      setGrabberVariantBusy(null);
    }
  }
  async function downloadGrabbed() {
    if (downloadingGrabbed) return;
    const chosen = grabbed.filter((item) => grabChecked[item.url]);
    if (!chosen.length) return;
    setDownloadingGrabbed(true);
    setGrabNotice("Queueing " + chosen.length + " item(s)…");
    let queued = 0;
    let failed = 0;
    try {
    // Sequential on purpose: each item goes through the same path a
    // one-off add would take — DASH inspects first and takes the best
    // quality, HLS and direct files queue directly — so every item
    // lands in the job store as its own job with its own checkpoint.
    for (const item of chosen) {
      try {
        if (item.kind === "dash") {
          // Referer is the page the manifest was grabbed from — the host
          // that linked to it, not the manifest itself.
          const result = await invoke<Variant[]>("inspect_video_url", { url: item.url, referer: grabUrl.trim() });
          const variants = result.filter((variant) => variant.downloadable && variant.url);
          variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
          const best = variants[0];
          if (!best) { failed += 1; continue; }
          // Await the add and count what actually queued — the old
          // fire-and-forget call counted every item while only the last
          // survived the stale closure.
          if (await add(item.url, item.name, "dash", best.representationId, best.audioRepresentationId, undefined, grabScheduleAt || undefined)) queued += 1; else failed += 1;
        } else if (item.kind === "hls") {
          // Inspect first, like DASH: a live playlist must queue
          // through the live recorder, not the VOD path (which
          // rejects it). If the inspect fails, queue it exactly as
          // before — the start call surfaces the real error then.
          let ok: boolean;
          try {
            const result = await invoke<Variant[]>("inspect_video_url", { url: item.url, referer: grabUrl.trim() });
            const variants = result.filter((variant) => variant.downloadable && variant.url);
            variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
            const best = variants[0];
            // VOD passes the chosen variant's media URL + audio rendition
            // like the live branch does — without them the download falls
            // back to the first variant.
            if (best?.isLive) ok = await add(item.url, item.name, "hls", best.url, best.audioRepresentationId, true, grabScheduleAt || undefined);
            else if (best) ok = await add(item.url, item.name, "hls", best.url, best.audioRepresentationId, undefined, grabScheduleAt || undefined);
            else ok = await add(item.url, item.name, "hls", undefined, undefined, undefined, grabScheduleAt || undefined);
          } catch { ok = await add(item.url, item.name, "hls", undefined, undefined, undefined, grabScheduleAt || undefined); }
          if (ok) queued += 1; else failed += 1;
        } else {
          if (await add(item.url, item.name, "direct", undefined, undefined, undefined, grabScheduleAt || undefined)) queued += 1; else failed += 1;
        }
      } catch { failed += 1; }
    }
    } finally {
      setDownloadingGrabbed(false);
    }
    setGrabNotice(failed
      ? "Queued " + queued + " of " + chosen.length + " grabbed items (" + failed + " could not be started)."
      : "Queued " + queued + " grabbed item" + (queued === 1 ? "" : "s") + ".");
  }
  async function saveFfmpegPath() {
    setFfmpegSaving(true);
    setFfmpegStatus("Verifying FFmpeg…");
    try {
      const status = await invoke<string>("set_ffmpeg_path", { path: ffmpegPath });
      setFfmpegStatus(status);
    } catch (error) {
      setFfmpegStatus(String(error));
    } finally { setFfmpegSaving(false); }
  }
  async function registerNativeHost() {
    const extensionId = nativeExtensionId.trim();
    if (!extensionId) { setNativeSetupStatus("Enter your browser extension ID first."); return; }
    setNativeSettingUp(true);
    setNativeSetupStatus("Registering local browser connection…");
    try {
      const message = await invoke<string>("register_native_host", {
        browser: nativeBrowser,
        extensionId
      });
      // Save the ID once: later startups re-register from it silently.
      const ids = readNativeIds();
      ids[nativeBrowser] = extensionId;
      localStorage.setItem(NATIVE_IDS_KEY, JSON.stringify(ids));
      setConnectedBrowsers(listNativeBrowsers());
      setShowAddBrowser(false);
      setNativeBanner(null);
      // P1-23: the connection is fixed — a dismissal stored for the
      // old banner must not hide a future one.
      setDismissedBannerKind("");
      setNativeSetupStatus(message);
    } catch (error) {
      setNativeSetupStatus(String(error));
    } finally {
      setNativeSettingUp(false);
    }
  }
  // Opens the Add form on the first browser that is not connected yet,
  // with a fresh ID field (Firefox keeps its fixed ID).
  function openAddBrowser() {
    const connected = new Set(connectedBrowsers.map((item) => item.browser));
    const next = NATIVE_BROWSERS.find((item) => !connected.has(item.id));
    if (!next) return;
    setNativeBrowser(next.id);
    setNativeExtensionId(next.id === "firefox" ? "thunderload@example.com" : "");
    setShowAddBrowser(true);
  }
  async function removeNativeBrowser(browser: string) {
    // One removal at a time — the buttons are disabled while one runs,
    // and this guard covers a second click before the state update lands.
    if (removingNativeBrowser) return;
    const entry = connectedBrowsers.find((item) => item.browser === browser);
    if (!entry) return;
    const name = nativeBrowserName(browser);
    setRemovingNativeBrowser(browser);
    setNativeSetupStatus(`Removing ${name}…`);
    try {
      // The backend decides whether the registration must stay: only
      // Chrome and Opera on Windows share one, and the frontend cannot
      // tell that from two browsers that merely loaded the same
      // unpacked folder (same extension ID, separate registrations).
      // The still-connected list comes from storage, not state, so a
      // click before a state update cannot see a stale list.
      const remainingBrowsers = Object.entries(readNativeIds())
        .filter(([itemBrowser]) => itemBrowser !== browser)
        .map(([itemBrowser, extensionId]) => ({ browser: itemBrowser, extensionId }));
      const result = await invoke<{ message: string; registrationKept: boolean }>("unregister_native_host", {
        browser,
        extensionId: entry.extensionId,
        remainingBrowsers,
      });
      const ids = readNativeIds();
      delete ids[browser];
      localStorage.setItem(NATIVE_IDS_KEY, JSON.stringify(ids));
      const remaining = listNativeBrowsers();
      setConnectedBrowsers(remaining);
      if (!remaining.length) setNativeBanner("missing-id");
      setNativeSetupStatus(result.registrationKept
        ? `${name} removed. Its browser registration stays — another connected browser uses the same extension ID.`
        : `${name} removed.`);
    } catch (error) {
      setNativeSetupStatus(String(error));
    } finally {
      setRemovingNativeBrowser(null);
    }
  }
  async function downloadExtensionFile(kind: "chromium" | "firefox") {
    const isFirefox = kind === "firefox";
    const fileName = isFirefox ? "ThunderLoad-Firefox.xpi" : "ThunderLoad-Chromium.zip";
    setExtensionFileSaving(true);
    setExtensionFileStatus("Choose where to save " + fileName + "…");
    try {
      const destination = await save({
        defaultPath: fileName,
        filters: [{ name: isFirefox ? "Firefox add-on" : "Chromium extension package", extensions: [isFirefox ? "xpi" : "zip"] }]
      });
      if (!destination) { setExtensionFileStatus(""); return; }
      const saved = await invoke<string>("save_extension_file", { browser: kind, destination });
      setExtensionFileStatus("Saved " + fileName + " to " + saved + ".");
    } catch (error) {
      setExtensionFileStatus(String(error));
    } finally {
      setExtensionFileSaving(false);
    }
  }
  async function openFolder() {
    try { await invoke("open_download_folder"); }
    catch (error) { setNotice("Could not open download folder: " + String(error)); }
  }
  // Sidebar navigation also closes the inspector (and any open context
  // menu): details belong to the page they were opened from.
  function goTo(next: Page) {
    setPage(next);
    setSelectedId(null);
    setContextMenu(null);
  }
  // The per-row controls, unchanged from the old card layout: they
  // appear on row hover (EagleGet pattern) and always in the inspector,
  // so touch and keyboard users never lose them.
  function actionButtons(item: Item) {
    return <>
      {item.status === "Downloading" && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void pause(item); }}>Pause</button>}
      {item.status === "Paused" && !item.live && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void resume(item); }}>Resume</button>}
      {(item.status === "Failed" || item.status === "Interrupted") && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void retry(item); }}>{item.targetPath && !item.live ? "Resume" : "Retry"}</button>}
      {canCancel(item) && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void cancel(item); }}>Cancel</button>}
      {canRemove(item) && <button className="remove-button" title="Remove" aria-label="Remove" disabled={busyIds.includes(item.id)} onClick={(event) => { event.stopPropagation(); void remove(item.id); }}>×</button>}
    </>;
  }
  // Direction A table: six data columns by default (Name · Progress ·
  // Size · Speed · Time left · Status), plus an optional Transfer kind
  // column behind the Columns toggle. A real <table>, not custom rows —
  // numbers are tabular and right aligned, and a click selects the
  // row for the inspector. Headers do not sort (no sort state exists).
  function downloadTable(list: Item[], emptyTitle: string, emptyHint: string, showKindColumn = false) {
    // One coloured primary per view: the empty state offers a ghost
    // button, the page heading keeps the "+ Add" primary.
    if (!list.length) return <div className="empty-state"><span className="empty-icon">↓</span><strong>{emptyTitle}</strong><p>{emptyHint}</p><button className="secondary-button" onClick={() => setAddOpen(true)}>＋ Add download</button></div>;
    return <div className="table-scroll"><table className="dl-table">
      <thead><tr><th className="col-name">Name</th><th className="col-progress">Progress</th><th className="col-num">Size</th><th className="col-num">Speed</th><th className="col-num">Time left</th>{showKindColumn && <th className="col-kind">Transfer kind</th>}<th className="col-status">Status</th></tr></thead>
      <tbody>{list.map((item) => {
        const speed = speedCell(item);
        const eta = etaCell(item);
        return <tr key={item.id} className={"dl-row" + (selectedId === item.id ? " selected" : "")} onClick={() => { setSelectedId(item.id); setInspectorTab("overview"); }} tabIndex={0} onContextMenu={(event) => { event.preventDefault(); setSelectedId(item.id); setInspectorTab("overview"); setContextMenu({ x: Math.min(event.clientX, window.innerWidth - 190), y: Math.min(event.clientY, window.innerHeight - 230), id: item.id }); }} onKeyDown={(event) => { if (event.target !== event.currentTarget) return; if (event.key === "Enter" || event.key === " ") { event.preventDefault(); setSelectedId(item.id); setInspectorTab("overview"); } }}>
          <td className="col-name"><span className="dl-name" title={item.name}>{item.name}</span><span className="dl-sub" title={item.url}>{hostOf(item.url) || item.kind}</span>{item.error && <span className="error-text">{item.error}</span>}</td>
          <td className="col-progress"><div className="progress"><span style={{ width: item.percent + "%" }} /></div><span className="dl-percent">{item.percent}%</span></td>
          <td className="col-num">{item.total ? bytes(item.total) : "–"}</td>
          <td className={"col-num" + (speed === "–" ? " muted" : "")}>{speed}</td>
          <td className={"col-num" + (eta === "–" ? " muted" : "")}>{eta}</td>
          {showKindColumn && <td className="col-kind">{item.kind === "hls" ? "HLS" : item.kind === "dash" ? "DASH" : item.kind === "ytdlp" ? "yt-dlp" : "Direct"}</td>}
          <td className="col-status"><Status value={item.status} /><span className="row-actions">{actionButtons(item)}</span></td>
        </tr>;
      })}</tbody>
    </table></div>;
  }
  // Row right-click menu: the same actions as the hover row controls,
  // under the same conditions — Pause/Resume, Retry, Cancel, Remove.
  function contextMenuPanel() {
    if (!contextMenu) return null;
    const item = items.find((entry) => entry.id === contextMenu.id);
    if (!item) return null;
    return <div className="context-overlay" onClick={() => setContextMenu(null)} onContextMenu={(event) => { event.preventDefault(); setContextMenu(null); }}>
      <div className="context-menu" style={{ left: contextMenu.x, top: contextMenu.y }} role="menu" onClick={(event) => event.stopPropagation()}>
        {item.status === "Downloading" && <button role="menuitem" onClick={() => { setContextMenu(null); void pause(item); }}>Pause</button>}
        {item.status === "Paused" && !item.live && <button role="menuitem" onClick={() => { setContextMenu(null); void resume(item); }}>Resume</button>}
        {(item.status === "Failed" || item.status === "Interrupted") && <button role="menuitem" onClick={() => { setContextMenu(null); void retry(item); }}>{item.targetPath && !item.live ? "Resume" : "Retry"}</button>}
        {canCancel(item) && <button role="menuitem" onClick={() => { setContextMenu(null); void cancel(item); }}>Cancel</button>}
        {canRemove(item) && <><hr /><button role="menuitem" disabled={busyIds.includes(item.id)} onClick={() => { setContextMenu(null); void remove(item.id); }}>Remove</button></>}
      </div>
    </div>;
  }
  // Right inspector (Motrix pattern): Overview · Files · Segments ·
  // Activity. Every tab shows only what the app actually knows — no
  // invented history, no invented segment data.
  function inspectorPanel() {
    const item = items.find((entry) => entry.id === selectedId);
    if (!item) return null;
    const tabs: Array<[InspectorTab, string]> = [["overview", "Overview"], ["files", "Files"], ["segments", "Segments"], ["activity", "Activity"]];
    return <aside className="inspector" aria-label="Download details">
      <div className="inspector-head">
        <div><strong title={item.name}>{item.name}</strong><Status value={item.status} /></div>
        <button className="remove-button" title="Close details" aria-label="Close details" onClick={() => setSelectedId(null)}>×</button>
      </div>
      <div className="inspector-tabs">{tabs.map(([id, label]) => <button key={id} className={"inspector-tab" + (inspectorTab === id ? " selected" : "")} onClick={() => setInspectorTab(id)}>{label}</button>)}</div>
      {inspectorTab === "overview" && <div className="inspector-body">
        <dl className="kv">
          <div><dt>Status</dt><dd>{item.status} · {item.percent}%</dd></div>
          <div><dt>URL</dt><dd className="kv-wrap" title={item.url}>{item.url || "—"}</dd></div>
          <div><dt>Size</dt><dd>{item.total ? bytes(item.total) : "Unknown"}</dd></div>
          <div><dt>Downloaded</dt><dd>{bytes(item.downloaded)}</dd></div>
          <div><dt>Speed</dt><dd>{speedCell(item) || "—"}</dd></div>
          <div><dt>Time left</dt><dd>{etaCell(item) || "—"}</dd></div>
          <div><dt>Saved to</dt><dd className="kv-wrap" title={item.targetPath || ""}>{item.targetPath || "Not started yet"}</dd></div>
          <div><dt>Created</dt><dd>{new Date(item.createdAt).toLocaleString()}</dd></div>
          {item.scheduledAt && <div><dt>Scheduled for</dt><dd>{new Date(item.scheduledAt).toLocaleString()}</dd></div>}
          <div><dt>Resume capability</dt><dd>{item.live ? "No — live recording, Pause is terminal" : item.targetPath && ["Paused", "Failed", "Interrupted", "Queued"].includes(item.status) ? "Yes — checkpoint saved" : item.targetPath ? "Partial data on disk — a checkpoint is saved when the transfer pauses" : "No"}</dd></div>
          {item.error && <div><dt>Error</dt><dd className="error-text">{item.error}</dd></div>}
        </dl>
        <p className="inspector-detail">{item.detail}</p>
        <div className="inspector-actions">{actionButtons(item)}</div>
      </div>}
      {inspectorTab === "files" && <div className="inspector-body">
        <div className="file-row"><span className="file-type">{item.kind === "hls" ? "▶" : item.kind === "dash" ? "◫" : item.kind === "ytdlp" ? "▷" : "↓"}</span><div><strong title={item.name}>{item.name}</strong><small>{item.total ? bytes(item.total) : bytes(item.downloaded) + " downloaded so far"}</small></div></div>
        <dl className="kv">
          <div><dt>Location</dt><dd className="kv-wrap" title={item.targetPath || ""}>{item.targetPath || "Not started yet"}</dd></div>
          <div><dt>Transfer kind</dt><dd>{item.kind === "hls" ? "HLS playlist" : item.kind === "dash" ? "DASH manifest" : item.kind === "ytdlp" ? "yt-dlp" : "Direct file"}</dd></div>
        </dl>
        <p className="footnote">Partial data is kept next to the target while a transfer is incomplete. Pause saves a checkpoint; Resume continues from it.</p>
      </div>}
      {inspectorTab === "segments" && <div className="inspector-body">
        <dl className="kv">
          <div><dt>Connections</dt><dd>{item.connections !== undefined ? item.connections + " per file" : connections + " per file (current setting — this job started before its connections were recorded)"}</dd></div>
          <div><dt>Segments</dt><dd>{item.segmentsDone !== undefined && item.segmentsTotal !== undefined ? item.segmentsDone + " of " + item.segmentsTotal + " done" : "This transfer does not report per-segment progress."}</dd></div>
          <div><dt>Downloaded</dt><dd>{bytes(item.downloaded)}{item.total ? " of " + bytes(item.total) : ""}</dd></div>
        </dl>
        <p className="footnote">Connections are the ones this job started with. New jobs use Settings → Connections per file. Segment counts show while a segmented transfer runs; a transfer that does not report segments says so above.</p>
      </div>}
      {inspectorTab === "activity" && <div className="inspector-body">
        <ul className="activity-list">
          <li><strong>Created</strong><span>{new Date(item.createdAt).toLocaleString()}</span></li>
          {item.scheduledAt && <li><strong>Scheduled for</strong><span>{new Date(item.scheduledAt).toLocaleString()}</span></li>}
          <li><strong>Current status</strong><span>{item.status} — {item.detail}</span></li>
          {item.error && <li><strong>Error</strong><span className="error-text">{item.error}</span></li>}
        </ul>
        <p className="footnote">Only events this app has actually seen are listed here.</p>
      </div>}
    </aside>;
  }
  // Add dialog: the old Downloads-page form, moved behind the one
  // primary "+ Add" button (Motrix "+" pattern). Behaviour unchanged.
  function addDialog() {
    if (!addOpen) return null;
    const closeAdd = () => { setAddOpen(false); setScheduleAt(""); };
    return <div className="dialog-overlay" role="dialog" aria-label="Add a download" onClick={closeAdd}>
      <div className="dialog" onClick={(event) => event.stopPropagation()}>
        <div className="dialog-head"><div><h2>Add a download</h2><p>Direct file links and supported HLS playlists.</p></div><button className="remove-button" title="Close" aria-label="Close" onClick={closeAdd}>×</button></div>
        <form onSubmit={(event) => { event.preventDefault(); if (addingUrl) return; setAddingUrl(true); void add(url, filename, undefined, undefined, undefined, undefined, scheduleAt || undefined, true).finally(() => setAddingUrl(false)); }}>
          <label htmlFor="download-url">Download URL</label>
          <input id="download-url" className="field" value={url} onChange={(event) => setUrl(event.target.value)} placeholder="https://example.com/archive.zip" autoFocus />
          <div className="add-actions"><div className="filename-field"><label htmlFor="download-name">Save as <small>OPTIONAL</small></label><input id="download-name" className="field" value={filename} onChange={(event) => setFilename(event.target.value)} placeholder="Use original filename" /></div><button type="submit" className="primary-button" disabled={addingUrl}>{addingUrl ? "Adding…" : "Add to queue ↗"}</button></div>
          <div className="schedule-row"><label htmlFor="schedule-at">Start later <small>OPTIONAL</small></label><input id="schedule-at" type="datetime-local" className="field schedule-field" min={scheduleMin} value={scheduleAt} onChange={(event) => setScheduleAt(event.target.value)} /></div>
        </form>
        <div className="notice">{notice}</div>
        {url.trim() && <button className="text-link" onClick={() => { const clean = url.trim(); setVariants([]); setVideoNotice(""); setInspectedUrl(""); setVideoUrl(clean); closeAdd(); setGrabberOpen(true); void inspect(clean); }}>Inspect media qualities instead →</button>}
        <div className="dialog-foot"><span className="connection-tag">{connections} CONNECTIONS</span><button className="quiet-button" onClick={closeAdd}>Done</button></div>
      </div>
    </div>;
  }
  // LinkGrabber dialog (same dialog shape as the grabber). It lists
  // links the clipboard watcher caught — one Download button per link,
  // nothing else inside. A YouTube/social page URL goes through the same
  // inspect flow a paste would take, so the user still picks a format.
  function linkGrabberDialog() {
    if (!linkGrabberOpen) return null;
    return <div className="dialog-overlay" role="dialog" aria-label="Link grabber" onClick={() => setLinkGrabberOpen(false)}>
      <div className="dialog" onClick={(event) => event.stopPropagation()}>
        <div className="dialog-head"><div><h2>Link grabber</h2></div><button className="remove-button" title="Close" aria-label="Close" onClick={() => setLinkGrabberOpen(false)}>×</button></div>
        {grabbedLinks.length === 0
          ? <div className="notice">No links waiting.</div>
          : <div className="variants">
            {grabbedLinks.map((url) => <div className="variant" key={url}>
              <div><strong title={url}>{url.length > 64 ? url.slice(0, 64) + "…" : url}</strong></div>
              <button className="secondary-button" disabled={linkGrabberBusy === url} onClick={async () => {
                // P2: double-click guard — one add per link at a time —
                // and a failed add says so here in the dialog.
                if (linkGrabberBusy !== null) return;
                setLinkGrabberBusy(url);
                setLinkGrabberNotice("");
                try {
                  const queued = await add(url);
                  if (queued) {
                    setGrabbedLinks((current) => current.filter((entry) => entry !== url));
                  } else {
                    setLinkGrabberNotice("That link could not be queued.");
                  }
                } catch (error) {
                  setLinkGrabberNotice(String(error));
                } finally {
                  setLinkGrabberBusy(null);
                }
              }}>{linkGrabberBusy === url ? "Queueing…" : "Download"}</button>
            </div>)}
          </div>}
        {linkGrabberNotice && <div className="notice">{linkGrabberNotice}</div>}
        <div className="dialog-foot"><span className="connection-tag">{grabbedLinks.length} LINK{grabbedLinks.length === 1 ? "" : "S"}</span><button className="quiet-button" onClick={() => setLinkGrabberOpen(false)}>Done</button></div>
      </div>
    </div>;
  }
  // Video grabber dialog (Direction A: the grabber is not a main-window
  // page — it is a dialog reachable from the Queues page and from the
  // Add form's "Inspect media qualities" link). Content is the old
  // Video page, unchanged.
  function grabberDialog() {
    if (!grabberOpen) return null;
    const closeGrabber = () => { setGrabberOpen(false); setGrabScheduleAt(""); };
    // P2: subtitles come from ALL inspected variants, deduplicated by
    // URL — the old code only showed variants[0]'s tracks.
    const allSubtitles = Array.from(new Map(variants.flatMap((variant) => variant.subtitles ?? []).map((subtitle) => [subtitle.url, subtitle])).values());
    return <div className="dialog-overlay" role="dialog" aria-label="Video grabber" onClick={closeGrabber}>
      <div className="dialog dialog-wide" onClick={(event) => event.stopPropagation()}>
        <div className="dialog-head"><div><h2>Video grabber</h2><p>Inspect one media URL, or grab every media file from one page.</p></div><button className="remove-button" title="Close" aria-label="Close" onClick={closeGrabber}>×</button></div>
        <div className="video-panel">
          <div className="panel-heading"><div><h2>Inspect a media URL</h2><p>Quality choices appear only when the source exposes them.</p></div></div>
          <form onSubmit={(event) => { event.preventDefault(); void inspect(); }}>
            <label htmlFor="video-url">Page or media URL</label>
            <input id="video-url" className="field" value={videoUrl} onChange={(event) => setVideoUrl(event.target.value)} placeholder="https://cdn.example.com/master.m3u8" />
            <div className="video-submit"><button className="primary-button" type="submit" disabled={inspecting}>{inspecting ? "Inspecting…" : "Inspect URL"}</button></div>
          </form>
          {videoNotice && <div className="notice">{videoNotice}</div>}
          {variants.length > 0 && <div className="variants"><div className="variants-heading"><h3>Available streams</h3><span>{variants.length} OPTIONS</span></div>
            {variants.map((variant, index) => <div className="variant" key={variant.url + index}>
              <div><strong>{variant.isLive ? "Live · " : ""}{variant.quality}</strong><small>{variant.kind.toUpperCase()}{variant.width && variant.height ? " · " + variant.width + " × " + variant.height : ""}{variant.bandwidth ? " · " + Math.round(variant.bandwidth / 1000) + " kbps" : ""}{variant.fps ? " · " + Math.round(variant.fps) + " fps" : ""}</small>{variant.note && <p>{variant.note}</p>}</div>
              <button className="secondary-button" disabled={!variant.downloadable || grabberVariantBusy === variant.url + index} onClick={() => void downloadVariant(variant, variant.url + index)}>{grabberVariantBusy === variant.url + index ? "Queueing…" : variant.downloadable ? "Download" : "Unavailable"}</button>
            </div>)}
            {allSubtitles.length > 0 && <div className="variants"><div className="variants-heading"><h3>Subtitles</h3><span>{allSubtitles.length} TRACKS</span></div>
              {allSubtitles.map((subtitle) => <div className="variant" key={subtitle.url}>
                <div><strong>{subtitle.name || subtitle.language}</strong><small>{subtitle.language} · {subtitle.format.toUpperCase()}</small></div>
                <button className="secondary-button" onClick={() => void downloadSubtitle(subtitle)}>Download</button>
              </div>)}
            </div>}
          </div>}
          <div className="rule" />
          <div className="variants-heading"><h3>Grab a page</h3><span>SAME-PAGE ONLY · MAX 100</span></div>
          <p className="footnote">Collect every media file linked from one page — video, audio, playlists, documents and archives. This reads the page's static HTML only, so pages that need a login or load media with JavaScript may show nothing; the browser extension scans the live page instead.</p>
          <form onSubmit={(event) => { event.preventDefault(); void grabPage(); }}>
            <label htmlFor="grab-url">Page URL</label>
            <input id="grab-url" className="field" value={grabUrl} onChange={(event) => setGrabUrl(event.target.value)} placeholder="https://example.com/gallery" />
            <div className="video-submit"><button className="primary-button" type="submit" disabled={grabbing}>{grabbing ? "Grabbing…" : "Grab page"}</button></div>
          </form>
          {grabNotice && <div className="notice">{grabNotice}</div>}
          {grabbed.length > 0 && <div className="variants">
            <div className="variants-heading"><h3>Found on this page</h3><span>{grabbed.length} ITEMS</span></div>
            <div className="grab-actions">
              <button className="quiet-button" onClick={() => setGrabChecked(Object.fromEntries(grabbed.map((item) => [item.url, true])))}>Select all</button>
              <button className="quiet-button" onClick={() => setGrabChecked({})}>Select none</button>
              <button className="primary-button" onClick={() => void downloadGrabbed()} disabled={downloadingGrabbed || !grabbed.some((item) => grabChecked[item.url])}>{downloadingGrabbed ? "Queueing…" : "Download selected (" + grabbed.filter((item) => grabChecked[item.url]).length + ")"}</button>
            </div>
            <div className="schedule-row"><label htmlFor="grab-schedule-at">Start later <small>OPTIONAL</small></label><input id="grab-schedule-at" type="datetime-local" className="field schedule-field" min={scheduleMin} value={grabScheduleAt} onChange={(event) => setGrabScheduleAt(event.target.value)} /></div>
            {grabTruncated && <div className="notice">Showing the first 100 items — download in batches, or narrow the page.</div>}
            {grabbed.map((item) => <div className="variant" key={item.url}>
              <label className="grab-item"><input type="checkbox" checked={!!grabChecked[item.url]} onChange={(event) => setGrabChecked((current) => ({ ...current, [item.url]: event.target.checked }))} /><span><strong>{item.name}</strong><small>{item.kind.toUpperCase()} · {hostOf(item.url)}</small></span></label>
            </div>)}
          </div>}
          <div className="support-note"><strong>Format support</strong><p>Direct HTTP/HTTPS files use resumable transfers. HLS supports completed VOD, unencrypted or AES-128 encrypted, with Pause/Resume/Cancel. Live HLS streams can be recorded as they arrive — the partial file is kept when you stop. DASH supports static MPDs with SegmentTemplate/SegmentTimeline, SegmentList, SegmentBase and multi-period, with Pause/Resume/Cancel; separate audio/video requires FFmpeg. Live/dynamic MPDs and DRM are not supported. YouTube and social video pages are inspected with yt-dlp — paste a video page URL above and pick a format; yt-dlp and Deno download on first use from Settings. "Grab a page" below lists media files a page links to directly.</p></div>
        </div>
      </div>
    </div>;
  }

  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-mark">↯</div><div><strong>ThunderLoad</strong><small>DOWNLOAD MANAGER</small></div></div>
      <div className="nav-caption">WORKSPACE</div>
      <nav>
        <button title="Downloads" aria-label="Downloads" className={"nav-item " + (page === "downloads" ? "selected" : "")} onClick={() => goTo("downloads")}><span>↓</span>Downloads</button>
        <button title="Queues" aria-label="Queues" className={"nav-item " + (page === "queues" ? "selected" : "")} onClick={() => goTo("queues")}><span>≡</span>Queues{queued > 0 && <em>{queued}</em>}</button>
        <button title="Scheduler" aria-label="Scheduler" className={"nav-item " + (page === "scheduler" ? "selected" : "")} onClick={() => goTo("scheduler")}><span>◷</span>Scheduler{scheduledItems.length > 0 && <em>{scheduledItems.length}</em>}</button>
      </nav>
      <div className="sidebar-bottom">
        <div className="engine-line"><i />Local engine</div>
        <button title="Settings" aria-label="Settings" className={"nav-item " + (page === "settings" ? "selected" : "")} onClick={() => goTo("settings")}><span>⚙</span>Settings</button>
        {/* Version shown here is display-only; the release version lives in thunderload/Cargo.toml [workspace.package]. */}
        <div className="version">ThunderLoad · 0.3.7</div>
      </div>
    </aside>

    <main className="main">
      <header className="topbar"><div>THUNDERLOAD <span>/</span> {title.toUpperCase()}</div><button className="quiet-button folder-button" onClick={openFolder}>Open downloads folder ↗</button></header>
      <div className="content-row">
      <div className="page">
        {nativeBanner && dismissedBannerKind !== nativeBanner && <div className="setup-banner" role="status">
          <div><strong>{nativeBanner === "missing-id" ? "Browser connection not set up yet" : "Browser connection could not be restored"}</strong><p>{nativeBanner === "missing-id"
            ? "Downloads from the browser extension need a one-time connection. It takes a minute: install the extension, copy its ID, and connect it in Settings."
            : "ThunderLoad tried to reconnect a browser whose extension ID was saved before, but registration failed. Open the browser setup to try again."}</p></div>
          <div className="setup-banner-actions">
            <button className="secondary-button" onClick={() => goTo("settings")}>Set up browser connection →</button>
            <button className="quiet-button" onClick={() => { if (nativeBanner) setDismissedBannerKind(nativeBanner); }}>Dismiss</button>
          </div>
        </div>}
        <div className="page-heading"><div><h1>{title}</h1><p>{subtitle}</p></div>{page !== "settings" && <button className="primary-button" onClick={() => setAddOpen(true)}>＋ Add</button>}</div>
        {/* P1-25: the global notice lives here, above the view switch,
          so it shows on every page — it used to render only on
          Downloads (and inside the Add dialog), leaving Queues,
          Scheduler and Settings silent. */}
        {notice !== "Ready." && <div className="notice">{notice}</div>}

        {page === "downloads" && <>
          <div className="list-toolbar">
            <div className="tabs" role="tablist" aria-label="Filter by status">
              {(["all", "active", "completed", "failed"] as StatusTab[]).map((tab) => <button key={tab} role="tab" aria-selected={statusTab === tab} className={"tab" + (statusTab === tab ? " selected" : "")} onClick={() => setStatusTab(tab)}>{tab === "all" ? "All" : tab === "active" ? "Active" : tab === "completed" ? "Completed" : "Failed"} <em>{tabCounts[tab]}</em></button>)}
            </div>
            <input className="field search-field" type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search downloads…" aria-label="Search downloads" />
            <button className={"quiet-button" + (showKind ? " toggle-on" : "")} aria-pressed={showKind} title="Show or hide the Transfer kind column" onClick={() => setShowKind((current) => !current)}>Transfer kind column</button>
            {(done > 0 || items.some((item) => item.status === "Cancelled")) && <button className="quiet-button" onClick={clearFinished}>Clear finished</button>}
          </div>
          <div className="chips" role="group" aria-label="Filter by file type">
            {(["all", "video", "music", "documents", "compressed", "programs", "other"] as KindFilter[]).map((type) => <button key={type} className={"chip" + (kindFilter === type ? " selected" : "")} aria-pressed={kindFilter === type} onClick={() => setKindFilter(type)}>{type === "all" ? "All" : type === "video" ? "Video" : type === "music" ? "Music" : type === "documents" ? "Documents" : type === "compressed" ? "Compressed" : type === "programs" ? "Programs" : "Other"} <em>{kindCounts[type]}</em></button>)}
          </div>
          {downloadTable(kindFiltered,
            statusTab === "all" ? "Download list is empty." : statusTab === "active" ? "No active transfers." : statusTab === "completed" ? "No completed downloads yet." : "No failed downloads.",
            statusTab === "all" ? "Add new download URL." : statusTab === "active" ? "Active, queued and paused transfers appear here." : statusTab === "completed" ? "Finished and cancelled downloads appear here." : "Failed and interrupted transfers appear here with a Retry action.",
            showKind)}
        </>}

        {page === "queues" && <>
          <section className="summary">
            <div><strong>{active}</strong><span>Active</span></div><div><strong>{queued}</strong><span>Waiting</span></div><div><strong>{done}</strong><span>Completed</span></div><div><strong>{attention}</strong><span>Failed</span></div>
          </section>
          <div className="list-toolbar">
            <p className="queue-note">Jobs start automatically, up to your active transfer limit.</p>
            <button className="secondary-button" onClick={() => setGrabberOpen(true)}>Video grabber ▷</button>
          </div>
          {downloadTable(queueItems, "Queue is empty.", "Add a download to begin. Waiting, running and failed transfers appear here.")}
          <p className="footnote">Direct HTTP, HLS and DASH downloads keep partial files on disk. Pause saves a checkpoint, Resume continues from it, and startup automatically recovers saved checkpoints. Jobs are also recorded in a durable job database, so the queue — including queued and scheduled entries — survives an app restart.</p>
        </>}

        {page === "scheduler" && <>
          {scheduledItems.length ? <div className="sched-list">{scheduledItems.map((item) => <div className="sched-row" key={item.id}>
            <div><strong title={item.name}>{item.name}</strong><small title={item.url}>{hostOf(item.url) || item.kind} · </small><Status value={item.status} /></div>
            <div className="sched-time">{item.scheduledAt ? new Date(item.scheduledAt).toLocaleString() : "—"}</div>
            {canCancel(item) && <button className="quiet-button transfer-control" onClick={() => void cancel(item)}>Cancel</button>}
          </div>)}</div> : <div className="empty-state"><span className="empty-icon">◷</span><strong>No scheduled downloads</strong><p>Add a download and choose a start time.</p><button className="secondary-button" onClick={() => setAddOpen(true)}>＋ Add download</button></div>}
          <p className="footnote">Scheduled downloads start automatically at their time — a wake task is registered when you schedule one. Cancelling a scheduled download also removes its wake task.</p>
        </>}

        {page === "settings" && <section className="panel settings-panel">
          <div className="settings-section"><div><h2>Transfer behaviour</h2><p>Saved locally. Applies to new downloads.</p></div>
            <label className="setting-row"><span><strong>Connections per file</strong><small>Parallel connections per file, for direct, HLS and DASH downloads. Applies to new downloads; a resumed direct download keeps the connections it started with; HLS/DASH resumes use the current setting.</small></span><select className="select-field" value={connections} onChange={(event) => setConnections(Number(event.target.value))}>{[1, 2, 4, 8, 16].map((n) => <option key={n} value={n}>{n} {n === 1 ? "connection" : "connections"}</option>)}</select></label>
            <label className="setting-row"><span><strong>Simultaneous downloads</strong><small>Maximum number of active queue items.</small></span><select className="select-field" value={parallel} onChange={(event) => setParallel(Number(event.target.value))}>{[1, 2, 3, 4].map((n) => <option key={n} value={n}>{n} {n === 1 ? "download" : "downloads"}</option>)}</select></label>
            <label className="setting-row"><span><strong>Speed limit</strong><small>Applies to active direct, HLS and DASH transfers in each ThunderLoad process. Browser companion and desktop share this saved setting; each process enforces its own aggregate cap. Zero means unlimited.</small></span><select className="select-field" value={speedLimit} onChange={(event) => setSpeedLimit(Number(event.target.value))}>
              <option value={0}>Unlimited</option><option value={262144}>256 KB/s</option><option value={524288}>512 KB/s</option><option value={1048576}>1 MB/s</option><option value={2097152}>2 MB/s</option><option value={5242880}>5 MB/s</option><option value={10485760}>10 MB/s</option>
            </select></label>
          </div>
          <div className="rule" />
          <div className="settings-section"><div><h2>Clipboard watcher</h2><p>Watch the clipboard for media links while this app is focused.</p></div>
            <label className="setting-row"><span><strong>Watch clipboard for links</strong><small>Off by default. When on, a copied media link opens the LinkGrabber dialog — nothing downloads on its own. Only while this app is focused.</small></span><input type="checkbox" checked={clipboardWatch} onChange={(event) => setClipboardWatch(event.target.checked)} /></label>
          </div>
          <div className="rule" />
          <div className="settings-section"><div><h2>Storage</h2><p>ThunderLoad uses a dedicated folder in your system Downloads directory.</p></div><div className="storage-row"><span>Downloads / ThunderLoad</span><button className="secondary-button" onClick={openFolder}>Open folder ↗</button></div></div>
          <div className="rule" />
          <div className="settings-section">
            <div><h2>DASH video support</h2><p>Static DASH VOD with separate audio/video tracks needs FFmpeg to combine the selected quality into one MP4 file.</p></div>
            <div className="ffmpeg-row">
              <label htmlFor="ffmpeg-path">FFmpeg executable path</label>
              <input id="ffmpeg-path" className="field" value={ffmpegPath} onChange={(event) => setFfmpegPath(event.target.value)} placeholder="ffmpeg" />
              <button className="secondary-button" onClick={saveFfmpegPath} disabled={ffmpegSaving}>{ffmpegSaving ? "Checking…" : "Verify & save"}</button>
            </div>
            {ffmpegStatus && <div className="notice native-host-notice">{ffmpegStatus}</div>}
            <p className="footnote">ThunderLoad verifies the executable by running <code>ffmpeg -version</code>, then saves its path in your local app configuration. An empty value clears the custom path and falls back to FFmpeg on PATH.</p>
          </div>
          <div className="rule" />
          <div className="settings-section">
            <div><h2>Browser connection</h2><p>Register the optional local engine for adaptive video downloads. Direct file downloads work without this connection. Connect as many browsers as you like — each one stays connected and can send downloads at the same time.</p></div>
            {connectedBrowsers.map((item) => (
              <div className="setting-row" key={item.browser}>
                <div><strong>{nativeBrowserName(item.browser)}</strong><small>Extension ID {item.extensionId}</small></div>
                <button className="quiet-button" disabled={removingNativeBrowser !== null} onClick={() => removeNativeBrowser(item.browser)}>{removingNativeBrowser === item.browser ? "Removing…" : "Remove"}</button>
              </div>
            ))}
            {connectedBrowsers.length < NATIVE_BROWSERS.length && !showAddBrowser && (
              <div className="setting-row">
                <div><strong>Add another browser</strong><small>Chrome, Edge, Brave, Opera, Vivaldi or Firefox — each one works at the same time.</small></div>
                <button className="secondary-button" disabled={removingNativeBrowser !== null} onClick={openAddBrowser}>Add browser</button>
              </div>
            )}
            {showAddBrowser && (
              <div className="native-host-grid">
                <label><span>Browser</span><select className="select-field" value={nativeBrowser} onChange={(event) => {
                  const next = event.target.value;
                  setNativeBrowser(next);
                  setNativeExtensionId(readNativeIds()[next] || (next === "firefox" ? "thunderload@example.com" : ""));
                }}>
                  {NATIVE_BROWSERS.filter((item) => !connectedBrowsers.some((entry) => entry.browser === item.id)).map((item) => (
                    <option key={item.id} value={item.id}>{item.name}</option>
                  ))}
                </select></label>
                <label><span>Extension ID</span><input className="field" value={nativeExtensionId} onChange={(event) => setNativeExtensionId(event.target.value)} placeholder={nativeBrowser === "firefox" ? "thunderload@example.com" : "32 characters a–p"} /></label>
                <button className="secondary-button" onClick={registerNativeHost} disabled={nativeSettingUp || removingNativeBrowser !== null}>{nativeSettingUp ? "Registering…" : "Connect browser"}</button>
              </div>
            )}
            {nativeSetupStatus && <div className="notice native-host-notice">{nativeSetupStatus}</div>}
            <ol className="setup-steps">
              <li><strong>Install the extension first</strong> — download it below in Browser extension files and load it in your browser (Chromium: Load unpacked in developer mode; Firefox: Install Add-on From File).</li>
              <li><strong>Find the extension ID</strong> — open <code>chrome://extensions</code> (Edge: <code>edge://extensions</code>, Brave: <code>brave://extensions</code>, Opera: <code>opera://extensions</code>, Vivaldi: <code>vivaldi://extensions</code>), turn <strong>Developer mode</strong> on, and copy the <strong>ID</strong> shown under the ThunderLoad extension. Firefox uses the fixed ID <code>thunderload@example.com</code> — it is filled in for you.</li>
              <li><strong>Connect it here</strong> — paste the ID above, choose your browser, and click <strong>Connect browser</strong>. ThunderLoad copies the native host into place, writes the browser manifest, and registers it. You only do this once per browser.</li>
              <li><strong>Restart the browser</strong> and allow <strong>Native messaging</strong> in the ThunderLoad extension when prompted. ThunderLoad re-registers automatically on later startups if the registration ever goes missing.</li>
            </ol>
          </div>
          <div className="rule" />
          <div className="settings-section">
            <div><h2>Browser extension files</h2><p>Download the extension package for your browser and load it in developer mode. No store listing is needed for this path.</p></div>
            <div className="setting-row">
              <div><strong>Chrome · Edge · Brave · Opera · Vivaldi — ThunderLoad-Chromium.zip</strong><small>Extract the ZIP, then open chrome://extensions (Edge: edge://extensions, Brave: brave://extensions, Opera: opera://extensions, Vivaldi: vivaldi://extensions), turn Developer mode on, click “Load unpacked” and select the extracted folder.</small></div>
              <button className="secondary-button" onClick={() => downloadExtensionFile("chromium")} disabled={extensionFileSaving}>Download ZIP</button>
            </div>
            <div className="setting-row">
              <div><strong>Firefox — ThunderLoad-Firefox.xpi</strong><small>Open about:addons, click the gear icon, choose “Install Add-on From File” and select the .xpi file.</small></div>
              <button className="secondary-button" onClick={() => downloadExtensionFile("firefox")} disabled={extensionFileSaving}>Download XPI</button>
            </div>
            <p className="footnote">Firefox release only accepts Mozilla-signed add-ons, so until the AMO-signed build exists this file loads on Firefox Developer Edition or Nightly (with xpinstall.signatures.required switched off). It is not promised to work on release Firefox today.</p>
            {extensionFileStatus && <div className="notice native-host-notice">{extensionFileStatus}</div>}
          </div>
          <div className="rule" />
          <div className="settings-section">
            <div><h2>YouTube &amp; social downloads</h2><p>YouTube and social video pages are downloaded with yt-dlp (Deno runs its JavaScript for full quality). Neither tool is bundled with the installer — download each once, here, and ThunderLoad verifies it before use.</p></div>
            <div className="setting-row">
              <div><strong>yt-dlp {ytdlpTools ? (ytdlpTools.ytdlp.present ? "· Installed" + (ytdlpTools.ytdlp.version ? " · " + ytdlpTools.ytdlp.version : "") : "· Not installed") : "· Checking…"}</strong><small>Extracts video formats from YouTube, Instagram, TikTok, X, Facebook, Reddit, Twitch and Vimeo.</small></div>
              <button className="secondary-button" onClick={() => void downloadYtDlpTool("ytdlp", "yt-dlp")} disabled={ytdlpDownloading !== null}>Download yt-dlp</button>
            </div>
            {ytdlpToolProgress?.tool === "yt-dlp" && <div className="notice native-host-notice">yt-dlp download ho raha hai{ytdlpToolProgress.percent !== undefined ? " — " + Math.round(ytdlpToolProgress.percent) + "%" : "…"}{toolSpeedEta()}</div>}
            <div className="setting-row">
              <div><strong>Deno {ytdlpTools ? (ytdlpTools.deno.present ? "· Installed" + (ytdlpTools.deno.version ? " · " + ytdlpTools.deno.version : "") : "· Not installed") : "· Checking…"}</strong><small>JavaScript runtime yt-dlp needs for YouTube's full quality. Without it, YouTube falls back to fewer formats.</small></div>
              <button className="secondary-button" onClick={() => void downloadYtDlpTool("deno", "Deno")} disabled={ytdlpDownloading !== null}>Download Deno</button>
            </div>
            {ytdlpToolProgress?.tool === "Deno" && <div className="notice native-host-notice">Deno download ho raha hai{ytdlpToolProgress.percent !== undefined ? " — " + Math.round(ytdlpToolProgress.percent) + "%" : "…"}{toolSpeedEta()}</div>}
            {ytdlpUpdateStatus && <div className="notice native-host-notice">{ytdlpUpdateStatus}</div>}
            <div className="setting-row">
              <div><strong>Update yt-dlp</strong><small>YouTube changes often — an extractor error usually means yt-dlp needs an update. This downloads and installs the latest release.</small></div>
              <button className="secondary-button" onClick={() => void checkYtDlpUpdate()} disabled={ytdlpCheckingUpdate}>{ytdlpCheckingUpdate ? "Updating…" : "Update yt-dlp"}</button>
            </div>
            <div className="setting-row">
              <div><strong>Download it yourself instead</strong><small>Or download the file yourself from the official release page and drop it in the tools folder — ThunderLoad picks it up automatically.</small></div>
              <div style={{ display: "flex", gap: "8px" }}>
                <button className="secondary-button" onClick={() => void invoke("open_ytdlp_release_page", { tool: "ytdlp" }).catch((error) => setYtdlpUpdateStatus("Could not open the yt-dlp release page: " + String(error)))}>yt-dlp release page</button>
                <button className="secondary-button" onClick={() => void invoke("open_ytdlp_release_page", { tool: "deno" }).catch((error) => setYtdlpUpdateStatus("Could not open the Deno release page: " + String(error)))}>Deno release page</button>
                <button className="secondary-button" onClick={() => void invoke("open_tools_folder").catch((error) => setYtdlpUpdateStatus("Could not open the tools folder: " + String(error)))}>Open tools folder</button>
              </div>
            </div>
            <p className="footnote">Tools folder: {ytdlpTools ? ytdlpTools.toolsDir : "…"}</p>
          </div>
          <div className="rule" />
          <div className="settings-section"><div><h2>About</h2><p>Local desktop transfer engine · Version 0.3.7</p>{/* Display-only; the release version lives in thunderload/Cargo.toml [workspace.package]. */}</div><div className="about-line"><span>Supported media</span><strong>Direct files · HLS VOD · Live HLS recording · Static DASH VOD · YouTube &amp; social (yt-dlp)</strong></div><div className="about-line"><span>Direct downloads</span><strong>Pause · Resume · Cancel · Speed limit · Schedule</strong></div><div className="about-line"><span>Still in development</span><strong>Live DASH</strong></div></div>
        </section>}

      </div>
      {inspectorPanel()}
      </div>
      <footer className="statusbar"><span className="statusbar-speed">↓ {aggregateLabel}</span><span>{active} active · {queued} queued · {done} completed · {attention} failed</span><span className="statusbar-right"><i className="engine-dot" />Local engine · Downloads stay on this device.</span></footer>
    </main>
    {addDialog()}
    {grabberDialog()}
    {linkGrabberDialog()}
    {contextMenuPanel()}
  </div>;
}

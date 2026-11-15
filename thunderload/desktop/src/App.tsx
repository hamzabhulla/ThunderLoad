import { useEffect, useMemo, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { save } from "@tauri-apps/plugin-dialog";

type Page = "downloads" | "queues" | "scheduler" | "settings";
type StatusTab = "all" | "active" | "completed" | "failed";
type InspectorTab = "overview" | "files" | "segments" | "activity";
type Kind = "direct" | "hls" | "dash";
type Status = "Queued" | "Downloading" | "Pausing" | "Paused" | "Cancelling" | "Completed" | "Failed" | "Interrupted" | "Cancelled";
type Variant = { kind: string; quality: string; width?: number; height?: number; bandwidth?: number; url: string; downloadable: boolean; note?: string; representationId?: string; audioRepresentationId?: string; audioOnly?: boolean };
type GrabItem = { url: string; kind: string; name: string };
type GrabResult = { items: GrabItem[]; truncated: boolean };
type Progress = { downloaded: number; total?: number; percent?: number; phase: string };
type Item = { id: string; name: string; url: string; kind: Kind; status: Status; percent: number; downloaded: number; total?: number; detail: string; error?: string; createdAt: number; targetPath?: string; scheduledAt?: number; source?: "native"; representationId?: string; audioRepresentationId?: string; segmentsDone?: number; segmentsTotal?: number };
type StoredJob = { id: string; name: string; url: string; kind: string; origin: string; state: string; downloaded: number; total?: number; percent?: number; segmentsDone?: number; segmentsTotal?: number; targetPath?: string; checkpointPath?: string; representationId?: string; audioRepresentationId?: string; scheduledAtMs?: number; detail: string; error?: string; createdAtMs: number; updatedAtMs: number };

const HISTORY_KEY = "thunderload.history.v2";
const CONNECTIONS_KEY = "thunderload.connections.v1";
const PARALLEL_KEY = "thunderload.parallel.v1";
const SPEED_LIMIT_KEY = "thunderload.speed-limit.v1";
const NATIVE_IDS_KEY = "thunderload.native-extension-ids.v1";

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

// Refusal shown when a pasted URL inspects as a webpage instead of a
// direct media file — the same wording as the Format support note on the
// Video page.
const YOUTUBE_PAGE_MESSAGE = "A YouTube watch page is not a direct media URL; page extraction of its streams is not supported";

function bytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return (value / Math.pow(1024, unit)).toFixed(unit ? 1 : 0) + " " + units[unit];
}
function readItems(): Item[] {
  try {
    const saved = JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
    if (!Array.isArray(saved)) return [];
    return saved.map((item: Item) => forgetStaleSchedule(item.source === "native"
      ? item
      : item.status === "Downloading" || item.status === "Pausing"
        ? item.targetPath
          ? { ...item, status: "Queued", detail: "Recovering saved partial download…" }
          : { ...item, status: "Interrupted", detail: "App closed before a resumable checkpoint was created" }
        : item));
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
  const kind: Kind = row.kind === "hls" ? "hls" : row.kind === "dash" ? "dash" : "direct";
  const status: Status = (["Queued", "Downloading", "Paused", "Completed", "Failed", "Interrupted", "Cancelled"].includes(row.state) ? row.state : "Failed") as Status;
  const percent = row.percent !== undefined
    ? Math.max(0, Math.min(100, row.percent))
    : row.total ? Math.max(0, Math.min(100, Math.round(row.downloaded / row.total * 100))) : 0;
  return {
    id: row.id, name: row.name || nameFromUrl(row.url), url: row.url, kind, status, percent,
    downloaded: row.downloaded, total: row.total, detail: row.detail || "",
    error: row.error, createdAt: row.createdAtMs, targetPath: row.targetPath,
    scheduledAt: row.scheduledAtMs, representationId: row.representationId,
    audioRepresentationId: row.audioRepresentationId,
    segmentsDone: row.segmentsDone, segmentsTotal: row.segmentsTotal
  };
}
function itemToSyncJob(item: Item) {
  return {
    id: item.id, name: item.name, url: item.url, kind: item.kind, status: item.status,
    downloaded: item.downloaded, total: item.total ?? null, percent: item.percent,
    targetPath: item.targetPath ?? null, createdAt: item.createdAt,
    scheduledAt: item.scheduledAt ?? null, representationId: item.representationId ?? null,
    audioRepresentationId: item.audioRepresentationId ?? null, detail: item.detail,
    error: item.error ?? null
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
const CANCELLABLE_STATUSES: Status[] = ["Downloading", "Pausing", "Paused", "Failed", "Interrupted", "Queued"];
function canCancel(item: Item): boolean {
  return CANCELLABLE_STATUSES.includes(item.status);
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
  const [items, setItems] = useState<Item[]>(readItems);
  const [variants, setVariants] = useState<Variant[]>([]);
  const [inspecting, setInspecting] = useState(false);
  const [notice, setNotice] = useState("Ready.");
  const [videoNotice, setVideoNotice] = useState("");
  // Site grabber (Slice 7): paste-a-page-URL path. The desktop reads
  // the page's static HTML only, so `grabbed` may miss media that a
  // login or JavaScript would reveal; the extension sees the live page.
  const [grabUrl, setGrabUrl] = useState("");
  const [grabbed, setGrabbed] = useState<GrabItem[]>([]);
  const [grabChecked, setGrabChecked] = useState<Record<string, boolean>>({});
  const [grabTruncated, setGrabTruncated] = useState(false);
  const [grabbing, setGrabbing] = useState(false);
  const [grabNotice, setGrabNotice] = useState("");
  const [connections, setConnections] = useState(() => readSetting(CONNECTIONS_KEY, 1, [1]));
  const [parallel, setParallel] = useState(() => readSetting(PARALLEL_KEY, 2, [1, 2, 3, 4]));
  const [speedLimit, setSpeedLimit] = useState(() => readSetting(SPEED_LIMIT_KEY, 0, [0, 262144, 524288, 1048576, 2097152, 5242880, 10485760]));
  const [clock, setClock] = useState(Date.now());
  const [nativeBrowser, setNativeBrowser] = useState("chrome");
  const [nativeExtensionId, setNativeExtensionId] = useState(() => readNativeIds()["chrome"] || "");
  const [nativeSetupStatus, setNativeSetupStatus] = useState("");
  const [nativeSettingUp, setNativeSettingUp] = useState(false);
  // Non-blocking startup prompt: "missing-id" when no extension ID was
  // ever saved, "register-failed" when a saved ID could not be
  // re-registered. Dismissal lasts for this app session only.
  const [nativeBanner, setNativeBanner] = useState<"missing-id" | "register-failed" | null>(null);
  const [nativeBannerDismissed, setNativeBannerDismissed] = useState(false);
  const [extensionFileStatus, setExtensionFileStatus] = useState("");
  const [extensionFileSaving, setExtensionFileSaving] = useState(false);
  const [ffmpegPath, setFfmpegPath] = useState("ffmpeg");
  const [ffmpegStatus, setFfmpegStatus] = useState("");
  const [ffmpegSaving, setFfmpegSaving] = useState(false);
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
  const speedSamples = useRef(new Map<string, { downloaded: number; at: number; rate: number | null }>());

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
  // Closed status vocabulary for the tabs: terminal-but-cancelled jobs
  // sit with Completed, Failed/Interrupted sit with Failed, everything
  // else is Active — so no job is hidden from every tab.
  function tabOf(item: Item): "active" | "completed" | "failed" {
    if (item.status === "Completed" || item.status === "Cancelled") return "completed";
    if (item.status === "Failed" || item.status === "Interrupted") return "failed";
    return "active";
  }

  const active = items.filter((item) => item.status === "Downloading" || item.status === "Pausing" || item.status === "Cancelling").length;
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

  useEffect(() => { localStorage.setItem(HISTORY_KEY, JSON.stringify(items)); }, [items]);
  useEffect(() => { localStorage.setItem(CONNECTIONS_KEY, String(connections)); }, [connections]);
  useEffect(() => { localStorage.setItem(PARALLEL_KEY, String(parallel)); }, [parallel]);
  useEffect(() => {
    localStorage.setItem(SPEED_LIMIT_KEY, String(speedLimit));
    void invoke("set_speed_limit", { bytesPerSecond: speedLimit }).catch((error) => setNotice("Could not apply speed limit: " + String(error)));
  }, [speedLimit]);
  useEffect(() => {
    const timer = window.setInterval(() => setClock(Date.now()), 1000);
    return () => window.clearInterval(timer);
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
        else if (grabberOpen) setGrabberOpen(false);
        else if (addOpen) setAddOpen(false);
        else setSelectedId(null);
      } else if (!addOpen && !grabberOpen && !contextMenu && selectedId && !isTypingTarget(event.target) && !event.ctrlKey && !event.metaKey && !event.altKey) {
        const item = items.find((entry) => entry.id === selectedId);
        if (!item) return;
        if (event.key === "Delete" || event.key === "Backspace") {
          event.preventDefault();
          // Same gate as the row × button and context-menu Remove: a
          // live transfer must not be killed (and its partial file
          // deleted) from the keyboard.
          if (canRemove(item)) void remove(selectedId);
        } else if (event.key.toLowerCase() === "p") {
          if (item.status === "Downloading") void pause(item);
          else if (item.status === "Paused") void resume(item);
        }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [addOpen, grabberOpen, selectedId, items, contextMenu]);

  // Slice 5: seed the queue from the durable job database on startup,
  // then send the merged desktop list back so jobs the store does not
  // know yet (pre-Slice-5 history, queued jobs that never started)
  // enter the database. Stored rows win by id; local history fills the
  // rest. Browser-started rows keep coming through the native poll.
  const seededRef = useRef(false);
  useEffect(() => {
    if (seededRef.current) return;
    seededRef.current = true;
    void invoke<Array<StoredJob>>("get_jobs").then((rows) => {
      const stored = rows.filter((row) => row.origin !== "native").map(storedJobToItem);
      const byId = new Map(itemsRef.current.map((item) => [item.id, item]));
      for (const item of stored) byId.set(item.id, item);
      // Drop stale schedules from stored rows too (a started/completed
      // job whose row still carries its old start time), or the next
      // startup would put it back on the Scheduler page.
      const merged = Array.from(byId.values()).sort((a, b) => b.createdAt - a.createdAt).map(forgetStaleSchedule);
      setItems(merged);
      const desktop = merged.filter((item) => item.source !== "native");
      if (desktop.length) void invoke("sync_jobs", { jobs: desktop.map(itemToSyncJob) }).catch(() => {});
    }).catch(() => {});
  }, []);

  // Fix 4: on startup, check the native-host registration for every
  // browser whose extension ID was saved once before, and re-register
  // silently when it went missing (browser reset, registry cleaner,
  // moved install). When no ID was ever saved, show a prominent
  // non-blocking banner instead — never a modal, never a silent no-op.
  const nativeCheckedRef = useRef(false);
  useEffect(() => {
    if (nativeCheckedRef.current) return;
    nativeCheckedRef.current = true;
    const entries = Object.entries(readNativeIds());
    if (!entries.length) { setNativeBanner("missing-id"); return; }
    void (async () => {
      let failed = false;
      for (const [browser, extensionId] of entries) {
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
        }>>("get_native_downloads");
        if (disposed) return;
        const external: Item[] = jobs.map((job) => ({
          id: "native-" + job.id,
          name: job.name || "Browser download",
          url: job.url || "",
          kind: job.kind === "hls" ? "hls" : job.kind === "dash" ? "dash" : "direct",
          status: (["Downloading", "Paused", "Completed", "Failed", "Interrupted", "Cancelled"].includes(job.status || "") ? job.status : "Failed") as Status,
          percent: Math.max(0, Math.min(100, job.percent || 0)),
          downloaded: job.downloaded || 0,
          total: job.total ?? undefined,
          detail: job.detail || "Started from browser extension",
          error: job.error,
          createdAt: job.createdAt || Date.now(),
          targetPath: job.targetPath,
          representationId: job.representationId,
          audioRepresentationId: job.audioRepresentationId,
          source: "native"
        }));
        setItems((current) => {
          const merged = new Map(current.filter((item) => item.source !== "native").map((item) => [item.id, item]));
          for (const item of external) merged.set(item.id, item);
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

  useEffect(() => {
    let alive = true;
    let cleanup: Array<() => void> = [];
    (async () => {
      const listeners = await Promise.all([
        listen<{ jobId: string; progress: Progress }>("download-progress", ({ payload }) => {
          // Record a speed sample for the Motrix rule: the rate comes
          // from the bytes moved since the previous event, never from
          // a single absolute value.
          const now = Date.now();
          const prev = speedSamples.current.get(payload.jobId);
          if (prev) {
            const dt = (now - prev.at) / 1000;
            const rate = dt > 0 ? Math.max(0, (payload.progress.downloaded - prev.downloaded) / dt) : prev.rate;
            speedSamples.current.set(payload.jobId, { downloaded: payload.progress.downloaded, at: now, rate });
          } else {
            speedSamples.current.set(payload.jobId, { downloaded: payload.progress.downloaded, at: now, rate: null });
          }
          setItems((current) => current.map((item) => item.id !== payload.jobId ? item : {
            ...item,
            downloaded: payload.progress.downloaded,
            total: payload.progress.total,
            percent: Math.max(0, Math.min(100, payload.progress.percent ?? item.percent)),
            detail: item.kind === "hls"
              ? "Downloading media segments · " + Math.round(payload.progress.percent ?? 0) + "%"
              : bytes(payload.progress.downloaded) + (payload.progress.total ? " of " + bytes(payload.progress.total) : " downloaded")
          }));
        }),
        listen<{ jobId: string; path: string }>("download-complete", ({ payload }) => {
          // Terminal state: the rate sample is done, and a schedule
          // that fired long ago must not linger on the Scheduler page.
          speedSamples.current.delete(payload.jobId);
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Completed", percent: 100, detail: "Saved · " + payload.path, error: undefined, scheduledAt: undefined }
            : item));
          setNotice("Download completed.");
        }),
        listen<{ jobId: string; targetPath: string }>("download-paused", ({ payload }) => {
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Paused", targetPath: payload.targetPath, detail: "Paused · partial file saved", error: undefined }
            : item));
        }),
        listen<{ jobId: string }>("download-cancelled", ({ payload }) => {
          speedSamples.current.delete(payload.jobId);
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Cancelled", detail: "Cancelled · partial file removed", targetPath: undefined, downloaded: 0, percent: 0, scheduledAt: undefined }
            : item));
        }),
listen<{ jobId: string; error: string; targetPath?: string }>("download-error", ({ payload }) => {
          speedSamples.current.delete(payload.jobId);
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
    const slots = Math.max(0, parallel - active);
    const next = items.filter((item) => item.source !== "native" && item.status === "Queued" && (!item.scheduledAt || item.scheduledAt <= clock)).slice(0, slots);
    if (!next.length) return;
    const ids = new Set(next.map((item) => item.id));
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
        if (item.kind === "hls") {
          // Pass the paused/failed job's target back so the backend
          // reuses its on-disk checkpoint instead of reserving a fresh
          // target and restarting from scratch. The returned path is
          // stored so Cancel works while the job is running.
          const targetPath = await invoke<string>("start_hls_download", { jobId: item.id, url: item.url, filename: item.name, targetPath: item.targetPath || null, scheduledAt: null });
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath } : entry));
        } else if (item.kind === "dash") {
          // Pass the paused/failed job's target back so the backend
          // reuses its on-disk checkpoint instead of reserving a fresh
          // target and restarting from scratch. The returned path is
          // stored so Pause/Cancel work while the job is running.
          const targetPath = await invoke<string>("start_dash_download", {
            jobId: item.id, url: item.url, representationId: item.representationId,
            audioRepresentationId: item.audioRepresentationId || null, filename: item.name,
            targetPath: item.targetPath || null, scheduledAt: null
          });
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath } : entry));
        } else {
          const targetPath = await invoke<string>("start_download", {
            jobId: item.id, url: item.url, filename: item.name, targetPath: item.targetPath || null, connections, scheduledAt: null
          });
          setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, targetPath } : entry));
        }
      } catch (error) {
        setItems((current) => current.map((entry) => entry.id === item.id
          ? { ...entry, status: "Failed", detail: "Could not start", error: String(error) }
          : entry));
      }
    });
  }, [items, active, parallel, connections, clock]);

  async function add(value: string, saveAs?: string, requestedKind?: Kind, representationId?: string, audioRepresentationId?: string) {
    const clean = value.trim();
    if (!/^https?:\/\//i.test(clean)) { setNotice("Enter a valid HTTP or HTTPS URL."); return; }
    let kind: Kind = requestedKind || "direct";
    try {
      if (!requestedKind) {
        const pathname = new URL(clean).pathname.toLowerCase();
        if (pathname.endsWith(".m3u8")) kind = "hls";
        else if (pathname.endsWith(".mpd")) kind = "dash";
      }
    } catch { setNotice("That URL is not valid."); return; }
    const scheduledAt = scheduleAt ? new Date(scheduleAt).getTime() : undefined;
    if (scheduledAt !== undefined && (!Number.isFinite(scheduledAt) || scheduledAt <= Date.now())) {
      setNotice("Choose a future start time, or clear the schedule field.");
      return;
    }
    // Paste path only (no requestedKind): ask the engine to classify the
    // URL before any job exists. Callers that pass requestedKind already
    // classified it — the Video page inspected the URL, the grabber only
    // yields media links, and a retry re-queues a job that was already
    // accepted — so they skip this probe. inspect_video rejects a webpage
    // (its error says the URL "is a webpage"); that verdict refuses the
    // add with the Format-support wording and creates no job, so a
    // YouTube watch page can no longer reach start_download as a fake
    // direct file. Any other probe failure (server refused the metadata
    // probe, network down) is not a webpage verdict: the URL queues
    // exactly as before and start_download still validates it.
    if (!requestedKind) {
      setNotice("Checking link…");
      try {
        await invoke<Variant[]>("inspect_video_url", { url: clean, referer: null });
      } catch (error) {
        if (String(error).includes("is a webpage")) {
          setNotice(YOUTUBE_PAGE_MESSAGE);
          return;
        }
      }
    }
    const suggested = nameFromUrl(clean);
    const item: Item = {
      id: crypto.randomUUID(), name: (saveAs || "").trim() || (kind === "hls" || kind === "dash" ? "video.mp4" : suggested),
      url: clean, kind, status: "Queued", percent: 0, downloaded: 0,
      representationId, audioRepresentationId,
      detail: scheduledAt ? "Scheduled for " + new Date(scheduledAt).toLocaleString() : "Waiting in queue",
      createdAt: Date.now(),
      scheduledAt
    };
    const updated = [item, ...items];
    try { localStorage.setItem(HISTORY_KEY, JSON.stringify(updated)); } catch { /* Queue effect retries persistence. */ }
    setItems(updated);
    // Slice 5: record the queued job in the durable store immediately,
    // so it survives a restart even if it never gets a download slot
    // before the app closes.
    void invoke("sync_jobs", { jobs: [itemToSyncJob(item)] }).catch(() => {});
    setNotice(scheduledAt ? "Added to queue. Registering scheduled wake…" : "Added to queue.");
    if (scheduledAt) {
      void invoke<string>("schedule_app_wake", { jobId: item.id, localDateTime: scheduleAt })
        .then((status) => setNotice(status))
        .catch((error) => setNotice("Queued locally, but a scheduled wake task could not be registered: " + String(error)));
    }
    setUrl(""); setFilename(""); setScheduleAt("");
  }
  async function pause(item: Item) {
    try {
      if (item.source === "native") {
        await invoke("control_native_download", { jobId: item.id.replace(/^native-/, ""), action: "pause" });
      } else {
        await invoke("pause_download", { jobId: item.id });
      }
      setItems((current) => current.map((entry) => entry.id === item.id
        ? { ...entry, status: "Pausing", detail: "Pausing · saving checkpoint…" }
        : entry));
    } catch (error) { setNotice(String(error)); }
  }
  async function resume(item: Item) {
    if (item.source === "native") {
      if (item.status === "Interrupted" && item.targetPath) {
        // The browser companion has exited. Hand its persistent checkpoint
        // to the desktop engine so the user can still continue the download
        // — direct, HLS and DASH checkpoints all live next to the target.
        await invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {});
        const recovered: Item = {
          ...item, id: crypto.randomUUID(), source: undefined, status: "Queued",
          detail: "Recovering browser checkpoint in desktop engine", error: undefined
        };
        setItems((current) => [recovered, ...current.filter((entry) => entry.id !== item.id)]);
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
      try {
        if (item.status === "Interrupted") {
          // No native process owns this job anymore: delete its orphaned
          // checkpoint locally (if any) and drop its record.
          if (item.targetPath) {
            await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath });
          }
          await invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {});
          setItems((current) => current.map((entry) => entry.id === item.id
            ? { ...entry, status: "Cancelled", targetPath: undefined, downloaded: 0, percent: 0, detail: "Cancelled · checkpoint removed" }
            : entry));
        } else {
          // Direct, HLS and DASH browser jobs alike: the companion owns
          // the checkpoint, so cancel goes through its control channel
          // and the companion deletes its own partial files.
          await invoke("control_native_download", { jobId: item.id.replace(/^native-/, ""), action: "cancel" });
          setItems((current) => current.map((entry) => entry.id === item.id
            ? { ...entry, status: "Cancelling", detail: "Cancelling browser download…" }
            : entry));
        }
      } catch (error) { setNotice(String(error)); }
      return;
    }
    if (!item.targetPath) {
      // Queued jobs never started and have nothing on disk: local flip only.
      setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "Cancelled", detail: "Cancelled" } : entry));
      // Slice 5: the durable row must flip too, or the job would come
      // back as Queued on the next startup.
      void invoke("cancel_queued_job", { jobId: item.id }).catch(() => {});
      return;
    }
    try {
      await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath });
      setItems((current) => current.map((entry) => entry.id === item.id
        ? { ...entry, status: "Cancelling", detail: "Cancelling and deleting partial file…" }
        : entry));
    } catch (error) { setNotice(String(error)); }
  }
  async function remove(id: string) {
    const item = items.find((entry) => entry.id === id);
    if (item?.scheduledAt) {
      await invoke("cancel_app_wake", { jobId: item.id }).catch(() => {});
    }
    // A live browser job is cancelled, not removed: the × button on an
    // active native job acts as Cancel (all kinds alike).
    if (item?.source === "native"
        && ["Downloading", "Pausing", "Paused", "Failed"].includes(item.status)) {
      await cancel(item);
      return;
    }
    if (item?.source === "native") {
      // D10: the desktop never deletes a companion-owned checkpoint with
      // its own cancel_download — only an Interrupted job (no companion
      // left) has its orphaned files removed locally. Dismissing asks a
      // live companion to cancel first and waits for it, so the companion
      // cannot resurrect the record afterwards.
      if (item.status === "Interrupted" && item.targetPath) {
        await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath }).catch(() => {});
      }
      await invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {});
    } else if (item?.targetPath && item.status !== "Completed") {
      await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath }).catch(() => {});
    }
    // Slice 5: drop the durable row too, or a removed job would be
    // re-seeded from the database on the next startup.
    if (item?.source !== "native") {
      void invoke("remove_job", { jobId: id }).catch(() => {});
    }
    // The job is gone: its rate sample must not linger and skew the
    // status-bar aggregate.
    speedSamples.current.delete(id);
    setItems((current) => current.filter((entry) => entry.id !== id || entry.status === "Downloading" || entry.status === "Pausing"));
  }
  function retry(item: Item) {
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
    // Slice 5: drop the durable rows too, or cleared jobs would be
    // re-seeded from the database on the next startup.
    await Promise.all(finished.filter((item) => item.source !== "native")
      .map((item) => invoke("remove_job", { jobId: item.id }).catch(() => {})));
    finished.forEach((item) => speedSamples.current.delete(item.id));
    setItems((current) => current.filter((item) => item.status !== "Completed" && item.status !== "Cancelled"));
  }
  async function inspect() {
    const clean = videoUrl.trim();
    if (!/^https?:\/\//i.test(clean)) { setVideoNotice("Paste a valid HTTP or HTTPS media URL."); return; }
    setInspecting(true); setVariants([]); setVideoNotice("Inspecting media URL…");
    try {
      const result = await invoke<Variant[]>("inspect_video_url", { url: clean, referer: null });
      setVariants(result);
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
  async function downloadGrabbed() {
    const chosen = grabbed.filter((item) => grabChecked[item.url]);
    if (!chosen.length) return;
    setGrabNotice("Queueing " + chosen.length + " item(s)…");
    let queued = 0;
    let failed = 0;
    // Sequential on purpose: each item goes through the same path a
    // one-off add would take — DASH inspects first and takes the best
    // quality, HLS and direct files queue directly — so every item
    // lands in the job store as its own job with its own checkpoint.
    for (const item of chosen) {
      try {
        if (item.kind === "dash") {
          const result = await invoke<Variant[]>("inspect_video_url", { url: item.url, referer: null });
          const variants = result.filter((variant) => variant.downloadable && variant.url);
          variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
          const best = variants[0];
          if (!best) { failed += 1; continue; }
          add(item.url, item.name, "dash", best.representationId, best.audioRepresentationId);
        } else if (item.kind === "hls") {
          add(item.url, item.name, "hls");
        } else {
          add(item.url, item.name, "direct");
        }
        queued += 1;
      } catch { failed += 1; }
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
      setNativeBanner(null);
      setNativeSetupStatus(message);
    } catch (error) {
      setNativeSetupStatus(String(error));
    } finally {
      setNativeSettingUp(false);
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
      {item.status === "Paused" && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void resume(item); }}>Resume</button>}
      {(item.status === "Failed" || item.status === "Interrupted") && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void retry(item); }}>{item.targetPath ? "Resume" : "Retry"}</button>}
      {canCancel(item) && <button className="quiet-button transfer-control" onClick={(event) => { event.stopPropagation(); void cancel(item); }}>Cancel</button>}
      {canRemove(item) && <button className="remove-button" title="Remove" aria-label="Remove" onClick={(event) => { event.stopPropagation(); void remove(item.id); }}>×</button>}
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
          {showKindColumn && <td className="col-kind">{item.kind === "hls" ? "HLS" : item.kind === "dash" ? "DASH" : "Direct"}</td>}
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
        {item.status === "Paused" && <button role="menuitem" onClick={() => { setContextMenu(null); void resume(item); }}>Resume</button>}
        {(item.status === "Failed" || item.status === "Interrupted") && <button role="menuitem" onClick={() => { setContextMenu(null); void retry(item); }}>{item.targetPath ? "Resume" : "Retry"}</button>}
        {canCancel(item) && <button role="menuitem" onClick={() => { setContextMenu(null); void cancel(item); }}>Cancel</button>}
        {canRemove(item) && <><hr /><button role="menuitem" onClick={() => { setContextMenu(null); void remove(item.id); }}>Remove</button></>}
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
          <div><dt>Resume capability</dt><dd>{item.targetPath ? "Yes — checkpoint saved" : "No"}</dd></div>
          {item.error && <div><dt>Error</dt><dd className="error-text">{item.error}</dd></div>}
        </dl>
        <p className="inspector-detail">{item.detail}</p>
        <div className="inspector-actions">{actionButtons(item)}</div>
      </div>}
      {inspectorTab === "files" && <div className="inspector-body">
        <div className="file-row"><span className="file-type">{item.kind === "hls" ? "▶" : "↓"}</span><div><strong title={item.name}>{item.name}</strong><small>{item.total ? bytes(item.total) : bytes(item.downloaded) + " downloaded so far"}</small></div></div>
        <dl className="kv">
          <div><dt>Location</dt><dd className="kv-wrap" title={item.targetPath || ""}>{item.targetPath || "Not started yet"}</dd></div>
          <div><dt>Transfer kind</dt><dd>{item.kind === "hls" ? "HLS playlist" : item.kind === "dash" ? "DASH manifest" : "Direct file"}</dd></div>
        </dl>
        <p className="footnote">Partial data is kept next to the target while a transfer is incomplete. Pause saves a checkpoint; Resume continues from it.</p>
      </div>}
      {inspectorTab === "segments" && <div className="inspector-body">
        <dl className="kv">
          <div><dt>Connections</dt><dd>{connections} per file</dd></div>
          <div><dt>Segments</dt><dd>{item.segmentsDone !== undefined && item.segmentsTotal !== undefined ? item.segmentsDone + " of " + item.segmentsTotal + " done" : "This transfer does not report per-segment progress."}</dd></div>
          <div><dt>Downloaded</dt><dd>{bytes(item.downloaded)}{item.total ? " of " + bytes(item.total) : ""}</dd></div>
        </dl>
        <p className="footnote">Checkpoint-safe downloads currently use one stream. Multi-connection resume is still being developed.</p>
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
    return <div className="dialog-overlay" role="dialog" aria-label="Add a download" onClick={() => setAddOpen(false)}>
      <div className="dialog" onClick={(event) => event.stopPropagation()}>
        <div className="dialog-head"><div><h2>Add a download</h2><p>Direct file links and supported HLS playlists.</p></div><button className="remove-button" title="Close" aria-label="Close" onClick={() => setAddOpen(false)}>×</button></div>
        <form onSubmit={(event) => { event.preventDefault(); add(url, filename); }}>
          <label htmlFor="download-url">Download URL</label>
          <input id="download-url" className="field" value={url} onChange={(event) => setUrl(event.target.value)} placeholder="https://example.com/archive.zip" autoFocus />
          <div className="add-actions"><div className="filename-field"><label htmlFor="download-name">Save as <small>OPTIONAL</small></label><input id="download-name" className="field" value={filename} onChange={(event) => setFilename(event.target.value)} placeholder="Use original filename" /></div><button type="submit" className="primary-button">Add to queue ↗</button></div>
          <div className="schedule-row"><label htmlFor="schedule-at">Start later <small>OPTIONAL</small></label><input id="schedule-at" type="datetime-local" className="field schedule-field" min={new Date(clock - new Date(clock).getTimezoneOffset() * 60000).toISOString().slice(0, 16)} value={scheduleAt} onChange={(event) => setScheduleAt(event.target.value)} /></div>
        </form>
        <div className="notice">{notice}</div>
        {url.trim() && <button className="text-link" onClick={() => { setVideoUrl(url.trim()); setAddOpen(false); setGrabberOpen(true); }}>Inspect media qualities instead →</button>}
        <div className="dialog-foot"><span className="connection-tag">{connections} CONNECTIONS</span><button className="quiet-button" onClick={() => setAddOpen(false)}>Done</button></div>
      </div>
    </div>;
  }
  // Video grabber dialog (Direction A: the grabber is not a main-window
  // page — it is a dialog reachable from the Queues page and from the
  // Add form's "Inspect media qualities" link). Content is the old
  // Video page, unchanged.
  function grabberDialog() {
    if (!grabberOpen) return null;
    return <div className="dialog-overlay" role="dialog" aria-label="Video grabber" onClick={() => setGrabberOpen(false)}>
      <div className="dialog dialog-wide" onClick={(event) => event.stopPropagation()}>
        <div className="dialog-head"><div><h2>Video grabber</h2><p>Inspect one media URL, or grab every media file from one page.</p></div><button className="remove-button" title="Close" aria-label="Close" onClick={() => setGrabberOpen(false)}>×</button></div>
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
              <div><strong>{variant.quality}</strong><small>{variant.kind.toUpperCase()}{variant.width && variant.height ? " · " + variant.width + " × " + variant.height : ""}{variant.bandwidth ? " · " + Math.round(variant.bandwidth / 1000) + " kbps" : ""}</small>{variant.note && <p>{variant.note}</p>}</div>
              <button className="secondary-button" disabled={!variant.downloadable} onClick={() => add(variant.kind === "dash" ? videoUrl : variant.url, "video.mp4", variant.kind === "hls" ? "hls" : variant.kind === "dash" ? "dash" : "direct", variant.representationId, variant.audioRepresentationId)}>{variant.downloadable ? "Download" : "Unavailable"}</button>
            </div>)}
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
              <button className="primary-button" onClick={() => void downloadGrabbed()} disabled={!grabbed.some((item) => grabChecked[item.url])}>Download selected ({grabbed.filter((item) => grabChecked[item.url]).length})</button>
            </div>
            {grabTruncated && <div className="notice">Showing the first 100 items — download in batches, or narrow the page.</div>}
            {grabbed.map((item) => <div className="variant" key={item.url}>
              <label className="grab-item"><input type="checkbox" checked={!!grabChecked[item.url]} onChange={(event) => setGrabChecked((current) => ({ ...current, [item.url]: event.target.checked }))} /><span><strong>{item.name}</strong><small>{item.kind.toUpperCase()} · {hostOf(item.url)}</small></span></label>
            </div>)}
          </div>}
          <div className="support-note"><strong>Format support</strong><p>Direct HTTP/HTTPS files use resumable transfers. HLS supports completed unencrypted VOD, with Pause/Resume/Cancel. DASH supports static MPDs with SegmentTemplate/SegmentTimeline, SegmentList, SegmentBase and multi-period, with Pause/Resume/Cancel; separate audio/video requires FFmpeg. Live/dynamic MPDs and DRM are not supported. A YouTube watch page is not a direct media URL; page extraction of its streams is not supported, but "Grab a page" above lists media files a page links to directly.</p></div>
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
        <div className="version">ThunderLoad · 0.3.3</div>
      </div>
    </aside>

    <main className="main">
      <header className="topbar"><div>THUNDERLOAD <span>/</span> {title.toUpperCase()}</div><button className="quiet-button folder-button" onClick={openFolder}>Open downloads folder ↗</button></header>
      <div className="content-row">
      <div className="page">
        {nativeBanner && !nativeBannerDismissed && <div className="setup-banner" role="status">
          <div><strong>{nativeBanner === "missing-id" ? "Browser connection not set up yet" : "Browser connection could not be restored"}</strong><p>{nativeBanner === "missing-id"
            ? "Downloads from the browser extension need a one-time connection. It takes a minute: install the extension, copy its ID, and connect it in Settings."
            : "ThunderLoad tried to reconnect a browser whose extension ID was saved before, but registration failed. Open the browser setup to try again."}</p></div>
          <div className="setup-banner-actions">
            <button className="secondary-button" onClick={() => goTo("settings")}>Set up browser connection →</button>
            <button className="quiet-button" onClick={() => setNativeBannerDismissed(true)}>Dismiss</button>
          </div>
        </div>}
        <div className="page-heading"><div><h1>{title}</h1><p>{subtitle}</p></div>{page !== "settings" && <button className="primary-button" onClick={() => setAddOpen(true)}>＋ Add</button>}</div>

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
          {notice !== "Ready." && <div className="notice">{notice}</div>}
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
            <label className="setting-row"><span><strong>Connections per file</strong><small>Checkpoint-safe downloads currently use one stream. Multi-connection resume is still being developed.</small></span><select className="select-field" value={connections} disabled><option value={1}>1 connection</option></select></label>
            <label className="setting-row"><span><strong>Simultaneous downloads</strong><small>Maximum number of active queue items.</small></span><select className="select-field" value={parallel} onChange={(event) => setParallel(Number(event.target.value))}>{[1, 2, 3, 4].map((n) => <option key={n} value={n}>{n} {n === 1 ? "download" : "downloads"}</option>)}</select></label>
            <label className="setting-row"><span><strong>Speed limit</strong><small>Applies to active direct, HLS and DASH transfers in each ThunderLoad process. Browser companion and desktop share this saved setting; each process enforces its own aggregate cap. Zero means unlimited.</small></span><select className="select-field" value={speedLimit} onChange={(event) => setSpeedLimit(Number(event.target.value))}>
              <option value={0}>Unlimited</option><option value={262144}>256 KB/s</option><option value={524288}>512 KB/s</option><option value={1048576}>1 MB/s</option><option value={2097152}>2 MB/s</option><option value={5242880}>5 MB/s</option><option value={10485760}>10 MB/s</option>
            </select></label>
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
            <div><h2>Browser connection</h2><p>Register the optional local engine for adaptive video downloads. Direct file downloads work without this connection.</p></div>
            <div className="native-host-grid">
              <label><span>Browser</span><select className="select-field" value={nativeBrowser} onChange={(event) => {
                const next = event.target.value;
                setNativeBrowser(next);
                setNativeExtensionId(readNativeIds()[next] || (next === "firefox" ? "thunderload@example.com" : ""));
              }}>
                <option value="chrome">Google Chrome</option>
                <option value="edge">Microsoft Edge</option>
                <option value="brave">Brave</option>
                <option value="firefox">Firefox</option>
              </select></label>
              <label><span>Extension ID</span><input className="field" value={nativeExtensionId} onChange={(event) => setNativeExtensionId(event.target.value)} placeholder={nativeBrowser === "firefox" ? "thunderload@example.com" : "32 characters a–p"} /></label>
              <button className="secondary-button" onClick={registerNativeHost} disabled={nativeSettingUp}>{nativeSettingUp ? "Registering…" : "Connect browser"}</button>
            </div>
            {nativeSetupStatus && <div className="notice native-host-notice">{nativeSetupStatus}</div>}
            <ol className="setup-steps">
              <li><strong>Install the extension first</strong> — download it below in Browser extension files and load it in your browser (Chromium: Load unpacked in developer mode; Firefox: Install Add-on From File).</li>
              <li><strong>Find the extension ID</strong> — open <code>chrome://extensions</code> (Edge: <code>edge://extensions</code>, Brave: <code>brave://extensions</code>), turn <strong>Developer mode</strong> on, and copy the <strong>ID</strong> shown under the ThunderLoad extension. Firefox uses the fixed ID <code>thunderload@example.com</code> — it is filled in for you.</li>
              <li><strong>Connect it here</strong> — paste the ID above, choose your browser, and click <strong>Connect browser</strong>. ThunderLoad copies the native host into place, writes the browser manifest, and registers it. You only do this once per browser.</li>
              <li><strong>Restart the browser</strong> and allow <strong>Native messaging</strong> in the ThunderLoad extension when prompted. ThunderLoad re-registers automatically on later startups if the registration ever goes missing.</li>
            </ol>
          </div>
          <div className="rule" />
          <div className="settings-section">
            <div><h2>Browser extension files</h2><p>Download the extension package for your browser and load it in developer mode. No store listing is needed for this path.</p></div>
            <div className="setting-row">
              <div><strong>Chrome · Edge · Brave · Opera — ThunderLoad-Chromium.zip</strong><small>Extract the ZIP, then open chrome://extensions (Edge: edge://extensions, Brave: brave://extensions, Opera: opera://extensions), turn Developer mode on, click “Load unpacked” and select the extracted folder.</small></div>
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
          <div className="settings-section"><div><h2>About</h2><p>Local desktop transfer engine · Version 0.3.3</p></div><div className="about-line"><span>Supported media</span><strong>Direct files · HLS VOD · Static DASH VOD</strong></div><div className="about-line"><span>Direct downloads</span><strong>Pause · Resume · Cancel · Speed limit · Schedule</strong></div><div className="about-line"><span>Still in development</span><strong>Multi-connection resume · Live DASH</strong></div></div>
        </section>}

      </div>
      {inspectorPanel()}
      </div>
      <footer className="statusbar"><span className="statusbar-speed">↓ {aggregateLabel}</span><span>{active} active · {queued} queued · {done} completed · {attention} failed</span><span className="statusbar-right"><i className="engine-dot" />Local engine · Downloads stay on this device.</span></footer>
    </main>
    {addDialog()}
    {grabberDialog()}
    {contextMenuPanel()}
  </div>;
}

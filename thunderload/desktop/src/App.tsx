import { useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

type Page = "downloads" | "queue" | "video" | "settings";
type Kind = "direct" | "hls" | "dash";
type Status = "Queued" | "Downloading" | "Pausing" | "Paused" | "Cancelling" | "Completed" | "Failed" | "Interrupted" | "Cancelled";
type Variant = { kind: string; quality: string; width?: number; height?: number; bandwidth?: number; url: string; downloadable: boolean; note?: string; representationId?: string; audioRepresentationId?: string; audioOnly?: boolean };
type Progress = { downloaded: number; total?: number; percent?: number; phase: string };
type Item = { id: string; name: string; url: string; kind: Kind; status: Status; percent: number; downloaded: number; total?: number; detail: string; error?: string; createdAt: number; targetPath?: string; scheduledAt?: number; source?: "native"; representationId?: string; audioRepresentationId?: string };

const HISTORY_KEY = "thunderload.history.v2";
const CONNECTIONS_KEY = "thunderload.connections.v1";
const PARALLEL_KEY = "thunderload.parallel.v1";
const SPEED_LIMIT_KEY = "thunderload.speed-limit.v1";

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
    return saved.map((item: Item) => item.status === "Downloading" || item.status === "Pausing"
      ? item.kind === "direct" && item.targetPath
        ? { ...item, status: "Queued", detail: "Recovering saved partial download…" }
        : { ...item, status: "Interrupted", detail: "App closed before a resumable checkpoint was created" }
      : item);
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
function pageInfo(page: Page) {
  const values = {
    downloads: ["Downloads", "A clear view of what is moving and what is finished."],
    queue: ["Queue", "Transfers waiting, running, or needing attention."],
    video: ["Video grabber", "Find exposed media streams and choose an available quality."],
    settings: ["Settings", "A few useful controls. No decorative switches."]
  };
  return values[page];
}
function Status({ value }: { value: Status }) {
  return <span className={"status status-" + value.toLowerCase()}><i />{value}</span>;
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
  const [connections, setConnections] = useState(() => readSetting(CONNECTIONS_KEY, 1, [1]));
  const [parallel, setParallel] = useState(() => readSetting(PARALLEL_KEY, 2, [1, 2, 3, 4]));
  const [speedLimit, setSpeedLimit] = useState(() => readSetting(SPEED_LIMIT_KEY, 0, [0, 262144, 524288, 1048576, 2097152, 5242880, 10485760]));
  const [clock, setClock] = useState(Date.now());
  const [nativeBrowser, setNativeBrowser] = useState("chrome");
  const [nativeExtensionId, setNativeExtensionId] = useState("");
  const [nativeSetupStatus, setNativeSetupStatus] = useState("");
  const [nativeSettingUp, setNativeSettingUp] = useState(false);

  const active = items.filter((item) => item.status === "Downloading" || item.status === "Pausing" || item.status === "Cancelling").length;
  const queued = items.filter((item) => item.status === "Queued").length;
  const done = items.filter((item) => item.status === "Completed").length;
  const attention = items.filter((item) => item.status === "Failed" || item.status === "Interrupted").length;
  const ordered = useMemo(() => items.slice().sort((a, b) => b.createdAt - a.createdAt), [items]);
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
          status: (["Downloading", "Completed", "Failed", "Interrupted"].includes(job.status || "") ? job.status : "Failed") as Status,
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
    let cleanup: Array<() => void> = [];
    (async () => {
      const listeners = await Promise.all([
        listen<{ jobId: string; progress: Progress }>("download-progress", ({ payload }) => {
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
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Completed", percent: 100, detail: "Saved · " + payload.path, error: undefined }
            : item));
          setNotice("Download completed.");
        }),
        listen<{ jobId: string; targetPath: string }>("download-paused", ({ payload }) => {
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Paused", targetPath: payload.targetPath, detail: "Paused · partial file saved", error: undefined }
            : item));
        }),
        listen<{ jobId: string }>("download-cancelled", ({ payload }) => {
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Cancelled", detail: "Cancelled · partial file removed", targetPath: undefined, downloaded: 0, percent: 0 }
            : item));
        }),
listen<{ jobId: string; error: string; targetPath?: string }>("download-error", ({ payload }) => {
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Failed", detail: "Transfer interrupted · partial data kept", targetPath: payload.targetPath || item.targetPath, error: payload.error }
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
    const next = items.filter((item) => item.status === "Queued" && (!item.scheduledAt || item.scheduledAt <= clock)).slice(0, slots);
    if (!next.length) return;
    const ids = new Set(next.map((item) => item.id));
    setItems((current) => current.map((item) => ids.has(item.id) ? { ...item, status: "Downloading", detail: "Starting transfer…" } : item));
    next.forEach(async (item) => {
      try {
        if (item.kind === "hls") {
          await invoke("start_hls_download", { jobId: item.id, url: item.url, filename: item.name });
        } else if (item.kind === "dash") {
          await invoke("start_dash_download", {
            jobId: item.id, url: item.url, representationId: item.representationId,
            audioRepresentationId: item.audioRepresentationId || null, filename: item.name
          });
        } else {
          const targetPath = await invoke<string>("start_download", {
            jobId: item.id, url: item.url, filename: item.name, targetPath: item.targetPath || null, connections
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

  function add(value: string, saveAs?: string, requestedKind?: Kind, representationId?: string, audioRepresentationId?: string) {
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
    const suggested = nameFromUrl(clean);
    const item: Item = {
      id: crypto.randomUUID(), name: (saveAs || "").trim() || (kind === "hls" || kind === "dash" ? "video.mp4" : suggested),
      url: clean, kind, status: "Queued", percent: 0, downloaded: 0,
      representationId, audioRepresentationId,
      detail: scheduleAt && new Date(scheduleAt).getTime() > Date.now()
        ? "Scheduled for " + new Date(scheduleAt).toLocaleString()
        : "Waiting in queue",
      createdAt: Date.now(),
      scheduledAt: scheduleAt ? new Date(scheduleAt).getTime() : undefined
    };
    setItems((current) => [item, ...current]);
    setNotice("Added to queue.");
    setUrl(""); setFilename(""); setScheduleAt("");
  }
  async function pause(item: Item) {
    try {
      await invoke("pause_download", { jobId: item.id });
      setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "Pausing", detail: "Pausing · saving checkpoint…" } : entry));
    } catch (error) { setNotice(String(error)); }
  }
  function resume(item: Item) {
    if (item.kind !== "direct") { retry(item); return; }
    setItems((current) => current.map((entry) => entry.id === item.id
      ? { ...entry, status: "Queued", error: undefined, detail: "Resume queued from saved checkpoint" }
      : entry));
  }
  async function cancel(item: Item) {
    if (item.kind !== "direct" || !item.targetPath) {
      setItems((current) => current.map((entry) => entry.id === item.id ? { ...entry, status: "Cancelled", detail: "Cancelled" } : entry));
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
    if (item?.source === "native") {
      await invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {});
    } else if (item?.targetPath && item.kind === "direct" && item.status !== "Completed") {
      await invoke("cancel_download", { jobId: item.id, targetPath: item.targetPath }).catch(() => {});
    }
    setItems((current) => current.filter((entry) => entry.id !== id || entry.status === "Downloading" || entry.status === "Pausing"));
  }
  function retry(item: Item) {
    if (item.source === "native") { add(item.url, item.name, item.kind, item.representationId, item.audioRepresentationId); return; }
    if (item.kind === "direct" && item.targetPath) { resume(item); return; }
    add(item.url, item.name, item.kind, item.representationId, item.audioRepresentationId);
  }
  async function clearFinished() {
    const finished = items.filter((item) => item.status === "Completed" || item.status === "Cancelled");
    await Promise.all(finished.filter((item) => item.source === "native")
      .map((item) => invoke("dismiss_native_download", { jobId: item.id.replace(/^native-/, "") }).catch(() => {})));
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
      setNativeSetupStatus(message);
    } catch (error) {
      setNativeSetupStatus(String(error));
    } finally {
      setNativeSettingUp(false);
    }
  }
  async function openFolder() {
    try { await invoke("open_download_folder"); }
    catch (error) { setNotice("Could not open download folder: " + String(error)); }
  }
  function transferList(list: Item[]) {
    if (!list.length) return <div className="empty-state"><span className="empty-icon">↓</span><strong>No transfers yet</strong><p>Add a direct file URL to begin. Your activity will appear here.</p></div>;
    return <div className="transfer-list">{list.map((item) => <article className="transfer" key={item.id}>
      <div className="file-type">{item.kind === "hls" ? "▶" : "↓"}</div>
      <div className="transfer-body">
        <div className="transfer-top"><strong title={item.name}>{item.name}</strong><Status value={item.status} /></div>
        <div className="transfer-detail" title={item.url}>{item.detail}</div>
        {(item.status === "Downloading" || item.status === "Pausing" || item.status === "Paused" || item.status === "Completed") && <div className="progress"><span style={{ width: item.percent + "%" }} /></div>}
        {item.error && <div className="error-text">{item.error}</div>}
      </div>
      <div className="transfer-actions">
        {item.status === "Downloading" && item.kind === "direct" && item.source !== "native" && <button className="quiet-button transfer-control" onClick={() => void pause(item)}>Pause</button>}
        {item.status === "Paused" && <button className="quiet-button transfer-control" onClick={() => resume(item)}>Resume</button>}
        {(item.status === "Failed" || item.status === "Interrupted") && <button className="quiet-button transfer-control" onClick={() => retry(item)}>{item.kind === "direct" && item.targetPath ? "Resume" : "Retry"}</button>}
        {(item.status === "Downloading" || item.status === "Pausing" || item.status === "Paused" || item.status === "Failed" || item.status === "Interrupted" || item.status === "Queued") && item.kind === "direct" && item.source !== "native" && <button className="quiet-button transfer-control" onClick={() => void cancel(item)}>Cancel</button>}
        {item.status !== "Downloading" && item.status !== "Pausing" && item.status !== "Cancelling" && (item.source !== "native" || item.status === "Completed" || item.status === "Failed" || item.status === "Interrupted") && <button className="remove-button" title="Remove" aria-label="Remove" onClick={() => void remove(item.id)}>×</button>}
      </div>
    </article>)}</div>;
  }

  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-mark">↯</div><div><strong>ThunderLoad</strong><small>DOWNLOAD MANAGER</small></div></div>
      <div className="nav-caption">WORKSPACE</div>
      <nav>
        <button className={"nav-item " + (page === "downloads" ? "selected" : "")} onClick={() => setPage("downloads")}><span>↓</span>Downloads</button>
        <button className={"nav-item " + (page === "queue" ? "selected" : "")} onClick={() => setPage("queue")}><span>≡</span>Queue{queued > 0 && <em>{queued}</em>}</button>
        <button className={"nav-item " + (page === "video" ? "selected" : "")} onClick={() => setPage("video")}><span>▷</span>Video grabber</button>
      </nav>
      <div className="sidebar-bottom">
        <div className="engine-line"><i />Local engine</div>
        <button className={"nav-item " + (page === "settings" ? "selected" : "")} onClick={() => setPage("settings")}><span>⚙</span>Settings</button>
        <div className="version">ThunderLoad · 0.2.0</div>
      </div>
    </aside>

    <main className="main">
      <header className="topbar"><div>THUNDERLOAD <span>/</span> {title.toUpperCase()}</div><button className="quiet-button folder-button" onClick={openFolder}>Open downloads folder ↗</button></header>
      <div className="page">
        <div className="page-heading"><div><h1>{title}</h1><p>{subtitle}</p></div>{page === "downloads" && <button className="primary-button" onClick={() => document.getElementById("download-url")?.focus()}>＋ Add download</button>}</div>

        {page === "downloads" && <>
          <section className="summary">
            <div><strong>{active}</strong><span>Active</span></div><div><strong>{queued}</strong><span>Queued</span></div><div><strong>{done}</strong><span>Completed</span></div><div><strong>{attention}</strong><span>Needs attention</span></div>
          </section>
          <section className="panel">
            <div className="panel-heading"><div><h2>Add a download</h2><p>Direct file links and supported HLS playlists.</p></div><span className="connection-tag">{connections} CONNECTIONS</span></div>
            <form onSubmit={(event) => { event.preventDefault(); add(url, filename); }}>
              <label htmlFor="download-url">Download URL</label>
              <input id="download-url" className="field" value={url} onChange={(event) => setUrl(event.target.value)} placeholder="https://example.com/archive.zip" />
              <div className="add-actions"><div className="filename-field"><label htmlFor="download-name">Save as <small>OPTIONAL</small></label><input id="download-name" className="field" value={filename} onChange={(event) => setFilename(event.target.value)} placeholder="Use original filename" /></div><button type="submit" className="primary-button">Add to queue ↗</button></div>
              <div className="schedule-row"><label htmlFor="schedule-at">Start later <small>OPTIONAL</small></label><input id="schedule-at" type="datetime-local" className="field schedule-field" min={new Date(clock - new Date(clock).getTimezoneOffset() * 60000).toISOString().slice(0, 16)} value={scheduleAt} onChange={(event) => setScheduleAt(event.target.value)} /></div>
            </form>
            <div className="notice">{notice}</div>
            {url.trim() && <button className="text-link" onClick={() => { setVideoUrl(url.trim()); setPage("video"); }}>Inspect media qualities instead →</button>}
          </section>
          <section className="panel">
            <div className="panel-heading"><div><h2>Recent transfers</h2><p>Latest activity on this device.</p></div><button className="quiet-button" onClick={clearFinished}>Clear finished</button></div>
            {transferList(ordered.slice(0, 6))}
            {ordered.length > 6 && <button className="text-link" onClick={() => setPage("queue")}>View all transfers →</button>}
          </section>
        </>}

        {page === "queue" && <section className="panel queue-panel">
          <div className="summary queue-summary"><div><strong>{active}</strong><span>Active</span></div><div><strong>{queued}</strong><span>Waiting</span></div><div><strong>{done}</strong><span>Completed</span></div><div><strong>{attention}</strong><span>Failed</span></div></div>
          <div className="panel-heading"><div><h2>All transfers</h2><p>Jobs start automatically, up to your active transfer limit.</p></div><button className="quiet-button" onClick={clearFinished}>Clear finished</button></div>
          {transferList(ordered)}
          <p className="footnote">Direct HTTP downloads keep a partial file on disk. Pause saves a checkpoint, Resume continues with HTTP Range when the server permits it, and startup automatically recovers saved direct-download checkpoints. Scheduled jobs start at their selected local time.</p>
        </section>}

        {page === "video" && <section className="panel video-panel">
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
              <button className="secondary-button" disabled={!variant.downloadable} onClick={() => add(videoUrl, "video.mp4", variant.kind === "hls" ? "hls" : variant.kind === "dash" ? "dash" : "direct", variant.representationId, variant.audioRepresentationId)}>{variant.downloadable ? "Download" : "Unavailable"}</button>
            </div>)}
          </div>}
          <div className="support-note"><strong>Format support</strong><p>Direct HTTP/HTTPS files use resumable transfers. HLS supports completed unencrypted VOD. DASH supports static MPDs with SegmentTemplate/SegmentTimeline; separate audio/video requires FFmpeg. Live/dynamic MPDs, DRM, SegmentBase/SegmentList and multi-period DASH are not supported. A YouTube watch page is not a direct media URL; page extraction is not available in this build.</p></div>
        </section>}

        {page === "settings" && <section className="panel settings-panel">
          <div className="settings-section"><div><h2>Transfer behaviour</h2><p>Saved locally. Applies to new downloads.</p></div>
            <label className="setting-row"><span><strong>Connections per file</strong><small>Checkpoint-safe downloads currently use one stream. Multi-connection resume is still being developed.</small></span><select className="select-field" value={connections} disabled><option value={1}>1 connection</option></select></label>
            <label className="setting-row"><span><strong>Simultaneous downloads</strong><small>Maximum number of active queue items.</small></span><select className="select-field" value={parallel} onChange={(event) => setParallel(Number(event.target.value))}>{[1, 2, 3, 4].map((n) => <option key={n} value={n}>{n} {n === 1 ? "download" : "downloads"}</option>)}</select></label>
            <label className="setting-row"><span><strong>Speed limit</strong><small>Applies to direct desktop HTTP transfers, including active jobs. Zero means unlimited.</small></span><select className="select-field" value={speedLimit} onChange={(event) => setSpeedLimit(Number(event.target.value))}>
              <option value={0}>Unlimited</option><option value={262144}>256 KB/s</option><option value={524288}>512 KB/s</option><option value={1048576}>1 MB/s</option><option value={2097152}>2 MB/s</option><option value={5242880}>5 MB/s</option><option value={10485760}>10 MB/s</option>
            </select></label>
          </div>
          <div className="rule" />
          <div className="settings-section"><div><h2>Storage</h2><p>ThunderLoad uses a dedicated folder in your system Downloads directory.</p></div><div className="storage-row"><span>Downloads / ThunderLoad</span><button className="secondary-button" onClick={openFolder}>Open folder ↗</button></div></div>
          <div className="rule" />
          <div className="settings-section">
            <div><h2>Browser connection</h2><p>Register the optional local engine for adaptive video downloads. Direct file downloads work without this connection.</p></div>
            <div className="native-host-grid">
              <label><span>Browser</span><select className="select-field" value={nativeBrowser} onChange={(event) => {
                setNativeBrowser(event.target.value);
                if (event.target.value === "firefox" && !nativeExtensionId) setNativeExtensionId("thunderload@example.com");
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
            <p className="footnote">Copy the ID from the browser's Extensions page. After registration, restart the browser and allow Native messaging in the ThunderLoad extension.</p>
          </div>
          <div className="rule" />
          <div className="settings-section"><div><h2>About</h2><p>Local desktop transfer engine · Version 0.2.0</p></div><div className="about-line"><span>Supported media</span><strong>Direct files · HLS</strong></div><div className="about-line"><span>Direct downloads</span><strong>Pause · Resume · Cancel · Speed limit · Schedule</strong></div><div className="about-line"><span>Still in development</span><strong>DASH remux · Multi-connection resume · HLS pause</strong></div></div>
        </section>}

        <footer className="app-footer"><span>ThunderLoad</span><span>Downloads stay on this device.</span></footer>
      </div>
    </main>
  </div>;
}

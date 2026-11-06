import { useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

type Page = "downloads" | "queue" | "video" | "settings";
type Kind = "direct" | "hls" | "dash";
type Status = "Scheduled" | "Queued" | "Downloading" | "Pausing" | "Paused" | "Completed" | "Failed" | "Interrupted" | "Cancelling" | "Cancelled";
type Variant = { kind: string; quality: string; width?: number; height?: number; bandwidth?: number; url: string; representationId?: string; downloadable: boolean; note?: string };
type Progress = { downloaded: number; total?: number; percent?: number; phase: string };
type Item = { id: string; name: string; url: string; kind: Kind; representationId?: string; status: Status; percent: number; downloaded: number; total?: number; detail: string; error?: string; createdAt: number };
type StoredJob = { id: string; url: string; filename: string; kind: string; representationId?: string; status: Status; targetPath: string; connections: number; downloaded: number; total?: number; percent: number; detail: string; error?: string; createdAt: number; updatedAt: number; scheduledAt?: number };
function fromStoredJob(job: StoredJob): Item {
  return { id: job.id, name: job.filename, url: job.url, kind: job.kind === "hls" ? "hls" : job.kind === "dash" ? "dash" : "direct", representationId: job.representationId, status: job.status, percent: job.percent || 0, downloaded: job.downloaded || 0, total: job.total, detail: job.detail, error: job.error, createdAt: job.createdAt };
}

const CONNECTIONS_KEY = "thunderload.connections.v1";
const PARALLEL_KEY = "thunderload.parallel.v1";
const SPEED_KEY = "thunderload.speed-limit-kbps.v1";

function bytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return (value / Math.pow(1024, unit)).toFixed(unit ? 1 : 0) + " " + units[unit];
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
  const [videoUrl, setVideoUrl] = useState("");
  const [items, setItems] = useState<Item[]>([]);
  const [variants, setVariants] = useState<Variant[]>([]);
  const [inspecting, setInspecting] = useState(false);
  const [notice, setNotice] = useState("Ready.");
  const [videoNotice, setVideoNotice] = useState("");
  const [connections, setConnections] = useState(() => readSetting(CONNECTIONS_KEY, 8, [1, 2, 4, 8, 12, 16]));
  const [parallel, setParallel] = useState(() => readSetting(PARALLEL_KEY, 2, [1, 2, 3, 4]));
  const [speedLimitKbps, setSpeedLimitKbps] = useState(() => readSetting(SPEED_KEY, 0, [0, 64, 128, 256, 512, 1024, 2048, 5120]));
  const [scheduleAt, setScheduleAt] = useState("");
  const [nativeBrowser, setNativeBrowser] = useState("chrome");
  const [nativeExtensionId, setNativeExtensionId] = useState("");
  const [nativeSetupStatus, setNativeSetupStatus] = useState("");
  const [nativeSettingUp, setNativeSettingUp] = useState(false);

  const active = items.filter((item) => ["Downloading", "Pausing", "Cancelling"].includes(item.status)).length;
  const queued = items.filter((item) => ["Queued", "Scheduled"].includes(item.status)).length;
  const done = items.filter((item) => item.status === "Completed").length;
  const attention = items.filter((item) => ["Failed", "Interrupted"].includes(item.status)).length;
  const ordered = useMemo(() => items.slice().sort((a, b) => b.createdAt - a.createdAt), [items]);
  const [title, subtitle] = pageInfo(page);

  useEffect(() => { localStorage.setItem(CONNECTIONS_KEY, String(connections)); }, [connections]);
  useEffect(() => { localStorage.setItem(PARALLEL_KEY, String(parallel)); void invoke("set_max_active_command", { count: parallel }); }, [parallel]);
  useEffect(() => { localStorage.setItem(SPEED_KEY, String(speedLimitKbps)); void invoke("set_speed_limit_command", { bytesPerSecond: Math.round(speedLimitKbps * 1024) }); }, [speedLimitKbps]);

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
            detail: item.kind !== "direct"
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
        listen<{ jobId: string; error: string }>("download-error", ({ payload }) => {
          setItems((current) => current.map((item) => item.id === payload.jobId
            ? { ...item, status: "Failed", detail: "Transfer failed", error: payload.error }
            : item));
          setNotice(payload.error);
        }),
        listen<StoredJob>("job-updated", ({ payload }) => {
          const incoming = fromStoredJob(payload);
          setItems((current) => current.some((item) => item.id === incoming.id)
            ? current.map((item) => item.id === incoming.id ? incoming : item)
            : [incoming, ...current]);
        })
      ]);
      if (!alive) listeners.forEach((fn) => fn());
      else cleanup = listeners;
    })().catch((error) => setNotice(String(error)));
    void invoke<StoredJob[]>("list_downloads_command").then((saved) => {
      if (alive) setItems(saved.map(fromStoredJob));
    }).catch((error) => setNotice("Could not restore saved queue: " + String(error)));
    return () => { alive = false; cleanup.forEach((fn) => fn()); };
  }, []);

  async function add(value: string, saveAs?: string, requestedKind?: Kind, representationId?: string) {
    const clean = value.trim();
    if (!/^https?:\/\//i.test(clean)) { setNotice("Enter a valid HTTP or HTTPS URL."); return; }
    let kind: Kind = requestedKind || "direct";
    try {
      const pathname = new URL(clean).pathname.toLowerCase();
      if (!requestedKind && pathname.endsWith(".m3u8")) kind = "hls";
      else if (!requestedKind && pathname.endsWith(".mpd")) kind = "dash";
    } catch { setNotice("That URL is not valid."); return; }
    const suggested = nameFromUrl(clean);
    const item: Item = {
      id: crypto.randomUUID(),
      name: (saveAs || "").trim() || (kind === "hls" || kind === "dash" ? "video.mp4" : suggested),
      url: clean, kind, representationId, status: "Queued", percent: 0, downloaded: 0,
      detail: "Saving to persistent queue", createdAt: Date.now()
    };
    try {
      const scheduledAt = scheduleAt ? new Date(scheduleAt).getTime() : null;
      if (scheduledAt !== null && (!Number.isFinite(scheduledAt) || scheduledAt <= Date.now())) {
        setNotice("Choose a schedule time in the future.");
        return;
      }
      const stored = await invoke<StoredJob>("enqueue_download_command", {
        jobId: item.id, url: clean, filename: item.name, kind, connections,
        scheduledAt, representationId: representationId || null
      });
      const savedItem = fromStoredJob(stored);
      setItems((current) => [savedItem, ...current.filter((entry) => entry.id !== savedItem.id)]);
      setNotice(scheduledAt ? "Download scheduled." : "Added to persistent queue.");
      setUrl(""); setFilename(""); setScheduleAt("");
    } catch (error) {
      setItems((current) => [{ ...item, status: "Failed", error: String(error), detail: "Could not save job" }, ...current]);
      setNotice("Could not add download: " + String(error));
    }
  }
  async function remove(id: string) {
    try {
      await invoke("remove_download_command", { jobId: id });
      setItems((current) => current.filter((item) => item.id !== id));
    } catch (error) { setNotice(String(error)); }
  }
  async function retry(item: Item) {
    try { await invoke("resume_download_command", { jobId: item.id }); }
    catch (error) { setNotice(String(error)); }
  }
  async function pause(item: Item) {
    try { await invoke("pause_download_command", { jobId: item.id }); }
    catch (error) { setNotice(String(error)); }
  }
  async function cancel(item: Item) {
    try { await invoke("cancel_download_command", { jobId: item.id }); }
    catch (error) { setNotice(String(error)); }
  }
  function clearFinished() {
    const finished = items.filter((item) => ["Completed", "Cancelled"].includes(item.status));
    finished.forEach((item) => { void remove(item.id); });
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
    if (!list.length) return <div className="empty-state"><span className="empty-icon">↓</span><strong>No transfers yet</strong><p>Add a direct file, HLS playlist or supported DASH manifest.</p></div>;
    return <div className="transfer-list">{list.map((item) => <article className="transfer" key={item.id}>
      <div className="file-type">{item.kind === "direct" ? "↓" : "▶"}</div>
      <div className="transfer-body">
        <div className="transfer-top"><strong title={item.name}>{item.name}</strong><Status value={item.status} /></div>
        <div className="transfer-detail" title={item.url}>{item.detail}</div>
        {(["Downloading", "Pausing", "Paused", "Completed"].includes(item.status)) && <div className="progress"><span style={{ width: item.percent + "%" }} /></div>}
        {item.error && <div className="error-text">{item.error}</div>}
      </div>
      <div className="transfer-actions">
        {item.status === "Downloading" && <button className="quiet-button" onClick={() => pause(item)}>Pause</button>}
        {["Paused", "Interrupted", "Failed"].includes(item.status) && <button className="quiet-button" onClick={() => retry(item)}>Resume</button>}
        {["Queued", "Scheduled", "Downloading", "Pausing", "Paused"].includes(item.status) && <button className="quiet-button" onClick={() => cancel(item)}>Cancel</button>}
        {["Completed", "Cancelled", "Failed", "Interrupted"].includes(item.status) && <button className="remove-button" title="Remove" aria-label="Remove" onClick={() => remove(item.id)}>×</button>}
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
            <div className="panel-heading"><div><h2>Add a download</h2><p>Direct files, HLS and supported static DASH VOD manifests.</p></div><span className="connection-tag">{connections} CONNECTIONS</span></div>
            <form onSubmit={(event) => { event.preventDefault(); add(url, filename); }}>
              <label htmlFor="download-url">Download URL</label>
              <input id="download-url" className="field" value={url} onChange={(event) => setUrl(event.target.value)} placeholder="https://example.com/archive.zip" />
              <div className="add-actions"><div className="filename-field"><label htmlFor="download-name">Save as <small>OPTIONAL</small></label><input id="download-name" className="field" value={filename} onChange={(event) => setFilename(event.target.value)} placeholder="Use original filename" /></div><button type="submit" className="primary-button">Add to queue ↗</button></div>
              <label htmlFor="schedule-at">Schedule start <small>OPTIONAL · LOCAL TIME</small></label>
              <input id="schedule-at" className="field" type="datetime-local" value={scheduleAt} onChange={(event) => setScheduleAt(event.target.value)} />
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
          <p className="footnote">Downloads that are interrupted by closing the app can be retried. Resuming partial files is not available in this build.</p>
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
              <button className="secondary-button" disabled={!variant.downloadable} onClick={() => add(variant.url, "video.mp4", variant.kind === "hls" ? "hls" : variant.kind === "dash" ? "dash" : "direct", variant.representationId)}>{variant.downloadable ? "Download" : "Unavailable"}</button>
            </div>)}
          </div>}
          <div className="support-note"><strong>What works</strong><p>Direct media files and unencrypted HLS playlists. DASH quality detection is available, but DASH downloading is not. A YouTube watch page is not a direct media URL; this desktop build does not extract its streams.</p></div>
        </section>}

        {page === "settings" && <section className="panel settings-panel">
          <div className="settings-section"><div><h2>Transfer behaviour</h2><p>Saved locally. Applies to new downloads.</p></div>
            <label className="setting-row"><span><strong>Connections per file</strong><small>Used when the server supports byte-range requests.</small></span><select className="select-field" value={connections} onChange={(event) => setConnections(Number(event.target.value))}>{[1, 2, 4, 8, 12, 16].map((n) => <option key={n} value={n}>{n} {n === 1 ? "connection" : "connections"}</option>)}</select></label>
            <label className="setting-row"><span><strong>Simultaneous downloads</strong><small>Maximum number of active queue items.</small></span><select className="select-field" value={parallel} onChange={(event) => setParallel(Number(event.target.value))}>{[1, 2, 3, 4].map((n) => <option key={n} value={n}>{n} {n === 1 ? "download" : "downloads"}</option>)}</select></label>
            <label className="setting-row"><span><strong>Speed limit per download</strong><small>0 means unlimited; applies to running transfers.</small></span><select className="select-field" value={speedLimitKbps} onChange={(event) => setSpeedLimitKbps(Number(event.target.value))}>{[0, 64, 128, 256, 512, 1024, 2048, 5120].map((n) => <option key={n} value={n}>{n === 0 ? "Unlimited" : n >= 1024 ? (n / 1024) + " MB/s" : n + " KB/s"}</option>)}</select></label>
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
          <div className="settings-section"><div><h2>About</h2><p>Local desktop transfer engine · Version 0.2.0</p></div><div className="about-line"><span>Supported media</span><strong>Direct files · HLS</strong></div><div className="about-line"><span>Not available yet</span><strong>Resume · DASH remux · Speed limit</strong></div></div>
        </section>}

        <footer className="app-footer"><span>ThunderLoad</span><span>Downloads stay on this device.</span></footer>
      </div>
    </main>
  </div>;
}

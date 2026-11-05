import { useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

type Variant = { kind: string; quality: string; width?: number; height?: number; bandwidth?: number; url: string; downloadable: boolean; note?: string };
type Progress = { downloaded: number; total?: number; percent?: number; phase: string };
type DownloadItem = { id: string; name: string; url: string; status: string; percent: number; speed: string };

const formatBytes = (value: number) => {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`;
};

export default function App() {
  const [url, setUrl] = useState("");
  const [filename, setFilename] = useState("");
  const [items, setItems] = useState<DownloadItem[]>([]);
  const [variants, setVariants] = useState<Variant[]>([]);
  const [notice, setNotice] = useState("Ready. Paste a downloadable URL to get started.");
  const [connections, setConnections] = useState(8);
  const [speedLimit, setSpeedLimit] = useState("Unlimited");
  const totalActive = items.filter((item) => item.status === "Downloading").length;
  const completed = items.filter((item) => item.status === "Completed").length;
  const totalDownloaded = useMemo(() => items.filter((item) => item.status === "Completed").length, [items]);

  useEffect(() => {
    let unlistenProgress: (() => void) | undefined;
    let unlistenComplete: (() => void) | undefined;
    let unlistenError: (() => void) | undefined;
    (async () => {
      unlistenProgress = await listen<{ jobId: string; progress: Progress }>("download-progress", (event) => {
        const { jobId, progress } = event.payload;
        setItems((current) => current.map((item) => item.id === jobId ? {
          ...item, percent: progress.percent ?? item.percent,
          status: progress.phase === "complete" ? "Completed" : "Downloading",
          speed: `${formatBytes(progress.downloaded)} / ${progress.total ? formatBytes(progress.total) : "?"}`,
        } : item));
      });
      unlistenComplete = await listen<{ jobId: string; path: string }>("download-complete", ({ payload }) => {
        setItems((current) => current.map((item) => item.id === payload.jobId ? { ...item, status: "Completed", percent: 100, speed: "Saved" } : item));
        setNotice(`Saved: ${payload.path}`);
      });
      unlistenError = await listen<{ jobId: string; error: string }>("download-error", ({ payload }) => {
        setItems((current) => current.map((item) => item.id === payload.jobId ? { ...item, status: "Failed", speed: payload.error } : item));
        setNotice(payload.error);
      });
    })().catch((error) => setNotice(String(error)));
    return () => { unlistenProgress?.(); unlistenComplete?.(); unlistenError?.(); };
  }, []);

  async function addDownload(downloadUrl = url, name = filename || "download.bin") {
    if (!/^https?:\/\//i.test(downloadUrl)) { setNotice("Enter a valid http or https URL."); return; }
    try {
      const id = await invoke<string>("start_download", { url: downloadUrl, filename: name, connections });
      setItems((current) => [{ id, name, url: downloadUrl, status: "Downloading", percent: 0, speed: "Starting…" }, ...current]);
      setNotice("Download started.");
      setUrl(""); setFilename("");
    } catch (error) { setNotice(String(error)); }
  }

  async function inspectUrl() {
    if (!/^https?:\/\//i.test(url)) { setNotice("Paste an http or https media URL first."); return; }
    try {
      const result = await invoke<Variant[]>("inspect_video_url", { url, referer: null });
      setVariants(result);
      setNotice(`${result.length} stream option(s) found.`);
    } catch (error) { setNotice(String(error)); }
  }

  async function downloadVariant(variant: Variant) {
    if (!variant.downloadable) return;
    if (variant.kind === "hls") {
      setNotice("HLS downloading is currently available from the browser extension native host. Desktop HLS command is the next integration step.");
      return;
    }
    await addDownload(variant.url, filename || "video.mp4");
  }

  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-icon">T</div><div><b>ThunderLoad</b><small>DOWNLOAD MANAGER</small></div></div>
      <nav><button className="nav active">▦ <span>Downloads</span></button><button className="nav">◷ <span>Queue</span></button><button className="nav">▤ <span>Video grabber</span></button><button className="nav">⚙ <span>Settings</span></button></nav>
      <div className="sidebar-bottom"><span className="pulse"/> Engine ready <small>Starter build · v0.1.0</small></div>
    </aside>
    <main className="main-content">
      <header className="page-header"><div><div className="eyebrow">DOWNLOAD WORKSPACE</div><h1>Downloads</h1><p>Fast, quiet, and under your control.</p></div><div className="header-chip">● Local engine</div></header>
      <section className="stats-grid">
        <div className="stat-card"><span>Active downloads</span><strong>{totalActive}</strong><small>Currently transferring</small></div>
        <div className="stat-card"><span>Completed</span><strong>{completed}</strong><small>In this session</small></div>
        <div className="stat-card"><span>Saved items</span><strong>{totalDownloaded}</strong><small>Session count</small></div>
      </section>
      <section className="panel add-panel">
        <div className="panel-title"><div><h2>Add a download</h2><p>Paste a direct file URL or a supported media manifest.</p></div><span className="tag">MULTI-CONNECTION</span></div>
        <label>Download URL<input value={url} onChange={(event) => setUrl(event.target.value)} placeholder="https://example.com/file.zip" /></label>
        <div className="input-row"><label>Save as<input value={filename} onChange={(event) => setFilename(event.target.value)} placeholder="Optional filename" /></label><div className="action-stack"><button className="button secondary-button" onClick={inspectUrl}>Inspect video</button><button className="button primary-button" onClick={() => addDownload()}>Start download ↗</button></div></div>
        {variants.length > 0 && <div className="variant-list"><b>Detected streams</b>{variants.map((variant, index) => <div className="variant" key={`${variant.url}-${index}`}><span>{variant.quality}<small>{variant.kind.toUpperCase()} {variant.note ? `· ${variant.note}` : ""}</small></span><button disabled={!variant.downloadable} onClick={() => downloadVariant(variant)}>{variant.downloadable ? "Download" : "Unavailable"}</button></div>)}</div>}
        <div className="notice">{notice}</div>
      </section>
      <section className="panel queue-panel"><div className="panel-title"><div><h2>Recent downloads</h2><p>Your current session activity.</p></div><button className="text-button" onClick={() => setItems([])}>Clear list</button></div>
        {items.length === 0 ? <div className="empty-state"><div className="empty-icon">↓</div><b>No downloads yet</b><span>Your downloads will appear here as soon as you start one.</span></div> : <div className="download-list">{items.map((item) => <article className="download-row" key={item.id}><div className="file-icon">↓</div><div className="file-info"><b>{item.name}</b><small>{item.speed}</small><div className="progress"><i style={{ width: `${item.percent}%` }}/></div></div><span className={`state ${item.status.toLowerCase()}`}>{item.status}</span></article>)}</div>}
      </section>
      <section className="panel settings-panel"><div className="panel-title"><div><h2>Quick performance settings</h2><p>These controls set the starter engine defaults.</p></div></div><div className="settings-grid"><label>Connections per file<select value={connections} onChange={(event) => setConnections(Number(event.target.value))}>{[2,4,6,8,12,16].map((n) => <option key={n} value={n}>{n} connections</option>)}</select></label><label>Speed limit<select value={speedLimit} onChange={(event) => setSpeedLimit(event.target.value)}><option>Unlimited</option><option>1 MB/s</option><option>5 MB/s</option><option>10 MB/s</option></select></label><div className="mode-card"><span>Video scan mode</span><b>On demand</b></div></div><p className="small-note">Speed limiting and persistent preferences are visual settings in this starter; they are not wired to the engine yet. Quiet video scanning is user-triggered from the browser extension.</p></section>
      <footer>ThunderLoad Starter · Video support is limited to exposed, unencrypted HLS and direct media URLs. DASH qualities can be detected but are not downloaded in this starter.</footer>
    </main>
  </div>;
}

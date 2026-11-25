(() => {
  if (globalThis.__thunderLoadScannerInstalled) return;
  globalThis.__thunderLoadScannerInstalled = true;

  // Same alias as popup.js/background.js: `browser` in Firefox,
  // `chrome` in Chrome. (Slice 7c, Gemini claim 9 — consistency only.)
  const api = globalThis.browser ?? chrome;

  const found = new Map();
  // Same media set as the engine grabber (engine/src/grabber.rs) so
  // both surfaces agree on what counts. Playlists first, then files.
  const MEDIA_EXT = /\.(?:m3u8|mpd|mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav|ogg|opus|flac|pdf|zip)(?:$|[?#])/i;
  const SEGMENT_EXT = /\.(?:ts|m4s|cmfv|cmfa)(?:$|[?#])/i;
  // Mirror of background.js: YouTube's DASH manifest URL (no .mpd
  // suffix) and the service-endpoint exclusion. Kept in sync with
  // YOUTUBE_DASH_MANIFEST / isYouTubeServiceUrl() in background.js.
  const YOUTUBE_DASH_MANIFEST = /(?:^|[\/.])youtube\.com\/api\/manifest\/dash(?:[\/?#]|$)/i;
  const YOUTUBE_SERVICE_PATH = /^\/(?:s|api|youtubei)\//i;
  function isYouTubeServiceUrl(url) {
    try {
      const parsed = new URL(url);
      const host = parsed.hostname.toLowerCase();
      return (host === "youtube.com" || host.endsWith(".youtube.com") || host === "youtubei.googleapis.com")
        && YOUTUBE_SERVICE_PATH.test(parsed.pathname);
    } catch (_) { return false; }
  }
  // Slice 3: pages yt-dlp knows how to extract — same host list as
  // background.js YTDLP_HOSTS. On these pages the pill prefers the
  // yt-dlp panel over a bare sniffed manifest.
  const YTDLP_HOSTS = ["youtube.com", "youtu.be", "instagram.com", "tiktok.com", "twitter.com", "x.com", "facebook.com", "fb.watch", "reddit.com", "twitch.tv", "vimeo.com"];
  function isYtDlpPageUrl(url) {
    try {
      const host = new URL(url).hostname.toLowerCase();
      return YTDLP_HOSTS.some((listed) => host === listed || host.endsWith("." + listed));
    } catch (_) { return false; }
  }
  function normalizeMissingTool(missing) {
    if (Array.isArray(missing)) return typeof missing[0] === "string" && missing[0] ? missing[0] : null;
    return typeof missing === "string" && missing ? missing : null;
  }
  function sanitizeSubtitleFilename(name) {
    const cleaned = String(name || "subtitles.vtt").replace(/[\u0000-\u001f\\/:*?"<>|]+/g, "_");
    const dot = cleaned.lastIndexOf(".");
    if (dot > 0) {
      return (cleaned.slice(0, dot).slice(0, 180) || "subtitles") + cleaned.slice(dot);
    }
    return cleaned.slice(0, 180) || "subtitles.vtt";
  }
  function ytdlpDisplayName(tool) {
    const normalized = Array.isArray(tool) ? (tool[0] || "ytdlp") : tool;
    return normalized === "deno" ? "Deno" : "yt-dlp";
  }
  // The grabber caps at 100 items per page (same cap as the engine);
  // `truncated` tells the popup the page held more.
  const MAX_FOUND = 100;
  let truncated = false;
  // Distinct media URLs seen by the scan currently running, if any.
  // scan() recomputes `truncated` from this set at its end, so the
  // flag is exact for the current DOM even when `found` is already
  // full from an earlier scan (duplicates would otherwise never hit
  // the cap check in add() again — R1 follow-up).
  let currentScanUrls = null;
  // The cap flag must actually reach the background: add() returns
  // early once the cap is hit, so no later item message can carry it.
  // Notify once per scan with an item-less message instead.
  let truncatedNotified = false;
  function notifyTruncated() {
    if (truncatedNotified) return;
    truncatedNotified = true;
    api.runtime.sendMessage({ type: "VIDEO_SOURCES_FOUND", sources: [], truncated: true }).catch(() => {});
  }
  let scanScheduled = false;
  let activeVideo = null;
  let hideTimer = null;
  // Slice 1b: the pill auto-hides a few seconds after appearing.
  // PILL_AUTO_HIDE_MS is the default (Hamza's number if he corrects).
  const PILL_AUTO_HIDE_MS = 3000;
  let autoHideTimer = null;
  // Slice 1b: shown when the nativeMessaging permission cannot be
  // requested or is denied — one short sentence, no raw errors. The
  // extensions page differs per browser: about:addons on Firefox,
  // edge://extensions on Edge, opera://extensions on Opera, and
  // chrome://extensions everywhere else Chromium.
  const MANUAL_PERMISSION_STEPS = (() => {
    if (typeof globalThis.browser !== "undefined") {
      return "Allow it manually: about:addons → ThunderLoad → Details → Permissions → enable native messaging, then reload.";
    }
    const userAgent = typeof navigator !== "undefined" ? navigator.userAgent || "" : "";
    const extensionsPage = /Edg\//.test(userAgent)
      ? "edge://extensions"
      : /OPR\//.test(userAgent)
        ? "opera://extensions"
        : "chrome://extensions";
    return "Allow it manually: " + extensionsPage + " → ThunderLoad → Details → Permissions → enable native messaging, then reload.";
  })();
  let buttonHost = null;
  let button = null;
  let status = null;
  let overlayRoot = null;
  let overlayUnavailable = false;

  function classify(url) {
    if (!url || !/^https?:\/\//i.test(url)) return null;
    if (/\.m3u8(?:$|[?#])/i.test(url)) return "HLS playlist";
    if (/\.mpd(?:$|[?#])/i.test(url)) return "DASH manifest";
    // YouTube's DASH manifest has no .mpd suffix; match the manifest
    // URL itself, as background.js does. This check must run before
    // the service-URL exclusion below, so /api/manifest/dash is
    // never mistaken for a service endpoint.
    if (YOUTUBE_DASH_MANIFEST.test(url)) return "DASH manifest";
    if (SEGMENT_EXT.test(url)) return null;
    // Mirror of isYouTubeServiceUrl() in background.js (the two files
    // run in different contexts, so the predicate is duplicated here):
    // YouTube service endpoints serve JSON/search data with an
    // audio/video Content-Type, not media — they must never appear
    // as "Direct media".
    if (isYouTubeServiceUrl(url)) return null;
    if (MEDIA_EXT.test(url)) return "Direct media";
    return null;
  }

  function candidateSources(video) {
    const ownUrls = [
      video.currentSrc,
      video.src,
      ...Array.from(video.querySelectorAll("source")).map((node) => node.src)
    ];
    const ownSources = Array.from(new Map(ownUrls.map((url) => {
      const kind = classify(url);
      return kind ? [url, { url, kind }] : null;
    }).filter(Boolean)).values());
    if (ownSources.length) return ownSources;

    // Media Source Extensions often expose a blob: URL on the video element.
    // In that case offer discovered page media as explicit choices instead of
    // guessing which manifest belongs to the player.
    return Array.from(found.values())
      .filter((item) => item.kind === "HLS playlist" || item.kind === "DASH manifest" || item.kind === "Direct media")
      .sort((a, b) => b.discoveredAt - a.discoveredAt)
      .slice(0, 8);
  }

  function sourceFor(video) {
    const candidates = candidateSources(video);
    return candidates.length === 1 ? candidates[0] : null;
  }

  function add(url, label) {
    const kind = classify(url);
    if (!kind) return;
    // Count every classifiable URL this scan sees, duplicates included,
    // so scan() can tell a still-overflowing page from a shrunk one.
    if (currentScanUrls) currentScanUrls.add(url);
    if (found.has(url)) return;
    if (found.size >= MAX_FOUND) { truncated = true; notifyTruncated(); return; }
    const item = { url, kind, label: label || kind, discoveredAt: Date.now() };
    found.set(url, item);
    api.runtime.sendMessage({ type: "VIDEO_SOURCES_FOUND", sources: [item], truncated }).catch(() => {});
  }

  function ensureOverlay() {
    if (buttonHost || overlayUnavailable) return;
    buttonHost = document.createElement("div");
    buttonHost.style.cssText = "all:initial;position:fixed;inset:0;z-index:2147483646;pointer-events:none;";
    try {
      overlayRoot = buttonHost.attachShadow({ mode: "closed" });
    } catch (_) {
      // Auto-injection also runs on non-HTML documents (feeds, SVG, XML),
      // where a shadow root cannot be created. Skip the overlay quietly;
      // discovered sources still reach the popup and the background.
      overlayUnavailable = true;
      buttonHost = null;
      overlayRoot = null;
      return;
    }
    const style = document.createElement("style");
    style.textContent = [
      '.wrap{position:fixed;display:none;pointer-events:none;z-index:2147483647}',
      'button.tl-btn{pointer-events:auto;display:inline-flex;align-items:center;justify-content:center;gap:6px;height:32px;padding:0 12px;border:1px solid #1e293b;border-radius:999px;background:#0b1220;color:#e8f1fc;font:600 12px/1 system-ui,-apple-system,"Segoe UI",sans-serif;letter-spacing:.1px;box-shadow:0 4px 14px rgba(0,0,0,.35);cursor:pointer;transition:background .14s ease,border-color .14s ease}',
      'button.tl-btn:hover,button.tl-btn:focus-visible{background:#111c2e;border-color:#30d158;outline:none}',
      'button.tl-btn svg{width:14px;height:14px;flex:none}',
      'button.tl-btn:disabled{opacity:.55;cursor:wait}',
      '.message{display:none;position:absolute;top:38px;right:0;max-width:210px;padding:6px 8px;border:1px solid rgba(205,220,238,.2);border-radius:5px;background:rgba(17,25,35,.94);color:#e4edf8;font:10px/1.35 system-ui,-apple-system,"Segoe UI",sans-serif;white-space:normal;box-shadow:0 2px 10px rgba(0,0,0,.2)}',
      '.message.show{display:block}',
      '.panel{display:none;position:absolute;top:38px;right:0;width:300px;max-width:calc(100vw - 16px);max-height:min(420px,calc(100vh - 24px));overflow:hidden;padding:12px;border:1px solid #1e293b;border-radius:10px;background:#0b1220;box-shadow:0 8px 24px rgba(0,0,0,.45);pointer-events:auto}',
      '.panel.show{display:flex;flex-direction:column}',
      '.p-head,.p-progress,.p-connect{flex:none}',
      '.p-scroll{overflow-y:auto;min-height:0}',
      '.p-q{pointer-events:none;flex:none;padding:2px 6px;border:1px solid #1e293b;border-radius:4px;background:#0b1220;color:#aebdce;font:600 10px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-connect{pointer-events:auto;display:block;width:100%;min-height:30px;margin-top:10px;padding:0 12px;border:1px solid #30d158;border-radius:8px;background:transparent;color:#7ee2a0;font:600 12px/1.2 system-ui,-apple-system,"Segoe UI",sans-serif;cursor:pointer}',
      '.p-connect:hover{background:rgba(48,209,88,.15)}',
      '.p-connect:disabled{opacity:.5;cursor:wait}',
      '.p-note{display:none;margin-top:8px;color:#aebdce;font:10px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-note.show{display:block}',
      '.p-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:10px}',
      '.p-title{display:flex;align-items:center;gap:6px;min-width:0;color:#e8f1fc;font:600 12px/1.2 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-title svg{width:13px;height:13px;flex:none}',
      '.p-title-text{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
      '.p-close{pointer-events:auto;flex:none;width:22px;height:22px;padding:0;border:0;border-radius:5px;background:transparent;color:#8b9eb2;font:16px/1 system-ui,-apple-system,"Segoe UI",sans-serif;cursor:pointer}',
      '.p-close:hover{background:#1e293b;color:#e8f1fc}',
      '.p-qualities{display:grid;gap:6px}',
      '.p-subtitles{display:grid;gap:6px;margin-top:10px}',
      '.p-sub-head{color:#aebdce;font:600 11px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-row{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 10px;border:1px solid #1e293b;border-radius:6px;background:#111c2e}',
      '.p-row.best{border-color:#30d158}',
      '.p-best{flex:none;padding:1px 5px;border:1px solid #30d158;border-radius:4px;color:#7ee2a0;font:600 9px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-label{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#e4edf8;font:500 11px/1.3 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-dl{pointer-events:auto;flex:none;min-height:24px;padding:0 9px;border:1px solid #30d158;border-radius:5px;background:transparent;color:#7ee2a0;font:600 10px/1 system-ui,-apple-system,"Segoe UI",sans-serif;cursor:pointer}',
      '.p-dl:hover{background:rgba(48,209,88,.15)}',
      '.p-dl:disabled{opacity:.5;cursor:wait}',
      '.p-progress{margin-bottom:10px;padding:10px;border:1px solid #1e293b;border-radius:6px;background:#111c2e}',
      '.p-progress-title{color:#e4edf8;font:600 11px/1.3 system-ui,-apple-system,"Segoe UI",sans-serif;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
      '.p-progress-bar{height:6px;margin-top:8px;border-radius:3px;background:#1e293b;overflow:hidden}',
      '.p-progress-fill{height:100%;background:#30d158}',
      '.p-progress-text{margin-top:6px;color:#aebdce;font:10px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-empty{padding:8px;color:#8b9eb2;font:10px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}',
      '.p-other{margin-top:10px;border-top:1px solid #1e293b;padding-top:8px}',
      '.p-other summary{list-style:none;color:#aebdce;font:600 11px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif;cursor:pointer}',
      '.p-other summary::-webkit-details-marker{display:none}',
      '.p-other summary::before{content:"▸ ";color:#7ee2a0}',
      '.p-other[open] summary::before{content:"▾ "}',
      '.p-other-list{display:grid;gap:6px;margin-top:8px}',
      '.p-link{pointer-events:auto;display:block;width:100%;margin-top:8px;padding:0;border:0;background:transparent;color:#7ee2a0;font:500 11px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif;cursor:pointer;text-align:left;text-decoration:underline}',
      '@media(prefers-reduced-motion:reduce){button.tl-btn{transition:none}}'
    ].join("\n");
    const wrap = document.createElement("div");
    wrap.className = "wrap";
    button = document.createElement("button");
    button.type = "button";
    button.className = "tl-btn";
    button.title = "Download this video with ThunderLoad";
    button.setAttribute("aria-label", "Download this video with ThunderLoad");
    button.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M13 2 3 14h7l-1 7 10-12h-7l1-7z" fill="#3ce165"/></svg><span>Download this video</span>';
    status = document.createElement("div");
    status.className = "message";
    const panel = document.createElement("div");
    panel.className = "panel";
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      const target = activeVideo;
      if (!target) { showStatus("Move your pointer over a video first."); return; }
      const candidates = candidateSources(target);
      // Slice 3: on a YouTube/social page the sniffed DASH manifest
      // alone is not a usable result — yt-dlp formats are the primary
      // path, the manifest is only the fallback. Any other candidate
      // (a direct file on Instagram/TikTok, a real .mpd elsewhere)
      // still opens the normal panel.
      const hasRealCandidate = candidates.some((item) => !YOUTUBE_DASH_MANIFEST.test(item.url));
      if (isYtDlpPageUrl(location.href) && !hasRealCandidate) {
        openYtDlpPanel(target);
        return;
      }
      if (!candidates.length) {
        // Slice 1b: the panel still opens — with the reason plus the
        // "Connect desktop app" button — so the permission prompt is
        // reachable even when nothing is detected.
        openEmptyPanel(target);
      } else {
        // Best-ranked source first (DASH, then HLS, then Direct media);
        // the rest go in the panel's collapsed other-media section.
        const ranked = [...candidates].sort((a, b) => rankKind(a.kind) - rankKind(b.kind));
        openPanelFor(ranked[0], target, ranked.slice(1));
      }
    });
    wrap.append(button, status, panel);
    overlayRoot.append(style, wrap);
    document.documentElement.append(buttonHost);
    overlayRoot.wrap = wrap;
    wrap.addEventListener("pointerenter", () => { clearTimeout(hideTimer); clearTimeout(autoHideTimer); });
    // Leaving the pill restarts the auto-hide clock too: scheduleHide
    // bails out for a playing video, so without this the pill would
    // stay up forever after hovering it while the video plays.
    wrap.addEventListener("pointerleave", () => { scheduleHide(); scheduleAutoHide(); });
  }

  // Panel state: the source the panel is showing, the other
  // candidates on the page, and its inspected variants (null while
  // the inspect is in flight, [] when nothing downloadable exists).
  // panelInspectError carries the inspect failure text (e.g. the
  // login label for a gated manifest) so the panel can show it
  // instead of the generic empty text — Slice 5 fix: the panel used
  // to swallow the error and go dead-end.
  let panelPrimary = null;
  let panelOthers = [];
  let panelTarget = null;
  let panelVariants = null;
  let panelInspectError = null;

  // Slice 1b follow-up: the last connect result, re-applied whenever
  // the connect fragment is rebuilt — so an in-flight inspect
  // resolving after a click does not wipe the feedback.
  let lastConnectNote = "";

  // Pushes a connect result to the live note (if any) as well as
  // remembering it: covers a re-render landing mid-click, when the
  // note the handler closed over is already detached.
  function applyConnectNote(text) {
    lastConnectNote = text;
    const live = panelEl()?.querySelector(".p-note");
    if (live) {
      live.textContent = text;
      live.classList.add("show");
    }
  }

  function panelEl() {
    return overlayRoot?.querySelector(".panel") || null;
  }

  function showStatus(message) {
    if (!status) return;
    panelEl()?.classList.remove("show");
    status.textContent = message;
    status.classList.add("show");
    // The message lives in the pill wrap: keep the pill up while it
    // is shown, then restart the auto-hide clock.
    clearTimeout(autoHideTimer);
    clearTimeout(showStatus.timer);
    showStatus.timer = setTimeout(() => { status?.classList.remove("show"); scheduleAutoHide(); }, 3500);
  }

  function rankKind(kind) {
    if (kind === "DASH manifest") return 0;
    if (kind === "HLS playlist") return 1;
    return 2;
  }

  // Quality label from a real variant only: "{height}p" (+ " HD" at
  // 720p and up), "Audio" for audio-only streams, otherwise the
  // engine's own quality string. Slice 6: a live row is prefixed
  // "Live · ", and the frame rate rides next to the quality when the
  // source carries it — shown only when present, never guessed.
  function qualityLabel(variant) {
    const height = variant?.height || 0;
    let label;
    if (height > 0) label = height + "p" + (height >= 720 ? " HD" : "");
    else if (variant?.audioOnly || variant?.audio_only) label = "Audio";
    else label = variant?.quality || "Stream";
    if (variant?.isLive || variant?.is_live) label = "Live · " + label;
    if (variant?.fps) label += " · " + Math.round(variant.fps) + " fps";
    return label;
  }

  // Slice 6: subtitle tracks the engine surfaced for the first
  // variant (they ride on every variant). One row per language with
  // its own Download button — the format is what the source carries.
  function subtitleSection(variants, target) {
    const subtitles = variants?.[0]?.subtitles || [];
    if (!subtitles.length) return null;
    const section = document.createElement("div");
    section.className = "p-subtitles";
    const heading = document.createElement("div");
    heading.className = "p-sub-head";
    heading.textContent = "Subtitles";
    section.append(heading);
    for (const subtitle of subtitles) {
      const row = document.createElement("div");
      row.className = "p-row";
      const label = document.createElement("span");
      label.className = "p-label";
      label.textContent = subtitle.name || subtitle.language || "Subtitles";
      const download = document.createElement("button");
      download.type = "button";
      download.className = "p-dl";
      download.textContent = "Download";
      download.addEventListener("click", () => downloadSubtitle(subtitle, target));
      row.append(label, download);
      section.append(row);
    }
    return section;
  }

  // Slice 6: one subtitle download. An HLS subtitle playlist
  // (format "m3u8") goes to the host, which assembles it into one
  // .vtt; a single file (vtt/ttml/srt) downloads directly in the
  // browser. Success shows as a completed progress state, failure
  // as an error state — same surface as a video download.
  async function downloadSubtitle(subtitle, target) {
    if (!subtitle?.url || !target) return;
    panelDownloadState = { label: subtitle.name || subtitle.language || "Subtitles", percent: 0, phase: "preparing" };
    panelDownloadSource = subtitle.url;
    progressSection();
    try {
      if (subtitle.format === "m3u8") {
        const result = await api.runtime.sendMessage({
          type: "NATIVE_REQUEST",
          action: "hls.subtitles.download",
          payload: { playlistUrl: subtitle.url, url: subtitle.url, filename: sanitizeSubtitleFilename((subtitle.language || "subtitles") + ".vtt"), referer: location.href }
        });
        if (!result?.ok) throw new Error(result?.error || "Could not download subtitles.");
      } else {
        const result = await api.runtime.sendMessage({
          type: "BROWSER_DOWNLOAD",
          url: subtitle.url,
          filename: sanitizeSubtitleFilename((subtitle.language || "subtitles") + "." + (subtitle.format || "vtt"))
        });
        if (!result?.ok) throw new Error(result?.error || "Could not download subtitles.");
      }
      panelDownloadState = { label: subtitle.name || subtitle.language || "Subtitles", phase: "complete" };
      progressSection();
    } catch (error) {
      panelDownloadState = { label: subtitle.name || subtitle.language || "Subtitles", phase: "error", text: error?.message || String(error) };
      if (isPanelOpen()) progressSection();
    }
  }

  // Slice 6: a note when downloads intercepted while the desktop app
  // was down are still waiting for the handoff — the user should know
  // the file is queued, not lost. Appended after Connect; guarded
  // against duplicates across re-renders.
  function appendInterceptNote(panel) {
    if (!panel) return;
    api.runtime.sendMessage({ type: "GET_INTERCEPT_STATUS" }).then((result) => {
      const pending = Array.isArray(result?.pending) ? result.pending : [];
      if (!pending.length) return;
      if (!panel.isConnected || panel.querySelector(".p-intercept-note")) return;
      const note = document.createElement("div");
      note.className = "p-note show p-intercept-note";
      note.textContent = pending.length === 1
        ? "1 download is waiting for the desktop app."
        : pending.length + " downloads are waiting for the desktop app.";
      panel.append(note);
    }).catch(() => {});
  }

  // Slice 1b: a long (often gibberish) filename is shown short —
  // truncated with … — never the full hash. The full name stays in
  // the row's title attribute for hover.
  const OTHER_NAME_MAX = 26;
  function otherLabel(source) {
    try {
      const parsed = new URL(source.url);
      const name = decodeURIComponent(parsed.pathname.split("/").pop() || "");
      if (name) return name.length > OTHER_NAME_MAX ? name.slice(0, OTHER_NAME_MAX - 1) + "…" : name;
      return parsed.hostname;
    } catch (_) { return source.kind || "Media"; }
  }
  function otherFullName(source) {
    try {
      const parsed = new URL(source.url);
      const name = decodeURIComponent(parsed.pathname.split("/").pop() || "");
      return name || parsed.hostname;
    } catch (_) { return source.kind || "Media"; }
  }
  // Quality slot for an "Other media" row: exactly what the source
  // carries. A manifest source carries its format (DASH/HLS); a
  // direct file carries only its container — never a guessed
  // resolution.
  function otherQuality(source) {
    if (source.kind === "DASH manifest") return "DASH";
    if (source.kind === "HLS playlist") return "HLS";
    try {
      const ext = new URL(source.url).pathname.split("/").pop()?.split(".").pop() || "";
      if (/^[a-z0-9]{2,5}$/i.test(ext)) return ext.toUpperCase();
    } catch (_) {}
    return source.kind || "Media";
  }

  function variantRow(variant, target, isBest) {
    const row = document.createElement("div");
    row.className = "p-row" + (isBest ? " best" : "");
    const label = document.createElement("span");
    label.className = "p-label";
    label.textContent = qualityLabel(variant);
    const download = document.createElement("button");
    download.type = "button";
    download.className = "p-dl";
    download.textContent = "Download";
    download.addEventListener("click", () => startDownload({ ...panelPrimary, selectedVariant: variant }, target));
    row.append(label);
    if (isBest) {
      const best = document.createElement("span");
      best.className = "p-best";
      best.textContent = "Best";
      row.append(best);
    }
    row.append(download);
    return row;
  }

  // Direct media has no qualities to list — one row for the file
  // itself: its short name, its container, and a Download button.
  function directRow(source, target) {
    const row = document.createElement("div");
    row.className = "p-row";
    const label = document.createElement("span");
    label.className = "p-label";
    label.textContent = otherLabel(source);
    label.title = otherFullName(source);
    const quality = document.createElement("span");
    quality.className = "p-q";
    quality.textContent = otherQuality(source);
    const download = document.createElement("button");
    download.type = "button";
    download.className = "p-dl";
    download.textContent = "Download";
    download.addEventListener("click", () => startDownload(source, target));
    row.append(label, quality, download);
    return row;
  }

  // Shared panel header: the video/page title at the top (Slice 5,
  // IDM-style) + a clean close control. The title truncates with an
  // ellipsis — long page titles never push the panel wider. Closing
  // the panel restarts the pill's auto-hide clock.
  function pageTitle() {
    const title = (document.title || "").trim();
    return title || "Download this video";
  }

  function panelHead(titleText) {
    const head = document.createElement("div");
    head.className = "p-head";
    const title = document.createElement("span");
    title.className = "p-title";
    const bolt = document.createElement("span");
    bolt.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M13 2 3 14h7l-1 7 10-12h-7l1-7z" fill="#3ce165"/></svg>';
    const text = document.createElement("span");
    text.className = "p-title-text";
    text.textContent = titleText || pageTitle();
    title.append(bolt, text);
    const close = document.createElement("button");
    close.type = "button";
    close.className = "p-close";
    close.textContent = "×";
    close.setAttribute("aria-label", "Close");
    close.addEventListener("click", () => {
      panelEl()?.classList.remove("show");
      // Slice 5 fix: a finished download's state never survives the
      // panel being closed — reopening starts clean.
      if (panelDownloadState?.phase === "complete" || panelDownloadState?.phase === "error") {
        panelDownloadState = null;
        panelDownloadJobId = null;
        panelDownloadSource = null;
      }
      scheduleAutoHide();
    });
    head.append(title, close);
    return head;
  }

  // Slice 5: live in-panel progress for a download started from
  // this panel. The background returns the native jobId at start;
  // the native host then reports download.progress/complete/error
  // events with the same jobId, forwarded as NATIVE_EVENTs. The
  // panel only ever tracks its own download.
  let panelDownloadJobId = null;
  let panelDownloadState = null; // { label, percent, downloaded, total, phase, text } | null
  let panelDownloadSource = null; // URL of the download the panel is tracking, or null

  // Slice 5 fix: progress state belongs to one download on one
  // source. A terminal state (complete/error) never carries over to
  // the next panel open, and an in-flight download's progress never
  // follows the panel to a different video — reopening the panel for
  // the same source keeps its live progress.
  function resetPanelDownloadStateFor(sourceUrl) {
    if (!panelDownloadState) {
      panelDownloadJobId = null;
      panelDownloadSource = null;
      return;
    }
    const terminal = panelDownloadState.phase === "complete" || panelDownloadState.phase === "error";
    if (terminal || panelDownloadSource !== sourceUrl) {
      panelDownloadState = null;
      panelDownloadJobId = null;
      panelDownloadSource = null;
    }
  }

  function formatBytes(bytes) {
    if (typeof bytes !== "number" || !Number.isFinite(bytes) || bytes < 0) return "";
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
    return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " GB";
  }

  function progressText(state) {
    if (state.phase === "error") return state.text || "Download failed.";
    if (state.phase === "complete") return "Download complete";
    if (state.phase === "preparing") return "Preparing download…";
    const percent = typeof state.percent === "number" ? Math.max(0, Math.min(100, Math.round(state.percent))) : null;
    const downloaded = formatBytes(state.downloaded);
    const total = formatBytes(state.total);
    if (percent !== null && downloaded && total) return percent + "% · " + downloaded + " of " + total + " · Downloading…";
    if (percent !== null && downloaded) return percent + "% · " + downloaded + " · Downloading…";
    if (percent !== null) return percent + "% · Downloading…";
    return "Downloading…";
  }

  // Creates (or updates, or removes) the progress section directly
  // under the panel header. Called after every render — renderPanel
  // and renderYtDlpPanel both replaceChildren() — and on every
  // download event, so the bar stays live without rebuilding the
  // whole panel. Returns the section, or null when no download is
  // in flight for this panel.
  function progressSection() {
    const panel = panelEl();
    if (!panel) return null;
    let section = panel.querySelector(".p-progress");
    if (!panelDownloadState) {
      if (section) section.remove();
      return null;
    }
    if (!section) {
      section = document.createElement("div");
      section.className = "p-progress";
      const title = document.createElement("div");
      title.className = "p-progress-title";
      const bar = document.createElement("div");
      bar.className = "p-progress-bar";
      const fill = document.createElement("div");
      fill.className = "p-progress-fill";
      bar.append(fill);
      const text = document.createElement("div");
      text.className = "p-progress-text";
      section.append(title, bar, text);
      const head = panel.querySelector(".p-head");
      if (head && head.nextSibling) panel.insertBefore(section, head.nextSibling);
      else panel.append(section);
    }
    section.querySelector(".p-progress-title").textContent = panelDownloadState.label || "Download";
    const percent = typeof panelDownloadState.percent === "number"
      ? Math.max(0, Math.min(100, Math.round(panelDownloadState.percent))) : 0;
    section.querySelector(".p-progress-fill").style.width = percent + "%";
    section.querySelector(".p-progress-text").textContent = progressText(panelDownloadState);
    return section;
  }

  // Slice 1b: the permission action, reachable in both panel
  // states (media found and no media found). The note below it
  // reports the result — granted, denied, or the manual steps — and
  // stays in the panel so the user can retry or follow the steps.
  // Returns a fragment (button + note) for the caller to append.
  // onGranted (optional) runs after a successful grant only — e.g.
  // to re-run the inspect that failed for lack of the permission,
  // so the panel re-renders from the fresh result instead of
  // leaving the stale error on screen next to the "connected"
  // note. A denied or failed grant never runs it: the manual
  // steps stay, unchanged.
  function connectButton(onGranted) {
    const connect = document.createElement("button");
    connect.type = "button";
    connect.className = "p-connect";
    connect.textContent = "Connect desktop app";
    const note = document.createElement("div");
    note.className = "p-note";
    if (lastConnectNote) {
      note.textContent = lastConnectNote;
      note.classList.add("show");
    }
    connect.addEventListener("click", async () => {
      connect.disabled = true;
      note.classList.remove("show");
      try {
        const result = await api.runtime.sendMessage({ type: "REQUEST_NATIVE_PERMISSION" });
        if (result?.granted) {
          applyConnectNote("Desktop app connected. You can download now.");
          if (typeof onGranted === "function") onGranted();
        } else {
          applyConnectNote(MANUAL_PERMISSION_STEPS);
        }
      } catch (_) {
        applyConnectNote(MANUAL_PERMISSION_STEPS);
      } finally {
        note.classList.add("show");
        connect.disabled = false;
      }
    });
    const frag = document.createDocumentFragment();
    frag.append(connect, note);
    return frag;
  }

  // Slice 1b: the no-media panel. The pill click used to show only
  // a status message here, which left the permission prompt
  // unreachable — the popup has no Download button when nothing is
  // detected, so the prompt could never appear. The panel opens
  // instead, with the reason plus the connect button.
  function openEmptyPanel(target) {
    panelPrimary = null;
    panelTarget = target;
    panelOthers = [];
    panelVariants = [];
    panelInspectError = null;
    resetPanelDownloadStateFor(null);
    const panel = panelEl();
    if (!panel) return;
    panel.replaceChildren();
    panel.append(panelHead());
    const empty = document.createElement("div");
    empty.className = "p-empty";
    empty.textContent = "No downloadable media URL is exposed by this player. DRM or protected streams may not be accessible.";
    panel.append(empty);
    panel.append(connectButton());
    appendInterceptNote(panel);
    status?.classList.remove("show");
    panel.classList.add("show");
    clearTimeout(autoHideTimer);
    // Slice 6: GenericIE fallback — nothing was detected, so ask
    // yt-dlp's generic extractor what the page itself exposes. If it
    // finds formats the panel becomes the formats panel; if not (or
    // the user closed the panel meanwhile) the empty panel stays.
    tryGenericInspect(target);
  }

  async function tryGenericInspect(target) {
    let result;
    try {
      result = await api.runtime.sendMessage({ type: "YTDLP_INSPECT_GENERIC", pageUrl: location.href });
    } catch (_) { return; }
    if (panelTarget !== target || panelPrimary !== null || !panelEl()?.classList.contains("show")) return;
    if (!result?.ok) return;
    const variants = (result.variants || []).filter((item) => item?.downloadable && item?.url);
    if (!variants.length) return;
    variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
    panelYtDlp = { variants, title: result.title || "", generic: true };
    renderYtDlpPanel();
  }

  // Slice 3: yt-dlp panel state. null = inspect in flight;
  // { variants, title } after a successful inspect;
  // { toolsMissing } when yt-dlp/Deno is not installed yet;
  // { error } when the inspect failed.
  let panelYtDlp = null;
  let ytdlpDenoMissing = false;
  let ytdlpToolProgressText = "";

  function ytdlpVariantRow(variant, target, isBest) {
    const row = document.createElement("div");
    row.className = "p-row" + (isBest ? " best" : "");
    const label = document.createElement("span");
    label.className = "p-label";
    label.textContent = qualityLabel(variant);
    const download = document.createElement("button");
    download.type = "button";
    download.className = "p-dl";
    download.textContent = "Download";
    download.addEventListener("click", () => startYtDlpDownload(variant, target));
    row.append(label);
    if (isBest) {
      const best = document.createElement("span");
      best.className = "p-best";
      best.textContent = "Best";
      row.append(best);
    }
    row.append(download);
    return row;
  }

  // One named tool-download button (consent + progress, FINAL
  // requirement): the button names the tool ("Download yt-dlp" /
  // "Download Deno"), the progress line names it too ("yt-dlp
  // download ho raha hai — 45%") — never a generic label
  // label anywhere. The tap itself is the consent: nothing is
  // fetched until the user presses this button. The manual link
  // below it is only the backup.
  function ytdlpToolDownloadFragment(tool, onDone) {
    const toolId = normalizeMissingTool(tool) || "ytdlp";
    const frag = document.createDocumentFragment();
    const name = ytdlpDisplayName(toolId);
    const downloadBtn = document.createElement("button");
    downloadBtn.type = "button";
    downloadBtn.className = "p-connect";
    downloadBtn.textContent = "Download " + name;
    const progress = document.createElement("div");
    progress.className = "p-note p-tool-progress";
    if (ytdlpToolProgressText) {
      progress.textContent = ytdlpToolProgressText;
      progress.classList.add("show");
    }
    const note = document.createElement("div");
    note.className = "p-note";
    downloadBtn.addEventListener("click", async () => {
      downloadBtn.disabled = true;
      note.classList.remove("show");
      try {
        const result = await api.runtime.sendMessage({ type: "YTDLP_ENSURE_TOOL", tool: toolId });
        if (result?.ok) {
          if (typeof onDone === "function") onDone();
        } else {
          note.textContent = result?.error || "Download failed.";
          note.classList.add("show");
          downloadBtn.disabled = false;
        }
      } catch (error) {
        note.textContent = error?.message || String(error);
        note.classList.add("show");
        downloadBtn.disabled = false;
      }
    });
    const manual = document.createElement("button");
    manual.type = "button";
    manual.className = "p-link";
    manual.textContent = "or download it yourself";
    manual.addEventListener("click", () => {
      api.runtime.sendMessage({ type: "YTDLP_OPEN_MANUAL_PAGE", tool: toolId }).catch(() => {});
    });
    frag.append(downloadBtn, progress, note, manual);
    return frag;
  }

  function renderYtDlpPanel() {
    const panel = panelEl();
    if (!panel) return;
    panel.replaceChildren();
    // Slice 5: the video title at the top, the quality list as the
    // primary rows (best first, one Download per row), live progress
    // under the header, Connect at the bottom. The layout matches
    // thunderload/extension/panel-mockup.html.
    panel.append(panelHead(panelYtDlp?.title || undefined));
    progressSection();
    if (panelYtDlp === null) {
      const loading = document.createElement("div");
      loading.className = "p-empty";
      loading.textContent = "Getting formats…";
      panel.append(loading);
    } else if (panelYtDlp.toolsMissing) {
      const name = ytdlpDisplayName(panelYtDlp.toolsMissing);
      const empty = document.createElement("div");
      empty.className = "p-empty";
      empty.textContent = name + " is not installed yet. Download it to get formats from this page.";
      panel.append(empty);
      panel.append(ytdlpToolDownloadFragment(panelYtDlp.toolsMissing, () => {
        if (panelTarget) openYtDlpPanel(panelTarget);
      }));
    } else if (panelYtDlp.error) {
      const empty = document.createElement("div");
      empty.className = "p-empty";
      empty.textContent = panelYtDlp.error;
      panel.append(empty);
      // A successful grant re-runs the inspect for this panel — the
      // same onDone pattern as the tool-download fragment — so the
      // stale permission error clears and the formats load. The
      // "connected" note survives the re-render via lastConnectNote.
      panel.append(connectButton(() => {
        if (panelTarget) openYtDlpPanel(panelTarget);
      }));
    } else {
      const variants = panelYtDlp.variants || [];
      const scroll = document.createElement("div");
      scroll.className = "p-scroll";
      if (variants.length) {
        const list = document.createElement("div");
        list.className = "p-qualities";
        for (const [index, variant] of variants.entries()) list.append(ytdlpVariantRow(variant, panelTarget, index === 0));
        scroll.append(list);
        // Slice 6: subtitle tracks ride on the first variant.
        const subs = subtitleSection(variants, panelTarget);
        if (subs) scroll.append(subs);
      } else {
        const empty = document.createElement("div");
        empty.className = "p-empty";
        empty.textContent = "No downloadable formats found for this page.";
        scroll.append(empty);
      }
      if (ytdlpDenoMissing) {
        const note = document.createElement("div");
        note.className = "p-empty";
        note.textContent = "Deno is not installed — YouTube shows fewer formats without it.";
        scroll.append(note);
        scroll.append(ytdlpToolDownloadFragment("deno", () => {
          ytdlpDenoMissing = false;
          renderYtDlpPanel();
        }));
      }
      if (scroll.childElementCount) panel.append(scroll);
      panel.append(connectButton());
    }
    appendInterceptNote(panel);
    status?.classList.remove("show");
    panel.classList.add("show");
    clearTimeout(autoHideTimer);
  }

  function openYtDlpPanel(target) {
    panelPrimary = null;
    panelTarget = target;
    panelOthers = [];
    panelVariants = null;
    panelInspectError = null;
    resetPanelDownloadStateFor(location.href);
    panelYtDlp = null;
    ytdlpDenoMissing = false;
    ytdlpToolProgressText = "";
    renderYtDlpPanel();
    (async () => {
      try {
        const result = await api.runtime.sendMessage({ type: "YTDLP_INSPECT", pageUrl: location.href });
        if (panelTarget !== target || !panelEl()?.classList.contains("show")) return;
        if (!result?.ok) {
          const missingTool = normalizeMissingTool(result?.toolsMissing);
          panelYtDlp = missingTool
            ? { toolsMissing: missingTool }
            : { error: result?.error || "Could not inspect this page." };
          // Slice 6 fix (MINOR 1): the normal inspect found nothing
          // usable — try the GenericIE fallback before settling on
          // the error state. toolsMissing is the exception: generic
          // needs the same yt-dlp binary, so it cannot run either.
          if (!missingTool) tryGenericInspect(target);
        } else {
          const variants = (result.variants || []).filter((item) => item?.downloadable && item?.url);
          variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
          panelYtDlp = { variants, title: result.title || "" };
          if (!variants.length) {
            // Slice 6 fix (MINOR 1): no usable formats from the normal
            // inspect — try the GenericIE fallback.
            tryGenericInspect(target);
          } else {
            // Deno check: after a successful inspect, a missing Deno
            // means YouTube fell back to fewer formats — offer the
            // download right here instead of failing silently.
            try {
              const toolStatus = await api.runtime.sendMessage({ type: "YTDLP_TOOLS_STATUS" });
              if (toolStatus?.ok && toolStatus.deno && toolStatus.deno.present === false) ytdlpDenoMissing = true;
            } catch (_) { /* Tools status is best-effort here. */ }
          }
        }
        if (panelEl()?.classList.contains("show")) renderYtDlpPanel();
      } catch (_) {
        if (panelTarget !== target) return;
        panelYtDlp = { error: "Could not inspect this page." };
        // Slice 6 fix (MINOR 1): a failed inspect is not the end —
        // try the GenericIE fallback before settling on the error.
        tryGenericInspect(target);
        if (panelEl()?.classList.contains("show")) renderYtDlpPanel();
      }
    })();
  }

  async function startYtDlpDownload(variant, target) {
    if (!variant || !target || !button) return;
    // Slice 5: the panel stays open with live progress instead of
    // closing to a status toast. The start response carries the
    // native jobId; download.progress/complete/error events for that
    // jobId update the section (see the NATIVE_EVENT handler).
    panelDownloadJobId = null;
    panelDownloadState = { label: qualityLabel(variant), percent: 0, phase: "preparing" };
    panelDownloadSource = location.href;
    progressSection();
    button.disabled = true;
    try {
      const formatSelector = variant.audioRepresentationId
        ? variant.representationId + "+" + variant.audioRepresentationId
        : variant.representationId;
      const filename = panelYtDlp?.title ? panelYtDlp.title + ".mp4" : "video.mp4";
      const result = await api.runtime.sendMessage({
        type: "YTDLP_DOWNLOAD",
        pageUrl: location.href,
        formatSelector,
        filename
      });
      if (!result?.ok) throw new Error(result?.error || "Could not start the download.");
      panelDownloadJobId = result.jobId || null;
      if (!panelDownloadJobId) {
        // No jobId to track (older host): fall back to the toast.
        panelDownloadState = null;
        progressSection();
        showStatus(result.message || "Download added.");
      }
    } catch (error) {
      panelDownloadState = { label: qualityLabel(variant), phase: "error", text: error?.message || String(error) };
      if (isPanelOpen()) progressSection();
      else showStatus(error?.message || String(error));
    } finally {
      button.disabled = false;
    }
  }

  function renderPanel() {
    const panel = panelEl();
    if (!panel || !panelPrimary) return;
    panel.replaceChildren();
    // Slice 5: the video/page title at the top, the quality list as
    // the primary rows (best first, one Download per row), live
    // progress under the header, Connect at the bottom. Header +
    // progress + connect stay visible; only the lists scroll, inside
    // the panel, so the panel fits one viewport. The layout matches
    // thunderload/extension/panel-mockup.html.
    panel.append(panelHead());
    progressSection();
    const scroll = document.createElement("div");
    scroll.className = "p-scroll";
    if (panelPrimary.kind === "DASH manifest" || panelPrimary.kind === "HLS playlist") {
      const list = document.createElement("div");
      list.className = "p-qualities";
      if (panelVariants === null) {
        const loading = document.createElement("div");
        loading.className = "p-empty";
        loading.textContent = "Checking qualities…";
        list.append(loading);
      } else if (!panelVariants.length) {
        // Slice 5 fix: this manifest WAS detected — a failed or
        // empty inspect is not "no media". Show the inspect error
        // (e.g. the login label for a gated manifest) and offer
        // "Download anyway": the old primary's fallback, starting
        // the detected stream directly. The download path inspects
        // again, and a failure there surfaces via the panel's error
        // event, which already shows the label.
        const empty = document.createElement("div");
        empty.className = "p-empty";
        empty.textContent = panelInspectError || "No downloadable qualities are exposed.";
        list.append(empty);
        const fallback = document.createElement("button");
        fallback.type = "button";
        fallback.className = "p-connect";
        fallback.textContent = "Download anyway";
        fallback.addEventListener("click", () => startDownload(panelPrimary, panelTarget));
        list.append(fallback);
      } else {
        for (const [index, variant] of panelVariants.entries()) list.append(variantRow(variant, panelTarget, index === 0));
      }
      scroll.append(list);
      // Slice 6: subtitle tracks ride on the first variant.
      const subs = subtitleSection(panelVariants, panelTarget);
      if (subs) scroll.append(subs);
    } else {
      // Direct media has no qualities to list — one row for the file
      // itself: its short name, its container, and a Download button.
      const list = document.createElement("div");
      list.className = "p-qualities";
      list.append(directRow(panelPrimary, panelTarget));
      scroll.append(list);
    }
    if (panelOthers.length) {
      const details = document.createElement("details");
      details.className = "p-other";
      const summary = document.createElement("summary");
      summary.textContent = "Other media on this page (" + panelOthers.length + ")";
      details.append(summary);
      const otherList = document.createElement("div");
      otherList.className = "p-other-list";
      for (const source of panelOthers) {
        const row = document.createElement("div");
        row.className = "p-row";
        const label = document.createElement("span");
        label.className = "p-label";
        label.textContent = otherLabel(source);
        label.title = otherFullName(source);
        const quality = document.createElement("span");
        quality.className = "p-q";
        quality.textContent = otherQuality(source);
        const download = document.createElement("button");
        download.type = "button";
        download.className = "p-dl";
        download.textContent = "Inspect";
        download.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          // Switching to another source re-opens the panel for it.
          openPanelFor(source, panelTarget, [panelPrimary, ...panelOthers.filter((item) => item !== source)]);
        });
        row.append(label, quality, download);
        otherList.append(row);
      }
      details.append(otherList);
      scroll.append(details);
    }
    if (scroll.childElementCount) panel.append(scroll);
    // Same stale-error pattern as the yt-dlp panel: a manifest
    // inspect that failed for lack of the permission leaves its
    // error text here, so a successful grant re-runs that inspect —
    // only when an inspect error is actually showing.
    panel.append(connectButton(() => {
      if (panelInspectError && panelPrimary && panelTarget) inspectPanelVariants(panelPrimary, panelTarget);
    }));
    appendInterceptNote(panel);
    status?.classList.remove("show");
    panel.classList.add("show");
    clearTimeout(autoHideTimer);
  }

  function openPanelFor(source, target, others) {
    panelPrimary = source;
    panelTarget = target;
    panelOthers = others || [];
    panelVariants = null;
    panelInspectError = null;
    resetPanelDownloadStateFor(source.url);
    renderPanel();
    if (source.kind === "Direct media") return;
    inspectPanelVariants(source, target);
  }

  async function inspectPanelVariants(source, target) {
    try {
      const result = await api.runtime.sendMessage({
        type: "INSPECT_MEDIA",
        url: source.url,
        pageUrl: location.href
      });
      // The user may have switched sources while inspecting.
      if (panelPrimary !== source) return;
      const variants = (result?.variants || []).filter((item) => item?.downloadable && item?.url);
      if (!result?.ok || !variants.length) {
        // Slice 5 fix: the inspect failed (or returned no usable
        // qualities) for a manifest we DID detect. Keep the error
        // text so renderPanel can show it with the "Download
        // anyway" fallback instead of the generic empty text.
        // Do not re-open the panel if the user already closed it.
        panelVariants = [];
        panelInspectError = !result?.ok ? (result?.error || "Could not inspect this stream.") : null;
        if (panelEl()?.classList.contains("show")) renderPanel();
        return;
      }
      panelInspectError = null;
      variants.sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bandwidth || 0) - (a.bandwidth || 0));
      panelVariants = variants;
      if (panelEl()?.classList.contains("show")) renderPanel();
    } catch (error) {
      if (panelPrimary !== source) return;
      panelVariants = [];
      panelInspectError = error?.message || "Could not inspect this stream.";
      if (panelEl()?.classList.contains("show")) renderPanel();
    }
  }

  async function startDownload(source, target) {
    if (!source || !target || !button) return;
    // Slice 5: the panel stays open with live progress instead of
    // closing to a status toast (same shape as startYtDlpDownload).
    const label = source.selectedVariant
      ? qualityLabel(source.selectedVariant)
      : source.kind === "Direct media" ? otherLabel(source) : source.kind || "Download";
    panelDownloadJobId = null;
    panelDownloadState = { label, percent: 0, phase: "preparing" };
    panelDownloadSource = source.url;
    progressSection();
    button.disabled = true;
    try {
      const result = await api.runtime.sendMessage({
        type: "QUICK_DOWNLOAD_MEDIA",
        url: source.url,
        kind: source.kind,
        selectedVariant: source.selectedVariant || null,
        pageUrl: location.href,
        filename: target.getAttribute("data-filename") || ""
      });
      if (!result?.ok) throw new Error(result?.error || "Could not start the download.");
      panelDownloadJobId = result.jobId || null;
      if (!panelDownloadJobId) {
        // No jobId to track (older host): fall back to the toast.
        panelDownloadState = null;
        progressSection();
        showStatus(result.message || "Download added.");
      }
    } catch (error) {
      panelDownloadState = { label, phase: "error", text: error?.message || String(error) };
      if (isPanelOpen()) progressSection();
      else showStatus(error?.message || String(error));
    } finally {
      button.disabled = false;
    }
  }

  function positionOverlay(video) {
    if (!buttonHost || !overlayRoot?.wrap || !video?.isConnected) return;
    const rect = video.getBoundingClientRect();
    if (rect.width < 64 || rect.height < 48 || rect.bottom < 0 || rect.top > innerHeight) {
      overlayRoot.wrap.style.display = "none";
      return;
    }
    const right = Math.min(innerWidth - 8, rect.right - 10);
    const top = Math.max(8, rect.top + 10);
    overlayRoot.wrap.style.display = "block";
    overlayRoot.wrap.style.left = "auto";
    overlayRoot.wrap.style.right = Math.max(8, innerWidth - right) + "px";
    overlayRoot.wrap.style.top = top + "px";
  }

  function isPanelOpen() {
    return !!panelEl()?.classList.contains("show");
  }
  // Slice 1b: the pill hides a few seconds after appearing and
  // reappears on cursor-over-video; the clock restarts on mouse
  // movement (show() and the pointermove handler both reschedule).
  // The pill never hides while the download panel is open.
  function scheduleAutoHide() {
    clearTimeout(autoHideTimer);
    autoHideTimer = setTimeout(() => {
      if (isPanelOpen()) return;
      if (overlayRoot?.wrap) overlayRoot.wrap.style.display = "none";
      activeVideo = null;
    }, PILL_AUTO_HIDE_MS);
  }

  function scheduleHide() {
    clearTimeout(hideTimer);
    hideTimer = setTimeout(() => {
      // A playing video keeps its button: the control is for the
      // video being watched, not only for the hovered one.
      if (activeVideo && !activeVideo.paused) return;
      // Slice 1b: never hide under an open panel.
      if (isPanelOpen()) return;
      if (overlayRoot?.wrap) overlayRoot.wrap.style.display = "none";
      activeVideo = null;
    }, 140);
  }

  function attachVideo(video, index) {
    if (video.dataset.thunderLoadAttached === "1") return;
    video.dataset.thunderLoadAttached = "1";
    const show = () => {
      const source = sourceFor(video);
      if (source) add(source.url, "Video " + (index + 1));
      activeVideo = video;
      ensureOverlay();
      clearTimeout(hideTimer);
      positionOverlay(video);
      scheduleAutoHide();
    };
    video.addEventListener("pointerenter", show, { passive: true });
    video.addEventListener("pointermove", () => {
      if (activeVideo === video) { positionOverlay(video); scheduleAutoHide(); }
      else if (video.matches(":hover")) show();
    }, { passive: true });
    video.addEventListener("loadedmetadata", () => {
      const source = sourceFor(video);
      if (source) add(source.url, "Video " + (index + 1));
    }, { passive: true });
    video.addEventListener("play", () => {
      const source = sourceFor(video);
      if (source) add(source.url, "Video " + (index + 1));
      // A playing video shows its button even before any hover.
      show();
    }, { passive: true });
    video.addEventListener("pause", () => {
      if (activeVideo === video) scheduleHide();
    }, { passive: true });
    video.addEventListener("pointerleave", scheduleHide, { passive: true });
    const source = sourceFor(video);
    if (source) add(source.url, "Video " + (index + 1));
  }

  function scan() {
    // Recompute truncation from the current DOM on every scan (R1):
    // a page that shrank below both caps must stop reporting truncated.
    // The recompute at the end uses the distinct URLs this scan saw,
    // not `found` (which is accumulative), so a repeat scan of an
    // unchanged overflowing page still reports truncated.
    truncated = false;
    truncatedNotified = false;
    currentScanUrls = new Set();
    let anchorsOverCap = false;
    const videos = Array.from(document.querySelectorAll("video"));
    videos.forEach(attachVideo);
    // Count video sources for this scan even when attachVideo
    // early-returns for already-attached videos (R1 follow-up):
    // add() dedups against `found`, so re-adding is free, and the
    // per-scan set is what the truncation recompute reads.
    videos.forEach((video, index) => {
      const source = sourceFor(video);
      if (source) add(source.url, "Video " + (index + 1));
    });
    // Audio elements expose their sources the same way videos do, but
    // get no hover overlay — their entries go straight to the popup.
    Array.from(document.querySelectorAll("audio")).forEach((audio, audioIndex) => {
      const urls = [
        audio.currentSrc,
        audio.src,
        ...Array.from(audio.querySelectorAll("source")).map((node) => node.src)
      ];
      urls.forEach((url) => {
        if (classify(url)) add(url, "Audio " + (audioIndex + 1));
      });
    });
    // Same-page media links: the grabber's main new source. Only the
    // first 500 anchors are read so a link-heavy page stays cheap; the
    // 100-item cap does the rest. A page with more than 500 anchors
    // counts as truncated too, so the popup says the list is partial.
    // data:/blob: links never classify.
    const links = Array.from(document.querySelectorAll("a[href]"));
    if (links.length > 500) anchorsOverCap = true;
    for (const link of links.slice(0, 500)) {
      const href = link.href;
      if (classify(href)) {
        let name = href;
        try { name = decodeURIComponent(new URL(href).pathname.split("/").pop() || href); } catch (_) {}
        add(href, "Link · " + name);
      }
    }
    // Include manifest and direct media resources for the extension popup.
    for (const entry of performance.getEntriesByType("resource").slice(-700)) {
      const kind = classify(entry.name);
      if (kind) add(entry.name, kind);
    }
    // Final word on truncation for this scan: more distinct media than
    // the cap, or more anchors than the anchor bound. A mid-scan notify
    // from add() may already have fired; the guard dedups it.
    truncated = currentScanUrls.size > MAX_FOUND || anchorsOverCap;
    currentScanUrls = null;
    if (truncated) notifyTruncated();
    if (activeVideo && !activeVideo.matches(":hover")) scheduleHide();
    return found.size;
  }

  function scheduleScan() {
    if (scanScheduled) return;
    scanScheduled = true;
    requestAnimationFrame(() => {
      scanScheduled = false;
      scan();
    });
  }

  api.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message?.type === "SCAN_NOW") {
      sendResponse({ ok: true, count: scan(), truncated });
      return false;
    }
    if (message?.type === "GET_SCANNED_MEDIA") {
      sendResponse({ sources: Array.from(found.values()), truncated });
      return false;
    }
    // Slice 3: tool-download progress forwarded by the background
    // (NATIVE_EVENT from the native host's ytdlp.tool_progress).
    // The line names the tool, per the FINAL requirement.
    if (message?.type === "NATIVE_EVENT" && message.event?.event === "ytdlp.tool_progress") {
      const tool = ytdlpDisplayName(message.event.tool === "deno" ? "deno" : "ytdlp");
      const percent = message.event.percent;
      ytdlpToolProgressText = tool + " download ho raha hai"
        + (typeof percent === "number" ? " — " + Math.round(percent) + "%" : "…");
      const live = panelEl()?.querySelector(".p-tool-progress");
      if (live) {
        live.textContent = ytdlpToolProgressText;
        live.classList.add("show");
      }
      return false;
    }
    // Slice 5: live in-panel progress for a download started from
    // this panel. The host reports download.progress/complete/error
    // with the native jobId; only the matching download updates
    // this panel, and only while it is open. A completed or failed
    // download stops tracking so a later download starts clean.
    if (message?.type === "NATIVE_EVENT" && panelDownloadJobId && message.event?.jobId === panelDownloadJobId) {
      const event = message.event;
      if (event.event === "download.progress" && event.progress) {
        const progress = event.progress;
        panelDownloadState = {
          label: panelDownloadState?.label || "Download",
          percent: typeof progress.percent === "number" ? progress.percent : null,
          downloaded: typeof progress.downloaded === "number" ? progress.downloaded : null,
          total: typeof progress.total === "number" ? progress.total : null,
          phase: "downloading"
        };
        if (isPanelOpen()) progressSection();
      } else if (event.event === "download.complete") {
        panelDownloadState = { ...(panelDownloadState || { label: "Download" }), phase: "complete", percent: 100 };
        if (isPanelOpen()) progressSection();
        panelDownloadJobId = null;
      } else if (event.event === "download.error") {
        panelDownloadState = { ...(panelDownloadState || { label: "Download" }), phase: "error", text: event.error || "Download failed." };
        if (isPanelOpen()) progressSection();
        panelDownloadJobId = null;
      }
      return false;
    }
    return false;
  });

  const observer = new MutationObserver(scheduleScan);
  observer.observe(document.documentElement || document, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ["src"]
  });
  try {
    const resourceObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        const kind = classify(entry.name);
        if (kind) add(entry.name, kind);
      }
      const hovered = activeVideo || Array.from(document.querySelectorAll("video")).find((video) => video.matches(":hover"));
      if (hovered && !activeVideo) {
        const source = sourceFor(hovered);
        if (source) add(source.url, "Video");
        activeVideo = hovered;
        ensureOverlay();
        clearTimeout(hideTimer);
        positionOverlay(hovered);
        scheduleAutoHide();
      }
    });
    resourceObserver.observe({ type: "resource", buffered: true });
  } catch (_) {
    // Resource observation is optional; direct video URLs remain detectable.
  }
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") scheduleScan();
  });
  window.addEventListener("scroll", () => {
    if (activeVideo) positionOverlay(activeVideo);
  }, { passive: true });
  window.addEventListener("resize", () => {
    if (activeVideo) positionOverlay(activeVideo);
  }, { passive: true });
  // Slice 2: MAIN-world hook bridge. The hook (hook.js, world MAIN,
  // document_start) sees MediaSource/blob/fetch URLs the isolated
  // world cannot. Findings arrive via window.postMessage guarded by
  // a per-page random token the hook announces in a HELLO handshake.
  // The token is NOT a secret from the page (page scripts can read
  // postMessage traffic) — it stops accidental collisions and casual
  // spoofing. Every finding is re-validated here (source + token +
  // payload shape + this world's own classifier), and segments never
  // become sources.
  let hookToken = null;
  let hookHelloReceived = false;
  // Slice 3 MINOR (a): the HELLO must echo the nonce of the latest
  // PING. The first HELLO no longer wins unconditionally — a hostile
  // page that answers the first PING before the real hook is undone
  // by the next re-PING, when the real hook echoes the fresh nonce
  // and the bridge re-latches. This cannot authenticate the hook
  // (postMessage traffic is readable by the page), so it bounds the
  // denial-of-service to a page that actively races every re-PING;
  // findings are still re-classified here and downloads still go
  // through the native host's inspect, so a spoofed finding can only
  // plant a fake list entry, never a download.
  let hookNonce = null;
  function newHookNonce() {
    return (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function")
      ? globalThis.crypto.randomUUID()
      : "tl-n-" + Math.random().toString(36).slice(2) + Date.now().toString(36);
  }
  // Evidence-only state for blob: players (never a source list):
  // blob URL -> { mediaSourceId, mimeTypes, duration, segments,
  // bytes }. It proves a blob player exists and is streaming; the
  // downloadable media itself still comes through add(), from the
  // manifest URLs the hook reports alongside.
  const blobMedia = new Map();
  const BLOB_MEDIA_MAX = 20;
  function hookPost(type) {
    try {
      if (type === "THUNDERLOAD_HOOK_PING") {
        hookNonce = newHookNonce();
        window.postMessage({ __thunderLoadHook: true, type, nonce: hookNonce }, "*");
      } else {
        window.postMessage({ __thunderLoadHook: true, type }, "*");
      }
    } catch (_) {}
  }
  function handleHookMessage(event) {
    try {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || data.__thunderLoadHook !== true) return;
      if (data.type === "THUNDERLOAD_HOOK_HELLO") {
        if (typeof data.token !== "string" || !data.token) return;
        if (data.nonce !== hookNonce) return;
        hookToken = data.token;
        hookHelloReceived = true;
        return;
      }
      if (!hookHelloReceived || data.token !== hookToken) return;
      if (data.type === "THUNDERLOAD_HOOK_FINDING") {
        if (typeof data.url !== "string") return;
        // Re-classify in the isolated world: the hook's kind is only
        // accepted if this world's own classifier agrees — segments
        // and YouTube service URLs can never become sources this way.
        const kind = classify(data.url);
        if (!kind) return;
        add(data.url, kind);
        return;
      }
      if (data.type === "THUNDERLOAD_HOOK_BLOB") {
        if (typeof data.blobUrl !== "string" || !/^blob:/i.test(data.blobUrl)) return;
        if (blobMedia.size >= BLOB_MEDIA_MAX && !blobMedia.has(data.blobUrl)) return;
        blobMedia.set(data.blobUrl, {
          mediaSourceId: typeof data.mediaSourceId === "number" ? data.mediaSourceId : null,
          mimeTypes: Array.isArray(data.mimeTypes) ? data.mimeTypes.filter((m) => typeof m === "string") : [],
          duration: Number.isFinite(data.duration) ? data.duration : null,
          segments: 0,
          bytes: 0
        });
        // A blob player just started: the manifest likely arrived
        // alongside, so rescan now instead of waiting for the next
        // observer tick.
        scheduleScan();
        return;
      }
      if (data.type === "THUNDERLOAD_HOOK_MSE") {
        if (typeof data.mediaSourceId !== "number") return;
        for (const info of blobMedia.values()) {
          if (info.mediaSourceId === data.mediaSourceId) {
            if (Number.isFinite(data.segments)) info.segments = data.segments;
            if (Number.isFinite(data.bytes)) info.bytes = data.bytes;
            if (Array.isArray(data.mimeTypes)) {
              info.mimeTypes = data.mimeTypes.filter((m) => typeof m === "string");
            }
            if (Number.isFinite(data.duration)) info.duration = data.duration;
          }
        }
        return;
      }
    } catch (_) { /* A malformed hook message is ignored. */ }
  }
  window.addEventListener("message", handleHookMessage);
  // Ping the hook for its token; one retry covers a late hook
  // install, and a 30 s re-PING re-latches on the real hook's HELLO
  // if a hostile page ever won the first race (Slice 3 MINOR (a)).
  // Without a HELLO the bridge stays silent and detection degrades
  // to the DOM scan + webRequest paths.
  hookPost("THUNDERLOAD_HOOK_PING");
  setTimeout(() => { if (!hookHelloReceived) hookPost("THUNDERLOAD_HOOK_PING"); }, 1000);
  setInterval(() => { hookPost("THUNDERLOAD_HOOK_PING"); }, 30000);

  scan();
})();
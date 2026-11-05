(() => {
  if (globalThis.__thunderLoadScannerInstalled) return;
  globalThis.__thunderLoadScannerInstalled = true;

  const found = new Map();
  const MEDIA_EXT = /\.(?:m3u8|mpd|mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav)(?:$|[?#])/i;
  const SEGMENT_EXT = /\.(?:ts|m4s|cmfv|cmfa)(?:$|[?#])/i;
  let scanScheduled = false;
  let activeVideo = null;
  let hideTimer = null;
  let buttonHost = null;
  let button = null;
  let status = null;
  let overlayRoot = null;

  function classify(url) {
    if (!url || !/^https?:\/\//i.test(url)) return null;
    if (/\.m3u8(?:$|[?#])/i.test(url)) return "HLS playlist";
    if (/\.mpd(?:$|[?#])/i.test(url)) return "DASH manifest";
    if (SEGMENT_EXT.test(url)) return null;
    if (MEDIA_EXT.test(url)) return "Direct media";
    return null;
  }

  function sourceFor(video) {
    const candidates = [
      video.currentSrc,
      video.src,
      ...Array.from(video.querySelectorAll("source")).map((node) => node.src)
    ];
    for (const url of candidates) {
      const kind = classify(url);
      if (kind) return { url, kind };
    }
    // MSE players may expose blob: as currentSrc while one HLS manifest
    // remains visible in the page's resource list.
    const hls = Array.from(found.values()).filter((item) => item.kind === "HLS playlist");
    return hls.length === 1 ? { url: hls[0].url, kind: hls[0].kind } : null;
  }

  function add(url, label) {
    const kind = classify(url);
    if (!kind || found.has(url) || found.size >= 80) return;
    const item = { url, kind, label: label || kind, discoveredAt: Date.now() };
    found.set(url, item);
    chrome.runtime.sendMessage({ type: "VIDEO_SOURCES_FOUND", sources: [item] }).catch(() => {});
  }

  function ensureOverlay() {
    if (buttonHost) return;
    buttonHost = document.createElement("div");
    buttonHost.style.cssText = "all:initial;position:fixed;inset:0;z-index:2147483646;pointer-events:none;";
    overlayRoot = buttonHost.attachShadow({ mode: "closed" });
    const style = document.createElement("style");
    style.textContent = [
      '.wrap{position:fixed;display:none;pointer-events:none;z-index:2147483647}',
      'button{pointer-events:auto;display:inline-flex;align-items:center;justify-content:center;gap:6px;height:29px;padding:0 10px;border:1px solid rgba(205,220,238,.27);border-radius:6px;background:rgba(20,29,40,.88);color:#e8f1fc;font:600 11px/1 system-ui,-apple-system,"Segoe UI",sans-serif;letter-spacing:.1px;box-shadow:0 2px 10px rgba(0,0,0,.18);backdrop-filter:blur(8px);opacity:.78;cursor:pointer;transition:opacity .14s ease,background .14s ease,border-color .14s ease}',
      'button:hover,button:focus-visible{opacity:1;background:rgba(35,52,72,.96);border-color:rgba(165,201,242,.62);outline:none}',
      'button svg{width:13px;height:13px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}',
      '.message{display:none;position:absolute;top:34px;right:0;max-width:210px;padding:6px 8px;border:1px solid rgba(205,220,238,.2);border-radius:5px;background:rgba(17,25,35,.94);color:#e4edf8;font:10px/1.35 system-ui,-apple-system,"Segoe UI",sans-serif;white-space:normal;box-shadow:0 2px 10px rgba(0,0,0,.2)}',
      '.message.show{display:block}',
      '@media(prefers-reduced-motion:reduce){button{transition:none}}'
    ].join("\n");
    const wrap = document.createElement("div");
    wrap.className = "wrap";
    button = document.createElement("button");
    button.type = "button";
    button.title = "Download with ThunderLoad";
    button.setAttribute("aria-label", "Download with ThunderLoad");
    button.innerHTML = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 2.5v8m-3-3 3 3 3-3M3 13.5h10"/></svg><span>Download</span>';
    status = document.createElement("div");
    status.className = "message";
    button.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const target = activeVideo;
      const source = target && sourceFor(target);
      if (!source) { showStatus("This video does not expose a downloadable media URL."); return; }
      button.disabled = true;
      showStatus("Preparing download…");
      try {
        const result = await chrome.runtime.sendMessage({
          type: "QUICK_DOWNLOAD_MEDIA",
          url: source.url,
          kind: source.kind,
          pageUrl: location.href,
          filename: target.getAttribute("data-filename") || ""
        });
        if (!result?.ok) throw new Error(result?.error || "Could not start the download.");
        showStatus(result.message || "Download added.");
      } catch (error) {
        showStatus(error?.message || String(error));
      } finally {
        button.disabled = false;
      }
    });
    wrap.append(button, status);
    overlayRoot.append(style, wrap);
    document.documentElement.append(buttonHost);
    overlayRoot.wrap = wrap;
    wrap.addEventListener("pointerenter", () => clearTimeout(hideTimer));
    wrap.addEventListener("pointerleave", scheduleHide);
  }

  function showStatus(message) {
    if (!status) return;
    status.textContent = message;
    status.classList.add("show");
    clearTimeout(showStatus.timer);
    showStatus.timer = setTimeout(() => status?.classList.remove("show"), 3000);
  }

  function positionOverlay(video) {
    if (!buttonHost || !overlayRoot?.wrap || !video?.isConnected) return;
    const rect = video.getBoundingClientRect();
    if (rect.width < 180 || rect.height < 100 || rect.bottom < 0 || rect.top > innerHeight) {
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

  function scheduleHide() {
    clearTimeout(hideTimer);
    hideTimer = setTimeout(() => {
      if (overlayRoot?.wrap) overlayRoot.wrap.style.display = "none";
      activeVideo = null;
    }, 140);
  }

  function attachVideo(video, index) {
    if (video.dataset.thunderLoadAttached === "1") return;
    video.dataset.thunderLoadAttached = "1";
    const show = () => {
      const source = sourceFor(video);
      if (!source || source.kind === "DASH manifest") return;
      activeVideo = video;
      add(source.url, "Video " + (index + 1));
      ensureOverlay();
      clearTimeout(hideTimer);
      positionOverlay(video);
    };
    video.addEventListener("pointerenter", show, { passive: true });
    video.addEventListener("pointermove", () => {
      if (activeVideo === video) positionOverlay(video);
      else if (video.matches(":hover")) show();
    }, { passive: true });
    video.addEventListener("loadedmetadata", () => {
      const source = sourceFor(video);
      if (source) add(source.url, "Video " + (index + 1));
    }, { passive: true });
    video.addEventListener("play", () => {
      const source = sourceFor(video);
      if (source) add(source.url, "Video " + (index + 1));
    }, { passive: true });
    video.addEventListener("pointerleave", scheduleHide, { passive: true });
    const source = sourceFor(video);
    if (source) add(source.url, "Video " + (index + 1));
  }

  function scan() {
    const videos = Array.from(document.querySelectorAll("video"));
    videos.forEach(attachVideo);
    // Include manifest and direct media resources for the extension popup.
    for (const entry of performance.getEntriesByType("resource").slice(-700)) {
      const kind = classify(entry.name);
      if (kind) add(entry.name, kind);
    }
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

  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message?.type === "SCAN_NOW") {
      sendResponse({ ok: true, count: scan() });
      return false;
    }
    if (message?.type === "GET_SCANNED_MEDIA") {
      sendResponse({ sources: Array.from(found.values()) });
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
        if (source && source.kind !== "DASH manifest") {
          activeVideo = hovered;
          ensureOverlay();
          clearTimeout(hideTimer);
          positionOverlay(hovered);
        }
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
  scan();
})();
(() => {
  if (globalThis.__thunderLoadScannerInstalled) return;
  globalThis.__thunderLoadScannerInstalled = true;

  const found = new Map();
  const MEDIA_EXT = /\.(?:m3u8|mpd|mp4|m4v|mov|webm|mkv|avi|mp3|m4a|aac|wav)(?:$|[?#])/i;
  const SEGMENT_EXT = /\.(?:ts|m4s|cmfv|cmfa)(?:$|[?#])/i;
  let scanScheduled = false;

  function classify(url) {
    if (/\.m3u8(?:$|[?#])/i.test(url)) return "HLS playlist";
    if (/\.mpd(?:$|[?#])/i.test(url)) return "DASH manifest";
    if (SEGMENT_EXT.test(url)) return null;
    if (MEDIA_EXT.test(url)) return "Direct media";
    return null;
  }

  function add(url, label) {
    if (!url || !/^https?:\/\//i.test(url)) return;
    const kind = classify(url);
    if (!kind || found.size >= 80) return;
    if (!found.has(url)) {
      found.set(url, { url, kind, label: label || kind, discoveredAt: Date.now() });
      chrome.runtime.sendMessage({ type: "VIDEO_SOURCES_FOUND", sources: [{ url, kind, label: label || kind }] }).catch(() => {});
    }
  }

  function scan() {
    document.querySelectorAll("video").forEach((video, index) => {
      add(video.currentSrc, `Video ${index + 1}`);
      add(video.src, `Video ${index + 1}`);
      video.querySelectorAll("source").forEach((source) => add(source.src, `Video ${index + 1} source`));
    });
    const entries = performance.getEntriesByType("resource");
    for (const entry of entries.slice(-1000)) add(entry.name, "Page media resource");
  }

  function scheduleScan() {
    if (scanScheduled) return;
    scanScheduled = true;
    setTimeout(() => {
      scanScheduled = false;
      scan();
    }, 450);
  }

  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message?.type === "SCAN_NOW") {
      scan();
      sendResponse({ ok: true, count: found.size });
    }
    if (message?.type === "GET_SCANNED_MEDIA") {
      scan();
      sendResponse({ sources: Array.from(found.values()) });
    }
    return true;
  });

  // Short-lived, local-only observation; no page overlays or playback interruption.
  const observer = new MutationObserver(scheduleScan);
  observer.observe(document.documentElement || document, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ["src"],
  });
  scan();
  let scansRemaining = 30;
  const timer = setInterval(() => {
    if (!document.hidden) scan();
    scansRemaining -= 1;
    if (scansRemaining <= 0) {
      clearInterval(timer);
      observer.disconnect();
    }
  }, 2000);
})();

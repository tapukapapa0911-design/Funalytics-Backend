(async () => {
  const MAX_CACHE_AGE_MS = 24 * 60 * 60 * 1000;
  const BUILD_VERSION = "live-nav-v23";
  const LAST_SYNCED_DATE_KEY = "lastSyncedDate";
  const LAST_SYNC_ATTEMPT_KEY = "lastSyncAttempt";
  const CACHED_NAV_DATE_KEY = "cachedNavDate";
  const NAV_SYNC_THROTTLE_MS = 15 * 60 * 1000;
  const NAV_SYNC_TIMEOUT_MS = 40 * 1000;
  const NAV_SYNC_RETRY_DELAYS_MS = [0, 10000];
  const SYNC_IN_FLIGHT_FLAG = "__fundpulseNavSyncInFlight";
  const SW_NAV_SYNC_TAG = "funalytics-nav-sync";
  const SW_NAV_PERIODIC_TAG = "funalytics-nav-daily";
  const NAV_CACHE_CLEARED_KEY = "fundpulse-nav-cache-cleared-v2";

  const loadScript = (src) => new Promise((resolve, reject) => {
    const versionedSrc = src.includes("?") ? `${src}&v=${BUILD_VERSION}` : `${src}?v=${BUILD_VERSION}`;
    const existing = document.querySelector(`script[data-live-src="${src}"]`);
    if (existing) {
      resolve();
      return;
    }
    const script = document.createElement("script");
    script.src = versionedSrc;
    script.async = false;
    script.dataset.liveSrc = src;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.body.appendChild(script);
  });

  const loadOptionalScript = async (src) => {
    try {
      await loadScript(src);
      return true;
    } catch (error) {
      console.warn(`[live-data-version] optional script skipped: ${src}`, error);
      return false;
    }
  };

  const isoDateValue = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return 0;
    const parsed = /^\d{4}-\d{2}-\d{2}$/.test(raw) ? new Date(`${raw}T00:00:00`) : new Date(raw);
    if (Number.isNaN(parsed.getTime()) || parsed.getUTCFullYear() < 2000) return 0;
    return parsed.getTime();
  };

  const navDateOf = (data) => data?.liveNavDate || "";

  const localTodayIso = () => {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, "0");
    const day = String(now.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };

  const readLastSyncAttempt = () => {
    try {
      const raw = Number(localStorage.getItem(LAST_SYNC_ATTEMPT_KEY) || 0);
      return Number.isFinite(raw) ? raw : 0;
    } catch {
      return 0;
    }
  };

  const readLastSyncedDate = () => {
    try {
      return String(localStorage.getItem(LAST_SYNCED_DATE_KEY) || "").trim();
    } catch {
      return "";
    }
  };

  const markSyncAttempt = () => {
    try {
      localStorage.setItem(LAST_SYNC_ATTEMPT_KEY, String(Date.now()));
    } catch {}
  };

  const markSyncSuccessForToday = () => {
    try {
      localStorage.setItem(LAST_SYNC_ATTEMPT_KEY, String(Date.now()));
      localStorage.setItem(LAST_SYNCED_DATE_KEY, localTodayIso());
    } catch {}
  };

  const readCachedNavDate = () => {
    try {
      return String(localStorage.getItem(CACHED_NAV_DATE_KEY) || "").trim();
    } catch {
      return "";
    }
  };

  const writeCachedNavDate = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return;
    try {
      localStorage.setItem(CACHED_NAV_DATE_KEY, normalized);
    } catch {}
  };

  const shouldReplaceDataset = (currentData, nextData) => {
    const currentDate = isoDateValue(navDateOf(currentData));
    const nextDate = isoDateValue(navDateOf(nextData));
    if (nextDate > currentDate) return true;
    if (currentDate > nextDate) return false;
    if (currentDate && nextDate && currentDate === nextDate) return false;
    const currentFunds = Array.isArray(currentData?.funds) ? currentData.funds.length : 0;
    const nextFunds = Array.isArray(nextData?.funds) ? nextData.funds.length : 0;
    return nextFunds >= currentFunds;
  };

  const DATA_FRESHNESS_WINDOW_MS = 26 * 60 * 60 * 1000;
  const isRecentNavDate = (value) => {
    const time = isoDateValue(value);
    if (!time) return false;
    return (Date.now() - time) <= DATA_FRESHNESS_WINDOW_MS;
  };

  const waitForDataProvider = async () => {
    for (let attempt = 0; attempt < 40; attempt += 1) {
      if (window.LiveDataVersion?.dataProvider) return window.LiveDataVersion.dataProvider;
      await new Promise((resolve) => window.setTimeout(resolve, 50));
    }
    return null;
  };

  const clearStaleNavCaches = () => {
    const cacheApi = window.LiveDataVersion?.cache;
    const schema = window.LiveDataVersion?.schema;
    if (!cacheApi || !schema?.cache) return;
    cacheApi.purgeStaleByPrefix?.("live-funalytics", MAX_CACHE_AGE_MS);
    cacheApi.purgeKeysOlderThan?.([schema.cache.datasetKey], MAX_CACHE_AGE_MS);
    cacheApi.purgeKeysOlderThan?.(["fundpulse-live-data-v8"], MAX_CACHE_AGE_MS);
    cacheApi.remove?.("fundpulse-live-data-v5");
    cacheApi.remove?.("fundpulse-live-data-v6");
    cacheApi.remove?.("fundpulse-live-data-v7");
    cacheApi.remove?.("live-funalytics-latest-nav-cache-v6");
    cacheApi.remove?.("live-funalytics-latest-nav-cache-v7");
    cacheApi.remove?.("live-funalytics-nav-scheme-list-v6");
    cacheApi.remove?.("live-funalytics-nav-scheme-list-v7");
    cacheApi.remove?.("live-funalytics-nav-match-map-v6");
    cacheApi.remove?.("live-funalytics-nav-match-map-v7");
    cacheApi.remove?.("live-funalytics-nav-resolver-cache-v6");
    cacheApi.remove?.("live-funalytics-nav-resolver-cache-v7");
    cacheApi.remove?.("live-funalytics-nav-fallback-cache-v6");
    cacheApi.remove?.("live-funalytics-nav-fallback-cache-v7");
  };

  window.LIVE_NAV_SNAPSHOT = null;
  const backupData = window.LiveDataVersion.validation.clone(window.FUND_APP_DATA || {});
  window.EXCEL_BACKUP_DATA = backupData;

  clearStaleNavCaches();
  const backendBase = () => window.LiveDataVersion?.apiClients?.backendBase?.()
    || "https://funalytics-backend.onrender.com";
  const navUrl = () => `${backendBase()}/nav`;
  const wait = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

  const fetchLiveSnapshot = async () => {
    const url = navUrl();
    let lastError = null;
    for (const delayMs of NAV_SYNC_RETRY_DELAYS_MS) {
      if (delayMs > 0) {
        await wait(delayMs);
      }
      markSyncAttempt();
      try {
        const response = await fetch(url, {
          cache: "no-store",
          signal: AbortSignal.timeout(NAV_SYNC_TIMEOUT_MS)
        });
        if (!response.ok) throw new Error(`nav ${response.status}`);
        const payload = await response.json();
        if (!payload || !Array.isArray(payload.items)) {
          throw new Error("Invalid /nav payload");
        }
        window.LIVE_NAV_SNAPSHOT = payload;
        return payload;
      } catch (error) {
        lastError = error;
      }
    }
    console.warn("[live-data-version] /nav fetch failed after retries", lastError);
    return null;
  };

  const dataProvider = await waitForDataProvider();
  if (!dataProvider) {
    console.warn("[live-data-version] dataProvider unavailable during bootstrap; using backup data");
    window.FUND_APP_DATA = backupData;
    await loadScript("./src/app.js");
    window.setTimeout(() => {
      loadOptionalScript("./assets/vendor/jszip.min.js")
        .then(() => loadOptionalScript("./src/workbook-import.js"));
    }, 0);
    return;
  }

  const bootData = dataProvider.primeAppData({ backupData });
  const initialData = bootData;
  window.FUND_APP_DATA = initialData;
  if (navDateOf(initialData) && isoDateValue(navDateOf(initialData)) >= isoDateValue(readCachedNavDate())) {
    writeCachedNavDate(navDateOf(initialData));
  }

  await loadScript("./src/app.js");

  window.setTimeout(() => {
    loadOptionalScript("./assets/vendor/jszip.min.js")
      .then(() => loadOptionalScript("./src/workbook-import.js"));
  }, 0);

  const applyDatasetUpdate = (nextData, reason) => {
    if (!nextData) return;
    if (!shouldReplaceDataset(window.FUND_APP_DATA, nextData)) return false;
    window.FUND_APP_DATA = nextData;
    const notify = () => {
      window.dispatchEvent(new CustomEvent("live-data:updated", { detail: { data: nextData, reason } }));
    };
    if ("requestAnimationFrame" in window) {
      window.requestAnimationFrame(notify);
      return true;
    }
    window.setTimeout(notify, 0);
    return true;
  };

  const currentBackupData = () => window.EXCEL_BACKUP_DATA || backupData;

  const applySnapshotData = async (reason, snapshot = null) => {
    try {
      const nextData = await dataProvider.refreshSnapshotData({
        backupData: currentBackupData(),
        snapshot: snapshot || window.LIVE_NAV_SNAPSHOT
      });
      const commit = () => applyDatasetUpdate(nextData, reason);
      if ("requestAnimationFrame" in window) {
        window.requestAnimationFrame(commit);
      } else {
        window.setTimeout(commit, 0);
      }
      return true;
    } catch (error) {
      console.warn("[live-data-version] snapshot apply failed", error);
      return false;
    }
  };

  const registerServiceWorkerSync = async () => {
    if (!("serviceWorker" in navigator) || location.protocol === "file:") return null;
    try {
      const existing = await navigator.serviceWorker.getRegistration();
      const registration = existing || await navigator.serviceWorker.register("./service-worker.js");
      const readyRegistration = await navigator.serviceWorker.ready.catch(() => registration);

      if ("sync" in readyRegistration) {
        readyRegistration.sync.register(SW_NAV_SYNC_TAG).catch(() => {});
      }

      if ("periodicSync" in readyRegistration) {
        try {
          const permission = await navigator.permissions?.query?.({ name: "periodic-background-sync" });
          if (!permission || permission.state === "granted") {
            await readyRegistration.periodicSync.register(SW_NAV_PERIODIC_TAG, {
              minInterval: 12 * 60 * 60 * 1000
            });
          }
        } catch {}
      }
      return readyRegistration;
    } catch (error) {
      console.warn("[live-data-version] service worker nav sync setup failed", error);
      return null;
    }
  };

  const clearServiceWorkerNavCache = async (registration) => {
    if (!registration) return;
    try {
      if (localStorage.getItem(NAV_CACHE_CLEARED_KEY) === "true") return;
      const worker = registration.active || registration.waiting || registration.installing;
      worker?.postMessage({ type: "CLEAR_NAV_CACHE" });
      localStorage.setItem(NAV_CACHE_CLEARED_KEY, "true");
    } catch {}
  };

  const triggerServiceWorkerNavSync = async (registration) => {
    if (!registration) return;
    const worker = registration.active || registration.waiting || registration.installing;
    worker?.postMessage({
      type: "TRIGGER_NAV_SYNC",
      navUrl: navUrl()
    });
  };

  const shouldSkipSyncFetch = () => {
    if (window[SYNC_IN_FLIGHT_FLAG]) return true;
    if (readLastSyncedDate() === localTodayIso()) return true;
    const lastAttemptAt = readLastSyncAttempt();
    if (lastAttemptAt && (Date.now() - lastAttemptAt) < NAV_SYNC_THROTTLE_MS) return true;
    return false;
  };

  const runBackgroundSnapshotSync = async () => {
    if (shouldSkipSyncFetch()) return;
    window[SYNC_IN_FLIGHT_FLAG] = true;
    try {
      const snapshot = await fetchLiveSnapshot();
      if (!snapshot?.items?.length) return;

      const snapshotDate = String(snapshot.latestDate || "").trim();
      const cachedNavDate = readCachedNavDate();
      if (snapshotDate && snapshotDate === cachedNavDate) {
        markSyncSuccessForToday();
        return;
      }

      const nextData = await dataProvider.refreshAppData({
        backupData: currentBackupData(),
        forceLive: true,
        snapshotOverride: snapshot
      });
      const applied = applyDatasetUpdate(nextData, "snapshot-sync");
      if (applied) {
        writeCachedNavDate(snapshotDate || navDateOf(window.FUND_APP_DATA));
        markSyncSuccessForToday();
        return;
      }

      window.FUND_APP_DATA = nextData;
      window.dispatchEvent(new CustomEvent("live-data:updated", {
        detail: { data: nextData, reason: "snapshot-sync", force: true }
      }));
      writeCachedNavDate(snapshotDate || navDateOf(nextData));
      markSyncSuccessForToday();
    } catch (error) {
      console.warn("[live-data-version] background snapshot sync failed", error);
    } finally {
      window[SYNC_IN_FLIGHT_FLAG] = false;
    }
  };

  const startBackgroundSyncAfterPaint = () => {
    const runner = async () => {
      const registration = await registerServiceWorkerSync();
      await clearServiceWorkerNavCache(registration);
      await wait(150);
      await triggerServiceWorkerNavSync(registration);
      await runBackgroundSnapshotSync();
    };

    if ("requestIdleCallback" in window) {
      window.requestIdleCallback(() => {
        window.setTimeout(runner, 0);
      }, { timeout: 3000 });
      return;
    }

    window.setTimeout(runner, 2000);
  };

  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(() => {
      startBackgroundSyncAfterPaint();
    });
  });
})();

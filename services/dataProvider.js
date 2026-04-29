window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.dataProvider = (() => {
  const schema = window.LiveDataVersion.schema;
  const cache = window.LiveDataVersion.cache;
  const { ensureAppShape, clone } = window.LiveDataVersion.validation;
  const mapper = window.LiveDataVersion.dataMapper;
  const api = window.LiveDataVersion.apiClients;
  const DATASET_TTL_MS = 15 * 60 * 1000;

  const readBackupData = async () => {
    if (window.EXCEL_BACKUP_DATA) return clone(window.EXCEL_BACKUP_DATA);
    const response = await fetch("./mockData/excel-backup.json", { cache: "no-store" });
    if (!response.ok) throw new Error("Backup dataset unavailable");
    return response.json();
  };

  const localIsoDate = (value) => {
    if (value === null || value === undefined || value === "") return "";
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime()) || date.getUTCFullYear() < 2000) return "";
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };

  const latestSnapshotDate = () => {
    const explicit = localIsoDate(window.LIVE_NAV_SNAPSHOT?.latestDate);
    if (explicit) return explicit;
    const rows = Array.isArray(window.LIVE_NAV_SNAPSHOT?.items) ? window.LIVE_NAV_SNAPSHOT.items : [];
    return rows
      .map((row) => localIsoDate(row?.date || row?.latestDate))
      .filter(Boolean)
      .sort()
      .at(-1) || "";
  };

  const fetchBackendSnapshot = async () => {
    const payload = await api.fetchNavSnapshot();
    if (!payload || !Array.isArray(payload.items)) {
      throw new Error("Invalid /nav payload");
    }
    window.LIVE_NAV_SNAPSHOT = payload;
    return payload;
  };

  const dateValue = (value) => {
    const iso = localIsoDate(value);
    if (!iso) return 0;
    const parsed = new Date(`${iso}T00:00:00`);
    return Number.isNaN(parsed.getTime()) ? 0 : parsed.getTime();
  };

  const navDateOf = (data) => data?.liveNavDate || "";
  const hasUsableLiveNavData = (data) => {
    if (!data) return false;
    if (dateValue(data?.liveNavDate)) return true;
    return Array.isArray(data?.funds) && data.funds.some((fund) => (
      Number.isFinite(Number(fund?.latestNav)) && dateValue(fund?.liveNavDate)
    ));
  };

  const latestDateFromRows = (rows = [], fallback = "") => {
    const rowLatest = rows
      .map((row) => localIsoDate(row?.date || row?.latestDate))
      .filter(Boolean)
      .sort((left, right) => dateValue(left) - dateValue(right))
      .at(-1);
    return rowLatest || fallback || "";
  };

  const sanitizeDatasetDates = (data, fallbackData) => {
    const safe = ensureAppShape(data, fallbackData);
    const authoritativeDate = latestDateFromRows(
      (safe?.funds || []).map((fund) => ({ latestDate: fund?.liveNavDate || fund?.latestNavDate || null })),
      safe?.liveNavDate || fallbackData?.liveNavDate || ""
    );
    if (!authoritativeDate || safe.liveNavDate === authoritativeDate) return safe;
    return { ...safe, liveNavDate: authoritativeDate };
  };

  const markSyncingIfNoLiveNav = (data, fallbackData) => {
    const safe = sanitizeDatasetDates(data, fallbackData);
    if (hasUsableLiveNavData(safe)) return safe;
    return {
      ...safe,
      liveNavDate: null,
      liveNavStatus: "syncing"
    };
  };

  const RECENT_SNAPSHOT_WINDOW_MS = 26 * 60 * 60 * 1000;

  const pickFresherDataset = (left, right, fallbackData) => {
    const safeLeft = left ? sanitizeDatasetDates(left, fallbackData) : null;
    const safeRight = right ? sanitizeDatasetDates(right, fallbackData) : null;
    if (!safeLeft) return safeRight;
    if (!safeRight) return safeLeft;

    const leftDate = dateValue(navDateOf(safeLeft));
    const rightDate = dateValue(navDateOf(safeRight));
    if (rightDate > leftDate) return safeRight;
    if (leftDate > rightDate) return safeLeft;

    const leftFunds = Array.isArray(safeLeft.funds) ? safeLeft.funds.length : 0;
    const rightFunds = Array.isArray(safeRight.funds) ? safeRight.funds.length : 0;
    return rightFunds >= leftFunds ? safeRight : safeLeft;
  };

  const readCachedDataset = (backupData) => {
    const cached = cache.readJson(schema.cache.datasetKey);
    if (!(cache.isFresh(cached, DATASET_TTL_MS) && cached?.data)) return null;
    const sanitized = sanitizeDatasetDates(cached.data, backupData);
    return hasUsableLiveNavData(sanitized) ? sanitized : null;
  };

  const isSnapshotRecent = () => {
    const rows = Array.isArray(window.LIVE_NAV_SNAPSHOT?.items) ? window.LIVE_NAV_SNAPSHOT.items : [];
    if (!rows.length) return false;

    const generatedAt = new Date(window.LIVE_NAV_SNAPSHOT?.generatedAt || "");
    if (!Number.isNaN(generatedAt.getTime())) {
      return Date.now() - generatedAt.getTime() <= RECENT_SNAPSHOT_WINDOW_MS;
    }

    const snapshotDate = latestSnapshotDate();
    if (!snapshotDate) return false;
    const parsed = new Date(`${snapshotDate}T00:00:00`);
    if (Number.isNaN(parsed.getTime())) return false;
    return Date.now() - parsed.getTime() <= RECENT_SNAPSHOT_WINDOW_MS;
  };

  const snapshotRowsIfRecent = () => {
    const rows = Array.isArray(window.LIVE_NAV_SNAPSHOT?.items) ? window.LIVE_NAV_SNAPSHOT.items : [];
    return isSnapshotRecent() ? rows : [];
  };

  const snapshotRowsIfFresherThanBackup = (backupLatestDate = "") => {
    const rows = Array.isArray(window.LIVE_NAV_SNAPSHOT?.items) ? window.LIVE_NAV_SNAPSHOT.items : [];
    if (!rows.length) return [];
    const snapshotDate = latestSnapshotDate() || latestDateFromRows(rows, "");
    if (dateValue(snapshotDate) > dateValue(backupLatestDate)) return rows;
    if (dateValue(snapshotDate) === dateValue(backupLatestDate) && isSnapshotRecent()) return rows;
    return snapshotRowsIfRecent();
  };

  const buildSnapshotDataset = (backupData) => {
    const snapshotRows = snapshotRowsIfFresherThanBackup(backupData?.liveNavDate || backupData?.latestDate);
    if (!snapshotRows.length) return null;
    const { data: merged } = mapper.mergeLatestNavSync(backupData, snapshotRows);
    merged.liveNavDate = latestSnapshotDate() || latestDateFromRows(snapshotRows, merged.liveNavDate || backupData.liveNavDate || "");
    merged.liveNavStatus = "fresh";
    return sanitizeDatasetDates(merged, backupData);
  };

  const buildSnapshotDatasetChunked = async (backupData, snapshotPayload = null) => {
    const snapshotRows = Array.isArray(snapshotPayload?.items)
      ? snapshotPayload.items
      : snapshotRowsIfFresherThanBackup(backupData?.liveNavDate || backupData?.latestDate);
    if (!snapshotRows.length) return null;
    const { data: merged } = await mapper.mergeLatestNav(backupData, snapshotRows);
    merged.liveNavDate = localIsoDate(snapshotPayload?.latestDate)
      || latestSnapshotDate()
      || latestDateFromRows(snapshotRows, merged.liveNavDate || backupData.liveNavDate || "");
    merged.liveNavStatus = "fresh";
    return sanitizeDatasetDates(merged, backupData);
  };

  const primeAppData = ({ backupData }) => {
    const cachedData = readCachedDataset(backupData);
    const snapshotData = buildSnapshotDataset(backupData);
    return markSyncingIfNoLiveNav(pickFresherDataset(
      pickFresherDataset(cachedData, snapshotData, backupData),
      sanitizeDatasetDates(backupData, backupData),
      backupData
    ), backupData);
  };

  const persistDataset = (data) => {
    const sanitized = markSyncingIfNoLiveNav(data, data);
    cache.writeJson(schema.cache.datasetKey, { savedAt: Date.now(), data: sanitized });
    return sanitized;
  };

  const refreshAppData = async ({ backupData, forceLive = false, snapshotOverride = null }) => {
    const safeBackup = ensureAppShape(backupData, backupData) || await readBackupData();
    const cachedData = readCachedDataset(safeBackup);
    const snapshotData = buildSnapshotDataset(safeBackup);

    try {
      if (!forceLive && cachedData) {
        return persistDataset(ensureAppShape({ ...cachedData, liveNavStatus: "fresh" }, safeBackup));
      }

      const snapshot = snapshotOverride && Array.isArray(snapshotOverride?.items)
        ? snapshotOverride
        : await fetchBackendSnapshot();
      const latestNavRows = Array.isArray(snapshot?.items) ? snapshot.items : [];
      if (!latestNavRows.length) {
        const fallback = pickFresherDataset(
          pickFresherDataset(cachedData, snapshotData, safeBackup),
          sanitizeDatasetDates(safeBackup, safeBackup),
          safeBackup
        );
        return persistDataset(ensureAppShape({ ...fallback, liveNavStatus: "last-available" }, safeBackup));
      }
      const { data: merged } = await mapper.mergeLatestNav(safeBackup, latestNavRows);
      merged.liveNavDate = localIsoDate(snapshot?.latestDate) || latestDateFromRows(latestNavRows, merged.liveNavDate || safeBackup.liveNavDate || "");
      merged.liveNavStatus = "fresh";
      return persistDataset(ensureAppShape(merged, safeBackup));
    } catch (error) {
      console.warn("[live-data-version] Latest NAV refresh failed, using cache/backup:", error);
      const fallback = pickFresherDataset(
        pickFresherDataset(cachedData, snapshotData, safeBackup),
        sanitizeDatasetDates(safeBackup, safeBackup),
        safeBackup
      );
      return persistDataset(ensureAppShape({ ...fallback, liveNavStatus: "last-available" }, safeBackup));
    }
  };

  const refreshSnapshotData = async ({ backupData, snapshot }) => {
    const safeBackup = ensureAppShape(backupData, backupData) || await readBackupData();
    const cachedData = readCachedDataset(safeBackup);
    const snapshotData = await buildSnapshotDatasetChunked(safeBackup, snapshot || window.LIVE_NAV_SNAPSHOT);
    if (snapshotData) {
      return persistDataset(snapshotData);
    }
    const fallback = pickFresherDataset(
      cachedData,
      sanitizeDatasetDates(safeBackup, safeBackup),
      safeBackup
    );
    return persistDataset(ensureAppShape({ ...fallback, liveNavStatus: "last-available" }, safeBackup));
  };

  return { readBackupData, primeAppData, refreshAppData, refreshSnapshotData };
})();

import { runNavIngestion } from "../services/navIngestionService.js";
import { writeSnapshotFile } from "../services/snapshotStore.js";
import { logger } from "../utils/logger.js";
import { buildLiveSnapshotPayload, clearResponseCache } from "../routes/fundRoutes.js";
import { readNavCachePayload } from "../services/navStore.js";

const MIN_FULL_NAV_ROWS = 12000;
const NAV_SYNC_COOLDOWN_MS = 5 * 60 * 1000;
let running = false;
let lastSyncFinishedAt = 0;

function formatDuration(durationMs) {
  return `${(Number(durationMs || 0) / 1000).toFixed(1)}s`;
}

function buildExecutionSummary(result = {}, startedAt = Date.now()) {
  const durationMs = Number(result?.durationMs || (Date.now() - startedAt) || 0);
  const status = String(result?.status || "unknown");
  return {
    success: !["error", "partial-rejected", "stale-rejected", "running"].includes(status),
    status,
    fetched: Number(result?.fetched || 0),
    matched: Number(result?.matched || result?.count || 0),
    updated: Number(result?.updated || result?.inserted || 0),
    failed: Number(result?.failed || 0),
    latestDate: String(result?.latestDate || ""),
    generatedAt: String(result?.generatedAt || ""),
    durationMs,
    duration: formatDuration(durationMs)
  };
}

export async function syncNavData(options = {}) {
  const startedAt = Date.now();
  const effectiveMinRows = Number.isFinite(Number(options?.minRows)) && Number(options.minRows) > 0
    ? Math.floor(Number(options.minRows))
    : MIN_FULL_NAV_ROWS;
  const force = options?.force === true;
  if (running) {
    const summary = buildExecutionSummary({
      status: "running",
      durationMs: Date.now() - startedAt
    }, startedAt);
    logger.warn("nav-sync-already-running", summary);
    return summary;
  }
  if (!force && lastSyncFinishedAt && (Date.now() - lastSyncFinishedAt) < NAV_SYNC_COOLDOWN_MS) {
    const summary = buildExecutionSummary({
      status: "skipped",
      durationMs: Date.now() - startedAt
    }, startedAt);
    logger.info("nav-sync-cooldown-skip", summary);
    return summary;
  }
  running = true;
  try {
    const ingestionResult = await runNavIngestion({
      force,
      minRows: effectiveMinRows
    });
    if (ingestionResult?.status === "no-new-nav") {
      const resultObject = buildExecutionSummary({
        status: "no-new-nav",
        latestDate: String(ingestionResult?.latestDate || ""),
        matched: Number(ingestionResult?.matched || ingestionResult?.count || 0),
        fetched: Number(ingestionResult?.fetched || 0),
        failed: Number(ingestionResult?.failed || 0),
        generatedAt: String(ingestionResult?.generatedAt || ""),
        durationMs: Date.now() - startedAt
      }, startedAt);
      logger.info("nav-sync-no-change", resultObject);
      lastSyncFinishedAt = Date.now();
      return resultObject;
    }

    if (ingestionResult?.status === "partial-rejected") {
      const resultObject = buildExecutionSummary({
        status: "partial-rejected",
        latestDate: String(ingestionResult?.latestDate || ""),
        fetched: Number(ingestionResult?.fetched || ingestionResult?.count || 0),
        matched: Number(ingestionResult?.matched || 0),
        failed: Number(ingestionResult?.failed || 0),
        generatedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAt
      }, startedAt);
      logger.warn("nav-sync-partial", resultObject);
      lastSyncFinishedAt = Date.now();
      return resultObject;
    }

    if (ingestionResult?.status === "stale-rejected") {
      const resultObject = buildExecutionSummary({
        status: "stale-rejected",
        latestDate: String(ingestionResult?.latestDate || ""),
        fetched: Number(ingestionResult?.fetched || ingestionResult?.count || 0),
        matched: Number(ingestionResult?.matched || 0),
        failed: Number(ingestionResult?.failed || 0),
        generatedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAt
      }, startedAt);
      logger.warn("nav-sync-stale", resultObject);
      lastSyncFinishedAt = Date.now();
      return resultObject;
    }

    const navCache = await readNavCachePayload();
    const snapshotPayload = navCache?.items?.length
      ? {
          generatedAt: new Date().toISOString(),
          latestDate: String(navCache.latestDate || ingestionResult?.latestDate || ""),
          lastFetchTimestamp: new Date().toISOString(),
          count: Number(navCache.items.length || 0),
          items: navCache.items
        }
      : await buildLiveSnapshotPayload();
    const snapshotCount = Number(snapshotPayload?.count || (Array.isArray(snapshotPayload?.items) ? snapshotPayload.items.length : 0) || 0);
    if (snapshotCount < effectiveMinRows) {
      const resultObject = buildExecutionSummary({
        status: "partial-rejected",
        latestDate: String(snapshotPayload?.latestDate || ingestionResult?.latestDate || ""),
        fetched: Number(ingestionResult?.fetched || snapshotCount),
        matched: snapshotCount,
        failed: Number(ingestionResult?.failed || 0),
        generatedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAt
      }, startedAt);
      logger.warn("nav-sync-snapshot-partial", resultObject);
      lastSyncFinishedAt = Date.now();
      return resultObject;
    }

    const snapshot = writeSnapshotFile(snapshotPayload);
    clearResponseCache();
    const resultObject = buildExecutionSummary({
      status: "updated",
      fetched: Number(ingestionResult?.fetched || snapshot.count || 0),
      matched: Number(ingestionResult?.matched || snapshot.count || 0),
      updated: Number(ingestionResult?.updated || ingestionResult?.inserted || snapshot.count || 0),
      failed: Number(ingestionResult?.failed || 0),
      latestDate: String(snapshot.latestDate || ingestionResult?.latestDate || ""),
      generatedAt: String(snapshot.generatedAt || ""),
      durationMs: Date.now() - startedAt
    }, startedAt);
    logger.info("nav-sync-updated", resultObject);
    lastSyncFinishedAt = Date.now();
    return resultObject;
  } catch (error) {
    logger.error("nav-sync-failed", {
      error: String(error?.message || error),
      durationMs: Date.now() - startedAt
    });
    throw error;
  } finally {
    running = false;
  }
}

export async function triggerNavUpdate(options = {}) {
  return syncNavData(options);
}

import { runNavIngestion } from "../services/navIngestionService.js";
import { writeSnapshotFile } from "../services/snapshotStore.js";
import { logger } from "../utils/logger.js";
import { buildLiveSnapshotPayload, clearResponseCache } from "../routes/fundRoutes.js";

const MIN_FULL_NAV_ROWS = 12000;
let running = false;

export async function triggerNavUpdate(options = {}) {
  const startedAt = Date.now();
  const effectiveMinRows = Number.isFinite(Number(options?.minRows)) && Number(options.minRows) > 0
    ? Math.floor(Number(options.minRows))
    : MIN_FULL_NAV_ROWS;
  if (running) {
    logger.warn("NAV update skipped because a run is already in progress");
    return {
      status: "running",
      latestDate: "",
      count: 0,
      generatedAt: "",
      durationMs: Date.now() - startedAt
    };
  }
  running = true;
  try {
    logger.info("NAV update started");
    const ingestionResult = await runNavIngestion({
      force: options?.force === true,
      minRows: effectiveMinRows
    });
    console.log("NAV fetch complete");
    if (ingestionResult?.status === "no-new-nav") {
      const resultObject = {
        status: "no-new-nav",
        latestDate: String(ingestionResult?.latestDate || ""),
        count: Number(ingestionResult?.count || ingestionResult?.processed || 0),
        generatedAt: String(ingestionResult?.generatedAt || ""),
        durationMs: Date.now() - startedAt
      };
      logger.info("NAV snapshot write skipped because existing snapshot is fresh", resultObject);
      return resultObject;
    }

    if (ingestionResult?.status === "partial-rejected") {
      const resultObject = {
        status: "partial-rejected",
        latestDate: String(ingestionResult?.latestDate || ""),
        count: Number(ingestionResult?.count || ingestionResult?.processed || 0),
        generatedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAt
      };
      logger.warn("NAV snapshot write skipped because AMFI feed was partial", resultObject);
      return resultObject;
    }

    const snapshotPayload = await buildLiveSnapshotPayload();
    const snapshotCount = Number(snapshotPayload?.count || (Array.isArray(snapshotPayload?.items) ? snapshotPayload.items.length : 0) || 0);
    if (snapshotCount < effectiveMinRows) {
      const resultObject = {
        status: "partial-rejected",
        latestDate: String(snapshotPayload?.latestDate || ingestionResult?.latestDate || ""),
        count: snapshotCount,
        generatedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAt
      };
      logger.warn(`NAV snapshot write skipped: ${snapshotCount} rows available, minimum ${effectiveMinRows} required`, resultObject);
      return resultObject;
    }

    const snapshot = writeSnapshotFile(snapshotPayload);
    clearResponseCache();
    logger.info("NAV updated successfully");
    const resultObject = {
      status: ingestionResult?.status === "no-new-nav" ? "no-new-nav" : "updated",
      latestDate: String(snapshot.latestDate || ingestionResult?.latestDate || ""),
      count: Number(snapshot.count || 0),
      generatedAt: String(snapshot.generatedAt || ""),
      durationMs: Date.now() - startedAt
    };
    console.log("Returning result", resultObject);
    return resultObject;
  } catch (error) {
    logger.error("NAV update failed", error?.message || error);
    throw error;
  } finally {
    running = false;
  }
}

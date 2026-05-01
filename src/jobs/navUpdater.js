import { runNavIngestion } from "../services/navIngestionService.js";
import { writeSnapshotFile } from "../services/snapshotStore.js";
import { logger } from "../utils/logger.js";
import { buildLiveSnapshotPayload, clearResponseCache } from "../routes/fundRoutes.js";

let running = false;

export async function triggerNavUpdate() {
  const startedAt = Date.now();
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
    const ingestionResult = await runNavIngestion();
    console.log("NAV fetch complete");
    const snapshot = writeSnapshotFile(await buildLiveSnapshotPayload());
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

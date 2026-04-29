import { runNavIngestion } from "../services/navIngestionService.js";
import { writeSnapshotFile } from "../services/snapshotStore.js";
import { logger } from "../utils/logger.js";
import { buildLiveSnapshotPayload, clearResponseCache } from "../routes/fundRoutes.js";

let running = false;

export async function triggerNavUpdate() {
  if (running) {
    logger.warn("NAV update skipped because a run is already in progress");
    return {
      source: "amfi",
      status: "running",
      processed: 0,
      latestDate: "",
      snapshotCount: 0
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
      source: "amfi",
      status: ingestionResult?.status === "no-new-nav" ? "no-new-nav" : "updated",
      processed: Number(ingestionResult?.processed || 0),
      latestDate: String(snapshot.latestDate || ingestionResult?.latestDate || ""),
      snapshotCount: Number(snapshot.count || 0)
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

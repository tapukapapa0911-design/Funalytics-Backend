import { runNavIngestion } from "../services/navIngestionService.js";
import { writeSnapshotFile } from "../services/snapshotStore.js";
import { logger } from "../utils/logger.js";
import { buildLiveSnapshotPayload, clearResponseCache } from "../routes/fundRoutes.js";

let running = false;

export async function triggerNavUpdate() {
  if (running) {
    logger.warn("NAV update skipped because a run is already in progress");
    return null;
  }
  running = true;
  try {
    logger.info("NAV update started");
    const result = await runNavIngestion();
    const snapshot = writeSnapshotFile(await buildLiveSnapshotPayload());
    clearResponseCache();
    logger.info("NAV updated successfully");
    return {
      ...result,
      snapshotLatestDate: snapshot.latestDate,
      snapshotCount: snapshot.count
    };
  } catch (error) {
    logger.error("NAV update failed", error?.message || error);
    throw error;
  } finally {
    running = false;
  }
}

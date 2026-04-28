import { writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runNavIngestion } from "../services/navIngestionService.js";
import { logger } from "../utils/logger.js";
import { buildLiveSnapshotPayload, clearResponseCache } from "../routes/fundRoutes.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const snapshotOutputPath = path.resolve(__dirname, "../../../mockData/live-nav-snapshot.js");

let running = false;

async function writeSnapshotFile() {
  const snapshot = await buildLiveSnapshotPayload();
  const content = `window.LIVE_NAV_SNAPSHOT = ${JSON.stringify(snapshot)};\n`;
  await writeFile(snapshotOutputPath, content, "utf8");
  return snapshot;
}

export async function triggerNavUpdate() {
  if (running) {
    logger.warn("NAV update skipped because a run is already in progress");
    return null;
  }
  running = true;
  try {
    logger.info("NAV update started");
    const result = await runNavIngestion();
    const snapshot = await writeSnapshotFile();
    clearResponseCache();
    logger.info(`NAV snapshot refreshed at ${snapshot.latestDate || "unknown-date"} with ${snapshot.count} items`);
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

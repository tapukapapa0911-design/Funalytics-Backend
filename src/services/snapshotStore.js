import fs from "node:fs";
import path from "node:path";
import { logger } from "../utils/logger.js";

const snapshotDirPath = path.join(process.cwd(), "data");
const snapshotFilePath = path.join(process.cwd(), "data", "live-nav-snapshot.json");

const defaultSnapshot = () => ({
  generatedAt: "",
  latestDate: "",
  items: []
});

function ensureSnapshotFile() {
  if (!fs.existsSync(snapshotDirPath)) {
    fs.mkdirSync(snapshotDirPath, { recursive: true });
    logger.info(`Created snapshot data directory at ${snapshotDirPath}`);
  }

  if (!fs.existsSync(snapshotFilePath)) {
    fs.writeFileSync(snapshotFilePath, JSON.stringify(defaultSnapshot(), null, 2), "utf8");
    logger.info(`Created NAV snapshot file at ${snapshotFilePath}`);
  }
}

export function readSnapshotFile() {
  ensureSnapshotFile();
  try {
    const raw = fs.readFileSync(snapshotFilePath, "utf8");
    const parsed = JSON.parse(raw);
    return {
      generatedAt: String(parsed?.generatedAt || ""),
      latestDate: String(parsed?.latestDate || ""),
      lastFetchTimestamp: String(parsed?.lastFetchTimestamp || ""),
      count: Number(parsed?.count || (Array.isArray(parsed?.items) ? parsed.items.length : 0) || 0),
      items: Array.isArray(parsed?.items) ? parsed.items : []
    };
  } catch (error) {
    logger.warn(`Snapshot read failed, recreating default snapshot: ${error?.message || error}`);
    const fallback = defaultSnapshot();
    fs.writeFileSync(snapshotFilePath, JSON.stringify(fallback, null, 2), "utf8");
    return fallback;
  }
}

export function writeSnapshotFile(snapshot) {
  ensureSnapshotFile();
  const normalized = {
    generatedAt: String(snapshot?.generatedAt || ""),
    latestDate: String(snapshot?.latestDate || ""),
    lastFetchTimestamp: String(snapshot?.lastFetchTimestamp || ""),
    count: Number(snapshot?.count || (Array.isArray(snapshot?.items) ? snapshot.items.length : 0) || 0),
    items: Array.isArray(snapshot?.items) ? snapshot.items : []
  };
  fs.writeFileSync(snapshotFilePath, JSON.stringify(normalized, null, 2), "utf8");
  logger.info(`NAV snapshot updated at ${snapshotFilePath} (${normalized.latestDate || "unknown-date"}, ${normalized.count} items)`);
  return normalized;
}

export { ensureSnapshotFile, snapshotDirPath, snapshotFilePath };

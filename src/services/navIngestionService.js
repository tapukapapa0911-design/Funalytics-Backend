import { fetchAmfiNavFeed } from "./amfiService.js";
import { getLatestNavDate, saveNavRecords } from "./navStore.js";
import { readSnapshotFile } from "./snapshotStore.js";
import { logger } from "../utils/logger.js";

const MIN_FULL_NAV_ROWS = 12000;
const SNAPSHOT_FRESH_WINDOW_MS = 20 * 60 * 60 * 1000;

function toDateKey(value) {
  if (!value) return "";
  const raw = String(value || "").trim();
  const plainDate = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (plainDate) return `${plainDate[1]}-${plainDate[2]}-${plainDate[3]}`;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function isGeneratedRecently(value) {
  const generatedAt = new Date(value || "");
  if (Number.isNaN(generatedAt.getTime())) return false;
  return Date.now() - generatedAt.getTime() <= SNAPSHOT_FRESH_WINDOW_MS;
}

function currentDisplayEligibleNavDate() {
  const now = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  const cutoff = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate() - 1
  ));
  while (cutoff.getUTCDay() === 0 || cutoff.getUTCDay() === 6) {
    cutoff.setUTCDate(cutoff.getUTCDate() - 1);
  }
  const year = cutoff.getUTCFullYear();
  const month = String(cutoff.getUTCMonth() + 1).padStart(2, "0");
  const day = String(cutoff.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export async function runNavIngestion(options = {}) {
  logger.info("NAV ingestion started");
  const startedAt = Date.now();
  const minRows = Number.isFinite(Number(options?.minRows)) && Number(options.minRows) > 0
    ? Math.floor(Number(options.minRows))
    : MIN_FULL_NAV_ROWS;
  const force = options?.force === true;

  if (!force) {
    const existingSnapshot = readSnapshotFile();
    const liveLatestDate = await getLatestNavDate();
    const latestAvailableDate = [String(existingSnapshot?.latestDate || ""), String(liveLatestDate || "")]
      .filter(Boolean)
      .sort()
      .at(-1) || "";
    if (
      isGeneratedRecently(existingSnapshot?.generatedAt)
      && latestAvailableDate >= currentDisplayEligibleNavDate()
    ) {
      const summary = {
        source: "amfi",
        status: "no-new-nav",
        count: Number(existingSnapshot?.count || 0),
        processed: 0,
        latestDate: latestAvailableDate,
        generatedAt: String(existingSnapshot?.generatedAt || ""),
        durationMs: Date.now() - startedAt
      };
      logger.info("Existing NAV snapshot generated within last 20 hours", summary);
      return summary;
    }
  }

  const records = await fetchAmfiNavFeed();
  if (!records.length) {
    throw new Error("AMFI feed returned no NAV rows");
  }

  const existingLiveLatestDate = await getLatestNavDate();
  const incomingLatestDate = records
    .map((record) => toDateKey(record.navDate))
    .filter(Boolean)
    .sort()
    .at(-1) || "";

  if (
    existingLiveLatestDate
    && incomingLatestDate
    && incomingLatestDate < existingLiveLatestDate
  ) {
    const summary = {
      source: "amfi",
      status: "stale-rejected",
      count: records.length,
      processed: records.length,
      latestDate: existingLiveLatestDate,
      incomingLatestDate,
      durationMs: Date.now() - startedAt
    };
    logger.warn(
      `AMFI snapshot rejected because it would regress NAV date from ${existingLiveLatestDate} to ${incomingLatestDate}`,
      summary
    );
    return summary;
  }

  if (records.length < minRows) {
    const summary = {
      source: "amfi",
      status: "partial-rejected",
      count: records.length,
      processed: records.length,
      latestDate: incomingLatestDate,
      durationMs: Date.now() - startedAt
    };
    logger.warn(`AMFI partial NAV feed rejected: ${records.length} rows fetched, minimum ${minRows} required`, summary);
    return summary;
  }

  const bulkResult = await saveNavRecords(records);
  const summary = {
    source: "amfi",
    status: "updated",
    processed: records.length,
    inserted: bulkResult.upsertedCount || 0,
    updated: bulkResult.modifiedCount || 0,
    latestDate: incomingLatestDate,
    durationMs: Date.now() - startedAt
  };
  logger.info("NAV ingestion completed", summary);
  return summary;
}

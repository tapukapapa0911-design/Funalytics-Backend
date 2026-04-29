import { fetchAmfiNavFeed } from "./amfiService.js";
import { getLatestNavDate, saveNavRecords } from "./navStore.js";
import { logger } from "../utils/logger.js";

function toDateKey(value) {
  if (!value) return "";
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 10);
}

export async function runNavIngestion() {
  logger.info("NAV ingestion started");
  const startedAt = Date.now();
  const records = await fetchAmfiNavFeed();
  if (!records.length) {
    throw new Error("AMFI feed returned no NAV rows");
  }

  const incomingLatestDate = records
    .map((record) => toDateKey(record.navDate))
    .filter(Boolean)
    .sort()
    .at(-1) || "";

  const existingLatestDate = await getLatestNavDate();

  if (incomingLatestDate && existingLatestDate && incomingLatestDate <= existingLatestDate) {
    const summary = {
      source: "amfi",
      status: "no-new-nav",
      processed: records.length,
      latestDate: existingLatestDate,
      fetchedDate: incomingLatestDate,
      durationMs: Date.now() - startedAt
    };
    logger.info("No new NAV available from AMFI", summary);
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

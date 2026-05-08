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
  const startedAt = Date.now();
  const minRows = Number.isFinite(Number(options?.minRows)) && Number(options.minRows) > 0
    ? Math.floor(Number(options.minRows))
    : MIN_FULL_NAV_ROWS;
  const force = options?.force === true;
  const summary = {
    success: false,
    source: "amfi",
    status: "started",
    fetched: 0,
    matched: 0,
    failed: 0,
    inserted: 0,
    updated: 0,
    latestDate: "",
    generatedAt: "",
    durationMs: 0
  };

  logger.info("nav-sync-start", {
    force,
    minRows,
    startedAt: new Date(startedAt).toISOString()
  });

  try {
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
        summary.status = "no-new-nav";
        summary.success = true;
        summary.matched = Number(existingSnapshot?.count || 0);
        summary.latestDate = latestAvailableDate;
        summary.generatedAt = String(existingSnapshot?.generatedAt || "");
        summary.durationMs = Date.now() - startedAt;
        logger.info("nav-sync-skip", {
          status: summary.status,
          latestDate: summary.latestDate,
          matched: summary.matched,
          durationMs: summary.durationMs
        });
        return summary;
      }
    }

    let feedResult;
    try {
      feedResult = await fetchAmfiNavFeed();
    } catch (error) {
      throw new Error(`amfi-fetch-failed: ${error?.message || error}`);
    }

    const records = Array.isArray(feedResult?.records) ? feedResult.records : [];
    summary.fetched = Number(feedResult?.fetched || records.length || 0);
    summary.matched = Number(feedResult?.matched || records.length || 0);
    summary.failed = Number(feedResult?.failed || 0);

    if (!records.length) {
      throw new Error("AMFI feed returned no NAV rows");
    }

    const existingLiveLatestDate = await getLatestNavDate();
    const incomingLatestDate = records
      .map((record) => toDateKey(record.navDate))
      .filter(Boolean)
      .sort()
      .at(-1) || "";
    summary.latestDate = incomingLatestDate;

    if (
      existingLiveLatestDate
      && incomingLatestDate
      && incomingLatestDate < existingLiveLatestDate
    ) {
      summary.status = "stale-rejected";
      summary.success = true;
      summary.latestDate = existingLiveLatestDate;
      summary.incomingLatestDate = incomingLatestDate;
      summary.durationMs = Date.now() - startedAt;
      logger.warn("nav-sync-stale-rejected", {
        status: summary.status,
        latestDate: summary.latestDate,
        incomingLatestDate,
        fetched: summary.fetched,
        matched: summary.matched,
        failed: summary.failed,
        durationMs: summary.durationMs
      });
      return summary;
    }

    if (records.length < minRows) {
      summary.status = "partial-rejected";
      summary.success = true;
      summary.durationMs = Date.now() - startedAt;
      logger.warn("nav-sync-partial-rejected", {
        status: summary.status,
        latestDate: summary.latestDate,
        fetched: summary.fetched,
        matched: summary.matched,
        failed: summary.failed,
        durationMs: summary.durationMs
      });
      return summary;
    }

    let bulkResult;
    try {
      bulkResult = await saveNavRecords(records);
    } catch (error) {
      throw new Error(`nav-write-failed: ${error?.message || error}`);
    }

    summary.status = "updated";
    summary.success = true;
    summary.inserted = Number(bulkResult?.upsertedCount || 0);
    summary.updated = Number(bulkResult?.modifiedCount || 0);
    summary.durationMs = Date.now() - startedAt;
    logger.info("nav-sync-finish", {
      status: summary.status,
      latestDate: summary.latestDate,
      fetched: summary.fetched,
      matched: summary.matched,
      failed: summary.failed,
      inserted: summary.inserted,
      updated: summary.updated,
      durationMs: summary.durationMs
    });
    return summary;
  } catch (error) {
    summary.status = "error";
    summary.durationMs = Date.now() - startedAt;
    logger.error("nav-sync-error", {
      status: summary.status,
      latestDate: summary.latestDate,
      fetched: summary.fetched,
      matched: summary.matched,
      failed: summary.failed,
      durationMs: summary.durationMs,
      error: String(error?.message || error)
    });
    throw error;
  }
}

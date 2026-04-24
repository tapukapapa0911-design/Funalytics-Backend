import { Fund } from "../models/Fund.js";
import { fetchAmfiNavFeed } from "./amfiService.js";
import { fetchFallbackNavs } from "./mfapiFallbackService.js";
import { logger } from "../utils/logger.js";

function buildBulkOperations(records) {
  const now = new Date();
  return records.map((record) => ({
    updateOne: {
      filter: { schemeCode: record.schemeCode },
      update: {
        $set: {
          schemeName: record.schemeName,
          nav: record.nav,
          isin: record.isin || "",
          navDate: record.navDate,
          lastUpdated: now,
          source: record.source || "amfi"
        },
        $setOnInsert: {
          createdAt: now
        }
      },
      upsert: true
    }
  }));
}

async function ingestFromFallback() {
  const existingCodes = await Fund.find({}, { schemeCode: 1, _id: 0 }).lean();
  const fallbackRows = await fetchFallbackNavs(existingCodes.map((item) => item.schemeCode));
  if (!fallbackRows.length) {
    throw new Error("Fallback source returned no NAV rows");
  }
  const bulkResult = await Fund.bulkWrite(buildBulkOperations(fallbackRows), { ordered: false });
  return {
    source: "mfapi",
    processed: fallbackRows.length,
    inserted: bulkResult.upsertedCount || 0,
    updated: bulkResult.modifiedCount || 0
  };
}

export async function runNavIngestion() {
  logger.info("NAV ingestion started");
  const startedAt = Date.now();

  try {
    const records = await fetchAmfiNavFeed();
    const bulkResult = await Fund.bulkWrite(buildBulkOperations(records), { ordered: false });
    const summary = {
      source: "amfi",
      processed: records.length,
      inserted: bulkResult.upsertedCount || 0,
      updated: bulkResult.modifiedCount || 0,
      durationMs: Date.now() - startedAt
    };
    logger.info("NAV ingestion completed", summary);
    return summary;
  } catch (error) {
    logger.error("AMFI ingestion failed, switching to mfapi fallback", error.message);
    const fallbackSummary = await ingestFromFallback();
    fallbackSummary.durationMs = Date.now() - startedAt;
    logger.warn("Fallback NAV ingestion completed", fallbackSummary);
    return fallbackSummary;
  }
}

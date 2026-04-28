import express from "express";
import fs from "node:fs";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { env } from "../config/env.js";
import { findFundBySchemeCode, getAllFunds, getFundCount, getLatestFund, searchFunds } from "../services/navStore.js";
import { triggerNavUpdate } from "../jobs/navUpdater.js";
import { ensureSnapshotFile, readSnapshotFile, snapshotFilePath } from "../services/snapshotStore.js";
import { logger } from "../utils/logger.js";

const router = express.Router();
const responseCache = new Map();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const backupJsonPath = path.resolve(__dirname, "../../../mockData/excel-backup.json");
let appFundLookupPromise = null;

function getCached(key) {
  const entry = responseCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    responseCache.delete(key);
    return null;
  }
  return entry.payload;
}

function setCached(key, payload, ttlMs = env.cacheTtlMs) {
  responseCache.set(key, {
    payload,
    expiresAt: Date.now() + ttlMs
  });
}

export function clearResponseCache(prefix = "") {
  if (!prefix) {
    responseCache.clear();
    return;
  }
  for (const key of responseCache.keys()) {
    if (key.startsWith(prefix)) responseCache.delete(key);
  }
}

const clean = (value) => value === null || value === undefined ? "" : String(value).trim();
const canon = (value) => clean(value)
  .toLowerCase()
  .replace(/\(g\)|regular|direct|growth|plan|fund|option|reinvestment|payout|bonus|dividend|idcw|-/g, " ")
  .replace(/[^a-z0-9]+/g, " ")
  .trim();
const keyOf = (value) => clean(value)
  .toLowerCase()
  .replace(/[^a-z0-9]+/g, "-")
  .replace(/^-+|-+$/g, "")
  .slice(0, 90);

const liveRowPriority = (schemeName = "") => {
  const text = clean(schemeName).toLowerCase();
  const isDirect = /\bdirect\b/.test(text);
  const isIncome = /\b(idcw|dividend|payout|bonus|income)\b/.test(text);
  const isGrowth = /\bgrowth\b/.test(text) || /\(g\)/.test(text);
  if (!isDirect && isGrowth && !isIncome) return 5;
  if (!isDirect && !isIncome) return 4;
  if (isGrowth && !isIncome) return 3;
  if (!isIncome) return 2;
  return 1;
};

const nameKeys = (value) => {
  const base = canon(value);
  const compact = base.replace(/\band\b/g, " ").replace(/\s+/g, " ").trim();
  const noSpaces = compact.replace(/\s+/g, "");
  return [...new Set([base, compact, noSpaces].filter(Boolean))];
};

const toIsoDate = (value) => {
  if (!value) return "";
  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString().slice(0, 10);
};

async function loadAppFundLookup() {
  if (appFundLookupPromise) return appFundLookupPromise;
  appFundLookupPromise = readFile(backupJsonPath, "utf8")
    .then((content) => JSON.parse(content))
    .then((data) => {
      const appFunds = [];
      for (const fund of data?.funds || []) {
        const rawNames = [
          clean(fund?.fundName),
          clean(fund?.rawFundName)
        ].filter(Boolean);
        const keys = new Set([
          ...nameKeys(fund?.fundName),
          ...nameKeys(fund?.rawFundName)
        ]);
        appFunds.push({
          id: fund.id || `fund-${keyOf(fund?.category)}-${keyOf(fund?.fundName)}`,
          category: clean(fund?.category),
          fundName: clean(fund?.fundName),
          rawFundName: clean(fund?.rawFundName),
          aliases: rawNames.map((name) => name.toLowerCase()),
          keys: [...keys].filter(Boolean)
        });
      }
      return appFunds;
    })
    .catch((error) => {
      logger.warn("Snapshot lookup fallback unavailable", error?.message || error);
      return [];
    });
  return appFundLookupPromise;
}

export async function buildLiveSnapshotPayload() {
  const allNavData = await getAllFunds();
  const appFunds = await loadAppFundLookup();
  const previousSnapshot = readSnapshotFile();
  console.log("AMFI total records:", allNavData.length);
  console.log("App fund targets:", appFunds.length);
  const preparedRows = allNavData
    .map((fund) => ({
      schemeCode: String(fund?.schemeCode || ""),
      schemeName: clean(fund?.schemeName),
      schemeNameLower: clean(fund?.schemeName).toLowerCase(),
      schemeNameCanon: canon(fund?.schemeName),
      isinGrowth: String(fund?.isinGrowth || fund?.isin || ""),
      nav: Number(fund?.nav),
      date: String(fund?.date || toIsoDate(fund?.navDate) || ""),
      source: "amfi"
    }))
    .filter((row) => row.schemeCode && row.schemeName && row.schemeNameCanon && row.date && Number.isFinite(row.nav));

  const scoreCandidate = (appFund, row) => {
    let best = -1;

    for (const alias of appFund.aliases || []) {
      if (!alias) continue;
      if (row.schemeNameLower.includes(alias) || alias.includes(row.schemeNameLower)) {
        best = Math.max(best, 1000 + alias.length);
      }
    }

    for (const key of appFund.keys || []) {
      if (!key) continue;
      if (row.schemeNameCanon.includes(key) || key.includes(row.schemeNameCanon)) {
        best = Math.max(best, key.length);
      }
    }

    if (best >= 0) return best;

    const rowTokens = new Set(row.schemeNameCanon.split(" ").filter(Boolean));
    let overlap = 0;
    for (const key of appFund.keys || []) {
      const tokens = key.split(" ").filter(Boolean);
      const shared = tokens.filter((token) => rowTokens.has(token)).length;
      if (shared >= 2) {
        overlap = Math.max(overlap, shared * 10 + key.length);
      }
    }
    return overlap > 0 ? overlap : -1;
  };

  const matchedItems = [];

  for (const appFund of appFunds) {
    let bestRow = null;
    let bestScore = -1;

    for (const row of preparedRows) {
      const score = scoreCandidate(appFund, row);
      if (score < 0) continue;
      if (!bestRow) {
        bestRow = row;
        bestScore = score;
        continue;
      }

      const bestDate = new Date(`${bestRow.date}T00:00:00`).getTime();
      const nextDate = new Date(`${row.date}T00:00:00`).getTime();
      const bestPriority = liveRowPriority(bestRow.schemeName);
      const nextPriority = liveRowPriority(row.schemeName);

      if (
        score > bestScore ||
        (score === bestScore && nextDate > bestDate) ||
        (score === bestScore && nextDate === bestDate && nextPriority > bestPriority)
      ) {
        bestRow = row;
        bestScore = score;
      }
    }

    if (!bestRow) continue;

    matchedItems.push({
      targetId: appFund.id,
      schemeCode: bestRow.schemeCode,
      schemeName: bestRow.schemeName,
      isinGrowth: bestRow.isinGrowth,
      nav: bestRow.nav,
      date: bestRow.date,
      source: bestRow.source
    });
  }

  const uniqueByTarget = new Map();
  for (const item of matchedItems) {
    if (!uniqueByTarget.has(item.targetId)) {
      uniqueByTarget.set(item.targetId, item);
      continue;
    }
    const current = uniqueByTarget.get(item.targetId);
    const currentDate = new Date(`${current.date}T00:00:00`).getTime();
    const nextDate = new Date(`${item.date}T00:00:00`).getTime();
    const currentPriority = liveRowPriority(current.schemeName);
    const nextPriority = liveRowPriority(item.schemeName);
    if (
      nextDate > currentDate ||
      (nextDate === currentDate && nextPriority > currentPriority)
    ) {
      uniqueByTarget.set(item.targetId, item);
    }
  }

  const items = [...uniqueByTarget.values()].sort((left, right) => {
    const dateDelta = String(right.date || "").localeCompare(String(left.date || ""));
    if (dateDelta !== 0) return dateDelta;
    const priorityDelta = liveRowPriority(right.schemeName) - liveRowPriority(left.schemeName);
    if (priorityDelta !== 0) return priorityDelta;
    return left.schemeName.localeCompare(right.schemeName);
  });

  console.log("Filtered funds count:", items.length);
  console.log("Sample matched names:", items.slice(0, 10).map((fund) => fund.schemeName));

  const safeItems = items.length
    ? items
    : (Array.isArray(previousSnapshot?.items) ? previousSnapshot.items : []);
  if (!items.length && safeItems.length) {
    logger.warn(`Snapshot matcher produced 0 items, reusing previous snapshot with ${safeItems.length} funds`);
  }

  const latestDate = safeItems.reduce((latest, fund) => {
    const current = String(fund?.date || "");
    return current > latest ? current : latest;
  }, String(previousSnapshot?.latestDate || ""));
  const lastFetchTimestamp = allNavData
    .map((fund) => fund?.lastUpdated instanceof Date ? fund.lastUpdated.getTime() : new Date(fund?.lastUpdated || 0).getTime())
    .filter(Number.isFinite)
    .sort((left, right) => right - left)
    .at(0) || 0;
  return {
    generatedAt: new Date().toISOString(),
    latestDate,
    lastFetchTimestamp: lastFetchTimestamp ? new Date(lastFetchTimestamp).toISOString() : "",
    count: safeItems.length,
    items: safeItems
  };
}

router.get("/health", async (_req, res) => {
  res.json({ status: "ok" });
});

router.get("/meta/last-updated", async (_req, res) => {
  const latest = await getLatestFund();
  res.json({
    lastUpdated: latest?.lastUpdated || null,
    latestNavDate: toIsoDate(latest?.navDate),
    totalFunds: await getFundCount()
  });
});

router.get("/update-nav", async (_req, res) => {
  try {
    logger.info("NAV update trigger received");
    const result = await triggerNavUpdate();
    console.log("Returning result");
    res.json({
      success: true,
      message: "NAV updated",
      result: result || null
    });
  } catch (error) {
    logger.error("NAV update trigger failed", error?.message || error);
    res.status(500).json({
      success: false,
      message: "NAV update failed",
      error: error?.message || "Unknown error"
    });
  }
});

router.get("/nav", async (_req, res, next) => {
  try {
    res.set("Cache-Control", "public, max-age=60");
    ensureSnapshotFile();
    const data = JSON.parse(fs.readFileSync(snapshotFilePath, "utf-8"));
    res.json({
      latestDate: data.latestDate || "",
      items: Array.isArray(data.items) ? data.items : []
    });
  } catch (error) {
    next(error);
  }
});

router.get("/funds", async (req, res, next) => {
  try {
    res.set("Cache-Control", "public, max-age=60");
    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 5000);
    const cacheKey = `funds:${page}:${limit}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    const [items, total] = await Promise.all([
      getAllFunds().then((rows) => rows.slice((page - 1) * limit, (page - 1) * limit + limit)),
      getFundCount()
    ]);

    const payload = { page, limit, total, items };
    setCached(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

router.get("/api/snapshot", async (_req, res, next) => {
  try {
    res.set("Cache-Control", "public, max-age=60");
    const cached = getCached("snapshot");
    if (cached) return res.json(cached);
    const payload = readSnapshotFile();
    setCached("snapshot", payload, 15 * 60 * 1000);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

router.get("/api/cron", async (_req, res, next) => {
  try {
    const result = await triggerNavUpdate();
    res.json({
      ok: true,
      result: result || null
    });
  } catch (error) {
    next(error);
  }
});

router.get("/fund/:schemeCode", async (req, res, next) => {
  try {
    const fund = await findFundBySchemeCode(String(req.params.schemeCode));
    if (!fund) return res.status(404).json({ error: "Fund not found" });
    res.json(fund);
  } catch (error) {
    next(error);
  }
});

router.get("/search", async (req, res, next) => {
  try {
    const query = String(req.query.q || "").trim();
    if (!query) return res.json({ items: [] });

    const cacheKey = `search:${query.toLowerCase()}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    const items = await searchFunds(query);

    const payload = { items };
    setCached(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

export { router as fundRoutes };

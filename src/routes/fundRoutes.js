import express from "express";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { env } from "../config/env.js";
import { findFundBySchemeCode, getAllFunds, getFundCount, getLatestFund, searchFunds } from "../services/navStore.js";
import { triggerNavUpdate } from "../jobs/navUpdater.js";
import { readSnapshotFile } from "../services/snapshotStore.js";
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

const safeResponse = (obj) => {
  const out = {};
  for (const [key, value] of Object.entries(obj || {})) {
    if (!Array.isArray(value)) out[key] = value;
  }
  if (typeof obj?.items?.length === "number") out.count = obj.items.length;
  return out;
};

const summariseNavUpdate = (result = {}, fallback = {}) => safeResponse({
  status: String(result?.status || fallback?.status || "unknown"),
  latestDate: String(result?.latestDate || fallback?.latestDate || ""),
  count: typeof result?.count === "number"
    ? result.count
    : typeof result?.snapshotCount === "number"
      ? result.snapshotCount
      : Array.isArray(result?.items)
        ? result.items.length
        : Number(fallback?.count || 0),
  generatedAt: String(result?.generatedAt || fallback?.generatedAt || ""),
  durationMs: Number(result?.durationMs || fallback?.durationMs || 0),
  ...(result?.skipped ? { skipped: true } : {}),
  ...(result?.reason ? { reason: String(result.reason) } : {})
});

const istIsoDate = (offsetDays = 0) => {
  const istNow = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  istNow.setUTCDate(istNow.getUTCDate() + offsetDays);
  return istNow.toISOString().slice(0, 10);
};

const isGeneratedWithinHours = (value, hours) => {
  const generatedAt = new Date(value || "");
  if (Number.isNaN(generatedAt.getTime())) return false;
  return Date.now() - generatedAt.getTime() <= hours * 60 * 60 * 1000;
};

const shouldSkipRedundantNavUpdate = (snapshot) => {
  const latestDate = String(snapshot?.latestDate || "");
  return (latestDate === istIsoDate() || latestDate === istIsoDate(-1))
    && Number(snapshot?.count || 0) > 1000
    && isGeneratedWithinHours(snapshot?.generatedAt, 23);
};

async function handleNavUpdateRequest(_req, res) {
  const startedAt = Date.now();
  try {
    logger.info("NAV update trigger received");

    const existing = readSnapshotFile();
    if (shouldSkipRedundantNavUpdate(existing)) {
      return res.status(200).json(safeResponse({
        status: "skipped",
        latestDate: existing.latestDate,
        count: existing.count || 0,
        generatedAt: existing.generatedAt,
        durationMs: Date.now() - startedAt,
        skipped: true,
        reason: "already fresh for today"
      }));
    }

    const result = await triggerNavUpdate();
    const updated = readSnapshotFile();
    return res.status(200).json(summariseNavUpdate(result, {
      latestDate: updated.latestDate,
      count: updated.count,
      generatedAt: updated.generatedAt,
      durationMs: Date.now() - startedAt
    }));
  } catch (error) {
    logger.error("NAV update trigger failed", error?.message || error);
    return res.status(500).json(safeResponse({
      status: "error",
      latestDate: "",
      count: 0,
      generatedAt: "",
      durationMs: Date.now() - startedAt,
      error: String(error?.message || "NAV update failed")
    }));
  }
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
  console.log("AMFI total records:", allNavData.length);
  const items = allNavData
    .map((fund, index) => ({
      targetId: `amfi-${index + 1}`,
      schemeCode: String(fund?.schemeCode || ""),
      schemeName: clean(fund?.schemeName),
      isinGrowth: String(fund?.isinGrowth || fund?.isin || ""),
      nav: Number(fund?.nav),
      date: String(fund?.date || toIsoDate(fund?.navDate) || ""),
      source: "amfi"
    }))
    .filter((row) => row.schemeCode && row.schemeName && row.date && Number.isFinite(row.nav));

  console.log("Snapshot full AMFI count:", items.length);
  console.log("Sample AMFI names:", items.slice(0, 10).map((fund) => fund.schemeName));

  const latestDate = items.reduce((latest, fund) => {
    const current = String(fund?.date || "");
    return current > latest ? current : latest;
  }, "");
  const lastFetchTimestamp = allNavData
    .map((fund) => fund?.lastUpdated instanceof Date ? fund.lastUpdated.getTime() : new Date(fund?.lastUpdated || 0).getTime())
    .filter(Number.isFinite)
    .sort((left, right) => right - left)
    .at(0) || 0;
  return {
    generatedAt: new Date().toISOString(),
    latestDate,
    lastFetchTimestamp: lastFetchTimestamp ? new Date(lastFetchTimestamp).toISOString() : "",
    count: items.length,
    items
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

router.get("/update-nav", handleNavUpdateRequest);
router.post("/update-nav", handleNavUpdateRequest);

router.get("/nav", async (_req, res, next) => {
  try {
    res.set("Cache-Control", "public, max-age=60");
    const data = readSnapshotFile();
    res.json(data);
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
  const startedAt = Date.now();
  try {
    const existing = readSnapshotFile();
    if (shouldSkipRedundantNavUpdate(existing)) {
      return res.json(summariseNavUpdate({
        status: "no-new-nav",
        latestDate: existing.latestDate,
        count: existing.count,
        generatedAt: existing.generatedAt,
        durationMs: Date.now() - startedAt,
        skipped: true
      }));
    }

    const result = await triggerNavUpdate();
    const updated = readSnapshotFile();
    res.json(summariseNavUpdate(result, {
      latestDate: updated.latestDate,
      count: updated.count,
      generatedAt: updated.generatedAt,
      durationMs: Date.now() - startedAt
    }));
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


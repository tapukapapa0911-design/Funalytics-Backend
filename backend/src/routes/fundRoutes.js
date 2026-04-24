import express from "express";
import { Fund } from "../models/Fund.js";
import { env } from "../config/env.js";

const router = express.Router();
const responseCache = new Map();

function getCached(key) {
  const entry = responseCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    responseCache.delete(key);
    return null;
  }
  return entry.payload;
}

function setCached(key, payload) {
  responseCache.set(key, {
    payload,
    expiresAt: Date.now() + env.cacheTtlMs
  });
}

router.get("/health", async (_req, res) => {
  const latest = await Fund.findOne().sort({ lastUpdated: -1 }).lean();
  res.json({
    ok: true,
    latestUpdate: latest?.lastUpdated || null
  });
});

router.get("/meta/last-updated", async (_req, res) => {
  const latest = await Fund.findOne().sort({ lastUpdated: -1 }).lean();
  res.json({
    lastUpdated: latest?.lastUpdated || null,
    totalFunds: await Fund.estimatedDocumentCount()
  });
});

router.get("/funds", async (req, res, next) => {
  try {
    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 5000);
    const cacheKey = `funds:${page}:${limit}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    const [items, total] = await Promise.all([
      Fund.find({})
        .sort({ schemeName: 1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .lean(),
      Fund.estimatedDocumentCount()
    ]);

    const payload = { page, limit, total, items };
    setCached(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

router.get("/fund/:schemeCode", async (req, res, next) => {
  try {
    const fund = await Fund.findOne({ schemeCode: String(req.params.schemeCode) }).lean();
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

    const items = await Fund.find(
      { $text: { $search: query } },
      { score: { $meta: "textScore" }, schemeCode: 1, schemeName: 1, nav: 1, navDate: 1, lastUpdated: 1 }
    )
      .sort({ score: { $meta: "textScore" } })
      .limit(50)
      .lean();

    const payload = { items };
    setCached(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

export { router as fundRoutes };

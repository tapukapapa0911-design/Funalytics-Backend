import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose from "mongoose";
import { Fund } from "../models/Fund.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const cacheDir = path.resolve(__dirname, "../../data");
const cachePath = path.join(cacheDir, "nav-cache.json");

function isDbReady() {
  return mongoose.connection.readyState === 1;
}

function toIsoDate(value) {
  if (!value) return "";
  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString().slice(0, 10);
}

async function readFileCache() {
  try {
    const raw = await readFile(cachePath, "utf8");
    const parsed = JSON.parse(raw);
    return {
      latestDate: parsed?.latestDate || "",
      updatedAt: parsed?.updatedAt || "",
      items: Array.isArray(parsed?.items) ? parsed.items : []
    };
  } catch {
    return { latestDate: "", updatedAt: "", items: [] };
  }
}

async function writeFileCache(items = []) {
  await mkdir(cacheDir, { recursive: true });
  const normalizedItems = items.map((item) => ({
    schemeCode: String(item.schemeCode || ""),
    schemeName: String(item.schemeName || ""),
    nav: Number(item.nav),
    isin: String(item.isin || ""),
    navDate: toIsoDate(item.navDate),
    lastUpdated: item.lastUpdated ? new Date(item.lastUpdated).toISOString() : new Date().toISOString(),
    source: String(item.source || "amfi")
  })).filter((item) => item.schemeCode && item.schemeName && Number.isFinite(item.nav) && item.navDate);
  const latestDate = normalizedItems.map((item) => item.navDate).sort().at(-1) || "";
  const payload = {
    latestDate,
    updatedAt: new Date().toISOString(),
    items: normalizedItems
  };
  await writeFile(cachePath, JSON.stringify(payload), "utf8");
  return payload;
}

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

export async function getAllFunds() {
  if (isDbReady()) {
    return Fund.find({}).sort({ schemeName: 1 }).lean();
  }
  const fileCache = await readFileCache();
  return [...fileCache.items].sort((left, right) => String(left.schemeName).localeCompare(String(right.schemeName)));
}

export async function getLatestFund() {
  if (isDbReady()) {
    return Fund.findOne().sort({ navDate: -1, lastUpdated: -1 }).lean();
  }
  const fileCache = await readFileCache();
  return [...fileCache.items]
    .sort((left, right) => {
      const dateDelta = String(right.navDate || "").localeCompare(String(left.navDate || ""));
      if (dateDelta !== 0) return dateDelta;
      return String(right.lastUpdated || "").localeCompare(String(left.lastUpdated || ""));
    })
    .at(0) || null;
}

export async function getFundCount() {
  if (isDbReady()) {
    return Fund.estimatedDocumentCount();
  }
  const fileCache = await readFileCache();
  return fileCache.items.length;
}

export async function getLatestNavDate() {
  const latest = await getLatestFund();
  return toIsoDate(latest?.navDate);
}

export async function saveNavRecords(records = []) {
  if (isDbReady()) {
    const bulkResult = await Fund.bulkWrite(buildBulkOperations(records), { ordered: false });
    const items = await Fund.find({}).sort({ schemeName: 1 }).lean();
    await writeFileCache(items);
    return bulkResult;
  }
  const nowIso = new Date().toISOString();
  const items = records.map((record) => ({
    schemeCode: record.schemeCode,
    schemeName: record.schemeName,
    nav: record.nav,
    isin: record.isin || "",
    navDate: toIsoDate(record.navDate),
    lastUpdated: nowIso,
    source: record.source || "amfi"
  }));
  await writeFileCache(items);
  return {
    upsertedCount: items.length,
    modifiedCount: 0
  };
}

export async function findFundBySchemeCode(schemeCode) {
  if (isDbReady()) {
    return Fund.findOne({ schemeCode: String(schemeCode) }).lean();
  }
  const fileCache = await readFileCache();
  return fileCache.items.find((item) => String(item.schemeCode) === String(schemeCode)) || null;
}

export async function searchFunds(query) {
  const term = String(query || "").trim().toLowerCase();
  if (!term) return [];
  if (isDbReady()) {
    return Fund.find(
      { $text: { $search: query } },
      { score: { $meta: "textScore" }, schemeCode: 1, schemeName: 1, nav: 1, navDate: 1, lastUpdated: 1 }
    )
      .sort({ score: { $meta: "textScore" } })
      .limit(50)
      .lean();
  }
  const fileCache = await readFileCache();
  return fileCache.items
    .filter((item) => String(item.schemeName || "").toLowerCase().includes(term))
    .slice(0, 50);
}

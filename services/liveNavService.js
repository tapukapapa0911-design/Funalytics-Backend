window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.liveNavService = (() => {
  const schema = window.LiveDataVersion.schema;
  const cache = window.LiveDataVersion.cache;
  const api = window.LiveDataVersion.apiClients;
  const matcher = window.LiveDataVersion.matcher;

  const NAV_CACHE_KEY = schema.cache.navResolverKey;
  const NAV_FALLBACK_KEY = schema.cache.navFallbackKey;
  const MATCH_CACHE_KEY = schema.cache.navMatchMapKey;
  const NAV_TTL_MS = 15 * 60 * 1000;
  const FALLBACK_TTL_MS = 24 * 60 * 60 * 1000;
  const MASTER_TTL_MS = 10 * 60 * 1000;
  const AGREEMENT_TOLERANCE = 0.005;
  let masterRowsMemo = { savedAt: 0, rows: [] };
  let masterRowsInFlight = null;

  const DEBUG = () => Boolean(
    window.LIVE_CONFIG?.debugNav ||
    window.LiveDataVersion?.config?.debugNav ||
    window.__FUNALYTICS_NAV_DEBUG__
  );

  const log = (...args) => {
    if (DEBUG()) console.info("[live-data-version][liveNavService]", ...args);
  };

  const warn = (...args) => {
    if (DEBUG()) console.warn("[live-data-version][liveNavService]", ...args);
  };

  const normalizeDate = (value) => api.parseDisplayDate ? api.parseDisplayDate(value) : String(value || "");
  const dateValue = (value) => {
    const iso = normalizeDate(value);
    if (!iso) return 0;
    const parsed = new Date(`${iso}T00:00:00`);
    return Number.isNaN(parsed.getTime()) ? 0 : parsed.getTime();
  };

  const navCache = () => cache.readJson(NAV_CACHE_KEY) || { savedAt: 0, byFundId: {} };
  const fallbackCache = () => cache.readJson(NAV_FALLBACK_KEY) || { savedAt: 0, byFundId: {} };
  const matchCache = () => cache.readJson(MATCH_CACHE_KEY) || { savedAt: 0, mappings: {} };

  const saveFundNav = (fundId, payload) => {
    const next = navCache();
    next.byFundId[fundId] = payload;
    next.savedAt = Date.now();
    cache.writeJson(NAV_CACHE_KEY, next);

    const fallback = fallbackCache();
    fallback.byFundId[fundId] = payload;
    fallback.savedAt = Date.now();
    cache.writeJson(NAV_FALLBACK_KEY, fallback);
  };

  const getCachedFundNav = (fundId) => {
    const fresh = navCache();
    if (cache.isFresh(fresh, NAV_TTL_MS) && fresh.byFundId?.[fundId]) {
      return fresh.byFundId[fundId];
    }
    const fallback = fallbackCache();
    if (cache.isFresh(fallback, FALLBACK_TTL_MS) && fallback.byFundId?.[fundId]) {
      return fallback.byFundId[fundId];
    }
    return null;
  };

  const persistMatch = (fundId, payload) => {
    const current = matchCache();
    current.mappings[fundId] = payload;
    current.savedAt = Date.now();
    cache.writeJson(MATCH_CACHE_KEY, current);
  };

  const scoreAgreement = (left, right) => {
    if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
    const base = Math.max(Math.abs(left), Math.abs(right), 1);
    return Math.abs(left - right) / base <= AGREEMENT_TOLERANCE;
  };

  const chooseBestNav = (values) => {
    const valid = values.filter((entry) => Number.isFinite(Number(entry?.nav)));
    if (!valid.length) return null;
    const sourcePriority = (source = "") => {
      if (source === "amfi-master") return 4;
      if (source === "amfi-history") return 3;
      return 0;
    };
    const ordered = [...valid].sort((left, right) => {
      const dateDelta = dateValue(right.date) - dateValue(left.date);
      if (dateDelta !== 0) return dateDelta;
      return sourcePriority(right.source) - sourcePriority(left.source);
    });
    if (ordered.length === 1) return { chosen: ordered[0], warning: true, agreedSources: [ordered[0].source] };

    const newestDate = dateValue(ordered[0].date);
    const newest = ordered.filter((entry) => dateValue(entry.date) === newestDate);
    const newestAmfi = newest.find((entry) => entry.source === "amfi-master" || entry.source === "amfi-history");
    if (newestAmfi) {
      const agreeingNewest = newest.filter((entry) => scoreAgreement(Number(newestAmfi.nav), Number(entry.nav)));
      if (agreeingNewest.length >= 2) {
        const avg = agreeingNewest.reduce((sum, entry) => sum + Number(entry.nav), 0) / agreeingNewest.length;
        return {
          chosen: { ...newestAmfi, nav: Number(avg.toFixed(4)) },
          warning: false,
          agreedSources: agreeingNewest.map((entry) => entry.source)
        };
      }
      return { chosen: newestAmfi, warning: false, agreedSources: [newestAmfi.source] };
    }

    for (const base of newest) {
      const agreeing = newest.filter((entry) => scoreAgreement(Number(base.nav), Number(entry.nav)));
      if (agreeing.length >= 2) {
        const avg = agreeing.reduce((sum, entry) => sum + Number(entry.nav), 0) / agreeing.length;
        return {
          chosen: { ...base, nav: Number(avg.toFixed(4)) },
          warning: false,
          agreedSources: agreeing.map((entry) => entry.source)
        };
      }
    }

    const amfi = ordered.find((entry) => entry.source === "amfi-master" || entry.source === "amfi-history");
    return {
      chosen: amfi || ordered[0],
      warning: true,
      agreedSources: [amfi?.source || ordered[0].source]
    };
  };

  const buildStaticFallback = (fund) => ({
    nav: Number(fund.latestNav ?? 0),
    date: normalizeDate(fund.latestDate),
    source: "backup",
    targetId: fund.id,
    schemeCode: String(fund.schemeCode || ""),
    schemeName: String(fund.liveSchemeName || fund.fundName || "")
  });

  const searchCandidates = async () => [];

  const buildMasterIndex = (rows) => {
    const bySchemeCode = new Map();
    const byNameKey = new Map();
    const byBrand = new Map();
    const byCategory = new Map();
    const byCategoryPrefix = new Map();

    rows.forEach((row) => {
      bySchemeCode.set(String(row.schemeCode), row);
      const nameKey = matcher.normalizedName(row.schemeName || "");
      if (nameKey && !byNameKey.has(nameKey)) byNameKey.set(nameKey, row);

      const brand = matcher.extractBrand(row.schemeName || "");
      if (brand) {
        if (!byBrand.has(brand)) byBrand.set(brand, []);
        byBrand.get(brand).push(row);
      }

      const category = matcher.extractCategory(row.schemeName || "");
      if (category) {
        if (!byCategory.has(category)) byCategory.set(category, []);
        byCategory.get(category).push(row);

        const prefix = matcher.firstToken(row.schemeName || "");
        if (prefix) {
          const bucketKey = `${category}::${prefix}`;
          if (!byCategoryPrefix.has(bucketKey)) byCategoryPrefix.set(bucketKey, []);
          byCategoryPrefix.get(bucketKey).push(row);
        }
      }
    });

    return { bySchemeCode, byNameKey, byBrand, byCategory, byCategoryPrefix };
  };

  const pickCandidatePool = (fund, masterRows, masterIndex) => {
    const fundName = fund.fundName || fund.rawFundName || "";
    const normalized = matcher.normalizedName(fundName);
    const exact = masterIndex.byNameKey.get(normalized);
    if (exact) return [exact];

    const brand = matcher.extractBrand(fundName);
    const category = matcher.extractCategory(fund.category || fundName);
    const firstPrefix = matcher.firstToken(fundName);
    const prefixTokens = normalized.split(" ").slice(0, 2);
    const pool = new Map();

    const categoryPrefixKey = `${category}::${firstPrefix}`;
    (masterIndex.byCategoryPrefix.get(categoryPrefixKey) || []).forEach((row) => pool.set(row.schemeCode, row));
    (masterIndex.byBrand.get(brand) || []).forEach((row) => pool.set(row.schemeCode, row));
    (masterIndex.byCategory.get(category) || []).forEach((row) => pool.set(row.schemeCode, row));

    if (!pool.size) {
      masterRows.forEach((row) => {
        const rowName = matcher.normalizedName(row.schemeName || "");
        if (!rowName) return;
        const prefixOk = !prefixTokens.length || prefixTokens.every((token) => rowName.includes(token));
        if (prefixOk) pool.set(row.schemeCode, row);
      });
    }

    return [...pool.values()].slice(0, 120);
  };

  const discoverCandidate = async (fund, masterRows, masterIndex) => {
    const cached = matchCache().mappings?.[fund.id];
    if (cached?.schemeCode) {
      const exact = masterIndex.bySchemeCode.get(String(cached.schemeCode));
      if (exact) return exact;
    }

    const narrowedRows = pickCandidatePool(fund, masterRows, masterIndex);
    let candidate = matcher.matchFundToScheme(fund, narrowedRows)?.row || null;
    if (candidate) {
      persistMatch(fund.id, {
        schemeCode: candidate.schemeCode,
        isinGrowth: candidate.isinGrowth || "",
        schemeName: candidate.schemeName
      });
    }

    return candidate;
  };

  const resolveMasterCandidate = (fund, masterRows, masterIndex) => {
    const cached = matchCache().mappings?.[fund.id];
    if (cached?.schemeCode) {
      const exact = masterIndex.bySchemeCode.get(String(cached.schemeCode));
      if (exact) return exact;
    }
    const narrowedRows = pickCandidatePool(fund, masterRows, masterIndex);
    const candidate = matcher.matchFundToScheme(fund, narrowedRows)?.row || null;
    if (candidate) {
      persistMatch(fund.id, {
        schemeCode: candidate.schemeCode,
        isinGrowth: candidate.isinGrowth || "",
        schemeName: candidate.schemeName
      });
    }
    return candidate;
  };

  const buildMasterRows = async () => {
    if ((Date.now() - masterRowsMemo.savedAt) <= MASTER_TTL_MS && masterRowsMemo.rows.length) {
      return masterRowsMemo.rows;
    }
    if (masterRowsInFlight) return masterRowsInFlight;

    masterRowsInFlight = (async () => {
      const merged = new Map();
      const mergeRow = (row) => {
        const key = String(row.schemeCode || matcher.normalizedName(row.schemeName || ""));
        if (!key) return;
        const existing = merged.get(key);
        if (!existing) {
          merged.set(key, row);
          return;
        }
        const existingDate = dateValue(existing.date);
        const nextDate = dateValue(row.date);
        if (nextDate > existingDate || (nextDate === existingDate && row.source === "backend-master")) {
          merged.set(key, row);
        }
      };

      try {
        const amfiRows = await api.fetchAmfiLatestNav();
        amfiRows.forEach((row) => mergeRow({ ...row, source: "amfi-master" }));
      } catch (error) {
        warn("amfi master fetch failed", error);
      }

      const rows = [...merged.values()].filter((row) => row.schemeCode && row.schemeName);
      masterRowsMemo = { savedAt: Date.now(), rows };
      return rows;
    })();

    try {
      return await masterRowsInFlight;
    } finally {
      masterRowsInFlight = null;
    }
  };

  const fetchVerifiedNav = async (fund, candidate) => {
    const cached = getCachedFundNav(fund.id);
    if (Number.isFinite(Number(candidate?.nav))) {
      const payload = {
        nav: Number(candidate.nav),
        date: normalizeDate(candidate.date || fund.latestDate),
        source: candidate?.source || "amfi-master",
        warning: false,
        agreedSources: [candidate?.source || "amfi-master"]
      };
      saveFundNav(fund.id, payload);
      return payload;
    }

    if (cached && Number.isFinite(Number(cached.nav))) {
      return { ...cached, source: "cache" };
    }

    return buildStaticFallback(fund);
  };

  const runWithConcurrency = async (items, limit, worker) => {
    const results = [];
    const queue = [...items];
    const runners = Array.from({ length: Math.min(limit, items.length || 0) }, async () => {
      while (queue.length) {
        const item = queue.shift();
        results.push(await worker(item));
      }
    });
    await Promise.all(runners);
    return results;
  };

  const fetchLiveNav = async (fund, masterRows, masterIndex) => {
    try {
      const candidate = await discoverCandidate(fund, masterRows, masterIndex);
      const verified = await fetchVerifiedNav(fund, candidate || {});
      return {
        targetId: fund.id,
        schemeCode: String(candidate?.schemeCode || fund.schemeCode || ""),
        schemeName: String(candidate?.schemeName || fund.liveSchemeName || fund.fundName || ""),
        isinGrowth: String(candidate?.isinGrowth || ""),
        nav: Number(verified.nav),
        date: normalizeDate(verified.date || fund.latestDate),
        source: verified.source,
        warning: Boolean(verified.warning)
      };
    } catch (error) {
      warn("fund nav fetch failed", fund.fundName, error);
      const fallback = getCachedFundNav(fund.id) || buildStaticFallback(fund);
      return {
        targetId: fund.id,
        schemeCode: String(fund.schemeCode || ""),
        schemeName: String(fund.liveSchemeName || fund.fundName || ""),
        isinGrowth: "",
        nav: Number(fallback.nav),
        date: normalizeDate(fallback.date || fund.latestDate),
        source: fallback.source || "backup",
        warning: true
      };
    }
  };

  const resolveLatestRows = async (funds) => {
    const masterRows = await buildMasterRows();
    const masterIndex = buildMasterIndex(masterRows);
    const fastPathRows = funds.map((fund) => {
      const candidate = resolveMasterCandidate(fund, masterRows, masterIndex);
      if (!candidate || !Number.isFinite(Number(candidate.nav))) return null;
      return {
        targetId: fund.id,
        schemeCode: String(candidate.schemeCode || fund.schemeCode || ""),
        schemeName: String(candidate.schemeName || fund.liveSchemeName || fund.fundName || ""),
        isinGrowth: String(candidate.isinGrowth || ""),
        nav: Number(candidate.nav),
        date: normalizeDate(candidate.date || fund.latestDate),
        source: `${candidate.source || "amfi-master"}-fast-path`,
        warning: false
      };
    });

    if (masterRows.length && fastPathRows.every(Boolean)) {
      return fastPathRows;
    }

    return runWithConcurrency(funds, 6, async (fund) => {
      const row = await fetchLiveNav(fund, masterRows, masterIndex);
      log("resolved", fund.fundName, row.source, row.nav);
      return row;
    });
  };

  return {
    fetchLiveNav,
    resolveLatestRows
  };
})();

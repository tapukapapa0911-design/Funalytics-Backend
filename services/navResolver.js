window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.navResolver = (() => {
  const schema = window.LiveDataVersion.schema;
  const cache = window.LiveDataVersion.cache;
  const api = window.LiveDataVersion.apiClients;
  const matcher = window.LiveDataVersion.matcher;

  const DEBUG = () => Boolean(
    window.LIVE_CONFIG?.debugNav ||
    window.LiveDataVersion?.config?.debugNav ||
    window.__FUNALYTICS_NAV_DEBUG__
  );

  const log = (...args) => {
    if (DEBUG()) console.info("[live-data-version][nav]", ...args);
  };

  const warn = (...args) => {
    if (DEBUG()) console.warn("[live-data-version][nav]", ...args);
  };

  const normalizeDate = (value) => api.parseDisplayDate ? api.parseDisplayDate(value) : String(value || "");

  const schemeListCache = () => cache.readJson(schema.cache.navSchemeListKey);
  const matchCache = () => cache.readJson(schema.cache.navMatchMapKey) || { savedAt: 0, mappings: {} };
  const navCache = () => cache.readJson(schema.cache.navResolverKey) || { savedAt: 0, byFundId: {} };
  const navFallbackCache = () => cache.readJson(schema.cache.navFallbackKey) || { savedAt: 0, byFundId: {} };

  const persistMatch = (fundId, payload) => {
    const current = matchCache();
    current.mappings[fundId] = payload;
    current.savedAt = Date.now();
    cache.writeJson(schema.cache.navMatchMapKey, current);
  };

  const persistNav = (fundId, payload, persistent = false) => {
    const volatile = navCache();
    volatile.byFundId[fundId] = payload;
    volatile.savedAt = Date.now();
    cache.writeJson(schema.cache.navResolverKey, volatile);

    if (persistent) {
      const fallback = navFallbackCache();
      fallback.byFundId[fundId] = payload;
      fallback.savedAt = Date.now();
      cache.writeJson(schema.cache.navFallbackKey, fallback);
    }
  };

  const getCachedNav = (fundId) => {
    const volatile = navCache();
    if (cache.isFresh(volatile, schema.cache.navTtlMs) && volatile.byFundId?.[fundId]) {
      return volatile.byFundId[fundId];
    }
    const fallback = navFallbackCache();
    return fallback.byFundId?.[fundId] || null;
  };

  const resolveSchemeList = async () => {
    const cached = schemeListCache();
    if (cache.isFresh(cached, schema.cache.schemeListTtlMs) && Array.isArray(cached?.rows)) {
      return cached.rows;
    }
    const rows = await api.fetchAmfiLatestNav();
    cache.writeJson(schema.cache.navSchemeListKey, { savedAt: Date.now(), rows });
    return rows;
  };

  const resolveSchemeMatch = (fund, schemeRows) => {
    const cached = matchCache().mappings?.[fund.id];
    if (cached?.schemeCode) {
      const cachedRow = schemeRows.find((row) => String(row.schemeCode) === String(cached.schemeCode));
      if (cachedRow) return { row: cachedRow, confidence: cached.confidence ?? 1, method: cached.method || "cache" };
    }

    const matched = matcher.matchFundToScheme(fund, schemeRows);
    if (matched?.row) {
      persistMatch(fund.id, {
        schemeCode: matched.row.schemeCode,
        isinGrowth: matched.row.isinGrowth || "",
        confidence: matched.confidence,
        method: matched.method,
        schemeName: matched.row.schemeName
      });
    }
    return matched;
  };

  const chooseBetween = (primary, secondary) => {
    if (!primary) return secondary || null;
    if (!secondary) return primary;
    const diff = primary.nav > 0 ? Math.abs(primary.nav - secondary.nav) / primary.nav : 0;
    if (diff > 0.01) {
      const primaryTime = Date.parse(primary.date || "");
      const secondaryTime = Date.parse(secondary.date || "");
      warn("NAV cross-check mismatch >", {
        primary,
        secondary,
        diff
      });
      if (Number.isFinite(primaryTime) && Number.isFinite(secondaryTime) && secondaryTime > primaryTime) {
        return secondary;
      }
    }
    return primary;
  };

  const fetchResolvedNav = async (fund, match) => {
    const cached = getCachedNav(fund.id);
    const amfiPayload = match?.row?.nav
      ? {
          nav: Number(match.row.nav),
          date: normalizeDate(match.row.date),
          source: "amfi"
        }
      : null;

    const resolved = amfiPayload || cached;
    if (!resolved || !Number.isFinite(Number(resolved.nav))) {
      const backupNav = Number(fund.latestNav ?? fund.nav);
      return {
        nav: Number.isFinite(backupNav) ? backupNav : 0,
        date: normalizeDate(fund.latestDate),
        source: cached ? "cache" : "backup"
      };
    }

    const payload = {
      nav: Number(resolved.nav),
      date: normalizeDate(resolved.date || match?.row?.date || fund.latestDate),
      source: resolved.source || "cache",
      confidence: match?.confidence ?? null
    };
    persistNav(fund.id, payload, true);
    return payload;
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

  const resolveLatestRows = async (funds) => {
    const schemeRows = await resolveSchemeList();
    const tasks = funds.map((fund) => ({ fund, match: resolveSchemeMatch(fund, schemeRows) }));

    const resolved = await runWithConcurrency(tasks, 6, async ({ fund, match }) => {
      const nav = await fetchResolvedNav(fund, match);
      log("resolved", fund.fundName, { source: nav.source, confidence: match?.confidence ?? null });
      return {
        schemeCode: String(match?.row?.schemeCode || fund.schemeCode || ""),
        schemeName: String(match?.row?.schemeName || fund.liveSchemeName || fund.fundName || ""),
        isinGrowth: String(match?.row?.isinGrowth || ""),
        nav: Number(nav.nav),
        date: nav.date || fund.latestDate || "",
        source: nav.source,
        targetId: fund.id,
        matchConfidence: match?.confidence ?? null
      };
    });

    return resolved.filter((row) => Number.isFinite(row.nav));
  };

  return {
    resolveLatestRows
  };
})();

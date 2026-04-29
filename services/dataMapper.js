window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.dataMapper = (() => {
  const { clone, ensureAppShape } = window.LiveDataVersion.validation;
  const { buildParameterBreakdown, scoreFromBreakdown, trendFromHistory, summarizeCategory, stdev } = window.LiveDataVersion.calculations;
  const schema = window.LiveDataVersion.schema;
  const matcher = window.LiveDataVersion.matcher;

  const canon = (value) => String(value || "")
    .toLowerCase()
    .replace(/\(g\)|regular|direct|growth|plan|fund|option|reinvestment|payout|bonus|dividend|idcw|-/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

  const nameKeys = (value) => {
    const base = canon(value);
    const compact = base.replace(/\band\b/g, " ").replace(/\s+/g, " ").trim();
    const noSpaces = compact.replace(/\s+/g, "");
    return [...new Set([base, compact, noSpaces].filter(Boolean))];
  };

  const tokenKeys = (value) => nameKeys(value)
    .flatMap((key) => key.split(" "))
    .map((token) => token.trim())
    .filter((token) => token.length >= 4);

  const buildBackupLookup = (backupData) => {
    const rows = new Map();
    for (const fund of backupData?.funds || []) {
      const keys = new Set([
        ...nameKeys(fund.fundName),
        ...nameKeys(fund.rawFundName)
      ]);
      keys.forEach((key) => {
        if (key) rows.set(key, fund);
      });
    }
    return rows;
  };

  const classifyPlan = (row) => {
    const text = String(row?.schemeName || "").toLowerCase();
    return {
      isDirect: /\bdirect\b/.test(text),
      isRegular: /\bregular\b/.test(text),
      isGrowth: /\bgrowth\b|\(g\)/.test(text),
      isIncome: /\bidcw\b|\bdividend\b|\bpayout\b|\bbonus\b/.test(text)
    };
  };

  const preferRegularRows = (rows) => {
    if (!Array.isArray(rows) || !rows.length) return [];
    const buckets = [
      rows.filter((row) => {
        const plan = classifyPlan(row);
        return !plan.isDirect && plan.isGrowth && !plan.isIncome;
      }),
      rows.filter((row) => {
        const plan = classifyPlan(row);
        return plan.isRegular && plan.isGrowth && !plan.isIncome;
      }),
      rows.filter((row) => {
        const plan = classifyPlan(row);
        return !plan.isDirect && !plan.isIncome;
      }),
      rows.filter((row) => {
        const plan = classifyPlan(row);
        return plan.isGrowth && !plan.isIncome;
      }),
      rows.filter((row) => {
        const plan = classifyPlan(row);
        return !plan.isIncome;
      }),
      rows
    ];
    return buckets.find((bucket) => bucket.length) || rows;
  };

  const normalizeSnapshotRows = (backupData, latestRows) => {
    const snapshotRows = Array.isArray(latestRows) ? latestRows : [];
    const snapshotDates = snapshotRows
      .map((row) => String(row?.date || row?.navDate || "").slice(0, 10))
      .filter(Boolean)
      .sort();
    const snapshotLatestDate = snapshotDates.at(-1) || "";

    console.log("Snapshot funds:", snapshotRows.length);

    const validFunds = snapshotRows.filter((fund) => {
      const schemeCode = String(fund?.schemeCode || "").trim();
      const schemeName = String(fund?.schemeName || "").trim();
      const nav = Number(fund?.nav);
      const date = String(fund?.date || fund?.navDate || "").slice(0, 10);
      if (!schemeCode || !schemeName || !Number.isFinite(nav) || !date) return false;
      if (snapshotLatestDate && date !== snapshotLatestDate) return false;
      return true;
    });

    console.log("Filtered valid funds:", validFunds.length);

    const growthFunds = preferRegularRows(validFunds).length === validFunds.length
      ? validFunds.filter((fund) => {
          const plan = classifyPlan(fund);
          return !plan.isDirect && plan.isGrowth && !plan.isIncome;
        })
      : validFunds.filter((fund) => {
          const plan = classifyPlan(fund);
          return !plan.isDirect && plan.isGrowth && !plan.isIncome;
        });

    console.log("Growth funds:", growthFunds.length);

    return growthFunds.map((fund) => ({
      schemeName: String(fund.schemeName || "").trim(),
      nav: Number(fund.nav),
      date: String(fund.date || fund.navDate || "").slice(0, 10),
      schemeCode: String(fund.schemeCode || "").trim(),
      isinGrowth: String(fund.isinGrowth || fund.isin || "").trim(),
      targetId: String(fund.targetId || "").trim(),
      source: String(fund.source || "render-snapshot")
    }));
  };

  const yieldToBrowser = () => new Promise((resolve) => {
    if ("requestAnimationFrame" in window) {
      window.requestAnimationFrame(() => window.setTimeout(resolve, 0));
      return;
    }
    window.setTimeout(resolve, 0);
  });

  const mergeLatestNavSync = (backupData, latestRows) => {
    const next = clone(backupData);
    const normalizedRows = normalizeSnapshotRows(backupData, latestRows);
    const mappedFunds = [];
    const latestByTargetId = new Map();
    const latestBySchemeCode = new Map();
    const latestByNameKey = new Map();
    const latestByToken = new Map();

    const pushUnique = (bucket, row) => {
      if (!bucket || !row) return;
      if (!bucket.some((entry) => String(entry?.schemeCode || "") === String(row?.schemeCode || ""))) {
        bucket.push(row);
      }
    };

    for (const row of normalizedRows) {
      if (row?.targetId) {
        if (!latestByTargetId.has(row.targetId)) latestByTargetId.set(row.targetId, []);
        latestByTargetId.get(row.targetId).push(row);
      }
      if (row?.schemeCode) {
        latestBySchemeCode.set(String(row.schemeCode), row);
      }
      for (const key of nameKeys(row?.schemeName)) {
        if (!key) continue;
        if (!latestByNameKey.has(key)) latestByNameKey.set(key, []);
        latestByNameKey.get(key).push(row);
      }
      for (const token of tokenKeys(row?.schemeName)) {
        if (!latestByToken.has(token)) latestByToken.set(token, []);
        latestByToken.get(token).push(row);
      }
    }

    for (const fund of backupData?.funds || []) {
      const candidates = [];

      (latestByTargetId.get(fund.id) || []).forEach((row) => pushUnique(candidates, row));

      if (fund.schemeCode) {
        pushUnique(candidates, latestBySchemeCode.get(String(fund.schemeCode)) || null);
      }

      [...nameKeys(fund.fundName), ...nameKeys(fund.rawFundName)]
        .forEach((key) => (latestByNameKey.get(key) || []).forEach((row) => pushUnique(candidates, row)));

      if (!candidates.length) {
        [...tokenKeys(fund.fundName), ...tokenKeys(fund.rawFundName)]
          .forEach((token) => (latestByToken.get(token) || []).forEach((row) => pushUnique(candidates, row)));
      }

      let liveMatch = null;
      if (candidates.length && matcher?.matchFundToScheme) {
        liveMatch = matcher.matchFundToScheme(fund, preferRegularRows(candidates))?.row || null;
      }

      if (!liveMatch || !Number.isFinite(Number(liveMatch.nav))) continue;
      mappedFunds.push({
        schemeCode: liveMatch.schemeCode,
        schemeName: liveMatch.schemeName,
        isinGrowth: liveMatch.isinGrowth,
        targetId: fund.id,
        latestNav: liveMatch.nav,
        liveNavDate: liveMatch.date
      });
    }

    const mappedById = new Map(mappedFunds.map((entry) => [entry.targetId, entry]));
    next.funds = next.funds.map((fund) => {
      const liveMatch = mappedById.get(fund.id);
      if (!liveMatch) return fund;
      return {
        ...fund,
        schemeCode: liveMatch.schemeCode,
        liveSchemeName: liveMatch.schemeName,
        latestNav: liveMatch.latestNav,
        liveNavDate: liveMatch.liveNavDate || fund.liveNavDate || fund.latestNavDate || null
      };
    });

    next.liveNavDate = next.funds.map((fund) => fund.liveNavDate).filter(Boolean).sort().at(-1) || next.liveNavDate || null;
    return { data: next, mappedFunds };
  };

  const mergeLatestNav = async (backupData, latestRows, options = {}) => {
    const next = clone(backupData);
    const normalizedRows = normalizeSnapshotRows(backupData, latestRows);
    const mappedFunds = [];
    const latestByTargetId = new Map();
    const latestBySchemeCode = new Map();
    const latestByNameKey = new Map();
    const latestByToken = new Map();
    const rowChunkSize = Math.max(100, Number(options.rowChunkSize) || 500);
    const fundChunkSize = Math.max(5, Number(options.fundChunkSize) || 20);

    const pushUnique = (bucket, row) => {
      if (!bucket || !row) return;
      if (!bucket.some((entry) => String(entry?.schemeCode || "") === String(row?.schemeCode || ""))) {
        bucket.push(row);
      }
    };

    for (let index = 0; index < normalizedRows.length; index += 1) {
      const row = normalizedRows[index];
      if (row?.targetId) {
        if (!latestByTargetId.has(row.targetId)) latestByTargetId.set(row.targetId, []);
        latestByTargetId.get(row.targetId).push(row);
      }
      if (row?.schemeCode) {
        latestBySchemeCode.set(String(row.schemeCode), row);
      }
      for (const key of nameKeys(row?.schemeName)) {
        if (!key) continue;
        if (!latestByNameKey.has(key)) latestByNameKey.set(key, []);
        latestByNameKey.get(key).push(row);
      }
      for (const token of tokenKeys(row?.schemeName)) {
        if (!latestByToken.has(token)) latestByToken.set(token, []);
        latestByToken.get(token).push(row);
      }
      if ((index + 1) % rowChunkSize === 0) {
        await yieldToBrowser();
      }
    }

    const funds = Array.isArray(backupData?.funds) ? backupData.funds : [];
    for (let index = 0; index < funds.length; index += 1) {
      const fund = funds[index];
      const candidates = [];

      (latestByTargetId.get(fund.id) || []).forEach((row) => pushUnique(candidates, row));

      if (fund.schemeCode) {
        pushUnique(candidates, latestBySchemeCode.get(String(fund.schemeCode)) || null);
      }

      [...nameKeys(fund.fundName), ...nameKeys(fund.rawFundName)]
        .forEach((key) => (latestByNameKey.get(key) || []).forEach((row) => pushUnique(candidates, row)));

      if (!candidates.length) {
        [...tokenKeys(fund.fundName), ...tokenKeys(fund.rawFundName)]
          .forEach((token) => (latestByToken.get(token) || []).forEach((row) => pushUnique(candidates, row)));
      }

      let liveMatch = null;
      if (candidates.length && matcher?.matchFundToScheme) {
        liveMatch = matcher.matchFundToScheme(fund, preferRegularRows(candidates))?.row || null;
      }

      if (liveMatch && Number.isFinite(Number(liveMatch.nav))) {
        mappedFunds.push({
          schemeCode: liveMatch.schemeCode,
          schemeName: liveMatch.schemeName,
          isinGrowth: liveMatch.isinGrowth,
          targetId: fund.id,
          latestNav: liveMatch.nav,
          liveNavDate: liveMatch.date
        });
      }

      if ((index + 1) % fundChunkSize === 0) {
        await yieldToBrowser();
      }
    }

    const mappedById = new Map(mappedFunds.map((entry) => [entry.targetId, entry]));
    const nextFunds = [];
    for (let index = 0; index < next.funds.length; index += 1) {
      const fund = next.funds[index];
      const liveMatch = mappedById.get(fund.id);
      nextFunds.push(liveMatch ? {
        ...fund,
        schemeCode: liveMatch.schemeCode,
        liveSchemeName: liveMatch.schemeName,
        latestNav: liveMatch.latestNav,
        liveNavDate: liveMatch.liveNavDate || fund.liveNavDate || fund.latestNavDate || null
      } : fund);

      if ((index + 1) % fundChunkSize === 0) {
        await yieldToBrowser();
      }
    }

    next.funds = nextFunds;
    next.liveNavDate = next.funds.map((fund) => fund.liveNavDate).filter(Boolean).sort().at(-1) || next.liveNavDate || null;
    return { data: next, mappedFunds };
  };

  const mergeLatestNavChunked = (backupData, latestRows, options = {}) => mergeLatestNav(backupData, latestRows, options);

  const applyHistoryToFunds = (data, historiesByFundId) => {
    const next = clone(data);

    next.funds = next.funds.map((fund) => {
      const liveSeries = historiesByFundId[fund.id];
      if (!Array.isArray(liveSeries) || !liveSeries.length) return fund;

      const calc = window.LiveDataVersion.calculations;
      const { deriveTrailingReturn, sharpeFromHistory, sortinoFromHistory, volatilityFromHistory } = calc;

      // ── Returns from NAV history (CAGR) ────────────────────────────────────────
      const oneYear   = deriveTrailingReturn(liveSeries, 1) ?? fund.oneYear;
      const threeYear = deriveTrailingReturn(liveSeries, 3) ?? fund.threeYear;
      const fiveYear  = deriveTrailingReturn(liveSeries, 5) ?? fund.fiveYear;

      // ── Risk metrics computed from 3Y monthly NAV series ──────────────────────
      // All three use identical monthly-returns window to match Excel/SEBI convention
      const liveSharpe    = sharpeFromHistory(liveSeries);
      const liveSortino   = sortinoFromHistory(liveSeries);
      const liveVolatility = volatilityFromHistory(liveSeries);

      // Fall back to backup if live computation yields null
      // (e.g. fund has less than 12 months of data)
      const sharpe     = liveSharpe    ?? fund.sharpe;
      const sortino    = liveSortino   ?? fund.sortino;
      const volatility = liveVolatility ?? fund.volatility;

      // ── Latest NAV for fund card display ──────────────────────────────────────
      // liveSeries is ascending (oldest first), so last item is most recent
      const latestNavPoint = liveSeries.at(-1);
      const latestNav  = latestNavPoint?.nav ?? fund.latestNav ?? null;
      const latestDate = latestNavPoint?.date ?? fund.latestDate;

      // ── PE and PB ──────────────────────────────────────────────────────────────
      // These are portfolio fundamental metrics (not derivable from NAV).
      // They are provided by fund factsheet data when configured, else use
      // backup values.
      // The pePbData is injected into each fund object by dataProvider before
      // applyHistoryToFunds is called. Use it if available, else keep backup.
      const pe = (Number.isFinite(Number(fund.livePe)) && fund.livePe > 0)
        ? fund.livePe
        : fund.pe;
      const pb = (Number.isFinite(Number(fund.livePb)) && fund.livePb > 0)
        ? fund.livePb
        : fund.pb;

      const updatedHistory = [
        ...(fund.history || []).slice(0, Math.max((fund.history || []).length - 1, 0)),
        {
          ...(fund.history || []).at(-1),
          date: latestDate,
          oneYear,
          threeYear,
          fiveYear,
          sharpe,
          sortino,
          pe,
          pb
        }
      ];

      return {
        ...fund,
        latestDate,
        latestNav,
        oneYear,
        threeYear,
        fiveYear,
        sharpe,
        sortino,
        volatility,
        pe,
        pb,
        averageReturn: calc.mean(
          [oneYear, threeYear, fiveYear].filter((v) => Number.isFinite(v))
        ) ?? fund.averageReturn,
        history: updatedHistory
      };
    });

    const byCategory = new Map();
    for (const fund of next.funds) {
      const key = fund.category;
      if (!byCategory.has(key)) byCategory.set(key, []);
      byCategory.get(key).push(fund);
    }

    for (const funds of byCategory.values()) {
      const peers = funds.map((fund) => ({
        oneYear: fund.oneYear,
        threeYear: fund.threeYear,
        fiveYear: fund.fiveYear,
        sharpe: fund.sharpe,
        pe: fund.pe,
        pb: fund.pb,
        sortino: fund.sortino,
        volatility: fund.volatility
      }));

      funds.forEach((fund) => {
        const breakdown = buildParameterBreakdown(fund, peers, schema.scoring.modern);
        fund.parameterBreakdown = breakdown.some((entry) => entry.contribution !== null) ? breakdown : fund.parameterBreakdown;
        const computedScore = scoreFromBreakdown(fund.parameterBreakdown);
        if (Number.isFinite(computedScore)) fund.dashboardScore = computedScore;
      });

      const ordered = [...funds].sort((a, b) => (b.dashboardScore || 0) - (a.dashboardScore || 0));
      ordered.forEach((fund, index) => {
        fund.rank = index + 1;
        const scoreHistory = (fund.history || []).map((point) => ({ score: point.score ?? fund.dashboardScore }));
        const { trend, delta } = trendFromHistory(scoreHistory);
        fund.trend = trend;
        fund.trendDelta = delta;
        fund.consistency = Number(stdev((fund.history || []).map((point) => Number(point.score))).toFixed(2));
        fund.flag = fund.rank <= 5 ? "Top Performers" : (fund.flag || "Core");
      });
    }

    next.summaries = [...byCategory.entries()].map(([category, funds]) => summarizeCategory(category, funds));
    next.analysis = schema.appAnalysis;
    next.generatedFrom = "live-amfi + excel-backup";
    next.categories = [...byCategory.keys()].sort();
    next.latestDate = next.funds.map((fund) => fund.latestDate).filter(Boolean).sort().at(-1) || next.latestDate;

    return ensureAppShape(next, data);
  };

  return { mergeLatestNav, mergeLatestNavChunked, mergeLatestNavSync, applyHistoryToFunds, buildBackupLookup };
})();

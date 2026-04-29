window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.calculations = (() => {
  const RISK_FREE_RATE_ANNUAL = 0.065;

  const mean = (values) => {
    const list = values.filter((value) => Number.isFinite(value));
    if (!list.length) return null;
    return list.reduce((sum, value) => sum + value, 0) / list.length;
  };

  const stdev = (values) => {
    const list = values.filter((value) => Number.isFinite(value));
    if (list.length < 2) return 0;
    const avg = mean(list);
    const variance = list.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (list.length - 1);
    return Math.sqrt(variance);
  };

  const cagr = (startNav, endNav, years) => {
    if (!(Number.isFinite(startNav) && Number.isFinite(endNav) && startNav > 0 && endNav > 0 && years > 0)) return null;
    return (endNav / startNav) ** (1 / years) - 1;
  };

  const deriveTrailingReturn = (series, years) => {
    if (!Array.isArray(series) || !series.length) return null;
    const latest = series[series.length - 1];
    if (!latest?.date || !Number.isFinite(latest.nav)) return null;
    const latestDate = new Date(`${latest.date}T00:00:00Z`);
    const targetDate = new Date(latestDate);
    targetDate.setUTCFullYear(targetDate.getUTCFullYear() - years);

    let candidate = null;
    for (const point of series) {
      if (!point?.date || !Number.isFinite(point.nav)) continue;
      const pointDate = new Date(`${point.date}T00:00:00Z`);
      if (pointDate <= targetDate) {
        candidate = point;
      } else {
        break;
      }
    }

    if (!candidate) return null;
    return cagr(candidate.nav, latest.nav, years);
  };

  const annualizedReturn = (series) => {
    if (!Array.isArray(series) || series.length < 2) return null;
    const first = series[0];
    const last = series[series.length - 1];
    if (!first?.date || !last?.date || !first?.nav || !last?.nav) return null;
    const years = (new Date(last.date) - new Date(first.date)) / (365.25 * 24 * 3600 * 1000);
    if (years < 0.1) return null;
    return cagr(first.nav, last.nav, years);
  };

  const dailyReturns = (series) => {
    const returns = [];
    for (let i = 1; i < series.length; i += 1) {
      const prev = series[i - 1].nav;
      const curr = series[i].nav;
      if (prev > 0 && Number.isFinite(curr)) {
        returns.push((curr - prev) / prev);
      }
    }
    return returns;
  };

  // ─── NEW: Monthly NAV series from full daily series ───────────────────────
  // Takes the full ascending NAV series, returns one NAV per month end
  const toMonthlySeries = (series) => {
    if (!Array.isArray(series) || !series.length) return [];
    const byMonth = new Map();
    for (const point of series) {
      const key = point.date.slice(0, 7); // "YYYY-MM"
      byMonth.set(key, point); // last entry per month wins (month-end)
    }
    return [...byMonth.values()].sort((a, b) => a.date.localeCompare(b.date));
  };

  // ─── NEW: Monthly returns array from monthly NAV series ───────────────────
  const monthlyReturns = (monthlySeries) => {
    const returns = [];
    for (let i = 1; i < monthlySeries.length; i++) {
      const prev = monthlySeries[i - 1].nav;
      const curr = monthlySeries[i].nav;
      if (prev > 0 && Number.isFinite(curr)) returns.push((curr - prev) / prev);
    }
    return returns;
  };

  // ─── NEW: Get 3Y monthly window from full series ──────────────────────────
  const last3YearMonthly = (series) => {
    const monthly = toMonthlySeries(series);
    if (monthly.length < 6) return [];
    // Take up to 36 months (3 years)
    return monthly.slice(-37); // 37 items = 36 return periods
  };

  // ─── NEW: Sharpe Ratio (matches Excel/SEBI convention) ───────────────────
  // Uses 3Y monthly returns, annualized with sqrt(12)
  const sharpeFromHistory = (series) => {
    const monthly = last3YearMonthly(series);
    if (monthly.length < 13) return null; // need at least 12 return periods
    const returns = monthlyReturns(monthly);
    if (returns.length < 12) return null;
    const avg = mean(returns);
    const std = stdev(returns);
    if (!std || std === 0) return null;
    const rfrMonthly = RISK_FREE_RATE_ANNUAL / 12;
    // Sharpe = (avg monthly excess return / monthly std) × sqrt(12)
    const sharpe = ((avg - rfrMonthly) / std) * Math.sqrt(12);
    return Number(sharpe.toFixed(2));
  };

  // ─── NEW: Sortino Ratio (matches Excel convention) ────────────────────────
  // Uses 3Y monthly returns, downside deviation only
  const sortinoFromHistory = (series) => {
    const monthly = last3YearMonthly(series);
    if (monthly.length < 13) return null;
    const returns = monthlyReturns(monthly);
    if (returns.length < 12) return null;
    const avg = mean(returns);
    const rfrMonthly = RISK_FREE_RATE_ANNUAL / 12;
    const annualizedExcessReturn = (avg - rfrMonthly) * 12;
    // Downside deviation: only returns below RFR
    const downsideReturns = returns.filter((r) => r < rfrMonthly);
    if (!downsideReturns.length) return null;
    const downsideVariance = downsideReturns.reduce((sum, r) => sum + Math.pow(r - rfrMonthly, 2), 0) / downsideReturns.length;
    const downsideStdAnnual = Math.sqrt(downsideVariance) * Math.sqrt(12);
    if (!downsideStdAnnual || downsideStdAnnual === 0) return null;
    const sortino = annualizedExcessReturn / downsideStdAnnual;
    return Number(sortino.toFixed(2));
  };

  // ─── NEW: Annualized Volatility from history ──────────────────────────────
  const volatilityFromHistory = (series) => {
    const monthly = last3YearMonthly(series);
    if (monthly.length < 13) return null;
    const returns = monthlyReturns(monthly);
    if (returns.length < 12) return null;
    return Number((stdev(returns) * Math.sqrt(12)).toFixed(4));
  };

  const rankScore = (rows, metric, higherBetter, currentValue) => {
    if (!Number.isFinite(currentValue)) return { rank: null, normalized: null };
    const comparable = rows
      .map((row) => Number(row?.[metric]))
      .filter((value) => Number.isFinite(value));
    if (!comparable.length) return { rank: null, normalized: null };
    const rank = comparable.filter((value) => higherBetter ? value > currentValue : value < currentValue).length + 1;
    const total = comparable.length;
    const normalized = rank === 1 || total <= 1 ? 1 : 1 - ((rank - 1) / (total - 1));
    return { rank, normalized };
  };

  const buildParameterBreakdown = (fund, peers, weights) => weights.map((rule) => {
    const rawValue = Number(fund?.[rule.metric]);
    const { rank, normalized } = rankScore(peers, rule.metric, rule.higherBetter, rawValue);
    return {
      label: rule.label,
      metric: rule.metric,
      value: Number.isFinite(rawValue) ? rawValue : null,
      rank,
      weight: rule.weight,
      direction: rule.higherBetter ? "higher" : "lower",
      normalized,
      contribution: normalized === null ? null : normalized * rule.weight * 100
    };
  });

  const scoreFromBreakdown = (breakdown) => {
    const valid = breakdown.filter((item) => Number.isFinite(item.normalized));
    if (!valid.length) return null;
    const weighted = valid.reduce((sum, item) => sum + (item.normalized * item.weight), 0);
    const availableWeight = valid.reduce((sum, item) => sum + item.weight, 0);
    const completeness = valid.length / breakdown.length;
    return Math.round((weighted / availableWeight) * completeness * 100);
  };

  const trendFromHistory = (history) => {
    const scores = (history || []).map((point) => Number(point?.score)).filter((value) => Number.isFinite(value));
    if (!scores.length) return { trend: "Stable", delta: 0 };
    const delta = Number((scores[scores.length - 1] - scores[0]).toFixed(1));
    return {
      trend: Math.abs(delta) <= 2 ? "Stable" : (delta > 0 ? "Improving" : "Declining"),
      delta
    };
  };

  const summarizeCategory = (category, funds) => {
    const ordered = [...funds].sort((a, b) => (a.rank || 999) - (b.rank || 999));
    return {
      category,
      totalFunds: funds.length,
      categoryAverageReturn: Number(((mean(funds.map((fund) => fund.averageReturn)) || 0) * 100).toFixed(2)),
      categoryAverageScore: Number((mean(funds.map((fund) => fund.dashboardScore)) || 0).toFixed(1)),
      consistencyScore: Number((mean(funds.map((fund) => fund.consistency)) || 0).toFixed(2)),
      topPerformer: ordered[0]?.fundName || "",
      topScore: ordered[0]?.dashboardScore || 0,
      topTrend: ordered[0]?.trend || "Stable",
      latestDate: funds.map((fund) => fund.latestDate).filter(Boolean).sort().at(-1) || ""
    };
  };

  return {
    mean,
    stdev,
    cagr,
    deriveTrailingReturn,
    RISK_FREE_RATE_ANNUAL,
    annualizedReturn,
    dailyReturns,
    buildParameterBreakdown,
    scoreFromBreakdown,
    trendFromHistory,
    summarizeCategory,
    RISK_FREE_RATE_ANNUAL,
    toMonthlySeries,
    monthlyReturns,
    sharpeFromHistory,
    sortinoFromHistory,
    volatilityFromHistory
  };
})();

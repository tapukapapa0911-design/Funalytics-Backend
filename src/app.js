let appData = loadStoredData() || window.FUND_APP_DATA;

const APP_NAME = "Funalytics";
const APP_DESCRIPTION = "Funalytics is a smart mutual fund analytics platform that transforms Excel-based data into clear rankings, performance insights, and decision-ready dashboards.";

const state = {
  theme: localStorage.getItem("fundpulse-live-theme-v1") || "dark",
  category: "Large Cap Fund",
  tab: "dashboard",
  sort: "rank",
  horizon: "oneYear",
  dataset: "new",
  period: "1Y",
  fundView: localStorage.getItem("fundpulse-live-fund-view") || "all",
  query: "",
  pickerTarget: "category",
  selectedFundId: null,
  compareFundAId: null,
  compareFundBId: null,
  topInsightOpen: false
};

let deferredInstallPrompt = null;
let canShowInstall = false;
let isAppReady = false;
const INSTALL_FLOW_KEY = "live_install_flow_done";
const ONBOARDING_KEY = "funalytics_live_onboarding_done";
const LEGACY_ONBOARDING_KEY = "live_onboarding_done";
const BROWSER_INSTALL_CTA_KEY = "live_browser_install_cta_enabled";
const FAVORITES_KEY = "fundpulse-live-favorite-funds";
const UPDATE_BANNER_DISMISSED_KEY = "live-funalytics-update-banner-dismissed";
const UPDATE_VERSION_KEY = "live-funalytics-app-version";
const UPDATE_CHECK_URL = "./update.json";
const UPDATE_CHECK_INTERVAL = 10 * 60 * 1000;
const ONBOARDING_STEPS = 5;
const LAST_ONBOARDING_INDEX = 4;
let currentOnboardingIndex = 0;
let previousHorizonValue = state.horizon;
const MODAL_ANIMATION_MS = 220;
const MODAL_HISTORY_KEY = "__funalyticsModal";
let modalHistoryArmed = false;
let modalHistoryNavigating = false;
let serverUpdateVersion = null;
let updateCheckTimer = null;
const deferredRenderJobs = new Map();
let controlStateFrame = 0;
let searchInputTimer = 0;
const APP_DATA_STORAGE_KEY = "fundpulse-live-data-v8";
const UI_IDLE_APPLY_MS = 900;

const markUserInteraction = () => {
  window.__fundpulseLastInteractionAt = Date.now();
};

const runWhenUiIdle = (task, delay = 180) => {
  const attempt = () => {
    const lastInteractionAt = Number(window.__fundpulseLastInteractionAt || 0);
    const elapsed = Date.now() - lastInteractionAt;
    if (lastInteractionAt && elapsed < UI_IDLE_APPLY_MS) {
      window.setTimeout(attempt, delay);
      return;
    }
    task();
  };
  attempt();
};

window.__fundpulseRunWhenUiIdle = runWhenUiIdle;
window.__fundpulseMarkInteraction = markUserInteraction;

const isoDateValue = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return 0;
  const parsed = /^\d{4}-\d{2}-\d{2}$/.test(raw) ? new Date(`${raw}T00:00:00`) : new Date(raw);
  if (Number.isNaN(parsed.getTime()) || parsed.getUTCFullYear() < 2000) return 0;
  return parsed.getTime();
};

const navDateOf = (data) => data?.liveNavDate || "";

const cancelDeferredRenderJob = (key) => {
  const active = deferredRenderJobs.get(key);
  if (!active) return;
  if (active.type === "idle" && "cancelIdleCallback" in window) {
    window.cancelIdleCallback(active.id);
  } else {
    window.clearTimeout(active.id);
  }
  deferredRenderJobs.delete(key);
};

const scheduleDeferredRenderJob = (key, task, timeout = 60) => {
  cancelDeferredRenderJob(key);
  const runner = () => {
    deferredRenderJobs.delete(key);
    task();
  };
  if ("requestIdleCallback" in window) {
    const id = window.requestIdleCallback(runner, { timeout });
    deferredRenderJobs.set(key, { type: "idle", id });
    return;
  }
  const id = window.setTimeout(runner, 16);
  deferredRenderJobs.set(key, { type: "timeout", id });
};

const $ = (id) => document.getElementById(id);
const modalRoot = () => $("global-modal");
const modalContent = () => $("global-modal-content");
const modalCard = () => $("global-modal-card");
const modalBackdrop = () => $("global-modal-backdrop");
const onboardingEl = () => $("onboarding");
const onboardContainerEl = () => $("onboardContainer");
const splashScreenEl = () => $("splashScreen");
const profileInstallButtonEl = () => $("installAppBtn");

const ensureHashRoute = () => {
  if (!window.location.hash) {
    window.location.hash = "#dashboard";
  }
};

const routeFromHash = () => {
  const raw = String(window.location.hash || "").replace(/^#/, "").trim();
  return ["dashboard", "funds", "insights", "compare", "profile"].includes(raw) ? raw : "dashboard";
};

function loadStoredData() {
  try {
    const raw = localStorage.getItem("fundpulse-live-data-v8");
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.funds) || !Array.isArray(parsed.summaries)) return null;
    const liveDate = String(parsed.liveNavDate || "").trim();
    if (liveDate && (!/^\d{4}-\d{2}-\d{2}$/.test(liveDate) || Number(liveDate.slice(0, 4)) < 2000)) return null;
    return parsed;
  } catch (error) {
    console.warn("Funalytics cached live data read skipped", error);
    return null;
  }
}

function selectPreferredData(stored, injected) {
  if (!stored) return injected;
  if (!injected) return stored;

  const storedDate = isoDateValue(navDateOf(stored));
  const injectedDate = isoDateValue(navDateOf(injected));
  if (injectedDate > storedDate) return injected;
  if (storedDate > injectedDate) return stored;

  const storedFunds = Array.isArray(stored.funds) ? stored.funds.length : 0;
  const injectedFunds = Array.isArray(injected.funds) ? injected.funds.length : 0;
  return injectedFunds >= storedFunds ? injected : stored;
}

function standardizeFundName(value) {
  return String(value || "")
    .replace(/\bIcici\b/g, "ICICI")
    .replace(/\bPru\b/g, "Prudential");
}

function extractVolatility(fund) {
  const direct = Number(fund?.volatility);
  if (Number.isFinite(direct)) return direct;

  const sources = [
    ...(fund?.parameterBreakdown || []),
    ...((fund?.analysisHistory?.old || []).flatMap((point) => point?.parameters || [])),
    ...((fund?.analysisHistory?.new || []).flatMap((point) => point?.parameters || []))
  ];
  const match = sources.find((item) => item?.metric === "volatility" && Number.isFinite(Number(item?.value)));
  return match ? Number(match.value) : null;
}

function normalizeAppData(data) {
  if (!data || !Array.isArray(data.funds) || !Array.isArray(data.summaries)) return data;
  const duplicateFundName = "Parag Parikh Elss Tax Saver Fund Tax Saver Fund (G)";
  return {
    ...data,
    summaries: data.summaries.map((summary) => ({
      ...summary,
      topPerformer: standardizeFundName(summary.topPerformer)
    })),
    funds: data.funds
      .filter((fund) => String(fund?.fundName || "") !== duplicateFundName)
      .map((fund) => ({
      ...fund,
      fundName: standardizeFundName(fund.fundName),
      rawFundName: standardizeFundName(fund.rawFundName),
      volatility: extractVolatility(fund),
      analysisHistory: {
        old: (fund?.analysisHistory?.old || []).map((point) => ({
          ...point,
          parameters: (point?.parameters || []).map((parameter) => ({ ...parameter }))
        })),
        new: (fund?.analysisHistory?.new || []).map((point) => ({
          ...point,
          parameters: (point?.parameters || []).map((parameter) => ({ ...parameter }))
        }))
      },
      history: (fund?.history || []).map((point) => ({ ...point })),
      parameterBreakdown: (fund?.parameterBreakdown || []).map((parameter) => ({ ...parameter }))
    }))
  };
}

appData = selectPreferredData(loadStoredData(), window.FUND_APP_DATA);
appData = normalizeAppData(appData);

const escapeHtml = (value) => String(value)
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;");

const formatPct = (value, digits = 1) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(digits)}%`;
};

const safeIsoDate = (value) => {
  const raw = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) && Number(raw.slice(0, 4)) >= 2000 ? raw : "";
};

const formatDateValue = (iso) => {
  const safeIso = safeIsoDate(iso);
  if (!safeIso) return "Latest NAV syncing";
  const [year, month, day] = safeIso.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(date.getTime())) return "Latest NAV syncing";
  return date.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
};

const navDateSuffix = () => appData?.liveNavStatus === "last-available" ? " (last available)" : "";
const formatDate = (iso) => iso ? `Data as of ${formatDateValue(iso)}${navDateSuffix()}` : "Latest NAV syncing";
const hasLiveNav = (fund) => Number.isFinite(Number(fund?.latestNav)) && Boolean(String(fund?.liveNavDate || "").trim());

const scoreLabel = (value) => String(Math.round(value || 0));
const scoreOf = (fund) => fund?.dashboardScore ?? 0;
const consistencyOf = (fund) => {
  const raw = Number(fund?.consistency);
  return Number.isFinite(raw) ? raw : null;
};
const volatilityOf = (fund) => {
  const raw = Number(fund?.volatility);
  if (!Number.isFinite(raw)) return null;
  return Math.abs(raw) <= 1 ? raw * 100 : raw;
};
const riskOf = (fund) => {
  const volatility = volatilityOf(fund);
  if (volatility !== null) return volatility;
  const fallback = Number(fund?.consistency);
  return Number.isFinite(fallback) ? fallback : null;
};
const riskLabel = (fund) => {
  const value = riskOf(fund);
  return value === null ? "—" : `${value.toFixed(1)} vol`;
};
const riskBandLabel = (fund) => {
  const value = riskOf(fund);
  if (value === null) return "—";
  if (value <= 4) return "Low";
  if (value <= 8) return "Moderate";
  return "High";
};
const consistencyBandLabel = (fund) => {
  const value = consistencyOf(fund);
  if (value === null) return "—";
  if (value >= 8) return "High";
  if (value >= 5) return "Medium";
  return "Low";
};
const horizonLabel = () => ({ oneYear: "1Y", threeYear: "3Y", fiveYear: "5Y" })[state.horizon];
const periodCount = () => ({ "1M": 2, "3M": 2, "6M": 3, "1Y": 5 })[state.period] || 5;
const datasetLabel = () => state.dataset === "new" ? "Latest dataset" : "Historical dataset";
const trendTone = (trend) => trend === "Improving" ? "trend-up" : (trend === "Declining" ? "trend-down" : "trend-stable");
const trendMarkup = (trend) => `<span class="trend-chip ${trendTone(trend)}"><i aria-hidden="true"></i><span>${escapeHtml(trend)}</span></span>`;

const metricAt = (point) => {
  const value = point?.[state.horizon];
  return typeof value === "number" ? value * 100 : null;
};

const allCategoryFunds = () => appData.funds
  .filter((fund) => fund.category === state.category)
  .sort((a, b) => (a.rank || 999) - (b.rank || 999));

const allFunds = () => [...(appData?.funds || [])];

const loadFavoriteFundIds = () => {
  try {
    const parsed = JSON.parse(localStorage.getItem(FAVORITES_KEY) || "[]");
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch {
    return new Set();
  }
};

const saveFavoriteFundIds = (ids) => {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify([...ids]));
};

const favoriteFundIds = () => loadFavoriteFundIds();

const isFavoriteFund = (fundId) => favoriteFundIds().has(fundId);

const toggleFavoriteFund = (fundId) => {
  const ids = favoriteFundIds();
  if (ids.has(fundId)) ids.delete(fundId);
  else ids.add(fundId);
  saveFavoriteFundIds(ids);
};

const horizonValueOf = (fund) => {
  const value = fund?.[state.horizon];
  return typeof value === "number" ? value : null;
};
const fundMetricReturnLabel = (fund) => {
  const horizonValue = horizonValueOf(fund);
  if (horizonValue !== null) return formatPct(horizonValue);
  if (typeof fund?.threeYear === "number") return formatPct(fund.threeYear);
  if (typeof fund?.oneYear === "number") return formatPct(fund.oneYear);
  if (typeof fund?.fiveYear === "number") return formatPct(fund.fiveYear);
  return "—";
};

const horizonRankMap = (funds = allCategoryFunds()) => {
  const ordered = [...funds].sort((a, b) => {
    const aValue = horizonValueOf(a);
    const bValue = horizonValueOf(b);
    if (aValue === null && bValue === null) return (a.fundOrder || 999999) - (b.fundOrder || 999999);
    if (aValue === null) return 1;
    if (bValue === null) return -1;
    if (bValue !== aValue) return bValue - aValue;
    return (a.fundOrder || 999999) - (b.fundOrder || 999999);
  });
  return new Map(ordered.map((fund, index) => [fund.id, index + 1]));
};

const consistencyRankMap = (funds = allCategoryFunds()) => {
  const ordered = [...funds].sort((a, b) => {
    const aValue = consistencyOf(a);
    const bValue = consistencyOf(b);
    if (aValue === null && bValue === null) return (a.fundOrder || 999999) - (b.fundOrder || 999999);
    if (aValue === null) return 1;
    if (bValue === null) return -1;
    if (aValue !== bValue) return bValue - aValue;
    return (a.fundOrder || 999999) - (b.fundOrder || 999999);
  });
  return new Map(ordered.map((fund, index) => [fund.id, index + 1]));
};

const currentRankingMap = (funds = allCategoryFunds()) => {
  if (state.sort === "return") return horizonRankMap(funds);
  if (state.sort === "consistency") return consistencyRankMap(funds);
  return new Map(funds.map((fund) => [fund.id, fund.rank || 999]));
};

const summaryForCategory = () => appData.summaries.find((item) => item.category === state.category) || appData.summaries[0];

const latestNavDateForFunds = (funds = []) => {
  return funds
    .filter((fund) => hasLiveNav(fund))
    .map((fund) => String(fund?.liveNavDate || "").trim())
    .filter((candidate) => candidate && isoDateValue(candidate))
    .sort((left, right) => isoDateValue(left) - isoDateValue(right))
    .at(-1) || null;
};

const latestNavDateForCategory = (category = state.category) => {
  const funds = allFunds().filter((fund) => fund.category === category);
  return latestNavDateForFunds(funds) || appData?.liveNavDate || null;
};

const historyFor = (fund) => {
  const datasetHistory = fund?.analysisHistory?.[state.dataset] || [];
  return (datasetHistory.length ? datasetHistory : (fund?.history || [])).slice(-periodCount());
};

const mergedFundHistory = (fund) => {
  const buckets = new Map();
  const collect = (points = [], source = "unknown") => {
    points.forEach((point, index) => {
      const rawDate = point?.date || `${source}-${index}`;
      const key = /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : `${source}-${index}`;
      const existing = buckets.get(key) || { date: /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : null, score: null, oneYear: null, threeYear: null, fiveYear: null, source };
      buckets.set(key, {
        ...existing,
        date: existing.date || (/^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : null),
        score: point?.score ?? existing.score ?? scoreOf(fund),
        oneYear: typeof point?.oneYear === "number" ? point.oneYear : existing.oneYear ?? fund?.oneYear ?? null,
        threeYear: typeof point?.threeYear === "number" ? point.threeYear : existing.threeYear ?? fund?.threeYear ?? null,
        fiveYear: typeof point?.fiveYear === "number" ? point.fiveYear : existing.fiveYear ?? fund?.fiveYear ?? null,
        sharpe: point?.sharpe ?? existing.sharpe ?? fund?.sharpe ?? null,
        parameters: point?.parameters || existing.parameters || fund?.parameterBreakdown || []
      });
    });
  };

  collect(fund?.analysisHistory?.old, "historical");
  collect(fund?.history, "combined");
  collect(fund?.analysisHistory?.new, "latest");

  const merged = [...buckets.values()].sort((a, b) => (a.date || "").localeCompare(b.date || ""));
  if (merged.length) return merged;

  return [{
    date: fund?.latestDate || null,
    score: scoreOf(fund),
    oneYear: fund?.oneYear ?? null,
    threeYear: fund?.threeYear ?? null,
    fiveYear: fund?.fiveYear ?? null,
    parameters: fund?.parameterBreakdown || []
  }];
};

const chartHistoryFor = (fund) => {
  const mergedHistory = mergedFundHistory(fund);
  const datedBuckets = new Map();

  const addPoint = (key, label, point) => {
    const value = metricAt(point);
    if (value === null) return;
    datedBuckets.set(key, { key, label, value });
  };

  mergedHistory.forEach((point, index) => {
    const rawDate = point?.date;
    const key = rawDate ? `date:${rawDate}` : `fallback:${index}`;
    const label = rawDate ? rawDate.slice(5) : `P${index + 1}`;
    addPoint(key, label, point);
  });

  const datedSeries = [...datedBuckets.values()].sort((a, b) => a.key.localeCompare(b.key));
  if (datedSeries.length >= 2) return datedSeries.slice(-periodCount());

  const fallbackSeries = [];
  const oldPoint = fund?.analysisHistory?.old?.at(-1);
  const combinedPoint = fund?.history?.at(-1);
  const newPoint = fund?.analysisHistory?.new?.at(-1);

  if (oldPoint && metricAt(oldPoint) !== null) fallbackSeries.push({ key: "source:historical", label: "Historical", value: metricAt(oldPoint) });
  if (combinedPoint && metricAt(combinedPoint) !== null) fallbackSeries.push({ key: "source:combined", label: "Combined", value: metricAt(combinedPoint) });
  if (newPoint && metricAt(newPoint) !== null) fallbackSeries.push({ key: "source:latest", label: "Latest", value: metricAt(newPoint) });

  return fallbackSeries.slice(-Math.max(2, Math.min(periodCount(), fallbackSeries.length)));
};

const selectedFund = () => {
  const funds = allCategoryFunds();
  if (!funds.length) return null;
  const found = funds.find((fund) => fund.id === state.selectedFundId);
  if (found) return found;
  state.selectedFundId = funds[0].id;
  return funds[0];
};

const ensureCompareFunds = () => {
  const funds = allCategoryFunds();
  const ids = new Set(funds.map((fund) => fund.id));
  if (!ids.has(state.compareFundAId)) state.compareFundAId = funds[0]?.id || null;
  if (!ids.has(state.compareFundBId)) state.compareFundBId = funds[1]?.id || funds[0]?.id || null;
};

const compareFunds = () => {
  ensureCompareFunds();
  const funds = allCategoryFunds();
  return {
    fundA: funds.find((fund) => fund.id === state.compareFundAId) || funds[0] || null,
    fundB: funds.find((fund) => fund.id === state.compareFundBId) || funds[1] || funds[0] || null
  };
};

const visibleFunds = () => {
  const categoryFunds = allCategoryFunds();
  const displayRanks = currentRankingMap(categoryFunds);
  const query = state.query.trim().toLowerCase();
  const selectedCategory = state.category;
  const favorites = favoriteFundIds();
  const sourceFunds = query
    ? allFunds()
    : state.fundView === "favorites"
      ? allFunds().filter((fund) => favorites.has(fund.id))
      : categoryFunds;
  let funds = sourceFunds.filter((fund) => {
    if (state.fundView === "favorites" && !favorites.has(fund.id)) return false;
    if (!query) return true;
    return fund.fundName.toLowerCase().includes(query);
  });

  const sorters = {
    rank: (a, b) => {
      if (scoreOf(b) !== scoreOf(a)) return scoreOf(b) - scoreOf(a);
      return (a.rank || 999) - (b.rank || 999);
    },
    return: (a, b) => {
      const aValue = horizonValueOf(a);
      const bValue = horizonValueOf(b);
      if (aValue === null && bValue === null) return (displayRanks.get(a.id) || 999) - (displayRanks.get(b.id) || 999);
      if (aValue === null) return 1;
      if (bValue === null) return -1;
      if (bValue !== aValue) return bValue - aValue;
      return (displayRanks.get(a.id) || 999) - (displayRanks.get(b.id) || 999);
    },
    consistency: (a, b) => {
      const aValue = consistencyOf(a);
      const bValue = consistencyOf(b);
      if (aValue === null && bValue === null) return (displayRanks.get(a.id) || 999) - (displayRanks.get(b.id) || 999);
      if (aValue === null) return 1;
      if (bValue === null) return -1;
      if (aValue !== bValue) return bValue - aValue;
      return (displayRanks.get(a.id) || 999) - (displayRanks.get(b.id) || 999);
    }
  };

  const queryRank = (fund) => {
    if (!query) return 0;
    const name = fund.fundName.toLowerCase();
    if (name === query) return 0;
    if (name.startsWith(query)) return 1;
    const index = name.indexOf(query);
    return index === -1 ? 999 : 2 + index;
  };

  const categoryPriority = (fund) => (fund.category === selectedCategory ? 0 : 1);

  funds = [...funds]
    .sort((a, b) => {
      if (query) {
        const queryDiff = queryRank(a) - queryRank(b);
        if (queryDiff) return queryDiff;
      }
      const categoryDiff = categoryPriority(a) - categoryPriority(b);
      if (categoryDiff) return categoryDiff;
      return sorters[state.sort](a, b);
    })
    .map((fund) => ({ ...fund, displayRank: displayRanks.get(fund.id) || fund.rank || 999 }));
  return query || state.fundView === "favorites" ? funds.slice(0, 60) : funds.slice(0, 10);
};

const renderFallback = (targetId, title, body) => {
  const target = $(targetId);
  if (!target) return;
  target.innerHTML = `<article class="insight-card fallback-card"><p class="eyebrow">Unavailable</p><h3>${escapeHtml(title)}</h3><p class="story-copy">${escapeHtml(body)}</p></article>`;
};

const resetModalState = () => {
  const card = modalCard();
  const backdrop = modalBackdrop();
  if (card) {
    card.style.transform = "";
    card.style.opacity = "";
  }
  if (backdrop) {
    backdrop.style.opacity = "";
  }
};

const openGlobalModal = (html, options = {}) => {
  const root = modalRoot();
  const content = modalContent();
  const card = modalCard();
  if (!root || !content || !card) return false;
  resetModalState();
  root.classList.remove("open");
  root.hidden = false;
  root.setAttribute("aria-hidden", "false");
  content.innerHTML = "";
  card.dataset.modalKind = options.kind || "default";
  card.dataset.modalSize = options.size || "default";
  content.innerHTML = html;
  if (!options.skipHistory && !modalHistoryArmed) {
    window.history.pushState({ ...(window.history.state || {}), [MODAL_HISTORY_KEY]: true }, "");
    modalHistoryArmed = true;
  }
  requestAnimationFrame(() => root.classList.add("open"));
  document.body.classList.add("modal-open");
  return true;
};

const closeGlobalModal = (immediate = false, options = {}) => {
  const root = modalRoot();
  const content = modalContent();
  const card = modalCard();
  if (!root || !content || !card) return;
  if (!options.fromHistory && modalHistoryArmed && !modalHistoryNavigating) {
    modalHistoryNavigating = true;
    window.history.back();
    return;
  }
  root.classList.remove("open");
  root.setAttribute("aria-hidden", "true");
  document.body.classList.remove("modal-open");
  card.dataset.modalKind = "default";
  card.dataset.modalSize = "default";
  const finalize = () => {
    if (!root.classList.contains("open")) {
      resetModalState();
      content.innerHTML = "";
      root.hidden = true;
      modalHistoryArmed = false;
      modalHistoryNavigating = false;
    }
  };
  if (immediate) {
    finalize();
    return;
  }
  setTimeout(finalize, MODAL_ANIMATION_MS);
};

const bottomNavEl = () => document.querySelector(".bottom-nav");

const optionValue = (type) => {
  if (type === "tab") return state.tab;
  if (type === "horizon") return state.horizon;
  if (type === "sort") return state.sort;
  if (type === "period") return state.period;
  return null;
};

const applyOptionValue = (type, value, direction = 0) => {
  if (!value || optionValue(type) === value) return;
  if (type === "tab") {
    switchTab(value, direction);
    return;
  }
  if (type === "horizon") {
    previousHorizonValue = state.horizon;
    state.horizon = value;
    renderCurrentView();
    syncControlState();
    return;
  }
  if (type === "sort") {
    state.sort = value;
    renderFunds();
    syncControlState();
    return;
  }
  if (type === "period") {
    state.period = value;
    renderDashboard();
    syncControlState();
  }
};

const bindFundCardInteractions = (card, openCard) => {
  card.addEventListener("click", openCard);
  card.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openCard();
    }
  });
};

const methodologyMarkup = () => `
  <div class="detail-hero">
    <p class="eyebrow">Methodology</p>
    <h2>Insights &amp; Methodology</h2>
    <p class="muted">A complete guide to how ${APP_NAME} reads the workbook, builds rankings, explains category performance, and turns the data into usable insights.</p>
  </div>
  <div class="accordion-list">
    <details open><summary>Overview</summary><ul><li>${APP_NAME} is a data-driven mutual fund dashboard built on top of an Excel workbook backend.</li><li>The dashboard score is a quick category-relative quality read for each fund.</li><li>The app is designed to help users compare performance, ranking, and stability more clearly.</li></ul></details>
    <details><summary>Key Factors</summary><ul><li>Returns across <strong>1Y</strong>, <strong>3Y</strong>, and <strong>5Y</strong> windows.</li><li><strong>Consistency</strong> through volatility and steadiness of results.</li><li><strong>Relative performance</strong> inside the selected category.</li></ul></details>
    <details><summary>Scoring Logic</summary><ul><li>The workbook converts the available metrics into a combined weighted score.</li><li>Category filtering and ranking happen inside the workbook-backed data pipeline.</li><li>Higher score means a stronger overall read for the current category framework.</li></ul></details>
    <details><summary>Interpretation Guide</summary><ul><li><strong>70+</strong> -> Strong</li><li><strong>50-70</strong> -> Average</li><li><strong>Below 50</strong> -> Weak</li></ul></details>
    <details><summary>Ranking Methodology</summary><ul><li>Funds are ranked within their category rather than across the full market.</li><li>The app mirrors workbook-style filtering and sorting so ranks update with category changes.</li><li>Rank helps explain who leads the category right now.</li></ul></details>
    <details><summary>Consistency &amp; Risk</summary><ul><li>Consistency reflects how stable a fund has been across the visible return history.</li><li>Risk is inferred from volatility and variation in outcomes.</li><li>A high-return fund with weak consistency may still deserve caution.</li></ul></details>
    <details><summary>Data &amp; Excel Logic</summary><ul><li>Data comes from workbook-backed sheets including historical analysis, latest analysis, and processed dashboard data.</li><li>The app mirrors Excel-style <strong>FILTER</strong>, <strong>UNIQUE</strong>, and <strong>SORT</strong> behavior.</li><li>Users can upload an updated workbook extract to refresh the analysis.</li></ul></details>
    <details><summary>AI Insights</summary><ul><li>Insight cards translate ranking, trend, and category comparisons into simple human-readable takeaways.</li><li>They are meant to support interpretation, not replace user judgment.</li><li>The goal is clarity, not prediction.</li></ul></details>
    <details><summary>Disclaimer</summary><ul><li>Past performance does not guarantee future results.</li><li>This app is for educational and informational use only.</li><li>It should not be treated as financial advice.</li></ul></details>
  </div>
`;

const setTheme = (theme) => {
  state.theme = theme;
  document.body.classList.toggle("light", theme === "light");
  $("profileThemeValue").textContent = theme === "light" ? "Light" : "Dark";
  document.querySelector('meta[name="theme-color"]')?.setAttribute("content", theme === "light" ? "#EEF2FB" : "#070A12");
  localStorage.setItem("fundpulse-live-theme-v1", theme);
};

const latestMetric = (series, key) => {
  const point = [...series].reverse().find((item) => typeof item?.[key] === "number");
  return point?.[key] ?? null;
};

const categoryLatest = (funds) => {
  const category = categorySeries(funds);
  return category.at(-1)?.avg ?? null;
};

const normalizeProgress = (value, ceiling = 100) => Math.max(0, Math.min(100, (value / ceiling) * 100));
const scoreTone = (value) => (value >= 70 ? "strong" : value >= 50 ? "neutral" : "weak");

const buildStorySignals = (fund, funds) => {
  const currentReturn = typeof fund?.[state.horizon] === "number" ? fund[state.horizon] * 100 : null;
  const categoryReturn = categoryLatest(funds);
  const score = scoreOf(fund);
  const scoreLead = score - (summaryForCategory()?.categoryAverageScore || 0);
  const tags = [];

  if (score >= 75) tags.push({ label: "Strong Performer", tone: "good" });
  else if (score <= 45) tags.push({ label: "Watchlist", tone: "warn" });

  if (fund.trend === "Improving") tags.push({ label: "Improving", tone: "good" });
  if (fund.trend === "Declining") tags.push({ label: "Declining Trend", tone: "bad" });
  if (fund.trend === "Stable") tags.push({ label: "Stable", tone: "neutral" });

  const latestComparison = currentReturn !== null && categoryReturn !== null ? currentReturn - categoryReturn : null;

  return { score, scoreLead, currentReturn, categoryReturn, latestComparison, tags };
};

const buildHeadlineInsight = (fund, funds) => {
  const peerPct = allCategoryFunds().length ? Math.round(((allCategoryFunds().length - fund.rank + 1) / allCategoryFunds().length) * 100) : 0;
  const trendPhrase = fund.trend === "Improving"
    ? "showing fresh momentum"
    : fund.trend === "Declining"
      ? "showing recent slowdown"
      : "holding a steady trend";
  return `${fund.fundName} is outperforming ${peerPct}% of peers in ${state.category}, ${trendPhrase}.`;
};

const storySentence = (fund, funds) => {
  const signals = buildStorySignals(fund, funds);
  const returnStrength = typeof fund.threeYear === "number" && typeof fund.fiveYear === "number" && fund.threeYear > 0.12 && fund.fiveYear > 0.12
    ? "Strong 3Y and 5Y performance keep the long-term picture healthy."
    : "Long-term performance is more balanced, so short-term moves matter more here.";
  const shortTermRead = typeof fund.oneYear === "number" && fund.oneYear < 0.02
    ? "Recent 1Y softness suggests near-term weakness."
    : "Recent 1Y returns remain supportive.";
  const compareRead = signals.latestComparison === null
    ? "Category comparison is limited for the visible period."
    : signals.latestComparison >= 0
      ? `It is running ${signals.latestComparison.toFixed(1)} pts ahead of the category average.`
      : `It is trailing the category average by ${Math.abs(signals.latestComparison).toFixed(1)} pts.`;
  return `${returnStrength} ${shortTermRead} ${compareRead}`;
};

const clampValue = (value, min = 0, max = 1) => Math.max(min, Math.min(max, value));

const normalizeInsightMetric = (value, peers, { invert = false } = {}) => {
  const numbers = peers.filter((item) => Number.isFinite(item));
  if (!numbers.length || !Number.isFinite(value)) return 0.5;
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  if (Math.abs(max - min) < 0.0001) return 0.7;
  const normalized = (value - min) / (max - min);
  const adjusted = invert ? 1 - normalized : normalized;
  return clampValue(0.18 + adjusted * 0.72, 0.16, 0.98);
};

const fundReturnStrength = (fund) => {
  const slices = [
    typeof fund?.oneYear === "number" ? { value: fund.oneYear * 100, weight: 0.28 } : null,
    typeof fund?.threeYear === "number" ? { value: fund.threeYear * 100, weight: 0.34 } : null,
    typeof fund?.fiveYear === "number" ? { value: fund.fiveYear * 100, weight: 0.38 } : null
  ].filter(Boolean);
  if (!slices.length) return null;
  const totalWeight = slices.reduce((sum, slice) => sum + slice.weight, 0) || 1;
  return slices.reduce((sum, slice) => sum + slice.value * slice.weight, 0) / totalWeight;
};

const topPerformerTrendSignal = (fund) => {
  const shortTerm = typeof fund?.oneYear === "number" ? fund.oneYear * 100 : null;
  const longTerms = [fund?.threeYear, fund?.fiveYear].filter((value) => typeof value === "number").map((value) => value * 100);
  const longTerm = longTerms.length ? longTerms.reduce((sum, value) => sum + value, 0) / longTerms.length : null;
  const delta = shortTerm !== null && longTerm !== null
    ? shortTerm - longTerm
    : Number.isFinite(Number(fund?.trendDelta))
      ? Number(fund.trendDelta)
      : 0;
  const strengthening = fund?.trend === "Improving" || (fund?.trend !== "Declining" && delta >= 0);
  return strengthening
    ? {
        tone: "strengthening",
        emoji: "🔥",
        title: "Strengthening",
        summary: "Recent return momentum is holding up against the longer-term track."
      }
    : {
        tone: "weakening",
        emoji: "📉",
        title: "Weakening",
        summary: "Recent return momentum has cooled versus the longer-term base."
      };
};

const buildFundDna = (fund, funds) => {
  const returnStrength = fundReturnStrength(fund);
  const returnPeers = funds.map(fundReturnStrength);
  const consistencyValue = consistencyOf(fund);
  const consistencyPeers = funds.map(consistencyOf);
  const riskValue = volatilityOf(fund);
  const riskPeers = funds.map(volatilityOf);
  const shortTerm = typeof fund?.oneYear === "number" ? fund.oneYear * 100 : null;
  const mediumTerm = typeof fund?.threeYear === "number" ? fund.threeYear * 100 : null;
  const longTerm = typeof fund?.fiveYear === "number" ? fund.fiveYear * 100 : null;
  const momentumValue = shortTerm !== null && mediumTerm !== null ? shortTerm - mediumTerm : Number(fund?.trendDelta) || 0;
  const momentumPeers = funds.map((item) => {
    const short = typeof item?.oneYear === "number" ? item.oneYear * 100 : null;
    const medium = typeof item?.threeYear === "number" ? item.threeYear * 100 : null;
    return short !== null && medium !== null ? short - medium : Number(item?.trendDelta) || 0;
  });
  const stabilityValue = consistencyValue !== null && riskValue !== null ? consistencyValue - riskValue : consistencyValue;
  const stabilityPeers = funds.map((item) => {
    const itemConsistency = consistencyOf(item);
    const itemRisk = volatilityOf(item);
    return itemConsistency !== null && itemRisk !== null ? itemConsistency - itemRisk : itemConsistency;
  });
  return [
    {
      key: "return",
      label: "Return",
      note: returnStrength === null ? "Limited history" : `${returnStrength >= 12 ? "Strong" : returnStrength >= 8 ? "Balanced" : "Measured"} return profile`,
      scale: normalizeInsightMetric(returnStrength, returnPeers),
      accent: "teal",
      displayValue: returnStrength === null ? "—" : `${returnStrength.toFixed(1)}%`
    },
    {
      key: "risk",
      label: "Risk",
      note: riskValue === null ? "No volatility read" : `${normalizeInsightMetric(riskValue, riskPeers, { invert: true }) >= 0.6 ? "Controlled" : "Elevated"} category risk`,
      scale: normalizeInsightMetric(riskValue, riskPeers, { invert: true }),
      accent: "violet",
      displayValue: riskValue === null ? "—" : `${riskValue.toFixed(1)}`
    },
    {
      key: "consistency",
      label: "Consistency",
      note: consistencyValue === null ? "Limited history" : `${normalizeInsightMetric(consistencyValue, consistencyPeers) >= 0.62 ? "Above average" : "Category aligned"} consistency`,
      scale: normalizeInsightMetric(consistencyValue, consistencyPeers),
      accent: "blue",
      displayValue: consistencyValue === null ? "—" : consistencyValue.toFixed(1)
    },
    {
      key: "stability",
      label: "Stability",
      note: stabilityValue === null ? "Limited history" : `${normalizeInsightMetric(stabilityValue, stabilityPeers) >= 0.6 ? "Steady" : "Mixed"} behaviour across cycles`,
      scale: normalizeInsightMetric(stabilityValue, stabilityPeers),
      accent: "indigo",
      displayValue: stabilityValue === null ? "—" : stabilityValue.toFixed(1)
    },
    {
      key: "momentum",
      label: "Momentum",
      note: momentumValue >= 0 ? "Short-term trend improving" : "Short-term trend cooling",
      scale: normalizeInsightMetric(momentumValue, momentumPeers),
      accent: "purple",
      displayValue: `${momentumValue >= 0 ? "+" : ""}${momentumValue.toFixed(1)}`
    }
  ];
};

const renderFundDnaRadar = (dimensions) => {
const isLight = document.body.classList.contains("light");
  const width = 290;
  const height = 290;
  const cx = width / 2;
  const cy = height / 2;
  const radius = 105;
  const count = dimensions.length;
  const angleFor = (index) => (-Math.PI / 2) + ((Math.PI * 2) / count) * index;
  const pointFor = (scale, index) => {
    const angle = angleFor(index);
    return {
      x: cx + Math.cos(angle) * radius * scale,
      y: cy + Math.sin(angle) * radius * scale
    };
  };
  const rings = [0.2, 0.4, 0.6, 0.8, 1].map((ring) => {
    const points = dimensions.map((_, index) => {
      const point = pointFor(ring, index);
      return `${point.x.toFixed(1)},${point.y.toFixed(1)}`;
    }).join(" ");
    return `<polygon points="${points}" fill="none"stroke="${isLight ? 'rgba(15,23,42,0.18)' : 'rgba(255,255,255,0.08)'}"
opacity="1" stroke-width="${ring === 1 ? 1.1 : 1}"/>`;
  }).join("");
  const axes = dimensions.map((item, index) => {
    const outer = pointFor(1, index);
    const label = pointFor(1.12, index);
    const labelDelay = 150 + (index * 80);
    return `
      <line class="fund-radar__axis" style="--axis-delay:${80 + (index * 45)}ms" x1="${cx}" y1="${cy}" x2="${outer.x.toFixed(1)}" y2="${outer.y.toFixed(1)}" stroke="currentColor" opacity="0.12"/>
      <text class="fund-radar__label" data-label-index="${index}" style="--label-delay:${labelDelay}ms" x="${label.x.toFixed(1)}" y="${label.y.toFixed(1)}" text-anchor="middle" dominant-baseline="central" fill="${isLight ? '#0f172a' : '#e2e8f0'}"
opacity="1" font-size="12" font-weight="700">${escapeHtml(item.label)}</text>
    `;
  }).join("");
  const areaPoints = dimensions.map((item, index) => {
    const point = pointFor(item.scale, index);
    return `${point.x.toFixed(1)},${point.y.toFixed(1)}`;
  }).join(" ");
  const nodes = dimensions.map((item, index) => {
    const point = pointFor(item.scale, index);
    return `<circle class="fund-radar__node" style="--node-delay:${160 + (index * 60)}ms" cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="4.5" fill="#fff" filter="url(#glow)" stroke="url(#fundRadarStroke)" stroke-width="2"/>`;
  }).join("");
  return `
    <svg viewBox="0 0 ${width} ${height}" class="fund-radar fund-radar--animated" role="img" aria-label="Fund DNA radar chart">
      <defs>
        <linearGradient id="fundRadarFill" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#58d0ff" stop-opacity="0.24"/>
          <stop offset="55%" stop-color="#4a63ff" stop-opacity="0.18"/>
          <stop offset="100%" stop-color="#9fc8ff" stop-opacity="0.26"/>
        </linearGradient>
        <linearGradient id="fundRadarStroke" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#58d0ff"/>
          <stop offset="52%" stop-color="#4a63ff"/>
          <stop offset="100%" stop-color="#9fc8ff"/>
        </linearGradient>
        <radialGradient id="fundRadarGlow" cx="50%" cy="50%" r="60%">
          <stop offset="0%" stop-color="#58d0ff" stop-opacity="0.28"/>
          <stop offset="100%" stop-color="#58d0ff" stop-opacity="0"/>
        </radialGradient>
      </defs>
      <circle cx="${cx}" cy="${cy}" r="${radius * 0.94}" fill="${isLight ? 'rgba(88,208,255,0.08)' : 'url(#fundRadarGlow)'}"/>
      ${rings}
      ${axes}
      <polygon class="fund-radar__area" points="${areaPoints}" fill="${isLight ? 'rgba(74,99,255,0.2)' : 'url(#fundRadarFill)'}"
stroke="${isLight ? '#4a63ff' : 'url(#fundRadarStroke)'}" stroke-width="2.4"/>
      ${nodes}
    </svg>
  `;
};

const buildWhyThisFund = (fund, funds, summary) => {
  const insights = [];
  const avgThreeYear = funds.map((item) => typeof item?.threeYear === "number" ? item.threeYear * 100 : null).filter(Number.isFinite);
  const avgFiveYear = funds.map((item) => typeof item?.fiveYear === "number" ? item.fiveYear * 100 : null).filter(Number.isFinite);
  const avgConsistency = funds.map(consistencyOf).filter(Number.isFinite);
  const avgVolatility = funds.map(volatilityOf).filter(Number.isFinite);
  const threeYear = typeof fund?.threeYear === "number" ? fund.threeYear * 100 : null;
  const fiveYear = typeof fund?.fiveYear === "number" ? fund.fiveYear * 100 : null;
  const consistencyValue = consistencyOf(fund);
  const volatilityValue = volatilityOf(fund);
  const trendSignal = topPerformerTrendSignal(fund);

  if ((fund.rank || 999) === 1 || scoreOf(fund) >= Number(summary?.topScore || 0)) {
    insights.push("Top-ranked in the category on the current dashboard score.");
  }
  if (fiveYear !== null && avgFiveYear.length && fiveYear > (avgFiveYear.reduce((sum, value) => sum + value, 0) / avgFiveYear.length)) {
    insights.push("Strong long-term performance versus category peers.");
  } else if (threeYear !== null && avgThreeYear.length && threeYear > (avgThreeYear.reduce((sum, value) => sum + value, 0) / avgThreeYear.length)) {
    insights.push("Recent medium-term returns are running ahead of category peers.");
  }
  if (consistencyValue !== null && avgConsistency.length && consistencyValue > (avgConsistency.reduce((sum, value) => sum + value, 0) / avgConsistency.length)) {
    insights.push("Above-average consistency inside this category.");
  }
  if (volatilityValue !== null && avgVolatility.length && volatilityValue < (avgVolatility.reduce((sum, value) => sum + value, 0) / avgVolatility.length)) {
    insights.push("Lower volatility than most funds in the same category.");
  }
  insights.push(trendSignal.tone === "strengthening"
    ? "Recent performance momentum is improving against its own long-term baseline."
    : "Recent trend is weakening even though the broader history stays relevant.");
  if (scoreOf(fund) > Number(summary?.categoryAverageScore || 0)) {
    insights.push(`Dashboard score sits ${(scoreOf(fund) - Number(summary?.categoryAverageScore || 0)).toFixed(1)} points above the category average.`);
  }
  return [...new Set(insights)].slice(0, 4);
};

const topPerformerDetailMarkup = (fund, funds, summary) => {
  const trend = topPerformerTrendSignal(fund);
  const dna = buildFundDna(fund, funds);
  const whyThisFund = buildWhyThisFund(fund, funds, summary);
  const metricRows = [
    ["1Y Return", formatPct(fund.oneYear)],
    ["3Y Return", formatPct(fund.threeYear)],
    ["5Y Return", formatPct(fund.fiveYear)],
    ["Consistency", consistencyOf(fund) === null ? "—" : consistencyOf(fund).toFixed(1)],
    ["Risk / Volatility", riskLabel(fund)]
  ];
  return `
    <div class="detail-hero">
      <p class="eyebrow">${escapeHtml(state.category)} | Top Performer</p>
      <h2>${escapeHtml(fund.fundName)}</h2>
      <span class="rank-badge">Top performer</span>
    </div>
    <article class="insight-card top-performer-inline-card top-performer-detail-card">
      <div class="inline-insight-trend inline-insight-trend--${trend.tone}">
        <div class="inline-insight-trend__lead">
          <strong>${trend.emoji} ${trend.title}</strong>
        </div>
        <p>${escapeHtml(trend.summary)}</p>
      </div>
      <div class="inline-insight-section">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Fund DNA</p>
          </div>
        </div>
        <div class="inline-dna-layout">
          <div class="inline-dna-radar">
            ${renderFundDnaRadar(dna)}
          </div>
          <div class="fund-dna-list inline-dna-list">
          ${dna.map((item, index) => `
            <div class="fund-dna-row fund-dna-row--${item.accent}" data-dna-index="${index}">
              <div class="fund-dna-row__head">
                <strong>${escapeHtml(item.label)}</strong>
                <span>${escapeHtml(item.displayValue)}</span>
              </div>
              <div class="fund-dna-track">
                <i class="fund-dna-fill" style="--dna-scale:${item.scale.toFixed(3)}; --dna-delay:${140 + (index * 100)}ms"></i>
              </div>
            </div>
          `).join("")}
          </div>
        </div>
      </div>
      <div class="inline-insight-section">
        <div class="section-head compact">
          <div>
            <p class="eyebrow">Metrics</p>
          </div>
        </div>
        <div class="inline-metric-grid">

  <div class="inline-metric-card">
    <span>Score</span>
    <strong>${scoreLabel(scoreOf(fund))}</strong>
  </div>

  <div class="inline-metric-card">
    <span>Consistency</span>
    <strong>${consistencyOf(fund)?.toFixed(1) || "—"}</strong>
  </div>

  <div class="inline-metric-card">
    <span>Volatility</span>
    <strong>${riskLabel(fund)}</strong>
  </div>

  <div class="inline-metric-card">
    <span>1Y</span>
    <strong>${formatPct(fund.oneYear)}</strong>
  </div>

  <div class="inline-metric-card">
    <span>3Y</span>
    <strong>${formatPct(fund.threeYear)}</strong>
  </div>

  <div class="inline-metric-card">
    <span>5Y</span>
    <strong>${formatPct(fund.fiveYear)}</strong>
  </div>

</div>
      </div>
      <div class="inline-insight-section">
        <div class="fund-insight-why">
          <div class="section-head compact">
            <div>
              <h3>💡 Why this fund?</h3>
            </div>
          </div>
          <ul class="fund-insight-list">
            ${whyThisFund.map((insight) => `<li>${escapeHtml(insight)}</li>`).join("")}
          </ul>
        </div>
      </div>
    </article>
  `;
};

const openTopPerformerInsight = () => {
  const summary = summaryForCategory();
  const funds = allCategoryFunds();
  const fund = funds[0] || selectedFund();
  if (!summary || !fund) return;
  openGlobalModal(topPerformerDetailMarkup(fund, funds, summary), { kind: "detail", size: "wide" });
};

const openPicker = (target) => {
  state.pickerTarget = target;
  const html = renderPicker();
  if (!openGlobalModal(html, { kind: "picker" })) return;
  document.querySelectorAll("[data-picker-choice]").forEach((button) => {
    button.addEventListener("click", () => {
      const selected = button.dataset.pickerChoice;
      if (state.pickerTarget === "compareFundA") {
        state.compareFundAId = selected;
      } else if (state.pickerTarget === "compareFundB") {
        state.compareFundBId = selected;
      } else {
        state.category = selected;
        state.selectedFundId = null;
        state.compareFundAId = null;
        state.compareFundBId = null;
        state.query = "";
        if ($("searchInput")) $("searchInput").value = "";
      }
      closeGlobalModal();
      renderCurrentView();
    });
  });
};

const closePicker = () => {
  closeGlobalModal();
};

const tabOrder = ["dashboard", "funds", "insights", "compare", "profile"];

const syncTabUi = () => {
  document.querySelectorAll(".bottom-nav button").forEach((item) => item.classList.toggle("active", item.dataset.tab === state.tab));
  document.querySelectorAll(".screen").forEach((screen) => screen.classList.toggle("active", screen.id === `screen-${state.tab}`));
};

const resetViewScroll = (behavior = "auto") => {
  const app = $("app");
  if (app) app.scrollTop = 0;
  document.documentElement.scrollTop = 0;
  document.body.scrollTop = 0;
  window.scrollTo({ top: 0, behavior });
};

const switchTab = (tab, direction = 0) => {
  if (!tab || tab === state.tab) return;
  const nextScreen = $(`screen-${tab}`);
  if (!nextScreen) return;
  state.tab = tab;
  window.location.hash = `#${tab}`;
  syncTabUi();
  nextScreen.style.setProperty("--tab-shift", `${direction * 12}px`);
  renderCurrentView();
  requestAnimationFrame(() => resetViewScroll("auto"));
};

const renderPicker = () => {
  const isFundPicker = state.pickerTarget === "compareFundA" || state.pickerTarget === "compareFundB";
  const categoryOptions = Array.isArray(appData?.categories) ? appData.categories : [];
  const items = isFundPicker ? allCategoryFunds().map((fund) => ({ value: fund.id, label: fund.fundName })) : categoryOptions.map((category) => ({ value: category, label: category }));
  const current = state.pickerTarget === "compareFundA"
    ? state.compareFundAId
    : state.pickerTarget === "compareFundB"
      ? state.compareFundBId
      : state.category;
  if (!items.length) {
    return `
      <div class="picker-modal">
        <div class="picker-header">
          <div>
            <p class="eyebrow">${isFundPicker ? "Fund" : "Category"}</p>
            <h3>${isFundPicker ? "Choose fund" : "Choose Category"}</h3>
          </div>
        </div>
        <div class="picker-scroll">
          <div class="empty-chart">No options are available for this selection yet.</div>
        </div>
      </div>
    `;
  }
  return `
    <div class="picker-modal">
      <div class="picker-header">
        <div>
          <p class="eyebrow">${isFundPicker ? "Fund" : "Category"}</p>
          <h3>${isFundPicker ? "Choose fund" : "Choose Category"}</h3>
        </div>
      </div>
      <div class="picker-scroll">
        <div class="picker-list">
          ${items.map((item) => `
            <button class="${item.value === current ? "active" : ""}" data-picker-choice="${escapeHtml(item.value)}">
              <span>${escapeHtml(item.label)}</span>
              <i aria-hidden="true"></i>
            </button>
          `).join("")}
        </div>
      </div>
    </div>
  `;
};

const renderChrome = () => {
  const showControls = ["dashboard", "funds", "insights", "compare"].includes(state.tab);
  document.querySelector(".controls-strip").classList.toggle("hidden", !showControls);
};

const syncOverlayState = () => {};

const closeAllOverlays = () => closeGlobalModal();

const updateSegmentedPill = (group) => {
  if (!(group instanceof HTMLElement)) return;
  const buttons = [...group.querySelectorAll("button")];
  const active = buttons.find((button) => button.classList.contains("active"));
  if (!active) {
    group.style.setProperty("--pill-visible", "0");
    return;
  }
  if (group.matches(".dashboard-return-tabs, .ranking-filter, .compare-return-filter") && buttons.length === 3) {
    const activeIndex = Math.max(0, buttons.indexOf(active));
    group.style.setProperty("--pill-visible", "1");
    group.style.setProperty("--selector-shift", `${activeIndex * 100}%`);
    return;
  }
  const setPillFrame = (button) => {
    group.style.setProperty("--pill-visible", "1");
    group.style.setProperty("--pill-x", `${button.offsetLeft - group.scrollLeft}px`);
    group.style.setProperty("--pill-y", `${button.offsetTop}px`);
    group.style.setProperty("--pill-w", `${button.offsetWidth}px`);
    group.style.setProperty("--pill-h", `${button.offsetHeight}px`);
  };
  setPillFrame(active);
};

const updateAllSegmentedPills = () => {
  document.querySelectorAll(".dashboard-return-tabs, .ranking-filter, .compare-return-filter, .chip-row, .time-filter, .fund-view-toggle").forEach(updateSegmentedPill);
};

const bindSegmentedPillTracking = () => {
  document.querySelectorAll(".dashboard-return-tabs, .ranking-filter, .compare-return-filter, .chip-row, .time-filter, .fund-view-toggle").forEach((group) => {
    if (!(group instanceof HTMLElement) || group.dataset.pillBound === "true") return;
    group.dataset.pillBound = "true";
    group.addEventListener("scroll", () => updateSegmentedPill(group), { passive: true });
  });
};

const syncControlState = () => {
  document.querySelectorAll("[data-horizon]").forEach((button) => {
    button.classList.toggle("active", button.dataset.horizon === state.horizon);
  });
  document.querySelectorAll(".time-filter button").forEach((button) => {
    button.classList.toggle("active", button.dataset.period === state.period);
  });
  document.querySelectorAll(".chip").forEach((button) => {
    button.classList.toggle("active", button.dataset.sort === state.sort);
  });
  document.querySelectorAll("[data-fund-view]").forEach((button) => {
    button.classList.toggle("active", button.dataset.fundView === state.fundView);
  });
  if (controlStateFrame) {
    cancelAnimationFrame(controlStateFrame);
  }
  controlStateFrame = requestAnimationFrame(() => {
    controlStateFrame = 0;
    bindSegmentedPillTracking();
    updateAllSegmentedPills();
  });
};

const renderHeaderControls = () => {
  const categories = Array.isArray(appData?.categories) ? appData.categories : [];
  if (!categories.length) return;
  if (!categories.includes(state.category)) state.category = categories[0];
  $("categoryValue").textContent = state.category;
  const { fundA, fundB } = compareFunds();
  if ($("compareFundAValue")) $("compareFundAValue").textContent = fundA?.fundName || "Select fund";
  if ($("compareFundBValue")) $("compareFundBValue").textContent = fundB?.fundName || "Select fund";
};

const renderDashboard = () => {
  const summary = summaryForCategory();
  const funds = allCategoryFunds();
  const fund = funds[0] || selectedFund();
  if (!summary || !fund) {
    renderFallback("screen-dashboard", "Dashboard data unavailable", "Choose a category with valid workbook data to see the dashboard.");
    return;
  }
  const categoryAvgReturn = funds.map((item) => item[state.horizon]).filter((value) => typeof value === "number");
  const avgReturn = categoryAvgReturn.length ? categoryAvgReturn.reduce((a, b) => a + b, 0) / categoryAvgReturn.length : null;

  $("heroCategory").textContent = state.category;
  $("heroDate").textContent = formatDate(latestNavDateForCategory(state.category));
  const heroScoreValue = scoreOf(fund);
  $("heroScore").textContent = scoreLabel(heroScoreValue);
  $("heroScoreBadge")?.setAttribute("data-score-tone", scoreTone(heroScoreValue));
  $("topPerformer").textContent = fund.fundName;
  $("avgReturn").textContent = formatPct(avgReturn);
  $("categoryAverage").textContent = scoreLabel(summary.categoryAverageScore);
  $("consistency").textContent = Number.isFinite(Number(fund?.consistency)) ? `${Number(fund.consistency).toFixed(1)} vol` : "—";
  $("totalFunds").textContent = summary.totalFunds;
  $("dashboardInsight").textContent = `${fund.fundName} is loading live category insight...`;

  const insightMount = $("topPerformerInsightMount");
  if (insightMount) {
    insightMount.innerHTML = "";
    insightMount.classList.remove("is-open");
    insightMount.setAttribute("aria-hidden", "true");
  }

  const chartTarget = $("lineChart");
  const tooltip = $("performanceTooltip");
  if (chartTarget) {
    chartTarget.innerHTML = `<div class="empty-chart">Loading performance chart...</div>`;
  }
  if (tooltip) {
    tooltip.hidden = false;
    tooltip.textContent = `Loading live comparison for ${fund.fundName}...`;
  }

  const categoryKey = `${state.category}|${state.horizon}|${state.period}|${fund.id}`;
  scheduleDeferredRenderJob(`dashboard:${categoryKey}`, () => {
    if (state.tab !== "dashboard" || state.category !== summary.category) return;
    const history = historyFor(fund);
    const signals = buildStorySignals(fund, funds);
    const categoryAverage = categorySeries(funds);
    const outperformCount = history.filter((point) => {
      const label = point.date ? point.date.slice(5) : point.label;
      const cat = categoryAverage.find((item) => item.label === label);
      return metricAt(point) !== null && cat && metricAt(point) >= cat.avg;
    }).length;
    $("dashboardInsight").textContent = `${buildHeadlineInsight(fund, funds)} ${storySentence(fund, funds)} It beat the category average in ${outperformCount} of ${history.length} visible periods.`;
    renderPerformanceChart("lineChart", fund, funds, categoryAverage);
    if (tooltip) {
      tooltip.hidden = false;
      tooltip.textContent = `Top fund: ${fund.fundName} | Return view: ${horizonLabel()} | Score lead: ${signals.scoreLead >= 0 ? "+" : ""}${signals.scoreLead.toFixed(1)}`;
    }
  }, 90);
};

const categorySeries = (funds) => {
  const buckets = new Map();
  funds.forEach((fund) => {
    chartHistoryFor(fund).forEach((point) => {
      if (!buckets.has(point.key)) buckets.set(point.key, { label: point.label, values: [] });
      buckets.get(point.key).values.push(point.value);
    });
  });
  return [...buckets.entries()]
    .map(([key, bucket]) => ({
      key,
      label: bucket.label,
      avg: bucket.values.reduce((a, b) => a + b, 0) / bucket.values.length
    }))
    .sort((a, b) => a.key.localeCompare(b.key))
    .slice(-periodCount());
};

const renderPerformanceChart = (id, fund, funds, category = null) => {
  const target = $(id);
  if (!target || !fund) return;
  const fundHistory = chartHistoryFor(fund);
  const categoryData = Array.isArray(category) ? category : categorySeries(funds);
  const labels = [...new Set([...fundHistory.map((point) => point.label), ...categoryData.map((point) => point.label)])];
  const selectedValues = labels.map((label) => fundHistory.find((point) => point.label === label)?.value ?? null);
  const categoryValues = labels.map((label) => categoryData.find((point) => point.label === label)?.avg ?? null);
  renderMultiLineSvg(target, [
    { name: "Top fund", values: selectedValues, color: "#0F766E" },
    { name: "Category average", values: categoryValues, color: "#2563EB" }
  ], labels, $("performanceTooltip"));
};

const renderMultiLineSvg = (target, series, labels, tooltipEl = null) => {
  const width = 360;
  const height = 220;
  const padding = 34;
  const values = series.flatMap((item) => item.values).filter((value) => value !== null && value !== undefined);
  if (!values.length) {
    target.innerHTML = `<div class="empty-chart">Performance history is unavailable for the selected view.</div>`;
    return;
  }
  const min = Math.min(...values, 0) - 2;
  const max = Math.max(...values, 1) + 2;
  const denom = max - min || 1;
  const x = (index) => padding + index * ((width - padding * 2) / Math.max(1, labels.length - 1));
  const y = (value) => height - padding - ((value - min) / denom) * (height - padding * 2);
  const grid = [min, (min + max) / 2, max].map((tick) => `<g><line x1="${padding}" y1="${y(tick)}" x2="${width - padding}" y2="${y(tick)}" stroke="currentColor" opacity=".08"/><text x="6" y="${y(tick) + 4}" fill="currentColor" opacity=".55" font-size="9">${tick.toFixed(0)}%</text></g>`).join("");
  const lines = series.map((item) => {
    const points = item.values.map((value, index) => value === null ? null : `${x(index)},${y(value)}`).filter(Boolean).join(" ");
    const isPrimary = item === series[0];
    const dots = item.values.map((value, index) => value === null ? "" : `<circle class="chart-point${isPrimary ? " primary" : ""}" data-chart-index="${index}" data-series-name="${escapeHtml(item.name)}" cx="${x(index)}" cy="${y(value)}" r="${isPrimary ? 4.5 : 3.5}" fill="${item.color}"><title>${item.name}: ${value.toFixed(2)}%</title></circle>`).join("");
    return `<polyline class="chart-line" points="${points}" fill="none" stroke="${item.color}" stroke-width="${isPrimary ? 4 : 2.5}" stroke-linecap="round" stroke-linejoin="round"/>${dots}`;
  }).join("");
  const labelsSvg = labels.map((label, index) => `<text x="${x(index)}" y="${height - 8}" text-anchor="middle" fill="currentColor" opacity=".58" font-size="10" font-weight="800">${escapeHtml(label)}</text>`).join("");
  const guideLine = tooltipEl ? `<line class="chart-guide-line" id="${target.id || "chart"}GuideLine" x1="${x(Math.max(0, labels.length - 1))}" y1="${padding / 2}" x2="${x(Math.max(0, labels.length - 1))}" y2="${height - padding}" stroke="currentColor" opacity=".16" stroke-width="1.2" stroke-dasharray="4 4"/>` : "";
  const hoverZones = tooltipEl ? labels.map((label, index) => {
    const zoneX = index === 0 ? padding : x(index) - ((width - padding * 2) / Math.max(1, labels.length - 1)) / 2;
    const zoneWidth = labels.length === 1 ? width - padding * 2 : (width - padding * 2) / Math.max(1, labels.length - 1);
    return `<rect class="chart-hit" data-chart-index="${index}" x="${zoneX}" y="${padding / 2}" width="${zoneWidth}" height="${height - padding}" fill="transparent"/>`;
  }).join("") : "";
  const legendHtml = `<div class="chart-legend">${series.map((item) => `<span class="chart-legend-item" style="color:${item.color}"><i aria-hidden="true"></i><span>${escapeHtml(item.name)}</span></span>`).join("")}</div>`;
  target.innerHTML = `${legendHtml}<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Top fund versus category average">${grid}${guideLine}${lines}${hoverZones}${labelsSvg}</svg>`;
  if (tooltipEl) {
    const svg = target.querySelector("svg");
    const guide = target.querySelector(".chart-guide-line");
    const showTooltip = (index) => {
      tooltipEl.hidden = false;
      tooltipEl.textContent = `${labels[index]} | ${series.map((item) => `${item.name}: ${item.values[index] === null ? "-" : `${item.values[index].toFixed(1)}%`}`).join(" | ")}`;
      if (guide) {
        const pos = x(index);
        guide.setAttribute("x1", `${pos}`);
        guide.setAttribute("x2", `${pos}`);
      }
    };
    target.querySelectorAll("[data-chart-index]").forEach((zone) => {
      zone.addEventListener("mouseenter", () => showTooltip(Number(zone.dataset.chartIndex)));
      zone.addEventListener("click", () => showTooltip(Number(zone.dataset.chartIndex)));
      zone.addEventListener("touchstart", () => showTooltip(Number(zone.dataset.chartIndex)), { passive: true });
    });
    target.querySelectorAll(".chart-point").forEach((point) => {
      const activatePoint = () => showTooltip(Number(point.dataset.chartIndex));
      point.addEventListener("mouseenter", activatePoint);
      point.addEventListener("click", activatePoint);
      point.addEventListener("touchstart", activatePoint, { passive: true });
    });
    if (svg) {
      const handlePointerAt = (clientX) => {
        const rect = svg.getBoundingClientRect();
        const relativeX = Math.max(padding, Math.min(width - padding, ((clientX - rect.left) / rect.width) * width));
        const step = labels.length <= 1 ? 0 : (width - padding * 2) / Math.max(1, labels.length - 1);
        const index = labels.length <= 1 ? 0 : Math.max(0, Math.min(labels.length - 1, Math.round((relativeX - padding) / step)));
        showTooltip(index);
      };
      svg.addEventListener("pointermove", (event) => handlePointerAt(event.clientX));
      svg.addEventListener("pointerdown", (event) => handlePointerAt(event.clientX));
      svg.addEventListener("touchmove", (event) => {
        const touch = event.touches?.[0];
        if (touch) handlePointerAt(touch.clientX);
      }, { passive: true });
    }
    showTooltip(Math.max(0, labels.length - 1));
  }
};

const renderFunds = () => {
  const query = state.query.trim();
  const funds = visibleFunds();
  const favorites = favoriteFundIds();
  const fundList = $("fundList");
  const rankingFilterMount = $("rankingFilterMount");
  if (rankingFilterMount) {
    if (state.sort === "return") {
      let filter = rankingFilterMount.querySelector(".ranking-filter");
      if (!filter) {
        rankingFilterMount.innerHTML = `
          <div class="quick-filter-row ranking-filter selector-container" aria-label="Ranking return horizon">
            <button class="${state.horizon === "oneYear" ? "active" : ""}" data-horizon="oneYear">1Y</button>
            <button class="${state.horizon === "threeYear" ? "active" : ""}" data-horizon="threeYear">3Y</button>
            <button class="${state.horizon === "fiveYear" ? "active" : ""}" data-horizon="fiveYear">5Y</button>
          </div>
        `;
        rankingFilterMount.querySelectorAll("[data-horizon]").forEach((button) => {
          button.addEventListener("click", () => applyOptionValue("horizon", button.dataset.horizon, 0));
        });
      }
    } else if (rankingFilterMount.innerHTML.trim()) {
      rankingFilterMount.innerHTML = "";
    }
  }
  fundList?.classList.toggle("saved-empty", !funds.length && state.fundView === "favorites");
  if (!funds.length) {
    if (state.fundView === "favorites") {
      fundList.innerHTML = `
        <div class="saved-empty-state" role="status" aria-live="polite">
          <div class="saved-empty-state__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <path d="M3.5 7.5a2 2 0 0 1 2-2H10l2 2h6.5a2 2 0 0 1 2 2v7a2 2 0 0 1-2 2H5.5a2 2 0 0 1-2-2z"></path>
              <path d="m15.2 11.8.35 1.08h1.14l-.92.66.35 1.08-.92-.67-.92.67.35-1.08-.92-.66h1.14z"></path>
            </svg>
          </div>
          <h3>No saved funds match this view yet.</h3>
          <p>Funds you save will appear here for quick access.</p>
        </div>
      `;
    } else {
      const emptyCopy = `No funds are available in ${state.category} for the current search.`;
      fundList.innerHTML = `<div class="list-note">${escapeHtml(emptyCopy)}</div>`;
    }
    return;
  }
  const note = query
? `Searching all results for "${query}"`
    : state.fundView === "favorites"
      ? `Saved funds | ${state.category} funds appear first | ${formatDate(latestNavDateForCategory(state.category))}`
      : `Top 10 funds in ${state.category} | ${formatDate(latestNavDateForCategory(state.category))}`;
  fundList.innerHTML = `
    <div class="list-note">${escapeHtml(note)}</div>
    ${funds.map((fund) => `
      <article class="fund-card" data-fund-id="${escapeHtml(fund.id)}" role="button" tabindex="0" aria-label="Open ${escapeHtml(fund.fundName)} detail">
        <div class="fund-card-top">
          <div>
            <p class="eyebrow">${escapeHtml(fund.category)}</p>
            <h3 class="fund-name">${escapeHtml(fund.fundName)}</h3>
          </div>
          <div class="fund-card-actions">
            <button class="favorite-toggle ${favorites.has(fund.id) ? "is-active" : ""}" data-favorite-id="${escapeHtml(fund.id)}" aria-label="${favorites.has(fund.id) ? "Remove from saved funds" : "Save fund"}">${favorites.has(fund.id) ? "★" : "☆"}</button>
            <div class="rank-badge">#${fund.displayRank || fund.rank}</div>
          </div>
        </div>
        ${state.sort === "consistency" ? `
          <div class="return-strip">
            <div class="return-pill selected"><small>Consistency</small><strong>${consistencyOf(fund) === null ? "—" : consistencyOf(fund).toFixed(1)}</strong></div>
            <div class="return-pill"><small>Volatility</small><strong>${riskLabel(fund)}</strong></div>
            <div class="return-pill"><small>Score</small><strong>${scoreLabel(scoreOf(fund))}</strong></div>
          </div>
        ` : `
          <div class="return-strip">
            <div class="return-pill ${state.horizon === "oneYear" && state.sort === "return" ? "selected" : ""}"><small>1Y</small><strong>${formatPct(fund.oneYear)}</strong></div>
            <div class="return-pill ${state.horizon === "threeYear" && state.sort === "return" ? "selected" : ""}"><small>3Y</small><strong>${formatPct(fund.threeYear)}</strong></div>
            <div class="return-pill ${state.horizon === "fiveYear" && state.sort === "return" ? "selected" : ""}"><small>5Y</small><strong>${formatPct(fund.fiveYear)}</strong></div>
          </div>
        `}

        <div class="fund-card-bottom">
          <span class="muted">${riskLabel(fund)}</span>
          <strong>${escapeHtml(fund.flag || "Core")}</strong>
        </div>
        <div class="trend-row">
          ${trendMarkup(fund.trend)}
      <div class="nav-box${hasLiveNav(fund) ? "" : " nav-box--empty"}">
        <small>NAV</small>
        <strong>${hasLiveNav(fund) ? `₹${Number(fund.latestNav).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "—"}</strong>
      </div>
          <span>Score ${scoreLabel(scoreOf(fund))}</span>
        </div>
      </article>
    `).join("")}
  `;
  $("fundList").querySelectorAll("[data-favorite-id]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      toggleFavoriteFund(button.dataset.favoriteId);
      renderFunds();
    });
  });
  document.querySelectorAll("[data-fund-id]").forEach((card) => {
    const openCard = () => {
      const fundId = card.dataset.fundId;
      if (!fundId) return;
      state.selectedFundId = fundId;
      openDetail(fundId);
    };
    bindFundCardInteractions(card, openCard);
  });
};

const renderInsights = () => {
  const funds = allCategoryFunds();
  const summary = summaryForCategory();
  const fund = selectedFund() || funds[0];
  if (!summary || !fund) {
    renderFallback("insightsList", "Insights unavailable", "There is not enough category data available to generate insights for this selection.");
    $("analysisChart").innerHTML = `<div class="empty-chart">Leadership data is unavailable for this category.</div>`;
    return;
  }
  const signals = buildStorySignals(fund, funds);
  const chartFunds = funds.slice(0, 8);
  const tags = signals.tags.map((tag) => `<span class="story-tag ${tag.tone}">${escapeHtml(tag.label)}</span>`).join("");
  const comparisonBars = [
    {
      label: "Vs category score",
      value: normalizeProgress(Math.max(0, signals.scoreLead + 50), 100),
      note: `${signals.scoreLead >= 0 ? "+" : ""}${signals.scoreLead.toFixed(1)} score`
    },
    {
      label: "Vs category return",
      value: normalizeProgress(Math.max(0, (signals.latestComparison ?? 0) + 20), 40),
      note: signals.latestComparison === null ? "No comparable read" : `${signals.latestComparison >= 0 ? "+" : ""}${signals.latestComparison.toFixed(1)} pts`
    },
    {
      label: "Category rank strength",
      value: normalizeProgress(allCategoryFunds().length - fund.rank + 1, Math.max(1, allCategoryFunds().length)),
      note: `Rank #${fund.rank} of ${allCategoryFunds().length}`
    }
  ];
  $("insightsList").innerHTML = `
    <article class="insight-card primary story-card">
      <p class="eyebrow">Headline insight</p>
      <h3>${escapeHtml(buildHeadlineInsight(fund, funds))}</h3>
      <p class="story-copy">${escapeHtml(storySentence(fund, funds))}</p>
      <div class="story-tags">${tags}</div>
    </article>
    <article class="insight-card story-card">
      <p class="eyebrow">Performance breakdown</p>
      <h3>${typeof fund.threeYear === "number" && fund.threeYear > 0.12 ? "Strong long-term return profile" : "Balanced long-term profile with mixed signals"}</h3>
      <p class="story-copy">${typeof fund.oneYear === "number" && fund.oneYear < 0.02 ? "Recent 1Y softness suggests short-term weakness." : "Recent 1Y performance is still constructive."}</p>
    </article>
    <article class="insight-card story-card">
      <p class="eyebrow">Trend interpretation</p>
      <h3>${fund.trend === "Improving" ? "Momentum is building" : fund.trend === "Declining" ? "Momentum is weakening" : "Momentum remains stable"}</h3>
      <p class="story-copy">${escapeHtml(fund.fundName)} currently carries ${riskLabel(fund)} with rank #${fund.rank} in ${escapeHtml(state.category)}.</p>
    </article>
    <article class="insight-card story-card full-span">
      <p class="eyebrow">Comparison insight</p>
      <h3>How the selected fund stacks up right now</h3>
      <div class="story-metrics">
        ${comparisonBars.map((item) => `
          <div class="story-progress">
            <div class="story-progress-head"><strong>${escapeHtml(item.label)}</strong><span>${escapeHtml(item.note)}</span></div>
            <div class="story-progress-track"><i style="width:${item.value}%"></i></div>
          </div>
        `).join("")}
      </div>
    </article>
    <article class="insight-card story-card">
      <p class="eyebrow">Category snapshot</p>
      <h3>${escapeHtml(state.category)} has ${summary.totalFunds} funds and an average dashboard score of ${summary.categoryAverageScore.toFixed(1)}.</h3>
      <p class="story-copy">${escapeHtml(summary.topPerformer)} leads the category while the current view uses the ${datasetLabel().toLowerCase()}.</p>
    </article>
  `;
  const analysisChart = $("analysisChart");
  if (analysisChart) {
    analysisChart.innerHTML = `<div class="empty-chart">Loading category leadership chart...</div>`;
  }
  const insightKey = `${state.category}|${state.horizon}|insights`;
  scheduleDeferredRenderJob(`insights:${insightKey}`, () => {
    if (state.tab !== "insights" || state.category !== summary.category) return;
    renderReturnBarChart("analysisChart", chartFunds, false);
  }, 120);
};

const renderReturnBarChart = (id, funds, highlightSelected = true) => {
  const target = $(id);
  if (!target) return;
  if (!funds.length) {
    target.innerHTML = `<div class="empty-chart">No comparable funds are available in this category.</div>`;
    return;
  }
  const width = 360;
  const height = Math.max(170, funds.length * 34 + 34);
  const values = funds.map((fund) => (fund[state.horizon] || 0) * 100);
  const max = Math.max(1, ...values.map((value) => Math.abs(value)));
  const rows = funds.map((fund, index) => {
    const value = values[index];
    const barWidth = Math.abs(value / max) * 170;
    const y = 26 + index * 34;
    const color = highlightSelected && fund.id === state.selectedFundId ? "#0F766E" : "#60A5FA";
    return `<text x="12" y="${y + 14}" fill="currentColor" opacity=".72" font-size="10" font-weight="800">${escapeHtml(fund.fundName.slice(0, 24))}</text><rect x="176" y="${y}" width="${barWidth}" height="18" rx="9" fill="${color}" opacity=".9"/><text x="${182 + barWidth}" y="${y + 14}" fill="currentColor" font-size="10" font-weight="900">${value.toFixed(1)}%</text>`;
  }).join("");
  target.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Fund return comparison">${rows}</svg>`;
};

const renderVerticalReturnBarChart = (id, funds) => {
  const target = $(id);
  if (!target) return;
  if (!funds.length) {
    target.innerHTML = `<div class="empty-chart">Choose funds to unlock comparison.</div>`;
    return;
  }
  const width = 360;
  const height = 250;
  const padding = { top: 28, right: 18, bottom: 48, left: 32 };
  const values = funds.map((fund) => ((fund?.[state.horizon] || 0) * 100));
  const max = Math.max(1, ...values.map((value) => Math.abs(value)));
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const barWidth = Math.min(90, plotWidth / Math.max(2, funds.length * 1.6));
  const gap = funds.length > 1 ? (plotWidth - barWidth * funds.length) / (funds.length - 1) : 0;
  const baseline = padding.top + plotHeight;
  const ticks = [0, max / 2, max];

  const grid = ticks.map((tick) => {
    const y = baseline - (tick / max) * plotHeight;
    return `<g><line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(255,255,255,0.08)" opacity=".08"/><text x="6" y="${y + 4}" fill="currentColor" opacity=".55" font-size="9">${tick.toFixed(0)}%</text></g>`;
  }).join("");

  const bars = funds.map((fund, index) => {
    const value = values[index];
    const barHeight = Math.max(4, Math.abs(value / max) * plotHeight);
    const x = padding.left + index * (barWidth + gap);
    const y = baseline - barHeight;
    const color = index === 0 ? "#0F766E" : "#2563EB";
    return `
      <g>
        <rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="16" fill="${color}" opacity=".92"/>
        <text x="${x + barWidth / 2}" y="${y - 8}" text-anchor="middle" fill="currentColor" font-size="11" font-weight="900">${value.toFixed(1)}%</text>
        <text x="${x + barWidth / 2}" y="${height - 22}" text-anchor="middle" fill="currentColor" opacity=".7" font-size="10" font-weight="800">${escapeHtml((fund.fundName || "").split(" ").slice(0, 2).join(" "))}</text>
      </g>
    `;
  }).join("");

  target.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Vertical return comparison">${grid}${bars}</svg>`;
};

const renderCompare = () => {
  const { fundA, fundB } = compareFunds();
  if (!fundA || !fundB) {
    $("compareContent").innerHTML = `<article class="compare-card full-span"><p class="eyebrow">Compare</p><h3>Comparison unavailable</h3><p class="muted">Select a category with at least two funds to unlock cross-fund comparison.</p></article>`;
    return;
  }
  const compareKey = `${fundA.id}|${fundB.id}|${state.category}`;
  const activeA = fundA[state.horizon];
  const activeB = fundB[state.horizon];
  const leader = (activeA || 0) === (activeB || 0)
    ? `Both funds are aligned on ${horizonLabel()} return at ${formatPct(activeA)}.`
    : `${activeA > activeB ? fundA.fundName : fundB.fundName} leads the ${horizonLabel()} view by ${formatPct(Math.abs((activeA || 0) - (activeB || 0)))}.`;
  const metricRows = [
    ["Dashboard score", scoreLabel(scoreOf(fundA)), scoreLabel(scoreOf(fundB))],
    ["Current rank", `#${fundA.rank}`, `#${fundB.rank}`],
    ["1Y return", formatPct(fundA.oneYear), formatPct(fundB.oneYear)],
    ["3Y return", formatPct(fundA.threeYear), formatPct(fundB.threeYear)],
    ["5Y return", formatPct(fundA.fiveYear), formatPct(fundB.fiveYear)],
    ["Risk indicator", riskLabel(fundA), riskLabel(fundB)],
    ["Trend", fundA.trend, fundB.trend]
  ];
  const compareContent = $("compareContent");
  const existingKey = compareContent?.dataset.compareKey;
  if (compareContent && existingKey === compareKey && compareContent.querySelector(".compare-return-filter")) {
    const quickRead = $("compareLeaderText");
    const horizonHeading = $("compareHorizonHeading");
    if (quickRead) quickRead.textContent = leader;
    if (horizonHeading) horizonHeading.textContent = `${horizonLabel()} return comparison`;
    renderVerticalReturnBarChart("comparePageChart", [fundA, fundB]);
    return;
  }
  compareContent.innerHTML = `
    <article class="compare-card">
      <p class="eyebrow">Fund A</p>
      <h3>${escapeHtml(fundA.fundName)}</h3>
      <p class="muted">Rank #${fundA.rank} | Score ${scoreLabel(scoreOf(fundA))}</p>
    </article>
    <article class="compare-card">
      <p class="eyebrow">Fund B</p>
      <h3>${escapeHtml(fundB.fundName)}</h3>
      <p class="muted">Rank #${fundB.rank} | Score ${scoreLabel(scoreOf(fundB))}</p>
    </article>
    <article class="compare-card full-span">
      <p class="eyebrow">Side-by-side metrics</p>
      <h3>Compare key mutual fund metrics</h3>
      <div class="compare-metrics">
        <div class="compare-metrics-head"><span>Metric</span><span>${escapeHtml(fundA.fundName.slice(0, 16))}</span><span>${escapeHtml(fundB.fundName.slice(0, 16))}</span></div>
        ${metricRows.map((row) => `<div class="compare-metric-row"><strong>${row[0]}</strong><span>${row[1]}</span><span>${row[2]}</span></div>`).join("")}
      </div>
    </article>
    <article class="compare-card full-span">
      <p class="eyebrow">Quick read</p>
      <h3 id="compareLeaderText">${escapeHtml(leader)}</h3>
      <p class="muted">Use the return selector below to switch between 1Y, 3Y, and 5Y. The comparison stays locked to ${escapeHtml(state.category)} so the read stays category-accurate.</p>
    </article>
    <article class="compare-card full-span">
      <div class="section-head">
        <div>
          <p class="eyebrow">Return comparison</p>
          <h3 id="compareHorizonHeading">${horizonLabel()} return comparison</h3>
        </div>
      </div>
        <div class="quick-filter-row compare-return-filter selector-container">
          <button class="${state.horizon === "oneYear" ? "active" : ""}" data-horizon="oneYear">1Y</button>
          <button class="${state.horizon === "threeYear" ? "active" : ""}" data-horizon="threeYear">3Y</button>
          <button class="${state.horizon === "fiveYear" ? "active" : ""}" data-horizon="fiveYear">5Y</button>
        </div>
      <div id="comparePageChart" class="bar-chart"></div>
    </article>
  `;
  compareContent.dataset.compareKey = compareKey;
  renderVerticalReturnBarChart("comparePageChart", [fundA, fundB]);
  compareContent.querySelectorAll("[data-horizon]").forEach((button) => {
    button.addEventListener("click", () => {
      applyOptionValue("horizon", button.dataset.horizon, 0);
    });
  });
};

const renderProfile = () => {
  const stored = localStorage.getItem("fundpulse-live-upload-name");
  $("uploadStatus").textContent = stored ? stored.slice(0, 22) : "Choose .xlsx";
  const installButton = profileInstallButtonEl();
  const installValue = $("installStatus");
  const installed = isInstalledApp();
  if (installValue) {
    installValue.textContent = installed ? "Installed" : "Tap to install";
  }
  if (installButton) {
    const available = !installed && Boolean(deferredInstallPrompt);
    installButton.hidden = !available;
    installButton.style.display = available ? "" : "none";
  }
};

const isStandaloneMode = () => window.matchMedia?.("(display-mode: standalone)")?.matches || window.navigator.standalone === true;
const isInstalledApp = () => window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
const hasSeenOnboarding = () => localStorage.getItem(ONBOARDING_KEY) === "true" || localStorage.getItem(LEGACY_ONBOARDING_KEY) === "true";
const hasCompletedInstallFlow = () => localStorage.getItem(INSTALL_FLOW_KEY) === "true";
const shouldShowBrowserInstallCta = () => localStorage.getItem(BROWSER_INSTALL_CTA_KEY) === "true";

const setAppReady = () => {
  if (isAppReady) return;
  isAppReady = true;
  document.documentElement.classList.add("app-ready");
};

const markOnboardingDone = () => {
  localStorage.setItem(ONBOARDING_KEY, "true");
  localStorage.setItem(LEGACY_ONBOARDING_KEY, "true");
};

const showMainApp = () => {
  $("skeleton")?.classList.add("hide");
  $("app")?.classList.remove("is-loading");
};

const loadDashboard = () => {
  state.tab = "dashboard";
  ensureHashRoute();
  window.location.hash = "#dashboard";
  syncTabUi();
  renderCurrentView();
  renderChrome();
  showMainApp();
};

const enterApp = () => {
  loadDashboard();
  updateInstallButton();
  setUpdateBannerMessage("New update available • Tap to refresh");
  setUpdateBanner(false);
};

const updateOnboardingPanelHeight = (index = currentOnboardingIndex) => {
  const panel = document.querySelector(".onboarding-panel");
  const container = onboardContainerEl();
  if (!panel || !container) return;
  const steps = [...container.querySelectorAll(".onboarding-step")];
  const activeStep = steps[Math.max(0, Math.min(index, steps.length - 1))];
  if (!activeStep) return;
  requestAnimationFrame(() => {
    panel.style.height = `${activeStep.scrollHeight}px`;
  });
};

const updateOnboardingPosition = (index) => {
  const container = onboardContainerEl();
  if (!container) return;
  const safeIndex = Math.max(0, Math.min(index, LAST_ONBOARDING_INDEX));
  currentOnboardingIndex = safeIndex;
  container.style.transform = `translateX(-${safeIndex * 100}%)`;
  updateOnboardingPanelHeight(safeIndex);
  const finishButton = $("letsGoBtn") || $("onboardingFinish");
  if (finishButton) {
    finishButton.textContent = currentOnboardingIndex === LAST_ONBOARDING_INDEX ? "Let’s Go" : "Next";
  }
};

const hideOnboarding = () => {
  const overlay = onboardingEl();
  if (!overlay) {
    showMainApp();
    return;
  }
  overlay.hidden = true;
  overlay.style.display = "none";
  overlay.setAttribute("aria-hidden", "true");
  overlay.remove();
  showMainApp();
};

const finishOnboarding = () => {
  markOnboardingDone();
  localStorage.setItem(INSTALL_FLOW_KEY, "true");
  if (!isInstalledApp()) {
    localStorage.setItem(BROWSER_INSTALL_CTA_KEY, "true");
  }
  hideOnboarding();
  enterApp();
};

const handleOnboardingNext = () => {
  currentOnboardingIndex = Math.min(currentOnboardingIndex, LAST_ONBOARDING_INDEX);
  if (currentOnboardingIndex < LAST_ONBOARDING_INDEX) {
    updateOnboardingPosition(currentOnboardingIndex + 1);
  } else {
    finishOnboarding();
  }
};

const showInstallCard = () => {
  const overlay = onboardingEl();
  if (!overlay) {
    enterApp();
    return;
  }
  overlay.hidden = false;
  overlay.style.display = "";
  overlay.setAttribute("aria-hidden", "false");
  currentOnboardingIndex = 0;
  updateOnboardingPosition(0);
  updateInstallButton();
};

const showOnboardingSlides = () => {
  const overlay = onboardingEl();
  if (!overlay) {
    enterApp();
    return;
  }
  overlay.hidden = false;
  overlay.style.display = "";
  overlay.setAttribute("aria-hidden", "false");
  currentOnboardingIndex = 1;
  updateOnboardingPosition(1);
  updateInstallButton();
};

const startExperience = () => {
  setAppReady();
  if (hasSeenOnboarding()) {
    hideOnboarding();
    enterApp();
    return;
  }
  if (isInstalledApp()) {
    document.querySelectorAll(".install-btn").forEach((el) => el.remove());
    showOnboardingSlides();
    return;
  }
  if (hasCompletedInstallFlow()) {
    showOnboardingSlides();
    return;
  }
  showInstallCard();
};

const playSplashAndBoot = () => {
  const splash = splashScreenEl();
  if (!splash) {
    startExperience();
    return;
  }
  window.setTimeout(() => {
    splash.classList.add("hide");
    window.setTimeout(() => {
      splash.hidden = true;
      startExperience();
    }, 160);
  }, 90);
};

const setUpdateBanner = (visible) => {
  const banner = $("updateBanner");
  if (!banner) return;
  banner.hidden = !visible;
  banner.classList.toggle("visible", visible);
};

const setUpdateBannerMessage = (message) => {
  const banner = $("updateBanner");
  if (!banner) return;
  banner.textContent = message;
};

const compareVersionStrings = (left, right) => {
  const a = String(left || "").split(".").map((part) => Number(part) || 0);
  const b = String(right || "").split(".").map((part) => Number(part) || 0);
  const length = Math.max(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    const aValue = a[index] || 0;
    const bValue = b[index] || 0;
    if (aValue > bValue) return 1;
    if (aValue < bValue) return -1;
  }
  return 0;
};

const syncUpdateBannerState = () => {
  const dismissedVersion = sessionStorage.getItem(UPDATE_BANNER_DISMISSED_KEY);
  const shouldShow = Boolean(serverUpdateVersion) && dismissedVersion !== serverUpdateVersion;
  if (shouldShow) {
    setUpdateBannerMessage("New update available • Tap to refresh");
  }
  setUpdateBanner(shouldShow);
};

const checkForServerUpdate = async () => {
  try {
    const response = await fetch(UPDATE_CHECK_URL, { cache: "no-store" });
    if (!response.ok) {
      syncUpdateBannerState();
      return;
    }
    const data = await response.json();
    if (!data || !data.version) {
      syncUpdateBannerState();
      return;
    }
    const storedVersion = localStorage.getItem(UPDATE_VERSION_KEY);
    if (!storedVersion) {
      localStorage.setItem(UPDATE_VERSION_KEY, data.version);
      serverUpdateVersion = null;
      syncUpdateBannerState();
      return;
    }
    const hasNewVersion = Boolean(data.updateAvailable) && compareVersionStrings(data.version, storedVersion) > 0;
    serverUpdateVersion = hasNewVersion ? data.version : null;
    syncUpdateBannerState();
  } catch (error) {
    console.error("Update check failed", error);
    syncUpdateBannerState();
  }
};

const scheduleUpdateChecks = () => {
  window.setTimeout(() => {
    checkForServerUpdate();
    if (updateCheckTimer) window.clearInterval(updateCheckTimer);
    updateCheckTimer = window.setInterval(checkForServerUpdate, UPDATE_CHECK_INTERVAL);
  }, 2000);
};

const bindPullToRefreshGuard = () => {
  /* Keep the hook in place for future refinement, but avoid aggressive
     touch interception that can block normal app scrolling on mobile. */
};

const updateInstallButton = () => {
  const button = profileInstallButtonEl();
  if (!button) return;
  const installed = isInstalledApp();
  if (installed || !canShowInstall || !deferredInstallPrompt) {
    button.hidden = true;
    button.style.display = "none";
    return;
  }
  const available = Boolean(deferredInstallPrompt);
  button.hidden = !available;
  button.style.display = available ? "" : "none";
  const installValue = $("installStatus");
  if (installValue) {
    installValue.textContent = "Tap to install";
  }
};

const openDetail = (fundId) => {
  const fund = appData.funds.find((item) => item.id === fundId);
  if (!fund) {
    openGlobalModal(`<div class="detail-hero"><p class="eyebrow">Fund detail</p><h2>Fund unavailable</h2><p class="muted">The selected fund record could not be loaded.</p></div>`, { kind: "detail" });
    return;
  }
  openGlobalModal(`<div class="detail-hero"><p class="eyebrow">${escapeHtml(state.category)}</p><h2>${escapeHtml(fund.fundName)}</h2><p class="muted">Loading fund details...</p></div>`, { kind: "detail" });
  requestAnimationFrame(() => {
  const detailContent = modalContent();
  if (!detailContent) return;
  const displayRank = currentRankingMap(allCategoryFunds()).get(fund.id) || fund.rank;
  const fundHistory = mergedFundHistory(fund);
  const rows = fundHistory.length
    ? fundHistory.map((point) => `<tr><td>${escapeHtml(point.date || "-")}</td><td>${scoreLabel(point.score)}</td><td>${formatPct(point.oneYear)}</td><td>${formatPct(point.threeYear)}</td><td>${formatPct(point.fiveYear)}</td></tr>`).join("")
    : `<tr><td colspan="5">No historical rows available for this fund.</td></tr>`;
  const params = (fundHistory.slice(-1)[0]?.parameters || fund.parameterBreakdown || []).map((param, index) => {
    const pct = Math.max(0, Math.min(100, Math.round((param.normalized || 0) * 100)));
    const contribution = param.contribution === null || param.contribution === undefined ? "-" : param.contribution.toFixed(1);
    return `<div class="parameter-row"><div><strong>${escapeHtml(param.label)}</strong><small>${param.value ?? "-"} | rank ${param.rank || "-"}</small></div><span>${contribution}</span><div class="parameter-track"><i style="--bar-width:${pct}%; --bar-delay:${120 + (index * 100)}ms"></i></div></div>`;
  }).join("");
  detailContent.innerHTML = `
    <div class="detail-hero">
      <p class="eyebrow">${escapeHtml(state.category)} | ${datasetLabel()}</p>
      <h2>${escapeHtml(fund.fundName)}</h2>
      <span class="rank-badge">Rank #${displayRank}</span>
    </div>
<div class="detail-grid">
      <div class="mini-stat"><small>Score</small><strong>${scoreLabel(scoreOf(fund))}</strong></div>
      <div class="mini-stat"><small>Trend</small><strong>${trendMarkup(fund.trend)}</strong></div>
      <div class="mini-stat"><small>Risk</small><strong>${riskLabel(fund)}</strong></div>
      <div class="mini-stat"><small>1Y</small><strong>${formatPct(fund.oneYear)}</strong></div>
      <div class="mini-stat"><small>3Y</small><strong>${formatPct(fund.threeYear)}</strong></div>
      <div class="mini-stat"><small>5Y</small><strong>${formatPct(fund.fiveYear)}</strong></div>
    </div>
     <div class="chart-panel"><div class="section-head"><div><p class="eyebrow">Score movement</p><h3>Historical dashboard score</h3></div></div><div id="detailLine" class="line-chart"></div><div id="detailHistoryState" class="chart-tooltip"></div></div>
    <div class="insight-card primary"><p class="eyebrow">Summary</p><h3>${escapeHtml(fund.fundName)} is ranked #${displayRank} in ${escapeHtml(fund.category)} with dashboard score ${scoreLabel(scoreOf(fund))}.</h3></div>
    <div class="insight-card"><div style="display:flex; justify-content:space-between; align-items:center;">
  <p class="eyebrow">Parameter contribution</p>
  <p class="eyebrow" style="margin:0;">Score</p>
</div>
<div class="parameter-list">${params || "<p class='muted'>Parameter contribution is unavailable for this record.</p>"}</div></div>
    <table class="history-table"><thead><tr><th>Date</th><th>Score</th><th>1Y</th><th>3Y</th><th>5Y</th></tr></thead><tbody>${rows}</tbody></table>
  `;
  if (fundHistory.length) {
    const validScorePoints = fundHistory.filter((point) => typeof point?.score === "number");
    if (validScorePoints.length > 1) {
      renderMultiLineSvg($("detailLine"), [{ name: "Dashboard score", values: validScorePoints.map((point) => point.score), color: "#0F766E" }], validScorePoints.map((point) => (point.date || "Current").slice(-5)), $("detailHistoryState"));
      $("detailHistoryState").hidden = false;
    } else if (validScorePoints.length === 1) {
      const onlyPoint = validScorePoints[0];
      renderMultiLineSvg($("detailLine"), [{ name: "Dashboard score", values: [onlyPoint.score], color: "#0F766E" }], [(onlyPoint.date || "Latest").slice(-5)], $("detailHistoryState"));
      $("detailHistoryState").textContent = "Limited historical data";
    } else {
      $("detailLine").innerHTML = `<div class="empty-chart">Historical score data is unavailable for this fund.</div>`;
      $("detailHistoryState").textContent = "Latest metrics are still available above.";
    }
  } else {
    $("detailLine").innerHTML = `<div class="empty-chart">Historical score data is unavailable for this fund.</div>`;
    $("detailHistoryState").textContent = "Latest metrics are still available above.";
  }
  });
};

const buildMiniReportChart = (funds) => {
  const width = 620;
  const height = 260;
  const padding = { top: 30, right: 28, bottom: 44, left: 36 };
  const values = funds.map((fund) => scoreOf(fund));
  const max = Math.max(10, ...values);
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const barWidth = Math.min(74, plotWidth / Math.max(2, funds.length * 1.6));
  const gap = funds.length > 1 ? (plotWidth - barWidth * funds.length) / (funds.length - 1) : 0;
  const ticks = [0, Math.round(max / 2), max];
  const grid = ticks.map((tick) => {
    const y = height - padding.bottom - (tick / max) * plotHeight;
    return `<g><line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="#cbd5e1"/><text x="6" y="${y + 4}" fill="#64748b" font-size="11">${tick}</text></g>`;
  }).join("");
  const bars = funds.map((fund, index) => {
    const x = padding.left + index * (barWidth + gap);
    const value = scoreOf(fund);
    const barHeight = (value / max) * plotHeight;
    const y = height - padding.bottom - barHeight;
    const color = index === 0 ? "#4a63ff" : "#58d0ff";
    return `<g><rect x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="14" fill="${color}"/><text x="${x + barWidth / 2}" y="${y - 8}" text-anchor="middle" fill="#0f172a" font-size="11" font-weight="800">${scoreLabel(value)}</text><text x="${x + barWidth / 2}" y="${height - 18}" text-anchor="middle" fill="#64748b" font-size="10" font-weight="700">${escapeHtml(fund.fundName.slice(0, 14))}</text></g>`;
  }).join("");
  return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Top fund dashboard scores">${grid}${bars}</svg>`;
};

const buildReportHtml = () => {
  const fund = selectedFund();
  const summary = summaryForCategory();
  const categoryFunds = allCategoryFunds();
  const topFunds = categoryFunds.slice(0, 10);
  const chartFunds = categoryFunds.slice(0, 5);
  const rows = historyFor(fund).map((point) => `<tr><td>${point.date}</td><td>${scoreLabel(point.score)}</td><td>${formatPct(point.oneYear)}</td><td>${formatPct(point.threeYear)}</td><td>${formatPct(point.fiveYear)}</td></tr>`).join("");
  const leagueTable = topFunds.map((item) => `<tr><td>#${item.rank}</td><td>${escapeHtml(item.fundName)}</td><td>${scoreLabel(scoreOf(item))}</td><td>${formatPct(item.oneYear)}</td><td>${formatPct(item.threeYear)}</td><td>${formatPct(item.fiveYear)}</td><td>${riskLabel(item)}</td></tr>`).join("");
  return `
    <html><head><title>${APP_NAME} Report</title><style>
      body{font-family:Inter,Arial,sans-serif;padding:32px;color:#0f172a;background:#f8fafc} h1,h2,h3{margin:0 0 12px} .muted{color:#64748b;line-height:1.5}
      .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}
      .card{border:1px solid #dbe3ef;border-radius:16px;padding:18px;background:#fff;box-shadow:0 10px 28px rgba(15,23,42,.05)}
      .wide{grid-column:1/-1}.two{display:grid;grid-template-columns:1.2fr .8fr;gap:16px}
      table{width:100%;border-collapse:collapse;margin-top:16px;font-size:13px} th,td{padding:10px;border-bottom:1px solid #e5e7eb;text-align:left}
      .badge{display:inline-block;padding:6px 10px;border-radius:999px;background:#edf1ff;color:#4a63ff;font-weight:700;font-size:12px}
      ul{padding-left:18px;line-height:1.7}
      .cover{padding:28px;border-radius:24px;background:linear-gradient(135deg,#f8faff,#eaf0fb);border:1px solid #dbe3ef;box-shadow:0 12px 36px rgba(15,23,42,.06)}
      .cover h1{font-size:34px}.cover p{max-width:720px}
      .chart-box{margin-top:16px;padding:18px;border:1px solid #dbe3ef;border-radius:18px;background:#fff}
      .page-break{page-break-before:always}
    </style></head><body>
      <section class="cover">
        <p class="badge">${APP_NAME} category report</p>
        <h1>${state.category}</h1>
        <p class="muted">${formatDate(latestNavDateForCategory(state.category))} | ${datasetLabel()} | Built from the Excel-backed dashboard logic for category-level review, ranking, and fund comparison.</p>
      </section>
      <div class="grid">
        <div class="card"><h3>Category</h3><p>${state.category}</p></div>
        <div class="card"><h3>Total Funds</h3><p>${summary.totalFunds}</p></div>
        <div class="card"><h3>Average Score</h3><p>${summary.categoryAverageScore.toFixed(1)}</p></div>
        <div class="card"><h3>Average Return</h3><p>${summary.categoryAverageReturn.toFixed(1)}%</p></div>
      </div>
      <div class="card wide"><h2>Executive Summary</h2><p class="muted">${state.category} contains ${summary.totalFunds} analysed funds. The category leader is <strong>${escapeHtml(summary.topPerformer)}</strong>, while the selected reference fund is <strong>${escapeHtml(fund.fundName)}</strong> at rank #${fund.rank} with dashboard score ${scoreLabel(scoreOf(fund))}. Use the league table and historical section below for deeper analysis.</p></div>
      <div class="chart-box">${buildMiniReportChart(chartFunds)}</div>
      <div class="two">
        <div class="card"><h2>Selected Fund Snapshot</h2><p><span class="badge">Rank #${fund.rank}</span></p><p class="muted">Dashboard score ${scoreLabel(scoreOf(fund))} | ${horizonLabel()} ${formatPct(fund[state.horizon])} | ${riskLabel(fund)} | ${fund.trend}</p></div>
        <div class="card"><h2>Category Leader</h2><p><span class="badge">${escapeHtml(summary.topPerformer)}</span></p><p class="muted">Top category score ${scoreLabel(summary.topScore)} with ${summary.topTrend.toLowerCase()} trend.</p></div>
      </div>
      <div class="card wide page-break"><h2>Top 10 Funds In Category</h2><table><thead><tr><th>Rank</th><th>Fund</th><th>Score</th><th>1Y</th><th>3Y</th><th>5Y</th><th>Risk</th></tr></thead><tbody>${leagueTable}</tbody></table></div>
      <div class="card wide"><h2>Selected Fund Historical Data</h2><table><thead><tr><th>Date</th><th>Score</th><th>1Y</th><th>3Y</th><th>5Y</th></tr></thead><tbody>${rows}</tbody></table></div>
      <div class="card wide"><h2>How To Read This Report</h2><ul><li><strong>Dashboard Score</strong> summarizes workbook-driven ranking logic.</li><li><strong>Rank</strong> compares funds within the selected category.</li><li><strong>Risk Indicator</strong> reflects score volatility across available periods.</li><li><strong>Returns</strong> help compare shorter and longer horizon performance.</li></ul></div>
      <div class="card wide"><h2>Methodology</h2><p class="muted">This report is generated from the Excel-backed dashboard logic using category-filtered data, rank-based weighted scoring, and available period histories from the workbook sheets. It is designed to support user-led analysis, not to provide financial advice.</p></div>
    </body></html>
  `;
};

const syncStateToData = () => {
  if (!appData?.categories?.length) return;
  if (!appData.categories.includes(state.category)) {
    state.category = appData.categories[0];
  }
  state.selectedFundId = null;
  state.compareFundAId = null;
  state.compareFundBId = null;
  const categoryFunds = allCategoryFunds();
  if (!categoryFunds.length) return;
  state.selectedFundId = categoryFunds[0].id;
  state.compareFundAId = categoryFunds[0].id;
  state.compareFundBId = categoryFunds[1]?.id || categoryFunds[0].id;
};

const handleUpload = async (file) => {
  if (!file) return;
  localStorage.setItem("fundpulse-live-upload-name", file.name);
  $("uploadStatus").textContent = file.name.slice(0, 22);
  if (file.name.endsWith(".json")) {
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const parsed = JSON.parse(String(reader.result));
        if (!["excel-dashboard", "live-dashboard"].includes(parsed.analysis) || !parsed.funds || !parsed.summaries) throw new Error("Invalid data");
        appData = normalizeAppData(parsed);
        localStorage.setItem(APP_DATA_STORAGE_KEY, JSON.stringify(appData));
        syncStateToData();
        renderAll();
        $("uploadStatus").textContent = "Updated";
      } catch {
        $("uploadStatus").textContent = "Invalid file";
      }
    };
    reader.readAsText(file);
    return;
  }
  $("uploadStatus").textContent = "Importing...";
  try {
    const imported = normalizeAppData(await window.WorkbookImporter.buildDataFromWorkbookFile(file));
    appData = imported;
    localStorage.removeItem(APP_DATA_STORAGE_KEY);
    localStorage.setItem(APP_DATA_STORAGE_KEY, JSON.stringify(imported));
    syncStateToData();
    renderAll();
    $("uploadStatus").textContent = "Updated";
  } catch (error) {
    console.error(`${APP_NAME} workbook import failed`, error);
    $("uploadStatus").textContent = "Import failed";
  }
};

const renderAll = () => {
  try {
    renderChrome();
    renderHeaderControls();
    renderDashboard();
    renderFunds();
    renderInsights();
    renderCompare();
    renderProfile();
    syncControlState();
  } catch (error) {
    console.error(`${APP_NAME} render failure`, error);
    const activeScreen = document.querySelector(".screen.active");
    if (activeScreen) {
      activeScreen.innerHTML = `<article class="insight-card fallback-card"><p class="eyebrow">Something went wrong</p><h3>We could not load this view cleanly.</h3><p class="story-copy">Try switching category or tab again. The app caught the error instead of blanking the screen.</p></article>`;
    }
  }
};

const renderCurrentView = () => {
  renderChrome();
  renderHeaderControls();

  if (state.tab === "dashboard") renderDashboard();
  if (state.tab === "funds") renderFunds();
  if (state.tab === "insights") renderInsights();
  if (state.tab === "compare") renderCompare();
  if (state.tab === "profile") renderProfile();

  syncControlState();
};

const persistLiveDataWhenIdle = (data) => {
  const save = () => {
    try {
      localStorage.setItem(APP_DATA_STORAGE_KEY, JSON.stringify(data));
    } catch (error) {
      console.warn(`${APP_NAME} live data cache write skipped`, error);
    }
  };

  if ("requestIdleCallback" in window) {
    window.requestIdleCallback(save, { timeout: 1500 });
  } else {
    window.setTimeout(save, 120);
  }
};

const bindEvents = () => {
  $("themeToggle").addEventListener("click", () => setTheme(state.theme === "dark" ? "light" : "dark"));
  $("profileTheme").addEventListener("click", () => setTheme(state.theme === "dark" ? "light" : "dark"));
  $("categoryTrigger").addEventListener("click", () => openPicker("category"));
  $("compareFundATrigger").addEventListener("click", () => openPicker("compareFundA"));
  $("compareFundBTrigger").addEventListener("click", () => openPicker("compareFundB"));
  const topPerformerCard = $("topPerformerCard");
  if (topPerformerCard) {
    bindFundCardInteractions(topPerformerCard, openTopPerformerInsight);
  }
  $("global-modal-backdrop").addEventListener("click", closeGlobalModal);
  $("global-modal-close").addEventListener("click", closeGlobalModal);
  window.addEventListener("popstate", () => {
    const root = modalRoot();
    if (!root || root.hidden || root.getAttribute("aria-hidden") === "true") return;
    modalHistoryNavigating = true;
    closeGlobalModal(false, { fromHistory: true });
  });

  document.querySelectorAll(".bottom-nav [data-tab]").forEach((button) => {
    button.addEventListener("click", () => switchTab(button.dataset.tab, 0));
  });

  document.querySelectorAll("[data-horizon]").forEach((button) => {
    button.addEventListener("click", () => {
      applyOptionValue("horizon", button.dataset.horizon, 0);
    });
  });

  document.querySelectorAll(".time-filter button").forEach((button) => {
    button.addEventListener("click", () => {
      applyOptionValue("period", button.dataset.period, 0);
    });
  });

  document.querySelectorAll(".chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      applyOptionValue("sort", chip.dataset.sort, 0);
    });
  });

  document.querySelectorAll("[data-fund-view]").forEach((button) => {
    button.addEventListener("click", () => {
      state.fundView = button.dataset.fundView || "all";
      localStorage.setItem("fundpulse-live-fund-view", state.fundView);
      renderFunds();
      syncControlState();
    });
  });

$("searchInput").addEventListener("input", (event) => {
  const nextValue = event.target.value;
  if (searchInputTimer) {
    window.clearTimeout(searchInputTimer);
  }
  searchInputTimer = window.setTimeout(() => {
    searchInputTimer = 0;
    state.query = nextValue;
    renderFunds();
  }, 70);
});

  $("clearSearch").addEventListener("click", () => {
    state.query = "";
    $("searchInput").value = "";
    renderFunds();
  });

  $("methodologyButton").addEventListener("click", () => {
    openGlobalModal(methodologyMarkup(), { kind: "docs", size: "wide" });
  });

  $("onboardingInstallButton")?.addEventListener("click", async () => {
    if (deferredInstallPrompt) {
      deferredInstallPrompt.prompt();
      try {
        await deferredInstallPrompt.userChoice;
      } finally {
        deferredInstallPrompt = null;
        updateInstallButton();
      }
    }
    localStorage.removeItem(BROWSER_INSTALL_CTA_KEY);
    localStorage.setItem(INSTALL_FLOW_KEY, "true");
    showOnboardingSlides();
  });

  $("onboardingSkipInstall")?.addEventListener("click", () => {
    localStorage.removeItem(BROWSER_INSTALL_CTA_KEY);
    localStorage.setItem(INSTALL_FLOW_KEY, "true");
    showOnboardingSlides();
  });
  profileInstallButtonEl()?.addEventListener("click", async () => {
    if (!deferredInstallPrompt) return;
    deferredInstallPrompt.prompt();
    try {
      const choice = await deferredInstallPrompt.userChoice;
      if (choice?.outcome === "accepted") {
        const installValue = $("installStatus");
        if (installValue) installValue.textContent = "Installed";
        updateInstallButton();
      }
    } finally {
      deferredInstallPrompt = null;
      canShowInstall = false;
      updateInstallButton();
    }
  });
  document.querySelectorAll(".onboarding-next").forEach((button) => {
    button.addEventListener("click", handleOnboardingNext);
  });
  document.querySelectorAll(".onboarding-skip-all").forEach((button) => {
    button.addEventListener("click", finishOnboarding);
  });

  document.addEventListener("keydown", (event) => {
    markUserInteraction();
    if (event.key === "Escape") closeAllOverlays();
  });

  ["pointerdown", "touchstart", "wheel"].forEach((eventName) => {
    document.addEventListener(eventName, markUserInteraction, { passive: true });
  });

  window.addEventListener("hashchange", () => {
    markUserInteraction();
    const nextTab = routeFromHash();
    if (nextTab !== state.tab) {
      state.tab = nextTab;
      syncTabUi();
      renderCurrentView();
      requestAnimationFrame(() => resetViewScroll("auto"));
    }
  });

  window.addEventListener("load", () => {
    const app = $("app");
    if (!window.location.hash) {
      window.location.hash = "#dashboard";
    }
    if (app && !app.querySelector(".screen.active")) {
      loadDashboard();
      return;
    }
    const activeScreen = document.querySelector(".screen.active");
    if (activeScreen && !activeScreen.children.length) {
      loadDashboard();
    }
  });

  $("excelUpload").addEventListener("change", (event) => handleUpload(event.target.files[0]));
  $("updateBanner")?.addEventListener("click", () => {
    if (serverUpdateVersion) {
      sessionStorage.setItem(UPDATE_BANNER_DISMISSED_KEY, serverUpdateVersion);
      localStorage.setItem(UPDATE_VERSION_KEY, serverUpdateVersion);
      serverUpdateVersion = null;
      setUpdateBanner(false);
    }
    window.location.reload();
  });
  $("saveReport").addEventListener("click", () => {
    const reportWindow = window.open("", "_blank", "width=1100,height=800");
    if (!reportWindow) return;
    reportWindow.document.write(buildReportHtml());
    reportWindow.document.close();
    reportWindow.focus();
    setTimeout(() => reportWindow.print(), 300);
  });

  window.addEventListener("beforeinstallprompt", (event) => {
    event.preventDefault();
    deferredInstallPrompt = event;
    canShowInstall = true;
    updateInstallButton();
  });

  window.addEventListener("appinstalled", () => {
    localStorage.setItem(INSTALL_FLOW_KEY, "true");
    deferredInstallPrompt = null;
    canShowInstall = false;
    updateInstallButton();
  });

  window.matchMedia?.("(display-mode: standalone)")?.addEventListener?.("change", updateInstallButton);
  window.addEventListener("resize", () => requestAnimationFrame(updateAllSegmentedPills));
  bindPullToRefreshGuard();
};

const registerServiceWorkerWhenIdle = () => {
  if (!("serviceWorker" in navigator) || location.protocol === "file:") return;
  const startRegistration = () => {
    navigator.serviceWorker.register("./service-worker.js").catch(() => {});
  };
  if ("requestIdleCallback" in window) {
    window.requestIdleCallback(startRegistration, { timeout: 1200 });
  } else {
    window.setTimeout(startRegistration, 180);
  }
};

const init = () => {
  document.documentElement.classList.remove("app-ready");
  const onboardingDone = hasSeenOnboarding();
  if (onboardingDone) {
    onboardingEl()?.setAttribute("hidden", "true");
    if (onboardingEl()) onboardingEl().style.display = "none";
  }
  ensureHashRoute();
  state.tab = routeFromHash();
  setTheme(state.theme);
  bindEvents();
  updateInstallButton();
  syncTabUi();
  playSplashAndBoot();
};

window.resetOnboarding = () => {
  localStorage.removeItem(ONBOARDING_KEY);
  localStorage.removeItem(LEGACY_ONBOARDING_KEY);
  location.reload();
};

let pendingLiveUpdateData = null;
let liveUpdateFrameScheduled = false;

window.addEventListener("live-data:updated", (event) => {
  pendingLiveUpdateData = event.detail?.data || null;
  const forceLiveUpdate = event.detail?.force === true;
  if (liveUpdateFrameScheduled) return;
  liveUpdateFrameScheduled = true;

  const applyPendingUpdate = () => {
    liveUpdateFrameScheduled = false;
    const nextData = normalizeAppData(pendingLiveUpdateData);
    pendingLiveUpdateData = null;
    if (!nextData?.funds || !nextData?.summaries) return;

    const currentDate = safeIsoDate(appData?.liveNavDate);
    const nextDate = safeIsoDate(nextData?.liveNavDate);
    if (!forceLiveUpdate && currentDate && nextDate && currentDate === nextDate) return;

    appData = nextData;
    persistLiveDataWhenIdle(nextData);
    syncStateToData();
    renderCurrentView();
    if ($("uploadStatus")) {
      $("uploadStatus").textContent = "Live synced";
    }
  };

  if ("requestAnimationFrame" in window) {
    window.requestAnimationFrame(() => window.requestAnimationFrame(applyPendingUpdate));
    return;
  }
  window.setTimeout(applyPendingUpdate, 0);
});

init();
registerServiceWorkerWhenIdle();
scheduleUpdateChecks();

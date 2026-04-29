window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.validation = (() => {
  const clone = (value) => JSON.parse(JSON.stringify(value));

  const ensureArray = (value) => Array.isArray(value) ? value : [];

  const ensureFundShape = (fund, backupFund = {}) => ({
    ...clone(backupFund),
    ...clone(fund),
    history: ensureArray(fund?.history ?? backupFund?.history),
    parameterBreakdown: ensureArray(fund?.parameterBreakdown ?? backupFund?.parameterBreakdown),
    analysisHistory: {
      old: ensureArray(fund?.analysisHistory?.old ?? backupFund?.analysisHistory?.old),
      new: ensureArray(fund?.analysisHistory?.new ?? backupFund?.analysisHistory?.new)
    }
  });

  const ensureAppShape = (data, backupData) => {
    const safeBackup = backupData ? clone(backupData) : null;
    if (!data || !Array.isArray(data.funds) || !Array.isArray(data.summaries)) {
      return safeBackup;
    }

    const backupFunds = new Map(ensureArray(safeBackup?.funds).map((fund) => [fund.id, fund]));
    return {
      ...(safeBackup || {}),
      ...clone(data),
      categories: ensureArray(data.categories?.length ? data.categories : safeBackup?.categories),
      summaries: ensureArray(data.summaries?.length ? data.summaries : safeBackup?.summaries),
      funds: ensureArray(data.funds).map((fund) => ensureFundShape(fund, backupFunds.get(fund.id)))
    };
  };

  return { ensureAppShape, clone };
})();

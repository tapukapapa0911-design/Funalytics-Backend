function parseAmfiDate(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return parsed;

  const [day, month, year] = raw.split(/[-/]/);
  const monthMap = {
    jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
    jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11
  };
  if (day && month && year) {
    const monthIndex = monthMap[String(month).slice(0, 3).toLowerCase()];
    if (monthIndex !== undefined) return new Date(Date.UTC(Number(year), monthIndex, Number(day)));
    return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  }
  return null;
}

function normalizeSchemeName(value) {
  return String(value || "").trim().replace(/\s+/g, " ");
}

export function parseNavAllText(rawText) {
  const deduped = new Map();
  const rows = String(rawText || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => /^\d+;/.test(line));

  for (const row of rows) {
    const parts = row.split(";");
    if (parts.length < 6) continue;
    const [schemeCodeRaw, isinGrowth, isinReinvestment, schemeNameRaw, navRaw, dateRaw] = parts;
    const schemeCode = String(schemeCodeRaw || "").trim();
    const schemeName = normalizeSchemeName(schemeNameRaw);
    const nav = Number(navRaw);
    const navDate = parseAmfiDate(dateRaw);
    if (!schemeCode || !schemeName || !Number.isFinite(nav) || !navDate) continue;

    deduped.set(schemeCode, {
      schemeCode,
      schemeName,
      nav,
      navDate,
      isin: String(isinGrowth || isinReinvestment || "").trim(),
      source: "amfi"
    });
  }

  return [...deduped.values()];
}

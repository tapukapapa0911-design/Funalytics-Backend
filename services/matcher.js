window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.matcher = (() => {
  const STOP_WORDS = new Set([
    "direct", "growth", "plan", "option", "idcw", "regular", "reinvestment",
    "payout", "dividend", "bonus", "fund", "the", "of", "and", "g", "erstwhile"
  ]);
  const BRANDS = [
    "bank of india", "baroda bnp paribas", "canara robeco", "mahindra manulife", "motilal oswal",
    "icici", "prudential", "hdfc", "sbi", "dsp", "nippon", "axis", "kotak", "aditya", "birla",
    "invesco", "franklin", "tata", "bandhan", "mirae", "parag", "pgim", "uti", "edelweiss",
    "hsbc", "lic", "union", "quant", "whiteoak", "bajaj", "jm", "iti", "sundaram", "samco"
  ];
  const CATEGORIES = [
    "large cap", "mid cap", "small cap", "flexi cap", "multi cap", "value", "focused", "bluechip",
    "contra", "elss", "arbitrage", "balanced advantage", "dynamic asset allocation", "liquid",
    "conservative hybrid", "aggressive hybrid", "short duration", "ultra short", "corporate bond",
    "dynamic bond", "banking psu", "money market", "overnight", "low duration", "medium duration",
    "large and mid cap", "large mid cap"
  ];

  const clean = (value) => String(value || "")
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ")
    .replace(/\berstwhile\b/g, " ")
    .replace(/\bpru\b/g, "prudential")
    .replace(/\bsl\b/g, "sun life")
    .replace(/smallcap/g, "small cap")
    .replace(/midcap/g, "mid cap")
    .replace(/multicap/g, "multi cap")
    .replace(/flexicap/g, "flexi cap")
    .replace(/largecap/g, "large cap")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const normalizedName = (value) => clean(value)
    .split(" ")
    .filter(Boolean)
    .filter((token) => !STOP_WORDS.has(token))
    .join(" ");

  const tokens = (value) => normalizedName(value).split(" ").filter(Boolean);

  const extractBrand = (value) => {
    const text = normalizedName(value);
    return BRANDS.find((brand) => text.includes(brand)) || "";
  };

  const extractCategory = (value) => {
    const text = normalizedName(value);
    return CATEGORIES.find((category) => text.includes(category)) || "";
  };

  const prefixKey = (value, count = 3) => tokens(value).slice(0, count).join(" ");
  const firstToken = (value) => tokens(value)[0] || "";
  const looksRegular = (value) => /(?:\bregular\b|\breg\b|\(g\))/i.test(String(value || ""));
  const looksDirect = (value) => /(?:\bdirect\b|\bdir\b)/i.test(String(value || ""));

  const jaccard = (leftTokens, rightTokens) => {
    const left = new Set(leftTokens);
    const right = new Set(rightTokens);
    if (!left.size || !right.size) return 0;
    let intersection = 0;
    left.forEach((token) => {
      if (right.has(token)) intersection += 1;
    });
    return intersection / new Set([...left, ...right]).size;
  };

  const withPlanBias = (fundName, schemeName, score) => {
    let next = score;
    const inputLooksRegular = looksRegular(fundName) || !looksDirect(fundName);
    const inputLooksDirect = looksDirect(fundName);
    const candidate = String(schemeName || "");

    if (inputLooksRegular) {
      if (/regular/i.test(candidate)) next += 0.28;
      if (/growth/i.test(candidate)) next += 0.12;
      if (/direct/i.test(candidate)) next -= 0.42;
    }

    if (inputLooksDirect) {
      if (/direct/i.test(candidate)) next += 0.18;
      if (/regular/i.test(candidate)) next -= 0.14;
    }

    if (/idcw|dividend|bonus|payout/i.test(candidate)) next -= 0.28;
    return next;
  };

  const scoreCandidate = (fund, row) => {
    const fundNorm = normalizedName(fund.fundName || fund.rawFundName);
    const rowNorm = normalizedName(row.schemeName);
    if (!fundNorm || !rowNorm) return 0;
    if (fundNorm === rowNorm) return 1;

    const fundPrefix = prefixKey(fundNorm);
    const rowPrefix = prefixKey(rowNorm);
    const fundBrand = extractBrand(fundNorm);
    const rowBrand = extractBrand(rowNorm);
    const fundCategory = extractCategory(fund.category || fundNorm);
    const rowCategory = extractCategory(rowNorm);

    let score = jaccard(tokens(fundNorm), tokens(rowNorm));
    if (fundPrefix && fundPrefix === rowPrefix) score = Math.max(score, 0.92);
    if (fundBrand && rowBrand && fundBrand === rowBrand) score += 0.08;
    if (fundCategory && rowCategory && fundCategory === rowCategory) score += 0.08;
    if (rowNorm.includes(fundPrefix) || fundNorm.includes(rowPrefix)) score += 0.05;
    score = withPlanBias(fund.fundName || fund.rawFundName || "", row.schemeName, score);
    return Math.min(score, 0.99);
  };

  const matchFundToScheme = (fund, schemeRows) => {
    if (!Array.isArray(schemeRows) || !schemeRows.length) return null;

    if (fund.schemeCode) {
      const exact = schemeRows.find((row) => String(row.schemeCode) === String(fund.schemeCode));
      if (exact) return { row: exact, confidence: 1, method: "scheme-code" };
    }

    const exact = schemeRows.find((row) => normalizedName(row.schemeName) === normalizedName(fund.fundName || fund.rawFundName));
    if (exact) return { row: exact, confidence: 1, method: "exact" };

    let best = null;
    for (const row of schemeRows) {
      const confidence = scoreCandidate(fund, row);
      if (!best || confidence > best.confidence) {
        best = { row, confidence, method: "fuzzy" };
      }
    }

    return best && best.confidence >= 0.7 ? best : null;
  };

  return {
    normalizedName,
    extractBrand,
    extractCategory,
    prefixKey,
    firstToken,
    matchFundToScheme
  };
})();

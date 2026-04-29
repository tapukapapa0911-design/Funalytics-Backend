(function () {
  const OLD_WEIGHTS = {
    "1Y": ["oneYear", true, 0.10],
    "3Y": ["threeYear", true, 0.20],
    "5Y": ["fiveYear", true, 0.25],
    "Sharpe": ["sharpe", true, 0.15],
    "Volatility": ["volatility", false, 0.30]
  };

  const NEW_WEIGHTS = {
    "1Y": ["oneYear", true, 0.10],
    "3Y": ["threeYear", true, 0.20],
    "5Y": ["fiveYear", true, 0.25],
    "Sharpe": ["sharpe", true, 0.15],
    "P/E": ["pe", false, 0.10],
    "P/B": ["pb", false, 0.10],
    "Sortino": ["sortino", true, 0.10]
  };

  const BUILTIN_DATE_FORMAT_IDS = new Set([14, 15, 16, 17, 18, 19, 20, 21, 22, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 45, 46, 47, 50, 51, 52, 53, 54, 55, 56, 57, 58]);

  const clean = (value) => value === null || value === undefined ? "" : String(value).trim();
  const canon = (value) => clean(value).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const keyOf = (value) => clean(value).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 90);
  const standardizeName = (value) => clean(value).replace(/\bIcici\b/g, "ICICI");
  const excelProper = (value) => clean(value).toLowerCase().replace(/[A-Za-z]+/g, (match) => match.charAt(0).toUpperCase() + match.slice(1));
  const normalizeTrend = (value) => {
    const text = clean(value);
    if (text.includes("Improving")) return "Improving";
    if (text.includes("Declining")) return "Declining";
    return text ? "Stable" : "";
  };

  const normNum = (value) => {
    if (value === null || value === undefined || value === "" || value === "-") return null;
    if (typeof value === "number") {
      return Number.isFinite(value) ? value : null;
    }
    const numeric = Number(String(value).replace(/,/g, "").trim());
    return Number.isFinite(numeric) ? numeric : null;
  };

  const mean = (values) => {
    const cleanValues = values.filter((value) => typeof value === "number" && Number.isFinite(value));
    if (!cleanValues.length) return null;
    return cleanValues.reduce((sum, value) => sum + value, 0) / cleanValues.length;
  };

  const sampleStdev = (values) => {
    const cleanValues = values.filter((value) => typeof value === "number" && Number.isFinite(value));
    if (cleanValues.length <= 1) return 0;
    const avg = mean(cleanValues);
    const variance = cleanValues.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (cleanValues.length - 1);
    return Math.sqrt(variance);
  };

  const round = (value, digits = 0) => {
    if (!Number.isFinite(value)) return value;
    const factor = 10 ** digits;
    return Math.round(value * factor) / factor;
  };

  const excelDateToIso = (serial) => {
    if (!Number.isFinite(serial)) return null;
    const utcDays = Math.floor(serial - 25569);
    const fractionalDay = serial - Math.floor(serial);
    const milliseconds = Math.round((utcDays * 86400 + fractionalDay * 86400) * 1000);
    return new Date(milliseconds).toISOString().slice(0, 10);
  };

  const normDate = (value) => {
    if (!value && value !== 0) return null;
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) return trimmed.slice(0, 10);
      const parsed = Date.parse(trimmed);
      if (!Number.isNaN(parsed)) return new Date(parsed).toISOString().slice(0, 10);
      return trimmed.slice(0, 10);
    }
    if (typeof value === "number") return excelDateToIso(value);
    if (value instanceof Date && !Number.isNaN(value.getTime())) return value.toISOString().slice(0, 10);
    return null;
  };

  const parseXml = (xmlText) => {
    const doc = new DOMParser().parseFromString(xmlText, "application/xml");
    if (doc.querySelector("parsererror")) {
      throw new Error("Could not parse workbook XML");
    }
    return doc;
  };

  const xmlText = async (zip, path) => {
    const file = zip.file(path);
    return file ? file.async("string") : "";
  };

  const sheetPath = (base, target) => {
    const normalizedTarget = clean(target).replace(/\\/g, "/");
    if (!normalizedTarget) return "";
    if (normalizedTarget.startsWith("/")) return normalizedTarget.replace(/^\//, "");
    const baseParts = base.split("/").slice(0, -1);
    normalizedTarget.split("/").forEach((segment) => {
      if (!segment || segment === ".") return;
      if (segment === "..") {
        baseParts.pop();
      } else {
        baseParts.push(segment);
      }
    });
    return baseParts.join("/");
  };

  const getSharedStrings = async (zip) => {
    const xml = await xmlText(zip, "xl/sharedStrings.xml");
    if (!xml) return [];
    const doc = parseXml(xml);
    return [...doc.getElementsByTagName("si")].map((si) =>
      [...si.getElementsByTagName("t")].map((node) => node.textContent || "").join("")
    );
  };

  const getDateStyleIndexes = async (zip) => {
    const xml = await xmlText(zip, "xl/styles.xml");
    if (!xml) return new Set();
    const doc = parseXml(xml);
    const customFormats = new Map(
      [...doc.getElementsByTagName("numFmt")].map((node) => [Number(node.getAttribute("numFmtId")), node.getAttribute("formatCode") || ""])
    );
    const xfs = [...doc.getElementsByTagName("cellXfs")[0]?.getElementsByTagName("xf") || []];
    const indexes = new Set();
    xfs.forEach((xf, index) => {
      const formatId = Number(xf.getAttribute("numFmtId"));
      const formatCode = customFormats.get(formatId) || "";
      if (BUILTIN_DATE_FORMAT_IDS.has(formatId) || /([ymdhis])/.test(formatCode.replace(/\[[^\]]+\]/g, "").toLowerCase())) {
        indexes.add(index);
      }
    });
    return indexes;
  };

  const getWorkbookSheets = async (zip) => {
    const workbookXml = await xmlText(zip, "xl/workbook.xml");
    const relsXml = await xmlText(zip, "xl/_rels/workbook.xml.rels");
    if (!workbookXml || !relsXml) throw new Error("Workbook structure is incomplete");
    const workbook = parseXml(workbookXml);
    const rels = parseXml(relsXml);
    const relationshipMap = new Map(
      [...rels.getElementsByTagName("Relationship")].map((rel) => [rel.getAttribute("Id"), sheetPath("xl/workbook.xml", rel.getAttribute("Target"))])
    );
    return new Map(
      [...workbook.getElementsByTagName("sheet")].map((sheet) => [
        sheet.getAttribute("name"),
        relationshipMap.get(sheet.getAttribute("r:id")) || ""
      ])
    );
  };

  const cellColumn = (reference) => {
    const match = String(reference || "").match(/[A-Z]+/);
    return match ? match[0] : "";
  };

  const readCellValue = (cell, sharedStrings, dateStyleIndexes) => {
    const type = cell.getAttribute("t");
    const styleIndex = Number(cell.getAttribute("s"));
    if (type === "inlineStr") {
      return [...cell.getElementsByTagName("t")].map((node) => node.textContent || "").join("");
    }
    const valueNode = cell.getElementsByTagName("v")[0];
    if (!valueNode) return "";
    const raw = valueNode.textContent || "";
    if (type === "s") return sharedStrings[Number(raw)] ?? "";
    if (type === "b") return raw === "1";
    if (type === "str" || type === "e") return raw;
    const numeric = Number(raw);
    if (Number.isFinite(numeric)) {
      if (dateStyleIndexes.has(styleIndex)) return excelDateToIso(numeric);
      return numeric;
    }
    return raw;
  };

  const readWorksheet = async (zip, path, sharedStrings, dateStyleIndexes) => {
    const xml = await xmlText(zip, path);
    if (!xml) throw new Error(`Missing worksheet: ${path}`);
    const doc = parseXml(xml);
    const rows = [];
    [...doc.getElementsByTagName("row")].forEach((row) => {
      const cells = {};
      [...row.getElementsByTagName("c")].forEach((cell) => {
        cells[cellColumn(cell.getAttribute("r"))] = readCellValue(cell, sharedStrings, dateStyleIndexes);
      });
      rows.push({
        index: Number(row.getAttribute("r")) || rows.length + 1,
        cells
      });
    });
    return rows;
  };

  const rankScore = (rows, row, metric, higherBetter) => {
    const value = row[metric];
    if (value === null || value === undefined) return [null, null];
    let rank = 1;
    rows.forEach((other) => {
      if (other[metric] === null || other[metric] === undefined) return;
      if (higherBetter ? other[metric] > value : other[metric] < value) rank += 1;
    });
    const total = rows.length;
    const score = rank === 1 || total <= 1 ? 1 : 1 - ((rank - 1) / (total - 1));
    return [rank, score];
  };

  const scorePeriodGroups = (rawRows) => {
    const groups = new Map();
    rawRows.forEach((row) => {
      const bucketKey = [row.model, row.date, canon(row.category)].join("|");
      if (!groups.has(bucketKey)) groups.set(bucketKey, []);
      groups.get(bucketKey).push(row);
    });

    const scored = [];
    groups.forEach((rows) => {
      const weights = rows[0]?.model === "old" ? OLD_WEIGHTS : NEW_WEIGHTS;
      const parameterCount = Object.keys(weights).length;
      rows.forEach((row) => {
        let weightedSum = 0;
        let availableWeight = 0;
        let availableCount = 0;
        const parameters = [];
        Object.entries(weights).forEach(([label, [metric, higherBetter, weight]]) => {
          const [rank, normalized] = rankScore(rows, row, metric, higherBetter);
          const value = row[metric];
          let contribution = null;
          if (normalized !== null) {
            weightedSum += normalized * weight;
            availableWeight += weight;
            availableCount += 1;
            contribution = normalized * weight * 100;
          }
          parameters.push({
            label,
            metric,
            value,
            rank,
            weight,
            direction: higherBetter ? "higher" : "lower",
            normalized,
            contribution
          });
        });

        const finalScore = availableWeight
          ? Math.round((weightedSum / availableWeight) * (availableCount / parameterCount) * 100)
          : null;

        scored.push({
          ...row,
          periodScore: finalScore,
          parameters
        });
      });
    });

    const periodGroups = new Map();
    scored.forEach((row) => {
      const bucketKey = [row.model, row.date, canon(row.category)].join("|");
      if (!periodGroups.has(bucketKey)) periodGroups.set(bucketKey, []);
      periodGroups.get(bucketKey).push(row);
    });

    periodGroups.forEach((rows) => {
      rows.forEach((row) => {
        const score = row.periodScore;
        row.periodRank = score === null
          ? null
          : rows.reduce((count, other) => count + ((other.periodScore !== null && other.periodScore > score) ? 1 : 0), 1);
        row.periodFlag = row.periodRank !== null && row.periodRank <= 5 ? "Top Performers" : "";
      });
    });

    return scored;
  };

  const readRawSheet = (rows, model) => {
    const items = [];
    rows.forEach(({ index, cells }) => {
      if (index < 4) return;
      if (!(cells.A && cells.B && cells.C)) return;
      const item = {
        model,
        date: normDate(cells.A),
        category: clean(cells.B),
        fundName: clean(cells.C),
        displayName: standardizeName(excelProper(cells.C)),
        oneYear: normNum(cells.D),
        threeYear: normNum(cells.E),
        fiveYear: normNum(cells.F),
        sharpe: normNum(cells.G)
      };
      if (model === "old") {
        item.volatility = normNum(cells.H);
      } else {
        item.pe = normNum(cells.H);
        item.pb = normNum(cells.I);
        item.sortino = normNum(cells.J);
      }
      items.push(item);
    });
    return items;
  };

  const readDashboardBackend = (rows) => {
    const category = standardizeName(rows.find((row) => row.index === 1)?.cells?.B || "");
    const mappedRows = new Map();
    rows.forEach(({ index, cells }) => {
      if (index < 5 || !cells.A) return;
      const fundName = standardizeName(cells.A);
      mappedRows.set(`${canon(category)}|${canon(fundName)}`, {
        category,
        fundName,
        dashboardScore: normNum(cells.G),
        consistency: normNum(cells.H),
        rank: normNum(cells.I) !== null ? Number(cells.I) : null,
        flag: clean(cells.J) || "Core",
        trend: normalizeTrend(cells.K) || "Stable"
      });
    });
    return { category, rows: mappedRows };
  };

  const buildData = async (zip, sourceLabel) => {
    const sharedStrings = await getSharedStrings(zip);
    const dateStyleIndexes = await getDateStyleIndexes(zip);
    const sheets = await getWorkbookSheets(zip);

    const rawOld = readRawSheet(await readWorksheet(zip, sheets.get("RawData_Old"), sharedStrings, dateStyleIndexes), "old");
    const rawNew = readRawSheet(await readWorksheet(zip, sheets.get("RawData_New"), sharedStrings, dateStyleIndexes), "new");
    const dashboardBackend = readDashboardBackend(await readWorksheet(zip, sheets.get("Dashboard_Backend"), sharedStrings, dateStyleIndexes));
    const rawRows = [...rawOld, ...rawNew];
    const scoredRows = scorePeriodGroups(rawRows);

    const pointBuckets = new Map();
    scoredRows.forEach((row) => {
      if (row.periodScore === null) return;
      const bucketKey = [row.date, canon(row.category), canon(row.fundName)].join("|");
      if (!pointBuckets.has(bucketKey)) pointBuckets.set(bucketKey, []);
      pointBuckets.get(bucketKey).push(row);
    });

    const points = [];
    pointBuckets.forEach((rows) => {
      const preferred = [...rows].sort((a, b) => (a.model === "new" ? 1 : 0) - (b.model === "new" ? 1 : 0)).at(-1);
      const clone = { ...preferred };
      clone.dashboardScore = round(mean(rows.map((item) => item.periodScore)), 2);
      clone.periodFlags = rows.map((item) => item.periodFlag).filter(Boolean);
      clone.volatility = [...rows].reverse().find((item) => item.volatility !== null && item.volatility !== undefined)?.volatility ?? clone.volatility ?? null;
      clone.parameterBreakdown = clone.parameters || [];
      points.push(clone);
    });

    const fundOrder = new Map();
    let orderIndex = 0;
    rawRows.forEach((row) => {
      const fundKey = `${canon(row.category)}|${canon(row.fundName)}`;
      if (!fundOrder.has(fundKey)) {
        fundOrder.set(fundKey, orderIndex);
        orderIndex += 1;
      }
    });

    const fundBuckets = new Map();
    points.forEach((point) => {
      const fundKey = `${canon(point.category)}|${canon(point.fundName)}`;
      if (!fundBuckets.has(fundKey)) fundBuckets.set(fundKey, []);
      fundBuckets.get(fundKey).push(point);
    });

    const analysisBuckets = new Map();
    scoredRows.forEach((row) => {
      if (row.periodScore === null) return;
      const fundKey = `${canon(row.category)}|${canon(row.fundName)}`;
      if (!analysisBuckets.has(fundKey)) analysisBuckets.set(fundKey, { old: [], new: [] });
      analysisBuckets.get(fundKey)[row.model].push({
        date: row.date,
        score: row.periodScore,
        rank: row.periodRank,
        oneYear: row.oneYear,
        threeYear: row.threeYear,
        fiveYear: row.fiveYear,
        sharpe: row.sharpe,
        pe: row.pe ?? null,
        pb: row.pb ?? null,
        sortino: row.sortino ?? null,
        volatility: row.volatility,
        parameters: row.parameters || []
      });
    });

    const funds = [];
    fundBuckets.forEach((history, fundKey) => {
      const sortedHistory = [...history].sort((a, b) => String(a.date).localeCompare(String(b.date)));
      const latest = sortedHistory[sortedHistory.length - 1];
      const numericScores = sortedHistory.map((item) => item.dashboardScore).filter((value) => value !== null && value !== undefined);
      const trendDelta = (numericScores.at(-1) ?? 0) - (numericScores[0] ?? 0);
      const latestReturns = [latest.oneYear, latest.threeYear, latest.fiveYear].filter((value) => value !== null && value !== undefined);
      funds.push({
        id: `fund-${keyOf(latest.category)}-${keyOf(latest.displayName)}`,
        category: latest.category,
        fundName: latest.displayName,
        rawFundName: latest.fundName,
        fundOrder: fundOrder.get(fundKey) ?? 999999,
        latestDate: latest.date,
        oneYear: latest.oneYear ?? null,
        threeYear: latest.threeYear ?? null,
        fiveYear: latest.fiveYear ?? null,
        sharpe: latest.sharpe ?? [...sortedHistory].reverse().find((item) => item.sharpe !== null && item.sharpe !== undefined)?.sharpe ?? null,
        pe: latest.pe ?? [...sortedHistory].reverse().find((item) => item.pe !== null && item.pe !== undefined)?.pe ?? null,
        pb: latest.pb ?? [...sortedHistory].reverse().find((item) => item.pb !== null && item.pb !== undefined)?.pb ?? null,
        sortino: latest.sortino ?? [...sortedHistory].reverse().find((item) => item.sortino !== null && item.sortino !== undefined)?.sortino ?? null,
        volatility: latest.volatility ?? [...sortedHistory].reverse().find((item) => item.volatility !== null && item.volatility !== undefined)?.volatility ?? null,
        averageReturn: latestReturns.length ? mean(latestReturns) : null,
        dashboardScore: numericScores.length ? Math.round(mean(numericScores)) : 0,
        consistency: numericScores.length > 1 ? sampleStdev(numericScores) : 0,
        trend: Math.abs(trendDelta) <= 2 ? "Stable" : (trendDelta > 0 ? "Improving" : "Declining"),
        trendDelta: round(trendDelta, 1),
        periodFlags: latest.periodFlags || [],
        parameterBreakdown: latest.parameterBreakdown || [],
        analysisHistory: {
          old: [...(analysisBuckets.get(fundKey)?.old || [])].sort((a, b) => String(a.date).localeCompare(String(b.date))),
          new: [...(analysisBuckets.get(fundKey)?.new || [])].sort((a, b) => String(a.date).localeCompare(String(b.date)))
        },
        history: sortedHistory.map((item) => ({
          date: item.date,
          score: item.dashboardScore,
          oneYear: item.oneYear ?? null,
          threeYear: item.threeYear ?? null,
          fiveYear: item.fiveYear ?? null,
          sharpe: item.sharpe ?? null,
          pe: item.pe ?? null,
          pb: item.pb ?? null,
          sortino: item.sortino ?? null,
          volatility: item.volatility ?? null
        }))
      });
    });

    const categoryGroups = new Map();
    funds.forEach((fund) => {
      if (!categoryGroups.has(fund.category)) categoryGroups.set(fund.category, []);
      categoryGroups.get(fund.category).push(fund);
    });

    categoryGroups.forEach((group) => {
      const ordered = [...group].sort((a, b) => (b.dashboardScore - a.dashboardScore) || (a.fundOrder - b.fundOrder));
      const scoreCounts = new Map();
      const scores = ordered.map((fund) => fund.dashboardScore);
      ordered.forEach((fund) => {
        const seen = scoreCounts.get(fund.dashboardScore) || 0;
        scoreCounts.set(fund.dashboardScore, seen + 1);
        fund.rank = scores.reduce((count, score) => count + (score > fund.dashboardScore ? 1 : 0), 1) + seen;
        fund.flag = fund.rank <= 5 || (fund.periodFlags || []).length ? "Top Performers" : "Core";
      });
    });

    funds.forEach((fund) => {
      const backendRow = dashboardBackend.rows.get(`${canon(fund.category)}|${canon(fund.fundName)}`);
      if (!backendRow) return;
      if (backendRow.dashboardScore !== null) fund.dashboardScore = Math.round(backendRow.dashboardScore);
      if (backendRow.consistency !== null) fund.consistency = backendRow.consistency;
      if (backendRow.rank !== null) fund.rank = backendRow.rank;
      fund.flag = backendRow.flag || fund.flag || "Core";
      fund.trend = backendRow.trend || fund.trend || "Stable";
    });

    const summaries = [...categoryGroups.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([category, group]) => {
      const ordered = [...group].sort((a, b) => (a.rank || 999) - (b.rank || 999));
      const averageReturns = group.map((fund) => fund.averageReturn).filter((value) => value !== null && value !== undefined);
      const consistencyValues = group.map((fund) => fund.consistency).filter((value) => value !== null && value !== undefined);
      return {
        category,
        totalFunds: group.length,
        categoryAverageReturn: averageReturns.length ? round(mean(averageReturns) * 100, 2) : 0,
        categoryAverageScore: round(mean(group.map((fund) => fund.dashboardScore)), 1),
        consistencyScore: consistencyValues.length ? round(mean(consistencyValues), 2) : 0,
        topPerformer: ordered[0]?.fundName || "",
        topScore: ordered[0]?.dashboardScore || 0,
        topTrend: ordered[0]?.trend || "Stable",
        latestDate: [...group].map((fund) => fund.latestDate).filter(Boolean).sort().at(-1) || ""
      };
    });

    const sortedFunds = [...funds].sort((a, b) => a.category.localeCompare(b.category) || (a.rank || 999) - (b.rank || 999) || a.fundOrder - b.fundOrder);
    return {
      generatedFrom: sourceLabel,
      analysis: "excel-dashboard",
      latestDate: sortedFunds.map((fund) => fund.latestDate).filter(Boolean).sort().at(-1) || "",
      categories: [...categoryGroups.keys()].sort(),
      summaries,
      funds: sortedFunds,
      scoring: {
        description: "Dashboard score uses the workbook's weighted ranking method, then averages available period scores for each fund.",
        weights: [
          { label: "1Y", weight: 0.10, direction: "higher is better" },
          { label: "3Y", weight: 0.20, direction: "higher is better" },
          { label: "5Y", weight: 0.25, direction: "higher is better" },
          { label: "Sharpe", weight: 0.15, direction: "higher is better" },
          { label: "P/E", weight: 0.10, direction: "lower is better" },
          { label: "P/B", weight: 0.10, direction: "lower is better" },
          { label: "Sortino", weight: 0.10, direction: "higher is better" }
        ]
      }
    };
  };

  const buildDataFromWorkbookBuffer = async (buffer, sourceLabel = "uploaded-workbook.xlsx") => {
    if (!window.JSZip) throw new Error("Workbook import library is unavailable");
    const zip = await window.JSZip.loadAsync(buffer);
    return buildData(zip, sourceLabel);
  };

  const buildDataFromWorkbookFile = async (file) => {
    const buffer = await file.arrayBuffer();
    return buildDataFromWorkbookBuffer(buffer, file.name || "uploaded-workbook.xlsx");
  };

  window.WorkbookImporter = {
    buildDataFromWorkbookBuffer,
    buildDataFromWorkbookFile
  };
}());

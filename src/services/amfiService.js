import axios from "axios";
import { env } from "../config/env.js";
import { parseNavAllText } from "../utils/parser.js";
import { withRetry } from "../utils/retry.js";

const http = axios.create({
  timeout: env.requestTimeoutMs,
  responseType: "text"
});

export async function fetchAmfiNavFeed() {
  return withRetry(async () => {
    const response = await http.get(env.amfiUrl, {
      headers: {
        "User-Agent": "Funalytics-Live-Backend/1.0"
      }
    });
    const rawText = String(response.data || "");
    const totalRows = rawText
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .filter((line) => /^\d+;/.test(line))
      .length;
    const records = parseNavAllText(rawText);
    return {
      records,
      fetched: totalRows,
      matched: records.length,
      failed: Math.max(0, totalRows - records.length)
    };
  }, { retries: 3, baseDelayMs: 750 });
}

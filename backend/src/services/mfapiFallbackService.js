import axios from "axios";
import { env } from "../config/env.js";

const http = axios.create({
  timeout: env.requestTimeoutMs
});

function parseMfapiDate(raw) {
  const [day, month, year] = String(raw || "").split("-");
  if (!day || !month || !year) return null;
  return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
}

async function fetchSchemeLatestNav(schemeCode) {
  const response = await http.get(`https://api.mfapi.in/mf/${schemeCode}`);
  const latest = Array.isArray(response.data?.data) ? response.data.data[0] : null;
  if (!latest) return null;
  const nav = Number(latest.nav);
  const navDate = parseMfapiDate(latest.date);
  if (!Number.isFinite(nav) || !navDate) return null;
  return {
    schemeCode: String(schemeCode),
    schemeName: String(response.data?.meta?.scheme_name || "").trim(),
    nav,
    navDate,
    source: "mfapi"
  };
}

export async function fetchFallbackNavs(schemeCodes = []) {
  const results = [];
  for (const schemeCode of schemeCodes) {
    try {
      const item = await fetchSchemeLatestNav(schemeCode);
      if (item) results.push(item);
    } catch {
      // continue
    }
  }
  return results;
}

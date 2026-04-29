window.LiveDataVersion = window.LiveDataVersion || {};

window.LiveDataVersion.apiClients = (() => {
  const RENDER_BACKEND_BASE = "https://funalytics-backend.onrender.com";

  const fetchWithTimeout = async (url, options = {}, timeoutMs = 10000) => {
    const response = await fetch(url, {
      ...options,
      cache: "no-store",
      signal: AbortSignal.timeout(timeoutMs)
    });
    return response;
  };

  const backendBase = () => {
    const configured = String(window.LIVE_CONFIG?.backendApiBase || "").trim().replace(/\/+$/, "");
    return configured || RENDER_BACKEND_BASE;
  };

  const fetchJson = async (url, options = {}, timeoutMs = 10000) => {
    const response = await fetchWithTimeout(url, options, timeoutMs);
    if (!response.ok) throw new Error(`HTTP ${response.status}: ${url}`);
    return response.json();
  };

  const parseDisplayDate = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (/^\d{2}-\d{2}-\d{4}$/.test(raw)) {
      const [day, month, year] = raw.split("-");
      return `${year}-${month}-${day}`;
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return "";
    return parsed.toISOString().slice(0, 10);
  };

  const fetchNavSnapshot = async () => fetchJson(`${backendBase()}/nav`);

  return {
    backendBase,
    fetchJson,
    fetchNavSnapshot,
    parseDisplayDate
  };
})();

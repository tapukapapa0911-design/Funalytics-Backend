const SHELL_CACHE = "funalytics-shell-v11";
const NAV_CACHE = "funalytics-nav-v2";
const NAV_SYNC_TAG = "funalytics-nav-sync";
const NAV_PERIODIC_SYNC_TAG = "funalytics-nav-daily";
const DEFAULT_NAV_URL = "https://funalytics-backend.onrender.com/nav";
const NAV_TIMEOUT_MS = 35000;

const APP_SHELL = [
  "./",
  "./index.html",
  "./src/styles.css",
  "./src/app.js",
  "./src/bootstrap.js",
  "./constants/schema.js",
  "./utils/cache.js",
  "./utils/validation.js",
  "./services/calculations.js",
  "./services/apiClients.js",
  "./services/matcher.js",
  "./services/dataMapper.js",
  "./services/dataProvider.js",
  "./mockData/live-nav-snapshot.js",
  "./mockData/excel-backup.json",
  "./manifest.json?v=live-1",
  "./manifest.webmanifest",
  "./icons/light-logo.png",
  "./icons/dark-logo.png"
];

const withTimeout = (input, init = {}, timeoutMs = NAV_TIMEOUT_MS) => {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(input, {
    ...init,
    signal: controller.signal
  }).finally(() => clearTimeout(timeoutId));
};

const isNavRequest = (request) => {
  try {
    const url = new URL(request.url);
    return request.method === "GET" && url.pathname === "/nav";
  } catch {
    return false;
  }
};

const resolveNavUrl = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return DEFAULT_NAV_URL;
  return raw.endsWith("/nav") ? raw : `${raw.replace(/\/+$/, "")}/nav`;
};

const refreshNavCache = async (navUrl = DEFAULT_NAV_URL) => {
  const cache = await caches.open(NAV_CACHE);
  const response = await withTimeout(navUrl, { cache: "no-store" });
  if (response && response.ok) {
    await cache.put(navUrl, response.clone());
  }
  return response;
};

self.addEventListener("install", (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(SHELL_CACHE).then((cache) => cache.addAll(APP_SHELL))
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter((key) => ![SHELL_CACHE, NAV_CACHE].includes(key)).map((key) => caches.delete(key)));
    await self.clients.claim();
  })());
});

self.addEventListener("message", (event) => {
  const data = event.data || {};
  if (data.type === "SKIP_WAITING") {
    self.skipWaiting();
    return;
  }
  if (data.type === "CLEAR_NAV_CACHE") {
    event.waitUntil?.(caches.delete(NAV_CACHE));
    return;
  }
  if (data.type === "TRIGGER_NAV_SYNC") {
    const navUrl = resolveNavUrl(data.navUrl);
    event.waitUntil?.(refreshNavCache(navUrl).catch(() => null));
    return;
  }
});

self.addEventListener("sync", (event) => {
  if (event.tag === NAV_SYNC_TAG) {
    event.waitUntil(refreshNavCache().catch(() => null));
  }
});

self.addEventListener("periodicsync", (event) => {
  if (event.tag === NAV_PERIODIC_SYNC_TAG) {
    event.waitUntil(refreshNavCache().catch(() => null));
  }
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  const isSameOrigin = url.origin === self.location.origin;
  const isNavigation = request.mode === "navigate" || request.destination === "document";

  if (isNavRequest(request)) {
    event.respondWith((async () => {
      const cache = await caches.open(NAV_CACHE);
      const cached = await cache.match(request.url);
      const networkPromise = withTimeout(request, { cache: "no-store" })
        .then(async (response) => {
          if (response && response.ok) {
            await cache.put(request.url, response.clone());
          }
          return response;
        })
        .catch(() => null);

      if (cached) {
        event.waitUntil(networkPromise);
        return cached;
      }

      const fresh = await networkPromise;
      if (fresh) return fresh;
      return Response.error();
    })());
    return;
  }

  if (isNavigation) {
    event.respondWith(
      fetch(request)
        .then((response) => {
          const clone = response.clone();
          caches.open(SHELL_CACHE).then((cache) => cache.put("./index.html", clone));
          return response;
        })
        .catch(async () => (await caches.match(request)) || (await caches.match("./index.html")))
    );
    return;
  }

  if (!isSameOrigin) return;

  event.respondWith(
    caches.match(request).then((cached) => {
      if (cached) return cached;
      return fetch(request).then((response) => {
        if (response && response.ok) {
          const clone = response.clone();
          caches.open(SHELL_CACHE).then((cache) => cache.put(request, clone));
        }
        return response;
      });
    })
  );
});

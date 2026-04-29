# live-data-version

This folder is a separate replica of the Funalytics app that keeps the original app untouched.

## What changed

- UI shell is cloned from the current app.
- Data bootstraps from `mockData/excel-backup.json`.
- Live refresh attempts use the AMFI NAV feed only.
- Live data is mapped back into the same normalized `excel-dashboard`-style shape the UI already expects.

## Architecture

- `constants/schema.js`: workbook-derived scoring weights, cache keys, source definitions
- `utils/cache.js`: local cache helpers
- `utils/validation.js`: data-shape safety
- `services/apiClients.js`: AMFI fetch clients
- `services/matcher.js`: scheme matching helpers for resilient NAV mapping
- `services/navResolver.js`: AMFI NAV resolution and caching
- `services/calculations.js`: workbook-equivalent scoring and return helpers
- `services/dataMapper.js`: live data -> workbook schema mapping
- `services/dataProvider.js`: boot, refresh, cache, fallback orchestration
- `mockData/excel-backup.json`: fallback dataset converted from the workbook export
- `src/bootstrap.js`: loads cached/backup data first, then refreshes live in the background
- `backend/`: standalone Node.js + MongoDB NAV ingestion API for production use

## Notes

- The live version keeps the original app contract intact.
- If live fetch fails or a field is unavailable from verified sources, the app falls back to the cached/backup workbook-converted dataset.
- This keeps the UI stable while gradually replacing workbook-derived values with live values.

## Backend API Option

This folder now includes a backend service at `backend/` that can:

- ingest AMFI NAV daily
- store all schemes in MongoDB
- expose REST APIs for the live app

To make the frontend prefer the local backend, add before scripts in `index.html`:

```html
<script>
  window.LIVE_CONFIG = {
    backendApiBase: "https://your-render-service.onrender.com"
  };
</script>
```

If `backendApiBase` is not configured or the backend is unavailable, the app falls back to the existing client-side live NAV resolver automatically.

You can also set it without editing files:

```js
localStorage.setItem("fundpulse-live-backend-api-base", "https://your-render-service.onrender.com");
location.reload();
```

## Live Data Sources

| Metric | Source | Frequency | Notes |
|---|---|---|---|
| Latest NAV | AMFI NAVAll.txt | Daily | Official, authoritative |
| 1Y / 3Y / 5Y Returns | Excel backup | Static | Workbook-derived fallback values |
| Sharpe Ratio | Excel backup | Static | Workbook-derived fallback values |
| Sortino Ratio | Excel backup | Static | Workbook-derived fallback values |
| Volatility | Excel backup | Static | Workbook-derived fallback values |
| PE Ratio | Excel backup (quarterly manual update) | Static | No free public API for portfolio P/E |
| PB Ratio | Excel backup (quarterly manual update) | Static | No free public API for portfolio P/B |

Note: PE and PB remain workbook-backed portfolio metrics until a separate verified source is introduced.

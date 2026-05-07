# Live Data Version Backend

This backend lives entirely inside `live-data-version/` and powers the live NAV layer for the Funalytics live app.

## Features

- Daily AMFI NAV ingestion
- Daily snapshot regeneration for the frontend bundle
- MongoDB bulk upserts keyed by `schemeCode`
- Retry + logging
- REST API for the live frontend
- Daily cron starts at `01:00 AM IST`
- Retries at `01:30 AM`, `02:00 AM`, and `02:30 AM` if needed

## Setup

1. Copy `.env.example` to `.env`
2. Set `MONGODB_URI`
3. Install dependencies:
   - `npm install`
4. Start:
   - `npm run dev`

## Endpoints

- `GET /health`
- `GET /funds`
- `GET /api/snapshot`
- `GET /api/cron`
- `GET /nav-summary`
- `GET /fund/:schemeCode`
- `GET /search?q=keyword`
- `GET /meta/last-updated`

## Frontend hook

To make `live-data-version/index.html` use this backend first, add before app scripts:

```html
<script>
  window.LIVE_CONFIG = {
    backendApiBase: "http://localhost:4000"
  };
</script>
```

## Production deployment

Deployment files included:

- [render.yaml](C:\Users\ameen\Documents\Codex\2026-04-17-files-mentioned-by-the-user-mutual\mutual-fund-dashboard-app\live-data-version\backend\render.yaml)
- [.nvmrc](C:\Users\ameen\Documents\Codex\2026-04-17-files-mentioned-by-the-user-mutual\mutual-fund-dashboard-app\live-data-version\backend\.nvmrc)
- [RENDER_DEPLOY.md](C:\Users\ameen\Documents\Codex\2026-04-17-files-mentioned-by-the-user-mutual\mutual-fund-dashboard-app\live-data-version\backend\RENDER_DEPLOY.md)

The app already:

- respects `PORT`
- runs an initial NAV update on startup
- uses environment-driven MongoDB configuration

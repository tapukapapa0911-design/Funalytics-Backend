# Render Deployment Guide

This backend is already production-runnable. The files added here only make deployment and uptime easier.

## What To Deploy

Deploy this folder:

- `C:\Users\ameen\Documents\Codex\2026-04-17-files-mentioned-by-the-user-mutual\mutual-fund-dashboard-app\live-data-version\backend`

## Render Setup

1. Push the repo to GitHub.
2. In Render, create a **Web Service**.
3. Point it at the repo.
4. Set the root directory to:
   - `live-data-version/backend`
5. Render can also read:
   - [render.yaml](C:\Users\ameen\Documents\Codex\2026-04-17-files-mentioned-by-the-user-mutual\mutual-fund-dashboard-app\live-data-version\backend\render.yaml)

## Required Environment Variables

Set these in Render:

- `MONGODB_URI`
- `NODE_ENV=production`
- `TZ=Asia/Kolkata`
- `AMFI_URL=https://www.amfiindia.com/spages/NAVAll.txt`
- `REQUEST_TIMEOUT_MS=15000`
- `CACHE_TTL_MS=60000`

`PORT` is injected by Render automatically, but the app already supports it.

## Start / Health

- Build command: `npm install`
- Start command: `npm start`
- Health check path: `/health`

## Cron Behavior

Cron is started automatically when the server boots:

- Daily NAV update: `10:30 PM IST`

No separate cron service is required for the current design.

## Validation After Deploy

Check:

- `GET /health`
- `GET /funds`
- `GET /meta/last-updated`

You should also see startup logs showing:

- MongoDB connected
- server listening
- cron scheduled
- initial NAV update attempted

## Frontend URL

After Render gives you a URL like:

- `https://funalytics-live-nav-backend.onrender.com`

use that URL as `backendApiBase` in both live theme apps.

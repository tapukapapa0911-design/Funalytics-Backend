Render + cron-job.org setup

1. Deploy this backend as a Render Web Service.
2. Start command:
   `npm start`
3. Render health check path:
   `/health`
4. Daily cron-job.org target:
   `https://YOUR-RENDER-URL/update-nav`

Recommended cron-job.org schedule (Asia/Kolkata):
- 01:00
- 01:30
- 02:00
- 02:30

Do not keep older 11 PM / 11:30 PM jobs enabled once these 1 AM onward jobs are active.

Useful checks:
- `/health` -> basic uptime check
- `/nav` -> latest app snapshot used by the frontend
- `/meta/last-updated` -> last successful cache timestamp and latest NAV date

Notes:
- Render free tier sleeps. That is okay because cron-job.org will wake it up.
- The frontend can point to this backend with:
  `localStorage.setItem("fundpulse-live-backend-api-base", "https://YOUR-RENDER-URL")`
- The backend writes the latest snapshot into:
  `data/live-nav-snapshot.json`

Render + cron-job.org setup

1. Deploy this backend as a Render Web Service.
2. Start command:
   `npm start`
3. Render health check path:
   `/health`
4. Daily cron-job.org target:
   `https://YOUR-RENDER-URL/update-nav`

Recommended cron-job.org schedule (Asia/Kolkata):
- 00:00
- 00:15
- 00:30
- 06:00
- every 15 minutes from 06:15 to 23:45 only if you want aggressive retry coverage

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

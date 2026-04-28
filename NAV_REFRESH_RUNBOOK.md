# NAV Refresh Runbook

## Source of truth

- AMFI only: [NAVAll.txt](https://www.amfiindia.com/spages/NAVAll.txt)
- No third-party NAV sources are used by the active web app path.

## Schedule

- `00:00 IST` primary run
- `00:15 IST` retry 1 if the midnight run failed
- `00:30 IST` retry 2 if the prior run still failed
- `06:00 IST` retry 3 if the overnight runs all failed
- `06:15 IST` onward every 15 minutes until `23:45 IST` if the `06:00 IST` run also failed

Retries stop as soon as one run succeeds or AMFI returns "no new NAV available".

## Snapshot flow

1. Backend ingests AMFI data into MongoDB.
2. Backend rewrites `mockData/live-nav-snapshot.js`.
3. Frontend fetches `/api/snapshot` on app load.
4. UI updates the existing `Data as of ...` label from the live NAV date in that snapshot.

## Failure behavior

- Network / HTTP / parsing failure:
  - keep last known good cached NAV data
  - UI shows `Data as of <date> (last available)`
- AMFI returns the same or older NAV date:
  - keep existing cache
  - log `No new NAV available`
  - do not show an error to the user

## Quick integration test

1. Start backend and verify `/api/snapshot` returns `latestDate`.
2. Open the web app and confirm the header date matches `/api/snapshot.latestDate`.
3. Trigger `GET /api/cron` manually and confirm:
   - snapshot regenerates
   - `latestDate` stays the same on holidays/no-update days
   - `latestDate` advances when AMFI publishes a newer date
4. Simulate a backend outage and confirm the frontend keeps showing the last good date with `(last available)`.

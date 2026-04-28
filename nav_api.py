import json
import logging
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, "funds.sqlite3")
AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
HOST = "127.0.0.1"
PORT = 4000
IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(message)s")
STATE_LOCK = threading.Lock()
SCHEDULER_STATE = {
    "lastResult": "idle",
    "lastError": None,
    "lastAttemptAt": None,
    "lastSuccessAt": None,
    "lastNavDate": None,
    "retryCount": 0,
    "nextAttemptAt": None,
}


def now_ist():
    return datetime.now(IST)


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


CONN = get_conn()


def init_db():
    with CONN:
        CONN.execute(
            """
            CREATE TABLE IF NOT EXISTS funds (
                schemeCode TEXT PRIMARY KEY,
                schemeName TEXT NOT NULL,
                nav REAL NOT NULL,
                isin TEXT DEFAULT '',
                navDate TEXT NOT NULL,
                lastUpdated TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'amfi',
                createdAt TEXT NOT NULL,
                updatedAt TEXT NOT NULL
            )
            """
        )
        CONN.execute("CREATE INDEX IF NOT EXISTS idx_funds_scheme_name ON funds(schemeName)")


def fetch_text(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "Funalytics-Live-NAV/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def parse_amfi_date(raw):
    raw = (raw or "").strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def parse_nav_lines(raw_text):
    deduped = {}
    for line in (raw_text or "").splitlines():
        line = line.strip()
        if not line or not line[:1].isdigit():
            continue
        parts = line.split(";")
        if len(parts) < 6:
            continue
        scheme_code, isin_growth, isin_reinvestment, scheme_name, nav_raw, date_raw = parts[:6]
        try:
            nav = float(nav_raw)
        except ValueError:
            continue
        scheme_code = scheme_code.strip()
        scheme_name = " ".join((scheme_name or "").strip().split())
        if not scheme_code or not scheme_name:
            continue
        deduped[scheme_code] = {
            "schemeCode": scheme_code,
            "schemeName": scheme_name,
            "nav": nav,
            "isin": (isin_growth or isin_reinvestment or "").strip(),
            "navDate": parse_amfi_date(date_raw),
            "source": "amfi",
        }
    return list(deduped.values())


def latest_cached_nav_date():
    row = CONN.execute("SELECT navDate FROM funds ORDER BY navDate DESC, lastUpdated DESC LIMIT 1").fetchone()
    return row["navDate"] if row else None


def latest_date_from_records(records):
    dates = sorted({item.get("navDate") for item in records if item.get("navDate")})
    return dates[-1] if dates else None


def set_scheduler_state(**kwargs):
    with STATE_LOCK:
        SCHEDULER_STATE.update(kwargs)


def scheduler_snapshot():
    with STATE_LOCK:
        return dict(SCHEDULER_STATE)


def next_midnight_after(moment):
    target = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    if target <= moment:
        target += timedelta(days=1)
    return target


def next_retry_after_failure(attempt_time, retry_count):
    day_start = attempt_time.replace(hour=0, minute=0, second=0, microsecond=0)
    six_am = day_start.replace(hour=6)
    end_of_day = day_start.replace(hour=23, minute=59)

    if retry_count <= 2:
      candidate = attempt_time + timedelta(minutes=15)
      return candidate if candidate <= end_of_day else next_midnight_after(attempt_time)

    if retry_count == 3 and attempt_time < six_am:
        return six_am

    candidate = attempt_time + timedelta(minutes=15)
    return candidate if candidate <= end_of_day else next_midnight_after(attempt_time)


def schedule_next_attempt(result, attempt_time):
    status = result.get("status")
    retry_count = result.get("retryCount", 0)
    if status in {"updated", "no-new"}:
        return next_midnight_after(attempt_time)
    return next_retry_after_failure(attempt_time, retry_count)


def upsert_funds(records):
    now = datetime.now(IST).isoformat()
    rows = [
        (
            item["schemeCode"],
            item["schemeName"],
            item["nav"],
            item.get("isin", ""),
            item["navDate"],
            now,
            item.get("source", "amfi"),
            now,
            now,
        )
        for item in records
    ]
    with CONN:
        CONN.executemany(
            """
            INSERT INTO funds (
                schemeCode, schemeName, nav, isin, navDate, lastUpdated, source, createdAt, updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(schemeCode) DO UPDATE SET
                schemeName=excluded.schemeName,
                nav=excluded.nav,
                isin=excluded.isin,
                navDate=excluded.navDate,
                lastUpdated=excluded.lastUpdated,
                source=excluded.source,
                updatedAt=excluded.updatedAt
            """,
            rows,
        )


def ingest_amfi():
    attempt_time = now_ist()
    logging.info("Starting AMFI NAV ingestion")
    set_scheduler_state(lastAttemptAt=attempt_time.isoformat())
    last_error = None
    cached_latest = latest_cached_nav_date()
    for attempt in range(3):
        try:
            raw = fetch_text(AMFI_URL)
            records = parse_nav_lines(raw)
            if not records:
                raise RuntimeError("No valid AMFI NAV rows parsed")
            latest_incoming = latest_date_from_records(records)
            if not latest_incoming:
                raise RuntimeError("No valid AMFI NAV date parsed")
            if cached_latest and latest_incoming <= cached_latest:
                logging.info("No new NAV available. Cached=%s Incoming=%s", cached_latest, latest_incoming)
                result = {
                    "status": "no-new",
                    "latestNavDate": cached_latest,
                    "rowCount": len(records),
                    "retryCount": 0,
                }
                next_attempt = schedule_next_attempt(result, attempt_time)
                set_scheduler_state(
                    lastResult="no-new",
                    lastError=None,
                    lastNavDate=cached_latest,
                    retryCount=0,
                    nextAttemptAt=next_attempt.isoformat(),
                )
                return result

            upsert_funds(records)
            logging.info("AMFI NAV ingestion complete: %s rows, latestDate=%s", len(records), latest_incoming)
            result = {
                "status": "updated",
                "latestNavDate": latest_incoming,
                "rowCount": len(records),
                "retryCount": 0,
            }
            next_attempt = schedule_next_attempt(result, attempt_time)
            set_scheduler_state(
                lastResult="updated",
                lastError=None,
                lastSuccessAt=now_ist().isoformat(),
                lastNavDate=latest_incoming,
                retryCount=0,
                nextAttemptAt=next_attempt.isoformat(),
            )
            return result
        except Exception as exc:
            last_error = exc
            logging.warning("AMFI fetch failed on attempt %s: %s", attempt + 1, exc)
            time.sleep(0.75 * (2 ** attempt))
    cached_latest = latest_cached_nav_date()
    retry_count = scheduler_snapshot().get("retryCount", 0) + 1
    next_attempt = schedule_next_attempt({"status": "failed", "retryCount": retry_count}, attempt_time)
    logging.error("AMFI ingestion failed after retries: %s", last_error)
    set_scheduler_state(
        lastResult="failed",
        lastError=str(last_error),
        lastNavDate=cached_latest,
        retryCount=retry_count,
        nextAttemptAt=next_attempt.isoformat(),
    )
    return {
        "status": "failed",
        "latestNavDate": cached_latest,
        "rowCount": 0,
        "retryCount": retry_count,
        "error": str(last_error),
    }


def build_snapshot_payload():
    rows = CONN.execute(
        "SELECT schemeCode, schemeName, nav, isin, navDate, lastUpdated, source FROM funds ORDER BY schemeName ASC"
    ).fetchall()
    items = [
        {
            "targetId": "",
            "schemeCode": row["schemeCode"],
            "schemeName": row["schemeName"],
            "isinGrowth": row["isin"],
            "nav": row["nav"],
            "date": row["navDate"],
            "source": row["source"],
        }
        for row in rows
    ]
    latest_date = items[-1]["date"] if items else None
    if items:
        latest_date = max(item["date"] for item in items if item.get("date"))
    return {
        "generatedAt": now_ist().isoformat(),
        "latestDate": latest_date,
        "count": len(items),
        "items": items,
    }


def scheduler_loop():
    if not scheduler_snapshot().get("nextAttemptAt"):
        set_scheduler_state(nextAttemptAt=next_midnight_after(now_ist()).isoformat())
    while True:
        now = now_ist()
        state = scheduler_snapshot()
        next_attempt_raw = state.get("nextAttemptAt")
        next_attempt = datetime.fromisoformat(next_attempt_raw) if next_attempt_raw else next_midnight_after(now)
        sleep_seconds = max(15, int((next_attempt - now).total_seconds()))
        time.sleep(min(sleep_seconds, 60))
        if now_ist() < next_attempt:
            continue
        try:
            ingest_amfi()
        except Exception as exc:
            logging.exception("Scheduled ingestion failed: %s", exc)
            retry_count = scheduler_snapshot().get("retryCount", 0) + 1
            next_retry = schedule_next_attempt({"status": "failed", "retryCount": retry_count}, now_ist())
            set_scheduler_state(
                lastResult="failed",
                lastError=str(exc),
                retryCount=retry_count,
                nextAttemptAt=next_retry.isoformat(),
            )


def serialize_row(row):
    return {
        "schemeCode": row["schemeCode"],
        "schemeName": row["schemeName"],
        "nav": row["nav"],
        "isin": row["isin"],
        "navDate": row["navDate"],
        "lastUpdated": row["lastUpdated"],
        "source": row["source"],
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._send(204, {})

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        try:
            if path == "/health":
                row = CONN.execute("SELECT lastUpdated, navDate FROM funds ORDER BY navDate DESC, lastUpdated DESC LIMIT 1").fetchone()
                return self._send(200, {
                    "ok": True,
                    "latestUpdate": row["lastUpdated"] if row else None,
                    "latestNavDate": row["navDate"] if row else None,
                    "scheduler": scheduler_snapshot(),
                })

            if path == "/meta/last-updated":
                row = CONN.execute("SELECT lastUpdated, navDate FROM funds ORDER BY navDate DESC, lastUpdated DESC LIMIT 1").fetchone()
                total = CONN.execute("SELECT COUNT(*) AS count FROM funds").fetchone()["count"]
                return self._send(200, {
                    "lastUpdated": row["lastUpdated"] if row else None,
                    "latestNavDate": row["navDate"] if row else None,
                    "totalFunds": total,
                    "scheduler": scheduler_snapshot(),
                })

            if path == "/api/snapshot":
                return self._send(200, build_snapshot_payload())

            if path == "/meta/scheduler":
                return self._send(200, scheduler_snapshot())

            if path == "/funds":
                page = max(int(query.get("page", ["1"])[0]), 1)
                limit = min(max(int(query.get("limit", ["5000"])[0]), 1), 5000)
                total = CONN.execute("SELECT COUNT(*) AS count FROM funds").fetchone()["count"]
                rows = CONN.execute(
                    "SELECT schemeCode, schemeName, nav, isin, navDate, lastUpdated, source FROM funds ORDER BY schemeName ASC LIMIT ? OFFSET ?",
                    (limit, (page - 1) * limit),
                ).fetchall()
                return self._send(200, {"page": page, "limit": limit, "total": total, "items": [serialize_row(row) for row in rows]})

            if path.startswith("/fund/"):
                scheme_code = path.split("/fund/", 1)[1].strip()
                row = CONN.execute(
                    "SELECT schemeCode, schemeName, nav, isin, navDate, lastUpdated, source FROM funds WHERE schemeCode = ?",
                    (scheme_code,),
                ).fetchone()
                if not row:
                    return self._send(404, {"error": "Fund not found"})
                return self._send(200, serialize_row(row))

            if path == "/search":
                keyword = " ".join(query.get("q", [""])).strip()
                if not keyword:
                    return self._send(200, {"items": []})
                pattern = f"%{keyword.lower()}%"
                rows = CONN.execute(
                    """
                    SELECT schemeCode, schemeName, nav, isin, navDate, lastUpdated, source
                    FROM funds
                    WHERE lower(schemeName) LIKE ?
                    ORDER BY schemeName ASC
                    LIMIT 50
                    """,
                    (pattern,),
                ).fetchall()
                return self._send(200, {"items": [serialize_row(row) for row in rows]})

            return self._send(404, {"error": "Not found"})
        except Exception as exc:
            logging.exception("Request failed: %s", exc)
            return self._send(500, {"error": "Internal server error"})


def main():
    init_db()
    ingest_amfi()
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    logging.info("Live NAV API listening on http://%s:%s", HOST, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()

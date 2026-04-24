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
    return datetime.now(IST).date().isoformat()


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
    logging.info("Starting AMFI NAV ingestion")
    last_error = None
    for attempt in range(3):
        try:
            raw = fetch_text(AMFI_URL)
            records = parse_nav_lines(raw)
            if not records:
                raise RuntimeError("No valid AMFI NAV rows parsed")
            upsert_funds(records)
            logging.info("AMFI NAV ingestion complete: %s rows", len(records))
            return True
        except Exception as exc:
            last_error = exc
            logging.warning("AMFI fetch failed on attempt %s: %s", attempt + 1, exc)
            time.sleep(0.75 * (2 ** attempt))
    logging.error("AMFI ingestion failed after retries: %s", last_error)
    return False


def maybe_daily_refresh_loop():
    while True:
        now = datetime.now(IST)
        target = now.replace(hour=22, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        sleep_seconds = max(30, int((target - now).total_seconds()))
        time.sleep(sleep_seconds)
        try:
            ingest_amfi()
        except Exception as exc:
            logging.exception("Scheduled ingestion failed: %s", exc)


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
                row = CONN.execute("SELECT lastUpdated FROM funds ORDER BY lastUpdated DESC LIMIT 1").fetchone()
                return self._send(200, {"ok": True, "latestUpdate": row["lastUpdated"] if row else None})

            if path == "/meta/last-updated":
                row = CONN.execute("SELECT lastUpdated FROM funds ORDER BY lastUpdated DESC LIMIT 1").fetchone()
                total = CONN.execute("SELECT COUNT(*) AS count FROM funds").fetchone()["count"]
                return self._send(200, {"lastUpdated": row["lastUpdated"] if row else None, "totalFunds": total})

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
    thread = threading.Thread(target=maybe_daily_refresh_loop, daemon=True)
    thread.start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    logging.info("Live NAV API listening on http://%s:%s", HOST, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()

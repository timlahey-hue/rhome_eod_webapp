# app/ingest.py
from __future__ import annotations

import os
import json
import time
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logger = logging.getLogger("ingest")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(levelname)s:ingest:%(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# Configuration (env-first)
# ------------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "eod.db")

SIMPRO_BASE_URL = os.getenv("SIMPRO_BASE_URL", "https://rhome.simprosuite.com")  # your tenant base
TOKEN_URL = os.getenv("SIMPRO_TOKEN_URL", f"{SIMPRO_BASE_URL.rstrip('/')}/oauth2/token")
API_BASE = os.getenv("SIMPRO_API_BASE", f"{SIMPRO_BASE_URL.rstrip('/')}/api/v1")
SIMPRO_CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID", "")
SIMPRO_CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET", "")
SIMPRO_COMPANY_ID = os.getenv("SIMPRO_COMPANY_ID", "")  # optional; many endpoints infer it

PAGE_SIZE = int(os.getenv("SIMPRO_PAGE_SIZE", "100"))
INGEST_BUDGET_SECONDS = int(os.getenv("INGEST_BUDGET_SECONDS", "25"))

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------
def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

def _ensure_tables(conn: sqlite3.Connection) -> None:
    # Run/log table we control (do NOT touch 'snapshot' view/table)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            ended_at   TEXT,
            ok         INTEGER,
            jobs_tried INTEGER DEFAULT 0,
            jobs_inserted INTEGER DEFAULT 0,
            note       TEXT
        )
        """
    )
    # A raw jobs table that won't collide with existing views
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs_raw (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            number TEXT,
            status TEXT,
            name TEXT,
            customer_name TEXT,
            start_date TEXT,
            due_date TEXT,
            total REAL,
            payload TEXT NOT NULL
        )
        """
    )
    conn.commit()

def _begin_run(conn: sqlite3.Connection, note: str = None) -> int:
    started_at = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO ingest_runs (started_at, note) VALUES (?, ?)",
        (started_at, note),
    )
    conn.commit()
    return int(cur.lastrowid)

def _finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    ok: bool,
    jobs_tried: int,
    jobs_inserted: int,
    extra_note: str = None,
) -> None:
    ended_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE ingest_runs SET ended_at=?, ok=?, jobs_tried=?, jobs_inserted=?, note=COALESCE(note,'') || ? WHERE id=?",
        (ended_at, 1 if ok else 0, jobs_tried, jobs_inserted, ("" if not extra_note else f" {extra_note}"), run_id),
    )
    conn.commit()

# ------------------------------------------------------------------------------
# Simpro API helpers
# ------------------------------------------------------------------------------
def _get_token() -> str:
    logger.info("[ingest] Authenticating with Simpro")
    if not SIMPRO_CLIENT_ID or not SIMPRO_CLIENT_SECRET:
        raise RuntimeError(
            "SIMPRO_CLIENT_ID and SIMPRO_CLIENT_SECRET must be set as environment variables."
        )

    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": SIMPRO_CLIENT_ID,
            "client_secret": SIMPRO_CLIENT_SECRET,
        },
        timeout=20,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Token request failed ({resp.status_code}): {resp.text}")
    token = resp.json().get("access_token", "")
    logger.info(f"[ingest] Token acquired (len={len(token)})")
    if not token:
        raise RuntimeError("No access_token in token response.")
    return token

def _api_get(path: str, token: str, params: Dict[str, Any] = None) -> requests.Response:
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    return requests.get(url, headers=headers, params=params or {}, timeout=20)

def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None

def _insert_job(conn: sqlite3.Connection, job: Dict[str, Any]) -> int:
    # Try to normalize common fields; keep full payload as text JSON.
    job_id = _coalesce(job.get("id"), job.get("jobId"), job.get("JobID"), job.get("ID"))
    if job_id is None:
        return 0

    number = _coalesce(job.get("number"), job.get("jobNumber"), job.get("Number"))
    status = _coalesce(job.get("status"), job.get("Status"))
    name = _coalesce(job.get("name"), job.get("jobName"), job.get("JobName"))
    customer_name = _coalesce(
        job.get("customerName"),
        (job.get("customer") or {}).get("name") if isinstance(job.get("customer"), dict) else None,
    )
    start_date = _coalesce(job.get("startDate"), job.get("StartDate"))
    due_date = _coalesce(job.get("dueDate"), job.get("DueDate"))
    total = _coalesce(job.get("total"), job.get("Total"))
    company_id = _coalesce(job.get("companyId"), job.get("CompanyID"), SIMPRO_COMPANY_ID or None)

    payload = json.dumps(job, separators=(",", ":"), ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO jobs_raw(id, company_id, number, status, name, customer_name, start_date, due_date, total, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            company_id=excluded.company_id,
            number=excluded.number,
            status=excluded.status,
            name=excluded.name,
            customer_name=excluded.customer_name,
            start_date=excluded.start_date,
            due_date=excluded.due_date,
            total=excluded.total,
            payload=excluded.payload
        """,
        (job_id, company_id, number, status, name, customer_name, start_date, due_date, total, payload),
    )
    return 1

# ------------------------------------------------------------------------------
# Public ingest functions
# ------------------------------------------------------------------------------
def ingest_live() -> Dict[str, Any]:
    """
    Fetches jobs from Simpro and stores them into SQLite (jobs_raw + ingest_runs).
    Avoids touching any 'snapshot' view/table entirely.
    """
    token = _get_token()

    t0 = time.monotonic()
    budget = INGEST_BUDGET_SECONDS

    with _connect() as conn:
        _ensure_tables(conn)
        run_id = _begin_run(conn, note="live")

        logger.info(f"[ingest] Starting live ingest (budget={budget}s, company_id={SIMPRO_COMPANY_ID or 0})")
        jobs_tried = 0
        jobs_inserted = 0
        ok = True
        note = ""

        try:
            # Simple paginated pull; adjust path/params here if your API differs.
            page = 1
            while True:
                if time.monotonic() - t0 > budget:
                    logger.warning("[ingest] time budget reached; stopping at page=%s", page)
                    note = "time_budget_reached"
                    break

                params = {"page": page, "perPage": PAGE_SIZE}
                if SIMPRO_COMPANY_ID:
                    params["companyId"] = SIMPRO_COMPANY_ID

                resp = _api_get("jobs", token, params=params)
                if resp.status_code != 200:
                    ok = False
                    note = f"api_status_{resp.status_code}"
                    logger.warning("[ingest] jobs page %s returned %s: %s", page, resp.status_code, resp.text)
                    break

                data = resp.json()
                items: List[Dict[str, Any]]

                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                    items = data["data"]
                else:
                    # Unexpected shape; treat as empty
                    items = []

                if not items:
                    # No more pages
                    break

                for job in items:
                    jobs_tried += 1
                    try:
                        jobs_inserted += _insert_job(conn, job)
                    except sqlite3.Error as db_e:
                        ok = False
                        logger.warning("[ingest] failed to insert job: %s", db_e)

                page += 1

        except requests.RequestException as e:
            ok = False
            note = f"http_error:{e.__class__.__name__}"
            logger.warning("[ingest] HTTP error: %s", e)
        except Exception as e:
            ok = False
            note = f"unexpected:{e.__class__.__name__}"
            logger.warning("[ingest] Unexpected error: %s", e)

        _finish_run(conn, run_id, ok, jobs_tried, jobs_inserted, extra_note=note)

    elapsed = time.monotonic() - t0
    logger.info(
        "[ingest] ingest_live finished in %.2fs (ok=%s, jobs=%d, tried=%d, run_id=%d)",
        elapsed, ok, jobs_inserted, jobs_tried, run_id
    )
    return {
        "ok": ok,
        "elapsed_sec": round(elapsed, 2),
        "jobs_inserted": jobs_inserted,
        "jobs_tried": jobs_tried,
        "run_id": run_id,
        "note": note,
    }

def ingest_demo() -> Dict[str, Any]:
    """
    Inserts a few example records locally (no API calls) to verify the pipeline.
    """
    demo_jobs = [
        {
            "id": 1001, "number": "J-1001", "name": "Boiler Replacement",
            "status": "In Progress", "customerName": "Acme Properties",
            "startDate": "2025-08-10", "dueDate": "2025-08-17", "total": 12500.00,
        },
        {
            "id": 1002, "number": "J-1002", "name": "HVAC Tune-up",
            "status": "Scheduled", "customerName": "Bluebird Cafe",
            "startDate": "2025-08-12", "dueDate": "2025-08-12", "total": 450.00,
        },
        {
            "id": 1003, "number": "J-1003", "name": "Emergency Callout",
            "status": "Complete", "customerName": "Sunrise Apartments",
            "startDate": "2025-08-08", "dueDate": "2025-08-08", "total": 320.00,
        },
    ]

    t0 = time.monotonic()
    with _connect() as conn:
        _ensure_tables(conn)
        run_id = _begin_run(conn, note="demo")
        jobs_tried = 0
        jobs_inserted = 0
        for job in demo_jobs:
            jobs_tried += 1
            jobs_inserted += _insert_job(conn, job)
        _finish_run(conn, run_id, ok=True, jobs_tried=jobs_tried, jobs_inserted=jobs_inserted, extra_note="demo_data")

    elapsed = time.monotonic() - t0
    logger.info(
        "[ingest] ingest_demo finished in %.2fs (ok=%s, jobs=%d, tried=%d, run_id=%d)",
        elapsed, True, jobs_inserted, jobs_tried, run_id
    )
    return {
        "ok": True,
        "elapsed_sec": round(elapsed, 2),
        "jobs_inserted": jobs_inserted,
        "jobs_tried": jobs_tried,
        "run_id": run_id,
        "note": "demo",
    }

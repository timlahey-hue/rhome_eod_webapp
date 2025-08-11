import os
import time
import json
import logging
from typing import Dict, Tuple, Any, List, Optional
from urllib.parse import urljoin

import sqlite3
import requests
from datetime import datetime, timezone

logger = logging.getLogger("ingest")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:ingest:%(message)s")

DB_PATH = os.getenv("DB_PATH", "eod.db")

# ---- Env helpers -------------------------------------------------------------

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def _normalized_base() -> Tuple[str, str]:
    """
    Returns (TENANT_BASE, API_BASE)
    - SIMPRO_TENANT_BASE is required, like https://yourtenant.simprosuite.com
    - SIMPRO_API_BASE is optional; if not supplied, default to TENANT_BASE + '/api/v1.0'
    """
    tenant_base = _require_env("SIMPRO_TENANT_BASE").rstrip("/")
    api_base = os.getenv("SIMPRO_API_BASE", tenant_base + "/api/v1.0").rstrip("/")
    return tenant_base, api_base

def _join(base: str, path: str) -> str:
    # urljoin is a bit odd with trailing slashes; ensure we keep our own semantics
    base = base.rstrip("/")
    path = path.lstrip("/")
    return f"{base}/{path}"


# ---- DB helpers --------------------------------------------------------------

def _conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _ensure_tables(conn)
    return conn

def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            ok INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            jobs_tried INTEGER NOT NULL DEFAULT 0,
            jobs_inserted INTEGER NOT NULL DEFAULT 0
        );
    """)
    # Very minimal jobs table; if you already have one, this won’t overwrite it.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id INTEGER PRIMARY KEY,
            number TEXT,
            name TEXT,
            status TEXT,
            scheduled_date TEXT,
            updated_at TEXT
        );
    """)
    conn.commit()


# ---- Simpro API --------------------------------------------------------------

def _get_token(tenant_base: str) -> Tuple[bool, str, Optional[str]]:
    """
    OAuth2 client_credentials token
    POST {tenant}/oauth2/token  (Basic auth with client_id/secret)
    """
    client_id = _require_env("SIMPRO_CLIENT_ID")
    client_secret = _require_env("SIMPRO_CLIENT_SECRET")

    token_url = _join(tenant_base, "/oauth2/token")
    try:
        logger.info("[ingest] Authenticating with Simpro")
        resp = requests.post(
            token_url,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=30,
        )
        if resp.status_code != 200:
            return False, f"oauth_status_{resp.status_code}", None
        token = resp.json().get("access_token")
        if not token:
            return False, "oauth_missing_token", None
        logger.info("[ingest] Token acquired (len=%s)", len(token))
        return True, "ok", token
    except Exception as e:
        return False, f"oauth_exc_{type(e).__name__}", None


def _api_get(api_base: str, token: str, path: str, params: Dict[str, Any]) -> requests.Response:
    url = _join(api_base, path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    return requests.get(url, headers=headers, params=params, timeout=30)


def _fetch_jobs_page(api_base: str, token: str, page: int = 1, page_size: int = 50) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """
    Try the Jobs listing with conservative defaults. Different tenants sometimes
    expose slightly different query param names. We try a couple of common ones
    before giving up so you don’t get a hard 404.
    """
    tried = []

    # Attempt 1: PageNumber / PageSize (most common)
    resp = _api_get(api_base, token, "/jobs", {"PageNumber": page, "PageSize": page_size})
    tried.append(("PageNumber/PageSize", resp.status_code))
    if resp.status_code == 200:
        try:
            data = resp.json()
            # Data may be list or an envelope with 'items' or 'data'
            if isinstance(data, list):
                return True, "ok", data
            if isinstance(data, dict):
                for key in ("items", "data", "results"):
                    if key in data and isinstance(data[key], list):
                        return True, "ok", data[key]
                # If dict but unknown shape, still return empty list
                return True, "ok", []
        except Exception:
            return False, "jobs_json_error", []

    # Attempt 2: page/size (fallback)
    resp2 = _api_get(api_base, token, "/jobs", {"page": page, "size": page_size})
    tried.append(("page/size", resp2.status_code))
    if resp2.status_code == 200:
        try:
            data = resp2.json()
            if isinstance(data, list):
                return True, "ok", data
            if isinstance(data, dict):
                for key in ("items", "data", "results"):
                    if key in data and isinstance(data[key], list):
                        return True, "ok", data[key]
                return True, "ok", []
        except Exception:
            return False, "jobs_json_error", []

    # If we got here, surface the most helpful detail (404 HTML pages happen when base path is wrong)
    if resp.status_code == 404 or resp2.status_code == 404:
        snippet = ""
        try:
            # In case the server returned an HTML 404 body, keep it short
            snippet = resp.text[:300] if resp.status_code == 404 else resp2.text[:300]
        except Exception:
            pass
        logger.warning("[ingest] jobs page %s returned 404: %s", page, ("\n\t" + snippet if snippet else ""))
        return False, "api_status_404", []

    return False, f"jobs_status_{resp2.status_code}", []


def _insert_jobs(conn: sqlite3.Connection, items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    inserted = 0
    for it in items:
        # Safely pick some common fields if they exist; ignore the rest.
        job_id = it.get("id") or it.get("jobId") or it.get("JobId")
        number = it.get("number") or it.get("jobNumber") or it.get("JobNumber")
        name = it.get("name") or it.get("jobName") or it.get("JobName")
        status = it.get("status") or it.get("statusName") or it.get("Status")
        scheduled = (
            it.get("scheduledDate")
            or it.get("startDate")
            or it.get("JobDate")
            or it.get("date")
        )
        updated = it.get("updatedAt") or it.get("lastModified") or datetime.now(timezone.utc).isoformat()

        # If we can’t find a numeric ID we’ll skip to avoid PK conflicts
        if job_id is None:
            continue

        conn.execute(
            """
            INSERT INTO jobs(job_id, number, name, status, scheduled_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                number=excluded.number,
                name=excluded.name,
                status=excluded.status,
                scheduled_date=excluded.scheduled_date,
                updated_at=excluded.updated_at
            """,
            (job_id, number, name, status, scheduled, updated),
        )
        inserted += 1
    conn.commit()
    return inserted


# ---- Public entrypoint -------------------------------------------------------

def ingest_live() -> Tuple[bool, str, int, int]:
    """
    Returns (ok, note, jobs_inserted, jobs_tried)

    Note strings:
      - 'ok'                     → success
      - 'api_status_404'         → base path likely wrong (set SIMPRO_API_BASE)
      - 'oauth_*'                → auth problem
      - 'jobs_status_*'          → non-200 listing response
      - 'jobs_json_error'        → invalid JSON shape
      - 'exc_*'                  → unexpected exception
    """
    start = time.time()
    tenant_base, api_base = _normalized_base()

    try:
        ok, note, token = _get_token(tenant_base)
        if not ok or not token:
            logger.info("[ingest] ingest_live finished in %.2fs (ok=%s, jobs=0, tried=0)", time.time() - start, ok)
            return False, note, 0, 0

        logger.info("[ingest] Starting live ingest (budget=25s)")
        with _conn() as conn:
            total_inserted = 0
            tried = 0

            # Page once for now (you can loop pages if needed)
            ok_jobs, note_jobs, items = _fetch_jobs_page(api_base, token, page=1, page_size=50)
            tried += 1
            if not ok_jobs:
                elapsed = time.time() - start
                logger.info("[ingest] ingest_live finished in %.2fs (ok=%s, jobs=%s, tried=%s)", elapsed, False, 0, tried)
                return False, note_jobs, 0, tried

            inserted = _insert_jobs(conn, items)
            total_inserted += inserted

        elapsed = time.time() - start
        logger.info("[ingest] ingest_live finished in %.2fs (ok=%s, jobs=%s, tried=%s)", elapsed, True, total_inserted, tried)
        return True, "ok", total_inserted, tried

    except Exception as e:
        elapsed = time.time() - start
        logger.exception("Unexpected error during ingest")
        return False, f"exc_{type(e).__name__}", 0, 0

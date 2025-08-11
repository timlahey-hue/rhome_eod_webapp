import os
import json
import time
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests

log = logging.getLogger("ingest")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

# ----- Config helpers ---------------------------------------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

def _db_conn() -> sqlite3.Connection:
    db_path = os.getenv("DB_PATH", "./eod.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn

def _ensure_tables(conn: sqlite3.Connection) -> None:
    # raw landing table (safe regardless of your existing UI schema)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simpro_jobs_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            payload TEXT NOT NULL,
            inserted_at TEXT NOT NULL
        )
    """)
    # optional minimal ingest log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            ok INTEGER NOT NULL,
            note TEXT
        )
    """)
    conn.commit()

# ----- Simpro API client ------------------------------------------------------

class SimproClient:
    def __init__(self):
        self.tenant_base = _env("SIMPRO_TENANT_BASE").rstrip("/")
        self.oauth_url = f"{self.tenant_base}/api/oauth/token"
        # Official API base is /api/v1.0
        self.api_base = f"{self.tenant_base}/api/v1.0"
        self.client_id = _env("SIMPRO_CLIENT_ID")
        self.client_secret = _env("SIMPRO_CLIENT_SECRET")
        self.username = _env("SIMPRO_USERNAME")
        self.password = _env("SIMPRO_PASSWORD")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def token(self) -> str:
        log.info("[ingest] Authenticating with Simpro")
        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            # scope is often optional, include if your tenant requires it:
            # "scope": "offline_access",
        }
        resp = requests.post(
            self.oauth_url,
            data=data,
            auth=(self.client_id, self.client_secret),
            timeout=20,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"OAuth failed ({resp.status_code}): {resp.text[:300]}")
        tok = resp.json().get("access_token")
        if not tok:
            raise RuntimeError("OAuth response missing access_token")
        self.session.headers["Authorization"] = f"Bearer {tok}"
        log.info("[ingest] Token acquired (len=%d)", len(tok))
        return tok

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, Any, str]:
        url = f"{self.api_base}{path}"
        r = self.session.get(url, params=params or {}, timeout=30)
        ctype = r.headers.get("content-type", "")
        body = r.text
        try:
            payload = r.json() if "application/json" in ctype else None
        except Exception:
            payload = None
        return r.status_code, payload, body

    # --- discovery helpers ---

    def list_companies(self) -> List[Dict[str, Any]]:
        # Try GET /companies
        code, js, body = self.get("/companies")
        if code == 200 and isinstance(js, list):
            return js
        # Some builds may expose company on /current-user
        code, js, body = self.get("/current-user")
        if code == 200 and isinstance(js, dict):
            # try common shapes
            if "companies" in js and isinstance(js["companies"], list):
                return js["companies"]
            if "companyId" in js:
                return [{"id": js["companyId"]}]
        raise RuntimeError(f"Could not list companies (status={code}). Body starts: {body[:300]}")

    def list_jobs_first_page(self, company_id: int) -> Tuple[int, Any, str]:
        # Per Simpro v1.0, jobs live under /companies/{companyId}/jobs
        return self.get(f"/companies/{company_id}/jobs", params={})
        # If your tenant requires different pagination, add:
        # params={"Page": 1, "PageSize": 100}

# ----- Public functions used by FastAPI routes --------------------------------

def check_api() -> Dict[str, Any]:
    """
    Return a diagnostic object showing status codes for:
    - /info
    - /companies
    - /companies/{companyId}/jobs (if we can determine a company)
    """
    started = time.time()
    client = SimproClient()
    out: Dict[str, Any] = {
        "ok": False,
        "base": client.api_base,
        "checks": [],
    }
    try:
        client.token()
        # /info
        code, js, body = client.get("/info")
        out["checks"].append({"path": "/info", "status": code})
        # /companies
        companies: List[Dict[str, Any]] = []
        try:
            companies = client.list_companies()
            out["checks"].append({"path": "/companies", "status": 200, "count": len(companies)})
        except Exception as e:
            out["checks"].append({"path": "/companies", "status": "error", "error": str(e)})

        # jobs (first page)
        comp_id_env = os.getenv("SIMPRO_COMPANY_ID", "").strip()
        company_id: Optional[int] = None
        if comp_id_env != "":
            try:
                company_id = int(comp_id_env)
            except Exception:
                pass
        if company_id is None and companies:
            # take the first company if present
            cid = companies[0].get("id") or companies[0].get("companyId")
            if cid is not None:
                try:
                    company_id = int(cid)
                except Exception:
                    pass

        if company_id is not None:
            code, js, body = client.list_jobs_first_page(company_id)
            out["checks"].append({"path": f"/companies/{company_id}/jobs", "status": code})
            if code == 404:
                # Record first 200 chars to help diagnose base URL issues
                out["note"] = f"jobs 404 - body starts: {body[:200]}"
        else:
            out["note"] = "No company id available; set SIMPRO_COMPANY_ID or ensure /companies works."

        out["ok"] = True
        return out
    finally:
        out["elapsed_sec"] = round(time.time() - started, 2)

def ingest_live() -> Dict[str, Any]:
    """
    Fetch first page of jobs and land raw JSON rows into SQLite.
    """
    started = datetime.now(timezone.utc)
    conn = _db_conn()
    _ensure_tables(conn)

    client = SimproClient()
    try:
        client.token()
    except Exception as e:
        return {"ok": False, "elapsed_sec": 0, "jobs_inserted": 0, "jobs_tried": 0, "note": f"oauth_failed: {e}"}

    # Work out company id
    comp_id_env = os.getenv("SIMPRO_COMPANY_ID", "").strip()
    company_id: Optional[int] = None
    if comp_id_env != "":
        try:
            company_id = int(comp_id_env)
        except Exception:
            return {"ok": False, "elapsed_sec": 0, "jobs_inserted": 0, "jobs_tried": 0, "note": "SIMPRO_COMPANY_ID must be an integer"}

    if company_id is None:
        try:
            companies = client.list_companies()
            cid = companies[0].get("id") or companies[0].get("companyId")
            company_id = int(cid) if cid is not None else None
        except Exception as e:
            return {"ok": False, "elapsed_sec": 0, "jobs_inserted": 0, "jobs_tried": 0, "note": f"companies_failed: {e}"}

    # Fetch jobs (first page)
    code, js, body = client.list_jobs_first_page(company_id)
    if code == 404:
        # This is the situation you’re seeing now – wrong base/path
        return {"ok": False, "elapsed_sec": 0.0, "jobs_inserted": 0, "jobs_tried": 0, "run_id": None, "note": "api_status_404 - check SIMPRO_TENANT_BASE and that /api/v1.0/companies/{companyId}/jobs exists", "body_snippet": body[:200]}
    if code != 200 or not isinstance(js, list):
        return {"ok": False, "elapsed_sec": 0.0, "jobs_inserted": 0, "jobs_tried": 0, "run_id": None, "note": f"jobs_get_failed status={code} body_starts={body[:200]}"}

    # Insert
    inserted = 0
    tried = 0
    now = datetime.now(timezone.utc).isoformat()
    for job in js:
        tried += 1
        job_id = None
        for k in ("id", "Id", "jobId", "JobId", "jobID", "JobID"):
            if isinstance(job, dict) and k in job:
                job_id = str(job[k])
                break
        conn.execute(
            "INSERT INTO simpro_jobs_raw(job_id, payload, inserted_at) VALUES (?,?,?)",
            (job_id, json.dumps(job), now),
        )
        inserted += 1
    conn.commit()

    finished = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO ingest_runs(started_at, finished_at, ok, note) VALUES (?,?,?,?)",
        (started.isoformat(), finished.isoformat(), 1, f"inserted={inserted}, tried={tried}")
    )
    conn.commit()

    return {
        "ok": True,
        "elapsed_sec": round((finished - started).total_seconds(), 2),
        "jobs_inserted": inserted,
        "jobs_tried": tried,
    }

# Simple demo that just seeds a couple of rows locally (no API call)
def ingest_demo() -> Dict[str, Any]:
    conn = _db_conn()
    _ensure_tables(conn)
    now = datetime.now(timezone.utc).isoformat()
    sample = [
        {"id": 1001, "name": "Demo Job A", "status": "In Progress"},
        {"id": 1002, "name": "Demo Job B", "status": "Completed"},
    ]
    for j in sample:
        conn.execute(
            "INSERT INTO simpro_jobs_raw(job_id, payload, inserted_at) VALUES (?,?,?)",
            (str(j["id"]), json.dumps(j), now),
        )
    conn.commit()
    return {"ok": True, "elapsed_sec": 0.0, "jobs_inserted": len(sample), "jobs_tried": len(sample), "note": "demo"}

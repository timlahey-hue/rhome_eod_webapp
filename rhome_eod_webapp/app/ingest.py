# rhome_eod_webapp/app/ingest.py
"""
Ingestion for the EOD dashboard.

- ingest_demo(): seeds synthetic rows so the UI always has something to show.
- ingest_live(base_url, client_id, client_secret, company_id): pulls live jobs
  from Simpro using the v1.0 single-job endpoint and stores a new snapshot.

Schema we maintain (tables):
  - snapshots(id INTEGER PK, snapshot_date TEXT, created_at TEXT)
  - job_rows( ... see ensure_schema() ... )
  - meta(k TEXT PRIMARY KEY, v TEXT)  # tiny k/v for last_good_job_id, etc.

Compatibility views for the UI:
  - snapshot => SELECT id, snapshot_date AS as_of, created_at FROM snapshots
  - job      => SELECT * FROM job_rows

Environment knobs (optional):
  - SIMPRO_ALLOWED_STAGES="Pending,Progress"  # stage names to keep (case-insensitive)
  - SIMPRO_SEED_JOB_ID="1250"                 # only used the very first run
  - SIMPRO_SCAN_BACK="150"                    # how far behind last good id to scan
  - SIMPRO_SCAN_FORWARD="150"                 # how far ahead to scan
  - SIMPRO_JOB_IDS="1220,1250,1255"           # explicit list (skips window scan)
  - EOD_DB_PATH="eod.db"                      # sqlite path (symlinked at startup)
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple

LOGGER = logging.getLogger("ingest")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s [ingest] %(message)s")
    handler.setFormatter(fmt)
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


# ----------------------------
# SQLite helpers
# ----------------------------

def db_path() -> str:
    return os.environ.get("EOD_DB_PATH", "eod.db")


def ensure_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    # Base tables
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          snapshot_date TEXT NOT NULL,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_rows (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          snapshot_id INTEGER NOT NULL,
          job_code TEXT,
          job_name TEXT,
          pm TEXT,
          hours_today REAL,
          labour_cost_today REAL,
          materials_cost_today REAL,
          cost_today REAL,
          actual_cost_to_date REAL,
          estimated_cost REAL,
          burn_pct REAL,
          gm_to_date REAL,
          invoiced_today REAL,
          mtd_hours REAL,
          days_since_update INTEGER,
          at_risk INTEGER,
          FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
        );

        CREATE TABLE IF NOT EXISTS meta (
          k TEXT PRIMARY KEY,
          v TEXT
        );

        -- Optional (kept for UI comfort; we'll leave it empty unless you
        -- decide to feed it later)
        CREATE TABLE IF NOT EXISTS timesheet (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          snapshot_id INTEGER NOT NULL,
          employee TEXT,
          hours REAL,
          FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
        );
        """
    )
    # Views the UI expects
    cur.executescript(
        """
        DROP VIEW IF EXISTS snapshot;
        CREATE VIEW snapshot AS
          SELECT id, snapshot_date AS as_of, created_at
          FROM snapshots;

        DROP VIEW IF EXISTS job;
        CREATE VIEW job AS
          SELECT * FROM job_rows;
        """
    )
    con.commit()


def kv_get(con: sqlite3.Connection, key: str) -> Optional[str]:
    row = con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return row[0] if row else None


def kv_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO meta(k,v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    con.commit()


def new_snapshot(con: sqlite3.Connection) -> int:
    now = dt.datetime.utcnow()
    as_of = now.date().isoformat()
    con.execute(
        "INSERT INTO snapshots(snapshot_date, created_at) VALUES(?, ?)",
        (as_of, now.isoformat()),
    )
    sid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    return int(sid)


def insert_job_rows(con: sqlite3.Connection, snapshot_id: int, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    cols = [
        "snapshot_id", "job_code", "job_name", "pm", "hours_today",
        "labour_cost_today", "materials_cost_today", "cost_today",
        "actual_cost_to_date", "estimated_cost", "burn_pct", "gm_to_date",
        "invoiced_today", "mtd_hours", "days_since_update", "at_risk",
    ]
    values = [
        (
            snapshot_id,
            r.get("job_code"),
            r.get("job_name"),
            r.get("pm"),
            r.get("hours_today", 0.0),
            r.get("labour_cost_today", 0.0),
            r.get("materials_cost_today", 0.0),
            r.get("cost_today", 0.0),
            r.get("actual_cost_to_date", 0.0),
            r.get("estimated_cost", 0.0),
            r.get("burn_pct"),
            r.get("gm_to_date"),
            r.get("invoiced_today", 0.0),
            r.get("mtd_hours", 0.0),
            r.get("days_since_update", 0),
            1 if r.get("at_risk") else 0,
        )
        for r in rows
    ]
    con.executemany(
        f"INSERT INTO job_rows({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
        values,
    )
    return len(values)


# ----------------------------
# Simpro HTTP
# ----------------------------

def get_token(base_url: str, client_id: str, client_secret: str) -> str:
    """
    OAuth2 client-credentials against {base}/oauth2/token
    """
    url = f"{base_url.rstrip('/')}/oauth2/token"
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            j = json.load(resp)
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", "ignore")
        raise RuntimeError(f"OAuth token HTTP {e.code}: {msg[:200]}") from e
    tok = j.get("access_token")
    if not tok:
        raise RuntimeError("No access_token in token response.")
    return tok


def fetch_job_v10(base_url: str, company_id: int, job_id: int, token: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    """
    GET /api/v1.0/companies/{companyId}/jobs/{jobId}
    Returns (status_code, json or None)
    """
    url = f"{base_url.rstrip('/')}/api/v1.0/companies/{company_id}/jobs/{job_id}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            code = resp.getcode()
            if code == 200:
                return 200, json.load(resp)
            return code, None
    except urllib.error.HTTPError as e:
        # 404 is expected for gaps in the id space
        return e.code, None
    except Exception:
        return 0, None


# ----------------------------
# Mapping helpers
# ----------------------------

def _dict_name(d: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(d, dict):
        return None
    # Try common shapes
    for k in ("Name", "FullName", "DisplayName", "name"):
        if k in d and d[k]:
            return str(d[k])
    # Given/Family style
    gn = (d.get("GivenName") or d.get("Given")) or ""
    fn = (d.get("FamilyName") or d.get("Family")) or ""
    n = f"{gn} {fn}".strip()
    return n or None


def job_stage(job: Dict[str, Any]) -> Optional[str]:
    """
    Try to pull 'Stage' from a few plausible locations in v1.0 job payloads.
    Falls back to Status.Name pattern matching if needed.
    """
    # Direct strings or nested dicts with Name
    for key in ("Stage", "JobStage", "StageName", "JobStageName"):
        v = job.get(key)
        if isinstance(v, dict):
            nm = _dict_name(v)
            if nm:
                return nm
        elif isinstance(v, str) and v.strip():
            return v.strip()

    # Some tenants only expose custom Status.Name; sometimes that includes "Pending"/"Progress"
    status = job.get("Status")
    if isinstance(status, dict):
        nm = _dict_name(status)
        if nm:
            lower = nm.lower()
            for s in ("pending", "progress", "complete", "archived"):
                if s in lower:
                    return s.title()
    elif isinstance(status, str) and status:
        lower = status.lower()
        for s in ("pending", "progress", "complete", "archived"):
            if s in lower:
                return s.title()
    return None


def pick(s: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in s and s[k] is not None:
            return s[k]
    return None


def map_job_to_row(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull the fields our UI expects, defaulting unknowns to 0/None.
    """
    # Job code / number
    code = pick(job, "JobNo", "JobNumber", "Number", "JobCode", "Code", "ID")
    job_code = str(code) if code is not None else str(job.get("ID", ""))

    # Name/description
    job_name = pick(job, "Name", "JobName", "Description")
    if not job_name:
        typ = pick(job, "Type") or "Job"
        cust = pick(job.get("Customer", {}) if isinstance(job.get("Customer"), dict) else {}, "CompanyName", "Name")
        job_name = f"{typ} - {cust}" if cust else str(typ)

    # Project Manager, best-effort
    pm = None
    for key in ("ProjectManager", "AssignedTo", "Manager", "Owner"):
        pm = _dict_name(job.get(key))
        if pm:
            break

    # Totals: the v1.0 payload has Total.ExTax / IncTax (observed)
    total = job.get("Total") or job.get("Totals") or {}
    ex_tax = None
    if isinstance(total, dict):
        ex_tax = pick(total, "ExTax", "ExTaxTotal", "EstimateExTax")

    # Actual cost to date if provided in some nested totals
    actual_cost = None
    if isinstance(total, dict):
        # Try a few plausible shapes
        actual_cost = (
            pick(total, "ActualCostsExTax", "ActualExTax", "ActualCosts")
            or pick(total.get("Actual", {}) if isinstance(total.get("Actual"), dict) else {}, "ExTax", "Cost")
            or None
        )

    estimated_cost = ex_tax if isinstance(ex_tax, (int, float)) else 0.0
    actual_cost_to_date = float(actual_cost) if isinstance(actual_cost, (int, float)) else 0.0

    gm_to_date = None
    if estimated_cost and isinstance(estimated_cost, (int, float)):
        try:
            gm_to_date = (estimated_cost - actual_cost_to_date) / float(estimated_cost)
        except Exception:
            gm_to_date = None

    # Days since update (best-effort)
    updated_keys = ("UpdatedOn", "UpdatedAt", "LastUpdated", "ModifiedOn", "ModifiedAt")
    updated_raw = None
    for k in updated_keys:
        v = job.get(k)
        if v:
            updated_raw = str(v)
            break
    days_since_update = 0
    if updated_raw:
        try:
            # Try a couple formats
            # 2025-08-10T13:45:00Z or 2025-08-10T13:45:00.123
            up = None
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    up = dt.datetime.strptime(updated_raw[:len(fmt)], fmt)
                    break
                except Exception:
                    pass
            if up:
                delta = dt.datetime.utcnow() - up
                days_since_update = max(0, int(delta.days))
        except Exception:
            pass

    # We don't have same-day time sheets from this endpoint; leave *_today zero.
    row = {
        "job_code": job_code,
        "job_name": job_name,
        "pm": pm or "",
        "hours_today": 0.0,
        "labour_cost_today": 0.0,
        "materials_cost_today": 0.0,
        "cost_today": 0.0,
        "actual_cost_to_date": actual_cost_to_date,
        "estimated_cost": float(estimated_cost or 0.0),
        "burn_pct": None,     # undefined with current payloads
        "gm_to_date": gm_to_date,
        "invoiced_today": 0.0,
        "mtd_hours": 0.0,
        "days_since_update": days_since_update,
        "at_risk": (gm_to_date is not None and gm_to_date < 0.0),
    }
    return row


# ----------------------------
# Public entry points
# ----------------------------

def ingest_demo() -> Dict[str, Any]:
    """
    Insert a small, randomly generated snapshot for demo/testing.
    """
    import random

    start = time.perf_counter()
    con = sqlite3.connect(db_path())
    try:
        ensure_schema(con)
        sid = new_snapshot(con)

        rng = random.Random(42 + sid)  # stable-ish demo
        rows: List[Dict[str, Any]] = []
        for i in range(10):
            est = rng.uniform(8000, 60000)
            act = est * rng.uniform(0.2, 0.9)
            gm = (est - act) / est if est else None
            rows.append(
                {
                    "job_code": f"DEMO-{sid}-{i+1}",
                    "job_name": f"Demo Job {i+1}",
                    "pm": rng.choice(["Alex", "Sam", "Taylor", "Jordan"]),
                    "hours_today": round(rng.uniform(0, 8), 1),
                    "labour_cost_today": round(rng.uniform(150, 900), 2),
                    "materials_cost_today": round(rng.uniform(100, 1500), 2),
                    "cost_today": 0.0,  # labour + materials could be used but keeping 0 keeps logic simple
                    "actual_cost_to_date": round(act, 2),
                    "estimated_cost": round(est, 2),
                    "burn_pct": None,
                    "gm_to_date": gm,
                    "invoiced_today": round(rng.uniform(0, 3000), 2),
                    "mtd_hours": round(rng.uniform(0, 160), 1),
                    "days_since_update": rng.randint(0, 4),
                    "at_risk": gm is not None and gm < 0.1,
                }
            )

        n = insert_job_rows(con, sid, rows)
        con.commit()
        ok = n > 0
        dur = time.perf_counter() - start
        LOGGER.info("ingest_demo finished in %.3fs (rows=%s ok=%s)", dur, n, ok)
        return {"ok": ok, "rows": n, "snapshot_id": sid, "duration_s": round(dur, 3)}
    finally:
        con.close()


def ingest_live(base_url: str, client_id: str, client_secret: str, company_id: int | str) -> Dict[str, Any]:
    """
    Pull active jobs from Simpro (Stage in allowed set) by scanning around the
    last good job id. Uses the only working endpoint family we observed:
    GET /api/v1.0/companies/{companyId}/jobs/{jobId}
    """
    start = time.perf_counter()
    base_url = (base_url or "").rstrip("/")
    if not base_url or not client_id or not client_secret:
        LOGGER.error("Missing required inputs (base_url/client_id/client_secret).")
        return {"ok": False, "error": "missing_credentials"}

    try:
        company_id = int(company_id)
    except Exception:
        company_id = 0

    # Allowed stages (default: Pending + Progress)
    raw_stages = os.environ.get("SIMPRO_ALLOWED_STAGES", "Pending,Progress")
    allowed_stages = {s.strip().lower() for s in raw_stages.split(",") if s.strip()}

    # Token
    try:
        token = get_token(base_url, client_id, client_secret)
        LOGGER.info("Authenticated with Simpro")
    except Exception as e:
        LOGGER.error("Auth failed: %s", e)
        return {"ok": False, "error": "auth_failed", "detail": str(e)}

    # Choose which job ids to probe
    con = sqlite3.connect(db_path())
    try:
        ensure_schema(con)

        # explicit list wins
        explicit = os.environ.get("SIMPRO_JOB_IDS")
        probe_ids: List[int]
        if explicit:
            probe_ids = []
            for t in explicit.split(","):
                t = t.strip()
                if t.isdigit():
                    probe_ids.append(int(t))
        else:
            seed = kv_get(con, "last_good_job_id")
            if seed and seed.isdigit():
                seed_id = int(seed)
            else:
                seed_id = int(os.environ.get("SIMPRO_SEED_JOB_ID", "1250") or 1250)

            back = int(os.environ.get("SIMPRO_SCAN_BACK", "150") or 150)
            fwd = int(os.environ.get("SIMPRO_SCAN_FORWARD", "150") or 150)

            low = max(1, seed_id - back)
            high = max(low, seed_id + fwd)
            probe_ids = list(range(low, high + 1))

        # Fetch and filter
        kept_rows: List[Dict[str, Any]] = []
        good_ids: List[int] = []
        newest_ok = 0

        # Keep this polite; Simpro rate limits can be touchy.
        for jid in probe_ids:
            code, job = fetch_job_v10(base_url, company_id, jid, token)
            if code == 200 and isinstance(job, dict):
                good_ids.append(jid)
                newest_ok = max(newest_ok, jid)

                stage = job_stage(job)
                stage_ok = True
                if allowed_stages:
                    stage_ok = (stage or "").strip().lower() in allowed_stages

                if stage_ok:
                    row = map_job_to_row(job)
                    # prefer numeric job_code if present to ease later sorting
                    row["job_code"] = str(jid) if (row.get("job_code") is None or not str(row["job_code"]).strip()) else str(row["job_code"])
                    kept_rows.append(row)

            # Tiny delay so a 300-id pass doesn't hammer the API
            time.sleep(0.05)

        # Write snapshot + rows
        sid = new_snapshot(con)
        n = insert_job_rows(con, sid, kept_rows)
        con.commit()

        # Remember the frontier so next run scans forward automatically
        if newest_ok:
            kv_set(con, "last_good_job_id", str(newest_ok))

        dur = time.perf_counter() - start
        LOGGER.info(
            "ingest_live finished in %.3fs (ok=%s, rows=%s, probed=%s, hits=%s, latest_ok_id=%s)",
            dur, n > 0, n, len(probe_ids), len(good_ids), newest_ok or "-"
        )
        return {
            "ok": True,
            "rows": n,
            "snapshot_id": sid,
            "probed": len(probe_ids),
            "hits": len(good_ids),
            "latest_ok_id": newest_ok or None,
            "duration_s": round(dur, 3),
        }
    finally:
        con.close()

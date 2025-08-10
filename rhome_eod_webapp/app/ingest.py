# app/ingest.py
# Single-file ingest that:
# - Authenticates to Simpro via client-credentials
# - Finds "active" jobs by Stage (Pending/In Progress), excludes Service jobs
# - Computes Estimated vs Actual GM, Estimated vs Actual Hours
# - Inserts a new snapshot + job rows into eod.db
# - Works with both main.py call styles: ingest_live() and ingest_live(base, cid, seconds, continue_on_error)

from __future__ import annotations

import os
import time
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

import logging
import requests

LOG = logging.getLogger("ingest")
if not LOG.handlers:
    h = logging.StreamHandler()
    fmt = logging.Formatter("INFO:ingest:[%(name)s] %(message)s")
    h.setFormatter(fmt)
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)

# ---------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        if default is None:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return default
    return str(v).strip()

def _db_path() -> Path:
    # Uvicorn entrypoint makes a symlink: ./eod.db -> /data/eod.db
    # Keep using the local file to respect that symlink.
    root = Path(__file__).resolve().parents[1]  # project/src/rhome_eod_webapp
    return root / "eod.db"

# ---------------------------------------------------------------------
# OAuth2 client-credentials to Simpro
# ---------------------------------------------------------------------

def _get_token(
    token_url: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> str:
    """
    Retrieve an access token via client-credentials and cache to /tmp/token.json
    """
    token_url = token_url or _env("SIMPRO_TOKEN_URL", "https://rhome.simprosuite.com/oauth2/token")
    client_id = client_id or _env("SIMPRO_CLIENT_ID")
    client_secret = client_secret or _env("SIMPRO_CLIENT_SECRET")

    LOG.info("Authenticating with Simpro")
    resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=20,
    )
    resp.raise_for_status()
    tok = resp.json().get("access_token")
    if not tok:
        raise RuntimeError("No access_token in token response")

    # Save to /tmp for shell snippets that expect it
    try:
        Path("/tmp").mkdir(parents=True, exist_ok=True)
        with open("/tmp/token.json", "w") as f:
            json.dump({"access_token": tok, "fetched_at": int(time.time())}, f)
    except Exception:
        # Non-fatal if we can't write /tmp
        pass

    LOG.info("Token acquired (len=%s)", len(tok))
    return tok

# ---------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def _api_base() -> str:
    return _env("SIMPRO_BASE_URL").rstrip("/") + "/api/v1.0"

def _get(path: str, token: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20) -> requests.Response:
    url = _api_base().rstrip("/") + "/" + path.lstrip("/")
    return requests.get(url, headers=_headers(token), params=params or {}, timeout=timeout)

# ---------------------------------------------------------------------
# Job retrieval strategy
# ---------------------------------------------------------------------
# We prefer not to rely on a fixed ID range.
# This strategy:
#   1) Quickly find a "high water mark" job ID by exponential probe + binary search.
#   2) Walk downwards from that ID, picking ACTIVE Project jobs (Stage Pending/In Progress).
#   3) Respect a time budget so the web request doesn't time out.
#
# This avoids hardcoding ranges and still converges fast across runs.

def _job_exists(job_id: int, cid: int, token: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[int]]:
    """
    Returns (exists, job_json, status_code)
    """
    try:
        r = _get(f"companies/{cid}/jobs/{job_id}", token)
        if r.status_code == 200:
            return True, r.json(), 200
        return False, None, r.status_code
    except requests.RequestException:
        return False, None, None

def _find_highest_job_id(cid: int, token: str, time_budget_s: float) -> Optional[int]:
    """
    Exponentially probe to find an upper bound, then binary search to find the highest existing job id.
    """
    t_end = time.time() + max(3.0, time_budget_s * 0.25)  # use up to 25% of the budget
    # Start from a recent-ish baseline if DB knows one
    last_seen = _db_last_seen_job_id()
    probe = max(100, last_seen or 1000)

    exists, _, sc = _job_exists(probe, cid, token)
    if not exists and sc == 404:
        # Try going down until we hit something that exists
        step = max(1, probe // 10)
        while time.time() < t_end and probe > 1:
            probe -= step
            ok, _, sc = _job_exists(probe, cid, token)
            if ok:
                break
            if sc == 404 and step > 1:
                step = max(1, step // 2)
        if not ok:
            return None
    else:
        # Ramp up until we get a 404
        while time.time() < t_end and probe < 5_000_000:
            nxt = probe * 2
            ok, _, sc = _job_exists(nxt, cid, token)
            if not ok and sc == 404:
                break
            if ok:
                probe = nxt
            else:
                # Network error or non-404: stop ramping and keep current probe
                break

    # Now we have a range [lo, hi] where lo exists, hi may not
    lo = max(1, probe)
    hi = probe * 2
    ok, _, sc = _job_exists(hi, cid, token)
    if ok:
        # hi exists, widen one more step (best effort)
        hi *= 2

    # Binary search down to the last existing id
    last_good = lo
    while time.time() < t_end and hi - lo > 1:
        mid = (lo + hi) // 2
        ok, _, sc = _job_exists(mid, cid, token)
        if ok:
            lo = mid
            last_good = mid
        elif sc == 404:
            hi = mid
        else:
            # Unknown response, shrink cautiously
            hi = mid
    return int(last_good) if last_good else None

def _normalize_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()

def _infer_stage(j: Dict[str, Any]) -> str:
    # Collect plausible fields
    candidates: List[str] = []
    v = j.get("stage")
    if isinstance(v, dict):
        candidates.append(_normalize_text(v.get("name")))
    else:
        candidates.append(_normalize_text(v))
    for k in ("stageName", "Stage", "jobStage", "jobStageName", "currentStage", "status", "statusName"):
        if k in j:
            candidates.append(_normalize_text(j.get(k)))
    text = " ".join(candidates).lower()
    if "archive" in text:
        return "Archived"
    if "complete" in text or "completed" in text:
        return "Complete"
    if "in progress" in text or "progress" in text:
        return "In Progress"
    if "pending" in text:
        return "Pending"
    return candidates[0] or "Unknown"

def _infer_type(j: Dict[str, Any]) -> str:
    for k in ("type", "jobType", "jobTypeName", "TypeName"):
        if k in j and j[k]:
            return _normalize_text(j[k])
    # Sometimes nested
    v = j.get("jobType") or j.get("typeObj") or {}
    if isinstance(v, dict) and v.get("name"):
        return _normalize_text(v["name"])
    return "Unknown"

def _first_number(j: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
    for k in keys:
        if "." in k:
            node = j
            ok = True
            for part in k.split("."):
                if isinstance(node, dict) and part in node:
                    node = node[part]
                else:
                    ok = False
                    break
            if ok and node is not None:
                try:
                    return float(node)
                except Exception:
                    pass
        else:
            if k in j and j[k] is not None:
                try:
                    return float(j[k])
                except Exception:
                    pass
    return None

def _first_text(j: Dict[str, Any], keys: Iterable[str]) -> str:
    for k in keys:
        if "." in k:
            node = j
            ok = True
            for part in k.split("."):
                if isinstance(node, dict) and part in node:
                    node = node[part]
                else:
                    ok = False
                    break
            if ok and node is not None:
                return _normalize_text(node)
        else:
            if k in j and j[k] is not None:
                return _normalize_text(j[k])
    return ""

def _extract_job_fields(j: Dict[str, Any]) -> Dict[str, Any]:
    job_id = int(_first_number(j, ["id", "jobId"]) or 0)

    job_code = _first_text(j, ["jobNumber", "code", "number", "jobCode"])
    job_name = _first_text(j, ["name", "jobName", "title"])
    client_name = (
        _first_text(j, ["customer.name", "customer.companyName", "customerName", "client.name", "clientName"])
    )

    stage = _infer_stage(j)
    jtype = _infer_type(j)

    # Pricing & costs (try multiple likely keys)
    est_rev = _first_number(
        j,
        [
            "quotedPriceExTax", "contractPriceExTax", "sellPriceExTax", "exTaxTotal",
            "Totals.ExTaxTotal", "totalExTax", "sellExTax",
        ],
    )
    est_cost = _first_number(
        j,
        [
            "estimatedCostExTax", "estimatedCostsExTax", "estimatedCost", "budget.exTaxCost",
            "Totals.CostExTax", "totalCostEstimatedExTax",
        ],
    )
    act_cost_td = _first_number(
        j,
        [
            "actualCostToDateExTax", "actualCostExTax", "costToDateExTax", "costToDate",
            "Totals.ActualCostExTax",
        ],
    )

    est_hours = _first_number(j, ["estimatedHours", "totalHoursEstimated", "budget.estimatedHours"])
    act_hours = _first_number(j, ["actualHoursToDate", "actualHours", "hoursToDate"])

    # "today" deltas – only if you later decide to ingest timesheets; for now keep 0
    hours_today = 0.0
    cost_today = 0.0

    # Derived
    gm_est_pct = None
    gm_act_pct = None
    if est_rev and est_cost is not None and est_rev > 0:
        gm_est_pct = (est_rev - est_cost) / est_rev
    if est_rev and act_cost_td is not None and est_rev > 0:
        gm_act_pct = (est_rev - act_cost_td) / est_rev

    cost_to_complete = None
    if est_cost is not None and act_cost_td is not None:
        cost_to_complete = max(0.0, est_cost - act_cost_td)

    return {
        "job_id": job_id,
        "job_code": job_code,
        "job_name": job_name,
        "client_name": client_name,
        "stage": stage,
        "type": jtype,
        "estimated_revenue": est_rev or 0.0,
        "estimated_cost": est_cost or 0.0,
        "actual_cost_to_date": act_cost_td or 0.0,
        "estimated_hours": est_hours or 0.0,
        "actual_hours": act_hours or 0.0,
        "hours_today": hours_today,
        "cost_today": cost_today,
        "gm_estimated_pct": gm_est_pct if gm_est_pct is not None else None,
        "gm_actual_pct": gm_act_pct if gm_act_pct is not None else None,
        "cost_to_complete": cost_to_complete if cost_to_complete is not None else None,
    }

def _is_active_project(fields: Dict[str, Any]) -> bool:
    stage = (fields.get("stage") or "").lower()
    jtype = (fields.get("type") or "").lower()
    if "service" in jtype:
        return False
    # Keep pending or (in) progress; exclude complete/archived
    if "archiv" in stage or "complete" in stage:
        return False
    if "pending" in stage or "progress" in stage:
        return True
    # Unknown stage: treat as inactive to be safe
    return False

# ---------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------

SCHEMA_SNAPSHOT = """
CREATE TABLE IF NOT EXISTS snapshot (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  as_of    TEXT NOT NULL
);
"""

SCHEMA_JOB = """
CREATE TABLE IF NOT EXISTS job (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,  -- row id
  snapshot_id           INTEGER NOT NULL,
  job_id                INTEGER NOT NULL,
  job_code              TEXT,
  job_name              TEXT,
  client_name           TEXT,
  stage                 TEXT,
  type                  TEXT,
  estimated_revenue     REAL,
  estimated_cost        REAL,
  actual_cost_to_date   REAL,
  estimated_hours       REAL,
  actual_hours          REAL,
  hours_today           REAL,
  cost_today            REAL,
  gm_estimated_pct      REAL,
  gm_actual_pct         REAL,
  cost_to_complete      REAL
);
"""

SCHEMA_META = """
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
"""

def _db_connect() -> sqlite3.Connection:
    dbp = _db_path()
    dbp.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(dbp))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def _db_init(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SNAPSHOT)
    conn.executescript(SCHEMA_JOB)
    conn.executescript(SCHEMA_META)
    conn.commit()

def _db_insert_snapshot(conn: sqlite3.Connection) -> int:
    as_of = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("INSERT INTO snapshot(as_of) VALUES (?)", (as_of,))
    conn.commit()
    return int(cur.lastrowid)

def _db_last_seen_job_id() -> Optional[int]:
    try:
        conn = _db_connect()
        # Use the highest job_id from the most recent snapshot, if any
        row = conn.execute(
            """
            SELECT MAX(j.job_id)
            FROM job j
            WHERE j.snapshot_id = (SELECT MAX(id) FROM snapshot)
            """
        ).fetchone()
        conn.close()
        if row and row[0]:
            return int(row[0])
    except Exception:
        pass
    return None

def _db_set_meta(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute("INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    conn.commit()

# ---------------------------------------------------------------------
# Public entry points expected by app.main
# ---------------------------------------------------------------------

def ingest_live(*args) -> Dict[str, Any]:
    """
    Supports two call styles:
      - ingest_live()  -> reads env for base/company/time budget
      - ingest_live(base, cid, seconds, continue_on_error)

    Returns a dict used by main.py for logging/response.
    """
    # Parse args or env
    if len(args) == 4:
        base, cid, seconds, _continue = args
        # "base" is not needed here; we rely on SIMPRO_BASE_URL env so main.py and direct calls behave the same
        os.environ.setdefault("SIMPRO_INGEST_SECONDS", str(int(seconds)))
    else:
        # nothing to do; read from env
        pass

    base_url = _env("SIMPRO_BASE_URL")  # validate present
    cid = int(_env("SIMPRO_COMPANY_ID", "0") or 0)
    seconds = int(_env("SIMPRO_INGEST_SECONDS", "25"))

    tok = _get_token()  # uses env token url + creds
    t0 = time.time()
    LOG.info("Starting live ingest (budget=%ss, company_id=%s)", seconds, cid)

    # DB init + new snapshot
    conn = _db_connect()
    _db_init(conn)
    snapshot_id = _db_insert_snapshot(conn)

    # Discover highest job id quickly
    hi = _find_highest_job_id(cid, tok, seconds)
    if not hi:
        LOG.warning("Could not discover highest job id within time budget")
        hi = (_db_last_seen_job_id() or 1200)

    # Walk downwards collecting active project jobs
    jobs: List[Dict[str, Any]] = []
    tried = 0
    t_end = t0 + seconds
    job_id = int(hi)

    while time.time() < t_end and job_id >= 1:
        tried += 1
        exists, j, sc = _job_exists(job_id, cid, tok)
        if exists and j:
            fields = _extract_job_fields(j)
            if _is_active_project(fields):
                jobs.append(fields)
        elif sc is None:
            # transient error; don't burn time here
            pass

        # Stop once we have "enough" current jobs, or continue until budget
        if len(jobs) >= 150 and time.time() > (t0 + seconds * 0.6):
            break
        job_id -= 1

    if time.time() >= t_end:
        LOG.warning("time budget reached; stopping at id=%s", job_id)

    # Insert job rows
    if jobs:
        cols = (
            "snapshot_id, job_id, job_code, job_name, client_name, stage, type, "
            "estimated_revenue, estimated_cost, actual_cost_to_date, "
            "estimated_hours, actual_hours, hours_today, cost_today, "
            "gm_estimated_pct, gm_actual_pct, cost_to_complete"
        )
        placeholders = ",".join(["?"] * 17)
        data = [
            (
                snapshot_id,
                r["job_id"],
                r["job_code"],
                r["job_name"],
                r["client_name"],
                r["stage"],
                r["type"],
                r["estimated_revenue"],
                r["estimated_cost"],
                r["actual_cost_to_date"],
                r["estimated_hours"],
                r["actual_hours"],
                r["hours_today"],
                r["cost_today"],
                r["gm_estimated_pct"],
                r["gm_actual_pct"],
                r["cost_to_complete"],
            )
            for r in jobs
        ]
        conn.executemany(f"INSERT INTO job({cols}) VALUES ({placeholders})", data)
        conn.commit()

    elapsed = time.time() - t0
    LOG.info(
        "ingest_live finished in %.2fs (ok=%s, jobs=%s, tried=%s, snapshot_id=%s)",
        elapsed,
        True,
        len(jobs),
        tried,
        snapshot_id,
    )
    try:
        _db_set_meta(conn, "last_snapshot_id", str(snapshot_id))
        if jobs:
            _db_set_meta(conn, "last_seen_job_id", str(max(j["job_id"] for j in jobs)))
    except Exception:
        pass
    finally:
        conn.close()

    return {
        "ok": True,
        "jobs": len(jobs),
        "tried": tried,
        "snapshot_id": snapshot_id,
        "elapsed_sec": round(elapsed, 3),
    }

def ingest() -> Dict[str, Any]:
    """
    Compatibility alias if main.py ever imports ingest().
    """
    return ingest_live()

def ingest_demo() -> Dict[str, Any]:
    """
    Very small/no-op demo that clones the latest snapshot rows (if any)
    into a new snapshot so the UI has something to show.
    This avoids network calls and avoids NameError in main.py.
    """
    conn = _db_connect()
    _db_init(conn)
    cur = conn.execute("SELECT MAX(id) FROM snapshot")
    row = cur.fetchone()
    if not row or not row[0]:
        # If nothing exists, fall back to a tiny live run with a tiny budget
        os.environ.setdefault("SIMPRO_INGEST_SECONDS", "10")
        try:
            return ingest_live()
        except Exception as e:
            # Still ensure an empty snapshot is created
            sid = _db_insert_snapshot(conn)
            conn.close()
            return {"ok": False, "jobs": 0, "tried": 0, "snapshot_id": sid, "error": str(e)}

    last_sid = int(row[0])
    new_sid = _db_insert_snapshot(conn)

    # Copy rows from last snapshot into the new one
    # Fetch column names except the PK id + snapshot_id
    cols = [r[1] for r in conn.execute("PRAGMA table_info(job)") if r[1] not in ("id", "snapshot_id")]
    if cols:
        select_cols = ", ".join(cols)
        insert_cols = ", ".join(["snapshot_id"] + cols)
        conn.execute(
            f"INSERT INTO job({insert_cols}) "
            f"SELECT ?, {select_cols} FROM job WHERE snapshot_id = ?",
            (new_sid, last_sid),
        )
        conn.commit()

    conn.close()
    return {"ok": True, "jobs": -1, "tried": 0, "snapshot_id": new_sid, "cloned_from": last_sid}

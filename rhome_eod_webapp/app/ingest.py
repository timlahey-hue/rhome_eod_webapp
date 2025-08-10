# app/ingest.py
from __future__ import annotations

import os
import time
import json
import math
import sqlite3
import logging
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

log = logging.getLogger("ingest")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

# -----------------------------
# Helpers
# -----------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else (default if default is not None else "")

def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()

def _db_path() -> str:
    # default symlink ./eod.db -> /data/eod.db is created by your start command
    here = os.path.dirname(__file__)
    return os.path.join(here, "..", "eod.db") if not os.path.exists(os.path.join(here, "eod.db")) \
        else os.path.join(here, "eod.db")

def _safe_get(d: Dict[str, Any], *path: str, default: Any = None) -> Any:
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            x = x.strip().replace(",", "")
            if x.endswith("%"):
                return float(x[:-1]) / 100.0
            return float(x)
        return default
    except Exception:
        return default

def _norm_pct(x: Any) -> float:
    """Normalize percent: accept 0..1, 0..100, or '34.5%' strings."""
    v = _to_float(x, default=0.0)
    if v > 1.0:
        # assume 0..100 range
        return v / 100.0
    return v

def _days_since(dt_str: Optional[str]) -> int:
    if not dt_str:
        return 0
    try:
        # accept ISO or 'YYYY-MM-DD' type strings
        dt_val = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        try:
            dt_val = datetime.strptime(dt_str, "%Y-%m-%d")
            dt_val = dt_val.replace(tzinfo=timezone.utc)
        except Exception:
            return 0
    today = datetime.now(tz=dt_val.tzinfo or timezone.utc)
    return max((today.date() - dt_val.date()).days, 0)

def _ensure_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    # Canonical tables used by the UI through views
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            created_at    TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_rows(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            job_code TEXT,
            job_name TEXT,
            pm TEXT,
            hours_today REAL DEFAULT 0,
            labour_cost_today REAL DEFAULT 0,
            materials_cost_today REAL DEFAULT 0,
            cost_today REAL DEFAULT 0,
            actual_cost_to_date REAL DEFAULT 0,
            estimated_cost REAL DEFAULT 0,
            burn_pct REAL DEFAULT 0,
            gm_to_date REAL DEFAULT 0,
            invoiced_today REAL DEFAULT 0,
            mtd_hours REAL DEFAULT 0,
            days_since_update INTEGER DEFAULT 0,
            at_risk INTEGER DEFAULT 0
        )
    """)
    # compatibility views for the UI queries
    cur.execute("CREATE VIEW IF NOT EXISTS snapshot AS SELECT id, snapshot_date AS as_of, created_at FROM snapshots")
    cur.execute("CREATE VIEW IF NOT EXISTS job      AS SELECT * FROM job_rows")
    con.commit()

# -----------------------------
# Simpro API
# -----------------------------

@dataclass
class SimproCfg:
    base_url: str
    client_id: str
    client_secret: str
    company_id: int
    http_timeout: int = 6

def _get_token(cfg: SimproCfg) -> str:
    # Uses your tenant token endpoint (client credentials grant).
    # Example: https://rhome.simprosuite.com/oauth2/token
    token_url = f"{cfg.base_url.rstrip('/')}/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
    }
    resp = requests.post(token_url, data=data, timeout=cfg.http_timeout)
    resp.raise_for_status()
    j = resp.json()
    tok = j.get("access_token") or ""
    if not tok:
        raise RuntimeError("No access_token in token response")
    return tok

def _fetch_job_v1_0(cfg: SimproCfg, token: str, job_id: int) -> Tuple[int, Optional[Dict[str, Any]]]:
    """Return (status_code, json_or_none) for companies/{cid}/jobs/{job_id} on v1.0."""
    url = f"{cfg.base_url.rstrip('/')}/api/v1.0/companies/{cfg.company_id}/jobs/{job_id}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=cfg.http_timeout)
    if resp.status_code != 200:
        return resp.status_code, None
    try:
        return resp.status_code, resp.json()
    except Exception:
        return resp.status_code, None

def _is_active_project(job: Dict[str, Any]) -> bool:
    # Skip Service
    typ = (_safe_get(job, "Type", default="") or "").strip().lower()
    if typ == "service":
        return False

    # Stage filtering
    stage = (
        _safe_get(job, "Stage", "Name")
        or _safe_get(job, "StageName")
        or _safe_get(job, "Status", "Name")
        or _safe_get(job, "Status")
        or ""
    )
    s = stage.strip().lower()
    # Accept: 'pending', 'progress', 'in progress'
    active = ("pending" in s) or ("progress" in s)
    return active

def _parse_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract values with resilience to missing fields.
    """
    # identifiers / labels
    job_code = (
        _safe_get(job, "JobNo") or
        _safe_get(job, "Number") or
        _safe_get(job, "Code") or
        str(_safe_get(job, "ID", default=""))
    )
    job_name = _safe_get(job, "Name") or _safe_get(job, "Description") or ""
    # PM not required for display
    pm_name = ""

    # Totals and hours
    # We try several likely paths found in Simpro payloads.
    tot_cost_est = (
        _to_float(_safe_get(job, "Totals", "Cost", "Estimate"))
        or _to_float(_safe_get(job, "Total", "Cost", "Estimate"))
        or 0.0
    )
    tot_cost_act = (
        _to_float(_safe_get(job, "Totals", "Cost", "Actual"))
        or _to_float(_safe_get(job, "Total", "Cost", "Actual"))
        or 0.0
    )
    est_gm = _norm_pct(_safe_get(job, "Totals", "NettMargin", "Estimate"))
    act_gm = _norm_pct(_safe_get(job, "Totals", "NettMargin", "Actual"))

    # Hours (fallback to 0 if not present)
    est_hours = (
        _to_float(_safe_get(job, "Totals", "Labour", "Hours", "Estimate"))
        or _to_float(_safe_get(job, "Labour", "Hours", "Estimate"))
        or 0.0
    )
    act_hours = (
        _to_float(_safe_get(job, "Totals", "Labour", "Hours", "Actual"))
        or _to_float(_safe_get(job, "Labour", "Hours", "Actual"))
        or 0.0
    )

    # "Days since update" from any likely field
    last_updated = (
        _safe_get(job, "LastUpdated")
        or _safe_get(job, "Updated")
        or _safe_get(job, "UpdatedAt")
        or _safe_get(job, "ModifiedOn")
        or _safe_get(job, "ModifiedDate")
        or ""
    )
    days_since = _days_since(last_updated)

    # Derived
    burn = 0.0
    if tot_cost_est > 0:
        burn = min(max(tot_cost_act / tot_cost_est, 0.0), 10.0)

    # Today/MoTD figures require timesheets/invoice lines; leave 0 for now.
    row = {
        "job_code": str(job_code),
        "job_name": job_name,
        "pm": pm_name,
        "hours_today": 0.0,
        "labour_cost_today": 0.0,
        "materials_cost_today": 0.0,
        "cost_today": 0.0,
        "actual_cost_to_date": round(tot_cost_act, 2),
        "estimated_cost": round(tot_cost_est, 2),
        "burn_pct": burn,
        "gm_to_date": act_gm,  # UI shows actual GM%; we also store est via a trick below
        "invoiced_today": 0.0,
        "mtd_hours": round(act_hours, 1),
        "days_since_update": days_since,
        "at_risk": 1 if (burn >= 0.80 or (act_gm > 0 and act_gm < 0.20)) else 0,
        # We’ll piggyback est_gm into a reserved key; not shown in the UI but useful
        "_est_gm": est_gm,
        "_est_hours": est_hours,
    }
    return row

# -----------------------------
# Main ingest
# -----------------------------

def _candidate_ids() -> List[int]:
    """
    Build a list of job IDs to try, bounded so we don't time out Render.
    Priority:
      1) SIMPRO_JOB_IDS         -> exact list
      2) SIMPRO_JOB_RANGE       -> start-end (inclusive)
      3) SIMPRO_JOB_ID_HINTS +/- (SCAN_BEFORE/AFTER)
      (deduped and capped by SIMPRO_SCAN_MAX_CALLS)
    """
    # Exact list
    exact = _env("SIMPRO_JOB_IDS", "").strip()
    if exact:
        ids = []
        for chunk in exact.split(","):
            chunk = chunk.strip()
            if chunk.isdigit():
                ids.append(int(chunk))
        return sorted(set(ids))

    # Explicit range
    rng = _env("SIMPRO_JOB_RANGE", "").strip()
    if rng and "-" in rng:
        a, b = rng.split("-", 1)
        try:
            lo, hi = int(a), int(b)
            if lo > hi:
                lo, hi = hi, lo
            hi = min(hi, lo + 5000)  # safety
            ids = list(range(lo, hi + 1))
            return ids
        except ValueError:
            pass

    # Hints ± window
    hints_str = _env("SIMPRO_JOB_ID_HINTS", "1200,1250,1300,1350")
    hints = []
    for h in hints_str.split(","):
        h = h.strip()
        if h.isdigit():
            hints.append(int(h))

    before = int(_env("SIMPRO_SCAN_BEFORE", "250"))
    after  = int(_env("SIMPRO_SCAN_AFTER", "120"))
    bag: List[int] = []
    for h in hints:
        lo, hi = max(1, h - before), h + after
        bag.extend(range(lo, hi + 1))

    # Dedup and cap
    cap = int(_env("SIMPRO_SCAN_MAX_CALLS", "180"))
    deduped = sorted(set(bag))
    if len(deduped) > cap:
        # sample evenly across the deduped set
        step = max(1, len(deduped) // cap)
        deduped = deduped[::step][:cap]
    return deduped

def _write_snapshot(con: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    _ensure_schema(con)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO snapshots (snapshot_date, created_at) VALUES (?, ?)",
        (date.today().isoformat(), _now_iso()),
    )
    sid = cur.lastrowid

    cur.executemany(
        """INSERT INTO job_rows (
               snapshot_id, job_code, job_name, pm,
               hours_today, labour_cost_today, materials_cost_today, cost_today,
               actual_cost_to_date, estimated_cost, burn_pct, gm_to_date,
               invoiced_today, mtd_hours, days_since_update, at_risk
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                sid,
                r["job_code"], r["job_name"], r["pm"],
                r["hours_today"], r["labour_cost_today"], r["materials_cost_today"], r["cost_today"],
                r["actual_cost_to_date"], r["estimated_cost"], r["burn_pct"], r["gm_to_date"],
                r["invoiced_today"], r["mtd_hours"], r["days_since_update"], r["at_risk"],
            )
            for r in rows
        ],
    )
    con.commit()
    return sid

def ingest_live() -> Tuple[bool, str]:
    """
    Live ingest entry point used by the FastAPI route.
    Returns (ok, message).
    """
    cfg = SimproCfg(
        base_url=_env("SIMPRO_BASE_URL"),
        client_id=_env("SIMPRO_CLIENT_ID"),
        client_secret=_env("SIMPRO_CLIENT_SECRET"),
        company_id=int(_env("SIMPRO_COMPANY_ID", "0") or "0"),
        http_timeout=int(_env("SIMPRO_HTTP_TIMEOUT", "6") or "6"),
    )
    if not (cfg.base_url and cfg.client_id and cfg.client_secret):
        return False, "Missing SIMPRO_* environment vars"

    log.info("[ingest] Authenticating with Simpro")
    tok = _get_token(cfg)
    log.info("[ingest] Token acquired (len=%s)", len(tok))

    ids = _candidate_ids()
    budget_sec = float(_env("SIMPRO_INGEST_BUDGET_SEC", "24"))
    t0 = time.time()

    rows: List[Dict[str, Any]] = []
    tried = 0
    hits = 0

    for job_id in ids:
        if time.time() - t0 > budget_sec:
            log.warning("[ingest] time budget reached; stopping at id=%s", job_id)
            break
        tried += 1

        try:
            sc, job = _fetch_job_v1_0(cfg, tok, job_id)
        except requests.RequestException as ex:
            log.warning("[ingest] HTTP error for job %s: %s", job_id, ex)
            continue

        if sc != 200 or not isinstance(job, dict):
            continue

        if not _is_active_project(job):
            # Not an active project -> skip
            continue

        row = _parse_job(job)
        rows.append(row)
        hits += 1

    # Even if we found zero rows, still write a snapshot so the UI updates cleanly.
    dbp = _db_path()
    os.makedirs(os.path.dirname(os.path.abspath(dbp)), exist_ok=True)
    con = sqlite3.connect(dbp)
    try:
        sid = _write_snapshot(con, rows)
    finally:
        con.close()

    log.info("[ingest] ingest_live finished in %.2fs (ok=True, jobs=%s, tried=%s, snapshot_id=%s)",
             time.time() - t0, hits, tried, sid if hits or True else "-")
    return True, f"snapshot={sid}, jobs={hits}, tried={tried}"

# -----------------------------
# Demo (kept small so import doesn't break)
# -----------------------------

def ingest_demo() -> Tuple[bool, str]:
    """
    Minimal demo snapshot so the UI button still works.
    """
    demo_jobs = [
        {
            "job_code": "DEMO-001",
            "job_name": "Sample Project A",
            "pm": "",
            "hours_today": 0.0,
            "labour_cost_today": 0.0,
            "materials_cost_today": 0.0,
            "cost_today": 0.0,
            "actual_cost_to_date": 12500.0,
            "estimated_cost": 20000.0,
            "burn_pct": 12500.0 / 20000.0,
            "gm_to_date": 0.32,
            "invoiced_today": 0.0,
            "mtd_hours": 40.0,
            "days_since_update": 1,
            "at_risk": 0,
        },
        {
            "job_code": "DEMO-002",
            "job_name": "Sample Project B",
            "pm": "",
            "hours_today": 0.0,
            "labour_cost_today": 0.0,
            "materials_cost_today": 0.0,
            "cost_today": 0.0,
            "actual_cost_to_date": 18000.0,
            "estimated_cost": 22000.0,
            "burn_pct": 18000.0 / 22000.0,
            "gm_to_date": 0.18,
            "invoiced_today": 0.0,
            "mtd_hours": 55.0,
            "days_since_update": 2,
            "at_risk": 1,
        },
    ]

    dbp = _db_path()
    con = sqlite3.connect(dbp)
    try:
        sid = _write_snapshot(con, demo_jobs)
    finally:
        con.close()
    return True, f"demo snapshot={sid}, jobs={len(demo_jobs)}"

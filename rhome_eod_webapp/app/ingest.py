# app/ingest.py
import os, re, time, math, sqlite3, logging, datetime as dt
from typing import Any, Dict, Optional
from . import simpro

log = logging.getLogger("ingest")
if not logging.getLogger().handlers:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

DB_PATH = os.path.join(os.path.dirname(__file__), "eod.db")

def _get(d: Dict[str, Any], *path, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def _f(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _parse_iso(dt_str: Optional[str]) -> Optional[dt.datetime]:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def _ensure_schema(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
          id INTEGER PRIMARY KEY,
          snapshot_date TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_rows (
          id INTEGER PRIMARY KEY,
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
          at_risk INTEGER
        )
    """)
    # Compatibility views the UI expects
    cur.execute("CREATE VIEW IF NOT EXISTS snapshot AS SELECT id, snapshot_date AS as_of, created_at FROM snapshots")
    cur.execute("CREATE VIEW IF NOT EXISTS job AS SELECT * FROM job_rows")
    con.commit()

def _max_seen_job_id(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    cur.execute("SELECT MAX(CASE WHEN job_code GLOB '[0-9]*' THEN CAST(job_code AS INTEGER) ELSE NULL END) FROM job_rows")
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def _insert_snapshot(con: sqlite3.Connection) -> int:
    now = dt.datetime.utcnow()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO snapshots (snapshot_date, created_at) VALUES (?, ?)",
        (now.date().isoformat(), now.replace(microsecond=0).isoformat()+"Z"),
    )
    con.commit()
    return cur.lastrowid

def _status_ok(job: Dict[str, Any], rx: re.Pattern) -> bool:
    status = _get(job, "Status", "Name") or _get(job, "Stage", "Name") or ""
    return bool(rx.search(status))

def _row_from_job(job: Dict[str, Any], snapshot_id: int) -> Dict[str, Any]:
    # Safe fallbacks based on examples you shared
    # Names
    job_id = _get(job, "ID")
    job_code = str(job_id) if job_id is not None else (_get(job, "JobNo") or "")
    job_name = (_get(job, "Customer", "CompanyName")
                or _get(job, "Site", "Name")
                or _get(job, "Description")
                or f"Job {job_code}")

    # Costs & margins (best-effort: field names differ a bit across tenants)
    ex_tax = _f(_get(job, "Total", "ExTax") or _get(job, "Totals", "Estimate", "ExTax"))
    actual = _f(_get(job, "Totals", "Actual", "ExTax") or _get(job, "Total", "Actual") or _get(job, "Total", "Cost"))
    estimated = ex_tax if ex_tax > 0 else _f(_get(job, "Totals", "Estimate", "IncTax"))

    burn_pct = (actual / estimated) if estimated > 0 else None
    gm_to_date = ((estimated - actual) / estimated) if estimated > 0 else None

    # “today” fields require separate timesheet/material endpoints.
    # Until we wire those up, keep 0 so the tiles don’t error.
    hours_today = 0.0
    labour_today = 0.0
    materials_today = 0.0
    cost_today = 0.0
    invoiced_today = 0.0
    mtd_hours = 0.0

    updated = _parse_iso(_get(job, "UpdatedOn") or _get(job, "LastUpdated"))
    days_since_update = None
    if updated:
        days_since_update = max(0, (dt.datetime.utcnow().replace(tzinfo=None) - updated.replace(tzinfo=None)).days)

    at_risk = 1 if (gm_to_date is not None and gm_to_date < 0) or (burn_pct is not None and burn_pct > 1.05) else 0

    return dict(
        snapshot_id=snapshot_id,
        job_code=job_code,
        job_name=job_name,
        pm=None,
        hours_today=hours_today,
        labour_cost_today=labour_today,
        materials_cost_today=materials_today,
        cost_today=cost_today,
        actual_cost_to_date=actual,
        estimated_cost=estimated,
        burn_pct=burn_pct if burn_pct is not None else None,
        gm_to_date=gm_to_date if gm_to_date is not None else None,
        invoiced_today=invoiced_today,
        mtd_hours=mtd_hours,
        days_since_update=days_since_update,
        at_risk=at_risk,
    )

def ingest_live(base_url: str, client_id: str, client_secret: str, company_id: Optional[int] = None):
    """
    Scans job IDs forward from the largest one we've already stored,
    stops after a run of 404s, filters by status ("Pending" or "In Progress"),
    and writes rows into snapshots/job_rows for the latest snapshot.
    """
    t0 = time.time()

    # ---- config via env (all optional except company id) ----
    company_id = int(company_id if company_id is not None else os.environ.get("SIMPRO_COMPANY_ID", "0"))
    allowed_status_rx = re.compile(os.environ.get("SIMPRO_ALLOWED_STATUS", r"(?i)\b(pending|in progress|progress)\b"))
    scan_ahead = int(os.environ.get("SIMPRO_SCAN_AHEAD", "800"))           # how many IDs to try past last seen
    stop_after_misses = int(os.environ.get("SIMPRO_STOP_AFTER_MISSES", "200"))  # consecutive 404s before early-stop
    seed_start = os.environ.get("SIMPRO_SEED_START_ID")                    # optional first-run speed-up

    # ---- auth & client ----
    token = simpro.get_token(base_url, client_id, client_secret)
    log.info("Authenticated with Simpro")
    client = simpro.Client(base_url, token)

    # ---- db + snapshot ----
    con = sqlite3.connect(DB_PATH)
    _ensure_schema(con)
    snap_id = _insert_snapshot(con)

    cur = con.cursor()
    # Figure out where to start scanning
    last_seen = _max_seen_job_id(con)
    start_id = last_seen + 1 if last_seen > 0 else (int(seed_start) if seed_start else 1)
    end_id = start_id + scan_ahead - 1

    found = 0
    kept = 0
    misses = 0

    # Try ascending job IDs with early stop on long 404 runs
    for job_id in range(start_id, end_id + 1):
        try:
            j = client.get_job(company_id, job_id)
        except Exception as e:
            # non-404 errors: log & keep going (transient or permission issues)
            log.warning("job %s: error %s", job_id, e)
            continue

        if j is None:
            misses += 1
            if misses >= stop_after_misses:
                log.info("Stopping scan at %s after %s consecutive 404s", job_id, misses)
                break
            continue

        # reset miss streak on a hit
        misses = 0
        found += 1

        # Filter by status
        if not _status_ok(j, allowed_status_rx):
            continue

        kept += 1
        row = _row_from_job(j, snap_id)
        cur.execute("""
            INSERT INTO job_rows (
              snapshot_id, job_code, job_name, pm,
              hours_today, labour_cost_today, materials_cost_today, cost_today,
              actual_cost_to_date, estimated_cost, burn_pct, gm_to_date,
              invoiced_today, mtd_hours, days_since_update, at_risk
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            row["snapshot_id"], row["job_code"], row["job_name"], row["pm"],
            row["hours_today"], row["labour_cost_today"], row["materials_cost_today"], row["cost_today"],
            row["actual_cost_to_date"], row["estimated_cost"], row["burn_pct"], row["gm_to_date"],
            row["invoiced_today"], row["mtd_hours"], row["days_since_update"], row["at_risk"],
        ))

    con.commit()
    con.close()

    dt_s = time.time() - t0
    ok = kept >= 0  # always true; we didn’t crash
    log.info("ingest_live finished in %.3fs (hits=%s, active=%s, ok=%s)", dt_s, found, kept, ok)
    return {"ok": ok, "hits": found, "active_kept": kept, "snapshot_id": snap_id}

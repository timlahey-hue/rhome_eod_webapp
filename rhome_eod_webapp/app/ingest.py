# app/ingest.py
import os, time, logging, sqlite3, datetime as dt, math
from typing import Dict, Any, Iterable, List, Optional, Tuple
import requests

LOG = logging.getLogger("ingest")
LOG.setLevel(logging.INFO)

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "eod.db"))

# -------- DB helpers --------

SCHEMA_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS snapshots (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_date DATE    NOT NULL,
  created_at    TEXT    NOT NULL
);
"""

SCHEMA_JOB_ROWS = """
CREATE TABLE IF NOT EXISTS job_rows (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id            INTEGER NOT NULL,
  job_code               TEXT,
  job_name               TEXT,
  pm                     TEXT,
  hours_today            REAL,
  labour_cost_today      REAL,
  materials_cost_today   REAL,
  cost_today             REAL,
  actual_cost_to_date    REAL,
  estimated_cost         REAL,
  burn_pct               REAL,
  gm_to_date             REAL,
  invoiced_today         REAL,
  mtd_hours              REAL,
  days_since_update      INTEGER,
  at_risk                INTEGER
);
"""

VIEWS = """
-- Keep the UI happy regardless of table names
DROP VIEW IF EXISTS snapshot;
CREATE VIEW snapshot AS
  SELECT id, snapshot_date AS as_of, created_at FROM snapshots;

DROP VIEW IF EXISTS job;
CREATE VIEW job AS
  SELECT * FROM job_rows;
"""

def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    return con

def _ensure_db() -> None:
    con = _connect()
    with con:
        con.executescript(SCHEMA_SNAPSHOTS)
        con.executescript(SCHEMA_JOB_ROWS)
        con.executescript(VIEWS)
    con.close()

def _new_snapshot(con: sqlite3.Connection) -> int:
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat()
    today = dt.date.today().isoformat()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO snapshots (snapshot_date, created_at) VALUES (?, ?)",
        (today, now),
    )
    return cur.lastrowid

def _insert_jobs(con: sqlite3.Connection, snapshot_id: int, rows: List[Tuple]) -> int:
    if not rows:
        return 0
    cur = con.cursor()
    cur.executemany(
        """
        INSERT INTO job_rows (
            snapshot_id, job_code, job_name, pm,
            hours_today, labour_cost_today, materials_cost_today, cost_today,
            actual_cost_to_date, estimated_cost, burn_pct, gm_to_date,
            invoiced_today, mtd_hours, days_since_update, at_risk
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    return cur.rowcount or 0

def _max_numeric_job_code(con: sqlite3.Connection) -> Optional[int]:
    # read the highest numeric job_code we've ever stored
    cur = con.cursor()
    cur.execute("""
        SELECT MAX(CAST(job_code AS INTEGER))
        FROM job_rows
        WHERE job_code GLOB '[0-9]*'
    """)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None

# -------- Simpro helpers --------

def _oauth_token(base: str, client_id: str, client_secret: str, timeout: int = 15) -> str:
    url = f"{base.rstrip('/')}/oauth2/token"
    r = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    tok = r.json().get("access_token", "")
    if not tok:
        raise RuntimeError("Simpro token response missing access_token")
    return tok

def _get_job_detail(base: str, token: str, company_id: int, job_id: int, timeout: int = 15) -> Tuple[int, Optional[Dict[str, Any]]]:
    # Only this endpoint family works in your tenant
    url = f"{base.rstrip('/')}/api/v1.0/companies/{company_id}/jobs/{job_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code == 200:
        return 200, r.json()
    return r.status_code, None

def _looks_active(job_json: Dict[str, Any]) -> bool:
    # Favor Status.Name if present; fall back to Stage.Name
    status = ""
    s = job_json.get("Status") or {}
    if isinstance(s, dict):
        status = (s.get("Name") or "").strip()
    if not status:
        st = job_json.get("Stage") or {}
        if isinstance(st, dict):
            status = (st.get("Name") or "").strip()

    status_l = status.lower()
    if any(w in status_l for w in ("complete", "completed", "invoiced", "cancelled", "void")):
        return False
    # User preference: keep Pending or Progress
    return ("pending" in status_l) or ("progress" in status_l)

def _row_from_job(snapshot_id: int, j: Dict[str, Any]) -> Tuple:
    job_id = j.get("ID")
    name = j.get("Name") or (j.get("Site") or {}).get("Name") or (j.get("Customer") or {}).get("CompanyName") or f"Job {job_id}"
    pm = (j.get("ProjectManager") or {}).get("Name") or ""

    # Totals / margins are very tenant-specific; be defensive.
    total_ex_tax = 0.0
    try:
        total_ex_tax = float(((j.get("Total") or {}).get("ExTax")) or 0)
    except Exception:
        total_ex_tax = 0.0

    gm = 0.0
    try:
        gm = float((((j.get("Totals") or {}).get("NettMargin") or {}).get("Actual")) or 0)
    except Exception:
        gm = 0.0

    # We don't have day-level movements without other endpoints — fill 0s.
    return (
        snapshot_id,
        str(job_id),                       # job_code
        name,                              # job_name
        pm,                                # pm
        0.0, 0.0, 0.0, 0.0,                # hours_today..cost_today
        total_ex_tax,                      # actual_cost_to_date (best available proxy)
        total_ex_tax,                      # estimated_cost (same proxy)
        0.0,                               # burn_pct
        gm,                                # gm_to_date
        0.0,                               # invoiced_today
        0.0,                               # mtd_hours
        0,                                 # days_since_update
        1 if gm < 0 else 0,                # at_risk (very conservative)
    )

# -------- Public entry points used by main.py --------

def ingest_demo(*_args, **_kwargs) -> Dict[str, Any]:
    """
    Keep this around so main.py can import it.
    Seeds some rows so the UI works even without Simpro.
    """
    t0 = time.time()
    _ensure_db()
    con = _connect()
    with con:
        sid = _new_snapshot(con)
        rows: List[Tuple] = []
        for i in range(10):
            rows.append((
                sid, f"{1000+i}", f"Demo Job {i+1}", "PM Demo",
                1.5, 200.0, 120.0, 320.0,
                15000.0 + i*500, 30000.0, 0.5, 0.25 + i*0.02,
                0.0, 12.0, i % 4, 0
            ))
        inserted = _insert_jobs(con, sid, rows)
    con.close()
    LOG.info("ingest_demo finished in %.3fs (inserted=%s)", time.time()-t0, inserted)
    return {"ok": True, "inserted": inserted, "snapshot_id": sid}

def ingest_live(base_url: str, client_id: str, client_secret: str, company_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Live ingest that works on tenants where listing endpoints are disabled.
    Strategy:
      1) OAuth2 with client credentials.
      2) Probe job IDs around the last seen ID (rolling window).
      3) Keep only active jobs (Pending / Progress).
      4) Store in snapshots + job_rows.
    """
    t0 = time.time()
    _ensure_db()

    base = (base_url or "").rstrip("/")
    cid = int(company_id) if (company_id is not None and str(company_id).isdigit()) else int(os.getenv("SIMPRO_COMPANY_ID", "0") or 0)

    timeout = int(os.getenv("SIMPRO_HTTP_TIMEOUT", "15"))
    scan_back = int(os.getenv("SIMPRO_SCAN_BACK", "300"))      # how far back from last seen id
    scan_forward = int(os.getenv("SIMPRO_SCAN_FWD", "150"))    # how far forward to look for new
    hard_cap = int(os.getenv("SIMPRO_SCAN_CAP", "500"))        # total ids per run hard cap
    seed = os.getenv("SIMPRO_DISCOVER_SEED")                   # optional one-time bootstrap

    ok = False
    non_404 = 0
    kept = 0
    inserted = 0
    sid: Optional[int] = None

    try:
        token = _oauth_token(base, client_id, client_secret, timeout=timeout)
        LOG.info("Authenticated with Simpro")
    except Exception as e:
        LOG.error("Simpro auth failed: %s", e)
        return {"ok": False, "error": str(e)}

    # Decide scan window
    con = _connect()
    try:
        last_seen = _max_numeric_job_code(con)
        if last_seen is None:
            if seed and str(seed).isdigit():
                last_seen = int(seed)
            else:
                # mild default if we've never seen anything
                last_seen = 1000

        start = max(1, last_seen - scan_back)
        end   = last_seen + scan_forward

        # Build ID list and cap it so we don't hammer the API
        ids: List[int] = list(range(start, end + 1))
        if len(ids) > hard_cap:
            # sample evenly to respect the cap
            step = max(1, math.floor(len(ids) / hard_cap))
            ids = ids[::step][:hard_cap]

        LOG.info("Scanning job ids company=%s range=%s..%s (count=%s)", cid, start, end, len(ids))

        # Probe
        hits: List[Dict[str, Any]] = []
        checked = 0
        for i in ids:
            status, data = _get_job_detail(base, token, cid, i, timeout=timeout)
            checked += 1
            if status == 200 and data:
                non_404 += 1
                if _looks_active(data):
                    hits.append(data)
            # light progress log each ~100 checks
            if checked % 100 == 0:
                LOG.info("...checked %d (200s=%d, kept=%d)", checked, non_404, len(hits))

        kept = len(hits)

        # Write DB
        with con:
            sid = _new_snapshot(con)
            rows = [_row_from_job(sid, j) for j in hits]
            inserted = _insert_jobs(con, sid, rows)

        ok = True
        return {"ok": ok, "snapshot_id": sid, "checked": checked, "hits": non_404, "kept": kept, "inserted": inserted}
    except Exception as e:
        LOG.exception("ingest_live error: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        con.close()
        LOG.info("ingest_live finished in %.3fs (ok=%s, kept=%s, inserted=%s)", time.time()-t0, ok, kept, inserted)

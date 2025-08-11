import os
import time
import logging
import sqlite3
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ------------------------------------------------------------------------------
# App & logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

app = FastAPI()

# If you serve static files (optional; remove if you don't use /static)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# ------------------------------------------------------------------------------
# Jinja helpers (fixes: UndefinedError: 'fmt_currency' / 'fmt_pct')
# ------------------------------------------------------------------------------
def fmt_currency(value: Optional[float]) -> str:
    if value is None:
        return "$0"
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return "$0"
    # No decimals to match prior look; change to :,.2f if you prefer cents
    return f"${d:,.0f}"

def fmt_int(value: Optional[float]) -> str:
    try:
        return f"{int(round(float(value or 0))):,}"
    except (ValueError, TypeError):
        return "0"

def fmt_pct(value: Optional[float]) -> str:
    if value is None:
        return "0.0%"
    try:
        f = float(value)
    except (ValueError, TypeError):
        return "0.0%"
    # If it's 0-1, assume ratio; if >1 assume already percent
    pct = f * 100.0 if -1.0 <= f <= 1.0 else f
    return f"{pct:.1f}%"

# make these available in templates
templates.env.globals.update(
    fmt_currency=fmt_currency,
    fmt_int=fmt_int,
    fmt_pct=fmt_pct,
)

# ------------------------------------------------------------------------------
# DB helpers (defensive: don't crash if table missing)
# ------------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "eod.db")

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_totals() -> Dict[str, Any]:
    """
    Returns a dict of totals; if the 'totals' table is missing or empty,
    return an empty dict and log, but DO NOT raise (prevents 500s).
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        # Check table existence
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='totals'")
        if not cur.fetchone():
            log.error("get_totals(): 'totals' table not found; returning empty dict")
            return {}
        # Try a few common shapes (row-per-key or single-latest row)
        # 1) row-per-key: key TEXT, value NUMERIC
        try:
            cur.execute("SELECT key, value FROM totals")
            rows = cur.fetchall()
            if rows and "key" in rows[0].keys():
                return {r["key"]: r["value"] for r in rows}
        except sqlite3.OperationalError:
            pass

        # 2) latest snapshot row: assume columns include the metrics
        #    (change columns to match your schema)
        snapshot_cols = [
            "labour_cost_today",
            "mtd_gm_pct",
            "mtd_gm",
            "mtd_revenue",
            "mtd_labour",
            "mtd_materials",
            "mtd_other",
        ]
        cols_csv = ", ".join(snapshot_cols)
        try:
            cur.execute(f"SELECT {cols_csv} FROM totals ORDER BY rowid DESC LIMIT 1")
            row = cur.fetchone()
            return dict(row) if row else {}
        except sqlite3.OperationalError:
            log.warning("get_totals(): 'totals' exists but schema unknown; returning {}")
            return {}
    except Exception as e:
        log.exception("get_totals(): unexpected error; returning {}")
        return {}

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/")
def home(request: Request):
    totals = get_totals()
    # Never let missing helpers or keys 500 the page again
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "totals": totals, "now": int(time.time())},
    )

# --- ingest -------------------------------------------------------------------
try:
    # local package import (this file sits in app/, alongside ingest.py)
    from .ingest import ingest_live
except Exception as e:  # pragma: no cover
    ingest_live = None
    log.error("ingest_live import failed; POST /ingest/live will return a stub")

@app.post("/ingest/live")
def ingest_live_route():
    """
    POST /ingest/live
    Always returns 200 with a JSON body describing what happened.
    """
    if ingest_live is None:
        log.error("ingest_live not available")
        return JSONResponse({"ok": False, "error": "ingest_live not available", "elapsed_sec": 0.0})

    started = time.time()
    try:
        result = ingest_live()
    except Exception as e:
        log.exception("ingest_live crashed")
        result = {"ok": False, "error": "ingest_live crashed", "note": str(e)}
    result.setdefault("elapsed_sec", round(time.time() - started, 3))
    return JSONResponse(result)

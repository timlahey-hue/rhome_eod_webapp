import os
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# -------- logging --------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("app")

# -------- FastAPI / Jinja setup --------
app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

def fmt_currency(x):
    try:
        if x is None:
            return "$0"
        return f"${float(x):,.0f}"
    except Exception:
        return "$0"

def fmt_pct(x):
    try:
        if x is None:
            return "0%"
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "0%"

def fmt_int(x):
    try:
        if x is None:
            return "0"
        return f"{int(round(float(x))):,}"
    except Exception:
        return "0"

# Make helpers available as globals in Jinja (so {{ fmt_currency(...) }} works)
templates.env.globals.update(
    fmt_currency=fmt_currency,
    fmt_pct=fmt_pct,
    fmt_int=fmt_int,
)

DB_PATH = os.environ.get("EOD_DB_PATH", "eod.db")

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    )
    return cur.fetchone() is not None

def get_totals() -> Dict[str, Any]:
    """Return the latest row from the `totals` table, or {} if the table is missing/empty."""
    try:
        conn = _get_conn()
        with conn:
            if not _table_exists(conn, "totals"):
                log.error("get_totals(): 'totals' table not found; returning empty dict")
                return {}
            row = conn.execute(
                "SELECT * FROM totals ORDER BY COALESCE(updated_at, '') DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else {}
    except Exception:
        log.exception("get_totals() failed; returning empty totals")
        return {}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def home(request: Request):
    totals = get_totals()  # Safe: returns {} if table missing/empty
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals,
            "now": datetime.utcnow().strftime("%Y-%m-%d"),
            # also pass helpers directly (belt & suspenders)
            "fmt_currency": fmt_currency,
            "fmt_pct": fmt_pct,
            "fmt_int": fmt_int,
        },
    )

# -------- Ingest Endpoint --------
from app import ingest  # keep this import here so the module loads after env is ready

@app.post("/ingest/live")
def run_live():
    try:
        res = ingest.ingest_live(budget_sec=25)  # <-- signature must accept budget_sec
        if isinstance(res, tuple):
            # Normalize any legacy tuple to a dict
            if res and isinstance(res[0], dict):
                res = res[0]
            else:
                res = {"ok": False, "error": "ingest_live returned unexpected type"}
        if not isinstance(res, dict):
            res = {"ok": False, "error": "ingest_live returned non-dict response"}
        return JSONResponse(res, status_code=(200 if res.get("ok") else 500))
    except Exception as e:
        log.exception("ingest/live failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

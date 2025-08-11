import os
import sqlite3
import logging
import inspect
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# IMPORTANT: relative import so we definitely load *our* ingest module
from . import ingest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI()

# Static & templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# ---- jinja helpers both as filters AND as context callables ----
def fmt_currency(v) -> str:
    try:
        return f"${float(v):,.0f}"
    except Exception:
        return "$0"

def fmt_pct(v) -> str:
    try:
        return f"{float(v) * 100:.1f}%"
    except Exception:
        return "0.0%"

templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_pct"] = fmt_pct

DB_PATH = os.getenv("DB_PATH", "eod.db")

def get_totals() -> Dict[str, Any]:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT
                labour_cost_today,
                hours_today,
                mtd_gm_pct
            FROM totals
            ORDER BY id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        conn.close()
        if not row:
            return {}
        return dict(row)
    except sqlite3.OperationalError as e:
        # 'no such table: totals' etc.
        logger.error("app:get_totals(): 'totals' table not found; returning empty dict")
        return {}
    except Exception as e:
        logger.exception("app:get_totals() failed; returning empty totals")
        return {}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def home(request: Request):
    totals = get_totals()
    # Pass the callables into the context since your templates call fmt_* like functions
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals or {},
            "fmt_currency": fmt_currency,
            "fmt_pct": fmt_pct,
        },
    )

@app.post("/ingest/live")
def run_live():
    """
    Call the ingest layer. Returns JSON no matter what.
    Backwards compatible with either ingest_live(budget_sec=) or ingest_live(budget=).
    Also normalizes odd return types.
    """
    try:
        # Prefer the new keyword
        res = ingest.ingest_live(budget_sec=25)
    except TypeError:
        # Older module that only accepts 'budget'
        res = ingest.ingest_live(budget=25)
    except Exception as e:
        logger.exception("app:ingest/live failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    # If some old version returns a tuple, coerce to dict
    if isinstance(res, tuple):
        res = res[0] if res and isinstance(res[0], dict) else {"ok": False, "error": "Unexpected tuple from ingest"}

    if not isinstance(res, dict):
        res = {"ok": False, "error": f"Unexpected return type from ingest: {type(res).__name__}"}

    status = 200 if res.get("ok") else 502
    return JSONResponse(res, status_code=status)

# Log exactly which ingest module & signature we loaded, to end the “which version is running?” confusion.
logger.info("Using ingest module: %s", getattr(ingest, "__file__", "unknown"))
try:
    logger.info("ingest.ingest_live signature: %s", inspect.signature(ingest.ingest_live))
except Exception:
    pass

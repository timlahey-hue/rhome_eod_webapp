import os
import sqlite3
import time
import logging
from typing import Dict, Any
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("app")

app = FastAPI()

BASE_DIR = os.path.dirname(__file__)
PROJECT_DIR = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_DIR, "eod.db")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# ---- Jinja filters so templates never crash ----
def fmt_currency(value) -> str:
    try:
        v = float(value or 0)
        return f"${v:,.2f}"
    except Exception:
        return "$0.00"

def fmt_pct(value) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
        # Accept 0.32 or 32; normalize
        if v > 1:
            v = v / 100.0
        return f"{v * 100:.1f}%"
    except Exception:
        return "—"

def fmt_num(value) -> str:
    try:
        return f"{float(value):,.0f}"
    except Exception:
        return "0"

templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_pct"] = fmt_pct
templates.env.filters["fmt_num"] = fmt_num
# ------------------------------------------------

# Mount /static if present (no problem if it isn't)
static_dir = os.path.join(PROJECT_DIR, "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

def get_totals() -> Dict[str, Any]:
    """Read simple key/value pairs from 'totals' table if it exists."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='totals'")
        if not cur.fetchone():
            log.error("get_totals(): 'totals' table not found; returning empty dict")
            return {}
        cur.execute("SELECT key, value FROM totals")
        rows = cur.fetchall()
        return {k: v for k, v in rows}
    except Exception as e:
        log.exception("get_totals() failed: %s", e)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    totals = get_totals()
    ctx = {"request": request, "totals": totals, "now": int(time.time())}
    return templates.TemplateResponse("index.html", ctx)

@app.get("/health")
def health():
    return {"ok": True, "time": int(time.time())}

@app.post("/ingest/live")
def ingest_live():
    """
    Runs a short ingest/probe. Always returns JSON;
    never raises, even if Simpro endpoints are 404.
    """
    try:
        from .ingest import run_live_ingest
    except Exception as e:
        log.error("ingest_live import failure: %s", e)
        return {"ok": False, "error": "ingest_import_error", "detail": str(e)}

    try:
        result = run_live_ingest(budget_seconds=25)
        return result
    except Exception as e:
        log.exception("ingest_live crashed: %s", e)
        return {"ok": False, "error": "ingest_runtime_exception", "detail": str(e)}

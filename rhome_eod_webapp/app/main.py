# rhome_eod_webapp/app/main.py
import os
import time
import sqlite3
import logging
import inspect
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("app")

app = FastAPI()

BASE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

# --- Static & Templates -------------------------------------------------------
static_dir = os.path.join(BASE_DIR, "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

def fmt_currency(value):
    try:
        if value is None:
            return "-"
        return "${:,.2f}".format(float(value))
    except Exception:
        return str(value)

def fmt_pct(value):
    try:
        if value is None:
            return "-"
        v = float(value)
        # If it looks like 0.123 treat as 12.3%; if 12.3 treat as 12.3%
        return "{:,.1f}%".format(v * 100.0 if abs(v) <= 1.0 else v)
    except Exception:
        return str(value)

def fmt_int(value):
    try:
        if value is None:
            return "0"
        return "{:,}".format(int(round(float(value))))
    except Exception:
        return str(value)

# Make these available to all templates
templates.env.globals.update(
    fmt_currency=fmt_currency, fmt_pct=fmt_pct, fmt_int=fmt_int
)
templates.env.filters["currency"] = fmt_currency
templates.env.filters["pct"] = fmt_pct
templates.env.filters["intfmt"] = fmt_int

# --- DB helpers ---------------------------------------------------------------
DB_PATH = os.environ.get("EOD_DB_PATH", os.path.join(ROOT_DIR, "eod.db"))

def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_totals():
    try:
        with _get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM totals ORDER BY snapshot_ts DESC LIMIT 1")
            row = cur.fetchone()
            return dict(row) if row else {}
    except Exception:
        # Don't explode the homepage if the table isn't present yet
        logger.error("get_totals(): 'totals' table not found; returning empty dict")
        return {}

# --- Routes -------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/")
def home(request: Request):
    totals = get_totals()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals,
            "now": datetime.now(),
            # also pass directly in case a template references them from context
            "fmt_currency": fmt_currency,
            "fmt_pct": fmt_pct,
            "fmt_int": fmt_int,
        },
    )

# Try to import the ingest module if present
try:
    from . import ingest  # your existing ingest module
except Exception:
    ingest = None

@app.post("/ingest/live")
def run_live():
    started = time.time()

    if ingest is None or not hasattr(ingest, "ingest_live"):
        logger.error("ingest_live not available")
        return JSONResponse(
            {"ok": False, "error": "ingest_live not available", "elapsed_sec": 0.0},
            status_code=200,
        )

    try:
        fn = ingest.ingest_live
        # Call signature-compatible: with budget_sec if supported, else without
        if "budget_sec" in inspect.signature(fn).parameters:
            res = fn(budget_sec=int(os.getenv("INGEST_BUDGET_SEC", "25")))
        else:
            res = fn()

        elapsed = round(time.time() - started, 3)
        if isinstance(res, dict):
            res.setdefault("elapsed_sec", elapsed)
            res.setdefault("ok", res.get("ok", True))
            return JSONResponse(res, status_code=200)
        else:
            return JSONResponse({"ok": True, "result": str(res), "elapsed_sec": elapsed}, status_code=200)
    except Exception as e:
        logger.exception("ingest/live failed")
        return JSONResponse({"ok": False, "error": str(e), "elapsed_sec": round(time.time() - started, 3)}, status_code=200)

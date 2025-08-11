import os
import sqlite3
import logging
from typing import Any, Dict, Tuple, Union

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------------------------------
# Logging
# ---------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# ---------------------------------
# App & Templates
# ---------------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

templates = Jinja2Templates(directory="app/templates")

# ---------------------------------
# Jinja helpers (currency & percent)
# ---------------------------------
def fmt_currency(value: Any) -> str:
    try:
        v = float(value or 0)
        # No decimals for dollars; change to :.2f if you want cents
        return f"${v:,.0f}"
    except (TypeError, ValueError):
        return "$0"

def fmt_pct(value: Any) -> str:
    try:
        v = float(value)
        # Accepts 0.1234 or 12.34; if value <= 1 we assume it's a ratio
        if abs(v) <= 1:
            v *= 100.0
        return f"{v:.1f}%"
    except (TypeError, ValueError):
        return "0.0%"

# Register as both filters and globals (so Jinja can call them either way)
templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_pct"] = fmt_pct
templates.env.globals.update(fmt_currency=fmt_currency, fmt_pct=fmt_pct)

# ---------------------------------
# DB helpers (very defensive)
# ---------------------------------
DB_PATH = os.getenv("EOD_DB_PATH", "eod.db")

def _connect_db():
    if not os.path.exists(DB_PATH):
        return None
    try:
        return sqlite3.connect(DB_PATH)
    except Exception:
        log.exception("Failed to open DB at %s", DB_PATH)
        return None

def get_totals() -> Dict[str, Any]:
    """
    Return a dict of totals used by the dashboard.
    This is deliberately defensive: if the DB or rows aren't there,
    we just return an empty dict so templates can .get() safely.
    """
    conn = _connect_db()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        # Adapt this query to your schema as needed. If you already had a working query, keep it.
        # Here we try to read a single-row 'totals' view/table if it exists.
        cur.execute("""
            SELECT
                COALESCE(hours_today, 0),
                COALESCE(labour_cost_today, 0),
                COALESCE(material_cost_today, 0),
                COALESCE(revenue_today, 0),
                COALESCE(mtd_gm_pct, 0)
            FROM totals
            LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            return {}
        return {
            "hours_today": row[0],
            "labour_cost_today": row[1],
            "material_cost_today": row[2],
            "revenue_today": row[3],
            "mtd_gm_pct": row[4],
        }
    except Exception:
        # If the table/view doesn't exist or anything else is wrong, don't blow up the page
        log.exception("get_totals() failed; returning empty totals")
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ---------------------------------
# Routes
# ---------------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    totals = get_totals()  # empty dict if DB missing
    # Always pass fmt_* too (belt & suspenders)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals,
            "fmt_currency": fmt_currency,
            "fmt_pct": fmt_pct,
        },
    )

# ---------------------------------
# Ingest/live
# ---------------------------------
# If your ingest code lives at app/ingest.py with def ingest_live(...):
try:
    import app.ingest as ingest
except Exception:
    ingest = None
    log.warning("app.ingest not importable; /ingest/live will return 501")

def _normalize_ingest_result(raw: Union[Dict[str, Any], Tuple[Any, ...], Any]) -> Tuple[Dict[str, Any], int]:
    """
    Accept either:
      - dict like {"ok": bool, ...}
      - (dict, status_code)
      - anything else -> convert to an error dict
    Returns (dict, http_status)
    """
    if isinstance(raw, tuple):
        # Common patterns: (dict, status), (ok, note) etc.
        if len(raw) == 2 and isinstance(raw[0], dict) and isinstance(raw[1], int):
            body, status = raw
            return body, status
        # Fallback for unexpected tuple shapes
        return {"ok": False, "error": "Unexpected tuple from ingest", "raw": str(raw)}, 500

    if isinstance(raw, dict):
        # If 'ok' is False, use 500; otherwise 200
        return raw, 200 if raw.get("ok") else 500

    # Anything else is an error
    return {"ok": False, "error": "Unexpected return from ingest", "raw": str(raw)}, 500

@app.post("/ingest/live")
def run_live():
    if ingest is None or not hasattr(ingest, "ingest_live"):
        return JSONResponse({"ok": False, "error": "ingest not available"}, status_code=501)
    try:
        raw = ingest.ingest_live(budget_sec=25)
        body, status = _normalize_ingest_result(raw)
        # If the ingest happens to include its own preferred status, respect it;
        # otherwise, fall back to 500 for ok=False and 200 for ok=True
        status = status if status else (200 if body.get("ok") else 500)
        return JSONResponse(body, status_code=status)
    except Exception as e:
        log.exception("ingest/live failed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

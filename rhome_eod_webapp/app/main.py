# app/main.py
import os
import logging
import sqlite3
from typing import Any, Dict, Tuple, Union

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

# If your ingest module lives at app/ingest.py this import will work with:
#   uvicorn app.main:app
from app import ingest

# -----------------------------------------------------------------------------
# App & logging
# -----------------------------------------------------------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")

logger = logging.getLogger("rhome")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

templates = Jinja2Templates(directory="app/templates")

# -----------------------------------------------------------------------------
# Jinja helpers (register as globals so templates can call them)
# -----------------------------------------------------------------------------
def fmt_currency(val: Any) -> str:
    try:
        return f"${float(val):,.2f}"
    except Exception:
        return "$0.00"

def fmt_percent(val: Any) -> str:
    try:
        return f"{float(val)*100:.1f}%"
    except Exception:
        return "0.0%"

def fmt_int(val: Any) -> str:
    try:
        return f"{int(float(val)):,}"
    except Exception:
        return "0"

templates.env.globals.update(
    fmt_currency=fmt_currency,
    fmt_percent=fmt_percent,
    fmt_int=fmt_int,
)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _default_totals() -> Dict[str, Any]:
    """Always return a dict so the template never gets 'totals' undefined."""
    return {
        "hours_today": 0.0,
        "labour_cost_today": 0.0,
        "material_cost_today": 0.0,
        "jobs_completed_today": 0,
        "revenue_today": 0.0,
    }

def _load_totals_from_db(db_path: str = "eod.db") -> Dict[str, Any]:
    """
    Best-effort: try to read a row of totals from your DB if present.
    If table/view names differ, this quietly falls back to defaults.
    """
    totals = _default_totals()
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # Try a few likely objects; ignore errors if they don't exist.
        tried_queries = [
            "SELECT * FROM dashboard_totals ORDER BY as_of DESC LIMIT 1",
            "SELECT * FROM totals ORDER BY as_of DESC LIMIT 1",
            "SELECT * FROM v_totals ORDER BY as_of DESC LIMIT 1",
        ]
        row = None
        for q in tried_queries:
            try:
                row = cur.execute(q).fetchone()
                if row:
                    break
            except sqlite3.Error:
                continue
        if row:
            for k in row.keys():
                totals[k] = row[k]
    except sqlite3.Error:
        # no DB / no table – that's fine; just use defaults
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return totals

def _normalize_ingest_response(res: Union[Dict[str, Any], Tuple[Any, int], None]) -> Tuple[Dict[str, Any], int]:
    """
    Accept either:
      - dict payload (expects payload['ok'])
      - (payload, status_code) tuple
      - None / unexpected -> convert to error
    """
    if isinstance(res, tuple):
        payload = res[0] if len(res) > 0 else {}
        status = res[1] if len(res) > 1 and isinstance(res[1], int) else 500
    else:
        payload = res if isinstance(res, dict) else {}
        status = 200 if payload.get("ok") else 500
    if not isinstance(payload, dict):
        payload = {"ok": False, "error": "Unexpected response from ingest"}
        status = 500
    return payload, status

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # The Render start command symlinks /data/eod.db -> ./eod.db
    totals = _load_totals_from_db("eod.db")
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals,  # ensure this is ALWAYS present
        },
    )

@app.post("/ingest/live")
def run_live():
    """
    Call your ingest function and be resilient to return type.
    No matter what ingest returns, this will respond with JSON (not a 500
    from our own handler).
    """
    try:
        res = ingest.ingest_live()
        payload, status = _normalize_ingest_response(res)
        return JSONResponse(payload, status_code=status)
    except Exception as e:
        logger.exception("live ingest crashed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/ingest/demo")
def run_demo():
    # Optional: keep the route around so clicking "Demo" never 500s
    return JSONResponse(
        {"ok": False, "error": "Demo ingest not implemented on this service."},
        status_code=501,
    )

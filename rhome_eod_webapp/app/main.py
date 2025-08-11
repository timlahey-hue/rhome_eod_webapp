# app/main.py
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from . import ingest  # our local ingest.py

log = logging.getLogger("app")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

app = FastAPI()

# Static files (if you have app/static)
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# ---- Jinja filters (fixes 'fmt_currency' / 'fmt_pct' undefined) ----
def fmt_currency(value):
    try:
        if value is None:
            return "$0"
        return "${:,.0f}".format(float(value))
    except Exception:
        return "$0"

def fmt_pct(value):
    try:
        if value is None:
            return "0%"
        return "{:.0f}%".format(float(value) * 100 if float(value) <= 1 else float(value))
    except Exception:
        return "0%"

templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_pct"] = fmt_pct

# ---- DB helpers ----
DB_PATH = Path(__file__).parent.parent / "eod.db"

def get_totals() -> Dict[str, Any]:
    if not DB_PATH.exists():
        log.error("get_totals(): DB file not found at %s; returning empty dict", DB_PATH)
        return {}
    try:
        con = sqlite3.connect(str(DB_PATH))
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        # If 'totals' table doesn't exist, handle gracefully
        cur.execute("""
            SELECT
                hours_today,
                labour_cost_today,
                mtd_gm_pct
            FROM totals
            ORDER BY asof DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        con.close()
        return dict(row) if row else {}
    except sqlite3.OperationalError as e:
        if "no such table: totals" in str(e).lower():
            log.error("get_totals(): 'totals' table not found; returning empty dict")
            return {}
        raise

# ---- Routes ----
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    totals = get_totals()
    # Provide defaults so the template always renders
    totals = {
        "hours_today": totals.get("hours_today", 0),
        "labour_cost_today": totals.get("labour_cost_today", 0),
        "mtd_gm_pct": totals.get("mtd_gm_pct", 0),
    }
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals,
        },
    )

@app.post("/ingest/live")
def run_live():
    """
    Intentionally return HTTP 200 even on failures so the UI never shows a raw 500.
    Check the 'ok' flag and 'note' in the JSON body to see what happened.
    """
    try:
        res = ingest.ingest_live(budget_sec=25)  # <-- signature accepts budget_sec
    except Exception as e:
        log.exception("ingest/live failed")
        res = {
            "ok": False,
            "elapsed_sec": 0,
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": -1,
            "note": "exception",
            "detail": str(e),
        }
    # Always 200; client logic should look at res["ok"]
    return JSONResponse(res, status_code=200)

@app.get("/health")
def health():
    return {"ok": True}

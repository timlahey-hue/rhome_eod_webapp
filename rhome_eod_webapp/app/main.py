import os
import time
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("app")

# -----------------------
# FastAPI + Templates
# -----------------------
app = FastAPI(title="RHOME EOD Dashboard")

BASE_DIR = Path(__file__).resolve().parent.parent  # .../rhome_eod_webapp
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"
DB_PATH = Path(os.getenv("DATABASE_PATH", BASE_DIR / "eod.db"))

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Register static (if present)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# -----------------------
# Jinja helpers (filters/globals)
# -----------------------
def _num(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def fmt_currency(v) -> str:
    n = _num(v)
    if n is None:
        return "—"
    return f"${n:,.2f}"

def fmt_int(v) -> str:
    n = _num(v)
    if n is None:
        return "—"
    return f"{int(round(n)):,}"

def fmt_pct(v) -> str:
    n = _num(v)
    if n is None:
        return "—"
    # If caller passes 0.27 we want 27.0%
    if 0 <= n <= 1:
        n = n * 100.0
    return f"{n:.1f}%"

# Make them available exactly as used in templates: {{ fmt_currency(...) }}
templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_int"] = fmt_int
templates.env.filters["fmt_pct"] = fmt_pct
templates.env.globals["fmt_currency"] = fmt_currency
templates.env.globals["fmt_int"] = fmt_int
templates.env.globals["fmt_pct"] = fmt_pct

# -----------------------
# DB helpers
# -----------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as cx:
        cx.execute("""
            CREATE TABLE IF NOT EXISTS totals (
                key TEXT PRIMARY KEY,
                value REAL
            )
        """)
        cx.commit()
    log.info("Startup OK. DB at %s", DB_PATH)

def get_totals() -> Dict[str, Any]:
    try:
        with get_conn() as cx:
            cur = cx.execute("SELECT key, value FROM totals")
            rows = cur.fetchall()
            data = {r["key"]: r["value"] for r in rows}
    except Exception as e:
        log.error("get_totals(): %s; returning empty dict", e)
        data = {}

    # Derived metrics (won't crash if missing)
    rev = _num(data.get("mtd_revenue"))
    gm = _num(data.get("mtd_gross_margin"))
    if rev and rev != 0 and gm is not None:
        data["mtd_gm_pct"] = gm / rev
    else:
        # Leave as None so the template shows "—"
        data.setdefault("mtd_gm_pct", None)

    # Provide sane defaults for cards we display
    defaults = {
        "labour_cost_today": 0,
        "revenue_today": 0,
        "mtd_revenue": 0,
        "mtd_gross_margin": 0,
        "mtd_gm_pct": None,
    }
    for k, v in defaults.items():
        data.setdefault(k, v)
    return data

def upsert_totals(pairs: Dict[str, Any]) -> None:
    if not pairs:
        return
    with get_conn() as cx:
        cx.executemany(
            "INSERT INTO totals(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [(k, float(v)) for k, v in pairs.items() if _num(v) is not None],
        )
        cx.commit()

# -----------------------
# Routes
# -----------------------
@app.get("/health")
def health() -> PlainTextResponse:
    return PlainTextResponse("ok")

@app.get("/")
def home(request: Request):
    totals = get_totals()
    # NOTE: Jinja helpers are registered globally so the template won't 500
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "totals": totals, "now": int(time.time())},
    )

@app.post("/ingest/live")
def ingest_live():
    """
    Kicks off a quick, synchronous 'probe' ingest to:
    - fetch a token (proves auth)
    - verify API base (explicitly requires SIMPRO_API_BASE, no guessing)
    - (optional) try a light endpoint if you've set SIMPRO_COMPANY_ID
    This returns JSON and never throws a 500.
    """
    from .ingest import run_live_ingest
    result = run_live_ingest()
    # Optionally persist a couple totals if returned
    if result.get("ok") and "totals" in result:
        try:
            upsert_totals(result["totals"])
        except Exception as e:
            log.warning("upsert_totals failed: %s", e)
    return JSONResponse(result)

# -----------------------
# App startup
# -----------------------
@app.on_event("startup")
def _startup():
    init_db()

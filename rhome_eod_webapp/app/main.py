# app/main.py
import os
import time
import logging
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

log = logging.getLogger("app")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:app:%(message)s")

app = FastAPI()

# Static + templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

def fmt_currency(value) -> str:
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return "$0"

# Make fmt_currency available in templates regardless of how they're written
templates.env.globals["fmt_currency"] = fmt_currency
templates.env.filters["currency"] = fmt_currency

# DB path; Render symlinks /data/eod.db -> ./eod.db
DB_PATH = "eod.db" if os.path.exists("eod.db") else os.getenv("EOD_DB_PATH", "eod.db")

def get_totals():
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("""
            SELECT labour_cost_today, jobs_completed_today
            FROM totals
            ORDER BY id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        con.close()
        if not row:
            return {}
        return {"labour_cost_today": row[0], "jobs_completed_today": row[1]}
    except sqlite3.OperationalError:
        log.error("get_totals(): 'totals' table not found; returning empty dict")
        return {}
    except Exception:
        log.exception("get_totals() failed; returning empty totals")
        return {}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def home(request: Request):
    totals = get_totals()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "totals": totals, "now": int(time.time())},
    )

# Try both package and plain import so we work in all layouts
try:
    from . import ingest as ingest_mod  # type: ignore
except Exception as e:
    log.error("failed to import .ingest: %s", e)
    try:
        import ingest as ingest_mod  # type: ignore
    except Exception as e2:
        log.error("also failed to import ingest: %s", e2)
        ingest_mod = None  # type: ignore

@app.post("/ingest/live")
def run_live():
    if not ingest_mod or not hasattr(ingest_mod, "ingest_live"):
        log.error("ingest_live not available")
        return JSONResponse({"ok": False, "error": "ingest_live not available", "elapsed_sec": 0.0})
    try:
        # Prefer new signature; fall back if older ingest is deployed
        try:
            res = ingest_mod.ingest_live(budget_sec=25)  # type: ignore
        except TypeError:
            res = ingest_mod.ingest_live()  # type: ignore
        return JSONResponse(res)
    except Exception as e:
        log.exception("ingest/live failed")
        return JSONResponse({"ok": False, "error": str(e)})

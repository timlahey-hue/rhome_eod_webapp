# app/main.py
import os
import sqlite3
import datetime as dt
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

# ---------------------------
# App & static/templates
# ---------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# ---------------------------
# Jinja helpers (registered as globals AND filters)
# ---------------------------
def _to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def fmt_currency(value):
    v = _to_float(value, 0.0)
    return f"${v:,.2f}"

def fmt_hours(value):
    v = _to_float(value, 0.0)
    return f"{v:.1f}"

def fmt_int(value):
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"

def fmt_pct(value):
    v = _to_float(value, 0.0)
    return f"{v*100:.0f}%"

def fmt_ts(value):
    if not value:
        return "-"
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    # assume ISO string
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)

templates.env.globals.update(
    fmt_currency=fmt_currency,
    fmt_hours=fmt_hours,
    fmt_int=fmt_int,
    fmt_pct=fmt_pct,
    fmt_ts=fmt_ts,
)
templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_hours"] = fmt_hours
templates.env.filters["fmt_int"] = fmt_int
templates.env.filters["fmt_pct"] = fmt_pct
templates.env.filters["fmt_ts"] = fmt_ts

# ---------------------------
# DB helpers (safe & optional)
# ---------------------------
DB_PATH = "eod.db"

def open_db():
    return sqlite3.connect(DB_PATH)

def _object_exists(conn, name):
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type in ('table','view') AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None

def get_last_run(conn):
    if _object_exists(conn, "snapshot_run"):
        row = conn.execute(
            "SELECT id, started_at, finished_at, ok, jobs, tried, note "
            "FROM snapshot_run ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            keys = ["id", "started_at", "finished_at", "ok", "jobs", "tried", "note"]
            return dict(zip(keys, row))
    return None

def get_totals(conn):
    # Try a few possible totals objects if you have a view/table for them.
    candidates = ["dashboard_totals", "totals", "v_totals"]
    for name in candidates:
        if _object_exists(conn, name):
            cur = conn.execute(f"SELECT * FROM {name} ORDER BY 1 DESC LIMIT 1")
            row = cur.fetchone()
            if row is not None:
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
    # Fallback: empty dict so template `totals.get('x', 0)` still works.
    return {}

# ---------------------------
# Import ingest module lazily & defensively
# ---------------------------
try:
    from . import ingest as ingest_mod   # don't import specific names; they may not exist
    _ingest_import_error = None
except Exception as e:
    ingest_mod = None
    _ingest_import_error = str(e)

# ---------------------------
# Routes
# ---------------------------
@app.get("/health")
def health():
    return {"ok": True, "time": dt.datetime.utcnow().isoformat() + "Z"}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    totals = {}
    last_run = None
    note = None
    db_ok = os.path.exists(DB_PATH)

    if db_ok:
        try:
            with open_db() as conn:
                totals = get_totals(conn) or {}
                last_run = get_last_run(conn)
        except Exception as e:
            # Don't crash; show a small note in the UI if you want to surface it.
            note = f"DB read failed: {e.__class__.__name__}"

    ctx = {
        "request": request,
        "totals": totals,   # <-- guarantees `totals` is defined for the template
        "last_run": last_run,
        "db_ok": db_ok,
        "note": note,
    }
    return templates.TemplateResponse("index.html", ctx)

@app.post("/ingest/live")
def run_live():
    if not ingest_mod or not hasattr(ingest_mod, "ingest_live"):
        return JSONResponse(
            {
                "ok": False,
                "error": "ingest_live is unavailable in this build",
                "detail": _ingest_import_error,
            },
            status_code=500,
        )
    res = ingest_mod.ingest_live()
    return JSONResponse(res, status_code=(200 if res.get("ok") else 500))

@app.post("/ingest/demo")
def run_demo():
    if not ingest_mod or not hasattr(ingest_mod, "ingest_demo"):
        return JSONResponse(
            {"ok": False, "error": "ingest_demo is unavailable in this build"},
            status_code=500,
        )
    res = ingest_mod.ingest_demo()
    return JSONResponse(res, status_code=(200 if res.get("ok") else 500))

@app.post("/ingest/backfill")
def run_backfill():
    if not ingest_mod or not hasattr(ingest_mod, "ingest_backfill"):
        return JSONResponse(
            {"ok": False, "error": "ingest_backfill is unavailable in this build"},
            status_code=500,
        )
    res = ingest_mod.ingest_backfill()
    return JSONResponse(res, status_code=(200 if res.get("ok") else 500))
    

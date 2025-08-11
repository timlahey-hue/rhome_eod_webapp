import os
import time
import logging
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

logger = logging.getLogger("app")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.environ.get(
    "EOD_DB_PATH",
    os.path.abspath(os.path.join(BASE_DIR, "..", "eod.db"))
)

app = FastAPI()

# Static & templates
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# ---- Safe formatters (make 500s impossible if values are None/missing)
def fmt_currency(x):
    try:
        x = 0 if x is None else x
        return "${:,.0f}".format(float(x))
    except Exception:
        return "-"

def fmt_pct(x, places=1):
    try:
        if x is None:
            return "-"
        return f"{float(x) * 100:.{places}f}%"
    except Exception:
        return "-"

def fmt_int(x):
    try:
        if x is None:
            return "0"
        return f"{int(round(float(x))):,}"
    except Exception:
        return "-"

# Expose helpers to Jinja (both as globals and filters so either call style works)
templates.env.globals.update(fmt_currency=fmt_currency, fmt_pct=fmt_pct, fmt_int=fmt_int)
templates.env.filters.update(currency=fmt_currency, pct=fmt_pct, intfmt=fmt_int)

def ensure_db():
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS totals (
                key   TEXT PRIMARY KEY,
                value REAL
            )
        """)
        con.commit()
    finally:
        try:
            con.close()
        except Exception:
            pass

def get_totals():
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        try:
            cur.execute("SELECT key, value FROM totals")
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            logger.error("get_totals(): 'totals' table not found; returning empty dict")
            return {}
        return {k: v for (k, v) in rows}
    except Exception as e:
        logger.exception("get_totals(): unexpected error")
        return {}
    finally:
        try:
            con.close()
        except Exception:
            pass

@app.on_event("startup")
def on_startup():
    ensure_db()
    logger.info("Startup OK. DB at %s", DB_PATH)

@app.get("/health")
def health():
    return {"ok": True, "time": int(time.time())}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    totals = get_totals()
    # NOTE: fmt_* helpers are registered globally, so template can call fmt_currency(...), fmt_pct(...), etc.
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "totals": totals, "now": int(time.time())}
    )

# ---- Ingest endpoints (always present)
try:
    from .ingest import ingest_live as _ingest_live, ingest_diag as _ingest_diag
except Exception as ex:
    logger.error("ingest functions not available at startup: %s", ex)
    _ingest_live = None
    _ingest_diag = None

@app.post("/ingest/live")
def ingest_live_endpoint():
    if _ingest_live is None:
        logger.error("ingest_live not available")
        return JSONResponse({"ok": False, "error": "ingest_live not available"}, status_code=200)
    return JSONResponse(_ingest_live(), status_code=200)

@app.get("/ingest/diag")
def ingest_diag_endpoint():
    if _ingest_diag is None:
        logger.error("ingest_diag not available")
        return JSONResponse({"ok": False, "error": "ingest_diag not available"}, status_code=200)
    return JSONResponse(_ingest_diag(), status_code=200)

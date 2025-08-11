import os
import time
import logging
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, Request, APIRouter
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

# -----------------------------------------------------------------------------
# App + logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("app")

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DB_PATH", BASE_DIR.parent / "eod.db"))

# Static + templates
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# -----------------------------------------------------------------------------
# DB helpers (creates schema + seeds safe defaults)
# -----------------------------------------------------------------------------
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS totals (
            key TEXT PRIMARY KEY,
            value REAL
        )
    """)
    # minimal seed so templates have something to show
    seed_keys = [
        "revenue_today", "revenue_mtd",
        "materials_cost_today", "materials_cost_mtd",
        "labour_cost_today", "labour_cost_mtd",
        "mtd_gm_pct", "jobs_ingested", "last_ingest_ts"
    ]
    for k in seed_keys:
        conn.execute(
            "INSERT INTO totals(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO NOTHING",
            (k, 0.0),
        )
    conn.commit()

def get_totals() -> dict:
    try:
        conn = _db()
        ensure_schema(conn)
        rows = conn.execute("SELECT key, value FROM totals").fetchall()
        conn.close()
        return {r["key"]: r["value"] for r in rows}
    except Exception as e:
        log.error("get_totals(): %s", e)
        return {}

def set_total(key: str, value: float) -> None:
    try:
        conn = _db()
        ensure_schema(conn)
        conn.execute(
            "INSERT INTO totals(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, float(value)),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.exception("set_total(%s): %s", key, e)

# -----------------------------------------------------------------------------
# Jinja helpers (always registered)
# -----------------------------------------------------------------------------
def _num(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)

def fmt_currency(x) -> str:
    n = _num(x, 0.0)
    return f"${n:,.0f}"

def fmt_int(x) -> str:
    n = _num(x, 0.0)
    return f"{int(round(n)):,}"

def fmt_pct(x) -> str:
    if x is None:
        return "0.0%"
    try:
        v = float(x)
        # if value looks like 0.12 treat as 12%
        if 0 <= v <= 1:
            v *= 100.0
        return f"{v:.1f}%"
    except Exception:
        return "0.0%"

def hms(epoch_seconds) -> str:
    try:
        now = int(time.time())
        delta = now - int(float(epoch_seconds or 0))
        if delta < 0:
            delta = 0
        if delta < 60:
            return f"{delta}s ago"
        if delta < 3600:
            return f"{delta//60}m ago"
        return f"{delta//3600}h ago"
    except Exception:
        return "n/a"

templates.env.globals.update(
    fmt_currency=fmt_currency,
    fmt_int=fmt_int,
    fmt_pct=fmt_pct,
    hms=hms,
)

# -----------------------------------------------------------------------------
# Minimal routes
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    totals = get_totals()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "totals": totals, "now": int(time.time())},
    )

# -----------------------------------------------------------------------------
# Ingest router (defined below for single-file drop-in)
# -----------------------------------------------------------------------------
router = APIRouter(prefix="/ingest", tags=["ingest"])

def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

def _simpro_base_url() -> str:
    """
    Build API base URL safely:
      1) SIMPRO_BASE_URL (full, e.g. https://rhome.simprosuite.com/api/v1.0)
      2) derive host from SIMPRO_TOKEN_URL -> https://<host>/api/v1.0
      3) fallback SIMPRO_TENANT -> https://<tenant>/api/v1.0
    """
    base = _env("SIMPRO_BASE_URL")
    if base:
        return base.rstrip("/")

    token_url = _env("SIMPRO_TOKEN_URL")
    if token_url:
        host = urlparse(token_url).netloc
        if host:
            return f"https://{host}/api/v1.0"

    tenant = _env("SIMPRO_TENANT")
    if tenant:
        tenant = tenant.replace("https://", "").replace("http://", "")
        return f"https://{tenant}/api/v1.0"

    # last-resort sane default (your tenant)
    return "https://rhome.simprosuite.com/api/v1.0"

def _simpro_token() -> str:
    token_url = _env("SIMPRO_TOKEN_URL")
    cid = _env("SIMPRO_CLIENT_ID")
    secret = _env("SIMPRO_CLIENT_SECRET")

    if not token_url or not cid or not secret:
        raise RuntimeError("Missing SIMPRO_* env vars (TOKEN_URL, CLIENT_ID, CLIENT_SECRET)")

    resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(cid, secret),
        timeout=20,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"token_status_{resp.status_code}: {resp.text[:200]}")
    tok = resp.json().get("access_token")
    if not tok:
        raise RuntimeError("token_missing")
    return tok

@router.get("/ping")
def ping():
    base = _simpro_base_url()
    try:
        tok = _simpro_token()
        # $metadata is common on OData APIs; it’s a safe, read-only probe
        meta = requests.get(f"{base}/$metadata", headers={"Authorization": f"Bearer {tok}"}, timeout=15)
        return {
            "ok": True,
            "base_url": base,
            "metadata_status": meta.status_code,
        }
    except Exception as e:
        return {"ok": False, "base_url": base, "error": str(e)}

@router.post("/live")
def ingest_live():
    """
    Tries a few likely Jobs endpoints and records very basic counts into totals.
    Does not assume a specific schema; just verifies reachability.
    """
    base = _simpro_base_url()
    tried = []
    try:
        tok = _simpro_token()
    except Exception as e:
        return {"ok": False, "note": f"auth_error:{e}", "tried": tried, "base_url": base}

    session = requests.Session()
    session.headers.update(
        {"Authorization": f"Bearer {tok}", "Accept": "application/json"}
    )

    # A compact, explicit list of candidate endpoints (order matters)
    candidates = [
        "Jobs?$top=1",
        "jobs?$top=1",
        "ServiceJobs?$top=1",
        "Projects?$top=1",
        "Companies(0)/Jobs?$top=1",
        "companies(0)/jobs?$top=1",
        "$metadata",  # last resort to confirm API is alive
    ]

    found = None
    status = None
    body = None

    for path in candidates:
        url = f"{base.rstrip('/')}/{path}"
        tried.append(url)
        try:
            r = session.get(url, timeout=20)
            status = r.status_code
            if status == 200:
                found = path
                body = r.json() if "application/json" in r.headers.get("Content-Type", "") else {}
                break
        except Exception as e:
            status = f"err:{e}"

    # Update minimal totals so the UI has something to show
    set_total("last_ingest_ts", time.time())

    if found and status == 200:
        # Try to infer a count
        count = 0
        if isinstance(body, dict) and "value" in body and isinstance(body["value"], list):
            count = len(body["value"])
        elif isinstance(body, list):
            count = len(body)
        else:
            count = 1

        set_total("jobs_ingested", count)
        return {
            "ok": True,
            "base_url": base,
            "endpoint_found": found,
            "status": status,
            "count": count,
            "tried": tried,
        }

    return {
        "ok": False,
        "note": "probe_404:no_jobs_endpoint_found",
        "status": status,
        "base_url": base,
        "tried": tried,
    }

app.include_router(router)

# -----------------------------------------------------------------------------
# Startup log
# -----------------------------------------------------------------------------
@app.on_event("startup")
def _startup():
    # ensure DB exists & seeded
    try:
        conn = _db()
        ensure_schema(conn)
        conn.close()
        log.info("Startup OK. DB at %s", DB_PATH)
    except Exception as e:
        log.exception("Startup failed: %s", e)

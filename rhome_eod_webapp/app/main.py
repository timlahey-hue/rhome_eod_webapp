# app/main.py
import os
import logging
from typing import Optional

from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Import your ingest function
from .ingest import ingest_live

# -----------------------------------------------------------------------------
# App & logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("app")

app = FastAPI()

# Static files (css, etc.)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Templates
templates = Jinja2Templates(directory="app/templates")

# -----------------------------------------------------------------------------
# Jinja filters & safe defaults so the homepage never crashes
# -----------------------------------------------------------------------------
def fmt_currency(value):
    try:
        return "${:,.2f}".format(float(value or 0))
    except Exception:
        return "$0.00"

def fmt_number(value):
    try:
        return "{:,.0f}".format(float(value or 0))
    except Exception:
        return "0"

templates.env.filters["fmt_currency"] = fmt_currency
templates.env.filters["fmt_number"] = fmt_number

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def normalize_result(res):
    """
    Accept:
      - dict -> infer status 200/500 from 'ok'
      - (dict, int) -> return as-is
    """
    if isinstance(res, tuple):
        payload, status = res
        if not isinstance(payload, dict):
            payload = {"ok": False, "error": "Unexpected return type from ingest"}
        if "ok" not in payload:
            payload["ok"] = (status == 200)
        return payload, status
    if isinstance(res, dict):
        return res, (200 if res.get("ok") else 500)
    return {"ok": False, "error": "Unexpected return from ingest"}, 500

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # Always pass 'totals' and 'rows' so the template never throws 'UndefinedError'
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": {},   # your template can safely do totals.get(...)
            "rows": [],     # if you list anything
        },
    )

@app.post("/ingest/live")
def run_live(company_id: Optional[int] = Query(default=None)):
    """
    Call the live ingest. Works with either:
      - dict return from ingest_live()
      - (dict, status_code) return from ingest_live()
    """
    try:
        res = ingest_live(company_id=company_id) if company_id is not None else ingest_live()
        payload, status = normalize_result(res)
        return JSONResponse(payload, status_code=status)
    except Exception as e:
        logger.exception("run_live crashed")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# Optional: keep this so hitting /ingest/demo doesn't 500 if it's not implemented
@app.post("/ingest/demo")
def run_demo():
    return JSONResponse({"ok": False, "error": "demo ingest not available"}, status_code=501)

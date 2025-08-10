import os
from fastapi import FastAPI, Request, Form, BackgroundTasks
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from . import db
from .metrics import compute_metrics
from .ingest import ingest, ingest_live
from .slack import share_summary

load_dotenv()

app = FastAPI(title="r:home EOD Dashboard")
@app.get("/health")
def health():
    return {"ok": True}
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

def fmt_currency(x):
  try:
    return "${:,.0f}".format(float(x))
  except Exception:
    return "—"
def fmt_pct(x):
  try:
    return f"{float(x)*100:.0f}%"
  except Exception:
    return "—"

@app.on_event("startup")
def startup():
  db.init_db()

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
  snap = db.get_latest_snapshot()
  rows = db.get_snapshot_rows(snap["id"]) if snap else []
  rows_py = [dict(r) for r in rows]
  totals, top5, at_risk, exceptions = compute_metrics(rows_py) if rows else ({}, [], [], [])
  return templates.TemplateResponse("index.html", {
    "request": request,
    "snapshot": snap,
    "rows": rows,
    "totals": totals,
    "top5": top5,
    "at_risk": at_risk,
    "exceptions": exceptions,
    "env_ok": {
      "SIMPRO_BASE_URL": bool(os.getenv("SIMPRO_BASE_URL")),
      "SIMPRO_CLIENT_ID": bool(os.getenv("SIMPRO_CLIENT_ID")),
      "SIMPRO_CLIENT_SECRET": bool(os.getenv("SIMPRO_CLIENT_SECRET")),
      "SLACK_WEBHOOK_URL": bool(os.getenv("SLACK_WEBHOOK_URL")),
    },
    "fmt_currency": fmt_currency,
    "fmt_pct": fmt_pct
  })

@app.post("/ingest/demo")
def run_demo_ingest():
  data = ingest_demo()
  return RedirectResponse(url="/", status_code=303)

@app.post("/ingest/live")
def run_live_ingest():
  base = os.getenv("SIMPRO_BASE_URL")
  cid = os.getenv("SIMPRO_CLIENT_ID")
  sec = os.getenv("SIMPRO_CLIENT_SECRET")
  co  = os.getenv("SIMPRO_COMPANY_ID") or None
  if not all([base, cid, sec]):
    return RedirectResponse(url="/?error=missing_creds", status_code=303)
  data = ingest_live()
  return RedirectResponse(url="/", status_code=303)

@app.post("/share/{snapshot_id}")
def share(snapshot_id: int):
  webhook = os.getenv("SLACK_WEBHOOK_URL")
  if not webhook:
    return RedirectResponse(url="/?error=no_slack", status_code=303)
  rows = db.get_snapshot_rows(snapshot_id)
  rows_py = [dict(r) for r in rows]
  totals, top5, at_risk, exceptions = compute_metrics(rows_py)
  summary = f"Hours: {totals.get('hours_today',0):.1f} | Labour: {totals.get('labour_cost_today',0):.0f} | Materials: {totals.get('materials_cost_today',0):.0f} | Invoiced: {totals.get('invoiced_today',0):.0f} | MTD GM%: {totals.get('mtd_gm_pct',0) if totals.get('mtd_gm_pct') else 0:.0%}"
  # Simple block summary
  blocks = [
    {"type": "header", "text": {"type": "plain_text", "text": "EOD — r:home", "emoji": True}},
    {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
  ]
  share_summary(webhook, summary, blocks=blocks)
  return RedirectResponse(url="/?shared=1", status_code=303)

@app.get("/snapshots/{snapshot_id}", response_class=HTMLResponse)
def view_snapshot(request: Request, snapshot_id: int):
  snaps = db.list_snapshots()
  match = [s for s in snaps if s["id"] == snapshot_id]
  if not match:
    return RedirectResponse(url="/", status_code=303)
  snap = match[0]
  rows = db.get_snapshot_rows(snapshot_id)
  rows_py = [dict(r) for r in rows]
  totals, top5, at_risk, exceptions = compute_metrics(rows_py)
  return templates.TemplateResponse("snapshot.html", {
    "request": request,
    "snapshot": snap,
    "rows": rows,
    "totals": totals,
    "top5": top5,
    "at_risk": at_risk,
    "exceptions": exceptions,
    "fmt_currency": fmt_currency,
    "fmt_pct": fmt_pct
  })

@app.get("/health")
def health():
  return {"ok": True}

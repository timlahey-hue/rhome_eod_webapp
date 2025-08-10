from datetime import date, datetime, timezone
import numpy as np
import random

from . import db
from .metrics import compute_metrics
from .simpro import get_access_token, list_companies, list_jobs_modified_since

def ingest_demo():
  """Generate a demo snapshot and persist it."""
  db.init_db()
  today = date.today().isoformat()
  snap_id = db.create_snapshot(today)

  rng = np.random.default_rng(42)
  avg_rate = 85.0
  jobs = [
      {"job_code": "JOB-4704", "job_name": "Richards Family — One Chicago Unit 4704", "pm": "Andreas"},
      {"job_code": "JOB-4804", "job_name": "Hughes Family — One Chicago Unit 4804", "pm": "Andreas"},
      {"job_code": "JOB-6102", "job_name": "Brian Flanagan — Unit 6102", "pm": "Danny"},
      {"job_code": "JOB-630201", "job_name": "Monish Shah — Unit 630201", "pm": "Danny"},
      {"job_code": "JOB-GLAKE", "job_name": "Gary Morrison — Lake House Upgrades", "pm": "Tim Bauer"},
      {"job_code": "JOB-4703", "job_name": "One Chicago — Unit 4703", "pm": "Kareem"},
      {"job_code": "JOB-SHOWRM", "job_name": "r:home Demo Space — Lighting & Audio", "pm": "Danny"},
  ]
  rows = []
  for j in jobs:
      hours_today = max(0, rng.normal(6, 2))
      labour_cost_today = hours_today * avg_rate
      materials_cost_today = max(0, rng.normal(500, 250))
      po_value_today = max(0, rng.normal(800, 600)) if rng.random() > 0.4 else 0.0
      invoiced_today = max(0, rng.normal(2500, 1500)) if rng.random() > 0.55 else 0.0

      actual_cost_to_date_before_today = max(0, rng.normal(28000, 12000))
      revenue_invoiced_to_date_before_today = max(0, rng.normal(42000, 15000))
      estimated_cost = max(
          actual_cost_to_date_before_today + labour_cost_today + materials_cost_today + rng.normal(5000, 4000), 20000
      )
      estimated_revenue = estimated_cost * rng.uniform(1.3, 1.6)
      mtd_hours_before_today = max(0, rng.normal(90, 25))
      mtd_hours = mtd_hours_before_today + hours_today
      days_since_update = int(max(0, rng.normal(2, 1.5)))
      cost_today = labour_cost_today + materials_cost_today

      row = {
          "job_code": j["job_code"],
          "job_name": j["job_name"],
          "pm": j["pm"],
          "hours_today": round(hours_today, 2),
          "labour_cost_today": round(labour_cost_today, 2),
          "materials_cost_today": round(materials_cost_today, 2),
          "po_value_today": round(po_value_today, 2),
          "invoiced_today": round(invoiced_today, 2),
          "actual_cost_to_date": round(actual_cost_to_date_before_today + labour_cost_today + materials_cost_today, 2),
          "revenue_invoiced_to_date": round(revenue_invoiced_to_date_before_today + invoiced_today, 2),
          "estimated_cost": round(estimated_cost, 2),
          "estimated_revenue": round(estimated_revenue, 2),
          "mtd_hours": round(mtd_hours, 2),
          "days_since_update": days_since_update,
          "cost_today": round(cost_today, 2),
      }
      # Derived
      burn_pct = (row["actual_cost_to_date"] / row["estimated_cost"]) if row["estimated_cost"] else None
      gm_to_date = ((row["revenue_invoiced_to_date"] - row["actual_cost_to_date"]) / row["revenue_invoiced_to_date"]) if row["revenue_invoiced_to_date"] else None
      row["burn_pct"] = burn_pct
      row["gm_to_date"] = gm_to_date
      row["at_risk"] = bool((burn_pct or 0) >= 0.80 or (gm_to_date or 1) < 0.20)
      rows.append(row)

  db.insert_job_rows(snap_id, rows)
  totals, top5, at_risk, exceptions = compute_metrics(rows)
  return {"snapshot_id": snap_id, "totals": totals, "rows": rows, "top5": top5, "at_risk": at_risk, "exceptions": exceptions}

def ingest_live(base_url, client_id, client_secret, company_id=None):
  """Minimal live ingest to validate connectivity (companies + jobs since midnight UTC).
  We'll expand to include cost centers, invoices, receipts, schedules.
  """
  db.init_db()
  today = date.today().isoformat()
  snap_id = db.create_snapshot(today)

  token = get_access_token(base_url, client_id, client_secret)
  companies = list_companies(base_url, token)
  if not companies:
    raise RuntimeError("No companies returned")
  if not company_id:
    # try common keys
    company_id = companies[0].get("ID") or companies[0].get("id") or companies[0].get("Id")

  midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
  jobs = list_jobs_modified_since(base_url, token, company_id, midnight)

  # Transform to basic rows (we'll fill additional fields in next iteration)
  rows = []
  for job in jobs:
    code = job.get("code") or job.get("jobCode") or job.get("Code")
    name = job.get("name") or job.get("JobName") or job.get("Description")
    pm = job.get("projectManager") or job.get("ProjectManager") or "—"

    # Placeholders for to-date and today metrics until we wire cost centers + timesheets
    row = {
      "job_code": code, "job_name": name, "pm": pm,
      "hours_today": 0.0, "labour_cost_today": 0.0, "materials_cost_today": 0.0, "cost_today": 0.0,
      "actual_cost_to_date": float(job.get("actualCostToDate", 0)) if isinstance(job.get("actualCostToDate", 0), (int,float)) else 0.0,
      "estimated_cost": float(job.get("estimatedCost", 0)) if isinstance(job.get("estimatedCost", 0), (int,float)) else 0.0,
      "burn_pct": None, "gm_to_date": None,
      "invoiced_today": 0.0, "mtd_hours": 0.0,
      "days_since_update": 0, "at_risk": False
    }
    # if we have both actual & estimated, compute burn
    if row["actual_cost_to_date"] and row["estimated_cost"]:
      row["burn_pct"] = row["actual_cost_to_date"] / row["estimated_cost"]
    rows.append(row)

  db.insert_job_rows(snap_id, rows)
  from .metrics import compute_metrics
  totals, top5, at_risk, exceptions = compute_metrics(rows)
  return {"snapshot_id": snap_id, "totals": totals, "rows": rows, "top5": top5, "at_risk": at_risk, "exceptions": exceptions}

import math

def safe_div(n, d):
  try:
    return (n / d) if d else None
  except Exception:
    return None

def compute_metrics(rows):
  """Compute summary metrics and organize for UI."""
  totals = {
    "hours_today": sum((r.get("hours_today") or 0) for r in rows),
    "labour_cost_today": sum((r.get("labour_cost_today") or 0) for r in rows),
    "materials_cost_today": sum((r.get("materials_cost_today") or 0) for r in rows),
    "po_value_today": sum((r.get("po_value_today") or 0) for r in rows),
    "invoiced_today": sum((r.get("invoiced_today") or 0) for r in rows),
  }
  mtd_cost = sum((r.get("actual_cost_to_date") or 0) for r in rows)
  mtd_revenue = sum((r.get("revenue_invoiced_to_date") or 0) for r in rows)
  totals["mtd_gm_pct"] = safe_div(mtd_revenue - mtd_cost, mtd_revenue)

  # Top 5 by cost added today
  top5 = sorted(rows, key=lambda r: (r.get("labour_cost_today",0)+r.get("materials_cost_today",0)), reverse=True)[:5]

  # At-risk
  at_risk = [r for r in rows if ((r.get("burn_pct") or 0) >= 0.80) or ((r.get("gm_to_date") or 1) < 0.20)]
  at_risk = sorted(at_risk, key=lambda r: (r.get("burn_pct") or 0), reverse=True)[:5]

  # Exceptions (idle >= 3 days)
  exceptions = [r for r in rows if (r.get("days_since_update") or 0) >= 3]

  return totals, top5, at_risk, exceptions

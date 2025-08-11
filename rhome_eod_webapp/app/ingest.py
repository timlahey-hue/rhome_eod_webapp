import os
import time
import logging
from typing import Dict, Any, Optional

import requests

logger = logging.getLogger("ingest")
logger.setLevel(logging.INFO)

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def _get_token(tenant_base: str, client_id: str, client_secret: str) -> str:
    url = tenant_base.rstrip("/") + "/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    logger.info("ingest:[ingest] Authenticating with Simpro")
    r = requests.post(url, data=data, timeout=20)
    r.raise_for_status()
    token = r.json().get("access_token", "")
    logger.info("ingest:[ingest] Token acquired (len=%d)", len(token))
    if not token:
        raise RuntimeError("No access_token in OAuth response")
    return token

def ingest_live(
    # NEW name
    budget_sec: Optional[int] = None,
    # Back-compat: if someone passes 'budget', we’ll still work
    budget: Optional[int] = None,
    # Accept and ignore extra kwargs to be future-proof
    **_: Any,
) -> Dict[str, Any]:
    """
    Run a single 'live ingest' pass. Always returns a dict:
      { ok, elapsed_sec, jobs_inserted, jobs_tried, run_id, note }
    """
    # normalize budget
    if budget_sec is None:
        budget_sec = budget if budget is not None else 25

    t0 = time.time()
    run_id = int(t0) % 10_000

    try:
        tenant_base = _require_env("SIMPRO_TENANT_BASE")
        client_id   = _require_env("SIMPRO_CLIENT_ID")
        client_secret = _require_env("SIMPRO_CLIENT_SECRET")
    except Exception as e:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": str(e),
        }

    try:
        token = _get_token(tenant_base, client_id, client_secret)
    except Exception as e:
        # OAuth failed
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": f"oauth_error: {e}",
        }

    logger.info("ingest:[ingest] Starting live ingest (budget=%ss)", budget_sec)

    # --- Example API probe (safe and minimal). If your real ingest hits
    # specific endpoints, replace this block with your real logic.
    jobs_inserted = 0
    jobs_tried = 0
    note = "ok"

    try:
        # This is intentionally a no-op probe so we don't crash on 404s.
        # Swap in your real endpoint(s) here.
        headers = {"Authorization": f"Bearer {token}"}
        # Example: ping an endpoint you know exists in your tenant.
        # If it 404s, we catch and report it.
        resp = requests.get(tenant_base.rstrip("/") + "/api", headers=headers, timeout=20)
        if resp.status_code >= 400:
            note = f"api_status_{resp.status_code}"
            logger.warning("ingest:[ingest] API probe returned %s", resp.status_code)
    except Exception as e:
        note = f"api_error: {e}"

    elapsed = round(time.time() - t0, 3)
    ok = note == "ok"

    logger.info(
        "ingest:[ingest] ingest_live finished in %.2fs (ok=%s, jobs=%d, tried=%d)",
        elapsed, ok, jobs_inserted, jobs_tried
    )

    return {
        "ok": ok,
        "elapsed_sec": elapsed,
        "jobs_inserted": jobs_inserted,
        "jobs_tried": jobs_tried,
        "run_id": run_id,
        "note": note,
    }

import os
import time
import logging
import textwrap
from typing import Dict, Any

import requests

log = logging.getLogger("ingest")

def _req_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise ValueError(f"Missing required environment variable: {name}")
    return val

def _tenant_base() -> str:
    base = _req_env("SIMPRO_TENANT_BASE")  # e.g. https://rhome.simprosuite.com
    return base.rstrip("/")

def _get_token() -> str:
    """Obtain OAuth2 client-credentials token from Simpro."""
    log.info("[ingest] Authenticating with Simpro")
    url = f"{_tenant_base()}/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": _req_env("SIMPRO_CLIENT_ID"),
        "client_secret": _req_env("SIMPRO_CLIENT_SECRET"),
    }
    resp = requests.post(url, data=data, timeout=20)
    if resp.status_code != 200:
        snippet = resp.text[:400].replace("\n", " ")
        raise RuntimeError(f"token_status_{resp.status_code}: {snippet}")
    tok = resp.json().get("access_token")
    if not tok:
        raise RuntimeError("token_missing_access_token")
    log.info("[ingest] Token acquired (len=%d)", len(tok))
    return tok

_run_id = 0

def ingest_live(budget_sec: int = 25) -> Dict[str, Any]:
    """
    Live ingest runner. Always returns a dict.
    - Accepts `budget_sec` by name (matches /ingest/live caller).
    - Never raises; returns ok=False with an error on any failure.
    """
    global _run_id
    _run_id += 1
    started = time.perf_counter()

    tried = 0
    inserted = 0

    try:
        log.info("[ingest] Starting live ingest (budget=%ss)", budget_sec)
        token = _get_token()

        # --- Example probe call (adjust endpoint to your API path when ready) ---
        # If this 404s or fails, we capture that and return ok=False (no 500s).
        tried += 1
        jobs_url = f"{_tenant_base()}/api/v1.0/jobs?page=1"
        hdrs = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        r = requests.get(jobs_url, headers=hdrs, timeout=20)

        if r.status_code != 200:
            body = r.text[:500]
            body_oneline = " ".join(body.split())
            log.warning("[ingest] jobs page 1 returned %s: \t%s", r.status_code, body_oneline)
            return {
                "ok": False,
                "elapsed_sec": round(time.perf_counter() - started, 2),
                "jobs_inserted": inserted,
                "jobs_tried": tried,
                "run_id": _run_id,
                "note": f"api_status_{r.status_code}",
            }

        # If you parse jobs and write to SQLite, do it here and bump `inserted`.
        # For now we just prove the round-trip works:
        return {
            "ok": True,
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "jobs_inserted": inserted,
            "jobs_tried": tried,
            "run_id": _run_id,
            "note": "probe_ok",
        }

    except Exception as e:
        log.exception("[ingest] ingest_live failed")
        return {
            "ok": False,
            "elapsed_sec": round(time.perf_counter() - started, 2),
            "jobs_inserted": inserted,
            "jobs_tried": tried,
            "run_id": _run_id,
            "error": str(e),
        }

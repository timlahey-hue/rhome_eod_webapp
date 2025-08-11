import os
import time
import logging
from typing import Dict, List, Tuple

import requests
from requests.auth import HTTPBasicAuth

log = logging.getLogger("ingest")

# Defaults; override with env if needed
SIMPRO_BASE_URL = os.environ.get("SIMPRO_BASE_URL", "https://rhome.simprosuite.com")
TOKEN_URL = os.environ.get("SIMPRO_TOKEN_URL", f"{SIMPRO_BASE_URL}/oauth2/token")
CLIENT_ID = os.environ.get("SIMPRO_CLIENT_ID")  # required
CLIENT_SECRET = os.environ.get("SIMPRO_CLIENT_SECRET")  # required

# Probing strategy:
# - We try a handful of common Jobs endpoints.
# - Treat 200/204/206/400/401/403 as "endpoint exists" (auth/params may differ).
# - 404 means path likely wrong, keep trying.
DEFAULT_API_BASES = [
    "/api/v1.0",
    "/api",           # in case your tenant uses /api without version
]
JOBS_PATHS = [
    "/Jobs",
    "/jobs",
    "/Companies(0)/Jobs",
    "/companies(0)/jobs",
    "/Companies(1)/Jobs",
    "/companies(1)/jobs",
    "/companies/0/jobs",  # alternate shape
]

# Allow overrides for unusual tenants
OVERRIDE_API_BASE = os.environ.get("SIMPRO_API_BASE")  # e.g. "/api/v1.0"
OVERRIDE_JOBS_PATH = os.environ.get("SIMPRO_JOBS_PATH")  # e.g. "/Companies(123)/Jobs"

def _jobs_candidates() -> List[str]:
    bases = [OVERRIDE_API_BASE] if OVERRIDE_API_BASE else DEFAULT_API_BASES
    paths = [OVERRIDE_JOBS_PATH] if OVERRIDE_JOBS_PATH else JOBS_PATHS
    out = []
    for b in bases:
        for p in paths:
            out.append(f"{b.rstrip('/')}/{p.lstrip('/')}")
    # also try top=1 variants
    out += [f"{u}?$top=1" for u in out]
    return out

def _exists_status(code: int) -> bool:
    return code in (200, 204, 206, 400, 401, 403)

def _get_token() -> Tuple[bool, str]:
    if not CLIENT_ID or not CLIENT_SECRET:
        return False, "missing SIMPRO_CLIENT_ID or SIMPRO_CLIENT_SECRET env"
    try:
        resp = requests.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET),
            timeout=15,
        )
    except requests.RequestException as e:
        return False, f"token_request_exception:{e}"
    if resp.status_code != 200:
        return False, f"token_status_{resp.status_code}"
    try:
        token = resp.json().get("access_token")
    except ValueError:
        return False, "token_json_error"
    if not token:
        return False, "token_missing_in_response"
    return True, token

def ingest_live() -> Dict:
    """
    Light 'probe' ingest that:
      1) Authenticates via client credentials
      2) Probes a few likely /Jobs endpoints
    It does NOT write to DB yet; goal is to stabilize auth & discovery.
    """
    run_id = int(time.time() * 1000) % 2_147_483_647
    started = time.time()
    log.info("[ingest] Authenticating with Simpro")

    ok, token_or_err = _get_token()
    if not ok:
        log.error("[ingest] %s", token_or_err)
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - started, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": f"auth_error:{token_or_err}",
        }

    token = token_or_err
    log.info("[ingest] Token acquired (len=%d)", len(token))

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    tried = []
    found = None
    for rel in _jobs_candidates():
        url = f"{SIMPRO_BASE_URL.rstrip('/')}/{rel.lstrip('/')}"
        tried.append(rel)
        try:
            # A lightweight probe; GET with small $top if present, otherwise HEAD or GET
            method = "GET" if ("?$top=" in url or rel.lower().endswith("/jobs")) else "GET"
            resp = session.request(method, url, timeout=15)
            if _exists_status(resp.status_code):
                found = {"path": rel, "status": resp.status_code}
                break
        except requests.RequestException:
            # network hiccup? keep going
            continue

    if not found:
        note = "probe_404:no_jobs_endpoint_found"
        log.warning("[ingest] no jobs endpoint found; tried: %s", ", ".join(tried))
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - started, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": note,
            "tried": tried,
        }

    # If we did find something, you can expand here to fetch and insert.
    # For now, just report success of discovery.
    return {
        "ok": True,
        "elapsed_sec": round(time.time() - started, 3),
        "jobs_inserted": 0,
        "jobs_tried": 0,
        "run_id": run_id,
        "note": f"jobs_endpoint:{found['path']} status={found['status']}",
        "tried": tried,
    }

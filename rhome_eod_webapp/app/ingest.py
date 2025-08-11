# app/ingest.py
import os
import time
import json
import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests

log = logging.getLogger("ingest")
log.setLevel(logging.INFO)

# ---- Configuration helpers ---------------------------------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val

def _derive_base_url_from_token_url(token_url: str) -> str:
    """
    Derive https://<tenant>.simprosuite.com from a token URL like
    https://<tenant>.simprosuite.com/oauth/token
    """
    p = urlparse(token_url)
    return f"{p.scheme}://{p.netloc}"

# ---- OAuth2 client-credentials -----------------------------------------------

def _get_access_token() -> Tuple[bool, Optional[str], str]:
    """
    Returns (ok, access_token or None, note)
    """
    token_url = _env("SIMPRO_TOKEN_URL")
    client_id = _env("SIMPRO_CLIENT_ID")
    client_secret = _env("SIMPRO_CLIENT_SECRET")

    try:
        # Simpro uses standard client-credentials on /oauth/token
        # Either HTTP Basic or form creds both work; we’ll send in the form.
        data = {
            "grant_type": "client_credentials",
            # Some tenants require a scope; leave blank unless you’ve configured one.
            # "scope": os.getenv("SIMPRO_SCOPE", ""),
        }
        resp = requests.post(
            token_url,
            data=data,
            auth=(client_id, client_secret),
            headers={"Accept": "application/json"},
            timeout=15,
        )
    except Exception as e:
        return False, None, f"token_error:{e}"

    if resp.status_code != 200:
        return False, None, f"token_status_{resp.status_code}"

    try:
        payload = resp.json()
    except Exception:
        return False, None, "token_parse_error"

    token = payload.get("access_token")
    if not token:
        return False, None, "token_missing_access_token"

    return True, token, "token_ok"

# ---- API probing --------------------------------------------------------------

def _pick_api_base(host_base: str) -> Tuple[str, str]:
    """
    Decide base path. Default to /api/v1.0, but allow override via env.
    Returns (api_base_url, note)
    """
    base_path = os.getenv("SIMPRO_API_BASE_PATH", "/api/v1.0").rstrip("/")
    api_base = f"{host_base}{base_path}"
    # We could probe $metadata or /info, but some tenants block those.
    # Keep it simple; log that we defaulted.
    return api_base, "api_base_defaulted"

def _first_working_jobs_path(
    api_base: str,
    token: str,
    company_id: str,
) -> Tuple[Optional[str], str, Optional[int]]:
    """
    Try a list of likely Jobs endpoints. Return (full_url, note, status_code)
    NOTE: We append a tiny query to keep payloads small when OData is supported.
    """
    # Order matters: companies/{id}/jobs is the most common shape for Jobs.
    candidates = [
        f"/companies/{company_id}/jobs",
        f"/Companies({company_id})/Jobs",  # OData casing
        "/jobs",
        "/Jobs",
    ]

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    last_status = None
    for path in candidates:
        # Prefer a very light probe ($top=1 when OData works; harmless if ignored)
        probe_url = f"{api_base}{path}"
        try:
            resp = requests.get(
                probe_url,
                params={"$top": 1},
                headers=headers,
                timeout=20,
            )
            last_status = resp.status_code
            if 200 <= resp.status_code < 300:
                return probe_url, f"probe_ok:{path}", resp.status_code
            else:
                log.warning("ingest: [ingest] API probe returned non-success status=%s (path=%s)",
                            resp.status_code, path)
        except Exception as e:
            log.warning("ingest: [ingest] probe error for %s: %s", path, e)

    note = f"probe_{last_status if last_status is not None else 'error'}:no_jobs_endpoint_found"
    return None, note, last_status

# ---- Fetch one small page of jobs (for smoke test) ----------------------------

def _fetch_sample_jobs(jobs_url: str, token: str) -> Tuple[bool, int, str]:
    """
    Returns (ok, count, note)
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    try:
        resp = requests.get(jobs_url, params={"$top": 5}, headers=headers, timeout=30)
    except Exception as e:
        return False, 0, f"jobs_error:{e}"

    if resp.status_code == 401:
        return False, 0, "api_status_401"
    if resp.status_code == 403:
        return False, 0, "api_status_403"
    if resp.status_code == 404:
        return False, 0, "api_status_404"
    if not (200 <= resp.status_code < 300):
        return False, 0, f"api_status_{resp.status_code}"

    # Try to count list length in a few common shapes
    try:
        data = resp.json()
    except Exception:
        return True, 0, "jobs_ok_parse_error"

    count = 0
    if isinstance(data, dict):
        if "value" in data and isinstance(data["value"], list):  # OData shape
            count = len(data["value"])
        elif "items" in data and isinstance(data["items"], list):
            count = len(data["items"])
        else:
            # If dict but unknown shape, best-effort: count heuristically
            for key in ("jobs", "Jobs", "results"):
                if key in data and isinstance(data[key], list):
                    count = len(data[key])
                    break
    elif isinstance(data, list):
        count = len(data)

    return True, count, "jobs_ok"

# ---- Public entrypoint used by /ingest/live -----------------------------------

def ingest_live(budget_sec: int = 25) -> Dict:
    """
    Smoke-test Simpro connectivity and locate the Jobs endpoint.
    Designed to be called by the FastAPI route and return a friendly JSON.
    """
    t0 = time.time()
    run_id = int(t0)  # simple run id
    company_id = os.getenv("SIMPRO_COMPANY_ID", "0")

    # 1) Token
    ok, token, note = _get_access_token()
    if not ok or not token:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": note,
        }
    log.info("ingest:[ingest] Token acquired (len=%s)", len(token))

    # 2) Build API base
    host_base = os.getenv("SIMPRO_BASE_URL")
    if not host_base:
        # Derive from token URL host
        host_base = _derive_base_url_from_token_url(_env("SIMPRO_TOKEN_URL"))
    api_base, base_note = _pick_api_base(host_base)
    if base_note != "api_base_defaulted":
        log.info("ingest:[ingest] %s", base_note)
    else:
        log.warning("ingest:[ingest] API base discovery failed; defaulting to /api/v1.0")

    # 3) Find a working Jobs URL
    jobs_url, probe_note, last_status = _first_working_jobs_path(api_base, token, company_id)
    if not jobs_url:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": probe_note,
        }

    # 4) Fetch a tiny page to confirm (don’t write to DB yet)
    ok, count, fetch_note = _fetch_sample_jobs(jobs_url, token)
    elapsed = round(time.time() - t0, 3)
    return {
        "ok": ok,
        "elapsed_sec": elapsed,
        "jobs_inserted": 0,  # not inserting yet; this is a connectivity/smoke test
        "jobs_tried": count,
        "run_id": run_id,
        "note": fetch_note if ok else fetch_note or probe_note,
        "jobs_endpoint": jobs_url,  # helpful for logs
    }

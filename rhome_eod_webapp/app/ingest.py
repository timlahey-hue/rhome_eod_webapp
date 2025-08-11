import os
import time
import logging
from typing import Dict, List, Tuple

import httpx

log = logging.getLogger("ingest")

TENANT = os.getenv("SIMPRO_TENANT", "rhome").strip()
BASE_URL = os.getenv("SIMPRO_BASE_URL", f"https://{TENANT}.simprosuite.com").rstrip("/")
TOKEN_URL = os.getenv("SIMPRO_TOKEN_URL", f"{BASE_URL}/oauth2/token")
CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET", "")

def _get_token() -> Tuple[bool, str, Dict]:
    """
    Fetch OAuth2 access token using client credentials.
    Returns (ok, token_or_error, raw_response_json).
    """
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                TOKEN_URL,
                data={"grant_type": "client_credentials"},
                auth=(CLIENT_ID, CLIENT_SECRET),
                headers={"Accept": "application/json"},
            )
        if resp.status_code != 200:
            note = f"token_status_{resp.status_code}"
            log.error("[ingest] auth_error:%s body=%s", note, resp.text)
            return False, note, {"status": resp.status_code, "body": resp.text}
        data = resp.json()
        token = data.get("access_token", "")
        if not token:
            log.error("[ingest] token missing in response: %s", data)
            return False, "token_missing", data
        return True, token, data
    except Exception as e:
        log.exception("[ingest] token request failed: %s", e)
        return False, str(e), {}

def _probe_jobs(token: str) -> Dict:
    """
    Try a list of likely endpoints to find Jobs or similar.
    We ONLY probe; we don't insert into DB here. Always returns a dict.
    """
    tried: List[str] = []
    candidates = [
        "/api/v1.0/Jobs?$top=1",
        "/api/v1.0/jobs?$top=1",
        "/Jobs?$top=1",
        "/jobs?$top=1",
        "/api/v1.0/ServiceJobs?$top=1",
        "/ServiceJobs?$top=1",
        "/api/v1.0/Companies(0)/Jobs?$top=1",
        "/api/v1.0/companies(0)/jobs?$top=1",
        "/api/v1.0/companies/0/jobs?$top=1",
        "/api/v1.0/Companies(1)/Jobs?$top=1",
        "/api/v1.0/companies(1)/jobs?$top=1",
        "/$metadata",
        "/api/v1.0/$metadata",
    ]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    with httpx.Client(timeout=15.0) as client:
        for path in candidates:
            url = f"{BASE_URL}{path}"
            tried.append(url)
            try:
                r = client.get(url, headers=headers)
            except Exception as e:
                log.warning("[ingest] probe exception for %s: %s", url, e)
                continue

            if r.status_code == 200:
                # We found a working endpoint. Return basic info.
                try:
                    data = r.json()
                except Exception:
                    data = {"raw": r.text}
                note = f"probe_200:{path}"
                log.info("[ingest] SUCCESS %s", note)
                return {
                    "ok": True,
                    "note": note,
                    "status": r.status_code,
                    "endpoint": url,
                    "sample": data,
                    "tried": tried,
                }
            else:
                log.warning("[ingest] API probe returned non-200 status=%s (%s)", r.status_code, path)

    return {
        "ok": False,
        "note": "probe_404:no_jobs_endpoint_found",
        "status": 404,
        "base_url": BASE_URL,
        "tried": tried,
    }

def run_live_ingest(budget_seconds: int = 25) -> Dict:
    """
    Entry point used by /ingest/live.
    - Gets a token
    - Probes likely job endpoints
    - Returns a compact JSON summary (never raises)
    """
    start = time.time()
    log.info("[ingest] Authenticating with Simpro")
    ok, token_or_err, raw = _get_token()
    if not ok:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - start, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": int(start),
            "note": f"auth_error:{token_or_err}",
        }

    log.info("[ingest] Token acquired (len=%s)", len(token_or_err))
    result = _probe_jobs(token_or_err)
    result["elapsed_sec"] = round(time.time() - start, 3)
    result["run_id"] = int(start)
    # Keep the same top-level shape you’ve been logging
    return result

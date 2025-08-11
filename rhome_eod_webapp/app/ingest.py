# app/ingest.py
import os
import time
import logging
from typing import Dict, Any, Optional, List

import requests

log = logging.getLogger("ingest")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="INGEST %(levelname)s: %(message)s")

# -------- Config (via env) --------
TENANT_BASE = os.getenv("SIMPRO_TENANT_BASE", "https://rhome.simprosuite.com").rstrip("/")
TOKEN_URL = os.getenv("SIMPRO_TOKEN_URL", f"{TENANT_BASE}/oauth2/token")
CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET", "")

# Candidate API bases to probe. We'll prefer those that return $metadata.
PROBE_BASES = ["/api/v1.1", "/api/v1.0", "/api/v1", "/odata", "/api"]

# Likely entity set names for “jobs”-ish data. We’ll sniff $metadata first.
POSSIBLE_JOB_SETS: List[str] = [
    "JobHeaders", "Jobs", "JobHeader", "Job", "JobsHeaders", "Projects",
]

def _token() -> Optional[str]:
    """Get OAuth token using client credentials."""
    if not CLIENT_ID or not CLIENT_SECRET:
        log.error("[ingest] missing SIMPRO_CLIENT_ID or SIMPRO_CLIENT_SECRET")
        return None
    try:
        resp = requests.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(CLIENT_ID, CLIENT_SECRET),
            timeout=15,
        )
    except Exception as e:
        log.error("[ingest] token request failed: %s", e)
        return None
    if resp.status_code != 200:
        log.error("[ingest] auth_error:token_status_%s body=%s", resp.status_code, resp.text[:200])
        return None
    tok = resp.json().get("access_token")
    if not tok:
        log.error("[ingest] token missing in response")
    return tok

def _get(url: str, token: str, timeout: int = 15) -> requests.Response:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    return requests.get(url, headers=headers, timeout=timeout)

def _discover_api_base(token: str) -> str:
    """Try to find an API base by probing $metadata. Fall back to /api/v1.0."""
    for base in PROBE_BASES:
        meta_url = f"{TENANT_BASE}{base}/$metadata"
        try:
            r = _get(meta_url, token)
            if r.status_code == 200 and ("EntitySet" in r.text or "<edmx:" in r.text):
                log.info("[ingest] discovered API base via $metadata: %s", base)
                return base
        except Exception as e:
            log.warning("[ingest] metadata probe failed for %s: %s", base, e)
    log.warning("[ingest] API base discovery failed; defaulting to /api/v1.0")
    return "/api/v1.0"

def _find_jobs_endpoint(token: str, base: str) -> Optional[str]:
    """Use $metadata to find a likely jobs entity set, then verify with a $top=1 probe."""
    # First: $metadata scrape for entity set names
    meta_url = f"{TENANT_BASE}{base}/$metadata"
    r = _get(meta_url, token)
    text = r.text if r.status_code == 200 else ""

    # If metadata lists the entity set, try it; otherwise brute‑force through candidates.
    names_to_try = []
    if text:
        for name in POSSIBLE_JOB_SETS:
            if f'Name="{name}"' in text or f"Name='{name}'" in text:
                names_to_try.append(name)

    if not names_to_try:
        names_to_try = POSSIBLE_JOB_SETS

    for name in names_to_try:
        probe = f"{TENANT_BASE}{base}/{name}?$top=1"
        pr = _get(probe, token)
        if pr.status_code < 300:
            return f"{base}/{name}"

    return None

def ingest_live(budget_sec: int = 25) -> Dict[str, Any]:
    """
    Live ingest runner.
    - Auth
    - Discover API base
    - Discover jobs endpoint (entity set)
    - Return a small sample count or a clear probe note.
    """
    t0 = time.time()
    out = {"ok": False, "jobs_inserted": 0, "jobs_tried": 0, "elapsed_sec": 0.0}

    tok = _token()
    if not tok:
        out["note"] = "auth_error:token_failed"
        out["elapsed_sec"] = round(time.time() - t0, 3)
        return out

    base = _discover_api_base(tok)
    jobs_path = _find_jobs_endpoint(tok, base)
    if not jobs_path:
        out["note"] = "probe_404:no_jobs_endpoint_found"
        out["api_base"] = base
        out["elapsed_sec"] = round(time.time() - t0, 3)
        return out

    # Fetch a tiny sample (no DB writes yet; we’re just validating the endpoint)
    list_url = f"{TENANT_BASE}{jobs_path}?$top=5"
    r = _get(list_url, tok)
    if r.status_code >= 300:
        out["note"] = f"fetch_{r.status_code}"
        out["api_base"] = base
        out["endpoint"] = jobs_path
        out["elapsed_sec"] = round(time.time() - t0, 3)
        return out

    # OData often returns {"value": [...]}
    tried = 0
    try:
        data = r.json()
        if isinstance(data, dict) and isinstance(data.get("value"), list):
            tried = len(data["value"])
        elif isinstance(data, list):
            tried = len(data)
    except Exception:
        tried = 0

    out.update({
        "ok": True,
        "note": "sample_ok",
        "jobs_tried": tried,
        "api_base": base,
        "endpoint": jobs_path,
        "elapsed_sec": round(time.time() - t0, 3),
    })
    return out
    

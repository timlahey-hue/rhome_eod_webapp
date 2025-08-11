import os
import logging
from typing import Dict, Any, Optional

import httpx

log = logging.getLogger("ingest")

# ENV required:
#   SIMPRO_CLIENT_ID
#   SIMPRO_CLIENT_SECRET
#   SIMPRO_TENANT_BASE  -> e.g. https://rhome.simprosuite.com
#
# ENV strongly recommended (no more guessing):
#   SIMPRO_API_BASE     -> e.g. https://api-us.simprocloud.com/v1.0
#                          (include the /v1.0 segment)
#
# Optional:
#   SIMPRO_COMPANY_ID   -> numeric company id; if provided, we probe a scoped path


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v:
        v = v.strip()
    return v


def _auth_token() -> Dict[str, Any]:
    tenant_base = _env("SIMPRO_TENANT_BASE")
    cid = _env("SIMPRO_CLIENT_ID")
    csecret = _env("SIMPRO_CLIENT_SECRET")

    if not tenant_base or not cid or not csecret:
        return {
            "ok": False,
            "note": "missing_env_vars",
            "missing": {
                "SIMPRO_TENANT_BASE": bool(tenant_base),
                "SIMPRO_CLIENT_ID": bool(cid),
                "SIMPRO_CLIENT_SECRET": bool(csecret),
            },
        }

    token_url = f"{tenant_base.rstrip('/')}/oauth2/token"
    try:
        r = httpx.post(
            token_url,
            auth=(cid, csecret),
            data={"grant_type": "client_credentials"},
            timeout=20.0,
        )
        if r.status_code != 200:
            return {
                "ok": False,
                "note": f"token_status_{r.status_code}",
                "status": r.status_code,
                "body": r.text[:500],
            }
        data = r.json()
        token = data.get("access_token")
        if not token:
            return {"ok": False, "note": "token_missing_access_token", "body": data}
        return {"ok": True, "token": token, "status": r.status_code}
    except Exception as e:
        return {"ok": False, "note": "token_exception", "error": str(e)}


def _probe_jobs(api_base: str, token: str, company_id: Optional[str]) -> Dict[str, Any]:
    """
    Try ONE clear endpoint rather than many blind guesses.
    If company_id provided, try a scoped path first; else try top-level.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # Candidate endpoints (ordered, minimal):
    candidates = []
    if company_id:
        # Common shapes used by Simpro's OData-ish API
        candidates.extend([
            f"{api_base.rstrip('/')}/Companies({company_id})/Jobs?$top=1",
            f"{api_base.rstrip('/')}/companies({company_id})/jobs?$top=1",
        ])
    # Unscoped
    candidates.extend([
        f"{api_base.rstrip('/')}/Jobs?$top=1",
        f"{api_base.rstrip('/')}/jobs?$top=1",
        f"{api_base.rstrip('/')}/ServiceJobs?$top=1",
        f"{api_base.rstrip('/')}/Projects?$top=1",
    ])

    tried = []
    for url in candidates:
        tried.append(url)
        try:
            resp = httpx.get(url, headers=headers, timeout=20.0)
            if resp.status_code == 200:
                return {"ok": True, "status": 200, "url": url, "count_hint": 1}
            if resp.status_code in (401, 403):
                return {"ok": False, "note": f"auth_{resp.status_code}", "status": resp.status_code, "url": url}
            # Keep trying if 404/400/etc.
        except Exception as e:
            log.warning("probe exception for %s: %s", url, e)

    return {
        "ok": False,
        "note": "probe_404:no_jobs_endpoint_found",
        "status": 404,
        "tried": tried,
    }


def run_live_ingest() -> Dict[str, Any]:
    """
    A safe 'live ingest' that:
      1) Fetches a token (proves auth).
      2) Requires SIMPRO_API_BASE to be set; if not set, returns a clear error.
      3) Probes a minimal jobs-like endpoint, returns structured info.
    It never raises; it always returns JSON.
    """
    # 1) Token
    tok = _auth_token()
    if not tok.get("ok"):
        log.error("[ingest] %s", tok.get("note"))
        return {"ok": False, **tok}

    token = tok["token"]

    # 2) API base
    api_base = _env("SIMPRO_API_BASE")
    if not api_base:
        # Stop the loop of guessing. We need you to set this env var correctly.
        example = "https://api-us.simprocloud.com/v1.0"
        return {
            "ok": False,
            "note": "missing_SIMPRO_API_BASE",
            "message": "Set SIMPRO_API_BASE to your regional Simpro API base, including the version segment.",
            "example": example,
        }

    # 3) Try a very small probe
    company_id = _env("SIMPRO_COMPANY_ID")
    probe = _probe_jobs(api_base, token, company_id)

    result: Dict[str, Any] = {"ok": probe.get("ok", False), "run_id": int(time.time()) if hasattr(__import__('time'), 'time') else None}
    result.update(probe)
    # For now we don't insert anything unless you want to (see main.py)
    return result

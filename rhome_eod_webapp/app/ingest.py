# rhome_eod_webapp/app/ingest.py
# Drop-in ingest that uses only the Python standard library (no httpx).
# It fetches a token, then probes likely Simpro API paths and returns a clear JSON result,
# never raising to FastAPI (so you don't get a 500 if something's off).

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import ssl
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("ingest")

# ---- Config from environment ----
TENANT = os.getenv("SIMPRO_TENANT", "").strip() or os.getenv("SIMPRO_SUBDOMAIN", "").strip()
CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET", "").strip()
SCOPE = os.getenv("SIMPRO_SCOPE", "").strip()  # often not required
API_BASE = os.getenv("SIMPRO_API_BASE", "").strip()  # e.g. "/api/v1.0"
TIMEOUT = float(os.getenv("SIMPRO_TIMEOUT", "8"))
VERIFY_TLS = os.getenv("SIMPRO_VERIFY_TLS", "true").lower() != "false"  # allow disabling in emergencies

# ---- Simple HTTP helper (urllib) ----
def _http(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    timeout: float = TIMEOUT,
) -> Tuple[int, Dict[str, str], bytes, Optional[BaseException]]:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    ctx = ssl.create_default_context()
    if not VERIFY_TLS:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            status = resp.getcode()
            body = resp.read()
            return status, dict(resp.headers), body, None
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b""
        return e.code, dict(getattr(e, "headers", {}) or {}), body, e
    except Exception as e:
        return 0, {}, b"", e

# ---- OAuth2: client_credentials ----
def _fetch_token() -> Tuple[Optional[str], Optional[str]]:
    if not TENANT or not CLIENT_ID or not CLIENT_SECRET:
        return None, "auth_error:missing_env (require SIMPRO_TENANT, SIMPRO_CLIENT_ID, SIMPRO_CLIENT_SECRET)"
    token_url = f"https://{TENANT}.simprosuite.com/oauth2/token"
    form = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    if SCOPE:
        form["scope"] = SCOPE
    data = urllib.parse.urlencode(form).encode("utf-8")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    status, _, body, err = _http("POST", token_url, headers=headers, data=data)
    if status != 200:
        note = f"auth_error:token_status_{status}"
        log.error("[ingest] %s", note)
        return None, note
    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        log.error("[ingest] token response not JSON")
        return None, "auth_error:token_parse"
    token = payload.get("access_token")
    if not token:
        return None, "auth_error:no_access_token"
    return token, None

# ---- Probe for a usable endpoint ----
def _build_probe_urls(base_url: str) -> List[str]:
    """
    Build a list of 'lightweight' GETs to discover a jobs-like endpoint.
    We try a few API versions and entity names. If SIMPRO_API_BASE is set,
    we only probe under that.
    """
    candidates: List[str] = []
    versions = [API_BASE] if API_BASE else ["/api/v1.0", "/api/v1.1", "/api/v2.0", "/api/v2.1", "/api/v3.0"]
    entities = [
        "Jobs",
        "jobs",
        "ServiceJobs",
        "Projects",
        # OData-style nested guesses (company id is a guess; some tenants don't use this shape)
        "Companies(0)/Jobs",
        "companies(0)/jobs",
        "Companies(1)/Jobs",
        "companies(1)/jobs",
        "companies/0/jobs",
    ]
    for ver in versions:
        v = ver if ver.startswith("/") else f"/{ver}"
        for ent in entities:
            # Add $top=1 to keep it light
            candidates.append(f"{base_url}{v}/{ent}?$top=1")
    # Also, if someone set API_BASE to a non-/api path, make sure we didn't double slash
    return [u.replace("//", "/").replace("https:/", "https://") for u in candidates]

def _probe_jobs(token: str) -> Tuple[Optional[str], List[str], Optional[str]]:
    """
    Try a handful of likely endpoints; return first that gives 200,
    plus the list of all URLs we tried (for display), and an error note if none worked.
    """
    base = f"https://{TENANT}.simprosuite.com"
    tried: List[str] = []
    auth_headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    for url in _build_probe_urls(base):
        tried.append(url)
        status, _, body, _ = _http("GET", url, headers=auth_headers)
        if status == 200:
            return url, tried, None
        # 401/403 indicates token ok but permissions/feature off; still keep going
        # 404 just means "not found here", so keep probing
        # Any 5xx we'll also continue probing others
    return None, tried, "probe_404:no_jobs_endpoint_found"

# ---- Public entrypoint called by FastAPI ----
def run_live_ingest() -> Dict:
    """
    Do a tiny 'live ingest' test: obtain token, probe for a jobs-like endpoint.
    Never raises; always returns a small JSON result the UI can render.
    """
    started = time.time()
    run_id = int(started)  # simple stamp for logs

    try:
        log.info("[ingest] Starting live ingest (budget ~%ss)", TIMEOUT)
        token, token_err = _fetch_token()
        if token_err:
            return {
                "ok": False,
                "elapsed_sec": round(time.time() - started, 3),
                "jobs_inserted": 0,
                "jobs_tried": 0,
                "run_id": run_id,
                "note": token_err,
            }

        probe_url, tried, probe_err = _probe_jobs(token)
        if probe_url:
            # We found an endpoint — this is where you’d normally pull data.
            # For now we only prove connectivity.
            note = f"ok:probe_success:{probe_url}"
            log.info("[ingest] %s", note)
            return {
                "ok": True,
                "elapsed_sec": round(time.time() - started, 3),
                "jobs_inserted": 0,
                "jobs_tried": 1,
                "run_id": run_id,
                "note": note,
                "tried": tried,
            }
        else:
            # Couldn’t find a usable endpoint; return what we tried so you can see it in the UI.
            log.warning("[ingest] no jobs endpoint found; tried %s", ", ".join(tried))
            return {
                "ok": False,
                "elapsed_sec": round(time.time() - started, 3),
                "jobs_inserted": 0,
                "jobs_tried": 0,
                "run_id": run_id,
                "note": probe_err or "probe_failed",
                "tried": tried,
                "base_url": f"https://{TENANT}.simprosuite.com",
            }

    except Exception as e:
        # Absolute last resort — never bubble up to FastAPI.
        log.exception("ingest exception")
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - started, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": f"ingest_exception:{type(e).__name__}",
        }

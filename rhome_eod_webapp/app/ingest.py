# rhome_eod_webapp/app/ingest.py
import os
import time
import logging
from typing import Dict, Tuple, Optional, List

import requests

# --------------------------------------------------------------------
# Configuration (override via environment variables in Render)
# --------------------------------------------------------------------
TENANT_BASE = os.getenv("SIMPRO_BASE_URL", "https://rhome.simprosuite.com").rstrip("/")
TOKEN_URL = os.getenv("SIMPRO_TOKEN_URL", f"{TENANT_BASE}/oauth2/token")
API_BASE_URL = os.getenv("SIMPRO_API_BASE_URL", f"{TENANT_BASE}/api/v1.0").rstrip("/")

CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID")
CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET")
SCOPE = os.getenv("SIMPRO_SCOPE", "").strip()

# Try several low-impact endpoints; first 2xx/3xx ends the probe.
# You can override with SIMPRO_PROBE_ENDPOINTS="/Companies?$top=1,/Jobs?$top=1"
PROBE_ENDPOINTS: List[str] = [
    p.strip()
    for p in os.getenv(
        "SIMPRO_PROBE_ENDPOINTS",
        "/Companies?$top=1, /Customers?$top=1, /Jobs?$top=1",
    ).split(",")
    if p.strip()
]

HTTP_TIMEOUT = float(os.getenv("SIMPRO_HTTP_TIMEOUT", "20"))

logger = logging.getLogger("ingest")


# --------------------------------------------------------------------
# OAuth2: Client Credentials
# --------------------------------------------------------------------
def _get_access_token(session: Optional[requests.Session] = None) -> Dict[str, str]:
    """
    Get an OAuth2 access token using Client Credentials.
    Sends credentials both via HTTP Basic and in the body for compatibility.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError(
            "SIMPRO_CLIENT_ID / SIMPRO_CLIENT_SECRET are not set in the environment."
        )

    data = {
        "grant_type": "client_credentials",  # per Simpro key file
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    if SCOPE:
        data["scope"] = SCOPE

    sess = session or requests
    resp = sess.post(
        TOKEN_URL,
        data=data,  # x-www-form-urlencoded by default
        auth=(CLIENT_ID, CLIENT_SECRET),  # many servers accept either/both
        timeout=HTTP_TIMEOUT,
    )
    # Don't raise immediately; we want to log status codes clearly.
    if resp.status_code != 200:
        raise RuntimeError(
            f"Token request failed (status={resp.status_code}): {resp.text[:300]}"
        )
    tok = resp.json()
    if "access_token" not in tok:
        raise RuntimeError(f"Token response missing 'access_token': {tok}")
    return tok


# --------------------------------------------------------------------
# Probing + helpers
# --------------------------------------------------------------------
def _auth_headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": "rhome-eod-dashboard/1.0",
    }


def _abs_api_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return API_BASE_URL + path


def _probe_api(access_token: str, session: Optional[requests.Session] = None) -> Tuple[int, str]:
    """
    Hit a few benign endpoints until we receive a 2xx/3xx or a hard auth failure.
    Returns (status_code, detail).
    """
    sess = session or requests
    headers = _auth_headers(access_token)

    last_status = 0
    last_detail = "no_attempts"
    for endpoint in PROBE_ENDPOINTS:
        url = _abs_api_url(endpoint)
        try:
            r = sess.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            last_status = r.status_code
            if r.status_code in (200, 201, 202, 204, 301, 302):
                return r.status_code, f"probe_ok:{endpoint}"
            if r.status_code in (401, 403):
                # Hard stop: credentials/scope issue
                return r.status_code, f"probe_auth_failed:{endpoint}"
            # For 404/400 etc., try the next candidate
            last_detail = f"probe_{r.status_code}:{endpoint}"
        except Exception as e:
            last_status = 0
            last_detail = f"probe_exception:{endpoint}:{type(e).__name__}:{e}"
    return last_status, last_detail


# --------------------------------------------------------------------
# Public entrypoint used by FastAPI route in main.py
# --------------------------------------------------------------------
def ingest_live(budget_sec: int = 25) -> Dict[str, object]:
    """
    Run a quick, time-bounded ingest cycle.
    Right now this performs:
      1) OAuth2 client-credentials auth
      2) A lightweight API probe to validate the token + base URL
    If the probe succeeds, this is where you would pull data and write to DB.
    Returns a dict compatible with the existing /ingest/live route.
    """
    t0 = time.time()
    run_id = int(t0)
    jobs_tried = 0
    jobs_inserted = 0
    note = "init"
    ok = False

    logger.info("[ingest] Authenticating with Simpro")
    try:
        with requests.Session() as s:
            token = _get_access_token(s)
            access_token = token["access_token"]
            logger.info("[ingest] Token acquired (len=%s)", len(access_token))

            logger.info("[ingest] Starting live ingest (budget=%ss)", budget_sec)

            # --- quick probe to separate auth vs. endpoint issues ---
            status, detail = _probe_api(access_token, s)
            if status in (401, 403):
                # Clear signal we’re authenticated incorrectly or missing scopes
                logger.warning("[ingest] API probe returned %s (%s)", status, detail)
                note = f"api_status_{status}"
                ok = False
            elif status >= 400 or status == 0:
                logger.warning(
                    "[ingest] API probe returned non-success status=%s (%s)", status, detail
                )
                note = f"api_status_{status or 'unknown'}"
                ok = False
            else:
                # At this point we know the token works against at least one endpoint.
                # TODO: pull and upsert your real data here.
                note = detail
                ok = True

    except Exception as e:
        logger.exception("[ingest] ingest_live failed")
        note = f"exception:{type(e).__name__}"

    elapsed = round(time.time() - t0, 3)
    return {
        "ok": bool(ok),
        "elapsed_sec": elapsed,
        "jobs_inserted": int(jobs_inserted),
        "jobs_tried": int(jobs_tried),
        "run_id": run_id,
        "note": note,
    }

# app/ingest.py
import os
import time
import logging
from typing import Dict, Any, Optional

import requests

log = logging.getLogger("ingest")

def _env(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v is not None:
        v = v.strip()
        if v == "":
            v = None
    return v

def _build_token_url() -> Optional[str]:
    # Prefer explicit token URL if provided
    token_url = _env("SIMPRO_TOKEN_URL")
    if token_url:
        return token_url

    # Fallback: build from tenant base
    tenant = _env("SIMPRO_TENANT_BASE")
    if tenant:
        # Accept either bare host or full https URL
        if tenant.startswith("http://") or tenant.startswith("https://"):
            base = tenant.rstrip("/")
        else:
            base = f"https://{tenant}"
        return f"{base}/oauth2/token"

    return None

def ingest_live(budget_sec: int = 25) -> Dict[str, Any]:
    """
    Try to authenticate with Simpro using OAuth2 Client Credentials.
    Always returns a dict payload; never raises or returns tuples.
    """
    t0 = time.monotonic()
    run_id = int(time.time() * 1000) % 10_000_000

    token_url = _build_token_url()
    client_id = _env("SIMPRO_CLIENT_ID")
    client_secret = _env("SIMPRO_CLIENT_SECRET")

    # Basic validation of env vars
    missing = [k for k, v in [
        ("SIMPRO_CLIENT_ID", client_id),
        ("SIMPRO_CLIENT_SECRET", client_secret),
        ("SIMPRO_TOKEN_URL/SIMPRO_TENANT_BASE", token_url),
    ] if not v]

    if missing:
        note = f"missing_env:{','.join(missing)}"
        elapsed = round(time.monotonic() - t0, 3)
        log.error("[ingest] Missing required env: %s", missing)
        return {
            "ok": False,
            "elapsed_sec": elapsed,
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": note,
        }

    log.info("[ingest] Authenticating with Simpro")
    try:
        resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
    except requests.RequestException as e:
        elapsed = round(time.monotonic() - t0, 3)
        log.exception("[ingest] Token request failed: %s", e)
        return {
            "ok": False,
            "elapsed_sec": elapsed,
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "token_request_exception",
        }

    if resp.status_code != 200:
        # Most common here is 401 if client_secret is wrong/truncated
        elapsed = round(time.monotonic() - t0, 3)
        log.warning("[ingest] API token call returned %s", resp.status_code)
        return {
            "ok": False,
            "elapsed_sec": elapsed,
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": f"api_status_{resp.status_code}",
        }

    data = {}
    try:
        data = resp.json()
    except Exception:
        pass

    access_token = data.get("access_token")
    if not access_token:
        elapsed = round(time.monotonic() - t0, 3)
        log.warning("[ingest] No access_token in response")
        return {
            "ok": False,
            "elapsed_sec": elapsed,
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "no_access_token",
        }

    # If you want to probe a specific API endpoint, set SIMPRO_PROBE_URL
    probe_url = _env("SIMPRO_PROBE_URL")
    if probe_url:
        log.info("[ingest] Probing API endpoint")
        try:
            r = requests.get(
                probe_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=15,
            )
            if r.status_code != 200:
                elapsed = round(time.monotonic() - t0, 3)
                log.warning("[ingest] Probe returned %s", r.status_code)
                return {
                    "ok": False,
                    "elapsed_sec": elapsed,
                    "jobs_inserted": 0,
                    "jobs_tried": 1,
                    "run_id": run_id,
                    "note": f"probe_status_{r.status_code}",
                }
        except requests.RequestException as e:
            elapsed = round(time.monotonic() - t0, 3)
            log.exception("[ingest] Probe failed: %s", e)
            return {
                "ok": False,
                "elapsed_sec": elapsed,
                "jobs_inserted": 0,
                "jobs_tried": 1,
                "run_id": run_id,
                "note": "probe_exception",
            }

    # Success (at least for token). You can expand here to fetch jobs.
    elapsed = round(time.monotonic() - t0, 3)
    log.info("[ingest] ingest_live finished in %.2fs (ok=True, jobs=0, tried=0)", elapsed)
    return {
        "ok": True,
        "elapsed_sec": elapsed,
        "jobs_inserted": 0,
        "jobs_tried": 0,
        "run_id": run_id,
        "note": "token_ok",
    }

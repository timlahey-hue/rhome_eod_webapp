import os
import time
import logging
import requests
import xml.etree.ElementTree as ET

logger = logging.getLogger("ingest")

SIMPRO_BASE_URL = os.environ.get("SIMPRO_BASE_URL", "https://rhome.simprosuite.com")
SIMPRO_TOKEN_URL = os.environ.get("SIMPRO_TOKEN_URL", f"{SIMPRO_BASE_URL}/oauth2/token")
SIMPRO_CLIENT_ID = os.environ.get("SIMPRO_CLIENT_ID", "")
SIMPRO_CLIENT_SECRET = os.environ.get("SIMPRO_CLIENT_SECRET", "")

def _json_result(ok: bool, note: str, t0: float, **kw):
    out = {"ok": ok, "note": note, "elapsed_sec": round(time.time() - t0, 3), "run_id": int(t0)}
    out.update(kw)
    return out

def _get_token():
    t0 = time.time()
    if not SIMPRO_CLIENT_ID or not SIMPRO_CLIENT_SECRET:
        return _json_result(False, "missing_env:SIMPRO_CLIENT_ID_or_SIMPRO_CLIENT_SECRET", t0)

    try:
        # EXACTLY like your working curl: Basic auth + grant_type=client_credentials
        r = requests.post(
            SIMPRO_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(SIMPRO_CLIENT_ID, SIMPRO_CLIENT_SECRET),
            timeout=30,
        )
    except Exception as e:
        logger.exception("[ingest] token request failed")
        return _json_result(False, "token_request_failed", t0, error=str(e))

    if r.status_code != 200:
        logger.error("[ingest] auth_error:token_status_%s %s", r.status_code, r.text)
        return _json_result(False, f"auth_error:token_status_{r.status_code}", t0)

    token = r.json().get("access_token")
    return _json_result(True, "token_ok", t0, token=token)

def _probe_candidates(session: requests.Session, candidates):
    """
    Try to discover an API base by probing for OData metadata or an index that returns 200.
    Returns (base_path, metadata_text_or_None).
    """
    for base in candidates:
        # Prefer $metadata if it's an OData service
        for tail in ("/$metadata", "/"):
            url = SIMPRO_BASE_URL + base + tail
            try:
                resp = session.get(url, timeout=15)
            except Exception:
                continue
            if resp.status_code == 200:
                return base, resp.text if tail == "/$metadata" else None
    return None, None

def _discover_jobs_entity(metadata_xml: str | None):
    if not metadata_xml:
        return None
    try:
        root = ET.fromstring(metadata_xml)
    except Exception:
        return None
    # Try to find an EntitySet with "job" in its name
    for es in root.findall(".//{*}EntitySet"):
        name = es.attrib.get("Name", "")
        if "job" in name.lower():
            return name  # e.g., "Jobs", "JobHeaders", etc.
    return None

def _get(session: requests.Session, path: str):
    return session.get(SIMPRO_BASE_URL + path, timeout=30)

def ingest_diag():
    """
    Lightweight diagnostics: verifies token, tries to discover base, and guesses a Jobs-like entity.
    """
    t0 = time.time()
    tok = _get_token()
    if not tok["ok"]:
        return tok

    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {tok['token']}"})

    base_candidates = ["/api/v1.0", "/api/v2.0", "/odata/v4", "/api", ""]
    base, meta = _probe_candidates(s, base_candidates)
    info = {
        "ok": True,
        "note": "diag_ok",
        "elapsed_sec": round(time.time() - t0, 3),
        "base_detected": base,
        "has_metadata": bool(meta),
    }
    if meta:
        info["jobs_entity_guess"] = _discover_jobs_entity(meta)
    return info

def ingest_live():
    """
    Try to locate a working Jobs-like endpoint and probe it. If none found,
    returns a structured diagnostic instead of failing.
    """
    t0 = time.time()
    tok = _get_token()
    if not tok["ok"]:
        return tok

    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {tok['token']}"})

    base_candidates = ["/api/v1.0", "/api/v2.0", "/odata/v4", "/api", ""]
    base, meta = _probe_candidates(s, base_candidates)
    tried = []

    if not base:
        # Keep behavior you saw in logs for transparency
        tried.append("/api/v1.0/Jobs")
        logger.warning("[ingest] API base discovery failed; defaulting to /api/v1.0")
        base = "/api/v1.0"

    # If we have metadata, try to auto-find any EntitySet with 'job' in the name
    jobs_entity = _discover_jobs_entity(meta) if meta else None
    if jobs_entity:
        path = f"{base}/{jobs_entity}"
        tried.append(path)
        r = _get(s, f"{path}?$top=1")
        if r.status_code == 200:
            return _json_result(True, "probe_ok", t0, jobs_tried=1, jobs_inserted=0, path=path)

    # Fall back to common guesses (case/shape variants)
    for path in [
        f"{base}/Jobs",
        f"{base}/jobs",
        f"{base}/Companies(0)/Jobs",
        f"{base}/companies/0/jobs",
        f"{base}/companies(1)/jobs",
    ]:
        tried.append(path)
        r = _get(s, f"{path}?$top=1")
        if r.status_code == 200:
            return _json_result(True, "probe_ok_path", t0, jobs_tried=1, jobs_inserted=0, path=path)

    logger.warning("[ingest] no jobs endpoint found; tried: %s", ", ".join(tried))
    return _json_result(False, "probe_404:no_jobs_endpoint_found", t0, jobs_tried=0, jobs_inserted=0, tried=tried)

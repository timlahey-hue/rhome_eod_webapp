# ingest.py
import os
import time
import json
import logging
import base64
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

# ---------- logging ----------
logger = logging.getLogger("ingest")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("INGEST %(levelname)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

REQ_TIMEOUT = (5, 20)  # (connect, read)

# ---------- env ----------
BASE_URL = os.getenv("SIMPRO_BASE_URL", "").strip().rstrip("/")
TOKEN_URL = os.getenv("SIMPRO_TOKEN_URL", "").strip()
CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET", "").strip()
SCOPE = os.getenv("SIMPRO_SCOPE", "api").strip()

# Optional hints/overrides
API_BASE_HINT = os.getenv("SIMPRO_API_BASE", "").strip().rstrip("/")    # e.g. "/api" or "/api/v1.0"
JOBS_ENTITY_HINT = os.getenv("SIMPRO_JOBS_ENTITY", "").strip()          # e.g. "Jobs"

# Reasonable service-root candidates observed across Simpro builds
DEFAULT_BASE_CANDIDATES = [
    "/api/v1.0",
    "/api",               # many tenants expose OData here
    "/odata",
    "/api/v1",            # just in case
    "/v1.0",
]

# ---------- helpers ----------
def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

def _require_env(name: str, value: str):
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

def _ensure_urls():
    _require_env("SIMPRO_BASE_URL", BASE_URL or "")
    # If TOKEN_URL not supplied, infer from tenant base
    if not TOKEN_URL:
        # Typical pattern from your key file:
        #   https://<tenant>.simprosuite.com/oauth2/token
        return f"{BASE_URL}/oauth2/token"
    return TOKEN_URL

def _get_token() -> str:
    token_url = _ensure_urls()
    logger.info("[ingest] Authenticating with Simpro")
    # Client Credentials grant. Simpro accepts either Basic auth or client_id/client_secret in form.
    # Prefer Basic for interoperability.
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials", "scope": SCOPE}
    resp = requests.post(token_url, headers=headers, data=data, timeout=REQ_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"token_status_{resp.status_code}")
    token = resp.json().get("access_token", "")
    if not token:
        raise RuntimeError("token_missing")
    logger.info("[indigest? nope][ingest] Token acquired (len=%d)", len(token))
    return token

def _fetch(url: str, headers: dict, params=None, accept_xml=False):
    h = dict(headers)
    if accept_xml:
        h["Accept"] = "application/xml, text/xml;q=0.9, */*;q=0.1"
    return requests.get(url, headers=h, params=params, timeout=REQ_TIMEOUT)

def _discover_service_root(base_url: str, token: str):
    """
    Try a series of service-root candidates and return:
        (service_root_url, entity_sets: set[str], raw_metadata_xml)
    or (None, None, None) if not found.
    """
    tried = []
    candidates = []
    if API_BASE_HINT:
        candidates.append(API_BASE_HINT)  # user override first
    candidates += DEFAULT_BASE_CANDIDATES

    for base in candidates:
        service_root = f"{base_url}{base}"
        meta_url = f"{service_root}/$metadata"
        try:
            r = _fetch(meta_url, _bearer(token), accept_xml=True)
            tried.append((base, r.status_code))
            if r.status_code == 200 and r.text:
                try:
                    # Parse entity sets from OData $metadata
                    entity_sets = set()
                    # OData namespaces vary; do a simple name search
                    root = ET.fromstring(r.text)
                    # Look for all <EntitySet Name="...">
                    for elem in root.iter():
                        if elem.tag.endswith("EntitySet") and "Name" in elem.attrib:
                            entity_sets.add(elem.attrib["Name"])
                    if entity_sets:
                        return service_root, entity_sets, r.text, tried
                    else:
                        # Metadata present but weird? Still return so we can inspect.
                        return service_root, set(), r.text, tried
                except ET.ParseError:
                    # Not XML; ignore and keep looking
                    pass
        except requests.RequestException:
            tried.append((base, "error"))
            continue
    return None, None, None, tried

def _choose_jobs_entity(entity_sets: set[str]) -> str | None:
    if not entity_sets:
        return None
    # Preferred names in order; extend if we learn your tenant uses a variant
    preferences = [
        "Jobs", "JobHeaders", "Job", "JobsV1", "ProjectJobs"
    ]
    # Honor explicit override
    if JOBS_ENTITY_HINT:
        return JOBS_ENTITY_HINT if JOBS_ENTITY_HINT in entity_sets else None
    for name in preferences:
        if name in entity_sets:
            return name
    # As a fallback, try a case-insensitive match for "job"
    lowered = {e.lower(): e for e in entity_sets}
    for key in ["jobs", "jobheaders", "job"]:
        if key in lowered:
            return lowered[key]
    return None

def _probe_jobs(service_root: str, token: str, jobs_entity: str):
    # Confirm the entity set exists by asking for one record
    probe_url = f"{service_root}/{jobs_entity}"
    r = _fetch(probe_url, _bearer(token), params={"$top": 1})
    return r.status_code, r.text[:4000] if r.text else ""

# ---------- public entry ----------
def ingest_live(budget_sec: int = 25) -> dict:
    """
    Returns a dict shaped like:
      {
        "ok": bool,
        "elapsed_sec": float,
        "jobs_inserted": int,
        "jobs_tried": int,
        "run_id": int,
        "note": str
      }
    We currently focus on service discovery + probe; once the path is confirmed,
    we’ll expand to actual ingestion.
    """
    t0 = time.time()
    run_id = int(t0)
    jobs_inserted = 0
    jobs_tried = 0
    note_parts = []

    try:
        token = _get_token()
        note_parts.append("auth_ok")
    except Exception as e:
        msg = f"auth_error:{e}"
        logger.error("[ingest] %s", msg)
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": jobs_inserted,
            "jobs_tried": jobs_tried,
            "run_id": run_id,
            "note": msg,
        }

    # Discover base
    service_root, entity_sets, metadata, tried = _discover_service_root(BASE_URL, token)
    if not service_root:
        logger.warning("[ingest] API base discovery failed; tried=%s", tried)
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": jobs_inserted,
            "jobs_tried": jobs_tried,
            "run_id": run_id,
            "note": "no_odata_metadata;" + json.dumps(tried),
        }

    base_path = service_root.replace(BASE_URL, "", 1) or "/"
    note_parts.append(f"base={base_path}")

    # Choose the likely jobs entity
    jobs_entity = _choose_jobs_entity(entity_sets or set())
    if not jobs_entity:
        logger.warning("[ingest] No jobs-like entity found in metadata; sets=%s", sorted(list(entity_sets or [])))
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": jobs_inserted,
            "jobs_tried": jobs_tried,
            "run_id": run_id,
            "note": "no_jobs_entity_in_metadata",
        }

    note_parts.append(f"jobs_set={jobs_entity}")

    # Probe it
    status, body = _probe_jobs(service_root, token, jobs_entity)
    if status != 200:
        logger.warning("[ingest] API probe returned non-success status=%s (path=%s/%s)", status, base_path, jobs_entity)
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": jobs_inserted,
            "jobs_tried": jobs_tried,
            "run_id": run_id,
            "note": f"probe_{status}:{base_path}/{jobs_entity}",
        }

    # If we get here, we can see the Jobs entity set. (Do real ingest next)
    logger.info("[ingest] Jobs endpoint confirmed at %s/%s", base_path, jobs_entity)

    return {
        "ok": True,
        "elapsed_sec": round(time.time() - t0, 3),
        "jobs_inserted": jobs_inserted,  # will be >0 once we implement inserts
        "jobs_tried": jobs_tried,
        "run_id": run_id,
        "note": ";".join(note_parts) or "ok",
    }

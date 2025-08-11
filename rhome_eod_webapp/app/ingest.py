import os
import re
import time
import logging
import httpx
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

LOG = logging.getLogger("ingest")
if not LOG.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="INGEST %(levelname)s: %(message)s"
    )

# --------- Helpers ---------

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    return v.strip()

def _bearer(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}

def _short(s: str, n: int = 140) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "…"

def _is_xml_ok(text: str) -> bool:
    try:
        ET.fromstring(text)
        return True
    except Exception:
        return False

# --------- OAuth ---------

def _get_token(base_url: str, client_id: str, client_secret: str) -> Tuple[bool, str, Optional[str]]:
    """
    Returns (ok, note, token)
      note examples:
        'token_ok'
        'auth_error:token_status_400'
        'auth_error:exception'
    """
    url = base_url.rstrip("/") + "/oauth2/token"
    data = {"grant_type": "client_credentials"}
    # Basic auth EXACTLY like: curl -u "<id>:<secret>"
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(url, data=data, auth=(client_id, client_secret))
            if resp.status_code != 200:
                LOG.error("[ingest] token request failed status=%s body=%s",
                          resp.status_code, _short(resp.text))
                return False, f"auth_error:token_status_{resp.status_code}", None
            j = resp.json()
            token = j.get("access_token")
            if not token:
                LOG.error("[ingest] token response missing access_token body=%s", _short(resp.text))
                return False, "auth_error:no_access_token", None
            LOG.info("[ingest] Token acquired (len=%d)", len(token))
            return True, "token_ok", token
    except Exception as e:
        LOG.exception("[ingest] exception requesting token")
        return False, "auth_error:exception", None

# --------- OData Discovery ---------

BASE_CANDIDATES = [
    "/odata/v1.0",
    "/api/v1.0",
    "/OData/v1.0",
    "/odata",
    "/api",
]

def _fetch_metadata(base_url: str, base_path: str, token: str) -> Tuple[int, str]:
    url = base_url.rstrip("/") + base_path + "/$metadata"
    headers = {
        **_bearer(token),
        "Accept": "application/xml, text/xml;q=0.9, */*;q=0.1",
    }
    with httpx.Client(timeout=15.0, headers=headers) as client:
        r = client.get(url)
        return r.status_code, r.text

def _discover_entity_sets(xml_text: str) -> List[str]:
    """
    Parse OData $metadata for EntitySet names.
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []
    names = []
    for elem in root.iter():
        if elem.tag.endswith("EntitySet"):
            name = elem.attrib.get("Name")
            if name:
                names.append(name)
    return names

def _choose_jobs_like(entity_sets: List[str]) -> Optional[str]:
    """
    Choose an entity set that likely represents jobs.
    Preference order: exact 'Jobs', then names containing 'Job' (case-insensitive).
    """
    if "Jobs" in entity_sets:
        return "Jobs"
    candidates = [n for n in entity_sets if re.search(r"job", n, re.I)]
    return candidates[0] if candidates else None

def _discover_base_and_jobs(base_url: str, token: str) -> Tuple[Optional[str], Optional[str], List[str], Dict[str, str]]:
    """
    Try candidate base paths; return (base_path, jobs_set, all_sets, meta_notes)
    meta_notes may include {base_path: "status_200"/"status_404"/"xml_invalid"} to help troubleshooting.
    """
    notes: Dict[str, str] = {}
    for base_path in BASE_CANDIDATES:
        status, text = _fetch_metadata(base_url, base_path, token)
        if status == 200 and _is_xml_ok(text):
            sets = _discover_entity_sets(text)
            if sets:
                jobs_set = _choose_jobs_like(sets)
                notes[base_path] = f"status_200 entity_sets={len(sets)}"
                return base_path, jobs_set, sets, notes
            notes[base_path] = "status_200 but no_entity_sets_found"
        else:
            notes[base_path] = f"status_{status}" if status else "status_unknown"
    return None, None, [], notes

# --------- Probing ---------

def _probe(client: httpx.Client, url: str) -> Tuple[int, Optional[Dict]]:
    try:
        r = client.get(url)
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j
    except Exception:
        return 0, None

def _count_from_payload(j: Optional[Dict]) -> Optional[int]:
    if not isinstance(j, dict):
        return None
    # OData typical shapes
    if "@odata.count" in j and isinstance(j["@odata.count"], int):
        return int(j["@odata.count"])
    if "value" in j and isinstance(j["value"], list):
        return len(j["value"])
    return None

# --------- Public entrypoint ---------

def ingest_live(budget_sec: int = 25) -> Dict:
    """
    Called by /ingest/live route. Returns a result dict with keys:
      ok, elapsed_sec, note, jobs_inserted, jobs_tried, run_id
      plus debugging fields when discovery fails.
    """
    t0 = time.time()
    run_id = int(t0)  # simple unique-ish id

    base_url = _env("SIMPRO_BASE_URL") or "https://rhome.simprosuite.com"
    client_id = _env("SIMPRO_CLIENT_ID")
    client_secret = _env("SIMPRO_CLIENT_SECRET")
    company_id = _env("SIMPRO_COMPANY_ID")  # optional

    if not client_id or not client_secret:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "auth_error:missing_client_env",
        }

    # 1) Token
    ok, note, token = _get_token(base_url, client_id, client_secret)
    if not ok or not token:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": note,
        }

    # 2) Discover base + entity sets via $metadata
    base_path, jobs_set, all_sets, meta_notes = _discover_base_and_jobs(base_url, token)
    if not base_path:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "metadata_discovery_failed",
            "discovery": meta_notes,
        }

    if not jobs_set:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "no_jobs_entity_in_metadata",
            "entity_sets": all_sets[:50],  # surface what we found
            "base_path": base_path,
        }

    # 3) Probe candidates
    headers = {
        **_bearer(token),
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }
    base = base_url.rstrip("/") + base_path
    urls = [
        f"{base}/{jobs_set}?$top=1&$count=true",
    ]
    if company_id and company_id.isdigit():
        urls.append(f"{base}/Companies({company_id})/{jobs_set}?$top=1&$count=true")

    tried = 0
    with httpx.Client(timeout=20.0, headers=headers) as client:
        for u in urls:
            tried += 1
            status, payload = _probe(client, u)
            if status == 200:
                cnt = _count_from_payload(payload)
                return {
                    "ok": True,
                    "elapsed_sec": round(time.time() - t0, 3),
                    "jobs_inserted": 0,  # not inserting yet; this is the connectivity gate
                    "jobs_tried": tried,
                    "run_id": run_id,
                    "note": f"probe_ok:{u}",
                    "found_count": cnt,
                    "base_path": base_path,
                    "entity_set": jobs_set,
                }
            elif status in (401, 403):
                return {
                    "ok": False,
                    "elapsed_sec": round(time.time() - t0, 3),
                    "jobs_inserted": 0,
                    "jobs_tried": tried,
                    "run_id": run_id,
                    "note": f"authz_error:status_{status}",
                    "probe_url": u,
                }
            elif status == 404:
                LOG.warning("[ingest] API probe returned 404 (%s)", u)
                # keep trying next URL
                last_404 = u
            else:
                LOG.warning("[ingest] API probe returned non-success status=%s (%s)", status, u)
                last_other = (status, u)

    # If we reached here, none succeeded
    return {
        "ok": False,
        "elapsed_sec": round(time.time() - t0, 3),
        "jobs_inserted": 0,
        "jobs_tried": tried,
        "run_id": run_id,
        "note": "probe_failed_no_jobs_endpoint_found",
        "base_path": base_path,
        "entity_sets": all_sets[:50],
    }

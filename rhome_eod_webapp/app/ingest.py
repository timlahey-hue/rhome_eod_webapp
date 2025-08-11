# app/ingest.py
import os, time, logging, requests
from typing import Optional, Tuple, Dict, Any

log = logging.getLogger("ingest")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s:ingest:%(message)s"))
    log.addHandler(handler)
log.setLevel(logging.INFO)

# --------- Environment ----------
BASE_DOMAIN = (os.getenv("SIMPRO_BASE_URL")
               or os.getenv("SIMPRO_BASE_DOMAIN")
               or "https://rhome.simprosuite.com").rstrip("/")
TOKEN_URL = (os.getenv("SIMPRO_TOKEN_URL")
             or f"{BASE_DOMAIN}/oauth2/token")

CLIENT_ID = os.getenv("SIMPRO_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("SIMPRO_CLIENT_SECRET", "").strip()
SCOPE = os.getenv("SIMPRO_SCOPE", "").strip() or None

TIMEOUT = 20

class SimproClient:
    def __init__(self, base_domain: str, client_id: str, client_secret: str, scope: Optional[str]):
        self.base_domain = base_domain.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self._access_token: Optional[str] = None
        self._token_expiry = 0.0
        self.session = requests.Session()

    # --------- Auth ----------
    def token(self) -> str:
        now = time.time()
        if self._access_token and (self._token_expiry - now) > 60:
            return self._access_token

        log.info("[ingest] Authenticating with Simpro")
        data = {"grant_type": "client_credentials"}
        if self.scope:
            data["scope"] = self.scope

        # Try with HTTP Basic first (many OAuth servers prefer this)
        try:
            r = self.session.post(
                TOKEN_URL,
                data=data,
                auth=(self.client_id, self.client_secret),
                timeout=TIMEOUT,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if r.status_code >= 400:
                # Fallback: put client creds in form body
                data["client_id"] = self.client_id
                data["client_secret"] = self.client_secret
                r = self.session.post(
                    TOKEN_URL,
                    data=data,
                    timeout=TIMEOUT,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
            r.raise_for_status()
        except Exception as e:
            code = getattr(getattr(e, "response", None), "status_code", "n/a")
            raise RuntimeError(f"token_error:{code}") from e

        payload = r.json()
        tok = payload.get("access_token")
        if not tok:
            raise RuntimeError("token_missing_access_token")
        self._access_token = tok
        self._token_expiry = now + float(payload.get("expires_in") or 3600)
        log.info("[ingest] Token acquired (len=%s)", len(tok))
        return tok

    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token()}"}

    # --------- Discovery ----------
    def discover_api_base(self) -> Tuple[str, str]:
        """
        Try well-known prefixes and probe /Info to validate.
        Returns (prefix, note) e.g. ("/api/v1.0", "ok") or ("", "api_status_404")
        """
        for prefix in ("/api/v1.0", "/v1.0", ""):
            url = f"{self.base_domain}{prefix}/Info"
            try:
                r = self.session.get(url, headers=self.headers(), timeout=TIMEOUT)
            except Exception:
                continue
            if r.status_code == 401:
                return prefix, "api_status_401"
            if 200 <= r.status_code < 300:
                return prefix, "ok"
        return "", "api_status_404"

    def probe_jobs(self, api_prefix: str) -> Tuple[Optional[str], str, int]:
        """
        Try a few likely Jobs paths + both query styles.
        Returns (jobs_path, note, http_status)
        """
        candidates = [
            f"{api_prefix}/Jobs",
            f"{api_prefix}/jobs",
            f"{api_prefix}/Projects/Jobs",
        ]
        for path in candidates:
            base = f"{self.base_domain}{path}"
            # Try pageSize first
            r = self.session.get(base, headers=self.headers(), params={"pageSize": 1}, timeout=TIMEOUT)
            if r.status_code == 200:
                return path, "ok", 200
            if r.status_code == 401:
                return None, "api_status_401", 401
            # Try OData-style $top
            r2 = self.session.get(base, headers=self.headers(), params={"$top": 1}, timeout=TIMEOUT)
            if r2.status_code == 200:
                return path, "ok", 200
            if r2.status_code == 401:
                return None, "api_status_401", 401
            # Keep the most recent non-200 code for logging
            last_status = r2.status_code if r2 is not None else r.status_code
        return None, "api_status_404", last_status if "last_status" in locals() else 404

    # --------- Fetch ----------
    def fetch_jobs_sample(self, jobs_path: str) -> Dict[str, Any]:
        url = f"{self.base_domain}{jobs_path}"
        # Prefer pageSize; fallback to $top
        r = self.session.get(url, headers=self.headers(), params={"pageSize": 1}, timeout=TIMEOUT)
        if r.status_code == 404:
            r = self.session.get(url, headers=self.headers(), params={"$top": 1}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()

# --------- Public entrypoint used by FastAPI route ----------
def ingest_live(budget_sec: int = 25) -> Dict[str, Any]:
    t0 = time.time()
    run_id = int(t0)
    jobs_inserted = 0
    jobs_tried = 0

    # Basic env validation
    if not CLIENT_ID or not CLIENT_SECRET:
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "missing_env:SIMPRO_CLIENT_ID_or_SIMPRO_CLIENT_SECRET",
        }

    client = SimproClient(BASE_DOMAIN, CLIENT_ID, CLIENT_SECRET, SCOPE)
    log.info("[ingest] Starting live ingest (budget=%ss)", budget_sec)

    # Discover API base
    try:
        api_prefix, base_note = client.discover_api_base()
    except RuntimeError as e:
        note = str(e)
        log.warning("[ingest] token acquisition failed (%s)", note)
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": note,
        }

    if base_note == "api_status_401":
        log.warning("[ingest] API base probe returned 401 (check Simpro scopes / allowed grant types)")
        return {
            "ok": False, "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0, "jobs_tried": 0, "run_id": run_id,
            "note": "api_status_401"
        }

    if base_note == "api_status_404":
        # Could not discover /Info; try the most common base anyway
        log.warning("[ingest] API base discovery failed; defaulting to /api/v1.0")
        api_prefix = "/api/v1.0"

    # Probe Jobs
    jobs_path, jobs_note, http_status = client.probe_jobs(api_prefix)
    if jobs_note != "ok":
        probe_hint = f"{api_prefix}/Jobs"
        log.warning("[ingest] API probe returned non-success status=%s (probe_%s:%s)",
                    http_status, http_status, probe_hint if jobs_path is None else jobs_path)
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": f"probe_{http_status}:{(jobs_path or probe_hint)}",
        }

    # Fetch a small sample to confirm we can read Jobs
    try:
        payload = client.fetch_jobs_sample(jobs_path)
        # Shape can vary; try a few common patterns
        items = payload.get("items") or payload.get("data") or payload.get("results") or payload
        if isinstance(items, dict):
            items = [items]
        jobs_tried = len(items) if isinstance(items, list) else 0
    except Exception:
        log.exception("[ingest] Jobs fetch failed")
        return {
            "ok": False,
            "elapsed_sec": round(time.time() - t0, 3),
            "jobs_inserted": 0,
            "jobs_tried": 0,
            "run_id": run_id,
            "note": "jobs_fetch_failed",
        }

    # (Optional) insert into DB here if needed. For now we confirm connectivity only.
    return {
        "ok": True,
        "elapsed_sec": round(time.time() - t0, 3),
        "jobs_inserted": jobs_inserted,
        "jobs_tried": jobs_tried,
        "run_id": run_id,
        "note": f"ok:{jobs_path}",
    }

if __name__ == "__main__":
    # Quick local smoke test
    print(ingest_live())

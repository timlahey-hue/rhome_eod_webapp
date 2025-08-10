import requests

def get_access_token(base_url: str, client_id: str, client_secret: str) -> str:
  r = requests.post(
    f"{base_url}/oauth2/token",
    data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    timeout=20
  )
  r.raise_for_status()
  return r.json()["access_token"]

def list_companies(base_url: str, token: str):
  h = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
  r = requests.get(f"{base_url}/api/v1.0/companies", headers=h, timeout=30)
  r.raise_for_status()
  return r.json()

def list_jobs_modified_since(base_url: str, token: str, company_id, since_dt):
  ims = since_dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
  h = {"Authorization": f"Bearer {token}", "Accept": "application/json", "If-Modified-Since": ims}
  url = f"{base_url}/api/v1.0/companies/{company_id}/jobs?pageSize=250"
  r = requests.get(url, headers=h, timeout=60)
  if r.status_code == 304:
    return []
  r.raise_for_status()
  return r.json()

import logging
import requests

log = logging.getLogger("simpro")

def get_token(base_url: str, client_id: str, client_secret: str) -> str | None:
    """
    Fetch an OAuth2 access token using the client-credentials grant.
    Returns the access_token string or None on failure.
    """
    url = base_url.rstrip("/") + "/oauth2/token"
    form = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    try:
        r = requests.post(url, data=form, timeout=20)
        r.raise_for_status()
        payload = r.json()
        token = payload.get("access_token") or payload.get("accessToken") or payload.get("token")
        if not token:
            log.error("Token response missing access_token: %s", payload)
            return None
        return token
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", "?")
        log.error("Token HTTP error %s from %s", code, url)
    except Exception as e:
        log.exception("Token request failed: %s", e)
    return None

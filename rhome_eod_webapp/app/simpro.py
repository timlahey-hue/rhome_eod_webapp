# app/simpro.py
import requests
import logging

log = logging.getLogger("simpro")

def get_token(base_url: str, client_id: str, client_secret: str, timeout: int = 20) -> str:
    base = base_url.rstrip("/")
    url = f"{base}/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    r = requests.post(url, data=data, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    tok = j.get("access_token", "")
    if not tok:
        raise RuntimeError("Simpro OAuth: no access_token in response")
    return tok

class Client:
    """
    Very small wrapper around the single-job endpoint that works on this tenant:
      GET /api/v1.0/companies/{companyId}/jobs/{jobId}
    """
    def __init__(self, base_url: str, token: str, timeout: int = 25):
        self.base = base_url.rstrip("/")
        self.sess = requests.Session()
        self.sess.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })
        self.timeout = timeout

    def get_job(self, company_id: int, job_id: int):
        url = f"{self.base}/api/v1.0/companies/{int(company_id)}/jobs/{int(job_id)}"
        r = self.sess.get(url, timeout=self.timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

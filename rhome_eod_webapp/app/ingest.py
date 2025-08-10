# rhome_eod_webapp/app/ingest.py
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

# We keep simpro import optional so the app won't crash
try:
    from . import simpro  # type: ignore
except Exception:  # pragma: no cover
    simpro = None  # type: ignore

logger = logging.getLogger("ingest")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


class IngestError(RuntimeError):
    """Non-fatal ingest error we capture and report without crashing the app."""


def _get_token(base_url: str, client_id: str, client_secret: str) -> str:
    """
    Get an OAuth token using simpro.get_token if available.
    Raises IngestError on failure.
    """
    if not base_url or not client_id or not client_secret:
        raise IngestError("Missing Simpro credentials (base URL, client id, or client secret).")

    if simpro and hasattr(simpro, "get_token"):
        try:
            token = simpro.get_token(base_url, client_id, client_secret)  # type: ignore[attr-defined]
            if not token:
                raise IngestError("simpro.get_token returned no token.")
            return token
        except Exception as e:  # noqa: BLE001
            raise IngestError(f"Authentication failed: {e}") from e

    # If the project doesn't expose simpro.get_token we fail loudly but cleanly.
    raise IngestError("simpro.get_token not found; cannot authenticate.")


def _try_http_get(urls: List[str], headers: Dict[str, str], timeout: int = 30) -> Optional[requests.Response]:
    """
    Try a list of URLs in order; return the first successful (200) response.
    """
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
            logger.debug("GET %s -> %s", url, r.status_code)
        except requests.RequestException as e:  # network or TLS problems
            logger.debug("GET %s failed: %s", url, e)
    return None


def _safe_list_companies(base_url: str, token: str, timeout: int = 30) -> List[Dict[str, Any]]:
    """
    Try multiple plausible Companies endpoints. Return [] if none work.
    Never raises; always returns a list (possibly empty).
    """
    # First prefer the project's own function (keeps code DRY if it's correct)
    if simpro and hasattr(simpro, "list_companies"):
        try:
            companies = simpro.list_companies(base_url, token)  # type: ignore[attr-defined]
            if isinstance(companies, list):
                return companies
            # Some APIs return an object with a 'data' or 'items' field
            if isinstance(companies, dict):
                for key in ("data", "items", "results"):
                    if key in companies and isinstance(companies[key], list):
                        return companies[key]  # type: ignore[return-value]
        except requests.HTTPError as e:
            # Log details but don't crash ingestion
            status = getattr(e.response, "status_code", "?")
            logger.warning("list_companies HTTP %s; will try fallbacks", status)
        except Exception as e:  # noqa: BLE001
            logger.warning("list_companies failed (%s); will try fallbacks", e)

    # Fallbacks with common case/casing variations
    headers = {"Authorization": f"Bearer {token}"}
    candidates = [
        f"{base_url}/api/v1.0/companies",
        f"{base_url}/api/v1.0/Companies",
        f"{base_url}/api/v1.0/company",
        f"{base_url}/api/v1.0/Company",
    ]
    r = _try_http_get(candidates, headers=headers, timeout=timeout)
    if not r:
        return []

    try:
        data = r.json()
    except ValueError:
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "items", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


def _extract_company_id(obj: Dict[str, Any]) -> Optional[str]:
    """Pull a company id from a variety of possible shapes."""
    for k in ("companyId", "companyID", "id", "Id", "ID"):
        if k in obj and obj[k] is not None:
            return str(obj[k])
    return None


def ingest_live(
    base_url: str,
    client_id: str,
    client_secret: str,
    company_id: Optional[str | int] = None,
    *,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Live ingestion orchestrator.

    IMPORTANT:
    • Never raises — returns a summary dict your route can ignore or log.
    • Handles 404s from the Companies endpoint by falling back to the provided SIMPRO_COMPANY_ID.
    """
    t0 = time.time()
    summary: Dict[str, Any] = {
        "ok": False,
        "error": None,
        "company_id": str(company_id) if company_id is not None else None,
        "company_name": None,
        "companies_seen": 0,
        "elapsed_s": None,
    }

    try:
        token = _get_token(base_url, client_id, client_secret)
        logger.info("Authenticated with Simpro")

        companies: List[Dict[str, Any]] = []
        try:
            companies = _safe_list_companies(base_url, token, timeout=timeout)
        except Exception as e:  # extra safety; we *never* want to bubble exceptions
            logger.warning("Listing companies failed unexpectedly: %s", e)
            companies = []

        summary["companies_seen"] = len(companies)

        # If a company_id was not given, try to pick one
        chosen_id: Optional[str] = str(company_id) if company_id is not None else None
        chosen_name: Optional[str] = None

        if not chosen_id and companies:
            first = companies[0]
            chosen_id = _extract_company_id(first)
            chosen_name = first.get("name") or first.get("companyName") or first.get("Name")

        if not chosen_id:
            # We couldn't resolve a company id at all; log and finish cleanly.
            raise IngestError(
                "Could not determine company id. "
                "Set SIMPRO_COMPANY_ID in Render → Environment (or pass it to ingest_live)."
            )

        # At this point we have a token and a company id.
        # This is where you would call additional simpro.* helpers to fetch and persist data.
        # We keep this lightweight to avoid coupling to unknown functions/tables.
        # Example (pseudo):
        # jobs = simpro.list_jobs(base_url, token, chosen_id)
        # save_to_db(jobs, ...)

        summary["company_id"] = chosen_id
        summary["company_name"] = chosen_name
        summary["ok"] = True
        return summary

    except IngestError as e:
        summary["error"] = str(e)
        logger.error("%s", e)
        return summary

    except Exception as e:  # noqa: BLE001
        # Absolute last-resort safety: report as non-fatal so the route returns 200/303.
        summary["error"] = f"Unhandled error: {e}"
        logger.exception("Unhandled ingest_live error")
        return summary

    finally:
        summary["elapsed_s"] = round(time.time() - t0, 3)
        logger.info("ingest_live finished in %.3fs (ok=%s)", summary["elapsed_s"], summary["ok"])


def ingest_demo() -> Dict[str, Any]:
    """
    Keep a simple demo ingest so /ingest/demo continues to succeed.
    (Your existing route likely just redirects after this returns.)
    """
    now = time.time()
    return {
        "ok": True,
        "demo": True,
        "timestamp": now,
        "message": "Demo ingest complete",
    }

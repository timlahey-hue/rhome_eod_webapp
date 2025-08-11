"""
Auto-register Jinja helper functions so templates like:
  {{ fmt_currency(value) }} and {{ value | fmt_pct }}
work without modifying main.py or the templates.

Drop this file in at: rhome_eod_webapp/app/__init__.py
"""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

try:
    # Starlette/ FastAPI templating
    from starlette.templating import Jinja2Templates  # type: ignore
except Exception:  # pragma: no cover
    Jinja2Templates = None  # type: ignore


# ------------------------
# Safe number coerce
# ------------------------
def _to_number(x: Any) -> float | None:
    if x is None:
        return None
    try:
        # Accept Decimal, int, float, str
        if isinstance(x, Decimal):
            x = float(x)
        elif isinstance(x, (int, float)):
            x = float(x)
        else:
            # strings (or other) -> Decimal -> float
            x = float(Decimal(str(x)))
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except (ValueError, TypeError, InvalidOperation):
        return None


# ------------------------
# Jinja helpers (callable and filter-friendly)
# ------------------------
def fmt_currency(x: Any, decimals: int = 0, symbol: str = "") -> str:
    """
    Minimal/forgiving currency-ish formatter.
    Default has no currency symbol so we "ignore currency" (no $/£),
    which avoids debate about which symbol to show.
    """
    n = _to_number(x)
    if n is None:
        return "—"
    fmt = f"{{:,.{decimals}f}}"
    return f"{symbol}{fmt.format(n)}" if symbol else fmt.format(n)


def fmt_pct(x: Any, digits: int = 1) -> str:
    """
    If the value looks like a ratio (<= 1), treat it as 0..1 and multiply by 100.
    Otherwise assume it's already a percent value (e.g., 37.5).
    """
    n = _to_number(x)
    if n is None:
        return "—"
    value = n * 100 if abs(n) <= 1 else n
    return f"{value:.{digits}f}%"


def fmt_int(x: Any) -> str:
    n = _to_number(x)
    if n is None:
        return "0"
    return f"{int(round(n)):,}"


def fmt_date(x: Any, fmt: str = "%Y-%m-%d") -> str:
    if x is None:
        return "—"
    if isinstance(x, (datetime, date)):
        dt = x
    else:
        # Very forgiving: try to parse common cases
        s = str(x).strip()
        # Try ISO-like first
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            # Fallback: just return as given if not parseable
            return s
    if isinstance(dt, datetime):
        return dt.strftime(fmt)
    return datetime(dt.year, dt.month, dt.day).strftime(fmt)


# ------------------------
# Auto-register on Jinja2Templates creation
# ------------------------
def _install_helpers_on_templates():
    if Jinja2Templates is None:
        return

    original_init = Jinja2Templates.__init__

    def patched_init(self, *args, **kwargs):  # type: ignore
        original_init(self, *args, **kwargs)
        helpers = {
            "fmt_currency": fmt_currency,
            "fmt_pct": fmt_pct,
            "fmt_int": fmt_int,
            "fmt_date": fmt_date,
        }
        # Make them available BOTH as globals (callable functions)
        # and as filters (so you can use value|fmt_pct).
        self.env.globals.update(helpers)
        self.env.filters.update(helpers)
        try:
            print("Jinja helpers registered on Jinja2Templates")
        except Exception:
            pass

    # Only patch once
    if getattr(Jinja2Templates.__init__, "_jinja_helpers_patched", False) is not True:
        patched_init._jinja_helpers_patched = True  # type: ignore[attr-defined]
        Jinja2Templates.__init__ = patched_init  # type: ignore

_install_helpers_on_templates()

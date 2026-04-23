"""
db_helpers.py — shared DB / book-parsing utilities.

Small helpers used across many fetch_* / build_* functions:

  _parse_rf, _parse_pm      Extract risk-factor and PM prefix from MACRO BOOK
                            names (e.g. 'JD_RF-BZ_Direcional' → ('RF-BZ', 'JD')).
  _prev_bday                Previous business day as a 'YYYY-MM-DD' string.
  fetch_all_latest_navs     Bulk-prime the NAV cache in one SQL query.
  _latest_nav               Most recent NAV ≤ date for a single desk, with cache.
  _NAV_CACHE                Module-level {(desk, date_str): nav} dict. Shared
                            by all callers that import from this module.
"""
import pandas as pd

from glpg_fetch import read_sql
from risk_config import ALL_FUNDS


# ── MACRO BOOK-name parsers ───────────────────────────────────────────────────
def _parse_rf(book: str) -> str:
    """Extract risk factor from BOOK name like 'JD_RF-BZ_Direcional' → 'RF-BZ'."""
    PMS = ("CI", "LF", "JD", "RJ", "QM", "MD")
    parts = book.split("_")
    if len(parts) >= 2 and parts[0] in PMS:
        return parts[1]
    return None   # structural book


def _parse_pm(book: str) -> str:
    """Extract PM prefix from BOOK name like 'JD_RF-BZ_Direcional' → 'JD'."""
    PMS = ("CI", "LF", "JD", "RJ", "QM", "MD")
    parts = book.split("_")
    if len(parts) >= 1 and parts[0] in PMS:
        return parts[0]
    return "Outros"


# ── Date helpers ──────────────────────────────────────────────────────────────
def _prev_bday(date_str: str) -> str:
    d = pd.Timestamp(date_str)
    return (d - pd.tseries.offsets.BusinessDay(1)).strftime("%Y-%m-%d")


# ── NAV cache (bulk prime + single lookup) ────────────────────────────────────
# Module-level cache for latest-NAV lookups; populated by fetch_all_latest_navs
# or on-demand by _latest_nav. Key: (desk, date_str).
_NAV_CACHE: dict = {}


def fetch_all_latest_navs(date_str: str) -> dict:
    """Bulk-fetch latest NAV (on or before date_str) for all known funds in one query.
       Side effect: populates the module-level _NAV_CACHE so subsequent _latest_nav
       calls hit memory instead of the DB. Returns the {desk: nav} dict as well.
    """
    desks = list(ALL_FUNDS.keys())
    # Include MACRO family names that aren't in ALL_FUNDS under the desk key we use at call sites
    extras = ["Galapagos Macro FIM", "GALAPAGOS ALBATROZ FIRF LP",
              "Frontier A\u00e7\u00f5es FIC FI"]
    desks = list({*desks, *extras})
    tds = ", ".join(f"'{d}'" for d in desks)
    df = read_sql(f"""
        SELECT DISTINCT ON ("TRADING_DESK") "TRADING_DESK", "NAV"
        FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" IN ({tds})
          AND "VAL_DATE" <= DATE '{date_str}'
        ORDER BY "TRADING_DESK", "VAL_DATE" DESC
    """)
    out = {}
    for r in df.itertuples(index=False):
        v = float(r.NAV) if pd.notna(r.NAV) else None
        _NAV_CACHE[(r.TRADING_DESK, date_str)] = v
        out[r.TRADING_DESK] = v
    # Ensure every requested desk has a sentinel entry (avoids re-querying on misses)
    for d in desks:
        _NAV_CACHE.setdefault((d, date_str), None)
    return out


def _latest_nav(desk: str, date_str: str):
    """
    Most recent NAV on or before `date_str` for `desk`. Hits _NAV_CACHE first;
    falls back to a direct query only for desks/dates not warmed up.
    """
    key = (desk, date_str)
    if key in _NAV_CACHE:
        return _NAV_CACHE[key]
    df = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = '{desk}'
          AND "VAL_DATE" <= DATE '{date_str}'
        ORDER BY "VAL_DATE" DESC LIMIT 1
    """)
    v = float(df["NAV"].iloc[0]) if not df.empty else None
    _NAV_CACHE[key] = v
    return v

"""
latest_bday.py
==============
Prints the latest business day (ANBIMA calendar) with data available in GLPG-DB01.
Falls back to weekday-backward from today if the DB query fails.

Usage:
    python latest_bday.py     # prints YYYY-MM-DD to stdout
"""
from __future__ import annotations

import sys
import warnings

import pandas as pd

warnings.filterwarnings("ignore")


def _fallback() -> str:
    d = pd.Timestamp("today").normalize() - pd.tseries.offsets.BusinessDay(1)
    return d.strftime("%Y-%m-%d")


def latest_bday() -> str:
    try:
        from glpg_fetch import read_sql
        df = read_sql(
            'SELECT MAX("VAL_DATE") AS d FROM "LOTE45"."LOTE_FUND_STRESS_RPM"'
        )
        d = df.iloc[0, 0]
        if d is None:
            return _fallback()
        return pd.Timestamp(d).strftime("%Y-%m-%d")
    except Exception:
        return _fallback()


if __name__ == "__main__":
    try:
        print(latest_bday())
    except Exception:
        print(_fallback())
        sys.exit(0)

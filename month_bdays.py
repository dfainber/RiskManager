"""
month_bdays.py — prints all VAL_DATEs with data in LOTE_FUND_STRESS_RPM for a given month.

Usage:
    python month_bdays.py 2026-04     # prints one YYYY-MM-DD per line
    python month_bdays.py             # defaults to current month
"""
from __future__ import annotations
import sys
import warnings
import pandas as pd

warnings.filterwarnings("ignore")


def month_bdays(ym: str) -> list[str]:
    year, month = int(ym[:4]), int(ym[5:7])
    first = f"{year:04d}-{month:02d}-01"
    last_day = pd.Timestamp(year, month, 1) + pd.offsets.MonthEnd(0)
    last = last_day.strftime("%Y-%m-%d")
    try:
        from glpg_fetch import read_sql
        df = read_sql(
            f'SELECT DISTINCT "VAL_DATE" FROM "LOTE45"."LOTE_FUND_STRESS_RPM" '
            f"WHERE \"VAL_DATE\" >= DATE '{first}' AND \"VAL_DATE\" <= DATE '{last}' "
            f'ORDER BY "VAL_DATE"'
        )
        return [pd.Timestamp(d).strftime("%Y-%m-%d") for d in df.iloc[:, 0]]
    except Exception:
        # Fallback: weekdays only (no holiday awareness)
        dates = pd.bdate_range(first, last)
        return [d.strftime("%Y-%m-%d") for d in dates]


if __name__ == "__main__":
    ym = sys.argv[1] if len(sys.argv) > 1 else pd.Timestamp("today").strftime("%Y-%m")
    for d in month_bdays(ym):
        print(d)

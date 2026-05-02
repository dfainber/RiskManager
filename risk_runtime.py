"""
risk_runtime.py — shared runtime state for the Risk Monitor kit.

Holds the target date (DATA_STR et al) computed from CLI args at import time,
plus the output directory. Kept in its own module so extracted `fetch_*` /
`compute_*` helpers can use `date_str: str = DATA_STR` as a default without
forcing a circular import back into `generate_risk_report.py`.
"""
import re
import sys
from pathlib import Path

import pandas as pd


def _parse_date_arg(s: str) -> str:
    """Validate a CLI date string. Must be YYYY-MM-DD and a real calendar date."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        sys.exit(f"Error: date must be YYYY-MM-DD, got {s!r}")
    try:
        pd.Timestamp(s)
    except ValueError:
        sys.exit(f"Error: invalid date {s!r}")
    return s


# Brazilian national holidays (B3 trading-day calendar). Hardcoded to keep
# risk_runtime DB-free; extend yearly. Carnaval / Sexta da Paixão / Corpus
# Christi derive from Easter — recomputed if a year is missing here.
_BR_HOLIDAYS: set[str] = {
    # 2024
    "2024-01-01", "2024-02-12", "2024-02-13", "2024-03-29", "2024-04-21",
    "2024-05-01", "2024-05-30", "2024-09-07", "2024-10-12", "2024-11-02",
    "2024-11-15", "2024-11-20", "2024-12-25",
    # 2025
    "2025-01-01", "2025-03-03", "2025-03-04", "2025-04-18", "2025-04-21",
    "2025-05-01", "2025-06-19", "2025-09-07", "2025-10-12", "2025-11-02",
    "2025-11-15", "2025-11-20", "2025-12-25",
    # 2026
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-04-03", "2026-04-21",
    "2026-05-01", "2026-06-04", "2026-09-07", "2026-10-12", "2026-11-02",
    "2026-11-15", "2026-11-20", "2026-12-25",
    # 2027
    "2027-01-01", "2027-02-08", "2027-02-09", "2027-03-26", "2027-04-21",
    "2027-05-01", "2027-05-27", "2027-09-07", "2027-10-12", "2027-11-02",
    "2027-11-15", "2027-11-20", "2027-12-25",
}


def _resolve_default_data_date() -> str:
    """Walk back from today (calendar) to the most recent BR trading day.

    Pre-fix: ``today − BusinessDay(1)`` returned the last weekday and silently
    landed on a holiday (e.g. 2026-05-02 Saturday → 2026-05-01 Labor Day, no
    data). Now skips weekends + ``_BR_HOLIDAYS`` entries.
    """
    d = pd.Timestamp("today").normalize()
    for _ in range(15):  # safety bound — won't realistically loop > 5 days
        d = d - pd.tseries.offsets.BusinessDay(1)
        if d.strftime("%Y-%m-%d") not in _BR_HOLIDAYS:
            return d.strftime("%Y-%m-%d")
    return d.strftime("%Y-%m-%d")  # gave up — return last attempted


# sys.argv[1] is consumed only when it looks like YYYY-MM-DD. Without this guard,
# any consumer that imports risk_runtime transitively (~all modules via data_fetch)
# crashes via sys.exit when its own argparse uses a non-date positional/flag —
# e.g. `pnl_server.py --port 5050` or `generate_credit_report.py --fund SEA_LION`.
_argv1 = sys.argv[1] if len(sys.argv) > 1 else None
DATA_STR = _parse_date_arg(_argv1) if (_argv1 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", _argv1)) else _resolve_default_data_date()
DATA     = pd.Timestamp(DATA_STR)
DATE_1Y  = DATA - pd.DateOffset(years=1)

OUT_DIR = Path(__file__).parent / "data" / "morning-calls"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def fmt_br_num(s: str) -> str:
    """Convert en-US number separators to pt-BR (e.g. '1,234.5' → '1.234,5')."""
    return s.replace(",", "_").replace(".", ",").replace("_", ".")

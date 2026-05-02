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


# sys.argv[1] is consumed only when it looks like YYYY-MM-DD. Without this guard,
# any consumer that imports risk_runtime transitively (~all modules via data_fetch)
# crashes via sys.exit when its own argparse uses a non-date positional/flag —
# e.g. `pnl_server.py --port 5050` or `generate_credit_report.py --fund SEA_LION`.
_argv1 = sys.argv[1] if len(sys.argv) > 1 else None
DATA_STR = _parse_date_arg(_argv1) if (_argv1 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", _argv1)) else (
    (pd.Timestamp("today") - pd.tseries.offsets.BusinessDay(1)).strftime("%Y-%m-%d")
)
DATA     = pd.Timestamp(DATA_STR)
DATE_1Y  = DATA - pd.DateOffset(years=1)

OUT_DIR = Path(__file__).parent / "data" / "morning-calls"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def fmt_br_num(s: str) -> str:
    """Convert en-US number separators to pt-BR (e.g. '1,234.5' → '1.234,5')."""
    return s.replace(",", "_").replace(".", ",").replace("_", ".")

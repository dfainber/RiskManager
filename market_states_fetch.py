"""Loads results from the standalone Market States project (separate daily batch).

The Market States project lives at `F:\\Bloomberg\\Quant\\MODELOS\\DFF\\Markets states\\`
and writes a pickle of all engine outputs to `regime_panel/artifacts_glpg.pkl`.
Kit consumes the pickle read-only — it does NOT recompute. If the pickle is
missing or older than `MARKET_STATES_MAX_AGE_DAYS` (default 7), this module
returns an empty dict so the kit renders a "stale" placeholder rather than
crash.

Override the artifacts path via `MARKET_STATES_ARTIFACTS_PATH` env var.

Output shape consumed by the renderers:
    {
        "asof":          "2026-04-30",
        "stale_days":    int,
        "regime":        "Q2",
        "regime_name":   "Reflation",
        "confidence":    0.98,
        "scores":        {"Q1": 2.0, "Q2": 6.0, "Q3": 0.0, "Q4": -8.0},
        "by_duration":   {"Trade": "Q2", "Trend": "Q3", "Tail": "Q2"},
        "duration_days": {"Trade": 20,   "Trend": 63,   "Tail": 252},
        "confluence":    2,           # 1..3 — number of durations matching modal
        "agree_all":     False,
        "risk_state":    "FEAR",      # EXTREME_FEAR / FEAR / NEUTRAL / GREED / EXTREME_GREED
        "ror_z":         -0.70,       # raw RoR z-score (sign: positive = risk-on)
        "ror_history":   [{"date": "...", "z": ...}, ...],   # last ~63 obs for sparkline
        "regime_history":[{"date": "...", "regime": "Q2"}, ...],   # last ~10 obs
    }
"""
from __future__ import annotations

import os
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_DEFAULT_ARTIFACTS = Path(r"F:\Bloomberg\Quant\MODELOS\DFF\Markets states\regime_panel\artifacts_glpg.pkl")
_MAX_AGE_DAYS = int(os.environ.get("MARKET_STATES_MAX_AGE_DAYS", "7"))

_QUAD_NAME = {
    "Q1": "Goldilocks",
    "Q2": "Reflation",
    "Q3": "Stagflation",
    "Q4": "Deflation",
}


def _artifacts_path() -> Path:
    return Path(os.environ.get("MARKET_STATES_ARTIFACTS_PATH", str(_DEFAULT_ARTIFACTS)))


def fetch_market_states_snapshot() -> dict[str, Any]:
    """Load the latest Market States artifacts pickle and return a clean snapshot.

    Returns {} if the pickle is missing, unreadable, or older than the max-age
    threshold (so the kit renders a placeholder rather than crash).
    """
    p = _artifacts_path()
    if not p.exists():
        print(f"  [market-states] artifacts not found at {p}; skipping section.")
        return {}

    # The pickle contains custom classes from the regime_panel package — its
    # parent dir must be on sys.path for unpickling to succeed.
    if str(p.parent) not in sys.path:
        sys.path.insert(0, str(p.parent))

    try:
        with open(p, "rb") as f:
            art = pickle.load(f)
    except Exception as e:
        print(f"  [market-states] pickle load failed ({e}); skipping section.")
        return {}

    try:
        ror_state = art["ror_state"]
        ror = art["ror"]
        confluence = art["vams_out"]["confluence"]
        durations = art["DURATIONS"]
        regime_today = str(art["discrete_regime_today"])
        conf_today = float(art["discrete_conf_today"])
        scores = {k: float(v) for k, v in art["discrete_scores_today"].items()}
        regime_hist = art["discrete_regime"]
    except KeyError as e:
        print(f"  [market-states] expected key missing ({e}); skipping section.")
        return {}

    asof = ror_state.index[-1]
    asof_date = asof.date() if hasattr(asof, "date") else asof
    stale_days = (datetime.now().date() - asof_date).days
    if stale_days > _MAX_AGE_DAYS:
        print(f"  [market-states] pickle stale by {stale_days}d (threshold {_MAX_AGE_DAYS}); skipping section.")
        return {}

    last_conf_row = confluence.iloc[-1]
    by_dur = {d: str(last_conf_row[d]) for d in ("Trade", "Trend", "Tail") if d in last_conf_row.index}

    # Mini history series for sparkline + regime-track strip
    ror_tail = ror.tail(63).dropna()
    ror_history = [
        {"date": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx), "z": float(v)}
        for idx, v in ror_tail.items()
    ]
    regime_tail = regime_hist.tail(20).dropna()
    regime_history = [
        {"date": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx), "regime": str(v)}
        for idx, v in regime_tail.items()
    ]

    return {
        "asof":           asof_date.isoformat() if hasattr(asof_date, "isoformat") else str(asof_date),
        "stale_days":     stale_days,
        "regime":         regime_today,
        "regime_name":    _QUAD_NAME.get(regime_today, regime_today),
        "confidence":     conf_today,
        "scores":         scores,
        "by_duration":    by_dur,
        "duration_days":  {k: int(v) for k, v in durations.items()},
        "confluence":     int(last_conf_row.get("confluence", 0)),
        "agree_all":      bool(last_conf_row.get("agree_all", False)),
        "risk_state":     str(ror_state.iloc[-1]),
        "ror_z":          float(ror.iloc[-1]),
        "ror_history":    ror_history,
        "regime_history": regime_history,
    }

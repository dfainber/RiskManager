"""Tests for metrics — pure stat helpers (DB-touching helpers excluded)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from metrics import (
    compute_pm_hs_var,
    compute_portfolio_vol_regime,
    compute_top_windows,
    compute_distribution_stats,
    compute_pa_outliers,
)


# ── compute_pm_hs_var ──────────────────────────────────────────────────────

def test_pm_hs_var_empty_returns_empty_dict():
    assert compute_pm_hs_var({}) == {}
    assert compute_pm_hs_var(None) == {}


def test_pm_hs_var_too_short_skips_pm():
    """PMs with < 21 obs should not appear in output."""
    short = np.random.RandomState(0).randn(15) * 5
    out = compute_pm_hs_var({"CI": short, "LF": [], "JD": None, "RJ": short})
    assert out == {}  # all below threshold


def test_pm_hs_var_returns_z_times_sigma():
    """v21 should equal z * std(w[-21:]) with z=1.645 default."""
    rng = np.random.RandomState(42)
    w = rng.randn(252) * 10  # σ ≈ 10 bps
    out = compute_pm_hs_var({"CI": w})
    assert "CI" in out
    expected_v21 = 1.645 * float(np.std(w[-21:], ddof=1))
    assert out["CI"]["v21"] == pytest.approx(expected_v21)
    expected_v252 = 1.645 * float(np.std(w[-252:], ddof=1))
    assert out["CI"]["v252"] == pytest.approx(expected_v252)
    assert out["CI"]["n"] == 252


def test_pm_hs_var_worst_is_positive_magnitude():
    """worst = -min(W) when min < 0; reported as positive number of bps lost."""
    w = np.array([-5, -10, -3, -50, 2, 1, 0] + [0] * 30)
    out = compute_pm_hs_var({"LF": w})
    assert out["LF"]["worst"] == 50.0


def test_pm_hs_var_no_loss_worst_zero():
    """All-positive series → worst = 0."""
    w = np.array([1.0] * 30)
    out = compute_pm_hs_var({"JD": w})
    assert out["JD"]["worst"] == 0.0


def test_pm_hs_var_drops_nans():
    """NaNs in w are filtered before σ computation."""
    rng = np.random.RandomState(1)
    w = rng.randn(50)
    w_with_nan = np.concatenate([w, [np.nan, np.nan, np.nan]])
    out_clean = compute_pm_hs_var({"CI": w})
    out_nan   = compute_pm_hs_var({"CI": w_with_nan})
    assert out_clean["CI"]["v21"] == pytest.approx(out_nan["CI"]["v21"])


def test_pm_hs_var_only_known_pms_returned():
    """Only CI/LF/JD/RJ keys are processed."""
    w = np.random.RandomState(0).randn(50)
    out = compute_pm_hs_var({"CI": w, "OTHER": w, "FAKE": w})
    assert set(out.keys()) <= {"CI", "LF", "JD", "RJ"}
    assert "OTHER" not in out


# ── compute_portfolio_vol_regime ──────────────────────────────────────────

def test_vol_regime_empty_input():
    assert compute_portfolio_vol_regime({}) == {}


def test_vol_regime_too_short_skipped():
    """len(w) < vol_window + 30 → skipped."""
    out = compute_portfolio_vol_regime({"MACRO": np.random.randn(40)}, vol_window=21)
    assert "MACRO" not in out


def test_vol_regime_recent_above_full_marks_elevated():
    """Last 21d much hotter than full window → ratio > 1, high pct_rank."""
    rng = np.random.RandomState(0)
    calm = rng.randn(200) * 5
    hot  = rng.randn(40)  * 30
    w = np.concatenate([calm, hot])
    out = compute_portfolio_vol_regime({"MACRO": w})
    row = out["MACRO"]
    assert row["ratio"] > 1.5
    assert row["pct_rank"] is not None
    assert row["regime"] in ("elevated", "stressed")
    assert row["vol_recent_pct"] > row["vol_full_pct"]


def test_vol_regime_constant_series_skipped():
    """Zero-vol full window → skipped (avoid div-by-zero)."""
    out = compute_portfolio_vol_regime({"X": np.zeros(100)})
    assert out == {}


def test_vol_regime_keys_complete():
    """Returned dict should expose all expected keys."""
    rng = np.random.RandomState(0)
    w = rng.randn(150) * 10
    out = compute_portfolio_vol_regime({"X": w})
    expected_keys = {
        "vol_recent_pct", "vol_full_pct", "ratio", "pct_rank", "regime",
        "n_obs", "n_roll", "vol_min_pct", "vol_max_pct",
        "vol_p25_pct", "vol_p50_pct", "vol_p75_pct",
        "z", "z_min", "z_max", "z_p25", "z_p50", "z_p75",
    }
    assert expected_keys.issubset(out["X"].keys())


# ── compute_top_windows ───────────────────────────────────────────────────

def test_top_windows_short_series_returns_none():
    assert compute_top_windows(None) is None
    assert compute_top_windows([1, 2, 3], window_days=21) is None


def test_top_windows_returns_disjoint_picks():
    """Picks must not overlap with previously picked windows."""
    rng = np.random.RandomState(0)
    w = rng.randn(200)
    out = compute_top_windows(w, k=5, window_days=21)
    assert out is not None
    for results in (out["worst"], out["best"]):
        ranges = [(r["start_idx"], r["end_idx"]) for r in results]
        for i, (s1, e1) in enumerate(ranges):
            for s2, e2 in ranges[i+1:]:
                assert e1 < s2 or e2 < s1, f"overlap: {(s1,e1)} ∩ {(s2,e2)}"


def test_top_windows_worst_has_lowest_sums():
    """Worst windows should have lower sum_bps than best windows."""
    rng = np.random.RandomState(2)
    w = rng.randn(200)
    out = compute_top_windows(w, k=3, window_days=21)
    if out["worst"] and out["best"]:
        assert max(r["sum_bps"] for r in out["worst"]) <= min(r["sum_bps"] for r in out["best"])


def test_top_windows_n_back_consistent():
    """n_back = n-1 - end_idx; window with end_idx == n-1 → n_back == 0."""
    w = np.arange(50, dtype=float)
    out = compute_top_windows(w, k=2, window_days=10)
    for r in out["worst"] + out["best"]:
        assert r["n_back"] == r["end_idx"] * (-1) + (out["n_obs"] - 1)


# ── compute_distribution_stats ────────────────────────────────────────────

def test_distribution_stats_short_returns_none():
    assert compute_distribution_stats(None) is None
    assert compute_distribution_stats([1, 2, 3]) is None  # < 30


def test_distribution_stats_returns_full_payload():
    rng = np.random.RandomState(0)
    w = rng.randn(252)
    out = compute_distribution_stats(w)
    for k in ("n", "min", "max", "mean", "sd", "var95", "var_p95"):
        assert k in out
    assert out["n"] == 252
    assert out["min"] == pytest.approx(float(np.min(w)))
    assert out["max"] == pytest.approx(float(np.max(w)))
    assert out["sd"]  == pytest.approx(float(np.std(w, ddof=1)))


def test_distribution_stats_with_actual_computes_percentile():
    w = np.linspace(-10, 10, 100)
    out = compute_distribution_stats(w, actual_bps=0.0)
    assert "actual" in out and "percentile" in out and "nvols" in out
    # 0 sits at roughly the 50th percentile of [-10,10]
    assert 45 <= out["percentile"] <= 55


def test_distribution_stats_var95_is_5th_pct():
    w = np.linspace(-100, 100, 1000)
    out = compute_distribution_stats(w)
    # 5th percentile of [-100, 100] linspace ≈ -90
    assert out["var95"] == pytest.approx(np.percentile(w, 5))


def test_distribution_stats_filters_nans():
    w = np.concatenate([np.random.randn(100), [np.nan, np.nan]])
    out = compute_distribution_stats(w)
    assert out["n"] == 100  # NaNs excluded


# ── compute_pa_outliers ───────────────────────────────────────────────────

def _build_pa_daily(today: str, n_history: int = 30, big_loss_today: bool = True) -> pd.DataFrame:
    """Synthetic df_daily with one stable product and one product with a big move today."""
    today_ts = pd.Timestamp(today)
    rng = np.random.RandomState(0)
    rows = []
    history_dates = pd.date_range(end=today_ts - pd.Timedelta(days=1), periods=n_history)
    for d in history_dates:
        rows.append({"FUNDO": "MACRO", "LIVRO": "PM_X", "PRODUCT": "STABLE",
                     "DATE": d, "dia_bps": float(rng.randn() * 0.5), "position_brl": 1_000_000.0})
        rows.append({"FUNDO": "MACRO", "LIVRO": "PM_X", "PRODUCT": "VOLATILE",
                     "DATE": d, "dia_bps": float(rng.randn() * 5.0), "position_brl": 1_000_000.0})
    big = -50.0 if big_loss_today else -1.0
    rows.append({"FUNDO": "MACRO", "LIVRO": "PM_X", "PRODUCT": "STABLE",
                 "DATE": today_ts, "dia_bps": big, "position_brl": 1_000_000.0})
    rows.append({"FUNDO": "MACRO", "LIVRO": "PM_X", "PRODUCT": "VOLATILE",
                 "DATE": today_ts, "dia_bps": -2.0, "position_brl": 1_000_000.0})
    return pd.DataFrame(rows)


def test_pa_outliers_empty_returns_empty():
    out = compute_pa_outliers(pd.DataFrame(), "2026-04-29")
    assert out.empty
    out2 = compute_pa_outliers(None, "2026-04-29")
    assert out2.empty


def test_pa_outliers_flags_statistical_anomaly():
    """STABLE has small σ historically → today's -50 bps should flag (z + bps)."""
    df = _build_pa_daily("2026-04-29")
    out = compute_pa_outliers(df, "2026-04-29")
    flagged = set(out["PRODUCT"])
    assert "STABLE" in flagged


def test_pa_outliers_flags_absolute_big_move():
    """Even when σ is high (low z), |bps| ≥ abs_floor_bps should flag."""
    today = "2026-04-29"
    rng = np.random.RandomState(0)
    rows = []
    history_dates = pd.date_range(end=pd.Timestamp(today) - pd.Timedelta(days=1), periods=30)
    for d in history_dates:
        # very high σ — z-test will not trigger easily
        rows.append({"FUNDO": "MACRO", "LIVRO": "PM_X", "PRODUCT": "WIDE",
                     "DATE": d, "dia_bps": float(rng.randn() * 20), "position_brl": 1_000_000.0})
    rows.append({"FUNDO": "MACRO", "LIVRO": "PM_X", "PRODUCT": "WIDE",
                 "DATE": pd.Timestamp(today), "dia_bps": -15.0, "position_brl": 1_000_000.0})
    df = pd.DataFrame(rows)
    out = compute_pa_outliers(df, today, abs_floor_bps=10.0)
    assert "WIDE" in set(out["PRODUCT"])


def test_pa_outliers_excludes_caixa_and_taxas():
    """Excluded livros (Caixa, Taxas e Custos) should never appear in output."""
    today = "2026-04-29"
    rng = np.random.RandomState(0)
    rows = []
    for d in pd.date_range(end=pd.Timestamp(today) - pd.Timedelta(days=1), periods=30):
        for livro in ("Caixa", "Taxas e Custos"):
            rows.append({"FUNDO": "MACRO", "LIVRO": livro, "PRODUCT": f"{livro}_pos",
                         "DATE": d, "dia_bps": float(rng.randn() * 0.1), "position_brl": 1.0})
    for livro in ("Caixa", "Taxas e Custos"):
        rows.append({"FUNDO": "MACRO", "LIVRO": livro, "PRODUCT": f"{livro}_pos",
                     "DATE": pd.Timestamp(today), "dia_bps": -100.0, "position_brl": 1.0})
    df = pd.DataFrame(rows)
    out = compute_pa_outliers(df, today)
    assert out.empty or set(out["LIVRO"]).isdisjoint({"Caixa", "Taxas e Custos"})


def test_pa_outliers_sorted_by_abs_bps_desc():
    """Output rows are sorted by |today_bps| descending."""
    df = _build_pa_daily("2026-04-29")
    out = compute_pa_outliers(df, "2026-04-29", abs_floor_bps=0.5, bps_min=0.1, z_min=0.1, min_obs=5)
    if len(out) >= 2:
        abs_bps = out["today_bps"].abs().tolist()
        assert abs_bps == sorted(abs_bps, reverse=True)

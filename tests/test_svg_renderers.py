"""Tests for svg_renderers — pure visual primitives, no DB."""
from __future__ import annotations

import base64

import numpy as np
import pandas as pd
import pytest

from svg_renderers import (
    make_sparkline,
    range_bar_svg,
    stop_bar_svg,
    range_line_svg,
    evo_spark_svg,
    multi_line_chart_svg,
)


# ── make_sparkline ─────────────────────────────────────────────────────────

def test_sparkline_empty_returns_empty_string():
    assert make_sparkline(pd.Series([], dtype=float), "#fff") == ""


def test_sparkline_too_short_returns_empty():
    assert make_sparkline(pd.Series([1.0]), "#fff") == ""


def test_sparkline_returns_valid_base64_png():
    s = pd.Series(np.linspace(0, 10, 30))
    out = make_sparkline(s, "#4ade80")
    assert out, "expected non-empty base64 string"
    # Decoded payload should start with PNG magic header
    assert base64.b64decode(out)[:8] == b"\x89PNG\r\n\x1a\n"


def test_sparkline_drops_nan_values():
    s = pd.Series([np.nan, 1.0, 2.0, np.nan, 3.0, 4.0])
    out = make_sparkline(s, "#facc15")
    assert out  # 4 valid points after dropna ≥ 2 → renders


# ── range_bar_svg ──────────────────────────────────────────────────────────

def test_range_bar_returns_svg_tag():
    svg = range_bar_svg(val=50, vmin=0, vmax=100, soft=70, hard=90)
    assert svg.startswith("<svg")
    assert "</svg>" in svg


def test_range_bar_clamps_value_above_max():
    """val above vmax should still render (clamped to 100%)."""
    svg = range_bar_svg(val=200, vmin=0, vmax=100, soft=70, hard=90)
    # No exception, valid SVG, label "100%"
    assert "100%" in svg


def test_range_bar_clamps_value_below_min():
    svg = range_bar_svg(val=-10, vmin=0, vmax=100, soft=70, hard=90)
    assert "0%" in svg


def test_range_bar_color_at_warn_threshold():
    """Below UTIL_WARN → green; between WARN and HARD → yellow; above HARD → red.
    UTIL_WARN=70, UTIL_HARD=85 by default. val/soft pct is what matters."""
    svg_green  = range_bar_svg(val=10, vmin=0, vmax=100, soft=20, hard=40)  # util=50
    svg_yellow = range_bar_svg(val=15, vmin=0, vmax=100, soft=20, hard=40)  # util=75
    svg_red    = range_bar_svg(val=18, vmin=0, vmax=100, soft=20, hard=40)  # util=90
    assert "#4ade80" in svg_green
    assert "#facc15" in svg_yellow
    assert "#f87171" in svg_red


def test_range_bar_alert_ring_above_threshold():
    """ALERT_THRESHOLD=80 by default — pct >= 80 triggers the orange pulsing ring."""
    svg_alert = range_bar_svg(val=85, vmin=0, vmax=100, soft=70, hard=90)
    assert "#fb923c" in svg_alert  # alert hue used by ring + dot
    svg_quiet = range_bar_svg(val=20, vmin=0, vmax=100, soft=70, hard=90)
    # alert zone overlay always present (uses #fb923c too) — but we look for the ring marker
    assert 'stroke="#fb923c"' in svg_alert  # ring stroke


def test_range_bar_zero_range_safe():
    """vmax == vmin should not crash."""
    svg = range_bar_svg(val=5, vmin=5, vmax=5, soft=5, hard=5)
    assert "<svg" in svg


# ── stop_bar_svg ───────────────────────────────────────────────────────────

def test_stop_bar_gain_renders_green():
    svg = stop_bar_svg(budget_abs=63, pnl_mtd=20, budget_max=63)
    assert "#4ade80" in svg  # green for gain


def test_stop_bar_loss_renders_red():
    svg = stop_bar_svg(budget_abs=63, pnl_mtd=-30, budget_max=63)
    assert "#f87171" in svg  # red for loss


def test_stop_bar_gancho_when_no_budget():
    """budget_abs=0 → GANCHO label should appear."""
    svg = stop_bar_svg(budget_abs=0, pnl_mtd=-5, budget_max=63)
    assert "GANCHO" in svg


def test_stop_bar_with_soft_mark():
    svg = stop_bar_svg(budget_abs=63, pnl_mtd=-10, budget_max=63, soft_mark=30)
    assert "-30" in svg  # soft mark label
    assert "#facc15" in svg  # yellow soft-mark line


def test_stop_bar_returns_well_formed_svg():
    svg = stop_bar_svg(budget_abs=100, pnl_mtd=-50, budget_max=120)
    assert svg.startswith("<svg")
    assert svg.endswith("</svg>")


# ── range_line_svg ─────────────────────────────────────────────────────────

def test_range_line_none_returns_empty():
    assert range_line_svg(v_cur=None, v_min=0, v_max=10) == ""
    assert range_line_svg(v_cur=5, v_min=None, v_max=10) == ""
    assert range_line_svg(v_cur=5, v_min=0, v_max=None) == ""


def test_range_line_invalid_range_returns_empty():
    """v_max <= v_min → empty."""
    assert range_line_svg(v_cur=5, v_min=10, v_max=10) == ""
    assert range_line_svg(v_cur=5, v_min=10, v_max=5)  == ""


def test_range_line_renders_with_p50():
    svg = range_line_svg(v_cur=5, v_min=0, v_max=10, v_p50=4)
    assert "<svg" in svg
    # The p50 tick mark should produce a vertical line
    assert "<line" in svg


def test_range_line_dot_color_terciles():
    """Bottom tercile → green; middle → yellow; top → red."""
    bottom = range_line_svg(v_cur=2, v_min=0, v_max=9)   # in [0, 3) → green
    middle = range_line_svg(v_cur=5, v_min=0, v_max=9)   # in [3, 6) → yellow
    top    = range_line_svg(v_cur=8, v_min=0, v_max=9)   # in [6, 9] → red
    assert "#4ade80" in bottom
    assert "#facc15" in middle
    assert "#f87171" in top


# ── evo_spark_svg ──────────────────────────────────────────────────────────

def test_evo_spark_empty_series_returns_empty():
    assert evo_spark_svg(pd.Series([], dtype=float), today_val=0.0) == ""
    assert evo_spark_svg(None, today_val=0.0) == ""


def test_evo_spark_renders_polyline():
    s = pd.Series(np.linspace(-1, 1, 50))
    svg = evo_spark_svg(s, today_val=0.5)
    assert "<polyline" in svg
    assert "média 252d" in svg


def test_evo_spark_handles_constant_series():
    """y_max == y_min should not divide by zero."""
    s = pd.Series([1.0] * 30)
    svg = evo_spark_svg(s, today_val=1.0)
    assert "<svg" in svg


# ── multi_line_chart_svg ───────────────────────────────────────────────────

def test_multiline_empty_returns_empty():
    assert multi_line_chart_svg([], []) == ""
    # series provided but no dates
    assert multi_line_chart_svg([], [{"label": "a", "values": [1], "color": "#fff"}]) == ""


def test_multiline_one_date_returns_empty():
    """Need n >= 2 for a line."""
    out = multi_line_chart_svg(
        [pd.Timestamp("2026-01-01")],
        [{"label": "a", "values": [1.0], "color": "#fff"}],
    )
    assert out == ""


def test_multiline_renders_legend_and_axes():
    dates = pd.date_range("2026-01-01", periods=10)
    series = [
        {"label": "MACRO", "values": [float(i) for i in range(10)], "color": "#4ade80"},
        {"label": "QUANT", "values": [float(10 - i) for i in range(10)], "color": "#fb923c"},
    ]
    svg = multi_line_chart_svg(dates, series, title="Test")
    assert "MACRO" in svg
    assert "QUANT" in svg
    assert "Test" in svg
    # Two polylines — one per series
    assert svg.count("<polyline") == 2


def test_multiline_handles_nan_values():
    """NaN values should be skipped, not crash."""
    dates = pd.date_range("2026-01-01", periods=5)
    series = [{"label": "x", "values": [1.0, np.nan, 3.0, np.nan, 5.0], "color": "#fff"}]
    svg = multi_line_chart_svg(dates, series)
    assert "<polyline" in svg  # 3 valid points → still renders


def test_multiline_all_nan_returns_empty():
    dates = pd.date_range("2026-01-01", periods=3)
    series = [{"label": "x", "values": [np.nan, np.nan, np.nan], "color": "#fff"}]
    assert multi_line_chart_svg(dates, series) == ""


def test_multiline_dash_style():
    dates = pd.date_range("2026-01-01", periods=3)
    series = [{"label": "x", "values": [1.0, 2.0, 3.0], "color": "#fff", "dash": "3,2"}]
    svg = multi_line_chart_svg(dates, series)
    assert 'stroke-dasharray="3,2"' in svg

"""
generate_market_review.py — Market Review pilot (Fase 1).

Standalone, não toca no daily. Produz HTML com 3 cards no padrão visual do
deck mensal (PPTX `20601_reuniao_mensal`):

  1. Risk-On / Risk-Off — barra horizontal de % change 1d, sorted, por categoria
  2. FX Z-Score — box chart com yearly H/L (linha), monthly H/L (box amarelo),
     ◆ Last, ● D-5
  3. Performance Ranking — fundos GLPG com MTD/YTD/12M/24M

Output: data/market-review/{DATE}_market_review.html

Uso:
  python generate_market_review.py            # latest bday
  python generate_market_review.py 2026-04-29
"""
from __future__ import annotations

import base64
import json
import re
import sys
from io import BytesIO
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

from glpg_fetch import read_sql

ROOT    = Path(__file__).parent
OUT_DIR = ROOT / "data" / "market-review"
PEERS_JSON = ROOT / "data" / "peers_data.json"

# ── Brand palette ─────────────────────────────────────────────────────────────
NAVY        = "#183B80"
NAVY_DEEP   = "#0c2048"
BLUE        = "#2AADF5"
BG          = "#f5f7fa"
CARD_BG     = "#ffffff"
LINE        = "#d6dde7"
TEXT        = "#0c1e3e"
MUTED       = "#4a5a72"
GREEN       = "#1E8C45"
RED         = "#C0392B"
YELLOW_FILL = "#F4D03F"  # box fill (z-score box, monthly H/L)
YELLOW_HATCH = "#E5B92E"
DOT_BLUE    = "#3b82f6"


# ─────────────────────────────────────────────────────────────────────────────
# Universe & metadata
# ─────────────────────────────────────────────────────────────────────────────

# Risk-on/off — simples 1d % change bar. Sinal mostra direção real;
# coluna de categoria informa se é risk-on or risk-off por convenção.
_RISK_BAR_ASSETS = [
    # (ticker, label, source, side)  side ∈ {"on","off"}
    ("IBOV",       "IBOV",            "EQUITIES_PRICES",  "on"),
    ("BRL",        "BRL/USD",         "FX",               "on"),
    ("AUD",        "AUD/USD",         "FX",               "on"),
    ("MXN",        "MXN/USD",         "FX",               "on"),
    ("BCOM Index", "BCOM (commod)",   "ECO_INDEX_COMMOD", "on"),
    ("EUR",        "EUR/USD",         "FX",               "on"),
    ("JPY",        "JPY/USD",         "FX",               "off"),
    ("CHF",        "CHF/USD",         "FX",               "off"),
    ("US10Y",      "US 10Y yld",      "UST10",            "off"),
]

# FX universe pra Z-Score chart — major + DM + EM
_FX_ZSCORE = ["AUD", "BRL", "CAD", "CHF", "EUR", "JPY", "MXN", "NZD"]

# Yields EM/DM (5Y maturity = TENOUR 1800 bd)
_YIELDS_EM = [
    ("BRASIL_ZERO_RATE_CURVE",         "Brasil"),
    ("MEXICO_ZERO_RATE_CURVE",         "México"),
    ("CHILE_ZERO_RATE_CURVE",          "Chile"),
    ("COLOMBIA_ZERO_RATE_CURVE",       "Colombia"),
    ("HUNGARY_ZERO_RATE_CURVE",        "Hungria"),
    ("POLAND_ZERO_RATE_CURVE",         "Polônia"),
    ("CZECH REPUBLIC_ZERO_RATE_CURVE", "Czech Rep"),
    ("INDIA_ZERO_RATE_CURVE",          "Índia"),
    ("SOUTH KOREA_ZERO_RATE_CURVE",    "Coreia Sul"),
    ("INDONESIA_ZERO_RATE_CURVES",     "Indonésia"),
    ("SOUTH AFRICA_ZERO_RATE_CURVE",   "África Sul"),
]
_YIELDS_DM = [
    ("US_ZERO_RATE_CURVE",          "EUA"),
    ("EU_ZERO_RATE_CURVES",         "Eurozona"),
    ("UK_ZERO_RATE_CURVE",          "UK"),
    ("JAPAN_ZERO_RATE_CURVE",       "Japão"),
    ("CANADA_ZERO_RATE_CURVE",      "Canadá"),
    ("SWITZERLAND_ZERO_RATE_CURVE", "Suíça"),
    ("NORWAY_ZERO_RATE_CURVE",      "Noruega"),
    ("SWEDEN_ZERO_RATE_CURVE",      "Suécia"),
    ("AUSTRALIA_ZERO_RATE_CURVE",   "Austrália"),
    ("NEW ZEALAND_ZERO_RATE_CURVE", "Nova Zelândia"),
]

# Performance — GLPG funds (subset principal)
_PERF_FUNDS = [
    "Galapagos Macro FIM",
    "Galapagos Quantitativo FIM",
    "Galapagos Evolution FIC FIM CP",
    "Galapagos Global Macro Q",
    "GALAPAGOS ALBATROZ FIRF LP",
    "Galapagos Baltra Icatu Qualif Prev FIM CP",
    "Frontier Ações FIC FI",
    "IDKA IPCA 3Y FIRF",
    "IDKA IPCA 10Y FIRF",
]


# ─────────────────────────────────────────────────────────────────────────────
# Date resolution
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_date(arg: str | None) -> str:
    if arg:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", arg):
            sys.exit(f"Formato inválido: '{arg}'. Use YYYY-MM-DD.")
        return arg
    df = read_sql("SELECT MAX(\"VAL_DATE\") AS d FROM \"LOTE45\".\"LOTE_TRADING_DESKS_NAV_SHARE\"")
    return pd.Timestamp(df.iloc[0, 0]).strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# Data fetchers
# ─────────────────────────────────────────────────────────────────────────────

def _fx_series(instrument: str, start: str, end: str) -> pd.Series:
    df = read_sql(f"""
        SELECT "DATE", "CLOSE"
          FROM public."FX_PRICES_SPOT"
         WHERE "INSTRUMENT" = '{instrument}'
           AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{end}'
         ORDER BY "DATE"
    """)
    if df.empty: return pd.Series(dtype=float)
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df.drop_duplicates("DATE").set_index("DATE")["CLOSE"].astype(float)


def _eco_series(instrument: str, field: str, start: str, end: str) -> pd.Series:
    df = read_sql(f"""
        SELECT "DATE", "VALUE"
          FROM public."ECO_INDEX"
         WHERE "INSTRUMENT" = '{instrument}' AND "FIELD" = '{field}'
           AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{end}'
         ORDER BY "DATE"
    """)
    if df.empty: return pd.Series(dtype=float)
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df.drop_duplicates("DATE").set_index("DATE")["VALUE"].astype(float)


def _equity_series(instrument: str, start: str, end: str) -> pd.Series:
    df = read_sql(f"""
        SELECT "DATE", "CLOSE"
          FROM public."EQUITIES_PRICES"
         WHERE "INSTRUMENT" = '{instrument}'
           AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{end}'
         ORDER BY "DATE"
    """)
    if df.empty: return pd.Series(dtype=float)
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df.drop_duplicates("DATE").set_index("DATE")["CLOSE"].astype(float)


def _us10y_series(start: str, end: str) -> pd.Series:
    """US Treasury constant-maturity 10Y. Yield in %.
    TENOUR=3600 corresponds to 10Y (DGS10) for the FRED chain."""
    df = read_sql(f"""
        SELECT y."DATE", y."YIELD"
          FROM public."YIELDS_GLOBAL_CURVES" y
          JOIN public."MAPS_GLOBAL_CURVES" m ON y."GLOBAL_CURVES_KEY" = m."GLOBAL_CURVES_KEY"
         WHERE m."CHAIN" = 'US_TREASURY_CONSTANT_MATURITY'
           AND y."TENOUR" = 3600
           AND y."DATE" >= DATE '{start}' AND y."DATE" <= DATE '{end}'
         ORDER BY y."DATE"
    """)
    if df.empty: return pd.Series(dtype=float)
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df.drop_duplicates("DATE").set_index("DATE")["YIELD"].astype(float)


def _zero_curve_series(chain: str, tenour: int, start: str, end: str) -> pd.Series:
    """Pull yield series for a given zero-rate curve at a fixed tenour (bdays)."""
    df = read_sql(f"""
        SELECT y."DATE", y."YIELD"
          FROM public."YIELDS_GLOBAL_CURVES" y
          JOIN public."MAPS_GLOBAL_CURVES" m ON y."GLOBAL_CURVES_KEY" = m."GLOBAL_CURVES_KEY"
         WHERE m."CHAIN" = '{chain}'
           AND y."TENOUR" = {tenour}
           AND y."DATE" >= DATE '{start}' AND y."DATE" <= DATE '{end}'
         ORDER BY y."DATE"
    """)
    if df.empty: return pd.Series(dtype=float)
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df.drop_duplicates("DATE").set_index("DATE")["YIELD"].astype(float)


def _nav_series(desk: str, start: str, end: str) -> pd.Series:
    """SHARE per unit (returns base) for a Galapagos fund."""
    df = read_sql(f"""
        SELECT "VAL_DATE", "SHARE"
          FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
         WHERE "TRADING_DESK" = '{desk}'
           AND "VAL_DATE" >= DATE '{start}' AND "VAL_DATE" <= DATE '{end}'
         ORDER BY "VAL_DATE"
    """)
    if df.empty: return pd.Series(dtype=float)
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
    return df.drop_duplicates("VAL_DATE").set_index("VAL_DATE")["SHARE"].astype(float)


# ─────────────────────────────────────────────────────────────────────────────
# Stats helpers
# ─────────────────────────────────────────────────────────────────────────────

# Maximum staleness allowed: if the series' last observation is older than
# STALE_BDAYS business days vs the report date, we drop the instrument from
# the chart instead of plotting a misleading "Last" snapshot from weeks ago.
STALE_BDAYS = 7


def _zscore_summary(s: pd.Series, end: pd.Timestamp) -> dict | None:
    """Compute rolling 252d z-score series and extract:
       yearly H/L (last 252 obs), monthly H/L (last 21 obs), last, d-5.
       Returns None if data is stale (last obs > STALE_BDAYS business days
       behind `end`) — prevents misleading mixed-date charts."""
    if s.empty or len(s) < 60:
        return None
    s = s.sort_index()
    s = s[s.index <= end]
    if s.empty: return None
    # Staleness check
    last_date = s.index[-1]
    bdays_behind = len(pd.bdate_range(last_date, end)) - 1
    if bdays_behind > STALE_BDAYS:
        return None
    mean = s.rolling(252, min_periods=60).mean()
    sd   = s.rolling(252, min_periods=60).std()
    z    = (s - mean) / sd
    z    = z.dropna()
    if len(z) < 21:
        return None
    z_last_year = z.iloc[-252:] if len(z) >= 252 else z
    z_last_mo   = z.iloc[-21:]
    return {
        "yhi": float(z_last_year.max()),
        "ylo": float(z_last_year.min()),
        "mhi": float(z_last_mo.max()),
        "mlo": float(z_last_mo.min()),
        "last": float(z.iloc[-1]),
        "d5":   float(z.iloc[-6]) if len(z) >= 6 else float(z.iloc[0]),
    }


def _pct_change(s: pd.Series, days: int = 1) -> float | None:
    if s.empty or len(s) <= days: return None
    p1 = float(s.iloc[-1])
    p0 = float(s.iloc[-1 - days])
    if p0 == 0: return None
    return (p1 / p0 - 1) * 100


def _bps_change(s: pd.Series, days: int = 1) -> float | None:
    """For yields: change in bps."""
    if s.empty or len(s) <= days: return None
    return (float(s.iloc[-1]) - float(s.iloc[-1 - days])) * 100


# ─────────────────────────────────────────────────────────────────────────────
# SVG renderers
# ─────────────────────────────────────────────────────────────────────────────

def _fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _risk_bar_png(rows: list[dict]) -> str:
    """Horizontal bar chart via matplotlib → base64 PNG (renderização robusta)."""
    valid = [r for r in rows if r["chg"] is not None]
    if not valid:
        return '<div style="padding:24px;color:#4a5a72">Sem dados</div>'
    n = len(valid)
    fig, ax = plt.subplots(figsize=(11, max(3.0, n * 0.45 + 0.8)),
                           facecolor="white")
    labels = []
    vals   = []
    sides  = []
    for r in valid:
        labels.append(r["label"])
        vals.append(r["chg"])
        sides.append(r["side"])
    colors = [GREEN if v >= 0 else RED for v in vals]
    y_pos = np.arange(n)
    bars = ax.barh(y_pos, vals, color=colors, height=0.62, zorder=3,
                   edgecolor="white", linewidth=0.5)
    ax.set_yticks(y_pos)
    ax.set_yticklabels([f"{lbl}" for lbl in labels], fontsize=11)
    ax.invert_yaxis()
    ax.axvline(0, color=NAVY_DEEP, linewidth=1.2, zorder=2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.set_facecolor("white")

    vmax = max(abs(v) for v in vals) or 1
    pad = vmax * 0.04
    for i, (v, side) in enumerate(zip(vals, sides)):
        unit = " bps" if labels[i].endswith("yld") else "%"
        x_text = v + pad if v >= 0 else v - pad
        ha = "left" if v >= 0 else "right"
        col = GREEN if v >= 0 else RED
        ax.text(x_text, i, f"{v:+.2f}{unit}", va="center", ha=ha,
                fontsize=10.5, fontweight="bold", color=col)
        # ON/OFF chip
        chip_x = -vmax * 1.18
        chip_col = GREEN if side == "on" else RED
        ax.text(chip_x, i, "ON" if side == "on" else "OFF",
                va="center", ha="center", fontsize=9.5, fontweight="bold",
                color=chip_col,
                bbox=dict(boxstyle="round,pad=0.2", facecolor=chip_col + "22",
                          edgecolor=chip_col, linewidth=0.8))

    ax.set_xlim(-vmax * 1.25, vmax * 1.25)
    ax.set_xlabel("Variação 1d", fontsize=10, color=MUTED)
    ax.tick_params(axis="x", colors=MUTED, labelsize=10)
    ax.grid(axis="x", linestyle=":", linewidth=0.5, color=LINE, zorder=1)

    b64 = _fig_to_b64(fig)
    return (f'<img src="data:image/png;base64,{b64}" '
            f'style="width:100%;max-width:1100px;display:block" alt="Risk-On/Off"/>')


def _risk_bar_svg(rows: list[dict], width: int = 880, row_h: int = 32) -> str:
    """Horizontal bar chart of 1d % change. Each row colored by side
    (risk-on green when up = risky / red when down; risk-off colors inverted
    to convey "risky" message)."""
    if not rows: return "<div class='empty'>Sem dados</div>"
    n = len(rows)
    h = n * row_h + 60
    pad_l, pad_r, pad_t, pad_b = 160, 80, 30, 20
    inner_w = width - pad_l - pad_r
    inner_h = h - pad_t - pad_b
    vals = [r["chg"] for r in rows if r["chg"] is not None]
    if not vals: return "<div class='empty'>Sem dados</div>"
    vmax = max(abs(v) for v in vals) or 1
    vmax *= 1.15
    cx = pad_l + inner_w / 2

    def x_of(v: float) -> float:
        return cx + (v / vmax) * (inner_w / 2)

    s = [f'<svg viewBox="0 0 {width} {h}" xmlns="http://www.w3.org/2000/svg" '
         f'width="100%" height="{h}" preserveAspectRatio="xMidYMid meet" '
         f'style="display:block;font-family:Inter,system-ui,sans-serif;max-width:100%">']
    # Title axis & gridlines
    s.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{width - pad_r}" y2="{pad_t}" '
             f'stroke="{LINE}" stroke-width="0.8"/>')
    s.append(f'<line x1="{pad_l}" y1="{h - pad_b}" x2="{width - pad_r}" y2="{h - pad_b}" '
             f'stroke="{LINE}" stroke-width="0.8"/>')
    # Vertical gridlines at -vmax, -vmax/2, 0, +vmax/2, +vmax
    for frac in (-1, -0.5, 0, 0.5, 1):
        gx = cx + frac * (inner_w / 2)
        col = NAVY_DEEP if frac == 0 else LINE
        sw  = 1.4 if frac == 0 else 0.6
        s.append(f'<line x1="{gx}" y1="{pad_t}" x2="{gx}" y2="{h - pad_b}" '
                 f'stroke="{col}" stroke-width="{sw}" '
                 f'{"" if frac == 0 else "stroke-dasharray=2,3"}/>')
        if frac != 0:
            s.append(f'<text x="{gx}" y="{h - pad_b + 14}" text-anchor="middle" '
                     f'font-size="11" fill="{MUTED}">{frac * vmax:+.2f}%</text>')
    s.append(f'<text x="{cx}" y="{h - pad_b + 14}" text-anchor="middle" '
             f'font-size="11" fill="{MUTED}" font-weight="700">0</text>')

    # Bars
    for i, r in enumerate(rows):
        y = pad_t + i * row_h + row_h / 2
        chg = r["chg"]
        cat = r["side"]
        # Label column
        cat_chip = f'''<rect x="{pad_l - 145}" y="{y - 11}" width="34" height="20"
                rx="3" fill="{GREEN if cat == 'on' else RED}" opacity="0.13"/>
                <text x="{pad_l - 128}" y="{y + 4}" text-anchor="middle" font-size="10"
                fill="{GREEN if cat == 'on' else RED}" font-weight="700">
                {'ON' if cat == 'on' else 'OFF'}</text>'''
        s.append(cat_chip)
        s.append(f'<text x="{pad_l - 100}" y="{y + 4}" font-size="13" fill="{TEXT}" '
                 f'font-weight="600">{r["label"]}</text>')
        if chg is None:
            s.append(f'<text x="{cx}" y="{y + 4}" text-anchor="middle" font-size="11" '
                     f'fill="{MUTED}">—</text>')
            continue
        x_end = x_of(chg)
        bar_x = min(cx, x_end)
        bar_w = abs(x_end - cx)
        # Color: positive = up (green if risk-on, else red if risk-off going up = risk-off);
        # for clarity we just go green/red based on direction.
        col = GREEN if chg >= 0 else RED
        s.append(f'<rect x="{bar_x}" y="{y - 8}" width="{bar_w}" height="16" '
                 f'rx="2" fill="{col}" opacity="0.85"/>')
        # Value label outside the bar
        if chg >= 0:
            s.append(f'<text x="{x_end + 6}" y="{y + 4}" font-size="12" fill="{col}" '
                     f'font-weight="700">{chg:+.2f}{r.get("unit", "%")}</text>')
        else:
            s.append(f'<text x="{x_end - 6}" y="{y + 4}" font-size="12" fill="{col}" '
                     f'font-weight="700" text-anchor="end">{chg:+.2f}{r.get("unit", "%")}</text>')
    s.append("</svg>")
    return "".join(s)


def _percentile_color(pct: float) -> str:
    """Map a percentile (0..1) to a 5-step temperature color (red → green)."""
    if pct < 0.20: return "#C0392B"   # bottom 20% — red
    if pct < 0.40: return "#E67E22"   # 20-40% — orange
    if pct < 0.60: return "#F4D03F"   # 40-60% — yellow (neutral)
    if pct < 0.80: return "#58D68D"   # 60-80% — light green
    return "#1E8C45"                  # top 20% — dark green


_ZBOX_FILL   = "#A8C4E8"  # light blue brand-aligned
_ZBOX_STROKE = NAVY_DEEP


def _zscore_box_png(items: list[dict]) -> str:
    """Z-score box-and-diamond chart via matplotlib → base64 PNG.
    Box mensal em azul claro (cor única, sem gradiente)."""
    if not items:
        return '<div style="padding:24px;color:#4a5a72">Sem dados</div>'
    n = len(items)
    fig, ax = plt.subplots(figsize=(max(8, n * 1.05), 4.6), facecolor="white")
    ax.set_facecolor("white")
    z_all = []
    for it in items:
        for k in ("yhi", "ylo", "mhi", "mlo", "last", "d5"):
            v = it["z"].get(k)
            if v is not None: z_all.append(v)
    ymax = max(3.0, max(z_all) + 0.3)
    ymin = min(-3.0, min(z_all) - 0.3)

    xs = np.arange(n)
    box_w = 0.46
    for i, it in enumerate(items):
        z = it["z"]
        # Yearly H-L line + caps
        ax.plot([i, i], [z["ylo"], z["yhi"]], color=TEXT, linewidth=1.4, zorder=2)
        for cap in (z["yhi"], z["ylo"]):
            ax.plot([i - 0.10, i + 0.10], [cap, cap], color=TEXT, linewidth=1.4, zorder=2)
        # Monthly box — single light-blue fill
        rect = mpatches.FancyBboxPatch(
            (i - box_w / 2, z["mlo"]), box_w, max(z["mhi"] - z["mlo"], 0.04),
            boxstyle="round,pad=0,rounding_size=0.06",
            linewidth=0.8, edgecolor=_ZBOX_STROKE, facecolor=_ZBOX_FILL,
            alpha=0.9, zorder=3,
        )
        ax.add_patch(rect)
        # D-5
        ax.plot([i], [z["d5"]], "o", color=DOT_BLUE, markersize=8,
                markeredgecolor=NAVY_DEEP, markeredgewidth=0.8, zorder=4)
        # Last (diamond)
        ax.plot([i], [z["last"]], "D", color="#0c0c0c", markersize=9,
                markeredgecolor="white", markeredgewidth=1.2, zorder=5)

    ax.set_xticks(xs)
    ax.set_xticklabels([it["label"] for it in items], rotation=-35, ha="left",
                       fontsize=10.5, color=TEXT)
    ax.set_xlim(-0.6, n - 0.4)
    ax.set_ylim(ymin, ymax)
    ax.axhline(0, color=NAVY_DEEP, linewidth=1.0, zorder=1)
    ax.set_ylabel("Z-Score (252d roll)", fontsize=10, color=MUTED)
    ax.tick_params(axis="y", colors=MUTED, labelsize=10)
    ax.grid(axis="y", linestyle=":", linewidth=0.5, color=LINE, zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.legend(handles=[
        mpatches.Patch(color=_ZBOX_FILL, label="Mês H/L"),
        plt.Line2D([0], [0], color=TEXT, linewidth=1.4, label="Ano H/L"),
        plt.Line2D([0], [0], marker="D", color="w", markerfacecolor="#0c0c0c",
                   markeredgecolor="white", markersize=9, label="◆ Last"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=DOT_BLUE,
                   markeredgecolor=NAVY_DEEP, markersize=8, label="● D-5"),
    ], loc="upper left", fontsize=9, frameon=False, ncol=4)

    b64 = _fig_to_b64(fig)
    return (f'<img src="data:image/png;base64,{b64}" '
            f'style="width:100%;max-width:1100px;display:block" alt="FX Z-Score"/>')


def _zscore_box_svg(items: list[dict], width: int = 880, height: int = 360) -> str:
    """Box-and-diamond chart: yearly line + monthly box + last (◆) + d-5 (●).
    Box é colorido pelo percentil de `last` dentro do range yearly (red →
    green = bottom → top). Quando os dados são tight (mês ≈ ano), é
    a coloração que dá o sinal direto."""
    if not items: return "<div class='empty'>Sem dados</div>"
    pad_l, pad_r, pad_t, pad_b = 60, 30, 36, 70
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    n = len(items)
    band_w = inner_w / n
    box_w = min(band_w * 0.55, 38)

    z_all = []
    for it in items:
        for k in ("yhi", "ylo", "mhi", "mlo", "last", "d5"):
            v = it["z"].get(k)
            if v is not None:
                z_all.append(v)
    if not z_all: return "<div class='empty'>Sem dados</div>"
    ymax = max(3.0, max(z_all) + 0.3)
    ymin = min(-3.0, min(z_all) - 0.3)

    def y_of(z: float) -> float:
        return pad_t + (1 - (z - ymin) / (ymax - ymin)) * inner_h

    # Explicit width AND height attributes so SVG renders reliably across
    # browsers (Edge sometimes ignores style:width:100% without explicit attr)
    s = [f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
         f'width="100%" height="{height}" preserveAspectRatio="xMidYMid meet" '
         f'style="display:block;font-family:Inter,system-ui,sans-serif;max-width:100%">']

    for tk in range(int(np.floor(ymin)), int(np.ceil(ymax)) + 1):
        ty = y_of(tk)
        col = NAVY_DEEP if tk == 0 else LINE
        sw  = 1.4 if tk == 0 else 0.5
        s.append(f'<line x1="{pad_l}" y1="{ty}" x2="{width - pad_r}" y2="{ty}" '
                 f'stroke="{col}" stroke-width="{sw}" '
                 f'{"" if tk == 0 else "stroke-dasharray=2,3"}/>')
        s.append(f'<text x="{pad_l - 8}" y="{ty + 4}" text-anchor="end" '
                 f'font-size="11" fill="{MUTED}">{tk:+d}</text>')

    for i, it in enumerate(items):
        cx = pad_l + (i + 0.5) * band_w
        z = it["z"]
        # Percentil de `last` no yearly range → color
        rng = z["yhi"] - z["ylo"]
        pct = (z["last"] - z["ylo"]) / rng if rng > 1e-6 else 0.5
        pct = max(0.0, min(1.0, pct))
        box_fill   = _percentile_color(pct)
        box_stroke = NAVY_DEEP

        # Yearly H-L line (faded version of box color, for cohesion)
        s.append(f'<line x1="{cx}" y1="{y_of(z["yhi"])}" x2="{cx}" y2="{y_of(z["ylo"])}" '
                 f'stroke="{TEXT}" stroke-width="1.4"/>')
        for cap in (z["yhi"], z["ylo"]):
            cy = y_of(cap)
            s.append(f'<line x1="{cx - 6}" y1="{cy}" x2="{cx + 6}" y2="{cy}" '
                     f'stroke="{TEXT}" stroke-width="1.4"/>')
        # Monthly box — colorido pelo percentil
        bx = cx - box_w / 2
        by_top = y_of(z["mhi"])
        by_bot = y_of(z["mlo"])
        bh = max(2, by_bot - by_top)
        s.append(f'<rect x="{bx}" y="{by_top}" width="{box_w}" height="{bh}" '
                 f'fill="{box_fill}" stroke="{box_stroke}" stroke-width="1.2" '
                 f'rx="3" opacity="0.85"/>')
        # D-5 dot
        d5y = y_of(z["d5"])
        s.append(f'<circle cx="{cx}" cy="{d5y}" r="6" fill="{DOT_BLUE}" '
                 f'stroke="{NAVY_DEEP}" stroke-width="1"/>')
        # Last diamond
        ly = y_of(z["last"])
        dr = 7
        poly = f'{cx},{ly - dr} {cx + dr},{ly} {cx},{ly + dr} {cx - dr},{ly}'
        s.append(f'<polygon points="{poly}" fill="#0c0c0c" stroke="#fff" stroke-width="1.4"/>')
        # Label
        lx = cx
        ly_lbl = height - pad_b + 18
        s.append(f'<text x="{lx}" y="{ly_lbl}" text-anchor="end" font-size="11.5" '
                 f'fill="{TEXT}" transform="rotate(-35 {lx} {ly_lbl})" '
                 f'font-weight="600">{it["label"]}</text>')

    s.append(f'<line x1="{pad_l}" y1="{pad_t + inner_h}" '
             f'x2="{width - pad_r}" y2="{pad_t + inner_h}" '
             f'stroke="{TEXT}" stroke-width="1"/>')

    # Legend with color-gradient explanation
    lg_y = 18
    s.append(f'<g transform="translate({pad_l}, {lg_y - 10})">')
    s.append(f'<text x="0" y="0" font-size="11" fill="{MUTED}" font-weight="600">'
             f'Cor do box = percentil de Last vs ano</text>')
    # 5 swatches
    sw_w = 22; sw_x0 = 220
    for i, col in enumerate(["#C0392B", "#E67E22", "#F4D03F", "#58D68D", "#1E8C45"]):
        s.append(f'<rect x="{sw_x0 + i * sw_w}" y="-8" width="{sw_w}" height="10" '
                 f'fill="{col}" opacity="0.85"/>')
    s.append(f'<text x="{sw_x0}" y="14" font-size="10" fill="{MUTED}">P0</text>')
    s.append(f'<text x="{sw_x0 + 5 * sw_w - 18}" y="14" font-size="10" fill="{MUTED}">P100</text>')
    s.append(f'</g>')
    # Right-side legend (markers)
    s.append(f'<text x="{width - pad_r}" y="{lg_y - 4}" text-anchor="end" font-size="11" '
             f'fill="{MUTED}">'
             f'<tspan font-weight="700">▏</tspan> ano H/L  '
             f'│  <tspan fill="{TEXT}" font-weight="700">◆</tspan> Last  '
             f'│  <tspan fill="{DOT_BLUE}" font-weight="700">●</tspan> D-5'
             f'</text>')

    s.append("</svg>")
    return "".join(s)


# ─────────────────────────────────────────────────────────────────────────────
# Card builders
# ─────────────────────────────────────────────────────────────────────────────

def build_risk_bar_card(end: str) -> str:
    end_ts = pd.Timestamp(end)
    start_ts = end_ts - pd.Timedelta(days=10)
    rows = []
    for inst, lbl, src, side in _RISK_BAR_ASSETS:
        s = pd.Series(dtype=float)
        unit = "%"
        if src == "FX":
            s = _fx_series(inst, start_ts.strftime("%Y-%m-%d"), end)
        elif src == "EQUITIES_PRICES":
            s = _equity_series(inst, start_ts.strftime("%Y-%m-%d"), end)
        elif src == "ECO_INDEX_COMMOD":
            s = _eco_series(inst, "COMMODITY INDEX", start_ts.strftime("%Y-%m-%d"), end)
        elif src == "UST10":
            s = _us10y_series(start_ts.strftime("%Y-%m-%d"), end)
            chg = _bps_change(s, 1)
            unit = " bps"
            rows.append({"label": lbl, "side": side, "chg": chg, "unit": unit})
            continue
        chg = _pct_change(s, 1)
        # FX "BRL" upstream is BRL price of 1 USD — invert sign so positive = BRL appreciating
        if src == "FX":
            chg = -chg if chg is not None else None
        rows.append({"label": lbl, "side": side, "chg": chg, "unit": unit})
    rows.sort(key=lambda r: (r["chg"] if r["chg"] is not None else 0), reverse=True)
    chart = _risk_bar_png(rows)
    return f"""
    <section class="mr-card">
      <div class="mr-card-head">
        <h2>Risk-On / Risk-Off</h2>
        <span class="mr-sub">Variação 1d · ordenado por magnitude · ON = ativo de risco / OFF = porto seguro</span>
      </div>
      <div class="mr-chart">{chart}</div>
    </section>"""


def _build_zscore_card(title: str, sub: str, items: list[dict], tab_id: str) -> str:
    """Generic z-score panel wrapped in a tab section."""
    chart = _zscore_box_png(items)
    return f"""
    <section class="mr-card mr-tab-panel" data-tab="{tab_id}">
      <div class="mr-card-head">
        <h2>{title}</h2>
        <span class="mr-sub">{sub}</span>
      </div>
      <div class="mr-chart">{chart}</div>
    </section>"""


def build_fx_zscore_card(end: str) -> str:
    end_ts = pd.Timestamp(end)
    start_ts = end_ts - pd.Timedelta(days=400)
    items = []
    for inst in _FX_ZSCORE:
        s = _fx_series(inst, start_ts.strftime("%Y-%m-%d"), end)
        if s.empty: continue
        z = _zscore_summary(s, end_ts)
        if z is None: continue
        items.append({"label": f"{inst}/USD", "z": z})
    return _build_zscore_card(
        "FX · Z-Score 1Y",
        "Linha = ano H/L · Box = mês H/L · ◆ Last · ● D-5 · z sobre janela rolante 252d",
        items, "fx",
    )


def build_yields_card(end: str, region: str) -> str:
    """region ∈ {'EM','DM'} — 5Y maturity zero rate curve z-score."""
    end_ts = pd.Timestamp(end)
    start_ts = end_ts - pd.Timedelta(days=400)
    universe = _YIELDS_EM if region == "EM" else _YIELDS_DM
    items = []
    for chain, label in universe:
        s = _zero_curve_series(chain, 1800, start_ts.strftime("%Y-%m-%d"), end)
        if s.empty: continue
        z = _zscore_summary(s, end_ts)
        if z is None: continue
        items.append({"label": label, "z": z})
    label_text = "EM" if region == "EM" else "DM"
    return _build_zscore_card(
        f"Yields {label_text} · 5Y Zero Rate · Z-Score 1Y",
        f"Curva zero {'Emerging' if region == 'EM' else 'Developed'} no vértice 5Y · z sobre 252d",
        items, f"yields_{region.lower()}",
    )


def build_macro_zscore_card(end: str) -> str:
    """Macro cross-asset: equity, FX, commodities, rates — broad picture."""
    end_ts = pd.Timestamp(end)
    start_ts = end_ts - pd.Timedelta(days=400)
    items = []

    # IBOV (equity BR)
    s = _equity_series("IBOV", start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "IBOV", "z": z})

    # BRL/USD (FX BR)
    s = _fx_series("BRL", start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "BRL/USD", "z": z})

    # EUR/USD (FX DM proxy)
    s = _fx_series("EUR", start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "EUR/USD", "z": z})

    # JPY/USD (FX safe-haven)
    s = _fx_series("JPY", start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "JPY/USD", "z": z})

    # BCOM (commodities)
    s = _eco_series("BCOM Index", "COMMODITY INDEX",
                    start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "BCOM", "z": z})

    # US 10Y (rates DM)
    s = _us10y_series(start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "US 10Y", "z": z})

    # BR 5Y nominal zero
    s = _zero_curve_series("BRASIL_ZERO_RATE_CURVE", 1800,
                           start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "BR 5Y", "z": z})

    # BR 2Y nominal zero
    s = _zero_curve_series("BRASIL_ZERO_RATE_CURVE", 720,
                           start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "BR 2Y", "z": z})

    return _build_zscore_card(
        "Macro Cross-Asset · Z-Score 1Y",
        "Equity / FX / Commodities / Rates — visão consolidada · z sobre 252d",
        items, "macro",
    )


def build_brasil_zscore_card(end: str) -> str:
    """Foco Brasil: IBOV, BRL, BR rates, BCOM, IPCA breakeven se possível."""
    end_ts = pd.Timestamp(end)
    start_ts = end_ts - pd.Timedelta(days=400)
    items = []

    s = _equity_series("IBOV", start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "IBOV", "z": z})

    s = _fx_series("BRL", start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "BRL/USD", "z": z})

    # BR zero rate curve at multiple tenors
    for ten, lbl in [(252, "BR 1Y"), (504, "BR 2Y"),
                     (1260, "BR 5Y nom"), (2520, "BR 10Y nom")]:
        s = _zero_curve_series("BRASIL_ZERO_RATE_CURVE", ten,
                               start_ts.strftime("%Y-%m-%d"), end)
        z = _zscore_summary(s, end_ts) if not s.empty else None
        if z: items.append({"label": lbl, "z": z})

    # BR IPCA real curve at 5Y
    for ten, lbl in [(1260, "BR 5Y real"), (2520, "BR 10Y real")]:
        s = _zero_curve_series("IPCA_ANBIMA", ten,
                               start_ts.strftime("%Y-%m-%d"), end)
        z = _zscore_summary(s, end_ts) if not s.empty else None
        if z: items.append({"label": lbl, "z": z})

    # BCOM
    s = _eco_series("BCOM Index", "COMMODITY INDEX",
                    start_ts.strftime("%Y-%m-%d"), end)
    z = _zscore_summary(s, end_ts) if not s.empty else None
    if z: items.append({"label": "BCOM", "z": z})

    return _build_zscore_card(
        "Brasil · Z-Score 1Y",
        "Equity, FX, curva pré, curva real e commodities · z sobre 252d",
        items, "brasil",
    )


# Mapping desk → short key (for peers lookup)
_DESK_TO_SHORT = {
    "Galapagos Macro FIM":                       "MACRO",
    "Galapagos Quantitativo FIM":                "QUANT",
    "Galapagos Evolution FIC FIM CP":            "EVOLUTION",
    "Galapagos Global Macro Q":                  "MACRO_Q",
    "GALAPAGOS ALBATROZ FIRF LP":                "ALBATROZ",
    "Galapagos Baltra Icatu Qualif Prev FIM CP": "BALTRA",
    "Frontier Ações FIC FI":                     "FRONTIER",
    "IDKA IPCA 3Y FIRF":                         "IDKA_3Y",
    "IDKA IPCA 10Y FIRF":                        "IDKA_10Y",
}


def _load_peers() -> dict:
    """Load peers JSON (same source as monthly review)."""
    if PEERS_JSON.exists():
        try:
            data = json.loads(PEERS_JSON.read_text(encoding="utf-8"))
            return data if "val_date" in data else data.get("latest", {})
        except Exception:
            pass
    return {}


def _peer_stats(short: str, peers_data: dict, window: str) -> dict | None:
    """Return {our_val, q1, q3, lo, hi, n, rank, quartile} or None."""
    try:
        from risk_config import _FUND_PEERS_GROUP
    except Exception:
        return None
    grp_key = _FUND_PEERS_GROUP.get(short)
    if not grp_key or not peers_data:
        return None
    g = peers_data.get("groups", {}).get(grp_key, {})
    peers = list(g.get("peers", []))
    if not peers:
        return None
    rows = [p for p in peers if p.get(window) is not None]
    if not rows:
        return None
    rows_sorted = sorted(rows, key=lambda p: -(p.get(window) or 0))
    vals = [float(r.get(window) or 0) * 100 for r in rows_sorted]  # in %
    n = len(rows_sorted)
    our_idx = next((i for i, r in enumerate(rows_sorted) if r.get("is_fund")), None)
    if our_idx is None:
        return None
    q = int(np.ceil((our_idx + 1) / n * 4))
    q = max(1, min(4, q))
    asc = sorted(vals)
    return {
        "our_val":  vals[our_idx],
        "rank":     our_idx + 1,
        "n":        n,
        "quartile": q,
        "q1":       asc[max(0, n // 4 - 1)],
        "q3":       asc[min(n - 1, 3 * n // 4 - 1)],
        "lo":       asc[0],
        "hi":       asc[-1],
    }


_QUARTILE_BG = {
    1: "#1E8C45",   # top: dark green
    2: "#58D68D",   # 2nd: light green
    3: "#E67E22",   # 3rd: orange
    4: "#C0392B",   # bottom: red
}


def _quartile_box_svg(stats: dict, w: int = 150, h: int = 22) -> str:
    """Mini horizontal box-and-diamond strip:
       line = peer min → max · yellow box = IQR (Q1-Q3) · ◆ = our position
       Box fill colored by our quartile (top=green / bottom=red)."""
    pad = 4
    lo, hi = stats["lo"], stats["hi"]
    if hi <= lo: hi = lo + 0.001
    q1, q3 = stats["q1"], stats["q3"]
    our = stats["our_val"]
    fill = _QUARTILE_BG[stats["quartile"]]

    def x_of(v):
        return pad + (v - lo) / (hi - lo) * (w - 2 * pad)

    cy = h / 2
    s = [f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
         f'xmlns="http://www.w3.org/2000/svg" style="display:block;vertical-align:middle">']
    # Range line
    s.append(f'<line x1="{pad}" y1="{cy}" x2="{w - pad}" y2="{cy}" '
             f'stroke="{TEXT}" stroke-width="1.2" stroke-linecap="round"/>')
    # End caps
    s.append(f'<line x1="{pad}" y1="{cy - 4}" x2="{pad}" y2="{cy + 4}" '
             f'stroke="{TEXT}" stroke-width="1.2"/>')
    s.append(f'<line x1="{w - pad}" y1="{cy - 4}" x2="{w - pad}" y2="{cy + 4}" '
             f'stroke="{TEXT}" stroke-width="1.2"/>')
    # IQR box
    bx1 = x_of(q1); bx2 = x_of(q3)
    s.append(f'<rect x="{bx1}" y="{cy - 6}" width="{max(2, bx2 - bx1)}" height="12" '
             f'fill="{fill}" opacity="0.85" stroke="{NAVY_DEEP}" stroke-width="0.7" rx="2"/>')
    # Our diamond
    dx = x_of(our)
    dr = 5.5
    poly = f'{dx},{cy - dr} {dx + dr},{cy} {dx},{cy + dr} {dx - dr},{cy}'
    s.append(f'<polygon points="{poly}" fill="#0c0c0c" stroke="#fff" stroke-width="1.2"/>')
    s.append("</svg>")
    return "".join(s)


def build_performance_card(end: str) -> str:
    end_ts = pd.Timestamp(end)
    year_start = end_ts.replace(month=1, day=1)
    month_start = end_ts.replace(day=1)
    start_ts = end_ts - pd.Timedelta(days=800)

    peers_data = _load_peers()
    peers_date = peers_data.get("val_date", "—")

    rows_html = []
    for desk in _PERF_FUNDS:
        s = _nav_series(desk, start_ts.strftime("%Y-%m-%d"), end)
        if s.empty:
            continue

        def ret_window(begin: pd.Timestamp) -> float | None:
            sb = s[s.index < begin]
            sa = s[s.index <= end_ts]
            if sb.empty or sa.empty: return None
            p0 = float(sb.iloc[-1]); p1 = float(sa.iloc[-1])
            if p0 == 0: return None
            return (p1 / p0 - 1) * 100

        mtd = ret_window(month_start)
        ytd = ret_window(year_start)
        m12 = ret_window(end_ts - pd.DateOffset(years=1))
        m24 = ret_window(end_ts - pd.DateOffset(years=2))

        short = _DESK_TO_SHORT.get(desk, desk)
        sts = {w: _peer_stats(short, peers_data, w) for w in ("MTD", "YTD", "12M", "24M")}

        def perf_cell(v: float | None) -> str:
            if v is None:
                return '<td class="mr-cell mr-num mr-empty">—</td>'
            cls = "mr-pos" if v >= 0 else "mr-neg"
            return f'<td class="mr-cell mr-num {cls}">{v:+.2f}%</td>'

        def q_cell(st: dict | None) -> str:
            if st is None:
                return '<td class="mr-cell mr-q-empty">—</td>'
            q = st["quartile"]
            bg = _QUARTILE_BG[q]
            return (f'<td class="mr-cell mr-q-cell" '
                    f'style="background:{bg};color:#ffffff">'
                    f'{q}º <span class="mr-q-rank-inline">#{st["rank"]}/{st["n"]}</span>'
                    f'</td>')

        label = desk.replace("Galapagos ", "").replace("GALAPAGOS ", "")
        rows_html.append(
            f'<tr><td class="mr-cell mr-fund">{label}</td>'
            f'{perf_cell(mtd)}{perf_cell(ytd)}{perf_cell(m12)}{perf_cell(m24)}'
            f'<td class="mr-cell mr-divider"></td>'
            f'{q_cell(sts["MTD"])}{q_cell(sts["YTD"])}'
            f'{q_cell(sts["12M"])}{q_cell(sts["24M"])}</tr>'
        )
    if not rows_html:
        return ""
    sub = f"Retorno + quartil ordinal vs peers · base = SHARE / peers ({peers_date})"
    return f"""
    <section class="mr-card">
      <div class="mr-card-head">
        <h2>Performance · Galapagos</h2>
        <span class="mr-sub">{sub}</span>
      </div>
      <table class="mr-table mr-perf-table">
        <thead>
          <tr class="mr-grouphdr">
            <th rowspan="2" class="mr-th-fund">Fundo</th>
            <th colspan="4" class="mr-grp-perf">Performance</th>
            <th rowspan="2" class="mr-divider"></th>
            <th colspan="4" class="mr-grp-q">Quartil vs peers</th>
          </tr>
          <tr>
            <th>MTD</th><th>YTD</th><th>12M</th><th>24M</th>
            <th>MTD</th><th>YTD</th><th>12M</th><th>24M</th>
          </tr>
        </thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
    </section>"""


# ─────────────────────────────────────────────────────────────────────────────
# Page assembly
# ─────────────────────────────────────────────────────────────────────────────

_CSS = f"""
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Inter', system-ui, sans-serif; background: {BG}; color: {TEXT};
       padding: 28px; font-size: 14px; }}
.mr-header {{
  display: flex; align-items: baseline; justify-content: space-between;
  border-bottom: 3px solid {NAVY}; padding-bottom: 12px; margin-bottom: 22px;
}}
.mr-header h1 {{ font-size: 22px; color: {NAVY}; letter-spacing: .04em; font-weight: 800; }}
.mr-header .mr-meta {{ color: {MUTED}; font-size: 13px; font-weight: 500; }}
.mr-card {{
  background: {CARD_BG};
  border: 1px solid {LINE}; border-top: 4px solid {NAVY};
  border-radius: 6px; padding: 18px 20px;
  box-shadow: 0 1px 4px rgba(12,30,80,0.05);
  margin-bottom: 22px;
}}
.mr-card-head h2 {{ font-size: 17px; color: {NAVY_DEEP}; font-weight: 700;
                    letter-spacing: .03em; margin-bottom: 4px; }}
.mr-sub {{ color: {MUTED}; font-size: 12px; font-weight: 500; }}
.mr-chart {{ margin-top: 14px; }}
.mr-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }}
.mr-table th {{
  background: {NAVY}; color: #ffffff; padding: 10px 12px;
  text-align: right; font-weight: 600; font-size: 12px;
  letter-spacing: .04em; text-transform: uppercase;
}}
.mr-table th.mr-th-fund {{ text-align: left; }}
.mr-cell {{ padding: 9px 12px; border-bottom: 1px solid #eef2f8; }}
.mr-cell.mr-fund {{ font-weight: 600; color: {NAVY_DEEP}; }}
.mr-cell.mr-num {{ text-align: right; font-family: 'JetBrains Mono', ui-monospace, monospace;
                   font-variant-numeric: tabular-nums; }}
.mr-pos {{ color: {GREEN}; font-weight: 700; }}
.mr-neg {{ color: {RED}; font-weight: 700; }}
.mr-empty {{ color: {MUTED}; }}

/* Performance table — PPTX style: 2 grupos lado-a-lado (Performance | Quartil) */
.mr-perf-table th {{ text-align: center; }}
.mr-perf-table th.mr-th-fund {{ text-align: left; }}
.mr-grouphdr th {{ font-size: 11.5px !important; }}
.mr-grouphdr th.mr-grp-perf {{ background: {NAVY_DEEP} !important; }}
.mr-grouphdr th.mr-grp-q    {{ background: {NAVY_DEEP} !important; }}
.mr-divider {{
  width: 6px !important; padding: 0 !important; background: transparent !important;
  border-bottom: 0 !important; border-left: 2px solid {LINE} !important;
}}
.mr-q-cell {{
  text-align: center !important; font-weight: 700;
  font-size: 13.5px; letter-spacing: .04em;
}}
.mr-q-rank-inline {{
  display: inline-block; margin-left: 4px; opacity: .85;
  font-family: 'JetBrains Mono', monospace; font-size: 11px; font-weight: 500;
}}
.mr-q-empty {{
  text-align: center !important; color: {MUTED}; font-style: italic;
  background: #fafbfd;
}}
.mr-cell.mr-num {{ text-align: right; }}

/* Tabs (Z-Score panels switcher) */
.mr-tabs {{
  display: flex; gap: 6px; margin-bottom: 14px; flex-wrap: wrap;
}}
.mr-tab-btn {{
  padding: 8px 18px; font-size: 13px; font-weight: 600;
  border: 1px solid {LINE}; background: #ffffff; color: {NAVY_DEEP};
  border-radius: 6px; cursor: pointer; letter-spacing: .03em;
  transition: all .15s ease;
}}
.mr-tab-btn:hover {{
  border-color: {NAVY}; background: rgba(24,59,128,0.05);
}}
.mr-tab-btn.active {{
  background: {NAVY}; color: #ffffff; border-color: {NAVY};
  box-shadow: 0 1px 4px rgba(12,30,80,0.18);
}}
.mr-panels .mr-tab-panel {{ display: none; }}
.mr-panels .mr-tab-panel.active {{ display: block; }}
"""


def build_html(end: str) -> str:
    end_ts = pd.Timestamp(end)
    # Z-score panels (toggleable via tabs)
    z_panels = [
        ("macro",      "Macro Cross-Asset", build_macro_zscore_card(end)),
        ("brasil",     "Brasil",            build_brasil_zscore_card(end)),
        ("fx",         "FX",                build_fx_zscore_card(end)),
        ("yields_em",  "Yields EM",         build_yields_card(end, "EM")),
        ("yields_dm",  "Yields DM",         build_yields_card(end, "DM")),
    ]
    tab_btns = "".join(
        f'<button class="mr-tab-btn{" active" if i == 0 else ""}" '
        f'data-target="{tid}" onclick="mrTab(\'{tid}\')">{label}</button>'
        for i, (tid, label, _) in enumerate(z_panels)
    )
    z_html = "".join(p[2] for p in z_panels)
    perf = build_performance_card(end)
    tabs_block = f"""
    <div class="mr-tabs">{tab_btns}</div>
    <div class="mr-panels">{z_html}</div>"""
    cards = [tabs_block, perf]
    body = "".join(c for c in cards if c)
    js = """
function mrTab(target) {
  document.querySelectorAll('.mr-tab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.target === target);
  });
  document.querySelectorAll('.mr-panels .mr-tab-panel').forEach(p => {
    p.classList.toggle('active', p.dataset.tab === target);
  });
}
document.addEventListener('DOMContentLoaded', () => {
  const first = document.querySelector('.mr-tab-btn');
  if (first) mrTab(first.dataset.target);
});
"""
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>Market Review — {end}</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>{_CSS}</style>
</head>
<body>
  <div class="mr-header">
    <h1>MARKET REVIEW</h1>
    <span class="mr-meta">{end_ts.strftime('%d/%m/%Y')}</span>
  </div>
  {body}
  <script>{js}</script>
</body>
</html>"""


def main() -> Path:
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    end = _resolve_date(arg)
    print(f">> Market Review pilot · data {end}")

    html = build_html(end)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{end}_market_review.html"
    out.write_text(html, encoding="utf-8")
    print(f"   -> {out}")
    return out


if __name__ == "__main__":
    main()

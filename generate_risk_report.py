"""
generate_risk_report.py
Gera o HTML diário de risco MM com barras de range 12m e sparklines 60d.
Usage: python generate_risk_report.py [YYYY-MM-DD]
"""
import sys
import base64
import io
import json
from pathlib import Path
from datetime import date, timedelta

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from glpg_fetch import read_sql

# ── Config ──────────────────────────────────────────────────────────────────
DATA_STR  = sys.argv[1] if len(sys.argv) > 1 else (
    (pd.Timestamp("today") - pd.tseries.offsets.BusinessDay(1)).strftime("%Y-%m-%d")
)
DATA      = pd.Timestamp(DATA_STR)
DATE_1Y   = DATA - pd.DateOffset(years=1)
DATE_60D  = DATA - pd.Timedelta(days=90)  # ~60 business days buffer

FUNDS = {
    "Galapagos Macro FIM":           {"short": "MACRO",      "level": 2, "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Galapagos Quantitativo FIM":    {"short": "QUANT",      "level": 2, "stress_col": "macro", "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Galapagos Evolution FIC FIM CP":{"short": "EVOLUTION",  "level": 3, "stress_col": "spec",  "var_soft": 1.75, "var_hard": 2.50, "stress_soft": 10.5, "stress_hard": 15.0},
}
# Funds in LOTE_FUND_STRESS (product-level, not RPM). Limits provisional — to be calibrated.
RAW_FUNDS = {
    "GALAPAGOS ALBATROZ FIRF LP": {"short": "ALBATROZ", "stress_col": "macro", "var_soft": 1.0, "var_hard": 1.5, "stress_soft": 5.0, "stress_hard": 8.0},
    "Galapagos Global Macro Q":   {"short": "MACRO_Q",  "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
}
ALL_FUNDS = {**FUNDS, **RAW_FUNDS}
OUT_DIR = Path(__file__).parent / "data" / "morning-calls"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Fetch data ───────────────────────────────────────────────────────────────
def fetch_pm_pnl_history():
    q = f"""
    SELECT DATE_TRUNC('month', "DATE") AS mes,
           "LIVRO",
           SUM("DIA") * 10000 AS pnl_mes_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'MACRO'
      AND "DATE" >= DATE '2025-01-01'
      AND "DATE" <= DATE '{DATA_STR}'
      AND "LIVRO" IN ('CI','Macro_LF','Macro_JD','Macro_RJ')
    GROUP BY DATE_TRUNC('month', "DATE"), "LIVRO"
    ORDER BY "LIVRO", mes
    """
    df = read_sql(q)
    df["mes"] = pd.to_datetime(df["mes"], utc=True).dt.tz_localize(None)
    return df

def fetch_risk_history():
    level_clause = " OR ".join([
        f"(\"TRADING_DESK\" = '{td}' AND \"LEVEL\" = {cfg['level']})"
        for td, cfg in FUNDS.items()
    ])
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE",
           SUM("PARAMETRIC_VAR")  AS var_total,
           SUM("SPECIFIC_STRESS") AS spec_stress,
           SUM("MACRO_STRESS")    AS macro_stress
    FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
    WHERE "VAL_DATE" >= DATE '{DATE_1Y.date()}'
      AND ({level_clause})
    GROUP BY "TRADING_DESK", "VAL_DATE"
    ORDER BY "TRADING_DESK", "VAL_DATE"
    """
    df = read_sql(q)
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).astype("datetime64[us]")
    return df

def fetch_risk_history_raw():
    """Fetch VaR/stress from LOTE_FUND_STRESS (product-level) for RAW_FUNDS, summed to fund level."""
    tds = ", ".join(f"'{td}'" for td in RAW_FUNDS)
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE",
           SUM("PVAR1DAY")        AS var_total,
           SUM("SPECIFIC_STRESS") AS spec_stress,
           SUM("MACRO_STRESS")    AS macro_stress
    FROM "LOTE45"."LOTE_FUND_STRESS"
    WHERE "VAL_DATE" >= DATE '{DATE_1Y.date()}'
      AND "TRADING_DESK" IN ({tds})
    GROUP BY "TRADING_DESK", "VAL_DATE"
    ORDER BY "TRADING_DESK", "VAL_DATE"
    """
    df = read_sql(q)
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).astype("datetime64[us]")
    return df

def fetch_frontier_mainboard(date_str: str) -> pd.DataFrame:
    """Latest available Long Only mainboard on or before date_str."""
    q = f"""
    SELECT *
    FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
    WHERE "VAL_DATE" = (
        SELECT MAX("VAL_DATE") FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
        WHERE "VAL_DATE" <= DATE '{date_str}'
    )
    ORDER BY "BOOK", "PRODUCT"
    """
    try:
        df = read_sql(q)
        if not df.empty:
            df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
        return df
    except Exception:
        return pd.DataFrame()

def fetch_frontier_exposure_data() -> tuple:
    """Fetch IBOV + SMLLBV compositions and sector mapping for Frontier exposure."""
    q_ibov = """
    SELECT "INSTRUMENT", "VALUE" AS weight
    FROM public."EQUITIES_COMPOSITION"
    WHERE "LIST_NAME" = 'IBOV'
      AND "DATE" = (SELECT MAX("DATE") FROM public."EQUITIES_COMPOSITION" WHERE "LIST_NAME" = 'IBOV')
    """
    q_smll = """
    SELECT "INSTRUMENT", "VALUE" AS weight
    FROM public."EQUITIES_COMPOSITION"
    WHERE "LIST_NAME" = 'SMLLBV'
      AND "DATE" = (SELECT MAX("DATE") FROM public."EQUITIES_COMPOSITION" WHERE "LIST_NAME" = 'SMLLBV')
    """
    q_sectors = """
    SELECT DISTINCT ON (ft."TICKER")
           ft."TICKER", cs."GLPG_SECTOR", cs."GLPG_MACRO_CLASSIFICATION"
    FROM q_models."FRONTIER_TARGETS" ft
    LEFT JOIN q_models."COMPANY_SECTORS" cs ON ft."GLOBAL_EQUITIES_KEY" = cs."GLOBAL_EQUITIES_KEY"
    ORDER BY ft."TICKER"
    """
    try:
        df_ibov    = read_sql(q_ibov)
        df_smll    = read_sql(q_smll)
        df_sectors = read_sql(q_sectors)
        return df_ibov, df_smll, df_sectors
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def fetch_aum_history():
    tds = ", ".join(f"'{t}'" for t in ALL_FUNDS)
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE", "NAV"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "VAL_DATE" >= DATE '{(DATE_1Y - timedelta(days=5)).date()}'
      AND "TRADING_DESK" IN ({tds})
    """
    df = read_sql(q)
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).astype("datetime64[us]")
    return df

# ── Build series ─────────────────────────────────────────────────────────────
def build_series(df_risk, df_aum, df_risk_raw=None):
    result = {}
    for td, cfg in FUNDS.items():
        rsk = df_risk[df_risk["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
        nav = df_aum [df_aum ["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
        rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
        nav["VAL_DATE"] = nav["VAL_DATE"].astype("datetime64[us]")
        # NAV lags VaR by up to a day (admin process). Use the latest NAV at-or-before each risk row.
        merged = pd.merge_asof(
            rsk, nav[["VAL_DATE", "NAV"]], on="VAL_DATE", direction="backward",
        ).dropna(subset=["NAV"])
        merged["var_pct"]   = merged["var_total"]    * -1 / merged["NAV"] * 100
        merged["spec_pct"]  = merged["spec_stress"]  * -1 / merged["NAV"] * 100
        merged["macro_pct"] = merged["macro_stress"] * -1 / merged["NAV"] * 100
        merged["stress_pct"] = merged[f"{cfg['stress_col']}_pct"]
        result[td] = merged.sort_values("VAL_DATE")
    if df_risk_raw is not None and not df_risk_raw.empty:
        for td, cfg in RAW_FUNDS.items():
            rsk = df_risk_raw[df_risk_raw["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
            nav = df_aum[df_aum["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
            if rsk.empty or nav.empty:
                continue
            rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
            nav["VAL_DATE"] = nav["VAL_DATE"].astype("datetime64[us]")
            merged = pd.merge_asof(
                rsk, nav[["VAL_DATE", "NAV"]], on="VAL_DATE", direction="backward",
            ).dropna(subset=["NAV"])
            if merged.empty:
                continue
            merged["var_pct"]   = merged["var_total"]    * -1 / merged["NAV"] * 100
            merged["spec_pct"]  = merged["spec_stress"]  * -1 / merged["NAV"] * 100
            merged["macro_pct"] = merged["macro_stress"] * -1 / merged["NAV"] * 100
            merged["stress_pct"] = merged[f"{cfg['stress_col']}_pct"]
            result[td] = merged.sort_values("VAL_DATE")
    return result

# ── Sparkline ────────────────────────────────────────────────────────────────
def make_sparkline(series: pd.Series, color: str, width=160, height=50) -> str:
    s = series.dropna().iloc[-60:]  # last 60 obs
    if len(s) < 2:
        return ""
    fig, ax = plt.subplots(figsize=(width / 72, height / 72), dpi=96)
    fig.patch.set_facecolor("none")
    ax.set_facecolor("none")
    ax.plot(s.values, color=color, linewidth=1.8, solid_capstyle="round")
    ax.fill_between(range(len(s)), s.values, alpha=0.15, color=color)
    ax.axis("off")
    ax.set_xlim(0, len(s) - 1)
    plt.tight_layout(pad=0)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", transparent=True, bbox_inches="tight", pad_inches=0)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()

ALERT_THRESHOLD = 80.0   # % no range 12m acima do qual gera alerta

# ── Carry formula ─────────────────────────────────────────────────────────────
STOP_BASE = 63.0
STOP_SEM  = 128.0
STOP_ANO  = 252.0

def carry_step(budget_abs: float, pnl: float, ytd: float) -> tuple[float, bool]:
    """Returns (next_budget_abs, gancho).  budget_abs is always positive."""
    extra = max(0.0, budget_abs - STOP_BASE)
    if pnl >= 0:
        next_abs = STOP_BASE + pnl * 0.5
        gancho   = False
    else:
        loss = abs(pnl)
        if extra > 0:
            li = min(loss, extra)
            lb = max(0.0, min(loss - extra, STOP_BASE))
            lx = max(0.0, loss - budget_abs)
            consumed = li * 0.25 + lb * 0.50 + lx * 1.0
        else:
            lw = min(loss, budget_abs)
            lx = max(0.0, loss - budget_abs)
            consumed = lw * 0.5 + lx * 1.0
        remaining = STOP_BASE - consumed
        gancho    = remaining <= 0
        next_abs  = 0.0 if gancho else remaining
    # semestral cap (if ytd negative)
    if ytd < 0:
        sem_cap = STOP_SEM - abs(ytd)
        next_abs = min(next_abs, max(sem_cap, 0.0))
    return next_abs, gancho

def build_stop_history(df_pnl: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Reconstruct monthly stop budget series per PM from PnL history."""
    overrides = {
        ("Macro_RJ", pd.Timestamp("2026-04-01")): 63.0,   # management override
    }
    pm_map = {
        "CI":       "CI",
        "Macro_LF": "LF",
        "Macro_JD": "JD",
        "Macro_RJ": "RJ",
    }
    CI_HARD = 233.0   # CI (Comitê) hard stop — fixed, no carry
    CI_SOFT = 150.0   # CI soft mark (shown on bar)

    result = {}
    for livro, pm in pm_map.items():
        sub = df_pnl[df_pnl["LIVRO"] == livro].sort_values("mes").copy()
        if sub.empty:
            continue
        rows = []
        budget = CI_HARD if pm == "CI" else STOP_BASE
        ytd    = 0.0
        prev_year = None
        for _, r in sub.iterrows():
            mes = r["mes"]
            pnl = r["pnl_mes_bps"]
            # new calendar year → reset
            if prev_year is not None and mes.year != prev_year:
                budget = CI_HARD if pm == "CI" else STOP_BASE
                ytd    = 0.0
            prev_year = mes.year
            # apply management override for START of this month (PMs only)
            key = (livro, mes)
            if key in overrides:
                budget = overrides[key]
            rows.append({"mes": mes, "budget_abs": budget, "pnl": pnl, "ytd": ytd,
                         "soft_mark": CI_SOFT if pm == "CI" else None})
            ytd += pnl
            if pm != "CI":
                budget, gancho = carry_step(budget, pnl, ytd)
                if gancho:
                    budget = 0.0
            # CI budget is fixed — no carry
        result[pm] = pd.DataFrame(rows)
    return result

# ── Range bar (SVG inline) ────────────────────────────────────────────────────
def range_bar_svg(val, vmin, vmax, soft, hard, width=220, height=48) -> str:
    if vmax == vmin:
        pct = 50.0
    else:
        pct = (val - vmin) / (vmax - vmin) * 100
    pct = max(0, min(100, pct))

    soft_x = (soft - vmin) / (vmax - vmin) * width if vmax != vmin else width * 0.7
    hard_x = (hard - vmin) / (vmax - vmin) * width if vmax != vmin else width * 0.9
    alert_x = ALERT_THRESHOLD / 100 * width
    soft_x = max(0, min(width, soft_x))
    hard_x = max(0, min(width, hard_x))
    dot_x  = pct / 100 * width

    util = val / soft * 100 if soft else 0
    bar_color = "#4ade80" if util < 70 else "#facc15" if util < 100 else "#f87171"
    above_alert = pct >= ALERT_THRESHOLD

    # Dot: pulsing ring if above alert threshold
    dot_r   = 7
    ring_r  = 11
    dot_color = "#fb923c" if above_alert else bar_color
    ring_svg = f'<circle cx="{dot_x:.1f}" cy="{height//2}" r="{ring_r}" fill="none" stroke="#fb923c" stroke-width="2" opacity="0.5"/>' if above_alert else ""

    pos_label = f"{pct:.0f}%"
    pos_color = "#fb923c" if above_alert else "white"

    # Alert zone: shaded region from 80% to end
    alert_zone = f'<rect x="{alert_x:.1f}" y="{height//2-4}" width="{width-alert_x:.1f}" height="8" rx="2" fill="#fb923c" opacity="0.12"/>' if True else ""

    svg = f"""<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <!-- background track -->
  <rect x="0" y="{height//2-4}" width="{width}" height="8" rx="4" fill="#2d2d3d"/>
  <!-- alert zone (>80%) -->
  {alert_zone}
  <!-- filled portion -->
  <rect x="0" y="{height//2-4}" width="{dot_x:.1f}" height="8" rx="4" fill="{dot_color}" opacity="0.7"/>
  <!-- 80% threshold tick -->
  <line x1="{alert_x:.1f}" y1="{height//2-8}" x2="{alert_x:.1f}" y2="{height//2+8}" stroke="#fb923c" stroke-width="1" stroke-dasharray="2,2" opacity="0.6"/>
  <!-- soft limit line -->
  <line x1="{soft_x:.1f}" y1="{height//2-10}" x2="{soft_x:.1f}" y2="{height//2+10}" stroke="#facc15" stroke-width="1.5" stroke-dasharray="3,2"/>
  <!-- hard limit line -->
  <line x1="{hard_x:.1f}" y1="{height//2-10}" x2="{hard_x:.1f}" y2="{height//2+10}" stroke="#f87171" stroke-width="1.5" stroke-dasharray="3,2"/>
  <!-- alert ring (if above 80%) -->
  {ring_svg}
  <!-- dot -->
  <circle cx="{dot_x:.1f}" cy="{height//2}" r="{dot_r}" fill="{dot_color}" stroke="#111" stroke-width="1.5"/>
  <!-- min label -->
  <text x="2" y="{height-2}" font-size="9" fill="#555" font-family="monospace">{vmin:.2f}</text>
  <!-- max label -->
  <text x="{width-2}" y="{height-2}" font-size="9" fill="#555" font-family="monospace" text-anchor="end">{vmax:.2f}</text>
  <!-- position % label above dot -->
  <text x="{dot_x:.1f}" y="{height//2-12}" font-size="11" fill="{pos_color}" font-family="monospace" text-anchor="middle" font-weight="bold">{pos_label}</text>
</svg>"""
    return svg

# ── Stop monitor bar (bidirectional SVG) ─────────────────────────────────────
def stop_bar_svg(budget_abs: float, pnl_mtd: float, budget_max: float,
                 width=300, height=54, soft_mark=None) -> str:
    """
    Single-track bidirectional bar.
    Origin = start of month (zero). Left = loss (red). Right = gain (green).
    Stop line = red vertical at -budget_abs.
    Soft mark = yellow dashed (CI only).
    Right side: 'margem Xbps' = distance from current PnL to hard stop.
    """
    LPAD, RPAD = 4, 4
    bmax = max(budget_max, STOP_BASE, 1.0)
    bar_w = width - LPAD - RPAD

    # origin at 55% → more space on the loss side
    origin_frac = 0.55
    origin_x    = LPAD + bar_w * origin_frac
    loss_px     = bar_w * origin_frac          # pixels available left of origin
    gain_px     = bar_w * (1 - origin_frac)    # pixels available right of origin
    loss_scale  = loss_px / (bmax * 1.05)      # px per bps, loss side
    gain_scale  = gain_px / (bmax * 0.65)      # px per bps, gain side (compressed)

    # current PnL dot/bar end position
    if pnl_mtd < 0:
        pnl_x = max(LPAD + 2.0, origin_x + pnl_mtd * loss_scale)
    else:
        pnl_x = min(float(width - RPAD - 2), origin_x + pnl_mtd * gain_scale)

    # stop line position
    if budget_abs > 0:
        stop_x = max(LPAD + 2.0, origin_x - budget_abs * loss_scale)
    else:
        stop_x = origin_x   # gancho: stop at zero

    # soft mark position (CI)
    soft_x = None
    if soft_mark is not None and soft_mark > 0:
        soft_x = max(LPAD + 2.0, origin_x - soft_mark * loss_scale)

    is_gain   = pnl_mtd >= 0
    bar_color = "#4ade80" if is_gain else "#f87171"
    fill_x    = min(origin_x, pnl_x)
    fill_w    = abs(pnl_x - origin_x)

    dist       = budget_abs + pnl_mtd      # room left (positive = safe)
    dist_color = "#4ade80" if dist > 0 else "#f87171"
    dist_label = f"+{dist:.0f}" if dist > 0 else f"{dist:.0f}"

    y_mid = 22   # shifted down from 16 so top label has room above the bar
    bh    = 12

    parts = [f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">']

    # Background track
    parts.append(f'<rect x="{LPAD}" y="{y_mid-bh//2}" width="{bar_w}" height="{bh}" rx="3" fill="#1e293b"/>')
    # Subtle gain-side tint
    parts.append(f'<rect x="{origin_x:.1f}" y="{y_mid-bh//2}" width="{gain_px:.1f}" height="{bh}" fill="#14241a" opacity="0.6"/>')

    # Blue budget-available bar: stop_x → origin_x (shows remaining room before stop)
    if budget_abs > 0:
        bud_x = stop_x
        bud_w = origin_x - stop_x
        if bud_w > 1:
            parts.append(f'<rect x="{bud_x:.1f}" y="{y_mid-bh//2}" width="{bud_w:.1f}" height="{bh}" fill="#1e4976" opacity="0.55"/>')

    # PnL fill (drawn on top of budget bar)
    if fill_w > 0.5:
        parts.append(f'<rect x="{fill_x:.1f}" y="{y_mid-bh//2}" width="{fill_w:.1f}" height="{bh}" rx="2" fill="{bar_color}" opacity="0.85"/>')

    # Origin tick
    parts.append(f'<line x1="{origin_x:.1f}" y1="{y_mid-bh//2-3}" x2="{origin_x:.1f}" y2="{y_mid+bh//2+3}" stroke="#64748b" stroke-width="1.5"/>')

    # Hard stop line
    if budget_abs > 0:
        parts.append(f'<line x1="{stop_x:.1f}" y1="{y_mid-bh//2-4}" x2="{stop_x:.1f}" y2="{y_mid+bh//2+4}" stroke="#f87171" stroke-width="2.5"/>')
        parts.append(f'<text x="{stop_x:.1f}" y="{y_mid+bh//2+15}" font-size="8" fill="#f87171" font-family="monospace" text-anchor="middle">-{budget_abs:.0f}</text>')
        # Base-63 tick: always shown as reference for non-CI PMs
        if soft_mark is None:  # i.e. not CI
            base_x = max(LPAD + 2.0, origin_x - STOP_BASE * loss_scale)
            # Only draw if base is not the same position as the stop line
            if abs(base_x - stop_x) > 3:
                parts.append(f'<line x1="{base_x:.1f}" y1="{y_mid-bh//2}" x2="{base_x:.1f}" y2="{y_mid+bh//2+13}" stroke="#60a5fa" stroke-width="1" stroke-dasharray="2,2" opacity="0.7"/>')
                parts.append(f'<text x="{base_x:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#60a5fa" font-family="monospace" text-anchor="middle">-{STOP_BASE:.0f}</text>')
    else:
        # Gancho: shade entire loss territory red
        parts.append(f'<rect x="{LPAD}" y="{y_mid-bh//2}" width="{origin_x-LPAD:.1f}" height="{bh}" rx="3" fill="#f8717130"/>')
        parts.append(f'<text x="{origin_x-4:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#fb923c" font-family="monospace" text-anchor="end">GANCHO</text>')

    # Soft mark (CI only)
    if soft_x is not None:
        parts.append(f'<line x1="{soft_x:.1f}" y1="{y_mid-bh//2-4}" x2="{soft_x:.1f}" y2="{y_mid+bh//2+4}" stroke="#facc15" stroke-width="1.5" stroke-dasharray="3,2"/>')
        parts.append(f'<text x="{soft_x:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#facc15" font-family="monospace" text-anchor="middle">-{soft_mark:.0f}</text>')

    # MTD value label above the bar tip (the only text label — keeps graphic self-contained)
    pnl_label = f"+{pnl_mtd:.1f}" if is_gain else f"{pnl_mtd:.1f}"
    parts.append(f'<text x="{pnl_x:.1f}" y="{y_mid-bh//2-5}" font-size="9" fill="{bar_color}" font-family="monospace" text-anchor="middle" font-weight="bold">{pnl_label}</text>')

    parts.append('</svg>')
    return '\n'.join(parts)

# ── MACRO Exposure ───────────────────────────────────────────────────────────

_RF_ORDER  = ["RF-BZ","RF-DM","RF-EM","FX-BRL","FX-DM","FX-EM",
              "RV-BZ","RV-DM","RV-EM","COMMODITIES","P-Metals"]
_RF_COLOR  = {"RF-BZ":"#60a5fa","RF-DM":"#93c5fd","RF-EM":"#bfdbfe",
              "FX-BRL":"#a78bfa","FX-DM":"#c4b5fd","FX-EM":"#ddd6fe",
              "RV-BZ":"#34d399","RV-DM":"#6ee7b7","RV-EM":"#a7f3d0",
              "COMMODITIES":"#fb923c","P-Metals":"#fbbf24"}
_EXCL_PRIM = {"Cash", "Provisions and Costs", "Margin"}
_RATE_PRIM = {"Brazil Sovereign Yield", "BRL Rate Curve", "BRD Rate Curve"}

def _parse_rf(book: str) -> str:
    """Extract risk factor from BOOK name like 'JD_RF-BZ_Direcional' → 'RF-BZ'."""
    PMS = ("CI","LF","JD","RJ","QM","MD")
    parts = book.split("_")
    if len(parts) >= 2 and parts[0] in PMS:
        return parts[1]
    return None   # structural book

def _parse_pm(book: str) -> str:
    """Extract PM prefix from BOOK name like 'JD_RF-BZ_Direcional' → 'JD'."""
    PMS = ("CI","LF","JD","RJ","QM","MD")
    parts = book.split("_")
    if len(parts) >= 1 and parts[0] in PMS:
        return parts[0]
    return "Outros"

_PM_LIVRO = {"CI": "CI", "LF": "Macro_LF", "JD": "Macro_JD", "RJ": "Macro_RJ", "QM": "Macro_QM"}

def fetch_macro_pnl_products(date_str: str) -> pd.DataFrame:
    """Daily PnL per LIVRO × PRODUCT from REPORT_ALPHA_ATRIBUTION."""
    livros = ", ".join(f"'{v}'" for v in _PM_LIVRO.values())
    return read_sql(f"""
        SELECT "LIVRO", "PRODUCT", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'MACRO'
          AND "DATE"  = DATE '{date_str}'
          AND "LIVRO" IN ({livros})
        GROUP BY "LIVRO", "PRODUCT"
    """)

def _prev_bday(date_str: str) -> str:
    d = pd.Timestamp(date_str)
    return (d - pd.tseries.offsets.BusinessDay(1)).strftime("%Y-%m-%d")

# ── Distribution analysis (252d) ────────────────────────────────────────────
# PORTIFOLIO names → label + scope used when joining with REPORT_ALPHA_ATRIBUTION
# Scope kind: 'fund' (sum all MACRO), 'livro' (LIVRO = value), 'rf' (books where _parse_rf == value)
_DIST_PORTFOLIOS = [
    # (portfolio_name, label, kind, key, fund_short)
    ("MACRO",       "MACRO total",        "fund",  "MACRO",    "MACRO"),
    ("EVOLUTION",   "EVOLUTION total",    "fund",  "EVOLUTION","EVOLUTION"),
    ("CI",          "CI (Comitê)",        "livro", "CI",       "MACRO"),
    ("LF",          "LF — Luiz Felipe",   "livro", "Macro_LF", "MACRO"),
    ("JD",          "JD — Joca Dib",      "livro", "Macro_JD", "MACRO"),
    ("RJ",          "RJ — Rodrigo Jafet", "livro", "Macro_RJ", "MACRO"),
    ("RF-BZ",       "Fator · RF-BZ",      "rf",    "RF-BZ",    "MACRO"),
    ("RF-DM",       "Fator · RF-DM",      "rf",    "RF-DM",    "MACRO"),
    ("RF-EM",       "Fator · RF-EM",      "rf",    "RF-EM",    "MACRO"),
    ("FX-BRL",      "Fator · FX-BRL",     "rf",    "FX-BRL",   "MACRO"),
    ("FX-DM",       "Fator · FX-DM",      "rf",    "FX-DM",    "MACRO"),
    ("FX-EM",       "Fator · FX-EM",      "rf",    "FX-EM",    "MACRO"),
    ("RV-BZ",       "Fator · RV-BZ",      "rf",    "RV-BZ",    "MACRO"),
    ("RV-DM",       "Fator · RV-DM",      "rf",    "RV-DM",    "MACRO"),
    ("RV-EM",       "Fator · RV-EM",      "rf",    "RV-EM",    "MACRO"),
    ("COMMODITIES", "Fator · COMMO",      "rf",    "COMMODITIES","MACRO"),
    ("P-Metals",    "Fator · P-Metals",   "rf",    "P-Metals", "MACRO"),
]

def fetch_pnl_distribution(date_str: str = DATA_STR) -> dict:
    """Fetch 252d W series per PORTIFOLIO for PORTIFOLIO_DATE = date_str.
       Returns dict {portfolio_name: np.array of W values (bps)}."""
    q = f"""
    SELECT "PORTIFOLIO", "DATE_SYNTHETIC_POSITION", "W"
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}'
    ORDER BY "PORTIFOLIO", "DATE_SYNTHETIC_POSITION"
    """
    df = read_sql(q)
    if df.empty:
        return {}
    return {p: g["W"].to_numpy() for p, g in df.groupby("PORTIFOLIO")}

def fetch_pnl_actual_by_cut(date_str: str = DATA_STR) -> dict:
    """Realized DIA (bps of NAV) today for fund / PM / factor cuts.
       Returns dict keyed by scope-key (FUNDO name, LIVRO, or RF)."""
    # Fund-level (MACRO, EVOLUTION, …)
    fund_df = read_sql(f"""
        SELECT "FUNDO", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "DATE" = DATE '{date_str}'
        GROUP BY "FUNDO"
    """)
    actuals = {f'fund:{r["FUNDO"]}': float(r["dia_bps"]) for _, r in fund_df.iterrows()}

    # LIVRO-level (PMs within MACRO)
    livro_df = read_sql(f"""
        SELECT "LIVRO", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "DATE" = DATE '{date_str}' AND "FUNDO" = 'MACRO'
        GROUP BY "LIVRO"
    """)
    for _, r in livro_df.iterrows():
        actuals[f'livro:{r["LIVRO"]}'] = float(r["dia_bps"])

    # RF-level (factor, via BOOK parse) — sum DIA of MACRO rows grouping by parsed RF
    book_df = read_sql(f"""
        SELECT "BOOK", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "DATE" = DATE '{date_str}' AND "FUNDO" = 'MACRO'
        GROUP BY "BOOK"
    """)
    if not book_df.empty:
        book_df["rf"] = book_df["BOOK"].apply(_parse_rf)
        rf_agg = book_df.dropna(subset=["rf"]).groupby("rf")["dia_bps"].sum()
        for rf, v in rf_agg.items():
            actuals[f'rf:{rf}'] = float(v)
    return actuals

def compute_distribution_stats(w_series, actual_bps=None):
    """Returns dict with forward-looking stats and (optional) backward comparison."""
    import numpy as np
    if w_series is None or len(w_series) < 30:
        return None
    w = np.asarray(w_series, dtype=float)
    w = w[~np.isnan(w)]
    if len(w) < 30:
        return None
    sd = float(np.std(w, ddof=1))
    out = {
        "n":     int(len(w)),
        "min":   float(np.min(w)),
        "max":   float(np.max(w)),
        "mean":  float(np.mean(w)),
        "sd":    sd,
        "var95": float(np.percentile(w,  5)),   # 5th pct (loss tail) = VaR 95%
        "var_p95": float(np.percentile(w, 95)), # 95th pct (gain tail)
    }
    if actual_bps is not None:
        pct = float((w < actual_bps).sum()) / len(w) * 100.0
        out["actual"]     = float(actual_bps)
        out["percentile"] = pct
        out["nvols"]      = actual_bps / sd if sd > 1e-9 else None
    return out

def _latest_nav(desk: str, date_str: str):
    """
    Most recent NAV on or before `date_str` for `desk`.
    NAV often lags the risk feed by up to a business day (admin process),
    so callers should tolerate forward-filling from D-1.
    """
    df = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = '{desk}'
          AND "VAL_DATE" <= DATE '{date_str}'
        ORDER BY "VAL_DATE" DESC LIMIT 1
    """)
    return float(df["NAV"].iloc[0]) if not df.empty else None


def fetch_macro_exposure(date_str: str = DATA_STR) -> tuple:
    """Returns (df_expo, df_var, aum) for the given date."""
    aum = _latest_nav("Galapagos Macro FIM", date_str) or 1.0

    expo = read_sql(f"""
        SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS", "PRIMITIVE_CLASS",
               SUM("DELTA")                   AS delta,
               SUM("DELTA" * "MOD_DURATION")  AS delta_dur
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Evolution FIC FIM CP'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "BOOK" ~* 'CI|JD|LF|QM|RJ'
          AND "BOOK" ~* 'Direcional|Relativo|Hedge|Volatilidade|SS'
        GROUP BY "BOOK", "PRODUCT", "PRODUCT_CLASS", "PRIMITIVE_CLASS"
    """)
    expo["rf"] = expo["BOOK"].apply(_parse_rf)
    expo = expo[expo["rf"].notna() & ~expo["PRIMITIVE_CLASS"].isin(_EXCL_PRIM)]
    # Exclude FX hedges: USDBRL futures in non-FX books
    expo = expo[~((expo["PRIMITIVE_CLASS"] == "FX") & (~expo["rf"].str.startswith("FX-", na=False)))]
    expo["pm"]      = expo["BOOK"].apply(_parse_pm)
    expo["pct_nav"] = expo["delta"]     * 100 / aum
    expo["dur_pct"] = expo["delta_dur"] * 100 / aum

    var_df = read_sql(f"""
        SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS",
               SUM("PARAMETRIC_VAR") AS var_brl
        FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
        WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
          AND "VAL_DATE"     = DATE '{date_str}'
          AND "LEVEL"        = 3
          AND "BOOK" IN ('RF-BZ','RF-DM','RF-EM','FX-BRL','FX-DM','FX-EM',
                         'RV-BZ','RV-DM','RV-EM','COMMODITIES','P-Metals')
        GROUP BY "BOOK", "PRODUCT", "PRODUCT_CLASS"
    """)
    var_df.rename(columns={"BOOK": "rf"}, inplace=True)
    var_df["var_pct"] = var_df["var_brl"] * -10000 / aum

    # Instrument volatility (annualised σ from STANDARD_DEVIATION_ASSETS, BOOK='MACRO')
    sigma_df = read_sql(f"""
        SELECT "INSTRUMENT", "STANDARD_DEVIATION" AS sigma
        FROM q_models."STANDARD_DEVIATION_ASSETS"
        WHERE "VAL_DATE" = DATE '{date_str}'
          AND "BOOK"     = 'MACRO'
    """)
    expo = expo.merge(sigma_df.rename(columns={"INSTRUMENT": "PRODUCT"}),
                      on="PRODUCT", how="left")

    return expo, var_df, aum

_ETF_TO_LIST = {"BOVA11": "IBOV", "SMAL11": "SMLLBV"}  # ETF → EQUITIES_COMPOSITION list


def _fetch_single_names_generic(date_str: str, desk: str, source: str,
                                 extra_where: str = "") -> tuple:
    """
    Generic BR single-name L/S with index explosion.

    Direct equities plus decomposition of:
      - IBOV future (WIN) → IBOV constituents
      - BOVA11 ETF       → IBOV constituents
      - SMAL11 ETF       → SMLLBV constituents

    Parameters
    ----------
    desk   : TRADING_DESK filter (None → no filter, any desk that matches `source`)
    source : TRADING_DESK_SHARE_SOURCE filter (the fund whose look-through we want)
    extra_where : optional extra predicate (e.g. BOOK IN (...)) added to both direct + fut queries.

    Returns
    -------
    (merged_df, nav, index_legs) or (None, None, None) if no NAV.
      merged_df columns: ticker, direct, from_idx, net, pct_nav
      index_legs: dict with per-source deltas {'WIN': x, 'BOVA11': y, 'SMAL11': z}
    """
    nav = _latest_nav(source, date_str)
    if nav is None:
        return None, None, None

    desk_clause = f"AND \"TRADING_DESK\" = '{desk}'" if desk else ""

    direct_all = read_sql(f"""
        SELECT "PRODUCT" AS ticker, SUM("DELTA") AS direct
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = '{source}'
          {desk_clause}
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "PRODUCT_CLASS"             IN ('Equity','Equity Receipts')
          AND "IS_STOCK"                  = TRUE
          {extra_where}
        GROUP BY "PRODUCT"
    """)

    # split ETFs that should be exploded vs. stocks kept as direct
    etf_mask = direct_all["ticker"].isin(_ETF_TO_LIST)
    direct_etf = direct_all[etf_mask].copy()
    direct    = direct_all[~etf_mask].copy()

    # ADR and ADR-option equity leg → map to BR ticker via PRIMITIVE_NAME.
    # Filter: PRIMITIVE_CLASS='Equity' drops FX legs; regex keeps only BR-style
    # tickers (VALE3, PETR4, ITSA11 etc.) and excludes foreign primitives (e.g. BABA → '9988 HK').
    adr = read_sql(f"""
        SELECT "PRIMITIVE_NAME" AS ticker, SUM("DELTA") AS adr_delta
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = '{source}'
          {desk_clause}
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "PRODUCT_CLASS"             IN ('ADR','ADR Options')
          AND "PRIMITIVE_CLASS"           = 'Equity'
          AND "PRIMITIVE_NAME"            ~ '^[A-Z]{{4}}[0-9]{{1,2}}$'
          {extra_where}
        GROUP BY "PRIMITIVE_NAME"
    """)
    adr_total = float(adr["adr_delta"].sum()) if not adr.empty else 0.0
    if not adr.empty:
        direct = direct.merge(adr, on="ticker", how="outer").fillna(0.0)
        direct["direct"] = direct["direct"] + direct["adr_delta"]
        direct = direct.drop(columns="adr_delta")

    fut = read_sql(f"""
        SELECT SUM("DELTA") AS delta
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = '{source}'
          {desk_clause}
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "PRODUCT_CLASS"             = 'IBOVSPFuture'
          AND "PRIMITIVE_CLASS"           = 'Equity'
          {extra_where}
    """)
    fut_delta = float(fut["delta"].iloc[0]) if not fut.empty and pd.notna(fut["delta"].iloc[0]) else 0.0

    # latest composition per list (IBOV + SMLLBV) as of date_str
    compo = read_sql(f"""
        WITH latest AS (
          SELECT "LIST_NAME", MAX("DATE") AS d
          FROM public."EQUITIES_COMPOSITION"
          WHERE "LIST_NAME" IN ('IBOV','SMLLBV')
            AND "DATE" <= DATE '{date_str}'
          GROUP BY "LIST_NAME"
        )
        SELECT ec."LIST_NAME" AS list, ec."INSTRUMENT" AS ticker, ec."VALUE" AS weight
        FROM public."EQUITIES_COMPOSITION" ec
        JOIN latest l
          ON l."LIST_NAME" = ec."LIST_NAME" AND l.d = ec."DATE"
    """)

    # individual ETF deltas for stats breakdown
    bova_delta = float(direct_etf.loc[direct_etf["ticker"] == "BOVA11", "direct"].sum()) if not direct_etf.empty else 0.0
    smal_delta = float(direct_etf.loc[direct_etf["ticker"] == "SMAL11", "direct"].sum()) if not direct_etf.empty else 0.0

    # IBOV explosion = fut (WIN) + BOVA11
    ibov_w = compo[compo["list"] == "IBOV"][["ticker", "weight"]].copy()
    ibov_w["from_idx_ibov"] = (fut_delta + bova_delta) * ibov_w["weight"]
    # SMLLBV explosion = SMAL11
    smll_w = compo[compo["list"] == "SMLLBV"][["ticker", "weight"]].copy()
    smll_w["from_idx_smll"] = smal_delta * smll_w["weight"]

    merged = direct.merge(ibov_w[["ticker", "from_idx_ibov"]], on="ticker", how="outer")
    merged = merged.merge(smll_w[["ticker", "from_idx_smll"]], on="ticker", how="outer").fillna(0.0)
    merged["from_idx"] = merged["from_idx_ibov"] + merged["from_idx_smll"]
    merged["net"]      = merged["direct"] + merged["from_idx"]
    merged["pct_nav"]  = merged["net"] * 100 / nav
    merged = merged[merged["net"].abs() > 1].copy()
    merged = merged.sort_values("pct_nav", key=lambda s: s.abs(), ascending=False)
    merged = merged[["ticker", "direct", "from_idx", "net", "pct_nav"]]

    index_legs = {
        "WIN":    fut_delta,
        "BOVA11": bova_delta,
        "SMAL11": smal_delta,
        "ADR":    adr_total,
    }
    return merged, nav, index_legs


def fetch_quant_single_names(date_str: str = DATA_STR) -> tuple:
    """QUANT single-name L/S — only rows from the QUANT desk (no FIC look-through)."""
    return _fetch_single_names_generic(
        date_str, desk="Galapagos Quantitativo FIM", source="Galapagos Quantitativo FIM"
    )


def fetch_albatroz_exposure(date_str: str = DATA_STR) -> tuple:
    """
    RF exposure snapshot for ALBATROZ — position-level from LOTE_PRODUCT_EXPO.
    Returns (df, nav) where df columns are:
      BOOK, PRODUCT_CLASS, PRODUCT, delta_brl, mod_dur (years), dv01_brl (R$/bp), indexador
    DV01 ≈ |DELTA × MOD_DURATION × 0.0001|.
    """
    nav = _latest_nav("GALAPAGOS ALBATROZ FIRF LP", date_str)
    if nav is None:
        return None, None

    df = read_sql(f"""
        SELECT "BOOK", "PRODUCT_CLASS", "PRODUCT",
               SUM("DELTA")                                                 AS delta_brl,
               MAX("MOD_DURATION")                                          AS mod_dur,
               SUM("DELTA" * COALESCE("MOD_DURATION", 0) * 0.0001)          AS dv01_brl
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK"              = 'GALAPAGOS ALBATROZ FIRF LP'
          AND "TRADING_DESK_SHARE_SOURCE" = 'GALAPAGOS ALBATROZ FIRF LP'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "DELTA" <> 0
        GROUP BY "BOOK", "PRODUCT_CLASS", "PRODUCT"
    """)
    if df.empty:
        return df, nav
    cls_to_idx = {
        "DI1Future": "Pré", "NTN-F": "Pré", "LTN": "Pré",
        "NTN-B": "IPCA",    "DAP Future": "IPCA", "DAPFuture": "IPCA",
        "NTN-C": "IGP-M",   "DAC Future": "IGP-M",
        "LFT": "CDI",       "Cash": "CDI",    "Overnight": "CDI",
        "Compromissada": "CDI",
    }
    df["indexador"] = df["PRODUCT_CLASS"].map(cls_to_idx).fillna("Outros")
    for c in ("delta_brl", "mod_dur", "dv01_brl"):
        df[c] = df[c].astype(float).fillna(0.0)
    return df, nav


def build_albatroz_exposure(df: pd.DataFrame, nav: float) -> str:
    """RF exposure card for ALBATROZ — summary by indexador + top positions by |DV01|."""
    if df is None or df.empty or not nav:
        return ""

    # Duration-weighted aggregate (only rows with non-zero delta contribute)
    abs_delta = df["delta_brl"].abs().sum()
    dur_w = (df["delta_brl"].abs() * df["mod_dur"]).sum() / abs_delta if abs_delta else 0.0
    total_dv01 = df["dv01_brl"].sum()
    total_delta = df["delta_brl"].sum()

    # ── By Indexador summary ───────────────────────────────────────────
    idx_order = ["Pré", "IPCA", "IGP-M", "CDI", "Outros"]
    by_idx = df.groupby("indexador").agg(
        delta_brl=("delta_brl", "sum"),
        dv01_brl=("dv01_brl", "sum"),
        gross_brl=("delta_brl", lambda s: s.abs().sum()),
        dur_w=("delta_brl", lambda s: (s.abs() * df.loc[s.index, "mod_dur"]).sum() / s.abs().sum()
                                     if s.abs().sum() else 0.0),
    ).reset_index()
    by_idx = by_idx.set_index("indexador").reindex(idx_order).reset_index().dropna(how="all", subset=["delta_brl"])

    def bp_pct(v_brl):
        pct = v_brl * 100 / nav
        color = "var(--up)" if v_brl >= 0 else "var(--down)"
        return f'<td class="t-num mono" style="color:{color}">{pct:+.2f}%</td>'

    def mm(v):
        return f"{v/1e6:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")

    def mm_cell(v):
        return f'<td class="t-num mono" style="color:var(--muted)">{mm(v)}</td>'

    def dv01_cell(v):
        if abs(v) < 1:
            return '<td class="t-num mono" style="color:var(--muted)">—</td>'
        color = "var(--up)" if v >= 0 else "var(--down)"
        return f'<td class="t-num mono" style="color:{color}">{v/1e3:+,.1f}</td>'

    def dur_cell(v):
        if v is None or abs(v) < 0.01:
            return '<td class="t-num mono" style="color:var(--muted)">—</td>'
        return f'<td class="t-num mono" style="color:var(--muted)">{v:.2f}</td>'

    idx_rows = "".join(
        f'<tr>'
        f'<td class="pa-name" style="font-weight:600">{r.indexador}</td>'
        + bp_pct(r.delta_brl)
        + mm_cell(r.gross_brl)
        + dur_cell(r.dur_w)
        + dv01_cell(r.dv01_brl)
        + "</tr>"
        for r in by_idx.itertuples(index=False)
    )
    idx_total_row = (
        '<tr class="pa-total-row">'
        '<td class="pa-name" style="font-weight:700">Total</td>'
        + bp_pct(total_delta)
        + f'<td class="t-num mono" style="color:var(--muted); font-weight:700">{mm(abs_delta)}</td>'
        + (f'<td class="t-num mono" style="color:var(--muted); font-weight:700">{dur_w:.2f}</td>'
           if dur_w else '<td class="t-num mono" style="color:var(--muted)">—</td>')
        + (f'<td class="t-num mono" style="font-weight:700; color:{"var(--up)" if total_dv01>=0 else "var(--down)"}">{total_dv01/1e3:+,.1f}</td>'
           if abs(total_dv01) >= 1 else '<td class="t-num mono" style="color:var(--muted)">—</td>')
        + "</tr>"
    )

    # ── Top positions by |DV01| ────────────────────────────────────────
    top = df.copy()
    top["abs_dv01"] = top["dv01_brl"].abs()
    top = top.sort_values("abs_dv01", ascending=False).head(15)
    pos_rows = "".join(
        f'<tr>'
        f'<td class="pa-name">{r.PRODUCT}</td>'
        f'<td class="pa-name" style="color:var(--muted); font-size:11px">{r.indexador}</td>'
        f'<td class="pa-name" style="color:var(--muted); font-size:11px">{r.BOOK}</td>'
        + bp_pct(r.delta_brl)
        + dur_cell(r.mod_dur)
        + dv01_cell(r.dv01_brl)
        + "</tr>"
        for r in top.itertuples(index=False)
    )

    nav_fmt = f"{nav/1e6:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposure RF</span>
        <span class="card-sub">— ALBATROZ · NAV R$ {nav_fmt}M · Duration agregada {dur_w:.2f}y · DV01 {total_dv01/1e3:+,.1f}k R$/bp</span>
      </div>

      <div class="sn-inline-stats mono" style="margin-bottom:8px">
        <span style="color:var(--muted); font-size:10.5px; letter-spacing:.12em; text-transform:uppercase">Por Indexador</span>
      </div>
      <table class="pa-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Indexador</th>
          <th style="text-align:right">Net (%NAV)</th>
          <th style="text-align:right">Gross (R$M)</th>
          <th style="text-align:right">Mod Dur (y)</th>
          <th style="text-align:right">DV01 (kR$/bp)</th>
        </tr></thead>
        <tbody>{idx_rows}</tbody>
        <tfoot>{idx_total_row}</tfoot>
      </table>

      <div class="sn-inline-stats mono" style="margin:16px 0 8px">
        <span style="color:var(--muted); font-size:10.5px; letter-spacing:.12em; text-transform:uppercase">Top 15 Posições — por |DV01|</span>
      </div>
      <table class="pa-table">
        <thead><tr>
          <th style="text-align:left">Produto</th>
          <th style="text-align:left">Idx</th>
          <th style="text-align:left">Book</th>
          <th style="text-align:right">Net (%NAV)</th>
          <th style="text-align:right">Mod Dur (y)</th>
          <th style="text-align:right">DV01 (kR$/bp)</th>
        </tr></thead>
        <tbody>{pos_rows}</tbody>
      </table>
    </section>"""


ALBATROZ_STOP_BPS = 150.0  # monthly loss budget (bps)


def build_albatroz_risk_budget(df_pa: pd.DataFrame) -> str:
    """
    Risk Budget card for ALBATROZ — 150 bps/month stop.
    Reuses the stop_bar_svg helper from the MACRO stop monitor.
    Pulls MTD + YTD alpha from the already-fetched `df_pa` (REPORT_ALPHA_ATRIBUTION leaves).
    """
    if df_pa is None or df_pa.empty:
        return ""
    alb = df_pa[df_pa["FUNDO"] == "ALBATROZ"]
    if alb.empty:
        return ""

    mtd_bps = float(alb["mtd_bps"].sum())
    ytd_bps = float(alb["ytd_bps"].sum())
    dia_bps = float(alb["dia_bps"].sum())

    budget_abs = ALBATROZ_STOP_BPS
    consumed_bps = abs(mtd_bps) if mtd_bps < 0 else 0.0
    consumed_pct = consumed_bps / budget_abs * 100.0

    # Status semáforo
    if consumed_pct >= 100:
        status_label, status_color = "🔴 STOP", "var(--down)"
    elif consumed_pct >= 70:
        status_label, status_color = "🟡 ATENÇÃO", "var(--warn)"
    elif consumed_pct >= 50:
        status_label, status_color = "🟡 SOFT", "var(--warn)"
    else:
        status_label, status_color = "🟢 OK", "var(--up)"

    margem_bps = budget_abs + mtd_bps  # distance from stop (in bps, positive = room left)
    bar = stop_bar_svg(budget_abs, mtd_bps, budget_abs * 1.2, width=340, height=56)

    dia_color = "var(--up)" if dia_bps >= 0 else "var(--down)"
    mtd_color = "var(--up)" if mtd_bps >= 0 else "var(--down)"
    ytd_color = "var(--up)" if ytd_bps >= 0 else "var(--down)"

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risk Budget</span>
        <span class="card-sub">— ALBATROZ · stop mensal {budget_abs:.0f} bps · alpha vs. CDI</span>
      </div>
      <table class="metric-table" data-no-sort="1" style="width:100%">
        <thead>
          <tr class="col-headers">
            <th>Status</th>
            <th style="text-align:right">DIA</th>
            <th style="text-align:right">MTD</th>
            <th style="text-align:right">YTD</th>
            <th>Consumo do stop</th>
            <th style="text-align:right">Margem</th>
          </tr>
        </thead>
        <tbody>
          <tr class="metric-row">
            <td class="metric-name"><span style="color:{status_color}; font-weight:700">{status_label}</span></td>
            <td class="value-cell mono" style="color:{dia_color}">{dia_bps:+.1f} bps</td>
            <td class="value-cell mono" style="color:{mtd_color}; font-weight:700">{mtd_bps:+.1f} bps</td>
            <td class="value-cell mono" style="color:{ytd_color}">{ytd_bps:+.1f} bps</td>
            <td class="bar-cell">{bar}</td>
            <td class="util-cell mono" style="color:{status_color}; font-weight:700">{margem_bps:+.1f} bps</td>
          </tr>
        </tbody>
      </table>
      <div class="bar-legend">
        consumo: <span style="color:{status_color}; font-weight:600">{consumed_pct:.0f}%</span> do stop
        &nbsp;·&nbsp; stop em <span class="mono">-{budget_abs:.0f} bps</span>
        &nbsp;·&nbsp; <span style="color:var(--muted)">origem = início do mês</span>
      </div>
    </section>"""


def fetch_evolution_single_names(date_str: str = DATA_STR) -> tuple:
    """
    EVOLUTION single-name L/S — full look-through into QUANT (Bracco, Quant_PA),
    Evo Strategy (FMN_*, FCO, AÇÕES BR LONG), Frontier Ações, and Macro FIM (CI_COMMODITIES).
    ADRs / ADR Options / ETFs / FIIs excluded automatically by the PRODUCT_CLASS filter.
    """
    return _fetch_single_names_generic(
        date_str, desk=None, source="Galapagos Evolution FIC FIM CP"
    )

def build_exposure_section(df_expo: pd.DataFrame, df_var: pd.DataFrame, aum: float,
                           df_expo_d1: pd.DataFrame = None, df_var_d1: pd.DataFrame = None,
                           df_pnl_prod: pd.DataFrame = None, pm_margem: dict = None) -> str:
    """One row per risk factor. Drill-down: Product_Class+Product with Expo/VaR/Δ. Toggle: PM VaR."""

    def mini_bar(val, scale=35.0, width=110, color=None):
        w = min(abs(val) / scale * (width / 2), width / 2)
        c = color or ("#f87171" if val < 0 else "#4ade80")
        mid = width / 2
        x  = mid - w if val < 0 else mid
        return (f'<svg width="{width}" height="12" style="vertical-align:middle">'
                f'<line x1="{mid}" y1="0" x2="{mid}" y2="12" stroke="#334155" stroke-width="1"/>'
                f'<rect x="{x:.1f}" y="2" width="{w:.1f}" height="8" rx="2" fill="{c}" opacity="0.85"/>'
                f'</svg>')

    def dual_bar(expo_val, dur_val, scale=35.0, width=160):
        """Two stacked bars: top=notional expo (% NAV), bottom=duration-weighted (% NAV). Rate only."""
        def bsvg(val, y0, bh, clr):
            w = min(abs(val) / scale * (width / 2), width / 2)
            c = clr if val < 0 else "#4ade80"
            mid = width / 2
            x = mid - w if val < 0 else mid
            tx = (mid - w - 3) if val < 0 else (mid + w + 3)
            anchor = "end" if val < 0 else "start"
            return (f'<rect x="{x:.1f}" y="{y0}" width="{w:.1f}" height="{bh}" rx="2" fill="{c}" opacity="0.85"/>'
                    f'<text x="{tx:.1f}" y="{y0+bh-1}" font-size="8" fill="{c}" '
                    f'font-family="monospace" text-anchor="{anchor}">{val:+.1f}%</text>')
        mid = width / 2
        return (f'<svg width="{width}" height="28" style="vertical-align:middle">'
                f'<line x1="{mid}" y1="0" x2="{mid}" y2="28" stroke="#334155" stroke-width="1"/>'
                f'<text x="2" y="8" font-size="7" fill="#475569">EXPO</text>'
                f'{bsvg(expo_val, 0, 9, "#f87171")}'
                f'<text x="2" y="22" font-size="7" fill="#475569">DUR</text>'
                f'{bsvg(dur_val, 14, 9, "#fb923c")}'
                f'</svg>')

    def fmt_brl(delta):
        return f'{delta/1e6:+.0f}M'

    def delta_str(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "—", "#475569"
        c = "#f87171" if val < 0 else "#4ade80"
        return f'{val:+.1f}', c

    def dv(val):
        """Numeric string for data-val attribute; empty string if None/NaN."""
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return ""
        return f'{val:.6f}'

    def sort_th(label, tbody_id, col_idx, paired, align="right", extra_style=""):
        paired_js = "true" if paired else "false"
        base = (f'font-size:8px;color:#475569;padding:2px 8px;cursor:pointer;'
                f'text-align:{align};user-select:none;{extra_style}')
        return (f'<th style="{base}" data-sort-col="{col_idx}" '
                f'onclick="sortTable(\'{tbody_id}\',{col_idx},{paired_js})">'
                f'{label}<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>')

    # ── D-1 lookup tables (keyed by (rf, PRODUCT, PRODUCT_CLASS)) ────────────
    d1_prod_expo = {}
    d1_prod_var  = {}
    d1_rf_var    = {}
    d1_rf_expo   = {}
    if df_expo_d1 is not None and not df_expo_d1.empty:
        for _, r in (df_expo_d1.groupby(["rf","PRODUCT","PRODUCT_CLASS"])
                                .agg(pct_nav=("pct_nav","sum")).reset_index()).iterrows():
            d1_prod_expo[(r["rf"], r["PRODUCT"], r["PRODUCT_CLASS"])] = r["pct_nav"]
        for _, r in (df_expo_d1.groupby("rf").agg(pct_nav=("pct_nav","sum")).reset_index()).iterrows():
            d1_rf_expo[r["rf"]] = r["pct_nav"]
    if df_var_d1 is not None and not df_var_d1.empty:
        for _, r in (df_var_d1.groupby(["rf","PRODUCT","PRODUCT_CLASS"])
                               .agg(var_pct=("var_pct","sum")).reset_index()).iterrows():
            d1_prod_var[(r["rf"], r["PRODUCT"], r["PRODUCT_CLASS"])] = r["var_pct"]
        for _, r in (df_var_d1.groupby("rf").agg(var_pct=("var_pct","sum")).reset_index()).iterrows():
            d1_rf_var[r["rf"]] = r["var_pct"]

    # ── DIA lookup: (livro, product) → dia_bps ───────────────────────────────
    dia_lookup = {}
    if df_pnl_prod is not None and not df_pnl_prod.empty:
        for _, r in df_pnl_prod.iterrows():
            dia_lookup[(r["LIVRO"], r["PRODUCT"])] = float(r["dia_bps"])

    # ── Summary: one row per RF ───────────────────────────────────────────────
    rf_expo = (df_expo.groupby("rf")
                      .agg(pct_nav=("pct_nav","sum"), dur_pct=("dur_pct","sum"), delta=("delta","sum"))
                      .reset_index())
    rf_var  = df_var.groupby("rf").agg(var_pct=("var_pct","sum")).reset_index()
    summary = rf_expo.merge(rf_var, on="rf", how="left")
    summary["_ord"] = summary["rf"].apply(lambda x: _RF_ORDER.index(x) if x in _RF_ORDER else 99)
    summary = summary.sort_values("_ord")

    # ── Product drill-down ───────────────────────────────────────────────────
    prod_expo = (df_expo.groupby(["rf","PRODUCT","PRODUCT_CLASS"])
                        .agg(pct_nav=("pct_nav","sum"), delta=("delta","sum"),
                             sigma=("sigma","mean"))
                        .reset_index()
                        .sort_values("pct_nav"))
    prod_var  = df_var.groupby(["rf","PRODUCT","PRODUCT_CLASS"]).agg(var_pct=("var_pct","sum")).reset_index()
    prod      = prod_expo.merge(prod_var, on=["rf","PRODUCT","PRODUCT_CLASS"], how="left")

    # ── PM × product breakdown for grouped PM VaR view ───────────────────────
    pm_prod = (df_expo.groupby(["rf","pm","PRODUCT","PRODUCT_CLASS"])
                      .agg(pct_nav=("pct_nav","sum"), delta=("delta","sum"),
                           sigma=("sigma","mean"))
                      .reset_index())
    # PM-level totals (for header rows)
    pm_tot = pm_prod.groupby(["rf","pm"]).agg(
        pct_nav_pm=("pct_nav","sum"), delta_pm=("delta","sum")).reset_index()
    # VaR attribution: delta-proportional from RF-level VaR
    rf_delta_tot = df_expo.groupby("rf").agg(rf_delta=("delta","sum")).reset_index()
    pm_tot = pm_tot.merge(rf_delta_tot, on="rf").merge(rf_var[["rf","var_pct"]], on="rf", how="left")
    pm_tot["pm_var_pct"] = np.where(
        pm_tot["rf_delta"] != 0,
        pm_tot["delta_pm"] / pm_tot["rf_delta"] * pm_tot["var_pct"],
        0.0
    )
    # Product-level VaR: same proportional attribution (product delta / rf delta)
    pm_prod = pm_prod.merge(rf_delta_tot, on="rf").merge(rf_var[["rf","var_pct"]], on="rf", how="left")
    pm_prod["prod_var_pct"] = np.where(
        pm_prod["rf_delta"] != 0,
        pm_prod["delta"] / pm_prod["rf_delta"] * pm_prod["var_pct"],
        0.0
    )

    rows = ""
    for _, r in summary.iterrows():
        rf    = r["rf"]
        rf_id = rf.replace("-","_").replace(" ","_")
        color = _RF_COLOR.get(rf, "#94a3b8")
        var_pct_raw = float(r["var_pct"]) if pd.notna(r.get("var_pct")) else None
        var_s  = f'{var_pct_raw:.1f}' if var_pct_raw is not None else "—"
        var_c  = "#f87171" if var_pct_raw is not None and var_pct_raw > 0 else "#94a3b8"

        # Summary bar
        is_rate = rf.startswith("RF-")
        if is_rate and pd.notna(r["dur_pct"]) and r["dur_pct"] != 0:
            bar = dual_bar(r["pct_nav"], r["dur_pct"])
            bar_w = "width:170px"
        else:
            bar = mini_bar(r["pct_nav"], color=color)
            bar_w = "width:120px"

        # Δ for summary row
        d1e = d1_rf_expo.get(rf)
        d1v = d1_rf_var.get(rf)
        dexp_raw = (r["pct_nav"] - d1e) if d1e is not None else None
        dvar_raw = (var_pct_raw - d1v) if (d1v is not None and var_pct_raw is not None) else None
        dexp_s, dexp_c = delta_str(dexp_raw)
        dvar_s, dvar_c = delta_str(dvar_raw)

        # ── PRODUTO drill rows ────────────────────────────────────────────────
        prod_rows = ""
        for _, p in prod[prod["rf"] == rf].iterrows():
            key = (rf, p["PRODUCT"], p["PRODUCT_CLASS"])
            vv  = float(p["var_pct"]) if pd.notna(p.get("var_pct")) else None
            var_s2 = f'{vv:.2f}%' if vv is not None else ""
            var_c2 = "#f87171" if vv and vv > 0 else "#64748b"
            pv_c   = "#f87171" if p["pct_nav"] < 0 else "#4ade80"

            de_raw = (p["pct_nav"] - d1_prod_expo[key]) if key in d1_prod_expo else None
            dv_raw2 = (vv - d1_prod_var[key]) if (vv is not None and key in d1_prod_var) else None
            de_s, de_c = delta_str(de_raw)
            dv_s2, dv_c2 = delta_str(dv_raw2)

            sig_raw = p["sigma"] if pd.notna(p.get("sigma")) else None
            sig_s = f'{sig_raw:.1f}' if sig_raw is not None else "—"
            prod_rows += (
                f'<tr style="border-top:1px solid #0f172a">'
                f'<td style="padding:2px 20px;font-size:10px;color:#94a3b8" data-val="{p["PRODUCT"]}">'
                f'  {p["PRODUCT"]} <span style="color:#334155;font-size:9px">{p["PRODUCT_CLASS"]}</span>'
                f'</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{pv_c};text-align:right" data-val="{p["pct_nav"]:.6f}">{p["pct_nav"]:+.1f}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:#94a3b8;text-align:right" data-val="{dv(sig_raw)}">{sig_s}</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{de_c};text-align:right" data-val="{dv(de_raw)}">{de_s}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{var_c2};text-align:right" data-val="{dv(vv)}">{var_s2}</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{dv_c2};text-align:right" data-val="{dv(dv_raw2)}">{dv_s2}%</td>'
                f'</tr>'
            )

        prod_tbody_id = f"tbody-prod-{rf_id}"
        prod_table = (
            f'<table style="width:100%;border-collapse:collapse;background:#080d14">'
            f'<thead><tr>'
            f'{sort_th("Produto", prod_tbody_id, 0, False, align="left")}'
            f'{sort_th("Expo%",   prod_tbody_id, 1, False)}'
            f'{sort_th("σ (bps)", prod_tbody_id, 2, False)}'
            f'{sort_th("ΔExpo",   prod_tbody_id, 3, False)}'
            f'{sort_th("VaR(bps)",    prod_tbody_id, 4, False)}'
            f'{sort_th("ΔVaR",    prod_tbody_id, 5, False)}'
            f'</tr></thead><tbody id="{prod_tbody_id}">{prod_rows}</tbody></table>'
        )

        rows += f"""
        <tr class="metric-row" style="cursor:pointer" onclick="toggleDrillMacro('{rf_id}')">
          <td style="font-size:12px;font-weight:bold;color:{color};padding:5px 12px;width:90px" data-val="{rf}">▶ {rf}</td>
          <td style="font-size:13px;font-family:monospace;font-weight:bold;color:{color};text-align:right;width:58px" data-val="{r["pct_nav"]:.6f}">{r["pct_nav"]:+.1f}%</td>
          <td style="font-size:10px;font-family:monospace;color:{dexp_c};text-align:right;width:48px" data-val="{dv(dexp_raw)}">{dexp_s}%</td>
          <td style="font-size:12px;font-family:monospace;color:{var_c};text-align:right;width:58px" data-val="{dv(var_pct_raw)}">{var_s}</td>
          <td style="font-size:10px;font-family:monospace;color:{dvar_c};text-align:right;width:48px" data-val="{dv(dvar_raw)}">{dvar_s}</td>
        </tr>
        <tr id="drill-{rf_id}" style="display:none">
          <td colspan="5" style="padding:0">{prod_table}</td>
        </tr>"""

    # ── D-1 PM totals (expo + VaR attribution) ───────────────────────────────
    d1_pm_expo_tot = {}
    d1_pm_var_tot  = {}
    d1_pm_prod_var = {}   # keyed by (pm, rf, PRODUCT, PRODUCT_CLASS)
    if df_expo_d1 is not None and not df_expo_d1.empty:
        _pm_d1 = df_expo_d1.groupby("pm").agg(pct_nav=("pct_nav","sum")).reset_index()
        d1_pm_expo_tot = dict(zip(_pm_d1["pm"], _pm_d1["pct_nav"]))
        if df_var_d1 is not None and not df_var_d1.empty:
            _rfd_d1   = df_expo_d1.groupby("rf").agg(rf_delta=("delta","sum")).reset_index()
            _pmrf_d1  = (df_expo_d1.groupby(["rf","pm","PRODUCT","PRODUCT_CLASS"])
                                   .agg(delta=("delta","sum")).reset_index())
            _rfvar_d1 = df_var_d1.groupby("rf").agg(var_pct=("var_pct","sum")).reset_index()
            _pmrf_d1  = _pmrf_d1.merge(_rfd_d1, on="rf").merge(_rfvar_d1, on="rf", how="left")
            _pmrf_d1["pv"] = np.where(_pmrf_d1["rf_delta"] != 0,
                                      _pmrf_d1["delta"] / _pmrf_d1["rf_delta"] * _pmrf_d1["var_pct"], 0.0)
            # PM total VaR D-1
            _pm_var_d1 = _pmrf_d1.groupby("pm").agg(pv=("pv","sum")).reset_index()
            d1_pm_var_tot = dict(zip(_pm_var_d1["pm"], _pm_var_d1["pv"]))
            # product-level VaR D-1
            for _, r in _pmrf_d1.iterrows():
                d1_pm_prod_var[(r["pm"], r["rf"], r["PRODUCT"], r["PRODUCT_CLASS"])] = r["pv"]

    # ── PM total VaR today (sum of prod_var_pct across all RF) ───────────────
    pm_var_today = pm_prod.groupby("pm").agg(var_tot=("prod_var_pct","sum")).reset_index()
    pm_var_today = dict(zip(pm_var_today["pm"], pm_var_today["var_tot"]))

    # ── Global PM VaR table — expandable, same structure as POSIÇÕES ─────────
    PM_ORDER_LIST = ["CI", "LF", "JD", "RJ", "QM"]
    pm_summary_rows = ""
    for pm_name in PM_ORDER_LIST:
        prod_sub = pm_prod[pm_prod["pm"] == pm_name].sort_values("pct_nav")
        if prod_sub.empty:
            continue
        pm_pct   = prod_sub["pct_nav"].sum()
        pm_delta = prod_sub["delta"].sum()
        pm_var   = pm_var_today.get(pm_name, 0.0)
        pm_color = "#60a5fa"
        vc    = "#f87171" if pm_var    > 0 else "#94a3b8"
        # weighted-average σ for PM (weight = |pct_nav|)
        _vs = prod_sub.dropna(subset=["sigma"])
        _w  = _vs["pct_nav"].abs()
        pm_sigma = (_w * _vs["sigma"]).sum() / _w.sum() if _w.sum() > 0 else None
        sig_s_pm = f'{pm_sigma:.1f}' if pm_sigma is not None else "—"

        # Δ PM
        d1e = d1_pm_expo_tot.get(pm_name)
        d1v = d1_pm_var_tot.get(pm_name)
        dexp_raw_pm = (pm_pct - d1e) if d1e is not None else None
        dvar_raw_pm = (pm_var - d1v) if d1v is not None else None
        dexp_s, dexp_c = delta_str(dexp_raw_pm)
        dvar_s, dvar_c = delta_str(dvar_raw_pm)
        _marg_val = (pm_margem or {}).get(pm_name)
        if _marg_val is None:
            _marg_s, _marg_c = "—", "#475569"
        else:
            _marg_s = f'{_marg_val:+.0f}'
            _marg_c = "#f87171" if _marg_val <= 0 else "#facc15" if _marg_val < 20 else "#4ade80"

        # DIA: sum of PnL DIA (bps) for all products of this PM
        livro_key = _PM_LIVRO.get(pm_name, pm_name)
        pm_dia = sum(
            dia_lookup.get((livro_key, p["PRODUCT"]), 0.0)
            for _, p in prod_sub.iterrows()
        )
        pm_dia_s = f'{pm_dia:+.0f}' if pm_dia != 0 else "—"
        pm_dia_c = "#f87171" if pm_dia < 0 else "#4ade80" if pm_dia > 0 else "#475569"

        # ── instrument drill rows ─────────────────────────────────────────────
        inst_tbody_id = f"tbody-inst-{pm_name}"
        inst_rows = ""
        for _, p in prod_sub.iterrows():
            ppv_c = "#f87171" if p["pct_nav"] < 0 else "#4ade80"
            pvc2  = "#f87171" if p["prod_var_pct"] > 0 else "#64748b"

            key_expo = (p["rf"], p["PRODUCT"], p["PRODUCT_CLASS"])
            key_var  = (pm_name, p["rf"], p["PRODUCT"], p["PRODUCT_CLASS"])
            de_raw_i = (p["pct_nav"] - d1_prod_expo[key_expo]) if key_expo in d1_prod_expo else None
            dv_raw_i = (p["prod_var_pct"] - d1_pm_prod_var[key_var]) if key_var in d1_pm_prod_var else None
            de_s, de_c = delta_str(de_raw_i)
            dv_s, dv_c = delta_str(dv_raw_i)

            isig_raw = p["sigma"] if pd.notna(p.get("sigma")) else None
            isig_s = f'{isig_raw:.1f}' if isig_raw is not None else "—"
            livro_key = _PM_LIVRO.get(pm_name, pm_name)
            dia_raw = dia_lookup.get((livro_key, p["PRODUCT"]))
            dia_s = f'{dia_raw:+.1f}' if dia_raw is not None else "—"
            dia_c = "#f87171" if (dia_raw is not None and dia_raw < 0) else "#4ade80" if (dia_raw is not None and dia_raw > 0) else "#475569"
            inst_rows += (
                f'<tr style="border-top:1px solid #0f172a;background:#0a0f18">'
                f'<td style="padding:2px 24px;font-size:10px;color:#94a3b8" colspan="2" data-val="{p["PRODUCT"]}">'
                f'  {p["PRODUCT"]} <span style="color:#334155;font-size:9px">{p["PRODUCT_CLASS"]}</span></td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{ppv_c};text-align:right" data-val="{p["pct_nav"]:.6f}">{p["pct_nav"]:+.1f}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:#94a3b8;text-align:right" data-val="{dv(isig_raw)}">{isig_s}</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{de_c};text-align:right" data-val="{dv(de_raw_i)}">{de_s}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{pvc2};text-align:right" data-val="{p["prod_var_pct"]:.6f}">{p["prod_var_pct"]:.1f}</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{dv_c};text-align:right" data-val="{dv(dv_raw_i)}">{dv_s}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{dia_c};text-align:right" data-val="{dv(dia_raw)}">{dia_s}</td>'
                f'</tr>'
            )

        drill_id = f"pmv-{pm_name}"
        pm_summary_rows += f"""
        <tr class="metric-row" style="cursor:pointer" onclick="toggleDrillPM('{drill_id}')">
          <td style="font-size:12px;font-weight:bold;color:{pm_color};padding:5px 12px;width:50px" colspan="2" data-val="{pm_name}">▶ {pm_name}</td>
          <td style="font-size:13px;font-family:monospace;font-weight:bold;color:#94a3b8;text-align:right;width:58px" data-val="{dv(pm_sigma)}">{sig_s_pm}</td>
          <td style="font-size:10px;font-family:monospace;color:{dexp_c};text-align:right;width:48px" data-val="{dv(dexp_raw_pm)}">{dexp_s}%</td>
          <td style="font-size:12px;font-family:monospace;color:{vc};text-align:right;width:58px" data-val="{pm_var:.6f}">{pm_var:.1f}</td>
          <td style="font-size:10px;font-family:monospace;color:{dvar_c};text-align:right;width:48px" data-val="{dv(dvar_raw_pm)}">{dvar_s}%</td>
          <td style="font-size:12px;font-family:monospace;font-weight:bold;color:{_marg_c};text-align:right;width:60px" data-val="{dv(_marg_val)}">{_marg_s}</td>
          <td style="font-size:12px;font-family:monospace;color:{pm_dia_c};text-align:right;width:58px" data-val="{pm_dia:.2f}">{pm_dia_s}</td>
        </tr>
        <tr id="{drill_id}" style="display:none">
          <td colspan="8" style="padding:0">
            <table style="width:100%;border-collapse:collapse">
              <thead><tr>
                <th colspan="2" style="font-size:8px;color:#475569;padding:2px 8px;text-align:left;cursor:pointer;user-select:none"
                    data-sort-col="0" onclick="sortTable('{inst_tbody_id}',0,false)">Instrumento<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
                {sort_th("Expo%", inst_tbody_id, 1, False)}
                {sort_th("σ (bps)", inst_tbody_id, 2, False)}
                {sort_th("ΔExpo", inst_tbody_id, 3, False)}
                {sort_th("VaR(bps)",  inst_tbody_id, 4, False)}
                {sort_th("ΔVaR",  inst_tbody_id, 5, False)}
                {sort_th("DIA",   inst_tbody_id, 6, False)}
              </tr></thead>
              <tbody id="{inst_tbody_id}">{inst_rows}</tbody>
            </table>
          </td>
        </tr>"""

    global_pm_table = f"""
    <table style="width:100%;border-collapse:collapse;margin-bottom:6px">
      <thead><tr>
        <th colspan="2" style="font-size:8px;color:#475569;padding:2px 8px;text-align:left;cursor:pointer;user-select:none"
            data-sort-col="0" onclick="sortTable('tbody-pmv',0,true)">PM<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
        {sort_th("σ (bps)", "tbody-pmv", 1, True)}
        {sort_th("ΔExpo",  "tbody-pmv", 2, True)}
        {sort_th("VaR(bps)",   "tbody-pmv", 3, True)}
        {sort_th("ΔVaR",   "tbody-pmv", 4, True)}
        {sort_th("Margem", "tbody-pmv", 5, True)}
        {sort_th("DIA",    "tbody-pmv", 6, True)}
      </tr></thead>
      <tbody id="tbody-pmv">{pm_summary_rows}</tbody>
    </table>"""

    toggle_btns = (
        '<div style="display:inline-flex;gap:2px;margin-left:12px;vertical-align:middle">'
        '<button id="mbtn-pos" onclick="setMacroView(\'pos\')"'
        ' style="background:#1e3a5f;color:#60a5fa;border:none;border-radius:3px;padding:2px 8px;font-size:9px;cursor:pointer;letter-spacing:1px">POSIÇÕES</button>'
        '<button id="mbtn-pmv" onclick="setMacroView(\'pmv\')"'
        ' style="background:transparent;color:#475569;border:none;border-radius:3px;padding:2px 8px;font-size:9px;cursor:pointer;letter-spacing:1px">PM VaR</button>'
        '</div>'
    )

    js = """<script>
var _macroSort = {};
function _getVal(row, colIdx) {
    var td = row.cells ? row.cells[colIdx] : null;
    if (!td) return {n: NaN, s: ''};
    var v = td.getAttribute('data-val');
    if (v === null || v === '') return {n: NaN, s: ''};
    var f = parseFloat(v);
    return {n: f, s: v};
}
function sortTable(tbodyId, colIdx, paired) {
    var key = tbodyId + ':' + colIdx;
    var asc = _macroSort[key] !== true;
    _macroSort[key] = asc;
    var tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    var rows = Array.from(tbody.rows);
    function cmp(a, b) {
        var va = _getVal(a, colIdx), vb = _getVal(b, colIdx);
        var na = va.n, nb = vb.n;
        if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
        if (!isNaN(na)) return -1;
        if (!isNaN(nb)) return  1;
        return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    }
    if (paired) {
        var pairs = [];
        for (var i = 0; i + 1 < rows.length; i += 2) pairs.push([rows[i], rows[i+1]]);
        pairs.sort(function(a,b){ return cmp(a[0], b[0]); });
        pairs.forEach(function(p){ tbody.appendChild(p[0]); tbody.appendChild(p[1]); });
    } else {
        rows.sort(cmp);
        rows.forEach(function(r){ tbody.appendChild(r); });
    }
    var table = tbody.closest('table');
    if (!table) return;
    var thead = table.querySelector('thead');
    if (!thead) return;
    thead.querySelectorAll('.sort-ind').forEach(function(el){ el.textContent=' ▲▼'; el.style.opacity='0.3'; });
    var act = thead.querySelector('[data-sort-col="' + colIdx + '"] .sort-ind');
    if (act) { act.textContent = asc ? ' ▲' : ' ▼'; act.style.opacity = '1'; }
}
function setMacroView(v) {
    ['pos','pmv'].forEach(function(b) {
        var btn = document.getElementById('mbtn-' + b);
        btn.style.background = b===v ? '#1e3a5f' : 'transparent';
        btn.style.color = b===v ? '#60a5fa' : '#475569';
    });
    document.getElementById('macro-rf-view').style.display = v==='pos' ? '' : 'none';
    document.getElementById('macro-pm-view').style.display = v==='pmv' ? '' : 'none';
}
function toggleDrillMacro(id) {
    var row = document.getElementById('drill-' + id);
    if (row) row.style.display = row.style.display === 'none' ? '' : 'none';
}
function toggleDrillPM(id) {
    var row = document.getElementById(id);
    if (row) row.style.display = row.style.display === 'none' ? '' : 'none';
}
</script>"""

    return f"""
    {js}
    <div style="margin-top:28px">
      <div style="font-size:11px; color:#64748b; letter-spacing:2px; text-transform:uppercase;
                  padding-bottom:8px; border-bottom:1px solid #2d2d3d; margin-bottom:4px;
                  display:flex; align-items:center; justify-content:space-between">
        <span>Exposição MACRO
          <span style="font-size:9px;letter-spacing:0;text-transform:none;color:#475569">(clique no fator)</span>
        </span>
        {toggle_btns}
      </div>
      <div id="macro-rf-view">
        <table style="width:100%; border-collapse:collapse; margin-bottom:6px">
          <thead>
            <tr>
              <th style="font-size:9px;color:#475569;padding:4px 12px;text-align:left;letter-spacing:1px;text-transform:uppercase;cursor:pointer;user-select:none"
                  data-sort-col="0" onclick="sortTable('tbody-rf',0,true)">Fator<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
              <th style="font-size:9px;color:#475569;padding:4px 8px;text-align:right;letter-spacing:1px;text-transform:uppercase;cursor:pointer;user-select:none"
                  data-sort-col="1" onclick="sortTable('tbody-rf',1,true)">% NAV<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
              <th style="font-size:9px;color:#475569;padding:4px 8px;text-align:right;letter-spacing:1px;text-transform:uppercase;cursor:pointer;user-select:none"
                  data-sort-col="2" onclick="sortTable('tbody-rf',2,true)">Δ Expo<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
              <th style="font-size:9px;color:#475569;padding:4px 8px;letter-spacing:1px;text-transform:uppercase">Barra</th>
              <th style="font-size:9px;color:#475569;padding:4px 8px;text-align:right;letter-spacing:1px;text-transform:uppercase;cursor:pointer;user-select:none"
                  data-sort-col="3" onclick="sortTable('tbody-rf',3,true)">VaR (bps)<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
              <th style="font-size:9px;color:#475569;padding:4px 8px;text-align:right;letter-spacing:1px;text-transform:uppercase;cursor:pointer;user-select:none"
                  data-sort-col="4" onclick="sortTable('tbody-rf',4,true)">Δ VaR<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
            </tr>
          </thead>
          <tbody id="tbody-rf">{rows}</tbody>
        </table>
      </div>
      <div id="macro-pm-view" style="display:none">{global_pm_table}</div>
    </div>"""

def build_single_names_section(fund_short: str, sub_label: str,
                                df: pd.DataFrame, nav: float,
                                index_legs: dict) -> str:
    """
    Inline single-name L/S section for a given fund. Full detail (stats header + Top 8 L/S).
    `index_legs` is a dict of {leg_name: delta_in_brl} for each exploded index leg
    (WIN, BOVA11, SMAL11 …). Zero legs are hidden from the header.
    """
    if df is None or df.empty:
        return ""

    longs  = df[df["net"] > 0].sort_values("net", ascending=False).head(8)
    shorts = df[df["net"] < 0].sort_values("net", ascending=True).head(8)
    gross_l = df.loc[df["net"] > 0, "net"].sum()
    gross_s = df.loc[df["net"] < 0, "net"].sum()
    gross   = abs(gross_l) + abs(gross_s)
    net     = gross_l + gross_s
    n_total = len(df)
    n_long  = int((df["net"] > 0).sum())
    n_short = int((df["net"] < 0).sum())

    def fmt_row(row):
        net_pct    = row["net"]      * 100 / nav
        direct_pct = row["direct"]   * 100 / nav
        idx_pct    = row["from_idx"] * 100 / nav
        color      = "var(--up)" if row["net"] > 0 else "var(--down)"
        dir_disp   = f"{direct_pct:+.2f}%" if abs(row["direct"])   > 1 else "—"
        idx_disp   = f"{idx_pct:+.2f}%"    if abs(row["from_idx"]) > 1 else "—"
        return (
            f'<tr>'
            f'<td class="t-name">{row["ticker"]}</td>'
            f'<td class="t-num mono">{dir_disp}</td>'
            f'<td class="t-num mono">{idx_disp}</td>'
            f'<td class="t-num mono" style="color:{color}; font-weight:700">{net_pct:+.2f}%</td>'
            f'</tr>'
        )

    def side_table(title, color_var, rows_df, count):
        if rows_df.empty:
            body = '<tr><td colspan="4" class="t-empty">(sem posições)</td></tr>'
        else:
            body = "".join(fmt_row(r) for _, r in rows_df.iterrows())
        return f"""
        <div class="sn-side">
          <div class="sn-side-head" style="color:{color_var}">
            {title} <span class="sn-side-count">· {count} names</span>
          </div>
          <table class="sn-table">
            <thead><tr>
              <th style="text-align:left">Ticker</th>
              <th style="text-align:right">Direct</th>
              <th style="text-align:right">From Idx</th>
              <th style="text-align:right">Net %NAV</th>
            </tr></thead>
            <tbody>{body}</tbody>
          </table>
        </div>"""

    # Breakdown of the index-explosion leg — only show non-zero sources
    leg_bits = []
    for leg_name, delta in (index_legs or {}).items():
        if nav and abs(delta) > 1:
            leg_bits.append(f"{leg_name} {delta*100/nav:+.2f}%")
    legs_html = (" &nbsp;|&nbsp; " + " · ".join(leg_bits)) if leg_bits else ""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Single-Name Exposure</span>
        <span class="card-sub">— {fund_short} · {sub_label}</span>
      </div>
      <div class="sn-inline-stats mono">
        Gross L <span style="color:var(--up)">{gross_l*100/nav:+.2f}%</span> &nbsp;|&nbsp;
        Gross S <span style="color:var(--down)">{gross_s*100/nav:+.2f}%</span> &nbsp;|&nbsp;
        Gross <span style="color:var(--text)">{gross*100/nav:.2f}%</span> &nbsp;|&nbsp;
        Net <span style="color:var(--text)">{net*100/nav:+.2f}%</span> &nbsp;|&nbsp;
        {n_total} names ({n_long}L / {n_short}S){legs_html}
      </div>
      <div class="sn-sides">
        {side_table("TOP 8 LONG",  "var(--up)",   longs,  n_long)}
        {side_table("TOP 8 SHORT", "var(--down)", shorts, n_short)}
      </div>
    </section>"""

def _dist_entries(fund_short):
    return [p for p in _DIST_PORTFOLIOS if p[4] == fund_short]

def _kind_tag(kind):
    label = {"fund":"FUND", "livro":"PM", "rf":"FATOR"}.get(kind, "")
    color = {"FUND":"var(--accent-2)", "PM":"var(--text)", "FATOR":"var(--muted)"}.get(label, "var(--muted)")
    return label, color

def build_distribution_card(fund_short: str, dist_map_now: dict, dist_map_prev: dict, actuals: dict) -> str:
    """Single card with toggle between Backward (D-1 carteira + realized DIA) and Forward (D carteira profile)."""
    bw_table = _build_backward_table(fund_short, dist_map_prev, actuals)
    fw_table = _build_forward_table(fund_short, dist_map_now)
    if not bw_table and not fw_table:
        return ""
    dck_id = f"dist-{fund_short.lower()}"
    return f"""
    <section class="card" id="{dck_id}">
      <div class="card-head" style="display:flex;align-items:center;justify-content:space-between">
        <div>
          <span class="card-title">Distribuição 252d</span>
          <span class="card-sub">— {fund_short} · bps de NAV</span>
        </div>
        <div class="dist-toggle">
          <button class="dist-btn active" data-mode="backward" onclick="setDistMode('{dck_id}','backward')">Backward</button>
          <button class="dist-btn"        data-mode="forward"  onclick="setDistMode('{dck_id}','forward')">Forward</button>
        </div>
      </div>
      <div class="dist-view active" data-mode="backward">{bw_table or '<div class="empty-view">Sem dados backward (D-1 sem simulação).</div>'}</div>
      <div class="dist-view"        data-mode="forward" style="display:none">{fw_table or '<div class="empty-view">Sem dados forward (D sem simulação).</div>'}</div>
    </section>"""

def _build_backward_table(fund_short: str, dist_map_prev: dict, actuals: dict) -> str:
    """Backward-looking: yesterday's carteira × last 252d, with today's realized DIA overlayed.
       Answers: 'where did today's move land in the historical distribution of D-1 carteira?'"""
    if not dist_map_prev:
        return ""
    rows = ""
    for portfolio_name, label, kind, key, fs in _dist_entries(fund_short):
        w = dist_map_prev.get(portfolio_name)
        if w is None or len(w) < 30:
            continue
        actual = actuals.get(f"{kind}:{key}")
        stats = compute_distribution_stats(w, actual)
        if stats is None or abs(stats["max"] - stats["min"]) < 1e-6:
            continue

        if "actual" not in stats:
            continue  # nothing to compare; skip backward row

        a   = stats["actual"]; pct = stats["percentile"]; nv = stats["nvols"]
        col = "var(--up)" if a > 0 else "var(--down)" if a < 0 else "var(--muted)"
        pct_col = "var(--down)" if pct < 10 else "var(--up)" if pct > 90 else "var(--text)"
        nv_s    = f"{nv:+.2f}" if nv is not None else "—"
        tag, tag_c = _kind_tag(kind)

        rows += f"""
        <tr class="metric-row">
          <td class="dist-tag" style="color:{tag_c}">{tag}</td>
          <td class="dist-name">{label}</td>
          <td class="dist-num mono" style="color:{col}; font-weight:700">{a:+.0f}</td>
          <td class="dist-num mono" style="color:{pct_col}; font-weight:600">{pct:.0f}°</td>
          <td class="dist-num mono">{nv_s}</td>
        </tr>"""

    if not rows:
        return ""
    return f"""
      <table class="metric-table dist-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left;width:54px">Tipo</th>
            <th style="text-align:left">Nome</th>
            <th style="text-align:right;width:70px">DIA</th>
            <th style="text-align:right;width:80px">Percentil</th>
            <th style="text-align:right;width:60px">#σ</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        <b>DIA</b> = PnL realizado hoje (bps NAV) · <b>Percentil</b> = posição ordinal de DIA na distribuição hipotética (5° e 95° são caudas) · <b>#σ</b> = DIA / σ histórico
      </div>"""

def _build_forward_table(fund_short: str, dist_map: dict) -> str:
    """Forward-looking: today's carteira × last 252d. Describes expected P&L profile."""
    if not dist_map:
        return ""
    rows = ""
    for portfolio_name, label, kind, key, fs in _dist_entries(fund_short):
        w = dist_map.get(portfolio_name)
        if w is None or len(w) < 30:
            continue
        stats = compute_distribution_stats(w, None)
        if stats is None or abs(stats["max"] - stats["min"]) < 1e-6:
            continue
        tag, tag_c = _kind_tag(kind)
        rows += f"""
        <tr class="metric-row">
          <td class="dist-tag" style="color:{tag_c}">{tag}</td>
          <td class="dist-name">{label}</td>
          <td class="dist-num mono" style="color:var(--down)">{stats['min']:.0f}</td>
          <td class="dist-num mono" style="color:var(--warn)">{stats['var95']:.0f}</td>
          <td class="dist-num mono" style="color:var(--muted)">{stats['mean']:+.1f}</td>
          <td class="dist-num mono" style="color:var(--accent-2)">{stats['var_p95']:+.0f}</td>
          <td class="dist-num mono" style="color:var(--up)">{stats['max']:+.0f}</td>
          <td class="dist-num mono" style="color:var(--muted)">{stats['sd']:.1f}</td>
        </tr>"""

    if not rows:
        return ""
    return f"""
      <table class="metric-table dist-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left;width:54px">Tipo</th>
            <th style="text-align:left">Nome</th>
            <th style="text-align:right;width:60px">Min</th>
            <th style="text-align:right;width:70px">VaR 95</th>
            <th style="text-align:right;width:60px">Média</th>
            <th style="text-align:right;width:70px">VaR +95</th>
            <th style="text-align:right;width:60px">Max</th>
            <th style="text-align:right;width:55px">σ</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        <b>Min/Max</b> = extremos dos 252 PnLs hipotéticos · <b>VaR 95</b> = 5° percentil (cauda de perda) · <b>VaR +95</b> = 95° percentil (cauda de ganho) · <b>Média/σ</b> = expectativa diária e vol
      </div>"""

def build_stop_section(stop_history: dict[str, pd.DataFrame], df_pnl_today: pd.DataFrame) -> str:
    """Build the stop monitor HTML section."""
    PM_ORDER  = ["CI", "LF", "JD", "RJ"]
    PM_LABELS = {"CI": "CI (Comitê)", "LF": "Luiz Felipe", "JD": "Joca Dib",
                 "RJ": "Rodrigo Jafet"}
    LIVRO_MAP = {"CI": "CI", "LF": "Macro_LF", "JD": "Macro_JD",
                 "RJ": "Macro_RJ"}

    rows = ""
    for pm in PM_ORDER:
        if pm not in stop_history:
            continue
        hist = stop_history[pm]
        if hist.empty:
            continue

        # Current month row
        cur_mes   = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
        cur_row   = hist[hist["mes"] == cur_mes]
        budget    = cur_row["budget_abs"].iloc[0] if not cur_row.empty else STOP_BASE
        soft_mark = cur_row["soft_mark"].iloc[0] if not cur_row.empty and "soft_mark" in cur_row.columns else None

        # PnL MTD from df_pnl_today
        livro = LIVRO_MAP[pm]
        pnl_mtd_row = df_pnl_today[df_pnl_today["LIVRO"] == livro]
        pnl_mtd = float(pnl_mtd_row["mes_bps"].iloc[0]) if not pnl_mtd_row.empty else 0.0
        pnl_ytd = float(pnl_mtd_row["ytd_bps"].iloc[0]) if not pnl_mtd_row.empty else 0.0

        # Historical budget distribution
        bmin = hist["budget_abs"].min()
        bmax = hist["budget_abs"].max()
        bpct = (budget - bmin) / (bmax - bmin) * 100 if bmax != bmin else 50

        # Status
        CI_SOFT = 150.0
        if budget == 0:
            status_label = "⚡ GANCHO"
            status_color = "#fb923c"
        elif pm == "CI":
            if pnl_mtd <= -budget:
                status_label, status_color = "🔴 STOP", "#f87171"
            elif pnl_mtd <= -CI_SOFT:
                status_label, status_color = "🟡 SOFT", "#facc15"
            else:
                status_label, status_color = "🟢 OK", "#4ade80"
        else:
            consumed = abs(pnl_mtd) / budget if budget > 0 and pnl_mtd < 0 else 0
            if consumed >= 1.0:
                status_label, status_color = "🔴 STOP", "#f87171"
            elif consumed >= 0.7:
                status_label, status_color = "🟡 ATENÇÃO", "#facc15"
            else:
                status_label, status_color = "🟢 OK", "#4ade80"

        bar = stop_bar_svg(budget, pnl_mtd, bmax, soft_mark=soft_mark if pd.notna(soft_mark) else None)
        spark = make_sparkline(hist.set_index("mes")["budget_abs"], "#60a5fa", width=140)

        ytd_color = "#4ade80" if pnl_ytd >= 0 else "#f87171"

        # Semestral / anual: compact inline flags (not full bars)
        sem_used  = abs(pnl_ytd) if pnl_ytd < 0 else 0
        ano_used  = abs(pnl_ytd) if pnl_ytd < 0 else 0
        sem_pct   = sem_used / STOP_SEM * 100
        ano_pct   = ano_used / STOP_ANO * 100
        def cap_chip(label, pct, limit):
            color = "#f87171" if pct >= 100 else "#facc15" if pct >= 70 else "#334155"
            return f'<span style="color:{color};font-size:9px">{label} {pct:.0f}%</span>'
        sem_chip = cap_chip("SEM", sem_pct, STOP_SEM) if pm != "CI" and sem_pct > 0 else ""
        ano_chip = cap_chip("ANO", ano_pct, STOP_ANO) if pm != "CI" and ano_pct > 0 else ""

        # Margem number color based on consumption
        if pm == "CI":
            ci_consumed = abs(pnl_mtd) / CI_SOFT if pnl_mtd < 0 else 0
            margem_color = "#f87171" if ci_consumed >= 1.0 else "#facc15" if ci_consumed >= 0.7 else "#60a5fa"
        elif budget == 0:
            margem_color = "#fb923c"  # gancho
        else:
            _c = abs(pnl_mtd) / budget if pnl_mtd < 0 else 0
            margem_color = "#f87171" if _c >= 1.0 else "#facc15" if _c >= 0.7 else "#60a5fa"

        margem_val = budget + pnl_mtd
        margem_str = str(int(round(margem_val))) if margem_val > 0 else ("–" if budget == 0 else str(int(round(margem_val))))

        rows += f"""
        <tr class="metric-row">
          <td class="pm-name">{PM_LABELS[pm]}</td>
          <td class="pm-margem mono" style="color:{margem_color}">{margem_str}</td>
          <td class="bar-cell">{bar}</td>
          <td class="pm-hist mono">
            <span style="color:{ytd_color}">YTD {pnl_ytd:+.0f}bps</span>
            &nbsp;{sem_chip}&nbsp;{ano_chip}
          </td>
          <td class="pm-status" style="color:{status_color}">{status_label}</td>
          <td class="spark-cell"><img src="data:image/png;base64,{spark}" height="34"/></td>
        </tr>"""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risk Budget Monitor</span>
        <span class="card-sub">— MACRO · stop por PM</span>
      </div>
      <table class="metric-table stop-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left">PM</th>
            <th style="text-align:right">Margem (bps)</th>
            <th>Perf. MTD vs Stop</th>
            <th style="text-align:right">Histórico</th>
            <th>Status</th>
            <th>Budget trend</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        Barra: <span style="color:var(--accent-2)">azul</span> = budget disponível · <span style="color:var(--up)">verde</span> = ganho MTD · <span style="color:var(--down)">vermelho</span> = consumo · Budget (bps) = base 63 + carrego
      </div>
    </section>"""

# ── Alert commentary ─────────────────────────────────────────────────────────
ALERT_COMMENTS = {
    ("var",    "MACRO"):     "VaR MACRO no percentil elevado do ano — verificar concentração de posições e se houve aumento intencional de risco.",
    ("var",    "QUANT"):     "VaR QUANT próximo do máximo histórico recente — confirmar se expansão é consistente com o regime de mercado.",
    ("var",    "EVOLUTION"): "VaR EVOLUTION alto em termos históricos. Fundo multi-estratégia — revisar qual book está puxando (FRONTIER, CREDITO ou MACRO interno).",
    ("stress", "MACRO"):     "Stress MACRO acima do 80° percentil — cenário de stress testado está materializando-se? Revisar posição de maior contribuição.",
    ("stress", "QUANT"):     "Stress QUANT elevado no histórico. Verificar concentração em classes sistemáticas (RF, FX, RV) que dominam o cenário de stress.",
    ("stress", "EVOLUTION"): "Stress EVOLUTION alto. Atenção especial à parcela de crédito (cotas júnior) — marcação pode subestimar perda real no cenário de stress.",
}

def fetch_pa_leaves(date_str: str = DATA_STR) -> pd.DataFrame:
    """
    Leaf-level PA rows for MACRO / QUANT / EVOLUTION / GLOBAL.
    Grouped by (FUNDO, CLASSE, GRUPO, LIVRO, BOOK, PRODUCT) with DIA/MTD/YTD/12M in bps,
    plus `position_brl` — the leaf's current position (POSITION on `date_str`).
    Values are alpha (bps) vs. benchmark (q_models.REPORT_ALPHA_ATRIBUTION is alpha by design).
    """
    q = f"""
    SELECT
      "FUNDO", "CLASSE", "GRUPO", "LIVRO", "BOOK", "PRODUCT",
      SUM(CASE WHEN "DATE" = DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS dia_bps,
      SUM(CASE WHEN "DATE" >= DATE_TRUNC('month', DATE '{date_str}')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS mtd_bps,
      SUM(CASE WHEN "DATE" >= DATE_TRUNC('year', DATE '{date_str}')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS ytd_bps,
      SUM(CASE WHEN "DATE" >  (DATE '{date_str}' - INTERVAL '12 months')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS m12_bps,
      SUM(CASE WHEN "DATE" = DATE '{date_str}'
               THEN COALESCE("POSITION",0) ELSE 0 END) AS position_brl
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" IN ('MACRO','QUANT','EVOLUTION','GLOBAL','ALBATROZ')
      AND "DATE" >  (DATE '{date_str}' - INTERVAL '12 months')
      AND "DATE" <= DATE '{date_str}'
    GROUP BY "FUNDO","CLASSE","GRUPO","LIVRO","BOOK","PRODUCT"
    """
    df = read_sql(q)
    num_cols = ["dia_bps","mtd_bps","ytd_bps","m12_bps","position_brl"]
    df[num_cols] = df[num_cols].astype(float).fillna(0.0)
    # Dust filter: drop leaves with zero contribution across every horizon AND no position.
    # Keeps totals intact (dropped rows were 0 anyway) and trims ~20-30% of JSON payload.
    dust = (
        (df["dia_bps"].abs()     < 1e-6)
        & (df["mtd_bps"].abs()   < 1e-6)
        & (df["ytd_bps"].abs()   < 1e-6)
        & (df["m12_bps"].abs()   < 1e-6)
        & (df["position_brl"].abs() < 1e-6)
    )
    df = df[~dust].reset_index(drop=True)
    return df


_FUND_DESK_FOR_EXPO = {
    "MACRO":     "Galapagos Macro FIM",
    "QUANT":     "Galapagos Quantitativo FIM",
    "EVOLUTION": "Galapagos Evolution FIC FIM CP",
    "MACRO_Q":   "Galapagos Global Macro Q",
    "ALBATROZ":  "GALAPAGOS ALBATROZ FIRF LP",
}

# PRODUCT_CLASS → higher-level risk factor. None = non-directional (Cash, LFT) — excluded.
_PRODCLASS_TO_FACTOR = {
    "DI1Future": "Pré BZ", "NTN-F": "Pré BZ", "LTN": "Pré BZ",
    "NTN-B": "IPCA BZ", "DAP Future": "IPCA BZ", "DAPFuture": "IPCA BZ",
    "NTN-C": "IGP-M BZ", "DAC Future": "IGP-M BZ",
    "Equity": "Equity BR", "Equity Receipts": "Equity BR",
    "Equity Options": "Equity BR", "IBOVSPFuture": "Equity BR",
    "ETF BR": "Equity BR",
    "ADR": "ADR", "ADR Options": "ADR", "ADR Receipts": "ADR",
    "USTreasury": "Rates DM", "USTreasuryFuture": "Rates DM", "BondFuture": "Rates DM",
    "CommodityFuture": "Commodities", "CommodityOption": "Commodities",
    "Currencies Forward": "FX", "USDBRLFuture": "FX", "Currencies": "FX",
    "ExchangeFuture": "FX", "FXOption": "FX",
    # Non-directional (excluded)
    "LFT": None, "Cash": None, "Margin": None, "Provisions and Costs": None,
    "Overnight": None, "Compromissada": None, "ETF BR RF": None,
}


def fetch_fund_position_changes(short: str, date_str: str, d1_str: str) -> pd.DataFrame:
    """
    Factor-level %NAV exposure D-0 vs D-1 for a fund.
    Returns df with columns (factor, pct_d0, pct_d1, delta_pp) or None if no data.
    EVOLUTION uses SHARE_SOURCE look-through; others use own-desk rows.
    """
    desk = _FUND_DESK_FOR_EXPO.get(short)
    if not desk:
        return None

    nav = _latest_nav(desk, date_str)
    if nav is None:
        return None

    if short == "EVOLUTION":
        scope = f'"TRADING_DESK_SHARE_SOURCE" = \'{desk}\''
    else:
        scope = (f'"TRADING_DESK" = \'{desk}\' AND '
                 f'"TRADING_DESK_SHARE_SOURCE" = \'{desk}\'')

    df = read_sql(f"""
        SELECT "VAL_DATE" AS val_date, "PRODUCT_CLASS",
               SUM("DELTA") AS delta_brl
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE {scope}
          AND "VAL_DATE" IN (DATE '{date_str}', DATE '{d1_str}')
          AND "DELTA" <> 0
        GROUP BY "VAL_DATE", "PRODUCT_CLASS"
    """)
    if df.empty:
        return None

    df["factor"] = df["PRODUCT_CLASS"].map(lambda pc: _PRODCLASS_TO_FACTOR.get(pc))
    df = df[df["factor"].notna()]
    if df.empty:
        return None

    df["pct_nav"] = df["delta_brl"] * 100 / nav
    df["date_key"] = pd.to_datetime(df["val_date"]).dt.strftime("%Y-%m-%d")

    pivot = df.pivot_table(
        index="factor", columns="date_key", values="pct_nav",
        aggfunc="sum", fill_value=0.0,
    )
    if date_str not in pivot.columns: pivot[date_str] = 0.0
    if d1_str   not in pivot.columns: pivot[d1_str]   = 0.0
    pivot["pct_d0"] = pivot[date_str]
    pivot["pct_d1"] = pivot[d1_str]
    pivot["delta"] = pivot["pct_d0"] - pivot["pct_d1"]
    return pivot[["pct_d0", "pct_d1", "delta"]].reset_index()


def fetch_pa_daily_per_product(date_str: str = DATA_STR, lookback_days: int = 90) -> pd.DataFrame:
    """
    Daily alpha per (FUNDO, LIVRO, PRODUCT) for the last `lookback_days`.
    Used to compute per-product volatility and flag today's outliers.
    """
    q = f"""
    SELECT "FUNDO", "LIVRO", "PRODUCT", "DATE",
           SUM("DIA") * 10000              AS dia_bps,
           ABS(SUM(COALESCE("POSITION",0))) AS position_brl
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" IN ('MACRO','QUANT','EVOLUTION','GLOBAL','ALBATROZ')
      AND "DATE" >  (DATE '{date_str}' - INTERVAL '{lookback_days} days')
      AND "DATE" <= DATE '{date_str}'
    GROUP BY "FUNDO","LIVRO","PRODUCT","DATE"
    HAVING ABS(SUM("DIA")) > 1e-7
    """
    df = read_sql(q)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.tz_localize(None)
    return df


def compute_pa_outliers(df_daily: pd.DataFrame, date_str: str,
                         z_min: float = 2.0, bps_min: float = 3.0,
                         abs_floor_bps: float = 10.0,
                         min_obs: int = 20) -> pd.DataFrame:
    """
    Flags products where today's alpha contribution is either:
      (a) statistically unusual (|z-score| >= z_min) AND materially impactful (|bps| >= bps_min), OR
      (b) simply absolutely large (|bps| >= abs_floor_bps), regardless of z —
          catches moves on historically-volatile names (e.g. AXIA) where σ is too wide
          for the z-test to trigger even on a real sell-off.
    Excludes non-directional livros (Caixa, Taxas e Custos).
    """
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    today = pd.Timestamp(date_str)
    excluded_livros = {"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}

    d = df_daily[~df_daily["LIVRO"].isin(excluded_livros)]

    past  = d[d["DATE"] < today]
    today_df = d[d["DATE"] == today]

    keys = ["FUNDO", "LIVRO", "PRODUCT"]

    # Implied daily return = dia_bps / position_brl (asset return, position-neutral)
    pos_col = "position_brl" if "position_brl" in past.columns else None
    if pos_col and (past[pos_col] > 0).any():
        valid_pos = past[past[pos_col] > 0].copy()
        valid_pos["implied_return"] = valid_pos["dia_bps"] / valid_pos[pos_col]
        stats = valid_pos.groupby(keys)["implied_return"].agg(sigma="std", n_obs="count").reset_index()
    else:
        stats = past.groupby(keys)["dia_bps"].agg(sigma="std", n_obs="count").reset_index()

    today_agg = today_df.groupby(keys).agg(
        today_bps=("dia_bps", "sum"),
        today_pos=("position_brl", "sum") if pos_col else ("dia_bps", "count"),
    ).reset_index()

    merged = today_agg.merge(stats, on=keys, how="left")
    merged["sigma"] = merged["sigma"].fillna(0.0)
    merged["n_obs"] = merged["n_obs"].fillna(0).astype(int)

    # Z = today's implied return / σ_implied_return (asset vol, position-neutral)
    merged["z"] = 0.0
    if pos_col:
        valid = (merged["sigma"] > 1e-9) & (merged["today_pos"] > 0)
        merged.loc[valid, "z"] = (
            (merged.loc[valid, "today_bps"] / merged.loc[valid, "today_pos"])
            / merged.loc[valid, "sigma"]
        )
    else:
        valid = merged["sigma"] > 0.05
        merged.loc[valid, "z"] = merged.loc[valid, "today_bps"] / merged.loc[valid, "sigma"]

    statistical = (
        (merged["z"].abs() >= z_min)
        & (merged["today_bps"].abs() >= bps_min)
        & (merged["n_obs"] >= min_obs)
    )
    absolute_big = (
        (merged["today_bps"].abs() >= abs_floor_bps)
        & (merged["n_obs"] >= min_obs)
    )
    flagged = merged[statistical | absolute_big].copy()
    flagged = flagged.sort_values("today_bps", key=lambda s: s.abs(), ascending=False)
    return flagged


def fetch_cdi_returns(date_str: str = DATA_STR) -> dict:
    """CDI cumulative simple-sum over DIA/MTD/YTD/12M windows (bps). Daily rate stored in ECO_INDEX."""
    q = f"""
    SELECT
      SUM(CASE WHEN "DATE" = DATE '{date_str}'                      THEN "VALUE" ELSE 0 END) * 10000 AS dia_bps,
      SUM(CASE WHEN "DATE" >= DATE_TRUNC('month', DATE '{date_str}')
                AND "DATE" <= DATE '{date_str}'                     THEN "VALUE" ELSE 0 END) * 10000 AS mtd_bps,
      SUM(CASE WHEN "DATE" >= DATE_TRUNC('year',  DATE '{date_str}')
                AND "DATE" <= DATE '{date_str}'                     THEN "VALUE" ELSE 0 END) * 10000 AS ytd_bps,
      SUM(CASE WHEN "DATE" >  (DATE '{date_str}' - INTERVAL '12 months')
                AND "DATE" <= DATE '{date_str}'                     THEN "VALUE" ELSE 0 END) * 10000 AS m12_bps
    FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = 'CDI' AND "FIELD" = 'YIELD'
    """
    df = read_sql(q)
    if df.empty:
        return {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
    r = df.iloc[0]
    return {"dia": float(r["dia_bps"]), "mtd": float(r["mtd_bps"]),
            "ytd": float(r["ytd_bps"]), "m12": float(r["m12_bps"])}


def _pa_escape(v) -> str:
    """Sanitize a tree key for use inside data-path attributes."""
    s = str(v) if v is not None and str(v) != "nan" else "—"
    return s.replace("|", "¦").replace('"', "'")


# PA display renames — strip "Macro_" prefix on PM names
_PA_LIVRO_RENAMES = {
    "Macro_JD": "JD", "Macro_LF": "LF", "Macro_RJ": "RJ",
    "Macro_MD": "MD", "Macro_QM": "QM", "Macro_AC": "AC",
    "Macro_DF": "DF", "Macro_FG": "FG",
}
# Rows that should always sit at the bottom of their sibling group and never be sorted.
_PA_PINNED_BOTTOM = {"Caixa", "Caixa USD", "Taxas e Custos", "Custos", "Caixa & Custos"}


def _pa_render_name(raw: str) -> str:
    """Map raw tree key to display label (e.g., Macro_JD → JD)."""
    return _PA_LIVRO_RENAMES.get(raw, raw)


def _evo_strategy(livro: str) -> str:
    """Bucket a LIVRO into a high-level strategy for the EVOLUTION Livro view."""
    if livro == "CI" or livro.startswith("Macro_"):
        return "Macro"
    if livro in {"Bracco", "Quant_PA", "HEDGE_SISTEMATICO"} or livro.startswith("SIST_"):
        return "Quant"
    if livro in {"FMN", "FCO", "FLO", "GIS FI FUNDO IMOBILIÁRIO", "RV BZ"}:
        return "Equities"
    if livro in {"Crédito", "CRÉDITO", "CredEstr"}:
        return "Crédito"
    return "Caixa & Custos"


# Default display orders (per level type). Lower = earlier.
# Names not found fall back to |YTD| desc.
_PA_ORDER_CLASSE = {
    "RF BZ": 10, "RF BZ IPCA": 11, "RF BZ IGP-M": 12, "RV BZ": 13,
    "RF Intl": 20, "RV Intl": 21,
    "BRLUSD": 30, "FX": 31, "Commodities": 32, "ETF Options": 33,
    "Credito": 50, "Crédito": 50, "CRÉDITO": 50, "CredEstr": 51,
}
_PA_ORDER_LIVRO = {
    # Macro PMs — prefer renamed, but also accept raw
    "CI": 100,
    "JD": 101, "Macro_JD": 101, "LF": 102, "Macro_LF": 102,
    "RJ": 103, "Macro_RJ": 103, "MD": 104, "Macro_MD": 104,
    "QM": 105, "Macro_QM": 105, "AC": 106, "Macro_AC": 106,
    "DF": 107, "Macro_DF": 107, "FG": 108, "Macro_FG": 108,
    # Quant
    "Bracco": 200, "Quant_PA": 201,
    "SIST_GLOBAL": 210, "SIST_COMMO": 211, "SIST_FX": 212, "SIST_RF": 213,
    "HEDGE_SISTEMATICO": 220,
    # Equities
    "FMN": 300, "FCO": 301, "FLO": 302, "GIS FI FUNDO IMOBILIÁRIO": 303,
    # Crédito
    "Crédito": 500, "CRÉDITO": 500, "CredEstr": 501,
    # Prev (Albatroz)
    "Prev": 600,
    # RV BZ sub-book inside LIVRO
    "RV BZ": 400,
}
_PA_ORDER_STRATEGY = {"Macro": 1, "Quant": 2, "Equities": 3, "Crédito": 4}

# Mapping from level column name → order dict
_PA_ORDER_BY_LEVEL = {
    "CLASSE":   _PA_ORDER_CLASSE,
    "LIVRO":    _PA_ORDER_LIVRO,
    "STRATEGY": _PA_ORDER_STRATEGY,
}


def _pa_bp_cell(v: float, bold: bool = False, heat_max: float = 0.0) -> str:
    """
    Render a bps value as a percentage with 2 decimals (1 bp = 0.01%).
    If `heat_max` > 0, apply a subtle green/red background proportional to |v|/heat_max.
    """
    pct = v / 100.0
    if abs(pct) < 0.005:  # rounds to 0.00%
        return '<td class="t-num mono" style="color:var(--muted)">—</td>'
    color = "var(--up)" if v >= 0 else "var(--down)"
    weight = "font-weight:700;" if bold else ""
    bg = ""
    if heat_max and heat_max > 0:
        rgb = "38,208,124" if v >= 0 else "255,90,106"
        alpha = min(abs(v) / heat_max, 1.0) * 0.14  # subtle so the text stays legible
        bg = f" background:rgba({rgb},{alpha:.2f});"
    return f'<td class="t-num mono" style="color:{color};{weight}{bg}">{pct:+.2f}%</td>'


def _pa_pos_cell(v: float) -> str:
    """Position cell — BRL in millions, hide dust."""
    if abs(v) < 1e5:  # <100k reais
        return '<td class="t-num mono pa-pos" style="color:var(--muted)">—</td>'
    m = v / 1e6
    return f'<td class="t-num mono pa-pos" style="color:var(--muted)">{m:,.1f}</td>'


_PA_AGG_LEN = 5  # dia, mtd, ytd, m12, position_brl


def _build_pa_tree(df: pd.DataFrame, levels: list) -> dict:
    """Build a nested dict tree from leaf rows, aggregating bps metrics + position up each level."""
    root = {"_children": {}, "_agg": [0.0] * _PA_AGG_LEN}
    val_cols = ["dia_bps","mtd_bps","ytd_bps","m12_bps","position_brl"]
    for r in df.itertuples(index=False):
        vals = [getattr(r, c) for c in val_cols]
        node = root
        node["_agg"] = [a + v for a, v in zip(node["_agg"], vals)]
        for lv in levels:
            key = _pa_escape(getattr(r, lv))
            if key not in node["_children"]:
                node["_children"][key] = {"_children": {}, "_agg": [0.0] * _PA_AGG_LEN}
            node = node["_children"][key]
            node["_agg"] = [a + v for a, v in zip(node["_agg"], vals)]
    return root


def _render_pa_tree_rows(node: dict, path: list, depth: int, levels: list, out: list):
    """
    Depth-first traversal. Non-pinned children sorted by declared order (fixed list
    inspired by Excel PA report) with |YTD| desc as tiebreak. Pinned children
    (Caixa/Custos/…) always appended at the end.
    """
    max_depth = len(levels)
    level_name = levels[depth] if depth < max_depth else ""
    order_dict = _PA_ORDER_BY_LEVEL.get(level_name, {})
    items = list(node["_children"].items())
    pinned = [kv for kv in items if kv[0] in _PA_PINNED_BOTTOM]
    regular = [kv for kv in items if kv[0] not in _PA_PINNED_BOTTOM]
    regular.sort(key=lambda kv: (order_dict.get(kv[0], 10_000), -abs(kv[1]["_agg"][2])))
    pinned.sort(key=lambda kv: kv[0])
    ordered = regular + pinned
    for name, child in ordered:
        cur_path = path + [name]
        has_children = bool(child["_children"]) and depth < max_depth - 1
        out.append({
            "name": name,
            "display": _pa_render_name(name),
            "depth": depth,
            "path": "|".join(cur_path),
            "parent": "|".join(path) if path else "",
            "has_children": has_children,
            "pinned": name in _PA_PINNED_BOTTOM,
            "agg": child["_agg"],
        })
        if has_children:
            _render_pa_tree_rows(child, cur_path, depth + 1, levels, out)


def _render_pa_row_html(r: dict, max_abs: list) -> str:
    """Server-side render of a single PA row (used for depth-0 rows only with lazy render)."""
    level_cls = f"pa-l{r['depth']}"
    pinned_cls = " pa-pinned" if r["pinned"] else ""
    expander = (
        '<span class="pa-exp" aria-hidden="true">▸</span>'
        if r["has_children"] else
        '<span class="pa-exp pa-exp-empty" aria-hidden="true"></span>'
    )
    base_cls = f"pa-row {level_cls}{pinned_cls}"
    if r["has_children"]:
        cls_click = f' onclick="togglePaRow(this)" class="{base_cls} pa-has-children"'
    else:
        cls_click = f' class="{base_cls}"'
    pinned_attr = ' data-pinned="1"' if r["pinned"] else ""
    row_attrs = (
        f' data-level="{r["depth"]}"'
        f' data-path="{r["path"]}"'
        f' data-parent="{r["parent"]}"'
        f'{pinned_attr}'
    )
    agg = r["agg"]
    cells = (
        f'<td class="pa-name">{expander}<span class="pa-label">{r["display"]}</span></td>'
        + _pa_bp_cell(agg[0], heat_max=max_abs[0])
        + _pa_bp_cell(agg[1], heat_max=max_abs[1])
        + _pa_bp_cell(agg[2], heat_max=max_abs[2])
        + _pa_bp_cell(agg[3], heat_max=max_abs[3])
    )
    return f'<tr{row_attrs}{cls_click}>{cells}</tr>'


def _build_pa_view(fund_short: str, df: pd.DataFrame, view_id: str,
                    levels: list, first_col_label: str, active: bool,
                    cdi: dict) -> str:
    """
    Render one PA hierarchical view with lazy-loaded descendants.
    - Only depth-0 rows are pre-rendered as HTML.
    - Deeper rows are embedded as compact JSON and instantiated on expand.
    - Heatmap: each metric cell gets a background tint proportional to |v|/col_max.
    """
    if df is None or df.empty:
        return ""
    tree = _build_pa_tree(df, levels)
    rows = []
    _render_pa_tree_rows(tree, [], 0, levels, rows)

    # Max absolute per column (DIA/MTD/YTD/12M/POS) for heatmap scaling
    max_abs = [0.0] * _PA_AGG_LEN
    for r in rows:
        for i, v in enumerate(r["agg"]):
            if abs(v) > max_abs[i]:
                max_abs[i] = abs(v)

    # Group rows by parent path for lazy rendering
    by_parent: dict = {}
    for r in rows:
        by_parent.setdefault(r["parent"], []).append({
            "n":  r["name"],
            "d":  r["display"],
            "pa": r["path"],
            "pr": r["parent"],
            "a":  [round(x, 4) for x in r["agg"]],
            "hc": 1 if r["has_children"] else 0,
            "pi": 1 if r["pinned"] else 0,
            "dp": r["depth"],
        })

    root_rows = by_parent.get("", [])
    # Server-render only depth-0 rows
    tbody_rows = []
    for child in root_rows:
        tbody_rows.append(_render_pa_row_html({
            "name": child["n"], "display": child["d"],
            "depth": child["dp"], "path": child["pa"], "parent": child["pr"],
            "has_children": bool(child["hc"]), "pinned": bool(child["pi"]),
            "agg": child["a"],
        }, max_abs))

    # Compact JSON: keep all levels (including root) so filter/expand-all can work uniformly
    data_id = f"pa-data-{fund_short}-{view_id}"
    json_blob = json.dumps(
        {"maxAbs": [round(x, 4) for x in max_abs], "byParent": by_parent},
        separators=(",", ":"), ensure_ascii=False,
    ).replace("</", "<\\/")

    t = tree["_agg"]
    total_row = (
        '<tr class="pa-total-row">'
        '<td class="pa-name" style="font-weight:700">Total Alpha</td>'
        + _pa_bp_cell(t[0], bold=True)
        + _pa_bp_cell(t[1], bold=True)
        + _pa_bp_cell(t[2], bold=True)
        + _pa_bp_cell(t[3], bold=True)
        + "</tr>"
    )
    cdi_row = (
        '<tr class="pa-bench-row">'
        '<td class="pa-name" style="color:var(--muted); font-style:italic">Benchmark (CDI)</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["dia"]/100:+.2f}%</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["mtd"]/100:+.2f}%</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["ytd"]/100:+.2f}%</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["m12"]/100:+.2f}%</td>'
        '</tr>'
    )
    nominal_row = (
        '<tr class="pa-nominal-row">'
        '<td class="pa-name" style="font-weight:700">Retorno Nominal</td>'
        + _pa_bp_cell(t[0] + cdi["dia"], bold=True)
        + _pa_bp_cell(t[1] + cdi["mtd"], bold=True)
        + _pa_bp_cell(t[2] + cdi["ytd"], bold=True)
        + _pa_bp_cell(t[3] + cdi["m12"], bold=True)
        + "</tr>"
    )

    # Sort arrow: default YTD desc (tree is already server-sorted that way)
    def th_sort(idx: int, label: str, active_idx: int = 2) -> str:
        arrow = ' <span class="pa-sort-arrow">▾</span>' if idx == active_idx else ''
        extra = ' pa-sort-active' if idx == active_idx else ''
        return (
            f'<th class="pa-sortable{extra}" data-pa-metric="{idx}"'
            f' onclick="sortPaMetric(this,{idx})" style="text-align:right; cursor:pointer">'
            f'{label}{arrow}</th>'
        )

    active_style = "" if active else ' style="display:none"'
    return f"""
    <div class="pa-view" data-pa-view="{view_id}" data-pa-id="{data_id}"
         data-sort-idx="2" data-sort-desc="1"{active_style}>
      <script type="application/json" id="{data_id}">{json_blob}</script>
      <table class="pa-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">{first_col_label}</th>
          {th_sort(0,'DIA')}
          {th_sort(1,'MTD')}
          {th_sort(2,'YTD')}
          {th_sort(3,'12M')}
        </tr></thead>
        <tbody>{''.join(tbody_rows)}</tbody>
        <tfoot>{total_row}{cdi_row}{nominal_row}</tfoot>
      </table>
    </div>"""


def build_pa_section_hier(fund_short: str, df_pa: pd.DataFrame, cdi: dict) -> str:
    """
    PA card with two hierarchical views (Por Classe / Por Livro) and tree drill-down.
    Default depth is 2 (top → PRODUCT). EVOLUTION Livro view uses 3 levels
    (Strategy → Livro → PRODUCT) so macro/quant/equities/crédito appear as the first drill-down.
    """
    pa_key = _FUND_PA_KEY.get(fund_short)
    if pa_key is None or df_pa is None or df_pa.empty:
        return ""
    df = df_pa[df_pa["FUNDO"] == pa_key].copy()
    if df.empty:
        return ""

    view_classe = _build_pa_view(
        fund_short, df, "classe",
        ["CLASSE", "PRODUCT"], "Classe / Produto", active=True, cdi=cdi,
    )

    if fund_short == "EVOLUTION":
        df_evo = df.copy()
        df_evo["STRATEGY"] = df_evo["LIVRO"].map(_evo_strategy)
        view_livro = _build_pa_view(
            fund_short, df_evo, "livro",
            ["STRATEGY", "LIVRO", "PRODUCT"], "Strategy / Livro / Produto",
            active=False, cdi=cdi,
        )
    else:
        view_livro = _build_pa_view(
            fund_short, df, "livro",
            ["LIVRO", "PRODUCT"], "Livro / Produto", active=False, cdi=cdi,
        )

    return f"""
    <section class="card pa-card" data-pa-fund="{fund_short}">
      <div class="card-head">
        <span class="card-title">Performance Attribution</span>
        <span class="card-sub">— {fund_short} · alpha (%) vs. benchmark · click p/ drill-down</span>
        <div class="pa-toolbar">
          <input class="pa-search" type="search" placeholder="🔍 buscar..." oninput="filterPa(this)" title="Filtrar por nome (busca parcial)"/>
          <button class="pa-btn" onclick="expandAllPa(this)"    title="Expandir tudo">⤢ Expandir</button>
          <button class="pa-btn" onclick="collapseAllPa(this)"  title="Colapsar tudo">⤡ Colapsar</button>
        </div>
        <div class="pa-view-toggle">
          <button class="pa-tgl active" data-pa-view="classe"
                  onclick="selectPaView(this,'classe')">Por Classe</button>
          <button class="pa-tgl" data-pa-view="livro"
                  onclick="selectPaView(this,'livro')">Por Livro</button>
        </div>
      </div>
      {view_classe}
      {view_livro}
    </section>"""


def build_frontier_lo_section(df: pd.DataFrame, date_str: str) -> str:
    """Long Only position/attribution card for Frontier Ações FIC FI."""
    if df is None or df.empty:
        return ""

    val_date = str(df["VAL_DATE"].iloc[0])[:10]
    stale = val_date != date_str

    total_row = df[df["PRODUCT"] == "TOTAL"]
    if total_row.empty:
        return ""
    tot = total_row.iloc[0]

    gross    = tot["% Cash"]
    delta    = tot["DELTA"]
    nav_brl  = tot["R$"] / gross if gross else 0

    att_d  = tot["TOTAL_ATRIBUTION_DAY"]   * 100
    att_m  = tot["TOTAL_ATRIBUTION_MONTH"] * 100
    att_y  = tot["TOTAL_ATRIBUTION_YEAR"]  * 100
    er_d   = tot["TOTAL_IBOD_Benchmark_DAY"]   * 100
    er_m   = tot["TOTAL_IBOD_Benchmark_MONTH"] * 100
    er_y   = tot["TOTAL_IBOD_Benchmark_YEAR"]  * 100
    ibov_d = tot["TOTAL_IBVSP_DAY"]   * 100
    ibov_m = tot["TOTAL_IBVSP_MONTH"] * 100
    ibov_y = tot["TOTAL_IBVSP_YEAR"]  * 100

    stocks = df[~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"])]
    stocks_valid = stocks.dropna(subset=["BETA", "% Cash"])
    pct_sum = stocks_valid["% Cash"].sum()
    w_beta = (stocks_valid["% Cash"] * stocks_valid["BETA"]).sum() / pct_sum if pct_sum else None

    def pct(v, decimals=2):
        try:
            f = float(v)
            return f"{'+'if f>0 else ''}{f*100:.{decimals}f}%"
        except Exception:
            return "—"

    def color(v):
        try:
            return "var(--up)" if float(v) >= 0 else "var(--down)"
        except Exception:
            return "inherit"

    def fmt_brl(v):
        try:
            return f"R$ {float(v)/1e6:.1f}M"
        except Exception:
            return "—"

    stale_badge = ' <span style="color:var(--warn);font-size:10px">D-1</span>' if stale else ""
    nav_fmt = f"{nav_brl/1e6:.1f}M" if nav_brl else "—"
    beta_fmt = f"{w_beta:.2f}" if w_beta is not None else "—"

    # ── Header metrics ─────────────────────────────────────────────────────────
    def metric_chip(label, val, c=None):
        col = c or color(val)
        return (f'<span class="sn-stat"><span class="sn-lbl">{label}</span>'
                f'<span class="sn-val mono" style="color:{col}">{pct(val)}</span></span>')

    header_metrics = f"""
      <div class="sn-inline-stats mono" style="margin-bottom:10px;flex-wrap:wrap;gap:6px 16px">
        <span class="sn-stat"><span class="sn-lbl">NAV</span><span class="sn-val mono">R$ {nav_fmt}</span></span>
        <span class="sn-stat"><span class="sn-lbl">Gross</span><span class="sn-val mono">{pct(gross)}</span></span>
        <span class="sn-stat"><span class="sn-lbl">Beta pond.</span><span class="sn-val mono">{beta_fmt}</span></span>
        <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
        {metric_chip("Attrib D", att_d/100)}
        {metric_chip("Attrib MTD", att_m/100)}
        {metric_chip("Attrib YTD", att_y/100)}
        <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
        {metric_chip("ER IBOD D", er_d/100)}
        {metric_chip("ER IBOD MTD", er_m/100)}
        {metric_chip("ER IBOD YTD", er_y/100)}
        <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
        {metric_chip("ER IBOV D", ibov_d/100)}
        {metric_chip("ER IBOV MTD", ibov_m/100)}
        {metric_chip("ER IBOV YTD", ibov_y/100)}
      </div>"""

    # ── Position table ─────────────────────────────────────────────────────────
    col_headers = """
      <tr class="col-headers">
        <th style="text-align:left;min-width:80px">Ticker</th>
        <th style="text-align:right">% Cash</th>
        <th style="text-align:right">Delta</th>
        <th style="text-align:right">Beta</th>
        <th style="text-align:right">#ADTV</th>
        <th style="text-align:right">Ret D</th>
        <th style="text-align:right">Ret MTD</th>
        <th style="text-align:right">Ret YTD</th>
        <th style="text-align:right">Attrib D</th>
        <th style="text-align:right">Attrib MTD</th>
        <th style="text-align:right">Attrib YTD</th>
        <th style="text-align:right">ER IBOD D</th>
        <th style="text-align:right">ER IBOD MTD</th>
        <th style="text-align:right">ER IBOD YTD</th>
      </tr>"""

    def make_row(row, is_subtotal=False, is_total=False):
        ticker = row["PRODUCT"]
        book   = row.get("BOOK", "")
        label  = f"{ticker}" + (f" <span style='color:var(--muted);font-size:9px'>({book})</span>" if is_total else "")
        bg = "background:rgba(30,144,255,0.08)" if is_total else ("background:rgba(255,255,255,0.04)" if is_subtotal else "")
        fw = "font-weight:700" if (is_subtotal or is_total) else ""

        def cell(v, is_pct=True, decimals=2):
            try:
                f = float(v)
                txt = f"{'+'if f>0 else ''}{f*100:.{decimals}f}%" if is_pct else f"{f:.{decimals}f}"
                col = color(f) if is_pct else "inherit"
                return f'<td class="mono" style="text-align:right;color:{col}">{txt}</td>'
            except Exception:
                return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'

        adtv_cell = cell(row.get("#ADTV"), is_pct=False, decimals=2) if not (is_subtotal or is_total) else '<td></td>'
        beta_cell = cell(row.get("BETA"), is_pct=False, decimals=2) if not (is_subtotal or is_total) else '<td></td>'
        ret_d     = cell(row.get("RETURN_DAY"))    if not (is_subtotal or is_total) else '<td></td>'
        ret_m     = cell(row.get("RETURN_MONTH"))  if not (is_subtotal or is_total) else '<td></td>'
        ret_y     = cell(row.get("RETURN_YEAR"))   if not (is_subtotal or is_total) else '<td></td>'

        return f"""<tr style="{bg};{fw}">
          <td style="padding-left:{'4px' if is_subtotal or is_total else '12px'};white-space:nowrap">{label}</td>
          {cell(row.get("% Cash"))}
          {cell(row.get("DELTA"))}
          {beta_cell}
          {adtv_cell}
          {ret_d}{ret_m}{ret_y}
          {cell(row.get("TOTAL_ATRIBUTION_DAY"))}
          {cell(row.get("TOTAL_ATRIBUTION_MONTH"))}
          {cell(row.get("TOTAL_ATRIBUTION_YEAR"))}
          {cell(row.get("TOTAL_IBOD_Benchmark_DAY"))}
          {cell(row.get("TOTAL_IBOD_Benchmark_MONTH"))}
          {cell(row.get("TOTAL_IBOD_Benchmark_YEAR"))}
        </tr>"""

    tbody = ""
    books = [b for b in df["BOOK"].unique() if b and b not in ("", None)]
    for book in books:
        book_rows = df[(df["BOOK"] == book) & (~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"]))]
        book_rows = book_rows.sort_values("% Cash", ascending=False, na_position="last")
        # Book header
        tbody += f'<tr><td colspan="14" style="padding:6px 4px 2px;font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;border-top:1px solid var(--border)">{book}</td></tr>'
        for _, r in book_rows.iterrows():
            tbody += make_row(r)
        # Subtotal for this book
        sub = df[(df["PRODUCT"] == "SUBTOTAL") & (df["BOOK"] == book)]
        if not sub.empty:
            tbody += make_row(sub.iloc[0], is_subtotal=True)

    # Grand total
    tbody += make_row(tot, is_total=True)

    table_html = f"""
      <div style="overflow-x:auto">
        <table class="metric-table" style="font-size:11px;min-width:900px">
          <thead>{col_headers}</thead>
          <tbody>{tbody}</tbody>
        </table>
      </div>"""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Long Only</span>
        <span class="card-sub">— Frontier Ações · {val_date}{stale_badge} · NAV R$ {nav_fmt} · Beta {beta_fmt}</span>
      </div>
      {header_metrics}
      {table_html}
    </section>"""


def build_frontier_exposure_section(df_lo: pd.DataFrame,
                                    df_ibov: pd.DataFrame,
                                    df_smll: pd.DataFrame,
                                    df_sectors: pd.DataFrame) -> str:
    """Active-weight exposure card for Frontier Long Only vs IBOV / IBOD."""
    if df_lo is None or df_lo.empty:
        return ""
    if df_ibov is None or df_ibov.empty:
        return ""

    # ── Positions ──────────────────────────────────────────────────────────────
    EXCLUDE = {"TOTAL", "SUBTOTAL"}
    # Handle encoding issues with "AÇÕES ALIENADAS"
    stocks = df_lo[df_lo["BOOK"].astype(str).str.strip() != ""].copy()
    stocks = stocks[~stocks["PRODUCT"].isin(EXCLUDE)].copy()
    stocks = stocks[stocks["% Cash"].notna()].copy()
    stocks = stocks.sort_values("% Cash", ascending=False).reset_index(drop=True)

    if stocks.empty:
        return ""

    total_row = df_lo[df_lo["PRODUCT"] == "TOTAL"]
    gross = float(total_row.iloc[0]["% Cash"]) if not total_row.empty else stocks["% Cash"].sum()
    cash_pct = max(0.0, 1.0 - gross)

    # ── Index weights ──────────────────────────────────────────────────────────
    ibov_wt = df_ibov.set_index("INSTRUMENT")["weight"].to_dict() if not df_ibov.empty else {}
    smll_wt = df_smll.set_index("INSTRUMENT")["weight"].to_dict() if not df_smll.empty else {}

    stocks["ibov_w"] = stocks["PRODUCT"].map(ibov_wt).fillna(0.0)
    stocks["smll_w"] = stocks["PRODUCT"].map(smll_wt).fillna(0.0)
    stocks["ibod_w"] = 0.5 * stocks["ibov_w"] + 0.5 * stocks["smll_w"]
    stocks["ibov_act"] = stocks["% Cash"] - stocks["ibov_w"]
    stocks["ibod_act"] = stocks["% Cash"] - stocks["ibod_w"]

    # ── Sector mapping ─────────────────────────────────────────────────────────
    if not df_sectors.empty:
        sec_map  = df_sectors.drop_duplicates("TICKER").set_index("TICKER")
        stocks["sector"] = stocks["PRODUCT"].map(sec_map["GLPG_SECTOR"]).fillna("Outros")
        stocks["macro"]  = stocks["PRODUCT"].map(sec_map["GLPG_MACRO_CLASSIFICATION"]).fillna("—")
    else:
        stocks["sector"] = "Outros"
        stocks["macro"]  = "—"

    # ── Header stats ───────────────────────────────────────────────────────────
    w_beta_num = (stocks["% Cash"] * stocks["BETA"].fillna(0)).sum()
    w_beta = w_beta_num / gross if gross > 0 else None

    # Ex-ante TE (beta mismatch only, σ_IBOV ≈ 20% annualized)
    _SIGMA_IBOV = 0.20
    te_ibov = abs((w_beta or 1.0) - 1.0) * _SIGMA_IBOV * 100  # in %
    te_ibod = te_ibov * 0.75  # IBOD has ~75% IBOV correlation

    def _pf(v, decimals=2, sign=False):
        try:
            f = float(v)
            s = "+" if (sign and f > 0) else ""
            return f"{s}{f*100:.{decimals}f}%"
        except Exception:
            return "—"

    def _col(v):
        try:
            return "var(--up)" if float(v) >= 0 else "var(--down)"
        except Exception:
            return "inherit"

    beta_fmt = f"{w_beta:.2f}" if w_beta is not None else "—"

    stats_bar = f"""
    <div class="sn-inline-stats mono" style="margin-bottom:12px;flex-wrap:wrap;gap:6px 16px">
      <span class="sn-stat"><span class="sn-lbl">Gross</span>
        <span class="sn-val mono">{_pf(gross)}</span></span>
      <span class="sn-stat"><span class="sn-lbl">Caixa</span>
        <span class="sn-val mono" style="color:var(--muted)">{_pf(cash_pct)}</span></span>
      <span class="sn-stat"><span class="sn-lbl">Beta pond.</span>
        <span class="sn-val mono">{beta_fmt}</span></span>
      <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
      <span class="sn-stat"><span class="sn-lbl">TE aprox vs IBOV</span>
        <span class="sn-val mono" style="color:var(--warn)">{te_ibov:.1f}%</span></span>
      <span class="sn-stat"><span class="sn-lbl">TE aprox vs IBOD</span>
        <span class="sn-val mono" style="color:var(--warn)">{te_ibod:.1f}%</span></span>
      <span style="font-size:9px;color:var(--muted);align-self:center">(TE estimado via β; σ<sub>IBOV</sub>=20%)</span>
    </div>"""

    # ── Toggle buttons ─────────────────────────────────────────────────────────
    uid = "loexpo"
    toggle_bar = f"""
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap">
      <div style="display:flex;gap:4px">
        <button id="{uid}-ibov-btn" class="toggle-btn active"
                onclick="loExpoBmk('{uid}','ibov')">IBOV</button>
        <button id="{uid}-ibod-btn" class="toggle-btn"
                onclick="loExpoBmk('{uid}','ibod')">IBOD</button>
      </div>
      <div style="display:flex;gap:4px;margin-left:auto">
        <button id="{uid}-name-btn" class="toggle-btn active"
                onclick="loExpoView('{uid}','name')">Por Nome</button>
        <button id="{uid}-sector-btn" class="toggle-btn"
                onclick="loExpoView('{uid}','sector')">Por Setor</button>
        <span id="{uid}-expand-btns" style="display:none;gap:4px;margin-left:4px;display:none">
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="loExpoExpandAll('{uid}')">▼ All</button>
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="loExpoCollapseAll('{uid}')">▶ All</button>
        </span>
      </div>
    </div>"""

    # ── Table header ───────────────────────────────────────────────────────────
    th = """<thead><tr>
      <th style="text-align:left;min-width:70px">Ticker</th>
      <th style="text-align:right">Posição</th>
      <th style="text-align:right" id="{uid}-th-bmk">Bench</th>
      <th style="text-align:right" id="{uid}-th-act">Ativo</th>
      <th style="text-align:right">Beta</th>
      <th style="text-align:right">ER D</th>
      <th style="text-align:right">ER MTD</th>
      <th style="text-align:right">ER YTD</th>
    </tr></thead>""".replace("{uid}", uid)

    def _tr(row, indent=False, is_sector=False, colspan=None):
        if is_sector:
            s = row["sector"]
            pos_tot = row["pos_tot"]
            ibov_tot = row["ibov_tot"]
            ibod_tot = row["ibod_tot"]
            ibov_a_tot = row["ibov_act_tot"]
            ibod_a_tot = row["ibod_act_tot"]
            return (f'<tr class="{uid}-sector-row" style="background:rgba(59,130,246,0.07);'
                    f'font-weight:700;cursor:pointer" onclick="loExpoToggleSector(this)">'
                    f'<td style="padding:5px 4px;white-space:nowrap">'
                    f'<span class="{uid}-sector-arrow" style="font-size:9px;margin-right:4px">▼</span>{s}</td>'
                    f'<td class="mono" style="text-align:right">{_pf(pos_tot)}</td>'
                    f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                    f'  data-ibov="{ibov_tot:.6f}" data-ibod="{ibod_tot:.6f}">{_pf(ibov_tot)}</td>'
                    f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(ibov_a_tot)}"'
                    f'  data-ibov="{ibov_a_tot:.6f}" data-ibod="{ibod_a_tot:.6f}">{_pf(ibov_a_tot, sign=True)}</td>'
                    f'<td></td><td></td><td></td><td></td></tr>')

        tk   = row["PRODUCT"]
        pos  = row["% Cash"]
        ibov_b = row["ibov_w"]
        ibod_b = row["ibod_w"]
        ibov_a = row["ibov_act"]
        ibod_a = row["ibod_act"]
        beta = row.get("BETA")
        er_d = row.get("TOTAL_IBOD_Benchmark_DAY")
        er_m = row.get("TOTAL_IBOD_Benchmark_MONTH")
        er_y = row.get("TOTAL_IBOD_Benchmark_YEAR")
        pad  = "padding-left:20px" if indent else "padding-left:4px"
        beta_td = (f'<td class="mono" style="text-align:right">{float(beta):.2f}</td>'
                   if pd.notna(beta) else '<td class="mono" style="text-align:right;color:var(--muted)">—</td>')

        def _ertd(v):
            try:
                f = float(v)
                s = "+" if f >= 0 else ""
                return (f'<td class="mono" style="text-align:right;color:{_col(f)}">'
                        f'{s}{f*100:.2f}%</td>')
            except Exception:
                return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'

        return (f'<tr>'
                f'<td style="{pad};white-space:nowrap">{tk}</td>'
                f'<td class="mono" style="text-align:right">{_pf(pos)}</td>'
                f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                f'  data-ibov="{ibov_b:.6f}" data-ibod="{ibod_b:.6f}">{_pf(ibov_b)}</td>'
                f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(ibov_a)}"'
                f'  data-ibov="{ibov_a:.6f}" data-ibod="{ibod_a:.6f}">{_pf(ibov_a, sign=True)}</td>'
                f'{beta_td}'
                f'{_ertd(er_d)}{_ertd(er_m)}{_ertd(er_y)}'
                f'</tr>')

    # ── By Name table ──────────────────────────────────────────────────────────
    name_rows = "".join(_tr(r) for _, r in stocks.iterrows())
    # Totals row
    tot_ibov_act = stocks["ibov_act"].sum()
    tot_ibod_act = stocks["ibod_act"].sum()
    tot_ibov_bmk = stocks["ibov_w"].sum()
    tot_ibod_bmk = stocks["ibod_w"].sum()
    name_rows += (f'<tr style="font-weight:700;border-top:2px solid var(--border)">'
                  f'<td>TOTAL</td>'
                  f'<td class="mono" style="text-align:right">{_pf(gross)}</td>'
                  f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                  f'  data-ibov="{tot_ibov_bmk:.6f}" data-ibod="{tot_ibod_bmk:.6f}">{_pf(tot_ibov_bmk)}</td>'
                  f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(tot_ibov_act)}"'
                  f'  data-ibov="{tot_ibov_act:.6f}" data-ibod="{tot_ibod_act:.6f}">{_pf(tot_ibov_act, sign=True)}</td>'
                  f'<td></td><td></td><td></td><td></td></tr>')
    name_rows += (f'<tr style="color:var(--muted)">'
                  f'<td style="padding-left:4px;font-style:italic">Caixa</td>'
                  f'<td class="mono" style="text-align:right">{_pf(cash_pct)}</td>'
                  f'<td colspan="6"></td></tr>')

    by_name_html = f"""
    <div id="{uid}-view-name" class="{uid}-view">
      <div style="overflow-x:auto">
        <table class="metric-table" style="font-size:11px;min-width:620px">
          {th}<tbody>{name_rows}</tbody>
        </table>
      </div>
    </div>"""

    # ── By Sector table ────────────────────────────────────────────────────────
    sector_order = (stocks.groupby("sector")["% Cash"].sum()
                    .sort_values(ascending=False).index.tolist())
    sector_rows = ""
    for sec in sector_order:
        grp = stocks[stocks["sector"] == sec].sort_values("% Cash", ascending=False)
        sec_pos   = grp["% Cash"].sum()
        sec_ibov  = grp["ibov_w"].sum()
        sec_ibod  = grp["ibod_w"].sum()
        sec_ibov_a = grp["ibov_act"].sum()
        sec_ibod_a = grp["ibod_act"].sum()
        macro_c = grp.iloc[0]["macro"] if not grp.empty else "—"
        sec_data = {"sector": f"{sec} <span style='font-size:9px;color:var(--muted);font-weight:400'>({macro_c})</span>",
                    "pos_tot": sec_pos, "ibov_tot": sec_ibov, "ibod_tot": sec_ibod,
                    "ibov_act_tot": sec_ibov_a, "ibod_act_tot": sec_ibod_a}
        sector_rows += _tr(sec_data, is_sector=True)
        for _, r in grp.iterrows():
            sector_rows += (f'<tr class="{uid}-child-row">' +
                            _tr(r, indent=True)[4:])  # strip leading <tr>

    by_sector_html = f"""
    <div id="{uid}-view-sector" class="{uid}-view" style="display:none">
      <div style="overflow-x:auto">
        <table class="metric-table" style="font-size:11px;min-width:620px">
          {th}<tbody>{sector_rows}</tbody>
        </table>
      </div>
    </div>"""

    # ── JavaScript ─────────────────────────────────────────────────────────────
    js = f"""<script>
(function() {{
  var _bmk = 'ibov';
  window.loExpoBmk = function(uid, bmk) {{
    _bmk = bmk;
    ['ibov','ibod'].forEach(function(b) {{
      var btn = document.getElementById(uid+'-'+b+'-btn');
      if (btn) btn.classList.toggle('active', b === bmk);
    }});
    document.querySelectorAll('.'+uid+'-bmk-cell').forEach(function(td) {{
      var v = parseFloat(td.dataset[bmk]);
      td.textContent = isNaN(v) ? '—' : (v*100).toFixed(2)+'%';
    }});
    document.querySelectorAll('.'+uid+'-act-cell').forEach(function(td) {{
      var v = parseFloat(td.dataset[bmk]);
      if (isNaN(v)) {{ td.textContent = '—'; return; }}
      td.textContent = (v >= 0 ? '+' : '') + (v*100).toFixed(2) + '%';
      td.style.color = v >= 0 ? 'var(--up)' : 'var(--down)';
    }});
  }};
  window.loExpoView = function(uid, view) {{
    ['name','sector'].forEach(function(v) {{
      var el = document.getElementById(uid+'-view-'+v);
      if (el) el.style.display = (v === view) ? '' : 'none';
      var btn = document.getElementById(uid+'-'+v+'-btn');
      if (btn) btn.classList.toggle('active', v === view);
    }});
    var expBtns = document.getElementById(uid+'-expand-btns');
    if (expBtns) expBtns.style.display = (view === 'sector') ? 'flex' : 'none';
  }};
  window.loExpoToggleSector = function(tr) {{
    var arrow = tr.querySelector('.{uid}-sector-arrow');
    var open = arrow ? arrow.textContent.trim() === '▼' : true;
    if (arrow) arrow.textContent = open ? '▶' : '▼';
    var sib = tr.nextElementSibling;
    while (sib && !sib.classList.contains('{uid}-sector-row')) {{
      sib.style.display = open ? 'none' : '';
      sib = sib.nextElementSibling;
    }}
  }};
  window.loExpoExpandAll = function(uid) {{
    document.querySelectorAll('.'+uid+'-sector-row').forEach(function(tr) {{
      var arrow = tr.querySelector('.'+uid+'-sector-arrow');
      if (arrow && arrow.textContent.trim() === '▶') window.loExpoToggleSector(tr);
    }});
  }};
  window.loExpoCollapseAll = function(uid) {{
    document.querySelectorAll('.'+uid+'-sector-row').forEach(function(tr) {{
      var arrow = tr.querySelector('.'+uid+'-sector-arrow');
      if (arrow && arrow.textContent.trim() === '▼') window.loExpoToggleSector(tr);
    }});
  }};
}})();
</script>"""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição vs Benchmark</span>
        <span class="card-sub">— Frontier Ações · Active Weight por nome e setor</span>
      </div>
      {stats_bar}
      {toggle_bar}
      {by_name_html}
      {by_sector_html}
      {js}
    </section>"""


REPORTS = [
    ("performance",  "PA"),
    ("exposure",     "Exposure"),
    ("single-name",  "Single-Name"),
    ("risk-monitor", "Risk Monitor"),
    ("analise",      "Análise"),
    ("distribution", "Distribuição 252d"),
    ("stop-monitor", "Risk Budget"),
    ("frontier-lo",  "Long Only"),
]
FUND_ORDER  = ["MACRO", "QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER"]
FUND_LABELS = {
    "MACRO": "Macro", "QUANT": "Quantitativo", "EVOLUTION": "Evolution",
    "MACRO_Q": "Macro Q", "ALBATROZ": "Albatroz", "FRONTIER": "Frontier",
}

# PA key in q_models.REPORT_ALPHA_ATRIBUTION for each fund short
_FUND_PA_KEY = {
    "MACRO":     "MACRO",
    "QUANT":     "QUANT",
    "EVOLUTION": "EVOLUTION",
    "MACRO_Q":   "GLOBAL",
    "ALBATROZ":  "ALBATROZ",
}

def build_data_quality_section(manifest: dict, series_map: dict, df_pa, df_pa_daily):
    """
    Returns (full_html, compact_html).
    full_html  — filterable detail table + sanity checks for the Qualidade tab.
    compact_html — compact status alert for the Summary page.
    """
    if manifest is None:
        return "", ""

    DATA_STR_  = manifest["requested_date"]
    d1_str_    = manifest["d1_str"]
    DATA_      = pd.Timestamp(DATA_STR_)
    expo_date  = manifest.get("expo_date", DATA_STR_)

    _NO_VAR_FUNDS: set = set()  # ALBATROZ/MACRO_Q now sourced from LOTE_FUND_STRESS

    # ── Build flat item list: one row per (fund, source) ──────────────────────
    # Each item: {fund_short, source_label, status, date_used, detail, problem}
    # status: "ok" | "stale" | "missing" | "na"
    items = []

    _PA_KEY = {"MACRO":"MACRO","QUANT":"QUANT","EVOLUTION":"EVOLUTION",
               "MACRO_Q":"GLOBAL","ALBATROZ":"ALBATROZ"}
    _TD_BY_SHORT = {cfg["short"]: td_ for td_, cfg in ALL_FUNDS.items()}

    def _pa_item(short):
        pa_key = _PA_KEY.get(short)
        if df_pa is None or df_pa.empty or pa_key is None:
            return dict(status="missing", date="—", detail="PA não disponível")
        sub = df_pa[df_pa["FUNDO"] == pa_key]
        if sub.empty:
            return dict(status="missing", date="—", detail=f"FUNDO={pa_key} não encontrado no PA")
        dia = float(sub["dia_bps"].sum())
        mtd = float(sub["mtd_bps"].sum())
        n   = len(sub)
        has_dia = sub["dia_bps"].abs().sum() > 0.5
        status = "ok" if has_dia else "stale"
        return dict(status=status, date=DATA_STR_ if has_dia else d1_str_,
                    detail=f"{n} instrumentos · DIA {dia:+.1f} bps · MTD {mtd:+.1f} bps")

    def _var_item(short):
        td = _TD_BY_SHORT.get(short)
        if td is None or td not in series_map:
            src = "LOTE_FUND_STRESS" if short in _NO_VAR_FUNDS else "LOTE_FUND_STRESS_RPM"
            return dict(status="missing", date="—", detail=f"Sem dados em {src}")
        s = series_map[td]
        s_avail = s[s["VAL_DATE"] <= DATA_]
        if s_avail.empty:
            return dict(status="missing", date="—", detail="Nenhum registro até a data")
        row = s_avail.iloc[-1]
        last_date = str(row["VAL_DATE"])[:10]
        stale = pd.Timestamp(last_date) < DATA_
        cfg = ALL_FUNDS[td]
        var_val = abs(float(row["var_pct"]))
        soft, hard = cfg.get("var_soft", 999), cfg.get("var_hard", 999)
        stress_val = row.get("stress_pct", None)
        stress_txt = f" · Stress {float(stress_val):.1f}%" if stress_val is not None and not pd.isna(stress_val) else ""
        over = " ⚠ ACIMA SOFT" if var_val >= soft else ""
        detail = f"VaR {var_val:.2f}% (soft {soft:.2f}%, hard {hard:.2f}%){stress_txt}{over}"
        return dict(status="stale" if stale else "ok", date=last_date, detail=detail)

    def _expo_item(short):
        if short == "ALBATROZ":
            ok = manifest.get("alb_expo_ok", False)
            rows = manifest.get("alb_expo_rows", 0)
            if not ok:
                return dict(status="missing", date="—", detail="Exposição ALBATROZ não disponível")
            return dict(status="ok", date=DATA_STR_, detail=f"{rows} posições")
        if short == "QUANT":
            return dict(status="na", date="—", detail="Exposição QUANT não implementada")
        ok = manifest.get("expo_ok", False)
        rows = manifest.get("expo_rows", 0)
        stale = expo_date != DATA_STR_
        if not ok:
            return dict(status="missing", date="—", detail="Exposição MACRO não disponível")
        return dict(status="stale" if stale else "ok", date=expo_date,
                    detail=f"{rows} posições" + (" (fallback D-1)" if stale else ""))

    def _sn_item(short):
        ok = manifest.get("quant_sn_ok" if short=="QUANT" else "evo_sn_ok", False)
        rows = manifest.get("quant_sn_rows" if short=="QUANT" else "evo_sn_rows", 0)
        if not ok:
            return dict(status="missing", date="—", detail="Single-Name não disponível")
        return dict(status="ok", date=DATA_STR_, detail=f"{rows} nomes")

    def _dist_item(short):
        today_ok = manifest.get("dist_today_ok", False)
        prev_ok  = manifest.get("dist_prev_ok", False)
        if today_ok:
            return dict(status="ok", date=DATA_STR_, detail="backward + forward disponíveis")
        if prev_ok:
            return dict(status="stale", date=d1_str_, detail="backward D-1 (fallback) · forward indisponível")
        return dict(status="missing", date="—", detail="Distribuição 252d não disponível")

    def _stop_item(short):
        if short == "ALBATROZ":
            ok = manifest.get("alb_expo_ok", False)
            return dict(status="ok" if ok else "missing", date=DATA_STR_ if ok else "—",
                        detail="Stop R$150k/mês (estimado via DV01)" if ok else "Exposure ALBATROZ não disponível")
        ok = manifest.get("stop_ok", False)
        has_pnl = manifest.get("stop_has_pnl", False)
        pms = manifest.get("stop_pms", [])
        pms_pnl = manifest.get("stop_pms_pnl", [])
        if not ok:
            return dict(status="missing", date="—", detail="Stop monitor não disponível")
        n_pms = len(pms)
        n_pnl = len(pms_pnl)
        detail = f"{n_pms} PMs · {n_pnl} com PnL MTD"
        if pms_pnl:
            detail += f" ({', '.join(pms_pnl)})"
        return dict(status="ok" if has_pnl else "stale", date=DATA_STR_,
                    detail=detail + ("" if has_pnl else " · PnL zero/ausente"))

    _SRC_DEFS = [
        ("PA / PnL",          ["MACRO","QUANT","EVOLUTION","MACRO_Q","ALBATROZ"], _pa_item),
        ("VaR / Stress",      ["MACRO","QUANT","EVOLUTION","MACRO_Q","ALBATROZ"], _var_item),
        ("Exposição",         ["MACRO","QUANT","ALBATROZ"],                       _expo_item),
        ("Single-Name",       ["QUANT","EVOLUTION"],                              _sn_item),
        ("Distribuição 252d", ["MACRO","EVOLUTION"],                              _dist_item),
        ("Stop / Budget",     ["MACRO","ALBATROZ"],                               _stop_item),
    ]

    all_issues   = []
    n_known_gaps = 0
    for src_label, funds, fn in _SRC_DEFS:
        for short in FUND_ORDER:
            if short not in funds:
                continue
            it = fn(short)
            st = it["status"]
            problem = st in ("missing", "stale")  # D-1 is always a warning
            if st == "na":
                n_known_gaps += 1
            elif problem:
                all_issues.append((short, src_label, st == "stale"))
            items.append({
                "fund": short,
                "source": src_label,
                "status": st,
                "date": it["date"],
                "detail": it["detail"],
                "problem": problem,
            })

    # ── Render detail table ───────────────────────────────────────────────────
    _ST_COLOR = {"ok": "#4ade80", "stale": "#facc15", "missing": "#f87171", "na": "#94a3b8"}
    _ST_LABEL = {"ok": "TODAY", "stale": "D-1", "missing": "MISSING", "na": "N/A"}

    detail_rows = ""
    for it in items:
        st   = it["status"]
        col  = _ST_COLOR[st]
        lbl  = _ST_LABEL[st]
        prob = "1" if it["problem"] else "0"
        bg   = 'background:rgba(248,113,113,0.05)' if st == "missing" else (
               'background:rgba(250,204,21,0.04)' if st == "stale" else "")
        detail_rows += (
            f'<tr data-problem="{prob}" style="{bg}">'
            f'<td style="padding:5px 12px;font-weight:600;white-space:nowrap">'
            f'{FUND_LABELS.get(it["fund"], it["fund"])}</td>'
            f'<td style="padding:5px 12px;color:var(--muted)">{it["source"]}</td>'
            f'<td style="padding:5px 10px;text-align:center">'
            f'<span style="color:{col};font-weight:700;font-size:11px">{lbl}</span></td>'
            f'<td style="padding:5px 12px;font-family:var(--mono);font-size:11px;color:var(--muted)">'
            f'{it["date"]}</td>'
            f'<td style="padding:5px 12px;font-size:11.5px">{it["detail"]}</td>'
            f'</tr>'
        )

    # ── Sanity checks table ───────────────────────────────────────────────────
    sanity_rows = ""
    for td, cfg in ALL_FUNDS.items():
        short = cfg["short"]
        s = series_map.get(td)
        if s is None or s.empty: continue
        s_avail = s[s["VAL_DATE"] <= DATA_]
        if s_avail.empty: continue
        var_today = abs(s_avail.iloc[-1]["var_pct"])
        vmin, vmax = s["var_pct"].abs().min(), s["var_pct"].abs().max()
        pct = (var_today - vmin) / (vmax - vmin) * 100 if vmax != vmin else 0
        soft, hard = cfg.get("var_soft", 999), cfg.get("var_hard", 999)
        if var_today >= hard or pct >= 90:
            col, tag = "#f87171", "⚠ ACIMA DO LIMITE"
        elif var_today >= soft or pct >= 70:
            col, tag = "#facc15", "alerta"
        else:
            col, tag = "#4ade80", "normal"
        sanity_rows += (
            f'<tr><td style="padding:5px 12px;white-space:nowrap">{FUND_LABELS[short]}</td>'
            f'<td style="padding:5px 12px">VaR vs. mandato</td>'
            f'<td style="text-align:center;padding:5px 8px">'
            f'<span style="color:{col};font-weight:700;font-size:11px">{tag}</span></td>'
            f'<td style="padding:5px 12px;color:var(--muted);font-size:11.5px">'
            f'VaR {var_today:.2f}% · soft {soft:.2f}% · hard {hard:.2f}% · {pct:.0f}° pct 12M</td></tr>'
        )
    if df_pa is not None and not df_pa.empty and df_pa_daily is not None and not df_pa_daily.empty:
        for short in FUND_ORDER:
            pa_key = _PA_KEY.get(short)
            if pa_key is None: continue
            sub_today = df_pa[df_pa["FUNDO"] == pa_key]
            dia_val = float(sub_today["dia_bps"].sum()) if not sub_today.empty else 0.0
            hist = df_pa_daily[(df_pa_daily["FUNDO"] == pa_key) & (df_pa_daily["DATE"] < DATA_)]
            if hist.empty: continue
            sigma = float(hist.groupby("DATE")["dia_bps"].sum().std())
            if sigma < 0.1: continue
            z = abs(dia_val) / sigma
            col = "#4ade80" if z < 1.5 else "#facc15" if z < 2.5 else "#f87171"
            tag = f"z={z:.1f}"
            sanity_rows += (
                f'<tr><td style="padding:5px 12px;white-space:nowrap">{FUND_LABELS.get(short,short)}</td>'
                f'<td style="padding:5px 12px">DIA PnL vs. σ histórico</td>'
                f'<td style="text-align:center;padding:5px 8px">'
                f'<span style="color:{col};font-weight:700;font-size:11px">{tag}</span></td>'
                f'<td style="padding:5px 12px;color:var(--muted);font-size:11.5px">'
                f'DIA {dia_val:+.1f} bps · σ={sigma:.1f} bps</td></tr>'
            )

    # ── Full HTML ─────────────────────────────────────────────────────────────
    full_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Qualidade de Dados</span>
        <span class="card-sub">— disponibilidade e sanidade · {DATA_STR_}</span>
      </div>

      <div style="display:flex;align-items:center;gap:16px;margin-bottom:14px;flex-wrap:wrap">
        <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">
          Filtro:
        </div>
        <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
          <input type="radio" name="dq_filter" value="all" checked
                 onchange="dqFilter('all')" style="accent-color:#3b82f6"> Todos
        </label>
        <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
          <input type="radio" name="dq_filter" value="problems"
                 onchange="dqFilter('problems')" style="accent-color:#f87171"> Só problemas
        </label>
        <div style="margin-left:auto;font-size:10px;color:var(--muted)">
          <span style="color:#4ade80;font-weight:700">TODAY</span> dados de hoje &nbsp;·&nbsp;
          <span style="color:#facc15;font-weight:700">D-1</span> fallback &nbsp;·&nbsp;
          <span style="color:#f87171;font-weight:700">MISSING</span> indisponível &nbsp;·&nbsp;
          <span style="color:#94a3b8;font-weight:700">N/A</span> lacuna conhecida
        </div>
      </div>

      <div style="overflow-x:auto;margin-bottom:24px">
        <table id="dq-detail-table" style="border-collapse:collapse;width:100%;min-width:600px">
          <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Fundo</th>
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Fonte</th>
            <th style="text-align:center;padding:6px 10px;color:var(--muted);font-size:11px;text-transform:uppercase">Status</th>
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Data</th>
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Detalhe</th>
          </tr></thead>
          <tbody id="dq-detail-body">{detail_rows}</tbody>
        </table>
      </div>

      {'<div style="border-top:1px solid var(--border);padding-top:16px"><div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Verificações de Sanidade</div><table style="border-collapse:collapse;width:100%"><thead><tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:5px 12px;color:var(--muted);font-size:11px">Fundo</th><th style="text-align:left;padding:5px 12px;color:var(--muted);font-size:11px">Verificação</th><th style="text-align:center;padding:5px 8px;color:var(--muted);font-size:11px">Status</th><th style="text-align:left;padding:5px 12px;color:var(--muted);font-size:11px">Detalhe</th></tr></thead><tbody>' + sanity_rows + '</tbody></table></div>' if sanity_rows else ''}
    </section>
    <script>
    window.dqFilter = function(val) {{
      var rows = document.querySelectorAll('#dq-detail-body tr');
      rows.forEach(function(r) {{
        if (val === 'all') r.style.display = '';
        else r.style.display = (r.dataset.problem === '1') ? '' : 'none';
      }});
    }};
    </script>"""

    # ── Compact alert for Summary page ────────────────────────────────────────
    n_missing = sum(1 for _, _, stale in all_issues if not stale)
    n_stale   = sum(1 for _, _, stale in all_issues if stale)
    gaps_note = (f' · <span style="color:#94a3b8">{n_known_gaps} lacuna(s) conhecida(s)</span>'
                 if n_known_gaps > 0 else "")
    if n_missing == 0 and n_stale == 0:
        status_dot = '<span style="color:#4ade80;font-size:16px">●</span>'
        status_txt = f'<span style="color:#4ade80">Todos os dados disponíveis para hoje</span>{gaps_note}'
    elif n_missing == 0:
        status_dot = '<span style="color:#facc15;font-size:16px">●</span>'
        status_txt = f'<span style="color:#facc15">{n_stale} fonte(s) usando dados de D-1</span>{gaps_note}'
    else:
        status_dot = '<span style="color:#f87171;font-size:16px">●</span>'
        status_txt = f'<span style="color:#f87171">{n_missing} fonte(s) indisponível · {n_stale} usando D-1</span>{gaps_note}'

    issue_lines = ""
    for short, source, stale in all_issues:
        c = "#facc15" if stale else "#f87171"
        t = "D-1" if stale else "MISSING"
        issue_lines += (f'<div style="display:flex;gap:8px;align-items:baseline;font-size:11px">'
                        f'<span style="color:{c};min-width:60px;font-weight:700">{t}</span>'
                        f'<span style="color:var(--muted)">{FUND_LABELS.get(short,short)} · {source}</span>'
                        f'</div>')

    compact_html = f"""
    <section class="card" style="margin-top:12px">
      <div class="card-head">
        <span class="card-title">Status dos Dados</span>
        <span class="card-sub">— {DATA_STR_}</span>
      </div>
      <div style="display:flex;gap:12px;align-items:center;margin-bottom:{'8px' if issue_lines else '0'}">
        {status_dot} {status_txt}
        <span style="color:var(--muted);font-size:11px;margin-left:auto">
          ver aba <em>Qualidade</em> para detalhe completo
        </span>
      </div>
      {('<div style="display:grid;grid-template-columns:1fr 1fr;gap:2px 16px;margin-top:4px">' + issue_lines + '</div>') if issue_lines else ''}
    </section>"""

    return full_html, compact_html


def build_html(series_map: dict, stop_hist: dict = None, df_today=None,
               df_expo=None, df_var=None, macro_aum=None,
               df_expo_d1=None, df_var_d1=None,
               df_pnl_prod=None, pm_margem=None,
               df_quant_sn=None, quant_nav=None, quant_legs=None,
               df_evo_sn=None, evo_nav=None, evo_legs=None,
               df_pa=None, cdi=None, df_pa_daily=None,
               df_alb_expo=None, alb_nav=None,
               df_frontier=None,
               df_frontier_ibov=None, df_frontier_smll=None, df_frontier_sectors=None,
               position_changes=None,
               dist_map=None, dist_map_prev=None, dist_actuals=None,
               expo_date_label=None, data_manifest=None) -> str:
    alerts = []
    td_by_short = {cfg["short"]: td for td, cfg in ALL_FUNDS.items()}
    sections = []  # list of (fund_short, report_id, html)

    def util_color(u):
        return "var(--up)" if u < 70 else "var(--warn)" if u < 100 else "var(--down)"

    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td is None:
            continue
        cfg = ALL_FUNDS[td]
        s = series_map.get(td)
        if s is None or s.empty:
            continue
        s_avail = s[s["VAL_DATE"] <= DATA]
        if s_avail.empty:
            continue
        tr = s_avail.iloc[-1]
        var_today    = abs(tr["var_pct"])
        stress_today = abs(tr["stress_pct"])
        var_util     = var_today    / cfg["var_soft"]    * 100
        str_util     = stress_today / cfg["stress_soft"] * 100

        var_abs, str_abs = var_today, stress_today
        var_min_abs,  var_max_abs  = s["var_pct"].abs().min(),    s["var_pct"].abs().max()
        str_min_abs,  str_max_abs  = s["stress_pct"].abs().min(), s["stress_pct"].abs().max()

        var_range_pct    = (var_abs - var_min_abs)  / (var_max_abs - var_min_abs) * 100  if var_max_abs != var_min_abs else 0
        stress_range_pct = (str_abs - str_min_abs)  / (str_max_abs - str_min_abs) * 100  if str_max_abs != str_min_abs else 0

        if var_range_pct >= ALERT_THRESHOLD:
            alerts.append((short, "VaR", var_range_pct, var_today, var_util, ALERT_COMMENTS.get(("var", short), "")))
        if stress_range_pct >= ALERT_THRESHOLD:
            alerts.append((short, "Stress", stress_range_pct, stress_today, str_util, ALERT_COMMENTS.get(("stress", short), "")))

        var_bar    = range_bar_svg(var_abs,  var_min_abs,  var_max_abs,  cfg["var_soft"],    cfg["var_hard"])
        stress_bar = range_bar_svg(str_abs,  str_min_abs,  str_max_abs,  cfg["stress_soft"], cfg["stress_hard"])

        spark_var    = make_sparkline(s.set_index("VAL_DATE")["var_pct"],    "#1a8fd1")
        spark_stress = make_sparkline(s.set_index("VAL_DATE")["stress_pct"], "#f472b6")

        risk_monitor_html = f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Risk Monitor</span>
            <span class="card-sub">— {short}</span>
          </div>
          <table class="metric-table" data-no-sort="1">
            <thead>
              <tr class="col-headers">
                <th>Métrica</th>
                <th style="text-align:right">Valor</th>
                <th>12M Range <span class="tick">▏80%</span></th>
                <th style="text-align:right">Utilização</th>
                <th>60D Trend</th>
              </tr>
            </thead>
            <tbody>
              <tr class="metric-row">
                <td class="metric-name">VaR 95% 1d</td>
                <td class="value-cell mono" style="color:{util_color(var_util)}">{var_today:.2f}%</td>
                <td class="bar-cell">{var_bar}</td>
                <td class="util-cell mono" style="color:{util_color(var_util)}">{var_util:.0f}% soft</td>
                <td class="spark-cell"><img src="data:image/png;base64,{spark_var}" height="38"/></td>
              </tr>
              <tr class="metric-row">
                <td class="metric-name">Stress</td>
                <td class="value-cell mono" style="color:{util_color(str_util)}">{stress_today:.2f}%</td>
                <td class="bar-cell">{stress_bar}</td>
                <td class="util-cell mono" style="color:{util_color(str_util)}">{str_util:.0f}% soft</td>
                <td class="spark-cell"><img src="data:image/png;base64,{spark_stress}" height="38"/></td>
              </tr>
            </tbody>
          </table>
          <div class="bar-legend">
            <span style="color:var(--warn)">─ ─</span> soft &nbsp;
            <span style="color:var(--down)">─ ─</span> hard &nbsp;
            <span style="color:#fb923c">▏</span> 80° pct (alerta)
          </div>
        </section>"""
        sections.append((short, "risk-monitor", risk_monitor_html))

    # Build alerts section
    # ── PA contribution alerts — filter by |dia_bps|, sorted by contribution ──
    PA_ALERT_MIN_BPS   = 5.0   # minimum absolute contribution to show
    PA_ALERT_HIGH_BPS  = 15.0  # threshold for red (large) vs yellow (medium)
    _PA_EXCL_LIVROS    = {"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}
    _PA_EXCL_CLASSES   = {"Caixa", "Custos"}

    pa_alert_items = ""
    if df_pa is not None and not df_pa.empty:
        try:
            _pa_filt = df_pa[
                ~df_pa["LIVRO"].isin(_PA_EXCL_LIVROS) &
                ~df_pa["CLASSE"].isin(_PA_EXCL_CLASSES) &
                (df_pa["dia_bps"].abs() >= PA_ALERT_MIN_BPS)
            ].copy()

            # z-score enrichment (best-effort — requires df_pa_daily)
            _zscore_map = {}
            if df_pa_daily is not None and not df_pa_daily.empty:
                _today = pd.Timestamp(DATA_STR)
                _hist  = df_pa_daily[df_pa_daily["DATE"] < _today]
                _sigma = (_hist.groupby(["FUNDO","LIVRO","PRODUCT"])["dia_bps"]
                               .std().reset_index().rename(columns={"dia_bps":"sigma"}))
                for r in _sigma.itertuples(index=False):
                    _zscore_map[(r.FUNDO, r.LIVRO, r.PRODUCT)] = float(r.sigma) if r.sigma > 0 else None

            _pa_filt = _pa_filt.sort_values("dia_bps", key=abs, ascending=False)
            max_abs  = _pa_filt["dia_bps"].abs().max() if not _pa_filt.empty else 1.0

            for r in _pa_filt.itertuples(index=False):
                bps        = float(r.dia_bps)
                abs_bps    = abs(bps)
                color      = "var(--up)" if bps > 0 else "var(--down)"
                bg_color   = "#0f2d1a" if bps > 0 else "#2d0f0f"
                border_col = "#22c55e" if bps > 0 else "#f87171"
                livro_disp = _pa_render_name(r.LIVRO)
                fund_disp  = FUND_LABELS.get(r.FUNDO, r.FUNDO)

                sigma = _zscore_map.get((r.FUNDO, r.LIVRO, r.PRODUCT))
                z_txt = f"z = {bps/sigma:+.1f}σ" if sigma else ""

                bar_pct = min(abs_bps / max_abs * 100, 100)
                bar_color = "#22c55e" if bps > 0 else "#f87171"

                pa_alert_items += f"""
                <div style="border:1px solid {border_col};border-radius:6px;padding:12px 16px;
                            background:{bg_color};display:flex;align-items:center;gap:16px">
                  <div style="flex:0 0 auto;text-align:right;min-width:80px">
                    <div style="font-size:28px;font-weight:700;color:{color};
                                font-variant-numeric:tabular-nums;line-height:1">{bps:+.1f}</div>
                    <div style="font-size:10px;color:var(--muted);margin-top:2px">bps</div>
                  </div>
                  <div style="flex:1;min-width:0">
                    <div style="font-size:15px;font-weight:700;color:var(--text);
                                white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{r.PRODUCT}</div>
                    <div style="font-size:11px;color:var(--muted);margin-top:2px">
                      {fund_disp} · {livro_disp}
                      {f'&nbsp;·&nbsp;<span style="color:var(--muted)">{z_txt}</span>' if z_txt else ''}
                    </div>
                    <div style="margin-top:6px;height:4px;border-radius:2px;background:#1e293b">
                      <div style="width:{bar_pct:.1f}%;height:100%;border-radius:2px;
                                  background:{bar_color};opacity:0.8"></div>
                    </div>
                  </div>
                </div>"""
        except Exception as e:
            print(f"  PA alerts failed ({e})")

    alerts_html = ""
    risk_items = ""
    if alerts:
        for fundo, metric, pct, val, util, comment in alerts:
            risk_items += f"""
            <div class="alert-item">
              <div class="alert-header">
                <span class="alert-badge">⚠</span>
                <span class="alert-title">{fundo} — {metric}</span>
                <span class="alert-stats">{val:.2f}% &nbsp;|&nbsp; {pct:.0f}° pct 12m &nbsp;|&nbsp; {util:.0f}% do soft</span>
              </div>
              <div class="alert-body">{comment}</div>
            </div>"""
    if risk_items or pa_alert_items:
        pa_grid = (f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));'
                   f'gap:10px;margin-top:{"12px" if risk_items else "0"}">'
                   + pa_alert_items + '</div>') if pa_alert_items else ""
        pa_header = (f'<div style="font-size:11px;color:var(--muted);text-transform:uppercase;'
                     f'letter-spacing:1px;margin-top:{"16px" if risk_items else "0"};'
                     f'margin-bottom:8px">PA — Contribuições do dia (|contrib| ≥ {PA_ALERT_MIN_BPS:.0f} bps)</div>'
                     if pa_alert_items else "")
        alerts_html = f"""
        <div class="alerts-section">
          <div class="alerts-header">Análise{' — Métricas acima do 80° percentil histórico' if risk_items else ''}</div>
          {risk_items}
          {pa_header}
          {pa_grid}
        </div>"""

    # MACRO-specific sections
    if stop_hist and df_today is not None:
        sections.append(("MACRO", "stop-monitor",
                         build_stop_section(stop_hist, df_today)))
    if df_expo is not None:
        _expo_html = build_exposure_section(df_expo, df_var, macro_aum, df_expo_d1, df_var_d1, df_pnl_prod, pm_margem)
        if expo_date_label:
            _stale_banner = (f'<div style="background:#7c2d12;color:#fca5a5;font-size:11px;padding:4px 12px;'
                             f'border-radius:4px;margin-bottom:6px">⚠ Dados de exposição indisponíveis para '
                             f'{DATA_STR} — exibindo {expo_date_label}</div>')
            _expo_html = _stale_banner + _expo_html
        sections.append(("MACRO", "exposure", _expo_html))

    # Distribution 252d sections (per fund) — combined card with Backward/Forward toggle
    if (dist_map or dist_map_prev) and dist_actuals is not None:
        for fs in ["MACRO", "EVOLUTION"]:
            html_sect = build_distribution_card(fs, dist_map or {}, dist_map_prev or {}, dist_actuals)
            if html_sect:
                sections.append((fs, "distribution", html_sect))

    # Single-name L/S — inline full-detail section per fund (no modal)
    sn_quant = build_single_names_section(
        "QUANT", "L/S real após decomposição de WIN / BOVA11 / SMAL11",
        df_quant_sn, quant_nav, quant_legs,
    )
    if sn_quant:
        sections.append(("QUANT", "single-name", sn_quant))

    sn_evo = build_single_names_section(
        "EVOLUTION", "look-through BR (QUANT + Evo Strategy + Frontier + Macro)",
        df_evo_sn, evo_nav, evo_legs,
    )
    if sn_evo:
        sections.append(("EVOLUTION", "single-name", sn_evo))

    # Performance Attribution — one card per fund (MACRO, QUANT, EVOLUTION, MACRO_Q, ALBATROZ)
    if df_pa is not None and not df_pa.empty:
        cdi_row = cdi or {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
        for short in FUND_ORDER:
            pa_html = build_pa_section_hier(short, df_pa, cdi_row)
            if pa_html:
                sections.append((short, "performance", pa_html))

    # ALBATROZ — RF exposure card (under the "exposure" report tab)
    if df_alb_expo is not None and not df_alb_expo.empty and alb_nav:
        alb_html = build_albatroz_exposure(df_alb_expo, alb_nav)
        if alb_html:
            sections.append(("ALBATROZ", "exposure", alb_html))

    # ALBATROZ — Risk Budget (150 bps/month stop)
    if df_pa is not None and not df_pa.empty:
        rb_html = build_albatroz_risk_budget(df_pa)
        if rb_html:
            sections.append(("ALBATROZ", "stop-monitor", rb_html))

    # FRONTIER — Long Only position/attribution tab
    if df_frontier is not None and not df_frontier.empty:
        frontier_html = build_frontier_lo_section(df_frontier, DATA_STR)
        if frontier_html:
            sections.append(("FRONTIER", "frontier-lo", frontier_html))
        # Frontier exposure card (active weight vs IBOV/IBOD, By Name/Sector toggle)
        if (df_frontier_ibov is not None and not df_frontier_ibov.empty):
            expo_html = build_frontier_exposure_section(
                df_frontier, df_frontier_ibov, df_frontier_smll, df_frontier_sectors)
            if expo_html:
                sections.append(("FRONTIER", "exposure", expo_html))

    # ── Per-fund Análise sections (outliers / movers / changes filtered by fund) ──
    _ANALISE_MOVERS_EXCLUDE = {
        "LIVRO":  {"Taxas e Custos", "Caixa", "Caixa USD"},
        "CLASSE": {"Custos", "Caixa"},
    }
    _ANALISE_THRESHOLD_PP = 0.30

    def _an_delta_pp(v):
        color = "var(--up)" if v > 0 else "var(--down)"
        sign = "+" if v > 0 else ""
        return f'<span style="color:{color}; font-weight:700">{sign}{v:.2f} pp</span>'

    _an_outliers_by_fund = {}
    if df_pa_daily is not None and not df_pa_daily.empty:
        try:
            _all_out = compute_pa_outliers(df_pa_daily, DATA_STR, z_min=2.0, bps_min=3.0, min_obs=20)
            if not _all_out.empty:
                for pa_key in _all_out["FUNDO"].unique():
                    _an_outliers_by_fund[pa_key] = _all_out[_all_out["FUNDO"] == pa_key]
        except Exception as e:
            print(f"  per-fund outliers failed ({e})")

    def _an_outliers_card(short):
        pa_key = _FUND_PA_KEY.get(short)
        sub = _an_outliers_by_fund.get(pa_key)
        if sub is None or sub.empty:
            body = '<div class="comment-empty">— sem eventos significativos (|z| ≥ 2σ · |contrib| ≥ 3 bps)</div>'
        else:
            items = ""
            for r in sub.itertuples(index=False):
                color = "var(--up)" if r.today_bps > 0 else "var(--down)"
                items += (
                    '<li>'
                    f'<span class="mono" style="font-weight:700">{r.PRODUCT}</span> '
                    f'<span class="mono" style="color:{color}">{r.today_bps:+.1f} bps</span> '
                    f'<span class="mono" style="color:var(--muted)">({r.z:+.1f}σ)</span>'
                    f' &nbsp;·&nbsp; <span style="color:var(--muted)">{_pa_render_name(r.LIVRO)}</span>'
                    '</li>'
                )
            body = f'<ul class="comment-list">{items}</ul>'
        return f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Outliers do dia</span>
            <span class="card-sub">— {FUND_LABELS.get(short, short)} · |z| ≥ 2σ (vs. 90d) · |contrib| ≥ 3 bps</span>
          </div>
          {body}
        </section>"""

    def _an_movers_rows(short, group_col):
        pa_key = _FUND_PA_KEY.get(short)
        if not pa_key or df_pa is None or df_pa.empty:
            return ""
        sub = df_pa[df_pa["FUNDO"] == pa_key]
        sub = sub[~sub[group_col].isin(_ANALISE_MOVERS_EXCLUDE.get(group_col, set()))]
        by = sub.groupby(group_col)["dia_bps"].sum()
        by = by[by.abs() > 0.5]
        pos = by[by > 0].sort_values(ascending=False).head(5)
        neg = by[by < 0].sort_values().head(5)
        render = _pa_render_name if group_col == "LIVRO" else (lambda s: s)
        def _fmt(color, items_s):
            if not len(items_s):
                return '<li class="comment-empty">— nada material</li>'
            return "".join(
                '<li>'
                f'<span class="mono" style="color:{color}; font-weight:600">{render(l)}</span> '
                f'<span class="mono">{"+" if v >= 0 else ""}{v/100:.2f}%</span>'
                '</li>'
                for l, v in items_s.items()
            )
        return f"""
        <div class="mov-split">
          <div>
            <div class="mov-col-title">Contribuintes</div>
            <ul class="comment-list">{_fmt("var(--up)", pos)}</ul>
          </div>
          <div>
            <div class="mov-col-title">Detratores</div>
            <ul class="comment-list">{_fmt("var(--down)", neg)}</ul>
          </div>
        </div>"""

    def _an_movers_card(short):
        livro_body  = _an_movers_rows(short, "LIVRO")
        classe_body = _an_movers_rows(short, "CLASSE")
        if not (livro_body or classe_body):
            return ""
        return f"""
        <section class="card sum-movers-card">
          <div class="card-head">
            <span class="card-title">Top Movers — DIA</span>
            <span class="card-sub">— {FUND_LABELS.get(short, short)} · alpha do dia (drop &lt; 0.5 bps · Caixa/Custos excluídos)</span>
            <div class="pa-view-toggle sum-tgl">
              <button class="pa-tgl active" data-mov-view="livro"
                      onclick="selectMoversView(this,'livro')">Por Livro</button>
              <button class="pa-tgl" data-mov-view="classe"
                      onclick="selectMoversView(this,'classe')">Por Classe</button>
            </div>
          </div>
          <div class="mov-view" data-mov-view="livro">{livro_body}</div>
          <div class="mov-view" data-mov-view="classe" style="display:none">{classe_body}</div>
        </section>"""

    def _an_changes_card(short):
        if short == "MACRO":
            if df_expo is None or df_expo_d1 is None:
                return ""
            d0 = df_expo.groupby(["pm", "rf"])["pct_nav"].sum().reset_index()
            d1 = df_expo_d1.groupby(["pm", "rf"])["pct_nav"].sum().reset_index().rename(
                columns={"pct_nav": "pct_d1"}
            )
            chg = d0.merge(d1, on=["pm", "rf"], how="outer").fillna(0.0)
            chg["delta"] = chg["pct_nav"] - chg["pct_d1"]
            chg = chg[chg["delta"].abs() >= _ANALISE_THRESHOLD_PP]
            chg = chg.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(10)
            if chg.empty:
                return ""
            rows = "".join(
                '<tr>'
                f'<td class="sum-fund" style="width:70px">{r.pm}</td>'
                f'<td class="mono" style="color:var(--muted)">{r.rf}</td>'
                f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_nav:+.2f}%</td>'
                f'<td class="mono" style="text-align:right">{_an_delta_pp(r.delta)}</td>'
                '</tr>'
                for r in chg.itertuples(index=False)
            )
            return f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Mudanças Significativas</span>
                <span class="card-sub">— MACRO · PM × fator · |Δ| ≥ {_ANALISE_THRESHOLD_PP:.2f} pp</span>
              </div>
              <table class="summary-movers">
                <thead><tr>
                  <th style="text-align:left; width:70px">PM</th>
                  <th style="text-align:left">Fator</th>
                  <th style="text-align:right">D-1</th>
                  <th style="text-align:right">D-0</th>
                  <th style="text-align:right">Δ</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </section>"""
        else:
            if not position_changes:
                return ""
            df_chg = position_changes.get(short)
            if df_chg is None or df_chg.empty:
                return ""
            big = df_chg[df_chg["delta"].abs() >= _ANALISE_THRESHOLD_PP]
            big = big.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(10)
            if big.empty:
                return ""
            rows = "".join(
                '<tr>'
                f'<td class="mono" style="color:var(--muted)">{r.factor}</td>'
                f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_d0:+.2f}%</td>'
                f'<td class="mono" style="text-align:right">{_an_delta_pp(r.delta)}</td>'
                '</tr>'
                for r in big.itertuples(index=False)
            )
            return f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Mudanças Significativas</span>
                <span class="card-sub">— {FUND_LABELS.get(short, short)} · por fator · |Δ| ≥ {_ANALISE_THRESHOLD_PP:.2f} pp</span>
              </div>
              <table class="summary-movers">
                <thead><tr>
                  <th style="text-align:left">Fator</th>
                  <th style="text-align:right">D-1</th>
                  <th style="text-align:right">D-0</th>
                  <th style="text-align:right">Δ</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </section>"""

    for short in FUND_ORDER:
        for card in (_an_outliers_card(short), _an_movers_card(short), _an_changes_card(short)):
            if card:
                sections.append((short, "analise", card))

    # Which fund×report combinations exist — used to enable/disable tabs and handle empty states
    available_pairs = {(f, r) for f, r, _ in sections}
    funds_with_data = sorted({f for f, _ in available_pairs}, key=FUND_ORDER.index)
    reports_with_data = [rid for rid, _ in REPORTS if any(rid == r for _, r in available_pairs)]

    # Sort sections so Per-Fund view shows reports in canonical REPORTS order
    _REPORT_IDX = {rid: i for i, (rid, _) in enumerate(REPORTS)}
    sections.sort(key=lambda x: (
        FUND_ORDER.index(x[0]) if x[0] in FUND_ORDER else 99,
        _REPORT_IDX.get(x[1], 99)
    ))

    # Render all sections wrapped for data-attribute filtering
    sections_html = "".join(
        f'<div id="sec-{f}-{r}" class="section-wrap" data-fund="{f}" data-report="{r}">{h}</div>'
        for f, r, h in sections
    )

    # ── Summary (cross-fund landing) ──────────────────────────────────────
    def _sum_bp_cell(bps: float) -> str:
        pct = bps / 100.0
        if abs(pct) < 0.005:
            return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
        color = "var(--up)" if bps >= 0 else "var(--down)"
        return f'<td class="mono" style="color:{color}; text-align:right">{pct:+.2f}%</td>'

    def _sum_util_cell(util):
        if util is None:
            return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
        color = "var(--up)" if util < 70 else "var(--warn)" if util < 100 else "var(--down)"
        return f'<td class="mono" style="color:{color}; text-align:right; font-weight:600">{util:.0f}%</td>'

    def _sum_var_cell(v):
        if v is None:
            return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
        return f'<td class="mono" style="color:var(--text); text-align:right">{v:.2f}%</td>'

    def _sum_dvar_cell(dvar):
        if dvar is None:
            return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
        dv_bps = dvar * 100  # pp → bps
        if abs(dv_bps) < 0.5:
            return '<td class="mono" style="color:var(--muted); text-align:right">flat</td>'
        color = "var(--down)" if dv_bps > 0 else "var(--up)"  # VaR up = more risk
        sign = "+" if dv_bps > 0 else ""
        return f'<td class="mono" style="color:{color}; text-align:right">{sign}{dv_bps:.0f} bps</td>'

    # Gather per-fund summary data
    summary_rows_html = ""
    for short in FUND_ORDER:
        td      = td_by_short.get(short)
        pa_key  = _FUND_PA_KEY.get(short)

        if pa_key and df_pa is not None and not df_pa.empty:
            sub = df_pa[df_pa["FUNDO"] == pa_key]
            a_dia = float(sub["dia_bps"].sum()) if not sub.empty else 0.0
            a_mtd = float(sub["mtd_bps"].sum()) if not sub.empty else 0.0
            a_ytd = float(sub["ytd_bps"].sum()) if not sub.empty else 0.0
            a_m12 = float(sub["m12_bps"].sum()) if not sub.empty else 0.0
        else:
            a_dia = a_mtd = a_ytd = a_m12 = 0.0

        var_today = var_util = dvar = None
        if td and td in ALL_FUNDS and series_map and td in series_map:
            s = series_map[td]
            s_avail = s[s["VAL_DATE"] <= DATA]
            if not s_avail.empty:
                var_today = abs(s_avail.iloc[-1]["var_pct"])
                cfg = ALL_FUNDS[td]
                var_util = var_today / cfg["var_soft"] * 100
                prev = s_avail.iloc[:-1]
                if not prev.empty:
                    dvar = var_today - abs(prev.iloc[-1]["var_pct"])

        stop_util = None
        if short == "MACRO" and pm_margem and stop_hist:
            utils = []
            cur_mes = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
            for pm, margem in pm_margem.items():
                hist = stop_hist.get(pm)
                if hist is None: continue
                cur_row = hist[hist["mes"] == cur_mes]
                if cur_row.empty: continue
                budget = float(cur_row["budget_abs"].iloc[0])
                if budget <= 0: continue
                consumed = budget - margem
                utils.append(max(consumed, 0) / budget * 100)
            if utils: stop_util = max(utils)
        elif short == "ALBATROZ":
            stop_util = (abs(a_mtd) / ALBATROZ_STOP_BPS * 100) if a_mtd < 0 else 0.0

        worst = max(x for x in (var_util, stop_util, 0) if x is not None)
        if worst >= 100:   status = "🔴"
        elif worst >= 70:  status = "🟡"
        else:              status = "🟢"

        summary_rows_html += (
            "<tr>"
            f'<td class="sum-status">{status}</td>'
            f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
            + _sum_bp_cell(a_dia) + _sum_bp_cell(a_mtd) + _sum_bp_cell(a_ytd) + _sum_bp_cell(a_m12)
            + _sum_var_cell(var_today) + _sum_util_cell(var_util) + _sum_util_cell(stop_util)
            + _sum_dvar_cell(dvar)
            + "</tr>"
        )

    fund_grid_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Status consolidado</span>
        <span class="card-sub">— {DATA_STR} · alpha vs. CDI · utilização de VaR e stop mensal</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="width:60px; text-align:center">Status</th>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">DIA</th>
          <th style="text-align:right">MTD</th>
          <th style="text-align:right">YTD</th>
          <th style="text-align:right">12M</th>
          <th style="text-align:right">VaR</th>
          <th style="text-align:right">Util VaR</th>
          <th style="text-align:right">Util Stop</th>
          <th style="text-align:right">Δ VaR D-1</th>
        </tr></thead>
        <tbody>{summary_rows_html}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:12px">
        <span style="color:var(--up)">🟢</span> util &lt; 70% &nbsp;·&nbsp;
        <span style="color:var(--warn)">🟡</span> 70–100% &nbsp;·&nbsp;
        <span style="color:var(--down)">🔴</span> ≥ 100% &nbsp;·&nbsp;
        <span style="color:var(--muted)">Status = pior entre utilização de VaR e stop mensal</span>
      </div>
    </section>"""

    # Top movers — DIA, with toggle Por Livro / Por Classe
    # Caixa / Custos livros e classes são operacionais (não-direcionais) — excluídos.
    _MOVERS_EXCLUDE = {
        "LIVRO":  {"Taxas e Custos", "Caixa", "Caixa USD"},
        "CLASSE": {"Custos", "Caixa"},
    }

    def _movers_rows(group_col: str) -> str:
        if df_pa is None or df_pa.empty:
            return ""
        exclude = _MOVERS_EXCLUDE.get(group_col, set())
        rows_html = ""
        for short in FUND_ORDER:
            pa_key = _FUND_PA_KEY.get(short)
            if not pa_key: continue
            sub = df_pa[df_pa["FUNDO"] == pa_key]
            if sub.empty: continue
            sub = sub[~sub[group_col].isin(exclude)]
            by = sub.groupby(group_col)["dia_bps"].sum()
            by = by[by.abs() > 0.5]
            pos = by[by > 0].sort_values(ascending=False).head(3)
            neg = by[by < 0].sort_values().head(3)
            render = _pa_render_name if group_col == "LIVRO" else (lambda s: s)
            pos_txt = " · ".join(
                f'<span style="color:var(--up)">{render(l)} +{v/100:.2f}%</span>'
                for l, v in pos.items()
            ) or '<span style="color:var(--muted)">—</span>'
            neg_txt = " · ".join(
                f'<span style="color:var(--down)">{render(l)} {v/100:.2f}%</span>'
                for l, v in neg.items()
            ) or '<span style="color:var(--muted)">—</span>'
            rows_html += (
                "<tr>"
                f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
                f'<td class="sum-movers">{pos_txt}</td>'
                f'<td class="sum-movers">{neg_txt}</td>'
                "</tr>"
            )
        return rows_html

    livro_rows  = _movers_rows("LIVRO")
    classe_rows = _movers_rows("CLASSE")

    def _movers_table(view_id, rows, active):
        style = "" if active else ' style="display:none"'
        return f"""
        <table class="summary-movers mov-view" data-mov-view="{view_id}"{style}>
          <thead><tr>
            <th style="text-align:left; width:120px">Fundo</th>
            <th style="text-align:left">Contribuintes</th>
            <th style="text-align:left">Detratores</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>"""

    movers_html = (f"""
    <section class="card sum-movers-card">
      <div class="card-head">
        <span class="card-title">Top Movers — DIA</span>
        <span class="card-sub">— alpha do dia (drop &lt; 0.5 bps) · Taxas/Custos excluídos</span>
        <div class="pa-view-toggle sum-tgl">
          <button class="pa-tgl active" data-mov-view="livro"
                  onclick="selectMoversView(this,'livro')">Por Livro</button>
          <button class="pa-tgl" data-mov-view="classe"
                  onclick="selectMoversView(this,'classe')">Por Classe</button>
        </div>
      </div>
      {_movers_table("livro",  livro_rows,  True)}
      {_movers_table("classe", classe_rows, False)}
    </section>""") if (livro_rows or classe_rows) else ""

    # Significant position changes — one card with sub-blocks per fund.
    # MACRO uses PM × factor detail (richer data already available).
    # Other funds use factor-level (PRODUCT_CLASS → grouped) from LOTE_PRODUCT_EXPO.
    changes_html = ""
    THRESHOLD_PP = 0.30

    def _delta_pp(v):
        color = "var(--up)" if v > 0 else "var(--down)"
        sign = "+" if v > 0 else ""
        return f'<span style="color:{color}; font-weight:700">{sign}{v:.2f} pp</span>'

    change_blocks = []

    # MACRO — PM × factor
    if df_expo is not None and df_expo_d1 is not None:
        try:
            d0 = df_expo.groupby(["pm", "rf"])["pct_nav"].sum().reset_index()
            d1 = df_expo_d1.groupby(["pm", "rf"])["pct_nav"].sum().reset_index().rename(
                columns={"pct_nav": "pct_d1"}
            )
            chg = d0.merge(d1, on=["pm", "rf"], how="outer").fillna(0.0)
            chg["delta"] = chg["pct_nav"] - chg["pct_d1"]
            chg = chg[chg["delta"].abs() >= THRESHOLD_PP]
            chg = chg.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(8)
            if not chg.empty:
                rows = "".join(
                    f"<tr>"
                    f'<td class="sum-fund" style="width:70px">{r.pm}</td>'
                    f'<td class="mono" style="color:var(--muted)">{r.rf}</td>'
                    f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                    f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_nav:+.2f}%</td>'
                    f'<td class="mono" style="text-align:right">{_delta_pp(r.delta)}</td>'
                    f"</tr>"
                    for r in chg.itertuples(index=False)
                )
                change_blocks.append(f"""
                <div class="comment-fund">
                  <div class="comment-title">Macro · PM × fator</div>
                  <table class="summary-movers">
                    <thead><tr>
                      <th style="text-align:left; width:60px">PM</th>
                      <th style="text-align:left">Fator</th>
                      <th style="text-align:right">D-1</th>
                      <th style="text-align:right">D-0</th>
                      <th style="text-align:right">Δ</th>
                    </tr></thead>
                    <tbody>{rows}</tbody>
                  </table>
                </div>""")
        except Exception as e:
            print(f"  changes (MACRO) block failed ({e})")

    # QUANT / EVOLUTION / MACRO_Q / ALBATROZ — factor-level
    if position_changes:
        for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ"):
            df_chg = position_changes.get(short)
            if df_chg is None or df_chg.empty:
                continue
            big = df_chg[df_chg["delta"].abs() >= THRESHOLD_PP]
            big = big.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(6)
            if big.empty:
                continue
            rows = "".join(
                f"<tr>"
                f'<td class="mono" style="color:var(--muted)">{r.factor}</td>'
                f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_d0:+.2f}%</td>'
                f'<td class="mono" style="text-align:right">{_delta_pp(r.delta)}</td>'
                f"</tr>"
                for r in big.itertuples(index=False)
            )
            change_blocks.append(f"""
            <div class="comment-fund">
              <div class="comment-title">{FUND_LABELS.get(short, short)} · por fator</div>
              <table class="summary-movers">
                <thead><tr>
                  <th style="text-align:left">Fator</th>
                  <th style="text-align:right">D-1</th>
                  <th style="text-align:right">D-0</th>
                  <th style="text-align:right">Δ</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </div>""")

    if change_blocks:
        changes_html = f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Mudanças Significativas</span>
            <span class="card-sub">— exposição D-0 vs D-1 · |Δ| ≥ {THRESHOLD_PP:.2f} pp · MACRO por PM×fator, outros por fator agregado</span>
          </div>
          <div class="comments-grid">{''.join(change_blocks)}</div>
        </section>"""

    # Comments — outlier detection per product (|z|≥2σ vs 90d + |bps|≥3)
    comments_html = ""
    if df_pa_daily is not None and not df_pa_daily.empty:
        try:
            outliers = compute_pa_outliers(df_pa_daily, DATA_STR, z_min=2.0, bps_min=3.0, min_obs=20)
            body_chunks = []
            for short in FUND_ORDER:
                pa_key = _FUND_PA_KEY.get(short)
                if not pa_key: continue
                sub = outliers[outliers["FUNDO"] == pa_key] if not outliers.empty else outliers
                label = FUND_LABELS.get(short, short)
                if sub is None or sub.empty:
                    body_chunks.append(
                        f'<div class="comment-fund">'
                        f'<div class="comment-title">{label}</div>'
                        f'<div class="comment-empty">— sem eventos significativos</div>'
                        f'</div>'
                    )
                else:
                    items = ""
                    for r in sub.itertuples(index=False):
                        color = "var(--up)" if r.today_bps > 0 else "var(--down)"
                        livro_disp = _pa_render_name(r.LIVRO)
                        items += (
                            f'<li>'
                            f'<span class="mono" style="font-weight:700">{r.PRODUCT}</span> '
                            f'<span class="mono" style="color:{color}">{r.today_bps:+.1f} bps</span> '
                            f'<span class="mono" style="color:var(--muted)">({r.z:+.1f}σ)</span> '
                            f'&nbsp;·&nbsp; <span style="color:var(--muted)">{livro_disp}</span>'
                            f'</li>'
                        )
                    body_chunks.append(
                        f'<div class="comment-fund">'
                        f'<div class="comment-title">{label}</div>'
                        f'<ul class="comment-list">{items}</ul>'
                        f'</div>'
                    )
            comments_html = f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Comments — Outliers do dia</span>
                <span class="card-sub">— produtos com |z| ≥ 2σ (vs. 90d) e |contrib| ≥ 3 bps · ignora Caixa/Custos</span>
              </div>
              <div class="comments-grid">{''.join(body_chunks)}</div>
            </section>"""
        except Exception as e:
            print(f"  comments block failed ({e})")

    # Data Quality section
    dq_full_html, dq_compact_html = build_data_quality_section(
        data_manifest, series_map, df_pa, df_pa_daily
    )

    summary_html = f"""
    <div class="section-wrap" data-view="summary">
      {fund_grid_html}
      {alerts_html}
      {comments_html}
      {movers_html}
      {changes_html}
      {dq_compact_html}
    </div>"""
    quality_html = f'<div class="section-wrap" data-view="quality">{dq_full_html}</div>'
    # Alerts relocated into Summary view — clear the global section so it doesn't duplicate
    alerts_html = ""

    # (Análise helpers and per-fund loop were relocated above `sections_html`; see earlier block.)

    # Mode switcher + sub-tabs
    mode_tabs_html = (
        '<button class="mode-tab" data-mode="summary" onclick="selectMode(\'summary\')">Summary</button>'
        '<button class="mode-tab" data-mode="fund"    onclick="selectMode(\'fund\')">Por Fundo</button>'
        '<button class="mode-tab" data-mode="report"  onclick="selectMode(\'report\')">Por Report</button>'
        '<button class="mode-tab" data-mode="quality" onclick="selectMode(\'quality\')" style="opacity:0.55;font-size:11px">Qualidade</button>'
    )
    report_subtabs_html = "".join(
        f'<button class="tab" data-target="{rid}" onclick="selectReport(\'{rid}\')">{label}</button>'
        for rid, label in REPORTS if rid in reports_with_data
    )
    fund_subtabs_html = "".join(
        f'<button class="tab" data-target="{s}" onclick="selectFund(\'{s}\')">{s}</button>'
        for s in funds_with_data
    )
    # JS constant: which reports exist per fund (for the jump bar)
    fund_reports_js = json.dumps({
        f: [rid for rid, _ in REPORTS if (f, rid) in available_pairs]
        for f in FUND_ORDER
    })
    report_labels_js = json.dumps({rid: label for rid, label in REPORTS})

    # Por Report mode removed — no per-report fund switcher needed

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<title>Risk Monitor — {DATA_STR}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg:#0b0d10;
    --bg-2:#111418;
    --panel:#14181d;
    --panel-2:#181d24;
    --line:#232a33;
    --line-2:#2d3540;
    --text:#e7ecf2;
    --muted:#8892a0;
    --accent:#0071BB;
    --accent-2:#1a8fd1;
    --accent-deep:#183C80;
    --up:#26d07c;
    --down:#ff5a6a;
    --warn:#f5c451;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{
    background: var(--bg); color: var(--text);
    font-family: 'Inter', system-ui, sans-serif;
    -webkit-font-smoothing: antialiased;
  }}
  h1, h2, .card-title, .modal-title, .brand h1 {{
    font-family: 'Gadugi', 'Inter', system-ui, sans-serif;
  }}
  body {{
    min-height:100vh;
    background:
      radial-gradient(1200px 600px at 10% -10%, rgba(0,113,187,.06), transparent 60%),
      radial-gradient(900px 500px at 110% 10%, rgba(26,143,209,.04), transparent 60%),
      var(--bg);
  }}
  .mono {{ font-family:'JetBrains Mono', ui-monospace, monospace; font-variant-numeric: tabular-nums; }}

  /* Header */
  header {{
    position:sticky; top:0; z-index:50;
    backdrop-filter:blur(14px);
    background:rgba(11,13,16,.78);
    border-bottom:1px solid var(--line);
  }}
  .hwrap {{
    max-width:1280px; margin:0 auto; padding:14px 22px;
    display:flex; align-items:center; gap:22px; flex-wrap:wrap;
  }}
  .brand {{ display:flex; align-items:center; gap:12px; }}
  .logo {{
    width:38px; height:38px; border-radius:10px;
    background:linear-gradient(135deg,#0071BB 0%,#183C80 100%);
    display:grid; place-items:center; overflow:hidden; flex-shrink:0;
    box-shadow:0 0 0 1px rgba(0,113,187,.35), 0 8px 24px -8px rgba(0,113,187,.4);
  }}
  .logo svg {{ width:26px; height:26px; }}
  .brand h1 {{ font-size:16px; margin:0; letter-spacing:.3px; font-weight:700; }}
  .brand p {{ margin:0; font-size:11px; color:var(--muted); letter-spacing:.18em; text-transform:uppercase; }}

  .tabs, .sub-tabs {{
    display:flex; gap:4px;
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:4px;
  }}
  .tab, .mode-tab {{
    padding:7px 14px; font-size:12.5px; font-weight:600;
    color:var(--muted); background:transparent; border:0; border-radius:9px;
    cursor:pointer; transition:all .18s ease; letter-spacing:.05em;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }}
  .tab:hover, .mode-tab:hover {{ color:var(--text); }}
  .tab.active, .mode-tab.active {{ color:#fff; background:linear-gradient(180deg, var(--accent-2), var(--accent)); }}
  .mode-switcher {{
    display:flex; gap:4px;
    background:var(--panel-2); border:1px solid var(--line);
    border-radius:12px; padding:4px;
  }}
  .sub-tabs {{ display:none; }}
  .sub-tabs.active {{ display:flex; }}
  .navrow {{
    max-width:1280px; margin:0 auto; padding:8px 22px;
    display:flex; align-items:center; gap:14px; flex-wrap:wrap;
  }}
  .navrow-label {{
    font-size:10px; color:var(--muted); letter-spacing:.18em; text-transform:uppercase;
  }}
  /* Report jump bar — shown below fund tabs in Por Fundo mode */
  #report-jump-bar {{
    display:none; gap:4px; flex-wrap:wrap; align-items:center;
    padding:6px 22px; max-width:1280px; margin:0 auto;
    border-top:1px solid rgba(255,255,255,0.04);
  }}
  body[data-mode="fund"] #report-jump-bar {{ display:flex; }}
  .jump-btn {{
    padding:4px 11px; font-size:11px; font-weight:600;
    color:var(--muted); background:transparent; border:1px solid var(--line);
    border-radius:7px; cursor:pointer; transition:all .15s ease;
    font-family:'Gadugi','Inter',system-ui,sans-serif; letter-spacing:.03em;
  }}
  .jump-btn:hover {{ color:var(--text); border-color:rgba(255,255,255,0.18); }}
  .jump-btn.active-jump {{ color:var(--accent-2); border-color:var(--accent-2); }}

  .controls {{ margin-left:auto; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
  .ctrl-group {{
    display:flex; align-items:center; gap:8px;
    background:var(--panel); border:1px solid var(--line);
    border-radius:10px; padding:6px 10px;
  }}
  .ctrl-group label {{
    font-size:10px; color:var(--muted); letter-spacing:.14em; text-transform:uppercase;
  }}
  .ctrl-group input[type=date] {{
    background:transparent; border:0; color:var(--text);
    font-family:'JetBrains Mono', monospace; font-size:12px;
    color-scheme:dark; outline:none;
  }}
  .btn-primary {{
    padding:8px 14px; border-radius:10px; border:1px solid var(--accent);
    background:linear-gradient(180deg, var(--accent-2), var(--accent));
    color:#fff; font-weight:600; font-size:11.5px; cursor:pointer;
    letter-spacing:.05em; text-transform:uppercase;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }}
  .btn-primary:hover {{ filter:brightness(1.08); }}
  .btn-accent {{
    padding:8px 14px; border-radius:10px; border:1px solid var(--accent);
    background:rgba(0,113,187,.12); color:var(--accent-2);
    font-weight:600; font-size:12px; cursor:pointer; letter-spacing:.04em;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }}
  .btn-accent:hover {{ background:rgba(0,113,187,.22); color:#fff; }}
  .btn-pdf {{
    padding:7px 12px; border-radius:9px; border:1px solid var(--line-2);
    background:var(--panel-2); color:var(--muted);
    font-weight:600; font-size:11px; cursor:pointer; letter-spacing:.04em;
    margin-left:6px;
  }}
  .btn-pdf:hover {{ border-color:var(--accent); color:var(--text); background:rgba(0,113,187,.10); }}

  /* Print / PDF export — remap CSS variables to a light palette so all
     inline `color:var(--up)` / backgrounds switch to paper-friendly tones. */
  @media print {{
    :root {{
      --bg:        #ffffff;
      --bg-2:      #ffffff;
      --panel:     #ffffff;
      --panel-2:   #f2f4f7;
      --line:      #d0d5dd;
      --line-2:    #b0b5c0;
      --text:      #111111;
      --muted:     #555555;
      --accent:    #003d5c;
      --accent-2:  #004a70;
      --up:        #0e7a32;
      --down:      #a8001a;
      --warn:      #8a6500;
    }}
    @page {{ size: A4 landscape; margin: 10mm 8mm; }}
    html, body {{
      background: #ffffff !important;
      color: #111111 !important;
      font-size: 11pt;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}
    .no-print, header .controls, header .mode-switcher,
    .navrow, .sub-tabs, #empty-state,
    .sn-switcher, .report-fund-switcher {{
      display: none !important;
    }}
    header {{
      padding: 4px 0 6px; border-bottom: 1px solid #000; margin-bottom: 6px;
      background: #ffffff !important;
    }}
    header h1 {{ font-size: 14pt !important; color: #000 !important; }}
    header p  {{ font-size: 9pt  !important; color: #333 !important; }}
    header .logo svg path {{ fill: #000 !important; }}
    main {{ padding: 0 !important; max-width: none !important; margin: 0 !important; }}

    .card, section.card {{
      background: #ffffff !important;
      border: 1px solid #999 !important;
      box-shadow: none !important;
      page-break-inside: avoid;
      margin-bottom: 8px !important;
      padding: 8px 10px !important;
    }}
    .card-head   {{ padding-bottom: 4px !important; margin-bottom: 6px !important; border-bottom: 1px solid #ddd !important; }}
    .card-title  {{ color: #000 !important; font-size: 11pt !important; letter-spacing: .1em !important; font-weight: 700 !important; }}
    .card-sub    {{ color: #444 !important; font-size: 9pt  !important; }}

    table {{ background: #ffffff !important; }}
    tbody tr {{ background: #ffffff !important; }}
    th {{
      font-size: 8.5pt !important;
      background: #f2f4f7 !important;
      color: #333 !important;
    }}
    td {{ font-size: 10pt !important; }}
    .mono {{ font-family: 'Courier New', monospace; }}

    /* Denser tables for paper */
    .summary-table td, .summary-table th,
    .summary-movers td, .summary-movers th,
    .pa-table td,      .pa-table th {{
      padding: 4px 7px !important;
      font-size: 9.5pt !important;
    }}
    .summary-table td.sum-fund {{ font-size: 10pt !important; color: #000 !important; font-weight: 700; }}

    .alert-item   {{ padding: 6px 8px !important; font-size: 9.5pt !important; background: #fff4d6 !important; border-left: 3px solid #a37500 !important; }}
    .comment-fund {{ padding: 6px 8px !important; font-size: 9.5pt !important; background: #fafbfc !important; border: 1px solid #d0d5dd !important; }}
    .comment-list li {{ font-size: 9pt !important; line-height: 1.5 !important; }}
    .comment-empty   {{ color: #777 !important; }}

    .section-wrap {{ page-break-inside: auto; }}
    .pa-sort-arrow {{ display: none !important; }}

    /* Keep heatmap very subtle so numbers remain readable */
    .pa-table td[style*="background:rgba(38,208"] {{ background: #e8f6ed !important; }}
    .pa-table td[style*="background:rgba(255,90"] {{ background: #fae9ec !important; }}

    .subtitle {{ font-size: 9pt !important; margin: 2px 0 6px !important; color: #333 !important; }}
  }}
  /* print-full: show all section-wraps even if filtered out by mode/fund/report */
  body.print-full .section-wrap {{ display: block !important; }}
  body.print-full .sn-switcher,
  body.print-full .report-fund-switcher {{ display: none !important; }}

  .date-hint {{ font-size:9px; color:var(--muted); margin-left:6px; }}
  .subtitle {{
    max-width:1280px; margin:10px auto 0; padding:0 22px;
    font-size:11px; color:var(--muted);
  }}

  /* Main */
  main {{ max-width:1280px; margin:0 auto; padding:18px 22px 40px; }}
  .section-wrap {{ display:block; }}
  .empty-view {{ padding:28px; text-align:center; color:var(--muted); font-size:12px; }}
  #empty-state {{
    padding:40px 16px; text-align:center; color:var(--muted); font-size:13px;
    border:1px dashed var(--line); border-radius:12px; margin-top:12px;
  }}

  /* legacy placeholder — report mode removed */
  body[data-mode="UNUSED"] #sections-container {{
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 12px; padding: 4px 18px; margin-top: 4px;
  }}
  body[data-mode="UNUSED"] #sections-container .section-wrap > .card {{
    background: transparent; border: 0;
    padding: 14px 0 16px; margin-bottom: 0;
    border-bottom: 1px solid var(--line);
    border-radius: 0;
  }}
  body[data-mode="UNUSED"] #sections-container .section-wrap:last-of-type > .card,
  body[data-mode="UNUSED"] #sections-container .section-wrap > .card:last-child {{
    border-bottom: 0;
  }}

  /* Distribution 252d table */
  .dist-table td, .dist-table th {{ vertical-align:middle; padding:7px 10px; }}
  .dist-table .dist-tag  {{ font-size:9px; font-weight:700; letter-spacing:1px; width:54px; }}
  .dist-table .dist-name {{ font-size:12px; color:var(--text); font-weight:500; }}
  .dist-table .dist-num  {{ font-size:12px; text-align:right; font-variant-numeric: tabular-nums; }}
  .dist-table .metric-row:hover {{ background:var(--panel-2); }}
  .dist-toggle {{ display:inline-flex; gap:2px; background:var(--panel-2); border:1px solid var(--line); border-radius:8px; padding:3px; }}
  .dist-btn {{ padding:5px 12px; font-size:11px; font-weight:600; color:var(--muted); background:transparent; border:0; border-radius:6px; cursor:pointer; letter-spacing:.04em; font-family:'Gadugi','Inter',system-ui,sans-serif; }}
  .dist-btn:hover {{ color:var(--text); }}
  .dist-btn.active {{ color:#fff; background:linear-gradient(180deg,var(--accent-2),var(--accent)); }}
  .dist-view {{ margin-top:10px; }}

  /* CSV export button (injected into each card-head) */
  .btn-csv {{
    margin-left: auto; padding: 4px 10px;
    font-size: 10px; font-weight: 600; letter-spacing: .06em;
    color: var(--muted); background: var(--panel-2);
    border: 1px solid var(--line); border-radius: 6px;
    cursor: pointer; font-family: 'Gadugi','Inter',system-ui,sans-serif;
    transition: all .15s ease;
  }}
  .btn-csv:hover {{ color: var(--text); border-color: var(--accent-2); background: rgba(0,113,187,.12); }}
  .card-head {{ gap: 8px; }}

  /* Stop table (Risk Budget Monitor) — widths tuned so the 300px SVG fits */
  .stop-table {{ table-layout: fixed; }}
  .stop-table td, .stop-table th {{ vertical-align: middle; white-space: nowrap; }}
  .stop-table th        {{ padding:6px 12px; }}
  .stop-table .pm-name   {{ font-size:12.5px; color:var(--text); padding:8px 12px; width:160px; font-weight:500; }}
  .stop-table .pm-margem {{ font-size:20px;   font-weight:700;  width:90px;  text-align:right; padding:8px 12px; font-variant-numeric: tabular-nums; }}
  .stop-table .bar-cell  {{ width:320px; padding:8px 10px; }}
  .stop-table .pm-hist   {{ font-size:11px;   width:170px; text-align:right; line-height:1.6; padding:8px 12px; font-variant-numeric: tabular-nums; }}
  .stop-table .pm-status {{ font-size:12px;   font-weight:600;  width:110px; padding:8px 12px; }}
  .stop-table .spark-cell {{ padding:2px 6px; width:auto; }}

  /* Cards */
  .card {{
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:18px 20px; margin-bottom:18px;
  }}
  .card-head {{
    display:flex; align-items:baseline; gap:8px;
    padding-bottom:10px; margin-bottom:10px;
    border-bottom:1px solid var(--line);
  }}
  .card-title {{
    font-size:11px; letter-spacing:.18em; text-transform:uppercase;
    color:var(--text); font-weight:700;
  }}
  .card-sub {{ font-size:11px; color:var(--muted); letter-spacing:.05em; }}

  table {{ width:100%; border-collapse:collapse; }}
  .col-headers th {{
    font-size:10px; color:var(--muted); letter-spacing:1.5px; text-transform:uppercase;
    padding:6px 12px; text-align:left; border-bottom:1px solid var(--line);
    font-weight:500;
  }}
  .metric-row td {{ padding:6px 12px; vertical-align:middle; }}
  .metric-name {{ font-size:12px; color:var(--muted); width:120px; }}
  .value-cell {{ font-size:20px; font-weight:700; width:80px; text-align:right; }}
  .bar-cell {{ width:260px; padding:4px 16px; }}
  .util-cell {{ font-size:12px; color:var(--muted); width:90px; text-align:right; }}
  .spark-cell {{ width:180px; padding:2px 8px; }}
  .metric-row:hover {{ background:var(--panel-2); }}
  .bar-legend {{ margin-top:10px; font-size:10px; color:var(--muted); line-height:1.8; }}
  .tick {{ color:#fb923c; font-size:9px; }}

  .sum-movers-card .card-head {{ display:flex; flex-wrap:wrap; align-items:center; gap:12px; }}
  .sum-tgl {{ margin-left:auto; }}

  /* Top Movers split (per-fund) */
  .mov-split {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; }}
  .mov-col-title {{
    font-size:10px; letter-spacing:.15em; text-transform:uppercase;
    color:var(--muted); font-weight:700; margin-bottom:6px;
    padding-bottom:5px; border-bottom:1px solid var(--line);
  }}

  /* Comments / outliers block */
  .comments-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:12px; }}
  .comment-fund {{ background:var(--bg-2); border:1px solid var(--line); border-radius:8px; padding:12px 14px; }}
  .comment-title {{
    font-size:11px; color:var(--text); letter-spacing:.1em; text-transform:uppercase;
    font-weight:700; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid var(--line);
  }}
  .comment-empty {{ font-size:11.5px; color:var(--muted); font-style:italic; padding:4px 0; }}
  .comment-list  {{ list-style:none; padding:0; margin:0; font-size:11.5px; line-height:1.7; }}
  .comment-list li {{ padding:3px 0; border-bottom:1px dotted var(--line); }}
  .comment-list li:last-child {{ border-bottom:none; }}

  /* Summary page — fund grid + top movers */
  .summary-table {{
    width:100%; border-collapse:collapse; font-size:13px;
    background:var(--bg-2); border-radius:8px; overflow:hidden;
  }}
  .summary-table th {{
    color:var(--muted); font-size:10px; letter-spacing:.12em; text-transform:uppercase;
    padding:10px 12px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .summary-table td {{ padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:middle; }}
  .summary-table tr:last-child td {{ border-bottom:none; }}
  .summary-table tr:hover {{ background:rgba(26,143,209,.04); }}
  .summary-table td.sum-status {{ text-align:center; font-size:17px; width:60px; }}
  .summary-table td.sum-fund   {{ font-weight:700; color:var(--text); font-size:13.5px; }}

  .summary-movers {{
    width:100%; border-collapse:collapse; font-size:12px;
    background:var(--bg-2); border-radius:8px; overflow:hidden;
  }}
  .summary-movers th {{
    color:var(--muted); font-size:10px; letter-spacing:.12em; text-transform:uppercase;
    padding:10px 12px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .summary-movers td {{ padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:middle; }}
  .summary-movers tr:last-child td {{ border-bottom:none; }}
  .summary-movers td.sum-fund   {{ font-weight:700; color:var(--text); width:130px; }}
  .summary-movers td.sum-movers {{ font-family:'JetBrains Mono', monospace; font-size:11.5px; }}

  /* Single-name inline section */
  .sn-inline-stats {{
    color:var(--muted); font-size:12px;
    margin: 4px 2px 14px; padding-bottom:10px;
    border-bottom:1px solid var(--line);
  }}

  /* Performance attribution — hierarchical tree */
  .pa-card .card-head {{ display:flex; flex-wrap:wrap; align-items:center; gap:12px; }}
  .pa-toolbar {{ margin-left:auto; display:flex; gap:6px; align-items:center; }}
  .pa-search {{
    background:rgba(255,255,255,0.06); border:1px solid var(--line);
    border-radius:7px; color:var(--text); font-size:11.5px;
    padding:4px 10px; outline:none; width:150px;
    font-family:'JetBrains Mono',monospace;
  }}
  .pa-search:focus {{ border-color:var(--accent-2); background:rgba(255,255,255,0.1); }}
  .pa-search::placeholder {{ color:var(--muted); }}
  .pa-btn {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 10px; border-radius:6px; font-size:11px; cursor:pointer;
  }}
  .pa-btn:hover {{ color:var(--text); border-color:var(--line-2); }}
  .pa-view-toggle {{ display:flex; gap:4px; }}
  .pa-tgl {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 12px; border-radius:7px; font-size:11px; font-weight:600;
    letter-spacing:.06em; cursor:pointer;
  }}
  .pa-tgl:hover {{ color:var(--text); border-color:var(--line-2); }}
  .pa-tgl.active {{ background:var(--accent); border-color:var(--accent); color:#fff; }}

  .pa-table {{
    width:100%; background:var(--bg-2); border-radius:8px; overflow:hidden;
    font-size:12px; border-collapse:collapse;
  }}
  .pa-table th {{
    color:var(--muted); font-size:10px; letter-spacing:.1em; text-transform:uppercase;
    padding:9px 10px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .pa-table td.pa-name {{ padding:6px 10px; color:var(--text); font-weight:600; }}
  .pa-table td.t-num   {{ padding:6px 10px; text-align:right; }}
  .pa-table tbody tr   {{ border-bottom:1px solid var(--line); }}
  .pa-table tbody tr:last-child {{ border-bottom:none; }}
  .pa-table tbody tr.pa-has-children {{ cursor:pointer; }}
  .pa-table tbody tr.pa-has-children:hover {{ background:rgba(26,143,209,.06); }}
  .pa-table tbody tr.pa-l0 td.pa-name {{ padding-left:10px;  font-weight:700; }}
  .pa-table tbody tr.pa-l1 td.pa-name {{ padding-left:30px;  font-weight:600; color:var(--text); }}
  .pa-table tbody tr.pa-l2 td.pa-name {{ padding-left:50px;  font-weight:400; color:var(--muted); }}
  .pa-table tbody tr.pa-l1        {{ background:rgba(255,255,255,.012); }}
  .pa-table tbody tr.pa-l2        {{ background:rgba(255,255,255,.024); font-size:11.5px; }}
  .pa-table tbody tr.pa-pinned td.pa-name {{ color:var(--muted); font-style:italic; }}
  .pa-table tbody tr.pa-pinned    {{ border-top:1px dashed var(--line); }}
  .pa-exp {{
    display:inline-block; width:14px; color:var(--muted);
    font-size:10px; margin-right:4px; transition:transform .15s;
  }}
  .pa-exp-empty {{ color:transparent; }}
  .pa-has-children.expanded .pa-exp {{ transform:rotate(90deg); color:var(--accent-2); }}
  .pa-table tfoot tr.pa-total-row {{
    background:var(--panel-2); border-top:1px solid var(--line-2);
  }}
  .pa-table tfoot tr.pa-bench-row {{
    background:transparent; border-top:1px dashed var(--line);
  }}
  .pa-table tfoot tr.pa-bench-row td {{ font-style:italic; }}
  .pa-table tfoot tr.pa-nominal-row {{
    background:var(--panel-2); border-top:1px solid var(--line-2);
  }}
  .pa-table tfoot td.pa-name {{ padding:8px 10px; }}
  .pa-table th.pa-sortable {{ user-select:none; }}
  .pa-table th.pa-sortable:hover {{ color:var(--text); }}
  .pa-table th.pa-sort-active {{ color:var(--accent-2); }}
  .pa-sort-arrow {{ font-size:9px; color:var(--accent-2); margin-left:2px; }}
  .pa-pos {{ color:var(--muted); }}

  /* Single-name fund switcher (Por Report > Single-Name) */
  .sn-switcher {{
    display:flex; align-items:center; gap:8px;
    max-width: 1200px; margin: 0 auto 14px; padding: 8px 16px;
    background: var(--panel); border:1px solid var(--line);
    border-radius:10px;
  }}
  .sn-switcher-label {{
    font-size:10.5px; text-transform:uppercase; letter-spacing:.14em;
    color:var(--muted); margin-right:6px;
  }}
  .sn-switcher .tab {{
    background:transparent; border:1px solid var(--line);
    color:var(--muted); padding:6px 14px; border-radius:7px;
    font-size:12px; font-weight:600; letter-spacing:.06em;
    cursor:pointer;
  }}
  .sn-switcher .tab:hover {{ color:var(--text); border-color:var(--line-2); }}
  .sn-switcher .tab.active {{
    background:var(--accent); border-color:var(--accent); color:#fff;
  }}
  .sn-sides {{ display:flex; gap:18px; }}
  .sn-side {{ flex:1; }}
  .sn-side-head {{ font-size:11px; font-weight:700; letter-spacing:.1em; margin-bottom:6px; }}
  .sn-side-count {{ color:var(--muted); font-weight:400; }}
  .sn-table {{ background:var(--bg-2); border-radius:8px; overflow:hidden; font-size:11.5px; }}
  .sn-table th {{
    color:var(--muted); font-size:10px; padding:8px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .sn-table td.t-name {{ padding:6px 8px; color:var(--text); font-weight:700; }}
  .sn-table td.t-num  {{ padding:6px 8px; text-align:right; color:var(--muted); }}
  .sn-table tr {{ border-bottom:1px solid var(--line); }}
  .sn-table tr:last-child {{ border-bottom:none; }}
  .sn-table .t-empty {{ padding:14px; text-align:center; color:var(--muted); font-style:italic; }}
  /* Alerts */
  .alerts-section {{
    margin-top: 28px;
    border: 1px solid #fb923c44;
    border-radius: 8px;
    overflow: hidden;
  }}
  .alerts-header {{
    background: #1c1409;
    color: #fb923c;
    font-size: 11px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    padding: 10px 16px;
    border-bottom: 1px solid #fb923c33;
  }}
  .alert-item {{
    padding: 12px 16px;
    border-bottom: 1px solid #1e1e2e;
  }}
  .alert-item:last-child {{ border-bottom: none; }}
  .alert-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 6px;
  }}
  .alert-badge {{
    color: #fb923c;
    font-size: 14px;
  }}
  .alert-title {{
    color: #fb923c;
    font-weight: bold;
    font-size: 13px;
  }}
  .alert-stats {{
    color: #64748b;
    font-size: 11px;
    font-family: monospace;
  }}
  .alert-body {{
    color: #cbd5e1;
    font-size: 12px;
    line-height: 1.6;
    padding-left: 24px;
  }}
</style>
<script>
(function() {{
  function getBRT() {{
    var now = new Date();
    var hour = parseInt(new Intl.DateTimeFormat('en', {{
      timeZone: 'America/Sao_Paulo', hour: 'numeric', hour12: false
    }}).format(now));
    var fmt = new Intl.DateTimeFormat('en-CA', {{
      timeZone: 'America/Sao_Paulo',
      year: 'numeric', month: '2-digit', day: '2-digit'
    }});
    var todayStr = fmt.format(now);
    if (hour < 21) {{
      var d = new Date(now);
      d.setDate(d.getDate() - 1);
      var wd = new Intl.DateTimeFormat('en', {{timeZone:'America/Sao_Paulo', weekday:'short'}}).format(d);
      while (wd === 'Sat' || wd === 'Sun') {{
        d.setDate(d.getDate() - 1);
        wd = new Intl.DateTimeFormat('en', {{timeZone:'America/Sao_Paulo', weekday:'short'}}).format(d);
      }}
      return {{ def: fmt.format(d), today: todayStr, hour: hour }};
    }} else {{
      return {{ def: todayStr, today: todayStr, hour: hour }};
    }}
  }}
  // --- Navigation state: 3 modes (summary / fund / quality) driven by URL hash ---
  function parseHash() {{
    var h = (location.hash || '').slice(1);
    if (!h || h === 'summary') return {{ mode: 'summary' }};
    var m;
    if ((m = h.match(/^fund=(.*)$/)))   return {{ mode: 'fund',   sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^report=(.*)$/))) return {{ mode: 'report', sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^quality$/)))     return {{ mode: 'quality', sel: '' }};
    return {{ mode: 'summary' }};
  }}
  function setHash(mode, sel) {{
    var h = (mode === 'summary') ? '' : (sel ? (mode + '=' + encodeURIComponent(sel)) : mode);
    if (history.replaceState) history.replaceState(null, '', h ? ('#' + h) : location.pathname + location.search);
    else location.hash = h;
  }}

  var _FUND_REPORTS = {fund_reports_js};
  var _REPORT_LABELS = {report_labels_js};

  function updateJumpBar(fund) {{
    var bar = document.getElementById('report-jump-bar');
    if (!bar) return;
    var rids = _FUND_REPORTS[fund] || [];
    bar.innerHTML = '';
    rids.forEach(function(rid) {{
      var btn = document.createElement('button');
      btn.className = 'jump-btn';
      btn.dataset.rid = rid;
      btn.textContent = _REPORT_LABELS[rid] || rid;
      btn.onclick = function() {{ jumpTo('sec-' + fund + '-' + rid); }};
      bar.appendChild(btn);
    }});
  }}
  window.jumpTo = function(id) {{
    var el = document.getElementById(id);
    if (!el) return;
    // Account for sticky header height
    var hdr = document.querySelector('header');
    var offset = hdr ? hdr.offsetHeight : 0;
    var top = el.getBoundingClientRect().top + window.pageYOffset - offset - 8;
    window.scrollTo({{ top: top, behavior: 'smooth' }});
    // Highlight active jump button
    document.querySelectorAll('.jump-btn').forEach(function(b) {{ b.classList.remove('active-jump'); }});
    var rid = id.split('-').slice(2).join('-');
    document.querySelectorAll('.jump-btn[data-rid="' + rid + '"]').forEach(function(b) {{
      b.classList.add('active-jump');
    }});
  }};

  function applyState() {{
    var st = parseHash();
    var mode = st.mode, sel = st.sel;
    document.body.dataset.mode = mode;
    // Mode tabs
    document.querySelectorAll('.mode-tab').forEach(function(t) {{
      t.classList.toggle('active', t.dataset.mode === mode);
    }});
    // Sub-tab bars visibility
    document.querySelectorAll('.sub-tabs').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.for === mode);
    }});
    // Default selection per mode
    if (mode === 'fund' && !sel) {{
      var first = document.querySelector('.sub-tabs[data-for="fund"] .tab');
      if (first) sel = first.dataset.target;
    }}
    if (mode === 'report' && !sel) {{
      var first = document.querySelector('.sub-tabs[data-for="report"] .tab');
      if (first) sel = first.dataset.target;
    }}
    // Sub-tab active state
    document.querySelectorAll('.sub-tabs .tab').forEach(function(t) {{
      var bar = t.closest('.sub-tabs');
      t.classList.toggle('active', bar.dataset.for === mode && t.dataset.target === sel);
    }});
    // Update report jump bar (only in fund mode)
    if (mode === 'fund' && sel) updateJumpBar(sel);
    // Section visibility
    document.querySelectorAll('.section-wrap').forEach(function(el) {{
      var show = false;
      if (mode === 'summary')      show = el.dataset.view === 'summary';
      else if (mode === 'quality') show = el.dataset.view === 'quality';
      else if (mode === 'fund')    show = el.dataset.fund === sel;
      else if (mode === 'report')  show = el.dataset.report === sel;
      el.style.display = show ? '' : 'none';
    }});
    // Empty-state
    var anyVisible = Array.prototype.some.call(
      document.querySelectorAll('.section-wrap'),
      function(el) {{ return el.style.display !== 'none'; }}
    );
    var empty = document.getElementById('empty-state');
    if (empty) empty.style.display = anyVisible ? 'none' : '';
  }}
  window.selectMode = function(mode, sel) {{
    if (!sel) {{
      try {{
        if (mode === 'fund')   sel = sessionStorage.getItem('risk_monitor_fund')   || '';
        if (mode === 'report') sel = sessionStorage.getItem('risk_monitor_report') || '';
      }} catch (e) {{}}
    }}
    if (mode === 'fund' && !sel) {{
      var f = document.querySelector('.sub-tabs[data-for="fund"] .tab');
      if (f) sel = f.dataset.target;
    }}
    if (mode === 'report' && !sel) {{
      var r = document.querySelector('.sub-tabs[data-for="report"] .tab');
      if (r) sel = r.dataset.target;
    }}
    setHash(mode, sel);
    applyState();
  }};
  window.selectFund = function(name) {{
    try {{ sessionStorage.setItem('risk_monitor_fund', name); }} catch (e) {{}}
    setHash('fund', name);
    applyState();
  }};
  window.selectReport = function(name) {{
    try {{ sessionStorage.setItem('risk_monitor_report', name); }} catch (e) {{}}
    setHash('report', name);
    applyState();
  }};
  window.addEventListener('hashchange', applyState);
  window.addEventListener('DOMContentLoaded', function() {{
    var info   = getBRT();
    var picker = document.getElementById('date-picker');
    var hint   = document.getElementById('date-hint');
    var loaded = '{DATA_STR}';
    picker.value = loaded;
    if (info.hour >= 21 && info.def === info.today) {{
      hint.textContent = 'após 21h';
      hint.style.color = 'var(--warn)';
    }} else if (loaded !== info.def) {{
      hint.textContent = 'default ' + info.def;
      hint.style.color = 'var(--muted)';
    }}
    applyState();
    injectCsvButtons();
    attachUniversalSort();
  }});
  window.goToDate = function(val) {{
    if (!val) return;
    var base = window.location.href.replace(/[^/\\\\]*$/, '').replace(/#.*$/, '');
    window.location.href = base + val + '_risk_monitor.html' + location.hash;
  }};
  // --- CSV export: scan visible tables inside a given element and download ---
  function escapeCSV(s) {{
    s = (s == null) ? '' : String(s).replace(/\\s+/g, ' ').trim();
    if (/[,";]/.test(s)) s = '"' + s.replace(/"/g, '""') + '"';
    return s;
  }}
  function tableToRows(tbl) {{
    var out = [];
    var rows = tbl.querySelectorAll('tr');
    for (var i = 0; i < rows.length; i++) {{
      var tr = rows[i];
      // skip hidden rows (drill-downs, toggled views)
      if (tr.offsetParent === null && tr.style.display !== '') continue;
      var cells = tr.querySelectorAll('th,td');
      if (!cells.length) continue;
      var line = [];
      for (var j = 0; j < cells.length; j++) {{
        var txt = cells[j].innerText || cells[j].textContent || '';
        line.push(escapeCSV(txt));
      }}
      out.push(line.join(','));
    }}
    return out;
  }}
  window.exportCardCSV = function(cardId, baseName) {{
    var el = document.getElementById(cardId);
    if (!el) return;
    var tables = el.querySelectorAll('table');
    var lines = [];
    tables.forEach(function(t, i) {{
      if (t.offsetParent === null) return;  // skip hidden tables
      if (i > 0 && lines.length) lines.push('');
      lines = lines.concat(tableToRows(t));
    }});
    if (!lines.length) return;
    var picker = document.getElementById('date-picker');
    var dt = (picker && picker.value) || '{DATA_STR}';
    var name = baseName + '_' + dt + '.csv';
    var blob = new Blob(['\\uFEFF' + lines.join('\\n')], {{ type: 'text/csv;charset=utf-8' }});
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a); a.click();
    setTimeout(function() {{ document.body.removeChild(a); URL.revokeObjectURL(a.href); }}, 100);
  }};
  // --- Universal table sort: clicking a <th> toggles asc/desc on that column ---
  function _cellKey(td) {{
    var dv = td.getAttribute('data-val');
    if (dv !== null && dv !== '') {{
      var n = parseFloat(dv);
      if (!isNaN(n)) return {{ n: n, s: dv }};
    }}
    var t = (td.innerText || td.textContent || '').trim();
    var clean = t.replace(/[%°\\s,+bps]/g, '').replace(/\\u2212/g, '-');
    var n2 = parseFloat(clean);
    if (!isNaN(n2)) return {{ n: n2, s: t }};
    return {{ n: NaN, s: t }};
  }}
  function sortTableByCol(table, colIdx, asc) {{
    var tbody = table.tBodies[0]; if (!tbody) return;
    var rows = Array.from(tbody.rows).filter(function(r) {{ return r.cells.length > colIdx; }});
    rows.sort(function(a, b) {{
      var va = _cellKey(a.cells[colIdx]), vb = _cellKey(b.cells[colIdx]);
      if (!isNaN(va.n) && !isNaN(vb.n)) return asc ? va.n - vb.n : vb.n - va.n;
      if (!isNaN(va.n)) return -1;
      if (!isNaN(vb.n)) return  1;
      return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    }});
    rows.forEach(function(r) {{ tbody.appendChild(r); }});
  }}
  function attachUniversalSort() {{
    // Exclude tables that already have data-sort-col handlers (exposure section)
    document.querySelectorAll('table').forEach(function(table) {{
      if (table.dataset.sortAttached === '1') return;
      if (table.dataset.noSort === '1') return;
      var headers = table.querySelectorAll('thead th');
      if (!headers.length) return;
      // Skip tables that already use sortTable(...) handlers (have onclick with sortTable)
      var alreadyCustom = Array.from(headers).some(function(h) {{
        return (h.getAttribute('onclick') || '').indexOf('sortTable(') >= 0;
      }});
      if (alreadyCustom) return;
      table.dataset.sortAttached = '1';
      var sortState = {{}};
      headers.forEach(function(th, idx) {{
        th.style.cursor = 'pointer';
        th.style.userSelect = 'none';
        var ind = document.createElement('span');
        ind.textContent = ' ▲▼';
        ind.style.opacity = '0.3';
        ind.style.fontSize = '8px';
        ind.style.marginLeft = '3px';
        th.appendChild(ind);
        th.addEventListener('click', function() {{
          var asc = !sortState[idx];
          sortState = {{}}; sortState[idx] = asc;
          sortTableByCol(table, idx, asc);
          headers.forEach(function(h2, j) {{
            var sp = h2.lastChild;
            if (!sp || sp.nodeName !== 'SPAN') return;
            if (j === idx) {{ sp.textContent = asc ? ' ▲' : ' ▼'; sp.style.opacity = '0.85'; }}
            else           {{ sp.textContent = ' ▲▼'; sp.style.opacity = '0.3'; }}
          }});
        }});
      }});
    }});
  }}
  function injectCsvButtons() {{
    var cards = document.querySelectorAll('section.card, .modal');
    cards.forEach(function(card, idx) {{
      var head = card.querySelector('.card-head, .modal-head');
      if (!head || head.querySelector('.btn-csv')) return;
      if (!card.id) card.id = 'csv-card-' + idx;
      var title = card.querySelector('.card-title, .modal-title');
      var base = title ? title.textContent.trim().replace(/[^a-z0-9]+/gi, '_').toLowerCase() : 'table';
      var btn = document.createElement('button');
      btn.className = 'btn-csv';
      btn.textContent = '⤓ CSV';
      btn.setAttribute('type','button');
      btn.onclick = function(e) {{ e.stopPropagation(); window.exportCardCSV(card.id, base); }};
      head.appendChild(btn);
    }});
  }}
  window.setDistMode = function(cardId, mode) {{
    var card = document.getElementById(cardId);
    if (!card) return;
    card.querySelectorAll('.dist-btn').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.mode === mode);
    }});
    card.querySelectorAll('.dist-view').forEach(function(v) {{
      v.style.display = (v.dataset.mode === mode) ? '' : 'none';
    }});
  }};
  // ── PDF export ─────────────────────────────────────────────────────────
  // mode === 'current' → imprime só o que está visível (respeita mode/fund)
  // mode === 'full'    → expande todas as PA trees e mostra todas as seções
  window.exportPdf = function(mode) {{
    var body = document.body;
    if (mode === 'full') body.classList.add('print-full');
    else                 body.classList.add('print-current');
    // We do NOT auto-expand PA trees — that produces dozens of pages of tiny type.
    // User expands manually what they want before clicking; default is level-0 only.
    setTimeout(function() {{
      window.print();
      setTimeout(function() {{
        body.classList.remove('print-full', 'print-current');
      }}, 500);
    }}, 120);
  }};

  // Summary > Top Movers view toggle (Por Livro / Por Classe)
  window.selectMoversView = function(btn, view) {{
    var card = btn.closest('.sum-movers-card');
    if (!card) return;
    card.querySelectorAll('.sum-tgl .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.movView === view);
    }});
    card.querySelectorAll('.mov-view').forEach(function(t) {{
      t.style.display = (t.dataset.movView === view) ? '' : 'none';
    }});
  }};
  // PA view toggle (Por Classe / Por Livro) inside a PA card
  window.selectPaView = function(btn, viewId) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    // Clear search when switching views
    var srch = card.querySelector('.pa-search');
    if (srch) srch.value = '';
    card.querySelectorAll('.pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.paView === viewId);
    }});
    card.querySelectorAll('.pa-view').forEach(function(v) {{
      v.style.display = (v.dataset.paView === viewId) ? '' : 'none';
    }});
  }};
  window.filterPa = function(input) {{
    var q = input.value.trim().toLowerCase();
    var card = input.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    if (!q) {{
      // Reset to root-only visible state
      view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
        tr.style.display = (parseInt(tr.dataset.level || '0') === 0) ? '' : 'none';
        tr.classList.remove('expanded');
      }});
      return;
    }}
    // Force-render all lazy children (expand any not-yet-rendered has-children)
    for (var guard = 0; guard < 50; guard++) {{
      var pending = Array.prototype.filter.call(
        view.querySelectorAll('tr.pa-has-children'),
        function(tr) {{ return tr.dataset.rendered !== '1'; }}
      );
      if (pending.length === 0) break;
      pending.forEach(function(tr) {{
        if (!tr.classList.contains('expanded')) window.togglePaRow(tr);
      }});
    }}
    // Build path → tr map for ancestor lookup
    var pathMap = {{}};
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      if (tr.dataset.path) pathMap[tr.dataset.path] = tr;
    }});
    // Find matching paths + all their ancestors
    var show = {{}};
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      var lbl = tr.querySelector('.pa-label');
      if (!lbl) return;
      if (lbl.textContent.trim().toLowerCase().indexOf(q) >= 0) {{
        var p = tr.dataset.path;
        while (p) {{
          show[p] = true;
          var anc = pathMap[p];
          p = anc ? anc.dataset.parent : '';
        }}
      }}
    }});
    // Apply visibility
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      tr.style.display = show[tr.dataset.path] ? '' : 'none';
    }});
  }};
  // ── PA lazy render helpers ─────────────────────────────────────────────
  function paDataFor(view) {{
    var id = view.dataset.paId;
    if (!id) return null;
    var cached = view._paData;
    if (cached) return cached;
    var s = document.getElementById(id);
    if (!s) return null;
    try {{ view._paData = JSON.parse(s.textContent); return view._paData; }}
    catch (e) {{ console.error('PA JSON parse failed', e); return null; }}
  }}
  function paEsc(s) {{
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }}
  function paPctCell(v, maxAbs) {{
    var pct = v / 100.0;
    if (Math.abs(pct) < 0.005) {{
      return '<td class="t-num mono" style="color:var(--muted)">—</td>';
    }}
    var color = v >= 0 ? 'var(--up)' : 'var(--down)';
    var rgb   = v >= 0 ? '38,208,124' : '255,90,106';
    var alpha = (maxAbs > 0) ? Math.min(Math.abs(v) / maxAbs, 1.0) * 0.14 : 0;
    var sign  = v >= 0 ? '+' : '';
    return '<td class="t-num mono" style="color:' + color
         + '; background:rgba(' + rgb + ',' + alpha.toFixed(2) + ')">'
         + sign + pct.toFixed(2) + '%</td>';
  }}
  function paPosCell(v) {{
    if (Math.abs(v) < 1e5) {{
      return '<td class="t-num mono pa-pos" style="color:var(--muted)">—</td>';
    }}
    var m = v / 1e6;
    var s = m.toLocaleString('pt-BR', {{minimumFractionDigits:1, maximumFractionDigits:1}});
    return '<td class="t-num mono pa-pos" style="color:var(--muted)">' + s + '</td>';
  }}
  function paRenderRow(node, maxAbs) {{
    var levelCls  = 'pa-l' + node.dp;
    var pinnedCls = node.pi ? ' pa-pinned' : '';
    var expander  = node.hc
      ? '<span class="pa-exp" aria-hidden="true">▸</span>'
      : '<span class="pa-exp pa-exp-empty" aria-hidden="true"></span>';
    var baseCls   = 'pa-row ' + levelCls + pinnedCls;
    var clsClick  = node.hc
      ? ' onclick="togglePaRow(this)" class="' + baseCls + ' pa-has-children"'
      : ' class="' + baseCls + '"';
    var pinAttr   = node.pi ? ' data-pinned="1"' : '';
    var attrs = ' data-level="' + node.dp + '" data-path="' + paEsc(node.pa)
              + '" data-parent="' + paEsc(node.pr) + '"' + pinAttr;
    var cells = '<td class="pa-name">' + expander
              + '<span class="pa-label">' + paEsc(node.d) + '</span></td>'
              + paPctCell(node.a[0], maxAbs[0])
              + paPctCell(node.a[1], maxAbs[1])
              + paPctCell(node.a[2], maxAbs[2])
              + paPctCell(node.a[3], maxAbs[3]);
    return '<tr' + attrs + clsClick + '>' + cells + '</tr>';
  }}
  function paRenderChildren(view, parentTr) {{
    if (parentTr.dataset.rendered === '1') return;
    var data = paDataFor(view);
    if (!data) return;
    var kids = (data.byParent || {{}})[parentTr.dataset.path] || [];
    var ordered = kids;
    // Only re-sort if user has explicitly clicked a sort header.
    // JSON default order already honours the server-side Excel-inspired order.
    if (view.dataset.userSorted === '1') {{
      var idx  = parseInt(view.dataset.sortIdx  || '2', 10);
      var desc = (view.dataset.sortDesc || '1') === '1';
      var reg = kids.filter(function(k) {{ return !k.pi; }});
      var pin = kids.filter(function(k) {{ return k.pi; }});
      reg.sort(function(a, b) {{
        var va = a.a[idx], vb = b.a[idx];
        var aZ = Math.abs(va) < 1e-6, bZ = Math.abs(vb) < 1e-6;
        if (aZ && bZ) return 0;
        if (aZ) return 1;
        if (bZ) return -1;
        return desc ? (vb - va) : (va - vb);
      }});
      ordered = reg.concat(pin);
    }}
    var html = ordered.map(function(k) {{ return paRenderRow(k, data.maxAbs); }}).join('');
    parentTr.insertAdjacentHTML('afterend', html);
    parentTr.dataset.rendered = '1';
  }}

  // PA tree expand/collapse (lazy)
  window.togglePaRow = function(tr) {{
    if (!tr) return;
    var view = tr.closest('.pa-view');
    if (!view) return;
    var path = tr.dataset.path;
    var willExpand = !tr.classList.contains('expanded');
    if (willExpand) paRenderChildren(view, tr);
    tr.classList.toggle('expanded', willExpand);
    var sel = 'tr[data-parent="' + (window.CSS && CSS.escape ? CSS.escape(path) : path) + '"]';
    view.querySelectorAll(sel).forEach(function(c) {{
      if (willExpand) {{
        c.style.display = '';
      }} else {{
        c.style.display = 'none';
        if (c.classList.contains('expanded')) window.togglePaRow(c);
      }}
    }});
  }};
  // Expand every parent in the currently-active view (recursive, lazy-renders as it goes)
  window.expandAllPa = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    // Loop: keep expanding until no collapsed parents remain visible
    for (var guard = 0; guard < 50; guard++) {{
      var pending = view.querySelectorAll('tr.pa-has-children:not(.expanded)');
      if (pending.length === 0) break;
      pending.forEach(function(tr) {{
        if (tr.style.display !== 'none') window.togglePaRow(tr);
      }});
    }}
  }};
  window.collapseAllPa = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    // Collapse top-level expanded rows; togglePaRow recursively collapses descendants.
    view.querySelectorAll('tr.pa-has-children.expanded[data-level="0"]').forEach(function(tr) {{
      window.togglePaRow(tr);
    }});
  }};
  // PA per-metric sort — preserves tree hierarchy (sorts siblings under each parent)
  window.sortPaMetric = function(th, idx) {{
    var view  = th.closest('.pa-view');
    if (!view) return;
    var tbody = view.querySelector('tbody');
    var curIdx  = parseInt(view.dataset.sortIdx || '2');
    var curDesc = (view.dataset.sortDesc || '1') === '1';
    var desc = (curIdx === idx) ? !curDesc : true;
    view.dataset.sortIdx   = String(idx);
    view.dataset.sortDesc  = desc ? '1' : '0';
    view.dataset.userSorted = '1';

    // Update arrow markers
    view.querySelectorAll('th.pa-sortable').forEach(function(h) {{
      h.classList.remove('pa-sort-active');
      var a = h.querySelector('.pa-sort-arrow');
      if (a) a.remove();
    }});
    th.classList.add('pa-sort-active');
    var arrow = document.createElement('span');
    arrow.className = 'pa-sort-arrow';
    arrow.textContent = desc ? ' ▾' : ' ▴';
    th.appendChild(arrow);

    function parseCell(tr, metricIdx) {{
      var cell = tr.children[1 + metricIdx];
      if (!cell) return 0;
      var t = cell.textContent.trim();
      if (t === '—' || t === '') return 0;
      return parseFloat(t.replace('+','').replace('%','').replace(',','.')) || 0;
    }}

    // group rows by parent path, separating pinned-bottom rows (Caixa/Custos/...)
    var byParentReg = {{}}, byParentPin = {{}};
    Array.prototype.forEach.call(tbody.children, function(tr) {{
      var p = tr.dataset.parent || '';
      if (tr.dataset.pinned === '1') {{
        (byParentPin[p] = byParentPin[p] || []).push(tr);
      }} else {{
        (byParentReg[p] = byParentReg[p] || []).push(tr);
      }}
    }});
    // sort regular siblings: positives → negatives → zeros, signed
    function paCmp(va, vb) {{
      var aZ = Math.abs(va) < 1e-6, bZ = Math.abs(vb) < 1e-6;
      if (aZ && bZ) return 0;
      if (aZ) return 1;   // zeros always last
      if (bZ) return -1;
      return desc ? (vb - va) : (va - vb);
    }}
    Object.keys(byParentReg).forEach(function(p) {{
      byParentReg[p].sort(function(a, b) {{
        return paCmp(parseCell(a, idx), parseCell(b, idx));
      }});
    }});
    // merge: regular first, pinned always last (not sorted)
    var byParent = {{}};
    var allKeys = new Set([].concat(Object.keys(byParentReg), Object.keys(byParentPin)));
    allKeys.forEach(function(p) {{
      byParent[p] = (byParentReg[p] || []).concat(byParentPin[p] || []);
    }});
    // DFS reconstruction
    var ordered = [];
    function visit(parentPath) {{
      (byParent[parentPath] || []).forEach(function(tr) {{
        ordered.push(tr);
        visit(tr.dataset.path);
      }});
    }}
    visit('');
    ordered.forEach(function(tr) {{ tbody.appendChild(tr); }});
  }};
}})();
</script>
</head>
<body>
<header>
  <div class="hwrap">
    <div class="brand">
      <div class="logo"><svg viewBox="0 0 47 45" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M39.5781 37.6377C36.1678 40.8382 31.8394 42.6304 27.2486 42.6304H27.1174C17.5423 42.5024 9.67243 34.8213 9.67243 25.4758C9.54127 17.9227 14.6567 11.2657 21.8708 8.96135C22.2643 9.72947 22.5266 10.6256 22.6578 11.3937C20.428 12.0338 18.4605 13.186 16.7554 14.8502C14.6567 16.7705 13.3451 19.3309 12.6892 21.8913V22.0193C12.0334 24.5797 12.1646 27.1401 12.9516 29.4444C13.6074 31.6208 14.7879 33.6691 16.493 35.3333C18.4605 37.2536 20.8215 38.6618 23.3136 39.3019C23.4448 39.3019 23.7071 39.43 23.8383 39.43C23.9694 39.43 23.9694 39.43 24.1006 39.43C24.1006 39.43 24.4941 39.558 25.1499 39.558C25.6746 39.686 26.3304 39.686 26.8551 39.686C27.6421 39.686 28.4291 39.686 29.3472 39.558C32.364 39.1739 35.3808 37.8937 37.7418 35.7174C39.8404 33.6691 41.0209 31.3647 41.2833 30.4686C41.6768 29.3164 41.5456 28.0362 41.0209 26.8841C40.4963 25.7319 39.4469 24.8358 38.1353 24.4517C36.8236 24.0676 35.512 24.0676 34.3315 24.7077C31.8394 25.8599 30.79 28.8043 32.1017 31.2367C32.364 31.6208 32.8887 31.8768 33.2822 31.6208C33.6757 31.3647 33.938 30.8527 33.6757 30.4686C32.8887 28.9324 33.5445 27.1401 35.1185 26.372C35.9055 25.9879 36.6925 25.9879 37.4795 26.244C38.2665 26.5 38.9223 27.0121 39.3158 27.7802C39.7093 28.5483 39.7093 29.3164 39.4469 30.0845C39.3158 30.4686 39.1846 30.7246 38.9223 30.9807C34.8562 34.6932 28.4291 34.4372 24.4941 30.4686C24.1006 30.0845 23.5759 30.0845 23.1825 30.4686C22.789 30.8527 22.789 31.3647 23.1825 31.7488C26.5927 35.3333 31.8394 36.3575 36.299 34.6932C34.4627 36.3575 32.2329 37.3816 29.8719 37.7657C27.7732 38.0217 26.0681 36.9976 25.5434 36.7415C22.002 34.4372 20.2968 30.2126 21.3461 25.8599C22.6578 20.4831 28.1667 17.1546 33.6757 18.4348C34.0692 18.5628 34.4627 18.5628 34.725 18.6908C35.9055 19.0749 37.2171 18.6908 37.873 17.5386C38.2665 16.7705 38.3976 16.0024 38.0041 15.3623C38.0041 15.2343 36.6925 11.9058 32.8887 9.08937C39.9716 11.3937 44.9559 17.9227 44.9559 25.4758C44.8247 30.0845 42.9884 34.3092 39.5781 37.6377ZM30.79 6.78503C30.1342 6.65701 29.4784 7.04107 29.2161 7.68116C28.9537 8.32126 29.2161 8.96136 29.8719 9.34541C33.938 11.6498 35.6432 14.5942 36.0366 15.6184C36.1678 15.7464 36.1678 15.8744 36.1678 15.8744C36.299 16.3865 36.1678 16.6425 36.1678 16.7705C36.0366 16.8986 35.7743 17.2826 35.2497 17.1546C34.8562 17.0266 34.4627 16.8986 33.938 16.7705C27.5109 15.3623 20.9526 19.3309 19.3787 25.6039C18.1982 30.3406 20.1657 35.0773 23.9694 37.7657C22.9201 37.5097 21.7396 37.1256 20.6903 36.4855C20.6903 36.4855 20.6903 36.4855 20.5592 36.4855C20.428 36.4855 20.2968 36.3575 20.2968 36.3575C17.6735 34.6932 15.5749 32.2609 14.6567 29.3164C13.2139 24.8358 14.2632 19.715 17.9358 16.1304C19.3787 14.8502 21.0838 13.8261 22.9201 13.186C23.0513 13.9541 23.0513 14.8502 23.0513 15.6184C23.0513 16.1304 23.4448 16.5145 23.9694 16.5145C24.4941 16.5145 24.8876 16.1304 24.8876 15.6184C24.8876 14.3382 24.7564 13.058 24.4941 11.9058C24.2318 10.4976 23.7071 9.08937 23.1825 7.80918C23.0513 7.55314 22.789 7.2971 22.5266 7.16908C22.2643 7.04107 22.002 7.04107 21.7396 7.16908C13.6074 9.47343 7.70496 17.0266 7.70496 25.4758C7.70496 35.8454 16.3619 44.4227 26.9862 44.4227C27.1174 44.4227 27.1174 44.4227 27.1174 44.4227C32.2329 44.4227 37.086 42.5024 40.7586 38.9179C44.5624 35.3333 46.5299 30.4686 46.5299 25.4758C46.661 16.2585 40.1028 8.44928 30.79 6.78503Z" fill="white"/></svg></div>
      <div>
        <h1>Galapagos <span style="font-weight:400;opacity:.75">CAPITAL</span></h1>
        <p>Risk Monitor</p>
      </div>
    </div>
    <nav class="mode-switcher" role="tablist">{mode_tabs_html}</nav>
    <div class="controls">
      <div class="ctrl-group">
        <label>Data</label>
        <input type="date" id="date-picker" value="{DATA_STR}"/>
        <span id="date-hint" class="date-hint mono"></span>
      </div>
      <button class="btn-primary" onclick="goToDate(document.getElementById('date-picker').value)">Ir</button>
      <button class="btn-pdf no-print" onclick="exportPdf('current')" title="Imprimir / salvar como PDF apenas a aba atual">⇣ PDF (aba)</button>
      <button class="btn-pdf no-print" onclick="exportPdf('full')"    title="Expande todas as PA trees e imprime tudo">⇣ PDF (completo)</button>
    </div>
  </div>
  <div class="navrow">
    <nav class="sub-tabs" data-for="fund"   role="tablist">{fund_subtabs_html}</nav>
    <nav class="sub-tabs" data-for="report" role="tablist">{report_subtabs_html}</nav>
  </div>
  <div id="report-jump-bar" role="tablist"></div>
</header>
<div class="subtitle">Data-base: <span class="mono">{DATA_STR}</span> &nbsp;·&nbsp; gerado em <span class="mono">{date.today().isoformat()}</span></div>
<main>
  {summary_html}
  {quality_html}
  <div id="sections-container">
    {sections_html}
  </div>
  <div id="empty-state" style="display:none">Sem dados para essa combinação de fundo × report.</div>
  {alerts_html}
</main>
</body>
</html>"""
    return html

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import time
    from concurrent.futures import ThreadPoolExecutor
    from glpg_fetch import read_sql

    t0 = time.time()
    print(f"Fetching data for {DATA_STR}...")
    d1_str = _prev_bday(DATA_STR)
    d2_str = _prev_bday(d1_str)

    # Fan out all independent DB queries to a thread pool.
    # Each fetch keeps its own connection; I/O-bound so GIL not an issue.
    with ThreadPoolExecutor(max_workers=12) as ex:
        fut_risk       = ex.submit(fetch_risk_history)
        fut_risk_raw   = ex.submit(fetch_risk_history_raw)
        fut_aum        = ex.submit(fetch_aum_history)
        fut_pm_pnl     = ex.submit(fetch_pm_pnl_history)
        fut_expo       = ex.submit(fetch_macro_exposure, DATA_STR)
        fut_expo_d1    = ex.submit(fetch_macro_exposure, d1_str)
        fut_expo_d2    = ex.submit(fetch_macro_exposure, d2_str)
        fut_pnl_prod   = ex.submit(fetch_macro_pnl_products, DATA_STR)
        fut_pnl_prod_d1= ex.submit(fetch_macro_pnl_products, d1_str)
        fut_quant_sn   = ex.submit(fetch_quant_single_names, DATA_STR)
        fut_quant_sn_d1= ex.submit(fetch_quant_single_names, d1_str)
        fut_evo_sn     = ex.submit(fetch_evolution_single_names, DATA_STR)
        fut_evo_sn_d1  = ex.submit(fetch_evolution_single_names, d1_str)
        fut_dist       = ex.submit(fetch_pnl_distribution, DATA_STR)
        fut_dist_prev  = ex.submit(fetch_pnl_distribution, d1_str)
        fut_dist_act   = ex.submit(fetch_pnl_actual_by_cut, DATA_STR)
        fut_pa         = ex.submit(fetch_pa_leaves, DATA_STR)
        fut_pa_daily   = ex.submit(fetch_pa_daily_per_product, DATA_STR)
        fut_cdi        = ex.submit(fetch_cdi_returns, DATA_STR)
        fut_alb        = ex.submit(fetch_albatroz_exposure, DATA_STR)
        fut_alb_d1     = ex.submit(fetch_albatroz_exposure, d1_str)
        fut_frontier   = ex.submit(fetch_frontier_mainboard, DATA_STR)
        fut_frontier_expo = ex.submit(fetch_frontier_exposure_data)
        fut_chg        = {
            short: ex.submit(fetch_fund_position_changes, short, DATA_STR, d1_str)
            for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ")
        }

    # ── Resolve results (sequential, with per-task fallback) ──────────────
    df_risk     = fut_risk.result()
    df_risk_raw = fut_risk_raw.result()
    df_aum      = fut_aum.result()
    df_pm_pnl   = fut_pm_pnl.result()
    series      = build_series(df_risk, df_aum, df_risk_raw)
    stop_hist = build_stop_history(df_pm_pnl)

    # PM MTD/YTD from PA leaves — avoids MES column (often NULL in REPORT_ALPHA_ATRIBUTION).
    try:
        df_pa = fut_pa.result()
    except Exception as e:
        print(f"  PA fetch failed ({e})")
        df_pa = None

    if df_pa is not None and not df_pa.empty:
        _macro_pa = df_pa[df_pa["FUNDO"] == "MACRO"]
        df_today = (
            _macro_pa
            .groupby("LIVRO", as_index=False)[["dia_bps", "mtd_bps", "ytd_bps"]]
            .sum()
            .rename(columns={"mtd_bps": "mes_bps"})
        )
    else:
        df_today = pd.DataFrame(columns=["LIVRO", "dia_bps", "mes_bps", "ytd_bps"])

    # Exposure: fall back to D-1 if today's lote hasn't landed yet.
    # Track actual date used so we can label stale sections.
    _expo_raw, _var_raw, _aum_raw = fut_expo.result()
    _expo_d1_raw, _var_d1_raw, _ = fut_expo_d1.result()
    expo_date_label = None  # None = today's data; otherwise the fallback date string
    if _expo_raw.empty and not _expo_d1_raw.empty:
        print(f"  Exposure missing for {DATA_STR} — using {d1_str}")
        expo_date_label = d1_str
        df_expo, df_var, macro_aum = _expo_d1_raw, _var_d1_raw, (_aum_raw or _latest_nav("Galapagos Macro FIM", d1_str) or 1.0)
        # D-1 becomes the new D-0; D-2 becomes the delta reference
        try:
            df_expo_d1, df_var_d1, _ = fut_expo_d2.result()
        except Exception:
            df_expo_d1, df_var_d1 = None, None
    else:
        df_expo, df_var, macro_aum = _expo_raw, _var_raw, _aum_raw
        try:
            df_expo_d1, df_var_d1, _ = _expo_d1_raw, _var_d1_raw, None
        except Exception as e:
            print(f"  D-1 fetch failed ({e}) — Δ columns will be blank")
            df_expo_d1, df_var_d1 = None, None

    # Pnl products: use D-1 if today is empty
    df_pnl_prod = fut_pnl_prod.result()
    if df_pnl_prod.empty:
        df_pnl_prod = fut_pnl_prod_d1.result()

    # PM margem (budget remaining in bps) from stop history
    cur_mes = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
    pm_margem = {}
    for pm, livro in _PM_LIVRO.items():
        if pm not in stop_hist:
            continue
        hist = stop_hist[pm]
        cur_row = hist[hist["mes"] == cur_mes]
        budget = float(cur_row["budget_abs"].iloc[0]) if not cur_row.empty else STOP_BASE
        pnl_row = df_today[df_today["LIVRO"] == livro]
        pnl_mtd = float(pnl_row["mes_bps"].iloc[0]) if not pnl_row.empty else 0.0
        pm_margem[pm] = budget + pnl_mtd

    # Single-names: fall back to D-1 if today's lote is missing
    try:
        df_quant_sn, quant_nav, quant_legs = fut_quant_sn.result()
        if df_quant_sn is None:
            df_quant_sn, quant_nav, quant_legs = fut_quant_sn_d1.result()
            if df_quant_sn is not None: print(f"  QUANT single-name: using {d1_str}")
    except Exception as e:
        print(f"  QUANT single-name fetch failed ({e})")
        df_quant_sn, quant_nav, quant_legs = None, None, None

    try:
        df_evo_sn, evo_nav, evo_legs = fut_evo_sn.result()
        if df_evo_sn is None:
            df_evo_sn, evo_nav, evo_legs = fut_evo_sn_d1.result()
            if df_evo_sn is not None: print(f"  EVOLUTION single-name: using {d1_str}")
    except Exception as e:
        print(f"  EVOLUTION single-name fetch failed ({e})")
        df_evo_sn, evo_nav, evo_legs = None, None, None

    try:
        dist_map      = fut_dist.result()
        dist_map_prev = fut_dist_prev.result()
        dist_actuals  = fut_dist_act.result()
    except Exception as e:
        print(f"  Distribution fetch failed ({e})")
        dist_map, dist_map_prev, dist_actuals = None, None, None

    # df_pa already resolved above (used to build df_today)

    try:
        df_pa_daily = fut_pa_daily.result()
    except Exception as e:
        print(f"  PA daily fetch failed ({e})")
        df_pa_daily = None

    try:
        cdi = fut_cdi.result()
    except Exception as e:
        print(f"  CDI fetch failed ({e})")
        cdi = None

    try:
        df_alb_expo, alb_nav = fut_alb.result()
        if df_alb_expo is None or df_alb_expo.empty:
            df_alb_expo, alb_nav = fut_alb_d1.result()
            if df_alb_expo is not None and not df_alb_expo.empty:
                print(f"  ALBATROZ exposure: using {d1_str}")
    except Exception as e:
        print(f"  ALBATROZ exposure fetch failed ({e})")
        df_alb_expo, alb_nav = None, None

    try:
        df_frontier = fut_frontier.result()
    except Exception as e:
        print(f"  Frontier LO fetch failed ({e})")
        df_frontier = None

    try:
        df_frontier_ibov, df_frontier_smll, df_frontier_sectors = fut_frontier_expo.result()
    except Exception as e:
        print(f"  Frontier exposure fetch failed ({e})")
        df_frontier_ibov = df_frontier_smll = df_frontier_sectors = pd.DataFrame()

    position_changes = {}
    for short, fut in fut_chg.items():
        try:
            position_changes[short] = fut.result()
        except Exception as e:
            print(f"  {short} position changes failed ({e})")
            position_changes[short] = None

    print(f"  ...fetches done in {time.time()-t0:.1f}s")

    # ── Data manifest: what landed and what is stale/missing ─────────────────
    _var_dates = {}
    for td, cfg in FUNDS.items():
        s = series.get(td)
        if s is not None and not s.empty:
            s_avail = s[s["VAL_DATE"] <= DATA]
            if not s_avail.empty:
                _var_dates[cfg["short"]] = s_avail.iloc[-1]["VAL_DATE"]
    data_manifest = {
        "requested_date": DATA_STR,
        "d1_str":         d1_str,
        # PA / PnL
        "pa_ok":          df_pa is not None and not df_pa.empty,
        "pa_has_today":   (df_pa is not None and not df_pa.empty and
                           not df_pa[df_pa["dia_bps"].abs() > 1e-6].empty),
        # VaR / Stress
        "var_dates":      _var_dates,    # short → actual date used (may be D-1)
        # Exposure
        "expo_ok":        df_expo is not None and not df_expo.empty,
        "expo_date":      expo_date_label or DATA_STR,
        # Single-names
        "quant_sn_ok":    df_quant_sn is not None,
        "evo_sn_ok":      df_evo_sn is not None,
        # Distribution
        "dist_today_ok":  bool(dist_map),
        "dist_prev_ok":   bool(dist_map_prev),
        # ALBATROZ
        "alb_expo_ok":    df_alb_expo is not None and not df_alb_expo.empty,
        # Stop monitor
        "stop_ok":        bool(pm_margem),
        "stop_has_pnl":   any(abs(pm_margem.get(pm, STOP_BASE) - STOP_BASE) > 1 for pm in pm_margem),
        # Detail for quality tab
        "expo_rows":      len(df_expo) if df_expo is not None and not df_expo.empty else 0,
        "quant_sn_rows":  len(df_quant_sn) if df_quant_sn is not None else 0,
        "evo_sn_rows":    len(df_evo_sn)   if df_evo_sn   is not None else 0,
        "alb_expo_rows":  len(df_alb_expo) if df_alb_expo is not None and not df_alb_expo.empty else 0,
        "stop_pms":       sorted(pm_margem.keys()) if pm_margem else [],
        "stop_pms_pnl":   [pm for pm, v in (pm_margem or {}).items() if abs(v - STOP_BASE) > 1],
    }

    html = build_html(series, stop_hist, df_today, df_expo, df_var, macro_aum, df_expo_d1, df_var_d1,
                      df_pnl_prod=df_pnl_prod, pm_margem=pm_margem,
                      df_quant_sn=df_quant_sn, quant_nav=quant_nav, quant_legs=quant_legs,
                      df_evo_sn=df_evo_sn, evo_nav=evo_nav, evo_legs=evo_legs,
                      df_pa=df_pa, cdi=cdi, df_pa_daily=df_pa_daily,
                      df_alb_expo=df_alb_expo, alb_nav=alb_nav,
                      df_frontier=df_frontier,
                      df_frontier_ibov=df_frontier_ibov,
                      df_frontier_smll=df_frontier_smll,
                      df_frontier_sectors=df_frontier_sectors,
                      position_changes=position_changes,
                      dist_map=dist_map, dist_map_prev=dist_map_prev, dist_actuals=dist_actuals,
                      expo_date_label=expo_date_label, data_manifest=data_manifest)
    out  = OUT_DIR / f"{DATA_STR}_risk_monitor.html"
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out}")

"""
generate_risk_report.py
Gera o HTML diário de risco MM com barras de range 12m e sparklines 60d.
Usage: python generate_risk_report.py [YYYY-MM-DD]
"""
import sys
import base64
import io
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from glpg_fetch import read_sql

# ── Config ──────────────────────────────────────────────────────────────────
DATA_STR  = sys.argv[1] if len(sys.argv) > 1 else "2026-04-16"
DATA      = pd.Timestamp(DATA_STR)
DATE_1Y   = DATA - pd.DateOffset(years=1)
DATE_60D  = DATA - pd.Timedelta(days=90)  # ~60 business days buffer

FUNDS = {
    "Galapagos Macro FIM":           {"short": "MACRO",      "level": 2, "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Galapagos Quantitativo FIM":    {"short": "QUANT",      "level": 2, "stress_col": "macro", "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Galapagos Evolution FIC FIM CP":{"short": "EVOLUTION",  "level": 3, "stress_col": "spec",  "var_soft": 1.75, "var_hard": 2.50, "stress_soft": 10.5, "stress_hard": 15.0},
}
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
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
    return df

def fetch_aum_history():
    tds = ", ".join(f"'{t}'" for t in FUNDS)
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE", "NAV"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "VAL_DATE" >= DATE '{(DATE_1Y - timedelta(days=5)).date()}'
      AND "TRADING_DESK" IN ({tds})
    """
    df = read_sql(q)
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
    return df

# ── Build series ─────────────────────────────────────────────────────────────
def build_series(df_risk, df_aum):
    result = {}
    for td, cfg in FUNDS.items():
        rsk = df_risk[df_risk["TRADING_DESK"] == td].copy()
        nav = df_aum[df_aum["TRADING_DESK"] == td].copy()
        merged = rsk.merge(nav[["VAL_DATE", "NAV"]], on="VAL_DATE", how="left").dropna(subset=["NAV"])
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
def range_bar_svg(val, vmin, vmax, soft, hard, width=220, height=36) -> str:
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
                 width=300, height=46, soft_mark=None) -> str:
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

    y_mid = 16
    bh    = 12

    parts = [f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">']

    # Background track
    parts.append(f'<rect x="{LPAD}" y="{y_mid-bh//2}" width="{bar_w}" height="{bh}" rx="3" fill="#1e293b"/>')
    # Subtle gain-side tint
    parts.append(f'<rect x="{origin_x:.1f}" y="{y_mid-bh//2}" width="{gain_px:.1f}" height="{bh}" fill="#14241a" opacity="0.6"/>')

    # PnL fill
    if fill_w > 0.5:
        parts.append(f'<rect x="{fill_x:.1f}" y="{y_mid-bh//2}" width="{fill_w:.1f}" height="{bh}" rx="2" fill="{bar_color}" opacity="0.85"/>')

    # Origin tick
    parts.append(f'<line x1="{origin_x:.1f}" y1="{y_mid-bh//2-3}" x2="{origin_x:.1f}" y2="{y_mid+bh//2+3}" stroke="#64748b" stroke-width="1.5"/>')

    # Hard stop line
    if budget_abs > 0:
        parts.append(f'<line x1="{stop_x:.1f}" y1="{y_mid-bh//2-4}" x2="{stop_x:.1f}" y2="{y_mid+bh//2+4}" stroke="#f87171" stroke-width="2.5"/>')
        parts.append(f'<text x="{stop_x:.1f}" y="{y_mid+bh//2+13}" font-size="8" fill="#f87171" font-family="monospace" text-anchor="middle">-{budget_abs:.0f}</text>')
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
          AND "MES"  <> 0
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

def fetch_macro_exposure(date_str: str = DATA_STR) -> tuple:
    """Returns (df_expo, df_var, aum) for the given date."""
    aum_df = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = 'Galapagos Macro FIM'
          AND "VAL_DATE" = DATE '{date_str}'
    """)
    aum = float(aum_df["NAV"].iloc[0]) if not aum_df.empty else 1.0

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

def fetch_quant_single_names(date_str: str = DATA_STR) -> tuple:
    """QUANT single-name L/S exposure with IBOV future (WIN) decomposed into constituents."""
    nav_df = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = 'Galapagos Quantitativo FIM'
          AND "VAL_DATE"     = DATE '{date_str}'
    """)
    if nav_df.empty:
        return None, None, None
    nav = float(nav_df["NAV"].iloc[0])

    direct = read_sql(f"""
        SELECT "PRODUCT" AS ticker, SUM("DELTA") AS direct
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK"              = 'Galapagos Quantitativo FIM'
          AND "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Quantitativo FIM'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "PRODUCT_CLASS"             = 'Equity'
          AND "IS_STOCK"                  = TRUE
        GROUP BY "PRODUCT"
    """)

    fut = read_sql(f"""
        SELECT SUM("DELTA") AS delta
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK"              = 'Galapagos Quantitativo FIM'
          AND "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Quantitativo FIM'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "PRODUCT_CLASS"             = 'IBOVSPFuture'
          AND "PRIMITIVE_CLASS"           = 'Equity'
    """)
    fut_delta = float(fut["delta"].iloc[0]) if not fut.empty and pd.notna(fut["delta"].iloc[0]) else 0.0

    ibov = read_sql(f"""
        SELECT "INSTRUMENT" AS ticker, "VALUE" AS weight
        FROM public."EQUITIES_COMPOSITION"
        WHERE "LIST_NAME" = 'IBOV'
          AND "DATE" = (
                SELECT MAX("DATE") FROM public."EQUITIES_COMPOSITION"
                WHERE "LIST_NAME" = 'IBOV' AND "DATE" <= DATE '{date_str}'
          )
    """)
    ibov["from_win"] = fut_delta * ibov["weight"]

    merged = pd.merge(direct, ibov[["ticker", "from_win"]], on="ticker", how="outer").fillna(0.0)
    merged["net"]     = merged["direct"] + merged["from_win"]
    merged["pct_nav"] = merged["net"] * 100 / nav
    merged = merged[merged["net"].abs() > 1].copy()  # drop dust
    merged = merged.sort_values("pct_nav", key=lambda s: s.abs(), ascending=False)

    return merged, nav, fut_delta

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

def build_quant_single_names_modal(df: pd.DataFrame, nav: float, fut_delta: float) -> str:
    """QUANT single-name L/S — rendered inside a modal overlay (hidden by default)."""
    if df is None or df.empty:
        return ""

    longs  = df[df["net"] > 0].sort_values("net", ascending=False).head(8)
    shorts = df[df["net"] < 0].sort_values("net", ascending=True).head(8)
    gross_l = df.loc[df["net"] > 0, "net"].sum()
    gross_s = df.loc[df["net"] < 0, "net"].sum()
    net     = gross_l + gross_s
    n_total = len(df)
    n_long  = int((df["net"] > 0).sum())
    n_short = int((df["net"] < 0).sum())

    def fmt_row(row):
        net_pct    = row["net"]      * 100 / nav
        direct_pct = row["direct"]   * 100 / nav
        from_pct   = row["from_win"] * 100 / nav
        color      = "var(--up)" if row["net"] > 0 else "var(--down)"
        dir_disp   = f"{direct_pct:+.2f}%" if abs(row["direct"]) > 1 else "—"
        win_disp   = f"{from_pct:+.2f}%"   if abs(row["from_win"]) > 1 else "—"
        return (
            f'<tr>'
            f'<td class="t-name">{row["ticker"]}</td>'
            f'<td class="t-num mono">{dir_disp}</td>'
            f'<td class="t-num mono">{win_disp}</td>'
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
              <th style="text-align:right">From WIN</th>
              <th style="text-align:right">Net %NAV</th>
            </tr></thead>
            <tbody>{body}</tbody>
          </table>
        </div>"""

    win_pct = fut_delta * 100 / nav if nav else 0.0
    return f"""
    <div id="sn-modal" class="modal-backdrop" role="dialog" aria-modal="true" aria-hidden="true">
      <div class="modal">
        <div class="modal-head">
          <div>
            <div class="modal-title">QUANT — Single-Name Exposure</div>
            <div class="modal-sub">após decomposição do WIN pela composição IBOV</div>
          </div>
          <button class="modal-close" onclick="closeSnModal()" aria-label="Fechar">×</button>
        </div>
        <div class="modal-stats mono">
          Gross L <span style="color:var(--up)">{gross_l*100/nav:+.2f}%</span> &nbsp;|&nbsp;
          Gross S <span style="color:var(--down)">{gross_s*100/nav:+.2f}%</span> &nbsp;|&nbsp;
          Net <span style="color:var(--text)">{net*100/nav:+.2f}%</span> &nbsp;|&nbsp;
          {n_total} names ({n_long}L / {n_short}S) &nbsp;|&nbsp;
          WIN leg {win_pct:+.2f}%
        </div>
        <div class="sn-sides">
          {side_table("TOP 8 LONG",  "var(--up)",   longs,  n_long)}
          {side_table("TOP 8 SHORT", "var(--down)", shorts, n_short)}
        </div>
      </div>
    </div>"""

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
        sem_chip = cap_chip("SEM", sem_pct, STOP_SEM) if pm != "CI" else ""
        ano_chip = cap_chip("ANO", ano_pct, STOP_ANO) if pm != "CI" else ""

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

REPORTS = [
    ("risk-monitor", "Risk Monitor"),
    ("stop-monitor", "Stop Monitor"),
    ("exposure",     "Exposure"),
    ("single-name",  "Single-Name"),
    ("distribution", "Distribuição 252d"),
]
FUND_ORDER = ["MACRO", "QUANT", "EVOLUTION"]
FUND_LABELS = {"MACRO": "Macro", "QUANT": "Quantitativo", "EVOLUTION": "Evolution"}

def build_html(series_map: dict, stop_hist: dict = None, df_today=None,
               df_expo=None, df_var=None, macro_aum=None,
               df_expo_d1=None, df_var_d1=None,
               df_pnl_prod=None, pm_margem=None,
               df_quant_sn=None, quant_nav=None, quant_fut_delta=None,
               dist_map=None, dist_map_prev=None, dist_actuals=None) -> str:
    alerts = []
    td_by_short = {cfg["short"]: td for td, cfg in FUNDS.items()}
    sections = []  # list of (fund_short, report_id, html)

    def util_color(u):
        return "var(--up)" if u < 70 else "var(--warn)" if u < 100 else "var(--down)"

    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td is None:
            continue
        cfg = FUNDS[td]
        s = series_map.get(td)
        if s is None or s.empty:
            continue
        today_dt = DATA
        today_row = s[s["VAL_DATE"] == today_dt]
        if today_row.empty:
            continue
        tr = today_row.iloc[0]
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
    alerts_html = ""
    if alerts:
        items = ""
        for fundo, metric, pct, val, util, comment in alerts:
            items += f"""
            <div class="alert-item">
              <div class="alert-header">
                <span class="alert-badge">⚠</span>
                <span class="alert-title">{fundo} — {metric}</span>
                <span class="alert-stats">{val:.2f}% &nbsp;|&nbsp; {pct:.0f}° pct 12m &nbsp;|&nbsp; {util:.0f}% do soft</span>
              </div>
              <div class="alert-body">{comment}</div>
            </div>"""
        alerts_html = f"""
        <div class="alerts-section">
          <div class="alerts-header">Análise — Métricas acima do 80° percentil histórico</div>
          {items}
        </div>"""

    # MACRO-specific sections
    if stop_hist and df_today is not None:
        sections.append(("MACRO", "stop-monitor",
                         build_stop_section(stop_hist, df_today)))
    if df_expo is not None:
        sections.append(("MACRO", "exposure",
                         build_exposure_section(df_expo, df_var, macro_aum, df_expo_d1, df_var_d1, df_pnl_prod, pm_margem)))

    # Distribution 252d sections (per fund) — combined card with Backward/Forward toggle
    if (dist_map or dist_map_prev) and dist_actuals is not None:
        for fs in ["MACRO", "EVOLUTION"]:
            html_sect = build_distribution_card(fs, dist_map or {}, dist_map_prev or {}, dist_actuals)
            if html_sect:
                sections.append((fs, "distribution", html_sect))

    # QUANT single-name: trigger card + modal
    single_name_modal_html = ""
    if df_quant_sn is not None and not df_quant_sn.empty:
        gross_l_pct = df_quant_sn.loc[df_quant_sn["net"] > 0, "net"].sum() * 100 / quant_nav
        gross_s_pct = df_quant_sn.loc[df_quant_sn["net"] < 0, "net"].sum() * 100 / quant_nav
        net_pct     = (df_quant_sn["net"].sum()) * 100 / quant_nav
        sn_trigger_html = f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Single-Name Exposure</span>
            <span class="card-sub">— L/S real após decomposição do WIN</span>
          </div>
          <div class="sn-trigger-row">
            <div class="sn-mini-stats mono">
              L <span style="color:var(--up)">{gross_l_pct:+.2f}%</span> &nbsp;·&nbsp;
              S <span style="color:var(--down)">{gross_s_pct:+.2f}%</span> &nbsp;·&nbsp;
              Net <span style="color:var(--text)">{net_pct:+.2f}%</span>
            </div>
            <button class="btn-accent" onclick="openSnModal()">Abrir detalhamento ↗</button>
          </div>
        </section>"""
        sections.append(("QUANT", "single-name", sn_trigger_html))
        single_name_modal_html = build_quant_single_names_modal(df_quant_sn, quant_nav, quant_fut_delta)

    # Which fund×report combinations exist — used to enable/disable tabs and handle empty states
    available_pairs = {(f, r) for f, r, _ in sections}
    funds_with_data = sorted({f for f, _ in available_pairs}, key=FUND_ORDER.index)
    reports_with_data = [rid for rid, _ in REPORTS if any(rid == r for _, r in available_pairs)]

    # Render all sections wrapped for data-attribute filtering
    sections_html = "".join(
        f'<div class="section-wrap" data-fund="{f}" data-report="{r}">{h}</div>'
        for f, r, h in sections
    )

    # Summary placeholder (Fase 2)
    summary_html = """
    <div class="section-wrap" data-view="summary">
      <section class="card">
        <div class="card-head">
          <span class="card-title">Summary</span>
          <span class="card-sub">— visão consolidada cross-fund</span>
        </div>
        <div style="padding:28px 8px; text-align:center; color:var(--muted); font-size:12.5px; line-height:1.8">
          Em construção (Fase 2).<br/>
          <span style="color:var(--muted); font-size:11px">Aqui virão: heatmap fundos × métricas, top alerts, deltas D/D, orçamentos.</span>
        </div>
      </section>
    </div>"""

    # Mode switcher + sub-tabs
    mode_tabs_html = (
        '<button class="mode-tab" data-mode="summary" onclick="selectMode(\'summary\')">Summary</button>'
        '<button class="mode-tab" data-mode="fund"    onclick="selectMode(\'fund\')">Por Fundo</button>'
        '<button class="mode-tab" data-mode="report"  onclick="selectMode(\'report\')">Por Report</button>'
    )
    fund_subtabs_html = "".join(
        f'<button class="tab" data-target="{s}" onclick="selectFund(\'{s}\')">{s}</button>'
        for s in funds_with_data
    )
    report_subtabs_html = "".join(
        f'<button class="tab" data-target="{rid}" onclick="selectReport(\'{rid}\')">{label}</button>'
        for rid, label in REPORTS if rid in reports_with_data
    )

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
    max-width:1280px; margin:14px auto 0; padding:0 22px;
    display:flex; align-items:center; gap:14px; flex-wrap:wrap;
  }}
  .navrow-label {{
    font-size:10px; color:var(--muted); letter-spacing:.18em; text-transform:uppercase;
  }}

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

  /* Report mode: merge sections of the same report into one visual container */
  body[data-mode="report"] #sections-container {{
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 12px; padding: 4px 18px; margin-top: 4px;
  }}
  body[data-mode="report"] #sections-container .section-wrap > .card {{
    background: transparent; border: 0;
    padding: 14px 0 16px; margin-bottom: 0;
    border-bottom: 1px solid var(--line);
    border-radius: 0;
  }}
  body[data-mode="report"] #sections-container .section-wrap:last-of-type > .card,
  body[data-mode="report"] #sections-container .section-wrap > .card:last-child {{
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

  /* QUANT single-name trigger */
  .sn-trigger-row {{
    display:flex; align-items:center; justify-content:space-between;
    padding:4px 2px; gap:16px;
  }}
  .sn-mini-stats {{ font-size:12.5px; color:var(--muted); }}

  /* Modal */
  .modal-backdrop {{
    position:fixed; inset:0; background:rgba(5,8,12,.72);
    backdrop-filter:blur(6px);
    display:none; align-items:center; justify-content:center;
    z-index:200; padding:24px;
  }}
  .modal-backdrop.open {{ display:flex; }}
  .modal {{
    background:var(--panel); border:1px solid var(--line-2);
    border-radius:14px; padding:20px 24px;
    width:min(900px, 100%); max-height:85vh; overflow:auto;
    box-shadow:0 20px 60px -10px rgba(0,0,0,.6), 0 0 0 1px rgba(0,113,187,.15);
  }}
  .modal-head {{
    display:flex; align-items:start; justify-content:space-between;
    padding-bottom:10px; margin-bottom:14px;
    border-bottom:1px solid var(--line);
  }}
  .modal-title {{
    font-size:13px; letter-spacing:.18em; text-transform:uppercase;
    color:var(--text); font-weight:700;
  }}
  .modal-sub {{ font-size:11px; color:var(--muted); margin-top:2px; }}
  .modal-close {{
    background:transparent; border:1px solid var(--line);
    color:var(--muted); font-size:18px; font-weight:300;
    width:30px; height:30px; border-radius:8px; cursor:pointer;
    display:grid; place-items:center;
  }}
  .modal-close:hover {{ color:var(--text); border-color:var(--line-2); }}
  .modal-stats {{ color:var(--muted); font-size:12px; margin-bottom:16px; }}
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
  // --- Navigation state: 3 modes (summary / fund / report) driven by URL hash ---
  function parseHash() {{
    var h = (location.hash || '').slice(1);
    if (!h || h === 'summary') return {{ mode: 'summary' }};
    var m;
    if ((m = h.match(/^fund=(.*)$/)))   return {{ mode: 'fund',   sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^report=(.*)$/))) return {{ mode: 'report', sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    return {{ mode: 'summary' }};
  }}
  function setHash(mode, sel) {{
    var h = (mode === 'summary') ? '' : (mode + '=' + encodeURIComponent(sel));
    if (history.replaceState) history.replaceState(null, '', h ? ('#' + h) : location.pathname + location.search);
    else location.hash = h;
  }}
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
    // Section visibility
    document.querySelectorAll('.section-wrap').forEach(function(el) {{
      var show = false;
      if (mode === 'summary')      show = el.dataset.view   === 'summary';
      else if (mode === 'fund')    show = el.dataset.fund   === sel;
      else if (mode === 'report')  show = el.dataset.report === sel;
      el.style.display = show ? '' : 'none';
    }});
    // Empty-state for fund×report with no data
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
  window.openSnModal = function() {{
    var m = document.getElementById('sn-modal');
    if (m) {{ m.classList.add('open'); m.setAttribute('aria-hidden','false'); }}
  }};
  window.closeSnModal = function() {{
    var m = document.getElementById('sn-modal');
    if (m) {{ m.classList.remove('open'); m.setAttribute('aria-hidden','true'); }}
  }};
  document.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') window.closeSnModal();
  }});
  document.addEventListener('click', function(e) {{
    var m = document.getElementById('sn-modal');
    if (m && e.target === m) window.closeSnModal();
  }});
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
    </div>
  </div>
</header>
<div class="navrow">
  <nav class="sub-tabs" data-for="fund"   role="tablist">{fund_subtabs_html}</nav>
  <nav class="sub-tabs" data-for="report" role="tablist">{report_subtabs_html}</nav>
</div>
<div class="subtitle">Data-base: <span class="mono">{DATA_STR}</span> &nbsp;·&nbsp; gerado em <span class="mono">{date.today().isoformat()}</span></div>
<main>
  {summary_html}
  <div id="sections-container">
    {sections_html}
  </div>
  <div id="empty-state" style="display:none">Sem dados para essa combinação de fundo × report.</div>
  {alerts_html}
</main>
{single_name_modal_html}
</body>
</html>"""
    return html

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Fetching data for {DATA_STR}...")
    df_risk    = fetch_risk_history()
    df_aum     = fetch_aum_history()
    df_pm_pnl  = fetch_pm_pnl_history()
    series     = build_series(df_risk, df_aum)
    stop_hist  = build_stop_history(df_pm_pnl)

    # Today's MTD + YTD per PM for the bar
    q_today = f"""
    SELECT "LIVRO",
           SUM("DIA")  * 10000 AS dia_bps,
           SUM("MES")  * 10000 AS mes_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'MACRO'
      AND "DATE" = DATE '{DATA_STR}'
      AND "MES" <> 0
      AND "LIVRO" IN ('CI','Macro_LF','Macro_JD','Macro_RJ')
    GROUP BY "LIVRO"
    """
    q_ytd = f"""
    SELECT "LIVRO", SUM("DIA") * 10000 AS ytd_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'MACRO'
      AND "DATE" >= DATE_TRUNC('year', DATE '{DATA_STR}')
      AND "DATE" <= DATE '{DATA_STR}'
      AND "LIVRO" IN ('CI','Macro_LF','Macro_JD','Macro_RJ')
    GROUP BY "LIVRO"
    """
    from glpg_fetch import read_sql
    df_today = read_sql(q_today).merge(read_sql(q_ytd), on="LIVRO", how="left")

    df_expo, df_var, macro_aum = fetch_macro_exposure(DATA_STR)
    d1_str = _prev_bday(DATA_STR)
    print(f"Fetching D-1 exposure ({d1_str})...")
    try:
        df_expo_d1, df_var_d1, _ = fetch_macro_exposure(d1_str)
    except Exception as e:
        print(f"  D-1 fetch failed ({e}) — Δ columns will be blank")
        df_expo_d1, df_var_d1 = None, None

    df_pnl_prod = fetch_macro_pnl_products(DATA_STR)

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

    try:
        df_quant_sn, quant_nav, quant_fut_delta = fetch_quant_single_names(DATA_STR)
    except Exception as e:
        print(f"  QUANT single-name fetch failed ({e})")
        df_quant_sn, quant_nav, quant_fut_delta = None, None, None

    try:
        dist_map = fetch_pnl_distribution(DATA_STR)
        dist_map_prev = fetch_pnl_distribution(d1_str)
        dist_actuals = fetch_pnl_actual_by_cut(DATA_STR)
    except Exception as e:
        print(f"  Distribution fetch failed ({e})")
        dist_map, dist_map_prev, dist_actuals = None, None, None

    html = build_html(series, stop_hist, df_today, df_expo, df_var, macro_aum, df_expo_d1, df_var_d1,
                      df_pnl_prod=df_pnl_prod, pm_margem=pm_margem,
                      df_quant_sn=df_quant_sn, quant_nav=quant_nav, quant_fut_delta=quant_fut_delta,
                      dist_map=dist_map, dist_map_prev=dist_map_prev, dist_actuals=dist_actuals)
    out  = OUT_DIR / f"{DATA_STR}_risk_monitor.html"
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out}")

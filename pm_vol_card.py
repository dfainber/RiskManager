r"""Standalone card: realized vol per PM + utilização do orçamento de risco.

Replica a lógica da aba VOl_PM de F:\Bloomberg\Pot\Relatorio_Performance_Atr.xlsb.xlsx
e extende para todos os PMs MACRO (CI, LF, JD, RJ).

Métricas:
  - VOL_21D / VOL_30D / VOL_60D = desvio padrão rolling do PnL diário (em bps)
  - Sharpe YTD  = (PnL YTD anualizado) / (VOL_30D × √252)
  - MTD / Margem = P&L acumulado no mês vs orçamento de stop restante

Output: data/morning-calls/pm_vol_card_<DATA>.html
"""

from __future__ import annotations
import sys, math, re
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from glpg_fetch import read_sql
from risk_config import _MACRO_DESK

# ── Config ──────────────────────────────────────────────────────────────────
def _parse_date_arg(s: str) -> str:
    """Validate a CLI date string. Must be YYYY-MM-DD and a real calendar date."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        sys.exit(f"Error: date must be YYYY-MM-DD, got {s!r}")
    try:
        pd.Timestamp(s)
    except ValueError:
        sys.exit(f"Error: invalid date {s!r}")
    return s


DATA_STR   = _parse_date_arg(sys.argv[1]) if len(sys.argv) > 1 else datetime.today().strftime("%Y-%m-%d")
STOP_BASE  = 63.0   # bps/month (base hard stop)
CI_HARD    = 233.0  # CI hard stop
CI_SOFT    = 150.0  # CI soft mark
Z95        = 1.645

_PM_LIVRO: dict[str, str] = {
    "CI":  "CI",
    "LF":  "Macro_LF",
    "JD":  "Macro_JD",
    "RJ":  "Macro_RJ",
}
_PM_LABEL: dict[str, str] = {
    "CI":  "CI (Comitê)",
    "LF":  "LF — Luiz Felipe",
    "JD":  "JD — Joca Dib",
    "RJ":  "RJ — Rodrigo Jafet",
}
_PM_COLOR: dict[str, str] = {
    "CI":  "#f0f4ff",   # off-white (branco)
    "LF":  "#93c5fd",   # light blue (azul claro)
    "JD":  "#2563eb",   # dark blue  (azul escuro)
    "RJ":  "#2dd4bf",   # teal       (azul-esverdeado — outra cor)
}

# ── Data fetch ───────────────────────────────────────────────────────────────
def _livros_sql() -> str:
    return ", ".join(f"'{v}'" for v in _PM_LIVRO.values())

def fetch_daily_pnl(date_str: str, lookback_days: int = 400) -> pd.DataFrame:
    livros = _livros_sql()
    df = read_sql(f"""
        SELECT "DATE" AS val_date,
               "LIVRO",
               SUM("DIA") * 10000 AS pnl_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'MACRO'
          AND "DATE" >= DATE '{date_str}' - INTERVAL '{lookback_days} days'
          AND "DATE" <= DATE '{date_str}'
          AND "LIVRO" IN ({livros})
        GROUP BY "DATE", "LIVRO"
        ORDER BY "DATE"
    """)
    if not df.empty:
        df["val_date"] = pd.to_datetime(df["val_date"])
        df["pnl_bps"]  = df["pnl_bps"].astype(float)
    return df


def fetch_ytd_pnl(date_str: str) -> pd.DataFrame:
    year_start = f"{pd.Timestamp(date_str).year}-01-01"
    livros = _livros_sql()
    return read_sql(f"""
        SELECT "LIVRO",
               SUM("DIA") * 10000 AS ytd_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'MACRO'
          AND "DATE" >= DATE '{year_start}'
          AND "DATE" <= DATE '{date_str}'
          AND "LIVRO" IN ({livros})
        GROUP BY "LIVRO"
    """)


def fetch_monthly_pnl(date_str: str) -> pd.DataFrame:
    year_start = f"{pd.Timestamp(date_str).year}-01-01"
    livros = _livros_sql()
    df = read_sql(f"""
        SELECT DATE_TRUNC('month', "DATE") AS mes,
               "LIVRO",
               SUM("DIA") * 10000 AS pnl_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'MACRO'
          AND "DATE" >= DATE '{year_start}'
          AND "DATE" <= DATE '{date_str}'
          AND "LIVRO" IN ({livros})
        GROUP BY DATE_TRUNC('month', "DATE"), "LIVRO"
        ORDER BY "LIVRO", mes
    """)
    if not df.empty:
        s = pd.to_datetime(df["mes"], utc=True)
        df["mes"] = s.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
    return df


# ── Chart analytics ─────────────────────────────────────────────────────────
def fetch_nav_series(date_str: str, lookback_days: int = 400) -> pd.DataFrame:
    """Historical daily NAV for Galapagos Macro FIM (needed to normalise lote VaR to bps)."""
    df = read_sql(f"""
        SELECT "VAL_DATE", SUM("NAV") AS nav
        FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = '{_MACRO_DESK}'
          AND "VAL_DATE" >= DATE '{date_str}' - INTERVAL '{lookback_days} days'
          AND "VAL_DATE" <= DATE '{date_str}'
        GROUP BY "VAL_DATE"
        ORDER BY "VAL_DATE"
    """)
    if not df.empty:
        df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
        df["nav"] = df["nav"].astype(float)
    return df


def fetch_lote_pm_var_series(date_str: str, lookback_days: int = 400,
                              df_nav: pd.DataFrame | None = None) -> dict:
    """Historical lote parametric VaR per PM → annualized implied vol (bps/year).
    Source: LOTE_FUND_STRESS_RPM, TREE=Main_Macro_Ativos, LEVEL=10 (per-book).
    PARAMETRIC_VAR is in BRL → divide by NAV → bps → / Z95 × sqrt(252) = annualized vol.
    """
    df_var = read_sql(f"""
        SELECT "VAL_DATE", "BOOK", SUM("PARAMETRIC_VAR") AS var_brl
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "TRADING_DESK" = '{_MACRO_DESK}'
          AND "TREE"         = 'Main_Macro_Ativos'
          AND "LEVEL"        = 10
          AND "VAL_DATE"     >= DATE '{date_str}' - INTERVAL '{lookback_days} days'
          AND "VAL_DATE"     <= DATE '{date_str}'
        GROUP BY "VAL_DATE", "BOOK"
        ORDER BY "VAL_DATE"
    """)
    if df_var.empty:
        return {}
    df_var["VAL_DATE"] = pd.to_datetime(df_var["VAL_DATE"])
    df_var["var_brl"]  = df_var["var_brl"].astype(float)

    if df_nav is None:
        df_nav = fetch_nav_series(date_str, lookback_days)
    if df_nav.empty:
        return {}

    nav_s = df_nav.set_index("VAL_DATE")["nav"].sort_index()

    result: dict = {}
    for pm in ("CI", "LF", "JD", "RJ"):
        mask   = df_var["BOOK"].str.startswith(f"{pm}_") | (df_var["BOOK"] == pm)
        pm_var = df_var[mask].groupby("VAL_DATE")["var_brl"].sum()
        if pm_var.empty:
            continue
        nav_aligned = nav_s.reindex(pm_var.index, method="ffill")
        valid       = nav_aligned > 0
        var_bps_1d  = abs(pm_var[valid]) * 10000 / nav_aligned[valid]
        vol_ann     = var_bps_1d / Z95 * math.sqrt(252)
        result[pm]  = {
            "dates":        [d.strftime("%Y-%m-%d") for d in vol_ann.index],
            "vol_estimada": [round(float(v), 1) for v in vol_ann.values],
        }
    return result


def compute_vol_series(df_daily: pd.DataFrame, windows: tuple = (21, 30, 60),
                       lote_pm_var: dict | None = None) -> dict:
    """Rolling annualized vol per PM for the history chart.
    Returns {pm: {dates:[...], vol_21d:[...], vol_30d:[...], vol_60d:[...], vol_estimada:[...]}}
    vol_estimada comes from lote_pm_var (LOTE_FUND_STRESS_RPM parametric); None where unavailable.
    """
    livro_to_pm = {v: k for k, v in _PM_LIVRO.items()}
    result: dict = {}
    for livro, pm in livro_to_pm.items():
        sub = df_daily[df_daily["LIVRO"] == livro].sort_values("val_date").copy()
        if sub.empty:
            continue
        pnl   = sub["pnl_bps"].values
        dates = [d.strftime("%Y-%m-%d") for d in sub["val_date"]]
        series: dict = {"dates": dates}
        pnl_s = pd.Series(pnl)
        for w in windows:
            rolled = pnl_s.rolling(w, min_periods=w).std() * math.sqrt(252)
            series[f"vol_{w}d"] = [round(v, 1) if pd.notna(v) else None for v in rolled]
        result[pm] = series

    # Merge lote vol_estimada, aligned to PnL dates via forward fill (lote may lag 1d)
    if lote_pm_var:
        for pm, pm_series in result.items():
            pnl_idx = pd.DatetimeIndex(pm_series["dates"])
            if pm in lote_pm_var and lote_pm_var[pm].get("dates"):
                lote_s  = pd.Series(
                    lote_pm_var[pm]["vol_estimada"],
                    index=pd.DatetimeIndex(lote_pm_var[pm]["dates"]),
                )
                aligned = lote_s.reindex(pnl_idx, method="ffill")
                pm_series["vol_estimada"] = [
                    round(float(v), 1) if not pd.isna(v) else None
                    for v in aligned
                ]
            else:
                pm_series["vol_estimada"] = [None] * len(pnl_idx)
    return result


def compute_quintile_analysis(df_daily: pd.DataFrame, window: int = 21) -> dict:
    """PnL-vs-risk quintile breakdown per PM.

    For each day i (after burn-in of `window` days):
      risk = vol_21d at day i (rolling std of last `window` days)
      pnl  = next-day PnL (day i+1, 1-day lag — no look-ahead)

    Returns {pm: {"summary": [...], "raw": [{vol_q, pnl_q}]}} where:
      summary  = bar chart data (median/p25/p75 per vol quintile)
      raw      = every (vol_q, pnl_q) pair for the matrix cross-tabulation
    """
    livro_to_pm = {v: k for k, v in _PM_LIVRO.items()}
    result: dict = {}
    for livro, pm in livro_to_pm.items():
        sub = df_daily[df_daily["LIVRO"] == livro].sort_values("val_date").copy()
        pnl = sub["pnl_bps"].values
        if len(pnl) < window + 2:
            continue
        rows = []
        for i in range(window - 1, len(pnl) - 1):
            vol_start   = float(np.std(pnl[i + 1 - window : i + 1], ddof=1))
            pnl_fwd     = float(pnl[i + 1])   # next single day
            window_date = sub.iloc[i + 1]["val_date"].strftime("%Y-%m-%d")
            rows.append({"vol": vol_start, "pnl_fwd": pnl_fwd, "date": window_date})
        if len(rows) < 10:
            continue
        obs = pd.DataFrame(rows)
        # rank(method="first") breaks all ties → qcut always produces exactly 5 bins
        obs["vol_q"] = pd.qcut(obs["vol"].rank(method="first"),     q=5, labels=[1, 2, 3, 4, 5])
        obs["pnl_q"] = pd.qcut(obs["pnl_fwd"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5])

        # Summary stats per vol quintile (distribution chart — uses full history)
        summary = []
        for q in [1, 2, 3, 4, 5]:
            grp     = obs[obs["vol_q"] == q]["pnl_fwd"]
            vol_grp = obs[obs["vol_q"] == q]["vol"]
            if grp.empty:
                continue
            summary.append({
                "label":   f"Q{q}",
                "vol_ann": round(float(vol_grp.median()) * math.sqrt(252), 1),
                "median":  round(float(grp.median()), 1),
                "p25":     round(float(grp.quantile(0.25)), 1),
                "p75":     round(float(grp.quantile(0.75)), 1),
                "mean":    round(float(grp.mean()), 1),
                "n":       int(len(grp)),
            })

        # Raw observations: include date + pnl_fwd for aggregate chart + matrix
        obs_clean = obs.dropna(subset=["vol_q", "pnl_q"])
        raw = [{"vol_q":   int(r.vol_q),
                "pnl_q":   int(r.pnl_q),
                "date":    r.date,
                "pnl_fwd": round(float(r.pnl_fwd), 1)}
               for r in obs_clean.itertuples(index=False)]

        result[pm] = {"summary": summary, "raw": raw}
    return result


# ── Classification ───────────────────────────────────────────────────────────
# Two distinct problems + two OK states
# vol_ratio  = vol_21d / budget_implied_vol   (1.0 = exactly at budget vol)
# pnl_sigma  = pnl_mtd / (vol_30d × sqrt(days_mtd))
_VOL_LOW  = 0.70   # below this fraction of budget-implied → "low risk"
_VOL_HIGH = 1.00   # above this → "high risk"
_SIGMA_BAD = -0.50  # worse than this MTD sigma → "bad PnL"

def classify_pm(vol_ratio: float, pnl_sigma: float) -> tuple[str, str]:
    """Return (code, label) for the PM regime.
    Codes: 'ok', 'watch', 'sub', 'excess'
    """
    if math.isnan(vol_ratio) or math.isnan(pnl_sigma):
        return "ok", "—"
    bad_pnl  = pnl_sigma < _SIGMA_BAD
    low_vol  = vol_ratio < _VOL_LOW
    high_vol = vol_ratio >= _VOL_HIGH
    if bad_pnl and high_vol:
        return "excess", "🔴 Risco Excessivo"
    if bad_pnl:
        # low or mid vol + bad PnL: not burning risk, but alpha quality is bad
        return "sub",    "🟠 Subutilização"
    if high_vol:
        return "watch",  "🟡 Vigiar"
    return "ok", "🟢 OK"


# ── Vol computation ──────────────────────────────────────────────────────────
def compute_rolling_vol(df_daily: pd.DataFrame, date_str: str,
                        windows=(21, 30, 60)) -> dict[str, dict]:
    """Rolling std, rolling Sharpe-21d, vol trend, and MTD sigma per PM."""
    livro_to_pm = {v: k for k, v in _PM_LIVRO.items()}
    ts = pd.Timestamp(date_str)
    month_start = ts.replace(day=1)
    result: dict[str, dict] = {}
    for livro, pm in livro_to_pm.items():
        sub_df = df_daily[df_daily["LIVRO"] == livro].sort_values("val_date").copy()
        if sub_df.empty:
            result[pm] = {}
            continue
        pnl = sub_df["pnl_bps"].values
        row: dict[str, float] = {}

        # Rolling vol at each window
        for w in windows:
            if len(pnl) >= w:
                row[f"vol_{w}d"] = float(np.std(pnl[-w:], ddof=1))
            else:
                row[f"vol_{w}d"] = float(np.std(pnl, ddof=1)) if len(pnl) > 1 else float("nan")
        row["vol_full"] = float(np.std(pnl, ddof=1)) if len(pnl) > 1 else float("nan")
        row["n"] = len(pnl)

        # Rolling Sharpe 21d: sum(pnl[-21]) / (std[-21] × sqrt(21))
        if len(pnl) >= 21:
            w21 = pnl[-21:]
            s21 = float(np.std(w21, ddof=1))
            row["sharpe_21d"] = float(np.sum(w21)) / (s21 * math.sqrt(21)) if s21 > 0 else float("nan")
        else:
            row["sharpe_21d"] = float("nan")

        # Vol trend: vol_21d / vol_60d — rising risk > 1
        v21 = row.get("vol_21d", float("nan"))
        v60 = row.get("vol_60d", float("nan"))
        row["vol_trend"] = v21 / v60 if (not math.isnan(v21) and not math.isnan(v60) and v60 > 0) else float("nan")

        # MTD PnL in sigmas: pnl_mtd / (vol_30d × sqrt(days_traded_mtd))
        mtd_mask = sub_df["val_date"] >= pd.Timestamp(month_start)
        mtd_pnl  = sub_df.loc[mtd_mask, "pnl_bps"].values
        days_mtd = max(1, len(mtd_pnl))
        pnl_mtd_sum = float(np.sum(mtd_pnl)) if len(mtd_pnl) > 0 else float("nan")
        v30 = row.get("vol_30d", float("nan"))
        if not math.isnan(pnl_mtd_sum) and not math.isnan(v30) and v30 > 0:
            row["pnl_sigma_mtd"] = pnl_mtd_sum / (v30 * math.sqrt(days_mtd))
        else:
            row["pnl_sigma_mtd"] = float("nan")

        result[pm] = row
    return result


def compute_budgets(df_monthly: pd.DataFrame, date_str: str) -> dict[str, dict]:
    """Reconstruct monthly carry budget per PM, return {pm: {budget, mtd, margem}}."""
    cur_mes = pd.Timestamp(date_str).to_period("M").to_timestamp()
    livro_to_pm = {v: k for k, v in _PM_LIVRO.items()}
    result: dict[str, dict] = {}
    for livro, pm in livro_to_pm.items():
        sub = df_monthly[df_monthly["LIVRO"] == livro].sort_values("mes").copy()
        if sub.empty:
            result[pm] = {"budget": STOP_BASE if pm != "CI" else CI_HARD, "mtd": 0.0, "margem": STOP_BASE}
            continue
        budget = CI_HARD if pm == "CI" else STOP_BASE
        ytd = 0.0
        for _, r in sub.iterrows():
            if r["mes"] == cur_mes:
                # current month: margem = budget + MTD (MTD is negative when losing)
                result[pm] = {
                    "budget": budget,
                    "mtd":    float(r["pnl_bps"]),
                    "margem": max(0.0, budget + float(r["pnl_bps"])),
                }
                break
            pnl = float(r["pnl_bps"])
            ytd += pnl
            if pm == "CI":
                continue
            # carry step
            if pnl > 0:
                budget = STOP_BASE
            else:
                loss   = -pnl
                within = min(loss, budget)
                excess = max(0.0, loss - budget)
                budget = max(0.0, budget - within * 0.5 - excess)
        else:
            # date is within current month, last row IS current month
            if pm not in result:
                result[pm] = {"budget": budget, "mtd": 0.0, "margem": max(0.0, budget)}
    return result


# ── HTML rendering ───────────────────────────────────────────────────────────
def _cell_color(val: float, warn_thr: float, bad_thr: float, reverse: bool = False) -> str:
    """Return inline color style. reverse=True → high=green (for Sharpe)."""
    if math.isnan(val):
        return ""
    if not reverse:
        if   val >= bad_thr:  return "color:var(--down)"
        elif val >= warn_thr: return "color:var(--warn)"
        else:                 return "color:var(--up)"
    else:
        if   val >= bad_thr:  return "color:var(--up)"
        elif val >= warn_thr: return "color:var(--warn)"
        else:                 return "color:var(--down)" if val < 0 else ""


def _fmt(v: float | None, decimals: int = 1, suffix: str = "") -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{v:,.{decimals}f}{suffix}"


def build_html_card(
    vol_map: dict[str, dict],
    ytd_df: pd.DataFrame,
    budget_map: dict[str, dict],
    date_str: str,
    vol_series: dict | None = None,
    quintile_data: dict | None = None,
) -> str:
    ytd_lookup = dict(zip(ytd_df["LIVRO"], ytd_df["ytd_bps"].astype(float))) if not ytd_df.empty else {}

    rows_html = []
    for pm in ("CI", "LF", "JD", "RJ"):
        vol   = vol_map.get(pm, {})
        bud   = budget_map.get(pm, {})
        livro = _PM_LIVRO[pm]

        v21        = vol.get("vol_21d",      float("nan"))
        v30        = vol.get("vol_30d",      float("nan"))
        v60        = vol.get("vol_60d",      float("nan"))
        sharpe_21  = vol.get("sharpe_21d",   float("nan"))
        vol_trend  = vol.get("vol_trend",    float("nan"))
        sig_mtd    = vol.get("pnl_sigma_mtd",float("nan"))

        ytd_bps = ytd_lookup.get(livro, float("nan"))
        budget  = bud.get("budget", STOP_BASE if pm != "CI" else CI_HARD)
        mtd_bps = bud.get("mtd",    float("nan"))
        margem  = bud.get("margem", float("nan"))

        # Annualized Sharpe (YTD)
        vol_ann    = v30 * math.sqrt(252) if not math.isnan(v30) else float("nan")
        sharpe_ytd = ytd_bps / vol_ann if (not math.isnan(vol_ann) and vol_ann > 0) else float("nan")

        # Budget-implied daily 1σ
        vol_budget = budget / (Z95 * math.sqrt(21.0))
        vol_ratio  = v21 / vol_budget if (not math.isnan(v21) and vol_budget > 0) else float("nan")

        # Classification
        regime_code, regime_label = classify_pm(vol_ratio, sig_mtd)

        # Row class for background highlight on problem rows
        row_cls = {"excess": "row-excess", "sub": "row-sub"}.get(regime_code, "")

        # Cell colors
        c21  = _cell_color(v21, vol_budget * _VOL_LOW, vol_budget * _VOL_HIGH)
        c30  = _cell_color(v30, vol_budget * _VOL_LOW, vol_budget * _VOL_HIGH)
        cs21 = _cell_color(sharpe_21, 0.2, 0.6, reverse=True) if not math.isnan(sharpe_21) else ""
        csyt = _cell_color(sharpe_ytd, 0.2, 0.6, reverse=True) if not math.isnan(sharpe_ytd) else ""
        # PnL sigma: > 0 = good, < -0.5 = bad, < -1 = very bad  (reverse: high is good)
        csig = _cell_color(sig_mtd, _SIGMA_BAD, -1.0, reverse=True) if not math.isnan(sig_mtd) else ""
        # Vol trend: > 1.2 = risk rising (warn), > 1.5 = red
        ctrd = _cell_color(vol_trend, 1.2, 1.5) if not math.isnan(vol_trend) else ""
        # Margem
        consumed = budget - margem if not math.isnan(margem) else float("nan")
        cmar = _cell_color(consumed, budget * 0.3, budget * 0.7)

        # Sigma MTD display: signed with + prefix
        sig_str = f"{sig_mtd:+.2f}σ" if not math.isnan(sig_mtd) else "—"
        # Vol trend display
        trd_str = f"{vol_trend:.2f}×" if not math.isnan(vol_trend) else "—"

        rows_html.append(f"""
        <tr class="{row_cls}">
          <td class="ta-l"><strong>{pm}</strong><br><small style="color:#888">{_PM_LABEL[pm]}</small></td>
          <td class="ta-l">{regime_label}</td>
          <td style="{c21}">{_fmt(v21)}</td>
          <td style="{c30}">{_fmt(v30)}</td>
          <td style="{ctrd}">{trd_str}</td>
          <td style="{cs21}">{_fmt(sharpe_21, 2)}</td>
          <td style="{csig}">{sig_str}</td>
          <td style="{csyt}">{_fmt(sharpe_ytd, 2)}</td>
          <td>{_fmt(budget, 0, " bps")}</td>
          <td style="{cmar}">{_fmt(margem, 0, " bps")}</td>
        </tr>""")

    rows = "\n".join(rows_html)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    import json
    vol_json         = json.dumps(vol_series or {})
    pm_color_json    = json.dumps(_PM_COLOR)
    pm_label_json    = json.dumps(_PM_LABEL)
    # Split quintile data into summary (bar chart) and raw (matrix)
    qd = quintile_data or {}
    quintile_summary_json = json.dumps({pm: v["summary"] for pm, v in qd.items() if "summary" in v})
    quintile_raw_json     = json.dumps({pm: v["raw"]     for pm, v in qd.items() if "raw"     in v})

    # Budget-implied annualized vol per PM (horizontal reference lines on vol chart)
    budget_vol_ann = {}
    for pm in _PM_LIVRO:
        bud = budget_map.get(pm, {})
        b   = bud.get("budget", CI_HARD if pm == "CI" else STOP_BASE)
        budget_vol_ann[pm] = round(b / (Z95 * math.sqrt(21.0)) * math.sqrt(252), 1)
    budget_vol_json = json.dumps(budget_vol_ann)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>PM Vol Card — {date_str}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg:#0f1117; --bg2:#16191f; --border:#2a2d35;
    --text:#e8eaf0; --text2:#9ba3b0;
    --up:#3ddc84; --down:#ff5f5f; --warn:#ffc107; --accent:#4e9af1;
  }}
  body {{ background:var(--bg); color:var(--text); font:14px/1.5 'Inter',sans-serif; margin:24px; }}
  h1 {{ font-size:18px; color:var(--accent); margin-bottom:4px; }}
  .meta {{ font-size:12px; color:var(--text2); margin-bottom:18px; }}

  /* chart card */
  .chart-card {{ background:var(--bg2); border:1px solid var(--border); border-radius:8px;
                 padding:16px; margin-bottom:24px; }}
  .chart-card h2 {{ font-size:14px; color:var(--text2); font-weight:600; margin:0 0 12px; }}
  .chart-controls {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:12px; align-items:center; }}
  .chart-controls label {{ font-size:12px; color:var(--text2); margin-right:4px; }}
  .tgl-btn {{ padding:4px 11px; border-radius:4px; border:1px solid #444;
               background:transparent; color:#555; cursor:pointer; font-size:12px; transition:all .15s; }}
  .tgl-btn.on {{ color:#fff; border-width:1px; }}
  .view-btn {{ padding:3px 12px; border-radius:4px; border:1px solid var(--border);
               background:transparent; color:var(--text2); cursor:pointer; font-size:12px; }}
  .view-btn.active {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
  .chart-wrap {{ position:relative; height:320px; }}

  /* table */
  .legend {{ display:flex; gap:20px; margin-bottom:14px; font-size:12px; color:var(--text2); flex-wrap:wrap; }}
  .legend span {{ display:flex; align-items:center; gap:5px; }}
  table {{ border-collapse:collapse; width:100%; }}
  th, td {{ padding:7px 12px; border-bottom:1px solid var(--border); white-space:nowrap; }}
  th {{ background:var(--bg2); color:var(--text2); font-size:12px; font-weight:600;
        text-align:center; cursor:pointer; user-select:none; }}
  td {{ text-align:right; }}
  td.ta-l {{ text-align:left; }}
  tr.row-excess td {{ background:rgba(255,95,95,0.07); }}
  tr.row-sub    td {{ background:rgba(255,165,0,0.07); }}
  tr:hover td {{ background:rgba(255,255,255,0.05); }}
  .footnote {{ font-size:11px; color:var(--text2); margin-top:14px; line-height:1.8; }}
</style>
</head>
<body>
<h1>Utilização de Risco por PM — MACRO</h1>
<div class="meta">Data: {date_str} &nbsp;·&nbsp; Gerado: {generated}</div>

<!-- ─── Unified chart ──────────────────────────────────────────────────────── -->
<div class="chart-card">
  <h2 id="chart-title">Vol Histórica Anualizada (bps/ano)</h2>
  <div class="chart-controls">
    <label>View:</label>
    <button class="view-btn active" data-view="vol"      onclick="switchView('vol')">Vol Histórica</button>
    <button class="view-btn"        data-view="quintile" onclick="switchView('quintile')">PnL × Quintil de Risco</button>
    <button class="view-btn"        data-view="matrix"   onclick="switchView('matrix')">Matriz PnL × Risco</button>
    <span style="width:16px"></span>
    <label>PM:</label>
    <span id="pm-toggles"></span>
  </div>
  <!-- period range — only visible in vol view -->
  <div id="vol-period-wrap" style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px;align-items:center">
    <label style="font-size:12px;color:var(--text2)">Período:</label>
    <button class="view-btn active" data-volperiod="all" onclick="setVolPeriod('all')">Tudo</button>
    <button class="view-btn"        data-volperiod="ytd" onclick="setVolPeriod('ytd')">YTD</button>
    <button class="view-btn"        data-volperiod="252" onclick="setVolPeriod('252')">252d</button>
    <button class="view-btn"        data-volperiod="21"  onclick="setVolPeriod('21')">21d</button>
  </div>
  <!-- sub-controls: only visible when quintile view is active -->
  <div id="quin-sub-controls" style="display:none;flex-wrap:wrap;gap:8px;margin-bottom:10px;align-items:center">
    <label style="font-size:12px;color:var(--text2)">Modo:</label>
    <button class="view-btn active" data-qmode="dist" onclick="setQuinMode('dist')">Distribuição</button>
    <button class="view-btn"        data-qmode="agg"  onclick="setQuinMode('agg')">Agregado</button>
    <span id="quin-period-wrap" style="display:none;gap:6px;align-items:center;margin-left:12px">
      <label style="font-size:12px;color:var(--text2)">Período:</label>
      <button class="view-btn active" data-period="all" onclick="setQuinPeriod('all')">Tudo</button>
      <button class="view-btn"        data-period="ytd" onclick="setQuinPeriod('ytd')">YTD</button>
      <button class="view-btn"        data-period="12m" onclick="setQuinPeriod('12m')">12m</button>
      <button class="view-btn"        data-period="21d" onclick="setQuinPeriod('21d')">21d</button>
    </span>
  </div>
  <div class="chart-wrap"><canvas id="mainChart"></canvas></div>
  <div id="chart-note" style="font-size:11px;color:var(--text2);margin-top:8px;"></div>
</div>

<!-- ─── Table ─────────────────────────────────────────────────────────────── -->
<div class="legend">
  <span>🔴 <strong>Risco Excessivo</strong> — vol alta + PnL negativo</span>
  <span>🟠 <strong>Subutilização</strong> — vol baixa + PnL negativo</span>
  <span>🟡 <strong>Vigiar</strong> — vol alta, gerando alfa</span>
  <span>🟢 <strong>OK</strong></span>
</div>
<table>
<thead>
<tr>
  <th class="ta-l">PM</th>
  <th class="ta-l">Regime</th>
  <th>VOL 21d<br><small>(bps/dia)</small></th>
  <th>VOL 30d<br><small>(bps/dia)</small></th>
  <th>Vol Trend<br><small>(21d÷60d)</small></th>
  <th>Sharpe 21d<br><small>(rolling)</small></th>
  <th>PnL MTD<br><small>(em σ)</small></th>
  <th>Sharpe YTD<br><small>(anual)</small></th>
  <th>Budget<br><small>(bps/mês)</small></th>
  <th>Margem<br><small>(bps)</small></th>
</tr>
</thead>
<tbody>
{rows}
</tbody>
</table>
<div class="footnote">
  <strong>Vol histórica</strong>: sólido = vol 21d · tracejado médio = vol 30d · tracejado curto = vol 60d · pontilhado fino = <em>vol estimada lote</em> (VaR paramétrico dos books do PM em LOTE_FUND_STRESS_RPM ÷ 1.645 × √252) · pontilhado esparso = budget implícito (budget ÷ (1.645 × √21) × √252).<br>
  <strong>Quintil de Risco</strong>: observações diárias independentes. Risco = vol_21d no dia i. PnL = dia i+1 (sem look-ahead).<br>
  Q1 = menor vol histórica · Q5 = maior vol histórica. Barras = mediana PnL 1d. Intervalo = P25–P75.<br>
  <strong>Sharpe 21d</strong> = PnL acumulado 21d ÷ (σ 21d × √21). <strong>Sharpe YTD</strong> = PnL YTD ÷ vol anualizada.<br>
  Threshold vol: budget ÷ (1.645 × √21). Low &lt;{_VOL_LOW:.0%} · High ≥{_VOL_HIGH:.0%}.
</div>

<script>
// ── Data ─────────────────────────────────────────────────────────────────────
const VOL_SERIES = {vol_json};
const Q_SUMMARY  = {quintile_summary_json};
const Q_RAW      = {quintile_raw_json};
const PM_COLOR   = {pm_color_json};
const PM_LABEL   = {pm_label_json};
const BUDGET_VOL = {budget_vol_json};
const PM_ORDER   = ["CI","LF","JD","RJ"];

// ── State ─────────────────────────────────────────────────────────────────────
let currentView  = "vol";
let quinMode     = "dist";    // "dist" | "agg"
let quinPeriod   = "all";     // "21d"  | "ytd" | "12m" | "all"
let volPeriod    = "all";     // "21" | "252" | "ytd" | "all"
let activePMs    = new Set(PM_ORDER);
let chart        = null;
const REF_DATE   = "{date_str}";

// ── Utils ─────────────────────────────────────────────────────────────────────
function hexAlpha(hex, a) {{
  const r=parseInt(hex.slice(1,3),16), g=parseInt(hex.slice(3,5),16), b=parseInt(hex.slice(5,7),16);
  return `rgba(${{r}},${{g}},${{b}},${{a}})`;
}}

// ── Toggle buttons ────────────────────────────────────────────────────────────
function setTglOn(btn, pm) {{
  btn.classList.add("on");
  btn.style.background   = PM_COLOR[pm];
  btn.style.borderColor  = PM_COLOR[pm];
  btn.style.color        = "#fff";
}}
function setTglOff(btn) {{
  btn.classList.remove("on");
  btn.style.background  = "transparent";
  btn.style.borderColor = "#444";
  btn.style.color       = "#555";
}}

function buildToggles() {{
  const wrap = document.getElementById("pm-toggles");
  PM_ORDER.forEach(pm => {{
    const btn = document.createElement("button");
    btn.className = "tgl-btn";
    btn.textContent = pm;
    btn.dataset.pm = pm;
    setTglOn(btn, pm);   // all active on load
    btn.onclick = () => {{
      if (activePMs.has(pm)) {{ activePMs.delete(pm); setTglOff(btn); }}
      else                   {{ activePMs.add(pm);    setTglOn(btn, pm); }}
      renderChart();
    }};
    wrap.appendChild(btn);
  }});
}}

function switchView(view) {{
  currentView = view;
  document.querySelectorAll("[data-view]").forEach(b =>
    b.classList.toggle("active", b.dataset.view === view));
  const titles = {{
    vol:      "Vol Histórica Anualizada (bps/ano)",
    quintile: "PnL 1d por Quintil de Risco (bps)",
    matrix:   "Matriz PnL × Risco — Quintis Cruzados",
  }};
  document.getElementById("chart-title").textContent = titles[view];
  const vp = document.getElementById("vol-period-wrap");
  if (vp) vp.style.display = view === "vol" ? "flex" : "none";
  renderChart();
}}

function setVolPeriod(period) {{
  volPeriod = period;
  document.querySelectorAll("[data-volperiod]").forEach(b =>
    b.classList.toggle("active", b.dataset.volperiod === period));
  renderChart();
}}

// ── Period filter ─────────────────────────────────────────────────────────────
function periodCutoff(period) {{
  const today = new Date(REF_DATE);
  if (period === "21d") {{ const d=new Date(today); d.setDate(d.getDate()-30); return d; }}
  if (period === "ytd") {{ return new Date(today.getFullYear(), 0, 1); }}
  if (period === "12m") {{ const d=new Date(today); d.setFullYear(d.getFullYear()-1); return d; }}
  return null; // "all" — no cutoff
}}

function filterRaw(raw, period) {{
  const cut = periodCutoff(period);
  if (!cut) return raw;
  return raw.filter(ob => new Date(ob.date) >= cut);
}}

function quinSubControls() {{
  return document.getElementById("quin-sub-controls");
}}

function setQuinMode(mode) {{
  quinMode = mode;
  document.querySelectorAll("[data-qmode]").forEach(b =>
    b.classList.toggle("active", b.dataset.qmode === mode));
  const periodWrap = document.getElementById("quin-period-wrap");
  if (periodWrap) periodWrap.style.display = mode === "agg" ? "inline-flex" : "none";
  renderChart();
}}

function setQuinPeriod(period) {{
  quinPeriod = period;
  document.querySelectorAll("[data-period]").forEach(b =>
    b.classList.toggle("active", b.dataset.period === period));
  renderChart();
}}

// ── Vol chart ─────────────────────────────────────────────────────────────────
function volPeriodCutoff() {{
  const today = new Date(REF_DATE);
  if (volPeriod === "ytd") return new Date(today.getFullYear(), 0, 1);
  if (volPeriod === "252") {{ const d=new Date(today); d.setFullYear(d.getFullYear()-1); return d; }}
  if (volPeriod === "21")  {{ const d=new Date(today); d.setDate(d.getDate()-31); return d; }}
  return null;
}}

function makeVolChart() {{
  const refPM = PM_ORDER.find(p => VOL_SERIES[p]);
  if (!refPM) return null;
  const allDates = VOL_SERIES[refPM].dates;

  // Filter to selected period
  const cut = volPeriodCutoff();
  const startIdx = cut
    ? allDates.findIndex(d => new Date(d) >= cut)
    : 0;
  const sliceFrom = startIdx < 0 ? 0 : startIdx;
  const dates = allDates.slice(sliceFrom);

  function sliceSeries(arr) {{
    if (!arr) return [];
    return arr.slice(sliceFrom);
  }}

  const datasets = [];

  PM_ORDER.filter(pm => activePMs.has(pm) && VOL_SERIES[pm]).forEach(pm => {{
    const col = PM_COLOR[pm];
    const s   = VOL_SERIES[pm];

    // ── realised vol series ──────────────────────────────────────────────────
    datasets.push({{ label: pm+" Vol 21d", data: sliceSeries(s.vol_21d),
      borderColor:col, backgroundColor:hexAlpha(col,0.07),
      borderWidth:2, pointRadius:0, tension:0.3, fill:false,
      _isMain:true, _pm:pm }});
    datasets.push({{ label: pm+" Vol 30d", data: sliceSeries(s.vol_30d),
      borderColor:hexAlpha(col,0.75), backgroundColor:"transparent",
      borderWidth:1.5, borderDash:[6,2], pointRadius:0, tension:0.3, fill:false,
      _pm:pm }});
    datasets.push({{ label: pm+" Vol 60d", data: sliceSeries(s.vol_60d),
      borderColor:hexAlpha(col,0.50), backgroundColor:"transparent",
      borderWidth:1, borderDash:[3,4], pointRadius:0, tension:0.3, fill:false,
      _pm:pm }});

    // ── Lote parametric vol (VOL_ESTIMADA) ───────────────────────────────────
    if (s.vol_estimada && s.vol_estimada.some(v=>v!==null)) {{
      datasets.push({{ label: pm+" Vol estimada (lote)", data: sliceSeries(s.vol_estimada),
        borderColor:hexAlpha(col,0.90), backgroundColor:"transparent",
        borderWidth:1.5, borderDash:[1,2], pointRadius:0, tension:0.1, fill:false,
        _isEstimada:true, _pm:pm }});
    }}

    // ── Budget implied flat reference ────────────────────────────────────────
    datasets.push({{ label: pm+" Budget vol", data: dates.map(()=>BUDGET_VOL[pm]),
      borderColor:hexAlpha(col,0.30), backgroundColor:"transparent",
      borderWidth:1, borderDash:[2,6], pointRadius:0, fill:false,
      _isBudget:true, _pm:pm }});
  }});

  return {{
    type:"line", data:{{ labels:dates, datasets }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      interaction:{{ mode:"index", intersect:false }},
      plugins:{{
        legend:{{ display:false }},
        tooltip:{{
          callbacks:{{
            label: ctx => {{
              const v = ctx.parsed.y;
              if (v===null||v===undefined) return null;
              const tag = ctx.dataset._isEstimada ? " [lote paramétrico]"
                        : ctx.dataset._isBudget   ? " [budget implícito]"
                        : ctx.dataset.label.includes("60d") ? " (60d)"
                        : ctx.dataset.label.includes("30d") ? " (30d)"
                        : " (21d)";
              return `${{ctx.dataset._pm}}${{tag}}: ${{v.toFixed(0)}} bps/ano`;
            }},
          }},
          filter: ctx => ctx.parsed.y !== null,
        }},
      }},
      scales:{{
        x:{{ ticks:{{color:"#9ba3b0",maxTicksLimit:10}}, grid:{{color:"#2a2d35"}} }},
        y:{{ ticks:{{color:"#9ba3b0",callback:v=>v+" bps"}}, grid:{{color:"#2a2d35"}},
             title:{{display:true,text:"Vol anualizada (bps/ano)",color:"#9ba3b0",font:{{size:11}}}} }},
      }},
    }},
  }};
}}

// ── Quintile bar chart ────────────────────────────────────────────────────────
function makeQuintileChart() {{
  const qLabels = ["Q1","Q2","Q3","Q4","Q5"];
  const datasets = [];
  PM_ORDER.filter(pm => activePMs.has(pm) && Q_SUMMARY[pm]).forEach(pm => {{
    const col = PM_COLOR[pm];
    const qs  = Q_SUMMARY[pm];
    const get = (q, k) => {{ const r=qs.find(x=>x.label===q); return r?r[k]:null; }};
    const medians  = qLabels.map(q=>get(q,"median"));
    const ranges   = qLabels.map(q=>{{ const r=qs.find(x=>x.label===q); return r?[r.p25,r.p75]:null; }});
    const ns       = qLabels.map(q=>get(q,"n")||0);
    const vol_anns = qLabels.map(q=>get(q,"vol_ann"));
    datasets.push({{ label:pm+" IQR", data:ranges, type:"bar",
      backgroundColor:hexAlpha(col,0.18), borderColor:"transparent", borderWidth:0,
      barPercentage:0.5, categoryPercentage:0.7, _pm:pm, _ns:ns, _vol:vol_anns }});
    datasets.push({{ label:PM_LABEL[pm], data:medians, type:"bar",
      backgroundColor:medians.map(v=>v==null?"transparent":v>=0?hexAlpha(col,0.8):hexAlpha("#ff5f5f",0.8)),
      borderColor:medians.map(v=>v==null?"transparent":v>=0?col:"#ff5f5f"),
      borderWidth:1, barPercentage:0.3, categoryPercentage:0.7, _pm:pm, _ns:ns, _vol:vol_anns }});
  }});
  return {{
    type:"bar", data:{{ labels:qLabels, datasets }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      interaction:{{ mode:"index", intersect:false }},
      plugins:{{
        legend:{{ display:false }},
        tooltip:{{ callbacks:{{
          title:ctx=>`Quintil de vol ${{ctx[0].label}}`,
          label:ctx=>{{
            if(ctx.dataset.label.includes("IQR")) return `${{ctx.dataset._pm}} P25–P75: [${{ctx.raw?ctx.raw[0]:"?"}}, ${{ctx.raw?ctx.raw[1]:"?"}}] bps`;
            const n=((ctx.dataset._ns||[])[ctx.dataIndex])||0;
            const vol=(ctx.dataset._vol||[])[ctx.dataIndex];
            return `${{ctx.dataset.label}}: mediana ${{ctx.parsed.y!=null?ctx.parsed.y.toFixed(0):"—"}} bps/dia (n=${{n}} dias, vol≈${{vol}} bps/ano)`;
          }},
        }} }},
      }},
      scales:{{
        x:{{ ticks:{{color:"#9ba3b0"}}, grid:{{color:"#2a2d35"}} }},
        y:{{ ticks:{{color:"#9ba3b0",callback:v=>v+" bps"}}, grid:{{color:"#2a2d35"}},
             title:{{display:true,text:"PnL 1d seguinte (bps)",color:"#9ba3b0",font:{{size:11}}}} }},
      }},
    }},
  }};
}}

// ── Aggregate PnL chart ────────────────────────────────────────────────────────
function makeAggChart() {{
  const qLabels = ["Q1","Q2","Q3","Q4","Q5"];
  const datasets = [];

  PM_ORDER.filter(pm => activePMs.has(pm) && Q_RAW[pm]).forEach(pm => {{
    const col  = PM_COLOR[pm];
    const filt = filterRaw(Q_RAW[pm], quinPeriod);

    // Sum pnl_fwd per vol_q bucket
    const sums   = {{1:0,2:0,3:0,4:0,5:0}};
    const counts = {{1:0,2:0,3:0,4:0,5:0}};
    filt.forEach(ob => {{ sums[ob.vol_q] += ob.pnl_fwd; counts[ob.vol_q]++; }});

    const vals = qLabels.map((_,i) => sums[i+1]);
    datasets.push({{
      label: PM_LABEL[pm],
      data:  vals,
      backgroundColor: vals.map(v => v >= 0 ? hexAlpha(col,0.75) : hexAlpha("#ff5f5f",0.75)),
      borderColor:     vals.map(v => v >= 0 ? col               : "#ff5f5f"),
      borderWidth: 1,
      barPercentage: 0.6, categoryPercentage: 0.75,
      _counts: qLabels.map((_,i) => counts[i+1]),
      _pm: pm,
    }});
  }});

  const periodLabel = {{all:"Tudo",ytd:"YTD","12m":"12m","21d":"21d"}}[quinPeriod]||"";
  return {{
    type:"bar",
    data:{{ labels:qLabels, datasets }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      interaction:{{ mode:"index", intersect:false }},
      plugins:{{
        legend:{{ display:false }},
        tooltip:{{ callbacks:{{
          title: ctx => `Vol Q${{ctx[0].label}} — PnL 1d agregado (${{periodLabel}})`,
          label: ctx => {{
            const n = (ctx.dataset._counts||[])[ctx.dataIndex]||0;
            return `${{ctx.dataset.label}}: ${{ctx.parsed.y>=0?"+":""}}${{ctx.parsed.y.toFixed(0)}} bps (n=${{n}} dias)`;
          }},
        }} }},
      }},
      scales:{{
        x:{{ ticks:{{color:"#9ba3b0"}}, grid:{{color:"#2a2d35"}},
             title:{{display:true,text:"Quintil de risco (vol 21d, Q1=baixo, Q5=alto)",color:"#9ba3b0",font:{{size:11}}}} }},
        y:{{ ticks:{{color:"#9ba3b0",callback:v=>(v>=0?"+":"")+v+" bps"}}, grid:{{color:"#2a2d35"}},
             title:{{display:true,text:"PnL 1d agregado (bps)",color:"#9ba3b0",font:{{size:11}}}} }},
      }},
    }},
  }};
}}

// ── Matrix view ───────────────────────────────────────────────────────────────
function renderMatrix() {{
  if (chart) {{ chart.destroy(); chart=null; }}
  document.getElementById("mainChart").style.display = "none";

  // Build 5×5 count matrix from selected PMs
  const mat = Array.from({{length:5}}, ()=>Array(5).fill(0));
  let total = 0;
  PM_ORDER.filter(pm=>activePMs.has(pm) && Q_RAW[pm]).forEach(pm=>{{
    Q_RAW[pm].forEach(ob=>{{
      const vq = ob.vol_q-1, pq = ob.pnl_q-1;
      if (vq>=0&&vq<5&&pq>=0&&pq<5) {{ mat[vq][pq]++; total++; }}
    }});
  }});
  const maxCount = Math.max(1, ...mat.flat());

  // Color helper: red for bottom-right (high risk, low PnL), green for top-left
  function cellColor(vq, pq, count) {{
    if (count===0) return "transparent";
    const intensity = count/maxCount;
    // vq = 0..4 (col index, low→high vol), pq = 0..4 (row index, low→high pnl)
    if (pq < vq)      return `rgba(220,60,60,${{0.15+intensity*0.55}})`;   // danger: low PnL, high vol
    else if (pq > vq) return `rgba(50,200,100,${{0.12+intensity*0.45}})`;  // good: high PnL, low vol
    else              return `rgba(150,160,180,${{0.15+intensity*0.50}})`; // diagonal: proportional
  }}

  let html = `<div style="overflow-x:auto"><table class="heat-matrix" style="border-collapse:collapse;margin:auto">
  <thead><tr>
    <th style="padding:8px 12px;color:#9ba3b0;font-size:11px;border:1px solid #2a2d35"></th>`;
  for (let vq=1;vq<=5;vq++) {{
    html += `<th style="padding:8px 16px;color:#9ba3b0;font-size:11px;text-align:center;border:1px solid #2a2d35">
      Vol Q${{vq}}${{vq===1?" <small style='display:block;color:#666'>(baixo risco)</small>":vq===5?" <small style='display:block;color:#666'>(alto risco)</small>":""}}</th>`;
  }}
  html += `<th style="padding:8px 12px;color:#9ba3b0;font-size:11px;border:1px solid #2a2d35">Total</th></tr></thead><tbody>`;

  // Rows: PnL Q5 on top (best), Q1 on bottom (worst)
  for (let pq=5;pq>=1;pq--) {{
    const rowTotal = mat.reduce((s,col)=>s+col[pq-1],0);
    html += `<tr><th style="padding:8px 12px;color:#9ba3b0;font-size:11px;text-align:right;border:1px solid #2a2d35;white-space:nowrap">
      PnL Q${{pq}}${{pq===5?" <small style='color:#666'>(melhor)</small>":pq===1?" <small style='color:#666'>(pior)</small>":""}}</th>`;
    for (let vq=1;vq<=5;vq++) {{
      const count = mat[vq-1][pq-1];
      const pct   = total>0 ? (count/total*100).toFixed(0) : 0;
      const bg    = cellColor(vq-1, pq-1, count);
      const fw    = count===Math.max(...mat[vq-1]) && count>0 ? "font-weight:700" : "";
      html += `<td style="padding:10px 16px;text-align:center;border:1px solid #2a2d35;background:${{bg}};${{fw}};font-size:13px">
        ${{count>0?count:"—"}}<br><small style="color:#9ba3b0;font-size:10px">${{count>0?pct+"%":""}}</small></td>`;
    }}
    html += `<td style="padding:8px 12px;text-align:center;color:#9ba3b0;font-size:12px;border:1px solid #2a2d35">${{rowTotal}}</td></tr>`;
  }}

  // Col totals
  html += `<tr><th style="padding:8px 12px;color:#9ba3b0;font-size:11px;text-align:right;border:1px solid #2a2d35">Total</th>`;
  for (let vq=1;vq<=5;vq++) {{
    const colTotal = mat[vq-1].reduce((s,v)=>s+v,0);
    html += `<td style="padding:8px 12px;text-align:center;color:#9ba3b0;font-size:12px;border:1px solid #2a2d35">${{colTotal}}</td>`;
  }}
  html += `<td style="padding:8px 12px;text-align:center;color:#9ba3b0;font-size:12px;border:1px solid #2a2d35">${{total}}</td></tr>`;
  html += `</tbody></table></div>`;

  let el = document.getElementById("matrix-container");
  if (!el) {{ el=document.createElement("div"); el.id="matrix-container"; document.getElementById("mainChart").parentNode.appendChild(el); }}
  el.innerHTML = html;
  el.style.display = "block";
}}

// ── Render dispatcher ─────────────────────────────────────────────────────────
function renderChart() {{
  const matEl    = document.getElementById("matrix-container");
  const canvasEl = document.getElementById("mainChart");
  const subCtrl  = document.getElementById("quin-sub-controls");

  // Show/hide quintile sub-controls
  if (subCtrl) subCtrl.style.display = currentView === "quintile" ? "flex" : "none";

  if (currentView === "matrix") {{
    canvasEl.style.display = "none";
    renderMatrix();
    document.getElementById("chart-note").textContent =
      "Linhas = quintil de PnL (Q5=melhor · Q1=pior) · Colunas = quintil de risco. " +
      "🟢 Verde = PnL > risco · 🔴 Vermelho = risco sem recompensa · Diagonal = proporcional.";
    return;
  }}
  if (matEl) matEl.style.display = "none";
  canvasEl.style.display = "block";
  if (chart) {{ chart.destroy(); chart=null; }}

  let cfg;
  if (currentView === "vol") {{
    cfg = makeVolChart();
  }} else if (quinMode === "agg") {{
    cfg = makeAggChart();
  }} else {{
    cfg = makeQuintileChart();
  }}
  if (!cfg) return;
  chart = new Chart(canvasEl.getContext("2d"), cfg);
  const notes = {{
    vol:      "Sólido = vol 21d · Tracejado médio = vol 30d · Tracejado curto = vol 60d · Pontilhado fino = vol estimada / lote paramétrico · Pontilhado esparso = budget implícito · Hover para valores",
    quintile: quinMode==="agg"
      ? "Barras = soma de PnL 21d por quintil de vol no período selecionado · Verde = PnL positivo · Vermelho = negativo"
      : "Barras = mediana PnL 21d por quintil de vol · Sombreado = P25–P75",
  }};
  document.getElementById("chart-note").textContent = notes[currentView]||"";
}}

// ── Init ──────────────────────────────────────────────────────────────────────
buildToggles();
renderChart();
</script>
</body>
</html>"""
    return html


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"PM Vol Card — {DATA_STR}")

    print("  Fetching daily PnL (400d)...")
    df_daily = fetch_daily_pnl(DATA_STR)

    print("  Fetching YTD PnL...")
    df_ytd = fetch_ytd_pnl(DATA_STR)

    print("  Fetching monthly PnL (for stop budget)...")
    df_monthly = fetch_monthly_pnl(DATA_STR)

    print("  Computing rolling vol...")
    vol_map = compute_rolling_vol(df_daily, DATA_STR)

    print("  Fetching historical NAV (for lote vol normalisation)...")
    df_nav = fetch_nav_series(DATA_STR)

    print("  Fetching lote PM VaR series (vol estimada)...")
    lote_pm_var = fetch_lote_pm_var_series(DATA_STR, df_nav=df_nav)

    print("  Computing vol series for chart...")
    vol_series = compute_vol_series(df_daily, lote_pm_var=lote_pm_var)

    print("  Computing quintile analysis...")
    quintile_data = compute_quintile_analysis(df_daily)

    print("  Computing stop budgets...")
    budget_map = compute_budgets(df_monthly, DATA_STR)

    print("  Building HTML...")
    html = build_html_card(vol_map, df_ytd, budget_map, DATA_STR, vol_series, quintile_data)

    out_dir = Path(__file__).parent / "data" / "morning-calls"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"pm_vol_card_{DATA_STR}.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  -> {out_path}")

    # Console summary
    print(f"\n  {'PM':<6} {'VOL21':>6} {'VOL30':>6} {'Trend':>6} {'Sh21d':>6} {'sMTD':>6} {'Margem':>7}  Regime")
    for pm in ("CI", "LF", "JD", "RJ"):
        vol = vol_map.get(pm, {})
        bud = budget_map.get(pm, {})
        budget   = bud.get("budget", STOP_BASE if pm != "CI" else CI_HARD)
        margem   = bud.get("margem", 0.0)
        v21      = vol.get("vol_21d",      float("nan"))
        v30      = vol.get("vol_30d",      float("nan"))
        vt       = vol.get("vol_trend",    float("nan"))
        sh21     = vol.get("sharpe_21d",   float("nan"))
        sig      = vol.get("pnl_sigma_mtd",float("nan"))
        vol_budget = budget / (Z95 * math.sqrt(21.0))
        vol_ratio  = v21 / vol_budget if (not math.isnan(v21) and vol_budget > 0) else float("nan")
        code, _ = classify_pm(vol_ratio, sig)
        print(f"  {pm:<6} {v21:6.2f} {v30:6.2f} {vt:6.2f} {sh21:+6.2f} {sig:+6.2f} {margem:7.1f}  {code}")


if __name__ == "__main__":
    main()

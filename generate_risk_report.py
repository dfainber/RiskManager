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
from evolution_diversification_card import (
    build_ratio_series          as _evo_build_ratio_series,
    compute_camada1             as _evo_compute_camada1,
    compute_camada3             as _evo_compute_camada3,
    fetch_direction_report      as _evo_fetch_direction_report,
    compute_camada_direcional   as _evo_compute_camada_direcional,
    compute_camada4             as _evo_compute_camada4,
)

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
# informative=True → VaR/Stress shown as reference (no limit, no util %). Used for Frontier (LO equity).
RAW_FUNDS = {
    "GALAPAGOS ALBATROZ FIRF LP": {"short": "ALBATROZ", "stress_col": "macro", "var_soft": 1.0, "var_hard": 1.5, "stress_soft": 5.0, "stress_hard": 8.0},
    "Galapagos Global Macro Q":   {"short": "MACRO_Q",  "stress_col": "spec",  "var_soft": 2.10, "var_hard": 3.00, "stress_soft": 21.0, "stress_hard": 30.0},
    "Frontier A\u00e7\u00f5es FIC FI": {"short": "FRONTIER", "stress_col": "macro", "var_soft": 99.0, "var_hard": 99.0, "stress_soft": 99.0, "stress_hard": 99.0, "informative": True},
}
# IDKA funds — benchmarked RF. Primary metric = BVaR (relative), secondary = VaR (reference, no limit).
# Data source: LOTE45.LOTE_PARAMETRIC_VAR_TABLE (RELATIVE_VAR_PCT, ABSOLUTE_VAR_PCT).
# Shape mirrors FUNDS/RAW_FUNDS but maps: var_* -> BVaR limits, stress_* -> VaR (no hard limit).
# Limits provisional — to be calibrated against the official mandate.
IDKA_FUNDS = {
    "IDKA IPCA 3Y FIRF":  {"short": "IDKA_3Y",  "primary": "bvar", "var_soft": 1.75, "var_hard": 2.50, "stress_soft": 99.0, "stress_hard": 99.0},
    "IDKA IPCA 10Y FIRF": {"short": "IDKA_10Y", "primary": "bvar", "var_soft": 3.50, "var_hard": 5.00, "stress_soft": 99.0, "stress_hard": 99.0},
}
ALL_FUNDS = {**FUNDS, **RAW_FUNDS, **IDKA_FUNDS}
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
      AND "LIVRO" IN ('CI','Macro_LF','Macro_JD','Macro_RJ','Macro_QM')
    GROUP BY DATE_TRUNC('month', "DATE"), "LIVRO"
    ORDER BY "LIVRO", mes
    """
    df = read_sql(q)
    df["mes"] = pd.to_datetime(df["mes"], utc=True).dt.tz_localize(None)
    return df


def fetch_macro_pm_pnl_daily(date_str: str = DATA_STR) -> pd.DataFrame:
    """Daily PnL per (LIVRO, DATE) for MACRO over the last ~400 days (≥ 252 business).
       Returns columns: VAL_DATE (datetime), LIVRO, pnl_bps.
       PnL = SUM(DIA) × 10000  (DIA is fraction of NAV, so this gives bps of NAV per day).
    """
    livros = ", ".join(f"'{v}'" for v in _PM_LIVRO.values())
    df = read_sql(f"""
        SELECT "DATE" AS "VAL_DATE", "LIVRO",
               SUM("DIA") * 10000 AS pnl_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'MACRO'
          AND "DATE" >= DATE '{date_str}' - INTERVAL '400 days'
          AND "DATE" <= DATE '{date_str}'
          AND "LIVRO" IN ({livros})
        GROUP BY "DATE", "LIVRO"
        ORDER BY "DATE"
    """)
    if not df.empty:
        df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
        df["pnl_bps"]  = df["pnl_bps"].astype(float)
    return df


def compute_pm_hs_var(dist_map: dict,
                       windows: tuple = (21, 63, 252),
                       z: float = 1.645) -> dict[str, dict]:
    """Parametric 1d VaR per PM from the Historical Simulation series —
       TODAY's portfolio × N historical days of factor returns.
       Source: dist_map (from PORTIFOLIO_DAILY_HISTORICAL_SIMULATION via fetch_pnl_distribution).
       For each PM (PORTFOLIO key in dist_map ∈ {"CI","LF","JD","RJ"}):
         σ_N  = sample std of W[-N:]  (W already in bps of NAV)
         VaR_N = z × σ_N                (default z=1.645 → 95%)
       Also reports the worst simulated day (magnitude, bps) across the full window.
       Returns {pm_code: {"v21": float, "v63": float, "v252": float, "worst": float, "n": int}}.
    """
    out: dict[str, dict] = {}
    if not dist_map:
        return out
    for pm in ("CI", "LF", "JD", "RJ"):
        w = dist_map.get(pm)
        if w is None or len(w) < 21:
            continue
        w = np.asarray(w, dtype=float)
        w = w[~np.isnan(w)]
        if len(w) < 21:
            continue
        row: dict = {"n": int(len(w))}
        for N in windows:
            if len(w) >= N:
                sigma = float(np.std(w[-N:], ddof=1))
                row[f"v{N}"] = z * sigma
            else:
                row[f"v{N}"] = float("nan")
        wmin = float(np.min(w))
        row["worst"] = -wmin if wmin < 0 else 0.0
        out[pm] = row
    return out

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

def fetch_risk_history_idka():
    """BVaR (RELATIVE_VAR_PCT) + VaR (ABSOLUTE_VAR_PCT) history for IDKA funds.
       Source: LOTE45.LOTE_PARAMETRIC_VAR_TABLE. Values are decimal fractions
       (0.029 = 2.9% of NAV). Positions summed to fund level.
    """
    tds = ", ".join(f"'{td}'" for td in IDKA_FUNDS)
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE",
           SUM("RELATIVE_VAR_PCT") AS bvar_pct_raw,
           SUM("ABSOLUTE_VAR_PCT") AS var_pct_raw
    FROM "LOTE45"."LOTE_PARAMETRIC_VAR_TABLE"
    WHERE "VAL_DATE" >= DATE '{DATE_1Y.date()}'
      AND "TRADING_DESK" IN ({tds})
    GROUP BY "TRADING_DESK", "VAL_DATE"
    ORDER BY "TRADING_DESK", "VAL_DATE"
    """
    df = read_sql(q)
    if df.empty:
        return df
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).astype("datetime64[us]")
    # Engine stores decimal fractions; convert to pct-of-NAV (same units as MACRO/QUANT)
    df["var_pct"]    = -df["bvar_pct_raw"] * 100.0   # BVaR as % (primary slot)
    df["stress_pct"] = -df["var_pct_raw"]  * 100.0   # VaR as % (secondary / reference)
    return df[["TRADING_DESK", "VAL_DATE", "var_pct", "stress_pct"]]

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

def compute_frontier_bvar_hs(df_frontier: pd.DataFrame, date_str: str = DATA_STR,
                              window_days: int = 756) -> dict | None:
    """Frontier BVaR 95% 1d vs. IBOV via historical simulation on current weights.
       window_days ≈ 3 business years. Returns None if data is insufficient.
    """
    if df_frontier is None or df_frontier.empty:
        return None

    stocks = df_frontier[df_frontier["BOOK"].astype(str).str.strip() != ""].copy()
    stocks = stocks[~stocks["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])]
    stocks = stocks[stocks["% Cash"].notna()][["PRODUCT", "% Cash"]]
    stocks = stocks.rename(columns={"PRODUCT": "ticker", "% Cash": "weight"})
    if stocks.empty:
        return None
    weights = dict(zip(stocks["ticker"], stocks["weight"].astype(float)))

    tks = ",".join(f"'{t}'" for t in list(weights.keys()) + ["IBOV"])
    q = f"""
    SELECT "INSTRUMENT", "DATE", "CLOSE"
    FROM public."EQUITIES_PRICES"
    WHERE "INSTRUMENT" IN ({tks})
      AND "DATE" >= DATE '{date_str}' - INTERVAL '{window_days + 120} days'
      AND "DATE" <= DATE '{date_str}'
    """
    df = read_sql(q)
    if df.empty or "IBOV" not in df["INSTRUMENT"].unique():
        return None
    df["DATE"] = pd.to_datetime(df["DATE"])
    wide = (df.pivot_table(index="DATE", columns="INSTRUMENT",
                           values="CLOSE", aggfunc="last")
              .sort_index())
    wide = wide.dropna(subset=["IBOV"])  # align to IBOV trading days
    wide = wide.ffill()                   # carry last known close across non-trading days
    rets = wide.pct_change().dropna(subset=["IBOV"])
    # EQUITIES_PRICES.CLOSE is raw (non-split-adjusted). Drop corporate-action jumps
    # by zeroing per-stock returns with |r| > 30% (B3 individual circuit breakers
    # cap real daily moves well below this — anything larger is a data artifact).
    stock_cols = [c for c in rets.columns if c != "IBOV"]
    mask_ca = rets[stock_cols].abs() > 0.30
    rets[stock_cols] = rets[stock_cols].mask(mask_ca, 0.0)
    rets = rets.tail(window_days)
    if len(rets) < 50:
        return None

    fund_ret = pd.Series(0.0, index=rets.index)
    for t, w in weights.items():
        if t in rets.columns:
            fund_ret = fund_ret + w * rets[t].fillna(0.0)
    er = fund_ret - rets["IBOV"]

    return {
        "bvar_pct":    -float(er.quantile(0.05)) * 100,  # 1d 95% BVaR, % of NAV
        "n_obs":       int(len(er)),
        "window_days": int(window_days),
        "mean_er_pct": float(er.mean()) * 100,
        "std_er_pct":  float(er.std())  * 100,
    }


_IDKA_BENCH_INSTRUMENT = {
    "IDKA IPCA 3Y FIRF":  "IDKA_IPCA_3A",
    "IDKA IPCA 10Y FIRF": "IDKA_IPCA_10A",
}


def compute_idka_bvar_hs(desk: str, date_str: str = DATA_STR,
                          window_days: int = 756) -> dict | None:
    """IDKA realized HS BVaR 95% 1d vs. its IDKA index benchmark.
       Uses fund cota (SHARE from LOTE_TRADING_DESKS_NAV_SHARE — flow-adjusted)
       minus IDKA index daily returns over up to `window_days` trading days.
       Fund-of-cota returns reflect historical positioning, which for replica
       funds should be close to current positioning.
    """
    bench = _IDKA_BENCH_INSTRUMENT.get(desk)
    if not bench:
        return None

    q_fund = f"""
    SELECT "VAL_DATE" AS date, "SHARE"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "TRADING_DESK" = '{desk}'
      AND "VAL_DATE" >= DATE '{date_str}' - INTERVAL '{window_days + 200} days'
      AND "VAL_DATE" <= DATE '{date_str}'
      AND "SHARE" IS NOT NULL
    ORDER BY "VAL_DATE"
    """
    q_bench = f"""
    SELECT "DATE" AS date, "VALUE"
    FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = '{bench}' AND "FIELD" = 'INDEX'
      AND "DATE" >= DATE '{date_str}' - INTERVAL '{window_days + 200} days'
      AND "DATE" <= DATE '{date_str}'
    ORDER BY "DATE"
    """
    df_f = read_sql(q_fund)
    df_b = read_sql(q_bench)
    if df_f.empty or df_b.empty:
        return None

    df_f["date"] = pd.to_datetime(df_f["date"])
    df_b["date"] = pd.to_datetime(df_b["date"])
    df_f = df_f.drop_duplicates("date").set_index("date").sort_index()
    df_b = df_b.drop_duplicates("date").set_index("date").sort_index()

    # Align to fund's calendar (fund has fewer days than index); inner-join dates
    merged = df_f.join(df_b, how="inner").dropna()
    if len(merged) < 30:
        return None
    merged["r_fund"]  = merged["SHARE"].pct_change()
    merged["r_bench"] = merged["VALUE"].pct_change()
    merged = merged.dropna()
    er = (merged["r_fund"] - merged["r_bench"]).tail(window_days)
    if len(er) < 30:
        return None

    return {
        "bvar_pct":    -float(er.quantile(0.05)) * 100,
        "n_obs":       int(len(er)),
        "window_days": int(window_days),
        "mean_er_pct": float(er.mean()) * 100,
        "std_er_pct":  float(er.std())  * 100,
    }


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
def build_series(df_risk, df_aum, df_risk_raw=None, df_risk_idka=None):
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
    # IDKA funds — BVaR/VaR already in pct units, no NAV normalization needed.
    if df_risk_idka is not None and not df_risk_idka.empty:
        for td in IDKA_FUNDS:
            rsk = df_risk_idka[df_risk_idka["TRADING_DESK"] == td].copy()
            if rsk.empty:
                continue
            rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
            result[td] = rsk.sort_values("VAL_DATE").reset_index(drop=True)
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
    ("SIST",        "QUANT total",        "fund",  "QUANT",    "QUANT"),
    ("CI",          "CI (Comitê)",        "livro", "CI",       "MACRO"),
    ("LF",          "LF — Luiz Felipe",   "livro", "Macro_LF", "MACRO"),
    ("JD",          "JD — Joca Dib",      "livro", "Macro_JD", "MACRO"),
    ("RJ",          "RJ — Rodrigo Jafet", "livro", "Macro_RJ", "MACRO"),
    ("QM",          "QM",                 "livro", "Macro_QM", "MACRO"),
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
    # QUANT sub-books (livros = sub-books, not PMs — reused the "livro" kind for drill-down layout)
    ("SIST_RF",     "Sub · RF",           "livro", "SIST_RF",     "QUANT"),
    ("SIST_FX",     "Sub · FX",           "livro", "SIST_FX",     "QUANT"),
    ("SIST_COMMO",  "Sub · Commo",        "livro", "SIST_COMMO",  "QUANT"),
    ("SIST_GLOBAL", "Sub · Global",       "livro", "SIST_GLOBAL", "QUANT"),
    ("Bracco",      "Bracco",             "livro", "Bracco",      "QUANT"),
    ("Quant_PA",    "Quant PA",           "livro", "Quant_PA",    "QUANT"),
    # FRONTIER — realized alpha vs IBOV series (no HS simulation available).
    # Series is fetched separately from frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD.
    ("FRONTIER",    "FRONTIER α vs IBOV", "fund",  "FRONTIER",    "FRONTIER"),
    # ALBATROZ — parked (waiting for engine HS).
]

def fetch_albatroz_alpha_series(date_str: str = DATA_STR, window_days: int = 252) -> np.ndarray:
    """Realized daily alpha vs CDI (bps of NAV) for Albatroz, last `window_days`
       business days ending at date_str. Source: q_models.REPORT_ALPHA_ATRIBUTION
       (the table stores alpha by design — DIA column × 10000 = alpha in bps)."""
    q = f"""
    SELECT "DATE", SUM("DIA") * 10000 AS alpha_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'ALBATROZ'
      AND "DATE" >  DATE '{date_str}' - INTERVAL '500 days'
      AND "DATE" <= DATE '{date_str}'
    GROUP BY "DATE"
    ORDER BY "DATE" DESC
    LIMIT {window_days}
    """
    try:
        df = read_sql(q)
    except Exception:
        return np.array([])
    if df.empty or df["alpha_bps"].isna().all():
        return np.array([])
    return df.sort_values("DATE")["alpha_bps"].astype(float).to_numpy()


def fetch_frontier_alpha_series(date_str: str = DATA_STR, window_days: int = 252) -> np.ndarray:
    """Realized daily alpha vs IBOV (bps of NAV) for Frontier, last `window_days`
       business days ending at date_str. Series is summed across positions per VAL_DATE.
       Returns empty array when data is missing/insufficient."""
    q = f"""
    SELECT "VAL_DATE", SUM("TOTAL_IBVSP_DAY") AS alpha_day
    FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
    WHERE "VAL_DATE" > DATE '{date_str}' - INTERVAL '500 days'
      AND "VAL_DATE" <= DATE '{date_str}'
    GROUP BY "VAL_DATE"
    ORDER BY "VAL_DATE" DESC
    LIMIT {window_days}
    """
    try:
        df = read_sql(q)
    except Exception:
        return np.array([])
    if df.empty or df["alpha_day"].isna().all():
        return np.array([])
    s = df.sort_values("VAL_DATE")["alpha_day"].astype(float) * 10_000.0
    return s.to_numpy()


def fetch_pnl_distribution(date_str: str = DATA_STR) -> dict:
    """Fetch 252d W series per PORTIFOLIO for PORTIFOLIO_DATE = date_str.
       Returns dict {portfolio_name: np.array of W values (bps)}.
       FRONTIER: realized alpha vs IBOV (no HS engine), merged under key 'FRONTIER'."""
    q = f"""
    SELECT "PORTIFOLIO", "DATE_SYNTHETIC_POSITION", "W"
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}'
    ORDER BY "PORTIFOLIO", "DATE_SYNTHETIC_POSITION"
    """
    df = read_sql(q)
    out = {p: g["W"].to_numpy() for p, g in df.groupby("PORTIFOLIO")} if not df.empty else {}
    # Frontier: realized alpha series spliced in under key "FRONTIER".
    fr = fetch_frontier_alpha_series(date_str)
    if len(fr) >= 30:
        out["FRONTIER"] = fr
    # Albatroz: parked — waiting for ALBATROZ in PORTIFOLIO_DAILY_HISTORICAL_SIMULATION
    # (same-carteira-across-252d semantics as MACRO, requested engine-side).
    # `fetch_albatroz_alpha_series` kept as a dormant helper for quick re-enable.
    return out

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

    # LIVRO-level (PMs within MACRO, sub-books within QUANT)
    livro_df = read_sql(f"""
        SELECT "LIVRO", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "DATE" = DATE '{date_str}' AND "FUNDO" IN ('MACRO','QUANT')
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

    # FRONTIER — realized α vs IBOV on the latest available VAL_DATE ≤ date_str.
    try:
        fr_df = read_sql(f"""
            SELECT SUM("TOTAL_IBVSP_DAY") * 10000 AS alpha_bps
            FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
            WHERE "VAL_DATE" = (
                SELECT MAX("VAL_DATE") FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
                WHERE "VAL_DATE" <= DATE '{date_str}'
            )
        """)
        if not fr_df.empty and pd.notna(fr_df["alpha_bps"].iloc[0]):
            actuals["fund:FRONTIER"] = float(fr_df["alpha_bps"].iloc[0])
    except Exception:
        pass

    # ALBATROZ — parked (see fetch_pnl_distribution).
    return actuals

def compute_portfolio_vol_regime(dist_map: dict, vol_window: int = 21) -> dict:
    """Vol regime from the Historical-Simulation W series (today's portfolio
    × N historical days). Isolates carteira-driven vol regime: independent
    of flows, consistent with the 252d distribution card.

    For each portfolio key (e.g. "MACRO", "EVOLUTION"):
      vol_recent  = std(W[-21:]) * sqrt(252)    — last 21 historical days
      vol_full    = std(W)       * sqrt(252)    — full available window
      ratio       = vol_recent / vol_full
      pct_rank    = percentile of std(W[-21:]) among all rolling 21d stds in W
      regime      = low/normal/elevated/stressed based on pct_rank

    W is in bps of NAV. Returned vols are in %.
    """
    out = {}
    if not dist_map:
        return out
    for pkey, w in dist_map.items():
        if w is None:
            continue
        w = np.asarray(w, dtype=float)
        w = w[~np.isnan(w)]
        if len(w) < vol_window + 30:
            continue
        vol_recent_bps = float(np.std(w[-vol_window:], ddof=1))
        vol_full_bps   = float(np.std(w,               ddof=1))
        if vol_full_bps < 1e-9:
            continue
        # Annualized vol in % of NAV: bps * sqrt(252) / 100
        vol_recent_pct = vol_recent_bps * np.sqrt(252) / 100.0
        vol_full_pct   = vol_full_bps   * np.sqrt(252) / 100.0
        ratio          = vol_recent_pct / vol_full_pct

        # Rolling 21d std across W → percentile + z-score of most-recent window
        n_roll = len(w) - vol_window + 1
        rolling_std = np.array([
            np.std(w[i:i+vol_window], ddof=1) for i in range(n_roll)
        ])
        cur_std = rolling_std[-1]
        pct_rank = float((rolling_std[:-1] < cur_std).mean()) * 100.0 \
                   if len(rolling_std) > 1 else None

        # Range stats (annualized %), for the visual strip
        sqrt252 = np.sqrt(252)
        rolling_ann_pct = rolling_std * sqrt252 / 100.0
        vol_min_pct = float(np.min(rolling_ann_pct))
        vol_max_pct = float(np.max(rolling_ann_pct))
        vol_p25_pct = float(np.percentile(rolling_ann_pct, 25))
        vol_p50_pct = float(np.percentile(rolling_ann_pct, 50))
        vol_p75_pct = float(np.percentile(rolling_ann_pct, 75))

        # z-score of current rolling std within the rolling-std distribution.
        # Note: samples overlap by construction (21d window, shift 1), so
        # N_eff ~ n_roll / 21. Treat z as directional, not a hard threshold.
        roll_mean = float(np.mean(rolling_std))
        roll_sd   = float(np.std(rolling_std, ddof=1))
        if roll_sd > 1e-12:
            z_series = (rolling_std - roll_mean) / roll_sd
            z_cur   = float(z_series[-1])
            z_min   = float(np.min(z_series))
            z_max   = float(np.max(z_series))
            z_p25   = float(np.percentile(z_series, 25))
            z_p50   = float(np.percentile(z_series, 50))
            z_p75   = float(np.percentile(z_series, 75))
        else:
            z_cur = z_min = z_max = z_p25 = z_p50 = z_p75 = None

        if pct_rank is None:        regime = "—"
        elif pct_rank < 20:         regime = "low"
        elif pct_rank < 70:         regime = "normal"
        elif pct_rank < 90:         regime = "elevated"
        else:                       regime = "stressed"

        out[pkey] = {
            "vol_recent_pct": vol_recent_pct,
            "vol_full_pct":   vol_full_pct,
            "ratio":          ratio,
            "pct_rank":       pct_rank,
            "regime":         regime,
            "n_obs":          len(w),
            "n_roll":         n_roll,
            "vol_min_pct":    vol_min_pct,
            "vol_max_pct":    vol_max_pct,
            "vol_p25_pct":    vol_p25_pct,
            "vol_p50_pct":    vol_p50_pct,
            "vol_p75_pct":    vol_p75_pct,
            "z":              z_cur,
            "z_min":          z_min,
            "z_max":          z_max,
            "z_p25":          z_p25,
            "z_p50":          z_p50,
            "z_p75":          z_p75,
        }
    return out

def range_line_svg(v_cur, v_min, v_max, v_p50=None,
                   width=220, height=28, fmt="{:.2f}") -> str:
    """Simple horizontal line from min to max with a highlighted dot at current.
       Optional median tick if v_p50 given. Labels for min and max at edges.
    """
    if v_cur is None or v_min is None or v_max is None or v_max <= v_min:
        return ""
    pad = 6
    x_min = pad
    x_max = width - pad
    def _x(v):
        return x_min + (v - v_min) / (v_max - v_min) * (x_max - x_min)
    cur_x = _x(v_cur)
    p50_x = _x(v_p50) if v_p50 is not None else None
    # Dot color by position within range (terciles)
    third = (v_max - v_min) / 3.0
    if v_cur >= v_min + 2*third:     dot_color = "#f87171"
    elif v_cur <= v_min + third:     dot_color = "#4ade80"
    else:                            dot_color = "#facc15"
    y = height // 2
    p50_svg = f'<line x1="{p50_x:.1f}" y1="{y-5}" x2="{p50_x:.1f}" y2="{y+5}" stroke="#64748b" stroke-width="1" opacity="0.7"/>' if p50_x is not None else ""
    return f"""<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <line x1="{x_min}" y1="{y}" x2="{x_max}" y2="{y}" stroke="#475569" stroke-width="2" stroke-linecap="round"/>
  <circle cx="{x_min}" cy="{y}" r="2.5" fill="#475569"/>
  <circle cx="{x_max}" cy="{y}" r="2.5" fill="#475569"/>
  {p50_svg}
  <circle cx="{cur_x:.1f}" cy="{y}" r="5.5" fill="{dot_color}" stroke="#0b1220" stroke-width="1.5"/>
  <text x="0" y="{height-2}" font-size="8" fill="#64748b" font-family="monospace">{fmt.format(v_min)}</text>
  <text x="{width}" y="{height-2}" font-size="8" fill="#64748b" font-family="monospace" text-anchor="end">{fmt.format(v_max)}</text>
</svg>"""

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

# Module-level cache for latest-NAV lookups; populated by fetch_all_latest_navs
# or on-demand by _latest_nav. Key: (desk, date_str).
_NAV_CACHE: dict = {}


def fetch_all_latest_navs(date_str: str) -> dict:
    """Bulk-fetch latest NAV (on or before date_str) for all known funds in one query.
       Side effect: populates the module-level _NAV_CACHE so subsequent _latest_nav
       calls hit memory instead of the DB. Returns the {desk: nav} dict as well.
    """
    desks = list(ALL_FUNDS.keys())
    # Include MACRO family names that aren't in ALL_FUNDS under the desk key we use at call sites
    extras = ["Galapagos Macro FIM", "GALAPAGOS ALBATROZ FIRF LP",
              "Frontier A\u00e7\u00f5es FIC FI"]
    desks = list({*desks, *extras})
    tds = ", ".join(f"'{d}'" for d in desks)
    df = read_sql(f"""
        SELECT DISTINCT ON ("TRADING_DESK") "TRADING_DESK", "NAV"
        FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" IN ({tds})
          AND "VAL_DATE" <= DATE '{date_str}'
        ORDER BY "TRADING_DESK", "VAL_DATE" DESC
    """)
    out = {}
    for _, r in df.iterrows():
        v = float(r["NAV"]) if pd.notna(r["NAV"]) else None
        _NAV_CACHE[(r["TRADING_DESK"], date_str)] = v
        out[r["TRADING_DESK"]] = v
    # Ensure every requested desk has a sentinel entry (avoids re-querying on misses)
    for d in desks:
        _NAV_CACHE.setdefault((d, date_str), None)
    return out


def _latest_nav(desk: str, date_str: str):
    """
    Most recent NAV on or before `date_str` for `desk`. Hits _NAV_CACHE first;
    falls back to a direct query only for desks/dates not warmed up.
    """
    key = (desk, date_str)
    if key in _NAV_CACHE:
        return _NAV_CACHE[key]
    df = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = '{desk}'
          AND "VAL_DATE" <= DATE '{date_str}'
        ORDER BY "VAL_DATE" DESC LIMIT 1
    """)
    v = float(df["NAV"].iloc[0]) if not df.empty else None
    _NAV_CACHE[key] = v
    return v


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


# QUANT exposure classification: BOOK → risk factor
_QUANT_BOOK_FACTOR = {
    "Bracco":            "Equity BR",
    "Quant_PA":          "Equity BR",
    "SIST_COMMO":        "Commodities",
    "SIST_FX":           "FX",
    "SIST_RF":           "Juros Nominais",
    "SIST_GLOBAL":       "FX",         # fallback (tipically FX/global factor)
    "HEDGE_SISTEMATICO": "Equity BR",  # default; override via primitive below
    "Caixa":             "CDI",
}


def _quant_classify_factor(book: str, primitive_class: str) -> str:
    """Factor classification for QUANT positions, by BOOK + primitive."""
    # Books' BRL Rate Curve leg (futures margin component) is small — bucket into
    # Juros Nominais regardless of book.
    if primitive_class == "BRL Rate Curve":
        return "Juros Nominais"
    if primitive_class in ("Brazil Sovereign Yield", "LFT Premium"):
        return "CDI"
    return _QUANT_BOOK_FACTOR.get(book, "Outros")


def fetch_quant_exposure(date_str: str = DATA_STR) -> tuple:
    """Exposure snapshot for QUANT — rows × BOOK × PRIMITIVE_CLASS.
       Returns (df, nav). df cols: BOOK, PRODUCT_CLASS, PRIMITIVE_CLASS, PRODUCT,
       delta, delta_dur, factor, sigma (annualised σ from STANDARD_DEVIATION_ASSETS).
    """
    nav = _latest_nav("Galapagos Quantitativo FIM", date_str)
    if nav is None:
        return None, None
    df = read_sql(f"""
        SELECT "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT",
               SUM("DELTA")                                          AS delta,
               SUM("DELTA" * COALESCE("MOD_DURATION", 0))            AS delta_dur,
               SUM("POSITION")                                       AS position
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK"              = 'Galapagos Quantitativo FIM'
          AND "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Quantitativo FIM'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "DELTA" <> 0
        GROUP BY "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT"
    """)
    if df.empty:
        return df, nav
    for c in ("delta", "delta_dur", "position"):
        df[c] = df[c].astype(float).fillna(0.0)
    df["factor"] = df.apply(
        lambda r: _quant_classify_factor(r["BOOK"], r["PRIMITIVE_CLASS"]),
        axis=1,
    )
    # σ per instrument — STANDARD_DEVIATION_ASSETS has rows per (INSTRUMENT, BOOK).
    # QUANT books: SIST, SIST_RF, SIST_FX, SIST_COMMO, SIST_GLOBAL, Bracco, Quant_PA.
    # Pull any of them and pick the row matching our df's BOOK when possible,
    # falling back to the first available match per PRODUCT.
    sigma_df = read_sql(f"""
        SELECT "INSTRUMENT" AS "PRODUCT", "BOOK", "STANDARD_DEVIATION" AS sigma
        FROM q_models."STANDARD_DEVIATION_ASSETS"
        WHERE "VAL_DATE" = DATE '{date_str}'
          AND "BOOK" IN ('SIST','SIST_RF','SIST_FX','SIST_COMMO','SIST_GLOBAL',
                         'Bracco','Quant_PA')
    """)
    if not sigma_df.empty:
        sig_exact = sigma_df.rename(columns={"sigma": "sigma_exact"})
        df = df.merge(sig_exact, on=["PRODUCT", "BOOK"], how="left")
        sig_any = (sigma_df.groupby("PRODUCT", as_index=False)
                           .agg(sigma_any=("sigma", "mean")))
        df = df.merge(sig_any, on="PRODUCT", how="left")
        df["sigma"] = df["sigma_exact"].fillna(df["sigma_any"])
        df = df.drop(columns=["sigma_exact", "sigma_any"])
    else:
        df["sigma"] = float("nan")
    return df, nav


def fetch_quant_var(date_str: str = DATA_STR) -> pd.DataFrame:
    """QUANT parametric VaR per (BOOK, PRODUCT, PRODUCT_CLASS) at LEVEL=3.
       Returns df with: BOOK, PRODUCT, PRODUCT_CLASS, var_brl, var_pct (bps of NAV, positive = loss).
    """
    nav = _latest_nav("Galapagos Quantitativo FIM", date_str) or 1.0
    df = read_sql(f"""
        SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS",
               SUM("PARAMETRIC_VAR") AS var_brl
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "TRADING_DESK" = 'Galapagos Quantitativo FIM'
          AND "VAL_DATE"     = DATE '{date_str}'
          AND "LEVEL"        = 3
        GROUP BY "BOOK", "PRODUCT", "PRODUCT_CLASS"
    """)
    if df.empty:
        df["var_pct"] = []
        return df
    df["var_brl"] = df["var_brl"].astype(float).fillna(0.0)
    # PARAMETRIC_VAR is negative (loss) at the source; convert to positive bps of NAV.
    df["var_pct"] = df["var_brl"] * -10000 / nav
    return df


# ── Unified Exposure card (shared between MACRO and QUANT) ──────────────────
# Columns (factor row + product drill-down):
#   Fator | Net %NAV | Net BRL | Gross %NAV | Gross BRL | Δ Expo | σ (bps) | VaR (bps) | Δ VaR
# Default sort: |Net| desc. Headers clickable (↑/↓). Drill children collapse with parent.

_UEXPO_JS = r"""<script>
if (!window.__uexpoJSLoaded) {
  window.__uexpoJSLoaded = true;
  window.__uexpoSortState = {};

  window.uexpoToggle = function(row) {
    var path  = row.getAttribute('data-expo-path');
    var caret = row.querySelector('.uexpo-caret');
    var table = row.closest('table');
    if (!table || !path) return;
    var sel   = 'tr[data-expo-parent="' + path.replace(/"/g,'\\"') + '"]';
    var kids  = table.querySelectorAll(sel);
    if (!kids.length) return;
    var opening = kids[0].style.display === 'none';
    kids.forEach(function(c){ c.style.display = opening ? '' : 'none'; });
    if (caret) caret.textContent = opening ? '▼' : '▶';
  };

  window.uexpoSort = function(th, sortKey) {
    var table = th.closest('table'); if (!table) return;
    var tbody = table.tBodies[0];   if (!tbody) return;
    var key   = (table.id || '') + ':' + sortKey;

    // The name column cycles through 3 states: A→Z, Z→A, default.
    // Numeric columns toggle asc/desc.
    var prev = window.__uexpoSortState[key];
    var state;
    if (sortKey === 'name') {
      state = (prev === 'asc') ? 'desc' : (prev === 'desc') ? 'default' : 'asc';
    } else {
      state = (prev === 'asc') ? 'desc' : 'asc';
    }
    window.__uexpoSortState = {}; window.__uexpoSortState[key] = state;

    // Group factor-level rows with their children (level=1 rows follow their parent).
    var groups = [];
    var cur = null;
    Array.from(tbody.rows).forEach(function(r){
      if (r.getAttribute('data-expo-lvl') === '0' || r.classList.contains('pa-total-row')) {
        cur = { head: r, kids: [], total: r.classList.contains('pa-total-row') };
        groups.push(cur);
      } else if (cur) {
        cur.kids.push(r);
      }
    });
    var totals = groups.filter(function(g){ return g.total; });
    var sortable = groups.filter(function(g){ return !g.total; });

    if (state === 'default') {
      sortable.sort(function(a, b){
        var va = parseInt(a.head.getAttribute('data-default-order'), 10);
        var vb = parseInt(b.head.getAttribute('data-default-order'), 10);
        return va - vb;
      });
    } else {
      var asc = (state === 'asc');
      sortable.sort(function(a, b){
        if (sortKey === 'name') {
          var na = a.head.getAttribute('data-sort-name') || '';
          var nb = b.head.getAttribute('data-sort-name') || '';
          return asc ? na.localeCompare(nb) : nb.localeCompare(na);
        }
        var va = parseFloat(a.head.getAttribute('data-sort-' + sortKey));
        var vb = parseFloat(b.head.getAttribute('data-sort-' + sortKey));
        var aNaN = isNaN(va), bNaN = isNaN(vb);
        if (aNaN && bNaN) {
          var na2 = a.head.getAttribute('data-sort-name') || '';
          var nb2 = b.head.getAttribute('data-sort-name') || '';
          return asc ? na2.localeCompare(nb2) : nb2.localeCompare(na2);
        }
        if (aNaN) return 1;
        if (bNaN) return -1;
        return asc ? va - vb : vb - va;
      });
    }

    sortable.forEach(function(g){
      tbody.appendChild(g.head);
      g.kids.forEach(function(k){ tbody.appendChild(k); });
    });
    totals.forEach(function(g){
      tbody.appendChild(g.head);
      g.kids.forEach(function(k){ tbody.appendChild(k); });
    });

    table.querySelectorAll('th[data-expo-sort]').forEach(function(h){
      var arrow = h.querySelector('.uexpo-arrow');
      if (!arrow) return;
      var active = (h === th) && (state !== 'default');
      var glyph  = state === 'asc' ? '↑' : state === 'desc' ? '↓' : '';
      arrow.textContent = (h === th) ? glyph : '';
      arrow.style.opacity = active ? '0.9' : '0';
      h.classList.toggle('uexpo-sort-active', active);
    });
  };
}
</script>"""


def _build_expo_unified_table(
    fund_key: str,
    nav: float,
    df: pd.DataFrame,
    df_d1: pd.DataFrame | None,
    df_var: pd.DataFrame | None,
    df_var_d1: pd.DataFrame | None,
    factor_order: list,
    table_id: str | None = None,
) -> str:
    """Render a unified Exposure factor × product table.
       Expected df columns: factor, PRODUCT, PRODUCT_CLASS, delta, pct_nav, sigma.
       Expected df_var columns: factor, PRODUCT, PRODUCT_CLASS, var_pct (bps, positive=loss).
       factor_order: ordered list of factor keys for initial display (default sort |Net| desc).
    """
    if df is None or df.empty:
        return ""
    tbl_id = table_id or f"tbl-uexpo-{fund_key}"

    # ── Aggregate to (factor, PRODUCT, PRODUCT_CLASS) at product level ────────
    prod = (df.assign(_abs=df["delta"].abs())
              .groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
              .agg(delta=("delta", "sum"),
                   gross_brl=("_abs", "sum"),
                   sigma=("sigma", "mean")))
    prod["net_pct"]   = prod["delta"]     * 100 / nav
    prod["gross_pct"] = prod["gross_brl"] * 100 / nav

    # D-1 product lookup
    d1_prod = {}
    if df_d1 is not None and not df_d1.empty:
        p1 = (df_d1.assign(_abs=df_d1["delta"].abs())
                    .groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                    .agg(delta=("delta", "sum")))
        p1["net_pct"] = p1["delta"] * 100 / nav
        for _, r in p1.iterrows():
            d1_prod[(r["factor"], r["PRODUCT"], r["PRODUCT_CLASS"])] = r["net_pct"]

    # VaR at product level
    v_prod = {}
    if df_var is not None and not df_var.empty:
        vp = (df_var.groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                    .agg(var_pct=("var_pct", "sum")))
        for _, r in vp.iterrows():
            v_prod[(r["factor"], r["PRODUCT"], r["PRODUCT_CLASS"])] = float(r["var_pct"])

    v1_prod = {}
    if df_var_d1 is not None and not df_var_d1.empty:
        vp1 = (df_var_d1.groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                         .agg(var_pct=("var_pct", "sum")))
        for _, r in vp1.iterrows():
            v1_prod[(r["factor"], r["PRODUCT"], r["PRODUCT_CLASS"])] = float(r["var_pct"])

    # ── Factor-level aggregates ───────────────────────────────────────────────
    def _sigma_weighted(sub: pd.DataFrame) -> float | None:
        s = sub.dropna(subset=["sigma"])
        w = s["net_pct"].abs()
        tot = w.sum()
        if tot <= 0:
            return None
        return float((w * s["sigma"]).sum() / tot)

    factors = []
    for f in prod["factor"].unique():
        sub = prod[prod["factor"] == f]
        net_pct   = float(sub["net_pct"].sum())
        gross_pct = float(sub["gross_pct"].sum())
        net_brl   = float(sub["delta"].sum())
        gross_brl = float(sub["gross_brl"].sum())
        sig       = _sigma_weighted(sub)
        var_pct   = None
        if v_prod:
            var_pct = sum(v_prod.get((f, r["PRODUCT"], r["PRODUCT_CLASS"]), 0.0)
                          for _, r in sub.iterrows())
        # D-1 factor-level
        d1_net_pct = None
        if df_d1 is not None and not df_d1.empty:
            sub1 = df_d1[df_d1["factor"] == f]
            if not sub1.empty:
                d1_net_pct = float(sub1["delta"].sum()) * 100 / nav
        d1_var_pct = None
        if v1_prod:
            d1_var_pct = sum(v1_prod.get((f, r["PRODUCT"], r["PRODUCT_CLASS"]), 0.0)
                             for _, r in sub.iterrows())
        factors.append(dict(
            factor=f, net_pct=net_pct, net_brl=net_brl,
            gross_pct=gross_pct, gross_brl=gross_brl,
            sigma=sig, var_pct=var_pct,
            d_expo=(net_pct - d1_net_pct) if d1_net_pct is not None else None,
            d_var=(var_pct - d1_var_pct) if (var_pct is not None and d1_var_pct is not None) else None,
            n_prods=len(sub),
        ))

    # Default sort: |Net| desc, with factor_order as tiebreaker
    order_idx = {f: i for i, f in enumerate(factor_order or [])}
    factors.sort(key=lambda r: (-abs(r["net_pct"]), order_idx.get(r["factor"], 99)))

    # ── Format helpers ────────────────────────────────────────────────────────
    def _num(v, n=1):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—", "var(--muted)"
        return f'{v:+.{n}f}', ("var(--up)" if v >= 0 else "var(--down)")

    def _abs_num(v, n=2, unit="%"):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        return f'{v:.{n}f}{unit}'

    def _money(v_brl):
        if v_brl is None or (isinstance(v_brl, float) and pd.isna(v_brl)):
            return "—"
        return f'{v_brl/1e6:+,.1f}M'.replace(",", "_").replace(".", ",").replace("_", ".")

    def _money_abs(v_brl):
        if v_brl is None or (isinstance(v_brl, float) and pd.isna(v_brl)):
            return "—"
        return f'{v_brl/1e6:,.1f}M'.replace(",", "_").replace(".", ",").replace("_", ".")

    def _dv(v):
        return "" if (v is None or (isinstance(v, float) and pd.isna(v))) else f'{v:.6f}'

    def _cell(txt, color=None, extra=""):
        col = f'color:{color};' if color else ''
        return (f'<td class="mono" style="text-align:right;font-size:11.5px;{col}{extra}" '
                f'>{txt}</td>')

    # ── Render factor rows + product children ────────────────────────────────
    body = ""
    tot_net = 0.0; tot_gross = 0.0; tot_net_brl = 0.0; tot_gross_brl = 0.0
    tot_var = 0.0; any_var = False
    for _default_idx, g in enumerate(factors):
        f = g["factor"]
        tot_net       += g["net_pct"]
        tot_gross     += g["gross_pct"]
        tot_net_brl   += g["net_brl"]
        tot_gross_brl += g["gross_brl"]
        if g["var_pct"] is not None:
            tot_var += g["var_pct"]; any_var = True
        path = f'{fund_key}-' + f.replace(' ', '_').replace('(', '').replace(')', '')

        net_s, net_c = _num(g["net_pct"], 2)
        gross_s = _abs_num(g["gross_pct"], 1)
        dexp_s, dexp_c = _num(g["d_expo"], 2)
        sig_s = _abs_num(g["sigma"], 1, unit="")
        var_raw = g["var_pct"]
        if var_raw is None:
            var_s, var_c = "—", "var(--muted)"
        else:
            var_s = f'{var_raw:.1f}'
            var_c = "var(--down)" if var_raw > 0 else "var(--muted)"
        dvar_s, dvar_c = _num(g["d_var"], 1)

        body += (
            f'<tr class="uexpo-row" data-expo-lvl="0" data-expo-path="{path}" '
            f'onclick="uexpoToggle(this)" style="cursor:pointer; border-top:1px solid var(--border)" '
            f'data-default-order="{_default_idx}" '
            f'data-sort-name="{f}" '
            f'data-sort-net="{-abs(g["net_pct"]):.6f}" '
            f'data-sort-dexp="{_dv(g["d_expo"])}" '
            f'data-sort-gross="{-g["gross_pct"]:.6f}" '
            f'data-sort-sigma="{_dv(g["sigma"])}" '
            f'data-sort-var="{_dv(var_raw)}" '
            f'data-sort-dvar="{_dv(g["d_var"])}">'
            f'<td class="sum-fund" style="font-weight:700"><span class="uexpo-caret">▶</span> {f} '
            f'<span style="color:var(--muted);font-size:10px;font-weight:400">· {g["n_prods"]}</span></td>'
            + _cell(f'{net_s}%', net_c, "font-weight:700")
            + _cell(f'{dexp_s}%' if dexp_s != '—' else '—', dexp_c)
            + _cell(gross_s, "var(--muted)")
            + _cell(sig_s, "var(--muted)")
            + _cell(var_s, var_c)
            + _cell(f'{dvar_s}' if dvar_s != '—' else '—', dvar_c)
            + '</tr>'
        )

        # Product children — sort by |net_pct| desc
        sub_prods = prod[prod["factor"] == f].copy()
        sub_prods["_abs"] = sub_prods["net_pct"].abs()
        sub_prods = sub_prods.sort_values("_abs", ascending=False)
        for _, p in sub_prods.iterrows():
            key = (f, p["PRODUCT"], p["PRODUCT_CLASS"])
            p_net = float(p["net_pct"])
            p_net_brl = float(p["delta"])
            p_gross = float(p["gross_pct"])
            p_gross_brl = float(p["gross_brl"])
            p_sig = float(p["sigma"]) if pd.notna(p["sigma"]) else None
            p_var = v_prod.get(key)
            p_d1 = d1_prod.get(key)
            # Δ Expo: if D-1 data exists globally, a missing product key means a
            # NEW position today → delta = today − 0 = today. Without this, new
            # positions rendered "—" and children stopped summing to factor total.
            if df_d1 is not None and not df_d1.empty:
                p_dexp = p_net - (p_d1 if p_d1 is not None else 0.0)
            else:
                p_dexp = None
            # Same logic for Δ VaR: treat missing D-1 as 0 when D-1 data exists.
            if p_var is not None and df_var_d1 is not None and not df_var_d1.empty:
                p_dvar = p_var - v1_prod.get(key, 0.0)
            else:
                p_dvar = None

            net_col = "var(--up)" if p_net >= 0 else "var(--down)"
            if p_var is None:
                pvar_s, pvar_c = "—", "var(--muted)"
            else:
                pvar_s = f'{p_var:.1f}'
                pvar_c = "var(--down)" if p_var > 0 else "var(--muted)"
            pdexp_s, pdexp_c = _num(p_dexp, 2)
            pdvar_s, pdvar_c = _num(p_dvar, 1)
            p_sig_s = _abs_num(p_sig, 1, unit="")
            body += (
                f'<tr class="uexpo-row" data-expo-lvl="1" data-expo-parent="{path}" '
                f'style="display:none; background:var(--bg-alt, rgba(0,0,0,0.12))">'
                f'<td style="padding-left:28px; font-size:11px; color:var(--muted)">'
                f'  <span style="color:var(--text); font-weight:600">{p["PRODUCT"]}</span> '
                f'<span style="color:var(--muted); font-size:10px">({p["PRODUCT_CLASS"]})</span>'
                f'</td>'
                + _cell(f'{p_net:+.2f}%', net_col)
                + _cell(f'{pdexp_s}%' if pdexp_s != '—' else '—', pdexp_c)
                + _cell(f'{p_gross:.2f}%', "var(--muted)")
                + _cell(p_sig_s, "var(--muted)")
                + _cell(pvar_s, pvar_c)
                + _cell(pdvar_s if pdvar_s != '—' else '—', pdvar_c)
                + '</tr>'
            )

    # ── Total row (pinned) ────────────────────────────────────────────────────
    # Net %NAV total omitted (user: net can cross long/short and aggregate is
    # not meaningful). Gross total kept — sum of |exposures| is the book size.
    body += (
        '<tr class="pa-total-row" data-pinned="1" '
        'style="border-top:2px solid var(--border); font-weight:700">'
        '<td class="sum-fund" style="font-weight:700">Total</td>'
        + '<td></td>'
        + '<td></td>'
        + _cell(f'{tot_gross:.1f}%', "var(--text)", "font-weight:700")
        + '<td></td>'
        + _cell(f'{tot_var:.1f}' if any_var else '—',
                "var(--down)" if (any_var and tot_var > 0) else "var(--muted)",
                "font-weight:700")
        + '<td></td>'
        + '</tr>'
    )

    def _th(label, sort_key, active=False, align="right"):
        arrow = '↓' if active else ''
        act_cls = ' uexpo-sort-active' if active else ''
        return (f'<th class="uexpo-sort-th{act_cls}" data-expo-sort="{sort_key}" '
                f'style="text-align:{align}; cursor:pointer; user-select:none" '
                f'onclick="uexpoSort(this,\'{sort_key}\')">'
                f'{label} <span class="uexpo-arrow" '
                f'style="font-size:9px;opacity:{0.9 if active else 0}">{arrow}</span></th>')

    header = (
        _th("Fator",      "name",  active=False, align="left")
        + _th("Net %NAV",   "net",   active=True)
        + _th("Δ Expo",     "dexp",  active=False)
        + _th("Gross %NAV", "gross", active=False)
        + _th("σ (bps)",    "sigma", active=False)
        + _th("VaR (bps)",  "var",   active=False)
        + _th("Δ VaR",      "dvar",  active=False)
    )

    return (
        f'<table id="{tbl_id}" class="summary-table" data-no-sort="1">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{body}</tbody>'
        f'</table>'
    )


_QUANT_FACTOR_ORDER = [
    "Equity BR", "FX", "Commodities",
    "Juros Nominais", "Juros Reais (IPCA)", "CDI", "Outros",
]

# QUANT BOOK → factor mapping for VaR aggregation (LEVEL=3 is by BOOK).
_QUANT_VAR_BOOK_FACTOR = {
    "Bracco":      "Equity BR",
    "Quant_PA":    "Equity BR",
    "SIST_COMMO":  "Commodities",
    "SIST_FX":     "FX",
    "SIST_RF":     "Juros Nominais",
    "SIST_GLOBAL": "FX",
    "Caixa":       "CDI",
    "CAIXA USD":   "CDI",
}


def _prepare_quant_var_for_unified(df_var: pd.DataFrame | None) -> pd.DataFrame | None:
    """Tag BOOK-level QUANT VaR with a factor column matching the Por Fator view."""
    if df_var is None or df_var.empty:
        return df_var
    out = df_var.copy()
    out["factor"] = out["BOOK"].map(_QUANT_VAR_BOOK_FACTOR).fillna("Outros")
    return out


def build_quant_exposure_section(df: pd.DataFrame, nav: float,
                                   df_d1: pd.DataFrame = None,
                                   df_var: pd.DataFrame = None,
                                   df_var_d1: pd.DataFrame = None) -> str:
    """QUANT exposure card — two views:
       1. Por Fator: unified factor × product table (Net/Gross + σ + VaR + Δs).
       2. Por Livro: factor × livro matrix (which livro drives each factor).
    """
    if df is None or df.empty or not nav:
        return ""

    df_var_tagged    = _prepare_quant_var_for_unified(df_var)
    df_var_d1_tagged = _prepare_quant_var_for_unified(df_var_d1)
    if df_d1 is not None and not df_d1.empty:
        if "pct_nav" not in df_d1.columns:
            df_d1 = df_d1.copy()
            df_d1["pct_nav"] = df_d1["delta"] * 100 / nav
    if "pct_nav" not in df.columns:
        df = df.copy()
        df["pct_nav"] = df["delta"] * 100 / nav

    factor_table = _build_expo_unified_table(
        fund_key="quant",
        nav=nav,
        df=df,
        df_d1=df_d1,
        df_var=df_var_tagged,
        df_var_d1=df_var_d1_tagged,
        factor_order=_QUANT_FACTOR_ORDER,
    )

    # ── Por Livro matrix (livro × factor, Net %NAV) ──────────────────────────
    def _pct(v):
        pct = v * 100 / nav if nav else 0
        if abs(pct) < 0.01:
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        col = "var(--up)" if v >= 0 else "var(--down)"
        return f'<td class="mono" style="text-align:right; color:{col}">{pct:+.2f}%</td>'

    pivot = df.pivot_table(
        index="BOOK", columns="factor", values="delta",
        aggfunc="sum", fill_value=0.0,
    )
    livro_factors = [f for f in _QUANT_FACTOR_ORDER if f in pivot.columns]
    pivot = pivot[livro_factors]
    pivot["_total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("_total", key=lambda s: s.abs(), ascending=False)

    livro_header = '<th style="text-align:left">Livro</th>' + "".join(
        f'<th style="text-align:right">{f}</th>' for f in livro_factors
    ) + '<th style="text-align:right">Total</th>'
    livro_rows = ""
    for livro, row in pivot.iterrows():
        cells = "".join(_pct(row[f]) for f in livro_factors)
        _tot_pct = row["_total"] * 100 / nav if nav else 0
        _tot_col = "var(--up)" if row["_total"] >= 0 else "var(--down)"
        cells += f'<td class="mono" style="text-align:right; font-weight:600; color:{_tot_col}">{_tot_pct:+.2f}%</td>'
        livro_rows += f'<tr><td class="sum-fund">{livro}</td>{cells}</tr>'
    totals_cells = "".join(_pct(pivot[f].sum()) for f in livro_factors)
    _gt_pct = pivot["_total"].sum() * 100 / nav if nav else 0
    _gt_col = "var(--up)" if pivot["_total"].sum() >= 0 else "var(--down)"
    totals_cells += f'<td class="mono" style="text-align:right; font-weight:700; color:{_gt_col}">{_gt_pct:+.2f}%</td>'
    livro_rows += (
        '<tr class="pa-total-row">'
        '<td class="sum-fund" style="font-weight:700">Total</td>'
        + totals_cells + "</tr>"
    )

    nav_fmt = f"{nav/1e6:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")

    return f"""
    {_UEXPO_JS}
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — QUANT</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · por fator de risco e por livro</span>
        <div class="pa-view-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-qexpo-view="factor"
                  onclick="selectQuantExpoView(this,'factor')">Por Fator</button>
          <button class="pa-tgl" data-qexpo-view="livro"
                  onclick="selectQuantExpoView(this,'livro')">Por Livro</button>
        </div>
      </div>

      <div class="qexpo-view" data-qexpo-view="factor">{factor_table}</div>

      <div class="qexpo-view" data-qexpo-view="livro" style="display:none">
        <table class="summary-table" data-no-sort="1">
          <thead><tr>{livro_header}</tr></thead>
          <tbody>{livro_rows}</tbody>
        </table>
      </div>

      <div class="bar-legend" style="margin-top:10px">
        <b>Por Fator</b>: factor × produto, mesma formatação do MACRO — Net/Gross em %NAV e BRL, Δ Expo vs D-1, σ (bps) e VaR (bps) por BOOK (LOTE_FUND_STRESS_RPM LEVEL=3).
        <b>Por Livro</b>: matriz livro × fator, Net %NAV. Fonte: LOTE_PRODUCT_EXPO, source = QUANT direto.
      </div>
    </section>"""


def build_evolution_exposure_section(df: pd.DataFrame, nav: float,
                                      df_d1: pd.DataFrame = None,
                                      df_var: pd.DataFrame = None,
                                      df_var_d1: pd.DataFrame = None,
                                      df_pnl_prod: pd.DataFrame = None) -> str:
    """EVOLUTION exposure — full look-through card with two views:
       1. POR STRATEGY (default): Strategy → LIVRO → Instrumento, 3-level drill.
       2. POR FATOR: factor × product using the shared _build_expo_unified_table.
       Columns mirror MACRO: %NAV, σ, Δ Expo, VaR(bps), Δ VaR, DIA.
    """
    if df is None or df.empty or not nav:
        return ""

    # ── Build the Por Fator view (reuse MACRO helper) ────────────────────────
    _expo_u    = df.rename(columns={"factor": "factor"}).copy()  # already tagged
    _var_u     = df_var.rename(columns={"rf": "factor"}).copy() if df_var is not None and not df_var.empty else None
    _expo_d1_u = df_d1.rename(columns={"factor": "factor"}).copy() if df_d1 is not None and not df_d1.empty else None
    _var_d1_u  = df_var_d1.rename(columns={"rf": "factor"}).copy() if df_var_d1 is not None and not df_var_d1.empty else None
    factor_table = _build_expo_unified_table(
        fund_key="evolution",
        nav=nav,
        df=_expo_u,
        df_d1=_expo_d1_u,
        df_var=_var_u,
        df_var_d1=_var_d1_u,
        factor_order=_RF_ORDER,
    )

    # ── Build the Por Strategy view (Strategy → LIVRO → Instrumento) ────────
    # VaR attribution: at the product level, allocate the factor's total VaR
    # proportionally to each product's delta share within that factor.
    rf_delta_tot = df.groupby("factor").agg(rf_delta=("delta", "sum")).reset_index()
    rf_var_tot = (df_var.groupby("rf").agg(var_pct=("var_pct", "sum")).reset_index()
                  if df_var is not None and not df_var.empty else pd.DataFrame(columns=["rf", "var_pct"]))
    rf_var_map = dict(zip(rf_var_tot["rf"], rf_var_tot["var_pct"])) if not rf_var_tot.empty else {}

    # Product-level agg: sum delta across primitives for the same instrument within a livro
    prod_agg = (df.groupby(["strategy", "livro", "factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                  .agg(delta=("delta", "sum"), sigma=("sigma", "mean")))
    prod_agg = prod_agg.merge(rf_delta_tot, on="factor", how="left")
    prod_agg["pct_nav"] = prod_agg["delta"] * 100 / nav
    prod_agg["prod_var_pct"] = prod_agg.apply(
        lambda r: (r["delta"] / r["rf_delta"] * rf_var_map.get(r["factor"], 0.0))
                  if r["rf_delta"] else 0.0,
        axis=1,
    )

    # D-1 product lookup (for Δ Expo / Δ VaR at instrument level)
    d1_prod = {}
    d1_prod_var = {}
    if df_d1 is not None and not df_d1.empty:
        rf_delta_tot_d1 = df_d1.groupby("factor").agg(rf_delta=("delta", "sum")).reset_index()
        rf_var_tot_d1 = (df_var_d1.groupby("rf").agg(var_pct=("var_pct", "sum")).reset_index()
                         if df_var_d1 is not None and not df_var_d1.empty
                         else pd.DataFrame(columns=["rf", "var_pct"]))
        rf_var_map_d1 = dict(zip(rf_var_tot_d1["rf"], rf_var_tot_d1["var_pct"])) if not rf_var_tot_d1.empty else {}
        p1 = (df_d1.groupby(["strategy", "livro", "factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                    .agg(delta=("delta", "sum")))
        p1 = p1.merge(rf_delta_tot_d1, on="factor", how="left")
        p1["pct_nav"] = p1["delta"] * 100 / nav
        p1["prod_var_pct"] = p1.apply(
            lambda r: (r["delta"] / r["rf_delta"] * rf_var_map_d1.get(r["factor"], 0.0))
                      if r["rf_delta"] else 0.0,
            axis=1,
        )
        for _, r in p1.iterrows():
            k = (r["strategy"], r["livro"], r["factor"], r["PRODUCT"], r["PRODUCT_CLASS"])
            d1_prod[k] = r["pct_nav"]
            d1_prod_var[k] = r["prod_var_pct"]

    # DIA lookup per (LIVRO, PRODUCT)
    dia_lookup = {}
    if df_pnl_prod is not None and not df_pnl_prod.empty:
        for _, r in df_pnl_prod.iterrows():
            dia_lookup[(r["LIVRO"], r["PRODUCT"])] = float(r["dia_bps"])

    def _num(v, n=1):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—", "var(--muted)"
        return f'{v:+.{n}f}', ("var(--up)" if v >= 0 else "var(--down)")

    def _abs_num(v, n=2, unit="%"):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        return f'{v:.{n}f}{unit}'

    def _dv(v):
        return "" if (v is None or (isinstance(v, float) and pd.isna(v))) else f'{v:.6f}'

    def _cell(txt, color=None, extra=""):
        col = f'color:{color};' if color else ''
        return (f'<td class="mono" style="text-align:right;font-size:11.5px;{col}{extra}">{txt}</td>')

    # Rollups per (strategy, livro) and per strategy
    livro_sum = (prod_agg.groupby(["strategy", "livro"], as_index=False)
                          .agg(pct_nav=("pct_nav", "sum"),
                               prod_var_pct=("prod_var_pct", "sum"),
                               gross_brl=("delta", lambda s: s.abs().sum())))
    strat_sum = (prod_agg.groupby("strategy", as_index=False)
                          .agg(pct_nav=("pct_nav", "sum"),
                               prod_var_pct=("prod_var_pct", "sum"),
                               gross_brl=("delta", lambda s: s.abs().sum())))

    # Weighted σ per (strategy, livro) and per strategy
    def _sigma_w(sub: pd.DataFrame) -> float | None:
        s = sub.dropna(subset=["sigma"])
        w = s["pct_nav"].abs()
        tot = w.sum()
        if tot <= 0:
            return None
        return float((w * s["sigma"]).sum() / tot)
    livro_sigma = {(r["strategy"], r["livro"]): _sigma_w(prod_agg[(prod_agg["strategy"] == r["strategy"]) & (prod_agg["livro"] == r["livro"])])
                   for _, r in livro_sum.iterrows()}
    strat_sigma = {r["strategy"]: _sigma_w(prod_agg[prod_agg["strategy"] == r["strategy"]])
                   for _, r in strat_sum.iterrows()}

    # D-1 rollups
    d1_livro = {}
    d1_strat = {}
    d1_livro_var = {}
    d1_strat_var = {}
    if df_d1 is not None and not df_d1.empty:
        p1_tmp = pd.DataFrame([
            dict(strategy=k[0], livro=k[1], factor=k[2], PRODUCT=k[3], PRODUCT_CLASS=k[4],
                 pct_nav=v, prod_var_pct=d1_prod_var.get(k, 0.0))
            for k, v in d1_prod.items()
        ])
        if not p1_tmp.empty:
            _l = p1_tmp.groupby(["strategy", "livro"]).agg(pct_nav=("pct_nav", "sum"),
                                                          prod_var_pct=("prod_var_pct", "sum"))
            for k, r in _l.iterrows():
                d1_livro[k] = r["pct_nav"]; d1_livro_var[k] = r["prod_var_pct"]
            _s = p1_tmp.groupby("strategy").agg(pct_nav=("pct_nav", "sum"),
                                               prod_var_pct=("prod_var_pct", "sum"))
            for k, r in _s.iterrows():
                d1_strat[k] = r["pct_nav"]; d1_strat_var[k] = r["prod_var_pct"]

    # ── Render rows ──────────────────────────────────────────────────────────
    body = ""
    strategies_sorted = sorted(
        strat_sum["strategy"].tolist(),
        key=lambda s: (_EVO_STRATEGY_ORDER.index(s) if s in _EVO_STRATEGY_ORDER else 99)
    )
    for strat in strategies_sorted:
        s_row = strat_sum[strat_sum["strategy"] == strat].iloc[0]
        s_pct  = float(s_row["pct_nav"])
        s_var  = float(s_row["prod_var_pct"])
        s_gross = float(s_row["gross_brl"]) * 100 / nav
        s_sig = strat_sigma.get(strat)
        s_dexp = (s_pct - d1_strat[strat]) if strat in d1_strat else None
        s_dvar = (s_var - d1_strat_var[strat]) if strat in d1_strat_var else None
        s_color = _EVO_STRATEGY_COLOR.get(strat, "#94a3b8")

        s_net_s, s_net_c   = _num(s_pct, 2)
        s_dexp_s, s_dexp_c = _num(s_dexp, 2)
        s_var_s = f'{s_var:.1f}' if s_var else "—"
        s_var_c = "var(--down)" if s_var > 0 else "var(--muted)"
        s_dvar_s, s_dvar_c = _num(s_dvar, 1)
        s_sig_s = _abs_num(s_sig, 1, unit="")
        strat_path = f'evo-strat-{strat}'

        # DIA at strategy level: sum across all livros in the strategy
        s_dia = 0.0
        for lv in prod_agg[prod_agg["strategy"] == strat]["livro"].unique():
            for _, p in prod_agg[(prod_agg["strategy"] == strat) & (prod_agg["livro"] == lv)].iterrows():
                s_dia += dia_lookup.get((lv, p["PRODUCT"]), 0.0)
        s_dia_s = f'{s_dia:+.0f}' if abs(s_dia) > 0.05 else "—"
        s_dia_c = "var(--down)" if s_dia < 0 else "var(--up)" if s_dia > 0 else "var(--muted)"

        body += (
            f'<tr class="uexpo-row" data-expo-lvl="0" data-expo-path="{strat_path}" '
            f'onclick="uexpoToggle(this)" style="cursor:pointer; border-top:1px solid var(--border)" '
            f'data-default-order="{strategies_sorted.index(strat)}" '
            f'data-sort-name="{strat}" '
            f'data-sort-net="{-abs(s_pct):.6f}" '
            f'data-sort-dexp="{_dv(s_dexp)}" '
            f'data-sort-gross="{-s_gross:.6f}" '
            f'data-sort-sigma="{_dv(s_sig)}" '
            f'data-sort-var="{_dv(s_var)}" '
            f'data-sort-dvar="{_dv(s_dvar)}" '
            f'data-sort-dia="{_dv(s_dia if abs(s_dia) > 0.05 else None)}">'
            f'<td class="sum-fund" style="font-weight:700;color:{s_color}"><span class="uexpo-caret">▶</span> {strat}</td>'
            + _cell(f'{s_net_s}%', s_net_c, "font-weight:700")
            + _cell(f'{s_dexp_s}%' if s_dexp_s != '—' else '—', s_dexp_c)
            + _cell(s_sig_s, "var(--muted)")
            + _cell(s_var_s, s_var_c)
            + _cell(f'{s_dvar_s}' if s_dvar_s != '—' else '—', s_dvar_c)
            + _cell(s_dia_s, s_dia_c)
            + '</tr>'
        )

        # Level 1: LIVROs within strategy
        livros_here = livro_sum[livro_sum["strategy"] == strat].sort_values(
            "pct_nav", key=lambda s: s.abs(), ascending=False
        )
        for _, lrow in livros_here.iterrows():
            lv = lrow["livro"]
            l_pct = float(lrow["pct_nav"])
            l_var = float(lrow["prod_var_pct"])
            l_gross = float(lrow["gross_brl"]) * 100 / nav
            l_sig = livro_sigma.get((strat, lv))
            k_l = (strat, lv)
            l_dexp = (l_pct - d1_livro[k_l]) if k_l in d1_livro else None
            l_dvar = (l_var - d1_livro_var[k_l]) if k_l in d1_livro_var else None

            l_net_s, l_net_c   = _num(l_pct, 2)
            l_dexp_s, l_dexp_c = _num(l_dexp, 2)
            l_var_s = f'{l_var:.1f}' if l_var else "—"
            l_var_c = "var(--down)" if l_var > 0 else "var(--muted)"
            l_dvar_s, l_dvar_c = _num(l_dvar, 1)
            l_sig_s = _abs_num(l_sig, 1, unit="")
            livro_path = f'{strat_path}-{lv}'

            # DIA for LIVRO: sum dia_bps across instruments
            l_dia = sum(dia_lookup.get((lv, p["PRODUCT"]), 0.0)
                        for _, p in prod_agg[(prod_agg["strategy"] == strat) & (prod_agg["livro"] == lv)].iterrows())
            l_dia_s = f'{l_dia:+.0f}' if abs(l_dia) > 0.05 else "—"
            l_dia_c = "var(--down)" if l_dia < 0 else "var(--up)" if l_dia > 0 else "var(--muted)"

            l_var_sort = _dv(l_var) if l_var else ""
            body += (
                f'<tr class="uexpo-row" data-expo-lvl="1" data-expo-parent="{strat_path}" '
                f'data-expo-path="{livro_path}" onclick="uexpoToggle(this)" '
                f'style="cursor:pointer; display:none; background:rgba(96,165,250,0.04)" '
                f'data-sort-name="{lv}" '
                f'data-sort-net="{-abs(l_pct):.6f}" '
                f'data-sort-dexp="{_dv(l_dexp)}" '
                f'data-sort-sigma="{_dv(l_sig)}" '
                f'data-sort-var="{l_var_sort}" '
                f'data-sort-dvar="{_dv(l_dvar)}" '
                f'data-sort-dia="{_dv(l_dia if abs(l_dia) > 0.05 else None)}">'
                f'<td style="padding-left:24px; font-weight:600; color:var(--text)">'
                f'<span class="uexpo-caret">▶</span> {lv}</td>'
                + _cell(f'{l_net_s}%', l_net_c)
                + _cell(f'{l_dexp_s}%' if l_dexp_s != '—' else '—', l_dexp_c)
                + _cell(l_sig_s, "var(--muted)")
                + _cell(l_var_s, l_var_c)
                + _cell(f'{l_dvar_s}' if l_dvar_s != '—' else '—', l_dvar_c)
                + _cell(l_dia_s, l_dia_c)
                + '</tr>'
            )

            # Level 2: Instruments within (strategy, livro)
            inst_rows = prod_agg[(prod_agg["strategy"] == strat) & (prod_agg["livro"] == lv)].copy()
            inst_rows["_abs"] = inst_rows["pct_nav"].abs()
            inst_rows = inst_rows.sort_values("_abs", ascending=False)
            for _, p in inst_rows.iterrows():
                p_net = float(p["pct_nav"])
                p_var = float(p["prod_var_pct"])
                p_sig = float(p["sigma"]) if pd.notna(p["sigma"]) else None
                key = (strat, lv, p["factor"], p["PRODUCT"], p["PRODUCT_CLASS"])
                p_d1 = d1_prod.get(key)
                if df_d1 is not None and not df_d1.empty:
                    p_dexp = p_net - (p_d1 if p_d1 is not None else 0.0)
                else:
                    p_dexp = None
                p_dvar = None
                if df_var_d1 is not None and not df_var_d1.empty:
                    p_dvar = p_var - d1_prod_var.get(key, 0.0)
                p_dia = dia_lookup.get((lv, p["PRODUCT"]))

                p_net_s, p_net_c = _num(p_net, 2)
                p_dexp_s, p_dexp_c = _num(p_dexp, 2)
                p_var_s = f'{p_var:.1f}' if abs(p_var) > 0.05 else "—"
                p_var_c = "var(--down)" if p_var > 0 else "var(--muted)"
                p_dvar_s, p_dvar_c = _num(p_dvar, 1)
                p_sig_s = _abs_num(p_sig, 1, unit="")
                p_dia_s = f'{p_dia:+.0f}' if (p_dia is not None and abs(p_dia) > 0.05) else "—"
                p_dia_c = "var(--down)" if (p_dia is not None and p_dia < 0) else "var(--up)" if (p_dia is not None and p_dia > 0) else "var(--muted)"
                p_var_sort = _dv(p_var) if abs(p_var) > 0.05 else ""
                p_dia_sort = _dv(p_dia) if (p_dia is not None and abs(p_dia) > 0.05) else ""
                p_product = p["PRODUCT"]
                body += (
                    f'<tr class="uexpo-row" data-expo-lvl="2" data-expo-parent="{livro_path}" '
                    f'style="display:none; background:rgba(0,0,0,0.18)" '
                    f'data-sort-name="{p_product}" '
                    f'data-sort-net="{-abs(p_net):.6f}" '
                    f'data-sort-dexp="{_dv(p_dexp)}" '
                    f'data-sort-sigma="{_dv(p_sig)}" '
                    f'data-sort-var="{p_var_sort}" '
                    f'data-sort-dvar="{_dv(p_dvar)}" '
                    f'data-sort-dia="{p_dia_sort}">'
                    f'<td style="padding-left:44px; font-size:11px; color:var(--muted)">'
                    f'<span style="color:var(--text); font-weight:500">{p["PRODUCT"]}</span> '
                    f'<span style="color:var(--muted); font-size:10px">({p["PRODUCT_CLASS"]} · {p["factor"]})</span>'
                    f'</td>'
                    + _cell(f'{p_net_s}%', p_net_c)
                    + _cell(f'{p_dexp_s}%' if p_dexp_s != '—' else '—', p_dexp_c)
                    + _cell(p_sig_s, "var(--muted)")
                    + _cell(p_var_s, p_var_c)
                    + _cell(f'{p_dvar_s}' if p_dvar_s != '—' else '—', p_dvar_c)
                    + _cell(p_dia_s, p_dia_c)
                    + '</tr>'
                )

    # Total row (pinned)
    tot_gross = (prod_agg["delta"].abs().sum()) * 100 / nav
    tot_var = sum(v for v in rf_var_map.values())
    body += (
        '<tr class="pa-total-row" data-pinned="1" '
        'style="border-top:2px solid var(--border); font-weight:700">'
        '<td class="sum-fund" style="font-weight:700">Total</td>'
        + '<td></td><td></td>'
        + '<td></td>'  # σ empty at total
        + _cell(f'{tot_var:.1f}' if tot_var else '—',
                "var(--down)" if tot_var > 0 else "var(--muted)",
                "font-weight:700")
        + '<td></td><td></td>'
        + '</tr>'
    )

    def _th(label, sort_key, active=False, align="right"):
        arrow = '↓' if active else ''
        act_cls = ' uexpo-sort-active' if active else ''
        return (f'<th class="uexpo-sort-th{act_cls}" data-expo-sort="{sort_key}" '
                f'style="text-align:{align}; cursor:pointer; user-select:none" '
                f'onclick="evoExpoSort(this,\'{sort_key}\')">'
                f'{label} <span class="uexpo-arrow" '
                f'style="font-size:9px;opacity:{0.9 if active else 0}">{arrow}</span></th>')

    header = (
        _th("Strategy / LIVRO / Instrumento", "name",  active=False, align="left")
        + _th("Net %NAV",   "net",   active=True)
        + _th("Δ Expo",     "dexp",  active=False)
        + _th("σ (bps)",    "sigma", active=False)
        + _th("VaR (bps)",  "var",   active=False)
        + _th("Δ VaR",      "dvar",  active=False)
        + _th("DIA (bps)",  "dia",   active=False)
    )

    strategy_table = (
        f'<table id="tbl-uexpo-evolution-strat" class="summary-table" data-no-sort="1">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{body}</tbody>'
        f'</table>'
    )

    nav_fmt = f"{nav/1e6:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")

    view_toggle_js = r"""<script>
if (!window.__evoExpoJSLoaded) {
  window.__evoExpoJSLoaded = true;
  window.selectEvoExpoView = function(btn, view) {
    var head = btn.closest('.card-head');
    if (!head) return;
    head.querySelectorAll('.pa-tgl').forEach(function(b) {
      b.classList.toggle('active', b === btn);
    });
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('.evo-expo-view').forEach(function(div) {
      div.style.display = div.getAttribute('data-evo-view') === view ? '' : 'none';
    });
  };

  // Cascading sort — sorts strategies, then LIVROs within each strategy,
  // then instruments within each LIVRO, by the same column + direction.
  window.evoExpoSort = function(th, sortKey) {
    var table = th.closest('table'); if (!table) return;
    var tbody = table.tBodies[0];    if (!tbody) return;
    if (!window.__uexpoSortState) window.__uexpoSortState = {};
    var key = (table.id || '') + ':' + sortKey;
    var prev = window.__uexpoSortState[key];
    var state;
    if (sortKey === 'name') {
      state = (prev === 'asc') ? 'desc' : (prev === 'desc') ? 'default' : 'asc';
    } else {
      state = (prev === 'asc') ? 'desc' : 'asc';
    }
    window.__uexpoSortState = {}; window.__uexpoSortState[key] = state;

    // Build nested tree: strategies → livros → instruments
    var strategies = [];
    var totalRow = null;
    var curStrat = null, curLivro = null;
    Array.from(tbody.rows).forEach(function(r){
      if (r.classList.contains('pa-total-row')) { totalRow = r; return; }
      var lvl = r.getAttribute('data-expo-lvl');
      if (lvl === '0') {
        curStrat = { head: r, livros: [] };
        strategies.push(curStrat);
        curLivro = null;
      } else if (lvl === '1' && curStrat) {
        curLivro = { head: r, instruments: [] };
        curStrat.livros.push(curLivro);
      } else if (lvl === '2' && curLivro) {
        curLivro.instruments.push(r);
      }
    });

    function val(r, k) {
      if (k === 'name') return r.getAttribute('data-sort-name') || '';
      var raw = r.getAttribute('data-sort-' + k);
      if (raw === null || raw === '') return null;
      var n = parseFloat(raw);
      return isNaN(n) ? null : n;
    }
    var asc = (state === 'asc');
    function cmp(a, b) {
      if (sortKey === 'name') {
        var sa = val(a, 'name'), sb = val(b, 'name');
        return asc ? sa.localeCompare(sb) : sb.localeCompare(sa);
      }
      var va = val(a, sortKey), vb = val(b, sortKey);
      var aN = (va === null), bN = (vb === null);
      if (aN && bN) {
        var na = val(a, 'name'), nb = val(b, 'name');
        return asc ? na.localeCompare(nb) : nb.localeCompare(na);
      }
      if (aN) return 1;
      if (bN) return -1;
      return asc ? va - vb : vb - va;
    }

    if (state === 'default') {
      strategies.sort(function(a, b){
        var va = parseInt(a.head.getAttribute('data-default-order')||'0', 10);
        var vb = parseInt(b.head.getAttribute('data-default-order')||'0', 10);
        return va - vb;
      });
    } else {
      strategies.sort(function(a, b){ return cmp(a.head, b.head); });
      strategies.forEach(function(s){
        s.livros.sort(function(a, b){ return cmp(a.head, b.head); });
        s.livros.forEach(function(l){
          l.instruments.sort(function(a, b){ return cmp(a, b); });
        });
      });
    }

    strategies.forEach(function(s){
      tbody.appendChild(s.head);
      s.livros.forEach(function(l){
        tbody.appendChild(l.head);
        l.instruments.forEach(function(i){ tbody.appendChild(i); });
      });
    });
    if (totalRow) tbody.appendChild(totalRow);

    table.querySelectorAll('th[data-expo-sort]').forEach(function(h){
      var arrow = h.querySelector('.uexpo-arrow');
      if (!arrow) return;
      var active = (h === th) && (state !== 'default');
      var glyph  = state === 'asc' ? '↑' : state === 'desc' ? '↓' : '';
      arrow.textContent = (h === th) ? glyph : '';
      arrow.style.opacity = active ? '0.9' : '0';
      h.classList.toggle('uexpo-sort-active', active);
    });
  };
}
</script>"""

    return f"""
    {_UEXPO_JS}
    {view_toggle_js}
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — EVOLUTION</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · look-through completo (MACRO + SIST + FRONTIER + CREDITO + EVO_STRAT)</span>
        <div class="pa-view-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-evo-view="strat"
                  onclick="selectEvoExpoView(this,'strat')">Por Strategy</button>
          <button class="pa-tgl" data-evo-view="factor"
                  onclick="selectEvoExpoView(this,'factor')">Por Fator</button>
        </div>
      </div>

      <div class="evo-expo-view" data-evo-view="strat">{strategy_table}</div>
      <div class="evo-expo-view" data-evo-view="factor" style="display:none">{factor_table}</div>

      <div class="bar-legend" style="margin-top:10px">
        <b>Por Strategy</b>: Strategy → LIVRO → Instrumento (3 níveis, click ▶ para expandir).
        VaR alocado proporcionalmente ao Δ do instrumento dentro do fator. DIA em bps via <code>REPORT_ALPHA_ATRIBUTION</code> (FUNDO='EVOLUTION').
        <b>Por Fator</b>: taxonomia RF-* (idem MACRO) — fator × instrumento. Fonte: <code>LOTE_PRODUCT_EXPO</code>
        (TRADING_DESK_SHARE_SOURCE = Evolution) + <code>LOTE_BOOK_STRESS_RPM</code> LEVEL=3.
        <b>σ:</b> proxy via <code>STANDARD_DEVIATION_ASSETS</code> (BOOK='MACRO').
      </div>
    </section>"""


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


# ── RF Exposure Map (IDKAs + Albatroz) ───────────────────────────────────────
# Factor classification by PRODUCT_CLASS. Real = IPCA-linked (sens to real rates).
# Nominal = pre-fixed rate. CDI = floating (no rate sensitivity). Other = everything else.
_RF_FACTOR_MAP = {
    # Real rates (IPCA-linked)
    "NTN-B": "real", "NTN-C": "real",
    "DAP Future": "real", "DAPFuture": "real", "DAC Future": "real",
    # Nominal rates (pre)
    "DI1Future": "nominal", "NTN-F": "nominal", "LTN": "nominal",
    # CDI / floating
    "LFT": "cdi", "Cash": "cdi", "Cash BRL": "cdi",
    "Overnight": "cdi", "Compromissada": "cdi",
}

# Maturity buckets: 6m, then 1-yr blocks to concentrate allocation granularity
# key → (lower_yrs, upper_yrs, label)
_RF_BUCKETS = [
    ("0-6m",   0.0,  0.5),
    ("6-12m",  0.5,  1.0),
    ("1-2y",   1.0,  2.0),
    ("2-3y",   2.0,  3.0),
    ("3-4y",   3.0,  4.0),
    ("4-5y",   4.0,  5.0),
    ("5-6y",   5.0,  6.0),
    ("6-7y",   6.0,  7.0),
    ("7-8y",   7.0,  8.0),
    ("8-9y",   8.0,  9.0),
    ("9-10y",  9.0, 10.0),
    ("10y+",  10.0, 99.0),
]


def _rf_classify(product_class: str) -> str:
    return _RF_FACTOR_MAP.get(product_class, "other")


def _rf_bucket(yrs: float) -> str | None:
    if yrs is None or yrs != yrs or yrs < 0:  # NaN-safe
        return None
    for label, lo, hi in _RF_BUCKETS:
        if lo <= yrs < hi:
            return label
    return _RF_BUCKETS[-1][0]


def fetch_rf_exposure_map(desk: str, date_str: str = DATA_STR,
                           lookthrough_only: bool = False) -> pd.DataFrame:
    """Fetch position-level RF exposure for a desk with look-through to Albatroz.
       LOTE_PRODUCT_EXPO decomposes each bond into multiple PRIMITIVE_CLASS rows
       (IPCA / IPCA Coupon / Brazil Sovereign Yield / BRL Rate Curve). To avoid
       double-counting rate exposure, pick ONE primitive per position to represent
       the dominant rate factor:
         - Real-rate instruments  (NTN-B, NTN-C, DAP Future) → 'IPCA Coupon'
         - Nominal instruments    (DI1Future, NTN-F, LTN)    → 'BRL Rate Curve'
         - CDI/floating           (LFT, Cash, Compromissada) → 'Brazil Sovereign Yield'
       Additional 'IPCA' primitive rows are kept separately for IPCA-index carry
       (face-value exposure to inflation).
       lookthrough_only: when True, pull ALL positions with TRADING_DESK_SHARE_SOURCE = desk
          regardless of which desk physically holds them. Used for EVOLUTION (FIC fund
          with no direct RF book — all RF exposure arrives via look-through).
       Returns columns: source, via (direct/via_albatroz/lookthrough), BOOK, PRODUCT_CLASS,
                        PRIMITIVE_CLASS, PRODUCT, expiry, days_to_exp,
                        delta_brl, mod_dur, ano_eq_brl, factor, bucket
    """
    albatroz_td = "GALAPAGOS ALBATROZ FIRF LP"
    if lookthrough_only:
        # Single query: all desks where `desk` is the share_source (Evolution semantics).
        q_lt = f"""
        SELECT 'lookthrough' AS via,
               "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT", "EXPIRY",
               MIN("DAYS_TO_EXPIRATION") AS days_to_exp,
               SUM("DELTA")               AS delta_brl,
               MAX("MOD_DURATION")        AS mod_dur,
               SUM("POSITION")            AS position_brl
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = '{desk}'
          AND "VAL_DATE" = DATE '{date_str}'
          AND "DELTA" <> 0
        GROUP BY "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT", "EXPIRY"
        """
        parts = [read_sql(q_lt)]
    else:
        # Two sets: (a) direct positions held by the desk itself;
        # (b) positions held inside Albatroz that are attributable to this desk's
        # share of Albatroz (reverse attribution — exploded look-through).
        q_direct = f"""
        SELECT 'direct' AS via,
               "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT", "EXPIRY",
               MIN("DAYS_TO_EXPIRATION") AS days_to_exp,
               SUM("DELTA")               AS delta_brl,
               MAX("MOD_DURATION")        AS mod_dur,
               SUM("POSITION")            AS position_brl
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK" = '{desk}'
          AND "TRADING_DESK_SHARE_SOURCE" = '{desk}'
          AND "VAL_DATE" = DATE '{date_str}'
          AND "DELTA" <> 0
        GROUP BY "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT", "EXPIRY"
        """
        parts = [read_sql(q_direct)]
        # Skip the Albatroz look-through for Albatroz itself (prevents self-join).
        if desk != albatroz_td:
            q_via = f"""
            SELECT 'via_albatroz' AS via,
                   "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT", "EXPIRY",
                   MIN("DAYS_TO_EXPIRATION") AS days_to_exp,
                   SUM("DELTA")               AS delta_brl,
                   MAX("MOD_DURATION")        AS mod_dur,
                   SUM("POSITION")            AS position_brl
            FROM "LOTE45"."LOTE_PRODUCT_EXPO"
            WHERE "TRADING_DESK" = '{albatroz_td}'
              AND "TRADING_DESK_SHARE_SOURCE" = '{desk}'
              AND "VAL_DATE" = DATE '{date_str}'
              AND "DELTA" <> 0
            GROUP BY "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT", "EXPIRY"
            """
            parts.append(read_sql(q_via))
    df = pd.concat([p for p in parts if p is not None and not p.empty], ignore_index=True)
    if df.empty:
        return df
    for c in ("delta_brl", "mod_dur", "days_to_exp", "position_brl"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["delta_brl"] = df["delta_brl"].fillna(0.0)

    # Pick ONE primitive per (PRODUCT_CLASS, PRODUCT, EXPIRY) to represent the
    # dominant rate factor (avoids double-counting the decomposed legs).
    _RATE_PRIM_BY_CLASS = {
        "NTN-B": "IPCA Coupon", "NTN-C": "IPCA Coupon",
        "DAP Future": "IPCA Coupon", "DAPFuture": "IPCA Coupon",
        "DAC Future": "IPCA Coupon",
        "DI1Future": "BRL Rate Curve", "NTN-F": "BRL Rate Curve",
        "LTN": "BRL Rate Curve",
        "LFT": "Brazil Sovereign Yield",
    }
    df["_rate_prim"] = df["PRODUCT_CLASS"].map(_RATE_PRIM_BY_CLASS)
    # Keep only the chosen rate primitive per product, OR the 'IPCA' primitive
    # (which separately represents inflation-index carry, never double-counts rates).
    keep_mask = (
        (df["PRIMITIVE_CLASS"] == df["_rate_prim"]) |
        (df["PRIMITIVE_CLASS"] == "IPCA") |
        df["_rate_prim"].isna()  # keep unknown product classes as 'other'
    )
    df = df[keep_mask].drop(columns="_rate_prim").copy()

    df["yrs_to_mat"] = (df["days_to_exp"].fillna(0.0) / 365.25)
    df["position_brl"] = df["position_brl"].fillna(0.0)
    # DELTA in LOTE_PRODUCT_EXPO is the duration-weighted notional (= POSITION × MOD_DURATION)
    # but with hedge-side sign convention: a long bond position carries NEGATIVE delta in the
    # IPCA Coupon / BRL Rate Curve primitives (representing the short-DI hedge).
    # Negate it to recover the position's actual rate exposure (long bond → positive ANO_EQ).
    rate_prims = {"IPCA Coupon", "BRL Rate Curve"}
    df["ano_eq_brl"] = df.apply(
        lambda r: -r["delta_brl"] if r["PRIMITIVE_CLASS"] in rate_prims else r["delta_brl"],
        axis=1,
    )
    df["factor"]    = df["PRODUCT_CLASS"].map(_rf_classify).fillna("other")
    # 'IPCA' primitive rows represent inflation-index carry (face value); override factor.
    df.loc[df["PRIMITIVE_CLASS"] == "IPCA", "factor"] = "ipca_idx"
    df["bucket"]    = df["yrs_to_mat"].apply(_rf_bucket)
    # 'via' already set by the SQL ('direct' | 'via_albatroz')
    return df


def build_rf_exposure_map_section(short: str, df: pd.DataFrame, nav: float,
                                   bench_dur_yrs: float, bench_label: str) -> str:
    """Grouped bar chart of ANO_EQ (% NAV) by maturity bucket × factor,
       with Fund/Bench/Relative toggle and cumulative lines.
    """
    if df is None or df.empty or not nav:
        return ""

    rel = df[df["factor"].isin(["real", "nominal"])].copy()
    if rel.empty:
        return ""
    # Express ANO_EQ in years (ano_eq_brl / NAV). One unit = 1 year of duration
    # per 100% NAV. Bench below is also in years, so bars/table/stats align.
    rel["yr"] = rel["ano_eq_brl"] / nav
    pivot = (rel.pivot_table(index="bucket", columns="factor", values="yr",
                             aggfunc="sum", fill_value=0.0))
    bucket_order = [b[0] for b in _RF_BUCKETS]
    pivot = pivot.reindex(index=bucket_order, columns=["real", "nominal"], fill_value=0.0)

    # Per-bucket arrays in years-equivalent (unit: yr × NAV fraction)
    fund_real_b = pivot["real"].tolist()
    fund_nom_b  = pivot["nominal"].tolist()

    cdi_bench = (bench_dur_yrs == 0)

    def bench_bucket_idx(bench_dur: float) -> int:
        for i, (_, lo, hi) in enumerate(_RF_BUCKETS):
            if lo <= bench_dur < hi:
                return i
        return len(_RF_BUCKETS) - 1

    # Benchmark per-bucket: IDKAs = 100% NAV concentrated at bench_dur bucket, real only
    bench_real_b = [0.0] * len(bucket_order)
    bench_nom_b  = [0.0] * len(bucket_order)
    if not cdi_bench:
        bench_real_b[bench_bucket_idx(bench_dur_yrs)] = bench_dur_yrs

    # Relative = fund - bench per bucket
    rel_real_b = [fund_real_b[i] - bench_real_b[i] for i in range(len(bucket_order))]
    rel_nom_b  = [fund_nom_b[i]  - bench_nom_b[i]  for i in range(len(bucket_order))]

    # Cumulative series
    def cumsum(arr):
        out, s = [], 0.0
        for v in arr:
            s += v
            out.append(s)
        return out

    cum_real = cumsum(fund_real_b)
    cum_nom  = cumsum(fund_nom_b)
    bench_cum_real = cumsum(bench_real_b)
    bench_cum_nom  = cumsum(bench_nom_b)
    rel_cum_real   = cumsum(rel_real_b)
    rel_cum_nom    = cumsum(rel_nom_b)

    # Legacy names kept for the stat-row / legend blocks below
    bench_real = bench_cum_real
    bench_nom  = bench_cum_nom

    # Via-Albatroz breakdown
    via_df = df[df["factor"].isin(["real", "nominal"])]
    via_alb_total = float(via_df[via_df["via"] == "via_albatroz"]["ano_eq_brl"].sum() / nav) if nav else 0.0

    # SVG geometry — absoluto uses 3 bars/bucket; relativo uses 2 bars/bucket
    W, H = 760, 300
    pad_l, pad_r, pad_t, pad_b = 46, 26, 26, 38
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b
    n_buckets = len(bucket_order)
    band_w = plot_w / n_buckets
    inter_bucket = 4          # gap between buckets
    bar_w_abs = (band_w - inter_bucket) / 3   # absoluto: 3 bars flush
    bar_w_rel = (band_w - inter_bucket) / 2   # relativo: 2 bars flush

    # Y scale — fit both absoluto and relativo series
    all_vals = (fund_real_b + fund_nom_b + bench_real_b + bench_nom_b +
                rel_real_b + rel_nom_b +
                cum_real + cum_nom + bench_cum_real + rel_cum_real + rel_cum_nom)
    y_max = max([abs(v) for v in all_vals] + [1.0]) * 1.15
    y_min = min([0.0] + [v for v in all_vals]) * 1.15
    if y_min >= 0:
        y_min = -y_max * 0.10

    def y_scale(v):
        return pad_t + plot_h * (1.0 - (v - y_min) / (y_max - y_min))

    def x_band(i):
        return pad_l + i * band_w

    def bar_rect(x0, v, cls, factor, bw):
        y_top = y_scale(max(v, 0))
        y_bot = y_scale(min(v, 0))
        return (f'<rect class="rf-bar {cls}" data-factor="{factor}" '
                f'x="{x0:.1f}" y="{y_top:.1f}" width="{bw:.1f}" height="{max(y_bot - y_top, 0.5):.1f}"/>')

    # Absoluto: Fund Real | Fund Nominal | Bench (flush within bucket).
    # Bench bar's data-factor reflects its composition so the factor filter
    # (Real/Nominal) hides it appropriately — e.g., IDKA bench is 100% Real,
    # so clicking Nominal filter should hide it.
    abs_bars = []
    for i in range(n_buckets):
        x0 = x_band(i) + inter_bucket / 2
        x1 = x0 + bar_w_abs
        x2 = x1 + bar_w_abs
        bench_total = bench_real_b[i] + bench_nom_b[i]
        # Determine bench factor tag from its composition
        if bench_real_b[i] != 0 and bench_nom_b[i] == 0:
            bench_tag = "real"
        elif bench_nom_b[i] != 0 and bench_real_b[i] == 0:
            bench_tag = "nominal"
        else:
            bench_tag = "bench"  # mixed or zero — always visible
        abs_bars.append(bar_rect(x0, fund_real_b[i], "rf-real", "real", bar_w_abs))
        abs_bars.append(bar_rect(x1, fund_nom_b[i],  "rf-nom",  "nominal", bar_w_abs))
        abs_bars.append(bar_rect(x2, bench_total, "rf-benchbar", bench_tag, bar_w_abs))
    abs_bars_svg = "".join(abs_bars)

    # Relativo: Relative Real | Relative Nominal (fund − bench per bucket, flush)
    rel_bars = []
    for i in range(n_buckets):
        x0 = x_band(i) + inter_bucket / 2
        x1 = x0 + bar_w_rel
        rel_bars.append(bar_rect(x0, rel_real_b[i], "rf-real", "real", bar_w_rel))
        rel_bars.append(bar_rect(x1, rel_nom_b[i],  "rf-nom",  "nominal", bar_w_rel))
    rel_bars_svg = "".join(rel_bars)

    def poly_points(values):
        pts = []
        for i, v in enumerate(values):
            cx = x_band(i) + band_w / 2
            pts.append(f"{cx:.1f},{y_scale(v):.1f}")
        return " ".join(pts)

    abs_cum_r = f'<polyline class="rf-cum rf-cum-real" data-factor="real"    points="{poly_points(cum_real)}"/>'
    abs_cum_n = f'<polyline class="rf-cum rf-cum-nom"  data-factor="nominal" points="{poly_points(cum_nom)}"/>'
    abs_cum_b = f'<polyline class="rf-cum rf-cum-bench" data-factor="bench" points="{poly_points([a + b for a, b in zip(bench_cum_real, bench_cum_nom)])}"/>'
    rel_cum_r = f'<polyline class="rf-cum rf-cum-real" data-factor="real"    points="{poly_points(rel_cum_real)}"/>'
    rel_cum_n = f'<polyline class="rf-cum rf-cum-nom"  data-factor="nominal" points="{poly_points(rel_cum_nom)}"/>'

    # Zero line
    y0 = y_scale(0)
    zero_line = f'<line class="rf-zero" x1="{pad_l}" y1="{y0:.1f}" x2="{W - pad_r}" y2="{y0:.1f}"/>'

    # Y-axis ticks (5 ticks) — values are in years
    y_axis = ""
    for k in range(5):
        t = y_min + (y_max - y_min) * k / 4
        y = y_scale(t)
        y_axis += f'<line class="rf-grid" x1="{pad_l}" y1="{y:.1f}" x2="{W - pad_r}" y2="{y:.1f}"/>'
        y_axis += f'<text class="rf-axis-lbl" x="{pad_l - 6:.1f}" y="{y + 3:.1f}" text-anchor="end">{t:+.1f}y</text>'

    # X-axis labels
    x_axis = ""
    for i, b in enumerate(bucket_order):
        cx = x_band(i) + band_w / 2
        x_axis += f'<text class="rf-axis-lbl" x="{cx:.1f}" y="{H - 12:.1f}" text-anchor="middle">{b}</text>'

    # Single chart with two mode groups — JS toggles between Absoluto / Relativo
    svg = f"""
    <svg class="rf-expo-svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
      {y_axis}
      {zero_line}
      <g class="rf-mode-group" data-rf-mode="absoluto">{abs_bars_svg}{abs_cum_r}{abs_cum_n}{abs_cum_b}</g>
      <g class="rf-mode-group" data-rf-mode="relativo" style="display:none">{rel_bars_svg}{rel_cum_r}{rel_cum_n}</g>
      {x_axis}
    </svg>
    """

    # ── Summary stats ─────────────────────────────────────────────────────────
    dur_real  = sum(fund_real_b)
    dur_nom   = sum(fund_nom_b)
    dur_total = dur_real + dur_nom
    bench_total = bench_dur_yrs
    gap_total = dur_total - bench_total
    cdi_weight = float(df[df["factor"] == "cdi"]["delta_brl"].sum() / nav * 100) if nav else 0.0

    # Table: one row per bucket, showing Fund / Bench / Relative totals (sum real+nominal).
    def pct_cell(v, color_bias=True):
        if abs(v) < 0.005:
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        col = ("var(--up)" if v >= 0 else "var(--down)") if color_bias else "var(--text)"
        return f'<td class="mono" style="text-align:right; color:{col}">{v:+.2f}</td>'

    tbl_rows = ""
    for i, b in enumerate(bucket_order):
        f_r = fund_real_b[i]; f_n = fund_nom_b[i]; f_t = f_r + f_n
        b_r = bench_real_b[i]; b_n = bench_nom_b[i]; b_t = b_r + b_n
        r_t = f_t - b_t
        tbl_rows += (
            "<tr>"
            f'<td class="pa-name" style="font-weight:600">{b}</td>'
            + pct_cell(f_r) + pct_cell(f_n) + pct_cell(f_t)
            + pct_cell(b_t, color_bias=False)
            + pct_cell(r_t)
            + "</tr>"
        )
    tbl_footer = (
        '<tr class="pa-total-row">'
        '<td class="pa-name" style="font-weight:700">Total (yr eq.)</td>'
        + pct_cell(sum(fund_real_b)) + pct_cell(sum(fund_nom_b)) + pct_cell(sum(fund_real_b) + sum(fund_nom_b))
        + pct_cell(sum(bench_real_b) + sum(bench_nom_b), color_bias=False)
        + pct_cell((sum(fund_real_b) + sum(fund_nom_b)) - (sum(bench_real_b) + sum(bench_nom_b)))
        + "</tr>"
    )

    bench_note = (f"benchmark {bench_label} · constant-maturity {bench_total:.1f}yr, 100% NAV"
                  if not cdi_bench else f"benchmark {bench_label} · duração zero")
    nav_fmt = f"{nav/1e6:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")
    via_chip = (f'<span class="sn-stat"><span class="sn-lbl">via Albatroz</span>'
                f'<span class="sn-val mono">{via_alb_total:+.2f}yr</span></span>'
                if abs(via_alb_total) > 0.005 and short.startswith("IDKA") else "")
    stat_row = (
        '<div class="sn-inline-stats mono" style="margin-bottom:12px; flex-wrap:wrap; gap:6px 18px">'
        f'<span class="sn-stat"><span class="sn-lbl">NAV</span><span class="sn-val mono">R$ {nav_fmt}M</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Juros Reais (IPCA)</span><span class="sn-val mono">{dur_real:+.2f}yr</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Juros Nominais</span><span class="sn-val mono">{dur_nom:+.2f}yr</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Total Fund</span><span class="sn-val mono">{dur_total:+.2f}yr</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Bench</span><span class="sn-val mono">{bench_total:+.2f}yr</span></span>'
        '<span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>'
        f'<span class="sn-stat"><span class="sn-lbl">CDI</span><span class="sn-val mono">{cdi_weight:+.1f}%NAV</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Gap (Fund − Bench)</span><span class="sn-val mono" style="color:{"var(--down)" if abs(gap_total)>0.5 else "var(--text)"}">{gap_total:+.2f}yr</span></span>'
        + via_chip +
        '</div>'
    )

    # ── Position-level table (by asset) ───────────────────────────────────────
    pos = df[df["factor"].isin(["real", "nominal", "ipca_idx"])].copy()
    pos["ano_eq_yr"] = pos["ano_eq_brl"] / nav if nav else 0.0
    pos["pct_nav"]   = pos["position_brl"] / nav * 100 if nav else 0.0
    pos = pos.sort_values("ano_eq_yr", key=lambda s: s.abs(), ascending=False)

    def _p_cell(v, fmt, color_bias=False):
        if v is None or v != v or (isinstance(v, (int, float)) and abs(v) < 1e-9):
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        col = "var(--text)"
        if color_bias:
            col = "var(--up)" if v >= 0 else "var(--down)"
        return f'<td class="mono" style="text-align:right; color:{col}">{fmt.format(v)}</td>'

    pos_rows = ""
    for _, r in pos.iterrows():
        via_tag = '<span style="color:var(--muted); font-size:10px">via Albatroz</span>' if r["via"] == "via_albatroz" else ""
        pos_rows += (
            "<tr>"
            f'<td class="pa-name">{r["PRODUCT"]} {via_tag}</td>'
            f'<td class="mono" style="color:var(--muted); font-size:11px">{r["BOOK"]}</td>'
            f'<td class="mono" style="color:var(--muted); font-size:11px">{r["factor"]}</td>'
            + _p_cell(r.get("yrs_to_mat"), "{:.2f}y")
            + _p_cell(r.get("mod_dur"),    "{:.2f}")
            + _p_cell(r.get("pct_nav"),    "{:+.2f}%")
            + _p_cell(r.get("ano_eq_yr"),  "{:+.3f}", color_bias=True)
            + "</tr>"
        )
    positions_table_html = f"""
      <table class="pa-table" style="margin-top:8px; font-size:11px">
        <thead><tr>
          <th style="text-align:left">Produto</th>
          <th style="text-align:left">Book</th>
          <th style="text-align:left">Fator</th>
          <th style="text-align:right">Maturidade</th>
          <th style="text-align:right">Duration</th>
          <th style="text-align:right">Position (%NAV)</th>
          <th style="text-align:right">ANO_EQ (yr)</th>
        </tr></thead>
        <tbody>{pos_rows}</tbody>
      </table>"""

    # Absoluto / Relativo mode + Ambos/Real/Nominal factor filter
    toggle_html = (
        '<div class="rf-toggles" style="display:flex; gap:10px; margin-left:auto; flex-wrap:wrap">'
        f'<div class="pa-view-toggle rf-mode-toggle">'
        f'<button class="pa-tgl active" data-rf-mode="absoluto" onclick="selectRfMode(this,\'absoluto\')">Absoluto</button>'
        f'<button class="pa-tgl"        data-rf-mode="relativo" onclick="selectRfMode(this,\'relativo\')">Relativo</button>'
        '</div>'
        f'<div class="pa-view-toggle rf-view-toggle">'
        f'<button class="pa-tgl active" data-rf-view="both"    onclick="selectRfView(this,\'both\')">Ambos</button>'
        f'<button class="pa-tgl"        data-rf-view="real"    onclick="selectRfView(this,\'real\')">Juros Reais</button>'
        f'<button class="pa-tgl"        data-rf-view="nominal" onclick="selectRfView(this,\'nominal\')">Juros Nominais</button>'
        '</div>'
        '</div>'
    )

    return f"""
    <section class="card" id="rf-expo-{short}">
      <div class="card-head">
        <span class="card-title">Exposure Map — fatores RF</span>
        <span class="card-sub">— {short} · ANO_EQ (%NAV) por bucket de maturidade · {bench_note}</span>
        {toggle_html}
      </div>
      {stat_row}
      <div style="overflow-x:auto">{svg}</div>
      <div class="rf-legend mono" style="margin-top:6px; font-size:10.5px; color:var(--muted); text-align:center">
        <span class="rf-legend-item"><span class="rf-swatch rf-real"></span> Fund Juros Reais (IPCA)</span>
        <span class="rf-legend-item"><span class="rf-swatch rf-nom"></span> Fund Juros Nominais (Pré)</span>
        <span class="rf-legend-item"><span class="rf-swatch rf-benchbar"></span> Benchmark</span>
        <span class="rf-legend-item"><span class="rf-swatch rf-cum"></span> Cumulativo (Fund)</span>
      </div>
      <div style="margin-top:14px">
        <button class="rf-tbl-toggle" onclick="toggleRfTable(this)"
                aria-expanded="false">▸ Mostrar tabela (por bucket)</button>
        <div class="rf-tbl-wrap" style="display:none">
          <table class="pa-table" style="margin-top:8px" data-no-sort="1">
            <thead><tr>
              <th style="text-align:left">Bucket</th>
              <th style="text-align:right">Juros Reais (yr)</th>
              <th style="text-align:right">Juros Nominais (yr)</th>
              <th style="text-align:right">Fund Total</th>
              <th style="text-align:right">Bench</th>
              <th style="text-align:right">Relative</th>
            </tr></thead>
            <tbody>{tbl_rows}</tbody>
            <tfoot>{tbl_footer}</tfoot>
          </table>
        </div>
      </div>
      <div style="margin-top:10px">
        <button class="rf-tbl-toggle" onclick="toggleRfTable(this)"
                aria-expanded="false">▸ Mostrar posições (por ativo)</button>
        <div class="rf-tbl-wrap" style="display:none">
          {positions_table_html}
        </div>
      </div>
    </section>"""


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

    # Indexador rows with drill-down: each parent row expands to show its instruments.
    idx_rows = ""
    for _i, r in enumerate(by_idx.itertuples(index=False)):
        idx_id = f"alb-idx-{_i}"
        # Parent (clickable) row
        idx_rows += (
            f'<tr class="alb-idx-row" data-idx-id="{idx_id}" '
            f'style="cursor:pointer" onclick="albToggleIdx(this)">'
            f'<td class="pa-name" style="font-weight:600">'
            f'<span class="alb-idx-arrow" style="font-size:9px;margin-right:6px;color:var(--accent-2)">▶</span>'
            f'{r.indexador}</td>'
            + bp_pct(r.delta_brl)
            + mm_cell(r.gross_brl)
            + dur_cell(r.dur_w)
            + dv01_cell(r.dv01_brl)
            + "</tr>"
        )
        # Child rows: all instruments in this indexador, sorted by |DV01| desc
        _kids = df[df["indexador"] == r.indexador].copy()
        _kids["_abs_dv01"] = _kids["dv01_brl"].abs()
        _kids = _kids.sort_values("_abs_dv01", ascending=False)
        for c in _kids.itertuples(index=False):
            _gross_c = abs(c.delta_brl)
            idx_rows += (
                f'<tr class="alb-idx-child" data-idx-parent="{idx_id}" style="display:none">'
                f'<td class="pa-name" style="padding-left:28px">'
                f'{c.PRODUCT} '
                f'<span style="color:var(--muted); font-size:10px">({c.BOOK})</span></td>'
                + bp_pct(c.delta_brl)
                + mm_cell(_gross_c)
                + dur_cell(c.mod_dur)
                + dv01_cell(c.dv01_brl)
                + "</tr>"
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

      <div class="sn-inline-stats mono" style="margin-bottom:8px; display:flex; align-items:center; gap:10px">
        <span style="color:var(--muted); font-size:10.5px; letter-spacing:.12em; text-transform:uppercase">Por Indexador</span>
        <span style="color:var(--muted); font-size:9.5px">· click pra expandir</span>
        <span style="margin-left:auto; display:flex; gap:4px">
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="albExpandAllIdx()">▼ All</button>
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="albCollapseAllIdx()">▶ All</button>
        </span>
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
      <script>
      (function() {{
        if (window.albToggleIdx) return;
        window.albToggleIdx = function(tr) {{
          var arrow = tr.querySelector('.alb-idx-arrow');
          var open = arrow && arrow.textContent.trim() === '▼';
          if (arrow) arrow.textContent = open ? '▶' : '▼';
          var id = tr.getAttribute('data-idx-id');
          if (!id) return;
          document.querySelectorAll('.alb-idx-child[data-idx-parent="'+id+'"]').forEach(function(r) {{
            r.style.display = open ? 'none' : '';
          }});
        }};
        window.albExpandAllIdx = function() {{
          document.querySelectorAll('.alb-idx-row').forEach(function(tr) {{
            var a = tr.querySelector('.alb-idx-arrow');
            if (a && a.textContent.trim() === '▶') window.albToggleIdx(tr);
          }});
        }};
        window.albCollapseAllIdx = function() {{
          document.querySelectorAll('.alb-idx-row').forEach(function(tr) {{
            var a = tr.querySelector('.alb-idx-arrow');
            if (a && a.textContent.trim() === '▼') window.albToggleIdx(tr);
          }});
        }};
      }})();
      </script>

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


def fetch_evolution_direct_single_names(date_str: str = DATA_STR) -> tuple:
    """EVOLUTION direct-only equity — positions held by the Evolution desk itself
       (FMN_*, FCO, AÇÕES BR LONG books), excluding look-through from QUANT/Frontier/MACRO.
       Used for the consolidated house views to avoid double-counting.
    """
    return _fetch_single_names_generic(
        date_str,
        desk="Galapagos Evolution FIC FIM CP",
        source="Galapagos Evolution FIC FIM CP",
    )


def fetch_evolution_single_names(date_str: str = DATA_STR) -> tuple:
    """
    EVOLUTION single-name L/S — full look-through into QUANT (Bracco, Quant_PA),
    Evo Strategy (FMN_*, FCO, AÇÕES BR LONG), Frontier Ações, and Macro FIM (CI_COMMODITIES).
    ADRs / ADR Options / ETFs / FIIs excluded automatically by the PRODUCT_CLASS filter.
    """
    return _fetch_single_names_generic(
        date_str, desk=None, source="Galapagos Evolution FIC FIM CP"
    )


# ── EVOLUTION Exposure (look-through, 3-level Strategy → LIVRO → Instr) ────
_EVO_STRATEGY_ORDER = ["MACRO", "SIST", "FRONTIER", "CREDITO", "EVO_STRAT", "CAIXA", "OUTROS"]
_EVO_STRATEGY_COLOR = {
    "MACRO":     "#60a5fa",
    "SIST":      "#a78bfa",
    "FRONTIER":  "#34d399",
    "CREDITO":   "#fbbf24",
    "EVO_STRAT": "#fb923c",
    "CAIXA":     "#64748b",
    "OUTROS":    "#94a3b8",
}

def _load_evo_livros_map() -> dict[str, str]:
    """Return {LIVRO: STRATEGY}. Cached at module scope on first call."""
    global _EVO_LIVRO_TO_STRAT
    try:
        return _EVO_LIVRO_TO_STRAT  # type: ignore[name-defined]
    except NameError:
        pass
    path = (Path(__file__).parent / ".claude" / "skills"
            / "evolution-risk-concentration" / "assets" / "livros-map.json")
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    out: dict[str, str] = {}
    for strat, livros in raw.items():
        if strat.startswith("_"):
            continue
        key = "EVO_STRAT" if strat == "EVO_STRATEGY" else strat
        for livro in livros:
            out[livro] = key
    _EVO_LIVRO_TO_STRAT = out  # type: ignore[name-defined]
    return out

# LIVROs that don't appear in the skill's livros-map.json — mapped here so
# they roll up into the right strategy. Keeps the JSON canonical (extracted
# from the skill) while still covering actual Evolution positions.
_EVO_LIVRO_EXTRA_STRATEGY = {
    "AÇÕES BR LONG":            "EVO_STRAT",
    "RV BZ":                    "EVO_STRAT",
    "Crédito":                  "CREDITO",
    "GIS FI FUNDO IMOBILIÁRIO": "CREDITO",
    "GIS CUSTOS E PROVISÕES":   "CAIXA",
    "Caixa":                    "CAIXA",
    "Caixa USD":                "CAIXA",
    "Taxas e Custos":           "CAIXA",
}

def _evo_classify_livro(desk: str, book: str) -> str:
    """Derive the EVOLUTION-level LIVRO from (TRADING_DESK, BOOK)."""
    bu = (book or "").upper()
    # Credit-flavored books (highest priority — before PM parse)
    if "CRED" in bu or "FIDC" in bu:
        return "Crédito"
    if "FUNDO IMOBILI" in bu:
        return "GIS FI FUNDO IMOBILIÁRIO"
    if "CUSTOS E PROVIS" in bu:
        return "GIS CUSTOS E PROVISÕES"
    # Cash / structural buckets
    if bu in ("CAIXA", "CAIXA BRL"):
        return "Caixa"
    if bu == "CAIXA USD":
        return "Caixa USD"
    if bu in ("", "MAIN", "DEFAULT", "TAXAS E CUSTOS"):
        return "Taxas e Custos"
    # Evolution direct equity (non-Frontier) — bucketed into EVO_STRAT
    if book == "AÇÕES BR LONG":
        return "AÇÕES BR LONG"
    # FRONTIER sub-books
    if book.startswith("FMN"):
        return "FMN"
    if book.startswith("FCO"):
        return "FCO"
    if book.startswith("FLO"):
        return "FLO"
    # MACRO PM prefix — works regardless of which desk the row came from.
    pm = _parse_pm(book)
    if pm in _PM_LIVRO:
        return _PM_LIVRO[pm]
    # Keep as-is (may match a LIVRO in livros-map.json directly — Giro_Master,
    # LF_RV-BZ_SS, Bracco, SIST_*, etc.)
    return book

def _evo_classify_factor(book: str, primitive_class: str) -> str | None:
    """Classify a position into the shared RF-* factor taxonomy used by MACRO
       Exposure (so we can reuse `_build_expo_unified_table`).
       Returns None for rows that should be excluded (Cash/Margin/Provisions)."""
    # MACRO look-through rows have BOOK names like 'JD_RF-BZ_Direcional'
    rf = _parse_rf(book)
    if rf is not None:
        return rf
    # Otherwise infer from PRIMITIVE_CLASS
    if primitive_class in _EXCL_PRIM:
        return None
    if primitive_class in ("BRL Rate Curve", "IPCA Coupon", "IPCA",
                           "Brazil Sovereign Yield", "LFT Premium"):
        return "RF-BZ"
    if primitive_class == "BRD Rate Curve":
        return "RF-DM"
    if primitive_class == "FX":
        return "FX-BRL"
    if primitive_class in ("Equity", "Equity Receipts"):
        return "RV-BZ"
    if primitive_class == "Commodities":
        return "COMMODITIES"
    return None

def fetch_evolution_exposure(date_str: str = DATA_STR) -> tuple:
    """EVOLUTION full look-through exposure.
       Returns (df, nav). df cols: TRADING_DESK, BOOK, PRODUCT, PRODUCT_CLASS,
         PRIMITIVE_CLASS, delta, livro, strategy, factor, pct_nav, sigma.
       Sources: LOTE_PRODUCT_EXPO (TRADING_DESK_SHARE_SOURCE = Evolution) +
                STANDARD_DEVIATION_ASSETS (σ per instrument, BOOK='MACRO' as proxy).
    """
    nav = _latest_nav("Galapagos Evolution FIC FIM CP", date_str)
    if nav is None:
        return None, None
    df = read_sql(f"""
        SELECT "TRADING_DESK", "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT",
               SUM("DELTA")    AS delta,
               SUM("POSITION") AS position
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Evolution FIC FIM CP'
          AND "VAL_DATE"                   = DATE '{date_str}'
          AND "DELTA" <> 0
        GROUP BY "TRADING_DESK", "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS", "PRODUCT"
    """)
    if df.empty:
        return df, nav
    for c in ("delta", "position"):
        df[c] = df[c].astype(float).fillna(0.0)

    livro_to_strat = {**_load_evo_livros_map(), **_EVO_LIVRO_EXTRA_STRATEGY}
    df["livro"]    = df.apply(lambda r: _evo_classify_livro(r["TRADING_DESK"], r["BOOK"]), axis=1)
    df["strategy"] = df["livro"].map(livro_to_strat).fillna("OUTROS")
    df["factor"]   = df.apply(lambda r: _evo_classify_factor(r["BOOK"], r["PRIMITIVE_CLASS"]),
                              axis=1)
    # Drop excluded rows (Cash/Margin/Provisions) before computing pct_nav
    df = df[df["factor"].notna()].copy()
    df["pct_nav"] = df["delta"] * 100 / nav

    # σ per instrument (same source as MACRO — proxy for Evolution positions)
    sigma_df = read_sql(f"""
        SELECT "INSTRUMENT" AS "PRODUCT", "STANDARD_DEVIATION" AS sigma
        FROM q_models."STANDARD_DEVIATION_ASSETS"
        WHERE "VAL_DATE" = DATE '{date_str}'
          AND "BOOK"     = 'MACRO'
    """)
    if not sigma_df.empty:
        df = df.merge(sigma_df, on="PRODUCT", how="left")
    else:
        df["sigma"] = float("nan")
    return df, nav


def fetch_evolution_var(date_str: str = DATA_STR) -> pd.DataFrame:
    """EVOLUTION VaR per (factor=BOOK at LEVEL=3, PRODUCT, PRODUCT_CLASS).
       Same shape as MACRO's var_df — column name `rf` used by _build_expo_unified_table
       after rename. Returns df with BOOK, PRODUCT, PRODUCT_CLASS, var_brl, var_pct.
    """
    nav = _latest_nav("Galapagos Evolution FIC FIM CP", date_str) or 1.0
    df = read_sql(f"""
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
    if df.empty:
        df["var_pct"] = []
        return df
    df["var_brl"] = df["var_brl"].astype(float).fillna(0.0)
    df["var_pct"] = df["var_brl"] * -10000 / nav
    df.rename(columns={"BOOK": "rf"}, inplace=True)
    return df


def fetch_evolution_pnl_products(date_str: str) -> pd.DataFrame:
    """Daily PnL per (LIVRO, PRODUCT) for FUNDO='EVOLUTION', in bps of NAV."""
    return read_sql(f"""
        SELECT "LIVRO", "PRODUCT", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'EVOLUTION'
          AND "DATE"  = DATE '{date_str}'
        GROUP BY "LIVRO", "PRODUCT"
    """)


def build_exposure_section(df_expo: pd.DataFrame, df_var: pd.DataFrame, aum: float,
                           df_expo_d1: pd.DataFrame = None, df_var_d1: pd.DataFrame = None,
                           df_pnl_prod: pd.DataFrame = None, pm_margem: dict = None) -> str:
    """POSIÇÕES (unified factor × produto table) + PM VaR toggle."""

    def delta_str(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "—", "#475569"
        c = "#f87171" if val < 0 else "#4ade80"
        return f'{val:+.1f}', c

    def dv(val):
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

    # ── D-1 lookup tables (for PM VaR view) ──────────────────────────────────
    d1_prod_expo = {}
    if df_expo_d1 is not None and not df_expo_d1.empty:
        for _, r in (df_expo_d1.groupby(["rf","PRODUCT","PRODUCT_CLASS"])
                                .agg(pct_nav=("pct_nav","sum")).reset_index()).iterrows():
            d1_prod_expo[(r["rf"], r["PRODUCT"], r["PRODUCT_CLASS"])] = r["pct_nav"]

    dia_lookup = {}
    if df_pnl_prod is not None and not df_pnl_prod.empty:
        for _, r in df_pnl_prod.iterrows():
            dia_lookup[(r["LIVRO"], r["PRODUCT"])] = float(r["dia_bps"])

    rf_var  = df_var.groupby("rf").agg(var_pct=("var_pct","sum")).reset_index()

    # ── PM × product breakdown for grouped PM VaR view ───────────────────────
    pm_prod = (df_expo.groupby(["rf","pm","PRODUCT","PRODUCT_CLASS"])
                      .agg(pct_nav=("pct_nav","sum"), delta=("delta","sum"),
                           sigma=("sigma","mean"))
                      .reset_index())
    rf_delta_tot = df_expo.groupby("rf").agg(rf_delta=("delta","sum")).reset_index()
    pm_prod = pm_prod.merge(rf_delta_tot, on="rf").merge(rf_var[["rf","var_pct"]], on="rf", how="left")
    pm_prod["prod_var_pct"] = np.where(
        pm_prod["rf_delta"] != 0,
        pm_prod["delta"] / pm_prod["rf_delta"] * pm_prod["var_pct"],
        0.0
    )

    # ── POSIÇÕES: unified factor × produto table ─────────────────────────────
    # Rename rf → factor for the shared helper.
    _expo_u    = df_expo.rename(columns={"rf": "factor"})
    _var_u     = df_var.rename(columns={"rf": "factor"})
    _expo_d1_u = df_expo_d1.rename(columns={"rf": "factor"}) if df_expo_d1 is not None and not df_expo_d1.empty else None
    _var_d1_u  = df_var_d1.rename(columns={"rf": "factor"})  if df_var_d1  is not None and not df_var_d1.empty  else None
    rf_view_table = _build_expo_unified_table(
        fund_key="macro",
        nav=aum,
        df=_expo_u,
        df_d1=_expo_d1_u,
        df_var=_var_u,
        df_var_d1=_var_d1_u,
        factor_order=_RF_ORDER,
    )

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
function _sortBodyByCol(tbody, colIdx, asc) {
    if (!tbody) return;
    var rows = Array.from(tbody.rows);
    rows.sort(function(a, b) {
        var va = _getVal(a, colIdx), vb = _getVal(b, colIdx);
        var na = va.n, nb = vb.n;
        if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
        if (!isNaN(na)) return -1;
        if (!isNaN(nb)) return  1;
        return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    });
    rows.forEach(function(r){ tbody.appendChild(r); });
}
// PM-level colIdx → instrument-level colIdx (instrument tables have extra "Expo%" col).
// PM:   0=PM  1=σ  2=ΔExpo  3=VaR  4=ΔVaR  5=Margem  6=DIA
// Inst: 0=Ins 1=Expo% 2=σ 3=ΔExpo 4=VaR 5=ΔVaR 6=DIA
var _PMV_TO_INST_COL = {0: 0, 1: 2, 2: 3, 3: 4, 4: 5, 6: 6};  // 5 (Margem) has no instrument counterpart
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
    // Cascade: when sorting the PM-level table, sort each PM's instrument table by the same key.
    if (tbodyId === 'tbody-pmv' && colIdx in _PMV_TO_INST_COL) {
        var instCol = _PMV_TO_INST_COL[colIdx];
        document.querySelectorAll('tbody[id^="tbody-inst-"]').forEach(function(tb){
            _sortBodyByCol(tb, instCol, asc);
        });
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
        if (btn) btn.classList.toggle('active', b===v);
    });
    var rfv = document.getElementById('macro-rf-view');
    var pmv = document.getElementById('macro-pm-view');
    if (rfv) rfv.style.display = v==='pos' ? '' : 'none';
    if (pmv) pmv.style.display = v==='pmv' ? '' : 'none';
}
function toggleDrillPM(id) {
    var row = document.getElementById(id);
    if (row) row.style.display = row.style.display === 'none' ? '' : 'none';
}
</script>"""

    nav_fmt = f"{aum/1e6:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")

    return f"""
    {_UEXPO_JS}
    {js}
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — MACRO</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · por fator de risco e por PM</span>
        <div class="pa-view-toggle" style="margin-left:auto">
          <button id="mbtn-pos" class="pa-tgl active" onclick="setMacroView('pos')">POSIÇÕES</button>
          <button id="mbtn-pmv" class="pa-tgl"        onclick="setMacroView('pmv')">PM VaR</button>
        </div>
      </div>
      <div id="macro-rf-view">{rf_view_table}</div>
      <div id="macro-pm-view" style="display:none">{global_pm_table}</div>
    </section>"""

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

_VR_PORTFOLIOS = {
    # fund_short -> list of (pkey, label, kind). kind in {"fund","livro"}.
    # Scope = fund total + books/PMs only (factor breakdown excluded).
    "MACRO": [
        (pk, lbl, kd) for pk, lbl, kd, _, fs in _DIST_PORTFOLIOS
        if fs == "MACRO" and kd in ("fund", "livro")
    ],
    "EVOLUTION": [("EVOLUTION", "EVOLUTION total", "fund")],
    "QUANT": [
        ("SIST",        "QUANT total",  "fund"),
        ("SIST_RF",     "Sub · RF",     "livro"),
        ("SIST_FX",     "Sub · FX",     "livro"),
        ("SIST_COMMO",  "Sub · Commo",  "livro"),
        ("SIST_GLOBAL", "Sub · Global", "livro"),
        ("Bracco",      "Bracco",       "livro"),
        ("Quant_PA",    "Quant PA",     "livro"),
    ],
}

def build_vol_regime_section(fund_short: str, vol_regime_map: dict) -> str:
    """Per-fund Vol Regime card: fund total + per-book rows.
       Carteira atual × janela HS. std rolling 21d vs. std da janela completa.
       Primary: pct_rank (non-parametric). Ratio = vol 21d / vol full."""
    portfolios = _VR_PORTFOLIOS.get(fund_short, [])
    if not portfolios or not vol_regime_map:
        return ""

    regime_col = {
        "low": "var(--up)", "normal": "var(--muted)",
        "elevated": "var(--warn)", "stressed": "var(--down)", "—": "var(--muted)",
    }
    def _pct_col(p):
        if p is None: return "var(--muted)"
        if p >= 90: return "var(--down)"
        if p >= 70: return "var(--warn)"
        if p < 20:  return "var(--up)"
        return "var(--text)"
    def _ratio_col(r):
        if r is None: return "var(--muted)"
        if r >= 1.5: return "var(--down)"
        if r >= 1.2: return "var(--warn)"
        if r <= 0.7: return "var(--up)"
        return "var(--text)"

    def _z_col(z):
        if z is None:      return "var(--muted)"
        if abs(z) >= 2:    return "var(--down)"
        if abs(z) >= 1:    return "var(--warn)"
        return "var(--text)"

    rows = ""
    any_data = False
    any_book_with_data = False
    n_obs_ref = None
    fund_label = FUND_LABELS.get(fund_short, fund_short)
    for pkey, label, kind in portfolios:
        r = vol_regime_map.get(pkey)
        tag, tag_c = _kind_tag(kind)
        is_fund = (kind == "fund")
        # Parent fund row = pinned, never sorts away. Children = hidden by default,
        # toggled via the ▶ caret in the parent.
        if is_fund:
            caret = f'<span class="vr-caret" data-fs="{fund_short}" style="cursor:pointer;user-select:none;color:var(--accent-2);font-weight:700;margin-right:4px">▶</span>'
            row_cls = 'metric-row vr-row vr-fund'
            row_attr = f' data-pinned="1" data-fs="{fund_short}"'
        else:
            caret = ""
            row_cls = 'metric-row vr-row vr-book'
            row_attr = f' data-parent="{fund_short}" style="display:none"'

        if not r:
            rows += (
                f'<tr class="{row_cls}"{row_attr}>'
                f'<td class="dist-tag" style="color:{tag_c}">{caret}{tag}</td>'
                f'<td class="dist-name">{label}</td>'
                '<td class="dist-num mono" style="color:var(--muted)">—</td>'
                '<td style="text-align:center">—</td>'
                '<td class="dist-num mono" style="color:var(--muted)">—</td>'
                '<td style="text-align:center">—</td>'
                '<td class="dist-num mono" style="color:var(--muted)">—</td>'
                '<td class="dist-num mono" style="color:var(--muted)">—</td>'
                '<td class="mono" style="color:var(--muted); text-align:center">—</td>'
                "</tr>"
            )
            continue
        any_data = True
        if not is_fund:
            any_book_with_data = True
        if n_obs_ref is None:
            n_obs_ref = r["n_obs"]
        vol_r = r["vol_recent_pct"]
        ratio = r["ratio"]
        pct_v = r["pct_rank"]
        z_v   = r["z"]
        regime = r["regime"]
        pct_s = f'{pct_v:.0f}°' if pct_v is not None else '—'
        z_s   = f'{z_v:+.2f}'   if z_v is not None   else '—'

        vol_range = range_line_svg(
            vol_r, r["vol_min_pct"], r["vol_max_pct"],
            v_p50=r["vol_p50_pct"], fmt="{:.2f}%",
        )
        z_range = range_line_svg(
            z_v, r["z_min"], r["z_max"],
            v_p50=r["z_p50"], fmt="{:+.1f}",
        ) if z_v is not None else ""

        rows += (
            f'<tr class="{row_cls}"{row_attr}>'
            f'<td class="dist-tag" style="color:{tag_c}">{caret}{tag}</td>'
            f'<td class="dist-name">{label}</td>'
            f'<td class="dist-num mono">{vol_r:.2f}%</td>'
            f'<td style="text-align:center;padding:4px 6px">{vol_range}</td>'
            f'<td class="dist-num mono" style="color:{_ratio_col(ratio)}; font-weight:700">{ratio:.2f}x</td>'
            f'<td style="text-align:center;padding:4px 6px">{z_range}</td>'
            f'<td class="dist-num mono" style="color:{_z_col(z_v)}; font-weight:700">{z_s}</td>'
            f'<td class="dist-num mono" style="color:{_pct_col(pct_v)}; font-weight:700">{pct_s}</td>'
            f'<td class="mono" style="color:{regime_col.get(regime,"var(--muted)")}; text-align:center; font-weight:700">{regime}</td>'
            "</tr>"
        )
    if not any_data:
        return ""
    sub = f"— {fund_short} · carteira atual × HS ({n_obs_ref}d)"
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Vol Regime</span>
        <span class="card-sub">{sub}</span>
      </div>
      <table class="metric-table dist-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left;width:70px">Tipo</th>
            <th style="text-align:left">Nome</th>
            <th style="text-align:right;width:80px">Vol 21d</th>
            <th style="text-align:center;width:230px">Range Vol</th>
            <th style="text-align:right;width:70px">Ratio</th>
            <th style="text-align:center;width:230px">Range z</th>
            <th style="text-align:right;width:70px">z</th>
            <th style="text-align:right;width:70px">Pct</th>
            <th style="text-align:center;width:90px">Regime</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        <b>▶</b> clique no caret para expandir books/PMs do fundo ·
        <b>Range Vol / Range z</b>: linha = [min, max] da janela HS; tick cinza = mediana; dot = atual ·
        <b>Ratio</b> = vol 21d / vol full (&gt;1.2 amarelo · &gt;1.5 vermelho · &lt;0.7 verde) ·
        <b>z</b> = (std 21d − média) / σ das rolling windows (direcional; N_eff baixo pela sobreposição) ·
        <b>Pct</b> = percentil da std(21d) ·
        <b>Regime</b>: low/normal/elevated/stressed via pct
      </div>
    </section>"""


def build_distribution_card(fund_short: str, dist_map_now: dict, dist_map_prev: dict,
                            actuals: dict) -> str:
    """Single card with toggle between Backward (D-1 carteira + realized DIA) and Forward (D carteira profile)."""
    bw_table = _build_backward_table(fund_short, dist_map_prev, actuals)
    fw_table = _build_forward_table(fund_short, dist_map_now)
    if not bw_table and not fw_table:
        return ""
    dck_id = f"dist-{fund_short.lower()}"
    # Frontier uses realized α vs IBOV (no HS engine); annotate the sub so it's clear.
    if fund_short == "FRONTIER":
        sub = f"— {fund_short} · α vs IBOV · bps de NAV · realizado"
    else:
        sub = f"— {fund_short} · bps de NAV"
    return f"""
    <section class="card" id="{dck_id}">
      <div class="card-head" style="display:flex;align-items:center;justify-content:space-between">
        <div>
          <span class="card-title">Distribuição 252d</span>
          <span class="card-sub">{sub}</span>
        </div>
        <div class="dist-toggle">
          <button class="dist-btn"        data-mode="backward" onclick="setDistMode('{dck_id}','backward')">Backward</button>
          <button class="dist-btn active" data-mode="forward"  onclick="setDistMode('{dck_id}','forward')">Forward</button>
        </div>
      </div>
      <div class="dist-view"        data-mode="backward" style="display:none">{bw_table or '<div class="empty-view">Sem dados backward (D-1 sem simulação).</div>'}</div>
      <div class="dist-view active" data-mode="forward">{fw_table or '<div class="empty-view">Sem dados forward (D sem simulação).</div>'}</div>
    </section>"""

def _build_backward_table(fund_short: str, dist_map_prev: dict, actuals: dict) -> str:
    """Backward-looking: yesterday's carteira × last 252d, with today's realized DIA overlayed.
       Answers: 'where did today's move land in the historical distribution of D-1 carteira?'

       Drill-down: fund row always visible; livro/rf rows hidden until fund row
       is clicked (caret toggle). Parent key = fund_short.
    """
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

        if kind == "fund":
            caret = f'<span class="dist-caret" data-dist-parent="{fund_short}">▶</span> '
            row_attrs = (f'class="metric-row dist-row-fund" data-dist-kind="fund" '
                         f'data-dist-key="{fund_short}" '
                         f'onclick="toggleDistChildren(this)" style="cursor:pointer"')
        else:
            caret = ''
            row_attrs = (f'class="metric-row dist-row-child" '
                         f'data-dist-kind="{kind}" data-dist-parent="{fund_short}" '
                         f'style="display:none"')

        rows += f"""
        <tr {row_attrs}>
          <td class="dist-tag" style="color:{tag_c}">{tag}</td>
          <td class="dist-name">{caret}{label}</td>
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
    """Forward-looking: today's carteira × last 252d. Describes expected P&L profile.
       Drill-down: fund row clickable, child rows (livro/rf) hidden by default.
    """
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
        if kind == "fund":
            caret = f'<span class="dist-caret" data-dist-parent="{fund_short}">▶</span> '
            row_attrs = (f'class="metric-row dist-row-fund" data-dist-kind="fund" '
                         f'data-dist-key="{fund_short}" '
                         f'onclick="toggleDistChildren(this)" style="cursor:pointer"')
        else:
            caret = ''
            row_attrs = (f'class="metric-row dist-row-child" '
                         f'data-dist-kind="{kind}" data-dist-parent="{fund_short}" '
                         f'style="display:none"')
        rows += f"""
        <tr {row_attrs}>
          <td class="dist-tag" style="color:{tag_c}">{tag}</td>
          <td class="dist-name">{caret}{label}</td>
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
            <th style="text-align:right;width:70px"><span class="kc">VaR</span> 95</th>
            <th style="text-align:right;width:60px">Média</th>
            <th style="text-align:right;width:70px"><span class="kc">VaR</span> +95</th>
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


# ── MACRO · PM Budget vs VaR (parametric + historic) ────────────────────────
#
# Compara, para cada PM do Macro:
#   · Budget mensal base (63 bps, o mesmo valor que inicia todo mês — sem carry)
#   · VaR paramétrico 1d do book (proporcional ao Δ do PM dentro do fator, em bps)
#   · VaR histórico 1d 95% para janelas 21d / 63d / 252d
#        VaR_hist_N = -quantile(pnl_diario_bps[-N:], 5%)
#   · Pior dia observado nos últimos 252d
#
# Objetivo: visualizar a calibração do budget contra múltiplas estimativas
# de risco do próprio PM. Se o VaR for maior que o budget, 1 dia ruim já
# esgota o orçamento do mês — sinal de miscalibração.

def fetch_macro_pm_book_var(date_str: str = DATA_STR) -> dict[str, float]:
    """Book-report VaR por PM do MACRO — **não diversificado entre books**.
       Fonte: LOTE_FUND_STRESS_RPM (TRADING_DESK=Macro FIM, TREE=Main_Macro_Ativos,
       LEVEL=10). Cada BOOK é uma folha tipo CI_RF-BZ_Direcional, JD_COMMODITIES_*, etc.
       Soma-se |PARAMETRIC_VAR| por prefixo de PM → magnitude de perda em bps de NAV.
       Usa |·| por book para preservar o caráter conservador "não diversificado"
       (não permite hedge natural entre books do mesmo PM)."""
    nav = _latest_nav("Galapagos Macro FIM", date_str) or 1.0
    df = read_sql(f"""
        SELECT "BOOK", SUM("PARAMETRIC_VAR") AS var_brl
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "TRADING_DESK" = 'Galapagos Macro FIM'
          AND "VAL_DATE"     = DATE '{date_str}'
          AND "TREE"         = 'Main_Macro_Ativos'
          AND "LEVEL"        = 10
        GROUP BY "BOOK"
    """)
    if df.empty:
        return {}
    out: dict[str, float] = {}
    for pm in ("CI", "LF", "JD", "RJ"):
        mask = df["BOOK"].str.startswith(f"{pm}_") | (df["BOOK"] == pm)
        # Sum SIGNED PARAMETRIC_VAR across the PM's books — within-PM offsets are
        # preserved (a long bond book + short DI hedge book partially cancel).
        # Magnitude afterwards converts to positive "loss in bps of NAV".
        # This is diversified WITHIN the PM's books but NOT across PMs (we never
        # mix books of different PMs).
        var_brl_signed_sum = float(df.loc[mask, "var_brl"].sum())
        out[pm] = float(abs(var_brl_signed_sum) * 10000 / nav) if nav else 0.0
    return out


def build_pm_budget_vs_var_section(pm_book_var: dict[str, float],
                                    pm_hs_var: dict[str, dict],
                                    pm_margem: dict[str, float]) -> str:
    """Card: tabela PM × {Margem atual, VaR book-report, VaR paramétrico HS 21/63/252, pior dia}.
       Thresholds de cor (por linha) relativos à Margem atual de cada PM:
         · val ≥ 2σ (= Margem × 2/1.645)   → vermelho
         · val ≥ Margem (= 1 VaR)          → laranja
         · val ≥ 1σ (= Margem / 1.645)     → amarelo
    """
    pm_order = ["CI", "LF", "JD", "RJ"]
    pm_label = {"CI": "CI (Comitê)", "LF": "Luiz Felipe",
                "JD": "Joca Dib", "RJ": "Rodrigo Jafet"}

    pm_param_var = pm_book_var or {}
    pm_margem    = pm_margem or {}
    pm_hs_var    = pm_hs_var or {}

    def _bps(v, decimals=1):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        return f'<td class="mono" style="text-align:right">{v:,.{decimals}f}</td>'

    def _var_cell(v, ref):
        """Cell colored by vol-multiples of the PM's current Margem (ref).
           ref = Margem atual do PM (interpreted as 1 VaR = 1.645σ).
        """
        if v is None or (isinstance(v, float) and pd.isna(v)) or ref is None or ref <= 0:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        sigma = ref / 1.645
        two_s = 2 * sigma
        if   v >= two_s: col = "var(--down)"    # > 2σ
        elif v >= ref:   col = "#fb923c"        # > Margem (1 VaR)
        elif v >= sigma: col = "var(--warn)"    # > 1σ
        else:            col = "var(--text)"
        return f'<td class="mono" style="text-align:right;color:{col}">{v:,.1f}</td>'

    def _ratio_cell(v, ref):
        """Dias até esgotar a Margem atual ao pace do maior VaR."""
        if v is None or (isinstance(v, float) and pd.isna(v)) or v <= 0 or ref is None or ref <= 0:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        days = ref / v
        if   days < 1:   col = "var(--down)"
        elif days < 2:   col = "#fca5a5"
        elif days < 4:   col = "var(--warn)"
        else:            col = "var(--up)"
        return f'<td class="mono" style="text-align:right;color:{col}">{days:,.1f}d</td>'

    rows = ""
    for pm in pm_order:
        if pm not in pm_param_var and pm not in pm_hs_var:
            continue
        margem  = pm_margem.get(pm)
        v_param = pm_param_var.get(pm)
        h = pm_hs_var.get(pm, {})
        v_21, v_63, v_252 = h.get("v21"), h.get("v63"), h.get("v252")
        worst  = h.get("worst")
        n_obs  = h.get("n", 0)
        # Max estimate — conservative risk pacer for "days to exhaust"
        _vals = [x for x in (v_param, v_21, v_63, v_252) if x is not None and not pd.isna(x)]
        v_max = max(_vals) if _vals else None
        # Margem cell color — standalone styling (just blue accent like stop monitor)
        marg_s = f'{margem:,.0f}' if margem is not None else '—'
        marg_html = f'<td class="mono" style="text-align:right;color:var(--accent-2);font-weight:700">{marg_s}</td>'
        rows += (
            f'<tr>'
            f'<td style="padding:5px 6px"><b>{pm}</b> '
            f'<span style="color:var(--muted);font-size:11px">· {pm_label[pm]}</span></td>'
            + marg_html
            + _var_cell(v_param, margem)
            + _var_cell(v_21,    margem)
            + _var_cell(v_63,    margem)
            + _var_cell(v_252,   margem)
            + _var_cell(worst,   margem)
            + _ratio_cell(v_max, margem)
            + f'<td class="mono" style="text-align:right;color:var(--muted);font-size:11px">{n_obs}</td>'
            + '</tr>'
        )

    if not rows:
        return ""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Budget vs VaR por PM — MACRO</span>
        <span class="card-sub">Margem atual × VaR paramétrico (Lote) × VaR hist 1d 95% (σ × 1.645)
        sobre posição atual × 21d/63d/252d · cor: múltiplos de vol vs Margem de cada PM</span>
      </div>

      <table class="summary-table">
        <thead>
          <tr>
            <th style="text-align:left">PM</th>
            <th style="text-align:right">Margem atual</th>
            <th style="text-align:right">VaR paramétrico</th>
            <th style="text-align:right">VaR hist 21d</th>
            <th style="text-align:right">VaR hist 63d</th>
            <th style="text-align:right">VaR hist 252d</th>
            <th style="text-align:right">Worst day pos.</th>
            <th style="text-align:right">Dias p/ esgotar</th>
            <th style="text-align:right">obs</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>

      <div class="bar-legend" style="margin-top:10px">
        <b>Margem atual</b>: orçamento remanescente do PM no mês (base + carry − PnL MTD) — mesmo valor do Risk Budget Monitor.
        <b>VaR paramétrico</b>: valor da tabela Lote (<code>LOTE_FUND_STRESS_RPM</code> PARAMETRIC_VAR, TREE
        <code>Main_Macro_Ativos</code>, LEVEL=10) somado signed across os books do PM, magnitude final.
        Diversificado entre books do mesmo PM (hedges internos se cancelam) mas não diversificado entre PMs distintos.
        <b>VaR hist 21/63/252d</b>: <code>1.645 × σ</code> dos retornos da <b>posição atual</b> aplicada a
        21/63/252d de retornos históricos dos fatores (fonte: <code>PORTIFOLIO_DAILY_HISTORICAL_SIMULATION</code>, mesma
        engine da Distribuição 252d).
        <b>Worst day posição</b>: pior dia simulado na janela 252d (posição atual × piores fatores históricos).
        <b>Dias p/ esgotar</b>: Margem atual ÷ maior das 4 estimativas de VaR.
        <b>Cores por linha</b>: referência = Margem atual do PM (1 VaR = 1.645σ). <br>
        <span style="color:var(--text)">&lt; 1σ</span> ·
        <span style="color:var(--warn)">≥ 1σ</span> ·
        <span style="color:#fb923c">≥ VaR (Margem)</span> ·
        <span style="color:var(--down)">≥ 2σ</span>.
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
    WHERE "FUNDO" IN ('MACRO','QUANT','EVOLUTION','GLOBAL','ALBATROZ','GFA','IDKAIPCAY3','IDKAIPCAY10')
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
    "FRONTIER":  "Frontier A\u00e7\u00f5es FIC FI",
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
    WHERE "FUNDO" IN ('MACRO','QUANT','EVOLUTION','GLOBAL','ALBATROZ','GFA')
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


def fetch_idka_index_returns(index_name: str, date_str: str = DATA_STR) -> dict:
    """IDKA benchmark index cumulative returns D/MTD/YTD/12M in bps.
       index_name: 'IDKA_IPCA_3A' ou 'IDKA_IPCA_10A'. Source: public.ECO_INDEX.
    """
    q = f"""
    SELECT "DATE", "VALUE"
    FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = '{index_name}' AND "FIELD" = 'INDEX'
      AND "DATE" >= DATE '{date_str}' - INTERVAL '400 days'
      AND "DATE" <= DATE '{date_str}'
    ORDER BY "DATE"
    """
    df = read_sql(q)
    if df.empty:
        return {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.drop_duplicates("DATE").set_index("DATE").sort_index()
    target = pd.Timestamp(date_str)
    last = df[df.index <= target]
    if last.empty:
        return {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
    v_now = float(last["VALUE"].iloc[-1])

    def ret(anchor):
        prior = df[df.index <= anchor]
        if prior.empty:
            return 0.0
        v0 = float(prior["VALUE"].iloc[-1])
        return (v_now / v0 - 1.0) * 10000 if v0 else 0.0

    prev_day = last["VALUE"].iloc[-2] if len(last) >= 2 else v_now
    dia = (v_now / float(prev_day) - 1.0) * 10000 if prev_day else 0.0
    month_start = target.to_period("M").to_timestamp()
    mtd = ret(month_start - pd.Timedelta(days=1))
    year_start = pd.Timestamp(f"{target.year}-01-01")
    ytd = ret(year_start - pd.Timedelta(days=1))
    m12 = ret(target - pd.DateOffset(years=1))
    return {"dia": dia, "mtd": mtd, "ytd": ytd, "m12": m12}


def fetch_idka_albatroz_weight(idka_desk: str, date_str: str = DATA_STR) -> float:
    """IDKA's Albatroz proportion (w_alb) as a fraction of IDKA NAV.

    Per user: sum TOTAL positions attributed to IDKA in Albatroz, compare
    to Albatroz NAV. Pro-rata share_source attribution means:
      slice_ratio = |POSITION attributed to IDKA| / |total POSITION in Albatroz|
      w_alb       = slice_ratio × Albatroz_NAV / IDKA_NAV

    ABS used because futures vs bonds vs cash can have mixed signs.
    Snapshot — applied uniformly across DIA/MTD/YTD/12M in the decomposition.
    """
    q = f"""
    SELECT "TRADING_DESK_SHARE_SOURCE" AS source, SUM(ABS("POSITION")) AS pos
    FROM "LOTE45"."LOTE_PRODUCT_EXPO"
    WHERE "TRADING_DESK" = 'GALAPAGOS ALBATROZ FIRF LP'
      AND "VAL_DATE"     = DATE '{date_str}'
    GROUP BY "TRADING_DESK_SHARE_SOURCE"
    """
    df = read_sql(q)
    if df.empty:
        return 0.0
    df["pos"] = df["pos"].astype(float)
    slice_val = float(df[df["source"] == idka_desk]["pos"].sum())
    total     = float(df["pos"].sum())
    if total == 0 or slice_val == 0:
        return 0.0
    alb_nav   = _latest_nav("GALAPAGOS ALBATROZ FIRF LP", date_str) or 0.0
    idka_nav  = _latest_nav(idka_desk, date_str) or 0.0
    if idka_nav == 0:
        return 0.0
    idka_cota_value = (slice_val / total) * alb_nav
    return idka_cota_value / idka_nav


def fetch_ibov_returns(date_str: str = DATA_STR) -> dict:
    """IBOV cumulative returns over DIA/MTD/YTD/12M windows (bps).
       Compound (1+r_i) products from EQUITIES_PRICES.CLOSE. One date per row (INSTRUMENT='IBOV').
    """
    q = f"""
    SELECT "DATE", "CLOSE"
    FROM public."EQUITIES_PRICES"
    WHERE "INSTRUMENT" = 'IBOV'
      AND "DATE" >= DATE '{date_str}' - INTERVAL '400 days'
      AND "DATE" <= DATE '{date_str}'
    ORDER BY "DATE"
    """
    df = read_sql(q)
    if df.empty:
        return {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df.drop_duplicates(subset=["DATE"]).set_index("DATE").sort_index()
    target = pd.Timestamp(date_str)
    last = df[df.index <= target]
    if last.empty:
        return {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
    p_now = float(last["CLOSE"].iloc[-1])

    def ret(anchor_date):
        prior = df[df.index <= anchor_date]
        if prior.empty:
            return 0.0
        p0 = float(prior["CLOSE"].iloc[-1])
        return (p_now / p0 - 1.0) * 10000 if p0 else 0.0

    # DIA: vs. previous trading day
    prev_day = last["CLOSE"].iloc[-2] if len(last) >= 2 else p_now
    dia = (p_now / float(prev_day) - 1.0) * 10000 if prev_day else 0.0
    # MTD: vs. close on the last trading day before month-start
    month_start = target.to_period("M").to_timestamp()
    mtd = ret(month_start - pd.Timedelta(days=1))
    # YTD: vs. close on the last trading day before year-start
    year_start = pd.Timestamp(f"{target.year}-01-01")
    ytd = ret(year_start - pd.Timedelta(days=1))
    # 12M: vs. close 12 months ago
    m12 = ret(target - pd.DateOffset(years=1))
    return {"dia": dia, "mtd": mtd, "ytd": ytd, "m12": m12}


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


def _build_pa_bench_decomp_view(fund_short: str, df: pd.DataFrame, cdi: dict,
                                  idka_index_ret: dict, w_alb: float,
                                  albatroz_pa_sum: dict, ibov: dict = None) -> str:
    """3-line bench decomposition table for IDKA PA (as a 3rd view).

    Engine stores each fund's PA vs. its own benchmark:
      - IDKA PA rows are already α vs. IDKA_index
      - Albatroz PA rows are already α vs. CDI

    Decomposition (per window):
      Total     = sum(IDKA_PA_bps)                       (α vs. IDKA_index directly)
      Via Alb   = w_alb × Albatroz_α_vs_CDI              (Albatroz's own α, scaled)
      Swap leg  = w_alb × (CDI − IDKA_index)             (bench-cross adjustment)
      Direct α  = Total − Via Alb − Swap                 (residual)

    Rationale: the Albatroz slice earns Albatroz_return = CDI + Albatroz_α.
    IDKA evaluates everything vs. IDKA_index, so the Albatroz slice's
    contribution to IDKA α is w_alb × (Albatroz_return − IDKA_index)
    = Via Alb + Swap leg. Direct α is what the fund's own direct holdings
    added beyond that.
    """
    windows = ["dia", "mtd", "ytd", "m12"]

    # IDKA PA rows are already α vs. IDKA_index (confirmed per Diego 2026-04-19)
    sum_idka_pa = {w: 0.0 for w in windows}
    for w in windows:
        col = f"{w}_bps"
        sum_idka_pa[w] = float(df[col].sum()) if col in df.columns else 0.0

    def _val(d, key):
        return float(d.get(key, 0.0)) if d else 0.0

    total = {}    # α vs. IDKA_index = sum_pa directly
    via_alb = {}  # w_alb × Albatroz α vs. CDI
    swap = {}     # w_alb × (CDI − idka_idx)
    direct = {}   # residual
    for w in windows:
        cdi_w = _val(cdi, w)
        idx_w = _val(idka_index_ret, w)
        alb_w = _val(albatroz_pa_sum, w)
        total[w]   = sum_idka_pa[w]
        via_alb[w] = w_alb * alb_w
        swap[w]    = w_alb * (cdi_w - idx_w)
        direct[w]  = total[w] - via_alb[w] - swap[w]

    def _cell(bps, bold=False):
        pct = bps / 100.0  # bps → %
        if abs(pct) < 0.005:
            return '<td class="pa-val mono" style="color:var(--muted); text-align:right">—</td>'
        col = "var(--up)" if bps >= 0 else "var(--down)"
        weight = "font-weight:700;" if bold else ""
        return f'<td class="pa-val mono" style="color:{col}; text-align:right; {weight}">{pct:+.2f}%</td>'

    def _row(label, vals, bold=False, sub=""):
        weight = "font-weight:700" if bold else ""
        sub_html = f' <span style="color:var(--muted); font-size:10px">{sub}</span>' if sub else ''
        return (
            f'<tr class="pa-row" style="{weight}">'
            f'<td class="pa-name" style="{weight}">{label}{sub_html}</td>'
            + _cell(vals["dia"], bold)
            + _cell(vals["mtd"], bold)
            + _cell(vals["ytd"], bold)
            + _cell(vals["m12"], bold)
            + '</tr>'
        )

    # Direct positions subtotal = Direct α (replica) + Swap leg (bench cross effect
    # caused by the decision to allocate to Albatroz). Swap is a consequence of the
    # allocation choice, so it belongs in the "Direct positions" block.
    direct_subtotal = {w: direct[w] + swap[w] for w in windows}

    rows = ""
    # Direct positions block (header + 2 sub-rows + subtotal)
    rows += (
        '<tr class="pa-row pa-group-header">'
        '<td class="pa-name" style="font-weight:700; color:var(--accent-2); '
        'text-transform:uppercase; letter-spacing:.05em; font-size:11px; padding-top:10px">Direct Positions</td>'
        '<td></td><td></td><td></td><td></td></tr>'
    )
    rows += _row(
        "&nbsp;&nbsp;↳ Direct α",
        direct, sub="(réplica vs. IDKA index, parcela direta)",
    )
    rows += _row(
        "&nbsp;&nbsp;↳ Swap leg",
        swap, sub="(CDI − IDKA_index) × w_alb — ajuste de bench pela alocação em Albatroz",
    )
    rows += _row(
        "<b>Direct subtotal</b>",
        direct_subtotal, bold=True, sub="soma Direct α + Swap",
    )
    # Via Albatroz (standalone)
    rows += (
        '<tr class="pa-row pa-group-header">'
        '<td class="pa-name" style="font-weight:700; color:var(--accent-2); '
        'text-transform:uppercase; letter-spacing:.05em; font-size:11px; padding-top:10px">Via Albatroz</td>'
        '<td></td><td></td><td></td><td></td></tr>'
    )
    rows += _row(
        "&nbsp;&nbsp;↳ Albatroz α",
        via_alb, sub=f"(Albatroz α vs. CDI × w_alb {w_alb:.1%})",
    )
    # Grand total
    rows += _row("<b>Total vs. IDKA benchmark</b>", total, bold=True)

    # ── Referência — retorno absoluto do fundo e dos benchmarks principais
    # (contexto: igual ao bloco "RETORNO ABSOLUTO / CDI / IBOV / IDKA_IPCA_3A"
    # do xlsx oficial da Controle)
    idx_label = "IDKA_IPCA_3A" if fund_short == "IDKA_3Y" else "IDKA_IPCA_10A"
    retorno_abs = {w: total[w] + float((idka_index_ret or {}).get(w, 0.0)) for w in windows}
    rows += (
        '<tr class="pa-row pa-group-header">'
        '<td class="pa-name" style="font-weight:700; color:var(--muted); '
        'text-transform:uppercase; letter-spacing:.05em; font-size:10.5px; padding-top:14px">Referência</td>'
        '<td></td><td></td><td></td><td></td></tr>'
    )
    rows += _row("&nbsp;&nbsp;Retorno Absoluto fund", retorno_abs, sub="= Total α + IDKA_index")
    rows += _row(f"&nbsp;&nbsp;{idx_label}", idka_index_ret or {}, sub="bench")
    rows += _row("&nbsp;&nbsp;CDI", cdi or {}, sub="ref. juros")

    return f"""
    <div class="pa-view" data-pa-view="bench" style="display:none">
      <div style="padding:8px 4px; font-size:11px; color:var(--muted); line-height:1.5">
        Decomposição α vs. IDKA benchmark · w_alb snapshot atual ({w_alb:.1%}) aplicado a todos os windows ·
        Swap leg compensa o fato de Albatroz ser benchmarkeado a CDI dentro de IDKA (bench IDKA_index).
      </div>
      <table class="pa-table" data-no-sort="1" style="margin-top:6px">
        <thead><tr>
          <th style="text-align:left">Componente</th>
          <th style="text-align:right">DIA</th>
          <th style="text-align:right">MTD</th>
          <th style="text-align:right">YTD</th>
          <th style="text-align:right">12M</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""


def build_pa_section_hier(fund_short: str, df_pa: pd.DataFrame, cdi: dict,
                           idka_index_ret: dict = None, w_alb: float = None,
                           albatroz_pa_sum: dict = None, ibov: dict = None) -> str:
    """
    PA card with hierarchical views (Por Classe / Por Livro).
    For IDKA funds, adds a 3rd view "Por Bench" with bench decomposition
    (Direct α / Via Albatroz / Swap leg / Total vs. IDKA index).
    """
    pa_key = _FUND_PA_KEY.get(fund_short)
    if pa_key is None or df_pa is None or df_pa.empty:
        return ""
    df = df_pa[df_pa["FUNDO"] == pa_key].copy()
    if df.empty:
        return ""

    # For IDKAs, the "Por Bench" decomposition is the default active view —
    # it's the most useful lens for a benchmarked RF fund.
    is_idka = fund_short in ("IDKA_3Y", "IDKA_10Y")
    bench_enabled = (is_idka and idka_index_ret and w_alb is not None
                     and albatroz_pa_sum is not None)

    view_classe = _build_pa_view(
        fund_short, df, "classe",
        ["CLASSE", "PRODUCT"], "Classe / Produto",
        active=(not bench_enabled),  # classe is default only if no bench view
        cdi=cdi,
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

    # 3rd view: bench decomposition — IDKA default
    view_bench = ""
    bench_toggle_btn = ""
    classe_active_cls = "active" if not bench_enabled else ""
    if bench_enabled:
        view_bench = _build_pa_bench_decomp_view(
            fund_short, df, cdi, idka_index_ret, w_alb, albatroz_pa_sum, ibov=ibov
        )
        # Force-display the bench view as active (override the default hidden)
        view_bench = view_bench.replace(
            'data-pa-view="bench" style="display:none"',
            'data-pa-view="bench" style="display:block"',
        )
        bench_toggle_btn = (
            '<button class="pa-tgl active" data-pa-view="bench" '
            'onclick="selectPaView(this,\'bench\')">Por Bench</button>'
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
          <button class="pa-tgl {classe_active_cls}" data-pa-view="classe"
                  onclick="selectPaView(this,'classe')">Por Classe</button>
          <button class="pa-tgl" data-pa-view="livro"
                  onclick="selectPaView(this,'livro')">Por Livro</button>
          {bench_toggle_btn}
        </div>
      </div>
      {view_classe}
      {view_livro}
      {view_bench}
    </section>"""


def build_frontier_lo_section(df: pd.DataFrame, date_str: str,
                               df_sectors: pd.DataFrame = None,
                               pa_hier_html: str = "") -> str:
    """Long Only position/attribution card for Frontier Ações FIC FI.
       df_sectors (optional): ticker → sector mapping for the Por Setor view.
       pa_hier_html (optional): pre-built hierarchical PA section (GFA rows from df_pa)
                                 rendered as a sub-tab. If empty, the sub-tab is omitted.
    """
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
    # ER columns (D/MTD/YTD) are bench-toggleable via the IBOV/IBOD buttons.
    # Each ER <td> carries data-ibov and data-ibod and the header text is rewritten
    # by fpaBmk() (inline JS below). Default bench = IBOV.
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
        <th style="text-align:right" class="fpa-th-er-d">ER IBOV D</th>
        <th style="text-align:right" class="fpa-th-er-m">ER IBOV MTD</th>
        <th style="text-align:right" class="fpa-th-er-y">ER IBOV YTD</th>
      </tr>"""

    def _num(x):
        try:
            return float(x)
        except Exception:
            return None

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

        def er_cell(v_ibov, v_ibod, v_cdi):
            # Default shows IBOV; data-ibod / data-cdi carry alternates.
            def _attr(v):
                f = _num(v)
                return "" if f is None else f"{f:.10f}"
            v = _num(v_ibov)
            if v is None:
                txt, col = "—", "var(--muted)"
            else:
                sign = "+" if v > 0 else ""
                txt  = f"{sign}{v*100:.2f}%"
                col  = "var(--up)" if v >= 0 else "var(--down)"
            return (f'<td class="mono fpa-er-cell" style="text-align:right;color:{col}" '
                    f'data-ibov="{_attr(v_ibov)}" data-ibod="{_attr(v_ibod)}" '
                    f'data-cdi="{_attr(v_cdi)}">{txt}</td>')

        adtv_cell = cell(row.get("#ADTV"), is_pct=False, decimals=2) if not (is_subtotal or is_total) else '<td></td>'
        beta_cell = cell(row.get("BETA"), is_pct=False, decimals=2) if not (is_subtotal or is_total) else '<td></td>'
        ret_d     = cell(row.get("RETURN_DAY"))    if not (is_subtotal or is_total) else '<td></td>'
        ret_m     = cell(row.get("RETURN_MONTH"))  if not (is_subtotal or is_total) else '<td></td>'
        ret_y     = cell(row.get("RETURN_YEAR"))   if not (is_subtotal or is_total) else '<td></td>'

        er_d = er_cell(row.get("TOTAL_IBVSP_DAY"),
                       row.get("TOTAL_IBOD_Benchmark_DAY"),
                       row.get("TOTAL_CDI_DAY"))
        er_m = er_cell(row.get("TOTAL_IBVSP_MONTH"),
                       row.get("TOTAL_IBOD_Benchmark_MONTH"),
                       row.get("TOTAL_CDI_MONTH"))
        er_y = er_cell(row.get("TOTAL_IBVSP_YEAR"),
                       row.get("TOTAL_IBOD_Benchmark_YEAR"),
                       row.get("TOTAL_CDI_YEAR"))

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
          {er_d}{er_m}{er_y}
        </tr>"""

    # ── By Name view (grouped by BOOK — default) ──────────────────────────────
    tbody = ""
    books = [b for b in df["BOOK"].unique() if b and b not in ("", None)]
    for book in books:
        book_rows = df[(df["BOOK"] == book) & (~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"]))]
        book_rows = book_rows.sort_values("% Cash", ascending=False, na_position="last")
        tbody += f'<tr><td colspan="14" style="padding:6px 4px 2px;font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;border-top:1px solid var(--border)">{book}</td></tr>'
        for _, r in book_rows.iterrows():
            tbody += make_row(r)
        sub = df[(df["PRODUCT"] == "SUBTOTAL") & (df["BOOK"] == book)]
        if not sub.empty:
            tbody += make_row(sub.iloc[0], is_subtotal=True)
    # Alienadas — aggregated P&L from stocks sold during the period (not in current portfolio)
    alien_row = df[df["PRODUCT"] == "AÇÕES ALIENADAS"]
    if not alien_row.empty:
        tbody += make_row(alien_row.iloc[0], is_subtotal=True)
    tbody += make_row(tot, is_total=True)

    by_name_html = f"""
      <div id="fpa-view-name" class="fpa-view">
        <div style="overflow-x:auto">
          <table class="metric-table" style="font-size:11px;min-width:900px">
            <thead>{col_headers}</thead>
            <tbody>{tbody}</tbody>
          </table>
        </div>
      </div>"""

    # ── By Sector view — sector headers with ▼/▶ toggle + aggregated ER ───────
    sector_html = ""
    if df_sectors is not None and not df_sectors.empty:
        sec_map = df_sectors.drop_duplicates("TICKER").set_index("TICKER")
        _stocks = df[(df["BOOK"].astype(str).str.strip() != "")
                     & (~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"]))
                     & (df["% Cash"].notna())].copy()
        _stocks["sector"] = _stocks["PRODUCT"].map(sec_map["GLPG_SECTOR"]).fillna("Outros")

        # Numeric coerce for aggregation
        _agg_cols = ["% Cash", "DELTA",
                     "TOTAL_ATRIBUTION_DAY","TOTAL_ATRIBUTION_MONTH","TOTAL_ATRIBUTION_YEAR",
                     "TOTAL_IBVSP_DAY","TOTAL_IBVSP_MONTH","TOTAL_IBVSP_YEAR",
                     "TOTAL_IBOD_Benchmark_DAY","TOTAL_IBOD_Benchmark_MONTH","TOTAL_IBOD_Benchmark_YEAR",
                     "TOTAL_CDI_DAY","TOTAL_CDI_MONTH","TOTAL_CDI_YEAR"]
        for c in _agg_cols:
            if c in _stocks.columns:
                _stocks[c] = pd.to_numeric(_stocks[c], errors="coerce")

        # Order sectors by gross % Cash desc
        sec_order = (_stocks.groupby("sector")["% Cash"].sum()
                     .sort_values(ascending=False).index.tolist())

        sec_tbody = ""
        sec_idx = 0
        for sec in sec_order:
            grp = _stocks[_stocks["sector"] == sec].sort_values("% Cash", ascending=False)
            sec_id = f"fpa-sec-{sec_idx}"; sec_idx += 1

            def _s(col): return float(grp[col].sum()) if col in grp.columns else 0.0
            def _attr(v): return "" if v is None else f"{v:.10f}"

            # Sector header cells — mirror the column layout: 11 "left" cols + 3 ER cells
            pos_s   = _s("% Cash")
            delta_s = _s("DELTA")
            at_d = _s("TOTAL_ATRIBUTION_DAY");  at_m = _s("TOTAL_ATRIBUTION_MONTH");  at_y = _s("TOTAL_ATRIBUTION_YEAR")
            iv_d = _s("TOTAL_IBVSP_DAY");        iv_m = _s("TOTAL_IBVSP_MONTH");        iv_y = _s("TOTAL_IBVSP_YEAR")
            id_d = _s("TOTAL_IBOD_Benchmark_DAY"); id_m = _s("TOTAL_IBOD_Benchmark_MONTH"); id_y = _s("TOTAL_IBOD_Benchmark_YEAR")
            cd_d = _s("TOTAL_CDI_DAY");          cd_m = _s("TOTAL_CDI_MONTH");          cd_y = _s("TOTAL_CDI_YEAR")
            _b = grp.dropna(subset=["BETA"])
            wbeta = ((_b["% Cash"] * _b["BETA"]).sum() / _b["% Cash"].sum()
                     if not _b.empty and _b["% Cash"].sum() > 0 else None)

            def _pct(v):
                f = float(v); s = "+" if f > 0 else ""
                return f"{s}{f*100:.2f}%"
            def _col(v):
                return "var(--up)" if float(v) >= 0 else "var(--down)"

            def _sec_er_td(vi, vo, vc):
                sign = "+" if vi >= 0 else ""
                return (f'<td class="mono fpa-er-cell" style="text-align:right;color:{_col(vi)}" '
                        f'data-ibov="{_attr(vi)}" data-ibod="{_attr(vo)}" data-cdi="{_attr(vc)}">'
                        f'{sign}{vi*100:.2f}%</td>')

            beta_cell_sec = (f'<td class="mono" style="text-align:right">{wbeta:.2f}</td>'
                             if wbeta is not None else '<td></td>')
            sec_tbody += (
                f'<tr class="fpa-sector-row" data-sec-id="{sec_id}" '
                f'style="background:rgba(59,130,246,0.08);font-weight:700;cursor:pointer" '
                f'onclick="fpaToggleSector(this)">'
                f'<td style="padding:5px 8px;white-space:nowrap">'
                f'<span class="fpa-sector-arrow" style="font-size:9px;margin-right:6px">▼</span>{sec}'
                f' <span style="color:var(--muted);font-weight:400;font-size:9px">({len(grp)})</span></td>'
                f'<td class="mono" style="text-align:right">{_pct(pos_s)}</td>'
                f'<td class="mono" style="text-align:right">{_pct(delta_s)}</td>'
                f'{beta_cell_sec}'
                f'<td></td>'  # #ADTV
                f'<td></td><td></td><td></td>'  # Ret D/M/Y
                f'<td class="mono" style="text-align:right;color:{_col(at_d)}">{_pct(at_d)}</td>'
                f'<td class="mono" style="text-align:right;color:{_col(at_m)}">{_pct(at_m)}</td>'
                f'<td class="mono" style="text-align:right;color:{_col(at_y)}">{_pct(at_y)}</td>'
                f'{_sec_er_td(iv_d, id_d, cd_d)}'
                f'{_sec_er_td(iv_m, id_m, cd_m)}'
                f'{_sec_er_td(iv_y, id_y, cd_y)}'
                f'</tr>'
            )
            # Child rows under this sector
            for _, r in grp.iterrows():
                child_tr = make_row(r)  # starts with <tr ...>
                # Inject marker classes/attrs to identify as sector child
                child_tr = child_tr.replace(
                    '<tr style=',
                    f'<tr class="fpa-sector-child" data-sec-parent="{sec_id}" style=',
                    1)
                sec_tbody += child_tr

        # Alienadas — shown as a summary row above the grand total
        _alien = df[df["PRODUCT"] == "AÇÕES ALIENADAS"]
        if not _alien.empty:
            sec_tbody += make_row(_alien.iloc[0], is_subtotal=True)
        # Grand total row at the bottom
        sec_tbody += make_row(tot, is_total=True)

        sector_html = f"""
      <div id="fpa-view-sector" class="fpa-view" style="display:none">
        <div style="overflow-x:auto">
          <table class="metric-table" data-no-sort="1" style="font-size:11px;min-width:900px">
            <thead>{col_headers}</thead>
            <tbody>{sec_tbody}</tbody>
          </table>
        </div>
      </div>"""

    table_html = by_name_html + sector_html

    # ── Highlights strip: top-3 ↑ / top-3 ↓ — one div per bench, toggled by JS ──
    def _highlights_div(col_name: str, bench_label: str, visible: bool) -> str:
        try:
            _hs = df[(df["BOOK"].astype(str).str.strip() != "")
                     & (~df["PRODUCT"].isin(["TOTAL","SUBTOTAL","AÇÕES ALIENADAS"]))
                     & (df[col_name].notna())]
            _by = _hs.set_index("PRODUCT")[col_name].astype(float)
            _by = _by[_by.abs() * 10_000.0 > 0.5]
            _up = _by[_by > 0].sort_values(ascending=False).head(3)
            _dn = _by[_by < 0].sort_values().head(3)
            def _chip(t, v, up):
                col = "var(--up)" if up else "var(--down)"
                sign = "+" if up else ""
                return (f'<span class="mono" style="color:{col};font-weight:700;'
                        f'padding:2px 8px;border:1px solid {col};border-radius:4px;'
                        f'background:rgba(0,0,0,.15)">{t} {sign}{v*100:.2f}%</span>')
            if _up.empty and _dn.empty:
                return ""
            ups = " ".join(_chip(t, v, True)  for t, v in _up.items()) or '<span style="color:var(--muted)">—</span>'
            dns = " ".join(_chip(t, v, False) for t, v in _dn.items()) or '<span style="color:var(--muted)">—</span>'
            disp = "" if visible else "display:none"
            return (
                f'<div class="fpa-highlights" data-bench="{bench_label.lower()}" '
                f'style="align-items:center;gap:12px;flex-wrap:wrap;padding:8px 12px;'
                f'margin:8px 0 12px;border:1px solid var(--line);border-radius:6px;'
                f'background:rgba(0,0,0,.15);display:{"flex" if visible else "none"}">'
                '<span style="font-size:10px;color:var(--muted);letter-spacing:.12em;'
                f'text-transform:uppercase">Highlights · α vs {bench_label} hoje</span>'
                f'<span style="color:var(--up);font-weight:700">↑</span> {ups}'
                '<span style="color:var(--line)">·</span>'
                f'<span style="color:var(--down);font-weight:700">↓</span> {dns}'
                '</div>'
            )
        except Exception:
            return ""

    highlights_html = (
        _highlights_div("TOTAL_IBVSP_DAY",          "IBOV", True)
      + _highlights_div("TOTAL_IBOD_Benchmark_DAY", "IBOD", False)
      + _highlights_div("TOTAL_CDI_DAY",            "CDI",  False)
    )

    # ── Toggle bar (bench IBOV/IBOD/CDI · view Por Nome/Por Setor) + JS ────────
    has_sector = (df_sectors is not None and not df_sectors.empty)
    view_buttons = ("" if not has_sector else """
      <div style="display:flex;gap:4px;margin-left:auto;align-items:center">
        <button class="toggle-btn active fpa-view-btn" data-view="name"
                onclick="fpaView('name')">Por Nome</button>
        <button class="toggle-btn fpa-view-btn" data-view="sector"
                onclick="fpaView('sector')">Por Setor</button>
        <span id="fpa-expand-btns" style="display:none;gap:4px;margin-left:4px">
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="fpaExpandAll()">▼ All</button>
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="fpaCollapseAll()">▶ All</button>
        </span>
      </div>""")
    toggle_bar = f"""
    <div style="display:flex;align-items:center;gap:8px;margin:0 0 10px;flex-wrap:wrap">
      <div style="display:flex;gap:4px">
        <button class="toggle-btn active fpa-btn" data-bench="ibov"
                onclick="fpaBmk('ibov')">IBOV</button>
        <button class="toggle-btn fpa-btn" data-bench="ibod"
                onclick="fpaBmk('ibod')">IBOD</button>
        <button class="toggle-btn fpa-btn" data-bench="cdi"
                onclick="fpaBmk('cdi')">CDI</button>
      </div>
      {view_buttons}
    </div>"""

    toggle_js = """
    <script>
    (function() {
      if (window.fpaBmk) return;  // only define once
      window.fpaBmk = function(bmk) {
        document.querySelectorAll('.fpa-btn').forEach(function(b) {
          b.classList.toggle('active', b.dataset.bench === bmk);
        });
        document.querySelectorAll('.fpa-highlights').forEach(function(d) {
          d.style.display = (d.dataset.bench === bmk) ? 'flex' : 'none';
        });
        var lbl = bmk.toUpperCase();
        document.querySelectorAll('.fpa-th-er-d').forEach(function(th) { th.textContent = 'ER '+lbl+' D'; });
        document.querySelectorAll('.fpa-th-er-m').forEach(function(th) { th.textContent = 'ER '+lbl+' MTD'; });
        document.querySelectorAll('.fpa-th-er-y').forEach(function(th) { th.textContent = 'ER '+lbl+' YTD'; });
        document.querySelectorAll('.fpa-er-cell').forEach(function(td) {
          var raw = td.dataset[bmk];
          if (raw === '' || raw === undefined) { td.textContent = '—'; td.style.color = 'var(--muted)'; return; }
          var v = parseFloat(raw);
          if (isNaN(v))                        { td.textContent = '—'; td.style.color = 'var(--muted)'; return; }
          td.textContent = (v >= 0 ? '+' : '') + (v*100).toFixed(2) + '%';
          td.style.color = v >= 0 ? 'var(--up)' : 'var(--down)';
        });
      };
      window.fpaView = function(view) {
        ['name','sector'].forEach(function(v) {
          var el = document.getElementById('fpa-view-'+v);
          if (el) el.style.display = (v === view) ? '' : 'none';
        });
        document.querySelectorAll('.fpa-view-btn').forEach(function(b) {
          b.classList.toggle('active', b.dataset.view === view);
        });
        var exp = document.getElementById('fpa-expand-btns');
        if (exp) exp.style.display = (view === 'sector') ? 'inline-flex' : 'none';
      };
      window.fpaToggleSector = function(tr) {
        var arrow = tr.querySelector('.fpa-sector-arrow');
        var open = arrow ? arrow.textContent.trim() === '▼' : true;
        if (arrow) arrow.textContent = open ? '▶' : '▼';
        var sid = tr.getAttribute('data-sec-id');
        if (!sid) return;
        document.querySelectorAll('.fpa-sector-child[data-sec-parent="'+sid+'"]').forEach(function(r) {
          r.style.display = open ? 'none' : '';
        });
      };
      window.fpaExpandAll = function() {
        document.querySelectorAll('.fpa-sector-row').forEach(function(tr) {
          var arrow = tr.querySelector('.fpa-sector-arrow');
          if (arrow && arrow.textContent.trim() === '▶') window.fpaToggleSector(tr);
        });
      };
      window.fpaCollapseAll = function() {
        document.querySelectorAll('.fpa-sector-row').forEach(function(tr) {
          var arrow = tr.querySelector('.fpa-sector-arrow');
          if (arrow && arrow.textContent.trim() === '▼') window.fpaToggleSector(tr);
        });
      };
      // Sub-tab switcher (Long Only / PA hierárquica)
      window.fpaTab = function(tab) {
        ['lo','pa'].forEach(function(t) {
          var el = document.getElementById('fpa-tab-'+t);
          if (el) el.style.display = (t === tab) ? '' : 'none';
        });
        document.querySelectorAll('.fpa-tab-btn').forEach(function(b) {
          b.classList.toggle('active', b.dataset.tab === tab);
        });
      };
    })();
    </script>"""

    # ── Sub-tabs (Long Only default · PA hierárquica if df_pa has GFA) ─────────
    if pa_hier_html:
        # Strip outer <section class="pa-card">…</section> so it nests cleanly inside the tab.
        # Simplest: render as-is; inner card becomes visually flush via CSS overrides.
        sub_tab_bar = """
    <div style="display:flex;gap:4px;margin:0 0 12px;border-bottom:1px solid var(--line);padding-bottom:6px">
      <button class="toggle-btn active fpa-tab-btn" data-tab="lo"
              onclick="fpaTab('lo')" style="font-weight:700">Long Only</button>
      <button class="toggle-btn fpa-tab-btn" data-tab="pa"
              onclick="fpaTab('pa')" style="font-weight:700">PA (hierárquica)</button>
    </div>"""
        lo_content = f"""<div id="fpa-tab-lo">{toggle_bar}{highlights_html}{table_html}</div>"""
        pa_content = (
            '<div id="fpa-tab-pa" style="display:none" class="fpa-pa-nested">'
            f'{pa_hier_html}'
            '</div>'
        )
        body = f"{sub_tab_bar}{lo_content}{pa_content}"
    else:
        body = f"{toggle_bar}{highlights_html}{table_html}"

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Performance Attribution</span>
        <span class="card-sub">— Frontier Ações · {val_date}{stale_badge} · NAV R$ {nav_fmt} · Beta {beta_fmt}</span>
      </div>
      {header_metrics}
      {body}
      {toggle_js}
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
    # Chain filters once; copy once at the end via sort_values/reset_index.
    _mask = (df_lo["BOOK"].astype(str).str.strip() != "") & \
            (~df_lo["PRODUCT"].isin(EXCLUDE)) & \
            (df_lo["% Cash"].notna())
    stocks = df_lo[_mask].sort_values("% Cash", ascending=False).reset_index(drop=True)

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

    # Ensure ER columns exist and are numeric (fill NaN with 0 for safe aggregation)
    for _col_name in ["TOTAL_IBVSP_DAY", "TOTAL_IBVSP_MONTH", "TOTAL_IBVSP_YEAR",
                      "TOTAL_IBOD_Benchmark_DAY", "TOTAL_IBOD_Benchmark_MONTH", "TOTAL_IBOD_Benchmark_YEAR"]:
        if _col_name not in stocks.columns:
            stocks[_col_name] = 0.0
        else:
            stocks[_col_name] = pd.to_numeric(stocks[_col_name], errors="coerce").fillna(0.0)

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
            pos_tot    = row["pos_tot"]
            ibov_tot   = row["ibov_tot"]
            ibod_tot   = row["ibod_tot"]
            ibov_a_tot = row["ibov_act_tot"]
            ibod_a_tot = row["ibod_act_tot"]
            er_ibov_d  = row.get("er_ibov_d_tot", 0.0) or 0.0
            er_ibov_m  = row.get("er_ibov_m_tot", 0.0) or 0.0
            er_ibov_y  = row.get("er_ibov_y_tot", 0.0) or 0.0
            er_ibod_d  = row.get("er_ibod_d_tot", 0.0) or 0.0
            er_ibod_m  = row.get("er_ibod_m_tot", 0.0) or 0.0
            er_ibod_y  = row.get("er_ibod_y_tot", 0.0) or 0.0
            wbeta      = row.get("w_beta")

            def _sec_er(vi, vo):
                col = _col(vo)
                s = "+" if vo >= 0 else ""
                return (f'<td class="mono {uid}-er-cell" style="text-align:right;color:{col}"'
                        f' data-ibov="{vi:.6f}" data-ibod="{vo:.6f}">{s}{vo*100:.2f}%</td>')

            beta_td_s = (f'<td class="mono" style="text-align:right">{wbeta:.2f}</td>'
                         if wbeta is not None else '<td></td>')
            return (f'<tr class="{uid}-sector-row" style="background:rgba(59,130,246,0.07);'
                    f'font-weight:700;cursor:pointer" onclick="loExpoToggleSector(this)">'
                    f'<td style="padding:5px 4px;white-space:nowrap">'
                    f'<span class="{uid}-sector-arrow" style="font-size:9px;margin-right:4px">▼</span>{s}</td>'
                    f'<td class="mono" style="text-align:right">{_pf(pos_tot)}</td>'
                    f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                    f'  data-ibov="{ibov_tot:.6f}" data-ibod="{ibod_tot:.6f}">{_pf(ibov_tot)}</td>'
                    f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(ibov_a_tot)}"'
                    f'  data-ibov="{ibov_a_tot:.6f}" data-ibod="{ibod_a_tot:.6f}">{_pf(ibov_a_tot, sign=True)}</td>'
                    f'{beta_td_s}'
                    f'{_sec_er(er_ibov_d, er_ibod_d)}'
                    f'{_sec_er(er_ibov_m, er_ibod_m)}'
                    f'{_sec_er(er_ibov_y, er_ibod_y)}'
                    f'</tr>')

        tk     = row["PRODUCT"]
        pos    = row["% Cash"]
        ibov_b = row["ibov_w"]
        ibod_b = row["ibod_w"]
        ibov_a = row["ibov_act"]
        ibod_a = row["ibod_act"]
        beta   = row.get("BETA")
        # IBOD ER (primary) and IBOV ER (for toggle)
        er_ibod_d_s = row.get("TOTAL_IBOD_Benchmark_DAY")
        er_ibod_m_s = row.get("TOTAL_IBOD_Benchmark_MONTH")
        er_ibod_y_s = row.get("TOTAL_IBOD_Benchmark_YEAR")
        er_ibov_d_s = row.get("TOTAL_IBVSP_DAY")
        er_ibov_m_s = row.get("TOTAL_IBVSP_MONTH")
        er_ibov_y_s = row.get("TOTAL_IBVSP_YEAR")
        pad  = "padding-left:20px" if indent else "padding-left:4px"
        beta_td = (f'<td class="mono" style="text-align:right">{float(beta):.2f}</td>'
                   if pd.notna(beta) else '<td class="mono" style="text-align:right;color:var(--muted)">—</td>')

        def _ertd(vi, vo):
            try:
                fi = float(vo)  # default: ibod
                fv = float(vi)
                s = "+" if fi >= 0 else ""
                return (f'<td class="mono {uid}-er-cell" style="text-align:right;color:{_col(fi)}"'
                        f' data-ibov="{fv:.6f}" data-ibod="{fi:.6f}">'
                        f'{s}{fi*100:.2f}%</td>')
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
                f'{_ertd(er_ibov_d_s, er_ibod_d_s)}'
                f'{_ertd(er_ibov_m_s, er_ibod_m_s)}'
                f'{_ertd(er_ibov_y_s, er_ibod_y_s)}'
                f'</tr>')

    # ── By Name table ──────────────────────────────────────────────────────────
    name_rows = "".join(_tr(r) for _, r in stocks.iterrows())
    # Totals row
    tot_ibov_act = stocks["ibov_act"].sum()
    tot_ibod_act = stocks["ibod_act"].sum()
    tot_ibov_bmk = stocks["ibov_w"].sum()
    tot_ibod_bmk = stocks["ibod_w"].sum()
    tot_er_ibov_d = stocks["TOTAL_IBVSP_DAY"].sum()
    tot_er_ibov_m = stocks["TOTAL_IBVSP_MONTH"].sum()
    tot_er_ibov_y = stocks["TOTAL_IBVSP_YEAR"].sum()
    tot_er_ibod_d = stocks["TOTAL_IBOD_Benchmark_DAY"].sum()
    tot_er_ibod_m = stocks["TOTAL_IBOD_Benchmark_MONTH"].sum()
    tot_er_ibod_y = stocks["TOTAL_IBOD_Benchmark_YEAR"].sum()

    def _tot_er(vi, vo):
        s = "+" if vo >= 0 else ""
        return (f'<td class="mono {uid}-er-cell" style="text-align:right;font-weight:700;color:{_col(vo)}"'
                f' data-ibov="{vi:.6f}" data-ibod="{vo:.6f}">{s}{vo*100:.2f}%</td>')

    name_rows += (f'<tr data-pinned="1" style="font-weight:700;border-top:2px solid var(--border)">'
                  f'<td>TOTAL</td>'
                  f'<td class="mono" style="text-align:right">{_pf(gross)}</td>'
                  f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                  f'  data-ibov="{tot_ibov_bmk:.6f}" data-ibod="{tot_ibod_bmk:.6f}">{_pf(tot_ibov_bmk)}</td>'
                  f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(tot_ibov_act)}"'
                  f'  data-ibov="{tot_ibov_act:.6f}" data-ibod="{tot_ibod_act:.6f}">{_pf(tot_ibov_act, sign=True)}</td>'
                  f'<td></td>'
                  f'{_tot_er(tot_er_ibov_d, tot_er_ibod_d)}'
                  f'{_tot_er(tot_er_ibov_m, tot_er_ibod_m)}'
                  f'{_tot_er(tot_er_ibov_y, tot_er_ibod_y)}'
                  f'</tr>')
    name_rows += (f'<tr data-pinned="1" style="color:var(--muted)">'
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
        sec_pos    = grp["% Cash"].sum()
        sec_ibov   = grp["ibov_w"].sum()
        sec_ibod   = grp["ibod_w"].sum()
        sec_ibov_a = grp["ibov_act"].sum()
        sec_ibod_a = grp["ibod_act"].sum()
        sec_er_ibov_d = grp["TOTAL_IBVSP_DAY"].sum()
        sec_er_ibov_m = grp["TOTAL_IBVSP_MONTH"].sum()
        sec_er_ibov_y = grp["TOTAL_IBVSP_YEAR"].sum()
        sec_er_ibod_d = grp["TOTAL_IBOD_Benchmark_DAY"].sum()
        sec_er_ibod_m = grp["TOTAL_IBOD_Benchmark_MONTH"].sum()
        sec_er_ibod_y = grp["TOTAL_IBOD_Benchmark_YEAR"].sum()
        # Weighted beta: Σ(pos_i × beta_i) / Σ(pos_i)
        _b = grp.dropna(subset=["BETA"])
        sec_wbeta = ((_b["% Cash"] * _b["BETA"]).sum() / _b["% Cash"].sum()
                     if not _b.empty and _b["% Cash"].sum() > 0 else None)
        macro_c = grp.iloc[0]["macro"] if not grp.empty else "—"
        sec_data = {
            "sector":       f"{sec} <span style='font-size:9px;color:var(--muted);font-weight:400'>({macro_c})</span>",
            "pos_tot":      sec_pos,
            "ibov_tot":     sec_ibov,   "ibod_tot":     sec_ibod,
            "ibov_act_tot": sec_ibov_a, "ibod_act_tot": sec_ibod_a,
            "er_ibov_d_tot": sec_er_ibov_d, "er_ibod_d_tot": sec_er_ibod_d,
            "er_ibov_m_tot": sec_er_ibov_m, "er_ibod_m_tot": sec_er_ibod_m,
            "er_ibov_y_tot": sec_er_ibov_y, "er_ibod_y_tot": sec_er_ibod_y,
            "w_beta":       sec_wbeta,
        }
        sector_rows += _tr(sec_data, is_sector=True)
        for _, r in grp.iterrows():
            sector_rows += (f'<tr class="{uid}-child-row">' +
                            _tr(r, indent=True)[4:])  # strip leading <tr>

    by_sector_html = f"""
    <div id="{uid}-view-sector" class="{uid}-view" style="display:none">
      <div style="overflow-x:auto">
        <table class="metric-table" data-no-sort="1" style="font-size:11px;min-width:620px">
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
    document.querySelectorAll('.'+uid+'-er-cell').forEach(function(td) {{
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


# ── Evolution — Diversification Benefit (3 camadas da skill) ────────────────
# Compute functions imported from evolution_diversification_card. Here we render
# with the main report's dark theme (CSS vars --text, --muted, --up, etc.).
# CREDITO VaR is winsorized (63d MAD, 3σ, causal) to absorb the dez/2025
# cotas-júnior spike — see docs/CREDITO_TREATMENT.md.

_EVO_EXPECTED_STRATS = ["MACRO", "SIST", "FRONTIER", "CREDITO", "EVO_STRAT", "CAIXA"]


def _evo_spark_svg(series: "pd.Series", today_val: float,
                   width: int = 560, height: int = 90) -> str:
    if series is None or series.empty:
        return ""
    ys = series.values.astype(float)
    y_min, y_max = float(np.nanmin(ys)), float(np.nanmax(ys))
    if y_max - y_min < 1e-9:
        y_max = y_min + 0.01
    pad = 10
    w, h = width - 2 * pad, height - 2 * pad
    def xy(i, v):
        x = pad + (i / (len(ys) - 1 or 1)) * w
        y = pad + (1 - (v - y_min) / (y_max - y_min)) * h
        return (x, y)
    pts = " ".join(f"{x:.1f},{y:.1f}"
                   for i, v in enumerate(ys) for (x, y) in [xy(i, v)])
    dx, dy = xy(len(ys) - 1, today_val)
    mean_val = float(np.nanmean(ys))
    my = pad + (1 - (mean_val - y_min) / (y_max - y_min)) * h
    return f"""
    <svg viewBox="0 0 {width} {height}" style="width:100%;max-width:{width}px;height:{height}px">
      <line x1="{pad}" y1="{my:.1f}" x2="{width - pad}" y2="{my:.1f}"
            stroke="var(--line-2)" stroke-dasharray="3 3" stroke-width="1"/>
      <polyline points="{pts}" fill="none" stroke="var(--accent-2)" stroke-width="1.6"/>
      <circle cx="{dx:.1f}" cy="{dy:.1f}" r="4"
              fill="var(--accent-2)" stroke="var(--bg)" stroke-width="2"/>
      <text x="{width - pad}" y="{my - 4:.1f}" text-anchor="end"
            font-size="10" fill="var(--muted)">média 252d: {mean_val:.2f}</text>
    </svg>"""


def _evo_pct_color(pct: float) -> str:
    """Cor para percentil 'high=bad' usando CSS vars do tema."""
    if pd.isna(pct):        return "var(--muted)"
    if pct < 70:            return "var(--up)"
    if pct < 85:            return "var(--warn)"
    if pct < 95:            return "var(--down)"
    return "var(--down)"


def _evo_pct_badge(pct: float) -> str:
    if pd.isna(pct):   return "—"
    if pct < 70:       return "🟢"
    if pct < 85:       return "🟡"
    if pct < 95:       return "🔴"
    return "⚫"


def _evo_render_camada1(rows: list) -> str:
    # Excluir CAIXA, OUTROS e CREDITO (CAIXA e OUTROS são ruidosos; CREDITO tem
    # caveat de cotas júnior — análise direcional se concentra em MACRO/SIST/
    # FRONTIER/EVO_STRAT).
    _EXCLUDED_C1 = {"CAIXA", "OUTROS", "CREDITO"}
    rows = [r for r in rows if r["strat"] not in _EXCLUDED_C1]
    elevated = [r for r in rows if not pd.isna(r["pct"]) and r["pct"] >= 70]
    tr_html = ""
    for r in rows:
        c     = _evo_pct_color(r["pct"])
        vstr  = "—" if pd.isna(r["var_today"]) else f"{r['var_today']:,.1f}"
        pstr  = "—" if pd.isna(r["pct"])       else f"P{r['pct']:.0f}"
        badge = _evo_pct_badge(r["pct"])
        tr_html += (
            f"<tr>"
            f"<td>{r['strat']}</td>"
            f"<td class='mono' style='text-align:right'>{vstr}</td>"
            f"<td class='mono' style='text-align:right;color:{c};"
            f"font-weight:600'>{pstr}</td>"
            f"<td style='text-align:center'>{badge}</td>"
            f"</tr>"
        )
    alert = ""
    if len(elevated) >= 3:
        names = ", ".join(r["strat"] for r in elevated)
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(245,196,81,0.12);border-left:3px solid var(--warn);"
                 f"border-radius:4px;font-size:13px;color:var(--text)'>"
                 f"⚠️ {len(elevated)} estratégias simultaneamente ≥ P70 "
                 f"({names}) — sinal de carregamento agregado</div>")
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Camada 1 — Utilização histórica por estratégia</span>
        <span class="card-sub">VaR em bps · percentil 252d próprio · sinal: quantas simultaneamente ≥ P70</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Estratégia</th>
          <th style="text-align:right"><span class="kc">VaR</span> (bps)</th>
          <th style="text-align:right">Percentil 252d</th>
          <th style="text-align:center">Estado</th>
        </tr></thead>
        <tbody>{tr_html}</tbody>
      </table>
      {alert}
      <div class="bar-legend" style="margin-top:10px">
        cada estratégia vs. próprio histórico 252d ·
        3+ simultâneas em P≥70 = bull market alignment ·
        CAIXA / OUTROS / CREDITO excluídos
      </div>
    </section>"""


def _evo_render_camada2(d: dict) -> str:
    state_color = _evo_pct_color(d["ratio_wins_pct"])
    state_label = ("diversificação saudável" if d["ratio_wins_pct"] < 60 else
                   "abaixo da média histórica" if d["ratio_wins_pct"] < 80 else
                   "estratégias alinhadas" if d["ratio_wins_pct"] < 95 else
                   "sem diversificação efetiva")

    # Strategy breakdown rows — CREDITO omitido da tabela (caveat cotas júnior).
    # A Σ abaixo continua exibindo raw e winsorizada (que inclui CREDITO clipado).
    rows = []
    for s in _EVO_EXPECTED_STRATS:
        if s == "CREDITO":
            continue
        v = d["strat_today_bps"].get(s)
        rows.append((s, float(v) if v is not None else None))
    if "OUTROS" in d["strat_today_bps"]:
        rows.append(("OUTROS", float(d["strat_today_bps"]["OUTROS"])))

    strat_rows_html = ""
    for s, v in rows:
        vstr  = "—" if v is None else f"{v:,.1f}"
        share = "—" if (v is None or d["var_soma_bps"] == 0) \
                     else f"{v/d['var_soma_bps']*100:.0f}%"
        strat_rows_html += (
            f"<tr><td>{s}</td>"
            f"<td class='mono' style='text-align:right'>{vstr}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>{share}</td></tr>"
        )
    strat_rows_html += (
        "<tr style='border-top:1.5px solid var(--line-2);font-weight:600'>"
        f"<td>Σ VaR estratégias <span style='color:var(--muted);font-weight:400'>(raw)</span></td>"
        f"<td class='mono' style='text-align:right'>{d['var_soma_bps']:,.1f}</td>"
        f"<td class='mono' style='text-align:right;color:var(--muted)'>100%</td></tr>"
    )
    if abs(d['var_soma_bps'] - d['var_soma_wins_bps']) > 0.01:
        strat_rows_html += (
            "<tr style='font-weight:700'>"
            f"<td>Σ VaR estratégias <span style='color:var(--up);font-weight:400'>(winsorizado)</span></td>"
            f"<td class='mono' style='text-align:right;color:var(--up)'>{d['var_soma_wins_bps']:,.1f}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>—</td></tr>"
        )

    saving_pct = (1.0 - d["ratio_wins"]) * 100
    cr_share    = d["credito_share_raw"]  * 100 if not pd.isna(d["credito_share_raw"])  else float("nan")
    cr_share_w  = d["credito_share_wins"] * 100 if not pd.isna(d["credito_share_wins"]) else float("nan")
    cr_color = ("var(--down)" if cr_share > 40
                else "var(--warn)" if cr_share > 25
                else "var(--muted)")

    n_wins = len(d["credito_wins_dates"])
    wins_note = ""
    if n_wins > 0:
        recent_wins = [pd.Timestamp(x).strftime("%Y-%m-%d")
                       for x in d["credito_wins_dates"][-3:]]
        wins_note = (
            f"<div style='margin-top:10px;padding:8px 12px;"
            f"background:rgba(245,196,81,0.12);border-left:3px solid var(--warn);"
            f"border-radius:4px;font-size:12px;color:var(--text);line-height:1.5'>"
            f"⚠️ CREDITO clipado em <b>{n_wins}</b> dias na janela 252d "
            f"(spike de cotas júnior). Últimos: {', '.join(recent_wins)}. "
            f"Ratio principal usa Σ winsorizado.</div>"
        )

    spark = _evo_spark_svg(d["ratio_wins_series"], d["ratio_wins"])

    pct_fmt    = "—" if pd.isna(d['ratio_wins_pct']) else f"{d['ratio_wins_pct']:.0f}"
    raw_pctfmt = "—" if pd.isna(d['ratio_pct'])      else f"{d['ratio_pct']:.0f}"

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Camada 2 — Diversification Benefit</span>
        <span class="card-sub">CREDITO winsorizado (63d MAD, 3σ, causal) · ratio principal = Σ winsorizado</span>
      </div>

      <div style="display:grid;grid-template-columns:1.2fr 1fr;gap:24px;align-items:start">
        <div>
          <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                      letter-spacing:.5px;margin-bottom:6px">
            VaR por estratégia (bps) — soma linear, corr=1
          </div>
          <table class="summary-table">
            <tbody>{strat_rows_html}</tbody>
          </table>
        </div>

        <div>
          <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                      letter-spacing:.5px;margin-bottom:6px">
            Ratio = VaR_real / Σ VaR_estratégias
          </div>
          <div style="font-size:44px;font-weight:700;line-height:1;
                      color:{state_color};font-family:'JetBrains Mono',monospace">
            {d['ratio_wins']:.2f}
          </div>
          <div style="color:{state_color};font-size:13px;margin-top:4px;font-weight:600">
            {state_label} · P{pct_fmt}
          </div>
          <div style="color:var(--muted);font-size:11px;margin-top:2px">
            (raw sem winsor.: {d['ratio']:.2f} / P{raw_pctfmt})
          </div>

          <div style="margin-top:16px;font-size:13px;line-height:1.7">
            <div><b>VaR real (fundo):</b>
              <span class="mono">{d['var_real_bps']:,.1f} bps</span></div>
            <div><b>Σ estratégias (winsor.):</b>
              <span class="mono">{d['var_soma_wins_bps']:,.1f} bps</span></div>
            <div><b>Benefício:</b>
              <span class="mono">
                −{d['var_soma_wins_bps'] - d['var_real_bps']:,.1f} bps
                ({saving_pct:.0f}% redução)
              </span></div>
            <div style="margin-top:6px"><b>Share CREDITO na Σ:</b>
              <span class="mono" style="color:{cr_color};font-weight:600">{cr_share:.0f}%</span>
              <span style="color:var(--muted)">raw</span> ·
              <span class="mono" style="color:var(--muted)">{cr_share_w:.0f}%</span>
              <span style="color:var(--muted)">winsor.</span>
            </div>
            <div style="margin-top:6px"><b>Percentil 252d:</b>
              <span class="mono">P{pct_fmt}</span>
              · média {d['ratio_wins_mean']:.2f}
              · range [{d['ratio_wins_min']:.2f}, {d['ratio_wins_max']:.2f}]
            </div>
          </div>
        </div>
      </div>

      {wins_note}

      <div style="margin-top:20px">
        <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                    letter-spacing:.5px;margin-bottom:4px">
          Ratio histórico (winsorizado) — últimos 252d · traço = média · dot = hoje
        </div>
        {spark}
      </div>

      <div class="bar-legend" style="margin-top:16px">
        ratio baixo → diversificação reduzindo VaR linear · acima do P80 = alinhamento atípico ·
        tratamento CREDITO: <a href="../../docs/CREDITO_TREATMENT.md" style="color:var(--accent-2)">docs/CREDITO_TREATMENT.md</a>
      </div>
    </section>"""


def _evo_render_camada3(c3: dict, c1_rows: list | None = None) -> str:
    if c3["corr_63d"] is None or not c3["pairs"]:
        return """
        <section class="card">
          <div class="card-head">
            <span class="card-title">Camada 3 — Correlação realizada</span>
            <span class="card-sub">dados insuficientes de PnL por LIVRO</span>
          </div>
        </section>"""

    # Significance filter: a pair only counts as "aligned" when BOTH strategies
    # are simultaneously ≥ P70 in Camada 1 *and* the pair's 63d correlation is
    # ≥ P85. This filters out high correlation between strategies that happen
    # to be small/idle (where correlation is mathematically unstable and
    # economically meaningless).
    c1_pct = {r["strat"]: r["pct"] for r in (c1_rows or [])
              if r.get("pct") is not None and not pd.isna(r.get("pct"))}

    corr = c3["corr_63d"]
    cols = list(corr.columns)

    rows_html = ""
    for i, a in enumerate(cols):
        cells = f"<td style='padding:6px 8px;font-weight:600'>{a}</td>"
        for j, b in enumerate(cols):
            if j < i:
                cells += "<td style='padding:6px 8px;color:var(--line-2)'>·</td>"
            elif j == i:
                cells += "<td style='padding:6px 8px;color:var(--line-2)'>—</td>"
            else:
                pair = next((p for p in c3["pairs"]
                             if p["a"] == a and p["b"] == b), None)
                if pair is None:
                    cells += "<td>—</td>"
                else:
                    color = _evo_pct_color(pair["c63_pct"])
                    pstr = "—" if pd.isna(pair['c63_pct']) else f"P{pair['c63_pct']:.0f}"
                    cells += (
                        f"<td style='padding:6px 8px;text-align:center' class='mono'>"
                        f"<span style='font-weight:600;color:{color}'>{pair['c63']:+.2f}</span>"
                        f"<br><span style='font-size:11px;color:var(--muted)'>{pstr}</span>"
                        f"</td>"
                    )
        rows_html += f"<tr>{cells}</tr>"

    header_cells = (
        "<th></th>"
        + "".join(f"<th style='color:var(--muted)'>{c}</th>" for c in cols)
    )

    pairs_sorted = sorted(c3["pairs"], key=lambda p: -abs(p["c63"]))
    pair_rows = ""
    for p in pairs_sorted:
        color = _evo_pct_color(p["c63_pct"])
        pstr = "—" if pd.isna(p["c63_pct"]) else f"P{p['c63_pct']:.0f}"
        pair_rows += (
            f"<tr>"
            f"<td>{p['a']} × {p['b']}</td>"
            f"<td class='mono' style='text-align:right'>{p['c21']:+.2f}</td>"
            f"<td class='mono' style='text-align:right;color:{color};font-weight:600'>{p['c63']:+.2f}</td>"
            f"<td class='mono' style='text-align:right;color:{color}'>{pstr}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>{p['c63_mean']:+.2f}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>[{p['c63_min']:+.2f}, {p['c63_max']:+.2f}]</td>"
            f"</tr>"
        )

    # Raw flagged = any pair with c63_pct ≥ 85
    # Significant flagged = raw AND both strategies ≥ P70 in Camada 1
    flagged_raw = [p for p in c3["pairs"]
                   if not pd.isna(p["c63_pct"]) and p["c63_pct"] >= 85]
    def _both_loaded(p):
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        return (pa is not None and pa >= 70 and pb is not None and pb >= 70)
    flagged_sig = [p for p in flagged_raw if _both_loaded(p)]
    flagged_quiet = [p for p in flagged_raw if not _both_loaded(p)]

    def _pair_str_with_c1(p):
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        sa = f"P{pa:.0f}" if pa is not None else "—"
        sb = f"P{pb:.0f}" if pb is not None else "—"
        return f"{p['a']}×{p['b']} (corr P{p['c63_pct']:.0f} · {p['a']} C1 {sa}, {p['b']} C1 {sb})"

    alert = ""
    if flagged_sig:
        names = ", ".join(_pair_str_with_c1(p) for p in flagged_sig)
        alert += (f"<div style='margin-top:10px;padding:8px 12px;"
                  f"background:rgba(255,90,106,0.12);border-left:3px solid var(--down);"
                  f"border-radius:4px;font-size:13px;color:var(--text)'>"
                  f"🚨 Alinhamento relevante (corr ≥ P85 · ambas estratégias C1 ≥ P70): {names}</div>")
    if flagged_quiet:
        names = ", ".join(_pair_str_with_c1(p) for p in flagged_quiet)
        alert += (f"<div style='margin-top:8px;padding:8px 12px;"
                  f"background:rgba(184,135,0,0.10);border-left:3px solid var(--warn);"
                  f"border-radius:4px;font-size:13px;color:var(--muted)'>"
                  f"🟡 Correlação alta mas estratégia(s) abaixo de C1 P70 (sinal desconsiderado): {names}</div>")
    if not flagged_raw:
        alert = ("<div style='margin-top:10px;padding:8px 12px;"
                 "background:rgba(74,222,128,0.08);border-left:3px solid var(--up);"
                 "border-radius:4px;font-size:13px;color:var(--muted)'>"
                 "✓ Nenhum par em alinhamento atípico (corr 63d &lt; P85).</div>")

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Camada 3 — Correlação realizada entre estratégias</span>
        <span class="card-sub">PnL diário · janela 63d · percentil 252d de rolling 63d · {c3['n_obs']} obs</span>
      </div>

      <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                  letter-spacing:.5px;margin-bottom:6px">
        Matriz 63d (percentil 252d abaixo)
      </div>
      <table class="summary-table" style="width:auto;margin-bottom:18px">
        <thead><tr>{header_cells}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>

      <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                  letter-spacing:.5px;margin-bottom:6px">
        Pares — 21d · 63d · P252d · média · range
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Par</th>
          <th style="text-align:right">21d</th>
          <th style="text-align:right">63d</th>
          <th style="text-align:right">P252d</th>
          <th style="text-align:right">média</th>
          <th style="text-align:right">range</th>
        </tr></thead>
        <tbody>{pair_rows}</tbody>
      </table>

      {alert}

      <div class="bar-legend" style="margin-top:16px">
        correlação positiva entre direcionais reduz diversificação ·
        P &gt; 85 = alinhamento atípico ·
        21d sensível · 63d baseline ·
        <b>filtro de significância</b>: par só conta como alinhamento relevante se <u>ambas</u>
        estratégias estão ≥ P70 na Camada 1 (caso contrário a correlação alta pode ser
        artefato de uma estratégia ociosa)
      </div>
    </section>"""


def _evo_render_camada_direcional(c_dir: dict) -> str:
    """Render da Camada Direcional — DELTA_SIST × DELTA_DISC por CATEGORIA.
       Mostra tabela com sinal de cada perna + badge do estado."""
    rows = c_dir.get("rows", [])
    if not rows:
        return ""

    state_badge = {
        "same-sign":  ("🟥 mesmo sinal",    "var(--down)"),
        "opposite":   ("🟩 sinais opostos", "var(--up)"),
        "only-sist":  ("⚪ só sistemático", "var(--muted)"),
        "only-disc":  ("⚪ só discricion.", "var(--muted)"),
        "dust":       ("— irrelevante",     "var(--line-2)"),
    }

    def _cell_bps(v, material=True):
        if abs(v) < 0.1:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        col = "var(--up)" if v >= 0 else "var(--down)"
        sign = "+" if v >= 0 else ""
        opacity = "" if material else "opacity:0.6;"
        return f'<td class="mono" style="text-align:right;color:{col};{opacity}">{sign}{v:.0f}</td>'

    def _cell_pct(v, material=True):
        vp = v * 100  # fraction → %
        col = "var(--up)" if vp >= 0 else "var(--down)"
        sign = "+" if vp >= 0 else ""
        weight = "font-weight:700;" if material else ""
        return f'<td class="mono" style="text-align:right;color:{col};{weight}">{sign}{vp:.1f}%</td>'

    body = ""
    for r in rows:
        label, color = state_badge.get(r["state"], ("—", "var(--muted)"))
        dim = "" if r["material"] else " (abaixo de 1% PL)"
        cat_style = "color:var(--text)" if r["material"] else "color:var(--muted)"
        body += (
            f'<tr>'
            f'<td style="{cat_style};padding:5px 4px"><b>{r["categoria"]}</b> '
            f'<span style="color:var(--muted);font-size:11px">· {r["tipo"]}{dim}</span></td>'
            + _cell_bps(r["sist_bps"], r["material"])
            + _cell_bps(r["disc_bps"], r["material"])
            + _cell_pct(r["pct_pl"],  r["material"])
            + f'<td style="text-align:center;padding:5px 4px;color:{color};font-size:12px">{label}</td>'
            + '</tr>'
        )

    same_cnt = c_dir.get("same_sign_count", 0)
    same_cats = c_dir.get("same_sign_categorias", [])
    th = c_dir.get("thresholds", {})

    if same_cnt >= 3:
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(255,90,106,0.12);border-left:3px solid var(--down);"
                 f"border-radius:4px;font-size:13px;color:var(--text)'>"
                 f"🚨 <b>{same_cnt} categorias</b> com sistemática e discricionária "
                 f"alinhadas no mesmo sinal: {', '.join(same_cats)}</div>")
    elif same_cnt >= 1:
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(184,135,0,0.10);border-left:3px solid var(--warn);"
                 f"border-radius:4px;font-size:13px;color:var(--text)'>"
                 f"🟡 {same_cnt} categoria(s) com mesmo sinal: {', '.join(same_cats)} "
                 f"(abaixo do gatilho de ≥3 da Camada 4)</div>")
    else:
        alert = ("<div style='margin-top:10px;padding:8px 12px;"
                 "background:rgba(74,222,128,0.08);border-left:3px solid var(--up);"
                 "border-radius:4px;font-size:13px;color:var(--muted)'>"
                 "✓ Nenhuma categoria com sistemática e discricionária no mesmo sinal.</div>")

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Matriz Direcional — SIST × DISC por categoria</span>
        <span class="card-sub">DELTA_SIST × DELTA_DISC · fonte <code>RISK_DIRECTION_REPORT</code> ·
        filtros: cada perna ≥ {th.get('min_leg_bps', 5):.0f} bps, categoria ≥ {th.get('min_cat_pct', 1):.0f}% PL</span>
      </div>

      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Categoria</th>
          <th style="text-align:right">Sist (bps)</th>
          <th style="text-align:right">Disc (bps)</th>
          <th style="text-align:right">% PL</th>
          <th style="text-align:center">Estado</th>
        </tr></thead>
        <tbody>{body}</tbody>
      </table>

      {alert}

      <div class="bar-legend" style="margin-top:14px">
        a <b>smoking gun</b> do alinhamento: se sistemática e discricionária estão long/short
        na mesma classe de ativo, a descorrelação esperada entre as duas metades do fundo desaparece.
        ≥3 categorias com mesmo sinal → condição 5 da Camada 4 (bull market alignment).
        "Sinais opostos" (🟩) é o estado saudável — as duas metades se hedgeiam.
      </div>
    </section>"""


def _evo_render_camada4_alert(c4: dict) -> str:
    """Caixa de alerta Camada 4 — exibida no TOPO do tab Diversificação do Evolution.
       Mostra quantas condições acenderam e detalhe de cada uma."""
    if not c4:
        return ""
    n_lit = c4.get("n_lit", 0)
    alert_fired = c4.get("alert", False)
    buckets = c4.get("buckets_pct", {})

    # Bucket summary bar
    bucket_html = " · ".join(
        f'<b>{k}</b> P{v:.0f}' for k, v in buckets.items()
    )

    cond_rows = ""
    for c in c4.get("conditions", []):
        mark  = "🔴" if c["lit"] else "⚪"
        label = f"<b>{c['name']}</b>"
        if c["id"] == 1 and c["detail"]:
            det = ", ".join(f"{k} P{v:.0f}" for k, v in c["detail"].items())
            det_html = f" · {det}" if det else ""
        elif c["id"] == 2 and c["detail"]:
            det = ", ".join(f"{k} P{v:.0f}" for k, v in c["detail"].items())
            det_html = f" · {det}" if det else ""
        elif c["id"] == 3 and c["detail"] is not None and not pd.isna(c["detail"]):
            det_html = f" · ratio em P{c['detail']:.0f}"
        elif c["id"] == 4 and c["detail"]:
            det = ", ".join(f"{p['a']}×{p['b']} P{p['c63_pct']:.0f}" for p in c["detail"])
            det_html = f" · {det}"
        elif c["id"] == 5 and c["detail"]:
            det_html = f" · {', '.join(c['detail'])}"
        else:
            det_html = ""
        cond_rows += (
            f'<li style="margin:4px 0;color:'
            f'{"var(--text)" if c["lit"] else "var(--muted)"}">{mark} {label}{det_html}</li>'
        )

    if alert_fired:
        title = "🚨 BULL MARKET ALIGNMENT — alerta disparado"
        subtitle = f"{n_lit} de 5 condições acesas (gatilho ≥ 3)"
        bg = "rgba(255,90,106,0.14)"; border = "var(--down)"
    elif n_lit >= 1:
        title = "🟡 Alinhamento parcial"
        subtitle = f"{n_lit} de 5 condições acesas (gatilho em 3)"
        bg = "rgba(184,135,0,0.10)"; border = "var(--warn)"
    else:
        title = "✓ Sem sinais de bull market alignment"
        subtitle = "0 de 5 condições acesas"
        bg = "rgba(74,222,128,0.06)"; border = "var(--up)"

    return f"""
    <section class="card" style="background:{bg};border-left:4px solid {border}">
      <div class="card-head">
        <span class="card-title">Camada 4 — {title}</span>
        <span class="card-sub">{subtitle}</span>
      </div>
      <div style="padding:4px 8px 8px;font-size:12px;color:var(--muted)">
        <b>Buckets direcionais:</b> {bucket_html or '—'}
      </div>
      <ul style="margin:0;padding:8px 24px 14px;font-size:13px;line-height:1.7;list-style:none">
        {cond_rows}
      </ul>
      <div class="bar-legend" style="margin-top:4px;padding:0 14px 12px">
        "Bull market alignment" = cenário onde as estratégias do Evolution ficam direcionais
        na mesma direção, a diversificação esperada desaparece, e o risco efetivo do fundo
        fica maior que o VaR linear sugere. Buckets unidos: FRONTIER+EVO_STRAT (pacote tático).
      </div>
    </section>"""


def build_evolution_diversification_section(date_str: str) -> tuple[str, dict]:
    """Retorna (fragmento HTML, c4_state).
       c4_state é usado pelo Summary pra headline quando o alerta acende."""
    try:
        d       = _evo_build_ratio_series(date_str)
        c1_rows = _evo_compute_camada1(d["strat_pivot"], d["effective_date"])
        c3      = _evo_compute_camada3(date_str, d["effective_date"])
        # Matriz direcional (nova)
        try:
            df_dir = _evo_fetch_direction_report(date_str)
            c_dir  = _evo_compute_camada_direcional(df_dir, d["nav"])
        except Exception as ee:
            print(f"  [Evolution] direction report fetch failed: {ee}")
            c_dir = {"rows": [], "same_sign_count": 0, "same_sign_categorias": [],
                     "thresholds": {"min_leg_bps": 5, "min_cat_pct": 1}}
        # Camada 4 (agregada)
        c4 = _evo_compute_camada4(c1_rows, d, c3, c_dir,
                                  d["strat_pivot"], d["effective_date"])
    except Exception as e:
        err_html = (f"<section class='card'><div class='card-head'>"
                    f"<span class='card-title'>Diversificação</span></div>"
                    f"<div style='color:var(--muted);padding:12px'>"
                    f"Sem dados para {date_str}: {e}</div></section>")
        return err_html, {}

    html = (_evo_render_camada4_alert(c4)
            + _evo_render_camada2(d)
            + _evo_render_camada1(c1_rows)
            + _evo_render_camada3(c3, c1_rows)
            + _evo_render_camada_direcional(c_dir))
    return html, c4


REPORTS = [
    ("performance",     "PA"),
    ("exposure",        "Exposure"),
    ("exposure-map",    "Exposure Map"),
    ("single-name",     "Single-Name"),
    ("risk-monitor",    "Risk Monitor"),
    ("diversification", "Diversificação"),
    ("analise",         "Análise"),
    ("distribution",    "Distribuição 252d"),
    ("vol-regime",      "Vol Regime"),
    ("stop-monitor",    "Risk Budget"),
    ("briefing",        "Briefing"),
]
FUND_ORDER  = ["MACRO", "QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER", "IDKA_3Y", "IDKA_10Y"]
FUND_LABELS = {
    "MACRO": "Macro", "QUANT": "Quantitativo", "EVOLUTION": "Evolution",
    "MACRO_Q": "Macro Q", "ALBATROZ": "Albatroz", "FRONTIER": "Frontier",
    "IDKA_3Y": "IDKA 3Y", "IDKA_10Y": "IDKA 10Y",
}

# PA key in q_models.REPORT_ALPHA_ATRIBUTION for each fund short.
#
# IDKA calibration vs. xlsx (F:/Macros/DADOS/Projetos/PA_Fundos/PA_REPORTS/YYYY_MM_DD_DAILY_PA.xlsx):
# ---------------------------------------------------------------------------------------------------
# Snapshot 2026-04-17 · IDKA 3Y "DISCRICIONÁRIO" TOTAL row (RF BZ + RF BZ IPCA only, exclui Caixa/Custos):
#   Xlsx:  DIA +7.58 bps · MÊS +85.98 bps · ANO -65.94 bps
#   DB:    DIA +7.58 bps · MÊS +86.17 bps · ANO -61.93 bps
# DIA bate exato. MÊS/ANO com residual de ~2–4 bps — provável rounding de display no xlsx vs floats
# no banco, não problema estrutural. Revisitar se algum dia o gap crescer para > 10 bps.
#
# IDKAs não têm coluna LEVEL em REPORT_ALPHA_ATRIBUTION — apenas leaf rows (87 p/ 3Y, 85 p/ 10Y).
# A hierarquia de níveis vem das colunas dimensionais: CLASSE → GRUPO → SUBCLASSE → LIVRO →
# BOOK → PRODUCT_CLASS → PRODUCT.
_FUND_PA_KEY = {
    "MACRO":     "MACRO",
    "QUANT":     "QUANT",
    "EVOLUTION": "EVOLUTION",
    "MACRO_Q":   "GLOBAL",
    "ALBATROZ":  "ALBATROZ",
    "FRONTIER":  "GFA",
    "IDKA_3Y":   "IDKAIPCAY3",
    "IDKA_10Y":  "IDKAIPCAY10",
}

# Livros that are benchmark-tracking (passive replica) per fund PA key.
# Their dia_bps contribution moves with the index, not with alpha — exclude
# from "≥ 1% NAV" alerts since the index itself is not a risk event.
_PA_BENCH_LIVROS = {
    "IDKAIPCAY3":  {"CI"},   # CI book tracks the IDKA 3Y index
    "IDKAIPCAY10": {"CI"},   # CI book tracks the IDKA 10Y index
}

def _pa_filter_alpha(df):
    """Drop PA rows from benchmark-tracking livros; keep only alpha-bearing contributions."""
    if df is None or df.empty:
        return df
    mask = True
    for pa_key, livros in _PA_BENCH_LIVROS.items():
        mask = mask & ~((df["FUNDO"] == pa_key) & (df["LIVRO"].isin(livros)))
    return df[mask]

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

    # Use the canonical mapping (includes IDKAIPCAY3 / IDKAIPCAY10).
    _PA_KEY = dict(_FUND_PA_KEY)
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
            ok = manifest.get("quant_expo_ok", False)
            rows = manifest.get("quant_expo_rows", 0)
            if not ok:
                return dict(status="missing", date="—", detail="Exposição QUANT não disponível")
            return dict(status="ok", date=DATA_STR_, detail=f"{rows} posições")
        if short == "EVOLUTION":
            ok = manifest.get("evo_expo_ok", False)
            rows = manifest.get("evo_expo_rows", 0)
            if not ok:
                return dict(status="missing", date="—", detail="Exposição EVOLUTION não disponível")
            return dict(status="ok", date=DATA_STR_, detail=f"{rows} posições")
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
        ("PA / PnL",          ["MACRO","QUANT","EVOLUTION","MACRO_Q","ALBATROZ","IDKA_3Y","IDKA_10Y"], _pa_item),
        ("VaR / Stress",      ["MACRO","QUANT","EVOLUTION","MACRO_Q","ALBATROZ","IDKA_3Y","IDKA_10Y"], _var_item),
        ("Exposição",         ["MACRO","QUANT","EVOLUTION","ALBATROZ"],           _expo_item),
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

    # ≥1% NAV single-product contribution (alpha only — benchmark-tracking livros excluded)
    if df_pa is not None and not df_pa.empty:
        _pa_alpha_sc = _pa_filter_alpha(df_pa)
        _PA_TO_SHORT = {v: k for k, v in _PA_KEY.items()}
        for short in FUND_ORDER:
            pa_key = _PA_KEY.get(short)
            if pa_key is None: continue
            sub = _pa_alpha_sc[
                (_pa_alpha_sc["FUNDO"] == pa_key) &
                (~_pa_alpha_sc["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos", "Prev"})) &
                (~_pa_alpha_sc["CLASSE"].isin({"Caixa", "Custos"}))
            ]
            if sub.empty:
                continue
            hits = sub[sub["dia_bps"].abs() >= 100.0].sort_values(
                "dia_bps", key=lambda s: s.abs(), ascending=False
            )
            if hits.empty:
                continue
            top = hits.iloc[0]
            n = len(hits)
            extra = f" · +{n-1} outros" if n > 1 else ""
            _b = float(top["dia_bps"])
            sanity_rows += (
                f'<tr><td style="padding:5px 12px;white-space:nowrap">{FUND_LABELS.get(short,short)}</td>'
                f'<td style="padding:5px 12px">Contribuição PA ≥ 1% NAV</td>'
                f'<td style="text-align:center;padding:5px 8px">'
                f'<span style="color:#f87171;font-weight:700;font-size:11px">🚨 {_b/100:+.2f}%</span></td>'
                f'<td style="padding:5px 12px;color:var(--muted);font-size:11.5px">'
                f'{top["PRODUCT"]} ({top["LIVRO"]}) {_b:+.0f} bps{extra}</td></tr>'
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


def _build_fund_mini_briefing(
    short: str, house_row: dict, series_map: dict, td_by_short: dict,
    df_pa, factor_matrix: dict, bench_matrix: dict,
    pm_margem=None, frontier_bvar=None, df_frontier=None,
) -> str:
    """Compact 1–2 min per-fund briefing card. Shows at top of each fund's
       reports as the "Briefing" tab.
    """
    if not house_row:
        return ""

    def _mm(v):
        try: return f"{v/1e6:,.1f}M".replace(",", "_").replace(".", ",").replace("_", ".")
        except Exception: return "—"

    td = td_by_short.get(short)
    cfg = ALL_FUNDS.get(td, {}) if td else {}

    # Δ VaR D-1 (bps)
    dvar_bps = None
    s = series_map.get(td) if td else None
    if s is not None and not s.empty:
        s_avail = s[s["VAL_DATE"] <= DATA]
        if len(s_avail) >= 2:
            v_today = abs(float(s_avail.iloc[-1]["var_pct"]))
            v_prev  = abs(float(s_avail.iloc[-2]["var_pct"]))
            dvar_bps = (v_today - v_prev) * 100

    # Util (primary metric)
    util = None
    if not cfg.get("informative"):
        soft = cfg.get("var_soft", 0)
        v_pct = house_row["bvar_pct"] if cfg.get("primary") == "bvar" else house_row["var_pct"]
        util = v_pct / soft * 100 if soft else None

    # Top contributors / detractors today (for funds with PA)
    pa_key = _FUND_PA_KEY.get(short)
    top_contrib = []
    top_detract = []
    big_hits = []  # PA contributions ≥ 1% (100 bps) of NAV — flagged as alerts
    if pa_key and df_pa is not None and not df_pa.empty:
        sub = df_pa[
            (df_pa["FUNDO"] == pa_key) &
            (~df_pa["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos"})) &
            (~df_pa["CLASSE"].isin({"Caixa", "Custos"}))
        ].copy()
        if not sub.empty:
            sub_sorted = sub.sort_values("dia_bps", ascending=False)
            top_contrib = sub_sorted[sub_sorted["dia_bps"] > 0.5].head(2)
            top_detract = sub_sorted[sub_sorted["dia_bps"] < -0.5].tail(2)
            # ≥ 1% alert must exclude benchmark-tracking livros (IDKA CI tracks
            # the index; a 200 bp move there is tracking, not alpha).
            sub_alpha = _pa_filter_alpha(sub)
            _big = sub_alpha[sub_alpha["dia_bps"].abs() >= 100.0].sort_values(
                "dia_bps", key=lambda s: s.abs(), ascending=False
            )
            big_hits = list(_big.itertuples(index=False))

    # Fund-level factor dominance (net of bench) — only for funds in factor_matrix
    dominant_factor = None
    if factor_matrix and bench_matrix:
        nets = {}
        for fk, allocs in factor_matrix.items():
            v = allocs.get(short, 0.0) - bench_matrix.get(fk, {}).get(short, 0.0)
            if abs(v) >= 1_000:
                nets[fk] = v
        if nets:
            dominant_factor = max(nets.items(), key=lambda x: abs(x[1]))

    # Stop margem (MACRO only)
    stop_min = None
    if short == "MACRO" and pm_margem:
        try:
            stop_min = min((pm, m) for pm, m in pm_margem.items() if m is not None)
        except ValueError:
            stop_min = None

    # ── Risk budget alert: VaR/BVaR > 1.5× orçamento MTD disponível ─────────
    # Remaining MTD budget (in bps of NAV). Only funds with explicit monthly
    # loss budget: MACRO (sum of PM margens) and ALBATROZ (150 bps − consumed).
    budget_remaining_bps = None
    if short == "MACRO" and pm_margem:
        mlist = [max(0.0, m) for m in pm_margem.values() if m is not None]
        if mlist:
            budget_remaining_bps = float(sum(mlist))
    elif short == "ALBATROZ" and df_pa is not None and not df_pa.empty:
        alb = df_pa[df_pa["FUNDO"] == "ALBATROZ"]
        if not alb.empty:
            mtd = float(alb["mtd_bps"].sum())
            budget_remaining_bps = max(0.0, ALBATROZ_STOP_BPS + min(0.0, mtd))

    budget_alert = None  # (remaining_bps, max_var_bps, var_bps, bvar_bps)
    if budget_remaining_bps is not None and budget_remaining_bps > 0:
        var_bps_abs  = abs(house_row.get("var_pct")  or 0.0) * 100
        bvar_bps_abs = abs(house_row.get("bvar_pct") or 0.0) * 100
        max_var_bps  = max(var_bps_abs, bvar_bps_abs)
        if max_var_bps > 1.5 * budget_remaining_bps:
            budget_alert = (budget_remaining_bps, max_var_bps, var_bps_abs, bvar_bps_abs)

    # ── Stat chips ────────────────────────────────────────────────────────────
    is_bvar = cfg.get("primary") == "bvar"
    is_frontier = (short == "FRONTIER")
    var_label = "BVaR 95%" if is_bvar else ("HS BVaR" if is_frontier else "VaR 95%")
    var_val = house_row["bvar_pct"] if (is_bvar or is_frontier) else house_row["var_pct"]

    def _chip(lbl, val, color=None):
        col = f' style="color:{color}"' if color else ''
        return (f'<span class="sn-stat"><span class="sn-lbl">{lbl}</span>'
                f'<span class="sn-val mono"{col}>{val}</span></span>')

    chips = (
        _chip("NAV", f"R$ {_mm(house_row['nav'])}") +
        _chip(var_label, f"{var_val:.2f}%") +
        (_chip("Util", f"{util:.0f}%",
               "var(--down)" if util and util >= 100
               else "var(--warn)" if util and util >= 70 else "var(--up)")
         if util is not None else "") +
        (_chip("Δ VaR D-1", f"{dvar_bps:+.0f} bps",
               "var(--down)" if dvar_bps and dvar_bps > 0 else "var(--up)")
         if dvar_bps is not None else "") +
        _chip("DIA",  f"{house_row['var_pct']*0 + house_row.get('var_pct',0) if False else 0}", None)  # placeholder
    )

    # Return chips from PA sums per window
    dia_bps = mtd_bps = ytd_bps = m12_bps = None
    if pa_key and df_pa is not None and not df_pa.empty:
        sub_all = df_pa[df_pa["FUNDO"] == pa_key]
        if not sub_all.empty:
            dia_bps = float(sub_all["dia_bps"].sum())
            mtd_bps = float(sub_all["mtd_bps"].sum())
            ytd_bps = float(sub_all["ytd_bps"].sum())
            m12_bps = float(sub_all["m12_bps"].sum())
    def _ret_chip(lbl, bps):
        if bps is None:
            return ""
        pct = bps / 100
        col = "var(--up)" if pct >= 0 else "var(--down)"
        return _chip(lbl, f"{'+' if pct>=0 else ''}{pct:.2f}%", col)

    chips = (
        _chip("NAV", f"R$ {_mm(house_row['nav'])}") +
        _chip(var_label, f"{var_val:.2f}%") +
        (_chip("Util", f"{util:.0f}%",
               "var(--down)" if util >= 100
               else "var(--warn)" if util >= 70 else "var(--up)")
         if util is not None else "") +
        (_chip("Δ D-1",
               f"{dvar_bps:+.0f} bps",
               "var(--down)" if dvar_bps > 0 else "var(--up)" if dvar_bps < 0 else None)
         if dvar_bps is not None else "") +
        '<span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>' +
        _ret_chip("DIA",  dia_bps) +
        _ret_chip("MTD",  mtd_bps) +
        _ret_chip("YTD",  ytd_bps) +
        _ret_chip("12M",  m12_bps)
    )

    # ── Headline ──────────────────────────────────────────────────────────────
    headline_parts = []
    # Rule 0: single PA contribution ≥ 1% (100 bps) of NAV
    if big_hits:
        _h = big_hits[0]
        _b = float(_h.dia_bps)
        _icon = "🟢" if _b > 0 else "🚨"
        _col  = "var(--up)" if _b > 0 else "var(--down)"
        _extra = f" +{len(big_hits)-1}" if len(big_hits) > 1 else ""
        headline_parts.append(
            f'{_icon} <b>{_h.PRODUCT}</b> <span style="color:{_col}">{_b/100:+.2f}%</span> (≥ 1% NAV){_extra}'
        )
    if budget_alert is not None:
        _rem, _mx, _v, _b = budget_alert
        headline_parts.append(
            f'🚨 <b>VaR {_mx:.0f} bps</b> > 1,5× budget MTD ({_rem:.0f} bps)'
        )
    if util is not None and util >= 85:
        headline_parts.append(f'🔴 <b>{util:.0f}% do soft limit</b>')
    elif dvar_bps is not None and abs(dvar_bps) >= 10:
        direction = "subiu" if dvar_bps > 0 else "caiu"
        headline_parts.append(f'VaR {direction} <b>{abs(dvar_bps):.0f} bps</b> vs D-1')
    if dia_bps is not None and abs(dia_bps) >= 5:
        sign = "+" if dia_bps > 0 else ""
        col = "var(--up)" if dia_bps > 0 else "var(--down)"
        headline_parts.append(f'alpha do dia <b style="color:{col}">{sign}{dia_bps/100:.2f}%</b>')
    if not headline_parts:
        headline_parts.append("dia tranquilo — sem eventos materiais")
    headline = " · ".join(headline_parts)

    # ── Insights bullets ──────────────────────────────────────────────────────
    bullets = []
    if big_hits:
        _items = " · ".join(
            (f'<b>{h.PRODUCT}</b> '
             f'<span class="mono" style="color:{"var(--up)" if float(h.dia_bps) > 0 else "var(--down)"}">'
             f'{float(h.dia_bps)/100:+.2f}%</span>')
            for h in big_hits[:4]
        )
        _lead_icon = "🟢" if all(float(h.dia_bps) > 0 for h in big_hits) else "🚨"
        bullets.append(
            f'<li>{_lead_icon} <b style="color:var(--down)">Contribuição ≥ 1% NAV:</b> {_items}</li>'
        )
    if budget_alert is not None:
        _rem, _mx, _v, _b = budget_alert
        _var_label_bps = "VaR"
        if _b > 0 and _v > 0:
            _val_str = f'VaR {_v:.0f} bps · BVaR {_b:.0f} bps'
        elif _b > 0:
            _val_str = f'BVaR {_b:.0f} bps'
            _var_label_bps = "BVaR"
        else:
            _val_str = f'VaR {_v:.0f} bps'
        _ratio = _mx / _rem if _rem > 0 else float("inf")
        bullets.append(
            f'<li>🚨 <b style="color:var(--down)">Alerta:</b> {_val_str} · '
            f'orçamento MTD disponível <span class="mono">{_rem:.0f} bps</span> · '
            f'razão <b class="mono" style="color:var(--down)">{_ratio:.1f}×</b> '
            f'(> 1,5× — uma perda típica esgota o budget e sobra stress)</li>'
        )
    if len(top_contrib) > 0:
        items = " · ".join(
            f'<b>{r.PRODUCT}</b> {r.dia_bps/100:+.2f}%'
            for r in top_contrib.itertuples(index=False)
        )
        bullets.append(f'<li><span style="color:var(--up)">▲</span> <b>Contribuintes:</b> {items}</li>')
    if len(top_detract) > 0:
        items = " · ".join(
            f'<b>{r.PRODUCT}</b> {r.dia_bps/100:+.2f}%'
            for r in top_detract.itertuples(index=False)
        )
        bullets.append(f'<li><span style="color:var(--down)">▼</span> <b>Detratores:</b> {items}</li>')
    # FRONTIER — net vs IBOV bench is misleading (90% long + 10% cash reads as "10% short").
    # Report gross equity allocation + weighted portfolio beta from df_frontier instead.
    if short == "FRONTIER" and df_frontier is not None and not df_frontier.empty:
        _tot = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
        if not _tot.empty:
            _gross = float(_tot.iloc[0]["% Cash"]) if pd.notna(_tot.iloc[0]["% Cash"]) else None
            _cash  = max(0.0, 1.0 - _gross) if _gross is not None else None
            _stocks = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL","SUBTOTAL","AÇÕES ALIENADAS"])]
            _sv = _stocks.dropna(subset=["BETA","% Cash"])
            _psum = _sv["% Cash"].sum()
            _wbeta = float((_sv["% Cash"] * _sv["BETA"]).sum() / _psum) if _psum else None
            parts = []
            if _gross is not None:
                parts.append(f'<b class="mono" style="color:var(--up)">{_gross*100:.0f}%</b> equity')
            if _cash is not None:
                parts.append(f'<span class="mono" style="color:var(--muted)">{_cash*100:.0f}% caixa</span>')
            if _wbeta is not None:
                beta_col = ("var(--down)" if _wbeta > 1.10
                            else "var(--warn)" if _wbeta > 1.05
                            else "var(--text)")
                parts.append(
                    f'β ponderado <b class="mono" style="color:{beta_col}">{_wbeta:.2f}</b>'
                    f' <span style="color:var(--muted);font-size:10px">(IBOV β=1.00)</span>'
                )
            if parts:
                bullets.append(
                    '<li>📊 <b>Alocação:</b> ' + ' · '.join(parts) + '</li>'
                )
    elif dominant_factor:
        fk, v = dominant_factor
        is_rate = fk in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
        if is_rate:
            # Convenção DV01 (ground truth): tomado = DV01 > 0 (short bond / short DI1F),
            # dado = DV01 < 0 (long bond / long DI1F). factor_matrix já vem nessa convenção.
            direction = "tomado" if v > 0 else "dado"
            dir_color = "var(--down)" if v > 0 else "var(--up)"
        else:
            direction = "longo" if v > 0 else "curto"
            dir_color = "var(--up)" if v > 0 else "var(--down)"
        # Per-fund bullets use RELATIVE measures only: yr-equivalent for rate factors,
        # %NAV for everything else. Absolute BRL is cross-fund territory.
        is_rate_unit = fk in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
        fund_nav = house_row["nav"] if house_row else 0
        if is_rate_unit and fund_nav:
            yr_eq = v / fund_nav
            val_str = f'<span class="mono">{yr_eq:+.2f} yr</span>'
        elif fund_nav:
            pct_nav = v / fund_nav * 100
            val_str = f'<span class="mono">{pct_nav:+.1f}% NAV</span>'
        else:
            val_str = '<span class="mono" style="color:var(--muted)">—</span>'
        bullets.append(
            f'<li>🎯 <b>Posição líquida:</b> '
            f'<b class="mono" style="color:{dir_color} !important">{direction}</b> em '
            f'<b>{fk}</b> · {val_str}</li>'
        )
    if stop_min and stop_min[1] < 30:
        pm, m = stop_min
        bullets.append(
            f'<li>⚠ <b>Stop apertado:</b> {pm} com <span class="mono">{m:.0f} bps</span> de margem</li>'
        )
    if short == "FRONTIER" and frontier_bvar:
        bullets.append(
            f'<li>📊 <b>BVaR HS vs IBOV:</b> <span class="mono">{frontier_bvar["bvar_pct"]:.2f}%</span> · '
            f'TE anualizada <span class="mono">{frontier_bvar["std_er_pct"]*(252**0.5):.1f}%</span></li>'
        )
    if not bullets:
        bullets.append('<li><span style="color:var(--muted)">— sem destaques materiais hoje</span></li>')

    # ── Commentary (prose synthesis, 1–3 sentences) ───────────────────────────
    commentary_parts = []

    # Sentence 1 — day summary
    if dia_bps is None:
        commentary_parts.append(
            "Sem PA disponível no dia; olhar apenas VaR e exposições abaixo."
        )
    elif abs(dia_bps) < 5 and (dvar_bps is None or abs(dvar_bps) < 5):
        commentary_parts.append(
            "Dia neutro — alpha e risco estáveis, nada material para reportar."
        )
    else:
        parts1 = []
        if abs(dia_bps) >= 5:
            sign = "+" if dia_bps > 0 else ""
            parts1.append(f"alpha do dia em <b>{sign}{dia_bps/100:.2f}%</b>")
        if dvar_bps is not None and abs(dvar_bps) >= 5:
            parts1.append(
                f"VaR {'↑' if dvar_bps > 0 else '↓'} <b>{abs(dvar_bps):.0f} bps</b> vs D-1"
            )
        commentary_parts.append("Dia com " + " e ".join(parts1) + ".")

    # Sentence 2 — list top contributor/detractor by absolute size (no ratios).
    # Only mention when the contribution itself is material (|bps| ≥ 3), and
    # avoid mixing sides — just report what pulled up and what pulled down.
    flow_parts = []
    if len(top_contrib) > 0:
        top_c = top_contrib.iloc[0]
        if abs(float(top_c["dia_bps"])) >= 3:
            flow_parts.append(
                f"contrib <b>{top_c['PRODUCT']}</b> <span style=\"color:var(--up)\">+{float(top_c['dia_bps'])/100:.2f}%</span>"
            )
    if len(top_detract) > 0:
        top_d = top_detract.iloc[-1]  # most negative
        if abs(float(top_d["dia_bps"])) >= 3:
            flow_parts.append(
                f"detrator <b>{top_d['PRODUCT']}</b> <span style=\"color:var(--down)\">{float(top_d['dia_bps'])/100:.2f}%</span>"
            )
    if flow_parts:
        commentary_parts.append("Fluxo: " + " · ".join(flow_parts) + ".")

    # Sentence 3 — positioning / warnings
    pos_notes = []
    if util is not None:
        if util >= 85:
            pos_notes.append(f"util VaR em <b>{util:.0f}%</b> do soft — próximo do limite")
        elif util >= 70:
            pos_notes.append(f"util VaR em {util:.0f}% — vigilância")
    # FRONTIER — report gross equity allocation + weighted beta (not net vs IBOV).
    if short == "FRONTIER" and df_frontier is not None and not df_frontier.empty:
        _tot2 = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
        if not _tot2.empty and pd.notna(_tot2.iloc[0]["% Cash"]):
            _gross2 = float(_tot2.iloc[0]["% Cash"])
            _stk = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL","SUBTOTAL","AÇÕES ALIENADAS"])]
            _stkv = _stk.dropna(subset=["BETA","% Cash"])
            _ps = _stkv["% Cash"].sum()
            _wb = float((_stkv["% Cash"] * _stkv["BETA"]).sum() / _ps) if _ps else None
            _bit = f"alocação equity <b>{_gross2*100:.0f}%</b>"
            if _wb is not None:
                _bit += f", β ponderado <b>{_wb:.2f}</b>"
            pos_notes.append(_bit)
    elif dominant_factor:
        fk, v = dominant_factor
        is_rate = fk in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
        # Convenção DV01: tomado = DV01 > 0 (short); dado = DV01 < 0 (long, ex: NTN-B comprado).
        dir_word = (("tomado" if v > 0 else "dado") if is_rate
                    else ("longo" if v > 0 else "curto"))
        fund_nav_tmp = house_row["nav"] if house_row else 0
        if is_rate and fund_nav_tmp:
            pos_notes.append(f"{dir_word} em {fk} ({v/fund_nav_tmp:+.2f} yr eq)")
        elif fund_nav_tmp:
            pos_notes.append(f"{dir_word} em {fk} ({v/fund_nav_tmp*100:+.1f}% NAV)")
        else:
            pos_notes.append(f"{dir_word} em {fk}")
    if stop_min and stop_min[1] < 30:
        pos_notes.append(f"PM {stop_min[0]} com apenas {stop_min[1]:.0f} bps de stop")
    if pos_notes:
        commentary_parts.append("Posicionamento: " + "; ".join(pos_notes) + ".")

    commentary = " ".join(commentary_parts)

    return f"""
    <section class="card brief-card">
      <div class="card-head">
        <span class="card-title">Briefing — {FUND_LABELS.get(short, short)}</span>
        <span class="card-sub">— {DATA_STR} · resumo rápido 1–2 min</span>
      </div>
      <div class="brief-headline">{headline}</div>
      <p class="brief-commentary">{commentary}</p>
      <div class="sn-inline-stats mono" style="margin-bottom:14px; flex-wrap:wrap; gap:6px 14px">
        {chips}
      </div>
      <ul class="brief-list">{"".join(bullets)}</ul>
      <div class="brief-footnote" style="margin-top:10px; padding-top:8px; border-top:1px solid var(--line); font-size:10.5px; color:var(--muted); line-height:1.5">
        <b>Convenção de juros:</b>
        <span style="color:var(--up);font-weight:700">dado</span> = DV01 &lt; 0 (long bond · ex: NTN-B comprado, long DI1F) ·
        <span style="color:var(--down);font-weight:700">tomado</span> = DV01 &gt; 0 (short bond · ex: DI1F vendido).
      </div>
    </section>"""


def _build_executive_briefing(
    *,
    house_rows, factor_matrix, bench_matrix, agg_rows,
    position_changes, vol_regime_map, pm_margem, df_pa,
    frontier_bvar, series_map, td_by_short, ibov, cdi,
) -> str:
    """Curated 3–5 min briefing. Pulls from already-computed structures.
       Rotates headline category to avoid daily repetition; hides sections
       that have nothing material to report.
    """
    def _mm(v):
        try: return f"{v/1e6:,.1f}M".replace(",", "_").replace(".", ",").replace("_", ".")
        except Exception: return "—"
    def _pct(v, sign=True):
        try:
            f = float(v)
            return f"{'+' if f>=0 and sign else ''}{f:.2f}%"
        except Exception: return "—"
    def _bps(v, sign=True):
        try:
            f = float(v)
            return f"{'+' if f>=0 and sign else ''}{f:.0f} bps"
        except Exception: return "—"

    parts_headline = []
    parts_risk   = []   # what moved
    parts_alpha  = []   # what pulled
    parts_insight = []  # non-obvious read
    parts_watch  = []   # attention bullets

    # ── Data summary numbers ──────────────────────────────────────────────────
    house_bvar_total = sum(r["bvar_brl"] for r in house_rows) if house_rows else 0
    house_var_total  = sum(r["var_brl"]  for r in house_rows) if house_rows else 0
    house_nav_total  = sum(r["nav"]      for r in house_rows) if house_rows else 0

    # Δ VaR D-1 per fund (bps of NAV)
    dvar_list = []  # (short, dvar_bps)
    for r in house_rows:
        td = td_by_short.get(r["short"])
        s = series_map.get(td) if td else None
        if s is None or s.empty:
            continue
        s_avail = s[s["VAL_DATE"] <= DATA]
        if len(s_avail) < 2:
            continue
        v_today = abs(float(s_avail.iloc[-1]["var_pct"]))
        v_prev  = abs(float(s_avail.iloc[-2]["var_pct"]))
        dvar_bps = (v_today - v_prev) * 100
        dvar_list.append((r["short"], dvar_bps, r["label"], v_today))
    dvar_list.sort(key=lambda x: abs(x[1]), reverse=True)

    # Top util VaR — use primary metric: BVaR for IDKAs (bvar-benchmarked),
    # absolute VaR for CDI-benchmarked funds. Matches the mandate soft limit.
    utils = []
    for r in house_rows:
        td = td_by_short.get(r["short"])
        cfg = ALL_FUNDS.get(td, {}) if td else {}
        if cfg.get("informative"):
            continue
        soft = cfg.get("var_soft", 99)
        v_pct = r["bvar_pct"] if cfg.get("primary") == "bvar" else r["var_pct"]
        util = v_pct / soft * 100 if soft else 0
        utils.append((r["short"], r["label"], util, v_pct, soft))
    utils.sort(key=lambda x: x[2], reverse=True)

    # ── HEADLINE ──────────────────────────────────────────────────────────────
    headline = None
    # Rule 0 (highest priority): any single PA contribution ≥ 1% (100 bps) do NAV.
    # Benchmark-tracking livros (IDKA CI) excluded — they move with the index, not alpha.
    if df_pa is not None and not df_pa.empty:
        _pa_alpha = _pa_filter_alpha(df_pa)
        _pa_hit = _pa_alpha[
            ~_pa_alpha["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}) &
            ~_pa_alpha["CLASSE"].isin({"Caixa", "Custos"}) &
            (_pa_alpha["dia_bps"].abs() >= 100.0)
        ].sort_values("dia_bps", key=lambda s: s.abs(), ascending=False)
        if not _pa_hit.empty:
            _r0 = _pa_hit.iloc[0]
            _PA_TO_SHORT = {v: k for k, v in _FUND_PA_KEY.items()}
            _fund_lbl = FUND_LABELS.get(_PA_TO_SHORT.get(_r0["FUNDO"], _r0["FUNDO"]), _r0["FUNDO"])
            _bps0 = float(_r0["dia_bps"])
            _col = "var(--up)" if _bps0 > 0 else "var(--down)"
            _icon = "🟢" if _bps0 > 0 else "🚨"
            _extra = ""
            if len(_pa_hit) > 1:
                _extra = f' · mais {len(_pa_hit)-1} contribuição(ões) ≥ 1% em outros produtos'
            headline = (
                f'{_icon} <b>{_r0["PRODUCT"]}</b> ({_fund_lbl}) contribuiu '
                f'<span class="mono" style="color:{_col}">{_bps0/100:+.2f}%</span> do NAV hoje{_extra}.'
            )
    # Rule 1: any fund ≥85% util VaR → flag
    if not headline and utils and utils[0][2] >= 85:
        s, lbl, u, v, soft = utils[0]
        headline = f'🔴 <b>{lbl}</b> em <span class="mono">{u:.0f}%</span> do soft limit VaR (<span class="mono">{v:.2f}%</span> vs soft {soft:.2f}%).'
    # Rule 2: Δ VaR material
    if not headline and dvar_list and abs(dvar_list[0][1]) >= 10:
        s, dv, lbl, v_today = dvar_list[0]
        direction = "subiu" if dv > 0 else "caiu"
        headline = f'<b>{lbl}</b> VaR {direction} <span class="mono">{abs(dv):.0f} bps</span> vs D-1 (agora <span class="mono">{v_today:.2f}%</span>).'
    # Rule 3: concentration — top fund absorbs >40% of house BVaR
    if not headline and house_rows and house_bvar_total > 0:
        house_rows_sorted = sorted(house_rows, key=lambda r: r["bvar_brl"], reverse=True)
        top = house_rows_sorted[0]
        pct = top["bvar_brl"] / house_bvar_total * 100
        if pct >= 40:
            headline = f'<b>{top["label"]}</b> absorve <span class="mono">{pct:.0f}%</span> do risco ativo da casa (<span class="mono">{_mm(top["bvar_brl"])}</span> de <span class="mono">{_mm(house_bvar_total)}</span> BVaR).'
    if not headline:
        headline = "Dia sem mudanças materiais — carteira em regime normal. Monitorar rotação gradual."

    # ── RISCO · O QUE MUDOU ───────────────────────────────────────────────────
    for s, dv, lbl, v_today in dvar_list[:3]:
        if abs(dv) < 5:  # threshold for materiality
            continue
        direction = "↑" if dv > 0 else "↓"
        parts_risk.append(
            f'<li><span class="mono">{direction} {abs(dv):.0f} bps</span> · '
            f'<b>{lbl}</b> VaR agora {v_today:.2f}%</li>'
        )
    # Top 2 position_changes (fund-agnostic)
    if position_changes:
        all_changes = []
        for short, df_ch in (position_changes or {}).items():
            if df_ch is None or df_ch.empty:
                continue
            for _, r in df_ch.head(5).iterrows():
                all_changes.append((short, r))
        # Just flag if there's anything with |Δ| ≥ 5 pp
        big_changes = [
            (short, r) for short, r in all_changes
            if abs(float(r.get("delta_pp", 0))) >= 5
        ]
        big_changes.sort(key=lambda x: abs(float(x[1].get("delta_pp", 0))), reverse=True)
        for short, r in big_changes[:2]:
            lbl = FUND_LABELS.get(short, short)
            factor = r.get("rf", r.get("CLASSE", ""))
            dpp = float(r.get("delta_pp", 0))
            direction = "↑" if dpp > 0 else "↓"
            parts_risk.append(
                f'<li><span class="mono">{direction} {abs(dpp):.1f} pp</span> · '
                f'<b>{lbl}</b> mudou exposição em <i>{factor}</i></li>'
            )

    # ── ALPHA · O QUE PUXOU ────────────────────────────────────────────────────
    # Cross-fund top contributors / detractors in BRL (dia_bps × NAV / 10000)
    if df_pa is not None and not df_pa.empty:
        _pa = df_pa.copy()
        _pa = _pa[~_pa["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos", "Prev"})]
        _pa = _pa[~_pa["CLASSE"].isin({"Caixa", "Custos"})]
        # Convert to BRL via fund NAV
        nav_by_pa = {}
        _PA_TO_SHORT = {v: k for k, v in _FUND_PA_KEY.items()}
        for r in house_rows:
            short = r["short"]
            pa_key = _FUND_PA_KEY.get(short)
            if pa_key:
                nav_by_pa[pa_key] = r["nav"]
        _pa["nav"] = _pa["FUNDO"].map(nav_by_pa).fillna(0)
        _pa["brl_dia"] = _pa["dia_bps"] * _pa["nav"] / 10000
        _pa = _pa[_pa["brl_dia"].abs() > 50_000]
        contribs = _pa.sort_values("brl_dia", ascending=False).head(2)
        detract  = _pa.sort_values("brl_dia").head(2)
        for _, r in contribs.iterrows():
            fund_lbl = FUND_LABELS.get(_PA_TO_SHORT.get(r["FUNDO"], r["FUNDO"]), r["FUNDO"])
            parts_alpha.append(
                f'<li><span class="mono up">{r["dia_bps"]:+.1f} bps</span> · '
                f'<b>{r["PRODUCT"]}</b> ({fund_lbl})</li>'
            )
        for _, r in detract.iterrows():
            fund_lbl = FUND_LABELS.get(_PA_TO_SHORT.get(r["FUNDO"], r["FUNDO"]), r["FUNDO"])
            parts_alpha.append(
                f'<li><span class="mono down">{r["dia_bps"]:+.1f} bps</span> · '
                f'<b>{r["PRODUCT"]}</b> ({fund_lbl})</li>'
            )

    # ── LEITURA DO DIA (non-obvious insights) ─────────────────────────────────
    # (a) Top-3 concentration of house risk
    if agg_rows:
        from collections import defaultdict
        inst_totals = defaultdict(float)
        for r in agg_rows:
            key = (r["factor"], r["product"])
            inst_totals[key] += r["brl"]
        sorted_inst = sorted(inst_totals.items(), key=lambda x: abs(x[1]), reverse=True)
        top3 = sorted_inst[:3]
        if top3:
            labels = " + ".join(f'<b>{k[1]}</b>' for k, _ in top3)
            top3_sum = sum(abs(v) for _, v in top3)
            # Rough share of house — compare against sum of |all| for that unit category
            same_unit = [abs(v) for (f, _), v in inst_totals.items()
                         if agg_rows[0]["unit"] == next((r["unit"] for r in agg_rows if r["factor"] == f), None)]
            parts_insight.append(
                f'<li><b>Concentração:</b> {labels} somam '
                f'<span class="mono">{_mm(top3_sum)}</span> de exposição — '
                f'as 3 maiores posições da casa.</li>'
            )

    # (b) Dominant factor (largest |net| across house)
    if factor_matrix and bench_matrix:
        net_by_factor = {}
        for fk, allocs in factor_matrix.items():
            benches = bench_matrix.get(fk, {})
            total_net = sum(
                allocs.get(s, 0.0) - benches.get(s, 0.0)
                for s in set(allocs) | set(benches)
            )
            if abs(total_net) > 1_000:
                net_by_factor[fk] = total_net
        if net_by_factor:
            dom = max(net_by_factor.items(), key=lambda x: abs(x[1]))
            # Para fatores de juros use terminologia "tomado"/"dado" (convenção DV01):
            #   tomado = DV01 > 0 (short bond / short DI1F; ganha com alta de juros) → vermelho
            #   dado   = DV01 < 0 (long bond / long DI1F; ex: NTN-B comprado)        → verde
            # factor_matrix já está na convenção DV01 (raw DELTA para MACRO df_expo;
            # rf_expo_maps é negado ao popular factor_matrix para cancelar o flip).
            is_rate = dom[0] in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
            if is_rate:
                direction = "tomado" if dom[1] > 0 else "dado"
                dir_color = "var(--down)" if dom[1] > 0 else "var(--up)"
            else:
                direction = "longo" if dom[1] > 0 else "curto"
                dir_color = "var(--up)"   if dom[1] > 0 else "var(--down)"
            parts_insight.append(
                f'<li><b>Fator dominante:</b> casa está '
                f'<b class="mono" style="color:{dir_color} !important; font-weight:700">{direction}</b> em '
                f'<b>{dom[0]}</b> · <span class="mono">{_mm(dom[1])}</span> líquido do bench.</li>'
            )

    # (c) Vol regime shift
    if vol_regime_map:
        stressed = [(k, v) for k, v in vol_regime_map.items() if v and v.get("regime") == "stressed"]
        elevated = [(k, v) for k, v in vol_regime_map.items() if v and v.get("regime") == "elevated"]
        if stressed:
            names = ", ".join(k for k, _ in stressed)
            parts_insight.append(
                f'<li><b>Vol regime stressed:</b> {names} — pct rank ≥ 90. Histórico sugere cautela em tamanho.</li>'
            )
        elif elevated and len(elevated) >= 2:
            names = ", ".join(k for k, _ in elevated)
            parts_insight.append(
                f'<li><b>Vol regime elevated:</b> {names} acima do p70 — vigilância gradual.</li>'
            )

    # ── ATENÇÃO (max 3 bullets) ────────────────────────────────────────────────
    for s, lbl, u, v, soft in utils[:2]:
        if u >= 70:
            parts_watch.append(
                f'<li><b>{lbl}</b> util VaR em <span class="mono">{u:.0f}%</span> do soft — revisar tamanho se subir mais.</li>'
            )
    # MACRO stop flag
    if pm_margem:
        for pm, margem in pm_margem.items():
            if margem <= 20:  # low margin = red zone
                parts_watch.append(
                    f'<li><b>MACRO · {pm}</b> margem de stop em <span class="mono">{margem:.0f} bps</span> — espaço curto.</li>'
                )
                break
    parts_watch = parts_watch[:3]

    # ── Render ────────────────────────────────────────────────────────────────
    def _section(title, items, empty_msg=None):
        if not items:
            return f'<p style="color:var(--muted); font-style:italic; margin-top:8px">{empty_msg or "— nada material hoje"}</p>' if empty_msg else ""
        return (
            f'<div class="brief-section-title">{title}</div>'
            f'<ul class="brief-list">{"".join(items)}</ul>'
        )

    # Benchmarks para context
    bench_line = ""
    if ibov and cdi:
        def _fmt(v):
            pct = v / 100 if v else 0
            col = "up" if pct >= 0 else "down"
            return f'<span class="mono {col}">{pct:+.2f}%</span>'
        bench_line = (
            f'<div class="brief-benchmarks">Benchmarks hoje: '
            f'IBOV {_fmt(ibov.get("dia"))} · CDI {_fmt(cdi.get("dia"))}</div>'
        )

    return f"""
    <section class="card brief-card">
      <div class="card-head">
        <span class="card-title">Briefing Executivo</span>
        <span class="card-sub">— {DATA_STR} · 3–5 min · visão curada sobre risco e delta</span>
      </div>
      <div class="brief-headline">{headline}</div>
      {bench_line}
      <div class="brief-grid">
        <div class="brief-col">
          {_section("Risco · o que mudou", parts_risk, "— VaR estável em todos os fundos vs D-1")}
          {_section("Alpha · o que puxou", parts_alpha, "— PnL do dia concentrado em ruído (nenhum contribuinte ≥ R$50k)")}
        </div>
        <div class="brief-col">
          {_section("Leitura do dia", parts_insight)}
          {_section("Atenção", parts_watch, "— sem red flags; nenhuma util ≥ 70%")}
        </div>
      </div>
      <div class="brief-annex">
        → Detalhe em: <a href="#" onclick="document.querySelector('.summary-table')?.scrollIntoView({{behavior:'smooth'}});return false">Status consolidado</a> ·
        <a href="#" onclick="document.querySelector('[class*=rf-brl-body]')?.closest('.card')?.scrollIntoView({{behavior:'smooth'}});return false">Breakdown por Fator</a> ·
        <a href="#" onclick="Array.from(document.querySelectorAll('.card-title')).find(function(x){{return x.textContent.indexOf('Top Posi')===0}})?.closest('.card')?.scrollIntoView({{behavior:'smooth'}});return false">Top Posições</a>
      </div>
      <div class="brief-footnote" style="margin-top:10px; padding-top:8px; border-top:1px solid var(--line); font-size:10.5px; color:var(--muted); line-height:1.5">
        <b>Convenção de juros:</b>
        <span style="color:var(--up);font-weight:700">dado</span> = DV01 &lt; 0 (long bond · ex: NTN-B comprado, long DI1F) ·
        <span style="color:var(--down);font-weight:700">tomado</span> = DV01 &gt; 0 (short bond · ex: DI1F vendido).
      </div>
    </section>"""


def build_html(series_map: dict, stop_hist: dict = None, df_today=None,
               df_expo=None, df_var=None, macro_aum=None,
               df_expo_d1=None, df_var_d1=None,
               df_pnl_prod=None, pm_margem=None,
               df_quant_sn=None, quant_nav=None, quant_legs=None,
               df_evo_sn=None, evo_nav=None, evo_legs=None,
               df_evo_direct=None,
               df_pa=None, cdi=None, ibov=None, df_pa_daily=None,
               idka_idx_ret=None, walb=None,
               df_alb_expo=None, alb_nav=None,
               df_quant_expo=None, quant_expo_nav=None,
               df_quant_expo_d1=None,
               df_quant_var=None, df_quant_var_d1=None,
               df_evo_expo=None, evo_expo_nav=None,
               df_evo_expo_d1=None,
               df_evo_var=None, df_evo_var_d1=None,
               df_evo_pnl_prod=None,
               rf_expo_maps=None,
               df_frontier=None, frontier_bvar=None,
               df_frontier_ibov=None, df_frontier_smll=None, df_frontier_sectors=None,
               position_changes=None,
               dist_map=None, dist_map_prev=None, dist_actuals=None,
               vol_regime_map=None,
               pm_book_var=None,
               expo_date_label=None, data_manifest=None) -> str:
    alerts = []
    td_by_short = {cfg["short"]: td for td, cfg in ALL_FUNDS.items()}
    sections = []  # list of (fund_short, report_id, html)
    evolution_c4_state = {}  # Camada 4 state — populated when EVOLUTION diversificação renders

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

        _is_idka = cfg.get("primary") == "bvar"
        _is_inf  = cfg.get("informative") is True
        if not _is_inf and var_range_pct >= ALERT_THRESHOLD:
            alerts.append((short, "BVaR" if _is_idka else "VaR", var_range_pct, var_today, var_util, ALERT_COMMENTS.get(("var", short), "")))
        if not (_is_idka or _is_inf) and stress_range_pct >= ALERT_THRESHOLD:
            alerts.append((short, "Stress", stress_range_pct, stress_today, str_util, ALERT_COMMENTS.get(("stress", short), "")))

        var_bar    = range_bar_svg(var_abs,  var_min_abs,  var_max_abs,  cfg["var_soft"],    cfg["var_hard"])
        stress_bar = range_bar_svg(str_abs,  str_min_abs,  str_max_abs,  cfg["stress_soft"], cfg["stress_hard"])

        spark_var    = make_sparkline(s.set_index("VAL_DATE")["var_pct"],    "#1a8fd1")
        spark_stress = make_sparkline(s.set_index("VAL_DATE")["stress_pct"], "#f472b6")

        # IDKA: primary metric is BVaR (relative to benchmark); secondary is absolute VaR
        # shown for reference only (no util %, no hard limit highlighted).
        # Frontier: LO equity — no limits, both VaR and Stress shown as informative.
        is_idka = cfg.get("primary") == "bvar"
        is_informative = cfg.get("informative") is True
        if is_idka:
            primary_label   = "BVaR 95%"
            secondary_label = "VaR 95% (ref)"
            secondary_util_html = (
                f'<td class="util-cell mono" style="color:var(--muted)">ref</td>'
            )
        elif is_informative:
            primary_label   = "VaR 95% (ref)"
            secondary_label = "Stress (ref)"
            secondary_util_html = (
                f'<td class="util-cell mono" style="color:var(--muted)">ref</td>'
            )
        else:
            primary_label   = "VaR 95% 1d"
            secondary_label = "Stress"
            secondary_util_html = (
                f'<td class="util-cell mono" style="color:{util_color(str_util)}">{str_util:.0f}% soft</td>'
            )

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
                <td class="metric-name">{primary_label}</td>
                <td class="value-cell mono" style="color:{'var(--muted)' if is_informative else util_color(var_util)}">{var_today:.2f}%</td>
                <td class="bar-cell">{'' if is_informative else var_bar}</td>
                <td class="util-cell mono" style="color:{'var(--muted)' if is_informative else util_color(var_util)}">{'ref' if is_informative else f'{var_util:.0f}% soft'}</td>
                <td class="spark-cell"><img src="data:image/png;base64,{spark_var}" height="38"/></td>
              </tr>
              <tr class="metric-row">
                <td class="metric-name">{secondary_label}</td>
                <td class="value-cell mono" style="color:{'var(--muted)' if (is_idka or is_informative) else util_color(str_util)}">{stress_today:.2f}%</td>
                <td class="bar-cell">{'' if (is_idka or is_informative) else stress_bar}</td>
                {secondary_util_html}
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

        # Diversificação — só para EVOLUTION, imediatamente após Risk Monitor
        if short == "EVOLUTION":
            try:
                div_html, _c4_state = build_evolution_diversification_section(DATA_STR)
                sections.append(("EVOLUTION", "diversification", div_html))
                # stash C4 state on the local closure for Summary to pick up
                evolution_c4_state = _c4_state
            except Exception as _e:
                sections.append(("EVOLUTION", "diversification",
                    f"<section class='card'><div class='card-head'>"
                    f"<span class='card-title'>Diversificação</span></div>"
                    f"<div style='color:var(--muted);padding:12px'>"
                    f"Falha ao montar: {_e}</div></section>"))
                evolution_c4_state = {}

    # Build alerts section
    # ── PA contribution alerts — filter by |dia_bps|, sorted by contribution ──
    PA_ALERT_MIN_BPS   = 5.0   # minimum absolute contribution to show
    PA_ALERT_HIGH_BPS  = 15.0  # threshold for red (large) vs yellow (medium)
    _PA_EXCL_LIVROS    = {"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}
    _PA_EXCL_CLASSES   = {"Caixa", "Custos"}

    pa_alert_items_size = ""
    pa_alert_items_fund = ""
    if df_pa is not None and not df_pa.empty:
        try:
            _pa_filt = df_pa[
                ~df_pa["LIVRO"].isin(_PA_EXCL_LIVROS) &
                ~df_pa["CLASSE"].isin(_PA_EXCL_CLASSES) &
                (df_pa["dia_bps"].abs() >= PA_ALERT_MIN_BPS)
            ].copy()

            _zscore_map = {}
            if df_pa_daily is not None and not df_pa_daily.empty:
                _today = pd.Timestamp(DATA_STR)
                _hist  = df_pa_daily[df_pa_daily["DATE"] < _today]
                _sigma = (_hist.groupby(["FUNDO","LIVRO","PRODUCT"])["dia_bps"]
                               .std().reset_index().rename(columns={"dia_bps":"sigma"}))
                for r in _sigma.itertuples(index=False):
                    _zscore_map[(r.FUNDO, r.LIVRO, r.PRODUCT)] = float(r.sigma) if r.sigma > 0 else None

            max_abs = _pa_filt["dia_bps"].abs().max() if not _pa_filt.empty else 1.0
            _fund_order_idx = {k: i for i, k in enumerate(FUND_ORDER)}

            def _card_html(r):
                bps = float(r.dia_bps); abs_bps = abs(bps)
                # ≥1% alert excludes benchmark-tracking livros (IDKA CI replica)
                _is_bench = r.LIVRO in _PA_BENCH_LIVROS.get(r.FUNDO, set())
                big = (abs_bps >= 100.0) and (not _is_bench)
                color      = "var(--up)" if bps > 0 else "var(--down)"
                bg_color   = "#0f2d1a" if bps > 0 else "#2d0f0f"
                # Red border + thicker line on ≥ 1% hits regardless of direction (size is the risk signal)
                border_col = "#dc2626" if big else ("#22c55e" if bps > 0 else "#f87171")
                border_w   = "2px" if big else "1px"
                livro_disp = _pa_render_name(r.LIVRO)
                fund_disp  = FUND_LABELS.get(r.FUNDO, r.FUNDO)
                sigma = _zscore_map.get((r.FUNDO, r.LIVRO, r.PRODUCT))
                z_txt = f"z = {bps/sigma:+.1f}σ" if sigma else ""
                bar_pct = min(abs_bps / max_abs * 100, 100)
                bar_color = "#22c55e" if bps > 0 else "#f87171"
                flag = '🚨 ' if big else ''
                big_tag = (
                    ' <span style="font-size:10px;color:#dc2626;font-weight:700;'
                    'border:1px solid #dc2626;border-radius:3px;padding:1px 5px;'
                    'margin-left:6px;letter-spacing:0.5px">≥ 1% NAV</span>'
                    if big else ''
                )
                return f"""
                <div style="border:{border_w} solid {border_col};border-radius:6px;padding:12px 16px;
                            background:{bg_color};display:flex;align-items:center;gap:16px">
                  <div style="flex:0 0 auto;text-align:right;min-width:80px">
                    <div style="font-size:28px;font-weight:700;color:{color};
                                font-variant-numeric:tabular-nums;line-height:1">{bps:+.1f}</div>
                    <div style="font-size:10px;color:var(--muted);margin-top:2px">bps</div>
                  </div>
                  <div style="flex:1;min-width:0">
                    <div style="font-size:15px;font-weight:700;color:var(--text);
                                white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{flag}{r.PRODUCT}{big_tag}</div>
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

            # Sort by |size| desc (current behavior)
            _by_size = _pa_filt.sort_values("dia_bps", key=abs, ascending=False)
            for r in _by_size.itertuples(index=False):
                pa_alert_items_size += _card_html(r)

            # Sort by fund (canonical FUND_ORDER), then |size| desc within fund
            _pa_filt["_fund_ord"] = _pa_filt["FUNDO"].map(
                {pa_key: _fund_order_idx.get(short, 99)
                 for short, pa_key in _FUND_PA_KEY.items()}
            ).fillna(99)
            _pa_filt["_abs"] = _pa_filt["dia_bps"].abs()
            _by_fund = _pa_filt.sort_values(["_fund_ord", "_abs"], ascending=[True, False])
            for r in _by_fund.itertuples(index=False):
                pa_alert_items_fund += _card_html(r)
        except Exception as e:
            print(f"  PA alerts failed ({e})")

    pa_alert_items = pa_alert_items_size  # keep existing consumers happy

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
        if pa_alert_items:
            _top_margin = "4px" if risk_items else "0"
            pa_grid = (
                f'<div class="pa-alert-view" data-pa-sort="size" style="margin-top:{_top_margin}">{pa_alert_items_size}</div>'
                f'<div class="pa-alert-view pa-alert-view-hidden" data-pa-sort="fund" style="margin-top:{_top_margin}">{pa_alert_items_fund}</div>'
            )
        else:
            pa_grid = ""
        pa_header = (
            f'<div style="display:flex;align-items:center;gap:12px;margin-top:{"16px" if risk_items else "0"};margin-bottom:8px">'
            f'<span style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">PA — Contribuições do dia (|contrib| ≥ {PA_ALERT_MIN_BPS:.0f} bps)</span>'
            f'<div class="pa-view-toggle pa-alert-toggle" style="margin-left:auto">'
            f'<button class="pa-tgl active" data-pa-sort="size" onclick="selectPaAlertSort(this,\'size\')">Por Tamanho</button>'
            f'<button class="pa-tgl"        data-pa-sort="fund" onclick="selectPaAlertSort(this,\'fund\')">Por Fundo</button>'
            f'</div></div>'
        ) if pa_alert_items else ""
        alerts_html = f"""
        <div class="alerts-section">
          <div class="alerts-header">Análise{' — Métricas acima do 80° percentil histórico' if risk_items else ''}</div>
          {risk_items}
          {pa_header}
          {pa_grid}
        </div>"""

    # MACRO-specific sections
    # Risk Budget tab = stop monitor (PnL × carry) + Budget vs VaR card combined
    # into a single section wrapper (avoids duplicate DOM ids for the same tab).
    if stop_hist and df_today is not None:
        _stop_html = build_stop_section(stop_hist, df_today)
        _bvv_html  = ""
        # Derive hybrid HS-based VaR per PM from today's dist_map (same source as
        # Distribuição 252d card: PORTIFOLIO_DAILY_HISTORICAL_SIMULATION).
        _pm_hs_var = compute_pm_hs_var(dist_map) if dist_map else {}
        if _pm_hs_var or pm_book_var:
            _bvv_html = build_pm_budget_vs_var_section(
                pm_book_var or {}, _pm_hs_var, pm_margem or {}
            ) or ""
        sections.append(("MACRO", "stop-monitor", _stop_html + _bvv_html))
    if df_expo is not None:
        _expo_html = build_exposure_section(df_expo, df_var, macro_aum, df_expo_d1, df_var_d1, df_pnl_prod, pm_margem)
        if expo_date_label:
            _stale_banner = (f'<div style="background:#7c2d12;color:#fca5a5;font-size:11px;padding:4px 12px;'
                             f'border-radius:4px;margin-bottom:6px">⚠ Dados de exposição indisponíveis para '
                             f'{DATA_STR} — exibindo {expo_date_label}</div>')
            _expo_html = _stale_banner + _expo_html
        sections.append(("MACRO", "exposure", _expo_html))

    # Distribution 252d sections (per fund) — combined card with Backward/Forward toggle.
    # Engine HS source (PORTIFOLIO_DAILY_HISTORICAL_SIMULATION): MACRO, QUANT, EVOLUTION.
    # FRONTIER: realized α vs IBOV (LONG_ONLY_MAINBOARD).
    # ALBATROZ/MACRO_Q/IDKA_3Y/IDKA_10Y sem série — parkeado em CLAUDE.md.
    if (dist_map or dist_map_prev) and dist_actuals is not None:
        for fs in ["MACRO", "EVOLUTION", "QUANT", "FRONTIER"]:
            html_sect = build_distribution_card(
                fs, dist_map or {}, dist_map_prev or {}, dist_actuals,
            )
            if html_sect:
                sections.append((fs, "distribution", html_sect))

    # Vol Regime sections (per fund) — standalone card using HS W series
    if vol_regime_map:
        for fs in ("MACRO", "EVOLUTION", "QUANT"):
            html_vr = build_vol_regime_section(fs, vol_regime_map)
            if html_vr:
                sections.append((fs, "vol-regime", html_vr))

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
        # Albatroz PA sum per window (for the IDKA bench decomposition)
        alb_pa_rows = df_pa[df_pa["FUNDO"] == "ALBATROZ"]
        albatroz_pa_sum = {
            "dia": float(alb_pa_rows["dia_bps"].sum()) if not alb_pa_rows.empty else 0.0,
            "mtd": float(alb_pa_rows["mtd_bps"].sum()) if not alb_pa_rows.empty else 0.0,
            "ytd": float(alb_pa_rows["ytd_bps"].sum()) if not alb_pa_rows.empty else 0.0,
            "m12": float(alb_pa_rows["m12_bps"].sum()) if not alb_pa_rows.empty else 0.0,
        }
        for short in FUND_ORDER:
            # FRONTIER PA is rendered via build_frontier_lo_section (Long Only card)
            # — it occupies the "performance" slot for FRONTIER. The GFA rows in
            # df_pa still feed Top Movers / Outliers / Mudanças Materiais.
            if short == "FRONTIER":
                continue
            idx_ret = (idka_idx_ret or {}).get(short)
            w_alb   = (walb or {}).get(short)
            pa_html = build_pa_section_hier(
                short, df_pa, cdi_row,
                idka_index_ret=idx_ret, w_alb=w_alb,
                albatroz_pa_sum=albatroz_pa_sum, ibov=ibov,
            )
            if pa_html:
                sections.append((short, "performance", pa_html))

    # ALBATROZ — RF exposure card (under the "exposure" report tab)
    if df_alb_expo is not None and not df_alb_expo.empty and alb_nav:
        alb_html = build_albatroz_exposure(df_alb_expo, alb_nav)
        if alb_html:
            sections.append(("ALBATROZ", "exposure", alb_html))

    # QUANT — exposure card (by factor + by livro × factor)
    if df_quant_expo is not None and not df_quant_expo.empty and quant_expo_nav:
        q_expo_html = build_quant_exposure_section(
            df_quant_expo, quant_expo_nav,
            df_d1=df_quant_expo_d1,
            df_var=df_quant_var,
            df_var_d1=df_quant_var_d1,
        )
        if q_expo_html:
            sections.append(("QUANT", "exposure", q_expo_html))

    # EVOLUTION — exposure card (Strategy → LIVRO → Instrumento + Por Fator)
    if df_evo_expo is not None and not df_evo_expo.empty and evo_expo_nav:
        e_expo_html = build_evolution_exposure_section(
            df_evo_expo, evo_expo_nav,
            df_d1=df_evo_expo_d1,
            df_var=df_evo_var,
            df_var_d1=df_evo_var_d1,
            df_pnl_prod=df_evo_pnl_prod,
        )
        if e_expo_html:
            sections.append(("EVOLUTION", "exposure", e_expo_html))

    # RF Exposure Map (IDKA 3Y, IDKA 10Y, Albatroz, MACRO, EVOLUTION)
    # MACRO/EVOLUTION use bench_dur=0 ("—" label) since they have no fixed-duration mandate.
    _RF_MAP_CFG = {
        "IDKA_3Y":   {"desk": "IDKA IPCA 3Y FIRF",              "bench_dur": 3.0,  "bench_label": "IDKA IPCA 3A"},
        "IDKA_10Y":  {"desk": "IDKA IPCA 10Y FIRF",             "bench_dur": 10.0, "bench_label": "IDKA IPCA 10A"},
        "ALBATROZ":  {"desk": "GALAPAGOS ALBATROZ FIRF LP",     "bench_dur": 0.0,  "bench_label": "CDI"},
        "MACRO":     {"desk": "Galapagos Macro FIM",            "bench_dur": 0.0,  "bench_label": "—"},
        "EVOLUTION": {"desk": "Galapagos Evolution FIC FIM CP", "bench_dur": 0.0,  "bench_label": "—"},
    }
    if rf_expo_maps:
        for short_k, cfg_k in _RF_MAP_CFG.items():
            df_k = rf_expo_maps.get(short_k)
            if df_k is None or df_k.empty:
                continue
            nav_k = _latest_nav(cfg_k["desk"], DATA_STR)
            if not nav_k:
                continue
            html_k = build_rf_exposure_map_section(
                short_k, df_k, nav_k, cfg_k["bench_dur"], cfg_k["bench_label"],
            )
            if html_k:
                sections.append((short_k, "exposure-map", html_k))

    # ALBATROZ — Risk Budget (150 bps/month stop)
    if df_pa is not None and not df_pa.empty:
        rb_html = build_albatroz_risk_budget(df_pa)
        if rb_html:
            sections.append(("ALBATROZ", "stop-monitor", rb_html))

    # FRONTIER — Performance Attribution lives in the PA tab (no separate LO tab);
    # Frontier is not in REPORT_ALPHA_ATRIBUTION, so build_pa_section_hier renders
    # nothing for it — this section fills the "performance" slot for Frontier.
    if df_frontier is not None and not df_frontier.empty:
        # Build the hierarchical PA (from GFA in df_pa) to embed as a sub-tab inside the LO card.
        cdi_row_fr = cdi or {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
        pa_hier_html = build_pa_section_hier(
            "FRONTIER", df_pa, cdi_row_fr, ibov=ibov,
        ) if (df_pa is not None and not df_pa.empty) else ""
        frontier_html = build_frontier_lo_section(
            df_frontier, DATA_STR,
            df_sectors=df_frontier_sectors,
            pa_hier_html=pa_hier_html,
        )
        if frontier_html:
            sections.append(("FRONTIER", "performance", frontier_html))
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

    def _an_movers_frontier():
        """Top movers for FRONTIER — por papel e por setor, já líquido do bench
           (usa TOTAL_IBVSP_DAY = excess return vs IBOV).
           Retorna (papel_body, setor_body) — strings HTML, ou (None, None)."""
        if df_frontier is None or df_frontier.empty:
            return None, None
        sub = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])].copy()
        sub = sub[sub["TOTAL_IBVSP_DAY"].notna()]
        if sub.empty:
            return None, None
        sub["alpha_pct"] = sub["TOTAL_IBVSP_DAY"].astype(float) * 100.0  # fração → %

        _THR = 0.01  # 1 bp: ignora ruído mínimo

        def _fmt_rows(df_in, col_name, color):
            if df_in is None or df_in.empty:
                return '<li class="comment-empty">— nada material</li>'
            out = ""
            for _, r in df_in.iterrows():
                v = float(r["alpha_pct"])
                out += (
                    '<li>'
                    f'<span class="mono" style="color:{color}; font-weight:600">{r[col_name]}</span> '
                    f'<span class="mono">{"+" if v >= 0 else ""}{v:.2f}%</span>'
                    '</li>'
                )
            return out

        def _split_html(pos_items, neg_items, col_name):
            return f"""
            <div class="mov-split">
              <div>
                <div class="mov-col-title">Contribuintes (α vs IBOV)</div>
                <ul class="comment-list">{_fmt_rows(pos_items, col_name, "var(--up)")}</ul>
              </div>
              <div>
                <div class="mov-col-title">Detratores (α vs IBOV)</div>
                <ul class="comment-list">{_fmt_rows(neg_items, col_name, "var(--down)")}</ul>
              </div>
            </div>"""

        # Por Papel
        papel_pos = sub[sub["alpha_pct"] >=  _THR].nlargest(5, "alpha_pct")
        papel_neg = sub[sub["alpha_pct"] <= -_THR].nsmallest(5, "alpha_pct")
        papel_body = _split_html(papel_pos, papel_neg, "PRODUCT")

        # Por Setor — join com sectors table
        setor_body = ""
        if df_frontier_sectors is not None and not df_frontier_sectors.empty:
            merged = sub.merge(
                df_frontier_sectors[["TICKER", "GLPG_SECTOR"]],
                left_on="PRODUCT", right_on="TICKER", how="left",
            )
            merged["GLPG_SECTOR"] = merged["GLPG_SECTOR"].fillna("—")
            by = merged.groupby("GLPG_SECTOR", as_index=False)["alpha_pct"].sum()
            by = by[by["alpha_pct"].abs() >= _THR]
            setor_pos = by[by["alpha_pct"] > 0].sort_values("alpha_pct", ascending=False).head(5)
            setor_neg = by[by["alpha_pct"] < 0].sort_values("alpha_pct").head(5)
            setor_body = _split_html(setor_pos, setor_neg, "GLPG_SECTOR")

        return papel_body, setor_body

    def _an_movers_card(short):
        # Frontier é Long Only — não tem PA com LIVRO/CLASSE. Usa alpha vs IBOV
        # direto do Long Only Mainboard, agrupado por papel ou por setor.
        if short == "FRONTIER":
            papel_body, setor_body = _an_movers_frontier()
            if not papel_body and not setor_body:
                return ""
            if setor_body:
                toggle = """
                <div class="pa-view-toggle sum-tgl">
                  <button class="pa-tgl active" data-mov-view="papel"
                          onclick="selectMoversView(this,'papel')">Por Papel</button>
                  <button class="pa-tgl" data-mov-view="setor"
                          onclick="selectMoversView(this,'setor')">Por Setor</button>
                </div>"""
                setor_div = f'<div class="mov-view" data-mov-view="setor" style="display:none">{setor_body}</div>'
            else:
                toggle = ""
                setor_div = ""
            return f"""
            <section class="card sum-movers-card">
              <div class="card-head">
                <span class="card-title">Top Movers — DIA</span>
                <span class="card-sub">— Frontier · α vs IBOV (líquido do bench) · drop &lt; 1 bp</span>
                {toggle}
              </div>
              <div class="mov-view" data-mov-view="papel">{papel_body}</div>
              {setor_div}
            </section>"""

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
        elif short == "FRONTIER" and df_frontier is not None and not df_frontier.empty:
            # Frontier has no PA in REPORT_ALPHA_ATRIBUTION. Use excess return vs IBOV
            # (TOTAL_IBVSP_*) to stay apples-to-apples with the other funds' alpha vs CDI.
            # Aggregate from the TOTAL row only (per-stock rows sum ER differently vs benchmark weights).
            tot_row = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
            if not tot_row.empty:
                a_dia = float(tot_row["TOTAL_IBVSP_DAY"].iloc[0])   * 10000
                a_mtd = float(tot_row["TOTAL_IBVSP_MONTH"].iloc[0]) * 10000
                a_ytd = float(tot_row["TOTAL_IBVSP_YEAR"].iloc[0])  * 10000
            else:
                a_dia = a_mtd = a_ytd = 0.0
            a_m12 = 0.0  # no 12M column in mainboard
        else:
            a_dia = a_mtd = a_ytd = a_m12 = 0.0

        var_today = var_util = dvar = None
        if td and td in ALL_FUNDS and series_map and td in series_map:
            s = series_map[td]
            s_avail = s[s["VAL_DATE"] <= DATA]
            if not s_avail.empty:
                var_today = abs(s_avail.iloc[-1]["var_pct"])
                cfg = ALL_FUNDS[td]
                # Informative funds (e.g., Frontier LO) show VaR but without util/limit check
                if not cfg.get("informative"):
                    var_util = var_today / cfg["var_soft"] * 100
                prev = s_avail.iloc[:-1]
                if not prev.empty:
                    dvar = var_today - abs(prev.iloc[-1]["var_pct"])

        # Frontier: replace absolute VaR with 3y HS BVaR vs IBOV (current weights)
        if short == "FRONTIER" and frontier_bvar:
            var_today = float(frontier_bvar["bvar_pct"])
            dvar = None  # no D-1 series for HS BVaR yet

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
            + _sum_var_cell(var_today) + _sum_util_cell(var_util)
            + _sum_dvar_cell(dvar)
            + "</tr>"
        )

    # ── Benchmark reference rows (IBOV, CDI) ───────────────────────────────────
    def _bench_row(label: str, returns: dict | None) -> str:
        # data-pinned="1" → sortTableByCol keeps this row at the bottom of tbody.
        if not returns:
            empty = '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
            return (
                '<tr class="bench-row" data-pinned="1">'
                '<td class="sum-status"></td>'
                f'<td class="sum-fund" style="font-style:italic; color:var(--muted)">{label}</td>'
                + empty * 4
                + empty * 3
                + '</tr>'
            )
        return (
            '<tr class="bench-row" data-pinned="1">'
            '<td class="sum-status"></td>'
            f'<td class="sum-fund" style="font-style:italic; color:var(--muted)">{label}</td>'
            + _sum_bp_cell(returns["dia"]) + _sum_bp_cell(returns["mtd"])
            + _sum_bp_cell(returns["ytd"]) + _sum_bp_cell(returns["m12"])
            + '<td class="mono" style="color:var(--muted); text-align:right">—</td>' * 3
            + '</tr>'
        )

    idka3  = (idka_idx_ret or {}).get("IDKA_3Y")
    idka10 = (idka_idx_ret or {}).get("IDKA_10Y")
    bench_rows_html = (
        _bench_row("IBOV",     ibov)
      + _bench_row("CDI",      cdi)
      + _bench_row("IDKA 3A",  idka3)
      + _bench_row("IDKA 10A", idka10)
    )

    # ── House-wide risk consolidated ───────────────────────────────────────────
    # Absolute VaR (% NAV) comes from series_map. BVaR (benchmark-relative) from:
    #   IDKAs   → series_map[td]["var_pct"] (engine BVaR; stress_pct holds abs VaR)
    #   Frontier→ frontier_bvar["bvar_pct"] (3y HS vs IBOV)
    #   Others  → BVaR vs CDI ≈ abs VaR (CDI has effectively zero daily vol)
    _BENCH_BY_FUND = {
        "MACRO": "CDI", "QUANT": "CDI", "EVOLUTION": "CDI", "MACRO_Q": "CDI", "ALBATROZ": "CDI",
        "FRONTIER": "IBOV", "IDKA_3Y": "IDKA 3A", "IDKA_10Y": "IDKA 10A",
    }
    house_rows = []
    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td is None:
            continue
        s = series_map.get(td)
        if s is None or s.empty:
            continue
        s_avail = s[s["VAL_DATE"] <= DATA]
        if s_avail.empty:
            continue
        cfg_ = ALL_FUNDS[td]
        nav_k = _latest_nav(td, DATA_STR)
        if not nav_k:
            continue
        last = s_avail.iloc[-1]
        if cfg_.get("primary") == "bvar":          # IDKAs
            abs_var_pct = abs(float(last.get("stress_pct", 0.0)))
            rel_var_pct = abs(float(last.get("var_pct",    0.0)))
        else:
            abs_var_pct = abs(float(last.get("var_pct", 0.0)))
            # Frontier: use HS BVaR vs. IBOV when we have it
            if short == "FRONTIER" and frontier_bvar:
                rel_var_pct = float(frontier_bvar["bvar_pct"])
            else:
                # CDI-benchmarked: BVaR ≈ absolute VaR since CDI vol ≈ 0
                rel_var_pct = abs_var_pct
        var_brl = abs_var_pct / 100.0 * nav_k
        bvar_brl = rel_var_pct / 100.0 * nav_k
        house_rows.append({
            "short":     short, "label": FUND_LABELS.get(short, short),
            "bench":     _BENCH_BY_FUND.get(short, "—"),
            "nav":       nav_k,
            "var_pct":   abs_var_pct, "var_brl":  var_brl,
            "bvar_pct":  rel_var_pct, "bvar_brl": bvar_brl,
        })

    # Rank: top 5 by absolute R$ VaR and by relative R$ VaR
    top5_abs = set(r["short"] for r in sorted(house_rows, key=lambda r: r["var_brl"],  reverse=True)[:5])
    top5_rel = set(r["short"] for r in sorted(house_rows, key=lambda r: r["bvar_brl"], reverse=True)[:5])

    # Per-fund mini briefing — registered as "briefing" report (first tab)
    _house_by_short = {r["short"]: r for r in house_rows}

    def _mm(v):
        try: return f"{v/1e6:,.1f}M".replace(",", "_").replace(".", ",").replace("_", ".")
        except Exception: return "—"

    house_rows_html = ""
    for r in house_rows:
        rank_abs = "🔺" if r["short"] in top5_abs else ""
        rank_rel = "🔷" if r["short"] in top5_rel else ""
        house_rows_html += (
            "<tr>"
            f'<td class="sum-fund">{r["label"]}</td>'
            f'<td class="mono" style="text-align:right; color:var(--muted)">{_mm(r["nav"])}</td>'
            f'<td class="mono" style="text-align:right; font-weight:600">{r["var_pct"]:.2f}% {rank_abs}</td>'
            f'<td class="mono" style="text-align:center; color:var(--muted)">{r["bench"]}</td>'
            f'<td class="mono" style="text-align:right; font-weight:600">{r["bvar_pct"]:.2f}% {rank_rel}</td>'
            "</tr>"
        )
    tot_nav      = sum(r["nav"]      for r in house_rows)
    tot_var_brl  = sum(r["var_brl"]  for r in house_rows)
    tot_bvar_brl = sum(r["bvar_brl"] for r in house_rows)
    tot_var_pct  = (tot_var_brl  / tot_nav * 100) if tot_nav else 0.0
    tot_bvar_pct = (tot_bvar_brl / tot_nav * 100) if tot_nav else 0.0
    house_total_row = (
        '<tr class="house-total-row">'
        '<td class="sum-fund" style="font-weight:700">Total (soma)</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{_mm(tot_nav)}</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{tot_var_pct:.2f}%</td>'
        '<td></td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{tot_bvar_pct:.2f}%</td>'
        '</tr>'
    )
    # ── Breakdown by risk factor — rows = factors, columns = funds (R$ exposure) ──
    # Factor sources:
    #   Real rates / Nominal rates / IPCA Index → rf_expo_maps (IDKAs + Albatroz)
    #   Equity BR (IBOV)                        → Frontier NAV (100% long); others pending
    factor_matrix = {  # factor_key -> {short: brl, ...}
        "Juros Reais (IPCA)": {},
        "Juros Nominais":     {},
        "IPCA Idx":           {},
        "Equity BR":          {},
        "Equity DM":          {},
        "Equity EM":          {},
        "FX":                 {},
        "Commodities":        {},
    }
    if rf_expo_maps:
        # rf_expo_maps.ano_eq_brl tem sign flip p/ chart (long duration = positivo).
        # factor_matrix usa convenção DV01 (tomado=positivo, dado=negativo).
        # → negar apenas "real" e "nominal" (fatores de taxa); "ipca_idx" é carry,
        # sem flip no rf_expo_maps, não precisa negar.
        _DV01_SIGN_FLIP = {"real": True, "nominal": True, "ipca_idx": False}
        for short_k, df_k in rf_expo_maps.items():
            if df_k is None or df_k.empty:
                continue
            # Evita double-counting de Albatroz: cada fundo (IDKA, MACRO, etc.)
            # tem via='via_albatroz' capturando sua fatia de Albatroz; ALBATROZ
            # row já tem Albatroz inteiro via 'direct'. Filtrar via=='direct'
            # garante que cada bond aparece em apenas uma linha.
            # EVOLUTION (lookthrough_only=True) tem via='lookthrough' para TUDO
            # (inclui Macro/Quant/Frontier slices que já estão em outras linhas)
            # → pular por inteiro para rate factors (sua exposição de juros é
            # indireta via filhos).
            if short_k == "EVOLUTION":
                continue
            df_direct = df_k[df_k["via"] == "direct"]
            if df_direct.empty:
                continue
            for factor_key, factor_col in [("Juros Reais (IPCA)", "real"), ("Juros Nominais", "nominal"), ("IPCA Idx", "ipca_idx")]:
                v = float(df_direct[df_direct["factor"] == factor_col]["ano_eq_brl"].sum())
                if _DV01_SIGN_FLIP.get(factor_col, False):
                    v = -v
                if abs(v) >= 1_000:
                    factor_matrix[factor_key][short_k] = v
    # Frontier = 100% equity BR long (NAV in R$)
    if df_frontier is not None and not df_frontier.empty:
        fr_tot = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
        if not fr_tot.empty:
            gross_pct = float(fr_tot["% Cash"].iloc[0])
            fr_nav = _latest_nav("Frontier A\u00e7\u00f5es FIC FI", DATA_STR) or 0
            if gross_pct and fr_nav:
                factor_matrix["Equity BR"]["FRONTIER"] = gross_pct * fr_nav
    # QUANT + EVOLUTION equity BR — use Evolution-direct only (not look-through)
    # to avoid double-counting positions already held via QUANT / Frontier.
    if df_quant_sn is not None and not df_quant_sn.empty:
        q_equity = float(df_quant_sn["net"].sum())
        if abs(q_equity) >= 1_000:
            factor_matrix["Equity BR"]["QUANT"] = q_equity
    _evo_sn_for_agg = df_evo_direct if df_evo_direct is not None and not df_evo_direct.empty else None
    if _evo_sn_for_agg is not None:
        e_equity = float(_evo_sn_for_agg["net"].sum())
        if abs(e_equity) >= 1_000:
            factor_matrix["Equity BR"]["EVOLUTION"] = e_equity

    # MACRO from df_expo (rf column = RV-BZ / RV-DM / RV-EM / FX-* / RF-BZ / COMMODITIES / P-Metals)
    if df_expo is not None and not df_expo.empty and macro_aum:
        rf_to_factor = {
            "RV-BZ":       "Equity BR",
            "RV-DM":       "Equity DM",
            "RV-EM":       "Equity EM",
            "COMMODITIES": "Commodities",
            "P-Metals":    "Commodities",
        }
        for rf_val, factor_key in rf_to_factor.items():
            v = float(df_expo[df_expo["rf"] == rf_val]["delta"].sum())
            if abs(v) >= 1_000:
                factor_matrix[factor_key]["MACRO"] = factor_matrix[factor_key].get("MACRO", 0.0) + v
        fx_delta = float(df_expo[df_expo["rf"].str.startswith("FX-", na=False)]["delta"].sum())
        if abs(fx_delta) >= 1_000:
            factor_matrix["FX"]["MACRO"] = fx_delta
        # MACRO nominal-rate duration: sum DELTA on RF-BZ / BRL Rate Curve primitive.
        # IMPORTANT: DELTA in LOTE_PRODUCT_EXPO is already duration-weighted
        # (= POSITION × MOD_DURATION). Do NOT multiply by MOD_DURATION again —
        # the prior `delta_dur` column squared it. Filtering to BRL Rate Curve
        # avoids double-counting primitives (IPCA Coupon, etc.) on hybrid rows.
        rf_bz_nom = df_expo[(df_expo["rf"] == "RF-BZ")
                            & (df_expo["PRIMITIVE_CLASS"] == "BRL Rate Curve")]
        if not rf_bz_nom.empty:
            nominal_brl_yr = float(rf_bz_nom["delta"].sum())
            if abs(nominal_brl_yr) >= 1_000:
                factor_matrix["Juros Nominais"]["MACRO"] = nominal_brl_yr

    # QUANT non-equity factors from df_quant_expo (has `factor` col already classified
    # by `_quant_classify_factor`). Equity BR was populated earlier via single-name.
    # Here: Juros Nominais (SIST_RF), FX (SIST_FX + SIST_GLOBAL), Commodities (SIST_COMMO).
    if df_quant_expo is not None and not df_quant_expo.empty:
        # For nominal rates, filter to BRL Rate Curve primitive to avoid double-count.
        quant_nominal = df_quant_expo[
            (df_quant_expo["factor"] == "Juros Nominais")
            & (df_quant_expo["PRIMITIVE_CLASS"] == "BRL Rate Curve")
        ]
        if not quant_nominal.empty:
            v = float(quant_nominal["delta"].sum())
            if abs(v) >= 1_000:
                factor_matrix["Juros Nominais"]["QUANT"] = (
                    factor_matrix["Juros Nominais"].get("QUANT", 0.0) + v
                )
        for q_fac in ("FX", "Commodities"):
            sub = df_quant_expo[df_quant_expo["factor"] == q_fac]
            if sub.empty:
                continue
            v = float(sub["delta"].sum())
            if abs(v) >= 1_000:
                factor_matrix[q_fac]["QUANT"] = (
                    factor_matrix[q_fac].get("QUANT", 0.0) + v
                )

    # Benchmark allocations per fund, per factor (for net-of-bench view).
    # Real rates: IDKAs have duration-concentrated real-rate bench (3y × NAV, 10y × NAV).
    # IPCA Idx:   IDKAs bench = 100% NAV of inflation carry.
    # Equity BR:  Frontier bench is IBOV = 100% NAV long equity.
    # All other factor×fund cells: bench = 0 (CDI-benchmarked funds or factor not in bench).
    nav_by_short = {}
    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td:
            nav_by_short[short] = _latest_nav(td, DATA_STR) or 0.0
    bench_matrix = {k: {} for k in factor_matrix}
    # Juros Reais/Nominais: factor_matrix está em convenção DV01 (long bond = negativo).
    # Bench long IPCA duration do IDKA também é "long bond" → DV01 negativo.
    # IPCA Idx / Equity: sem sign flip (face-value / notional raw), bench positivo.
    # Só setar bench quando o fundo tem gross data — evita "-100%" fantasma
    # em Líquido se o fetch do gross falhar.
    if nav_by_short.get("IDKA_3Y") and factor_matrix["Juros Reais (IPCA)"].get("IDKA_3Y"):
        bench_matrix["Juros Reais (IPCA)"]["IDKA_3Y"] = -3.0 * nav_by_short["IDKA_3Y"]
    if nav_by_short.get("IDKA_3Y") and factor_matrix["IPCA Idx"].get("IDKA_3Y"):
        bench_matrix["IPCA Idx"]["IDKA_3Y"]           =  1.0 * nav_by_short["IDKA_3Y"]
    if nav_by_short.get("IDKA_10Y") and factor_matrix["Juros Reais (IPCA)"].get("IDKA_10Y"):
        bench_matrix["Juros Reais (IPCA)"]["IDKA_10Y"] = -10.0 * nav_by_short["IDKA_10Y"]
    if nav_by_short.get("IDKA_10Y") and factor_matrix["IPCA Idx"].get("IDKA_10Y"):
        bench_matrix["IPCA Idx"]["IDKA_10Y"]           =   1.0 * nav_by_short["IDKA_10Y"]
    if nav_by_short.get("FRONTIER") and factor_matrix["Equity BR"].get("FRONTIER"):
        bench_matrix["Equity BR"]["FRONTIER"] = 1.0 * nav_by_short["FRONTIER"]

    factor_list = ["Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx", "Equity BR", "Equity DM", "Equity EM", "FX", "Commodities"]
    # Unit per factor:
    #   "yrs" → ano_eq_brl is duration-weighted BRL (BRL-years). Divide by NAV → anos.
    #   "pct" → ano_eq_brl is BRL notional. Divide by NAV → %NAV.
    # Duration factors (real/nominal rates) precisam de "yrs" pra não reportar
    # "+376% NAV" quando o valor real é "+3.76 anos de duration".
    _FACTOR_UNIT = {
        "Juros Reais (IPCA)": "yrs",
        "Juros Nominais":     "yrs",
        "IPCA Idx":           "pct",
        "Equity BR":          "pct",
        "Equity DM":          "pct",
        "Equity EM":          "pct",
        "FX":                 "pct",
        "Commodities":        "pct",
    }

    house_nav_tot = sum(v for v in nav_by_short.values() if v)
    def _render_factor_rows(net_of_bench: bool) -> str:
        rows = ""
        for factor_key in factor_list:
            allocations = factor_matrix.get(factor_key, {})
            benches     = bench_matrix.get(factor_key, {}) if net_of_bench else {}
            shorts_with_data = set(allocations.keys()) | set(benches.keys())
            if not shorts_with_data:
                continue
            unit = _FACTOR_UNIT.get(factor_key, "pct")
            # For rate factors (yrs), factor/bench matrices are stored in DV01
            # convention (long bond = negative). Flip sign for display so long
            # duration reads as positive (intuitive for the reader).
            sign_flip = -1 if unit == "yrs" else 1
            cells = ""
            total_brl = 0.0
            for short in FUND_ORDER:
                gross = allocations.get(short, 0.0)
                bench = benches.get(short, 0.0) if net_of_bench else 0.0
                v_brl = (gross - bench) * sign_flip
                nav_k = nav_by_short.get(short, 0.0)
                if abs(v_brl) < 1_000 or not nav_k:
                    cells += '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
                else:
                    total_brl += v_brl
                    col = "var(--up)" if v_brl >= 0 else "var(--down)"
                    if unit == "yrs":
                        yrs = v_brl / nav_k
                        cells += f'<td class="mono" style="text-align:right; color:{col}">{yrs:+.2f} yrs</td>'
                    else:
                        pct = v_brl / nav_k * 100
                        cells += f'<td class="mono" style="text-align:right; color:{col}">{pct:+.2f}%</td>'
            tot_col = "var(--up)" if total_brl >= 0 else "var(--down)"
            if unit == "yrs":
                tot_yrs = (total_brl / house_nav_tot) if house_nav_tot else 0.0
                tot_cell = f'<td class="mono" style="text-align:right; font-weight:700; color:{tot_col}">{tot_yrs:+.2f} yrs</td>'
            else:
                tot_pct = (total_brl / house_nav_tot * 100) if house_nav_tot else 0.0
                tot_cell = f'<td class="mono" style="text-align:right; font-weight:700; color:{tot_col}">{tot_pct:+.2f}%</td>'
            rows += (
                "<tr>"
                f'<td class="sum-fund">{factor_key}</td>'
                + cells
                + tot_cell
                + "</tr>"
            )
        return rows

    factor_rows_liquido = _render_factor_rows(net_of_bench=True)
    factor_rows_bruto   = _render_factor_rows(net_of_bench=False)
    factor_rows_html    = factor_rows_liquido  # keep legacy var name for below

    # Per-fund mini briefings. Rebuild sections_html so briefings come FIRST
    # within each fund's block (DOM order = tab order: briefing → ... → others).
    _briefing_by_short = {}
    for short in FUND_ORDER:
        hr = _house_by_short.get(short)
        if not hr:
            continue
        mini_html = _build_fund_mini_briefing(
            short, hr, series_map, td_by_short, df_pa,
            factor_matrix, bench_matrix,
            pm_margem=pm_margem if short == "MACRO" else None,
            frontier_bvar=frontier_bvar if short == "FRONTIER" else None,
            df_frontier=df_frontier if short == "FRONTIER" else None,
        )
        if mini_html:
            _briefing_by_short[short] = mini_html
            available_pairs.add((short, "briefing"))
    if "briefing" not in reports_with_data:
        reports_with_data = list(reports_with_data) + ["briefing"]

    # Rebuild sections_html: briefing comes LAST per fund (user parked it here
    # while its quality is validated — tab stays accessible but not the default).
    _sections_by_fund = {}
    for f, r, h in sections:
        _sections_by_fund.setdefault(f, []).append((r, h))
    _reordered_html = ""
    for f in FUND_ORDER:
        for r, h in _sections_by_fund.get(f, []):
            _reordered_html += (
                f'<div id="sec-{f}-{r}" class="section-wrap" data-fund="{f}" data-report="{r}">{h}</div>'
            )
        if f in _briefing_by_short:
            _reordered_html += (
                f'<div id="sec-{f}-briefing" class="section-wrap" '
                f'data-fund="{f}" data-report="briefing">{_briefing_by_short[f]}</div>'
            )
    sections_html = _reordered_html

    fund_col_headers = "".join(
        f'<th style="text-align:right">{FUND_LABELS.get(f, f)}</th>' for f in FUND_ORDER
    )
    # ── Cross-fund top positions — consolidated list ──────────────────────────
    # One row per (fund, factor, product); exclude via_albatroz to avoid double-counting
    # (those positions are already captured under ALBATROZ direct).
    agg_rows = []
    if rf_expo_maps:
        for short_k, df_k in rf_expo_maps.items():
            if df_k is None or df_k.empty:
                continue
            d = df_k[(df_k["via"] == "direct") & (df_k["factor"].isin(["real", "nominal"]))].copy()
            if d.empty:
                continue
            # ANO_EQ ×  NAV not needed — ano_eq_brl is already signed BRL-years
            for _, r in d.iterrows():
                brl = float(r["ano_eq_brl"])
                if abs(brl) < 1_000: continue
                agg_rows.append({
                    "fund":    FUND_LABELS.get(short_k, short_k),
                    "factor":  "Juros Reais (IPCA)" if r["factor"] == "real" else "Juros Nominais",
                    "product": r["PRODUCT"],
                    "brl":     brl,
                    "unit":    "BRL-yr",
                })
    # Frontier stocks
    if df_frontier is not None and not df_frontier.empty:
        fr_nav = _latest_nav("Frontier A\u00e7\u00f5es FIC FI", DATA_STR) or 0
        stocks = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])]
        stocks = stocks[stocks["% Cash"].notna()]
        for _, r in stocks.iterrows():
            brl = float(r["% Cash"]) * fr_nav
            if abs(brl) < 1_000: continue
            agg_rows.append({
                "fund": "Frontier", "factor": "Equity BR",
                "product": r["PRODUCT"], "brl": brl, "unit": "BRL",
            })
    # QUANT + EVOLUTION single-names (net delta per ticker) — Evolution-direct
    for short_k, df_sn in [("QUANT", df_quant_sn), ("EVOLUTION", df_evo_direct)]:
        if df_sn is None or df_sn.empty:
            continue
        for _, r in df_sn.iterrows():
            brl = float(r["net"])
            if abs(brl) < 10_000: continue
            agg_rows.append({
                "fund": FUND_LABELS.get(short_k, short_k), "factor": "Equity BR",
                "product": r["ticker"], "brl": brl, "unit": "BRL",
            })
    # MACRO equity (RV-DM / RV-EM) and commodities, aggregated by PRODUCT
    if df_expo is not None and not df_expo.empty:
        macro_focus = df_expo[df_expo["rf"].isin(["RV-BZ", "RV-DM", "RV-EM", "COMMODITIES", "P-Metals"])]
        grp = macro_focus.groupby(["rf", "PRODUCT"], as_index=False).agg(delta=("delta", "sum"))
        for _, r in grp.iterrows():
            brl = float(r["delta"])
            if abs(brl) < 10_000: continue
            factor_label = {"RV-BZ": "Equity BR", "RV-DM": "Equity DM", "RV-EM": "Equity EM",
                            "COMMODITIES": "Commodities", "P-Metals": "Commodities"}[r["rf"]]
            agg_rows.append({
                "fund": "Macro", "factor": factor_label, "product": r["PRODUCT"],
                "brl": brl, "unit": "BRL",
            })

    # Bruto total per (fund, factor) for pro-rata bench subtraction in Líquido view.
    fund_factor_gross = {}
    for r in agg_rows:
        key = (r["fund"], r["factor"])
        fund_factor_gross[key] = fund_factor_gross.get(key, 0.0) + r["brl"]

    # Líquido scale factor: (1 − bench/total_fund_factor_gross). Positions that
    # just replicate bench get scaled down pro-rata to their share of fund exposure.
    def _liquido_scale(fund_label, factor_label) -> float:
        short = next((s for s, lbl in FUND_LABELS.items() if lbl == fund_label), fund_label)
        factor_key = factor_label  # already matches bench_matrix keys
        bench = bench_matrix.get(factor_key, {}).get(short, 0.0)
        total = fund_factor_gross.get((fund_label, factor_label), 0.0)
        if abs(total) < 1e-6 or abs(bench) < 1e-6:
            return 1.0
        return 1.0 - bench / total

    def _render_top_rows(liquido: bool, mode_key: str) -> str:
        from collections import defaultdict
        scaled = []
        for r in agg_rows:
            brl = r["brl"]
            if liquido:
                brl = brl * _liquido_scale(r["fund"], r["factor"])
            if abs(brl) < 1_000:
                continue
            rr = dict(r); rr["brl"] = brl
            scaled.append(rr)

        by_fi = defaultdict(list)
        for r in scaled:
            by_fi[(r["factor"], r["product"])].append(r)

        instruments = []
        for (factor, product), rows in by_fi.items():
            total = sum(r["brl"] for r in rows)
            unit  = rows[0]["unit"]
            instruments.append({
                "factor": factor, "product": product, "total": total,
                "unit": unit, "holders": rows,
            })

        factor_totals = defaultdict(float)
        factor_unit   = {}
        for inst in instruments:
            factor_totals[inst["factor"]] += inst["total"]
            factor_unit[inst["factor"]] = inst["unit"]
        factor_order = sorted(factor_totals.keys(),
                              key=lambda f: abs(factor_totals[f]), reverse=True)

        html = ""
        for factor in factor_order:
            factor_insts = [i for i in instruments if i["factor"] == factor]
            factor_insts.sort(key=lambda i: abs(i["total"]), reverse=True)
            if not factor_insts:
                continue
            ftot = factor_totals[factor]
            ftot_col = "var(--up)" if ftot >= 0 else "var(--down)"
            fpath = f"{mode_key}|{factor}"
            html += (
                f'<tr class="tp-row tp-lvl-0" data-tp-path="{fpath}" '
                f'onclick="toggleTopPos(this)" style="cursor:pointer">'
                f'<td class="sum-fund" style="font-weight:700; letter-spacing:.05em; text-transform:uppercase">'
                f'<span class="tp-caret">▶</span> {factor}</td>'
                f'<td class="mono" style="color:var(--muted); font-size:11px">{len(factor_insts)} instrumentos</td>'
                f'<td class="mono" style="text-align:right; color:{ftot_col}; font-weight:700">{_mm(ftot)}</td>'
                f'<td class="mono" style="color:var(--muted); font-size:10.5px">{factor_unit.get(factor, "")}</td>'
                "</tr>"
            )
            for inst in factor_insts:
                col = "var(--up)" if inst["total"] >= 0 else "var(--down)"
                ipath = f"{mode_key}|{factor}|{inst['product']}"
                expandable = len(inst["holders"]) > 0
                caret_html = '<span class="tp-caret">▶</span> ' if expandable else '  '
                html += (
                    f'<tr class="tp-row tp-lvl-1" data-tp-parent="{fpath}" data-tp-path="{ipath}" '
                    f'onclick="toggleTopPos(this)" style="display:none; cursor:pointer">'
                    f'<td class="sum-fund" style="padding-left:22px">{caret_html}{inst["product"]}</td>'
                    f'<td class="mono" style="color:var(--muted); font-size:11px">{len(inst["holders"])} fundo(s)</td>'
                    f'<td class="mono" style="text-align:right; color:{col}; font-weight:600">{_mm(inst["total"])}</td>'
                    f'<td class="mono" style="color:var(--muted); font-size:10.5px">{inst["unit"]}</td>'
                    "</tr>"
                )
                inst["holders"].sort(key=lambda r: abs(r["brl"]), reverse=True)
                for h in inst["holders"]:
                    hcol = "var(--up)" if h["brl"] >= 0 else "var(--down)"
                    pct = (h["brl"] / inst["total"] * 100) if inst["total"] else 0
                    html += (
                        f'<tr class="tp-row tp-lvl-2" data-tp-parent="{ipath}" style="display:none">'
                        f'<td class="sum-fund" style="padding-left:44px; font-size:11.5px; color:var(--muted)">{h["fund"]}</td>'
                        f'<td class="mono" style="color:var(--muted); font-size:10.5px">{pct:+.0f}% da posição</td>'
                        f'<td class="mono" style="text-align:right; color:{hcol}; font-size:11.5px">{_mm(h["brl"])}</td>'
                        f'<td class="mono" style="color:var(--muted); font-size:10.5px">{h["unit"]}</td>'
                        "</tr>"
                    )
        return html

    top_rows_liquido = _render_top_rows(liquido=True,  mode_key="liq")
    top_rows_bruto   = _render_top_rows(liquido=False, mode_key="bru")

    top_positions_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Top Posições — consolidado</span>
        <span class="card-sub">— top 15 posições da casa por |exposure| · via_albatroz excluído (contado em ALBATROZ direto)</span>
        <div class="pa-view-toggle rf-brl-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-rf-brl="liquido" onclick="selectRfBrl(this,'liquido')">Líquido</button>
          <button class="pa-tgl"        data-rf-brl="bruto"   onclick="selectRfBrl(this,'bruto')">Bruto</button>
        </div>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fator / Instrumento / Fundo</th>
          <th style="text-align:left">Detalhe</th>
          <th style="text-align:right">Total</th>
          <th style="text-align:left">Unidade</th>
        </tr></thead>
        <tbody class="rf-brl-body" data-rf-brl="liquido">{top_rows_liquido}</tbody>
        <tbody class="rf-brl-body" data-rf-brl="bruto" style="display:none">{top_rows_bruto}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:10px; color:var(--muted)">
        Drill-down: clique em um <b>Fator</b> (ex: Real, Nominal, Commodities) para abrir seus instrumentos; clique em um <b>Instrumento</b> para ver em quais fundos está alocado e a %. <b>Líquido</b>: cada contribuição é escalada por (1 − bench/total_fundo_fator); <b>Bruto</b>: sem abater bench. EVOLUTION usa posições diretas (sem look-through); via_albatroz excluído (contado em ALBATROZ direto).
      </div>
    </section>"""

    by_factor_html = f"""
    <section class="card" id="breakdown-por-fator">
      <div class="card-head">
        <span class="card-title">Breakdown por Fator</span>
        <span class="card-sub">— exposure por fator × fundo · <b>Juros Reais/Nominais em anos de duration</b> (yrs); <b>IPCA Idx/Equity/FX/Commodities em %NAV nocional</b></span>
        <div class="pa-view-toggle rf-brl-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-rf-brl="liquido" onclick="selectRfBrl(this,'liquido')">Líquido</button>
          <button class="pa-tgl"        data-rf-brl="bruto"   onclick="selectRfBrl(this,'bruto')">Bruto</button>
        </div>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fator</th>
          {fund_col_headers}
          <th style="text-align:right">Total</th>
        </tr></thead>
        <tbody class="rf-brl-body" data-rf-brl="liquido">{factor_rows_liquido}</tbody>
        <tbody class="rf-brl-body" data-rf-brl="bruto" style="display:none">{factor_rows_bruto}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:10px; color:var(--muted)">
        <b>Líquido</b>: fundo − benchmark (IDKAs menos bench real-rate 3y/10y; Frontier menos IBOV 100% NAV). <b>Bruto</b>: exposição total sem abater bench.
        Cada célula = valor BRL / NAV do fundo × 100. Total = soma BRL / house NAV × 100. Real/Nominal/IPCA em %NAV·ano (yr-eq); Equity/FX/Commodities em %NAV nocional. Cobre IDKAs + Albatroz + Frontier + MACRO + QUANT + EVOLUTION. Crédito omitido por escopo.
      </div>
    </section>"""

    house_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risco VaR e BVaR por fundo</span>
        <span class="card-sub">— {DATA_STR} · VaR 95% 1d absoluto e vs. benchmark · top-5 destacados (🔺 absoluto · 🔷 relativo)</span>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">NAV</th>
          <th style="text-align:right"><span class="kc">VaR</span> abs (%)</th>
          <th style="text-align:center">Bench</th>
          <th style="text-align:right"><span class="kc">BVaR</span> rel (%)</th>
        </tr></thead>
        <tbody>{house_rows_html}</tbody>
        <tfoot>{house_total_row}</tfoot>
      </table>
      <div class="bar-legend" style="margin-top:10px">
        🔺 top-5 por risco absoluto (R$) &nbsp;·&nbsp;
        🔷 top-5 por risco ativo vs. benchmark (R$) &nbsp;·&nbsp;
        <span style="color:var(--muted)">ranking por R$ (não exibido); BVaR para fundos contra CDI ≈ VaR abs (CDI tem vol ≈ 0); Frontier usa HS BVaR vs. IBOV; IDKAs usam BVaR paramétrico do engine. Total = soma simples ponderada por NAV (sem benefício de diversificação).</span>
      </div>
    </section>"""

    fund_grid_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Status consolidado</span>
        <span class="card-sub">— {DATA_STR} · alpha vs. CDI (Frontier: ER vs. IBOV) · utilização de VaR</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="width:60px; text-align:center">Status</th>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">DIA</th>
          <th style="text-align:right">MTD</th>
          <th style="text-align:right">YTD</th>
          <th style="text-align:right">12M</th>
          <th style="text-align:right"><span class="kc">VaR</span></th>
          <th style="text-align:right">Util <span class="kc">VaR</span></th>
          <th style="text-align:right">Δ <span class="kc">VaR</span> D-1</th>
        </tr></thead>
        <tbody>{summary_rows_html}{bench_rows_html}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:12px">
        <span style="color:var(--up)">🟢</span> util &lt; 70% &nbsp;·&nbsp;
        <span style="color:var(--warn)">🟡</span> 70–100% &nbsp;·&nbsp;
        <span style="color:var(--down)">🔴</span> ≥ 100% &nbsp;·&nbsp;
        <span style="color:var(--muted)">Status = utilização de VaR (stop mensal vive no Risk Budget)</span>
      </div>
    </section>"""

    # ── Vol Regime card ──────────────────────────────────────────────────
    # Realized 21d vol (annualized) vs. 3y baseline. Primary signal = pct_rank
    # (non-parametric). Z-score is shown as secondary (N_eff ~36 from 21d
    # overlap → ~15% SE; use for direction, not hard thresholds).
    def _regime_tag(regime: str) -> str:
        colors = {
            "low":      ("var(--up)",     "low"),
            "normal":   ("var(--muted)",  "normal"),
            "elevated": ("var(--warn)",   "elevated"),
            "stressed": ("var(--down)",   "stressed"),
            "—":        ("var(--muted)",  "—"),
        }
        c, lbl = colors.get(regime, ("var(--muted)", regime))
        return f'<span class="mono" style="color:{c}; font-weight:700">{lbl}</span>'

    def _pct_color(p):
        if p is None or pd.isna(p):
            return "var(--muted)"
        if p >= 90: return "var(--down)"
        if p >= 70: return "var(--warn)"
        if p < 20:  return "var(--up)"
        return "var(--text)"

    def _z_color(z):
        if z is None or pd.isna(z):
            return "var(--muted)"
        if abs(z) >= 2: return "var(--down)"
        if abs(z) >= 1: return "var(--warn)"
        return "var(--text)"

    # fund_short -> portfolio key inside PORTIFOLIO_DAILY_HISTORICAL_SIMULATION.
    # Only funds whose W-series exists show vol regime; others fall through to "—".
    _FUND_PORTFOLIO_KEY = {
        "MACRO":     "MACRO",
        "EVOLUTION": "EVOLUTION",
        "QUANT":     "SIST",
    }

    vol_rows_html = ""
    if vol_regime_map:
        for short in FUND_ORDER:
            pkey = _FUND_PORTFOLIO_KEY.get(short)
            r    = vol_regime_map.get(pkey) if pkey else None
            if not r:
                vol_rows_html += (
                    "<tr>"
                    f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:center">—</td>'
                    "</tr>"
                )
                continue
            vol_r   = r["vol_recent_pct"]
            vol_f   = r["vol_full_pct"]
            ratio   = r["ratio"]
            pct_v   = r["pct_rank"]
            n_obs   = r["n_obs"]
            vol_rows_html += (
                "<tr>"
                f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
                f'<td class="mono" style="text-align:right">{vol_r:.2f}%</td>'
                f'<td class="mono" style="color:var(--muted); text-align:right">{vol_f:.2f}%</td>'
                f'<td class="mono" style="text-align:right; font-weight:700">{ratio:.2f}x</td>'
                f'<td class="mono" style="color:{_pct_color(pct_v)}; text-align:right; font-weight:700">'
                f'{pct_v:.0f}°</td>'
                f'<td style="text-align:center">{_regime_tag(r["regime"])}</td>'
                "</tr>"
            )

    vol_regime_html = ""
    if vol_rows_html and any(v for v in (vol_regime_map or {}).values()):
        vol_regime_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Vol Regime</span>
        <span class="card-sub">— carteira atual · vol 21d vs. janela total HS</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">Vol 21d</th>
          <th style="text-align:right">Vol full</th>
          <th style="text-align:right">Ratio</th>
          <th style="text-align:right">Pct</th>
          <th style="text-align:center">Regime</th>
        </tr></thead>
        <tbody>{vol_rows_html}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:12px">
        Carteira atual aplicada à janela de simulação histórica (W series). ·
        <b>Vol 21d</b> = std(W dos 21 dias mais recentes) × √252 ·
        <b>Vol full</b> = std(W da janela completa) × √252 ·
        <b>Ratio</b> = vol 21d / vol full ·
        <b>Pct</b> = percentil de std(21d) entre todas as janelas rolling de 21d ·
        <b>Regime</b>: low &lt;p20 · normal p20–70 · elevated p70–90 · stressed ≥p90
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

    # QUANT / EVOLUTION / MACRO_Q / ALBATROZ / FRONTIER — factor-level
    if position_changes:
        for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER"):
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

    # ── Executive briefing (curated top-of-summary card) ──────────────────────
    # Pulls from already-computed structures: house_rows, factor_matrix,
    # bench_matrix, agg_rows, position_changes, vol_regime_map, pm_margem,
    # df_pa, frontier_bvar, ibov, cdi.
    briefing_html = _build_executive_briefing(
        house_rows=house_rows, factor_matrix=factor_matrix,
        bench_matrix=bench_matrix, agg_rows=agg_rows,
        position_changes=position_changes, vol_regime_map=vol_regime_map,
        pm_margem=pm_margem, df_pa=df_pa, frontier_bvar=frontier_bvar,
        series_map=series_map, td_by_short=td_by_short,
        ibov=ibov, cdi=cdi,
    )

    # Camada 4 — headline no topo do Summary quando o alerta do Evolution está aceso
    # ou parcialmente aceso (≥1 condição). Link direto pro tab "Diversificação" do EVOLUTION.
    evo_c4_headline_html = ""
    if evolution_c4_state:
        n_lit = evolution_c4_state.get("n_lit", 0)
        alert_on = evolution_c4_state.get("alert", False)
        if n_lit >= 1:
            if alert_on:
                bg = "rgba(255,90,106,0.14)"; border = "var(--down)"
                icon = "🚨"; title = "EVOLUTION · Bull Market Alignment — alerta disparado"
            else:
                bg = "rgba(184,135,0,0.10)"; border = "var(--warn)"
                icon = "🟡"; title = "EVOLUTION · Alinhamento parcial de estratégias"
            lit_names = ", ".join(c["name"] for c in evolution_c4_state.get("conditions", []) if c["lit"])
            evo_c4_headline_html = f"""
    <section class="card" style="background:{bg};border-left:4px solid {border};margin-bottom:14px">
      <div style="padding:10px 16px;font-size:13px;color:var(--text)">
        <div style="font-weight:700;margin-bottom:4px">{icon} {title}</div>
        <div style="color:var(--muted);font-size:12px">
          {n_lit} de 5 condições acesas{' (gatilho ≥3)' if not alert_on else ''} · {lit_names}
          · <a href="#" onclick="if(typeof selectFund==='function'){{selectFund('EVOLUTION');setTimeout(function(){{var el=document.querySelector('[data-fund=\\'EVOLUTION\\'][data-report=\\'diversification\\']');if(el)el.scrollIntoView({{behavior:'smooth'}});}},100);}}return false;"
               style="color:var(--accent-blue);text-decoration:underline">ver detalhe →</a>
        </div>
      </div>
    </section>"""

    summary_html = f"""
    <div class="section-wrap" data-view="summary">
      {evo_c4_headline_html}
      {briefing_html}
      {fund_grid_html}
      {house_html}
      {by_factor_html}
      {vol_regime_html}
      {alerts_html}
      {comments_html}
      {movers_html}
      {changes_html}
      {top_positions_html}
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
    # Include both uppercase shorts (MACRO, QUANT, ...) AND mixed-case labels
    # (Macro, Quantitativo, Frontier, ...) + known sub-strategy tags so they all
    # get highlighted in card-subs.
    _EXTRA_FUND_TERMS = ["Evo Strategy", "Evolution FIC", "Evo"]
    _highlight_terms = (
        list(FUND_ORDER)
      + list(set(FUND_LABELS.values()))
      + _EXTRA_FUND_TERMS
    )
    fund_shorts_js   = json.dumps(_highlight_terms)
    fund_labels_js   = json.dumps(FUND_LABELS)
    fund_order_js    = json.dumps(list(FUND_ORDER))

    # Por Report mode removed — no per-report fund switcher needed

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta http-equiv="X-UA-Compatible" content="IE=edge"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
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

  /* ── Mobile responsive — viewport ≤ 768px ────────────────────────────── */
  @media (max-width: 768px) {{
    main {{ padding: 12px 10px 28px; }}
    .card {{ padding: 10px 12px; }}
    /* Wide tables: horizontal scroll instead of page overflow */
    .card table, .summary-table {{
      display: block; overflow-x: auto; white-space: nowrap;
      -webkit-overflow-scrolling: touch;
    }}
    .card-head {{ flex-wrap: wrap; gap: 6px; }}
    .card-title {{ font-size: 14px; }}
    .card-sub {{ font-size: 11px; }}
    /* Chip bars wrap to multiple lines */
    .fund-nav-chips, .mode-switcher, .sub-tabs {{ flex-wrap: wrap; }}
    /* Smaller monospace for dense tables */
    .mono {{ font-size: 11px; }}
    /* Hide decorative elements in header */
    header .brand p {{ display: none; }}
    header h1 {{ font-size: 14px; }}
  }}
  @media (max-width: 480px) {{
    main {{ padding: 8px 6px 24px; }}
    .card {{ padding: 8px 8px; border-radius: 6px; }}
    .card-title {{ font-size: 13px; }}
    .mono {{ font-size: 10px; }}
  }}
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
  .card-sub .fund-name {{
    color:var(--accent); font-weight:700; letter-spacing:.08em;
    padding:1px 6px; border-radius:4px;
    background:rgba(26,143,209,0.10); border:1px solid rgba(26,143,209,0.25);
  }}

  /* Utility: keep mixed-case inside uppercase-transformed parents (e.g. "VaR" in th) */
  .kc {{ text-transform:none !important; }}

  /* Frontier PA card — nested hierarchical PA (sub-tab) gets flush styling */
  .fpa-pa-nested > section.card {{
    background:transparent !important; border:none !important;
    padding:0 !important; margin:0 !important; box-shadow:none !important;
  }}
  .fpa-pa-nested > section.card > .card-head {{ padding:0 0 8px !important; }}

  /* Per-fund nav chips — only visible in "Por Report" mode.
     Click scrolls within the currently selected report to the chosen fund's
     section-wrap. In "Por Fundo" mode the user is already viewing one fund, so
     the chip bar would be redundant / confusing (and used to switch funds).
     Sticky below the header: as you scroll past one fund section the next
     section's chip bar naturally takes over (each bar highlights its own fund). */
  .fund-nav-chips {{
    position:sticky; top:var(--header-h, 72px); z-index:40;
    display:flex; flex-wrap:wrap; gap:6px;
    padding:8px 12px; margin-bottom:14px;
    background:rgba(17,20,26,.92); backdrop-filter:blur(8px);
    border:1px solid var(--line); border-radius:8px;
    font-size:11px; letter-spacing:.03em;
    box-shadow:0 4px 14px -8px rgba(0,0,0,.55);
  }}
  body:not([data-mode="report"]) .fund-nav-chips {{ display:none; }}
  .fund-nav-chips .chip-label {{
    color:var(--muted); text-transform:uppercase; letter-spacing:.12em;
    font-size:10px; padding:4px 6px; align-self:center;
  }}
  .fund-nav-chips .chip {{
    background:transparent; color:var(--muted);
    border:1px solid var(--line); border-radius:4px;
    padding:4px 10px; cursor:pointer; font-family:inherit;
    transition:all .12s ease;
  }}
  .fund-nav-chips .chip:hover {{ color:var(--text); border-color:var(--accent); }}
  .fund-nav-chips .chip.active {{
    background:var(--accent); color:#0b1220;
    border-color:var(--accent); font-weight:700;
  }}

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
  .rf-view-toggle {{ margin-left:auto; }}

  /* RF Exposure Map chart */
  .rf-expo-svg {{ display:block; margin:0 auto; }}
  .rf-expo-svg .rf-bar {{ stroke:none; opacity:.92; }}
  .rf-expo-svg .rf-real {{ fill:#f59e0b; }}          /* amber — real/IPCA */
  .rf-expo-svg .rf-nom  {{ fill:#14b8a6; }}          /* teal — nominal/pré */
  .rf-expo-svg .rf-benchbar {{ fill:#64748b; opacity:.9; }} /* slate — benchmark */
  .rf-expo-svg .rf-cum-bench {{ fill:none; stroke:#64748b; stroke-width:1.6; stroke-dasharray:4 3; }}
  .rf-expo-svg .rf-cum  {{ fill:none; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }}
  .rf-expo-svg .rf-cum-real {{ stroke:#b45309; stroke-dasharray:0; }}
  .rf-expo-svg .rf-cum-nom  {{ stroke:#0f766e; stroke-dasharray:0; }}
  .rf-expo-svg .rf-bench {{ fill:none; stroke:#64748b; stroke-width:1.4; stroke-dasharray:4 3; }}
  .rf-expo-svg .rf-gap {{ fill:none; stroke:#1a8fd1; stroke-width:2.2; }}
  .rf-expo-svg .rf-grid {{ stroke:var(--line); stroke-width:.7; opacity:.4; }}
  .rf-expo-svg .rf-zero {{ stroke:var(--muted); stroke-width:1; opacity:.6; }}
  .rf-expo-svg .rf-axis-lbl {{ fill:var(--muted); font-size:10.5px; font-family:'JetBrains Mono', monospace; }}
  .rf-expo-svg .rf-bench-marker {{ stroke:#f97316; stroke-width:2.2; stroke-dasharray:6 4; opacity:1; }}
  .rf-expo-svg .rf-bench-marker-lbl {{ fill:#f97316; font-weight:700; font-size:11px; }}
  .rf-expo-svg .rf-bench-marker-lbl-bg {{ fill:var(--bg); opacity:.85; }}

  .rf-legend {{ display:flex; flex-wrap:wrap; justify-content:center; gap:16px; align-items:center; }}
  .rf-legend-item {{ display:inline-flex; align-items:center; gap:5px; }}
  .rf-swatch {{ display:inline-block; width:12px; height:10px; border-radius:2px; vertical-align:middle; }}
  .rf-swatch.rf-real {{ background:#f59e0b; }}
  .rf-swatch.rf-nom  {{ background:#14b8a6; }}
  .rf-swatch.rf-benchbar {{ background:#64748b; }}
  .rf-swatch.rf-cum  {{ background:#b45309; height:2px; border-radius:0; }}
  .rf-swatch.rf-bench {{ background:transparent; border-top:2px dashed #64748b; height:0; width:14px; border-radius:0; }}
  .rf-swatch.rf-gap  {{ background:#1a8fd1; height:2px; border-radius:0; }}
  .rf-tbl-toggle {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:4px 10px; border-radius:6px; font-size:11px; cursor:pointer;
    font-family:'Inter', sans-serif; letter-spacing:.04em;
  }}
  .rf-tbl-toggle:hover {{ background:var(--panel-2); color:var(--text); }}

  /* Executive briefing (top of Summary) */
  .brief-card {{ border-left:3px solid var(--accent); }}
  .brief-headline {{
    font-size:16px; font-weight:600; color:var(--text);
    padding:10px 14px; margin:6px 0 12px; line-height:1.45;
    background:rgba(0,113,187,.06); border-radius:6px;
    border-left:2px solid var(--accent-2);
  }}
  .brief-benchmarks {{
    font-size:11px; color:var(--muted); margin-bottom:14px;
    letter-spacing:.04em; padding-left:4px;
  }}
  .brief-grid {{
    display:grid; grid-template-columns:1fr 1fr; gap:24px;
  }}
  @media (max-width:900px) {{ .brief-grid {{ grid-template-columns:1fr; gap:16px; }} }}
  .brief-col {{ min-width:0; }}
  .brief-section-title {{
    font-size:10px; letter-spacing:.14em; text-transform:uppercase;
    color:var(--accent-2); font-weight:700; margin:12px 0 6px;
    padding-bottom:4px; border-bottom:1px solid var(--line);
  }}
  .brief-list {{ list-style:none; margin:0; padding:0; font-size:12.5px; line-height:1.55; }}
  .brief-list li {{ padding:5px 0; border-bottom:1px dashed var(--line); }}
  .brief-list li:last-child {{ border-bottom:none; }}
  .brief-list li b {{ color:var(--text); font-weight:600; }}
  .brief-list li i {{ color:var(--muted); font-style:normal; }}
  .brief-annex {{
    margin-top:14px; padding-top:10px; border-top:1px solid var(--line);
    font-size:11px; color:var(--muted);
  }}
  .brief-annex a {{ color:var(--accent-2); text-decoration:none; border-bottom:1px dotted var(--accent-2); }}
  .brief-annex a:hover {{ color:var(--text); border-bottom-color:var(--text); }}
  .brief-commentary {{
    font-size:13px; color:var(--text); line-height:1.65;
    padding:10px 14px; margin:0 0 14px;
    background:rgba(26,143,209,.04); border-left:2px solid var(--accent-2);
    border-radius:6px;
  }}
  .brief-commentary b {{ color:var(--text); font-weight:600; }}

  /* PA Contribuições — Por Tamanho / Por Fundo grid (flows side-by-side) */
  .pa-alert-view {{
    display:grid;
    grid-template-columns:repeat(auto-fill, minmax(280px, 1fr));
    gap:10px;
  }}
  .pa-alert-view-hidden {{ display:none; }}

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
  .summary-table tr.bench-row:first-of-type td {{ border-top:2px solid var(--border); }}
  .summary-table tr.bench-row td {{ background:rgba(26,143,209,.03); }}
  .summary-table tr.bench-row td.sum-fund {{ font-weight:600; font-size:12.5px; }}

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

  /* Frontier Exposure toggles (IBOV/IBOD, Por Nome/Por Setor, ▼/▶ All).
     Same visual as .pa-tgl — dark bg, accent-blue active state. */
  .toggle-btn {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 12px; border-radius:7px; font-size:11px; font-weight:600;
    letter-spacing:.06em; cursor:pointer;
    font-family:'Inter', sans-serif;
  }}
  .toggle-btn:hover {{ color:var(--text); border-color:var(--line-2); background:rgba(0,113,187,.06); }}
  .toggle-btn.active {{ background:var(--accent); border-color:var(--accent); color:#fff; }}

  /* QUANT Exposure sortable column headers */
  .qexpo-sort-th {{ user-select:none; transition: color .12s ease; }}
  .qexpo-sort-th:hover {{ color:var(--accent-2); }}
  .qexpo-sort-th.qexpo-sort-active {{ color:var(--accent-2); }}
  .qexpo-sort-arrow {{ color:var(--accent-2); font-weight:700; margin-left:3px; }}

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

  function syncHeaderH() {{
    var h = document.querySelector('header');
    if (!h) return;
    // rAF: header height depends on sub-tabs visibility, which is set earlier
    // in the same tick — let layout settle before reading offsetHeight.
    requestAnimationFrame(function() {{
      document.documentElement.style.setProperty('--header-h', h.offsetHeight + 'px');
    }});
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
    // Recompute header height — it changes when #report-jump-bar shows/hides.
    syncHeaderH();
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
    attachVrCaretToggle();
    highlightFundNames();
    injectFundNavChips();
    syncHeaderH();
    window.addEventListener('resize', syncHeaderH);
  }});
  // --- Wrap fund shortnames in card-sub elements with an accent chip ---
  function highlightFundNames() {{
    var SHORTS = {fund_shorts_js};
    // Sort by length desc so "MACRO_Q" matches before "MACRO"
    SHORTS.sort(function(a,b) {{ return b.length - a.length; }});
    var subs = document.querySelectorAll('.card-sub');
    subs.forEach(function(el) {{
      if (el.dataset.fundHighlighted) return;
      var html = el.innerHTML;
      for (var i = 0; i < SHORTS.length; i++) {{
        var s = SHORTS[i];
        // Word-boundary match; avoid wrapping inside existing tags
        var re = new RegExp('(^|[^A-Za-z0-9_>])(' + s + ')(?![A-Za-z0-9_])', 'g');
        html = html.replace(re, '$1<span class="fund-name">$2</span>');
      }}
      el.innerHTML = html;
      el.dataset.fundHighlighted = '1';
    }});
  }}
  // --- Fund-nav chip bar injected above each per-fund section ---
  function injectFundNavChips() {{
    var LABELS = {fund_labels_js};
    var ORDER  = {fund_order_js};
    // Collect available funds (those with at least one visible section-wrap)
    var available = ORDER.filter(function(s) {{
      return document.querySelector('.section-wrap[data-fund="' + s + '"]');
    }});
    if (available.length < 2) return;
    // Build one chip bar per fund — insert at the top of the first section-wrap
    // for each fund (user sees it once, highlighting the active fund).
    available.forEach(function(fs) {{
      var wrap = document.querySelector('.section-wrap[data-fund="' + fs + '"]');
      if (!wrap || wrap.querySelector('.fund-nav-chips')) return;
      var bar = document.createElement('div');
      bar.className = 'fund-nav-chips';
      var lbl = document.createElement('span');
      lbl.className = 'chip-label';
      lbl.textContent = 'Ir para:';
      bar.appendChild(lbl);
      available.forEach(function(s) {{
        var btn = document.createElement('button');
        btn.className = 'chip' + (s === fs ? ' active' : '');
        btn.textContent = LABELS[s] || s;
        // In "report" mode: stay in report, just scroll to that fund's section
        // for the currently selected report. In "fund" mode: switch fund.
        btn.onclick = function() {{
          var mode = document.body.dataset.mode || 'summary';
          if (mode === 'report') {{
            var currentReport = (location.hash.match(/report=([^&]+)/) || [])[1];
            var target = currentReport
              ? document.querySelector('#sec-' + s + '-' + currentReport)
              : document.querySelector('.section-wrap[data-fund="' + s + '"]');
            if (target) target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
          }} else {{
            window.selectFund(s);
          }}
        }};
        bar.appendChild(btn);
      }});
      wrap.insertBefore(bar, wrap.firstChild);
    }});
  }}
  // --- Vol Regime: expand/collapse books under a fund row ---
  function attachVrCaretToggle() {{
    document.querySelectorAll('.vr-caret').forEach(function(el) {{
      el.addEventListener('click', function(e) {{
        e.stopPropagation();
        var fs = el.getAttribute('data-fs');
        if (!fs) return;
        var rows = document.querySelectorAll('tr.vr-book[data-parent="' + fs + '"]');
        if (!rows.length) return;
        // Decide toggle direction from the first row's current visibility.
        // display may be '' (visible) or 'none' (hidden); decide once to keep
        // all sibling rows in sync.
        var shouldOpen = (rows[0].style.display === 'none');
        rows.forEach(function(r) {{
          r.style.display = shouldOpen ? '' : 'none';
        }});
        el.textContent = shouldOpen ? '▼' : '▶';
      }});
    }});
  }}
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
    var all   = Array.from(tbody.rows).filter(function(r) {{ return r.cells.length > colIdx; }});
    var pinned = all.filter(function(r) {{ return r.dataset.pinned === '1'; }});
    var rows   = all.filter(function(r) {{ return r.dataset.pinned !== '1'; }});
    rows.sort(function(a, b) {{
      var va = _cellKey(a.cells[colIdx]), vb = _cellKey(b.cells[colIdx]);
      if (!isNaN(va.n) && !isNaN(vb.n)) return asc ? va.n - vb.n : vb.n - va.n;
      if (!isNaN(va.n)) return -1;
      if (!isNaN(vb.n)) return  1;
      return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    }});
    rows.forEach(function(r) {{ tbody.appendChild(r); }});
    pinned.forEach(function(r) {{ tbody.appendChild(r); }});
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
  // Top Posições — drill-down (Factor → Instrument → Fund)
  window.toggleTopPos = function(tr) {{
    var path = tr.getAttribute('data-tp-path');
    if (!path) return;
    var table = tr.closest('table'); if (!table) return;
    var direct = table.querySelectorAll('tr[data-tp-parent="' + path + '"]');
    var anyVisible = false;
    direct.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    direct.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    // Collapse grandchildren when collapsing
    if (!willOpen) {{
      var all = table.querySelectorAll('tr[data-tp-parent^="' + path + '|"]');
      all.forEach(function(r) {{ r.style.display = 'none'; }});
      // caret reset on closed children
      all.forEach(function(r) {{
        var c = r.querySelector('.tp-caret'); if (c) c.textContent = '▶';
      }});
    }}
    var caret = tr.querySelector('.tp-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // Breakdown por Fator — Líquido / Bruto toggle (net-of-bench vs gross)
  window.selectRfBrl = function(btn, mode) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-brl-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfBrl === mode);
    }});
    card.querySelectorAll('.rf-brl-body').forEach(function(v) {{
      v.style.display = (v.dataset.rfBrl === mode) ? '' : 'none';
    }});
  }};
  // Distribuição 252d — drill-down (fund row expands livro/rf children)
  window.toggleDistChildren = function(tr) {{
    var key = tr.getAttribute('data-dist-key');
    if (!key) return;
    var table = tr.closest('table'); if (!table) return;
    var children = table.querySelectorAll('tr.dist-row-child[data-dist-parent="' + key + '"]');
    var anyVisible = false;
    children.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    children.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    var caret = tr.querySelector('.dist-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // QUANT exposure — sort positions within each factor (respects hierarchy).
  // Clicking a header toggles direction on repeat click; clicking a different
  // header resets to its default direction.
  window.sortQuantExpoPositions = function(el, mode) {{
    var card = el.closest('.card');
    if (!card) return;
    // If the same header is clicked again, toggle direction between net_desc/asc,
    // or A-Z/Z-A, or gross desc/asc. For simplicity, we track "active" state;
    // repeat click on the active header flips direction where applicable.
    var wasActive = el.classList.contains('qexpo-sort-active');
    var newMode = mode;
    if (wasActive) {{
      if (mode === 'net_desc') newMode = 'net_asc';
      else if (mode === 'net_asc')  newMode = 'net_desc';
      else if (mode === 'gross')    newMode = 'gross_asc';
      else if (mode === 'gross_asc') newMode = 'gross';
      else if (mode === 'name')     newMode = 'name_desc';
      else if (mode === 'name_desc') newMode = 'name';
    }}
    // Clear active state across all headers; mark the clicked one.
    card.querySelectorAll('.qexpo-sort-th').forEach(function(th) {{
      th.classList.remove('qexpo-sort-active');
      var arrow = th.querySelector('.qexpo-sort-arrow');
      if (arrow) arrow.textContent = '';
    }});
    el.classList.add('qexpo-sort-active');
    var arrow = el.querySelector('.qexpo-sort-arrow');
    if (arrow) {{
      if (newMode.indexOf('asc') >= 0 || newMode === 'name_desc') arrow.textContent = '↑';
      else arrow.textContent = '↓';
    }}
    // Sort children
    var tbody = card.querySelector('.qexpo-view[data-qexpo-view="factor"] table tbody');
    if (!tbody) return;
    var parents = tbody.querySelectorAll('tr[data-qexpo-path]');
    parents.forEach(function(p) {{
      var path = p.getAttribute('data-qexpo-path');
      var children = Array.from(tbody.querySelectorAll('tr[data-qexpo-parent="' + path + '"]'));
      children.sort(function(a, b) {{
        var da = parseFloat(a.dataset.sortDelta || '0');
        var db = parseFloat(b.dataset.sortDelta || '0');
        var ga = parseFloat(a.dataset.sortAbs   || '0');
        var gb = parseFloat(b.dataset.sortAbs   || '0');
        if (newMode === 'gross')     return gb - ga;
        if (newMode === 'gross_asc') return ga - gb;
        if (newMode === 'net_desc')  return db - da;
        if (newMode === 'net_asc')   return da - db;
        if (newMode === 'name')      return (a.textContent || '').localeCompare(b.textContent || '');
        if (newMode === 'name_desc') return (b.textContent || '').localeCompare(a.textContent || '');
        return Math.abs(db) - Math.abs(da);  // abs_delta default
      }});
      var anchor = p;
      children.forEach(function(c) {{ anchor.after(c); anchor = c; }});
    }});
  }};
  // QUANT exposure — Por Fator drill-down (click factor to expand positions)
  window.toggleQuantExpoFactor = function(tr) {{
    var path = tr.getAttribute('data-qexpo-path');
    if (!path) return;
    var table = tr.closest('table'); if (!table) return;
    var children = table.querySelectorAll('tr[data-qexpo-parent="' + path + '"]');
    var anyVisible = false;
    children.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    children.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    var caret = tr.querySelector('.qexpo-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // QUANT exposure — Por Fator / Por Livro toggle
  window.selectQuantExpoView = function(btn, view) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.pa-view-toggle .pa-tgl').forEach(function(b) {{
      if (b.dataset.qexpoView)
        b.classList.toggle('active', b.dataset.qexpoView === view);
    }});
    card.querySelectorAll('.qexpo-view').forEach(function(v) {{
      v.style.display = (v.dataset.qexpoView === view) ? '' : 'none';
    }});
  }};
  // PA alerts — Por Tamanho / Por Fundo toggle (preserves grid layout)
  window.selectPaAlertSort = function(btn, mode) {{
    var container = btn.closest('.alerts-section') || document;
    container.querySelectorAll('.pa-alert-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.paSort === mode);
    }});
    container.querySelectorAll('.pa-alert-view').forEach(function(v) {{
      v.classList.toggle('pa-alert-view-hidden', v.dataset.paSort !== mode);
    }});
  }};
  // Exposure Map — Ambos/Real/Nominal factor filter. Bench bars always visible.
  window.selectRfView = function(btn, view) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-view-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfView === view);
    }});
    card.dataset.rfFactor = view;
    _rfApplyVisibility(card);
  }};
  // Exposure Map — Absoluto / Relativo mode toggle
  window.selectRfMode = function(btn, mode) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-mode-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfMode === mode);
    }});
    card.dataset.rfMode = mode;
    _rfApplyVisibility(card);
  }};
  function _rfApplyVisibility(card) {{
    var factor = card.dataset.rfFactor || 'both';
    var mode   = card.dataset.rfMode   || 'absoluto';
    card.querySelectorAll('.rf-expo-svg .rf-mode-group').forEach(function(g) {{
      g.style.display = (g.getAttribute('data-rf-mode') === mode) ? '' : 'none';
    }});
    card.querySelectorAll('.rf-expo-svg .rf-mode-group [data-factor]').forEach(function(el) {{
      var f = el.getAttribute('data-factor');
      if (f === 'bench') {{ el.style.display = ''; return; }}
      el.style.display = (factor === 'both' || f === factor) ? '' : 'none';
    }});
  }}
  // Expand/collapse the detail table under an Exposure Map card
  window.toggleRfTable = function(btn) {{
    var wrap = btn.nextElementSibling;
    if (!wrap) return;
    var open = wrap.style.display !== 'none';
    wrap.style.display = open ? 'none' : '';
    btn.textContent = (open ? '▸ Mostrar tabela' : '▾ Esconder tabela');
    btn.setAttribute('aria-expanded', open ? 'false' : 'true');
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
<noscript>
  <div style="background:#7c2d12;color:#fca5a5;border-bottom:1px solid #a16207;
              padding:10px 16px;font:13px/1.5 system-ui,sans-serif">
    <b>⚠ JavaScript desativado</b> — a navegação por abas e os toggles de cada card
    não vão funcionar. Se baixou este arquivo de um email no Windows e está vendo isto,
    provavelmente o sistema bloqueou os scripts por segurança. Para desbloquear:
    <b>clique com o botão direito no arquivo → Propriedades → marque "Desbloquear" → OK</b>,
    e abra de novo. Alternativamente, abra o arquivo no Chrome ou Edge.
    Todo o conteúdo abaixo continua legível como um relatório linear, mas as abas
    ficam todas expandidas.
  </div>
</noscript>
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
        fut_risk_idka  = ex.submit(fetch_risk_history_idka)
        fut_aum        = ex.submit(fetch_aum_history)
        fut_pm_pnl       = ex.submit(fetch_pm_pnl_history)
        fut_pm_book_var  = ex.submit(fetch_macro_pm_book_var,  DATA_STR)
        fut_expo       = ex.submit(fetch_macro_exposure, DATA_STR)
        fut_expo_d1    = ex.submit(fetch_macro_exposure, d1_str)
        fut_expo_d2    = ex.submit(fetch_macro_exposure, d2_str)
        fut_pnl_prod   = ex.submit(fetch_macro_pnl_products, DATA_STR)
        fut_pnl_prod_d1= ex.submit(fetch_macro_pnl_products, d1_str)
        fut_quant_sn   = ex.submit(fetch_quant_single_names, DATA_STR)
        fut_quant_sn_d1= ex.submit(fetch_quant_single_names, d1_str)
        fut_evo_sn     = ex.submit(fetch_evolution_single_names, DATA_STR)
        fut_evo_sn_d1  = ex.submit(fetch_evolution_single_names, d1_str)
        fut_evo_direct = ex.submit(fetch_evolution_direct_single_names, DATA_STR)
        fut_dist       = ex.submit(fetch_pnl_distribution, DATA_STR)
        fut_dist_prev  = ex.submit(fetch_pnl_distribution, d1_str)
        fut_dist_act   = ex.submit(fetch_pnl_actual_by_cut, DATA_STR)
        fut_pa         = ex.submit(fetch_pa_leaves, DATA_STR)
        fut_pa_daily   = ex.submit(fetch_pa_daily_per_product, DATA_STR)
        fut_cdi        = ex.submit(fetch_cdi_returns, DATA_STR)
        fut_ibov       = ex.submit(fetch_ibov_returns, DATA_STR)
        fut_idka_idx = {
            "IDKA_3Y":  ex.submit(fetch_idka_index_returns, "IDKA_IPCA_3A",  DATA_STR),
            "IDKA_10Y": ex.submit(fetch_idka_index_returns, "IDKA_IPCA_10A", DATA_STR),
        }
        fut_walb = {
            "IDKA_3Y":  ex.submit(fetch_idka_albatroz_weight, "IDKA IPCA 3Y FIRF",  DATA_STR),
            "IDKA_10Y": ex.submit(fetch_idka_albatroz_weight, "IDKA IPCA 10Y FIRF", DATA_STR),
        }
        fut_alb        = ex.submit(fetch_albatroz_exposure, DATA_STR)
        fut_quant_expo    = ex.submit(fetch_quant_exposure, DATA_STR)
        fut_quant_expo_d1 = ex.submit(fetch_quant_exposure, d1_str)
        fut_quant_var     = ex.submit(fetch_quant_var,      DATA_STR)
        fut_quant_var_d1  = ex.submit(fetch_quant_var,      d1_str)
        fut_evo_expo      = ex.submit(fetch_evolution_exposure, DATA_STR)
        fut_evo_expo_d1   = ex.submit(fetch_evolution_exposure, d1_str)
        fut_evo_var       = ex.submit(fetch_evolution_var,      DATA_STR)
        fut_evo_var_d1    = ex.submit(fetch_evolution_var,      d1_str)
        fut_evo_pnl_prod  = ex.submit(fetch_evolution_pnl_products, DATA_STR)
        fut_alb_d1     = ex.submit(fetch_albatroz_exposure, d1_str)
        fut_rf_expo = {
            "IDKA_3Y":   ex.submit(fetch_rf_exposure_map, "IDKA IPCA 3Y FIRF",  DATA_STR),
            "IDKA_10Y":  ex.submit(fetch_rf_exposure_map, "IDKA IPCA 10Y FIRF", DATA_STR),
            "ALBATROZ":  ex.submit(fetch_rf_exposure_map, "GALAPAGOS ALBATROZ FIRF LP", DATA_STR),
            "MACRO":     ex.submit(fetch_rf_exposure_map, "Galapagos Macro FIM", DATA_STR),
            "EVOLUTION": ex.submit(fetch_rf_exposure_map, "Galapagos Evolution FIC FIM CP",
                                   DATA_STR, True),
        }
        # Pre-warm NAV cache for today + D-1 so every _latest_nav() call hits memory.
        fut_navs    = ex.submit(fetch_all_latest_navs, DATA_STR)
        fut_navs_d1 = ex.submit(fetch_all_latest_navs, d1_str)
        fut_frontier   = ex.submit(fetch_frontier_mainboard, DATA_STR)
        fut_frontier_expo = ex.submit(fetch_frontier_exposure_data)
        fut_chg        = {
            short: ex.submit(fetch_fund_position_changes, short, DATA_STR, d1_str)
            for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER")
        }

    # ── Resolve results (sequential, with per-task fallback) ──────────────
    df_risk     = fut_risk.result()
    df_risk_raw = fut_risk_raw.result()
    try:
        df_risk_idka = fut_risk_idka.result()
    except Exception as e:
        print(f"  IDKA risk fetch failed ({e})")
        df_risk_idka = None
    df_aum      = fut_aum.result()
    df_pm_pnl   = fut_pm_pnl.result()
    try:
        pm_book_var = fut_pm_book_var.result()
    except Exception as e:
        print(f"  PM book-report VaR failed ({e})")
        pm_book_var = {}
    series      = build_series(df_risk, df_aum, df_risk_raw, df_risk_idka)
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
        ibov = fut_ibov.result()
    except Exception as e:
        print(f"  IBOV fetch failed ({e})")
        ibov = None

    idka_idx_ret = {}
    for k, fut in fut_idka_idx.items():
        try:
            idka_idx_ret[k] = fut.result()
        except Exception as e:
            print(f"  IDKA index returns ({k}) failed ({e})")
            idka_idx_ret[k] = None

    walb = {}
    for k, fut in fut_walb.items():
        try:
            walb[k] = fut.result()
        except Exception as e:
            print(f"  w_alb ({k}) failed ({e})")
            walb[k] = 0.0

    rf_expo_maps = {}
    for short_k, fut_k in fut_rf_expo.items():
        try:
            rf_expo_maps[short_k] = fut_k.result()
        except Exception as e:
            print(f"  RF exposure map ({short_k}) failed ({e})")
            rf_expo_maps[short_k] = None

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
        df_quant_expo, quant_expo_nav = fut_quant_expo.result()
    except Exception as e:
        print(f"  QUANT exposure fetch failed ({e})")
        df_quant_expo, quant_expo_nav = None, None
    try:
        df_quant_expo_d1, _ = fut_quant_expo_d1.result()
    except Exception as e:
        print(f"  QUANT D-1 exposure fetch failed ({e})")
        df_quant_expo_d1 = None
    try:
        df_quant_var = fut_quant_var.result()
    except Exception as e:
        print(f"  QUANT VaR fetch failed ({e})")
        df_quant_var = None
    try:
        df_quant_var_d1 = fut_quant_var_d1.result()
    except Exception as e:
        print(f"  QUANT D-1 VaR fetch failed ({e})")
        df_quant_var_d1 = None

    # EVOLUTION exposure (look-through, 3-level card)
    try:
        df_evo_expo, evo_expo_nav = fut_evo_expo.result()
    except Exception as e:
        print(f"  EVOLUTION exposure fetch failed ({e})")
        df_evo_expo, evo_expo_nav = None, None
    try:
        df_evo_expo_d1, _ = fut_evo_expo_d1.result()
    except Exception as e:
        print(f"  EVOLUTION D-1 exposure fetch failed ({e})")
        df_evo_expo_d1 = None
    try:
        df_evo_var = fut_evo_var.result()
    except Exception as e:
        print(f"  EVOLUTION VaR fetch failed ({e})")
        df_evo_var = None
    try:
        df_evo_var_d1 = fut_evo_var_d1.result()
    except Exception as e:
        print(f"  EVOLUTION D-1 VaR fetch failed ({e})")
        df_evo_var_d1 = None
    try:
        df_evo_pnl_prod = fut_evo_pnl_prod.result()
    except Exception as e:
        print(f"  EVOLUTION PnL per product fetch failed ({e})")
        df_evo_pnl_prod = None

    try:
        df_evo_direct, _evo_direct_nav, _ = fut_evo_direct.result()
    except Exception as e:
        print(f"  EVOLUTION direct fetch failed ({e})")
        df_evo_direct = None

    try:
        df_frontier = fut_frontier.result()
    except Exception as e:
        print(f"  Frontier LO fetch failed ({e})")
        df_frontier = None

    try:
        frontier_bvar = compute_frontier_bvar_hs(df_frontier, DATA_STR) if df_frontier is not None else None
    except Exception as e:
        print(f"  Frontier BVaR (HS) failed ({e})")
        frontier_bvar = None

    try:
        df_frontier_ibov, df_frontier_smll, df_frontier_sectors = fut_frontier_expo.result()
    except Exception as e:
        print(f"  Frontier exposure fetch failed ({e})")
        df_frontier_ibov = df_frontier_smll = df_frontier_sectors = pd.DataFrame()

    try:
        vol_regime_map = compute_portfolio_vol_regime(dist_map or {})
    except Exception as e:
        print(f"  Vol regime failed ({e})")
        vol_regime_map = {}

    position_changes = {}
    for short, fut in fut_chg.items():
        try:
            position_changes[short] = fut.result()
        except Exception as e:
            print(f"  {short} position changes failed ({e})")
            position_changes[short] = None

    # Resolve NAV pre-warms (side effect is populating _NAV_CACHE)
    try:
        fut_navs.result()
        fut_navs_d1.result()
    except Exception as e:
        print(f"  NAV warmup failed ({e})")

    print(f"  ...fetches done in {time.time()-t0:.1f}s")

    # ── Data manifest: what landed and what is stale/missing ─────────────────
    _var_dates = {}
    for td, cfg in ALL_FUNDS.items():
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
        "quant_expo_ok":  df_quant_expo is not None and not df_quant_expo.empty,
        "evo_expo_ok":    df_evo_expo is not None and not df_evo_expo.empty,
        # Stop monitor
        "stop_ok":        bool(pm_margem),
        "stop_has_pnl":   any(abs(pm_margem.get(pm, STOP_BASE) - STOP_BASE) > 1 for pm in pm_margem),
        # Detail for quality tab
        "expo_rows":      len(df_expo) if df_expo is not None and not df_expo.empty else 0,
        "quant_sn_rows":  len(df_quant_sn) if df_quant_sn is not None else 0,
        "evo_sn_rows":    len(df_evo_sn)   if df_evo_sn   is not None else 0,
        "alb_expo_rows":  len(df_alb_expo) if df_alb_expo is not None and not df_alb_expo.empty else 0,
        "quant_expo_rows": len(df_quant_expo) if df_quant_expo is not None and not df_quant_expo.empty else 0,
        "evo_expo_rows":  len(df_evo_expo) if df_evo_expo is not None and not df_evo_expo.empty else 0,
        "stop_pms":       sorted(pm_margem.keys()) if pm_margem else [],
        "stop_pms_pnl":   [pm for pm, v in (pm_margem or {}).items() if abs(v - STOP_BASE) > 1],
    }

    html = build_html(series, stop_hist, df_today, df_expo, df_var, macro_aum, df_expo_d1, df_var_d1,
                      df_pnl_prod=df_pnl_prod, pm_margem=pm_margem,
                      df_quant_sn=df_quant_sn, quant_nav=quant_nav, quant_legs=quant_legs,
                      df_evo_sn=df_evo_sn, evo_nav=evo_nav, evo_legs=evo_legs,
                      df_evo_direct=df_evo_direct,
                      df_pa=df_pa, cdi=cdi, ibov=ibov, df_pa_daily=df_pa_daily,
                      idka_idx_ret=idka_idx_ret, walb=walb,
                      df_alb_expo=df_alb_expo, alb_nav=alb_nav,
                      df_quant_expo=df_quant_expo, quant_expo_nav=quant_expo_nav,
                      df_quant_expo_d1=df_quant_expo_d1,
                      df_quant_var=df_quant_var, df_quant_var_d1=df_quant_var_d1,
                      df_evo_expo=df_evo_expo, evo_expo_nav=evo_expo_nav,
                      df_evo_expo_d1=df_evo_expo_d1,
                      df_evo_var=df_evo_var, df_evo_var_d1=df_evo_var_d1,
                      df_evo_pnl_prod=df_evo_pnl_prod,
                      rf_expo_maps=rf_expo_maps,
                      df_frontier=df_frontier,
                      frontier_bvar=frontier_bvar,
                      df_frontier_ibov=df_frontier_ibov,
                      df_frontier_smll=df_frontier_smll,
                      df_frontier_sectors=df_frontier_sectors,
                      position_changes=position_changes,
                      dist_map=dist_map, dist_map_prev=dist_map_prev, dist_actuals=dist_actuals,
                      vol_regime_map=vol_regime_map,
                      pm_book_var=pm_book_var,
                      expo_date_label=expo_date_label, data_manifest=data_manifest)
    out  = OUT_DIR / f"{DATA_STR}_risk_monitor.html"
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out}")

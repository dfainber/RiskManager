"""
data_fetch.py — DB fetch layer for the Risk Monitor kit.

All fetch_* functions (and their private helpers) sourced from
q_models / LOTE45 / public / frontier schemas on GLPG-DB01. No
HTML rendering, no UI logic — pure data in, pure dataframes out.

Consumers (generate_risk_report.py orchestrator + future
html_builders modules) import the fetches they need from here.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from glpg_fetch import read_sql
from risk_runtime import DATA_STR, DATE_1Y
from risk_config import (
    ALL_FUNDS, FUNDS, RAW_FUNDS, IDKA_FUNDS, RF_BENCH_FUNDS,
    _PM_LIVRO, _ETF_TO_LIST,
    _EXCL_PRIM,
    _QUANT_BOOK_FACTOR,
    _RF_FACTOR_MAP, _RF_BUCKETS,
    _EVO_LIVRO_EXTRA_STRATEGY,
    _FUND_DESK_FOR_EXPO,
    _PRODCLASS_TO_FACTOR,
)
from db_helpers import _parse_rf, _parse_pm, _prev_bday, _latest_nav, _require_nav


def fetch_pm_pnl_history() -> pd.DataFrame:
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
    # DATE_TRUNC('month', ...) vem do Postgres como TIMESTAMPTZ; se parsearmos
    # com utc=True + tz_localize(None) o horário fica defasado em +3h (2026-04-01
    # UTC virou 03:00 wall-clock naive). Converter p/ BRT antes de strip preserva
    # o timestamp 00:00:00 local → match com `cur_mes = Timestamp(data).to_period('M').to_timestamp()`.
    s = pd.to_datetime(df["mes"], utc=True)
    df["mes"] = s.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
    return df


def fetch_pm_book_pnl_history() -> pd.DataFrame:
    """Per-month PnL by (LIVRO, BOOK) for MACRO PMs — drill-down for stop history modal.
       Returns columns: mes (datetime, normalized BRT), LIVRO, BOOK, pnl_mes_bps (float)."""
    q = f"""
    SELECT DATE_TRUNC('month', "DATE") AS mes,
           "LIVRO", "BOOK",
           SUM("DIA") * 10000 AS pnl_mes_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'MACRO'
      AND "DATE" >= DATE '2025-01-01'
      AND "DATE" <= DATE '{DATA_STR}'
      AND "LIVRO" IN ('CI','Macro_LF','Macro_JD','Macro_RJ')
    GROUP BY DATE_TRUNC('month', "DATE"), "LIVRO", "BOOK"
    ORDER BY "LIVRO", mes, "BOOK"
    """
    df = read_sql(q)
    if df.empty:
        return df
    s = pd.to_datetime(df["mes"], utc=True)
    df["mes"] = s.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
    df["pnl_mes_bps"] = df["pnl_mes_bps"].astype(float)
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


def fetch_risk_history() -> pd.DataFrame:
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


def fetch_risk_history_raw() -> pd.DataFrame:
    """Fetch VaR/stress from LOTE_FUND_STRESS (product-level) for RAW_FUNDS, summed to fund level.
    Filter TREE='Main' to avoid 3× double-count on BALTRA (which has 3 TREE views:
    Main, Main_Macro_Gestores, Main_Macro_Ativos — each containing the full fund).
    Other RAW_FUNDS only have TREE='Main', so the filter is no-op for them."""
    tds = ", ".join(f"'{td}'" for td in RAW_FUNDS)
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE",
           SUM("PVAR1DAY")        AS var_total,
           SUM("SPECIFIC_STRESS") AS spec_stress,
           SUM("MACRO_STRESS")    AS macro_stress
    FROM "LOTE45"."LOTE_FUND_STRESS"
    WHERE "VAL_DATE" >= DATE '{DATE_1Y.date()}'
      AND "TRADING_DESK" IN ({tds})
      AND "TREE" = 'Main'
    GROUP BY "TRADING_DESK", "VAL_DATE"
    ORDER BY "TRADING_DESK", "VAL_DATE"
    """
    df = read_sql(q)
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).astype("datetime64[us]")
    return df


def fetch_risk_history_idka() -> pd.DataFrame:
    """BVaR (RELATIVE_VAR_PCT) + VaR (ABSOLUTE_VAR_PCT) history for IDKA funds.
       Source: LOTE45.LOTE_PARAMETRIC_VAR_TABLE. Values are decimal fractions
       (0.029 = 2.9% of NAV). Positions summed to fund level.

       IMPORTANT: The engine stores multiple "views" per primitive (filtered by
       different BOOKS subsets — {*} = wildcard, individual books, explicit
       lists). Summing all views triplicates (or more) the true fund VaR.
       Filter to BOOKS::text = '{*}' to get exactly the fund-level aggregate.
       See docs/IDKA_VAR_EXPLORATION.md for the full investigation.
    """
    tds = ", ".join(f"'{td}'" for td in IDKA_FUNDS)
    q = f"""
    SELECT "TRADING_DESK", "VAL_DATE",
           SUM("RELATIVE_VAR_PCT") AS bvar_pct_raw,
           SUM("ABSOLUTE_VAR_PCT") AS var_pct_raw
    FROM "LOTE45"."LOTE_PARAMETRIC_VAR_TABLE"
    WHERE "VAL_DATE" >= DATE '{DATE_1Y.date()}'
      AND "TRADING_DESK" IN ({tds})
      AND "BOOKS"::text = '{{*}}'
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


# Index identifiers in public.ECO_INDEX for benchmark return series.
_BENCH_INDEX_MAP = {
    "IMA-B":  ("IMA-B",  "INDEX"),
    # Add more bench indices here as RF_BENCH_FUNDS grows (e.g. IDA-Infra, IMA-S).
}


def fetch_risk_history_rf_bench() -> pd.DataFrame:
    """BVaR (active-return σ × 1.645) + abs VaR for RF_BENCH_FUNDS funds.

    For funds whose BVaR is NOT in LOTE_PARAMETRIC_VAR_TABLE (e.g. Nazca):
      • Pull daily NAV_SHARE.SHARE → fund returns
      • Pull benchmark INDEX from ECO_INDEX → bench returns
      • Compute active return per day, rolling 252d BVaR_95_1d = 1.645 × σ
      • Pull abs VaR from LOTE_FUND_STRESS (TREE='Main') / NAV — secondary slot

    Returns same shape as fetch_risk_history_idka:
      TRADING_DESK, VAL_DATE, var_pct (BVaR%), stress_pct (abs VaR%)
    """
    if not RF_BENCH_FUNDS:
        return pd.DataFrame(columns=["TRADING_DESK", "VAL_DATE", "var_pct", "stress_pct"])

    out_frames = []
    for td, cfg in RF_BENCH_FUNDS.items():
        bench = cfg.get("benchmark")
        idx_key = _BENCH_INDEX_MAP.get(bench)
        if idx_key is None:
            continue
        instr, field = idx_key

        # Fund NAV_SHARE → daily returns
        nav = read_sql(f"""
            SELECT "VAL_DATE", "SHARE", "NAV"
            FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
            WHERE "TRADING_DESK" = '{td}'
            ORDER BY "VAL_DATE"
        """)
        if nav.empty:
            continue
        nav["VAL_DATE"] = pd.to_datetime(nav["VAL_DATE"]).astype("datetime64[us]")
        nav["ret_fund"] = nav["SHARE"].pct_change()

        # Benchmark INDEX → daily returns
        bench_df = read_sql(f"""
            SELECT "DATE" AS "VAL_DATE", "VALUE" AS bench_idx
            FROM public."ECO_INDEX"
            WHERE "INSTRUMENT" = '{instr}' AND "FIELD" = '{field}'
            ORDER BY "DATE"
        """)
        if bench_df.empty:
            continue
        bench_df["VAL_DATE"] = pd.to_datetime(bench_df["VAL_DATE"]).astype("datetime64[us]")
        bench_df["ret_bench"] = bench_df["bench_idx"].pct_change()

        merged = nav.merge(bench_df[["VAL_DATE", "ret_bench"]], on="VAL_DATE", how="inner")
        merged = merged.dropna(subset=["ret_fund", "ret_bench"])
        merged["ret_active"] = merged["ret_fund"] - merged["ret_bench"]
        # Rolling 252d BVaR 95% 1d as % of NAV.
        merged["var_pct"] = -1.645 * merged["ret_active"].rolling(252, min_periods=63).std() * 100.0
        merged = merged.dropna(subset=["var_pct"])

        # Abs VaR from LOTE_FUND_STRESS (TREE='Main'), normalized by NAV.
        abs_var = read_sql(f"""
            SELECT "VAL_DATE", SUM("PVAR1DAY") AS var_total
            FROM "LOTE45"."LOTE_FUND_STRESS"
            WHERE "TRADING_DESK" = '{td}'
              AND "VAL_DATE" >= DATE '{DATE_1Y.date()}'
              AND "TREE" = 'Main'
            GROUP BY "VAL_DATE"
            ORDER BY "VAL_DATE"
        """)
        if not abs_var.empty:
            abs_var["VAL_DATE"] = pd.to_datetime(abs_var["VAL_DATE"]).astype("datetime64[us]")
            merged = merged.merge(abs_var, on="VAL_DATE", how="left")
            merged["stress_pct"] = -merged["var_total"] / merged["NAV"] * 100.0
        else:
            merged["stress_pct"] = 0.0

        merged["TRADING_DESK"] = td
        out_frames.append(merged[["TRADING_DESK", "VAL_DATE", "var_pct", "stress_pct"]])

    if not out_frames:
        return pd.DataFrame(columns=["TRADING_DESK", "VAL_DATE", "var_pct", "stress_pct"])
    return pd.concat(out_frames, ignore_index=True)


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


def fetch_aum_history() -> pd.DataFrame:
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


def fetch_idka_active_series(desk: str, idka_idx_name: str,
                               date_str: str = DATA_STR,
                               window_days: int = 252) -> np.ndarray:
    """Realized daily active return (fund − benchmark) in bps of NAV for an IDKA fund.

    Fund return: pct_change da SHARE em `LOTE_TRADING_DESKS_NAV_SHARE`.
    Benchmark:   pct_change do índice IDKA em `public.ECO_INDEX` (FIELD='INDEX').
    Retorna np.array(len ≤ window_days) de active returns × 10000 (bps de NAV).
    Array vazio se dados insuficientes.

    NOTA: esta série usa o BENCHMARK (ponto), não a Replication.
    Pra Replication (daily DV-match) ver `fetch_idka_replication_series`.
    """
    q_fund = f"""
    SELECT "VAL_DATE", "SHARE" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "TRADING_DESK" = '{desk}'
      AND "VAL_DATE" > DATE '{date_str}' - INTERVAL '500 days'
      AND "VAL_DATE" <= DATE '{date_str}'
    ORDER BY "VAL_DATE"
    """
    q_bench = f"""
    SELECT "DATE" AS val_date, "VALUE" FROM "public"."ECO_INDEX"
    WHERE "INSTRUMENT" = '{idka_idx_name}' AND "FIELD" = 'INDEX'
      AND "DATE" > DATE '{date_str}' - INTERVAL '500 days'
      AND "DATE" <= DATE '{date_str}'
    ORDER BY "DATE"
    """
    try:
        f = read_sql(q_fund); b = read_sql(q_bench)
    except Exception:
        return np.array([])
    if f.empty or b.empty:
        return np.array([])
    f["VAL_DATE"] = pd.to_datetime(f["VAL_DATE"])
    b["val_date"] = pd.to_datetime(b["val_date"])
    f = f.set_index("VAL_DATE")["SHARE"].astype(float).sort_index()
    b = b.set_index("val_date")["VALUE"].astype(float).sort_index()
    rf = f.pct_change()
    rb = b.pct_change()
    active = (rf - rb).dropna()
    if len(active) < 10:
        return np.array([])
    return (active.tail(window_days).to_numpy() * 10000).astype(float)


_NTNB_SEMI_COUPON = (1.06) ** 0.5 - 1  # ≈ 0.029563 (semi-annual coupon, NTN-B 6% a.a. effective)

_VNA_NTNB_CACHE: dict = {}  # date_str → VNA value (or None on miss)


def _get_vna_ntnb(date_str: str) -> float | None:
    """VNA_NTNB on date_str, or nearest published date within ±5 calendar days.
    Used to convert the 6% a.a. semi-coupon rate into the actual BRL cash flow
    (coupon = semi_coupon × VNA), which differs from rate × clean price when
    the bond trades away from par. Falls back to None if ECO_INDEX is missing
    a publication near the requested date."""
    if date_str in _VNA_NTNB_CACHE:
        return _VNA_NTNB_CACHE[date_str]
    try:
        df = read_sql(f"""
            SELECT "VALUE" FROM public."ECO_INDEX"
            WHERE "INSTRUMENT"='VNA_NTNB' AND "FIELD"='INDEX'
              AND "DATE" BETWEEN DATE '{date_str}' - INTERVAL '5 days'
                           AND DATE '{date_str}' + INTERVAL '5 days'
            ORDER BY ABS("DATE" - DATE '{date_str}') ASC, "DATE" ASC
            LIMIT 1
        """)
    except Exception:
        _VNA_NTNB_CACHE[date_str] = None
        return None
    if df.empty:
        _VNA_NTNB_CACHE[date_str] = None
        return None
    val = float(df["VALUE"].iloc[0])
    _VNA_NTNB_CACHE[date_str] = val
    return val


def _ntnb_total_return_pct_change(prices: pd.Series,
                                   maturity: pd.Timestamp | None = None) -> pd.Series:
    """Adjust ANBIMA clean-price pct_change to NTN-B total-return.

    Why: ANBIMA's UNIT_PRICE is the clean (ex-coupon) price. On NTN-B coupon
    dates the clean price drops by ~ the semi-coupon, producing a spurious
    negative return that does not exist in the IDKA index (which reinvests
    coupons internally).

    Coupon dates: NTN-B pays coupons every 6 months on its maturity-anniversary
    (e.g. NTN-B 2030-08-15 → coupons every Feb 15 / Aug 15). Without the
    `maturity` arg we fall back to the most common pair (May 15 / Nov 15).

    Fix: at each coupon transition captured in the series (the prior quote is
    strictly before the coupon date), add the BRL coupon as a fraction of the
    pre-coupon clean price: r_TR = r_clean + (semi_coupon × VNA(cup)) / P_prev.
    Falls back to additive rate-based correction (r_clean + semi_coupon) if
    VNA is unavailable.
    """
    if prices is None or prices.empty:
        return pd.Series(dtype=float)
    s = prices.sort_index()
    rets = s.pct_change()
    idx = s.index
    if len(idx) < 2:
        return rets
    if maturity is not None:
        m, d = int(maturity.month), int(maturity.day)
        m2 = ((m + 6 - 1) % 12) + 1
        anniv_md = [(m, d), (m2, d)]
    else:
        anniv_md = [(5, 15), (11, 15)]
    years = sorted(set(d.year for d in idx))
    for y in years:
        for month, day in anniv_md:
            try:
                target = pd.Timestamp(year=y, month=month, day=day)
            except ValueError:
                continue  # e.g. Feb 30 — shouldn't happen with NTN-B but be safe
            geq = idx[idx >= target]
            lt  = idx[idx <  target]
            if len(geq) == 0 or len(lt) == 0:
                continue
            first = geq[0]
            if (first - target) > pd.Timedelta(days=7):
                continue
            if not pd.notna(rets.loc[first]):
                continue
            prev_date = lt[-1]
            p_prev = float(s.loc[prev_date])
            if p_prev <= 0:
                continue
            vna = _get_vna_ntnb(target.strftime("%Y-%m-%d"))
            coupon_pct = (_NTNB_SEMI_COUPON * vna) / p_prev if (vna is not None and vna > 0) \
                else _NTNB_SEMI_COUPON
            rets.loc[first] = float(rets.loc[first]) + coupon_pct
    return rets


def _compute_idka_bench_replication(date_str: str, target_anos: int, tenour_du: int) -> pd.DataFrame:
    """Replicação do bench IDKA via DV-match — melhor carteira NTN-B que replica o ponto.
       (Metodologia igual IDKA_TABLES_GRAPHS.py, Pedro Igor.)

       **Dois conceitos distintos:**
       1. Benchmark no ponto = índice IDKA teórico (target_dm = anos / (1+y)).
          É um ÚNICO ponto de duração, referência abstrata.
       2. Bench replication (esta função) = 1-2 NTN-Bs que replicam o target_dm
          via weighted modified-duration matching. É a carteira concreta
          "best-fit" que o fundo teria de montar pra estar em cima do bench.

       Ambos têm a MESMA ANO_EQ total (target_dm). O que muda é a composição:
       a replicação distribui em tenors reais, o "ponto" não.
       Útil pro historical spread: a replication dá uma série temporal
       reconstruível via preços ANBIMA; o ponto vem do ECO_INDEX direto.
       Retorna DataFrame com 1-2 NTN-Bs que replicam o índice, com:
         INSTRUMENT, EXPIRATION_DATE, BDAYS, TIR (%), MD, W (peso 0-1), ANO_EQ_BENCH (= W × MD)

       target_anos: 3 (IDKA 3A) ou 10 (IDKA 10A)
       tenour_du:   756 (=3y) ou 2520 (=10y) — usado pra pegar TIR tenor do BR_YIELDS

       Fórmula:
         y = TIR NTN-B no tenor (from q_models.BR_YIELDS)
         target_dm = target_anos / (1 + y/100)
         MD(bond) = (DURATION_days / 252) / (1 + TIR/100)
         Acha 2 NTN-Bs straddling target_dm (maior <= target <= menor)
         Solve pesos: w_s*MD_s + w_l*MD_l = target_dm, w_s + w_l = 1
         ANO_EQ_BENCH = W × MD (positivo; sum ≈ target_dm)
       Retorna DataFrame vazio se qualquer fetch falhar."""
    try:
        y_df = read_sql(f"""
            SELECT "YIELD" FROM q_models."BR_YIELDS"
            WHERE "TYPE" = 'NTN-B' AND "TENOUR" = {tenour_du} AND "DATE" = DATE '{date_str}'
        """)
        if y_df.empty:
            return pd.DataFrame()
        y = float(y_df["YIELD"].iloc[0])
        target_dm = target_anos / (1 + y / 100.0)

        bonds = read_sql(f"""
            SELECT m."INSTRUMENT", m."EXPIRATION_DATE",
                   p."UNIT_PRICE", p."BUY_RATE" AS tir_pct, p."DURATION" AS dur_days
            FROM "public"."PRICES_ANBIMA_BR_PUBLIC_BONDS" p
            JOIN "public"."MAPS_ANBIMA_BR_PUBLIC_BONDS" m
              ON m."BR_PUBLIC_BONDS_KEY" = p."BR_PUBLIC_BONDS_KEY"
            WHERE p."REFERENCE_DATE" = DATE '{date_str}'
              AND m."INDEXER" = 'IPCA'
              AND m."EXPIRATION_DATE" > DATE '{date_str}'
              AND p."BUY_RATE" IS NOT NULL
              AND p."DURATION" IS NOT NULL
        """)
        if bonds.empty:
            return pd.DataFrame()
        bonds["EXPIRATION_DATE"] = pd.to_datetime(bonds["EXPIRATION_DATE"])
        bonds["BDAYS"] = (bonds["dur_days"] / 1).astype(int)  # ANBIMA DURATION is already in days → Macaulay
        # MD = Macaulay / (1+y) with Macaulay in years (= dur_days / 252)
        bonds["MD"] = (bonds["dur_days"].astype(float) / 252.0) / (1 + bonds["tir_pct"].astype(float) / 100.0)
        bonds = bonds[bonds["MD"] > 0].copy()
        if bonds.empty:
            return pd.DataFrame()

        above = bonds[bonds["MD"] >= target_dm]
        below = bonds[bonds["MD"] <= target_dm]
        if above.empty or below.empty:
            return pd.DataFrame()
        long_row  = above.loc[above["MD"].idxmin()].copy()      # menor MD acima do target
        short_row = below.loc[below["MD"].idxmax()].copy()      # maior MD abaixo do target

        if long_row["INSTRUMENT"] == short_row["INSTRUMENT"]:
            long_row["W"] = 1.0
            selected = pd.DataFrame([long_row])
        else:
            dm_s, dm_l = float(short_row["MD"]), float(long_row["MD"])
            w_s = (dm_l - target_dm) / (dm_l - dm_s)
            w_l = (target_dm - dm_s) / (dm_l - dm_s)
            long_row["W"]  = w_l
            short_row["W"] = w_s
            selected = pd.DataFrame([short_row, long_row])

        selected["ANO_EQ_BENCH"] = selected["W"].astype(float) * selected["MD"].astype(float)
        selected = selected[["INSTRUMENT", "EXPIRATION_DATE", "BDAYS", "tir_pct", "MD", "W", "ANO_EQ_BENCH"]].copy()
        selected = selected.rename(columns={"tir_pct": "TIR"})
        selected["TARGET_DM"] = target_dm
        selected["YIELD_TEN"] = y
        selected["TARGET_ANOS"] = target_anos
        selected = selected.sort_values("BDAYS").reset_index(drop=True)
        return selected
    except Exception as e:
        print(f"  [IDKA bench proxy] failed for {target_anos}y: {e}")
        return pd.DataFrame()


_IDKA_REP_CACHE = Path(__file__).parent / "data" / "idka_replication_cache.json"


def _load_idka_rep_cache() -> dict:
    if not _IDKA_REP_CACHE.exists():
        return {}
    try:
        return json.loads(_IDKA_REP_CACHE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_idka_rep_cache(cache: dict) -> None:
    try:
        _IDKA_REP_CACHE.parent.mkdir(parents=True, exist_ok=True)
        _IDKA_REP_CACHE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"  [IDKA rep cache] save failed: {e}")


def _ntnb_1d_total_return(p_prev: float, p_t: float,
                           t_prev: pd.Timestamp, t: pd.Timestamp,
                           maturity: pd.Timestamp) -> float:
    """1d total return of a single NTN-B over (t_prev, t]. On a coupon-day-
    crossing interval, adds (semi_coupon × VNA) / P_prev — the actual BRL
    coupon as a fraction of the pre-coupon clean price. Falls back to additive
    rate-based correction if VNA is unavailable."""
    clean_ret = p_t / p_prev - 1.0
    m, d = int(maturity.month), int(maturity.day)
    m2 = ((m + 6 - 1) % 12) + 1
    for y in range(t_prev.year, t.year + 1):
        for month, day in ((m, d), (m2, d)):
            try:
                cup = pd.Timestamp(year=y, month=month, day=day)
            except ValueError:
                continue
            if t_prev < cup <= t:
                vna = _get_vna_ntnb(cup.strftime("%Y-%m-%d"))
                if vna is None or vna <= 0 or p_prev <= 0:
                    return clean_ret + _NTNB_SEMI_COUPON
                return clean_ret + (_NTNB_SEMI_COUPON * vna) / p_prev
    return clean_ret


def _compute_single_replication_return(t: pd.Timestamp, t_prev: pd.Timestamp,
                                         target_anos: int, tenour_du: int) -> float | None:
    """1d total return (decimal) of a constant-DV NTN-B-only replication
    held over (t_prev, t]. Solves at t_prev's NTN-B universe, marks to t.
    Returns None if data missing."""
    rep = _compute_idka_bench_replication(t_prev.strftime("%Y-%m-%d"), target_anos, tenour_du)
    if rep.empty:
        return None
    inst_list = ", ".join(f"'{i}'" for i in rep["INSTRUMENT"].tolist())
    q = f"""
    SELECT p."REFERENCE_DATE" AS dt, m."INSTRUMENT", p."UNIT_PRICE"
    FROM "public"."PRICES_ANBIMA_BR_PUBLIC_BONDS" p
    JOIN "public"."MAPS_ANBIMA_BR_PUBLIC_BONDS" m
      ON m."BR_PUBLIC_BONDS_KEY" = p."BR_PUBLIC_BONDS_KEY"
    WHERE m."INSTRUMENT" IN ({inst_list})
      AND p."REFERENCE_DATE" IN (DATE '{t_prev.strftime("%Y-%m-%d")}', DATE '{t.strftime("%Y-%m-%d")}')
    """
    try:
        df = read_sql(q)
    except Exception:
        return None
    if df.empty:
        return None
    df["dt"] = pd.to_datetime(df["dt"])
    rep_idx = rep.set_index("INSTRUMENT")
    total = 0.0
    for inst in rep_idx.index:
        w = float(rep_idx.at[inst, "W"])
        mat = pd.to_datetime(rep_idx.at[inst, "EXPIRATION_DATE"])
        sub = df[df["INSTRUMENT"] == inst].set_index("dt")["UNIT_PRICE"].astype(float)
        if t_prev not in sub.index or t not in sub.index:
            return None
        total += w * _ntnb_1d_total_return(float(sub[t_prev]), float(sub[t]), t_prev, t, mat)
    return total


def _compute_idka_replication_returns(target_anos: int, tenour_du: int,
                                       scenario_dates) -> pd.Series:
    """Engine-style replication: for each scenario date t, return the 1d TR (bps)
    of a constant-DV NTN-B-only portfolio solved at t_prev's universe.
    Independent of the fund's actual position. Cached per (target_anos, date)
    to JSON — once a date is computed it never changes (depends only on
    historical data), so cold-start cost (~10–20s for 252-day backfill)
    is paid once and daily runs only fill the new tail."""
    cache = _load_idka_rep_cache()
    sub = cache.setdefault(str(target_anos), {})

    sorted_dates = sorted(pd.to_datetime(list(scenario_dates)))
    out: dict = {}
    dirty = False

    for i, t in enumerate(sorted_dates):
        ts = t.strftime("%Y-%m-%d")
        if ts in sub:
            out[t] = sub[ts]
            continue
        if i > 0:
            t_prev = sorted_dates[i - 1]
        else:
            try:
                t_prev = pd.Timestamp(_prev_bday(ts))
            except Exception:
                continue
        r = _compute_single_replication_return(t, t_prev, target_anos, tenour_du)
        if r is not None:
            sub[ts] = r * 10_000.0
            out[t] = sub[ts]
            dirty = True

    if dirty:
        _save_idka_rep_cache(cache)

    if not out:
        return pd.Series(dtype=float)
    return pd.Series(out).sort_index()


def fetch_idka_hs_replication_series(portfolio_name: str, target_anos: int, tenour_du: int,
                                      date_str: str = DATA_STR) -> np.ndarray:
    """HS active return for IDKA vs constant-DV NTN-B engine replication.

    Replication: at each historical scenario date t, a NTN-B-only constant-DV
    portfolio is solved at t_prev's universe and marked to t. The fund's actual
    position is not used (W cancels out of the replication side). Cached to
    data/idka_replication_cache.json — once a date is computed it never changes.
    Returns bps of NAV; empty array if data unavailable.
    """
    q_hs = f"""
    SELECT "DATE_SYNTHETIC_POSITION" AS dt, "W"
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}'
      AND "PORTIFOLIO" = '{portfolio_name}'
    ORDER BY dt
    """
    try:
        df_hs = read_sql(q_hs)
    except Exception:
        return np.array([])
    if df_hs.empty:
        return np.array([])
    df_hs["dt"] = pd.to_datetime(df_hs["dt"])

    rep_s = _compute_idka_replication_returns(target_anos, tenour_du, df_hs["dt"].tolist())
    if rep_s.empty:
        return np.array([])

    hs_s = df_hs.set_index("dt")["W"].astype(float).sort_index()
    aligned = hs_s.to_frame("W").join(rep_s.rename("rep"), how="inner")
    if len(aligned) < 30:
        return np.array([])
    return (aligned["W"] - aligned["rep"]).to_numpy()


def fetch_idka_hs_spread_series(portfolio_name: str, idx_name: str,
                                 target_anos: int, tenour_du: int,
                                 date_str: str = DATA_STR) -> np.ndarray:
    """Spread = replication − benchmark (bps) over the HS scenario dates.

    Replication: constant-DV NTN-B engine (same source as
    fetch_idka_hs_replication_series). Benchmark: pct_change of the IDKA index
    (ECO_INDEX, FIELD='INDEX'). Positive = NTN-B-only replication outperformed
    the actual IDKA index. Pure replication-strategy tracking error — no fund
    position dependency on either side."""
    q_dates = f"""
    SELECT "DATE_SYNTHETIC_POSITION" AS dt
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}' AND "PORTIFOLIO" = '{portfolio_name}'
    ORDER BY dt
    """
    try:
        df_dates = read_sql(q_dates)
    except Exception:
        return np.array([])
    if df_dates.empty:
        return np.array([])
    df_dates["dt"] = pd.to_datetime(df_dates["dt"])
    dt_min = df_dates["dt"].min().strftime("%Y-%m-%d")

    rep_s = _compute_idka_replication_returns(target_anos, tenour_du, df_dates["dt"].tolist())
    if rep_s.empty:
        return np.array([])

    q_bench = f"""
    SELECT "DATE" AS dt, "VALUE" FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = '{idx_name}' AND "FIELD" = 'INDEX'
      AND "DATE" >= DATE '{dt_min}' AND "DATE" <= DATE '{date_str}'
    ORDER BY dt
    """
    try:
        df_b = read_sql(q_bench)
    except Exception:
        return np.array([])
    if df_b.empty:
        return np.array([])
    df_b["dt"] = pd.to_datetime(df_b["dt"])
    bench_bps = (df_b.set_index("dt")["VALUE"].astype(float)
                 .sort_index().pct_change().dropna() * 10_000)

    aligned = bench_bps.to_frame("bench").join(rep_s.rename("rep"), how="inner")
    if len(aligned) < 30:
        return np.array([])
    return (aligned["rep"] - aligned["bench"]).to_numpy()


def fetch_idka_hs_active_series(portfolio_name: str, idx_name: str,
                                 date_str: str = DATA_STR) -> np.ndarray:
    """HS active return for an IDKA fund: current-position W minus benchmark index
    return on each historical scenario date (both in bps of NAV).

    portfolio_name: 'IDKA3Y' or 'IDKA10Y' (PORTIFOLIO column in PORTIFOLIO_DAILY_HISTORICAL_SIMULATION).
    idx_name:       'IDKA_IPCA_3A' or 'IDKA_IPCA_10A' (ECO_INDEX INSTRUMENT, FIELD='INDEX').
    """
    q_hs = f"""
    SELECT "DATE_SYNTHETIC_POSITION" AS dt, "W"
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}'
      AND "PORTIFOLIO" = '{portfolio_name}'
    ORDER BY dt
    """
    try:
        df_hs = read_sql(q_hs)
    except Exception:
        return np.array([])
    if df_hs.empty:
        return np.array([])

    df_hs["dt"] = pd.to_datetime(df_hs["dt"])
    dt_min = df_hs["dt"].min().strftime("%Y-%m-%d")

    q_bench = f"""
    SELECT "DATE" AS dt, "VALUE"
    FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = '{idx_name}' AND "FIELD" = 'INDEX'
      AND "DATE" >= DATE '{dt_min}'
      AND "DATE" <= DATE '{date_str}'
    ORDER BY dt
    """
    try:
        df_b = read_sql(q_bench)
    except Exception:
        return np.array([])
    if df_b.empty:
        return np.array([])

    df_hs = df_hs.set_index("dt")["W"].astype(float).sort_index()
    df_b  = df_b.assign(dt=pd.to_datetime(df_b["dt"])).set_index("dt")["VALUE"].astype(float).sort_index()
    bench_bps = df_b.pct_change().dropna() * 10_000
    aligned = df_hs.to_frame("W").join(bench_bps.rename("bench"), how="inner")
    if len(aligned) < 30:
        return np.array([])
    return (aligned["W"] - aligned["bench"]).to_numpy()


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


# ── VaR DoD attribution (D vs D-1 decomposition by leaf factor) ──────────────

# fund_key → (data_source, desk_full_name, metric_label, bench_primitive_or_None)
# bench_primitive: nome do PRIMITIVE no LOTE_PARAMETRIC_VAR_TABLE que representa
# a obrigação de tracking do índice (passivo). Por mandato deveria ser sempre -1×NAV;
# o engine às vezes recalibra pra -0.62/-0.71 (parking lot — engine recalibration).
_VAR_DOD_DISPATCH = {
    "IDKA_3Y":   ("idka_param", "IDKA IPCA 3Y FIRF",                       "BVaR", "IDKA IPCA 3Y"),
    "IDKA_10Y":  ("idka_param", "IDKA IPCA 10Y FIRF",                      "BVaR", "IDKA IPCA 10Y"),
    "MACRO":     ("rpm_book",   "Galapagos Macro FIM",                     "VaR",  None),
    "QUANT":     ("rpm_book",   "Galapagos Quantitativo FIM",              "VaR",  None),
    "EVOLUTION": ("rpm_book",   "Galapagos Evolution FIC FIM CP",          "VaR",  None),
    "ALBATROZ":  ("lote_fund",  "GALAPAGOS ALBATROZ FIRF LP",              "VaR",  None),
    "MACRO_Q":   ("lote_fund",  "Galapagos Global Macro Q",                "VaR",  None),
    "BALTRA":    ("rpm_book",   "Galapagos Baltra Icatu Qualif Prev FIM CP","VaR", None),
    "FRONTIER":  ("frontier_hs","Frontier Long Only",                       "BVaR", None),
}

_VAR_DOD_COLUMNS = [
    "label", "group",
    "contrib_d1_bps", "contrib_d_bps", "delta_bps",
    "pos_d1", "pos_d", "d_pos_pct",
    "vol_d1_bps", "vol_d_bps", "d_vol_bps",
    "pos_effect_bps", "vol_effect_bps",
    "sign", "override_note",
    "children",  # list[dict] | None — populated only for parent rows that explode (e.g. Albatroz)
]

_ALBATROZ_PRIMITIVE_LABEL = "GALAPAGOS ALBATROZ FIRF LP"
_ALBATROZ_DESK = "GALAPAGOS ALBATROZ FIRF LP"


def _empty_var_dod() -> pd.DataFrame:
    return pd.DataFrame(columns=_VAR_DOD_COLUMNS)


def _sign_of(delta_bps: float) -> str:
    if pd.isna(delta_bps) or abs(float(delta_bps)) < 0.5:
        return "0"
    return "+" if delta_bps > 0 else "-"


def _decompose_pos_constant_today(pos_d1, pos_d, contrib_d1, contrib_d):
    """Per-row attribution holding TODAY's position constant:
       pos_effect = Δpos × (contrib_d1 / pos_d1)   [position changed at yesterday's vol]
       vol_effect = pos_d × Δg                     [today's pos at change in vol-per-BRL]
                  = delta − pos_effect
       Edge cases: new position (pos_d1=0) → pos_effect=contrib_d, vol_effect=0;
                   closed position (pos_d=0) → pos_effect=-contrib_d1, vol_effect=0.
       Sums exactly to delta_bps. Returns (pos_effect, vol_effect).
       Both NaN if position info unavailable."""
    if pd.isna(pos_d1) or pd.isna(pos_d) or pd.isna(contrib_d1) or pd.isna(contrib_d):
        return (np.nan, np.nan)
    pos_d1 = float(pos_d1); pos_d = float(pos_d)
    cd1 = float(contrib_d1); cd = float(contrib_d)
    delta = cd - cd1
    # Closed position
    if abs(pos_d) < 1e-3 and abs(pos_d1) >= 1e-3:
        return (-cd1, 0.0)  # all from position closing
    # New position
    if abs(pos_d1) < 1e-3 and abs(pos_d) >= 1e-3:
        return (cd, 0.0)  # all from new position taking risk
    # Both zero
    if abs(pos_d1) < 1e-3 and abs(pos_d) < 1e-3:
        return (0.0, 0.0)
    g_d1 = cd1 / pos_d1
    pos_effect = (pos_d - pos_d1) * g_d1
    vol_effect = delta - pos_effect
    return (pos_effect, vol_effect)


def _var_dod_idka(desk: str, date_d: str, date_d1: str,
                   bench_primitive: str | None = None,
                   nav_d: float | None = None,
                   nav_d1: float | None = None) -> pd.DataFrame:
    """IDKA BVaR DoD via LOTE_PARAMETRIC_VAR_TABLE.
    Per PRIMITIVE; full pos/vol decomposition (symmetric, exact).

    Override aplicado no PRIMITIVE do bench (passivo): força DELTA = -NAV
    (ratio -1.00) sempre que o engine reportar magnitude diferente. Escala
    contrib_bps e vol_bps proporcionalmente. Razão: por mandato o passivo
    é 100% NAV; recalibrações intermitentes do engine para -0.62/-0.71
    geram ΔBVaR artificial sem mudança real de risco."""
    q = f"""
    SELECT "VAL_DATE", "PRIMITIVE",
           "DELTA",
           "RELATIVE_VAR_PCT" * -10000.0 AS contrib_bps,
           "RELATIVE_VOL_PCT" * 10000.0  AS vol_bps
    FROM "LOTE45"."LOTE_PARAMETRIC_VAR_TABLE"
    WHERE "VAL_DATE" IN (DATE '{date_d}', DATE '{date_d1}')
      AND "TRADING_DESK" = '{desk}'
      AND "BOOKS"::text = '{{*}}'
    """
    try:
        df = read_sql(q)
    except Exception:
        return _empty_var_dod()
    if df.empty:
        return _empty_var_dod()
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).dt.strftime("%Y-%m-%d")
    p_contrib = df.pivot_table(index="PRIMITIVE", columns="VAL_DATE",
                                values="contrib_bps", aggfunc="sum").fillna(0.0)
    p_pos = df.pivot_table(index="PRIMITIVE", columns="VAL_DATE",
                            values="DELTA", aggfunc="sum").fillna(0.0)
    p_vol = df.pivot_table(index="PRIMITIVE", columns="VAL_DATE",
                            values="vol_bps", aggfunc="sum").fillna(0.0)
    if date_d not in p_contrib.columns or date_d1 not in p_contrib.columns:
        return _empty_var_dod()

    # ── Bench-leg override (passivo = -1×NAV, sempre) ────────────────────────
    # Mandato: o passivo é 100% NAV por construção. Engine paramétrico
    # intermitentemente reporta ratios menores (-0.62/-0.71) por problema de
    # processo upstream — assumimos sempre -1.00 e escalamos contrib/vol
    # proporcionalmente. Note registrado apenas quando correção é material
    # (|ratio + 1| > 0.05) pra audit trail.
    override_notes: dict[str, str] = {}
    if bench_primitive and bench_primitive in p_pos.index:
        for date_col, nav in ((date_d, nav_d), (date_d1, nav_d1)):
            if nav is None or nav <= 0:
                continue
            engine_delta = float(p_pos.at[bench_primitive, date_col])
            if engine_delta == 0:
                continue
            ratio = engine_delta / nav
            target_delta = -float(nav)
            scale = target_delta / engine_delta  # preserves sign; ≈1.0 if engine OK
            p_pos.at[bench_primitive, date_col]     = target_delta
            p_contrib.at[bench_primitive, date_col] = float(p_contrib.at[bench_primitive, date_col]) * scale
            p_vol.at[bench_primitive, date_col]     = float(p_vol.at[bench_primitive, date_col]) * scale
            if abs(ratio + 1.0) > 0.05:
                override_notes[date_col] = f"engine ratio {ratio:+.2f} → forced to -1.00"

    out = pd.DataFrame(index=p_contrib.index)
    out["contrib_d1_bps"] = p_contrib[date_d1]
    out["contrib_d_bps"]  = p_contrib[date_d]
    out["delta_bps"]      = out["contrib_d_bps"] - out["contrib_d1_bps"]
    out["pos_d1"] = p_pos[date_d1]
    out["pos_d"]  = p_pos[date_d]
    den = out["pos_d1"].abs()
    out["d_pos_pct"] = np.where(
        den > 1e-3, (out["pos_d"] - out["pos_d1"]) / den * 100.0, np.nan
    )
    out["vol_d1_bps"] = p_vol[date_d1]
    out["vol_d_bps"]  = p_vol[date_d]
    out["d_vol_bps"]  = out["vol_d_bps"] - out["vol_d1_bps"]

    # Decomposição "today's pos constant": vol_effect = contrib pra ΔVaR vinda
    # de mudança de vol (holding pos at today's level). Soma exata com pos_effect.
    pe, ve = [], []
    for r in out.itertuples():
        p, v = _decompose_pos_constant_today(r.pos_d1, r.pos_d, r.contrib_d1_bps, r.contrib_d_bps)
        pe.append(p); ve.append(v)
    out["pos_effect_bps"] = pe
    out["vol_effect_bps"] = ve

    out["label"] = out.index.astype(str)
    out["group"] = pd.NA
    out["sign"]  = out["delta_bps"].apply(_sign_of)
    out["override_note"] = ""
    out["children"] = None
    if override_notes and bench_primitive in out.index:
        out.at[bench_primitive, "override_note"] = " · ".join(
            f"{d}: {n}" for d, n in sorted(override_notes.items())
        )

    # Albatroz look-through explosion: when IDKA holds Albatroz, expand the
    # 'GALAPAGOS ALBATROZ FIRF LP' primitive into Albatroz's underlying products
    # rescaled to IDKA NAV bps.
    if _ALBATROZ_PRIMITIVE_LABEL in out.index:
        kids = _explode_albatroz_for_idka(
            idka_desk=desk, date_d=date_d, date_d1=date_d1,
            parent_d_bps=float(out.at[_ALBATROZ_PRIMITIVE_LABEL, "contrib_d_bps"]),
            parent_d1_bps=float(out.at[_ALBATROZ_PRIMITIVE_LABEL, "contrib_d1_bps"]),
        )
        if kids:
            out.at[_ALBATROZ_PRIMITIVE_LABEL, "children"] = kids

    out = out[_VAR_DOD_COLUMNS].reset_index(drop=True)
    return out.sort_values("delta_bps", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)


def _explode_albatroz_for_idka(idka_desk: str, date_d: str, date_d1: str,
                                parent_d_bps: float, parent_d1_bps: float) -> list:
    """Build per-product children rows for the Albatroz line in an IDKA modal.
    Method:
      - Albatroz standalone PVAR1DAY per (BOOK, PRODUCT) on D and D-1 → contribs
        in Albatroz NAV bps (alb_total = sum).
      - Rescale: child_contrib_in_idka = albatroz_contrib_in_alb_bps × (parent / alb_total).
        Sum of children = parent (exact).
      - Position from LOTE_PRODUCT_EXPO with TRADING_DESK='ALBATROZ' AND
        TRADING_DESK_SHARE_SOURCE=idka_desk → IDKA's slice of Albatroz, in BRL.
    Returns list of row dicts (already filtered for material movers)."""
    # Albatroz NAV (denominator for Albatroz bps)
    nav_alb_d  = _latest_nav(_ALBATROZ_DESK, date_d)
    nav_alb_d1 = _latest_nav(_ALBATROZ_DESK, date_d1)
    if not nav_alb_d or not nav_alb_d1 or nav_alb_d <= 0 or nav_alb_d1 <= 0:
        return []

    # Albatroz per-(BOOK,PRODUCT) PVAR1DAY for both dates
    try:
        df_var = read_sql(f"""
            SELECT "VAL_DATE", "BOOK", "PRODUCT", "PVAR1DAY"
            FROM "LOTE45"."LOTE_FUND_STRESS"
            WHERE "VAL_DATE" IN (DATE '{date_d}', DATE '{date_d1}')
              AND "TRADING_DESK" = '{_ALBATROZ_DESK}'
              AND "PVAR1DAY" IS NOT NULL
        """)
    except Exception:
        return []
    if df_var.empty:
        return []
    df_var["VAL_DATE"] = pd.to_datetime(df_var["VAL_DATE"]).dt.strftime("%Y-%m-%d")
    df_var["key"] = df_var["BOOK"].astype(str) + "::" + df_var["PRODUCT"].astype(str)
    # Albatroz contrib in Albatroz NAV bps (positive = adds VaR)
    df_var["contrib_alb_bps"] = np.where(
        df_var["VAL_DATE"] == date_d,
        -df_var["PVAR1DAY"].astype(float) / float(nav_alb_d) * 10000.0,
        -df_var["PVAR1DAY"].astype(float) / float(nav_alb_d1) * 10000.0,
    )
    p = df_var.pivot_table(index="key", columns="VAL_DATE",
                            values="contrib_alb_bps", aggfunc="sum").fillna(0.0)
    if date_d not in p.columns or date_d1 not in p.columns:
        return []

    alb_total_d  = float(p[date_d].sum())
    alb_total_d1 = float(p[date_d1].sum())
    if abs(alb_total_d) < 1e-6 and abs(alb_total_d1) < 1e-6:
        return []
    scale_d  = (parent_d_bps  / alb_total_d)  if abs(alb_total_d)  > 1e-6 else 0.0
    scale_d1 = (parent_d1_bps / alb_total_d1) if abs(alb_total_d1) > 1e-6 else 0.0

    # IDKA's slice of Albatroz positions per (BOOK, PRODUCT) — look-through
    pos_lookup_d  = _fetch_albatroz_lookthrough_pos(idka_desk, date_d)
    pos_lookup_d1 = _fetch_albatroz_lookthrough_pos(idka_desk, date_d1)

    book_map = df_var.groupby("key")["BOOK"].first()
    prod_map = df_var.groupby("key")["PRODUCT"].first()

    children: list = []
    for key in p.index:
        c_alb_d  = float(p.at[key, date_d])
        c_alb_d1 = float(p.at[key, date_d1])
        c_d  = c_alb_d  * scale_d
        c_d1 = c_alb_d1 * scale_d1
        delta = c_d - c_d1
        # Filter immaterial children (consistent with main payload)
        if abs(c_d) < 0.05 and abs(c_d1) < 0.05:
            continue
        pos_d  = pos_lookup_d.get(key)
        pos_d1 = pos_lookup_d1.get(key)
        d_pos_pct = ((pos_d - pos_d1) / abs(pos_d1) * 100.0
                     if (pos_d is not None and pos_d1 is not None and abs(pos_d1) > 1e-3)
                     else None)
        pe, ve = _decompose_pos_constant_today(pos_d1, pos_d, c_d1, c_d)
        children.append({
            "label": str(prod_map.get(key, key)),
            "group": str(book_map.get(key, "")),
            "contrib_d1_bps": c_d1,
            "contrib_d_bps":  c_d,
            "delta_bps":      delta,
            "pos_d1":         pos_d1,
            "pos_d":          pos_d,
            "d_pos_pct":      d_pos_pct,
            "vol_d1_bps":     None,
            "vol_d_bps":      None,
            "d_vol_bps":      None,
            "pos_effect_bps": pe,
            "vol_effect_bps": ve,
            "sign":           _sign_of(delta),
            "override_note":  "",
        })
    # Sort children by |delta| desc
    children.sort(key=lambda x: -abs(x.get("delta_bps") or 0))
    return children


def _fetch_albatroz_lookthrough_pos(idka_desk: str, date_str: str) -> dict[str, float]:
    """IDKA's slice of Albatroz positions per (BOOK, PRODUCT) in BRL. Sums DELTA
    grouped by (BOOK, PRODUCT) when ALBATROZ holds positions on behalf of `idka_desk`.
    Uses TRADING_DESK_SHARE_SOURCE filter."""
    try:
        df = read_sql(f"""
            SELECT "BOOK", "PRODUCT", SUM("DELTA") AS pos_brl
            FROM "LOTE45"."LOTE_PRODUCT_EXPO"
            WHERE "VAL_DATE" = DATE '{date_str}'
              AND "TRADING_DESK" = '{_ALBATROZ_DESK}'
              AND "TRADING_DESK_SHARE_SOURCE" = '{idka_desk}'
              AND "DELTA" IS NOT NULL
            GROUP BY "BOOK", "PRODUCT"
        """)
    except Exception:
        return {}
    if df.empty:
        return {}
    return {f"{r.BOOK}::{r.PRODUCT}": float(r.pos_brl) for r in df.itertuples(index=False)}


def _var_dod_rpm(desk: str, date_d: str, date_d1: str, nav_d: float) -> pd.DataFrame:
    """MACRO/QUANT/EVOLUTION VaR DoD via LOTE_FUND_STRESS_RPM (LEVEL=10, BOOK aggregate).
    Vol proxy: using today's |DELTA| sum per BOOK from LOTE_PRODUCT_EXPO as a
    constant denominator for both days. vol_x_bps = contrib_x_bps / pos_pct_nav_d
    (bps de risco por 1% NAV em gross size). Captura mudança de "intensidade
    de vol" do BOOK holding-position-constant."""
    q = f"""
    SELECT "VAL_DATE", "BOOK", SUM("PARAMETRIC_VAR") AS var_brl
    FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
    WHERE "VAL_DATE" IN (DATE '{date_d}', DATE '{date_d1}')
      AND "TRADING_DESK" = '{desk}'
      AND "LEVEL" = 10
    GROUP BY "VAL_DATE", "BOOK"
    """
    try:
        df = read_sql(q)
    except Exception:
        return _empty_var_dod()
    if df.empty or nav_d <= 0:
        return _empty_var_dod()
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).dt.strftime("%Y-%m-%d")
    df["contrib_bps"] = -df["var_brl"].astype(float) / nav_d * 10000.0
    p = df.pivot_table(index="BOOK", columns="VAL_DATE",
                        values="contrib_bps", aggfunc="sum").fillna(0.0)
    if date_d not in p.columns or date_d1 not in p.columns:
        return _empty_var_dod()

    # Today's position per BOOK (proxy denominator for vol calc)
    pos_book = _fetch_pos_per_book(desk, date_d)
    pos_book_d1 = _fetch_pos_per_book(desk, date_d1)

    out = pd.DataFrame(index=p.index)
    out["contrib_d1_bps"] = p[date_d1]
    out["contrib_d_bps"]  = p[date_d]
    out["delta_bps"]      = out["contrib_d_bps"] - out["contrib_d1_bps"]
    out["label"] = out.index.astype(str)
    out["group"] = pd.NA

    # Fill pos via cross-join (real D and D-1)
    out["pos_d"]  = out.index.map(lambda b: pos_book.get(b, np.nan))
    out["pos_d1"] = out.index.map(lambda b: pos_book_d1.get(b, np.nan))
    den = out["pos_d1"].abs()
    out["d_pos_pct"] = np.where(den > 1e-3,
                                 (out["pos_d"] - out["pos_d1"]) / den * 100.0, np.nan)

    # vol per BRL of pos: g_x = contrib_x / pos_x (effective vol intensity, signed bps/BRL).
    # Mostrado como "vol_x_bps" pra eyeball regime change na mesma posição.
    den_d  = out["pos_d"].abs()
    den_d1 = out["pos_d1"].abs()
    out["vol_d_bps"]  = np.where(den_d  > 1e-3, out["contrib_d_bps"]  / den_d  * 1e6, np.nan)
    out["vol_d1_bps"] = np.where(den_d1 > 1e-3, out["contrib_d1_bps"] / den_d1 * 1e6, np.nan)
    out["d_vol_bps"]  = out["vol_d_bps"] - out["vol_d1_bps"]

    # Decomposição "today's pos constant": pos_effect + vol_effect = delta (exato).
    pe, ve = [], []
    for r in out.itertuples():
        p, v = _decompose_pos_constant_today(r.pos_d1, r.pos_d, r.contrib_d1_bps, r.contrib_d_bps)
        pe.append(p); ve.append(v)
    out["pos_effect_bps"] = pe
    out["vol_effect_bps"] = ve

    out["sign"] = out["delta_bps"].apply(_sign_of)
    out["override_note"] = ""
    out["children"] = None
    out = out[_VAR_DOD_COLUMNS].reset_index(drop=True)
    return out.sort_values("delta_bps", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)


def _fetch_pos_per_book(desk: str, date_str: str) -> dict[str, float]:
    """Sum of |DELTA| per BOOK from LOTE_PRODUCT_EXPO. Used as proxy denominator
    for vol calc in non-IDKA funds. Returns {} on failure."""
    try:
        df = read_sql(f"""
            SELECT "BOOK", SUM(ABS("DELTA")) AS pos_brl
            FROM "LOTE45"."LOTE_PRODUCT_EXPO"
            WHERE "VAL_DATE" = DATE '{date_str}' AND "TRADING_DESK" = '{desk}'
              AND "DELTA" IS NOT NULL
            GROUP BY "BOOK"
        """)
    except Exception:
        return {}
    if df.empty:
        return {}
    return {str(r.BOOK): float(r.pos_brl) for r in df.itertuples(index=False)}


def _fetch_pos_per_product(desk: str, date_str: str) -> dict[str, float]:
    """Sum of |DELTA| per (BOOK, PRODUCT) from LOTE_PRODUCT_EXPO."""
    try:
        df = read_sql(f"""
            SELECT "BOOK", "PRODUCT", SUM(ABS("DELTA")) AS pos_brl
            FROM "LOTE45"."LOTE_PRODUCT_EXPO"
            WHERE "VAL_DATE" = DATE '{date_str}' AND "TRADING_DESK" = '{desk}'
              AND "DELTA" IS NOT NULL
            GROUP BY "BOOK", "PRODUCT"
        """)
    except Exception:
        return {}
    if df.empty:
        return {}
    return {f"{r.BOOK}::{r.PRODUCT}": float(r.pos_brl) for r in df.itertuples(index=False)}


def _var_dod_lote_fund(desk: str, date_d: str, date_d1: str, nav_d: float) -> pd.DataFrame:
    """ALBATROZ/MACRO_Q VaR DoD via LOTE_FUND_STRESS (per PRODUCT, BOOK as group).
    BALTRA uses _var_dod_rpm now (LOTE_FUND_STRESS_RPM populated 2026-04-07+)."""
    q = f"""
    SELECT "VAL_DATE", "BOOK", "PRODUCT", "PVAR1DAY"
    FROM "LOTE45"."LOTE_FUND_STRESS"
    WHERE "VAL_DATE" IN (DATE '{date_d}', DATE '{date_d1}')
      AND "TRADING_DESK" = '{desk}'
      AND "TREE" = 'Main'
      AND "PVAR1DAY" IS NOT NULL
    """
    try:
        df = read_sql(q)
    except Exception:
        return _empty_var_dod()
    if df.empty or nav_d <= 0:
        return _empty_var_dod()
    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"]).dt.strftime("%Y-%m-%d")
    df["contrib_bps"] = -df["PVAR1DAY"].astype(float) / nav_d * 10000.0
    df["key"] = df["BOOK"].astype(str) + "::" + df["PRODUCT"].astype(str)
    p_contrib = df.pivot_table(index="key", columns="VAL_DATE",
                                values="contrib_bps", aggfunc="sum").fillna(0.0)
    book_map = df.groupby("key")["BOOK"].first()
    prod_map = df.groupby("key")["PRODUCT"].first()
    if date_d not in p_contrib.columns or date_d1 not in p_contrib.columns:
        return _empty_var_dod()

    out = pd.DataFrame(index=p_contrib.index)
    out["contrib_d1_bps"] = p_contrib[date_d1]
    out["contrib_d_bps"]  = p_contrib[date_d]
    out["delta_bps"]      = out["contrib_d_bps"] - out["contrib_d1_bps"]
    out["label"] = prod_map.reindex(out.index).astype(str).values
    out["group"] = book_map.reindex(out.index).astype(str).values

    # Position cross-join: LOTE_PRODUCT_EXPO per (BOOK, PRODUCT)
    pos_d_map  = _fetch_pos_per_product(desk, date_d)
    pos_d1_map = _fetch_pos_per_product(desk, date_d1)
    out["pos_d"]  = out.index.map(lambda k: pos_d_map.get(k, np.nan))
    out["pos_d1"] = out.index.map(lambda k: pos_d1_map.get(k, np.nan))
    den = out["pos_d1"].abs()
    out["d_pos_pct"] = np.where(den > 1e-3,
                                 (out["pos_d"] - out["pos_d1"]) / den * 100.0, np.nan)

    # vol per BRL of pos (signed effective bps/BRL, scaled by 1e6 for readability).
    den_d  = out["pos_d"].abs()
    den_d1 = out["pos_d1"].abs()
    out["vol_d_bps"]  = np.where(den_d  > 1e-3, out["contrib_d_bps"]  / den_d  * 1e6, np.nan)
    out["vol_d1_bps"] = np.where(den_d1 > 1e-3, out["contrib_d1_bps"] / den_d1 * 1e6, np.nan)
    out["d_vol_bps"]  = out["vol_d_bps"] - out["vol_d1_bps"]

    pe, ve = [], []
    for r in out.itertuples():
        p, v = _decompose_pos_constant_today(r.pos_d1, r.pos_d, r.contrib_d1_bps, r.contrib_d_bps)
        pe.append(p); ve.append(v)
    out["pos_effect_bps"] = pe
    out["vol_effect_bps"] = ve
    out["sign"] = out["delta_bps"].apply(_sign_of)
    out["override_note"] = ""
    out["children"] = None

    # Look-through regrouping: positions sourced from sub-funds (BALTRA holds
    # IDKAs, Albatroz) are grouped under synthetic parent rows.
    # NOTE: BALTRA migrado para LOTE_FUND_STRESS_RPM em 2026-04-28 (look-through
    # nativo). Esta path só ativa pra ALBATROZ/MACRO_Q que continuam em LOTE_FUND_STRESS.
    out = _regroup_lookthrough(out, desk, date_d)

    out = out[_VAR_DOD_COLUMNS].reset_index(drop=True)
    return out.sort_values("delta_bps", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)


_FUND_PRETTY = {
    "IDKA IPCA 10Y FIRF": "IDKA 10Y holdings",
    "IDKA IPCA 3Y FIRF":  "IDKA 3Y holdings",
    "GALAPAGOS ALBATROZ FIRF LP": "Albatroz holdings",
    "Galapagos Pinzon FIRF Ref DI": "Pinzon holdings",
}


def _pretty_source(desk: str) -> str:
    return _FUND_PRETTY.get(desk, desk + " holdings")


def _fetch_lookthrough_source_funds(desk: str, date_str: str) -> dict:
    """Map (BOOK, PRODUCT) → source_fund_label for look-through positions held
    by `desk`. Source funds are TRADING_DESKs that report into `desk` via
    TRADING_DESK_SHARE_SOURCE. Only includes external sources (not desk itself)."""
    try:
        df = read_sql(f"""
            SELECT "BOOK", "PRODUCT", "TRADING_DESK", SUM(ABS("DELTA")) AS gross_brl
            FROM "LOTE45"."LOTE_PRODUCT_EXPO"
            WHERE "VAL_DATE" = DATE '{date_str}'
              AND "TRADING_DESK_SHARE_SOURCE" = '{desk}'
              AND "TRADING_DESK" != '{desk}'
              AND "DELTA" IS NOT NULL
            GROUP BY "BOOK", "PRODUCT", "TRADING_DESK"
        """)
    except Exception:
        return {}
    if df.empty:
        return {}
    df = df.sort_values("gross_brl", ascending=False)
    out = {}
    for r in df.itertuples(index=False):
        key = (str(r.BOOK), str(r.PRODUCT))
        if key not in out:
            out[key] = str(r.TRADING_DESK)
    return out


def _regroup_lookthrough(out_df: pd.DataFrame, desk: str, date_d: str) -> pd.DataFrame:
    """Restructure rows: those whose (BOOK, PRODUCT) matches a look-through
    source fund get grouped under a synthetic parent row. Direct rows stay flat.
    Index of out_df is 'BOOK::PRODUCT'."""
    if out_df.empty:
        return out_df
    src_map = _fetch_lookthrough_source_funds(desk, date_d)
    if not src_map:
        return out_df

    sources: list = []
    for idx in out_df.index:
        parts = str(idx).split("::", 1)
        if len(parts) == 2:
            sources.append(src_map.get((parts[0], parts[1])))
        else:
            sources.append(None)
    tagged = out_df.copy()
    tagged["_src"] = sources

    direct = tagged[tagged["_src"].isna()].drop(columns=["_src"])
    grouped = tagged[tagged["_src"].notna()]
    if grouped.empty:
        return direct

    parent_records = []
    for src, sub in grouped.groupby("_src", sort=False):
        children = []
        for _, r in sub.iterrows():
            children.append({
                "label": r["label"], "group": r["group"],
                "contrib_d1_bps": (None if pd.isna(r["contrib_d1_bps"]) else float(r["contrib_d1_bps"])),
                "contrib_d_bps":  (None if pd.isna(r["contrib_d_bps"])  else float(r["contrib_d_bps"])),
                "delta_bps":      (None if pd.isna(r["delta_bps"])      else float(r["delta_bps"])),
                "pos_d1":         (None if pd.isna(r["pos_d1"])         else float(r["pos_d1"])),
                "pos_d":          (None if pd.isna(r["pos_d"])          else float(r["pos_d"])),
                "d_pos_pct":      (None if pd.isna(r["d_pos_pct"])      else float(r["d_pos_pct"])),
                "vol_d1_bps":     (None if pd.isna(r["vol_d1_bps"])     else float(r["vol_d1_bps"])),
                "vol_d_bps":      (None if pd.isna(r["vol_d_bps"])      else float(r["vol_d_bps"])),
                "d_vol_bps":      (None if pd.isna(r["d_vol_bps"])      else float(r["d_vol_bps"])),
                "pos_effect_bps": (None if pd.isna(r["pos_effect_bps"]) else float(r["pos_effect_bps"])),
                "vol_effect_bps": (None if pd.isna(r["vol_effect_bps"]) else float(r["vol_effect_bps"])),
                "sign":           str(r["sign"] or ""),
                "override_note":  "",
            })
        children.sort(key=lambda c: -abs(c.get("delta_bps") or 0))

        c_d1 = float(sub["contrib_d1_bps"].fillna(0).sum())
        c_d  = float(sub["contrib_d_bps"].fillna(0).sum())
        delta = c_d - c_d1
        pos_d1_t = float(sub["pos_d1"].abs().fillna(0).sum())
        pos_d_t  = float(sub["pos_d"].abs().fillna(0).sum())
        d_pos_pct = ((pos_d_t - pos_d1_t) / pos_d1_t * 100.0) if pos_d1_t > 1e-3 else None
        pe_t = float(sub["pos_effect_bps"].fillna(0).sum())
        ve_t = float(sub["vol_effect_bps"].fillna(0).sum())
        parent_records.append({
            "label": f"↻ {_pretty_source(src)}",
            "group": pd.NA,
            "contrib_d1_bps": c_d1,
            "contrib_d_bps":  c_d,
            "delta_bps":      delta,
            "pos_d1":         pos_d1_t if pos_d1_t > 0 else np.nan,
            "pos_d":          pos_d_t  if pos_d_t  > 0 else np.nan,
            "d_pos_pct":      d_pos_pct if d_pos_pct is not None else np.nan,
            "vol_d1_bps":     np.nan,
            "vol_d_bps":      np.nan,
            "d_vol_bps":      np.nan,
            "pos_effect_bps": pe_t,
            "vol_effect_bps": ve_t,
            "sign":           _sign_of(delta),
            "override_note":  "",
            "children":       children,
        })

    parents_df = pd.DataFrame(parent_records)
    parents_df.index = [f"[lookthrough]{p['label']}" for p in parent_records]
    return pd.concat([direct, parents_df])


def _var_dod_frontier(date_d: str, date_d1: str, window_days: int = 756) -> pd.DataFrame:
    """FRONTIER BVaR DoD via per-ticker component-VaR at the q05 worst-day scenario.

    For each date t in {D, D-1}:
      portfolio_active_return_s = Σ_i (w_i_t − w_ibov_i) × r_i_s   (s in 756d window)
      s* = arg s where portfolio_active_return_s = q05(series)
      component_i_t = (w_i_t − w_ibov_i) × r_i_s*
      Σ_i component_i_t = portfolio_active_return_s* = −BVaR_pct_t
      contrib_i_t (bps NAV) = −10000 × component_i_t   (positive = adds to BVaR)

    DoD per ticker:
      delta_bps = contrib_i_d − contrib_i_d1
    Decomposition uses _decompose_pos_constant_today on (active_weight, contrib_bps).
    Note: s* on D may differ from s* on D-1 — both effects are real.
    """
    def _frontier_weights(date_str: str) -> dict[str, float]:
        q = f"""
        SELECT "PRODUCT", "% Cash" AS w
        FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
        WHERE "VAL_DATE" = (
            SELECT MAX("VAL_DATE") FROM frontier."LONG_ONLY_DAILY_REPORT_MAINBOARD"
            WHERE "VAL_DATE" <= DATE '{date_str}'
        )
          AND "BOOK" IS NOT NULL AND TRIM("BOOK"::text) <> ''
          AND "PRODUCT" NOT IN ('TOTAL', 'SUBTOTAL')
          AND "% Cash" IS NOT NULL
        """
        try:
            df = read_sql(q)
        except Exception:
            return {}
        if df.empty:
            return {}
        return dict(zip(df["PRODUCT"].astype(str), df["w"].astype(float)))

    w_d  = _frontier_weights(date_d)
    w_d1 = _frontier_weights(date_d1)
    # Frontier mainboard upstream only persists current snapshot — fall back to
    # today's weights for D-1 when D-1 not available. Matches compute_frontier_bvar_hs
    # convention: D-1 BVaR is today's weights × D-1 history.
    weights_held_constant = False
    if not w_d:
        return _empty_var_dod()
    if not w_d1:
        w_d1 = w_d
        weights_held_constant = True

    q_ibov = """
    SELECT "INSTRUMENT", "VALUE" AS w
    FROM public."EQUITIES_COMPOSITION"
    WHERE "LIST_NAME" = 'IBOV'
      AND "DATE" = (SELECT MAX("DATE") FROM public."EQUITIES_COMPOSITION" WHERE "LIST_NAME" = 'IBOV')
    """
    try:
        df_ibov = read_sql(q_ibov)
    except Exception:
        return _empty_var_dod()
    ibov_wt = dict(zip(df_ibov["INSTRUMENT"].astype(str), df_ibov["w"].astype(float))) \
              if not df_ibov.empty else {}

    tickers = sorted(set(w_d) | set(w_d1) | set(ibov_wt))
    if not tickers:
        return _empty_var_dod()

    tks_sql = ",".join(f"'{t}'" for t in tickers + ["IBOV"])
    q_px = f"""
    SELECT "INSTRUMENT", "DATE", "CLOSE"
    FROM public."EQUITIES_PRICES"
    WHERE "INSTRUMENT" IN ({tks_sql})
      AND "DATE" >= DATE '{date_d}' - INTERVAL '{window_days + 120} days'
      AND "DATE" <= DATE '{date_d}'
    """
    try:
        df_px = read_sql(q_px)
    except Exception:
        return _empty_var_dod()
    if df_px.empty or "IBOV" not in df_px["INSTRUMENT"].unique():
        return _empty_var_dod()

    df_px["DATE"] = pd.to_datetime(df_px["DATE"])
    wide = (df_px.pivot_table(index="DATE", columns="INSTRUMENT",
                              values="CLOSE", aggfunc="last")
                 .sort_index()
                 .dropna(subset=["IBOV"])
                 .ffill())
    rets = wide.pct_change().dropna(subset=["IBOV"])
    stock_cols = [c for c in rets.columns if c != "IBOV"]
    rets[stock_cols] = rets[stock_cols].mask(rets[stock_cols].abs() > 0.30, 0.0)

    def _scenario_for(date_anchor: str, w_fund: dict[str, float]) -> tuple[pd.Series, str] | None:
        end = pd.to_datetime(date_anchor)
        rets_anchor = rets[rets.index <= end].tail(window_days)
        if len(rets_anchor) < 50:
            return None
        active = pd.Series(0.0, index=rets_anchor.index)
        for t, w in w_fund.items():
            aw = w - ibov_wt.get(t, 0.0)
            if abs(aw) < 1e-9 or t not in rets_anchor.columns:
                continue
            active = active + aw * rets_anchor[t].fillna(0.0)
        for t, w in ibov_wt.items():
            if t in w_fund:
                continue
            aw = -w
            if t not in rets_anchor.columns:
                continue
            active = active + aw * rets_anchor[t].fillna(0.0)
        if len(active.dropna()) < 30:
            return None
        q05 = active.quantile(0.05)
        s_star = (active - q05).abs().idxmin()
        return rets_anchor.loc[s_star], s_star.strftime("%Y-%m-%d")

    sc_d  = _scenario_for(date_d,  w_d)
    sc_d1 = _scenario_for(date_d1, w_d1)
    if sc_d is None or sc_d1 is None:
        return _empty_var_dod()
    r_d_row,  s_d  = sc_d
    r_d1_row, s_d1 = sc_d1

    rows = []
    for t in tickers:
        aw_d  = w_d.get(t,  0.0) - ibov_wt.get(t, 0.0)
        aw_d1 = w_d1.get(t, 0.0) - ibov_wt.get(t, 0.0)
        r_t_d  = float(r_d_row.get(t, 0.0))  if t in r_d_row.index  else 0.0
        r_t_d1 = float(r_d1_row.get(t, 0.0)) if t in r_d1_row.index else 0.0
        contrib_d_bps  = -aw_d  * r_t_d  * 10_000.0
        contrib_d1_bps = -aw_d1 * r_t_d1 * 10_000.0
        if abs(contrib_d_bps) < 0.05 and abs(contrib_d1_bps) < 0.05:
            continue
        # Pos in bps NAV (active weight), vol_d_bps = stress return × 10000 (bps)
        pos_d  = aw_d  * 10_000.0
        pos_d1 = aw_d1 * 10_000.0
        vol_d_bps  = -r_t_d  * 10_000.0
        vol_d1_bps = -r_t_d1 * 10_000.0
        den = abs(pos_d1) if abs(pos_d1) > 1e-3 else None
        d_pos_pct = ((pos_d - pos_d1) / den * 100.0) if den else np.nan
        d_vol_bps = vol_d_bps - vol_d1_bps
        pe, ve = _decompose_pos_constant_today(pos_d1, pos_d, contrib_d1_bps, contrib_d_bps)
        rows.append({
            "label":          t,
            "group":          ("IBOV" if t in ibov_wt else "Out-of-bench"),
            "contrib_d1_bps": contrib_d1_bps,
            "contrib_d_bps":  contrib_d_bps,
            "delta_bps":      contrib_d_bps - contrib_d1_bps,
            "pos_d1":         pos_d1,
            "pos_d":          pos_d,
            "d_pos_pct":      d_pos_pct,
            "vol_d1_bps":     vol_d1_bps,
            "vol_d_bps":      vol_d_bps,
            "d_vol_bps":      d_vol_bps,
            "pos_effect_bps": pe,
            "vol_effect_bps": ve,
            "sign":           _sign_of(contrib_d_bps - contrib_d1_bps),
            "override_note":  "",
            "children":       None,
        })

    if not rows:
        return _empty_var_dod()
    out = pd.DataFrame(rows)[_VAR_DOD_COLUMNS]
    out = out.sort_values("delta_bps", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    if weights_held_constant:
        out.attrs["modal_note"] = (
            "Frontier mainboard upstream sem histórico — D-1 usa pesos de hoje. "
            "BVaR DoD captura só shift de cenário (sem efeito de posição). "
            "Tabela mostra a composição atual do BVaR por ticker."
        )
    return out


def fetch_var_dod_decomposition(fund_key: str, date_str: str = DATA_STR) -> pd.DataFrame:
    """Day-over-day decomposition of VaR/BVaR by leaf factor.

    Output schema (uniform; pos/vol cols NaN where source lacks DELTA):
      label             — PRIMITIVE / BOOK / PRODUCT name
      group             — BOOK (for ALBATROZ/MACRO_Q/BALTRA), NaN otherwise
      contrib_d1_bps    — yesterday's contribution in bps of NAV (positive = adds VaR)
      contrib_d_bps     — today's contribution
      delta_bps         — contrib_d − contrib_d1 (sorted by |·| desc)
      pos_d1 / pos_d / d_pos_pct      — IDKA only (DELTA in BRL, % change)
      vol_d1_bps / vol_d_bps / d_vol_bps — IDKA only (RELATIVE_VOL_PCT × 10000)
      pos_effect_bps / vol_effect_bps — IDKA only (symmetric, sums to delta_bps)
      sign              — '+' (added risk), '-' (removed), '0' (|Δ| < 0.5 bps)

    Returns empty DataFrame if fund unsupported, no data, or D-1 missing.
    Source by family:
      IDKAs                    → LOTE_PARAMETRIC_VAR_TABLE (BOOKS='{*}')
      MACRO/QUANT/EVO/BALTRA   → LOTE_FUND_STRESS_RPM (LEVEL=10, BOOK aggregate)
      ALBATROZ/MACRO_Q         → LOTE_FUND_STRESS (per PRODUCT, TREE='Main')
      FRONTIER                 → frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD + EQUITIES_PRICES (HS BVaR per ticker)
    """
    cfg = _VAR_DOD_DISPATCH.get(fund_key)
    if cfg is None:
        return _empty_var_dod()
    source, desk, _metric, bench_primitive = cfg
    # Normalize to YYYY-MM-DD (matches the pivot column keys built downstream).
    date_d  = pd.to_datetime(date_str).strftime("%Y-%m-%d")
    date_d1 = pd.to_datetime(_prev_bday(date_d)).strftime("%Y-%m-%d")
    if source == "idka_param":
        nav_d  = _latest_nav(desk, date_d)
        nav_d1 = _latest_nav(desk, date_d1)
        return _var_dod_idka(desk, date_d, date_d1, bench_primitive,
                             nav_d, nav_d1)
    if source == "rpm_book":
        nav_d = _latest_nav(desk, date_d)
        if nav_d is None or nav_d <= 0:
            return _empty_var_dod()
        return _var_dod_rpm(desk, date_d, date_d1, float(nav_d))
    if source == "lote_fund":
        nav_d = _latest_nav(desk, date_d)
        if nav_d is None or nav_d <= 0:
            return _empty_var_dod()
        return _var_dod_lote_fund(desk, date_d, date_d1, float(nav_d))
    if source == "frontier_hs":
        return _var_dod_frontier(date_d, date_d1)
    return _empty_var_dod()


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
    # IDKAs: HS active return vs benchmark + vs DV-matched replication.
    for portfolio_name, idx_name, key, target_anos, tenour_du in [
        ("IDKA3Y",  "IDKA_IPCA_3A",  "IDKA_3Y",  3,  756),
        ("IDKA10Y", "IDKA_IPCA_10A", "IDKA_10Y", 10, 2520),
    ]:
        try:
            s = fetch_idka_hs_active_series(portfolio_name, idx_name, date_str)
            if len(s) >= 30:
                out[key] = s
        except Exception as e:
            print(f"  IDKA HS active series ({key}) failed: {e}")
        try:
            s_rep = fetch_idka_hs_replication_series(portfolio_name, target_anos, tenour_du, date_str)
            if len(s_rep) >= 30:
                out[f"{key}_REP"] = s_rep
        except Exception as e:
            print(f"  IDKA HS replication series ({key}_REP) failed: {e}")
        try:
            s_spr = fetch_idka_hs_spread_series(portfolio_name, idx_name, target_anos, tenour_du, date_str)
            if len(s_spr) >= 30:
                out[f"{key}_SPREAD"] = s_spr
        except Exception as e:
            print(f"  IDKA HS spread series ({key}_SPREAD) failed: {e}")
    # Albatroz: HS gross W already populated by main query under key 'ALBATROZ'. No override.
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
    actuals = {f'fund:{r.FUNDO}': float(r.dia_bps) for r in fund_df.itertuples(index=False)}

    # LIVRO-level (PMs within MACRO, sub-books within QUANT)
    livro_df = read_sql(f"""
        SELECT "LIVRO", SUM("DIA") * 10000 AS dia_bps
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "DATE" = DATE '{date_str}' AND "FUNDO" IN ('MACRO','QUANT')
        GROUP BY "LIVRO"
    """)
    for r in livro_df.itertuples(index=False):
        actuals[f'livro:{r.LIVRO}'] = float(r.dia_bps)

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


def fetch_macro_exposure(date_str: str = DATA_STR) -> tuple:
    """Returns (df_expo, df_var, aum) for the given date."""
    aum = _require_nav("Galapagos Macro FIM", date_str)

    expo = read_sql(f"""
        SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS", "PRIMITIVE_CLASS",
               SUM("DELTA")                   AS delta,
               SUM("DELTA" * "MOD_DURATION")  AS delta_dur
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Evolution FIC FIM CP'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "BOOK" ~* 'CI|JD|LF|RJ'
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
    nav = _require_nav("Galapagos Quantitativo FIM", date_str)
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


def fetch_albatroz_exposure(date_str: str = DATA_STR,
                            desk: str = "GALAPAGOS ALBATROZ FIRF LP") -> tuple:
    """
    RF exposure snapshot for an RF desk — position-level from LOTE_PRODUCT_EXPO.
    Returns (df, nav) where df columns are:
      BOOK, PRODUCT_CLASS, PRODUCT, delta_brl, mod_dur (years), dv01_brl (R$/bp), indexador

    DELTA is already duration-weighted (= POSITION × MOD_DURATION) for rate
    primitives ('IPCA Coupon', 'IGPM Coupon', 'BRL Rate Curve'), so:
      DV01_R$/bp = DELTA × 0.0001  (NOT × MOD_DURATION again)

    Each bond decomposes into multiple primitives (sovereign spread + inflation
    index face + rate curve coupon) with INCONSISTENT signs across primitives.
    Naive SUM(DELTA) gives garbage (e.g. NTN-C delta_d1 + delta_d sum cancellations
    don't reflect any real position concept). Pick ONE primitive per PRODUCT_CLASS
    that represents the dominant rate exposure.

    Default desk = ALBATROZ. Pass `desk=...` to reuse for BALTRA / IDKAs.
    """
    nav = _latest_nav(desk, date_str)
    if nav is None:
        return None, None

    df = read_sql(f"""
        SELECT "BOOK", "PRODUCT_CLASS", "PRODUCT",
               SUM("DELTA")                                                 AS delta_brl,
               MAX("MOD_DURATION")                                          AS mod_dur,
               SUM(CASE WHEN "MOD_DURATION" IS NOT NULL
                        THEN "DELTA" * 0.0001 ELSE 0 END)                   AS dv01_brl
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE "TRADING_DESK_SHARE_SOURCE" = '{desk}'
          AND "VAL_DATE"                  = DATE '{date_str}'
          AND "DELTA" <> 0
          AND (
            -- IPCA-linked rate primitive (NTN-B / DAP)
            ("PRODUCT_CLASS" IN ('NTN-B','DAP Future','DAPFuture')
                AND "PRIMITIVE_CLASS" = 'IPCA Coupon')
            OR
            -- IGP-M-linked rate primitive (NTN-C / DAC)
            ("PRODUCT_CLASS" IN ('NTN-C','DAC Future')
                AND "PRIMITIVE_CLASS" = 'IGPM Coupon')
            OR
            -- Nominal rate primitive (DI / NTN-F / LTN)
            ("PRODUCT_CLASS" IN ('DI1Future','NTN-F','LTN')
                AND "PRIMITIVE_CLASS" = 'BRL Rate Curve')
            OR
            -- Floating / cash / collateral — no primitive decomposition needed
            "PRODUCT_CLASS" NOT IN ('NTN-B','DAP Future','DAPFuture',
                                     'NTN-C','DAC Future',
                                     'DI1Future','NTN-F','LTN')
          )
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
    # NTN-C/DAC are IGP-M-linked (not IPCA) — engine emits 'IGPM Coupon' for
    # the rate curve and 'IGPM' for the inflation-index carry, analogous to
    # 'IPCA Coupon' / 'IPCA' for NTN-B/DAP. Mapping them to IPCA Coupon used
    # to silently drop NTN-C rows (keep_mask wouldn't match).
    _RATE_PRIM_BY_CLASS = {
        "NTN-B": "IPCA Coupon",
        "DAP Future": "IPCA Coupon", "DAPFuture": "IPCA Coupon",
        "NTN-C": "IGPM Coupon",
        "DAC Future": "IGPM Coupon",
        "DI1Future": "BRL Rate Curve", "NTN-F": "BRL Rate Curve",
        "LTN": "BRL Rate Curve",
        "LFT": "Brazil Sovereign Yield",
    }
    df["_rate_prim"] = df["PRODUCT_CLASS"].map(_RATE_PRIM_BY_CLASS)
    # Keep only the chosen rate primitive per product, OR the inflation-index
    # primitive ('IPCA' or 'IGPM' — face-value carry, never double-counts rates).
    keep_mask = (
        (df["PRIMITIVE_CLASS"] == df["_rate_prim"]) |
        (df["PRIMITIVE_CLASS"].isin(["IPCA", "IGPM"])) |
        df["_rate_prim"].isna()  # keep unknown product classes as 'other'
    )
    df = df[keep_mask].drop(columns="_rate_prim").copy()

    df["yrs_to_mat"] = (df["days_to_exp"].fillna(0.0) / 365.25)
    df["position_brl"] = df["position_brl"].fillna(0.0)
    # DELTA in LOTE_PRODUCT_EXPO is the duration-weighted notional (= POSITION × MOD_DURATION)
    # but with hedge-side sign convention: a long bond position carries NEGATIVE delta in the
    # rate-curve primitives (representing the short-DI hedge).
    # Negate it to recover the position's actual rate exposure (long bond → positive ANO_EQ).
    rate_prims = {"IPCA Coupon", "IGPM Coupon", "BRL Rate Curve"}
    df["ano_eq_brl"] = df.apply(
        lambda r: -r["delta_brl"] if r["PRIMITIVE_CLASS"] in rate_prims else r["delta_brl"],
        axis=1,
    )
    df["factor"]    = df["PRODUCT_CLASS"].map(_rf_classify).fillna("other")
    # Inflation-index primitives represent face-value carry — override factor.
    df.loc[df["PRIMITIVE_CLASS"] == "IPCA", "factor"] = "ipca_idx"
    df.loc[df["PRIMITIVE_CLASS"] == "IGPM", "factor"] = "igpm_idx"
    df["bucket"]    = df["yrs_to_mat"].apply(_rf_bucket)
    # 'via' already set by the SQL ('direct' | 'via_albatroz')
    return df


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
    nav = _require_nav("Galapagos Evolution FIC FIM CP", date_str)
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


def fetch_macro_pm_book_var(date_str: str = DATA_STR) -> dict[str, float]:
    """Book-report VaR por PM do MACRO — **não diversificado entre books**.
       Fonte: LOTE_FUND_STRESS_RPM (TRADING_DESK=Macro FIM, TREE=Main_Macro_Ativos,
       LEVEL=10). Cada BOOK é uma folha tipo CI_RF-BZ_Direcional, JD_COMMODITIES_*, etc.
       Soma-se |PARAMETRIC_VAR| por prefixo de PM → magnitude de perda em bps de NAV.
       Usa |·| por book para preservar o caráter conservador "não diversificado"
       (não permite hedge natural entre books do mesmo PM)."""
    nav = _require_nav("Galapagos Macro FIM", date_str)
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


def fetch_macro_pm_var_history(date_str: str = DATA_STR,
                                lookback_days: int = 121) -> pd.DataFrame:
    """MACRO PM-level VaR history (last `lookback_days` business days ending at
       `date_str`). Source: LOTE_FUND_STRESS_RPM (TRADING_DESK='Galapagos Macro
       FIM', TREE='Main_Macro_Ativos', LEVEL=10) — same query shape as the
       snapshot helper `fetch_macro_pm_book_var`, just expanded to a date range
       and pivoted to one column per PM.

       Output columns: VAL_DATE, CI, LF, JD, RJ — values in bps of NAV (positive
       loss magnitude). Within-PM book offsets are preserved before |·|, so this
       is diversified within a PM but not across PMs (consistent with snapshot)."""
    # Pull a wider date window (lookback × 1.45) so we cover weekends/holidays
    # and still land lookback_days *business* observations. Trim later.
    cal_window = max(lookback_days * 2, lookback_days + 30)
    df = read_sql(f"""
        SELECT "VAL_DATE", "BOOK", SUM("PARAMETRIC_VAR") AS var_brl
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "TRADING_DESK" = 'Galapagos Macro FIM'
          AND "TREE"         = 'Main_Macro_Ativos'
          AND "LEVEL"        = 10
          AND "VAL_DATE"    >= DATE '{date_str}' - INTERVAL '{cal_window} days'
          AND "VAL_DATE"    <= DATE '{date_str}'
        GROUP BY "VAL_DATE", "BOOK"
        ORDER BY "VAL_DATE"
    """)
    if df.empty:
        return pd.DataFrame(columns=["VAL_DATE", "CI", "LF", "JD", "RJ"])

    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
    rows: list[dict] = []
    for vd, sub in df.groupby("VAL_DATE"):
        # History loop: skip days with missing/non-positive NAV instead of raising —
        # one bad day shouldn't kill the whole 121-day series.
        nav = _latest_nav("Galapagos Macro FIM", vd.strftime("%Y-%m-%d"))
        if nav is None or nav <= 0:
            continue
        rec: dict = {"VAL_DATE": vd}
        for pm in ("CI", "LF", "JD", "RJ"):
            mask = sub["BOOK"].str.startswith(f"{pm}_") | (sub["BOOK"] == pm)
            v_brl_signed = float(sub.loc[mask, "var_brl"].sum())
            rec[pm] = float(abs(v_brl_signed) * 10000 / nav)
        rows.append(rec)

    out = pd.DataFrame(rows).sort_values("VAL_DATE")
    return out.tail(lookback_days).reset_index(drop=True)


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
      SUM(CASE WHEN "DATE" >= (DATE '{date_str}' - INTERVAL '12 months')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS m12_bps,
      SUM(CASE WHEN "DATE" = DATE '{date_str}'
               THEN COALESCE("POSITION",0) ELSE 0 END) AS position_brl
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" IN ('MACRO','QUANT','EVOLUTION','GLOBAL','ALBATROZ','BALTRA','GFA','IDKAIPCAY3','IDKAIPCAY10')
      AND "DATE" >= (DATE '{date_str}' - INTERVAL '12 months')
      AND "DATE" <= DATE '{date_str}'
    GROUP BY "FUNDO","CLASSE","GRUPO","LIVRO","BOOK","PRODUCT"
    """
    df = read_sql(q)
    num_cols = ["dia_bps","mtd_bps","ytd_bps","m12_bps","position_brl"]
    df[num_cols] = df[num_cols].astype(float).fillna(0.0)
    # Drop Macro_QM (descontinued PM) — historical PA rows linger but the book is closed.
    df = df[df["LIVRO"] != "Macro_QM"].reset_index(drop=True)
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

    unknown_pc = sorted(set(df["PRODUCT_CLASS"]) - set(_PRODCLASS_TO_FACTOR.keys()))
    if unknown_pc:
        print(f"WARNING [fetch_fund_position_changes:{short}]: dropped unknown PRODUCT_CLASS(es) {unknown_pc}",
              file=sys.stderr)
    df["factor"] = df["PRODUCT_CLASS"].map(lambda pc: _PRODCLASS_TO_FACTOR.get(pc))
    df = df[df["factor"].notna()]
    if df.empty:
        return None

    df["pct_nav"] = df["delta_brl"] * 100 / nav
    df["date_key"] = pd.to_datetime(df["val_date"]).dt.strftime("%Y-%m-%d")

    # When the entire D-1 side is missing (no rows for d1_str — e.g. Frontier
    # mainboard upstream has no history), emit no rows. Otherwise per-factor
    # NaNs would produce "D-1=+0.00% / D-0=+89%" phantom Δ rows that pass the
    # |Δ| gate as pure data-failure artifacts.
    pivot = df.pivot_table(
        index="factor", columns="date_key", values="pct_nav",
        aggfunc="sum", fill_value=0.0,
    )
    has_d1 = d1_str in pivot.columns
    if date_str not in pivot.columns: pivot[date_str] = 0.0
    if not has_d1:
        return None
    pivot["pct_d0"] = pivot[date_str]
    pivot["pct_d1"] = pivot[d1_str]
    pivot["delta"] = pivot["pct_d0"] - pivot["pct_d1"]
    return pivot[["pct_d0", "pct_d1", "delta"]].reset_index()


def fetch_fund_position_changes_by_product(short: str, date_str: str, d1_str: str) -> pd.DataFrame:
    """Instrument-level %NAV exposure D-0 vs D-1 for a fund.
    Returns df with (PRODUCT, factor, pct_d0, pct_d1, delta) or None if no data.
    Same scope rules as fetch_fund_position_changes."""
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
        SELECT "VAL_DATE" AS val_date, "PRODUCT", "PRODUCT_CLASS",
               SUM("DELTA") AS delta_brl
        FROM "LOTE45"."LOTE_PRODUCT_EXPO"
        WHERE {scope}
          AND "VAL_DATE" IN (DATE '{date_str}', DATE '{d1_str}')
          AND "DELTA" <> 0
        GROUP BY "VAL_DATE", "PRODUCT", "PRODUCT_CLASS"
    """)
    if df.empty:
        return None

    unknown_pc = sorted(set(df["PRODUCT_CLASS"]) - set(_PRODCLASS_TO_FACTOR.keys()))
    if unknown_pc:
        print(f"WARNING [fetch_fund_position_changes_by_product:{short}]: dropped unknown PRODUCT_CLASS(es) {unknown_pc}",
              file=sys.stderr)
    df["factor"] = df["PRODUCT_CLASS"].map(lambda pc: _PRODCLASS_TO_FACTOR.get(pc))
    df = df[df["factor"].notna()]
    if df.empty:
        return None

    df["pct_nav"] = df["delta_brl"] * 100 / nav
    df["date_key"] = pd.to_datetime(df["val_date"]).dt.strftime("%Y-%m-%d")

    # If D-1 side is entirely missing, emit no rows (avoids phantom Δ — see
    # twin guard in fetch_fund_position_changes above).
    pivot = df.pivot_table(
        index=["PRODUCT", "factor"], columns="date_key", values="pct_nav",
        aggfunc="sum", fill_value=0.0,
    )
    if d1_str not in pivot.columns:
        return None
    if date_str not in pivot.columns: pivot[date_str] = 0.0
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
      SUM(CASE WHEN "DATE" >= (DATE '{date_str}' - INTERVAL '12 months')
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


def fetch_usdbrl_returns(date_str: str = DATA_STR) -> dict:
    """BRL return vs USD over DIA/MTD/YTD/12M (bps). Source: public.FX_PRICES_SPOT
       (INSTRUMENT='BRL', column CLOSE = USD/BRL). Sign convention: USD/BRL up
       (BRL weakening) → negative (red); USD/BRL down (BRL strengthening) →
       positive (green). Returns the *negative* of the USD/BRL pct change."""
    q = f"""
    SELECT "DATE", "CLOSE"
    FROM public."FX_PRICES_SPOT"
    WHERE "INSTRUMENT" = 'BRL'
      AND "DATE" >= DATE '{date_str}' - INTERVAL '400 days'
      AND "DATE" <= DATE '{date_str}'
      AND "CLOSE" IS NOT NULL
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
        return -(p_now / p0 - 1.0) * 10000 if p0 else 0.0

    prev_day = last["CLOSE"].iloc[-2] if len(last) >= 2 else p_now
    dia = -(p_now / float(prev_day) - 1.0) * 10000 if prev_day else 0.0
    month_start = target.to_period("M").to_timestamp()
    mtd = ret(month_start - pd.Timedelta(days=1))
    year_start = pd.Timestamp(f"{target.year}-01-01")
    ytd = ret(year_start - pd.Timedelta(days=1))
    m12 = ret(target - pd.DateOffset(years=1))
    return {"dia": dia, "mtd": mtd, "ytd": ytd, "m12": m12}


def fetch_di1_3y_rate(date_str: str = DATA_STR, target_bdays: int = 756) -> dict:
    """3y-forward DI1 rate (constant tenor, rolls with date). For each DATE, picks
       the DI1 contract whose BDAYS is closest to target_bdays (default 756 ≈ 3y).
       Returns: {"rate": pct a.a., "prev": pct a.a., "dia_bps": rate change in bps}."""
    q = f"""
    SELECT "DATE", "INSTRUMENT", "CLOSE", "BDAYS"
    FROM public."FUTURES_PRICES"
    WHERE "INSTRUMENT" ~ '^DI1'
      AND "DATE" >= DATE '{date_str}' - INTERVAL '15 days'
      AND "DATE" <= DATE '{date_str}'
      AND "CLOSE" IS NOT NULL
      AND "BDAYS" BETWEEN {max(1, target_bdays - 252)} AND {target_bdays + 252}
    ORDER BY "DATE", "BDAYS"
    """
    df = read_sql(q)
    if df.empty:
        return {"rate": None, "prev": None, "dia_bps": 0.0, "instrument": None}
    df["DATE"] = pd.to_datetime(df["DATE"])
    df["dist"] = (df["BDAYS"] - target_bdays).abs()
    nearest = df.loc[df.groupby("DATE")["dist"].idxmin()].sort_values("DATE")
    if nearest.empty:
        return {"rate": None, "prev": None, "dia_bps": 0.0, "instrument": None}
    rate = float(nearest["CLOSE"].iloc[-1])
    instrument = str(nearest["INSTRUMENT"].iloc[-1])
    prev = float(nearest["CLOSE"].iloc[-2]) if len(nearest) >= 2 else rate
    dia_bps = (rate - prev) * 100
    return {"rate": rate, "prev": prev, "dia_bps": dia_bps, "instrument": instrument}


# ── Book PnL + Peers ──────────────────────────────────────────────────────────

_BOOK_PNL_FUND_MAP: dict[str, str] = {
    "Galapagos Macro FIM":            "Macro",
    "Galapagos Evolution FIC FIM CP": "Evolution",
    "Galapagos Quantitativo FIM":     "Quantitativo",
    "Galapagos Global Macro Q":       "Macro Q",
    "Frontier Ações FIC FI":          "Frontier",
    "FRONTIER LONG BIAS":             "Frontier LB",
    "GALAPAGOS ALBATROZ FIRF LP":     "Albatroz",
}

_PEERS_FILE = Path(os.environ.get(
    "PEERS_DATA_PATH",
    r"\\fs02\FS_GALAPAGOS\Bloomberg\Quant\Claude_GLPG_Fetch\peers_data.json",
))


def fetch_book_pnl(date_str: str = DATA_STR) -> dict:
    """Daily PnL by book / class / position for a specific date.

    Returns {generated_at, val_date, funds} where funds maps display name →
    {desk, total_pl, total_pl_pct, total_trade_pl, books[{book, pl, pl_pct,
    classes[{class, pl, pl_pct, positions[...]}]}]}.
    """
    fund_in = ", ".join(f"'{n}'" for n in _BOOK_PNL_FUND_MAP)
    q = f"""
        SELECT "TRADING_DESK","BOOK","PRODUCT_CLASS","DESCRIPTION",
               "PL","PL_PCT","POSITION_PL","TRADES_PL",
               "POSITION","AMOUNT","VAL_DATE"
        FROM "LOTE45"."LOTE_PRODUCT_BOOK_POSITION_PL"
        WHERE "TRADING_DESK" IN ({fund_in})
          AND "VAL_DATE" = DATE '{date_str}'
        ORDER BY "TRADING_DESK","BOOK","PRODUCT_CLASS","PL" DESC
    """
    df = read_sql(q)
    if df.empty:
        return {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
            "val_date": date_str,
            "funds": {},
        }

    val_date_str = str(df["VAL_DATE"].iloc[0])
    funds: dict[str, dict] = {}
    for desk, fdf in df.groupby("TRADING_DESK", sort=False):
        short = _BOOK_PNL_FUND_MAP.get(desk, desk)
        # Denominator for bps: fund NAV at val_date. Σ PL_PCT (per-position
        # PL/AMOUNT) is meaningless when aggregated by book/PM. Recompute
        # bps everywhere as PL / fund_nav so book/class/position bps add up.
        fund_nav = _latest_nav(desk, val_date_str)
        if not fund_nav:
            # Fallback: |sum AMOUNT| as NAV proxy (less accurate). If the
            # book is empty / cancelled, abs sum can be 0 — leave fund_nav
            # None and let _bps_pct short-circuit to 0.0 with a WARN.
            _abs_sum = abs(float(fdf["AMOUNT"].sum()))
            if _abs_sum > 1e-9:
                fund_nav = _abs_sum
            else:
                print(f"  [WARN] fetch_book_pnl: fund_nav unavailable for {desk} on {val_date_str} — bps will be 0")
                fund_nav = None

        def _bps_pct(pl: float) -> float:
            """Returns PL/NAV as decimal (frontend multiplies by 10000)."""
            if not fund_nav:
                return 0.0
            return float(pl) / float(fund_nav)

        books: list[dict] = []
        for book, bdf in fdf.groupby("BOOK", sort=False):
            classes: list[dict] = []
            for cls, cdf in bdf.groupby("PRODUCT_CLASS", sort=False):
                positions = sorted(
                    [
                        {
                            "desc":     str(r.DESCRIPTION or ""),
                            "pl":       round(float(r.PL or 0),          2),
                            "pl_pct":   round(_bps_pct(r.PL or 0),       8),
                            "pos_pl":   round(float(r.POSITION_PL or 0), 2),
                            "trade_pl": round(float(r.TRADES_PL or 0),   2),
                            "position": round(float(r.POSITION or 0),    2),
                            "amount":   round(float(r.AMOUNT or 0),      2),
                        }
                        for r in cdf.itertuples(index=False)
                    ],
                    key=lambda x: abs(x["pl"]), reverse=True,
                )
                cls_pl = float(cdf["PL"].sum())
                classes.append({
                    "class":     cls,
                    "pl":        round(cls_pl, 2),
                    "pl_pct":    round(_bps_pct(cls_pl), 8),
                    "positions": positions,
                })
            classes.sort(key=lambda x: abs(x["pl"]), reverse=True)
            book_pl = float(bdf["PL"].sum())
            books.append({
                "book":    book,
                "pl":      round(book_pl, 2),
                "pl_pct":  round(_bps_pct(book_pl), 8),
                "classes": classes,
            })
        books.sort(key=lambda x: abs(x["pl"]), reverse=True)
        fund_pl = float(fdf["PL"].sum())
        funds[short] = {
            "desk":          desk,
            "nav":           round(float(fund_nav), 2) if fund_nav else None,
            "total_pl":      round(fund_pl, 2),
            "total_pl_pct":  round(_bps_pct(fund_pl), 8),
            "total_trade_pl":round(float(fdf["TRADES_PL"].sum()), 2),
            "books":         books,
        }

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        "val_date": val_date_str,
        "funds": funds,
    }


_EXCEL_MARKET_PATH = os.environ.get(
    "MARKET_FALLBACK_XLSM",
    r"F:\Macros\Gestao\Miguel Ishimura\Prices_Close_Global - Copy.xlsm",
)


def _read_excel_market_fallback() -> dict:
    """Read current Bloomberg values from open Excel workbook (Prices_Close_Global).

    Returns {row_label: row_dict} with value/chg/chg_pct/chg_5d/chg_pct_5d/chg_1m/chg_pct_1m.
    Empty dict if Bloomberg/Excel not running or workbook not open.
    Column indices (0-based) assumed: label=0, BBG=1, val=2, chg_net=3, chg_pct=4,
    arrow=5, chg5_net=6, chg5_pct=7, arrow=8, chg1m_net=9, chg1m_pct=10.
    """
    import os
    try:
        import xlwings as xw
    except ImportError:
        return {}
    try:
        if not xw.apps:
            return {}
        app = xw.apps.active
        fname = os.path.basename(_EXCEL_MARKET_PATH).lower()
        wb = next((b for b in app.books
                   if os.path.basename(b.fullname).lower() == fname), None)
        if wb is None:
            wb = app.books.open(_EXCEL_MARKET_PATH)

        _BBG_ERR = -2146826259

        def _safe(v) -> float | None:
            if not isinstance(v, (int, float)):
                return None
            if abs(v - _BBG_ERR) < 1:
                return None
            return float(v)

        result: dict = {}
        # sheet → (label_col, val, chg_net, chg_pct, chg5_net, chg5_pct, chg1m_net, chg1m_pct)
        sheet_map = {
            "Janelas":     (0, 2, 3, 4, 6, 7,  9, 10),
            "Moedas":      (0, 2, 3, 4, 6, 7,  9, 10),
            "Commodities": (1, 3, 4, 5, 7, 8, 10, 11),
        }
        for shname, (lc, vc, nc, pc, n5c, p5c, n1c, p1c) in sheet_map.items():
            try:
                sh = wb.sheets[shname]
                data = sh.used_range.value
                if not data:
                    continue
                for row in data:
                    if not row or len(row) <= vc:
                        continue
                    label = str(row[lc]).strip() if row[lc] else ""
                    if not label or len(label) < 2:
                        continue
                    val = _safe(row[vc])
                    if val is None:
                        continue

                    def _get(idx):
                        return _safe(row[idx]) if len(row) > idx else None

                    result[label] = dict(
                        value=val,
                        chg=_get(nc), chg_pct=_get(pc),
                        chg_5d=_get(n5c), chg_pct_5d=_get(p5c),
                        chg_1m=_get(n1c), chg_pct_1m=_get(p1c),
                        history=[], from_excel=True,
                    )
            except Exception:
                continue
        return result
    except Exception:
        return {}


def fetch_market_snapshot(date_str: str) -> dict:
    """Comprehensive market snapshot — mirrors Excel Ferramenta_Diario structure.

    Sections: Janelas (Bolsas/Juros/Moeda/Commodities groups), Moedas, Commodities.
    Primary source: GLPG-DB01 (sparklines + history).
    Fallback: Bloomberg-linked Excel at _EXCEL_MARKET_PATH (current value + 1D/5D/1M).
    Row keys: label, value, unit, fmt, decimals, rate_mode, history,
              chg, chg_pct, chg_5d, chg_pct_5d, chg_1m, chg_pct_1m.
    """
    import logging as _logging
    from datetime import timedelta
    log = _logging.getLogger(__name__)
    start = (pd.Timestamp(date_str) - timedelta(days=70)).strftime("%Y-%m-%d")

    xl: dict = {}
    try:
        xl = _read_excel_market_fallback()
        if xl:
            log.info("Excel market fallback: %d instruments loaded", len(xl))
    except Exception:
        pass

    # ── Row builder ───────────────────────────────────────────────────────────
    def _row(label: str, df, val_col: str = "value", unit: str = "",
             fmt: str = "price", decimals: int = 2, rate_mode: bool = False,
             xl_key: str | None = None) -> dict:
        base: dict = dict(label=label, value=None, unit=unit, fmt=fmt,
                          decimals=decimals, rate_mode=rate_mode, history=[],
                          chg=None, chg_pct=None,
                          chg_5d=None, chg_pct_5d=None,
                          chg_1m=None, chg_pct_1m=None)
        has_db = df is not None and not df.empty
        if has_db:
            df = df.dropna(subset=[val_col]).copy()
            has_db = not df.empty
        if has_db:
            vals = df[val_col].astype(float)
            base["history"] = [None if pd.isna(v) else float(v)
                                for v in vals.tail(21).values]
            curr = float(vals.iloc[-1])
            base["value"] = curr

            def _delta(n: int):
                if len(vals) <= n:
                    return None, None
                prev = float(vals.iloc[-(n + 1)])
                d = curr - prev
                if rate_mode:
                    return round(d * 100, 2), None  # bps, no %
                pct = d / abs(prev) if prev else None
                return (round(d, max(decimals + 2, 4)),
                        round(pct, 6) if pct is not None else None)

            base["chg"],    base["chg_pct"]    = _delta(1)
            base["chg_5d"], base["chg_pct_5d"] = _delta(5)
            base["chg_1m"], base["chg_pct_1m"] = _delta(21)

        # Excel override: fresher Bloomberg data for value + changes
        key = xl_key or label
        if key in xl:
            xr = xl[key]
            if xr.get("value") is not None:
                for k in ("value", "chg", "chg_pct", "chg_5d", "chg_pct_5d",
                          "chg_1m", "chg_pct_1m"):
                    if xr.get(k) is not None:
                        base[k] = xr[k]
        return base

    # ── DB fetchers ───────────────────────────────────────────────────────────
    def _eco(instrument, field, label, unit="% a.a.", fmt="rate", decimals=2,
             scale=1.0, rate_mode=False, xl_key=None):
        try:
            df = read_sql(f"""
                SELECT "DATE", "VALUE" * {scale} AS value FROM public."ECO_INDEX"
                WHERE "INSTRUMENT" = '{instrument}' AND "FIELD" = '{field}'
                  AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{date_str}'
                  AND "VALUE" IS NOT NULL ORDER BY "DATE"
            """)
            return _row(label, df, unit=unit, fmt=fmt, decimals=decimals,
                        rate_mode=rate_mode, xl_key=xl_key)
        except Exception as e:
            log.warning("market ECO %s: %s", instrument, e)
            return _row(label, None, unit=unit, fmt=fmt, decimals=decimals,
                        rate_mode=rate_mode, xl_key=xl_key)

    def _fx(instrument, label, unit="R$/USD", decimals=4, xl_key=None):
        try:
            df = read_sql(f"""
                SELECT "DATE", "CLOSE" AS value FROM public."FX_PRICES_SPOT"
                WHERE "INSTRUMENT" = '{instrument}'
                  AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{date_str}'
                  AND "CLOSE" IS NOT NULL ORDER BY "DATE"
            """)
            return _row(label, df, unit=unit, fmt="price", decimals=decimals,
                        xl_key=xl_key)
        except Exception as e:
            log.warning("market FX %s: %s", instrument, e)
            return _row(label, None, unit=unit, fmt="price", decimals=decimals,
                        xl_key=xl_key)

    def _eq_br(label, instrument="IBOV", unit="pts", decimals=0, xl_key=None):
        try:
            df = read_sql(f"""
                SELECT "DATE", "CLOSE" AS value FROM public."EQUITIES_PRICES"
                WHERE "INSTRUMENT" = '{instrument}'
                  AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{date_str}'
                  AND "CLOSE" IS NOT NULL ORDER BY "DATE"
            """)
            return _row(label, df, unit=unit, fmt="index", decimals=decimals,
                        xl_key=xl_key)
        except Exception as e:
            log.warning("market BR eq %s: %s", instrument, e)
            return _row(label, None, unit=unit, fmt="index", decimals=decimals,
                        xl_key=xl_key)

    def _eq_global(ticker, label, unit="USD", decimals=2, xl_key=None):
        try:
            df = read_sql(f"""
                SELECT e."DATE", e."CLOSE" AS value
                FROM public."PRICES_GLOBAL_EQUITIES" e
                JOIN public."MAPS_GLOBAL_EQUITIES" m
                  ON m."GLOBAL_EQUITIES_KEY" = e."GLOBAL_EQUITIES_KEY"
                WHERE m."TICKER_RT" = '{ticker}'
                  AND e."DATE" >= DATE '{start}' AND e."DATE" <= DATE '{date_str}'
                  AND e."CLOSE" IS NOT NULL ORDER BY e."DATE" LIMIT 500
            """)
            return _row(label, df, unit=unit, fmt="index", decimals=decimals,
                        xl_key=xl_key)
        except Exception as e:
            log.warning("market global eq %s: %s", ticker, e)
            return _row(label, None, unit=unit, fmt="index", decimals=decimals,
                        xl_key=xl_key)

    def _di(label, target_bdays, lo, hi, xl_key=None):
        try:
            raw = read_sql(f"""
                SELECT "DATE", "CLOSE" AS rate, "BDAYS" FROM public."FUTURES_PRICES"
                WHERE "INSTRUMENT" ~ '^DI1'
                  AND "DATE" >= DATE '{start}' AND "DATE" <= DATE '{date_str}'
                  AND "CLOSE" IS NOT NULL AND "BDAYS" BETWEEN {lo} AND {hi}
                ORDER BY "DATE", "BDAYS"
            """)
            if raw.empty:
                return _row(label, None, unit="% a.a.", fmt="rate", decimals=2,
                            rate_mode=True, xl_key=xl_key)
            raw["dist"] = (raw["BDAYS"] - target_bdays).abs()
            best = (raw.loc[raw.groupby("DATE")["dist"].idxmin()]
                      [["DATE", "rate"]].rename(columns={"rate": "value"}))
            return _row(label, best, unit="% a.a.", fmt="rate", decimals=2,
                        rate_mode=True, xl_key=xl_key)
        except Exception as e:
            log.warning("market DI %s: %s", label, e)
            return _row(label, None, unit="% a.a.", fmt="rate", decimals=2,
                        rate_mode=True, xl_key=xl_key)

    def _us_rate(tenour, label, xl_key=None):
        try:
            df = read_sql(f"""
                SELECT yc."DATE", yc."YIELD" AS value
                FROM public."YIELDS_GLOBAL_CURVES" yc
                JOIN public."MAPS_GLOBAL_CURVES" mc
                  ON mc."GLOBAL_CURVES_KEY" = yc."GLOBAL_CURVES_KEY"
                WHERE mc."CHAIN" = 'US_TREASURY_CONSTANT_MATURITY'
                  AND yc."TENOUR" = {tenour}
                  AND yc."DATE" >= DATE '{start}' AND yc."DATE" <= DATE '{date_str}'
                  AND yc."YIELD" IS NOT NULL ORDER BY yc."DATE"
            """)
            return _row(label, df, unit="% a.a.", fmt="rate", decimals=3,
                        rate_mode=True, xl_key=xl_key)
        except Exception as e:
            log.warning("market US rate %s: %s", tenour, e)
            return _row(label, None, unit="% a.a.", fmt="rate", decimals=3,
                        rate_mode=True, xl_key=xl_key)

    def _fut_global(cod, label, unit="USD", decimals=2, xl_key=None):
        """CME global futures — front-month (nearest DAYS_TO_EXP > 0) per date."""
        try:
            df = read_sql(f"""
                SELECT fp."DATE", fp."CLOSE" AS value, fp."DAYS_TO_EXP"
                FROM public."FUTURES_GLOBAL_PRICES" fp
                JOIN public."FUTURES_GLOBAL_MAP" m ON m."INSTRUMENT" = fp."INSTRUMENT"
                WHERE m."COD_NAME" = '{cod}'
                  AND fp."DATE" >= DATE '{start}' AND fp."DATE" <= DATE '{date_str}'
                  AND fp."CLOSE" IS NOT NULL
                  AND fp."DAYS_TO_EXP" > 0
                ORDER BY fp."DATE", fp."DAYS_TO_EXP" ASC
                LIMIT 2000
            """)
            if df.empty:
                return _row(label, None, unit=unit, fmt="index", decimals=decimals,
                            xl_key=xl_key)
            df = (df.sort_values(["DATE", "DAYS_TO_EXP"])
                    .groupby("DATE")["value"].first()
                    .reset_index())
            return _row(label, df, unit=unit, fmt="index", decimals=decimals,
                        xl_key=xl_key)
        except Exception as e:
            log.warning("market futures %s: %s", cod, e)
            return _row(label, None, unit=unit, fmt="index", decimals=decimals,
                        xl_key=xl_key)

    def _null(label, unit="", fmt="price", decimals=2, rate_mode=False, xl_key=None):
        """Placeholder row — Excel-only instrument (no DB source)."""
        return _row(label, None, unit=unit, fmt=fmt, decimals=decimals,
                    rate_mode=rate_mode, xl_key=xl_key)

    # ── Section 1: Janelas ────────────────────────────────────────────────────
    janelas_groups = [
        {"label": "Bolsas", "rows": [
            _eq_br("Ibovespa",    "IBOV",  unit="pts",  decimals=0, xl_key="Ibovespa"),
            _eq_global("SPY",  "S&P 500",    unit="USD",  decimals=2, xl_key="S&P 500 BMF"),
            _eq_global("QQQ",  "Nasdaq",     unit="USD",  decimals=2, xl_key="Nasdaq"),
            _eq_global("DIA",  "Dow Jones",  unit="USD",  decimals=2, xl_key="Dow Jones"),
            _eq_global("IWM",  "Russell 2000", unit="USD", decimals=2, xl_key="Russell 2000"),
            _eq_global("MCHI", "MSCI China", unit="USD",  decimals=2, xl_key="MSCI China"),
        ]},
        {"label": "Juros", "rows": [
            _us_rate(504,  "US 2Y",  xl_key="US 2 YR"),
            _us_rate(1260, "US 5Y",  xl_key="US 5 YR"),
            _us_rate(2520, "US 10Y", xl_key="US 10 YR"),
            _di("DI Jan27", 189,  90, 280,  xl_key="DI Jan27"),
            _di("DI Jan30", 945, 700, 1100, xl_key="DI Jan30"),
            _di("DI Jan31", 1197, 1000, 1400, xl_key="DI Jan31"),
        ]},
        {"label": "Moeda", "rows": [
            _null("DXY",     unit="",       fmt="price", decimals=2, xl_key="DXY"),
            _fx("EUR",  "EUR/USD", unit="EUR/USD", decimals=4, xl_key="EUR"),
            _fx("JPY",  "USD/JPY", unit="USD/JPY", decimals=2, xl_key="JPY"),
            _fx("CHF",  "USD/CHF", unit="USD/CHF", decimals=4, xl_key="CHF"),
            _fx("GBP",  "GBP/USD", unit="GBP/USD", decimals=4, xl_key="GBP"),
            _fx("BRL",  "USD/BRL", unit="R$/USD",  decimals=4, xl_key="BRL"),
            _fx("MXN",  "USD/MXN", unit="USD/MXN", decimals=4, xl_key="MXN"),
        ]},
        {"label": "Commodities", "rows": [
            _fut_global("GC", "Gold",        unit="USD/oz",   decimals=1, xl_key="Gold (Spot)"),
            _fut_global("CL", "WTI Crude",   unit="USD/bbl",  decimals=2, xl_key="WTI Oil"),
            _fut_global("NG", "Natural Gas",  unit="USD/MMBtu",decimals=3, xl_key="Gás Natural"),
            _fut_global("SI", "Silver",      unit="USD/oz",   decimals=2, xl_key="SLV"),
            _fut_global("HG", "Cobre",       unit="USc/lb",   decimals=3, xl_key="CPER"),
            _fut_global("ZS", "Soja",        unit="USc/bu",   decimals=2, xl_key="Soja"),
            _fut_global("ZC", "Milho",       unit="USc/bu",   decimals=2, xl_key="Milho"),
        ]},
    ]

    # ── Section 2: Moedas ─────────────────────────────────────────────────────
    moedas_rows = [
        _null("DXY",     unit="",       fmt="price", decimals=2, xl_key="DXY"),
        _fx("GBP", "GBP/USD", unit="GBP/USD", decimals=4, xl_key="Reino Unido"),
        _fx("JPY", "USD/JPY", unit="USD/JPY", decimals=2, xl_key="Japao"),
        _fx("CAD", "USD/CAD", unit="USD/CAD", decimals=4, xl_key="Canadá"),
        _fx("CHF", "USD/CHF", unit="USD/CHF", decimals=4, xl_key="Suíça"),
        _fx("EUR", "EUR/USD", unit="EUR/USD", decimals=4, xl_key="Euro"),
        _fx("SEK", "USD/SEK", unit="USD/SEK", decimals=3, xl_key="Suécia"),
        _fx("NOK", "USD/NOK", unit="USD/NOK", decimals=3, xl_key="Noruega"),
        _fx("NZD", "NZD/USD", unit="NZD/USD", decimals=4, xl_key="N. Zelândia"),
        _fx("AUD", "AUD/USD", unit="AUD/USD", decimals=4, xl_key="Austrália"),
        _fx("BRL", "USD/BRL", unit="R$/USD",  decimals=4, xl_key="Brasil"),
        _fx("MXN", "USD/MXN", unit="USD/MXN", decimals=4, xl_key="México"),
        _fx("CLP", "USD/CLP", unit="USD/CLP", decimals=1, xl_key="Chile"),
        _fx("COP", "USD/COP", unit="USD/COP", decimals=0, xl_key="Colômbia"),
        _fx("CZK", "USD/CZK", unit="USD/CZK", decimals=3, xl_key="Rep. Checa"),
        _fx("HUF", "USD/HUF", unit="USD/HUF", decimals=1, xl_key="Hungria"),
        _fx("PLN", "USD/PLN", unit="USD/PLN", decimals=4, xl_key="Polônia"),
        _fx("KRW", "USD/KRW", unit="USD/KRW", decimals=0, xl_key="Coréia"),
        _fx("ZAR", "USD/ZAR", unit="USD/ZAR", decimals=4, xl_key="Africa do Sul"),
    ]

    # ── Section 3: Commodities ────────────────────────────────────────────────
    commodities_groups = [
        {"label": "Metais Industriais", "rows": [
            _fut_global("HG", "Cobre",    unit="USc/lb",  decimals=3, xl_key="Cobre"),
            _null("Minério de Ferro",      unit="USD/t",   fmt="index", decimals=2, xl_key="Minério Cing."),
            _fut_global("PA", "Paládio",  unit="USD/oz",  decimals=1, xl_key="Paladium"),
            _fut_global("PL", "Platina",  unit="USD/oz",  decimals=1, xl_key="Platina"),
        ]},
        {"label": "Energia", "rows": [
            _fut_global("CL", "WTI Crude",   unit="USD/bbl",    decimals=2, xl_key="WTI Oil"),
            _fut_global("BZ", "Brent",       unit="USD/bbl",    decimals=2, xl_key="Brent Crude"),
            _fut_global("NG", "Natural Gas", unit="USD/MMBtu",  decimals=3, xl_key="Gás Natural"),
            _fut_global("RB", "Gasoline",    unit="USD/gal",    decimals=3, xl_key="Gasoline RBOB"),
        ]},
        {"label": "Metais Preciosos", "rows": [
            _fut_global("GC", "Gold",   unit="USD/oz", decimals=1, xl_key="Gold (Spot)"),
            _fut_global("SI", "Silver", unit="USD/oz", decimals=2, xl_key="SLV"),
        ]},
        {"label": "Agricultura", "rows": [
            _fut_global("ZS", "Soja",  unit="USc/bu", decimals=2, xl_key="Soja"),
            _fut_global("ZC", "Milho", unit="USc/bu", decimals=2, xl_key="Milho"),
            _fut_global("ZW", "Trigo", unit="USc/bu", decimals=2, xl_key="Trigo"),
        ]},
    ]

    return {
        "val_date": date_str,
        "sections": [
            {"label": "Janelas",     "type": "groups", "groups": janelas_groups},
            {"label": "Moedas",      "type": "rows",   "rows":   moedas_rows},
            {"label": "Commodities", "type": "groups", "groups": commodities_groups},
        ],
    }


_PEERS_ARCHIVE_DIR = Path(__file__).parent / "data" / "peers_archive"


def _peers_unwrap(data: dict) -> dict:
    """Strip legacy {latest: {...}} wrapper if present."""
    if "val_date" in data:
        return data
    return data.get("latest", {})


def _peers_auto_archive(snap: dict) -> None:
    """Persist the network snapshot under data/peers_archive/peers_data_<val_date>.json
    (idempotent — only writes if file doesn't exist yet)."""
    val_date = snap.get("val_date")
    if not val_date:
        return
    _PEERS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    target = _PEERS_ARCHIVE_DIR / f"peers_data_{val_date}.json"
    if target.exists():
        return
    try:
        target.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass  # archive is best-effort; don't break the run if it fails


def _end_of_prev_month(date_str: str) -> str:
    """Last calendar day of the month preceding date_str (YYYY-MM-DD).
    Peers data is monthly cadence, so we compare apples-to-apples by anchoring
    to month-end snapshots (e.g. report on 15/Apr → March-end peers).
    """
    import calendar
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    if dt.month == 1:
        py, pm = dt.year - 1, 12
    else:
        py, pm = dt.year, dt.month - 1
    last = calendar.monthrange(py, pm)[1]
    return f"{py:04d}-{pm:02d}-{last:02d}"


def fetch_peers_data(date_str: str = DATA_STR, mode: str = "current") -> dict:
    """Peers snapshot for a target date — picks archived JSON.

    Modes:
      - "current": latest snapshot with val_date ≤ date_str (closest before).
                   Use this for "live" view at report date.
      - "eopm":    snapshot with val_date ≤ end-of-previous-month relative to
                   date_str. Use for apples-to-apples month-end comparison
                   (peers report monthly).

    Behaviour for both modes:
      1. Read the network JSON (latest) and auto-archive under
         data/peers_archive/peers_data_<val_date>.json (idempotent).
      2. Pick best archive ≤ anchor. If none, fall back to network with
         `_is_stale=True`.

    Returns {val_date, benchmarks, groups, _target_date, _anchor_date,
    _mode, _is_stale}. Empty dict if no source available.
    """
    network_snap: dict = {}
    try:
        network_snap = _peers_unwrap(json.loads(_PEERS_FILE.read_text(encoding="utf-8")))
        _peers_auto_archive(network_snap)
    except (OSError, json.JSONDecodeError):
        pass

    anchor = _end_of_prev_month(date_str) if mode == "eopm" else date_str

    best: tuple[str, dict] | None = None
    if _PEERS_ARCHIVE_DIR.exists():
        for f in sorted(_PEERS_ARCHIVE_DIR.glob("peers_data_*.json")):
            vd = f.stem.replace("peers_data_", "")
            if vd <= anchor and (best is None or vd > best[0]):
                try:
                    best = (vd, json.loads(f.read_text(encoding="utf-8")))
                except (OSError, json.JSONDecodeError):
                    continue

    if best is not None:
        snap = dict(best[1])
        snap["_target_date"] = date_str
        snap["_anchor_date"] = anchor
        snap["_mode"] = mode
        snap["_is_stale"] = (best[0] != anchor)
        return snap

    if network_snap:
        snap = dict(network_snap)
        snap["_target_date"] = date_str
        snap["_anchor_date"] = anchor
        snap["_mode"] = mode
        snap["_is_stale"] = (snap.get("val_date") != anchor)
        return snap

    return {}


# ──────────────────────────────────────────────────────────────────────────────
# Credit positions via SHARE_SOURCE look-through (BALTRA / EVOLUTION)
# ──────────────────────────────────────────────────────────────────────────────

_CREDIT_PRODUCT_CLASSES = (
    'Debenture', 'Debenture Infra',
    'CRI', 'CRA',
    'FIDC', 'FIDC NP',
    'NTN-B', 'NTN-C', 'NTN-F', 'LFT', 'LTN',
    'Funds BR',  # FIDC cotas live here; vanilla bond funds get filtered out below
    'Nota Comercial', 'Nota Comercial DI Spread',
)


def fetch_fund_credit_positions(trading_desk: str, date_str: str = DATA_STR) -> pd.DataFrame:
    """Look-through credit-relevant positions for any fund (used for the
    main report's BALTRA/EVOLUTION Crédito section).

    Source: ``LOTE45.LOTE_PRODUCT_EXPO`` filtered by ``TRADING_DESK_SHARE_SOURCE``
    (per the share-source rule — captures direct holdings + nested look-through).
    Joined to ``credit.asset_master`` for tipo_ativo / spread / rating / setor /
    grupo_economico / subordinacao. Deduplicated to one row per (product, book)
    since LOTE_PRODUCT_EXPO repeats the position across primitives.

    Returns a DataFrame with columns matching the credit-report renderers'
    expectations: book, product_class, produto, pos_brl, pl_position, pl,
    tipo_ativo, classe, indexador, spread, am_duration, rating, setor,
    grupo_economico, apelido_emissor, data_vencimento, data_emissao,
    taxa_emissao, subordinacao.
    """
    classes_in = ", ".join(f"'{c}'" for c in _CREDIT_PRODUCT_CLASSES)
    sql = f"""
SELECT DISTINCT ON (e."PRODUCT", e."BOOK")
  e."BOOK"           AS book,
  e."PRODUCT_CLASS"  AS product_class,
  e."PRODUCT"        AS produto,
  e."POSITION"       AS pos_brl,
  NULL::numeric      AS pl_position,
  NULL::numeric      AS pl,
  COALESCE(am.tipo_ativo, e."PRODUCT_CLASS") AS tipo_ativo,
  am.classe,
  am.indexador,
  am.spread,
  am.duration        AS am_duration,
  am.rating,
  am.setor,
  am.grupo_economico,
  am.apelido_emissor,
  am.data_vencimento,
  am.data_emissao,
  am.taxa_emissao,
  am.subordinacao
FROM "LOTE45"."LOTE_PRODUCT_EXPO" e
LEFT JOIN credit.asset_master am
  ON LOWER(am.nome_lote45) = LOWER(e."PRODUCT")
WHERE e."TRADING_DESK_SHARE_SOURCE" = '{trading_desk}'
  AND e."VAL_DATE" = '{date_str}'
  AND e."PRODUCT_CLASS" IN ({classes_in})
  AND e."POSITION" > 0
ORDER BY e."PRODUCT", e."BOOK", e."PRIMITIVE_NAME"
"""
    df = read_sql(sql)

    if df.empty:
        return df

    # Funds BR filter: keep only FIDCs (i.e., rows where am.tipo_ativo says
    # FIDC/FIDC NP). Vanilla bond funds (Pinzon FIRF Ref DI etc.) aren't
    # credit-risk holdings. Sovereigns + tranched corp credit pass through.
    is_funds_br = df["product_class"] == "Funds BR"
    is_fidc_cota = df["tipo_ativo"].isin(["FIDC", "FIDC NP"])
    df = df[(~is_funds_br) | is_fidc_cota].reset_index(drop=True)

    # Apply known issuer-name overrides (e.g., all "Cruz" → Santa Cruz)
    from credit.credit_data import normalize_issuer_overrides
    df = normalize_issuer_overrides(df)

    return df

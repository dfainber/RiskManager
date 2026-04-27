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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from glpg_fetch import read_sql
from risk_runtime import DATA_STR, DATA, DATE_1Y, DATE_60D
from risk_config import (
    ALL_FUNDS, FUNDS, RAW_FUNDS, IDKA_FUNDS,
    _PM_LIVRO, _ETF_TO_LIST,
    _EXCL_PRIM, _RATE_PRIM,
    _QUANT_BOOK_FACTOR, _QUANT_VAR_BOOK_FACTOR,
    _RF_FACTOR_MAP, _RF_BUCKETS,
    _EVO_LIVRO_EXTRA_STRATEGY,
    _FUND_DESK_FOR_EXPO,
    _PRODCLASS_TO_FACTOR,
)
from db_helpers import _parse_rf, _parse_pm, _prev_bday, _NAV_CACHE, _latest_nav


def fetch_pm_pnl_history() -> pd.DataFrame:
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
    # DATE_TRUNC('month', ...) vem do Postgres como TIMESTAMPTZ; se parsearmos
    # com utc=True + tz_localize(None) o horário fica defasado em +3h (2026-04-01
    # UTC virou 03:00 wall-clock naive). Converter p/ BRT antes de strip preserva
    # o timestamp 00:00:00 local → match com `cur_mes = Timestamp(data).to_period('M').to_timestamp()`.
    s = pd.to_datetime(df["mes"], utc=True)
    df["mes"] = s.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
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


def _ntnb_total_return_pct_change(prices: pd.Series,
                                   maturity: pd.Timestamp | None = None) -> pd.Series:
    """Adjust ANBIMA clean-price pct_change to approximate NTN-B total-return.

    Why: ANBIMA's UNIT_PRICE is the clean (ex-coupon) price. On NTN-B coupon
    dates the clean price drops by ~ the semi-coupon (~2.956%), producing a
    spurious -200~300 bps return that does not exist in the IDKA index
    (which reinvests coupons internally).

    Coupon dates: NTN-B pays coupons every 6 months on its maturity-anniversary
    (e.g. NTN-B 2030-08-15 → coupons every Feb 15 / Aug 15). Without the
    `maturity` arg we fall back to the most common pair (May 15 / Nov 15),
    which only covers part of the curve.

    Fix: at each coupon transition captured in the series (the prior quote is
    strictly before the coupon date), add back the semi-coupon to that day's
    return: r_TR = (1 + r_clean) * (1 + c) - 1.
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
            if pd.notna(rets.loc[first]):
                rets.loc[first] = (1 + rets.loc[first]) * (1 + _NTNB_SEMI_COUPON) - 1
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


def fetch_idka_hs_replication_series(portfolio_name: str, target_anos: int, tenour_du: int,
                                      date_str: str = DATA_STR) -> np.ndarray:
    """HS active return for IDKA vs DV-matched replication portfolio (current-date weights).

    Replication: 1-2 NTN-Bs solved via _compute_idka_bench_replication at date_str,
    weights applied to historical ANBIMA unit prices on each DATE_SYNTHETIC_POSITION.
    Returns bps of NAV; empty array if replication or price data unavailable.
    """
    rep = _compute_idka_bench_replication(date_str, target_anos, tenour_du)
    if rep.empty:
        return np.array([])

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

    inst_list = ", ".join(f"'{i}'" for i in rep["INSTRUMENT"].tolist())
    q_prices = f"""
    SELECT p."REFERENCE_DATE" AS dt, m."INSTRUMENT", p."UNIT_PRICE"
    FROM "public"."PRICES_ANBIMA_BR_PUBLIC_BONDS" p
    JOIN "public"."MAPS_ANBIMA_BR_PUBLIC_BONDS" m
      ON m."BR_PUBLIC_BONDS_KEY" = p."BR_PUBLIC_BONDS_KEY"
    WHERE m."INSTRUMENT" IN ({inst_list})
      AND p."REFERENCE_DATE" >= DATE '{dt_min}'
      AND p."REFERENCE_DATE" <= DATE '{date_str}'
    ORDER BY p."REFERENCE_DATE", m."INSTRUMENT"
    """
    try:
        df_p = read_sql(q_prices)
    except Exception:
        return np.array([])
    if df_p.empty:
        return np.array([])

    df_p["dt"] = pd.to_datetime(df_p["dt"])
    rep_indexed = rep.set_index("INSTRUMENT")
    rep_ret = pd.Series(dtype=float)
    for inst in rep_indexed.index:
        w = float(rep_indexed.at[inst, "W"])
        mat = pd.to_datetime(rep_indexed.at[inst, "EXPIRATION_DATE"])
        prices = (df_p[df_p["INSTRUMENT"] == inst]
                  .set_index("dt")["UNIT_PRICE"].astype(float).sort_index())
        # Total-return: adjusts coupon-date clean-price drops back into the return
        ret = _ntnb_total_return_pct_change(prices, maturity=mat).dropna() * 10_000 * w
        rep_ret = rep_ret.add(ret, fill_value=0.0)

    hs_s = df_hs.set_index("dt")["W"].astype(float).sort_index()
    aligned = hs_s.to_frame("W").join(rep_ret.rename("rep"), how="inner")
    if len(aligned) < 30:
        return np.array([])
    return (aligned["W"] - aligned["rep"]).to_numpy()


def fetch_idka_hs_spread_series(portfolio_name: str, idx_name: str,
                                 target_anos: int, tenour_du: int,
                                 date_str: str = DATA_STR) -> np.ndarray:
    """Spread = benchmark_return − replication_return (bps) over the HS window dates.

    Positive = IDKA index outperformed the NTN-B DV-match replication.
    Shows the tracking error of the replication vs the actual index — independent
    of the fund's own positions (W cancels out).
    Date grid = DATE_SYNTHETIC_POSITION for the given portfolio/date.
    """
    rep = _compute_idka_bench_replication(date_str, target_anos, tenour_du)
    if rep.empty:
        return np.array([])

    q_dates = f"""
    SELECT MIN("DATE_SYNTHETIC_POSITION") AS dt_min
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}' AND "PORTIFOLIO" = '{portfolio_name}'
    """
    try:
        df_dates = read_sql(q_dates)
    except Exception:
        return np.array([])
    if df_dates.empty or pd.isna(df_dates["dt_min"].iloc[0]):
        return np.array([])
    dt_min = pd.to_datetime(df_dates["dt_min"].iloc[0]).strftime("%Y-%m-%d")

    q_bench = f"""
    SELECT "DATE" AS dt, "VALUE" FROM public."ECO_INDEX"
    WHERE "INSTRUMENT" = '{idx_name}' AND "FIELD" = 'INDEX'
      AND "DATE" >= DATE '{dt_min}' AND "DATE" <= DATE '{date_str}'
    ORDER BY dt
    """
    inst_list = ", ".join(f"'{i}'" for i in rep["INSTRUMENT"].tolist())
    q_prices = f"""
    SELECT p."REFERENCE_DATE" AS dt, m."INSTRUMENT", p."UNIT_PRICE"
    FROM "public"."PRICES_ANBIMA_BR_PUBLIC_BONDS" p
    JOIN "public"."MAPS_ANBIMA_BR_PUBLIC_BONDS" m
      ON m."BR_PUBLIC_BONDS_KEY" = p."BR_PUBLIC_BONDS_KEY"
    WHERE m."INSTRUMENT" IN ({inst_list})
      AND p."REFERENCE_DATE" >= DATE '{dt_min}' AND p."REFERENCE_DATE" <= DATE '{date_str}'
    ORDER BY p."REFERENCE_DATE", m."INSTRUMENT"
    """
    try:
        df_b = read_sql(q_bench)
        df_p = read_sql(q_prices)
    except Exception:
        return np.array([])
    if df_b.empty or df_p.empty:
        return np.array([])

    df_b["dt"] = pd.to_datetime(df_b["dt"])
    bench_bps = (df_b.set_index("dt")["VALUE"].astype(float)
                 .sort_index().pct_change().dropna() * 10_000)

    df_p["dt"] = pd.to_datetime(df_p["dt"])
    rep_indexed = rep.set_index("INSTRUMENT")
    rep_bps = pd.Series(dtype=float)
    for inst in rep_indexed.index:
        w = float(rep_indexed.at[inst, "W"])
        mat = pd.to_datetime(rep_indexed.at[inst, "EXPIRATION_DATE"])
        prices = (df_p[df_p["INSTRUMENT"] == inst]
                  .set_index("dt")["UNIT_PRICE"].astype(float).sort_index())
        # Total-return: adjusts coupon-date clean-price drops back into the return
        rep_bps = rep_bps.add(
            _ntnb_total_return_pct_change(prices, maturity=mat).dropna() * 10_000 * w,
            fill_value=0.0,
        )

    aligned = bench_bps.to_frame("bench").join(rep_bps.rename("rep"), how="inner")
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
    WHERE "FUNDO" IN ('MACRO','QUANT','EVOLUTION','GLOBAL','ALBATROZ','BALTRA','GFA','IDKAIPCAY3','IDKAIPCAY10')
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
        books: list[dict] = []
        for book, bdf in fdf.groupby("BOOK", sort=False):
            classes: list[dict] = []
            for cls, cdf in bdf.groupby("PRODUCT_CLASS", sort=False):
                positions = sorted(
                    [
                        {
                            "desc":     str(r.DESCRIPTION or ""),
                            "pl":       round(float(r.PL or 0),          2),
                            "pl_pct":   round(float(r.PL_PCT or 0),      8),
                            "pos_pl":   round(float(r.POSITION_PL or 0), 2),
                            "trade_pl": round(float(r.TRADES_PL or 0),   2),
                            "position": round(float(r.POSITION or 0),    2),
                            "amount":   round(float(r.AMOUNT or 0),      2),
                        }
                        for r in cdf.itertuples(index=False)
                    ],
                    key=lambda x: abs(x["pl"]), reverse=True,
                )
                classes.append({
                    "class":     cls,
                    "pl":        round(float(cdf["PL"].sum()),     2),
                    "pl_pct":    round(float(cdf["PL_PCT"].sum()), 8),
                    "positions": positions,
                })
            classes.sort(key=lambda x: abs(x["pl"]), reverse=True)
            books.append({
                "book":    book,
                "pl":      round(float(bdf["PL"].sum()),     2),
                "pl_pct":  round(float(bdf["PL_PCT"].sum()), 8),
                "classes": classes,
            })
        books.sort(key=lambda x: abs(x["pl"]), reverse=True)
        funds[short] = {
            "desk":          desk,
            "total_pl":      round(float(fdf["PL"].sum()),       2),
            "total_pl_pct":  round(float(fdf["PL_PCT"].sum()),   8),
            "total_trade_pl":round(float(fdf["TRADES_PL"].sum()), 2),
            "books":         books,
        }

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        "val_date": val_date_str,
        "funds": funds,
    }


_EXCEL_MARKET_PATH = r"F:\Macros\Gestao\Miguel Ishimura\Prices_Close_Global - Copy.xlsm"


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


def fetch_peers_data() -> dict:
    """Latest peers snapshot from the shared JSON file.

    Returns {val_date, benchmarks, groups}. Handles both the legacy
    wrapped format ({latest: {...}}) and the current flat format.
    Empty dict if file is not accessible.
    """
    data = json.loads(_PEERS_FILE.read_text(encoding="utf-8"))
    if "val_date" in data:
        return data
    return data.get("latest", {})

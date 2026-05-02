"""
credit_data.py
==============
DB fetchers for the credit sub-project. Standalone — does not depend on the
risk-monitor modules. All queries use glpg_fetch.read_sql.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

from glpg_fetch import read_sql
from .credit_config import (
    CREDIT_FUNDS,
    INDEX_MAP,
    CREDIT_CURVE_CHAINS,
    EXCLUDE_PRODUCT_CLASSES,
)


# ────────────────────────────────────────────────────────────────────────────
# NAV / SHARE
# ────────────────────────────────────────────────────────────────────────────

def fetch_nav_history(trading_desk: str, start: Optional[str] = None) -> pd.DataFrame:
    """Returns DataFrame indexed by VAL_DATE with columns NAV, SHARE."""
    where_start = f"AND \"VAL_DATE\" >= '{start}'" if start else ""
    sql = f"""
SELECT "VAL_DATE"::DATE AS dt, "NAV", "SHARE"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" = %s
  {where_start}
ORDER BY "VAL_DATE"
"""
    df = read_sql(sql.replace("%s", f"'{trading_desk}'"))
    df["dt"] = pd.to_datetime(df["dt"])
    return df.set_index("dt")


def fetch_nav_at(trading_desk: str, dt: str) -> Optional[float]:
    df = read_sql(f"""
SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" = '{trading_desk}' AND "VAL_DATE" = '{dt}'
""")
    return float(df.iloc[0, 0]) if len(df) else None


# ────────────────────────────────────────────────────────────────────────────
# Positions (snapshot) joined with asset master
# ────────────────────────────────────────────────────────────────────────────

def fetch_positions_snapshot(trading_desk: str, dt: str,
                             include_cash_provisions: bool = False) -> pd.DataFrame:
    """
    Returns Sea Lion (or any credit fund) positions on `dt`.
    Joins LOTE_BOOK_OVERVIEW with credit.asset_master for rating/sector/grupo/spread.
    """
    excl = ""
    if not include_cash_provisions:
        ex = ", ".join(f"'{p}'" for p in EXCLUDE_PRODUCT_CLASSES)
        excl = f"AND b.\"PRODUCT_CLASS\" NOT IN ({ex})"

    sql = f"""
SELECT
  b."BOOK"           AS book,
  b."PRODUCT_CLASS"  AS product_class,
  b."PRODUCT"        AS produto,
  b."POSITION"       AS pos_brl,
  b."PL_POSITION"    AS pl_position,
  b."PL"             AS pl,
  am.tipo_ativo,
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
FROM "LOTE45"."LOTE_BOOK_OVERVIEW" b
LEFT JOIN credit.asset_master am
  ON LOWER(am.nome_lote45) = LOWER(b."PRODUCT")
WHERE b."TRADING_DESK" = '{trading_desk}'
  AND b."VAL_DATE" = '{dt}'
  {excl}
ORDER BY b."POSITION" DESC
"""
    return normalize_issuer_overrides(read_sql(sql))


# ────────────────────────────────────────────────────────────────────────────
# Issuer-name overrides (registration errors in credit.asset_master)
# ────────────────────────────────────────────────────────────────────────────

def normalize_issuer_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Apply known issuer overrides driven by the product name.

    Why: some rows in ``credit.asset_master`` have inconsistent or wrong
    apelido_emissor / grupo_economico / nome_emissor for FIDCs that
    legitimately belong to the same issuer (e.g. Santa Cruz tranches).
    Rather than touch the master file, we normalize on read so concentration
    and limit-utilization rollups behave correctly.

    Currently:
        - Any product with "Cruz" in the name → issuer = "Santa Cruz"
    """
    if df is None or df.empty or "produto" not in df.columns:
        return df
    has_cruz = df["produto"].astype(str).str.contains("Cruz", case=False, na=False)
    if has_cruz.any():
        for col in ("apelido_emissor", "grupo_economico", "nome_emissor"):
            if col in df.columns:
                df.loc[has_cruz, col] = "Santa Cruz"
    return df


# ────────────────────────────────────────────────────────────────────────────
# Issuer limits (per fund)
# ────────────────────────────────────────────────────────────────────────────

def fetch_price_quality_flags(trading_desk: str, dt: str, dt_prev: str) -> pd.DataFrame:
    """Sanity check: for the report date and day-before, every security/derivative
    held by `trading_desk` must have a non-null PRICE in LOTE_BOOK_OVERVIEW.

    3rd-party fund cotas, cash, broker margin balances and fee/provision rows
    are exempt — they don't receive a market mark via this table. Exempt
    PRODUCT_CLASSes:
        Funds BR · Cash · Margin · Provisions and Costs

    Returns a DataFrame with one row per flagged asset (only flagged rows;
    empty DataFrame means all clear). Columns: produto, product_class,
    pos_brl, price_today, price_prev, missing_today, missing_prev.
    """
    # GROUP BY PRODUCT in both CTEs prevents an N×M join explosion when the same
    # PRODUCT exists across multiple BOOKs (e.g. NTN-B 2030 in Caixa + RF Curto).
    # Without this, today=2 rows × prev=2 rows = 4 combined rows, all flagged if
    # any price is null — inflating the warning count in the emailed PDF.
    sql = f"""
WITH today AS (
  SELECT "PRODUCT" AS produto,
         MAX("PRODUCT_CLASS") AS product_class,
         SUM("POSITION") AS pos_brl,
         MAX(NULLIF("PRICE", 0)) AS price_today
  FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
  WHERE "TRADING_DESK" = '{trading_desk}' AND "VAL_DATE" = '{dt}'
  GROUP BY "PRODUCT"
),
prev AS (
  SELECT "PRODUCT" AS produto,
         MAX(NULLIF("PRICE", 0)) AS price_prev
  FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
  WHERE "TRADING_DESK" = '{trading_desk}' AND "VAL_DATE" = '{dt_prev}'
  GROUP BY "PRODUCT"
)
SELECT t.produto, t.product_class, t.pos_brl, t.price_today, p.price_prev
FROM today t
LEFT JOIN prev p ON t.produto = p.produto
WHERE t.product_class NOT IN ('Funds BR','Cash','Margin','Provisions and Costs')
"""
    df = read_sql(sql)
    if df.empty:
        return df
    # Post-NULLIF, both 0 and NULL prices are NaN — `== 0` would never fire.
    # A legitimately defaulted bond (price = ε > 0) is NOT treated as missing.
    df["missing_today"] = df["price_today"].isna()
    df["missing_prev"]  = df["price_prev"].isna()
    df["flagged"] = df["missing_today"] | df["missing_prev"]
    return df[df["flagged"]].drop(columns=["flagged"]).reset_index(drop=True)


def fetch_issuer_limits(fund_name: str) -> pd.DataFrame:
    sql = f"""
SELECT emissor, limit_pct, limit_text, analista, last_approval, renewal_date, obs
FROM credit.issuer_limits
WHERE fund_name = '{fund_name}'
ORDER BY limit_pct DESC NULLS LAST
"""
    return read_sql(sql)


# ────────────────────────────────────────────────────────────────────────────
# Indices (CDI, IMA-B, IDA-Infra, IDA-Ex-Infra)
# ────────────────────────────────────────────────────────────────────────────

def fetch_index_series(label: str, start: Optional[str] = None) -> pd.Series:
    """Returns daily series indexed by date. CDI is YIELD (1d) → cumulative, others are INDEX."""
    if label not in INDEX_MAP:
        raise ValueError(f"unknown index label: {label}")
    instrument, field = INDEX_MAP[label]
    where_start = f"AND \"DATE\" >= '{start}'" if start else ""
    sql = f"""
SELECT "DATE"::DATE AS dt, "VALUE"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" = '{instrument}' AND "FIELD" = '{field}'
  {where_start}
ORDER BY "DATE"
"""
    df = read_sql(sql)
    df["dt"] = pd.to_datetime(df["dt"])
    s = df.set_index("dt")["VALUE"].astype(float)
    s.name = label
    if label == "CDI":
        # CDI in ECO_INDEX is daily yield (1d return). Build cumulative index from 1.0.
        return (1.0 + s).cumprod()
    # Other indices already cumulative levels
    return s


def fetch_index_panel(labels: list[str], start: Optional[str] = None) -> pd.DataFrame:
    out = pd.concat([fetch_index_series(lbl, start) for lbl in labels], axis=1).ffill()
    return out


def fetch_cdi_1d_returns(start: Optional[str] = None) -> pd.Series:
    """CDI 1d returns (ECO_INDEX FIELD='YIELD' is the daily rate). Returns decimal."""
    where_start = f"AND \"DATE\" >= '{start}'" if start else ""
    sql = f"""
SELECT "DATE"::DATE AS dt, "VALUE"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" = 'CDI' AND "FIELD" = 'YIELD'
  {where_start}
ORDER BY "DATE"
"""
    df = read_sql(sql)
    df["dt"] = pd.to_datetime(df["dt"])
    s = df.set_index("dt")["VALUE"].astype(float)
    s.name = "cdi_1d"
    return s


def fetch_cdi_annual_rate(dt: str) -> Optional[float]:
    """CDI annualized rate (252) on the latest available date <= dt."""
    df = read_sql(f"""
SELECT "VALUE" FROM "ECO_INDEX"
WHERE "INSTRUMENT"='CDI' AND "FIELD"='YIELD' AND "DATE" <= '{dt}'
ORDER BY "DATE" DESC LIMIT 1
""")
    if not len(df):
        return None
    r1d = float(df.iloc[0, 0])
    return (1.0 + r1d) ** 252 - 1.0


def fetch_ipca_12m(dt: str) -> Optional[float]:
    """Current IPCA YoY (12m). Computed from IPCA_INDICE: (idx_today / idx_12m_ago) - 1."""
    try:
        df = read_sql(f"""
SELECT "DATE"::DATE AS dt, "VALUE"
FROM "ECO_INDEX"
WHERE "INSTRUMENT"='IPCA_INDICE'
  AND "DATE" >= ('{dt}'::DATE - INTERVAL '14 months')
  AND "DATE" <= '{dt}'
ORDER BY "DATE"
""")
    except Exception:
        return None
    if len(df) < 2:
        return None
    df["dt"] = pd.to_datetime(df["dt"])
    s = df.set_index("dt")["VALUE"].astype(float).sort_index()
    last = s.iloc[-1]
    target = pd.Timestamp(dt) - pd.DateOffset(months=12)
    prior_slice = s[s.index <= target]
    if not len(prior_slice):
        return None
    prior = prior_slice.iloc[-1]
    if prior <= 0:
        return None
    return (last / prior) - 1.0


# ────────────────────────────────────────────────────────────────────────────
# Credit curves (ANBIMA AAA / AA / A)
# ────────────────────────────────────────────────────────────────────────────

def fetch_credit_curve_today(rating_bucket: str, dt: str) -> pd.DataFrame:
    """Returns DataFrame with columns (tenor_bdays, rate) for the curve on `dt`."""
    chain = CREDIT_CURVE_CHAINS[rating_bucket]
    sql = f"""
SELECT y."DATE"::DATE AS dt, y."TENOUR" AS tenor_bdays, y."YIELD" AS rate
FROM "YIELDS_GLOBAL_CURVES" y
JOIN "MAPS_GLOBAL_CURVES" m ON y."GLOBAL_CURVES_KEY" = m."GLOBAL_CURVES_KEY"
WHERE m."CHAIN" = '{chain}' AND m."SOURCE" = 'ANBIMA'
  AND y."DATE" = '{dt}'
ORDER BY y."TENOUR"
"""
    return read_sql(sql)


def fetch_credit_curves(dt: str, dt_prev: Optional[str] = None) -> dict[str, pd.DataFrame]:
    """Today + optional D-1 curves for all 3 rating buckets."""
    out = {}
    for r in CREDIT_CURVE_CHAINS:
        cur = fetch_credit_curve_today(r, dt)
        if dt_prev:
            prev = fetch_credit_curve_today(r, dt_prev)
            cur = cur.merge(prev[["tenor_bdays", "rate"]].rename(columns={"rate": "rate_prev"}),
                            on="tenor_bdays", how="left")
        out[r] = cur
    return out

"""Export IDKA replication vs benchmark distributions to Excel.

Standalone investigation tool: dumps the daily series used by the
"Comparação" view in the Distribuição card (Repl − Bench spread) into
an Excel file with one sheet per IDKA, plus per-instrument contribution
columns and summary stats — useful to inspect outliers like the -186 bps
day in the spread distribution.

Each row = one historical date in the HS window.

Outputs:
  IDKA_3Y / IDKA_10Y sheets:
    DATE | BENCH_INDEX | BENCH_RET_BPS | <inst1>_PRICE | <inst1>_RET_W_BPS | ...
    | REP_RET_BPS | SPREAD_BPS (= REP - BENCH)
  Replication weights sheet:
    Replication weights at the report date for each IDKA.
  Summary sheet:
    min/p05/median/mean/p95/max/std for BENCH, REP, SPREAD per IDKA.

Run:
  python export_idka_repl_vs_bench.py
  python export_idka_repl_vs_bench.py --date 2026-04-24
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from glpg_fetch import read_sql  # type: ignore[import-not-found]
from data_fetch import _compute_idka_bench_replication, _ntnb_total_return_pct_change
from risk_runtime import DATA_STR, OUT_DIR


IDKAS = [
    # (label, portfolio_name, idx_name, target_anos, tenour_du)
    ("IDKA_3Y",  "IDKA3Y",  "IDKA_IPCA_3A",  3,   756),
    ("IDKA_10Y", "IDKA10Y", "IDKA_IPCA_10A", 10, 2520),
]


def _build_idka_frame(label: str, portfolio_name: str, idx_name: str,
                      target_anos: int, tenour_du: int,
                      date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (daily_df, weights_df). daily_df has dates × all components."""
    rep = _compute_idka_bench_replication(date_str, target_anos, tenour_du)
    if rep.empty:
        print(f"  [{label}] no replication weights — skipping")
        return pd.DataFrame(), pd.DataFrame()

    weights_df = rep.copy()
    weights_df.insert(0, "IDKA", label)

    # Date range comes from the HS portfolio window
    q_dates = f"""
    SELECT MIN("DATE_SYNTHETIC_POSITION") AS dt_min
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" = DATE '{date_str}' AND "PORTIFOLIO" = '{portfolio_name}'
    """
    df_dates = read_sql(q_dates)
    if df_dates.empty or pd.isna(df_dates["dt_min"].iloc[0]):
        print(f"  [{label}] no HS dates found")
        return pd.DataFrame(), weights_df
    dt_min = pd.to_datetime(df_dates["dt_min"].iloc[0]).strftime("%Y-%m-%d")

    # Benchmark index level + return
    bench = read_sql(f"""
        SELECT "DATE" AS dt, "VALUE"
        FROM public."ECO_INDEX"
        WHERE "INSTRUMENT" = '{idx_name}' AND "FIELD" = 'INDEX'
          AND "DATE" >= DATE '{dt_min}' AND "DATE" <= DATE '{date_str}'
        ORDER BY dt
    """)
    if bench.empty:
        print(f"  [{label}] no benchmark series")
        return pd.DataFrame(), weights_df
    bench["dt"] = pd.to_datetime(bench["dt"])
    bench = bench.set_index("dt").rename(columns={"VALUE": "BENCH_INDEX"}).astype(float)
    bench["BENCH_RET_BPS"] = bench["BENCH_INDEX"].pct_change() * 10_000

    # Replication: per-instrument prices + weighted contributions
    inst_list = ", ".join(f"'{i}'" for i in rep["INSTRUMENT"].tolist())
    prices = read_sql(f"""
        SELECT p."REFERENCE_DATE" AS dt, m."INSTRUMENT", p."UNIT_PRICE"
        FROM "public"."PRICES_ANBIMA_BR_PUBLIC_BONDS" p
        JOIN "public"."MAPS_ANBIMA_BR_PUBLIC_BONDS" m
          ON m."BR_PUBLIC_BONDS_KEY" = p."BR_PUBLIC_BONDS_KEY"
        WHERE m."INSTRUMENT" IN ({inst_list})
          AND p."REFERENCE_DATE" >= DATE '{dt_min}' AND p."REFERENCE_DATE" <= DATE '{date_str}'
        ORDER BY p."REFERENCE_DATE", m."INSTRUMENT"
    """)
    prices["dt"] = pd.to_datetime(prices["dt"])

    # Pivot prices wide; one column per instrument
    px_wide = prices.pivot(index="dt", columns="INSTRUMENT", values="UNIT_PRICE").sort_index()
    px_wide = px_wide.astype(float)

    rep_indexed = rep.set_index("INSTRUMENT")
    rep_total_bps = pd.Series(0.0, index=px_wide.index)
    rep_total_clean_bps = pd.Series(0.0, index=px_wide.index)
    out_cols: dict = {}
    for inst in rep_indexed.index:
        if inst not in px_wide.columns:
            continue
        w   = float(rep_indexed.at[inst, "W"])
        mat = pd.to_datetime(rep_indexed.at[inst, "EXPIRATION_DATE"])
        ret_clean_bps = px_wide[inst].pct_change() * 10_000
        ret_tr_bps    = _ntnb_total_return_pct_change(px_wide[inst], maturity=mat) * 10_000
        contrib_clean = ret_clean_bps * w
        contrib_tr    = ret_tr_bps * w
        out_cols[f"{inst}_PRICE"] = px_wide[inst]
        out_cols[f"{inst}_W"] = w
        out_cols[f"{inst}_MAT"] = mat
        out_cols[f"{inst}_RET_CLEAN_BPS"] = ret_clean_bps
        out_cols[f"{inst}_RET_TR_BPS"]    = ret_tr_bps
        out_cols[f"{inst}_CONTRIB_BPS"]   = contrib_tr
        rep_total_clean_bps = rep_total_clean_bps.add(contrib_clean, fill_value=0.0)
        rep_total_bps       = rep_total_bps.add(contrib_tr, fill_value=0.0)

    daily = bench.join(pd.DataFrame(out_cols), how="outer").sort_index()
    daily["REP_RET_CLEAN_BPS"] = rep_total_clean_bps  # legacy clean-price (with coupon artifact)
    daily["REP_RET_BPS"]       = rep_total_bps        # total-return adjusted
    daily["SPREAD_CLEAN_BPS"]  = daily["REP_RET_CLEAN_BPS"] - daily["BENCH_RET_BPS"]
    daily["SPREAD_BPS"]        = daily["REP_RET_BPS"]       - daily["BENCH_RET_BPS"]
    daily.index.name = "DATE"
    daily = daily.reset_index()
    return daily, weights_df


def _summary_row(label: str, series_name: str, s: pd.Series) -> dict:
    s = s.dropna()
    if s.empty:
        return {"IDKA": label, "SERIES": series_name, "N": 0}
    return {
        "IDKA":   label,
        "SERIES": series_name,
        "N":      int(len(s)),
        "MIN":    float(s.min()),
        "P05":    float(np.percentile(s, 5)),
        "MEDIAN": float(np.percentile(s, 50)),
        "MEAN":   float(s.mean()),
        "P95":    float(np.percentile(s, 95)),
        "MAX":    float(s.max()),
        "STD":    float(s.std(ddof=1)),
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=DATA_STR, help="Reference date YYYY-MM-DD (default: latest)")
    p.add_argument("--out", default=None, help="Output xlsx path")
    args = p.parse_args()

    date_str = args.date
    out_path = Path(args.out) if args.out else OUT_DIR / f"{date_str}_idka_repl_vs_bench.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    daily_frames: dict[str, pd.DataFrame] = {}
    weight_frames: list[pd.DataFrame] = []
    summary_rows: list[dict] = []

    print(f"Building IDKA replication-vs-bench Excel for {date_str}...")
    for label, portfolio_name, idx_name, target_anos, tenour_du in IDKAS:
        print(f"  · {label}")
        daily, weights = _build_idka_frame(label, portfolio_name, idx_name,
                                            target_anos, tenour_du, date_str)
        if not daily.empty:
            daily_frames[label] = daily
            for s_name in ("BENCH_RET_BPS", "REP_RET_BPS", "SPREAD_BPS"):
                summary_rows.append(_summary_row(label, s_name, daily[s_name]))
        if not weights.empty:
            weight_frames.append(weights)

    if not daily_frames:
        print("No data — nothing written.")
        return 1

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        # Summary first for quick eyeball
        if summary_rows:
            pd.DataFrame(summary_rows).to_excel(xw, sheet_name="Summary", index=False)
        if weight_frames:
            pd.concat(weight_frames, ignore_index=True).to_excel(
                xw, sheet_name="Weights", index=False
            )
        for label, df in daily_frames.items():
            df.to_excel(xw, sheet_name=label, index=False)

    print(f"Saved: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

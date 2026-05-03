"""Pass 2: probe known columns directly (after schema discovery in pass 1).

LOTE_FUND_STRESS_RPM: PARAMETRIC_VAR, HISTORICAL_VAR, MACRO_STRESS, SPECIFIC_STRESS
LOTE_FUND_STRESS:     PVAR1DAY, PVAR5DAYS, HVAR1DAY, HVAR5DAYS, MACRO_STRESS, SPECIFIC_STRESS

Compare SUM at canonical LEVEL/TREE against GUI ground truth.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from glpg_fetch import read_sql

DATE = "2026-04-28"

# (kit_code, trading_desk, level, source_table)
TARGETS = [
    ("QUANT",     "Galapagos Quantitativo FIM",     2,  "RPM"),
    ("MACRO",     "Galapagos Macro FIM",            2,  "RPM"),
    ("EVOLUTION", "Galapagos Evolution FIC FIM CP", 2,  "RPM"),
    ("MACRO_Q",   "Galapagos Global Macro Q",       None, "LFS"),  # LFS uses TREE='Main', not LEVEL
]

# Ground truth (signed % NAV)
GT = {
    "QUANT":     {"P.VaR1": -0.42, "P.VaR5": -0.96, "H.VaR1": -0.50, "H.VaR5": -1.01, "Stress": -3.97},
    "MACRO_Q":   {"P.VaR1": -0.18, "P.VaR5": -0.46, "H.VaR1": -0.22, "H.VaR5": -0.44, "Stress": -2.75},
    "MACRO":     {"AbsVaR_PrimTab": -0.76},
    "EVOLUTION": {"AbsVaR_PrimTab": -0.62},
}

# NAVs
fund_list = ", ".join(f"'{td}'" for _, td, _, _ in TARGETS)
nav = read_sql(f"""
    SELECT "TRADING_DESK", "NAV"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({fund_list})
""")
nav_map = dict(zip(nav["TRADING_DESK"], nav["NAV"].astype(float)))

print(f"\n=== DATE {DATE} ===\n")
print("NAVs:")
for code, td, _, _ in TARGETS:
    print(f"  {code:10s}  {td[:35]:35s}  {nav_map.get(td, 0):>16,.2f}")
print()

def fmt_pct(v_brl: float, n: float) -> str:
    return f"{-v_brl/n*100:+7.3f}%"

# ── RPM funds ──────────────────────────────────────────────────────────────
print("=" * 90)
print("LOTE_FUND_STRESS_RPM — sum at canonical LEVEL")
print("=" * 90)
print(f"\n{'fund':10s}  {'PARAMETRIC_VAR':>15s} {'%':>9s}  "
      f"{'HISTORICAL_VAR':>15s} {'%':>9s}  "
      f"{'MACRO_STRESS':>15s} {'%':>9s}  "
      f"{'SPECIFIC_STRESS':>15s} {'%':>9s}")
print("-" * 90)
for code, td, lvl, src in TARGETS:
    if src != "RPM":
        continue
    n = nav_map.get(td, 0)
    df = read_sql(f"""
        SELECT SUM("PARAMETRIC_VAR")  AS pvar,
               SUM("HISTORICAL_VAR")  AS hvar,
               SUM("MACRO_STRESS")    AS mstress,
               SUM("SPECIFIC_STRESS") AS sstress
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
          AND "LEVEL" = {lvl}
    """)
    if df.empty:
        print(f"  {code}: (no rows)")
        continue
    pvar = float(df["pvar"].iloc[0] or 0)
    hvar = float(df["hvar"].iloc[0] or 0)
    ms   = float(df["mstress"].iloc[0] or 0)
    ss   = float(df["sstress"].iloc[0] or 0)
    print(f"{code:10s}  {pvar:>15,.0f} {fmt_pct(pvar, n):>9s}  "
          f"{hvar:>15,.0f} {fmt_pct(hvar, n):>9s}  "
          f"{ms:>15,.0f} {fmt_pct(ms, n):>9s}  "
          f"{ss:>15,.0f} {fmt_pct(ss, n):>9s}")

# ── LFS funds (MACRO_Q) ─────────────────────────────────────────────────────
print()
print("=" * 90)
print("LOTE_FUND_STRESS — sum with TREE='Main'")
print("=" * 90)
print(f"\n{'fund':10s}  {'PVAR1DAY':>12s} {'%':>9s}  "
      f"{'PVAR5DAYS':>12s} {'%':>9s}  "
      f"{'HVAR1DAY':>12s} {'%':>9s}  "
      f"{'HVAR5DAYS':>12s} {'%':>9s}  "
      f"{'MACRO_STRESS':>14s} {'%':>9s}  "
      f"{'SPECIFIC_STRESS':>14s} {'%':>9s}")
print("-" * 130)
for code, td, _, src in TARGETS:
    if src != "LFS":
        continue
    n = nav_map.get(td, 0)
    df = read_sql(f"""
        SELECT SUM("PVAR1DAY")        AS pv1,
               SUM("PVAR5DAYS")       AS pv5,
               SUM("HVAR1DAY")        AS hv1,
               SUM("HVAR5DAYS")       AS hv5,
               SUM("MACRO_STRESS")    AS mstress,
               SUM("SPECIFIC_STRESS") AS sstress
        FROM "LOTE45"."LOTE_FUND_STRESS"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
          AND "TREE" = 'Main'
    """)
    if df.empty:
        print(f"  {code}: (no rows)")
        continue
    pv1 = float(df["pv1"].iloc[0] or 0)
    pv5 = float(df["pv5"].iloc[0] or 0)
    hv1 = float(df["hv1"].iloc[0] or 0)
    hv5 = float(df["hv5"].iloc[0] or 0)
    ms  = float(df["mstress"].iloc[0] or 0)
    ss  = float(df["sstress"].iloc[0] or 0)
    print(f"{code:10s}  {pv1:>12,.0f} {fmt_pct(pv1, n):>9s}  "
          f"{pv5:>12,.0f} {fmt_pct(pv5, n):>9s}  "
          f"{hv1:>12,.0f} {fmt_pct(hv1, n):>9s}  "
          f"{hv5:>12,.0f} {fmt_pct(hv5, n):>9s}  "
          f"{ms:>14,.0f} {fmt_pct(ms, n):>9s}  "
          f"{ss:>14,.0f} {fmt_pct(ss, n):>9s}")

# ── Side-by-side vs GUI ─────────────────────────────────────────────────────
print()
print("=" * 90)
print("GUI ground truth (for reference)")
print("=" * 90)
for code, gt in GT.items():
    print(f"\n  {code}:")
    for label, pct in gt.items():
        print(f"    {label:30s}  {pct:+7.2f}%")

print()
print("=" * 90)
print("INTERPRETATION GUIDE")
print("=" * 90)
print("""
Per the MACRO_Q primitive-tab dump (verified by hand):
  Sum of per-row 'Relative VaR' = -41,712 = headline P.VaR1 (-0.18% NAV)
  Sum of per-row 'Absolute VaR' = -104,356 (= -0.45%, no diversification)

If kit's column SUM == GUI P.VaR1, the column is the 'Relative VaR' equivalent
and the fetch is correct.

If kit's column SUM > GUI P.VaR1 by 2-3x, the column is the 'Absolute VaR'
(non-diversified magnitude) and the fetch overstates risk.

Compare the 'PVAR1DAY %' line for MACRO_Q to GUI P.VaR1 = -0.18%.
Compare the 'PARAMETRIC_VAR %' lines for QUANT/MACRO/EVO to GUI:
  QUANT P.VaR1 = -0.42% ; H.VaR1 = -0.50%
  MACRO  primitive Total Abs VaR Pct = -0.76%
  EVO    primitive Total Abs VaR Pct = -0.62%

Note: MACRO/EVO ground truth is the GUI 'Absolute VaR Pct' total which is
the non-diversified magnitude. So if the kit's PARAMETRIC_VAR sum matches
that non-diversified number, the kit is reading the wrong (overstated) column.
""")

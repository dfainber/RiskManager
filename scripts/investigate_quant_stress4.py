"""Round 4 — validate that LOTE_BOOK_STRESS at the right LEVEL replicates GUI."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import numpy as np
from glpg_fetch import read_sql

# Ground truth from APM GUI screenshots (28-abr 2026)
GUI = {
    "Galapagos Quantitativo FIM":     {"scenario": -1_431_806, "specific":   -530_521, "total": -1_962_327},
    "Galapagos Global Macro Q":       {"scenario":    -40_443, "specific":   -599_138, "total":   -639_581},
    "Galapagos Macro FIM":            {"scenario":   -168_767, "specific": -5_061_578, "total": -5_230_345},
}
DATE = "2026-04-28"

# ── 1. Sum LOTE_BOOK_STRESS by (TRADING_DESK, LEVEL, TREE) ─────────────────
print("=== LOTE_BOOK_STRESS sums per (LEVEL, TREE) ===")
sums = read_sql(f"""
    SELECT "TRADING_DESK", "LEVEL", "TREE",
           COUNT(*) AS n,
           SUM("SPECIFIC_STRESS") AS spec,
           SUM("MACRO_STRESS") AS macro,
           SUM("PARAMETRIC_VAR") AS pvar
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({", ".join(f"'{n}'" for n in GUI.keys())})
    GROUP BY "TRADING_DESK", "LEVEL", "TREE"
    ORDER BY "TRADING_DESK", "LEVEL", "TREE"
""")
for c in ("spec","macro","pvar"):
    sums[c] = sums[c].astype(float)
print(sums.to_string(index=False, float_format="{:>14,.0f}".format))

# ── 2. Per-fund: at each level, compare (spec + macro) to GUI total ────────
print("\n\n=== Compare LOTE_BOOK_STRESS aggregates to GUI ground truth ===")
print(f"{'Fund':40s} {'LVL':>4s} {'TREE':>20s} {'BOOK_spec':>14s} {'BOOK_macro':>14s} {'BOOK_sum':>14s}  | "
      f"{'GUI_spec':>14s} {'GUI_scn':>14s} {'GUI_tot':>14s}  delta_spec  delta_total")
for td, gv in GUI.items():
    sub = sums[sums["TRADING_DESK"] == td]
    for _, r in sub.iterrows():
        bs = r["spec"]; bm = r["macro"]; bt = bs + bm
        ds = bs - gv["specific"]
        dt = bt - gv["total"]
        print(f"{td:40s} {int(r['LEVEL']):4d} {str(r['TREE'])[:20]:>20s} "
              f"{bs:14,.0f} {bm:14,.0f} {bt:14,.0f}  | "
              f"{gv['specific']:14,.0f} {gv['scenario']:14,.0f} {gv['total']:14,.0f}  "
              f"{ds:+10,.0f}  {dt:+10,.0f}")

# ── 3. Same scan on LOTE_BOOK_STRESS_RPM (the "RPM" variant) ───────────────
print("\n\n=== LOTE_BOOK_STRESS_RPM sums per (LEVEL, TREE) ===")
sums_rpm = read_sql(f"""
    SELECT "TRADING_DESK", "LEVEL", "TREE",
           COUNT(*) AS n,
           SUM("SPECIFIC_STRESS") AS spec,
           SUM("MACRO_STRESS") AS macro,
           SUM("PARAMETRIC_VAR") AS pvar
    FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({", ".join(f"'{n}'" for n in GUI.keys())})
    GROUP BY "TRADING_DESK", "LEVEL", "TREE"
    ORDER BY "TRADING_DESK", "LEVEL", "TREE"
""")
for c in ("spec","macro","pvar"):
    sums_rpm[c] = sums_rpm[c].astype(float)
print(sums_rpm.to_string(index=False, float_format="{:>14,.0f}".format))

# ── 4. Same on LOTE_FUND_STRESS_RPM (what the kit uses today) ──────────────
print("\n\n=== LOTE_FUND_STRESS_RPM sums per (LEVEL) — what the kit uses today ===")
fund_rpm = read_sql(f"""
    SELECT "TRADING_DESK", "LEVEL",
           COUNT(*) AS n,
           SUM("SPECIFIC_STRESS") AS spec,
           SUM("MACRO_STRESS") AS macro,
           SUM("PARAMETRIC_VAR") AS pvar
    FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({", ".join(f"'{n}'" for n in GUI.keys())})
    GROUP BY "TRADING_DESK", "LEVEL"
    ORDER BY "TRADING_DESK", "LEVEL"
""")
for c in ("spec","macro","pvar"):
    fund_rpm[c] = fund_rpm[c].astype(float)
print(fund_rpm.to_string(index=False, float_format="{:>14,.0f}".format))

# ── 5. LOTE_FUND_STRESS (non-RPM) ──────────────────────────────────────────
print("\n\n=== LOTE_FUND_STRESS (non-RPM) sums per (TREE) ===")
nonrpm = read_sql(f"""
    SELECT "TRADING_DESK", "TREE",
           COUNT(*) AS n,
           SUM("SPECIFIC_STRESS") AS spec,
           SUM("MACRO_STRESS") AS macro,
           SUM("PVAR1DAY") AS pvar1d
    FROM "LOTE45"."LOTE_FUND_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({", ".join(f"'{n}'" for n in GUI.keys())})
    GROUP BY "TRADING_DESK", "TREE"
    ORDER BY "TRADING_DESK", "TREE"
""")
for c in ("spec","macro","pvar1d"):
    nonrpm[c] = nonrpm[c].astype(float)
print(nonrpm.to_string(index=False, float_format="{:>14,.0f}".format))

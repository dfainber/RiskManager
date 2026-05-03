"""Round 5 — find which (LEVEL, TREE) of LOTE_BOOK_STRESS matches GUI/xlsx
across ALL relevant funds. Also pull all PMs for MACRO to find where the 5x
overcount on MACRO is coming from.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import numpy as np
from glpg_fetch import read_sql

DATE = "2026-04-28"

# Ground truth: 28-abr from GUI screenshots + xlsx
# (xlsx is the regulatory output; GUI is engine live; they drift by minutes)
GROUND_TRUTH = {
    "Galapagos Quantitativo FIM":     {"gui_scn": -1_431_806, "gui_spec":   -530_521, "gui_tot": -1_962_327, "xlsx": -3.92, "src":"GUI 28-abr 8:01pm + xlsx 28-abr"},
    "Galapagos Global Macro Q":       {"gui_scn":    -40_443, "gui_spec":   -599_138, "gui_tot":   -639_581, "xlsx": -2.94, "src":"GUI 28-abr 8:06pm"},
    "Galapagos Macro FIM":            {"gui_scn":   -168_767, "gui_spec": -5_061_578, "gui_tot": -5_230_345, "xlsx": -4.05, "src":"GUI 28-abr 8:07pm"},
    "IDKA IPCA 10Y FIRF":             {"gui_scn": -8_881_534, "gui_spec":   -711_249, "gui_tot": -9_592_783, "xlsx":-10.69, "src":"GUI 28-abr 8:09pm"},
    # Below: only xlsx (no GUI yet). Will validate against -tot from xlsx pct.
    "Galapagos Evolution FIC FIM CP": {"xlsx_pct": -4.16},  # "Stress Test < 15%"
    "GALAPAGOS ALBATROZ FIRF LP":     {"xlsx_pct": -0.91},
    "Galapagos Baltra Icatu Qualif Prev FIM CP": {"xlsx_pct": -6.58},
    "IDKA IPCA 3Y FIRF":              {"xlsx_pct": -3.92},
}

# ── Pull NAV ────────────────────────────────────────────────────────────────
funds_q = ", ".join(f"'{n}'" for n in GROUND_TRUTH.keys())
nav_df = read_sql(f"""
    SELECT "TRADING_DESK", "NAV"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({funds_q})
""")
nav_lookup = nav_df.set_index("TRADING_DESK")["NAV"].astype(float).to_dict()

# ── LOTE_BOOK_STRESS sums per (LEVEL, TREE) ─────────────────────────────────
sums = read_sql(f"""
    SELECT "TRADING_DESK", "LEVEL", "TREE",
           COUNT(*) AS n,
           SUM("SPECIFIC_STRESS") AS spec,
           SUM("MACRO_STRESS")    AS macro
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({funds_q})
    GROUP BY "TRADING_DESK", "LEVEL", "TREE"
    ORDER BY "TRADING_DESK", "LEVEL", "TREE"
""")
for c in ("spec","macro"):
    sums[c] = sums[c].astype(float)
print("=== LOTE_BOOK_STRESS sums per (TD, LEVEL, TREE) ===")
print(sums.to_string(index=False, float_format="{:>14,.0f}".format))

# ── Per-fund: rank LEVEL/TREE combinations by closeness to GUI/xlsx total ──
print("\n\n=== Best-matching (LEVEL, TREE) per fund ===")
print(f"{'Fund':40s} {'L':>3s} {'TREE':>20s} {'predicted':>14s} {'target':>14s}  {'err_BRL':>12s}  {'err_pct':>8s}")
for td, gt in GROUND_TRUTH.items():
    nav = nav_lookup.get(td)
    if nav is None:
        print(f"  {td}: NO NAV — skip")
        continue
    # Build target in BRL
    if "gui_tot" in gt:
        target_brl = gt["gui_tot"]
        target_label = "GUI"
    else:
        target_brl = gt["xlsx_pct"] / 100.0 * nav
        target_label = "xlsx"
    sub = sums[sums["TRADING_DESK"] == td].copy()
    sub["pred"] = sub["spec"] + sub["macro"]
    sub["err_brl"] = sub["pred"] - target_brl
    sub["err_pct"] = sub["err_brl"] / nav * 100
    sub = sub.sort_values(by="err_brl", key=lambda s: s.abs())
    print(f"\n  {td}  (target {target_label}={target_brl:,.0f}; NAV={nav:,.0f})")
    for _, r in sub.head(6).iterrows():
        print(f"    L={int(r['LEVEL']):2d} TREE={str(r['TREE'])[:16]:>16s}  "
              f"pred={r['pred']:14,.0f}  err={r['err_brl']:+12,.0f} ({r['err_pct']:+.2f}%)")

# ── Look at MACRO at all LEVELs split by BOOK to find duplicates ──────────
print("\n\n=== MACRO at LEVEL=1 vs LEVEL=10: per BOOK breakdown ===")
macro_by_book = read_sql(f"""
    SELECT "LEVEL", "BOOK",
           COUNT(*) AS n,
           SUM("SPECIFIC_STRESS") AS spec,
           SUM("MACRO_STRESS") AS macro
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" = 'Galapagos Macro FIM'
    GROUP BY "LEVEL", "BOOK"
    ORDER BY "LEVEL", "BOOK"
""")
for c in ("spec","macro"):
    macro_by_book[c] = macro_by_book[c].astype(float)
print(macro_by_book.to_string(index=False, float_format="{:>14,.0f}".format))

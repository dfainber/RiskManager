"""Pass 3: hunt for the query that replicates MACRO FIM headline P.VaR1 = -0.64%.

Kit currently: SUM(PARAMETRIC_VAR) from LOTE_FUND_STRESS_RPM at LEVEL=2 → -0.57%.
Target: -0.64% (= -929,245 BRL on NAV 145.71M).

Try:
  A) RPM at every LEVEL (1, 2, 3, 10) — sum PARAMETRIC_VAR
  B) RPM at LEVEL=2 by TREE — maybe multi-tree double counts or single-tree misses
  C) LOTE_FUND_STRESS (the LFS table, used by MACRO_Q) with TREE='Main' — sum PVAR1DAY
  D) Same as A) but for EVOLUTION (cross-check)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from glpg_fetch import read_sql

DATE = "2026-04-28"

FUNDS = {
    "MACRO":     ("Galapagos Macro FIM",            -929_245,  -0.64,  145_710_752.13),
    "EVOLUTION": ("Galapagos Evolution FIC FIM CP",  None,      None,  None),
    "QUANT":     ("Galapagos Quantitativo FIM",     -206_919,  -0.42,   49_441_143.80),
}

print(f"\n=== DATE {DATE} ===\n")

# ── A) RPM at every LEVEL, all TREEs aggregated ─────────────────────────────
print("=" * 95)
print("A) LOTE_FUND_STRESS_RPM — SUM(PARAMETRIC_VAR) by LEVEL (TREE-aggregated)")
print("=" * 95)
for code, (td, tgt_brl, tgt_pct, nav) in FUNDS.items():
    df = read_sql(f"""
        SELECT "LEVEL",
               COUNT(*) AS n_rows,
               COUNT(DISTINCT "TREE") AS n_trees,
               SUM("PARAMETRIC_VAR") AS pvar
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
        GROUP BY "LEVEL"
        ORDER BY "LEVEL"
    """)
    nav_q = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "VAL_DATE" = DATE '{DATE}' AND "TRADING_DESK" = '{td}'
    """)
    n = float(nav_q["NAV"].iloc[0]) if not nav_q.empty else (nav or 1)
    print(f"\n--- {code} (NAV {n:,.2f}, target {tgt_pct}%  /  {tgt_brl}) ---")
    if df.empty:
        print("  (no rows)")
        continue
    for _, r in df.iterrows():
        pvar = float(r["pvar"] or 0)
        pct = -pvar / n * 100
        flag = "  <-- MATCH" if (tgt_brl is not None and abs(pvar - tgt_brl) / abs(tgt_brl) < 0.02) else ""
        print(f"  LEVEL={int(r['LEVEL']):3d}  rows={int(r['n_rows']):5d}  trees={int(r['n_trees'])}  "
              f"sum(PARAMETRIC_VAR)={pvar:>14,.0f}  pct={pct:+7.3f}%{flag}")

# ── B) RPM at LEVEL=2 split by TREE (look for tree-specific aggregation) ────
print()
print("=" * 95)
print("B) LOTE_FUND_STRESS_RPM — SUM(PARAMETRIC_VAR) at LEVEL=2 by TREE (MACRO/EVO)")
print("=" * 95)
for code in ["MACRO", "EVOLUTION"]:
    td, tgt_brl, tgt_pct, nav = FUNDS[code]
    df = read_sql(f"""
        SELECT "TREE",
               COUNT(*) AS n_rows,
               SUM("PARAMETRIC_VAR") AS pvar
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
          AND "LEVEL" = 2
        GROUP BY "TREE"
        ORDER BY "TREE"
    """)
    nav_q = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "VAL_DATE" = DATE '{DATE}' AND "TRADING_DESK" = '{td}'
    """)
    n = float(nav_q["NAV"].iloc[0]) if not nav_q.empty else (nav or 1)
    print(f"\n--- {code} (NAV {n:,.2f}) ---")
    if df.empty:
        print("  (no rows)")
        continue
    for _, r in df.iterrows():
        pvar = float(r["pvar"] or 0)
        pct = -pvar / n * 100
        print(f"  TREE={r['TREE'][:40]:40s}  rows={int(r['n_rows']):4d}  pvar={pvar:>14,.0f}  pct={pct:+7.3f}%")

# ── C) LOTE_FUND_STRESS (LFS) — does MACRO have entries here? ───────────────
print()
print("=" * 95)
print("C) LOTE_FUND_STRESS (LFS) — does MACRO/EVO have rows?  SUM(PVAR1DAY) by TREE")
print("=" * 95)
for code in ["MACRO", "EVOLUTION", "QUANT"]:
    td, tgt_brl, tgt_pct, nav = FUNDS[code]
    df = read_sql(f"""
        SELECT "TREE",
               COUNT(*) AS n_rows,
               SUM("PVAR1DAY")  AS pv1,
               SUM("HVAR1DAY")  AS hv1,
               SUM("MACRO_STRESS")    AS mstress,
               SUM("SPECIFIC_STRESS") AS sstress
        FROM "LOTE45"."LOTE_FUND_STRESS"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
        GROUP BY "TREE"
        ORDER BY "TREE"
    """)
    nav_q = read_sql(f"""
        SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "VAL_DATE" = DATE '{DATE}' AND "TRADING_DESK" = '{td}'
    """)
    n = float(nav_q["NAV"].iloc[0]) if not nav_q.empty else (nav or 1)
    print(f"\n--- {code} (NAV {n:,.2f}, target {tgt_pct}%) ---")
    if df.empty:
        print("  (no rows in LOTE_FUND_STRESS)")
        continue
    for _, r in df.iterrows():
        pv1 = float(r["pv1"] or 0)
        hv1 = float(r["hv1"] or 0)
        ms  = float(r["mstress"] or 0)
        ss  = float(r["sstress"] or 0)
        pct1 = -pv1 / n * 100
        flag = "  <-- MATCH" if (tgt_brl is not None and abs(pv1 - tgt_brl) / abs(tgt_brl) < 0.02) else ""
        print(f"  TREE={r['TREE'][:35]:35s}  rows={int(r['n_rows']):4d}  "
              f"PVAR1DAY={pv1:>14,.0f} ({pct1:+6.2f}%)  HVAR1DAY={hv1:>14,.0f}  "
              f"MS={ms:>14,.0f}  SS={ss:>14,.0f}{flag}")

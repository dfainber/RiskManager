"""Reverse-engineer which DB column the kit should read to match the
official APM 'Parametric VaR' tab numbers.

Ground truth (4 GUI screenshots, 2026-04-28):

  Fund         NAV          P.VaR1   P.VaR5   H.VaR1   H.VaR5   Stress
  QUANT        49,441,144   -0.42%   -0.96%   -0.50%   -1.01%   -3.97%
  MACRO_Q      23,251,428   -0.18%   -0.46%   -0.22%   -0.44%   -2.75%
  EVOLUTION    (NAV from DB) primitive Total Abs VaR Pct -0.76%

(EVOLUTION inferred from the primitive table: Macro Access cota + IDKA cota
look-through (BRD_*/DI_*) + direct VALE3/SPX/USDBRLSpot + Pinzon cash.)

Kit currently reads:
  - LOTE_FUND_STRESS_RPM."PARAMETRIC_VAR"      (QUANT, MACRO, EVO, BALTRA L=10)
  - LOTE_FUND_STRESS."PVAR1DAY"                (ALBATROZ, MACRO_Q, RAW_FUNDS)

This probe:
  1. Lists ALL numeric columns in both stress tables
  2. Pulls each candidate column SUM at the canonical fund LEVEL
  3. Divides by NAV → matches against GUI %s
  4. Flags which column = which GUI cell
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from glpg_fetch import read_sql

DATE = "2026-04-28"

FUNDS_RPM = {  # use LOTE_FUND_STRESS_RPM
    "QUANT":      ("Galapagos Quantitativo FIM",     2),
    "MACRO":      ("Galapagos Macro FIM",            2),
    "EVOLUTION":  ("Galapagos Evolution FIC FIM CP", 2),
}
FUNDS_LFS = {  # use LOTE_FUND_STRESS (product-level, TREE='Main' summed)
    "MACRO_Q":  "Galapagos Global Macro Q",
}

# Ground truth from GUI screenshots / primitive dumps (decimal % of NAV, signed)
# QUANT and MACRO_Q have the full APM headline panel.
# MACRO and EVO only have the primitive-tab Total Abs VaR Pct (single horizon).
GROUND_TRUTH = {
    "QUANT":     {"P.VaR1": -0.42, "P.VaR5": -0.96, "H.VaR1": -0.50, "H.VaR5": -1.01, "Stress": -3.97},
    "MACRO_Q":   {"P.VaR1": -0.18, "P.VaR5": -0.46, "H.VaR1": -0.22, "H.VaR5": -0.44, "Stress": -2.75},
    "MACRO":     {"Total Abs VaR Pct (Parametric tab)": -0.76},
    "EVOLUTION": {"Total Abs VaR Pct (Parametric tab)": -0.62},
}

# ── 1. Discover all numeric columns in both tables ──────────────────────────
print("=" * 75)
print("1. SCHEMA — column inventory of both stress tables")
print("=" * 75)
for tbl in ["LOTE_FUND_STRESS_RPM", "LOTE_FUND_STRESS"]:
    cols = read_sql(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE upper(table_schema) = 'LOTE45'
          AND upper(table_name) = '{tbl.upper()}'
        ORDER BY ordinal_position
    """)
    print(f"\n--- {tbl} ({len(cols)} cols) ---")
    print(cols.to_string(index=False))

# ── 2. RPM funds: pull every numeric col SUM at canonical LEVEL ─────────────
print()
print("=" * 75)
print("2. LOTE_FUND_STRESS_RPM — all numeric SUMs at canonical LEVEL")
print("=" * 75)

# Get NAVs
all_funds = list(set(td for td, _ in FUNDS_RPM.values()) | set(FUNDS_LFS.values()))
fund_list = ", ".join(f"'{n}'" for n in all_funds)
nav_q = f"""
SELECT "TRADING_DESK", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "VAL_DATE" = DATE '{DATE}'
  AND "TRADING_DESK" IN ({fund_list})
"""
nav_df = read_sql(nav_q)
nav_map = dict(zip(nav_df["TRADING_DESK"], nav_df["NAV"].astype(float)))
print("\nNAVs (DB):")
for k, v in nav_map.items():
    print(f"  {k[:40]:40s}  {v:>16,.2f}")

# Probe RPM columns: all candidates that look VaR/stress related
RPM_CANDIDATES = [
    "PARAMETRIC_VAR", "PARAMETRIC_VAR_5D", "PARAMETRIC_VAR_5",
    "HISTORICAL_VAR", "HISTORICAL_VAR_5D", "HISTORICAL_VAR_5",
    "PVAR1DAY", "PVAR5DAY", "HVAR1DAY", "HVAR5DAY",
    "P_VAR_1", "P_VAR_5", "H_VAR_1", "H_VAR_5",
    "VAR_1D", "VAR_5D",
    "MACRO_STRESS", "SPECIFIC_STRESS",
]

# Discover which actually exist
rpm_cols_df = read_sql("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'lote45' AND table_name = 'lote_fund_stress_rpm'
""")
rpm_existing = set(rpm_cols_df["column_name"].str.upper())
rpm_use = [c for c in RPM_CANDIDATES if c in rpm_existing]
print(f"\nRPM cols present from candidate list: {rpm_use}")

if rpm_use:
    sum_clause = ", ".join(f'SUM("{c}") AS {c.lower()}' for c in rpm_use)
    for code, (td, lvl) in FUNDS_RPM.items():
        q = f"""
        SELECT "TRADING_DESK", "LEVEL", {sum_clause}
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
          AND "LEVEL" = {lvl}
        GROUP BY "TRADING_DESK", "LEVEL"
        """
        df = read_sql(q)
        n = nav_map.get(td)
        gt = GROUND_TRUTH.get(code, {})
        print(f"\n--- {code} ({td}, LEVEL={lvl}) ---")
        if df.empty or n is None:
            print("  (no rows or no NAV)")
            continue
        print(f"  NAV = {n:,.2f}")
        for c in rpm_use:
            v = float(df[c.lower()].iloc[0]) if not pd.isna(df[c.lower()].iloc[0]) else 0.0
            pct = -v / n * 100
            # Try to identify match against GUI cells
            tag = ""
            for gt_label, gt_pct in gt.items():
                if gt_label == "NAV":
                    continue
                if abs(pct - gt_pct) < 0.02:  # within 2 bps tolerance
                    tag = f"  <-- matches GUI {gt_label} ({gt_pct:+.2f}%)"
                    break
            print(f"  {c:25s}  raw={v:>16,.2f}   pct={pct:+7.3f}%{tag}")

# ── 3. LFS funds (MACRO_Q): all numeric SUMs with TREE='Main' ───────────────
print()
print("=" * 75)
print("3. LOTE_FUND_STRESS — all numeric SUMs (TREE='Main')")
print("=" * 75)

LFS_CANDIDATES = [
    "PVAR1DAY", "PVAR5DAY", "HVAR1DAY", "HVAR5DAY",
    "PARAMETRIC_VAR", "HISTORICAL_VAR",
    "MACRO_STRESS", "SPECIFIC_STRESS",
]
lfs_cols_df = read_sql("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'lote45' AND table_name = 'lote_fund_stress'
""")
lfs_existing = set(lfs_cols_df["column_name"].str.upper())
lfs_use = [c for c in LFS_CANDIDATES if c in lfs_existing]
print(f"\nLFS cols present from candidate list: {lfs_use}")

if lfs_use:
    sum_clause = ", ".join(f'SUM("{c}") AS {c.lower()}' for c in lfs_use)
    for code, td in FUNDS_LFS.items():
        q = f"""
        SELECT "TRADING_DESK", {sum_clause}
        FROM "LOTE45"."LOTE_FUND_STRESS"
        WHERE "VAL_DATE" = DATE '{DATE}'
          AND "TRADING_DESK" = '{td}'
          AND "TREE" = 'Main'
        GROUP BY "TRADING_DESK"
        """
        df = read_sql(q)
        n = nav_map.get(td)
        gt = GROUND_TRUTH.get(code, {})
        print(f"\n--- {code} ({td}) ---")
        if df.empty or n is None:
            print("  (no rows or no NAV)")
            continue
        print(f"  NAV = {n:,.2f}")
        for c in lfs_use:
            v = float(df[c.lower()].iloc[0]) if not pd.isna(df[c.lower()].iloc[0]) else 0.0
            pct = -v / n * 100
            tag = ""
            for gt_label, gt_pct in gt.items():
                if gt_label == "NAV":
                    continue
                if abs(pct - gt_pct) < 0.02:
                    tag = f"  <-- matches GUI {gt_label} ({gt_pct:+.2f}%)"
                    break
            print(f"  {c:25s}  raw={v:>16,.2f}   pct={pct:+7.3f}%{tag}")

# ── 4. Summary verdict ──────────────────────────────────────────────────────
print()
print("=" * 75)
print("4. VERDICT")
print("=" * 75)
print("""
Look for the column whose pct matches each GUI cell.

GUI 'Relative VaR' (per-primitive) column SUMS to the headline P.VaR1
(verified: MACRO_Q sum of Relative VaR = -41,712 = headline P.VaR1).
'Absolute VaR' sums to a non-diversified magnitude (overstated ~2.5x).
So the right kit fetch reads the column equivalent to GUI 'Relative VaR'.

Kit currently reads:
  RPM 'PARAMETRIC_VAR'  -- if matches P.VaR1 in pct, kit is correct on column
  LFS 'PVAR1DAY'        -- same check for MACRO_Q

If neither matches, kit is reading the wrong column.
""")

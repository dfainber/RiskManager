"""Round 3 — find the position-level specific items in LOTE_BOOK_STRESS.

GUI screenshots (28-abr) prove:
    Stress = Scenario + Specific

QUANT:    Scenario -1,431,806 + Specific -530,521 = Total -1,962,327 (-3.97%)
MACRO_Q:  Scenario -40,443    + Specific -599,138 = Total -639,581  (-2.75%)
MACRO:    Scenario -168,767   + Specific -5,061,578 = Total -5,230,345 (-3.59%)

Specific items have a "Chosen Date" = worst historical day per asset.

DB columns in LOTE_FUND_STRESS_RPM:
- MACRO_STRESS:    MATCHES Scenario for QUANT (1.6% off), MISMATCHES MACRO (5x)
- SPECIFIC_STRESS: UNDERSTATES Specific by 30-43%

Hypothesis: LOTE_BOOK_STRESS (non-RPM) has the position-level specifics with
'Chosen Date' style data. Find columns, find the specific items, sum them.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import numpy as np
from glpg_fetch import read_sql

DATE = "2026-04-28"
FUND_TD = {
    "MACRO":      "Galapagos Macro FIM",
    "QUANT":      "Galapagos Quantitativo FIM",
    "EVOLUTION":  "Galapagos Evolution FIC FIM CP",
    "MACRO_Q":    "Galapagos Global Macro Q",
}

# ── 1. Schema discovery for ALL 5 stress-related tables in LOTE45 ──────────
TABLES = [
    "LOTE_BOOK_STRESS",
    "LOTE_BOOK_STRESS_RPM",
    "LOTE_FUND_STRESS",
    "LOTE_FUND_STRESS_RPM",
    "LOTE_BOOKS_PRODUCT_BUCKETS_RISK_REPORT",
]
print("=== Column schemas ===")
for tbl in TABLES:
    cols = read_sql(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'LOTE45' AND table_name = '{tbl}'
        ORDER BY ordinal_position
    """)
    print(f"\n  {tbl}: {len(cols)} cols")
    for _, r in cols.iterrows():
        print(f"     {r['column_name']:35s} {r['data_type']}")

# ── 2. For QUANT 28-abr, dump LOTE_BOOK_STRESS contents and look for the
#    specific items: Galapagos Pinzon, Com BMF Syn 4M, Galapagos Global, LFT Premium ─
print("\n\n=== LOTE_BOOK_STRESS — QUANT 28-abr (raw rows, top 30 by abs SPECIFIC_STRESS) ===")
q1 = read_sql(f"""
    SELECT *
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" = '{FUND_TD["QUANT"]}'
    ORDER BY ABS(COALESCE("SPECIFIC_STRESS", 0)) DESC
    LIMIT 30
""")
if not q1.empty:
    cols_to_show = [c for c in q1.columns if c not in ("VAL_DATE",)]
    print(q1[cols_to_show].to_string(index=False))

# ── 3. Same for MACRO ──────────────────────────────────────────────────────
print("\n\n=== LOTE_BOOK_STRESS — MACRO 28-abr (top 30 by abs SPECIFIC_STRESS) ===")
m1 = read_sql(f"""
    SELECT *
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" = '{FUND_TD["MACRO"]}'
    ORDER BY ABS(COALESCE("SPECIFIC_STRESS", 0)) DESC
    LIMIT 30
""")
if not m1.empty:
    cols_to_show = [c for c in m1.columns if c not in ("VAL_DATE",)]
    print(m1[cols_to_show].to_string(index=False))

# ── 4. Fund-level sums per metric column (find which sums to GUI numbers) ──
print("\n\n=== LOTE_BOOK_STRESS fund totals — 28-abr ===")
q_tot = read_sql(f"""
    SELECT "TRADING_DESK",
           COUNT(*) AS n_rows,
           SUM("SPECIFIC_STRESS") AS spec_sum,
           SUM("MACRO_STRESS") AS macro_sum,
           SUM("PVAR1DAY") AS pvar_sum
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({", ".join(f"'{n}'" for n in FUND_TD.values())})
    GROUP BY "TRADING_DESK"
""")
print(q_tot.to_string(index=False))

# ── 5. Also examine LOTE_BOOK_STRESS_RPM ──────────────────────────────────
print("\n\n=== LOTE_BOOK_STRESS_RPM fund totals — 28-abr ===")
q_rpm = read_sql(f"""
    SELECT "TRADING_DESK", "LEVEL",
           COUNT(*) AS n_rows,
           SUM("SPECIFIC_STRESS") AS spec_sum,
           SUM("MACRO_STRESS") AS macro_sum,
           SUM("PARAMETRIC_VAR") AS pvar_sum
    FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" IN ({", ".join(f"'{n}'" for n in FUND_TD.values())})
    GROUP BY "TRADING_DESK", "LEVEL"
    ORDER BY "TRADING_DESK", "LEVEL"
""")
print(q_rpm.to_string(index=False))

# ── 6. LOTE_BOOKS_PRODUCT_BUCKETS_RISK_REPORT ─────────────────────────────
print("\n\n=== LOTE_BOOKS_PRODUCT_BUCKETS_RISK_REPORT — 28-abr (sample 5 QUANT rows) ===")
q_pb = read_sql(f"""
    SELECT *
    FROM "LOTE45"."LOTE_BOOKS_PRODUCT_BUCKETS_RISK_REPORT"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" = '{FUND_TD["QUANT"]}'
    LIMIT 5
""")
if not q_pb.empty:
    print(q_pb.to_string(index=False))

# ── 7. Dump QUANT specific items from BOOK_STRESS by description matching ─
print("\n\n=== Look for Specific items by name (QUANT) ===")
q_spec = read_sql(f"""
    SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS",
           "SPECIFIC_STRESS", "MACRO_STRESS", "PVAR1DAY"
    FROM "LOTE45"."LOTE_BOOK_STRESS"
    WHERE "VAL_DATE" = DATE '{DATE}'
      AND "TRADING_DESK" = '{FUND_TD["QUANT"]}'
      AND ("PRODUCT" ILIKE '%Pinzon%'
        OR "PRODUCT" ILIKE '%BMF Syn%'
        OR "PRODUCT" ILIKE '%LFT%'
        OR "PRODUCT" ILIKE '%Galapagos%')
""")
print(q_spec.to_string(index=False))

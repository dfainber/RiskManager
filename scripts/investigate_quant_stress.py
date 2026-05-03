"""Reverse-engineer the QUANT (and MACRO/EVO) official 'Quant e Macro V' stress.

Ground truth (Asset_RiscoMercado_*.xlsx):
    Date       MACRO    QUANT    EVO(E)   EVO
    15/abr     4.53%    6.77%    —        4.20%
    16/abr     —        6.13%    —        —
    24/abr     3.92%    4.22%    4.36%    4.16%
    28/abr     4.05%    3.92%    4.50%    4.16%

Goal: find which combination of LOTE_FUND_STRESS_RPM columns + HS percentile
replicates the official numbers. Hint: parametric for macro books, HS for
L/S equity BR books (Bracco / Quant_PA).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from glpg_fetch import read_sql

DATES = ["2026-04-15", "2026-04-16", "2026-04-24", "2026-04-28"]
FUNDS = {
    "MACRO":      "Galapagos Macro FIM",
    "QUANT":      "Galapagos Quantitativo FIM",
    "EVOLUTION":  "Galapagos Evolution FIC FIM CP",
}

date_list = ", ".join(f"DATE '{d}'" for d in DATES)
fund_list = ", ".join(f"'{n}'" for n in FUNDS.values())

# ── 1. RPM per BOOK ─────────────────────────────────────────────────────────
q_rpm = f"""
SELECT "VAL_DATE", "TRADING_DESK", "BOOK", "LEVEL",
       SUM("PARAMETRIC_VAR")  AS pvar,
       SUM("SPECIFIC_STRESS") AS sstress,
       SUM("MACRO_STRESS")    AS mstress,
       COUNT(*)               AS n_rows
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "VAL_DATE" IN ({date_list})
  AND "TRADING_DESK" IN ({fund_list})
GROUP BY "VAL_DATE", "TRADING_DESK", "BOOK", "LEVEL"
ORDER BY "VAL_DATE", "TRADING_DESK", "LEVEL", "BOOK"
"""
rpm = read_sql(q_rpm)
rpm["VAL_DATE"] = pd.to_datetime(rpm["VAL_DATE"])
print("=== 1. LOTE_FUND_STRESS_RPM (BRL) ===")
print(f"rows: {len(rpm)} | levels: {sorted(rpm['LEVEL'].unique())}")
print()

# ── 2. NAV per fund per date ────────────────────────────────────────────────
q_nav = f"""
SELECT "VAL_DATE", "TRADING_DESK", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "VAL_DATE" IN ({date_list})
  AND "TRADING_DESK" IN ({fund_list})
"""
nav = read_sql(q_nav)
nav["VAL_DATE"] = pd.to_datetime(nav["VAL_DATE"])
nav = nav.set_index(["VAL_DATE", "TRADING_DESK"])["NAV"].astype(float)
print("=== 2. NAV ===")
print(nav.unstack(0).to_string(float_format="{:>16,.0f}".format))
print()

# ── 3. RPM totals by fund (LEVEL=2 = fund total) ────────────────────────────
print("=== 3. RPM totals (LEVEL=2 = fund aggregate) — % NAV ===")
lvl2 = rpm[rpm["LEVEL"] == 2].copy()
fund_tot = lvl2.groupby(["VAL_DATE", "TRADING_DESK"], as_index=False)[
    ["pvar", "sstress", "mstress"]
].sum()
for _, row in fund_tot.iterrows():
    n = nav.get((row["VAL_DATE"], row["TRADING_DESK"]))
    if n is None:
        continue
    p_pct = -row["pvar"]    / n * 100
    s_pct = -row["sstress"] / n * 100
    m_pct = -row["mstress"] / n * 100
    sum_sm = s_pct + m_pct
    rss    = (s_pct**2 + m_pct**2) ** 0.5
    print(f"  {row['VAL_DATE'].date()} {row['TRADING_DESK'][:35]:35s}  "
          f"PVaR={p_pct:6.2f}%  Spec={s_pct:6.2f}%  Macro={m_pct:6.2f}%  "
          f"S+M={sum_sm:6.2f}%  sqrt(S2+M2)={rss:6.2f}%")
print()

# ── 4. RPM by BOOK for QUANT only (the most interesting case) ───────────────
print("=== 4. QUANT — RPM by BOOK (LEVEL=3) — % NAV ===")
q_lvl3 = rpm[(rpm["LEVEL"] == 3) & (rpm["TRADING_DESK"] == FUNDS["QUANT"])].copy()
for d in pd.to_datetime(DATES):
    n = nav.get((d, FUNDS["QUANT"]))
    sub = q_lvl3[q_lvl3["VAL_DATE"] == d].copy()
    if sub.empty or n is None:
        continue
    sub["pvar_pct"]    = -sub["pvar"]    / n * 100
    sub["sstress_pct"] = -sub["sstress"] / n * 100
    sub["mstress_pct"] = -sub["mstress"] / n * 100
    sub = sub.sort_values("BOOK")
    print(f"\n  --- {d.date()} (NAV={n:,.0f}) ---")
    print(sub[["BOOK","pvar_pct","sstress_pct","mstress_pct"]].to_string(
        index=False, float_format="{:7.2f}".format))
print()

# ── 5. HS percentiles for SIST / MACRO / EVOLUTION on each date ─────────────
print("=== 5. PORTIFOLIO_DAILY_HISTORICAL_SIMULATION — percentiles of W ===")
q_hs = f"""
SELECT "PORTIFOLIO_DATE", "PORTIFOLIO",
       COUNT(*) AS n,
       MIN("W")  AS w_min,
       percentile_cont(0.005) WITHIN GROUP (ORDER BY "W") AS w_pct005,
       percentile_cont(0.01)  WITHIN GROUP (ORDER BY "W") AS w_pct01,
       percentile_cont(0.025) WITHIN GROUP (ORDER BY "W") AS w_pct025,
       percentile_cont(0.05)  WITHIN GROUP (ORDER BY "W") AS w_pct05,
       AVG("W")  AS w_mean,
       STDDEV("W") AS w_std
FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
WHERE "PORTIFOLIO_DATE" IN ({date_list})
  AND "PORTIFOLIO" IN ('SIST','MACRO','EVOLUTION')
GROUP BY "PORTIFOLIO_DATE", "PORTIFOLIO"
ORDER BY "PORTIFOLIO_DATE", "PORTIFOLIO"
"""
hs = read_sql(q_hs)
print(hs.to_string(index=False, float_format="{:8.5f}".format))
print()

# Save raw outputs for follow-up analysis
out_dir = Path(__file__).parent / "out"
out_dir.mkdir(exist_ok=True)
rpm.to_csv(out_dir / "quant_stress_rpm.csv", index=False)
nav.reset_index().to_csv(out_dir / "quant_stress_nav.csv", index=False)
hs.to_csv(out_dir / "quant_stress_hs.csv", index=False)
print(f"--> wrote 3 csvs to {out_dir}/")

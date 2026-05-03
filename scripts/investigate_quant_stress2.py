"""Round 2 — comprehensive formula tester.

Ground truth (Asset_RiscoMercado xlsx + mandato file):
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import numpy as np
from glpg_fetch import read_sql

OFFICIAL = {
    # (date, fund) -> stress%
    ("2026-04-15", "QUANT"):     6.77,
    ("2026-04-16", "QUANT"):     6.13,
    ("2026-04-24", "QUANT"):     4.22,
    ("2026-04-28", "QUANT"):     3.92,
    ("2026-04-15", "MACRO"):     4.53,
    ("2026-04-24", "MACRO"):     3.92,
    ("2026-04-28", "MACRO"):     4.05,
    ("2026-04-15", "EVOLUTION"): 4.20,
    ("2026-04-24", "EVOLUTION"): 4.16,
    ("2026-04-28", "EVOLUTION"): 4.16,
}
FUND_TD = {
    "MACRO":      "Galapagos Macro FIM",
    "QUANT":      "Galapagos Quantitativo FIM",
    "EVOLUTION":  "Galapagos Evolution FIC FIM CP",
}
DATES = sorted({d for d, _ in OFFICIAL})
date_list = ", ".join(f"DATE '{d}'" for d in DATES)
fund_list = ", ".join(f"'{n}'" for n in FUND_TD.values())

# ── Load NAV ────────────────────────────────────────────────────────────────
nav = read_sql(f"""
    SELECT "VAL_DATE", "TRADING_DESK", "NAV"
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "VAL_DATE" IN ({date_list})
      AND "TRADING_DESK" IN ({fund_list})
""")
nav["VAL_DATE"] = pd.to_datetime(nav["VAL_DATE"])
nav_lookup = nav.set_index(["VAL_DATE", "TRADING_DESK"])["NAV"].astype(float).to_dict()

# ── 1. RPM at all available LEVELs (2, 3, 10) ───────────────────────────────
rpm = read_sql(f"""
    SELECT "VAL_DATE", "TRADING_DESK", "BOOK", "PRODUCT", "PRODUCT_CLASS", "LEVEL",
           "PARAMETRIC_VAR" AS pvar, "SPECIFIC_STRESS" AS sstress,
           "MACRO_STRESS" AS mstress
    FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
    WHERE "VAL_DATE" IN ({date_list})
      AND "TRADING_DESK" IN ({fund_list})
""")
rpm["VAL_DATE"] = pd.to_datetime(rpm["VAL_DATE"])
print(f"RPM rows: {len(rpm)}, levels: {sorted(rpm['LEVEL'].unique())}")
print(f"Rows per level: {rpm.groupby('LEVEL').size().to_dict()}")
print()

# ── 2. Schema discovery — what tables in LOTE45 exist with 'STRESS' or 'SCENARIO'? ─
print("=== Schema scan: LOTE45 tables matching STRESS/SCENARIO/SHOCK ===")
schema = read_sql("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'LOTE45'
      AND (table_name ILIKE '%STRESS%'
           OR table_name ILIKE '%SCENARIO%'
           OR table_name ILIKE '%SHOCK%'
           OR table_name ILIKE '%RISK%')
    ORDER BY table_name
""")
print(schema.to_string(index=False))
print()

# ── 3. Aggregate at each level for each (date, fund) ─────────────────────────
def fund_aggs(level: int) -> pd.DataFrame:
    sub = rpm[rpm["LEVEL"] == level].copy()
    if sub.empty:
        return pd.DataFrame()
    grp = sub.groupby(["VAL_DATE", "TRADING_DESK"], as_index=False).agg(
        pvar_sum    =("pvar",    "sum"),
        sstress_sum =("sstress", "sum"),
        mstress_sum =("mstress", "sum"),
        pvar_abs_sum    =("pvar",    lambda s: s.abs().sum()),
        sstress_abs_sum =("sstress", lambda s: s.abs().sum()),
        mstress_abs_sum =("mstress", lambda s: s.abs().sum()),
        # Sum_|S+M|: net shock per row, then |.|, sum
        sm_abs_sum_per_row=("sstress", lambda s: 0),  # placeholder, override below
    )
    # Compute Sum_|S+M| per (date, fund) by row aggregation
    sub["sm"] = sub["sstress"].fillna(0) + sub["mstress"].fillna(0)
    sm_abs = sub.groupby(["VAL_DATE","TRADING_DESK"])["sm"].apply(lambda s: s.abs().sum()).reset_index(name="sm_abs_sum_per_row")
    grp = grp.drop(columns=["sm_abs_sum_per_row"]).merge(sm_abs, on=["VAL_DATE","TRADING_DESK"])
    return grp

agg2  = fund_aggs(2)   # fund total
agg3  = fund_aggs(3)   # by book
agg10 = fund_aggs(10)  # by leaf

# ── 4. HS portfolios for SIST/MACRO/EVOLUTION ───────────────────────────────
hs_pcts = read_sql(f"""
    SELECT "PORTIFOLIO_DATE" AS dt, "PORTIFOLIO" AS pf,
           percentile_cont(0.005) WITHIN GROUP (ORDER BY "W") AS w_pct005,
           percentile_cont(0.01)  WITHIN GROUP (ORDER BY "W") AS w_pct01,
           percentile_cont(0.025) WITHIN GROUP (ORDER BY "W") AS w_pct025,
           percentile_cont(0.05)  WITHIN GROUP (ORDER BY "W") AS w_pct05
    FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
    WHERE "PORTIFOLIO_DATE" IN ({date_list})
      AND "PORTIFOLIO" IN ('SIST','MACRO','EVOLUTION')
    GROUP BY "PORTIFOLIO_DATE", "PORTIFOLIO"
""")
hs_pcts["dt"] = pd.to_datetime(hs_pcts["dt"])

# ── 5. Hypothesis tester ────────────────────────────────────────────────────
def to_pct(brl, nav_val):
    return -brl / nav_val * 100 if nav_val else float("nan")

def build_predictions():
    rows = []
    for (date_s, fund_short), official in OFFICIAL.items():
        d = pd.Timestamp(date_s)
        td = FUND_TD[fund_short]
        n = nav_lookup.get((d, td))
        if n is None:
            continue
        a2 = agg2[(agg2["VAL_DATE"]==d) & (agg2["TRADING_DESK"]==td)]
        a3 = agg3[(agg3["VAL_DATE"]==d) & (agg3["TRADING_DESK"]==td)]
        a10= agg10[(agg10["VAL_DATE"]==d) & (agg10["TRADING_DESK"]==td)]
        if a2.empty:
            continue
        pv  = to_pct(a2["pvar_sum"].iloc[0], n)
        s2  = to_pct(a2["sstress_sum"].iloc[0], n)
        m2  = to_pct(a2["mstress_sum"].iloc[0], n)
        s2_abs_l3   = to_pct(a3["sstress_abs_sum"].iloc[0], n) if not a3.empty else float("nan")
        m2_abs_l3   = to_pct(a3["mstress_abs_sum"].iloc[0], n) if not a3.empty else float("nan")
        sm_abs_l3   = to_pct(a3["sm_abs_sum_per_row"].iloc[0], n) if not a3.empty else float("nan")
        s2_abs_l10  = to_pct(a10["sstress_abs_sum"].iloc[0], n) if not a10.empty else float("nan")
        m2_abs_l10  = to_pct(a10["mstress_abs_sum"].iloc[0], n) if not a10.empty else float("nan")
        sm_abs_l10  = to_pct(a10["sm_abs_sum_per_row"].iloc[0], n) if not a10.empty else float("nan")

        # HS (3 portfolios; pick the right one per fund)
        pf_map = {"MACRO":"MACRO","QUANT":"SIST","EVOLUTION":"EVOLUTION"}
        pf = pf_map[fund_short]
        hs = hs_pcts[(hs_pcts["dt"]==d) & (hs_pcts["pf"]==pf)]
        # W is in bps → divide by 100 to get pct
        if hs.empty:
            hs_pct1 = hs_pct005 = float("nan")
        else:
            hs_pct1   = -float(hs["w_pct01"].iloc[0])  / 100.0   # convert bps to %
            hs_pct005 = -float(hs["w_pct005"].iloc[0]) / 100.0

        # Also pull non-RPM table (different methodology: SUM by TRADING_DESK + TREE='Main')
        nonrpm = read_sql(f"""
            SELECT SUM("PVAR1DAY") AS pvar, SUM("SPECIFIC_STRESS") AS sstress,
                   SUM("MACRO_STRESS") AS mstress
            FROM "LOTE45"."LOTE_FUND_STRESS"
            WHERE "VAL_DATE" = DATE '{date_s}'
              AND "TRADING_DESK" = '{td}'
              AND "TREE" = 'Main'
        """) if not hasattr(build_predictions, "_nrpm_cache") else None
        # cache lookup
        ckey = (date_s, td)
        if not hasattr(build_predictions, "_nrpm_cache"):
            build_predictions._nrpm_cache = {}
        if ckey not in build_predictions._nrpm_cache:
            r = read_sql(f"""
                SELECT SUM("PVAR1DAY") AS pvar, SUM("SPECIFIC_STRESS") AS sstress,
                       SUM("MACRO_STRESS") AS mstress
                FROM "LOTE45"."LOTE_FUND_STRESS"
                WHERE "VAL_DATE" = DATE '{date_s}'
                  AND "TRADING_DESK" = '{td}'
                  AND "TREE" = 'Main'
            """)
            build_predictions._nrpm_cache[ckey] = r
        r = build_predictions._nrpm_cache[ckey]
        nr_pv = to_pct(float(r["pvar"].iloc[0] or 0), n)
        nr_s  = to_pct(float(r["sstress"].iloc[0] or 0), n)
        nr_m  = to_pct(float(r["mstress"].iloc[0] or 0), n)

        candidates = {
            # --- non-RPM table ---
            "[non-RPM] PVaR":               nr_pv,
            "[non-RPM] Spec":               nr_s,
            "[non-RPM] Macro":              nr_m,
            "[non-RPM] Spec+Macro":         nr_s + nr_m,
            # --- naive single columns ---
            "Spec (kit MACRO/EVO)":  s2,
            "Macro (kit QUANT)":     m2,
            "PVaR":                  pv,
            # --- additive ---
            "Spec + Macro":          s2 + m2,
            "Spec + Macro + PVaR":   s2 + m2 + pv,
            # --- RSS / quadratic combine ---
            "sqrt(S^2 + M^2)":       float(np.sqrt(s2*s2 + m2*m2)),
            "max(Spec, Macro)":      max(s2, m2),
            # --- absolute book-level (kills sign cancellation) ---
            "Sum_|Macro|@LEVEL3":       m2_abs_l3,
            "Sum_|Spec|@LEVEL3":        s2_abs_l3,
            "Sum_|Spec+Macro|@LEVEL3":  sm_abs_l3,
            "Sum_|Macro|@LEVEL10":      m2_abs_l10,
            "Sum_|Spec+Macro|@LEVEL10": sm_abs_l10,
            # --- HS-based ---
            "HS pct1 (1d)":          hs_pct1,
            "HS pct1 × sqrt3 (3d)":     hs_pct1 * np.sqrt(3),
            "HS pct1 × sqrt21 (1M)":    hs_pct1 * np.sqrt(21),
            "HS pct005 (1d)":        hs_pct005,
            # --- Hybrid: parametric on macro books + HS on equity books ---
            # Approx: Spec total + HS pct1 (since for QUANT, equity is mostly in Bracco/Quant_PA)
            "Spec + HS pct1 SIST":          s2 + hs_pct1,
            "Spec + HS pct1 × sqrt3":          s2 + hs_pct1 * np.sqrt(3),
            "Macro + HS pct1 × sqrt3":         m2 + hs_pct1 * np.sqrt(3),
            # --- User's sqrt(3) hint applied to current picks ---
            "Spec × sqrt3":                    s2 * np.sqrt(3),
            "Macro × sqrt3":                   m2 * np.sqrt(3),
            "(Spec + Macro) × sqrt3":          (s2 + m2) * np.sqrt(3),
            "Sum_|Spec+Macro|@L3 × sqrt3":        sm_abs_l3 * np.sqrt(3),
        }
        for name, val in candidates.items():
            rows.append({"date": date_s, "fund": fund_short, "official": official,
                         "formula": name, "predicted": val, "error": val - official})
    return pd.DataFrame(rows)

preds = build_predictions()

# ── 6. Rank formulas by RMSE across all (date, fund) pairs ─────────────────
rmse = preds.groupby("formula").agg(
    rmse=("error", lambda s: float(np.sqrt(np.mean(s.dropna()**2)))),
    mae =("error", lambda s: float(s.dropna().abs().mean())),
    n   =("error", "count"),
).sort_values("rmse").reset_index()
print("=== Ranking of formulas (across all 10 ground-truth points) ===")
print(rmse.to_string(index=False, float_format="{:7.3f}".format))
print()

# ── 7. Per-fund break-out for the top-5 ─────────────────────────────────────
top5 = rmse.head(5)["formula"].tolist()
print("=== Top-5 formulas, per (date, fund), error in pp ===")
piv = preds[preds["formula"].isin(top5)].pivot_table(
    index=["date","fund","official"], columns="formula", values="error")
print(piv.to_string(float_format="{:+6.2f}".format))
print()

print("=== Top-5 formulas, predicted values ===")
piv_v = preds[preds["formula"].isin(top5)].pivot_table(
    index=["date","fund","official"], columns="formula", values="predicted")
print(piv_v.to_string(float_format="{:6.2f}".format))

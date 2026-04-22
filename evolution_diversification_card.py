"""
evolution_diversification_card.py
=================================
Standalone preview of the EVOLUTION diversification-benefit card
(Camada 2 da skill evolution-risk-concentration).

Uso:
    python evolution_diversification_card.py          # usa a data mais recente
    python evolution_diversification_card.py 2026-04-17

Gera HTML em: data/morning-calls/evolution_diversification_<DATA>.html
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from glpg_fetch import read_sql

DESK = "Galapagos Evolution FIC FIM CP"
EXPECTED_STRATS = ["MACRO", "SIST", "FRONTIER", "CREDITO", "EVO_STRAT", "CAIXA"]
DIRECTIONAL_STRATS = ["MACRO", "SIST", "FRONTIER"]  # para Camada 3 (CREDITO excluído — cotas júnior)

LIVROS_MAP_PATH = (Path(__file__).parent / ".claude" / "skills"
                   / "evolution-risk-concentration" / "assets" / "livros-map.json")


def load_livros_map() -> dict[str, str]:
    """Retorna dict {LIVRO: estrategia}. Normaliza 'EVO_STRATEGY' -> 'EVO_STRAT'."""
    with open(LIVROS_MAP_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    out: dict[str, str] = {}
    for strat, livros in raw.items():
        if strat.startswith("_"):
            continue
        key = "EVO_STRAT" if strat == "EVO_STRATEGY" else strat
        for livro in livros:
            out[livro] = key
    return out


# ───────────────────── Data access ─────────────────────

def fetch_nav_series() -> pd.DataFrame:
    """NAV diário do Evolution (full history)."""
    return read_sql(f"""
        SELECT "VAL_DATE", "NAV"
        FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
        WHERE "TRADING_DESK" = '{DESK}'
        ORDER BY "VAL_DATE"
    """)


def fetch_strategy_var_history(date_str: str) -> pd.DataFrame:
    """VaR por estratégia em até 400 dias antes de `date_str`.
       LEVEL=2, TREE='Main_Macro_Gestores' (per SKILL.md queries doc)."""
    return read_sql(f"""
        SELECT "VAL_DATE", "BOOK",
               SUM("PARAMETRIC_VAR") AS "PARAMETRIC_VAR"
        FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
        WHERE "TRADING_DESK" = '{DESK}'
          AND "LEVEL"        = 2
          AND "TREE"         = 'Main_Macro_Gestores'
          AND "VAL_DATE"    >= DATE '{date_str}' - INTERVAL '400 days'
          AND "VAL_DATE"    <= DATE '{date_str}'
        GROUP BY "VAL_DATE", "BOOK"
        ORDER BY "VAL_DATE"
    """)


def fetch_fund_var_history(date_str: str) -> pd.DataFrame:
    """VaR agregado do fundo em até 400 dias antes de `date_str` (LEVEL=10)."""
    return read_sql(f"""
        SELECT "VAL_DATE",
               SUM("PARAMETRIC_VAR") AS "PARAMETRIC_VAR"
        FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
        WHERE "TRADING_DESK" = '{DESK}'
          AND "LEVEL"        = 10
          AND "VAL_DATE"    >= DATE '{date_str}' - INTERVAL '400 days'
          AND "VAL_DATE"    <= DATE '{date_str}'
        GROUP BY "VAL_DATE"
        ORDER BY "VAL_DATE"
    """)


def fetch_direction_report(date_str: str) -> pd.DataFrame:
    """Matriz direcional — DELTA_SISTEMATICO × DELTA_DISCRICIONARIO por CATEGORIA
       para Galapagos Evolution FIC FIM CP no dia `date_str`."""
    return read_sql(f"""
        SELECT "TIPO", "CATEGORIA", "NOME",
               "DELTA_SISTEMATICO", "DELTA_DISCRICIONARIO", "PCT_PL_TOTAL"
        FROM q_models."RISK_DIRECTION_REPORT"
        WHERE "FUNDO"    = '{DESK}'
          AND "VAL_DATE" = DATE '{date_str}'
    """)


def fetch_direction_report_history(date_str: str, lookback_days: int = 400) -> pd.DataFrame:
    """Histórico de RISK_DIRECTION_REPORT p/ computar P60 da magnitude
       combinada (|sist|+|disc|) por CATEGORIA na Camada Direcional."""
    return read_sql(f"""
        SELECT "VAL_DATE", "CATEGORIA", "NOME",
               "DELTA_SISTEMATICO", "DELTA_DISCRICIONARIO", "PCT_PL_TOTAL"
        FROM q_models."RISK_DIRECTION_REPORT"
        WHERE "FUNDO"    = '{DESK}'
          AND "VAL_DATE" >= DATE '{date_str}' - INTERVAL '{lookback_days} days'
          AND "VAL_DATE" <= DATE '{date_str}'
    """)


def fetch_pnl_by_livro(date_str: str) -> pd.DataFrame:
    """PnL diário por LIVRO em até 400 dias antes de `date_str`."""
    return read_sql(f"""
        SELECT "DATE" AS "VAL_DATE", "LIVRO",
               SUM("DIA") AS "PNL_DIA"
        FROM q_models."REPORT_ALPHA_ATRIBUTION"
        WHERE "FUNDO" = 'EVOLUTION'
          AND "DATE" >= DATE '{date_str}' - INTERVAL '400 days'
          AND "DATE" <= DATE '{date_str}'
        GROUP BY "DATE", "LIVRO"
        ORDER BY "DATE"
    """)


# ───────────────────── Compute ─────────────────────

def build_ratio_series(date_str: str) -> dict:
    """Retorna:
       {
         'date': 'YYYY-MM-DD',
         'nav': float,
         'strat_today_bps': {MACRO: 42.1, SIST: 18.3, ...},   # todos em bps positivos (= perda)
         'var_soma_bps': float,
         'var_real_bps': float,
         'ratio': float,
         'ratio_series': pd.Series (index=VAL_DATE, last 252d),
         'ratio_pct': percentil de hoje em 252d (0-100),
         'ratio_mean': média histórica 252d,
         'ratio_min':  min 252d,
         'ratio_max':  max 252d,
       }
    """
    nav_df   = fetch_nav_series()
    strat_df = fetch_strategy_var_history(date_str)
    fund_df  = fetch_fund_var_history(date_str)

    if nav_df.empty or strat_df.empty or fund_df.empty:
        raise RuntimeError(
            f"Dados incompletos para {date_str} — "
            f"nav={len(nav_df)} strat={len(strat_df)} fund={len(fund_df)}"
        )

    # Force VAL_DATE → pd.Timestamp
    for df in (nav_df, strat_df, fund_df):
        df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])

    nav_df = nav_df.set_index("VAL_DATE")["NAV"].astype(float)

    # Per-day NAV lookup: pad forward (NAV has 1 business-day lag vs risk tables)
    def nav_on(d: pd.Timestamp) -> float | None:
        sub = nav_df.loc[:d]
        return float(sub.iloc[-1]) if len(sub) else None

    # Consolidate strategy BOOKs: unexpected ones → OUTROS
    strat_df["BOOK"] = strat_df["BOOK"].where(
        strat_df["BOOK"].isin(EXPECTED_STRATS), "OUTROS"
    )
    strat_df = (strat_df.groupby(["VAL_DATE", "BOOK"], as_index=False)
                        ["PARAMETRIC_VAR"].sum())

    # Apply per-date NAV to convert to bps. PARAMETRIC_VAR is negative (loss at source).
    strat_df["nav_day"] = strat_df["VAL_DATE"].map(nav_on)
    strat_df["var_bps"] = (strat_df["PARAMETRIC_VAR"] * -10000
                           / strat_df["nav_day"].replace({0: np.nan}))

    fund_df["nav_day"] = fund_df["VAL_DATE"].map(nav_on)
    fund_df["var_bps"] = (fund_df["PARAMETRIC_VAR"] * -10000
                          / fund_df["nav_day"].replace({0: np.nan}))

    # Sum of strategy VaR per day
    sum_by_day = (strat_df.groupby("VAL_DATE")["var_bps"].sum()
                          .rename("var_soma_bps"))
    real_by_day = fund_df.set_index("VAL_DATE")["var_bps"].rename("var_real_bps")

    # Strategy × date pivot (bps), used by Camada 1 and for CREDITO winsorization
    strat_pivot = (strat_df.pivot_table(index="VAL_DATE", columns="BOOK",
                                        values="var_bps", aggfunc="sum")
                            .sort_index())

    # ── CREDITO treatment (see docs/CREDITO_TREATMENT.md) ──────────────────
    # Winsorize the CREDITO VaR series causally using a rolling 63-day robust
    # scale (median ± 3 × 1.4826·MAD). This smooths the December-2025 cotas-
    # júnior mark-to-market spike without deleting observations. Winsorized
    # CREDITO flows into the Σ VaR_s used for the ratio; raw values kept for
    # display and the CREDITO share metric.
    credito_series = (strat_pivot["CREDITO"]
                      if "CREDITO" in strat_pivot.columns
                      else pd.Series(0.0, index=strat_pivot.index))
    credito_wins, wins_dates = _winsorize_causal(credito_series)

    # Sum-of-strategies (winsorized): replace the raw CREDITO contribution
    # with the winsorized one, leaving the other strategies untouched.
    sum_raw   = sum_by_day.copy()
    delta_cr  = (credito_wins - credito_series).fillna(0.0)  # wins - raw
    sum_wins  = (sum_raw.add(delta_cr.reindex(sum_raw.index), fill_value=0.0)
                       .rename("var_soma_wins_bps"))

    joined = pd.concat([sum_by_day, sum_wins, real_by_day], axis=1).dropna()
    joined["var_credito_bps"]      = credito_series.reindex(joined.index)
    joined["var_credito_wins_bps"] = credito_wins.reindex(joined.index)
    joined["ratio"]      = joined["var_real_bps"] / joined["var_soma_bps"]
    joined["ratio_wins"] = joined["var_real_bps"] / joined["var_soma_wins_bps"]

    # Pick the target date: use exact if present, else last row ≤ date_str
    target = pd.to_datetime(date_str)
    eligible = joined.loc[:target]
    if eligible.empty:
        raise RuntimeError(f"Sem datas de referência ≤ {date_str}")
    today = eligible.iloc[-1]
    effective_date = eligible.index[-1]

    # Strategy breakdown today (bps) — raw values shown in the table
    strat_today = (strat_df[strat_df["VAL_DATE"] == effective_date]
                   .set_index("BOOK")["var_bps"].to_dict())

    # Historical 252d series (ending at effective_date)
    ratio_series_252      = joined["ratio"].loc[:effective_date].tail(252)
    ratio_wins_series_252 = joined["ratio_wins"].loc[:effective_date].tail(252)

    # Percentile of today (both raw and winsorized)
    def _pct(series: pd.Series, today_val: float) -> float:
        return float((series <= today_val).mean() * 100.0) \
               if len(series) >= 20 else float("nan")

    rank_pct      = _pct(ratio_series_252,      today["ratio"])
    rank_pct_wins = _pct(ratio_wins_series_252, today["ratio_wins"])

    # CREDITO share of Σ today (using raw values — the diagnostic we want to see)
    credito_share_raw  = (today["var_credito_bps"]      / today["var_soma_bps"]
                          if today["var_soma_bps"]  > 0 else float("nan"))
    credito_share_wins = (today["var_credito_wins_bps"] / today["var_soma_wins_bps"]
                          if today["var_soma_wins_bps"] > 0 else float("nan"))

    return {
        "date":            effective_date.strftime("%Y-%m-%d"),
        "effective_date":  effective_date,
        "nav":             float(nav_on(effective_date) or float("nan")),
        "strat_today_bps": strat_today,
        "strat_pivot":     strat_pivot,          # for Camada 1
        "var_soma_bps":    float(today["var_soma_bps"]),
        "var_real_bps":    float(today["var_real_bps"]),
        "ratio":           float(today["ratio"]),
        "ratio_series":    ratio_series_252,
        "ratio_pct":       rank_pct,
        "ratio_mean":      float(ratio_series_252.mean()),
        "ratio_min":       float(ratio_series_252.min()),
        "ratio_max":       float(ratio_series_252.max()),
        # Winsorized twin (primary model)
        "var_soma_wins_bps":  float(today["var_soma_wins_bps"]),
        "ratio_wins":         float(today["ratio_wins"]),
        "ratio_wins_series":  ratio_wins_series_252,
        "ratio_wins_pct":     rank_pct_wins,
        "ratio_wins_mean":    float(ratio_wins_series_252.mean()),
        "ratio_wins_min":     float(ratio_wins_series_252.min()),
        "ratio_wins_max":     float(ratio_wins_series_252.max()),
        # CREDITO diagnostics
        "var_credito_bps":      float(today["var_credito_bps"]),
        "var_credito_wins_bps": float(today["var_credito_wins_bps"]),
        "credito_share_raw":    credito_share_raw,
        "credito_share_wins":   credito_share_wins,
        "credito_wins_dates":   wins_dates,           # list of dates that got clipped
    }


def _winsorize_causal(series: pd.Series,
                      window: int = 63,
                      n_sigma: float = 3.0,
                      min_periods: int = 30) -> tuple[pd.Series, list[pd.Timestamp]]:
    """Causal winsorization using rolling robust scale (median ± n·1.4826·MAD).

    Applies only to the upper tail (VaR spikes); lower-tail clipping is skipped
    because low VaR days are not the concern here.

    Returns:
        (clipped_series, list_of_dates_where_clipping_occurred)

    Why MAD and not std: the December-2025 CREDITO spike pollutes standard-
    deviation estimates for weeks after the event, which would under-winsorize
    subsequent days. MAD is robust.

    Why causal (shift(1)): the rolling median on date t includes t itself. To
    avoid the metric "clipping itself", we build the cap from data up to t-1.
    """
    s = series.astype(float)
    # Past-only rolling stats
    past = s.shift(1)
    med = past.rolling(window, min_periods=min_periods).median()
    mad = (past - med).abs().rolling(window, min_periods=min_periods).median()
    scale = mad * 1.4826  # MAD → σ-equivalent under normal
    upper = med + n_sigma * scale

    clipped = s.where(s <= upper, upper)
    # Dates where clip actually changed the value (ignore NaN)
    changed_mask = (s > upper) & upper.notna()
    wins_dates = [d for d, flag in changed_mask.items() if flag]
    return clipped.fillna(s), wins_dates


# ─── Camada 1 — utilização histórica por estratégia ────────────────────────

def compute_camada1(strat_pivot: pd.DataFrame,
                    effective_date: pd.Timestamp) -> list[dict]:
    """Para cada estratégia: VaR hoje (bps), percentil 252d, estado.
       Retorna lista em ordem EXPECTED_STRATS + OUTROS."""
    rows: list[dict] = []
    window = strat_pivot.loc[:effective_date].tail(252)

    ordered = [s for s in EXPECTED_STRATS if s in strat_pivot.columns]
    extras  = [c for c in strat_pivot.columns if c not in EXPECTED_STRATS]
    for s in ordered + extras:
        series = window[s].dropna()
        if len(series) < 20:
            rows.append({"strat": s, "var_today": float("nan"),
                         "pct": float("nan"), "hist": series})
            continue
        today_val = float(series.iloc[-1])
        pct = float((series <= today_val).mean() * 100.0)
        rows.append({
            "strat":     s,
            "var_today": today_val,
            "pct":       pct,
            "hist":      series,
        })
    return rows


# ─── Camada 3 — correlação rolling entre PnLs das estratégias ──────────────

def compute_camada3(date_str: str,
                    effective_date: pd.Timestamp) -> dict:
    """PnL diário por estratégia → correlações 21d e 63d, percentil 252d de 63d.
       Retorna:
         {
           'corr_21d':  DataFrame (4x4) snapshot,
           'corr_63d':  DataFrame (4x4) snapshot,
           'pairs':     [{'a','b','c21','c63','c63_pct','c63_mean',
                          'c63_min','c63_max','series_63d'}, ...],
           'n_obs':     int (# observações PnL),
         }
    """
    df = fetch_pnl_by_livro(date_str)
    if df.empty:
        return {"corr_21d": None, "corr_63d": None, "pairs": [], "n_obs": 0}

    df["VAL_DATE"] = pd.to_datetime(df["VAL_DATE"])
    df = df[df["VAL_DATE"] <= effective_date]

    livros_map = load_livros_map()
    df["ESTRATEGIA"] = df["LIVRO"].map(livros_map).fillna("OUTROS")

    # Pivot: date × estratégia (sum PnL)
    pivot = (df.groupby(["VAL_DATE", "ESTRATEGIA"])["PNL_DIA"].sum()
               .unstack()
               .sort_index()
               .fillna(0.0))

    # Keep only directional strategies for the correlation matrix
    cols_present = [s for s in DIRECTIONAL_STRATS if s in pivot.columns]
    if len(cols_present) < 2:
        return {"corr_21d": None, "corr_63d": None, "pairs": [], "n_obs": len(pivot)}

    pivot_dir = pivot[cols_present]

    # Snapshot correlations (over last 21 / 63 days)
    corr_21 = pivot_dir.tail(21).corr()
    corr_63 = pivot_dir.tail(63).corr()

    # For each pair, rolling 63d correlation series (252 recent rolling values)
    pairs = []
    for i, a in enumerate(cols_present):
        for b in cols_present[i + 1:]:
            rolling = pivot_dir[a].rolling(63).corr(pivot_dir[b]).dropna()
            recent = rolling.tail(252)
            c63 = float(corr_63.loc[a, b])
            c21 = float(corr_21.loc[a, b])
            if len(recent) >= 20:
                pct = float((recent <= c63).mean() * 100.0)
                mean_val = float(recent.mean())
                min_val, max_val = float(recent.min()), float(recent.max())
            else:
                pct = float("nan")
                mean_val = min_val = max_val = float("nan")
            pairs.append({
                "a":         a,
                "b":         b,
                "c21":       c21,
                "c63":       c63,
                "c63_pct":   pct,
                "c63_mean":  mean_val,
                "c63_min":   min_val,
                "c63_max":   max_val,
                "series_63d": recent,
            })

    return {
        "corr_21d": corr_21,
        "corr_63d": corr_63,
        "pairs":    pairs,
        "n_obs":    len(pivot_dir),
    }


# ─── Matriz Direcional (DELTA_SIST × DELTA_DISC por CATEGORIA) ─────────────
#
# Pergunta respondida por esta camada:
#   "As duas metades do fundo (sistemática + discricionária) estão apontando
#    na MESMA direção nas mesmas classes de ativo?"
#
# Fonte: q_models.RISK_DIRECTION_REPORT — tabela com decomposição por categoria.
# Cada CATEGORIA tem linhas NOME={Net, Gross, individual instruments}.
# Usamos o 'Net' quando disponível, senão somamos os individuais (excluindo Gross).
#
# Thresholds (fixed):
#   · |PCT_PL_TOTAL| ≥ 1% — categoria material (ignora classes irrelevantes)
#   · |delta|/NAV  ≥ 5 bps — cada perna precisa de magnitude mínima pra contar
#
# Output: contagem de categorias onde sist/disc têm mesmo sinal, cada perna
# acima do threshold. ≥3 → condição 5 do "bull market alignment" (Camada 4).

def _compute_magnitude_p60_per_cat(df_hist: pd.DataFrame, pct_threshold: float = 60.0,
                                     lookback: int = 252) -> dict[str, float]:
    """P{pct_threshold} da magnitude combinada (|sist|+|disc| em BRL) por CATEGORIA,
       usando últimos `lookback` dias. Usado pra filtrar categorias com alinhamento
       nominal mas tamanho conjunto historicamente pequeno."""
    if df_hist is None or df_hist.empty:
        return {}
    # For each (VAL_DATE, CATEGORIA), pick Net if present else sum non-Gross
    def _agg_one(sub):
        net = sub[sub["NOME"] == "Net"]
        if not net.empty:
            s = float(net["DELTA_SISTEMATICO"].iloc[0])
            d = float(net["DELTA_DISCRICIONARIO"].iloc[0])
        else:
            ng = sub[sub["NOME"] != "Gross"]
            s = float(ng["DELTA_SISTEMATICO"].sum())
            d = float(ng["DELTA_DISCRICIONARIO"].sum())
        return abs(s) + abs(d)   # combined magnitude in BRL
    mag_by_day = (df_hist.groupby(["VAL_DATE", "CATEGORIA"])
                          .apply(_agg_one, include_groups=False)
                          .reset_index(name="mag"))
    mag_by_day["VAL_DATE"] = pd.to_datetime(mag_by_day["VAL_DATE"])
    # Last `lookback` dates
    last_date = mag_by_day["VAL_DATE"].max()
    cutoff = last_date - pd.Timedelta(days=lookback + 30)  # small buffer
    mag_by_day = mag_by_day[mag_by_day["VAL_DATE"] >= cutoff]
    # Percentil por categoria
    out = {}
    for cat, g in mag_by_day.groupby("CATEGORIA"):
        if len(g) >= 20:  # min obs for stable percentile
            out[cat] = float(g["mag"].quantile(pct_threshold / 100.0))
    return out


def compute_camada_direcional(df: pd.DataFrame, nav: float,
                               min_leg_bps: float = 5.0,
                               min_cat_pct: float = 1.0,
                               mag_p60_by_cat: dict | None = None) -> dict:
    """Agrega por CATEGORIA e conta categorias com DELTA_SIST e DELTA_DISC
       no mesmo sinal (filtros em cada perna + material + P60 histórico).

       Filtros pra contar como same-sign relevante:
         1. sinal(sist) == sinal(disc)           ← critério de alinhamento
         2. |sist_bps|  ≥ min_leg_bps (5)        ← cada perna mínima
         3. |disc_bps|  ≥ min_leg_bps (5)
         4. |pct_pl|    ≥ min_cat_pct (1%)       ← categoria material
         5. |sist_brl|+|disc_brl| ≥ P60 histórico da CATEGORIA (se fornecido)
            ← exclui alinhamento nominal com tamanho conjunto historicamente baixo
    """
    if df is None or df.empty or not nav:
        return {"rows": [], "same_sign_count": 0, "same_sign_categorias": [],
                "thresholds": {"min_leg_bps": min_leg_bps, "min_cat_pct": min_cat_pct,
                                "mag_pct": 60.0}}

    mag_p60_by_cat = mag_p60_by_cat or {}
    rows = []
    for cat, sub in df.groupby("CATEGORIA", sort=False):
        net_row = sub[sub["NOME"] == "Net"]
        if not net_row.empty:
            sist_brl = float(net_row["DELTA_SISTEMATICO"].iloc[0])
            disc_brl = float(net_row["DELTA_DISCRICIONARIO"].iloc[0])
            pct_pl   = float(net_row["PCT_PL_TOTAL"].iloc[0])
        else:
            non_gross = sub[sub["NOME"] != "Gross"]
            sist_brl = float(non_gross["DELTA_SISTEMATICO"].sum())
            disc_brl = float(non_gross["DELTA_DISCRICIONARIO"].sum())
            pct_pl   = float(non_gross["PCT_PL_TOTAL"].sum())

        sist_bps = sist_brl * 10000 / nav
        disc_bps = disc_brl * 10000 / nav
        combined_mag_brl = abs(sist_brl) + abs(disc_brl)

        leg_sist_material = abs(sist_bps) >= min_leg_bps
        leg_disc_material = abs(disc_bps) >= min_leg_bps
        if not leg_sist_material and not leg_disc_material:
            state = "dust"
        elif not leg_sist_material:
            state = "only-disc"
        elif not leg_disc_material:
            state = "only-sist"
        elif (sist_bps > 0) == (disc_bps > 0):
            state = "same-sign"
        else:
            state = "opposite"

        material = abs(pct_pl) * 100 >= min_cat_pct

        # New filter: P60 historical magnitude. If no historical data for the
        # category, mag_passes = True (don't penalize missing history).
        p60 = mag_p60_by_cat.get(cat)
        mag_passes = True if p60 is None else (combined_mag_brl >= p60)

        rows.append(dict(
            categoria=cat,
            tipo=sub["TIPO"].iloc[0],
            sist_brl=sist_brl, disc_brl=disc_brl,
            sist_bps=sist_bps, disc_bps=disc_bps,
            pct_pl=pct_pl, material=material, state=state,
            combined_mag_brl=combined_mag_brl,
            p60_threshold_brl=p60,
            mag_passes=mag_passes,
        ))

    same_sign_cats = [r["categoria"] for r in rows
                      if r["state"] == "same-sign" and r["material"] and r["mag_passes"]]
    # Reconhecidas-mas-filtradas (pra auditar o novo filtro)
    same_sign_nominal_only = [r["categoria"] for r in rows
                              if r["state"] == "same-sign" and r["material"]
                              and not r["mag_passes"]]
    state_order = {"same-sign": 0, "opposite": 1, "only-sist": 2, "only-disc": 3, "dust": 4}
    rows.sort(key=lambda r: (0 if r["material"] else 1,
                              state_order.get(r["state"], 9),
                              -abs(r["pct_pl"])))

    return {
        "rows": rows,
        "same_sign_count": len(same_sign_cats),
        "same_sign_categorias": same_sign_cats,
        "same_sign_nominal_only": same_sign_nominal_only,
        "thresholds": {"min_leg_bps": min_leg_bps, "min_cat_pct": min_cat_pct,
                        "mag_pct": 60.0},
    }


# ─── Camada 4 — Bull Market Alignment (alerta combinado 5 condições) ──────
#
# Spec na SKILL.md — dispara quando ≥3 condições acendem simultaneamente:
#   1. ≥3 dos 4 buckets direcionais {MACRO, SIST, FRONTIER_EVO, CREDITO} ≥ P70
#   2. ≥1 desses buckets ≥ P95
#   3. Diversification Ratio (Camada 2, winsorizado) ≥ P80
#   4. ≥1 par correlação 63d ≥ P85, RESPEITANDO filtro de significância
#      (ambas estratégias do par ≥ P70 na Camada 1)
#   5. Matriz direcional: ≥3 categorias com DELTA_SIST e DELTA_DISC mesmo sinal
#
# FRONTIER e EVO_STRAT são unidas num bucket só (pacote "tático direcional"):
# somamos as VaR series em bps e recomputamos o percentil 252d.

def _fronti_evo_pct(strat_pivot: pd.DataFrame,
                     effective_date: pd.Timestamp) -> float | None:
    """Percentil 252d da série VaR combinada FRONTIER+EVO_STRAT."""
    cols = [c for c in ("FRONTIER", "EVO_STRAT") if c in strat_pivot.columns]
    if not cols:
        return None
    combined = strat_pivot[cols].fillna(0.0).sum(axis=1)
    window = combined.loc[:effective_date].tail(252).dropna()
    if len(window) < 20:
        return None
    today_val = float(window.iloc[-1])
    return float((window <= today_val).mean() * 100.0)


def compute_camada4(c1_rows: list, d: dict, c3: dict, c_dir: dict,
                     strat_pivot: pd.DataFrame,
                     effective_date: pd.Timestamp) -> dict:
    """Avalia as 5 condições e decide se o alerta dispara (≥3 lit)."""
    c1_pct = {r["strat"]: r["pct"] for r in (c1_rows or [])
              if r.get("pct") is not None and not pd.isna(r.get("pct"))}

    fr_evo_pct = _fronti_evo_pct(strat_pivot, effective_date)
    buckets_pct = {
        "MACRO":        c1_pct.get("MACRO"),
        "SIST":         c1_pct.get("SIST"),
        "FRONTIER+EVO": fr_evo_pct,
        "CREDITO":      c1_pct.get("CREDITO"),
    }
    buckets_pct = {k: v for k, v in buckets_pct.items() if v is not None}

    # Condition 1: ≥3 of 4 buckets ≥ P70
    c1_hot = {k: v for k, v in buckets_pct.items() if v >= 70}
    cond1 = len(c1_hot) >= 3

    # Condition 2: ≥1 of 4 buckets ≥ P95
    c2_hot = {k: v for k, v in buckets_pct.items() if v >= 95}
    cond2 = len(c2_hot) >= 1

    # Condition 3: Diversification Ratio (winsorized) ≥ P80
    ratio_pct = d.get("ratio_wins_pct")
    cond3 = (ratio_pct is not None and not pd.isna(ratio_pct) and ratio_pct >= 80)

    # Condition 4: ≥1 pair corr ≥ P85 AND both C1 ≥ P70 (significance filter)
    pairs = c3.get("pairs") or []
    c4_hot = []
    for p in pairs:
        if pd.isna(p["c63_pct"]) or p["c63_pct"] < 85:
            continue
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        if pa is not None and pa >= 70 and pb is not None and pb >= 70:
            c4_hot.append(p)
    cond4 = len(c4_hot) >= 1

    # Condition 5: directional matrix ≥3 categories same sign
    same_sign_count = c_dir.get("same_sign_count", 0) if c_dir else 0
    cond5 = same_sign_count >= 3

    conditions = [
        dict(id=1, name="≥3 de 4 buckets em ≥ P70",
             lit=cond1, detail=c1_hot),
        dict(id=2, name="≥1 bucket em ≥ P95",
             lit=cond2, detail=c2_hot),
        dict(id=3, name="Ratio C2 (winsorizado) ≥ P80",
             lit=cond3, detail=ratio_pct),
        dict(id=4, name="≥1 par corr 63d ≥ P85 (filtro C1 ≥ P70)",
             lit=cond4, detail=c4_hot),
        dict(id=5, name="Matriz direcional ≥3 categorias mesmo sinal",
             lit=cond5, detail=c_dir.get("same_sign_categorias", []) if c_dir else []),
    ]
    n_lit = sum(1 for c in conditions if c["lit"])

    return dict(
        n_lit=n_lit,
        alert=n_lit >= 3,
        conditions=conditions,
        buckets_pct=buckets_pct,
    )


# ───────────────────── Render ─────────────────────

def _fmt_bps(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:,.1f}"


def _fmt_pct(x: float) -> str:
    return "—" if pd.isna(x) else f"{x:.0f}"


def _ratio_state(pct: float) -> tuple[str, str]:
    """(label, color) por percentil. Spec: <P60 verde, P60-80 amarelo, P80-95 vermelho, >P95 preto."""
    if pd.isna(pct):
        return ("—", "#888")
    if pct < 60:
        return ("diversificação saudável",       "#0e7a32")
    if pct < 80:
        return ("abaixo da média histórica",     "#b88700")
    if pct < 95:
        return ("estratégias alinhadas",         "#a8001a")
    return ("sem diversificação efetiva",        "#000")


def build_spark_svg(series: pd.Series, today_val: float,
                    width: int = 560, height: int = 90) -> str:
    """Sparkline SVG do ratio histórico 252d, com dot destacando hoje."""
    if series.empty:
        return ""
    ys = series.values.astype(float)
    y_min, y_max = float(np.nanmin(ys)), float(np.nanmax(ys))
    if y_max - y_min < 1e-9:
        y_max = y_min + 0.01
    pad = 10
    w = width - 2 * pad
    h = height - 2 * pad

    def xy(i: int, v: float) -> tuple[float, float]:
        x = pad + (i / (len(ys) - 1 or 1)) * w
        y = pad + (1 - (v - y_min) / (y_max - y_min)) * h
        return (x, y)

    pts = " ".join(f"{x:.1f},{y:.1f}" for i, v in enumerate(ys)
                                       for (x, y) in [xy(i, v)])
    dx, dy = xy(len(ys) - 1, today_val)

    # Mean line
    mean_val = float(np.nanmean(ys))
    my = pad + (1 - (mean_val - y_min) / (y_max - y_min)) * h

    return f"""
    <svg viewBox="0 0 {width} {height}" style="width:100%;max-width:{width}px;height:{height}px">
      <line x1="{pad}" y1="{my:.1f}" x2="{width - pad}" y2="{my:.1f}"
            stroke="#bbb" stroke-dasharray="3 3" stroke-width="1"/>
      <polyline points="{pts}" fill="none" stroke="#1f4eaa" stroke-width="1.6"/>
      <circle cx="{dx:.1f}" cy="{dy:.1f}" r="4"
              fill="#1f4eaa" stroke="#fff" stroke-width="2"/>
      <text x="{width - pad}" y="{my - 4:.1f}" text-anchor="end"
            font-size="10" fill="#888">média 252d: {mean_val:.2f}</text>
    </svg>
    """


def render_card(d: dict) -> str:
    """Renderiza o card standalone. Ratio principal = winsorizado (CREDITO clipado)."""
    state_label, state_color = _ratio_state(d["ratio_wins_pct"])

    # Strategy breakdown table — ordered EXPECTED_STRATS, then OUTROS
    rows = []
    for s in EXPECTED_STRATS:
        v = d["strat_today_bps"].get(s)
        rows.append((s, float(v) if v is not None else None))
    if "OUTROS" in d["strat_today_bps"]:
        rows.append(("OUTROS", float(d["strat_today_bps"]["OUTROS"])))

    strat_rows_html = ""
    for s, v in rows:
        vstr = "—" if v is None else f"{v:,.1f}"
        share = "—" if (v is None or d["var_soma_bps"] == 0) else (
            f"{v/d['var_soma_bps']*100:.0f}%"
        )
        # CREDITO row: show winsorized value + clip flag when applicable
        extra = ""
        if s == "CREDITO":
            cwins = d["var_credito_wins_bps"]
            craw  = d["var_credito_bps"]
            if abs(cwins - craw) > 0.01:
                extra = (f" <span style='color:#b88700;font-size:11px'>"
                         f"(winsor. → {cwins:,.1f})</span>")
            else:
                extra = " <span style='color:#888;font-size:11px'>⚠️</span>"
        strat_rows_html += (
            f"<tr><td>{s}{extra}</td>"
            f"<td style='text-align:right;font-family:monospace'>{vstr}</td>"
            f"<td style='text-align:right;color:#888'>{share}</td></tr>"
        )

    # Two totals: raw Σ and winsorized Σ
    strat_rows_html += (
        "<tr style='border-top:1.5px solid #ccc;font-weight:600'>"
        f"<td>Σ VaR estratégias <span style='color:#888;font-weight:400'>(raw)</span></td>"
        f"<td style='text-align:right;font-family:monospace'>"
        f"{d['var_soma_bps']:,.1f}</td>"
        f"<td style='text-align:right;color:#888'>100%</td>"
        "</tr>"
    )
    if abs(d['var_soma_bps'] - d['var_soma_wins_bps']) > 0.01:
        strat_rows_html += (
            "<tr style='font-weight:700'>"
            f"<td>Σ VaR estratégias <span style='color:#0e7a32;font-weight:400'>"
            f"(winsorizado)</span></td>"
            f"<td style='text-align:right;font-family:monospace;color:#0e7a32'>"
            f"{d['var_soma_wins_bps']:,.1f}</td>"
            f"<td style='text-align:right;color:#888'>—</td>"
            "</tr>"
        )

    spark       = build_spark_svg(d["ratio_wins_series"], d["ratio_wins"])
    nav_m       = d["nav"] / 1e6 if d["nav"] else 0.0
    saving_pct  = (1.0 - d["ratio_wins"]) * 100
    cr_share    = d["credito_share_raw"]  * 100 if not pd.isna(d["credito_share_raw"])  else float("nan")
    cr_share_w  = d["credito_share_wins"] * 100 if not pd.isna(d["credito_share_wins"]) else float("nan")
    cr_share_color = ("#a8001a" if cr_share > 40
                      else "#b88700" if cr_share > 25
                      else "#666")
    n_wins = len(d["credito_wins_dates"])
    wins_note = ""
    if n_wins > 0:
        recent_wins = [pd.Timestamp(x).strftime("%Y-%m-%d")
                       for x in d["credito_wins_dates"][-3:]]
        wins_note = (
            f"<div style='margin-top:10px;padding:8px 12px;background:#fff4e0;"
            f"border-left:3px solid #b88700;border-radius:4px;font-size:12px;"
            f"color:#554200;line-height:1.5'>"
            f"⚠️ CREDITO clipado em <b>{n_wins}</b> dias "
            f"na janela 252d (spike de cotas júnior). "
            f"Últimos: {', '.join(recent_wins)}. "
            f"Ratio principal usa Σ winsorizado.</div>"
        )

    return f"""
    <section class="card" style="max-width:760px;margin:24px auto;
                                 padding:20px 24px;border:1px solid #ddd;
                                 border-radius:10px;font-family:Inter,system-ui,
                                 sans-serif;color:#222;background:#fff">
      <div class="card-head" style="margin-bottom:14px">
        <div style="display:flex;justify-content:space-between;align-items:baseline;
                    gap:16px;flex-wrap:wrap">
          <span style="font-size:18px;font-weight:700">
            Camada 2 — Diversification Benefit
          </span>
          <span style="color:#888;font-size:13px">
            {d['date']} · NAV R$ {nav_m:,.0f}M · CREDITO winsorizado (63d MAD, 3σ)
          </span>
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1.2fr 1fr;gap:24px;align-items:start">

        <!-- Strategy breakdown -->
        <div>
          <div style="font-size:12px;color:#888;text-transform:uppercase;
                      letter-spacing:.5px;margin-bottom:6px">
            VaR por estratégia (bps) — soma linear, corr=1
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:14px">
            <thead>
              <tr style="border-bottom:1px solid #ccc;color:#888">
                <th style="text-align:left;padding:6px 4px">Estratégia</th>
                <th style="text-align:right;padding:6px 4px">VaR (bps)</th>
                <th style="text-align:right;padding:6px 4px">% soma</th>
              </tr>
            </thead>
            <tbody>{strat_rows_html}</tbody>
          </table>
        </div>

        <!-- Ratio panel -->
        <div>
          <div style="font-size:12px;color:#888;text-transform:uppercase;
                      letter-spacing:.5px;margin-bottom:6px">
            Ratio = VaR_real / Σ VaR_estratégias
          </div>

          <div style="font-size:44px;font-weight:700;line-height:1;
                      color:{state_color};font-family:'JetBrains Mono',monospace">
            {d['ratio_wins']:.2f}
          </div>
          <div style="color:{state_color};font-size:13px;margin-top:4px;
                      font-weight:600">
            {state_label} · P{_fmt_pct(d['ratio_wins_pct'])}
          </div>
          <div style="color:#888;font-size:11px;margin-top:2px">
            (ratio raw sem winsor.: {d['ratio']:.2f} / P{_fmt_pct(d['ratio_pct'])})
          </div>

          <div style="margin-top:16px;font-size:13px;line-height:1.7">
            <div><b>VaR real (fundo):</b>
              <span style="font-family:monospace">{d['var_real_bps']:,.1f} bps</span></div>
            <div><b>Σ estratégias (winsor.):</b>
              <span style="font-family:monospace">{d['var_soma_wins_bps']:,.1f} bps</span></div>
            <div><b>Benefício:</b>
              <span style="font-family:monospace">
                −{d['var_soma_wins_bps'] - d['var_real_bps']:,.1f} bps
                ({saving_pct:.0f}% de redução)
              </span></div>
            <div style="margin-top:6px"><b>Share CREDITO na Σ:</b>
              <span style="font-family:monospace;color:{cr_share_color};
                         font-weight:600">{cr_share:.0f}%</span>
              <span style="color:#888">(raw)</span>
              &middot; <span style="font-family:monospace;color:#666">
              {cr_share_w:.0f}%</span> <span style="color:#888">(winsor.)</span>
            </div>
            <div style="margin-top:6px"><b>Percentil 252d:</b>
              <span style="font-family:monospace">P{_fmt_pct(d['ratio_wins_pct'])}</span>
              · média {d['ratio_wins_mean']:.2f}
              · range [{d['ratio_wins_min']:.2f}, {d['ratio_wins_max']:.2f}]
            </div>
          </div>
        </div>
      </div>

      {wins_note}

      <!-- Historical sparkline -->
      <div style="margin-top:20px">
        <div style="font-size:12px;color:#888;text-transform:uppercase;
                    letter-spacing:.5px;margin-bottom:4px">
          Ratio histórico (winsorizado) — últimos 252d · traço = média · dot = hoje
        </div>
        {spark}
      </div>

      <div style="margin-top:16px;padding-top:14px;border-top:1px solid #eee;
                  font-size:12px;color:#666;line-height:1.6">
        <b>Leitura:</b> ratio baixo = correlação efetiva entre estratégias é baixa,
        diversificação está reduzindo o VaR linear. Ratio alto → estratégias
        andando juntas. Acima do P80 = alinhamento atípico. <b>Tratamento CREDITO:</b>
        o VaR do CREDITO é <b>winsorizado causalmente</b> (mediana ± 3 × 1.4826·MAD
        em janela 63d, tail superior apenas) para não deixar o spike de cotas júnior
        de dez/2025 distorcer o Σ. Ver <code>docs/CREDITO_TREATMENT.md</code>.
      </div>
    </section>
    """


def _pct_color(pct: float, scheme: str = "high_bad") -> str:
    """Color por percentil. scheme='high_bad' (verde baixo, vermelho alto)."""
    if pd.isna(pct):
        return "#888"
    if scheme == "high_bad":
        if pct < 50:    return "#0e7a32"   # verde
        if pct < 70:    return "#0e7a32"
        if pct < 85:    return "#b88700"   # amarelo
        if pct < 95:    return "#a8001a"   # vermelho
        return "#000"                       # extremo


def render_camada1(rows: list[dict]) -> str:
    """Renderiza tabela da Camada 1."""
    # Count simultaneously elevated (>= P70)
    elevated = [r for r in rows
                if not pd.isna(r["pct"]) and r["pct"] >= 70]

    tr_html = ""
    for r in rows:
        c = _pct_color(r["pct"])
        vstr = "—" if pd.isna(r["var_today"]) else f"{r['var_today']:,.1f}"
        pstr = "—" if pd.isna(r["pct"]) else f"P{r['pct']:.0f}"
        if pd.isna(r["pct"]):
            badge = "<span style='color:#888'>—</span>"
        elif r["pct"] < 70:
            badge = "<span style='color:#0e7a32'>🟢</span>"
        elif r["pct"] < 85:
            badge = "<span style='color:#b88700'>🟡</span>"
        elif r["pct"] < 95:
            badge = "<span style='color:#a8001a'>🔴</span>"
        else:
            badge = "<span style='color:#000'>⚫</span>"
        tr_html += (
            f"<tr>"
            f"<td style='padding:6px 4px'>{r['strat']}</td>"
            f"<td style='text-align:right;font-family:monospace;"
            f"padding:6px 4px'>{vstr}</td>"
            f"<td style='text-align:right;font-family:monospace;"
            f"padding:6px 4px;color:{c};font-weight:600'>{pstr}</td>"
            f"<td style='text-align:center;padding:6px 4px'>{badge}</td>"
            f"</tr>"
        )

    alert = ""
    if len(elevated) >= 3:
        names = ", ".join(r["strat"] for r in elevated)
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:#fff4e0;border-left:3px solid #b88700;"
                 f"border-radius:4px;font-size:13px;color:#554200'>"
                 f"⚠️ {len(elevated)} estratégias simultaneamente ≥ P70 "
                 f"({names}) — sinal de carregamento agregado</div>")

    return f"""
    <section class="card" style="max-width:760px;margin:24px auto;
                                 padding:20px 24px;border:1px solid #ddd;
                                 border-radius:10px;font-family:Inter,system-ui,
                                 sans-serif;color:#222;background:#fff">
      <div class="card-head" style="margin-bottom:14px">
        <span style="font-size:18px;font-weight:700">
          Camada 1 — Utilização histórica por estratégia
        </span>
        <div style="color:#888;font-size:13px;margin-top:4px">
          VaR em bps, percentil 252d próprio · sinal: quantas simultaneamente ≥ P70
        </div>
      </div>

      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <thead>
          <tr style="border-bottom:1px solid #ccc;color:#888">
            <th style="text-align:left;padding:6px 4px">Estratégia</th>
            <th style="text-align:right;padding:6px 4px">VaR (bps)</th>
            <th style="text-align:right;padding:6px 4px">Percentil 252d</th>
            <th style="text-align:center;padding:6px 4px">Estado</th>
          </tr>
        </thead>
        <tbody>{tr_html}</tbody>
      </table>

      {alert}

      <div style="margin-top:14px;padding-top:12px;border-top:1px solid #eee;
                  font-size:12px;color:#666;line-height:1.6">
        <b>Leitura:</b> cada estratégia comparada ao próprio histórico 252d.
        P70 = uma estratégia sozinha carregada pode ser escolha consciente;
        o sinal de concentração é <b>quantas simultaneamente</b> estão ≥ P70.
        3+ simultâneas é onde começa "bull market alignment".
      </div>
    </section>
    """


def render_camada3(c3: dict, c1_rows: list | None = None) -> str:
    """Renderiza matriz 63d + pares com percentil 252d.
       c1_rows: resultado de compute_camada1 — usado para o filtro de significância
       (pair só conta como alinhado se ambas estratégias ≥ P70 na Camada 1)."""
    if c3["corr_63d"] is None or not c3["pairs"]:
        return """
        <section class="card" style="max-width:760px;margin:24px auto;
                                     padding:20px 24px;border:1px solid #ddd;
                                     border-radius:10px;background:#fff;
                                     color:#666;font-family:Inter">
          <b>Camada 3 — Correlação realizada</b>
          <div style="margin-top:6px">Dados insuficientes de PnL por LIVRO.</div>
        </section>
        """

    corr = c3["corr_63d"]
    cols = list(corr.columns)

    # Matrix
    rows_html = ""
    for i, a in enumerate(cols):
        cells = f"<td style='padding:6px 8px;font-weight:600'>{a}</td>"
        for j, b in enumerate(cols):
            if j < i:
                cells += "<td style='padding:6px 8px;color:#ccc'>·</td>"
            elif j == i:
                cells += "<td style='padding:6px 8px;color:#bbb'>—</td>"
            else:
                # Find pair
                pair = next((p for p in c3["pairs"]
                             if p["a"] == a and p["b"] == b), None)
                if pair is None:
                    cells += "<td style='padding:6px 8px'>—</td>"
                else:
                    color = _pct_color(pair["c63_pct"])
                    cells += (
                        f"<td style='padding:6px 8px;text-align:center;"
                        f"font-family:monospace'>"
                        f"<span style='font-weight:600;color:{color}'>"
                        f"{pair['c63']:+.2f}</span>"
                        f"<br><span style='font-size:11px;color:#888'>"
                        f"P{pair['c63_pct']:.0f}</span>"
                        f"</td>"
                    )
        rows_html += f"<tr>{cells}</tr>"

    header_cells = (
        "<th style='padding:6px 8px'></th>"
        + "".join(f"<th style='padding:6px 8px;color:#888'>{c}</th>" for c in cols)
    )

    # Pair detail list (sorted by |c63| desc)
    pairs_sorted = sorted(c3["pairs"], key=lambda p: -abs(p["c63"]))
    pair_rows = ""
    for p in pairs_sorted:
        color = _pct_color(p["c63_pct"])
        pair_rows += (
            f"<tr>"
            f"<td style='padding:5px 4px'>{p['a']} × {p['b']}</td>"
            f"<td style='text-align:right;font-family:monospace;padding:5px 4px'>"
            f"{p['c21']:+.2f}</td>"
            f"<td style='text-align:right;font-family:monospace;padding:5px 4px;"
            f"color:{color};font-weight:600'>{p['c63']:+.2f}</td>"
            f"<td style='text-align:right;font-family:monospace;padding:5px 4px;"
            f"color:{color}'>P{p['c63_pct']:.0f}</td>"
            f"<td style='text-align:right;font-family:monospace;padding:5px 4px;"
            f"color:#888'>{p['c63_mean']:+.2f}</td>"
            f"<td style='text-align:right;font-family:monospace;padding:5px 4px;"
            f"color:#888'>[{p['c63_min']:+.2f}, {p['c63_max']:+.2f}]</td>"
            f"</tr>"
        )

    # Significance filter: pair only counts as aligned if BOTH strategies are
    # simultaneously ≥ P70 in Camada 1 AND the pair's 63d correlation is ≥ P85.
    c1_pct = {r["strat"]: r["pct"] for r in (c1_rows or [])
              if r.get("pct") is not None and not pd.isna(r.get("pct"))}
    flagged_raw = [p for p in c3["pairs"]
                   if not pd.isna(p["c63_pct"]) and p["c63_pct"] >= 85]
    def _both_loaded(p):
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        return (pa is not None and pa >= 70 and pb is not None and pb >= 70)
    flagged_sig   = [p for p in flagged_raw if _both_loaded(p)]
    flagged_quiet = [p for p in flagged_raw if not _both_loaded(p)]

    def _pair_str(p):
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        sa = f"P{pa:.0f}" if pa is not None else "—"
        sb = f"P{pb:.0f}" if pb is not None else "—"
        return f"{p['a']}×{p['b']} (corr P{p['c63_pct']:.0f} · C1 {p['a']} {sa}, {p['b']} {sb})"

    alert = ""
    if flagged_sig:
        names = ", ".join(_pair_str(p) for p in flagged_sig)
        alert += (f"<div style='margin-top:10px;padding:8px 12px;"
                  f"background:#ffe5e5;border-left:3px solid #a8001a;"
                  f"border-radius:4px;font-size:13px;color:#5a0000'>"
                  f"🚨 Alinhamento relevante (corr ≥ P85 · ambas C1 ≥ P70): {names}</div>")
    if flagged_quiet:
        names = ", ".join(_pair_str(p) for p in flagged_quiet)
        alert += (f"<div style='margin-top:8px;padding:8px 12px;"
                  f"background:#fff4e0;border-left:3px solid #b88700;"
                  f"border-radius:4px;font-size:13px;color:#554200'>"
                  f"🟡 Correlação alta mas estratégia(s) abaixo de C1 P70 (sinal desconsiderado): {names}</div>")
    if not flagged_raw:
        alert = ("<div style='margin-top:10px;padding:8px 12px;"
                 "background:#e6f7ec;border-left:3px solid #0e7a32;"
                 "border-radius:4px;font-size:13px;color:#1a5430'>"
                 "✓ Nenhum par em alinhamento atípico (corr 63d &lt; P85).</div>")

    return f"""
    <section class="card" style="max-width:760px;margin:24px auto;
                                 padding:20px 24px;border:1px solid #ddd;
                                 border-radius:10px;font-family:Inter,system-ui,
                                 sans-serif;color:#222;background:#fff">
      <div class="card-head" style="margin-bottom:14px">
        <span style="font-size:18px;font-weight:700">
          Camada 3 — Correlação realizada entre estratégias
        </span>
        <div style="color:#888;font-size:13px;margin-top:4px">
          PnL diário (REPORT_ALPHA_ATRIBUTION) — janela 63d · percentil 252d de rolling 63d
          · {c3['n_obs']} observações · foco em estratégias direcionais
        </div>
      </div>

      <div style="font-size:12px;color:#888;text-transform:uppercase;
                  letter-spacing:.5px;margin-bottom:6px">
        Matriz 63d (percentil 252d abaixo)
      </div>
      <table style="width:auto;border-collapse:collapse;font-size:14px;
                    margin-bottom:18px">
        <thead><tr style="border-bottom:1px solid #ccc">{header_cells}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>

      <div style="font-size:12px;color:#888;text-transform:uppercase;
                  letter-spacing:.5px;margin-bottom:6px">
        Pares — corr 21d · corr 63d · percentil 252d · média · range
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="border-bottom:1px solid #ccc;color:#888">
            <th style="text-align:left;padding:5px 4px">Par</th>
            <th style="text-align:right;padding:5px 4px">21d</th>
            <th style="text-align:right;padding:5px 4px">63d</th>
            <th style="text-align:right;padding:5px 4px">P252d</th>
            <th style="text-align:right;padding:5px 4px">média</th>
            <th style="text-align:right;padding:5px 4px">range</th>
          </tr>
        </thead>
        <tbody>{pair_rows}</tbody>
      </table>

      {alert}

      <div style="margin-top:14px;padding-top:12px;border-top:1px solid #eee;
                  font-size:12px;color:#666;line-height:1.6">
        <b>Leitura:</b> correlação positiva entre estratégias direcionais reduz
        diversificação. P &gt; 85 = alinhamento atípico na janela recente.
        21d é sensor rápido (mudanças recentes); 63d é o baseline de médio prazo
        que dispara o alerta se estiver no extremo alto do histórico próprio.
      </div>
    </section>
    """


def render_html(d: dict, c1_rows: list[dict], c3: dict) -> str:
    card1 = render_camada1(c1_rows)
    card2 = render_card(d)
    card3 = render_camada3(c3, c1_rows)
    return f"""<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>Evolution Risk Concentration — {d['date']}</title>
  <style>
    body {{
      margin:0;padding:32px 16px;background:#f5f7fa;
      font-family:Inter,system-ui,sans-serif;color:#222;
    }}
    h1 {{
      max-width:760px;margin:0 auto 8px;font-size:20px;font-weight:600;
    }}
    .sub {{
      max-width:760px;margin:0 auto 24px;font-size:13px;color:#888;
    }}
  </style>
</head>
<body>
  <h1>Evolution Risk Concentration — preview standalone</h1>
  <div class="sub">
    3 camadas (skill evolution-risk-concentration) em template isolado —
    sem integração com o relatório principal. Revisar antes de decidir onde encaixar.
  </div>
  {card1}
  {card2}
  {card3}
</body>
</html>
"""


# ───────────────────── Main ─────────────────────

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if date_arg is None:
        # Default: use "today" (BRT-aware not needed here — just pick yesterday's WD)
        date_arg = datetime.now().strftime("%Y-%m-%d")

    print(f"Building Evolution risk concentration card for {date_arg}...")
    d = build_ratio_series(date_arg)
    c1_rows = compute_camada1(d["strat_pivot"], d["effective_date"])
    c3 = compute_camada3(date_arg, d["effective_date"])

    out_dir = Path(__file__).parent / "data" / "morning-calls"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"evolution_diversification_{d['date']}.html"
    out_path.write_text(render_html(d, c1_rows, c3), encoding="utf-8")
    print(f"Saved: {out_path}")
    print(f"Effective date: {d['date']}")
    print(f"Camada 1: " + " | ".join(
        f"{r['strat']}={r['var_today']:.1f}bps P{r['pct']:.0f}"
        for r in c1_rows if not pd.isna(r['var_today'])
    ))
    print(f"Camada 2: Ratio wins = {d['ratio_wins']:.3f} "
          f"(P{_fmt_pct(d['ratio_wins_pct'])}) | raw = {d['ratio']:.3f} "
          f"(P{_fmt_pct(d['ratio_pct'])}) | "
          f"CREDITO clipado em {len(d['credito_wins_dates'])} dias")
    print(f"Camada 3: {len(c3['pairs'])} pares, "
          f"{c3['n_obs']} obs de PnL")

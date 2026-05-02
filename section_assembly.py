"""
section_assembly.py — companion to fund_renderers.build_per_fund_risk_monitor_sections.

Builds every (fund, report, html) tuple that is NOT already produced by
the per-fund Risk Monitor card: exposure cards, distribution + vol regime,
single-name L/S, PA, ALBATROZ + BALTRA + QUANT + EVOLUTION exposure,
credit look-through, RF exposure maps, ALBATROZ risk budget, FRONTIER
PA + Long Only, plus the cross-cutting Análise sections.

Returns the section tuples plus the pmovers data payload + modal scaffold,
which build_html injects at the page level.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from risk_runtime import DATA, DATA_STR
from risk_config import (
    FUND_ORDER,
    _MACRO_DESK, _ALBATROZ_DESK, _EVOLUTION_DESK, _BALTRA_DESK,
)
from db_helpers import _latest_nav
from metrics import compute_pm_hs_var
from expo_renderers import (
    build_quant_exposure_section,
    build_evolution_exposure_section,
    build_idka_exposure_section,
    build_rf_exposure_map_section,
    build_albatroz_exposure,
    build_exposure_section,
    build_frontier_exposure_section,
)
from pa_renderers import build_pa_section_hier
from pmovers_renderers import (
    build_pmovers_modal_scaffold,
    build_pmovers_data_payload,
    build_pmovers_trigger,
)
from credit_card_renderers import build_credit_section
from fund_renderers import (
    build_albatroz_risk_budget,
    build_single_names_section,
    build_vol_regime_section,
    build_distribution_card,
    build_stop_section,
    build_pm_budget_vs_var_section,
    build_frontier_lo_section,
    build_analise_sections,
)


def _latest_hs_var_bps(short: str, td_by_short: dict, series_map: dict) -> float | None:
    """Latest fund-level HS VaR (bps) — same source as the Risk Monitor card."""
    td = td_by_short.get(short)
    if td is None:
        return None
    s = series_map.get(td)
    if s is None or s.empty:
        return None
    s_avail = s[s["VAL_DATE"] <= DATA]
    if s_avail.empty:
        return None
    return abs(float(s_avail.iloc[-1]["var_pct"])) * 100.0


def _prev_hs_var_bps(short: str, td_by_short: dict, series_map: dict) -> float | None:
    """D-1 fund-level HS VaR (bps) — for delta vs today."""
    td = td_by_short.get(short)
    if td is None:
        return None
    s = series_map.get(td)
    if s is None or s.empty:
        return None
    s_avail = s[s["VAL_DATE"] <= DATA]
    if len(s_avail) < 2:
        return None
    return abs(float(s_avail.iloc[-2]["var_pct"])) * 100.0


def build_secondary_sections(
    d,
    *,
    td_by_short: dict,
    series_map: dict,
) -> tuple[list, str, str]:
    """Build all sections beyond per-fund Risk Monitor.

    Returns (sections, pmovers_data_script, pmovers_modal_html).
    Section tuples follow the same `(fund_short, report_id, html)` shape used
    by build_per_fund_risk_monitor_sections.
    """
    sections: list = []

    df_today      = d.df_today
    df_expo       = d.df_expo
    df_var        = d.df_var
    macro_aum     = d.macro_aum
    df_expo_d1    = d.df_expo_d1
    df_var_d1     = d.df_var_d1
    df_pnl_prod   = d.df_pnl_prod
    pm_margem     = d.pm_margem
    df_pa         = d.df_pa
    cdi           = d.cdi
    ibov          = d.ibov
    idka_idx_ret  = d.idka_idx_ret
    walb          = d.walb
    rf_expo_maps  = d.rf_expo_maps
    dist_map      = d.dist_map
    dist_map_prev = d.dist_map_prev
    dist_actuals  = d.dist_actuals
    vol_regime_map = d.vol_regime_map
    pm_book_var   = d.pm_book_var
    expo_date_label = d.expo_date_label
    df_quant_sn   = d.df_quant_sn
    quant_nav     = d.quant_nav
    quant_legs    = d.quant_legs
    df_evo_sn     = d.df_evo_sn
    evo_nav       = d.evo_nav
    evo_legs      = d.evo_legs
    df_alb_expo   = d.df_alb_expo
    alb_nav       = d.alb_nav
    df_baltra_expo = d.df_baltra_expo
    baltra_nav    = d.baltra_nav
    df_quant_expo    = d.df_quant_expo
    quant_expo_nav   = d.quant_expo_nav
    df_quant_expo_d1 = d.df_quant_expo_d1
    quant_expo_nav_d1 = d.quant_expo_nav_d1
    df_quant_var     = d.df_quant_var
    df_quant_var_d1  = d.df_quant_var_d1
    df_evo_expo      = d.df_evo_expo
    evo_expo_nav     = d.evo_expo_nav
    df_evo_expo_d1   = d.df_evo_expo_d1
    evo_expo_nav_d1  = d.evo_expo_nav_d1
    df_evo_var       = d.df_evo_var
    df_evo_var_d1    = d.df_evo_var_d1
    df_evo_pnl_prod  = d.df_evo_pnl_prod
    df_frontier      = d.df_frontier
    df_frontier_ibov = d.df_frontier_ibov
    df_frontier_smll = d.df_frontier_smll
    df_frontier_sectors = d.df_frontier_sectors

    def _hs(short: str) -> float | None:
        return _latest_hs_var_bps(short, td_by_short, series_map)

    def _hs_d1(short: str) -> float | None:
        return _prev_hs_var_bps(short, td_by_short, series_map)

    # MACRO-specific sections
    # Risk Budget tab = stop monitor (PnL × carry) + Budget vs VaR card combined
    # into a single section wrapper (avoids duplicate DOM ids for the same tab).
    if d.stop_hist and df_today is not None:
        # Position presence per PM — used to downgrade STOP→FLAT when book is closed.
        # Threshold: >0.05% of NAV in absolute exposure across the PM's books.
        pm_has_position = {}
        if df_expo is not None and not df_expo.empty and macro_aum:
            thr_brl = 0.0005 * macro_aum
            for _pm in ("CI", "LF", "JD", "RJ"):
                _sub = df_expo[df_expo["pm"] == _pm]
                pm_has_position[_pm] = bool(_sub["delta"].abs().sum() > thr_brl) if not _sub.empty else False
        _stop_html = build_stop_section(d.stop_hist, df_today, d.df_pm_book_pnl, pm_has_position)
        _bvv_html  = ""
        # Derive hybrid HS-based VaR per PM from today's dist_map (same source as
        # Distribuição 252d card: PORTIFOLIO_DAILY_HISTORICAL_SIMULATION).
        _pm_hs_var = compute_pm_hs_var(dist_map) if dist_map else {}
        if _pm_hs_var or pm_book_var:
            _bvv_html = build_pm_budget_vs_var_section(
                pm_book_var or {}, _pm_hs_var, pm_margem or {}
            ) or ""
        sections.append(("MACRO", "stop-monitor", _stop_html + _bvv_html))
    if df_expo is not None:
        _expo_html = build_exposure_section(df_expo, df_var, macro_aum, df_expo_d1, df_var_d1, df_pnl_prod, pm_margem,
                                            diversified_var_bps=_hs("MACRO"),
                                            diversified_var_bps_d1=_hs_d1("MACRO"))
        if expo_date_label:
            _stale_banner = (f'<div style="background:#7c2d12;color:#fca5a5;font-size:11px;padding:4px 12px;'
                             f'border-radius:4px;margin-bottom:6px">⚠ Dados de exposição indisponíveis para '
                             f'{DATA_STR} — exibindo {expo_date_label}</div>')
            _expo_html = _stale_banner + _expo_html
        sections.append(("MACRO", "exposure", _expo_html))

    # Distribution 252d sections (per fund) — combined card with Backward/Forward toggle.
    # Engine HS source (PORTIFOLIO_DAILY_HISTORICAL_SIMULATION): MACRO, QUANT, EVOLUTION.
    # FRONTIER: realized α vs IBOV (LONG_ONLY_MAINBOARD).
    # IDKAs: realized active return (fund − benchmark) via fetch_idka_active_series.
    # MACRO_Q sem série HS — parkeado. ALBATROZ via fetch_albatroz_alpha_series (α vs CDI).
    if (dist_map or dist_map_prev) and dist_actuals is not None:
        for fs in ["MACRO", "EVOLUTION", "QUANT", "FRONTIER", "IDKA_3Y", "IDKA_10Y", "ALBATROZ"]:
            html_sect = build_distribution_card(
                fs, dist_map or {}, dist_map_prev or {}, dist_actuals,
            )
            if html_sect:
                sections.append((fs, "distribution", html_sect))

    # Vol Regime sections (per fund) — standalone card using HS W series
    if vol_regime_map:
        for fs in ("MACRO", "EVOLUTION", "QUANT"):
            html_vr = build_vol_regime_section(fs, vol_regime_map)
            if html_vr:
                sections.append((fs, "vol-regime", html_vr))

    # Single-name L/S — inline full-detail section per fund (no modal)
    sn_quant = build_single_names_section(
        "QUANT", "L/S real após decomposição de WIN / BOVA11 / SMAL11",
        df_quant_sn, quant_nav, quant_legs,
    )
    if sn_quant:
        sections.append(("QUANT", "single-name", sn_quant))

    sn_evo = build_single_names_section(
        "EVOLUTION", "look-through BR (QUANT + Evo Strategy + Frontier + Macro)",
        df_evo_sn, evo_nav, evo_legs,
    )
    if sn_evo:
        sections.append(("EVOLUTION", "single-name", sn_evo))

    # Product-level top movers payload (per-fund popup from PA card) — built
    # before the PA loop so each card can render its trigger button.
    pmovers_data_script, _pmovers_funds = build_pmovers_data_payload(df_pa)
    pmovers_modal_html = build_pmovers_modal_scaffold()

    if df_pa is not None and not df_pa.empty:
        cdi_row = cdi or {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
        # Albatroz PA sum per window (for the IDKA bench decomposition)
        alb_pa_rows = df_pa[df_pa["FUNDO"] == "ALBATROZ"]
        albatroz_pa_sum = {
            "dia": float(alb_pa_rows["dia_bps"].sum()) if not alb_pa_rows.empty else 0.0,
            "mtd": float(alb_pa_rows["mtd_bps"].sum()) if not alb_pa_rows.empty else 0.0,
            "ytd": float(alb_pa_rows["ytd_bps"].sum()) if not alb_pa_rows.empty else 0.0,
            "m12": float(alb_pa_rows["m12_bps"].sum()) if not alb_pa_rows.empty else 0.0,
        }
        for short in FUND_ORDER:
            # FRONTIER PA is rendered via build_frontier_lo_section (Long Only card)
            # — it occupies the "performance" slot for FRONTIER. The GFA rows in
            # df_pa still feed Top Movers / Outliers / Mudanças Materiais.
            if short == "FRONTIER":
                continue
            idx_ret = (idka_idx_ret or {}).get(short)
            w_alb   = (walb or {}).get(short)
            pa_html = build_pa_section_hier(
                short, df_pa, cdi_row,
                idka_index_ret=idx_ret, w_alb=w_alb,
                albatroz_pa_sum=albatroz_pa_sum, ibov=ibov,
                pmovers_trigger=build_pmovers_trigger(short, has_data=(short in _pmovers_funds)),
            )
            if pa_html:
                sections.append((short, "performance", pa_html))

    # ALBATROZ — RF exposure card (under the "exposure" report tab)
    if df_alb_expo is not None and not df_alb_expo.empty and alb_nav:
        alb_html = build_albatroz_exposure(df_alb_expo, alb_nav, fund_label="ALBATROZ")
        if alb_html:
            sections.append(("ALBATROZ", "exposure", alb_html))

    # BALTRA — RF exposure card (same shape as ALBATROZ, CDI rows excluded)
    if df_baltra_expo is not None and not df_baltra_expo.empty and baltra_nav:
        baltra_html = build_albatroz_exposure(df_baltra_expo, baltra_nav, fund_label="BALTRA")
        if baltra_html:
            sections.append(("BALTRA", "exposure", baltra_html))

    # Credit look-through cards (BALTRA + EVOLUTION) for the "credit" tab.
    # Default mode is 'tipo' since rating coverage on cota look-through is
    # patchy; users can flip to Subordinação or Rating via the toggle.
    _ref_dt_for_alc = pd.to_datetime(DATA_STR).date() if DATA_STR else date.today()
    if d.df_baltra_credit is not None and not d.df_baltra_credit.empty and baltra_nav:
        b_credit_html = build_credit_section(
            d.df_baltra_credit, baltra_nav, "BALTRA", _ref_dt_for_alc,
            d.cdi_annual, d.ipca_annual, default_mode="subordinacao",
        )
        if b_credit_html:
            sections.append(("BALTRA", "credit", b_credit_html))
    if d.df_evo_credit is not None and not d.df_evo_credit.empty and evo_nav:
        e_credit_html = build_credit_section(
            d.df_evo_credit, evo_nav, "EVOLUTION", _ref_dt_for_alc,
            d.cdi_annual, d.ipca_annual, default_mode="subordinacao",
        )
        if e_credit_html:
            sections.append(("EVOLUTION", "credit", e_credit_html))

    # QUANT — exposure card (by factor + by livro × factor)
    if df_quant_expo is not None and not df_quant_expo.empty and quant_expo_nav:
        q_expo_html = build_quant_exposure_section(
            df_quant_expo, quant_expo_nav,
            df_d1=df_quant_expo_d1,
            df_var=df_quant_var,
            df_var_d1=df_quant_var_d1,
            diversified_var_bps=_hs("QUANT"),
            diversified_var_bps_d1=_hs_d1("QUANT"),
            nav_d1=quant_expo_nav_d1,
        )
        if q_expo_html:
            sections.append(("QUANT", "exposure", q_expo_html))

    # EVOLUTION — exposure card (Strategy → LIVRO → Instrumento + Por Fator)
    if df_evo_expo is not None and not df_evo_expo.empty and evo_expo_nav:
        e_expo_html = build_evolution_exposure_section(
            df_evo_expo, evo_expo_nav,
            df_d1=df_evo_expo_d1,
            df_var=df_evo_var,
            df_var_d1=df_evo_var_d1,
            df_pnl_prod=df_evo_pnl_prod,
            diversified_var_bps=_hs("EVOLUTION"),
            diversified_var_bps_d1=_hs_d1("EVOLUTION"),
            nav_d1=evo_expo_nav_d1,
        )
        if e_expo_html:
            sections.append(("EVOLUTION", "exposure", e_expo_html))

    # RF Exposure Map (IDKA 3Y, IDKA 10Y, Albatroz, MACRO, EVOLUTION)
    # MACRO/EVOLUTION use bench_dur=0 ("—" label) since they have no fixed-duration mandate.
    _RF_MAP_CFG = {
        "IDKA_3Y":   {"desk": "IDKA IPCA 3Y FIRF",  "bench_dur": 3.0,  "bench_label": "IDKA IPCA 3A"},
        "IDKA_10Y":  {"desk": "IDKA IPCA 10Y FIRF", "bench_dur": 10.0, "bench_label": "IDKA IPCA 10A"},
        "ALBATROZ":  {"desk": _ALBATROZ_DESK,       "bench_dur": 0.0,  "bench_label": "CDI"},
        "BALTRA":    {"desk": _BALTRA_DESK,         "bench_dur": 0.0,  "bench_label": "IPCA+"},
        "MACRO":     {"desk": _MACRO_DESK,          "bench_dur": 0.0,  "bench_label": "—"},
        "EVOLUTION": {"desk": _EVOLUTION_DESK,      "bench_dur": 0.0,  "bench_label": "—"},
    }
    if rf_expo_maps:
        for short_k, cfg_k in _RF_MAP_CFG.items():
            df_k = rf_expo_maps.get(short_k)
            if df_k is None or df_k.empty:
                continue
            nav_k = _latest_nav(cfg_k["desk"], DATA_STR)
            if not nav_k:
                continue
            html_k = build_rf_exposure_map_section(
                short_k, df_k, nav_k, cfg_k["bench_dur"], cfg_k["bench_label"],
            )
            if html_k:
                sections.append((short_k, "exposure-map", html_k))
            # IDKAs also get a position-level exposure table with Bruto/Líquido toggle
            if short_k in ("IDKA_3Y", "IDKA_10Y"):
                idka_html = build_idka_exposure_section(
                    short_k, df_k, nav_k, cfg_k["bench_dur"], cfg_k["bench_label"],
                    date_str=DATA_STR,
                )
                if idka_html:
                    sections.append((short_k, "exposure", idka_html))

    # ALBATROZ — Risk Budget (150 bps/month stop)
    if df_pa is not None and not df_pa.empty:
        rb_html = build_albatroz_risk_budget(df_pa)
        if rb_html:
            sections.append(("ALBATROZ", "stop-monitor", rb_html))

    # FRONTIER — Performance Attribution lives in the PA tab (no separate LO tab);
    # Frontier is not in REPORT_ALPHA_ATRIBUTION, so build_pa_section_hier renders
    # nothing for it — this section fills the "performance" slot for Frontier.
    if df_frontier is not None and not df_frontier.empty:
        # Build the hierarchical PA (from GFA in df_pa) to embed as a sub-tab inside the LO card.
        cdi_row_fr = cdi or {"dia": 0.0, "mtd": 0.0, "ytd": 0.0, "m12": 0.0}
        pa_hier_html = build_pa_section_hier(
            "FRONTIER", df_pa, cdi_row_fr, ibov=ibov,
            pmovers_trigger=build_pmovers_trigger("FRONTIER", has_data=("FRONTIER" in _pmovers_funds)),
        ) if (df_pa is not None and not df_pa.empty) else ""
        frontier_html = build_frontier_lo_section(
            df_frontier, DATA_STR,
            df_sectors=df_frontier_sectors,
            pa_hier_html=pa_hier_html,
        )
        if frontier_html:
            sections.append(("FRONTIER", "performance", frontier_html))
        # Frontier exposure card (active weight vs IBOV/IBOD, By Name/Sector toggle)
        if (df_frontier_ibov is not None and not df_frontier_ibov.empty):
            expo_html = build_frontier_exposure_section(
                df_frontier, df_frontier_ibov, df_frontier_smll, df_frontier_sectors)
            if expo_html:
                sections.append(("FRONTIER", "exposure", expo_html))

    sections += build_analise_sections(d, d.df_pa_daily)

    return sections, pmovers_data_script, pmovers_modal_html

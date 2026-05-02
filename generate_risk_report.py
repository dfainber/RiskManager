"""
generate_risk_report.py
Gera o HTML diário de risco MM com barras de range 12m e sparklines 60d.
Usage: python generate_risk_report.py [YYYY-MM-DD]
"""
from __future__ import annotations
import os
import sys
import json
from dataclasses import dataclass
from pathlib import Path
from datetime import date
from typing import Optional

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import matplotlib
matplotlib.use("Agg")

sys.path.insert(0, str(Path(__file__).parent))
from risk_runtime import DATA_STR, DATA, OUT_DIR, fmt_br_num
from risk_config import (
    FUNDS, RAW_FUNDS, IDKA_FUNDS, RF_BENCH_FUNDS, ALL_FUNDS,
    ALERT_THRESHOLD, UTIL_WARN, UTIL_HARD,
    STOP_BASE, ALBATROZ_STOP_BPS,
    REPORTS, FUND_ORDER, FUND_LABELS,
    _FUND_PA_KEY, _PA_BENCH_LIVROS,
    _PM_LIVRO,
    ALERT_COMMENTS,
    _FUND_PEERS_GROUP,
    _MACRO_DESK, _ALBATROZ_DESK, _EVOLUTION_DESK, _BALTRA_DESK, _FRONTIER_DESK,
)
from svg_renderers import make_sparkline, range_bar_svg, multi_line_chart_svg
from db_helpers import _prev_bday, fetch_all_latest_navs, _latest_nav, _require_nav
from metrics import (
    compute_pm_hs_var,
    compute_frontier_bvar_hs,
    compute_portfolio_vol_regime,
)
from pa_renderers import _pa_render_name, _pa_filter_alpha, build_pa_section_hier
from evo_renderers import build_evolution_diversification_section
from expo_renderers import (
    build_quant_exposure_section,
    build_evolution_exposure_section,
    build_idka_exposure_section,
    build_rf_exposure_map_section,
    build_albatroz_exposure,
    build_exposure_section,
    build_frontier_exposure_section,
)
from vardod_renderers import (
    build_vardod_modal_scaffold,
    build_vardod_data_payload,
    VARDOD_CSS,
    VARDOD_JS,
)
from pmovers_renderers import (
    build_pmovers_modal_scaffold,
    build_pmovers_data_payload,
    build_pmovers_trigger,
    PMOVERS_CSS,
    PMOVERS_JS,
)
from fund_renderers import (
    build_albatroz_risk_budget,
    build_single_names_section,
    build_vol_regime_section,
    build_distribution_card,
    build_stop_history,
    build_stop_section,
    build_pm_budget_vs_var_section,
    build_frontier_lo_section,
    build_data_quality_section,
    build_analise_sections,
    _build_fund_mini_briefing,
    _build_executive_briefing,
    build_per_fund_risk_monitor_sections,
)
from summary_renderers import (
    build_vol_regime_card,
    build_movers_card,
    build_changes_card,
    build_comments_card,
    build_factor_breakdown_card,
    build_top_positions_card,
    build_var_bvar_card,
    build_status_grid,
)
from data_fetch import (
    fetch_pm_pnl_history,
    fetch_pm_book_pnl_history,
    fetch_risk_history,
    fetch_risk_history_raw,
    fetch_risk_history_idka,
    fetch_risk_history_rf_bench,
    fetch_frontier_mainboard,
    fetch_frontier_exposure_data,
    fetch_aum_history,
    fetch_macro_pnl_products,
    fetch_pnl_distribution,
    fetch_pnl_actual_by_cut,
    fetch_macro_exposure,
    fetch_quant_single_names,
    fetch_quant_exposure,
    fetch_quant_var,
    fetch_albatroz_exposure,
    fetch_rf_exposure_map,
    fetch_evolution_direct_single_names,
    fetch_evolution_single_names,
    fetch_evolution_exposure,
    fetch_evolution_var,
    fetch_evolution_pnl_products,
    fetch_macro_pm_book_var,
    fetch_pa_leaves,
    fetch_fund_position_changes,
    fetch_fund_position_changes_by_product,
    fetch_macro_pm_var_history,
    fetch_pa_daily_per_product,
    fetch_idka_index_returns,
    fetch_idka_albatroz_weight,
    fetch_ibov_returns,
    fetch_cdi_returns,
    fetch_usdbrl_returns,
    fetch_di1_3y_rate,
    fetch_book_pnl,
    fetch_peers_data,
    fetch_market_snapshot,
    fetch_var_dod_decomposition,
    fetch_fund_credit_positions,
    _VAR_DOD_DISPATCH,
)
from credit_card_renderers import (
    build_credit_section,
    CREDIT_SECTION_CSS,
    CREDIT_ALOC_JS,
)
from credit.credit_data import fetch_cdi_annual_rate, fetch_ipca_12m, fetch_price_quality_flags
from html_assets import (
    MAIN_CSS, IOS_POLYFILL_JS,
    main_navigation_js, cards_data_js,
    pnl_tab_section_html, peers_tab_section_html,
)

# ── Config (fund mandates, thresholds, stops, display) moved to risk_config.py ─


# ── Report data bundle ───────────────────────────────────────────────────────
@dataclass
class ReportData:
    series_map:       dict
    stop_hist:        Optional[dict]            = None
    # MACRO
    df_today:         Optional[pd.DataFrame]    = None
    df_expo:          Optional[pd.DataFrame]    = None
    df_var:           Optional[pd.DataFrame]    = None
    macro_aum:        Optional[float]           = None
    df_expo_d1:       Optional[pd.DataFrame]    = None
    df_var_d1:        Optional[pd.DataFrame]    = None
    df_pnl_prod:      Optional[pd.DataFrame]    = None
    pm_margem:        Optional[dict]            = None
    # QUANT
    df_quant_sn:      Optional[pd.DataFrame]    = None
    quant_nav:        Optional[float]           = None
    quant_legs:       Optional[dict]            = None
    df_quant_expo:    Optional[pd.DataFrame]    = None
    quant_expo_nav:   Optional[float]           = None
    df_quant_expo_d1: Optional[pd.DataFrame]    = None
    quant_expo_nav_d1: Optional[float]          = None
    df_quant_var:     Optional[pd.DataFrame]    = None
    df_quant_var_d1:  Optional[pd.DataFrame]    = None
    # EVOLUTION
    df_evo_sn:        Optional[pd.DataFrame]    = None
    evo_nav:          Optional[float]           = None
    evo_legs:         Optional[dict]            = None
    df_evo_direct:    Optional[pd.DataFrame]    = None
    df_evo_expo:      Optional[pd.DataFrame]    = None
    evo_expo_nav:     Optional[float]           = None
    df_evo_expo_d1:   Optional[pd.DataFrame]    = None
    evo_expo_nav_d1:  Optional[float]           = None
    df_evo_var:       Optional[pd.DataFrame]    = None
    df_evo_var_d1:    Optional[pd.DataFrame]    = None
    df_evo_pnl_prod:  Optional[pd.DataFrame]    = None
    # ALBATROZ
    df_alb_expo:      Optional[pd.DataFrame]    = None
    alb_nav:          Optional[float]           = None
    df_baltra_expo:   Optional[pd.DataFrame]    = None
    baltra_nav:       Optional[float]           = None
    # Price quality (LOTE_BOOK_OVERVIEW PRICE non-null on D and D-1) — flagged rows only.
    alb_pq_flags:     Optional[pd.DataFrame]    = None
    baltra_pq_flags:  Optional[pd.DataFrame]    = None
    # Credit look-through (BALTRA, EVOLUTION) — see credit_card_renderers.build_credit_section
    df_baltra_credit: Optional[pd.DataFrame]    = None
    df_evo_credit:    Optional[pd.DataFrame]    = None
    cdi_annual:       Optional[float]           = None
    ipca_annual:      Optional[float]           = None
    # FRONTIER
    df_frontier:      Optional[pd.DataFrame]    = None
    frontier_bvar:    Optional[dict]            = None
    frontier_bvar_d1: Optional[dict]            = None
    df_frontier_ibov: Optional[pd.DataFrame]    = None
    df_frontier_smll: Optional[pd.DataFrame]    = None
    df_frontier_sectors: Optional[pd.DataFrame] = None
    # Cross-fund
    df_pa:            Optional[pd.DataFrame]    = None
    cdi:              Optional[pd.Series]       = None
    ibov:             Optional[pd.Series]       = None
    usdbrl:           Optional[dict]            = None
    di1_3y:           Optional[dict]            = None
    macro_pm_var_hist: Optional[pd.DataFrame]   = None
    df_pa_daily:      Optional[pd.DataFrame]    = None
    idka_idx_ret:     Optional[dict]            = None
    walb:             Optional[dict]            = None
    rf_expo_maps:     Optional[dict]            = None
    position_changes: Optional[dict]            = None
    dist_map:         Optional[dict]            = None
    dist_map_prev:    Optional[dict]            = None
    dist_actuals:     Optional[dict]            = None
    vol_regime_map:   Optional[dict]            = None
    pm_book_var:      Optional[dict]            = None
    expo_date_label:  Optional[str]             = None
    data_manifest:    Optional[dict]            = None
    book_pnl:         Optional[dict]            = None
    peers_data:       Optional[dict]            = None
    peers_data_eopm:  Optional[dict]            = None
    market_snap:      Optional[dict]            = None
    df_pm_book_pnl:   Optional[pd.DataFrame]    = None


# ── Fetch data ───────────────────────────────────────────────────────────────
# ── Build series ─────────────────────────────────────────────────────────────
def build_series(df_risk, df_aum, df_risk_raw=None, df_risk_idka=None, df_risk_rf_bench=None):
    result = {}
    for td, cfg in FUNDS.items():
        rsk = df_risk[df_risk["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
        nav = df_aum [df_aum ["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
        rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
        nav["VAL_DATE"] = nav["VAL_DATE"].astype("datetime64[us]")
        # NAV lags VaR by up to a day (admin process). Use the latest NAV at-or-before each risk row.
        merged = pd.merge_asof(
            rsk, nav[["VAL_DATE", "NAV"]], on="VAL_DATE", direction="backward",
        ).dropna(subset=["NAV"])
        merged["var_pct"]   = merged["var_total"]    * -1 / merged["NAV"] * 100
        merged["spec_pct"]  = merged["spec_stress"]  * -1 / merged["NAV"] * 100
        merged["macro_pct"] = merged["macro_stress"] * -1 / merged["NAV"] * 100
        merged["stress_pct"] = merged[f"{cfg['stress_col']}_pct"]
        result[td] = merged.sort_values("VAL_DATE")
    if df_risk_raw is not None and not df_risk_raw.empty:
        for td, cfg in RAW_FUNDS.items():
            rsk = df_risk_raw[df_risk_raw["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
            nav = df_aum[df_aum["TRADING_DESK"] == td].copy().sort_values("VAL_DATE")
            if rsk.empty or nav.empty:
                continue
            rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
            nav["VAL_DATE"] = nav["VAL_DATE"].astype("datetime64[us]")
            merged = pd.merge_asof(
                rsk, nav[["VAL_DATE", "NAV"]], on="VAL_DATE", direction="backward",
            ).dropna(subset=["NAV"])
            if merged.empty:
                continue
            merged["var_pct"]   = merged["var_total"]    * -1 / merged["NAV"] * 100
            merged["spec_pct"]  = merged["spec_stress"]  * -1 / merged["NAV"] * 100
            merged["macro_pct"] = merged["macro_stress"] * -1 / merged["NAV"] * 100
            merged["stress_pct"] = merged[f"{cfg['stress_col']}_pct"]
            result[td] = merged.sort_values("VAL_DATE")
    # IDKA funds — BVaR/VaR already in pct units, no NAV normalization needed.
    if df_risk_idka is not None and not df_risk_idka.empty:
        for td in IDKA_FUNDS:
            rsk = df_risk_idka[df_risk_idka["TRADING_DESK"] == td].copy()
            if rsk.empty:
                continue
            rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
            result[td] = rsk.sort_values("VAL_DATE").reset_index(drop=True)
    # RF bench-relative funds (Nazca, ...) — BVaR computed from realized active
    # returns; abs VaR from LOTE_FUND_STRESS. Already in pct units.
    if df_risk_rf_bench is not None and not df_risk_rf_bench.empty:
        for td in RF_BENCH_FUNDS:
            rsk = df_risk_rf_bench[df_risk_rf_bench["TRADING_DESK"] == td].copy()
            if rsk.empty:
                continue
            rsk["VAL_DATE"] = rsk["VAL_DATE"].astype("datetime64[us]")
            result[td] = rsk.sort_values("VAL_DATE").reset_index(drop=True)
    return result

# ── Sparkline ────────────────────────────────────────────────────────────────
# ── Carry formula ─────────────────────────────────────────────────────────────


# ── MACRO Exposure ───────────────────────────────────────────────────────────
# ── Unified Exposure card (shared between MACRO and QUANT) ──────────────────
# Columns (factor row + product drill-down):
#   Fator | Net %NAV | Net BRL | Gross %NAV | Gross BRL | Δ Expo | σ (bps) | VaR (bps) | Δ VaR
# Default sort: |Net| desc. Headers clickable (↑/↓). Drill children collapse with parent.

# ── RF Exposure Map (IDKAs + Albatroz) ───────────────────────────────────────
# ── EVOLUTION Exposure (look-through, 3-level Strategy → LIVRO → Instr) ────
# ── MACRO · PM Budget vs VaR (parametric + historic) ────────────────────────
#
# Compara, para cada PM do Macro:
#   · Budget mensal base (63 bps, o mesmo valor que inicia todo mês — sem carry)
#   · VaR paramétrico 1d do book (proporcional ao Δ do PM dentro do fator, em bps)
#   · VaR histórico 1d 95% para janelas 21d / 63d / 252d
#        VaR_hist_N = -quantile(pnl_diario_bps[-N:], 5%)
#   · Pior dia observado nos últimos 252d
#
# Objetivo: visualizar a calibração do budget contra múltiplas estimativas
# de risco do próprio PM. Se o VaR for maior que o budget, 1 dia ruim já
# esgota o orçamento do mês — sinal de miscalibração.

# ── Evolution — Diversification Benefit (3 camadas da skill) ────────────────
# Compute functions imported from evolution_diversification_card. Here we render
# with the main report's dark theme (CSS vars --text, --muted, --up, etc.).
# CREDITO VaR is winsorized (63d MAD, 3σ, causal) to absorb the dez/2025
# cotas-júnior spike — see docs/CREDITO_TREATMENT.md.


def _mkt_spark(history: list, up: bool = True, w: int = 80, h: int = 22) -> str:
    vals = [v for v in history if v is not None]
    if len(vals) < 2:
        return ""
    mn, mx = min(vals), max(vals)
    rng = mx - mn or 1e-10
    n = len(vals)
    pts = " ".join(
        f"{i/(n-1)*w:.1f},{h - (v-mn)/rng*(h-4) - 2:.1f}"
        for i, v in enumerate(vals)
    )
    color = "#26a65b" if up else "#e74c3c"
    return (
        f'<svg width="{w}" height="{h}" style="vertical-align:middle;display:block">'
        f'<polyline points="{pts}" fill="none" stroke="{color}"'
        f' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>'
        f'</svg>'
    )


def _build_market_section(snap: dict) -> str:
    if not snap or not snap.get("sections"):
        return ""

    val_date = snap.get("val_date", "")
    by_label = {s["label"]: s for s in snap["sections"]}

    # ── Helpers ───────────────────────────────────────────────────────────────
    _NULL = '<span class="mkt-null">—</span>'

    def _val(row: dict) -> str:
        v, fmt, dec = row.get("value"), row.get("fmt", "price"), row.get("decimals", 2)
        if v is None:
            return _NULL
        return f'{v:,.{dec}f}' if fmt == "index" else f'{v:.{dec}f}'

    def _dir(row: dict, win: str) -> float | None:
        rm = row.get("rate_mode", False)
        if win == "1d":
            return row.get("chg") if rm else row.get("chg_pct")
        if win == "5d":
            return row.get("chg_5d") if rm else row.get("chg_pct_5d")
        return row.get("chg_1m") if rm else row.get("chg_pct_1m")

    def _delta(row: dict, win: str) -> str:
        rm = row.get("rate_mode", False)
        if win == "1d":
            chg, pct = row.get("chg"), row.get("chg_pct")
        elif win == "5d":
            chg, pct = row.get("chg_5d"), row.get("chg_pct_5d")
        else:
            chg, pct = row.get("chg_1m"), row.get("chg_pct_1m")
        if rm:
            if chg is None:
                return _NULL
            c = "#26a65b" if chg >= 0 else "#e74c3c"
            s = "+" if chg >= 0 else ""
            return f'<span style="color:{c}">{s}{chg:.1f} bps</span>'
        else:
            if pct is None:
                return _NULL
            c = "#26a65b" if pct >= 0 else "#e74c3c"
            s = "+" if pct >= 0 else ""
            return f'<span style="color:{c}">{s}{pct*100:.2f}%</span>'

    def _arrow(d: float | None) -> str:
        if d is None:
            return _NULL
        c = "#26a65b" if d >= 0 else "#e74c3c"
        g = "↑" if d >= 0 else "↓"
        return f'<span style="color:{c}">{g}</span>'

    def _spark(row: dict, win: str = "1d") -> str:
        d = _dir(row, win)
        return _mkt_spark(row.get("history", []), up=d >= 0 if d is not None else True)

    def _unit(row: dict) -> str:
        u = row.get("unit", "")
        return f'<span class="mkt-unit">{u}</span>' if u else ""

    # ── Janelas panel row (toggle-driven: pre-renders 1D/5D/1M, JS shows one) ─
    def _row_janelas(row: dict) -> str:
        d1, d5, dm = _delta(row, "1d"), _delta(row, "5d"), _delta(row, "1m")
        a1 = _arrow(_dir(row, "1d"))
        a5 = _arrow(_dir(row, "5d"))
        am = _arrow(_dir(row, "1m"))
        spark = _spark(row)
        return (
            f'<tr>'
            f'<td class="mkt-lbl">{row["label"]}</td>'
            f'<td class="mkt-val">{_val(row)}{_unit(row)}</td>'
            f'<td class="mkt-chg">'
            f'<span class="mkt-dw mkt-d1d">{d1}</span>'
            f'<span class="mkt-dw mkt-d5d" style="display:none">{d5}</span>'
            f'<span class="mkt-dw mkt-d1m" style="display:none">{dm}</span>'
            f'</td>'
            f'<td class="mkt-arr">'
            f'<span class="mkt-dw mkt-d1d">{a1}</span>'
            f'<span class="mkt-dw mkt-d5d" style="display:none">{a5}</span>'
            f'<span class="mkt-dw mkt-d1m" style="display:none">{am}</span>'
            f'</td>'
            f'<td class="mkt-spark">{spark}</td>'
            f'</tr>'
        )

    # ── Full-table row (all 3 windows as fixed columns) ───────────────────────
    def _row_full(row: dict) -> str:
        d1, d5, dm = _delta(row, "1d"), _delta(row, "5d"), _delta(row, "1m")
        a1 = _arrow(_dir(row, "1d"))
        a5 = _arrow(_dir(row, "5d"))
        am = _arrow(_dir(row, "1m"))
        spark = _spark(row)
        return (
            f'<tr>'
            f'<td class="mkt-lbl">{row["label"]}</td>'
            f'<td class="mkt-val">{_val(row)}{_unit(row)}</td>'
            f'<td class="mkt-chg">{d1}</td><td class="mkt-arr">{a1}</td>'
            f'<td class="mkt-chg">{d5}</td><td class="mkt-arr">{a5}</td>'
            f'<td class="mkt-chg">{dm}</td><td class="mkt-arr">{am}</td>'
            f'<td class="mkt-spark">{spark}</td>'
            f'</tr>'
        )

    def _panel(group: dict, row_fn) -> str:
        rows = "".join(row_fn(r) for r in group["rows"])
        return (
            f'<div class="mkt-panel">'
            f'<div class="mkt-panel-hdr">{group["label"]}</div>'
            f'<table class="mkt-tbl" data-no-sort="1"><tbody>{rows}</tbody></table>'
            f'</div>'
        )

    def _full_table(rows: list) -> str:
        body = "".join(_row_full(r) for r in rows)
        return (
            f'<table class="mkt-tbl mkt-full-tbl">'
            f'<thead><tr>'
            f'<th style="text-align:left">Instrumento</th>'
            f'<th>Nível</th>'
            f'<th>1D</th><th></th>'
            f'<th>5D</th><th></th>'
            f'<th>1M</th><th></th>'
            f'<th></th>'
            f'</tr></thead>'
            f'<tbody>{body}</tbody>'
            f'</table>'
        )

    # ── Render sub-tabs ───────────────────────────────────────────────────────
    janelas_sec = by_label.get("Janelas", {})
    moedas_sec  = by_label.get("Moedas", {})
    commod_sec  = by_label.get("Commodities", {})

    jan_panels = "".join(
        _panel(g, _row_janelas) for g in janelas_sec.get("groups", [])
    )
    jan_html = (
        f'<div class="mkt-win-bar">'
        f'<span class="mkt-win-lbl">Janela:</span>'
        f'<button class="mkt-win-btn active" data-win="1d" onclick="selectMktWin(\'1d\')">1D</button>'
        f'<button class="mkt-win-btn" data-win="5d" onclick="selectMktWin(\'5d\')">5D</button>'
        f'<button class="mkt-win-btn" data-win="1m" onclick="selectMktWin(\'1m\')">1M</button>'
        f'</div>'
        f'<div class="mkt-panels-grid">{jan_panels}</div>'
    ) if jan_panels else ""

    moe_html = (
        f'<div style="padding:4px 0">{_full_table(moedas_sec.get("rows", []))}</div>'
    ) if moedas_sec else ""

    com_panels = "".join(
        _panel(g, _row_full) for g in commod_sec.get("groups", [])
    )
    com_html = (
        f'<div class="mkt-panels-grid">{com_panels}</div>'
    ) if com_panels else ""

    # ── Assemble ──────────────────────────────────────────────────────────────
    return f"""<div class="section-wrap" data-view="market">
  <section class="card">
    <div class="card-head" style="flex-wrap:wrap;gap:8px">
      <span class="card-title">Mercado</span>
      <span class="card-sub">— {val_date}</span>
      <div class="mkt-subtabs" style="margin-left:auto">
        <button class="mkt-stab active" data-stab="janelas" onclick="selectMktTab('janelas')">Janelas</button>
        <button class="mkt-stab" data-stab="moedas"  onclick="selectMktTab('moedas')">Moedas</button>
        <button class="mkt-stab" data-stab="commodities" onclick="selectMktTab('commodities')">Commodities</button>
      </div>
    </div>
    <div id="mkt-janelas"     class="mkt-view">{jan_html}</div>
    <div id="mkt-moedas"      class="mkt-view" style="display:none">{moe_html}</div>
    <div id="mkt-commodities" class="mkt-view" style="display:none">{com_html}</div>
  </section>
  <script>
  (function(){{
    if (window._mktInit) return; window._mktInit = true;
    window.selectMktTab = function(name) {{
      document.querySelectorAll('.mkt-stab').forEach(function(b){{
        b.classList.toggle('active', b.dataset.stab === name);
      }});
      document.querySelectorAll('.mkt-view').forEach(function(v){{
        v.style.display = v.id === 'mkt-'+name ? '' : 'none';
      }});
    }};
    window.selectMktWin = function(w) {{
      document.querySelectorAll('.mkt-win-btn').forEach(function(b){{
        b.classList.toggle('active', b.dataset.win === w);
      }});
      ['1d','5d','1m'].forEach(function(ww){{
        document.querySelectorAll('#mkt-janelas .mkt-d'+ww).forEach(function(el){{
          el.style.display = ww === w ? '' : 'none';
        }});
      }});
    }};
  }})();
  </script>
</div>"""


def _wrap_tpl(f: str, r: str, body: str) -> str:
    """Wrap a section body in the lazy-hydration <template> + inner <div> shell.

    Hydration order: <template> nodes are parsed by the browser but kept off the
    live DOM until JS clones them via _hydrateSection(fund, report). This avoids
    paying ~12k cells worth of layout/paint at first load.
    """
    return (
        f'<template class="tpl-section" data-fund="{f}" data-report="{r}">'
        f'<div id="sec-{f}-{r}" class="section-wrap" data-fund="{f}" data-report="{r}">{body}</div>'
        f'</template>'
    )


# ── Factor × fund risk matrix builder ────────────────────────────────────────
# Sources for each factor:
#   Real rates / Nominal rates / IPCA Index → rf_expo_maps (IDKAs + Albatroz)
#   Equity BR (IBOV)                        → Frontier NAV (100% long); QUANT/EVO single-names
#   Equity DM/EM, FX, Commodities (MACRO)   → df_expo
#   Juros Nominais / FX / Commodities (QUANT) → df_quant_expo
def _build_factor_matrix(
    *, rf_expo_maps: dict | None, df_frontier, df_quant_sn, df_evo_direct,
    df_expo, df_quant_expo, macro_aum, td_by_short: dict,
) -> tuple[dict, dict, dict]:
    """Build (factor_matrix, bench_matrix, nav_by_short) from already-fetched
    exposure data. factor_matrix and bench_matrix are factor → {fund_short → BRL}
    dicts; nav_by_short is fund_short → NAV. Convention: DV01 sign (long bond =
    negative) for rate factors; raw notional for IPCA Idx / Equity / FX /
    Commodities. Suppression threshold |v| < 1_000 BRL."""
    factor_matrix: dict[str, dict] = {
        "Juros Reais (IPCA)": {},
        "Juros Reais (IGPM)": {},
        "Juros Nominais":     {},
        "IPCA Idx":           {},
        "IGPM Idx":           {},
        "Equity BR":          {},
        "Equity DM":          {},
        "Equity EM":          {},
        "FX":                 {},
        "Commodities":        {},
    }
    if rf_expo_maps:
        # rf_expo_maps.ano_eq_brl tem sign flip p/ chart (long duration = positivo).
        # factor_matrix usa convenção DV01 (tomado=positivo, dado=negativo).
        # → negar apenas "real" e "nominal" (fatores de taxa); "ipca_idx" é carry,
        # sem flip no rf_expo_maps, não precisa negar.
        _DV01_SIGN_FLIP = {"real": True, "real_igpm": True, "nominal": True,
                           "ipca_idx": False, "igpm_idx": False}
        for short_k, df_k in rf_expo_maps.items():
            if df_k is None or df_k.empty:
                continue
            # EVOLUTION (lookthrough_only=True) tem via='lookthrough' para TUDO
            # (inclui Macro/Quant/Frontier slices que já estão em outras linhas)
            # → pular por inteiro para rate factors (sua exposição de juros é
            # indireta via filhos).
            if short_k == "EVOLUTION":
                continue
            df_direct = df_k[df_k["via"] == "direct"]
            if df_direct.empty:
                continue
            for factor_key, factor_col in [
                ("Juros Reais (IPCA)", "real"),
                ("Juros Reais (IGPM)", "real_igpm"),
                ("Juros Nominais",     "nominal"),
                ("IPCA Idx",           "ipca_idx"),
                ("IGPM Idx",           "igpm_idx"),
            ]:
                v = float(df_direct[df_direct["factor"] == factor_col]["ano_eq_brl"].sum())
                if _DV01_SIGN_FLIP.get(factor_col, False):
                    v = -v
                if abs(v) >= 1_000:
                    factor_matrix[factor_key][short_k] = v
    # Frontier = 100% equity BR long (NAV in R$)
    if df_frontier is not None and not df_frontier.empty:
        fr_tot = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
        if not fr_tot.empty:
            gross_pct = float(fr_tot["% Cash"].iloc[0])
            fr_nav = _latest_nav(_FRONTIER_DESK, DATA_STR) or 0
            if gross_pct and fr_nav:
                factor_matrix["Equity BR"]["FRONTIER"] = gross_pct * fr_nav
    # QUANT + EVOLUTION equity BR — Evolution-direct only (avoid double-count).
    if df_quant_sn is not None and not df_quant_sn.empty:
        q_equity = float(df_quant_sn["net"].sum())
        if abs(q_equity) >= 1_000:
            factor_matrix["Equity BR"]["QUANT"] = q_equity
    if df_evo_direct is not None and not df_evo_direct.empty:
        e_equity = float(df_evo_direct["net"].sum())
        if abs(e_equity) >= 1_000:
            factor_matrix["Equity BR"]["EVOLUTION"] = e_equity

    # MACRO from df_expo (rf column = RV-BZ / RV-DM / RV-EM / FX-* / RF-BZ / COMMODITIES / P-Metals)
    if df_expo is not None and not df_expo.empty and macro_aum:
        rf_to_factor = {
            "RV-BZ":       "Equity BR",
            "RV-DM":       "Equity DM",
            "RV-EM":       "Equity EM",
            "COMMODITIES": "Commodities",
            "P-Metals":    "Commodities",
        }
        for rf_val, factor_key in rf_to_factor.items():
            v = float(df_expo[df_expo["rf"] == rf_val]["delta"].sum())
            if abs(v) >= 1_000:
                factor_matrix[factor_key]["MACRO"] = factor_matrix[factor_key].get("MACRO", 0.0) + v
        fx_delta = float(df_expo[df_expo["rf"].str.startswith("FX-", na=False)]["delta"].sum())
        if abs(fx_delta) >= 1_000:
            factor_matrix["FX"]["MACRO"] = fx_delta
        # MACRO nominal-rate duration: DELTA on RF-BZ / BRL Rate Curve primitive.
        # DELTA in LOTE_PRODUCT_EXPO is already duration-weighted (= POSITION × MOD_DURATION);
        # filtering to BRL Rate Curve avoids double-counting primitives on hybrid rows.
        rf_bz_nom = df_expo[(df_expo["rf"] == "RF-BZ")
                            & (df_expo["PRIMITIVE_CLASS"] == "BRL Rate Curve")]
        if not rf_bz_nom.empty:
            nominal_brl_yr = float(rf_bz_nom["delta"].sum())
            if abs(nominal_brl_yr) >= 1_000:
                factor_matrix["Juros Nominais"]["MACRO"] = nominal_brl_yr

    # QUANT non-equity factors from df_quant_expo. Equity BR populated earlier via single-name.
    if df_quant_expo is not None and not df_quant_expo.empty:
        # Nominal rates: filter to BRL Rate Curve primitive to avoid double-count.
        quant_nominal = df_quant_expo[
            (df_quant_expo["factor"] == "Juros Nominais")
            & (df_quant_expo["PRIMITIVE_CLASS"] == "BRL Rate Curve")
        ]
        if not quant_nominal.empty:
            v = float(quant_nominal["delta"].sum())
            if abs(v) >= 1_000:
                factor_matrix["Juros Nominais"]["QUANT"] = (
                    factor_matrix["Juros Nominais"].get("QUANT", 0.0) + v
                )
        for q_fac in ("FX", "Commodities"):
            sub = df_quant_expo[df_quant_expo["factor"] == q_fac]
            if sub.empty:
                continue
            v = float(sub["delta"].sum())
            if abs(v) >= 1_000:
                factor_matrix[q_fac]["QUANT"] = (
                    factor_matrix[q_fac].get("QUANT", 0.0) + v
                )

    # Benchmark allocations per fund, per factor (for net-of-bench view).
    # Real rates: IDKAs have duration-concentrated real-rate bench (3y × NAV, 10y × NAV).
    # IPCA Idx:   IDKAs bench = 100% NAV of inflation carry.
    # Equity BR:  Frontier bench is IBOV = 100% NAV long equity.
    nav_by_short: dict[str, float] = {}
    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td:
            nav_by_short[short] = _latest_nav(td, DATA_STR) or 0.0
    bench_matrix: dict[str, dict] = {k: {} for k in factor_matrix}
    # Convenção DV01: long bond = negativo (factor_matrix + bench).
    # Só setar bench quando o fundo tem gross data — evita "-100%" fantasma
    # em Líquido se o fetch do gross falhar.
    if nav_by_short.get("IDKA_3Y") and factor_matrix["Juros Reais (IPCA)"].get("IDKA_3Y"):
        bench_matrix["Juros Reais (IPCA)"]["IDKA_3Y"] = -3.0 * nav_by_short["IDKA_3Y"]
    if nav_by_short.get("IDKA_3Y") and factor_matrix["IPCA Idx"].get("IDKA_3Y"):
        bench_matrix["IPCA Idx"]["IDKA_3Y"]           =  1.0 * nav_by_short["IDKA_3Y"]
    if nav_by_short.get("IDKA_10Y") and factor_matrix["Juros Reais (IPCA)"].get("IDKA_10Y"):
        bench_matrix["Juros Reais (IPCA)"]["IDKA_10Y"] = -10.0 * nav_by_short["IDKA_10Y"]
    if nav_by_short.get("IDKA_10Y") and factor_matrix["IPCA Idx"].get("IDKA_10Y"):
        bench_matrix["IPCA Idx"]["IDKA_10Y"]           =   1.0 * nav_by_short["IDKA_10Y"]
    if nav_by_short.get("FRONTIER") and factor_matrix["Equity BR"].get("FRONTIER"):
        bench_matrix["Equity BR"]["FRONTIER"] = 1.0 * nav_by_short["FRONTIER"]

    return factor_matrix, bench_matrix, nav_by_short


# ── PA contribution alerts builder ───────────────────────────────────────────
def _build_pa_alerts_html(alerts: list, df_pa, df_pa_daily) -> str:
    """Combined Análise card: VaR/Stress percentile alerts (top) + PA daily
    contribution cards (bottom) with size/fund sort toggle. Returns "" if
    neither has anything to show."""
    PA_ALERT_MIN_BPS = 5.0   # minimum absolute contribution to show
    _PA_EXCL_LIVROS  = {"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}
    _PA_EXCL_CLASSES = {"Caixa", "Custos"}
    # Drop benchmark-replication livros and IDKA funds entirely —
    # IDKA contributions are driven by the index, not active bets.
    _EXCL_FUNDOS_PA  = {"IDKAIPCAY3", "IDKAIPCAY10"}

    pa_alert_items_size = ""
    pa_alert_items_fund = ""
    if df_pa is not None and not df_pa.empty:
        try:
            _df_alpha = _pa_filter_alpha(df_pa)
            _pa_filt = _df_alpha[
                ~_df_alpha["FUNDO"].isin(_EXCL_FUNDOS_PA) &
                ~_df_alpha["LIVRO"].isin(_PA_EXCL_LIVROS) &
                ~_df_alpha["CLASSE"].isin(_PA_EXCL_CLASSES) &
                (_df_alpha["dia_bps"].abs() >= PA_ALERT_MIN_BPS)
            ].copy()

            _zscore_map = {}
            if df_pa_daily is not None and not df_pa_daily.empty:
                _today = pd.Timestamp(DATA_STR)
                _hist  = df_pa_daily[df_pa_daily["DATE"] < _today]
                _sigma = (_hist.groupby(["FUNDO","LIVRO","PRODUCT"])["dia_bps"]
                               .std().reset_index().rename(columns={"dia_bps":"sigma"}))
                for r in _sigma.itertuples(index=False):
                    _zscore_map[(r.FUNDO, r.LIVRO, r.PRODUCT)] = float(r.sigma) if r.sigma > 0 else None

            max_abs = _pa_filt["dia_bps"].abs().max() if not _pa_filt.empty else 1.0
            _fund_order_idx = {k: i for i, k in enumerate(FUND_ORDER)}

            def _card_html(r):
                bps = float(r.dia_bps); abs_bps = abs(bps)
                # ≥1% alert excludes benchmark-tracking livros (IDKA CI replica)
                _is_bench = r.LIVRO in _PA_BENCH_LIVROS.get(r.FUNDO, set())
                big = (abs_bps >= 100.0) and (not _is_bench)
                color      = "var(--up)" if bps > 0 else "var(--down)"
                bg_color   = "#0f2d1a" if bps > 0 else "#2d0f0f"
                # Red border + thicker line on ≥ 1% hits regardless of direction (size is the risk signal)
                border_col = "#dc2626" if big else ("#22c55e" if bps > 0 else "#f87171")
                border_w   = "2px" if big else "1px"
                livro_disp = _pa_render_name(r.LIVRO)
                fund_disp  = FUND_LABELS.get(r.FUNDO, r.FUNDO)
                sigma = _zscore_map.get((r.FUNDO, r.LIVRO, r.PRODUCT))
                z_txt = f"z = {bps/sigma:+.1f}σ" if sigma else ""
                bar_pct = min(abs_bps / max_abs * 100, 100)
                bar_color = "#22c55e" if bps > 0 else "#f87171"
                flag = '🚨 ' if big else ''
                big_tag = (
                    ' <span style="font-size:10px;color:#dc2626;font-weight:700;'
                    'border:1px solid #dc2626;border-radius:3px;padding:1px 5px;'
                    'margin-left:6px;letter-spacing:0.5px">≥ 1% NAV</span>'
                    if big else ''
                )
                return f"""
                <div style="border:{border_w} solid {border_col};border-radius:6px;padding:12px 16px;
                            background:{bg_color};display:flex;align-items:center;gap:16px">
                  <div style="flex:0 0 auto;text-align:right;min-width:80px">
                    <div style="font-size:28px;font-weight:700;color:{color};
                                font-variant-numeric:tabular-nums;line-height:1">{bps:+.1f}</div>
                    <div style="font-size:10px;color:var(--muted);margin-top:2px">bps</div>
                  </div>
                  <div style="flex:1;min-width:0">
                    <div style="font-size:15px;font-weight:700;color:var(--text);
                                white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{flag}{r.PRODUCT}{big_tag}</div>
                    <div style="font-size:11px;color:var(--muted);margin-top:2px">
                      {fund_disp} · {livro_disp}
                      {f'&nbsp;·&nbsp;<span style="color:var(--muted)">{z_txt}</span>' if z_txt else ''}
                    </div>
                    <div style="margin-top:6px;height:4px;border-radius:2px;background:#1e293b">
                      <div style="width:{bar_pct:.1f}%;height:100%;border-radius:2px;
                                  background:{bar_color};opacity:0.8"></div>
                    </div>
                  </div>
                </div>"""

            # Sort by |size| desc (current behavior)
            _by_size = _pa_filt.sort_values("dia_bps", key=abs, ascending=False)
            for r in _by_size.itertuples(index=False):
                pa_alert_items_size += _card_html(r)

            # Sort by fund (canonical FUND_ORDER), then |size| desc within fund
            _pa_filt["_fund_ord"] = _pa_filt["FUNDO"].map(
                {pa_key: _fund_order_idx.get(short, 99)
                 for short, pa_key in _FUND_PA_KEY.items()}
            ).fillna(99)
            _pa_filt["_abs"] = _pa_filt["dia_bps"].abs()
            _by_fund = _pa_filt.sort_values(["_fund_ord", "_abs"], ascending=[True, False])
            for r in _by_fund.itertuples(index=False):
                pa_alert_items_fund += _card_html(r)
        except Exception as e:
            print(f"  PA alerts failed ({e})")

    pa_alert_items = pa_alert_items_size

    risk_items = ""
    if alerts:
        for fundo, metric, pct, val, util, comment in alerts:
            risk_items += f"""
            <div class="alert-item">
              <div class="alert-header">
                <span class="alert-badge">⚠</span>
                <span class="alert-title">{fundo} — {metric}</span>
                <span class="alert-stats">{val:.2f}% &nbsp;|&nbsp; {pct:.0f}° pct 12m &nbsp;|&nbsp; {util:.0f}% do soft</span>
              </div>
              <div class="alert-body">{comment}</div>
            </div>"""
    if not (risk_items or pa_alert_items):
        return ""
    if pa_alert_items:
        _top_margin = "4px" if risk_items else "0"
        pa_grid = (
            f'<div class="pa-alert-view" data-pa-sort="size" style="margin-top:{_top_margin}">{pa_alert_items_size}</div>'
            f'<div class="pa-alert-view pa-alert-view-hidden" data-pa-sort="fund" style="margin-top:{_top_margin}">{pa_alert_items_fund}</div>'
        )
        pa_header = (
            f'<div style="display:flex;align-items:center;gap:12px;margin-top:{"16px" if risk_items else "0"};margin-bottom:8px">'
            f'<span style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">PA — Contribuições do dia (|contrib| ≥ {PA_ALERT_MIN_BPS:.0f} bps)</span>'
            f'<div class="pa-view-toggle pa-alert-toggle" style="margin-left:auto">'
            f'<button class="pa-tgl active" data-pa-sort="size" onclick="selectPaAlertSort(this,\'size\')">Por Tamanho</button>'
            f'<button class="pa-tgl"        data-pa-sort="fund" onclick="selectPaAlertSort(this,\'fund\')">Por Fundo</button>'
            f'</div></div>'
        )
    else:
        pa_grid = ""
        pa_header = ""
    return f"""
        <div class="alerts-section">
          <div class="alerts-header">Análise{' — Métricas acima do 80° percentil histórico' if risk_items else ''}</div>
          {risk_items}
          {pa_header}
          {pa_grid}
        </div>"""


# ── Status consolidado (cross-fund landing card) builders ───────────────────
def _sum_bp_cell(bps: float) -> str:
    pct = bps / 100.0
    if abs(pct) < 0.005:
        return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
    color = "var(--up)" if bps >= 0 else "var(--down)"
    return f'<td class="mono" style="color:{color}; text-align:right">{pct:+.2f}%</td>'


def _sum_util_cell(util):
    if util is None:
        return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
    color = "var(--up)" if util < UTIL_WARN else "var(--warn)" if util < UTIL_HARD else "var(--down)"
    return f'<td class="mono" style="color:{color}; text-align:right; font-weight:600">{util:.0f}%</td>'


def _sum_var_cell(v):
    if v is None:
        return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
    return f'<td class="mono" style="color:var(--text); text-align:right">{v:.2f}%</td>'


def _sum_dvar_cell(dvar):
    if dvar is None:
        return '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
    dv_bps = dvar * 100  # pp → bps
    if abs(dv_bps) < 0.5:
        return '<td class="mono" style="color:var(--muted); text-align:right">flat</td>'
    color = "var(--down)" if dv_bps > 0 else "var(--up)"  # VaR up = more risk
    sign = "+" if dv_bps > 0 else ""
    return f'<td class="mono" style="color:{color}; text-align:right">{sign}{dv_bps:.0f} bps</td>'


def _build_summary_rows_html(*, td_by_short: dict, df_pa, df_frontier,
                             frontier_bvar: dict | None, series_map: dict,
                             pm_margem: dict | None, stop_hist: dict | None) -> str:
    """Per-fund row HTML for the Status consolidado card (Summary view).
    Columns: status emoji, fund label, dia/mtd/ytd/m12 alpha bps cells, VaR %,
    Util % vs soft, Δ VaR D-1 bps. MACRO/ALBATROZ also surface stop-utilization
    via the worst-PM (MACRO) or absolute MTD vs 150 bps (ALBATROZ); the worst
    of (var_util, stop_util) drives the green/yellow/red dot."""
    rows = ""
    for short in FUND_ORDER:
        td      = td_by_short.get(short)
        pa_key  = _FUND_PA_KEY.get(short)

        if pa_key and df_pa is not None and not df_pa.empty:
            sub = df_pa[df_pa["FUNDO"] == pa_key]
            a_dia = float(sub["dia_bps"].sum()) if not sub.empty else 0.0
            a_mtd = float(sub["mtd_bps"].sum()) if not sub.empty else 0.0
            a_ytd = float(sub["ytd_bps"].sum()) if not sub.empty else 0.0
            a_m12 = float(sub["m12_bps"].sum()) if not sub.empty else 0.0
        elif short == "FRONTIER" and df_frontier is not None and not df_frontier.empty:
            # Frontier has no PA in REPORT_ALPHA_ATRIBUTION. Use excess return vs IBOV
            # (TOTAL_IBVSP_*) to stay apples-to-apples with the other funds' alpha vs CDI.
            # Aggregate from the TOTAL row only (per-stock rows sum ER differently vs benchmark weights).
            tot_row = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
            if not tot_row.empty:
                a_dia = float(tot_row["TOTAL_IBVSP_DAY"].iloc[0])   * 10000
                a_mtd = float(tot_row["TOTAL_IBVSP_MONTH"].iloc[0]) * 10000
                a_ytd = float(tot_row["TOTAL_IBVSP_YEAR"].iloc[0])  * 10000
            else:
                a_dia = a_mtd = a_ytd = 0.0
            a_m12 = 0.0  # no 12M column in mainboard
        else:
            a_dia = a_mtd = a_ytd = a_m12 = 0.0

        var_today = var_util = dvar = None
        if td and td in ALL_FUNDS and series_map and td in series_map:
            s = series_map[td]
            s_avail = s[s["VAL_DATE"] <= DATA]
            if not s_avail.empty:
                var_today = abs(s_avail.iloc[-1]["var_pct"])
                cfg = ALL_FUNDS[td]
                # Informative funds (e.g., Frontier LO) show VaR but without util/limit check
                if not cfg.get("informative"):
                    var_util = var_today / cfg["var_soft"] * 100
                prev = s_avail.iloc[:-1]
                if not prev.empty:
                    dvar = var_today - abs(prev.iloc[-1]["var_pct"])

        # Frontier: replace absolute VaR with 3y HS BVaR vs IBOV (current weights)
        if short == "FRONTIER" and frontier_bvar:
            var_today = float(frontier_bvar["bvar_pct"])
            dvar = None  # no D-1 series for HS BVaR yet

        stop_util = None
        _stop_tip = ""
        if short == "MACRO" and pm_margem and stop_hist:
            pm_utils: dict[str, float] = {}
            cur_mes = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
            for pm, margem in pm_margem.items():
                hist = stop_hist.get(pm)
                if hist is None: continue
                cur_row = hist[hist["mes"] == cur_mes]
                if cur_row.empty: continue
                budget = float(cur_row["budget_abs"].iloc[0])
                if budget <= 0: continue
                consumed = budget - margem
                pm_utils[pm] = max(consumed, 0) / budget * 100
            if pm_utils:
                stop_util  = max(pm_utils.values())
                _worst_pm  = max(pm_utils, key=pm_utils.get)
                _stop_tip  = f"Stop: {_worst_pm} {pm_utils[_worst_pm]:.0f}% consumido"
        elif short == "ALBATROZ":
            stop_util = (abs(a_mtd) / ALBATROZ_STOP_BPS * 100) if a_mtd < 0 else 0.0
            if stop_util:
                _stop_tip = f"Stop budget: {stop_util:.0f}% consumido"

        worst = max(x for x in (var_util, stop_util, 0) if x is not None)
        if worst >= 100:   status = "🔴"
        elif worst >= 70:  status = "🟡"
        else:              status = "🟢"

        _title_attr = f' title="{_stop_tip}"' if _stop_tip else ""
        rows += (
            f'<tr onclick="selectFund(\'{short}\')" style="cursor:pointer">'
            f'<td class="sum-status"{_title_attr}>{status}</td>'
            f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
            + _sum_bp_cell(a_dia) + _sum_bp_cell(a_mtd) + _sum_bp_cell(a_ytd) + _sum_bp_cell(a_m12)
            + _sum_var_cell(var_today) + _sum_util_cell(var_util)
            + _sum_dvar_cell(dvar)
            + "</tr>"
        )
    return rows


def _build_bench_rows_html(ibov, cdi, idka_idx_ret: dict | None) -> str:
    """Pinned bench rows (IBOV, CDI, IDKA 3A, IDKA 10A) for Status consolidado."""
    def _row(label: str, returns: dict | None) -> str:
        # data-pinned="1" → sortTableByCol keeps this row at the bottom of tbody.
        if not returns:
            empty = '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
            return (
                '<tr class="bench-row" data-pinned="1">'
                '<td class="sum-status"></td>'
                f'<td class="sum-fund" style="font-style:italic; color:var(--muted)">{label}</td>'
                + empty * 4
                + empty * 3
                + '</tr>'
            )
        return (
            '<tr class="bench-row" data-pinned="1">'
            '<td class="sum-status"></td>'
            f'<td class="sum-fund" style="font-style:italic; color:var(--muted)">{label}</td>'
            + _sum_bp_cell(returns["dia"]) + _sum_bp_cell(returns["mtd"])
            + _sum_bp_cell(returns["ytd"]) + _sum_bp_cell(returns["m12"])
            + '<td class="mono" style="color:var(--muted); text-align:right">—</td>' * 3
            + '</tr>'
        )
    idka3  = (idka_idx_ret or {}).get("IDKA_3Y")
    idka10 = (idka_idx_ret or {}).get("IDKA_10Y")
    return _row("IBOV", ibov) + _row("CDI", cdi) + _row("IDKA 3A", idka3) + _row("IDKA 10A", idka10)


# ── Cross-fund top positions builder ─────────────────────────────────────────
def _build_agg_rows(rf_expo_maps: dict | None, df_frontier, df_quant_sn,
                    df_evo_direct, df_expo) -> list[dict]:
    """One row per (fund, factor, product) for the Top Positions card.
    Excludes via_albatroz (already captured under ALBATROZ direct).
    Suppression: |brl| < 1_000 for rate factors, < 10_000 for equity/commodity."""
    agg_rows: list[dict] = []
    if rf_expo_maps:
        for short_k, df_k in rf_expo_maps.items():
            if df_k is None or df_k.empty:
                continue
            df_direct = df_k[(df_k["via"] == "direct") & (df_k["factor"].isin(["real", "nominal"]))].copy()
            if df_direct.empty:
                continue
            # ano_eq_brl is already signed BRL-years
            for r in df_direct.itertuples(index=False):
                brl = float(r.ano_eq_brl)
                if abs(brl) < 1_000: continue
                agg_rows.append({
                    "fund":    FUND_LABELS.get(short_k, short_k),
                    "factor":  "Juros Reais (IPCA)" if r.factor == "real" else "Juros Nominais",
                    "product": r.PRODUCT,
                    "brl":     brl,
                    "unit":    "BRL-yr",
                })
    # Frontier stocks
    if df_frontier is not None and not df_frontier.empty:
        fr_nav = _latest_nav(_FRONTIER_DESK, DATA_STR) or 0
        stocks = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])]
        stocks = stocks[stocks["% Cash"].notna()]
        for _, r in stocks.iterrows():
            brl = float(r["% Cash"]) * fr_nav
            if abs(brl) < 1_000: continue
            agg_rows.append({
                "fund": "Frontier", "factor": "Equity BR",
                "product": r["PRODUCT"], "brl": brl, "unit": "BRL",
            })
    # QUANT + EVOLUTION single-names (net delta per ticker) — Evolution-direct
    for short_k, df_sn in [("QUANT", df_quant_sn), ("EVOLUTION", df_evo_direct)]:
        if df_sn is None or df_sn.empty:
            continue
        for r in df_sn.itertuples(index=False):
            brl = float(r.net)
            if abs(brl) < 10_000: continue
            agg_rows.append({
                "fund": FUND_LABELS.get(short_k, short_k), "factor": "Equity BR",
                "product": r.ticker, "brl": brl, "unit": "BRL",
            })
    # MACRO equity (RV-DM / RV-EM) and commodities, aggregated by PRODUCT
    if df_expo is not None and not df_expo.empty:
        macro_focus = df_expo[df_expo["rf"].isin(["RV-BZ", "RV-DM", "RV-EM", "COMMODITIES", "P-Metals"])]
        grp = macro_focus.groupby(["rf", "PRODUCT"], as_index=False).agg(delta=("delta", "sum"))
        for r in grp.itertuples(index=False):
            brl = float(r.delta)
            if abs(brl) < 10_000: continue
            factor_label = {"RV-BZ": "Equity BR", "RV-DM": "Equity DM", "RV-EM": "Equity EM",
                            "COMMODITIES": "Commodities", "P-Metals": "Commodities"}[r.rf]
            agg_rows.append({
                "fund": "Macro", "factor": factor_label, "product": r.PRODUCT,
                "brl": brl, "unit": "BRL",
            })
    return agg_rows


# ── House-wide risk consolidated builder ─────────────────────────────────────
# Absolute VaR (% NAV) comes from series_map. BVaR (benchmark-relative) from:
#   IDKAs   → series_map[td]["var_pct"] (engine BVaR; stress_pct holds abs VaR)
#   Frontier→ frontier_bvar["bvar_pct"] (3y HS vs IBOV)
#   Others  → BVaR vs CDI ≈ abs VaR (CDI has effectively zero daily vol)
_BENCH_BY_FUND = {
    "MACRO": "CDI", "QUANT": "CDI", "EVOLUTION": "CDI", "MACRO_Q": "CDI", "ALBATROZ": "CDI",
    "BALTRA": "IPCA+",  # benchmark provisório — fundo prev real rates ~3-4Y duration
    "FRONTIER": "IBOV", "IDKA_3Y": "IDKA 3A", "IDKA_10Y": "IDKA 10A",
}


def _build_house_rows(series_map: dict, td_by_short: dict,
                      frontier_bvar: dict | None,
                      frontier_bvar_d1: dict | None) -> list[dict]:
    """Cross-fund VaR / BVaR snapshot used by the "Risco VaR e BVaR por fundo"
    card and the per-fund mini briefings. One row per fund in FUND_ORDER that
    has a series + NAV. BVaR for non-IDKA, non-FRONTIER funds equals abs VaR
    (CDI bench → near-zero daily vol)."""
    house_rows: list[dict] = []
    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td is None:
            continue
        s = series_map.get(td)
        if s is None or s.empty:
            continue
        s_avail = s[s["VAL_DATE"] <= DATA]
        if s_avail.empty:
            continue
        cfg_ = ALL_FUNDS[td]
        nav_k = _latest_nav(td, DATA_STR)
        if not nav_k:
            continue
        last = s_avail.iloc[-1]
        prev = s_avail.iloc[-2] if len(s_avail) >= 2 else None
        if cfg_.get("primary") == "bvar":          # IDKAs
            abs_var_pct    = abs(float(last.get("stress_pct", 0.0)))
            rel_var_pct    = abs(float(last.get("var_pct",    0.0)))
            abs_var_pct_d1 = abs(float(prev.get("stress_pct", 0.0))) if prev is not None else None
            bvar_pct_d1    = abs(float(prev.get("var_pct",    0.0))) if prev is not None else None
        else:
            abs_var_pct    = abs(float(last.get("var_pct", 0.0)))
            abs_var_pct_d1 = abs(float(prev.get("var_pct", 0.0))) if prev is not None else None
            if short == "FRONTIER" and frontier_bvar:
                rel_var_pct = float(frontier_bvar["bvar_pct"])
                bvar_pct_d1 = float(frontier_bvar_d1["bvar_pct"]) if frontier_bvar_d1 else None
            else:
                rel_var_pct = abs_var_pct
                bvar_pct_d1 = abs_var_pct_d1
        var_brl  = abs_var_pct / 100.0 * nav_k
        bvar_brl = rel_var_pct / 100.0 * nav_k
        house_rows.append({
            "short":       short, "label": FUND_LABELS.get(short, short),
            "bench":       _BENCH_BY_FUND.get(short, "—"),
            "nav":         nav_k,
            "var_pct":     abs_var_pct,  "var_brl":   var_brl,
            "bvar_pct":    rel_var_pct,  "bvar_brl":  bvar_brl,
            "var_pct_d1":  abs_var_pct_d1,
            "bvar_pct_d1": bvar_pct_d1,
        })
    return house_rows


def build_html(d: ReportData) -> str:
    (series_map, stop_hist, df_today, df_expo, df_var, macro_aum,
     df_expo_d1, df_var_d1, df_pnl_prod, pm_margem,
     df_quant_sn, quant_nav, quant_legs,
     df_evo_sn, evo_nav, evo_legs, df_evo_direct,
     df_alb_expo, alb_nav,
     df_baltra_expo, baltra_nav,
     df_frontier, frontier_bvar, frontier_bvar_d1, df_frontier_ibov, df_frontier_smll, df_frontier_sectors,
     df_pa, cdi, ibov, df_pa_daily, idka_idx_ret, walb, rf_expo_maps,
     position_changes, dist_map, dist_map_prev, dist_actuals,
     vol_regime_map, pm_book_var, expo_date_label, data_manifest,
     df_quant_expo, quant_expo_nav, df_quant_expo_d1, quant_expo_nav_d1, df_quant_var, df_quant_var_d1,
     df_evo_expo, evo_expo_nav, df_evo_expo_d1, evo_expo_nav_d1, df_evo_var, df_evo_var_d1,
     df_evo_pnl_prod) = (
        d.series_map, d.stop_hist, d.df_today, d.df_expo, d.df_var, d.macro_aum,
        d.df_expo_d1, d.df_var_d1, d.df_pnl_prod, d.pm_margem,
        d.df_quant_sn, d.quant_nav, d.quant_legs,
        d.df_evo_sn, d.evo_nav, d.evo_legs, d.df_evo_direct,
        d.df_alb_expo, d.alb_nav,
        d.df_baltra_expo, d.baltra_nav,
        d.df_frontier, d.frontier_bvar, d.frontier_bvar_d1, d.df_frontier_ibov, d.df_frontier_smll, d.df_frontier_sectors,
        d.df_pa, d.cdi, d.ibov, d.df_pa_daily, d.idka_idx_ret, d.walb, d.rf_expo_maps,
        d.position_changes, d.dist_map, d.dist_map_prev, d.dist_actuals,
        d.vol_regime_map, d.pm_book_var, d.expo_date_label, d.data_manifest,
        d.df_quant_expo, d.quant_expo_nav, d.df_quant_expo_d1, d.quant_expo_nav_d1, d.df_quant_var, d.df_quant_var_d1,
        d.df_evo_expo, d.evo_expo_nav, d.df_evo_expo_d1, d.evo_expo_nav_d1, d.df_evo_var, d.df_evo_var_d1,
        d.df_evo_pnl_prod)
    market_snap = d.market_snap or {}
    alerts = []
    td_by_short = {cfg["short"]: td for td, cfg in ALL_FUNDS.items()}
    sections = []  # list of (fund_short, report_id, html)
    evolution_c4_state = {}  # Camada 4 state — populated when EVOLUTION diversificação renders


    def _latest_hs_var_bps(short: str) -> float | None:
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

    def _prev_hs_var_bps(short: str) -> float | None:
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


    # Per-fund Risk Monitor sections (extracted to fund_renderers.py)
    _new_secs, _new_alerts, evolution_c4_state = (
        build_per_fund_risk_monitor_sections(d, td_by_short=td_by_short, series_map=series_map)
    )
    sections.extend(_new_secs)
    alerts.extend(_new_alerts)

    alerts_html = _build_pa_alerts_html(alerts, df_pa, df_pa_daily)

    # MACRO-specific sections
    # Risk Budget tab = stop monitor (PnL × carry) + Budget vs VaR card combined
    # into a single section wrapper (avoids duplicate DOM ids for the same tab).
    if stop_hist and df_today is not None:
        # Position presence per PM — used to downgrade STOP→FLAT when book is closed.
        # Threshold: >0.05% of NAV in absolute exposure across the PM's books.
        pm_has_position = {}
        if df_expo is not None and not df_expo.empty and macro_aum:
            thr_brl = 0.0005 * macro_aum
            for _pm in ("CI", "LF", "JD", "RJ"):
                _sub = df_expo[df_expo["pm"] == _pm]
                pm_has_position[_pm] = bool(_sub["delta"].abs().sum() > thr_brl) if not _sub.empty else False
        _stop_html = build_stop_section(stop_hist, df_today, d.df_pm_book_pnl, pm_has_position)
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
                                            diversified_var_bps=_latest_hs_var_bps("MACRO"),
                                            diversified_var_bps_d1=_prev_hs_var_bps("MACRO"))
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
            diversified_var_bps=_latest_hs_var_bps("QUANT"),
            diversified_var_bps_d1=_prev_hs_var_bps("QUANT"),
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
            diversified_var_bps=_latest_hs_var_bps("EVOLUTION"),
            diversified_var_bps_d1=_prev_hs_var_bps("EVOLUTION"),
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

    sections += build_analise_sections(d, df_pa_daily)

    available_pairs = {(f, r) for f, r, _ in sections}
    funds_with_data = sorted({f for f, _ in available_pairs}, key=FUND_ORDER.index)
    reports_with_data = [rid for rid, _ in REPORTS if any(rid == r for _, r in available_pairs)]

    # Sort sections so Per-Fund view shows reports in canonical REPORTS order
    _REPORT_IDX = {rid: i for i, (rid, _) in enumerate(REPORTS)}
    sections.sort(key=lambda x: (
        FUND_ORDER.index(x[0]) if x[0] in FUND_ORDER else 99,
        _REPORT_IDX.get(x[1], 99)
    ))

    # Render all sections inside <template> blocks. Templates' inner content is
    # parsed by the browser but kept off the live DOM until hydrated by JS,
    # which avoids the layout/paint cost of ~12k cells at first load.
    # `_hydrateSection(fund, report)` (in the page JS) clones the template into
    # the #sections-container div on demand the first time the user enters Per-Fundo
    # or Per-Report mode.
    sections_html = "".join(_wrap_tpl(f, r, h) for f, r, h in sections)

    summary_rows_html = _build_summary_rows_html(
        td_by_short=td_by_short, df_pa=df_pa, df_frontier=df_frontier,
        frontier_bvar=frontier_bvar, series_map=series_map,
        pm_margem=pm_margem, stop_hist=stop_hist,
    )
    bench_rows_html = _build_bench_rows_html(ibov, cdi, idka_idx_ret)

    # ── House-wide risk consolidated ───────────────────────────────────────────
    house_rows = _build_house_rows(series_map, td_by_short, frontier_bvar, frontier_bvar_d1)

    # Per-fund mini briefing — registered as "briefing" report (first tab)
    _house_by_short = {r["short"]: r for r in house_rows}

    factor_matrix, bench_matrix, nav_by_short = _build_factor_matrix(
        rf_expo_maps=rf_expo_maps, df_frontier=df_frontier,
        df_quant_sn=df_quant_sn, df_evo_direct=df_evo_direct,
        df_expo=df_expo, df_quant_expo=df_quant_expo,
        macro_aum=macro_aum, td_by_short=td_by_short,
    )

    # Per-fund mini briefings. Rebuild sections_html so briefings come FIRST
    # within each fund's block (DOM order = tab order: briefing → ... → others).
    _briefing_by_short = {}
    for short in FUND_ORDER:
        hr = _house_by_short.get(short)
        if not hr:
            continue
        mini_html = _build_fund_mini_briefing(
            short, hr, series_map, td_by_short, df_pa,
            factor_matrix, bench_matrix,
            pm_margem=pm_margem if short == "MACRO" else None,
            frontier_bvar=frontier_bvar if short == "FRONTIER" else None,
            df_frontier=df_frontier if short == "FRONTIER" else None,
        )
        if mini_html:
            _briefing_by_short[short] = mini_html
            available_pairs.add((short, "briefing"))
    if "briefing" not in reports_with_data:
        reports_with_data = list(reports_with_data) + ["briefing"]

    # Resolve P&L and Peers data early — needed both here and in section HTML
    _book_pnl       = d.book_pnl   or {}
    _peers          = d.peers_data or {}
    _peers_val_date = _peers.get("val_date", "—")
    _has_peers      = bool(_peers)

    # Register per-fund peers sections for funds that have a peers group
    _has_peers = bool(_peers)
    for _f, _pg in _FUND_PEERS_GROUP.items():
        if _pg and _has_peers:
            available_pairs.add((_f, "peers"))
    if any(_FUND_PEERS_GROUP.get(f) for f in FUND_ORDER) and _has_peers:
        if "peers" not in reports_with_data:
            reports_with_data = list(reports_with_data) + ["peers"]

    # Rebuild sections_html: briefing comes LAST per fund (user parked it here
    # while its quality is validated — tab stays accessible but not the default).
    # Peers section appended after briefing (bottom of each fund view).
    #
    # Each (fund, report) block is wrapped via module-level _wrap_tpl().
    _sections_by_fund = {}
    for f, r, h in sections:
        _sections_by_fund.setdefault(f, []).append((r, h))
    _reordered_html = ""
    for f in FUND_ORDER:
        for r, h in _sections_by_fund.get(f, []):
            _reordered_html += _wrap_tpl(f, r, h)
        if f in _briefing_by_short:
            _reordered_html += _wrap_tpl(f, "briefing", _briefing_by_short[f])
        _pg = _FUND_PEERS_GROUP.get(f)
        if _pg and _has_peers:
            _peers_body = (
                f'<section class="card">'
                f'<div class="card-head">'
                f'<span class="card-title">Peers — {FUND_LABELS.get(f, f)}</span>'
                f'<span class="card-sub" data-peers-sub="1">— {_peers_val_date} · grupo {_pg}</span>'
                f'<div class="pa-view-toggle" style="margin-left:auto;gap:6px;display:flex;align-items:center;flex-wrap:wrap">'
                f'<button class="pa-tgl active rpt-peers-anchor" data-anchor="current" onclick="rptSetPeersAnchor(\'current\')">Atual</button>'
                f'<button class="pa-tgl rpt-peers-anchor"        data-anchor="eopm"    onclick="rptSetPeersAnchor(\'eopm\')">Fim Mês Ant.</button>'
                f'<div style="width:1px;height:16px;background:var(--line);margin:0 2px"></div>'
                f'<button class="pa-tgl active" onclick="rptSetPeersMode(\'abs\')">Absoluto</button>'
                f'<button class="pa-tgl"        onclick="rptSetPeersMode(\'alpha\')">Alpha</button>'
                f'<div style="width:1px;height:16px;background:var(--line);margin:0 2px"></div>'
                f'<button class="pa-tgl active rpt-fpeers-vw" data-fview="charts" data-pg="{_pg}" onclick="rptSetFundPeersView(\'{_pg}\',\'charts\')">Gráficos</button>'
                f'<button class="pa-tgl rpt-fpeers-vw"        data-fview="table"  data-pg="{_pg}" onclick="rptSetFundPeersView(\'{_pg}\',\'table\')">Tabela</button>'
                f'</div></div>'
                f'<div class="rpt-peers-fund-strips" data-peers-group="{_pg}" '
                f'style="padding:8px 4px"></div>'
                f'<div class="rpt-peers-fund-tbl-wrap" data-peers-group="{_pg}" '
                f'style="overflow-x:auto;display:none">'
                f'<table class="summary-table rpt-peers-fund-tbl" data-no-sort="1" data-peers-group="{_pg}">'
                f'<tbody></tbody></table>'
                f'</div></section>'
            )
            _reordered_html += _wrap_tpl(f, "peers", _peers_body)
    sections_html = _reordered_html

    agg_rows = _build_agg_rows(rf_expo_maps, df_frontier, df_quant_sn, df_evo_direct, df_expo)

    top_positions_html = build_top_positions_card(agg_rows, bench_matrix)


    by_factor_html = build_factor_breakdown_card(factor_matrix, bench_matrix, nav_by_short)

    house_html = build_var_bvar_card(house_rows)

    fund_grid_html = build_status_grid(summary_rows_html, bench_rows_html)

    vol_regime_html = build_vol_regime_card(vol_regime_map)

    movers_html = build_movers_card(df_pa)

    changes_html = build_changes_card(df_expo, df_expo_d1, position_changes)

    # Δ VaR commentary: pull from VaR DoD attribution (full-suite coverage).
    # Threshold: 5 bps default; IDKAs use 2 bps (BVaR is smaller magnitude).
    # Prefetch all DoD dataframes once — shared with build_vardod_data_payload below.
    _DOD_THRESHOLD = {"IDKA_3Y": 2.0, "IDKA_10Y": 2.0}
    _DOD_THRESHOLD_DEFAULT = 5.0
    _dod_dfs: dict = {}
    for _fk in _VAR_DOD_DISPATCH:
        try:
            _dod_dfs[_fk] = fetch_var_dod_decomposition(_fk, DATA_STR)
        except Exception:
            _dod_dfs[_fk] = None

    def _dod_top_driver(fund_key: str) -> dict | None:
        df = _dod_dfs.get(fund_key)
        if df is None or df.empty:
            return None
        # Same zero-row filter as the modal payload
        mask = (df["contrib_d1_bps"].abs() >= 0.05) | (df["contrib_d_bps"].abs() >= 0.05)
        df = df[mask]
        if df.empty:
            return None
        delta_total = float(df["delta_bps"].sum())
        threshold = _DOD_THRESHOLD.get(fund_key, _DOD_THRESHOLD_DEFAULT)
        if abs(delta_total) < threshold:
            return None
        # Top by |delta| — exclude the bench primitive for IDKAs (passivo, not a
        # gestor decision; it's mechanical 100% NAV tracking with override applied).
        cfg = _VAR_DOD_DISPATCH.get(fund_key)
        bench_primitive = cfg[3] if cfg else None
        df_drivers = df[df["label"] != bench_primitive] if bench_primitive else df
        if df_drivers.empty:
            df_drivers = df  # fallback when only the bench primitive exists
        top_idx = df_drivers["delta_bps"].abs().idxmax()
        top = df_drivers.loc[top_idx]
        # Fund-level pos/marg decomp (sum across rows when available)
        pos_eff_total = None
        marg_eff_total = None
        if df["pos_effect_bps"].notna().any():
            pos_eff_total  = float(df["pos_effect_bps"].fillna(0).sum())
            marg_eff_total = float(df["vol_effect_bps"].fillna(0).sum())
        # Override flag if any row carries a note (engine artifact)
        override = any(str(o or "").strip() for o in df.get("override_note", []))
        return {
            "delta":        delta_total,
            "driver":       str(top.get("label", "—")),
            "driver_delta": float(top["delta_bps"]),
            "pos_eff":      pos_eff_total,
            "marg_eff":     marg_eff_total,
            "override":     override,
        }

    _var_commentary: dict = {}
    for _fk in _VAR_DOD_DISPATCH:
        _r = _dod_top_driver(_fk)
        if _r:
            _var_commentary[_fk] = _r

    comments_html = build_comments_card(df_pa_daily, _var_commentary or None)

    # Data Quality section
    dq_full_html, dq_compact_html = build_data_quality_section(
        data_manifest, series_map, df_pa, df_pa_daily
    )

    # ── Executive briefing (curated top-of-summary card) ──────────────────────
    # Pulls from already-computed structures: house_rows, factor_matrix,
    # bench_matrix, agg_rows, position_changes, vol_regime_map, pm_margem,
    # df_pa, frontier_bvar, ibov, cdi.
    briefing_html = _build_executive_briefing(
        house_rows=house_rows, factor_matrix=factor_matrix,
        bench_matrix=bench_matrix, agg_rows=agg_rows,
        position_changes=position_changes, vol_regime_map=vol_regime_map,
        pm_margem=pm_margem, df_pa=df_pa, frontier_bvar=frontier_bvar,
        series_map=series_map, td_by_short=td_by_short,
        ibov=ibov, cdi=cdi, usdbrl=d.usdbrl, di1_3y=d.di1_3y,
    )

    # Camada 4 — headline no topo do Summary quando o alerta do Evolution está aceso
    # ou parcialmente aceso (≥1 condição). Link direto pro tab "Diversificação" do EVOLUTION.
    evo_c4_headline_html = ""
    if evolution_c4_state:
        n_lit = evolution_c4_state.get("n_lit", 0)
        alert_on = evolution_c4_state.get("alert", False)
        if n_lit >= 1:
            if alert_on:
                bg = "rgba(255,90,106,0.14)"; border = "var(--down)"
                icon = "🚨"; title = "EVOLUTION · Bull Market Alignment — alerta disparado"
            else:
                bg = "rgba(184,135,0,0.10)"; border = "var(--warn)"
                icon = "🟡"; title = "EVOLUTION · Alinhamento parcial de estratégias"
            lit_names = ", ".join(c["name"] for c in evolution_c4_state.get("conditions", []) if c["lit"])
            evo_c4_headline_html = f"""
    <section class="card" style="background:{bg};border-left:4px solid {border};margin-bottom:14px">
      <div style="padding:10px 16px;font-size:13px;color:var(--text)">
        <div style="font-weight:700;margin-bottom:4px">{icon} {title}</div>
        <div style="color:var(--muted);font-size:12px">
          {n_lit} de 5 condições acesas{' (gatilho ≥3)' if not alert_on else ''} · {lit_names}
          · <a href="#" onclick="if(typeof selectFund==='function'){{selectFund('EVOLUTION');setTimeout(function(){{var el=document.querySelector('[data-fund=\\'EVOLUTION\\'][data-report=\\'diversification\\']');if(el)el.scrollIntoView({{behavior:'smooth'}});}},100);}}return false;"
               style="color:var(--accent-blue);text-decoration:underline">ver detalhe →</a>
        </div>
      </div>
    </section>"""

    summary_html = f"""
    <div class="section-wrap" data-view="summary">
      {evo_c4_headline_html}
      {briefing_html}
      {fund_grid_html}
      {house_html}
      {by_factor_html}
      {vol_regime_html}
      {alerts_html}
      {comments_html}
      {movers_html}
      {changes_html}
      {top_positions_html}
      {dq_compact_html}
    </div>"""
    quality_html = f'<div class="section-wrap" data-view="quality">{dq_full_html}</div>'
    # Alerts relocated into Summary view — clear the global section so it doesn't duplicate
    alerts_html = ""

    # ── Market tab ────────────────────────────────────────────────────────────
    market_section_html = _build_market_section(market_snap)

    # ── P&L tab section (house-level) ─────────────────────────────────────────
    _book_pnl_json   = json.dumps(_book_pnl,        separators=(",", ":"), ensure_ascii=False)
    _peers_json      = json.dumps(_peers,           separators=(",", ":"), ensure_ascii=False)
    _peers_eopm      = d.peers_data_eopm or {}
    _peers_eopm_json = json.dumps(_peers_eopm,      separators=(",", ":"), ensure_ascii=False)

    _pnl_baked_date = _book_pnl.get("val_date") or _prev_bday(DATA_STR)
    pnl_section_html = pnl_tab_section_html(_pnl_baked_date=_pnl_baked_date)

    # ── Peers tab section (house-level, group selector) ───────────────────────
    peers_section_html = peers_tab_section_html(_peers_val_date=_peers_val_date)

    mode_tabs_html = (
        '<button class="mode-tab" data-mode="summary" onclick="selectMode(\'summary\')">Summary</button>'
        '<button class="mode-tab" data-mode="fund"    onclick="selectMode(\'fund\')">Por Fundo</button>'
        '<button class="mode-tab" data-mode="report"  onclick="selectMode(\'report\')">Por Report</button>'
        '<button class="mode-tab" data-mode="quality" onclick="selectMode(\'quality\')" style="opacity:0.55;font-size:11px">Qualidade</button>'
        '<button class="mode-tab" data-mode="pnl"     onclick="selectMode(\'pnl\')">P&amp;L</button>'
        '<button class="mode-tab" data-mode="peers"   onclick="selectMode(\'peers\')">Peers</button>'
        '<button class="mode-tab" data-mode="market"  onclick="selectMode(\'market\')">Markets</button>'
    )
    report_subtabs_html = "".join(
        f'<button class="tab" data-target="{rid}" onclick="selectReport(\'{rid}\')">{label}</button>'
        for rid, label in REPORTS if rid in reports_with_data
    )
    fund_subtabs_html = "".join(
        f'<button class="tab" data-target="{s}" onclick="selectFund(\'{s}\')">{s}</button>'
        for s in funds_with_data
    )
    fund_reports_js = json.dumps({
        f: [rid for rid, _ in REPORTS if (f, rid) in available_pairs]
        for f in FUND_ORDER
    })
    report_labels_js = json.dumps({rid: label for rid, label in REPORTS})
    # Include both uppercase shorts (MACRO, QUANT, ...) AND mixed-case labels
    # (Macro, Quantitativo, Frontier, ...) + known sub-strategy tags so they all
    # get highlighted in card-subs.
    _EXTRA_FUND_TERMS = ["Evo Strategy", "Evolution FIC", "Evo"]
    _highlight_terms = (
        list(FUND_ORDER)
      + list(set(FUND_LABELS.values()))
      + _EXTRA_FUND_TERMS
    )
    fund_shorts_js   = json.dumps(_highlight_terms)
    fund_labels_js   = json.dumps(FUND_LABELS)
    fund_order_js    = json.dumps(list(FUND_ORDER))

    # VaR DoD attribution payload (single fetch for all supported funds)
    vardod_data_script, _vardod_funds = build_vardod_data_payload(DATA_STR, prefetched_dfs=_dod_dfs)
    vardod_modal_html = build_vardod_modal_scaffold()

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta http-equiv="X-UA-Compatible" content="IE=edge"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Risk Monitor — {DATA_STR}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>{MAIN_CSS}</style>
{VARDOD_CSS}
{PMOVERS_CSS}
<style>
{CREDIT_SECTION_CSS}
</style>
{main_navigation_js(fund_reports_js=fund_reports_js, report_labels_js=report_labels_js, DATA_STR=DATA_STR, fund_shorts_js=fund_shorts_js, fund_labels_js=fund_labels_js, fund_order_js=fund_order_js)}
</head>
<body>
<!-- iOS banner: visible only on iPhone/iPad (shown by the polyfill script at the end of <body>) -->
<div id="ios-banner" style="display:none; background:#fbbf24; color:#422006;
     border-bottom:1px solid #ca8a04; padding:10px 16px;
     font:13px/1.5 system-ui,-apple-system,sans-serif; text-align:center">
  📱 <b>iPhone/iPad:</b> se os botões não responderem, <b>abra o anexo no Safari</b> (tocar "Abrir em..." → Safari) ou <b>baixe o arquivo e abra direto no browser</b>.
</div>
<noscript>
  <div style="background:#7c2d12;color:#fca5a5;border-bottom:1px solid #a16207;
              padding:10px 16px;font:13px/1.5 system-ui,sans-serif">
    <b>⚠ JavaScript desativado</b> — a navegação por abas e os toggles de cada card
    não vão funcionar. Se baixou este arquivo de um email no Windows e está vendo isto,
    provavelmente o sistema bloqueou os scripts por segurança. Para desbloquear:
    <b>clique com o botão direito no arquivo → Propriedades → marque "Desbloquear" → OK</b>,
    e abra de novo. Alternativamente, abra o arquivo no Chrome ou Edge.
    Todo o conteúdo abaixo continua legível como um relatório linear, mas as abas
    ficam todas expandidas.
  </div>
</noscript>
<header>
  <div class="hwrap">
    <div class="brand"><svg class="rm-logo" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg" aria-label="Risk Monitor"><defs><linearGradient id="rmGrad" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="#2a7ec8"/><stop offset="100%" stop-color="#0a1f4a"/></linearGradient></defs><rect width="100" height="100" rx="18" fill="url(#rmGrad)"/><path d="M 16 74 L 32 58 L 44 64 L 58 44 L 70 52 L 80 30" stroke="#5db6f3" stroke-width="7" stroke-linecap="round" stroke-linejoin="round" fill="none"/><path d="M 80 30 L 68 28 M 80 30 L 80 42" stroke="#5db6f3" stroke-width="7" stroke-linecap="round" fill="none"/></svg><span class="rm-wordmark">Risk Monitor</span></div>
    <nav class="mode-switcher" role="tablist">{mode_tabs_html}</nav>
    <div class="controls">
      <div class="ctrl-group">
        <label>Data</label>
        <input type="date" id="date-picker" value="{DATA_STR}"/>
        <span id="date-hint" class="date-hint mono"></span>
      </div>
      <button class="btn-primary" onclick="goToDate(document.getElementById('date-picker').value)">Ir</button>
      <button class="btn-pdf no-print" onclick="exportPdf('current')" title="Imprimir / salvar como PDF apenas a aba atual">⇣ PDF (aba)</button>
      <button class="btn-pdf no-print" onclick="exportPdf('full')"    title="Expande todas as PA trees e imprime tudo">⇣ PDF (completo)</button>
    </div>
  </div>
  <div class="navrow">
    <nav class="sub-tabs" data-for="fund"   role="tablist">{fund_subtabs_html}</nav>
    <nav class="sub-tabs" data-for="report" role="tablist">{report_subtabs_html}</nav>
  </div>
  <div id="report-jump-bar" role="tablist"></div>
</header>
<div class="subtitle">Data-base: <span class="mono">{pd.to_datetime(DATA_STR).strftime("%d/%m/%Y")}</span> &nbsp;·&nbsp; gerado em <span class="mono">{date.today().strftime("%d/%m/%Y")}</span></div>
<main>
  {summary_html}
  {quality_html}
  {market_section_html}
  {pnl_section_html}
  {peers_section_html}
  <div id="sections-container">
    {sections_html}
  </div>
  <div id="empty-state" style="display:none">Sem dados para essa combinação de fundo × report.</div>
  {alerts_html}
</main>
{cards_data_js(_book_pnl_json=_book_pnl_json, _peers_json=_peers_json, _peers_eopm_json=_peers_eopm_json)}
<script>{IOS_POLYFILL_JS}</script>

{vardod_modal_html}
{vardod_data_script}
{VARDOD_JS}

{pmovers_modal_html}
{pmovers_data_script}
{PMOVERS_JS}

<script>{CREDIT_ALOC_JS}</script>
<footer class="powered-by"><span>Powered by</span><img src="data:image/png;base64,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" alt="Galápagos Capital"></footer>
</body>
</html>"""
    return html

def _fetch_all_data(date_str: str, d1_str: str, d2_str: str):
    """Submit every fetch_* future, resolve with per-task fallbacks, run the
    cheap post-fetch derivations (build_series, stop_hist, df_today, expo
    fallback, pm_margem, frontier_bvar, vol_regime). Returns a SimpleNamespace
    with one attribute per data slot — consumed by _build_report_data.
    """
    import time
    from concurrent.futures import ThreadPoolExecutor
    from types import SimpleNamespace

    t0 = time.time()

    with ThreadPoolExecutor(max_workers=12) as ex:
        fut_risk       = ex.submit(fetch_risk_history)
        fut_risk_raw   = ex.submit(fetch_risk_history_raw)
        fut_risk_idka  = ex.submit(fetch_risk_history_idka)
        fut_risk_rfb   = ex.submit(fetch_risk_history_rf_bench)
        # Price quality (LOTE_BOOK_OVERVIEW PRICE non-null on D and D-1) for funds
        # with direct credit-bearing instruments. ALBATROZ has CRIs/debentures;
        # BALTRA has NTN-Bs in the Prev book.
        fut_alb_pq     = ex.submit(fetch_price_quality_flags, _ALBATROZ_DESK, date_str, d1_str)
        fut_baltra_pq  = ex.submit(fetch_price_quality_flags, _BALTRA_DESK, date_str, d1_str)
        fut_aum        = ex.submit(fetch_aum_history)
        fut_pm_pnl       = ex.submit(fetch_pm_pnl_history)
        fut_pm_book_pnl  = ex.submit(fetch_pm_book_pnl_history)
        fut_pm_book_var  = ex.submit(fetch_macro_pm_book_var,  date_str)
        fut_expo       = ex.submit(fetch_macro_exposure, date_str)
        fut_expo_d1    = ex.submit(fetch_macro_exposure, d1_str)
        fut_expo_d2    = ex.submit(fetch_macro_exposure, d2_str)
        fut_pnl_prod   = ex.submit(fetch_macro_pnl_products, date_str)
        fut_pnl_prod_d1= ex.submit(fetch_macro_pnl_products, d1_str)
        fut_quant_sn   = ex.submit(fetch_quant_single_names, date_str)
        fut_quant_sn_d1= ex.submit(fetch_quant_single_names, d1_str)
        fut_evo_sn     = ex.submit(fetch_evolution_single_names, date_str)
        fut_evo_sn_d1  = ex.submit(fetch_evolution_single_names, d1_str)
        fut_evo_direct = ex.submit(fetch_evolution_direct_single_names, date_str)
        fut_dist       = ex.submit(fetch_pnl_distribution, date_str)
        fut_dist_prev  = ex.submit(fetch_pnl_distribution, d1_str)
        fut_dist_act   = ex.submit(fetch_pnl_actual_by_cut, date_str)
        fut_pa         = ex.submit(fetch_pa_leaves, date_str)
        fut_pa_daily   = ex.submit(fetch_pa_daily_per_product, date_str)
        fut_cdi        = ex.submit(fetch_cdi_returns, date_str)
        fut_ibov       = ex.submit(fetch_ibov_returns, date_str)
        fut_usdbrl     = ex.submit(fetch_usdbrl_returns, date_str)
        fut_di1_3y     = ex.submit(fetch_di1_3y_rate, date_str)
        fut_idka_idx = {
            "IDKA_3Y":  ex.submit(fetch_idka_index_returns, "IDKA_IPCA_3A",  date_str),
            "IDKA_10Y": ex.submit(fetch_idka_index_returns, "IDKA_IPCA_10A", date_str),
        }
        fut_walb = {
            "IDKA_3Y":  ex.submit(fetch_idka_albatroz_weight, "IDKA IPCA 3Y FIRF",  date_str),
            "IDKA_10Y": ex.submit(fetch_idka_albatroz_weight, "IDKA IPCA 10Y FIRF", date_str),
        }
        fut_alb        = ex.submit(fetch_albatroz_exposure, date_str)
        fut_quant_expo    = ex.submit(fetch_quant_exposure, date_str)
        fut_quant_expo_d1 = ex.submit(fetch_quant_exposure, d1_str)
        fut_quant_var     = ex.submit(fetch_quant_var,      date_str)
        fut_quant_var_d1  = ex.submit(fetch_quant_var,      d1_str)
        fut_evo_expo      = ex.submit(fetch_evolution_exposure, date_str)
        fut_evo_expo_d1   = ex.submit(fetch_evolution_exposure, d1_str)
        fut_evo_var       = ex.submit(fetch_evolution_var,      date_str)
        fut_evo_var_d1    = ex.submit(fetch_evolution_var,      d1_str)
        fut_evo_pnl_prod  = ex.submit(fetch_evolution_pnl_products, date_str)
        fut_alb_d1     = ex.submit(fetch_albatroz_exposure, d1_str)
        fut_baltra     = ex.submit(fetch_albatroz_exposure, date_str, _BALTRA_DESK)
        # Credit look-through positions for the new "Crédito" tab
        fut_baltra_cred = ex.submit(fetch_fund_credit_positions, _BALTRA_DESK, date_str)
        fut_evo_cred    = ex.submit(fetch_fund_credit_positions, _EVOLUTION_DESK, date_str)
        fut_rf_expo = {
            "IDKA_3Y":   ex.submit(fetch_rf_exposure_map, "IDKA IPCA 3Y FIRF",  date_str),
            "IDKA_10Y":  ex.submit(fetch_rf_exposure_map, "IDKA IPCA 10Y FIRF", date_str),
            "ALBATROZ":  ex.submit(fetch_rf_exposure_map, _ALBATROZ_DESK, date_str),
            "BALTRA":    ex.submit(fetch_rf_exposure_map, _BALTRA_DESK, date_str),
            "MACRO":     ex.submit(fetch_rf_exposure_map, _MACRO_DESK, date_str),
            "EVOLUTION": ex.submit(fetch_rf_exposure_map, _EVOLUTION_DESK, date_str, True),
        }
        # Pre-warm NAV cache for today + D-1 so every _latest_nav() call hits memory.
        fut_navs    = ex.submit(fetch_all_latest_navs, date_str)
        fut_navs_d1 = ex.submit(fetch_all_latest_navs, d1_str)
        fut_frontier   = ex.submit(fetch_frontier_mainboard, date_str)
        fut_frontier_expo = ex.submit(fetch_frontier_exposure_data)
        fut_chg        = {
            short: ex.submit(fetch_fund_position_changes, short, date_str, d1_str)
            for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER")
        }
        fut_chg_prod   = {
            short: ex.submit(fetch_fund_position_changes_by_product, short, date_str, d1_str)
            for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER")
        }
        fut_macro_pm_hist = ex.submit(fetch_macro_pm_var_history, date_str, 121)
        _pnl_date      = _prev_bday(date_str)
        fut_book_pnl    = ex.submit(fetch_book_pnl,        _pnl_date)
        fut_peers_data       = ex.submit(fetch_peers_data, date_str, "current")
        fut_peers_data_eopm  = ex.submit(fetch_peers_data, date_str, "eopm")
        fut_market_snap = ex.submit(fetch_market_snapshot, date_str)

    # ── Resolve results (sequential, with per-task fallback) ──────────────
    df_risk     = fut_risk.result()
    df_risk_raw = fut_risk_raw.result()
    try:
        df_risk_idka = fut_risk_idka.result()
    except Exception as e:
        print(f"  IDKA risk fetch failed ({e})")
        df_risk_idka = None
    try:
        df_risk_rfb = fut_risk_rfb.result()
    except Exception as e:
        print(f"  RF bench-relative risk fetch failed ({e})")
        df_risk_rfb = None
    try:
        alb_pq_flags = fut_alb_pq.result()
    except Exception as e:
        print(f"  ALBATROZ price quality fetch failed ({e})")
        alb_pq_flags = None
    try:
        baltra_pq_flags = fut_baltra_pq.result()
    except Exception as e:
        print(f"  BALTRA price quality fetch failed ({e})")
        baltra_pq_flags = None
    df_aum      = fut_aum.result()
    df_pm_pnl   = fut_pm_pnl.result()
    try:
        df_pm_book_pnl = fut_pm_book_pnl.result()
    except Exception as e:
        print(f"  PM book-level PnL history failed ({e})")
        df_pm_book_pnl = None
    try:
        pm_book_var = fut_pm_book_var.result()
    except Exception as e:
        print(f"  PM book-report VaR failed ({e})")
        pm_book_var = {}
    series      = build_series(df_risk, df_aum, df_risk_raw, df_risk_idka, df_risk_rfb)
    stop_hist = build_stop_history(df_pm_pnl)

    # PM MTD/YTD from PA leaves — avoids MES column (often NULL in REPORT_ALPHA_ATRIBUTION).
    try:
        df_pa = fut_pa.result()
    except Exception as e:
        print(f"  PA fetch failed ({e})")
        df_pa = None

    if df_pa is not None and not df_pa.empty:
        _macro_pa = df_pa[df_pa["FUNDO"] == "MACRO"]
        df_today = (
            _macro_pa
            .groupby("LIVRO", as_index=False)[["dia_bps", "mtd_bps", "ytd_bps"]]
            .sum()
            .rename(columns={"mtd_bps": "mes_bps"})
        )
    else:
        df_today = pd.DataFrame(columns=["LIVRO", "dia_bps", "mes_bps", "ytd_bps"])

    # Exposure: fall back to D-1 if today's lote hasn't landed yet.
    # Track actual date used so we can label stale sections.
    _expo_raw, _var_raw, _aum_raw = fut_expo.result()
    _expo_d1_raw, _var_d1_raw, _ = fut_expo_d1.result()
    expo_date_label = None  # None = today's data; otherwise the fallback date string
    if _expo_raw.empty and not _expo_d1_raw.empty:
        print(f"  Exposure missing for {date_str} — using {d1_str}")
        expo_date_label = d1_str
        # NAV must come from a real source; the historical `or 1.0` fallback could
        # mis-scale every %NAV / bps in this branch by ~10⁵× if both lookups fail.
        macro_aum = _aum_raw or _require_nav(_MACRO_DESK, d1_str)
        df_expo, df_var = _expo_d1_raw, _var_d1_raw
        # D-1 becomes the new D-0; D-2 becomes the delta reference
        try:
            df_expo_d1, df_var_d1, _ = fut_expo_d2.result()
        except Exception:
            df_expo_d1, df_var_d1 = None, None
    else:
        df_expo, df_var, macro_aum = _expo_raw, _var_raw, _aum_raw
        try:
            df_expo_d1, df_var_d1, _ = _expo_d1_raw, _var_d1_raw, None
        except Exception as e:
            print(f"  D-1 fetch failed ({e}) — Δ columns will be blank")
            df_expo_d1, df_var_d1 = None, None

    # Pnl products: use D-1 if today is empty
    df_pnl_prod = fut_pnl_prod.result()
    if df_pnl_prod.empty:
        df_pnl_prod = fut_pnl_prod_d1.result()

    # PM margem (budget remaining in bps) from stop history
    cur_mes = pd.Timestamp(date_str).to_period("M").to_timestamp()
    pm_margem = {}
    for pm, livro in _PM_LIVRO.items():
        if pm not in stop_hist:
            continue
        hist = stop_hist[pm]
        cur_row = hist[hist["mes"] == cur_mes]
        budget = float(cur_row["budget_abs"].iloc[0]) if not cur_row.empty else STOP_BASE
        pnl_row = df_today[df_today["LIVRO"] == livro]
        pnl_mtd = float(pnl_row["mes_bps"].iloc[0]) if not pnl_row.empty else 0.0
        pm_margem[pm] = budget + pnl_mtd

    # Single-names: fall back to D-1 if today's lote is missing
    try:
        df_quant_sn, quant_nav, quant_legs = fut_quant_sn.result()
        if df_quant_sn is None:
            df_quant_sn, quant_nav, quant_legs = fut_quant_sn_d1.result()
            if df_quant_sn is not None: print(f"  QUANT single-name: using {d1_str}")
    except Exception as e:
        print(f"  QUANT single-name fetch failed ({e})")
        df_quant_sn, quant_nav, quant_legs = None, None, None

    try:
        df_evo_sn, evo_nav, evo_legs = fut_evo_sn.result()
        if df_evo_sn is None:
            df_evo_sn, evo_nav, evo_legs = fut_evo_sn_d1.result()
            if df_evo_sn is not None: print(f"  EVOLUTION single-name: using {d1_str}")
    except Exception as e:
        print(f"  EVOLUTION single-name fetch failed ({e})")
        df_evo_sn, evo_nav, evo_legs = None, None, None

    try:
        dist_map      = fut_dist.result()
        dist_map_prev = fut_dist_prev.result()
        dist_actuals  = fut_dist_act.result()
    except Exception as e:
        print(f"  Distribution fetch failed ({e})")
        dist_map, dist_map_prev, dist_actuals = None, None, None

    try:
        df_pa_daily = fut_pa_daily.result()
    except Exception as e:
        print(f"  PA daily fetch failed ({e})")
        df_pa_daily = None

    try:
        cdi = fut_cdi.result()
    except Exception as e:
        print(f"  CDI fetch failed ({e})")
        cdi = None

    try:
        ibov = fut_ibov.result()
    except Exception as e:
        print(f"  IBOV fetch failed ({e})")
        ibov = None

    try:
        usdbrl = fut_usdbrl.result()
    except Exception as e:
        print(f"  USDBRL fetch failed ({e})")
        usdbrl = None

    try:
        di1_3y = fut_di1_3y.result()
    except Exception as e:
        print(f"  DI1 3y fetch failed ({e})")
        di1_3y = None

    idka_idx_ret = {}
    for k, fut in fut_idka_idx.items():
        try:
            idka_idx_ret[k] = fut.result()
        except Exception as e:
            print(f"  IDKA index returns ({k}) failed ({e})")
            idka_idx_ret[k] = None

    walb = {}
    for k, fut in fut_walb.items():
        try:
            walb[k] = fut.result()
        except Exception as e:
            print(f"  w_alb ({k}) failed ({e})")
            walb[k] = 0.0

    rf_expo_maps = {}
    for short_k, fut_k in fut_rf_expo.items():
        try:
            rf_expo_maps[short_k] = fut_k.result()
        except Exception as e:
            print(f"  RF exposure map ({short_k}) failed ({e})")
            rf_expo_maps[short_k] = None

    try:
        df_alb_expo, alb_nav = fut_alb.result()
        if df_alb_expo is None or df_alb_expo.empty:
            df_alb_expo, alb_nav = fut_alb_d1.result()
            if df_alb_expo is not None and not df_alb_expo.empty:
                print(f"  ALBATROZ exposure: using {d1_str}")
    except Exception as e:
        print(f"  ALBATROZ exposure fetch failed ({e})")
        df_alb_expo, alb_nav = None, None

    try:
        df_baltra_expo, baltra_nav = fut_baltra.result()
    except Exception as e:
        print(f"  BALTRA exposure fetch failed ({e})")
        df_baltra_expo, baltra_nav = None, None
    try:
        df_baltra_credit = fut_baltra_cred.result()
    except Exception as e:
        print(f"  BALTRA credit fetch failed ({e})")
        df_baltra_credit = None
    try:
        df_evo_credit = fut_evo_cred.result()
    except Exception as e:
        print(f"  EVOLUTION credit fetch failed ({e})")
        df_evo_credit = None
    try:
        cdi_annual = fetch_cdi_annual_rate(date_str)
    except Exception:
        cdi_annual = None
    try:
        ipca_annual = fetch_ipca_12m(date_str)
    except Exception:
        ipca_annual = None

    try:
        df_quant_expo, quant_expo_nav = fut_quant_expo.result()
    except Exception as e:
        print(f"  QUANT exposure fetch failed ({e})")
        df_quant_expo, quant_expo_nav = None, None
    try:
        df_quant_expo_d1, quant_expo_nav_d1 = fut_quant_expo_d1.result()
    except Exception as e:
        print(f"  QUANT D-1 exposure fetch failed ({e})")
        df_quant_expo_d1, quant_expo_nav_d1 = None, None
    try:
        df_quant_var = fut_quant_var.result()
    except Exception as e:
        print(f"  QUANT VaR fetch failed ({e})")
        df_quant_var = None
    try:
        df_quant_var_d1 = fut_quant_var_d1.result()
    except Exception as e:
        print(f"  QUANT D-1 VaR fetch failed ({e})")
        df_quant_var_d1 = None

    # EVOLUTION exposure (look-through, 3-level card)
    try:
        df_evo_expo, evo_expo_nav = fut_evo_expo.result()
    except Exception as e:
        print(f"  EVOLUTION exposure fetch failed ({e})")
        df_evo_expo, evo_expo_nav = None, None
    try:
        df_evo_expo_d1, evo_expo_nav_d1 = fut_evo_expo_d1.result()
    except Exception as e:
        print(f"  EVOLUTION D-1 exposure fetch failed ({e})")
        df_evo_expo_d1, evo_expo_nav_d1 = None, None
    try:
        df_evo_var = fut_evo_var.result()
    except Exception as e:
        print(f"  EVOLUTION VaR fetch failed ({e})")
        df_evo_var = None
    try:
        df_evo_var_d1 = fut_evo_var_d1.result()
    except Exception as e:
        print(f"  EVOLUTION D-1 VaR fetch failed ({e})")
        df_evo_var_d1 = None
    try:
        df_evo_pnl_prod = fut_evo_pnl_prod.result()
    except Exception as e:
        print(f"  EVOLUTION PnL per product fetch failed ({e})")
        df_evo_pnl_prod = None

    try:
        df_evo_direct, _evo_direct_nav, _ = fut_evo_direct.result()
    except Exception as e:
        print(f"  EVOLUTION direct fetch failed ({e})")
        df_evo_direct = None

    try:
        df_frontier = fut_frontier.result()
    except Exception as e:
        print(f"  Frontier LO fetch failed ({e})")
        df_frontier = None

    try:
        frontier_bvar    = compute_frontier_bvar_hs(df_frontier, date_str) if df_frontier is not None else None
        frontier_bvar_d1 = compute_frontier_bvar_hs(df_frontier, d1_str)   if df_frontier is not None else None
    except Exception as e:
        print(f"  Frontier BVaR (HS) failed ({e})")
        frontier_bvar    = None
        frontier_bvar_d1 = None

    try:
        df_frontier_ibov, df_frontier_smll, df_frontier_sectors = fut_frontier_expo.result()
    except Exception as e:
        print(f"  Frontier exposure fetch failed ({e})")
        df_frontier_ibov = df_frontier_smll = df_frontier_sectors = pd.DataFrame()

    try:
        vol_regime_map = compute_portfolio_vol_regime(dist_map or {})
    except Exception as e:
        print(f"  Vol regime failed ({e})")
        vol_regime_map = {}

    position_changes = {}
    for short, fut in fut_chg.items():
        try:
            position_changes[short] = fut.result()
        except Exception as e:
            print(f"  {short} position changes failed ({e})")
            position_changes[short] = None

    position_changes_prod = {}
    for short, fut in fut_chg_prod.items():
        try:
            position_changes_prod[short] = fut.result()
        except Exception as e:
            print(f"  {short} position changes (by product) failed ({e})")
            position_changes_prod[short] = None

    try:
        macro_pm_var_hist = fut_macro_pm_hist.result()
    except Exception as e:
        print(f"  MACRO PM VaR history failed ({e})")
        macro_pm_var_hist = None

    # Resolve NAV pre-warms (side effect is populating _NAV_CACHE)
    try:
        fut_navs.result()
        fut_navs_d1.result()
    except Exception as e:
        print(f"  NAV warmup failed ({e})")

    try:
        book_pnl = fut_book_pnl.result()
    except Exception as e:
        print(f"  Book PnL fetch failed ({e})")
        book_pnl = {}
    try:
        peers_data = fut_peers_data.result()
    except Exception as e:
        print(f"  Peers data fetch failed ({e})")
        peers_data = {}
    try:
        peers_data_eopm = fut_peers_data_eopm.result()
    except Exception as e:
        print(f"  Peers data EOPM fetch failed ({e})")
        peers_data_eopm = {}
    try:
        market_snap = fut_market_snap.result()
    except Exception as e:
        print(f"  Market snapshot fetch failed ({e})")
        market_snap = {}

    print(f"  ...fetches done in {time.time()-t0:.1f}s")

    return SimpleNamespace(
        # raw fetched
        series=series, stop_hist=stop_hist,
        df_today=df_today, df_expo=df_expo, df_var=df_var, macro_aum=macro_aum,
        df_expo_d1=df_expo_d1, df_var_d1=df_var_d1,
        df_pnl_prod=df_pnl_prod, pm_margem=pm_margem,
        df_quant_sn=df_quant_sn, quant_nav=quant_nav, quant_legs=quant_legs,
        df_quant_expo=df_quant_expo, quant_expo_nav=quant_expo_nav,
        df_quant_expo_d1=df_quant_expo_d1, quant_expo_nav_d1=quant_expo_nav_d1,
        df_quant_var=df_quant_var, df_quant_var_d1=df_quant_var_d1,
        df_evo_sn=df_evo_sn, evo_nav=evo_nav, evo_legs=evo_legs, df_evo_direct=df_evo_direct,
        df_evo_expo=df_evo_expo, evo_expo_nav=evo_expo_nav,
        df_evo_expo_d1=df_evo_expo_d1, evo_expo_nav_d1=evo_expo_nav_d1,
        df_evo_var=df_evo_var, df_evo_var_d1=df_evo_var_d1, df_evo_pnl_prod=df_evo_pnl_prod,
        df_alb_expo=df_alb_expo, alb_nav=alb_nav,
        df_baltra_expo=df_baltra_expo, baltra_nav=baltra_nav,
        alb_pq_flags=alb_pq_flags, baltra_pq_flags=baltra_pq_flags,
        df_baltra_credit=df_baltra_credit, df_evo_credit=df_evo_credit,
        cdi_annual=cdi_annual, ipca_annual=ipca_annual,
        df_frontier=df_frontier, frontier_bvar=frontier_bvar, frontier_bvar_d1=frontier_bvar_d1,
        df_frontier_ibov=df_frontier_ibov, df_frontier_smll=df_frontier_smll,
        df_frontier_sectors=df_frontier_sectors,
        df_pa=df_pa, cdi=cdi, ibov=ibov, usdbrl=usdbrl, di1_3y=di1_3y,
        macro_pm_var_hist=macro_pm_var_hist, df_pa_daily=df_pa_daily,
        idka_idx_ret=idka_idx_ret, walb=walb, rf_expo_maps=rf_expo_maps,
        position_changes=position_changes,
        dist_map=dist_map, dist_map_prev=dist_map_prev, dist_actuals=dist_actuals,
        vol_regime_map=vol_regime_map, pm_book_var=pm_book_var,
        expo_date_label=expo_date_label,
        book_pnl=book_pnl, peers_data=peers_data, peers_data_eopm=peers_data_eopm,
        market_snap=market_snap,
        df_pm_book_pnl=df_pm_book_pnl,
    )


def _build_report_data(date_str: str, d1_str: str, fd) -> "ReportData":
    """Assemble the data_manifest + ReportData from a fully-resolved fetched
    namespace (output of _fetch_all_data)."""
    _var_dates = {}
    for td, cfg in ALL_FUNDS.items():
        s = fd.series.get(td)
        if s is not None and not s.empty:
            s_avail = s[s["VAL_DATE"] <= DATA]
            if not s_avail.empty:
                _var_dates[cfg["short"]] = s_avail.iloc[-1]["VAL_DATE"]
    data_manifest = {
        "requested_date": date_str,
        "d1_str":         d1_str,
        # PA / PnL
        "pa_ok":          fd.df_pa is not None and not fd.df_pa.empty,
        "pa_has_today":   (fd.df_pa is not None and not fd.df_pa.empty and
                           not fd.df_pa[fd.df_pa["dia_bps"].abs() > 1e-6].empty),
        # VaR / Stress
        "var_dates":      _var_dates,    # short → actual date used (may be D-1)
        # Exposure
        "expo_ok":        fd.df_expo is not None and not fd.df_expo.empty,
        "expo_date":      fd.expo_date_label or date_str,
        # Single-names
        "quant_sn_ok":    fd.df_quant_sn is not None,
        "evo_sn_ok":      fd.df_evo_sn is not None,
        # Distribution
        "dist_today_ok":  bool(fd.dist_map),
        "dist_prev_ok":   bool(fd.dist_map_prev),
        # ALBATROZ
        "alb_expo_ok":    fd.df_alb_expo is not None and not fd.df_alb_expo.empty,
        "quant_expo_ok":  fd.df_quant_expo is not None and not fd.df_quant_expo.empty,
        "evo_expo_ok":    fd.df_evo_expo is not None and not fd.df_evo_expo.empty,
        # Stop monitor
        "stop_ok":        bool(fd.pm_margem),
        "stop_has_pnl":   any(abs(fd.pm_margem.get(pm, STOP_BASE) - STOP_BASE) > 1 for pm in fd.pm_margem),
        # Detail for quality tab
        "expo_rows":      len(fd.df_expo) if fd.df_expo is not None and not fd.df_expo.empty else 0,
        "quant_sn_rows":  len(fd.df_quant_sn) if fd.df_quant_sn is not None else 0,
        "evo_sn_rows":    len(fd.df_evo_sn)   if fd.df_evo_sn   is not None else 0,
        "alb_expo_rows":  len(fd.df_alb_expo) if fd.df_alb_expo is not None and not fd.df_alb_expo.empty else 0,
        "quant_expo_rows": len(fd.df_quant_expo) if fd.df_quant_expo is not None and not fd.df_quant_expo.empty else 0,
        "evo_expo_rows":  len(fd.df_evo_expo) if fd.df_evo_expo is not None and not fd.df_evo_expo.empty else 0,
        "stop_pms":       sorted(fd.pm_margem.keys()) if fd.pm_margem else [],
        "stop_pms_pnl":   [pm for pm, v in (fd.pm_margem or {}).items() if abs(v - STOP_BASE) > 1],
    }

    return ReportData(
        series_map=fd.series, stop_hist=fd.stop_hist,
        df_today=fd.df_today, df_expo=fd.df_expo, df_var=fd.df_var, macro_aum=fd.macro_aum,
        df_expo_d1=fd.df_expo_d1, df_var_d1=fd.df_var_d1,
        df_pnl_prod=fd.df_pnl_prod, pm_margem=fd.pm_margem,
        df_quant_sn=fd.df_quant_sn, quant_nav=fd.quant_nav, quant_legs=fd.quant_legs,
        df_quant_expo=fd.df_quant_expo, quant_expo_nav=fd.quant_expo_nav,
        df_quant_expo_d1=fd.df_quant_expo_d1, quant_expo_nav_d1=fd.quant_expo_nav_d1,
        df_quant_var=fd.df_quant_var, df_quant_var_d1=fd.df_quant_var_d1,
        df_evo_sn=fd.df_evo_sn, evo_nav=fd.evo_nav, evo_legs=fd.evo_legs, df_evo_direct=fd.df_evo_direct,
        df_evo_expo=fd.df_evo_expo, evo_expo_nav=fd.evo_expo_nav,
        df_evo_expo_d1=fd.df_evo_expo_d1, evo_expo_nav_d1=fd.evo_expo_nav_d1,
        df_evo_var=fd.df_evo_var, df_evo_var_d1=fd.df_evo_var_d1, df_evo_pnl_prod=fd.df_evo_pnl_prod,
        df_alb_expo=fd.df_alb_expo, alb_nav=fd.alb_nav,
        df_baltra_expo=fd.df_baltra_expo, baltra_nav=fd.baltra_nav,
        alb_pq_flags=fd.alb_pq_flags, baltra_pq_flags=fd.baltra_pq_flags,
        df_baltra_credit=fd.df_baltra_credit, df_evo_credit=fd.df_evo_credit,
        cdi_annual=fd.cdi_annual, ipca_annual=fd.ipca_annual,
        df_frontier=fd.df_frontier, frontier_bvar=fd.frontier_bvar, frontier_bvar_d1=fd.frontier_bvar_d1,
        df_frontier_ibov=fd.df_frontier_ibov, df_frontier_smll=fd.df_frontier_smll,
        df_frontier_sectors=fd.df_frontier_sectors,
        df_pa=fd.df_pa, cdi=fd.cdi, ibov=fd.ibov, usdbrl=fd.usdbrl, di1_3y=fd.di1_3y,
        macro_pm_var_hist=fd.macro_pm_var_hist,
        df_pa_daily=fd.df_pa_daily,
        idka_idx_ret=fd.idka_idx_ret, walb=fd.walb, rf_expo_maps=fd.rf_expo_maps,
        position_changes=fd.position_changes,
        dist_map=fd.dist_map, dist_map_prev=fd.dist_map_prev, dist_actuals=fd.dist_actuals,
        vol_regime_map=fd.vol_regime_map, pm_book_var=fd.pm_book_var,
        expo_date_label=fd.expo_date_label, data_manifest=data_manifest,
        book_pnl=fd.book_pnl, peers_data=fd.peers_data, peers_data_eopm=fd.peers_data_eopm,
        market_snap=fd.market_snap,
        df_pm_book_pnl=fd.df_pm_book_pnl,
    )


def _write_output(html: str, date_str: str) -> None:
    """Write the rendered HTML to OUT_DIR + mirror to RISK_MIRROR_PATH (env-overridable)."""
    out  = OUT_DIR / f"{date_str}_risk_monitor.html"
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out}")

    # Mirror copy to shared Risk_Manager Morningcall folder for distribution.
    # Two files written to share:
    #   {date_str}_risk_monitor.html  — dated archive (acumula histórico)
    #   ultimo_risk_monitor.html      — "latest" pointer (nome fixo, sempre
    #                                    sobrescreve — pra link/URL constante)
    # Mirror path overridable via RISK_MIRROR_PATH; empty/unset disables mirroring.
    mirror_path = os.environ.get("RISK_MIRROR_PATH", r"F:\Bloomberg\Risk_Manager\Data\Morningcall")
    if mirror_path:
        mirror_dir = Path(mirror_path)
        mirror = mirror_dir / f"{date_str}_risk_monitor.html"
        try:
            mirror_dir.mkdir(parents=True, exist_ok=True)
            mirror.write_text(html, encoding="utf-8")
            print(f"Saved (mirror): {mirror}")

            latest = mirror_dir / "ultimo_risk_monitor.html"
            latest.write_text(html, encoding="utf-8")
            print(f"Saved (ultimo): {latest}")
        except Exception as e:
            print(f"WARNING: mirror save failed for {mirror_dir}: {e!r}", file=sys.stderr)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"Fetching data for {DATA_STR}...")
    d1_str = _prev_bday(DATA_STR)
    d2_str = _prev_bday(d1_str)
    fd = _fetch_all_data(DATA_STR, d1_str, d2_str)
    report_data = _build_report_data(DATA_STR, d1_str, fd)
    html = build_html(report_data)
    _write_output(html, DATA_STR)


if __name__ == "__main__":
    main()

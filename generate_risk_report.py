"""
generate_risk_report.py
Gera o HTML diário de risco MM com barras de range 12m e sparklines 60d.
Usage: python generate_risk_report.py [YYYY-MM-DD]
"""
import sys
import json
from pathlib import Path
from datetime import date

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import matplotlib
matplotlib.use("Agg")

sys.path.insert(0, str(Path(__file__).parent))
from risk_runtime import DATA_STR, DATA, OUT_DIR, fmt_br_num as _fmt_br_num
from risk_config import (
    FUNDS, RAW_FUNDS, IDKA_FUNDS, ALL_FUNDS,
    ALERT_THRESHOLD, UTIL_WARN, UTIL_HARD,
    STOP_BASE, ALBATROZ_STOP_BPS,
    REPORTS, FUND_ORDER, FUND_LABELS,
    _FUND_PA_KEY, _PA_BENCH_LIVROS,
    _PM_LIVRO,
    ALERT_COMMENTS,
)
from svg_renderers import make_sparkline, range_bar_svg
from db_helpers import _prev_bday, fetch_all_latest_navs, _latest_nav
from metrics import (
    compute_pm_hs_var,
    compute_frontier_bvar_hs,
    compute_portfolio_vol_regime,
    compute_pa_outliers,
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
    _build_fund_mini_briefing,
    _build_executive_briefing,
)
from data_fetch import (
    fetch_pm_pnl_history,
    fetch_risk_history,
    fetch_risk_history_raw,
    fetch_risk_history_idka,
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
    fetch_pa_daily_per_product,
    fetch_idka_index_returns,
    fetch_idka_albatroz_weight,
    fetch_ibov_returns,
    fetch_cdi_returns,
)

# ── Config (fund mandates, thresholds, stops, display) moved to risk_config.py ─


# ── Fetch data ───────────────────────────────────────────────────────────────
# ── Build series ─────────────────────────────────────────────────────────────
def build_series(df_risk, df_aum, df_risk_raw=None, df_risk_idka=None):
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


def build_html(series_map: dict, stop_hist: dict = None, df_today=None,
               df_expo=None, df_var=None, macro_aum=None,
               df_expo_d1=None, df_var_d1=None,
               df_pnl_prod=None, pm_margem=None,
               df_quant_sn=None, quant_nav=None, quant_legs=None,
               df_evo_sn=None, evo_nav=None, evo_legs=None,
               df_evo_direct=None,
               df_pa=None, cdi=None, ibov=None, df_pa_daily=None,
               idka_idx_ret=None, walb=None,
               df_alb_expo=None, alb_nav=None,
               df_quant_expo=None, quant_expo_nav=None,
               df_quant_expo_d1=None,
               df_quant_var=None, df_quant_var_d1=None,
               df_evo_expo=None, evo_expo_nav=None,
               df_evo_expo_d1=None,
               df_evo_var=None, df_evo_var_d1=None,
               df_evo_pnl_prod=None,
               rf_expo_maps=None,
               df_frontier=None, frontier_bvar=None,
               df_frontier_ibov=None, df_frontier_smll=None, df_frontier_sectors=None,
               position_changes=None,
               dist_map=None, dist_map_prev=None, dist_actuals=None,
               vol_regime_map=None,
               pm_book_var=None,
               expo_date_label=None, data_manifest=None) -> str:
    alerts = []
    td_by_short = {cfg["short"]: td for td, cfg in ALL_FUNDS.items()}
    sections = []  # list of (fund_short, report_id, html)
    evolution_c4_state = {}  # Camada 4 state — populated when EVOLUTION diversificação renders

    def util_color(u):
        return "var(--up)" if u < 70 else "var(--warn)" if u < 100 else "var(--down)"

    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td is None:
            continue
        cfg = ALL_FUNDS[td]
        s = series_map.get(td)
        if s is None or s.empty:
            continue
        s_avail = s[s["VAL_DATE"] <= DATA]
        if s_avail.empty:
            continue
        tr = s_avail.iloc[-1]
        var_today    = abs(tr["var_pct"])
        stress_today = abs(tr["stress_pct"])
        var_util     = var_today    / cfg["var_soft"]    * 100
        str_util     = stress_today / cfg["stress_soft"] * 100

        var_abs, str_abs = var_today, stress_today
        var_min_abs,  var_max_abs  = s["var_pct"].abs().min(),    s["var_pct"].abs().max()
        str_min_abs,  str_max_abs  = s["stress_pct"].abs().min(), s["stress_pct"].abs().max()

        var_range_pct    = (var_abs - var_min_abs)  / (var_max_abs - var_min_abs) * 100  if var_max_abs != var_min_abs else 0
        stress_range_pct = (str_abs - str_min_abs)  / (str_max_abs - str_min_abs) * 100  if str_max_abs != str_min_abs else 0

        _is_idka = cfg.get("primary") == "bvar"
        _is_inf  = cfg.get("informative") is True
        if not _is_inf and var_range_pct >= ALERT_THRESHOLD:
            alerts.append((short, "BVaR" if _is_idka else "VaR", var_range_pct, var_today, var_util, ALERT_COMMENTS.get(("var", short), "")))
        if not (_is_idka or _is_inf) and stress_range_pct >= ALERT_THRESHOLD:
            alerts.append((short, "Stress", stress_range_pct, stress_today, str_util, ALERT_COMMENTS.get(("stress", short), "")))

        var_bar    = range_bar_svg(var_abs,  var_min_abs,  var_max_abs,  cfg["var_soft"],    cfg["var_hard"])
        stress_bar = range_bar_svg(str_abs,  str_min_abs,  str_max_abs,  cfg["stress_soft"], cfg["stress_hard"])

        spark_var    = make_sparkline(s.set_index("VAL_DATE")["var_pct"],    "#1a8fd1")
        spark_stress = make_sparkline(s.set_index("VAL_DATE")["stress_pct"], "#f472b6")

        # IDKA: primary metric is BVaR (relative to benchmark); secondary is absolute VaR
        # shown for reference only (no util %, no hard limit highlighted).
        # Frontier: LO equity — no limits, both VaR and Stress shown as informative.
        is_idka = cfg.get("primary") == "bvar"
        is_informative = cfg.get("informative") is True
        if is_idka:
            primary_label   = "BVaR 95%"
            secondary_label = "VaR 95% (ref)"
            secondary_util_html = (
                f'<td class="util-cell mono" style="color:var(--muted)">ref</td>'
            )
        elif is_informative:
            primary_label   = "VaR 95% (ref)"
            secondary_label = "Stress (ref)"
            secondary_util_html = (
                f'<td class="util-cell mono" style="color:var(--muted)">ref</td>'
            )
        else:
            primary_label   = "VaR 95% 1d"
            secondary_label = "Stress"
            secondary_util_html = (
                f'<td class="util-cell mono" style="color:{util_color(str_util)}">{str_util:.0f}% soft</td>'
            )

        risk_monitor_html = f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Risk Monitor</span>
            <span class="card-sub">— {short}</span>
          </div>
          <table class="metric-table" data-no-sort="1">
            <thead>
              <tr class="col-headers">
                <th>Métrica</th>
                <th style="text-align:right">Valor</th>
                <th>12M Range <span class="tick">▏80%</span></th>
                <th style="text-align:right">Utilização</th>
                <th>60D Trend</th>
              </tr>
            </thead>
            <tbody>
              <tr class="metric-row">
                <td class="metric-name">{primary_label}</td>
                <td class="value-cell mono" style="color:{'var(--muted)' if is_informative else util_color(var_util)}">{var_today:.2f}%</td>
                <td class="bar-cell">{'' if is_informative else var_bar}</td>
                <td class="util-cell mono" style="color:{'var(--muted)' if is_informative else util_color(var_util)}">{'ref' if is_informative else f'{var_util:.0f}% soft'}</td>
                <td class="spark-cell"><img src="data:image/png;base64,{spark_var}" height="38"/></td>
              </tr>
              <tr class="metric-row">
                <td class="metric-name">{secondary_label}</td>
                <td class="value-cell mono" style="color:{'var(--muted)' if (is_idka or is_informative) else util_color(str_util)}">{stress_today:.2f}%</td>
                <td class="bar-cell">{'' if (is_idka or is_informative) else stress_bar}</td>
                {secondary_util_html}
                <td class="spark-cell"><img src="data:image/png;base64,{spark_stress}" height="38"/></td>
              </tr>
            </tbody>
          </table>
          <div class="bar-legend">
            <span style="color:var(--warn)">─ ─</span> soft &nbsp;
            <span style="color:var(--down)">─ ─</span> hard &nbsp;
            <span style="color:#fb923c">▏</span> 80° pct (alerta)
          </div>
        </section>"""
        sections.append((short, "risk-monitor", risk_monitor_html))

        # Diversificação — só para EVOLUTION, imediatamente após Risk Monitor
        if short == "EVOLUTION":
            try:
                div_html, _c4_state = build_evolution_diversification_section(DATA_STR)
                sections.append(("EVOLUTION", "diversification", div_html))
                # stash C4 state on the local closure for Summary to pick up
                evolution_c4_state = _c4_state
            except Exception as _e:
                sections.append(("EVOLUTION", "diversification",
                    f"<section class='card'><div class='card-head'>"
                    f"<span class='card-title'>Diversificação</span></div>"
                    f"<div style='color:var(--muted);padding:12px'>"
                    f"Falha ao montar: {_e}</div></section>"))
                evolution_c4_state = {}

    # Build alerts section
    # ── PA contribution alerts — filter by |dia_bps|, sorted by contribution ──
    PA_ALERT_MIN_BPS   = 5.0   # minimum absolute contribution to show
    PA_ALERT_HIGH_BPS  = 15.0  # threshold for red (large) vs yellow (medium)
    _PA_EXCL_LIVROS    = {"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}
    _PA_EXCL_CLASSES   = {"Caixa", "Custos"}

    pa_alert_items_size = ""
    pa_alert_items_fund = ""
    if df_pa is not None and not df_pa.empty:
        try:
            # Drop benchmark-replication livros (e.g. CI in IDKAs) — their "alpha"
            # is just tracking error vs. the index they're paid to replicate, not
            # a directional bet. Keeps the card focused on active alpha.
            _df_alpha = _pa_filter_alpha(df_pa)
            _pa_filt = _df_alpha[
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

    pa_alert_items = pa_alert_items_size  # keep existing consumers happy

    alerts_html = ""
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
    if risk_items or pa_alert_items:
        if pa_alert_items:
            _top_margin = "4px" if risk_items else "0"
            pa_grid = (
                f'<div class="pa-alert-view" data-pa-sort="size" style="margin-top:{_top_margin}">{pa_alert_items_size}</div>'
                f'<div class="pa-alert-view pa-alert-view-hidden" data-pa-sort="fund" style="margin-top:{_top_margin}">{pa_alert_items_fund}</div>'
            )
        else:
            pa_grid = ""
        pa_header = (
            f'<div style="display:flex;align-items:center;gap:12px;margin-top:{"16px" if risk_items else "0"};margin-bottom:8px">'
            f'<span style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">PA — Contribuições do dia (|contrib| ≥ {PA_ALERT_MIN_BPS:.0f} bps)</span>'
            f'<div class="pa-view-toggle pa-alert-toggle" style="margin-left:auto">'
            f'<button class="pa-tgl active" data-pa-sort="size" onclick="selectPaAlertSort(this,\'size\')">Por Tamanho</button>'
            f'<button class="pa-tgl"        data-pa-sort="fund" onclick="selectPaAlertSort(this,\'fund\')">Por Fundo</button>'
            f'</div></div>'
        ) if pa_alert_items else ""
        alerts_html = f"""
        <div class="alerts-section">
          <div class="alerts-header">Análise{' — Métricas acima do 80° percentil histórico' if risk_items else ''}</div>
          {risk_items}
          {pa_header}
          {pa_grid}
        </div>"""

    # MACRO-specific sections
    # Risk Budget tab = stop monitor (PnL × carry) + Budget vs VaR card combined
    # into a single section wrapper (avoids duplicate DOM ids for the same tab).
    if stop_hist and df_today is not None:
        _stop_html = build_stop_section(stop_hist, df_today)
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
        _expo_html = build_exposure_section(df_expo, df_var, macro_aum, df_expo_d1, df_var_d1, df_pnl_prod, pm_margem)
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

    # Performance Attribution — one card per fund (MACRO, QUANT, EVOLUTION, MACRO_Q, ALBATROZ)
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
            )
            if pa_html:
                sections.append((short, "performance", pa_html))

    # ALBATROZ — RF exposure card (under the "exposure" report tab)
    if df_alb_expo is not None and not df_alb_expo.empty and alb_nav:
        alb_html = build_albatroz_exposure(df_alb_expo, alb_nav)
        if alb_html:
            sections.append(("ALBATROZ", "exposure", alb_html))

    # QUANT — exposure card (by factor + by livro × factor)
    if df_quant_expo is not None and not df_quant_expo.empty and quant_expo_nav:
        q_expo_html = build_quant_exposure_section(
            df_quant_expo, quant_expo_nav,
            df_d1=df_quant_expo_d1,
            df_var=df_quant_var,
            df_var_d1=df_quant_var_d1,
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
        )
        if e_expo_html:
            sections.append(("EVOLUTION", "exposure", e_expo_html))

    # RF Exposure Map (IDKA 3Y, IDKA 10Y, Albatroz, MACRO, EVOLUTION)
    # MACRO/EVOLUTION use bench_dur=0 ("—" label) since they have no fixed-duration mandate.
    _RF_MAP_CFG = {
        "IDKA_3Y":   {"desk": "IDKA IPCA 3Y FIRF",              "bench_dur": 3.0,  "bench_label": "IDKA IPCA 3A"},
        "IDKA_10Y":  {"desk": "IDKA IPCA 10Y FIRF",             "bench_dur": 10.0, "bench_label": "IDKA IPCA 10A"},
        "ALBATROZ":  {"desk": "GALAPAGOS ALBATROZ FIRF LP",     "bench_dur": 0.0,  "bench_label": "CDI"},
        "MACRO":     {"desk": "Galapagos Macro FIM",            "bench_dur": 0.0,  "bench_label": "—"},
        "EVOLUTION": {"desk": "Galapagos Evolution FIC FIM CP", "bench_dur": 0.0,  "bench_label": "—"},
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

    # ── Per-fund Análise sections (outliers / movers / changes filtered by fund) ──
    _ANALISE_MOVERS_EXCLUDE = {
        "LIVRO":  {"Taxas e Custos", "Caixa", "Caixa USD"},
        "CLASSE": {"Custos", "Caixa"},
    }
    _ANALISE_THRESHOLD_PP = 0.30

    def _an_delta_pp(delta, pct_d0):
        # Green = reduz risco (|posição hoje| < |posição ontem|)
        # Red   = aumenta risco (|posição hoje| > |posição ontem|)
        pct_d1 = pct_d0 - delta
        reduces_risk = abs(pct_d0) < abs(pct_d1)
        color = "var(--up)" if reduces_risk else "var(--down)"
        sign = "+" if delta > 0 else ""
        return f'<span style="color:{color}; font-weight:700">{sign}{delta:.2f} pp</span>'

    _an_outliers_by_fund = {}
    if df_pa_daily is not None and not df_pa_daily.empty:
        try:
            _all_out = compute_pa_outliers(df_pa_daily, DATA_STR, z_min=2.0, bps_min=3.0, min_obs=20)
            if not _all_out.empty:
                for pa_key in _all_out["FUNDO"].unique():
                    _an_outliers_by_fund[pa_key] = _all_out[_all_out["FUNDO"] == pa_key]
        except Exception as e:
            print(f"  per-fund outliers failed ({e})")

    def _an_outliers_card(short):
        pa_key = _FUND_PA_KEY.get(short)
        sub = _an_outliers_by_fund.get(pa_key)
        if sub is None or sub.empty:
            body = '<div class="comment-empty">— sem eventos significativos (|z| ≥ 2σ · |contrib| ≥ 3 bps)</div>'
        else:
            items = ""
            for r in sub.itertuples(index=False):
                color = "var(--up)" if r.today_bps > 0 else "var(--down)"
                items += (
                    '<li>'
                    f'<span class="mono" style="font-weight:700">{r.PRODUCT}</span> '
                    f'<span class="mono" style="color:{color}">{r.today_bps:+.1f} bps</span> '
                    f'<span class="mono" style="color:var(--muted)">({r.z:+.1f}σ)</span>'
                    f' &nbsp;·&nbsp; <span style="color:var(--muted)">{_pa_render_name(r.LIVRO)}</span>'
                    '</li>'
                )
            body = f'<ul class="comment-list">{items}</ul>'
        return f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Outliers do dia</span>
            <span class="card-sub">— {FUND_LABELS.get(short, short)} · |z| ≥ 2σ (vs. 90d) · |contrib| ≥ 3 bps</span>
          </div>
          {body}
        </section>"""

    def _an_movers_rows(short, group_col):
        pa_key = _FUND_PA_KEY.get(short)
        if not pa_key or df_pa is None or df_pa.empty:
            return ""
        sub = df_pa[df_pa["FUNDO"] == pa_key]
        sub = sub[~sub[group_col].isin(_ANALISE_MOVERS_EXCLUDE.get(group_col, set()))]
        by = sub.groupby(group_col)["dia_bps"].sum()
        by = by[by.abs() > 0.5]
        pos = by[by > 0].sort_values(ascending=False).head(5)
        neg = by[by < 0].sort_values().head(5)
        render = _pa_render_name if group_col == "LIVRO" else (lambda s: s)
        def _fmt(color, items_s):
            if not len(items_s):
                return '<li class="comment-empty">— nada material</li>'
            return "".join(
                '<li>'
                f'<span class="mono" style="color:{color}; font-weight:600">{render(l)}</span> '
                f'<span class="mono">{"+" if v >= 0 else ""}{v/100:.2f}%</span>'
                '</li>'
                for l, v in items_s.items()
            )
        return f"""
        <div class="mov-split">
          <div>
            <div class="mov-col-title">Contribuintes</div>
            <ul class="comment-list">{_fmt("var(--up)", pos)}</ul>
          </div>
          <div>
            <div class="mov-col-title">Detratores</div>
            <ul class="comment-list">{_fmt("var(--down)", neg)}</ul>
          </div>
        </div>"""

    def _an_movers_frontier():
        """Top movers for FRONTIER — por papel e por setor, já líquido do bench
           (usa TOTAL_IBVSP_DAY = excess return vs IBOV).
           Retorna (papel_body, setor_body) — strings HTML, ou (None, None)."""
        if df_frontier is None or df_frontier.empty:
            return None, None
        sub = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])].copy()
        sub = sub[sub["TOTAL_IBVSP_DAY"].notna()]
        if sub.empty:
            return None, None
        sub["alpha_pct"] = sub["TOTAL_IBVSP_DAY"].astype(float) * 100.0  # fração → %

        _THR = 0.01  # 1 bp: ignora ruído mínimo

        def _fmt_rows(df_in, col_name, color):
            if df_in is None or df_in.empty:
                return '<li class="comment-empty">— nada material</li>'
            out = ""
            for _, r in df_in.iterrows():
                v = float(r["alpha_pct"])
                out += (
                    '<li>'
                    f'<span class="mono" style="color:{color}; font-weight:600">{r[col_name]}</span> '
                    f'<span class="mono">{"+" if v >= 0 else ""}{v:.2f}%</span>'
                    '</li>'
                )
            return out

        def _split_html(pos_items, neg_items, col_name):
            return f"""
            <div class="mov-split">
              <div>
                <div class="mov-col-title">Contribuintes (α vs IBOV)</div>
                <ul class="comment-list">{_fmt_rows(pos_items, col_name, "var(--up)")}</ul>
              </div>
              <div>
                <div class="mov-col-title">Detratores (α vs IBOV)</div>
                <ul class="comment-list">{_fmt_rows(neg_items, col_name, "var(--down)")}</ul>
              </div>
            </div>"""

        # Por Papel
        papel_pos = sub[sub["alpha_pct"] >=  _THR].nlargest(5, "alpha_pct")
        papel_neg = sub[sub["alpha_pct"] <= -_THR].nsmallest(5, "alpha_pct")
        papel_body = _split_html(papel_pos, papel_neg, "PRODUCT")

        # Por Setor — join com sectors table
        setor_body = ""
        if df_frontier_sectors is not None and not df_frontier_sectors.empty:
            merged = sub.merge(
                df_frontier_sectors[["TICKER", "GLPG_SECTOR"]],
                left_on="PRODUCT", right_on="TICKER", how="left",
            )
            merged["GLPG_SECTOR"] = merged["GLPG_SECTOR"].fillna("—")
            by = merged.groupby("GLPG_SECTOR", as_index=False)["alpha_pct"].sum()
            by = by[by["alpha_pct"].abs() >= _THR]
            setor_pos = by[by["alpha_pct"] > 0].sort_values("alpha_pct", ascending=False).head(5)
            setor_neg = by[by["alpha_pct"] < 0].sort_values("alpha_pct").head(5)
            setor_body = _split_html(setor_pos, setor_neg, "GLPG_SECTOR")

        return papel_body, setor_body

    def _an_movers_card(short):
        # Frontier é Long Only — não tem PA com LIVRO/CLASSE. Usa alpha vs IBOV
        # direto do Long Only Mainboard, agrupado por papel ou por setor.
        if short == "FRONTIER":
            papel_body, setor_body = _an_movers_frontier()
            if not papel_body and not setor_body:
                return ""
            if setor_body:
                toggle = """
                <div class="pa-view-toggle sum-tgl">
                  <button class="pa-tgl active" data-mov-view="papel"
                          onclick="selectMoversView(this,'papel')">Por Papel</button>
                  <button class="pa-tgl" data-mov-view="setor"
                          onclick="selectMoversView(this,'setor')">Por Setor</button>
                </div>"""
                setor_div = f'<div class="mov-view" data-mov-view="setor" style="display:none">{setor_body}</div>'
            else:
                toggle = ""
                setor_div = ""
            return f"""
            <section class="card sum-movers-card">
              <div class="card-head">
                <span class="card-title">Top Movers — DIA</span>
                <span class="card-sub">— Frontier · α vs IBOV (líquido do bench) · drop &lt; 1 bp</span>
                {toggle}
              </div>
              <div class="mov-view" data-mov-view="papel">{papel_body}</div>
              {setor_div}
            </section>"""

        livro_body  = _an_movers_rows(short, "LIVRO")
        classe_body = _an_movers_rows(short, "CLASSE")
        if not (livro_body or classe_body):
            return ""
        return f"""
        <section class="card sum-movers-card">
          <div class="card-head">
            <span class="card-title">Top Movers — DIA</span>
            <span class="card-sub">— {FUND_LABELS.get(short, short)} · alpha do dia (drop &lt; 0.5 bps · Caixa/Custos excluídos)</span>
            <div class="pa-view-toggle sum-tgl">
              <button class="pa-tgl active" data-mov-view="livro"
                      onclick="selectMoversView(this,'livro')">Por Livro</button>
              <button class="pa-tgl" data-mov-view="classe"
                      onclick="selectMoversView(this,'classe')">Por Classe</button>
            </div>
          </div>
          <div class="mov-view" data-mov-view="livro">{livro_body}</div>
          <div class="mov-view" data-mov-view="classe" style="display:none">{classe_body}</div>
        </section>"""

    def _an_changes_card(short):
        if short == "MACRO":
            if df_expo is None or df_expo_d1 is None:
                return ""
            d0 = df_expo.groupby(["pm", "rf"])["pct_nav"].sum().reset_index()
            d1 = df_expo_d1.groupby(["pm", "rf"])["pct_nav"].sum().reset_index().rename(
                columns={"pct_nav": "pct_d1"}
            )
            chg = d0.merge(d1, on=["pm", "rf"], how="outer").fillna(0.0)
            chg["delta"] = chg["pct_nav"] - chg["pct_d1"]
            chg = chg[chg["delta"].abs() >= _ANALISE_THRESHOLD_PP]
            chg = chg.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(10)
            if chg.empty:
                return ""
            rows = "".join(
                '<tr>'
                f'<td class="sum-fund" style="width:70px">{r.pm}</td>'
                f'<td class="mono" style="color:var(--muted)">{r.rf}</td>'
                f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_nav:+.2f}%</td>'
                f'<td class="mono" style="text-align:right">{_an_delta_pp(r.delta, r.pct_nav)}</td>'
                '</tr>'
                for r in chg.itertuples(index=False)
            )
            return f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Mudanças Significativas</span>
                <span class="card-sub">— MACRO · PM × fator · |Δ| ≥ {_ANALISE_THRESHOLD_PP:.2f} pp · 🟢 reduz risco · 🔴 aumenta risco</span>
              </div>
              <table class="summary-movers">
                <thead><tr>
                  <th style="text-align:left; width:70px">PM</th>
                  <th style="text-align:left">Fator</th>
                  <th style="text-align:right">D-1</th>
                  <th style="text-align:right">D-0</th>
                  <th style="text-align:right">Δ</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </section>"""
        else:
            if not position_changes:
                return ""
            df_chg = position_changes.get(short)
            if df_chg is None or df_chg.empty:
                return ""
            big = df_chg[df_chg["delta"].abs() >= _ANALISE_THRESHOLD_PP]
            big = big.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(10)
            if big.empty:
                return ""
            rows = "".join(
                '<tr>'
                f'<td class="mono" style="color:var(--muted)">{r.factor}</td>'
                f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_d0:+.2f}%</td>'
                f'<td class="mono" style="text-align:right">{_an_delta_pp(r.delta, r.pct_d0)}</td>'
                '</tr>'
                for r in big.itertuples(index=False)
            )
            return f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Mudanças Significativas</span>
                <span class="card-sub">— {FUND_LABELS.get(short, short)} · por fator · |Δ| ≥ {_ANALISE_THRESHOLD_PP:.2f} pp · 🟢 reduz risco · 🔴 aumenta risco</span>
              </div>
              <table class="summary-movers">
                <thead><tr>
                  <th style="text-align:left">Fator</th>
                  <th style="text-align:right">D-1</th>
                  <th style="text-align:right">D-0</th>
                  <th style="text-align:right">Δ</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </section>"""

    for short in FUND_ORDER:
        for card in (_an_outliers_card(short), _an_movers_card(short), _an_changes_card(short)):
            if card:
                sections.append((short, "analise", card))

    # Which fund×report combinations exist — used to enable/disable tabs and handle empty states
    available_pairs = {(f, r) for f, r, _ in sections}
    funds_with_data = sorted({f for f, _ in available_pairs}, key=FUND_ORDER.index)
    reports_with_data = [rid for rid, _ in REPORTS if any(rid == r for _, r in available_pairs)]

    # Sort sections so Per-Fund view shows reports in canonical REPORTS order
    _REPORT_IDX = {rid: i for i, (rid, _) in enumerate(REPORTS)}
    sections.sort(key=lambda x: (
        FUND_ORDER.index(x[0]) if x[0] in FUND_ORDER else 99,
        _REPORT_IDX.get(x[1], 99)
    ))

    # Render all sections wrapped for data-attribute filtering
    sections_html = "".join(
        f'<div id="sec-{f}-{r}" class="section-wrap" data-fund="{f}" data-report="{r}">{h}</div>'
        for f, r, h in sections
    )

    # ── Summary (cross-fund landing) ──────────────────────────────────────
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

    # Gather per-fund summary data
    summary_rows_html = ""
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
        if short == "MACRO" and pm_margem and stop_hist:
            utils = []
            cur_mes = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
            for pm, margem in pm_margem.items():
                hist = stop_hist.get(pm)
                if hist is None: continue
                cur_row = hist[hist["mes"] == cur_mes]
                if cur_row.empty: continue
                budget = float(cur_row["budget_abs"].iloc[0])
                if budget <= 0: continue
                consumed = budget - margem
                utils.append(max(consumed, 0) / budget * 100)
            if utils: stop_util = max(utils)
        elif short == "ALBATROZ":
            stop_util = (abs(a_mtd) / ALBATROZ_STOP_BPS * 100) if a_mtd < 0 else 0.0

        worst = max(x for x in (var_util, stop_util, 0) if x is not None)
        if worst >= 100:   status = "🔴"
        elif worst >= 70:  status = "🟡"
        else:              status = "🟢"

        summary_rows_html += (
            "<tr>"
            f'<td class="sum-status">{status}</td>'
            f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
            + _sum_bp_cell(a_dia) + _sum_bp_cell(a_mtd) + _sum_bp_cell(a_ytd) + _sum_bp_cell(a_m12)
            + _sum_var_cell(var_today) + _sum_util_cell(var_util)
            + _sum_dvar_cell(dvar)
            + "</tr>"
        )

    # ── Benchmark reference rows (IBOV, CDI) ───────────────────────────────────
    def _bench_row(label: str, returns: dict | None) -> str:
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
    bench_rows_html = (
        _bench_row("IBOV",     ibov)
      + _bench_row("CDI",      cdi)
      + _bench_row("IDKA 3A",  idka3)
      + _bench_row("IDKA 10A", idka10)
    )

    # ── House-wide risk consolidated ───────────────────────────────────────────
    # Absolute VaR (% NAV) comes from series_map. BVaR (benchmark-relative) from:
    #   IDKAs   → series_map[td]["var_pct"] (engine BVaR; stress_pct holds abs VaR)
    #   Frontier→ frontier_bvar["bvar_pct"] (3y HS vs IBOV)
    #   Others  → BVaR vs CDI ≈ abs VaR (CDI has effectively zero daily vol)
    _BENCH_BY_FUND = {
        "MACRO": "CDI", "QUANT": "CDI", "EVOLUTION": "CDI", "MACRO_Q": "CDI", "ALBATROZ": "CDI",
        "FRONTIER": "IBOV", "IDKA_3Y": "IDKA 3A", "IDKA_10Y": "IDKA 10A",
    }
    house_rows = []
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
        if cfg_.get("primary") == "bvar":          # IDKAs
            abs_var_pct = abs(float(last.get("stress_pct", 0.0)))
            rel_var_pct = abs(float(last.get("var_pct",    0.0)))
        else:
            abs_var_pct = abs(float(last.get("var_pct", 0.0)))
            # Frontier: use HS BVaR vs. IBOV when we have it
            if short == "FRONTIER" and frontier_bvar:
                rel_var_pct = float(frontier_bvar["bvar_pct"])
            else:
                # CDI-benchmarked: BVaR ≈ absolute VaR since CDI vol ≈ 0
                rel_var_pct = abs_var_pct
        var_brl = abs_var_pct / 100.0 * nav_k
        bvar_brl = rel_var_pct / 100.0 * nav_k
        house_rows.append({
            "short":     short, "label": FUND_LABELS.get(short, short),
            "bench":     _BENCH_BY_FUND.get(short, "—"),
            "nav":       nav_k,
            "var_pct":   abs_var_pct, "var_brl":  var_brl,
            "bvar_pct":  rel_var_pct, "bvar_brl": bvar_brl,
        })

    # Rank: top 5 by absolute R$ VaR and by relative R$ VaR
    top5_abs = set(r["short"] for r in sorted(house_rows, key=lambda r: r["var_brl"],  reverse=True)[:5])
    top5_rel = set(r["short"] for r in sorted(house_rows, key=lambda r: r["bvar_brl"], reverse=True)[:5])

    # Per-fund mini briefing — registered as "briefing" report (first tab)
    _house_by_short = {r["short"]: r for r in house_rows}

    def _mm(v):
        try: return _fmt_br_num(f"{v/1e6:,.1f}M")
        except Exception: return "—"

    house_rows_html = ""
    for r in house_rows:
        rank_abs = "🔺" if r["short"] in top5_abs else ""
        rank_rel = "🔷" if r["short"] in top5_rel else ""
        house_rows_html += (
            "<tr>"
            f'<td class="sum-fund">{r["label"]}</td>'
            f'<td class="mono" style="text-align:right; color:var(--muted)">{_mm(r["nav"])}</td>'
            f'<td class="mono" style="text-align:right; font-weight:600">{r["var_pct"]:.2f}% {rank_abs}</td>'
            f'<td class="mono" style="text-align:right; font-weight:600">{r["bvar_pct"]:.2f}% {rank_rel}</td>'
            f'<td class="mono" style="text-align:center; color:var(--muted)">{r["bench"]}</td>'
            "</tr>"
        )
    tot_nav      = sum(r["nav"]      for r in house_rows)
    tot_var_brl  = sum(r["var_brl"]  for r in house_rows)
    tot_bvar_brl = sum(r["bvar_brl"] for r in house_rows)
    tot_var_pct  = (tot_var_brl  / tot_nav * 100) if tot_nav else 0.0
    tot_bvar_pct = (tot_bvar_brl / tot_nav * 100) if tot_nav else 0.0
    house_total_row = (
        '<tr class="house-total-row">'
        '<td class="sum-fund" style="font-weight:700">Total (soma)</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{_mm(tot_nav)}</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{tot_var_pct:.2f}%</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{tot_bvar_pct:.2f}%</td>'
        '<td></td>'
        '</tr>'
    )
    # ── Breakdown by risk factor — rows = factors, columns = funds (R$ exposure) ──
    # Factor sources:
    #   Real rates / Nominal rates / IPCA Index → rf_expo_maps (IDKAs + Albatroz)
    #   Equity BR (IBOV)                        → Frontier NAV (100% long); others pending
    factor_matrix = {  # factor_key -> {short: brl, ...}
        "Juros Reais (IPCA)": {},
        "Juros Nominais":     {},
        "IPCA Idx":           {},
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
        _DV01_SIGN_FLIP = {"real": True, "nominal": True, "ipca_idx": False}
        for short_k, df_k in rf_expo_maps.items():
            if df_k is None or df_k.empty:
                continue
            # Evita double-counting de Albatroz: cada fundo (IDKA, MACRO, etc.)
            # tem via='via_albatroz' capturando sua fatia de Albatroz; ALBATROZ
            # row já tem Albatroz inteiro via 'direct'. Filtrar via=='direct'
            # garante que cada bond aparece em apenas uma linha.
            # EVOLUTION (lookthrough_only=True) tem via='lookthrough' para TUDO
            # (inclui Macro/Quant/Frontier slices que já estão em outras linhas)
            # → pular por inteiro para rate factors (sua exposição de juros é
            # indireta via filhos).
            if short_k == "EVOLUTION":
                continue
            df_direct = df_k[df_k["via"] == "direct"]
            if df_direct.empty:
                continue
            for factor_key, factor_col in [("Juros Reais (IPCA)", "real"), ("Juros Nominais", "nominal"), ("IPCA Idx", "ipca_idx")]:
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
            fr_nav = _latest_nav("Frontier A\u00e7\u00f5es FIC FI", DATA_STR) or 0
            if gross_pct and fr_nav:
                factor_matrix["Equity BR"]["FRONTIER"] = gross_pct * fr_nav
    # QUANT + EVOLUTION equity BR — use Evolution-direct only (not look-through)
    # to avoid double-counting positions already held via QUANT / Frontier.
    if df_quant_sn is not None and not df_quant_sn.empty:
        q_equity = float(df_quant_sn["net"].sum())
        if abs(q_equity) >= 1_000:
            factor_matrix["Equity BR"]["QUANT"] = q_equity
    _evo_sn_for_agg = df_evo_direct if df_evo_direct is not None and not df_evo_direct.empty else None
    if _evo_sn_for_agg is not None:
        e_equity = float(_evo_sn_for_agg["net"].sum())
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
        # MACRO nominal-rate duration: sum DELTA on RF-BZ / BRL Rate Curve primitive.
        # IMPORTANT: DELTA in LOTE_PRODUCT_EXPO is already duration-weighted
        # (= POSITION × MOD_DURATION). Do NOT multiply by MOD_DURATION again —
        # the prior `delta_dur` column squared it. Filtering to BRL Rate Curve
        # avoids double-counting primitives (IPCA Coupon, etc.) on hybrid rows.
        rf_bz_nom = df_expo[(df_expo["rf"] == "RF-BZ")
                            & (df_expo["PRIMITIVE_CLASS"] == "BRL Rate Curve")]
        if not rf_bz_nom.empty:
            nominal_brl_yr = float(rf_bz_nom["delta"].sum())
            if abs(nominal_brl_yr) >= 1_000:
                factor_matrix["Juros Nominais"]["MACRO"] = nominal_brl_yr

    # QUANT non-equity factors from df_quant_expo (has `factor` col already classified
    # by `_quant_classify_factor`). Equity BR was populated earlier via single-name.
    # Here: Juros Nominais (SIST_RF), FX (SIST_FX + SIST_GLOBAL), Commodities (SIST_COMMO).
    if df_quant_expo is not None and not df_quant_expo.empty:
        # For nominal rates, filter to BRL Rate Curve primitive to avoid double-count.
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
    # All other factor×fund cells: bench = 0 (CDI-benchmarked funds or factor not in bench).
    nav_by_short = {}
    for short in FUND_ORDER:
        td = td_by_short.get(short)
        if td:
            nav_by_short[short] = _latest_nav(td, DATA_STR) or 0.0
    bench_matrix = {k: {} for k in factor_matrix}
    # Juros Reais/Nominais: factor_matrix está em convenção DV01 (long bond = negativo).
    # Bench long IPCA duration do IDKA também é "long bond" → DV01 negativo.
    # IPCA Idx / Equity: sem sign flip (face-value / notional raw), bench positivo.
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

    factor_list = ["Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx", "Equity BR", "Equity DM", "Equity EM", "FX", "Commodities"]
    # Unit per factor:
    #   "yrs" → ano_eq_brl is duration-weighted BRL (BRL-years). Divide by NAV → anos.
    #   "pct" → ano_eq_brl is BRL notional. Divide by NAV → %NAV.
    # Duration factors (real/nominal rates) precisam de "yrs" pra não reportar
    # "+376% NAV" quando o valor real é "+3.76 anos de duration".
    _FACTOR_UNIT = {
        "Juros Reais (IPCA)": "yrs",
        "Juros Nominais":     "yrs",
        "IPCA Idx":           "pct",
        "Equity BR":          "pct",
        "Equity DM":          "pct",
        "Equity EM":          "pct",
        "FX":                 "pct",
        "Commodities":        "pct",
    }

    house_nav_tot = sum(v for v in nav_by_short.values() if v)
    def _render_factor_rows(net_of_bench: bool) -> str:
        rows = ""
        for factor_key in factor_list:
            allocations = factor_matrix.get(factor_key, {})
            benches     = bench_matrix.get(factor_key, {}) if net_of_bench else {}
            shorts_with_data = set(allocations.keys()) | set(benches.keys())
            if not shorts_with_data:
                continue
            unit = _FACTOR_UNIT.get(factor_key, "pct")
            # DV01 convention everywhere: long bond = negative (bar goes DOWN).
            # factor_matrix/bench_matrix already stored in DV01 conv — no flip.
            sign_flip = 1
            cells = ""
            total_brl = 0.0
            for short in FUND_ORDER:
                gross = allocations.get(short, 0.0)
                bench = benches.get(short, 0.0) if net_of_bench else 0.0
                v_brl = (gross - bench) * sign_flip
                nav_k = nav_by_short.get(short, 0.0)
                if abs(v_brl) < 1_000 or not nav_k:
                    cells += '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
                else:
                    total_brl += v_brl
                    col = "var(--up)" if v_brl >= 0 else "var(--down)"
                    if unit == "yrs":
                        yrs = v_brl / nav_k
                        cells += f'<td class="mono" style="text-align:right; color:{col}">{yrs:+.2f} yrs</td>'
                    else:
                        pct = v_brl / nav_k * 100
                        cells += f'<td class="mono" style="text-align:right; color:{col}">{pct:+.2f}%</td>'
            tot_col = "var(--up)" if total_brl >= 0 else "var(--down)"
            if unit == "yrs":
                tot_yrs = (total_brl / house_nav_tot) if house_nav_tot else 0.0
                tot_cell = f'<td class="mono" style="text-align:right; font-weight:700; color:{tot_col}">{tot_yrs:+.2f} yrs</td>'
            else:
                tot_pct = (total_brl / house_nav_tot * 100) if house_nav_tot else 0.0
                tot_cell = f'<td class="mono" style="text-align:right; font-weight:700; color:{tot_col}">{tot_pct:+.2f}%</td>'
            rows += (
                "<tr>"
                f'<td class="sum-fund">{factor_key}</td>'
                + cells
                + tot_cell
                + "</tr>"
            )
        return rows

    factor_rows_liquido = _render_factor_rows(net_of_bench=True)
    factor_rows_bruto   = _render_factor_rows(net_of_bench=False)
    factor_rows_html    = factor_rows_liquido  # keep legacy var name for below

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

    # Rebuild sections_html: briefing comes LAST per fund (user parked it here
    # while its quality is validated — tab stays accessible but not the default).
    _sections_by_fund = {}
    for f, r, h in sections:
        _sections_by_fund.setdefault(f, []).append((r, h))
    _reordered_html = ""
    for f in FUND_ORDER:
        for r, h in _sections_by_fund.get(f, []):
            _reordered_html += (
                f'<div id="sec-{f}-{r}" class="section-wrap" data-fund="{f}" data-report="{r}">{h}</div>'
            )
        if f in _briefing_by_short:
            _reordered_html += (
                f'<div id="sec-{f}-briefing" class="section-wrap" '
                f'data-fund="{f}" data-report="briefing">{_briefing_by_short[f]}</div>'
            )
    sections_html = _reordered_html

    fund_col_headers = "".join(
        f'<th style="text-align:right">{FUND_LABELS.get(f, f)}</th>' for f in FUND_ORDER
    )
    # ── Cross-fund top positions — consolidated list ──────────────────────────
    # One row per (fund, factor, product); exclude via_albatroz to avoid double-counting
    # (those positions are already captured under ALBATROZ direct).
    agg_rows = []
    if rf_expo_maps:
        for short_k, df_k in rf_expo_maps.items():
            if df_k is None or df_k.empty:
                continue
            d = df_k[(df_k["via"] == "direct") & (df_k["factor"].isin(["real", "nominal"]))].copy()
            if d.empty:
                continue
            # ANO_EQ ×  NAV not needed — ano_eq_brl is already signed BRL-years
            for r in d.itertuples(index=False):
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
        fr_nav = _latest_nav("Frontier A\u00e7\u00f5es FIC FI", DATA_STR) or 0
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

    # Bruto total per (fund, factor) for pro-rata bench subtraction in Líquido view.
    fund_factor_gross = {}
    for r in agg_rows:
        key = (r["fund"], r["factor"])
        fund_factor_gross[key] = fund_factor_gross.get(key, 0.0) + r["brl"]

    # Líquido scale factor: (1 − bench/total_fund_factor_gross). Positions that
    # just replicate bench get scaled down pro-rata to their share of fund exposure.
    def _liquido_scale(fund_label, factor_label) -> float:
        short = next((s for s, lbl in FUND_LABELS.items() if lbl == fund_label), fund_label)
        factor_key = factor_label  # already matches bench_matrix keys
        bench = bench_matrix.get(factor_key, {}).get(short, 0.0)
        total = fund_factor_gross.get((fund_label, factor_label), 0.0)
        if abs(total) < 1e-6 or abs(bench) < 1e-6:
            return 1.0
        # Clamp to [0, 1]: bench e total podem ter sinais opostos (convenção)
        # o que causaria scale > 1 e Líquido > Bruto.
        return max(0.0, min(1.0, 1.0 - bench / total))

    def _render_top_rows(liquido: bool, mode_key: str) -> str:
        from collections import defaultdict
        scaled = []
        for r in agg_rows:
            brl = r["brl"]
            if liquido:
                brl = brl * _liquido_scale(r["fund"], r["factor"])
            if abs(brl) < 1_000:
                continue
            rr = dict(r); rr["brl"] = brl
            scaled.append(rr)

        by_fi = defaultdict(list)
        for r in scaled:
            by_fi[(r["factor"], r["product"])].append(r)

        instruments = []
        for (factor, product), rows in by_fi.items():
            total = sum(r["brl"] for r in rows)
            unit  = rows[0]["unit"]
            instruments.append({
                "factor": factor, "product": product, "total": total,
                "unit": unit, "holders": rows,
            })

        # Top 15 instrumentos por |total|
        instruments.sort(key=lambda i: abs(i["total"]), reverse=True)
        instruments = instruments[:15]

        factor_totals = defaultdict(float)
        factor_unit   = {}
        for inst in instruments:
            factor_totals[inst["factor"]] += inst["total"]
            factor_unit[inst["factor"]] = inst["unit"]
        factor_order = sorted(factor_totals.keys(),
                              key=lambda f: abs(factor_totals[f]), reverse=True)

        html = ""
        for factor in factor_order:
            factor_insts = [i for i in instruments if i["factor"] == factor]
            factor_insts.sort(key=lambda i: abs(i["total"]), reverse=True)
            if not factor_insts:
                continue
            ftot = factor_totals[factor]
            ftot_col = "var(--up)" if ftot >= 0 else "var(--down)"
            fpath = f"{mode_key}|{factor}"
            html += (
                f'<tr class="tp-row tp-lvl-0" data-tp-path="{fpath}" '
                f'onclick="toggleTopPos(this)" style="cursor:pointer">'
                f'<td class="sum-fund" style="font-weight:700; letter-spacing:.05em; text-transform:uppercase">'
                f'<span class="tp-caret">▶</span> {factor}</td>'
                f'<td class="mono" style="color:var(--muted); font-size:11px">{len(factor_insts)} instrumentos</td>'
                f'<td class="mono" style="text-align:right; color:{ftot_col}; font-weight:700">{_mm(ftot)}</td>'
                f'<td class="mono" style="color:var(--muted); font-size:10.5px">{factor_unit.get(factor, "")}</td>'
                "</tr>"
            )
            for inst in factor_insts:
                col = "var(--up)" if inst["total"] >= 0 else "var(--down)"
                ipath = f"{mode_key}|{factor}|{inst['product']}"
                expandable = len(inst["holders"]) > 0
                caret_html = '<span class="tp-caret">▶</span> ' if expandable else '  '
                html += (
                    f'<tr class="tp-row tp-lvl-1" data-tp-parent="{fpath}" data-tp-path="{ipath}" '
                    f'onclick="toggleTopPos(this)" style="display:none; cursor:pointer">'
                    f'<td class="sum-fund" style="padding-left:22px">{caret_html}{inst["product"]}</td>'
                    f'<td class="mono" style="color:var(--muted); font-size:11px">{len(inst["holders"])} fundo(s)</td>'
                    f'<td class="mono" style="text-align:right; color:{col}; font-weight:600">{_mm(inst["total"])}</td>'
                    f'<td class="mono" style="color:var(--muted); font-size:10.5px">{inst["unit"]}</td>'
                    "</tr>"
                )
                inst["holders"].sort(key=lambda r: abs(r["brl"]), reverse=True)
                for h in inst["holders"]:
                    hcol = "var(--up)" if h["brl"] >= 0 else "var(--down)"
                    pct = (h["brl"] / inst["total"] * 100) if inst["total"] else 0
                    html += (
                        f'<tr class="tp-row tp-lvl-2" data-tp-parent="{ipath}" style="display:none">'
                        f'<td class="sum-fund" style="padding-left:44px; font-size:11.5px; color:var(--muted)">{h["fund"]}</td>'
                        f'<td class="mono" style="color:var(--muted); font-size:10.5px">{pct:+.0f}% da posição</td>'
                        f'<td class="mono" style="text-align:right; color:{hcol}; font-size:11.5px">{_mm(h["brl"])}</td>'
                        f'<td class="mono" style="color:var(--muted); font-size:10.5px">{h["unit"]}</td>'
                        "</tr>"
                    )
        return html

    top_rows_liquido = _render_top_rows(liquido=True,  mode_key="liq")
    top_rows_bruto   = _render_top_rows(liquido=False, mode_key="bru")

    top_positions_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Top Posições — consolidado</span>
        <span class="card-sub">— top 15 posições da casa por |exposure| · via_albatroz excluído (contado em ALBATROZ direto)</span>
        <div class="pa-view-toggle rf-brl-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-rf-brl="liquido" onclick="selectRfBrl(this,'liquido')">Líquido</button>
          <button class="pa-tgl"        data-rf-brl="bruto"   onclick="selectRfBrl(this,'bruto')">Bruto</button>
        </div>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fator / Instrumento / Fundo</th>
          <th style="text-align:left">Detalhe</th>
          <th style="text-align:right">Total</th>
          <th style="text-align:left">Unidade</th>
        </tr></thead>
        <tbody class="rf-brl-body" data-rf-brl="liquido">{top_rows_liquido}</tbody>
        <tbody class="rf-brl-body" data-rf-brl="bruto" style="display:none">{top_rows_bruto}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:10px; color:var(--muted)">
        Drill-down: clique em um <b>Fator</b> (ex: Real, Nominal, Commodities) para abrir seus instrumentos; clique em um <b>Instrumento</b> para ver em quais fundos está alocado e a %. <b>Líquido</b>: cada contribuição é escalada por (1 − bench/total_fundo_fator); <b>Bruto</b>: sem abater bench. EVOLUTION usa posições diretas (sem look-through); via_albatroz excluído (contado em ALBATROZ direto).
      </div>
    </section>"""

    by_factor_html = f"""
    <section class="card" id="breakdown-por-fator">
      <div class="card-head">
        <span class="card-title">Breakdown por Fator</span>
        <span class="card-sub">— exposure por fator × fundo · <b>Juros Reais/Nominais em anos de duration</b> (yrs); <b>IPCA Idx/Equity/FX/Commodities em %NAV nocional</b></span>
        <div class="pa-view-toggle rf-brl-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-rf-brl="liquido" onclick="selectRfBrl(this,'liquido')">Líquido</button>
          <button class="pa-tgl"        data-rf-brl="bruto"   onclick="selectRfBrl(this,'bruto')">Bruto</button>
        </div>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fator</th>
          {fund_col_headers}
          <th style="text-align:right">Total</th>
        </tr></thead>
        <tbody class="rf-brl-body" data-rf-brl="liquido">{factor_rows_liquido}</tbody>
        <tbody class="rf-brl-body" data-rf-brl="bruto" style="display:none">{factor_rows_bruto}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:10px; color:var(--muted)">
        <b>Líquido</b>: fundo − benchmark (IDKAs menos bench real-rate 3y/10y; Frontier menos IBOV 100% NAV). <b>Bruto</b>: exposição total sem abater bench.
        Cada célula = valor BRL / NAV do fundo × 100. Total = soma BRL / house NAV × 100. Real/Nominal/IPCA em %NAV·ano (yr-eq); Equity/FX/Commodities em %NAV nocional. Cobre IDKAs + Albatroz + Frontier + MACRO + QUANT + EVOLUTION. Crédito omitido por escopo.
      </div>
    </section>"""

    house_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risco VaR e BVaR por fundo</span>
        <span class="card-sub">— {DATA_STR} · VaR 95% 1d absoluto e vs. benchmark · top-5 destacados (🔺 absoluto · 🔷 relativo)</span>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">NAV</th>
          <th style="text-align:right"><span class="kc">VaR</span></th>
          <th style="text-align:right"><span class="kc">BVaR</span></th>
          <th style="text-align:center">Bench</th>
        </tr></thead>
        <tbody>{house_rows_html}</tbody>
        <tfoot>{house_total_row}</tfoot>
      </table>
      <div class="bar-legend" style="margin-top:10px">
        🔺 top-5 por risco absoluto (R$) &nbsp;·&nbsp;
        🔷 top-5 por risco ativo vs. benchmark (R$) &nbsp;·&nbsp;
        <span style="color:var(--muted)">ranking por R$ (não exibido); BVaR para fundos contra CDI ≈ VaR abs (CDI tem vol ≈ 0); Frontier usa HS BVaR vs. IBOV; IDKAs usam BVaR paramétrico do engine. Total = soma simples ponderada por NAV (sem benefício de diversificação).</span>
      </div>
    </section>"""

    fund_grid_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Status consolidado</span>
        <span class="card-sub">— {DATA_STR} · alpha vs. CDI (Frontier: ER vs. IBOV) · utilização de VaR</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="width:60px; text-align:center">Status</th>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">DIA</th>
          <th style="text-align:right">MTD</th>
          <th style="text-align:right">YTD</th>
          <th style="text-align:right">12M</th>
          <th style="text-align:right"><span class="kc">VaR</span></th>
          <th style="text-align:right">Util <span class="kc">VaR</span></th>
          <th style="text-align:right">Δ <span class="kc">VaR</span> D-1</th>
        </tr></thead>
        <tbody>{summary_rows_html}{bench_rows_html}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:12px">
        <span style="color:var(--up)">🟢</span> util &lt; 70% &nbsp;·&nbsp;
        <span style="color:var(--warn)">🟡</span> 70–100% &nbsp;·&nbsp;
        <span style="color:var(--down)">🔴</span> ≥ 100% &nbsp;·&nbsp;
        <span style="color:var(--muted)">Status = utilização de VaR (stop mensal vive no Risk Budget)</span>
      </div>
    </section>"""

    # ── Vol Regime card ──────────────────────────────────────────────────
    # Realized 21d vol (annualized) vs. 3y baseline. Primary signal = pct_rank
    # (non-parametric). Z-score is shown as secondary (N_eff ~36 from 21d
    # overlap → ~15% SE; use for direction, not hard thresholds).
    def _regime_tag(regime: str) -> str:
        colors = {
            "low":      ("var(--up)",     "low"),
            "normal":   ("var(--muted)",  "normal"),
            "elevated": ("var(--warn)",   "elevated"),
            "stressed": ("var(--down)",   "stressed"),
            "—":        ("var(--muted)",  "—"),
        }
        c, lbl = colors.get(regime, ("var(--muted)", regime))
        return f'<span class="mono" style="color:{c}; font-weight:700">{lbl}</span>'

    def _pct_color(p):
        if p is None or pd.isna(p):
            return "var(--muted)"
        if p >= 90: return "var(--down)"
        if p >= 70: return "var(--warn)"
        if p < 20:  return "var(--up)"
        return "var(--text)"

    def _z_color(z):
        if z is None or pd.isna(z):
            return "var(--muted)"
        if abs(z) >= 2: return "var(--down)"
        if abs(z) >= 1: return "var(--warn)"
        return "var(--text)"

    # fund_short -> portfolio key inside PORTIFOLIO_DAILY_HISTORICAL_SIMULATION.
    # Only funds whose W-series exists show vol regime; others fall through to "—".
    _FUND_PORTFOLIO_KEY = {
        "MACRO":     "MACRO",
        "EVOLUTION": "EVOLUTION",
        "QUANT":     "SIST",
    }

    vol_rows_html = ""
    if vol_regime_map:
        for short in FUND_ORDER:
            pkey = _FUND_PORTFOLIO_KEY.get(short)
            r    = vol_regime_map.get(pkey) if pkey else None
            if not r:
                vol_rows_html += (
                    "<tr>"
                    f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:right">—</td>'
                    '<td class="mono" style="color:var(--muted); text-align:center">—</td>'
                    "</tr>"
                )
                continue
            vol_r   = r["vol_recent_pct"]
            vol_f   = r["vol_full_pct"]
            ratio   = r["ratio"]
            pct_v   = r["pct_rank"]
            n_obs   = r["n_obs"]
            vol_rows_html += (
                "<tr>"
                f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
                f'<td class="mono" style="text-align:right">{vol_r:.2f}%</td>'
                f'<td class="mono" style="color:var(--muted); text-align:right">{vol_f:.2f}%</td>'
                f'<td class="mono" style="text-align:right; font-weight:700">{ratio:.2f}x</td>'
                f'<td class="mono" style="color:{_pct_color(pct_v)}; text-align:right; font-weight:700">'
                f'{pct_v:.0f}°</td>'
                f'<td style="text-align:center">{_regime_tag(r["regime"])}</td>'
                "</tr>"
            )

    vol_regime_html = ""
    if vol_rows_html and any(v for v in (vol_regime_map or {}).values()):
        vol_regime_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Vol Regime</span>
        <span class="card-sub">— carteira atual · vol 21d vs. janela total HS</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">Vol 21d</th>
          <th style="text-align:right">Vol full</th>
          <th style="text-align:right">Ratio</th>
          <th style="text-align:right">Pct</th>
          <th style="text-align:center">Regime</th>
        </tr></thead>
        <tbody>{vol_rows_html}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:12px">
        Carteira atual aplicada à janela de simulação histórica (W series). ·
        <b>Vol 21d</b> = std(W dos 21 dias mais recentes) × √252 ·
        <b>Vol full</b> = std(W da janela completa) × √252 ·
        <b>Ratio</b> = vol 21d / vol full ·
        <b>Pct</b> = percentil de std(21d) entre todas as janelas rolling de 21d ·
        <b>Regime</b>: low &lt;p20 · normal p20–70 · elevated p70–90 · stressed ≥p90
      </div>
    </section>"""

    # Top movers — DIA, with toggle Por Livro / Por Classe
    # Caixa / Custos livros e classes são operacionais (não-direcionais) — excluídos.
    _MOVERS_EXCLUDE = {
        "LIVRO":  {"Taxas e Custos", "Caixa", "Caixa USD"},
        "CLASSE": {"Custos", "Caixa"},
    }

    def _movers_rows(group_col: str) -> str:
        if df_pa is None or df_pa.empty:
            return ""
        exclude = _MOVERS_EXCLUDE.get(group_col, set())
        rows_html = ""
        for short in FUND_ORDER:
            pa_key = _FUND_PA_KEY.get(short)
            if not pa_key: continue
            sub = df_pa[df_pa["FUNDO"] == pa_key]
            if sub.empty: continue
            sub = sub[~sub[group_col].isin(exclude)]
            by = sub.groupby(group_col)["dia_bps"].sum()
            by = by[by.abs() > 0.5]
            pos = by[by > 0].sort_values(ascending=False).head(3)
            neg = by[by < 0].sort_values().head(3)
            render = _pa_render_name if group_col == "LIVRO" else (lambda s: s)
            pos_txt = " · ".join(
                f'<span style="color:var(--up)">{render(l)} +{v/100:.2f}%</span>'
                for l, v in pos.items()
            ) or '<span style="color:var(--muted)">—</span>'
            neg_txt = " · ".join(
                f'<span style="color:var(--down)">{render(l)} {v/100:.2f}%</span>'
                for l, v in neg.items()
            ) or '<span style="color:var(--muted)">—</span>'
            rows_html += (
                "<tr>"
                f'<td class="sum-fund">{FUND_LABELS.get(short, short)}</td>'
                f'<td class="sum-movers">{pos_txt}</td>'
                f'<td class="sum-movers">{neg_txt}</td>'
                "</tr>"
            )
        return rows_html

    livro_rows  = _movers_rows("LIVRO")
    classe_rows = _movers_rows("CLASSE")

    def _movers_table(view_id, rows, active):
        style = "" if active else ' style="display:none"'
        return f"""
        <table class="summary-movers mov-view" data-mov-view="{view_id}"{style}>
          <thead><tr>
            <th style="text-align:left; width:120px">Fundo</th>
            <th style="text-align:left">Contribuintes</th>
            <th style="text-align:left">Detratores</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>"""

    movers_html = (f"""
    <section class="card sum-movers-card">
      <div class="card-head">
        <span class="card-title">Top Movers — DIA</span>
        <span class="card-sub">— alpha do dia (drop &lt; 0.5 bps) · Taxas/Custos excluídos</span>
        <div class="pa-view-toggle sum-tgl">
          <button class="pa-tgl active" data-mov-view="livro"
                  onclick="selectMoversView(this,'livro')">Por Livro</button>
          <button class="pa-tgl" data-mov-view="classe"
                  onclick="selectMoversView(this,'classe')">Por Classe</button>
        </div>
      </div>
      {_movers_table("livro",  livro_rows,  True)}
      {_movers_table("classe", classe_rows, False)}
    </section>""") if (livro_rows or classe_rows) else ""

    # Significant position changes — one card with sub-blocks per fund.
    # MACRO uses PM × factor detail (richer data already available).
    # Other funds use factor-level (PRODUCT_CLASS → grouped) from LOTE_PRODUCT_EXPO.
    changes_html = ""
    THRESHOLD_PP = 0.30

    def _delta_pp(v):
        color = "var(--up)" if v > 0 else "var(--down)"
        sign = "+" if v > 0 else ""
        return f'<span style="color:{color}; font-weight:700">{sign}{v:.2f} pp</span>'

    change_blocks = []

    # MACRO — PM × factor
    if df_expo is not None and df_expo_d1 is not None:
        try:
            d0 = df_expo.groupby(["pm", "rf"])["pct_nav"].sum().reset_index()
            d1 = df_expo_d1.groupby(["pm", "rf"])["pct_nav"].sum().reset_index().rename(
                columns={"pct_nav": "pct_d1"}
            )
            chg = d0.merge(d1, on=["pm", "rf"], how="outer").fillna(0.0)
            chg["delta"] = chg["pct_nav"] - chg["pct_d1"]
            chg = chg[chg["delta"].abs() >= THRESHOLD_PP]
            chg = chg.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(8)
            if not chg.empty:
                rows = "".join(
                    f"<tr>"
                    f'<td class="sum-fund" style="width:70px">{r.pm}</td>'
                    f'<td class="mono" style="color:var(--muted)">{r.rf}</td>'
                    f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                    f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_nav:+.2f}%</td>'
                    f'<td class="mono" style="text-align:right">{_delta_pp(r.delta)}</td>'
                    f"</tr>"
                    for r in chg.itertuples(index=False)
                )
                change_blocks.append(f"""
                <div class="comment-fund">
                  <div class="comment-title">Macro · PM × fator</div>
                  <table class="summary-movers">
                    <thead><tr>
                      <th style="text-align:left; width:60px">PM</th>
                      <th style="text-align:left">Fator</th>
                      <th style="text-align:right">D-1</th>
                      <th style="text-align:right">D-0</th>
                      <th style="text-align:right">Δ</th>
                    </tr></thead>
                    <tbody>{rows}</tbody>
                  </table>
                </div>""")
        except Exception as e:
            print(f"  changes (MACRO) block failed ({e})")

    # QUANT / EVOLUTION / MACRO_Q / ALBATROZ / FRONTIER — factor-level
    if position_changes:
        for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER"):
            df_chg = position_changes.get(short)
            if df_chg is None or df_chg.empty:
                continue
            big = df_chg[df_chg["delta"].abs() >= THRESHOLD_PP]
            big = big.sort_values("delta", key=lambda s: s.abs(), ascending=False).head(6)
            if big.empty:
                continue
            rows = "".join(
                f"<tr>"
                f'<td class="mono" style="color:var(--muted)">{r.factor}</td>'
                f'<td class="mono" style="text-align:right; color:var(--text); opacity:.75">{r.pct_d1:+.2f}%</td>'
                f'<td class="mono" style="text-align:right; color:var(--text)">{r.pct_d0:+.2f}%</td>'
                f'<td class="mono" style="text-align:right">{_delta_pp(r.delta)}</td>'
                f"</tr>"
                for r in big.itertuples(index=False)
            )
            change_blocks.append(f"""
            <div class="comment-fund">
              <div class="comment-title">{FUND_LABELS.get(short, short)} · por fator</div>
              <table class="summary-movers">
                <thead><tr>
                  <th style="text-align:left">Fator</th>
                  <th style="text-align:right">D-1</th>
                  <th style="text-align:right">D-0</th>
                  <th style="text-align:right">Δ</th>
                </tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </div>""")

    if change_blocks:
        changes_html = f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Mudanças Significativas</span>
            <span class="card-sub">— exposição D-0 vs D-1 · |Δ| ≥ {THRESHOLD_PP:.2f} pp · MACRO por PM×fator, outros por fator agregado</span>
          </div>
          <div class="comments-grid">{''.join(change_blocks)}</div>
        </section>"""

    # Comments — outlier detection per product (|z|≥2σ vs 90d + |bps|≥3)
    comments_html = ""
    if df_pa_daily is not None and not df_pa_daily.empty:
        try:
            outliers = compute_pa_outliers(df_pa_daily, DATA_STR, z_min=2.0, bps_min=3.0, min_obs=20)
            body_chunks = []
            for short in FUND_ORDER:
                pa_key = _FUND_PA_KEY.get(short)
                if not pa_key: continue
                sub = outliers[outliers["FUNDO"] == pa_key] if not outliers.empty else outliers
                label = FUND_LABELS.get(short, short)
                if sub is None or sub.empty:
                    body_chunks.append(
                        f'<div class="comment-fund">'
                        f'<div class="comment-title">{label}</div>'
                        f'<div class="comment-empty">— sem eventos significativos</div>'
                        f'</div>'
                    )
                else:
                    items = ""
                    for r in sub.itertuples(index=False):
                        color = "var(--up)" if r.today_bps > 0 else "var(--down)"
                        livro_disp = _pa_render_name(r.LIVRO)
                        items += (
                            f'<li>'
                            f'<span class="mono" style="font-weight:700">{r.PRODUCT}</span> '
                            f'<span class="mono" style="color:{color}">{r.today_bps:+.1f} bps</span> '
                            f'<span class="mono" style="color:var(--muted)">({r.z:+.1f}σ)</span> '
                            f'&nbsp;·&nbsp; <span style="color:var(--muted)">{livro_disp}</span>'
                            f'</li>'
                        )
                    body_chunks.append(
                        f'<div class="comment-fund">'
                        f'<div class="comment-title">{label}</div>'
                        f'<ul class="comment-list">{items}</ul>'
                        f'</div>'
                    )
            comments_html = f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Comments — Outliers do dia</span>
                <span class="card-sub">— produtos com |z| ≥ 2σ (vs. 90d) e |contrib| ≥ 3 bps · ignora Caixa/Custos</span>
              </div>
              <div class="comments-grid">{''.join(body_chunks)}</div>
            </section>"""
        except Exception as e:
            print(f"  comments block failed ({e})")

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
        ibov=ibov, cdi=cdi,
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

    # (Análise helpers and per-fund loop were relocated above `sections_html`; see earlier block.)

    # Mode switcher + sub-tabs
    mode_tabs_html = (
        '<button class="mode-tab" data-mode="summary" onclick="selectMode(\'summary\')">Summary</button>'
        '<button class="mode-tab" data-mode="fund"    onclick="selectMode(\'fund\')">Por Fundo</button>'
        '<button class="mode-tab" data-mode="report"  onclick="selectMode(\'report\')">Por Report</button>'
        '<button class="mode-tab" data-mode="quality" onclick="selectMode(\'quality\')" style="opacity:0.55;font-size:11px">Qualidade</button>'
    )
    report_subtabs_html = "".join(
        f'<button class="tab" data-target="{rid}" onclick="selectReport(\'{rid}\')">{label}</button>'
        for rid, label in REPORTS if rid in reports_with_data
    )
    fund_subtabs_html = "".join(
        f'<button class="tab" data-target="{s}" onclick="selectFund(\'{s}\')">{s}</button>'
        for s in funds_with_data
    )
    # JS constant: which reports exist per fund (for the jump bar)
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

    # Por Report mode removed — no per-report fund switcher needed

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
<style>
  :root {{
    --bg:#0b0d10;
    --bg-2:#111418;
    --panel:#14181d;
    --panel-2:#181d24;
    --line:#232a33;
    --line-2:#2d3540;
    --text:#e7ecf2;
    --muted:#8892a0;
    --accent:#0071BB;
    --accent-2:#1a8fd1;
    --accent-deep:#183C80;
    --up:#26d07c;
    --down:#ff5a6a;
    --warn:#f5c451;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{
    background: var(--bg); color: var(--text);
    font-family: 'Inter', system-ui, sans-serif;
    -webkit-font-smoothing: antialiased;
  }}
  h1, h2, .card-title, .modal-title, .brand h1 {{
    font-family: 'Gadugi', 'Inter', system-ui, sans-serif;
  }}
  body {{
    min-height:100vh;
    background:
      radial-gradient(1200px 600px at 10% -10%, rgba(0,113,187,.06), transparent 60%),
      radial-gradient(900px 500px at 110% 10%, rgba(26,143,209,.04), transparent 60%),
      var(--bg);
  }}
  .mono {{ font-family:'JetBrains Mono', ui-monospace, monospace; font-variant-numeric: tabular-nums; }}

  /* Header */
  header {{
    position:sticky; top:0; z-index:50;
    backdrop-filter:blur(14px);
    background:rgba(11,13,16,.78);
    border-bottom:1px solid var(--line);
  }}
  .hwrap {{
    max-width:1280px; margin:0 auto; padding:14px 22px;
    display:flex; align-items:center; gap:22px; flex-wrap:wrap;
  }}
  .brand {{ display:flex; align-items:center; gap:12px; }}
  .logo {{
    width:38px; height:38px; border-radius:10px;
    background:linear-gradient(135deg,#0071BB 0%,#183C80 100%);
    display:grid; place-items:center; overflow:hidden; flex-shrink:0;
    box-shadow:0 0 0 1px rgba(0,113,187,.35), 0 8px 24px -8px rgba(0,113,187,.4);
  }}
  .logo svg {{ width:26px; height:26px; }}
  .brand h1 {{ font-size:16px; margin:0; letter-spacing:.3px; font-weight:700; }}
  .brand p {{ margin:0; font-size:11px; color:var(--muted); letter-spacing:.18em; text-transform:uppercase; }}

  .tabs, .sub-tabs {{
    display:flex; gap:4px;
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:4px;
  }}
  .tab, .mode-tab {{
    padding:7px 14px; font-size:12.5px; font-weight:600;
    color:var(--muted); background:transparent; border:0; border-radius:9px;
    cursor:pointer; transition:all .18s ease; letter-spacing:.05em;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }}
  .tab:hover, .mode-tab:hover {{ color:var(--text); }}
  .tab.active, .mode-tab.active {{ color:#fff; background:linear-gradient(180deg, var(--accent-2), var(--accent)); }}
  .mode-switcher {{
    display:flex; gap:4px;
    background:var(--panel-2); border:1px solid var(--line);
    border-radius:12px; padding:4px;
  }}
  .sub-tabs {{ display:none; }}
  .sub-tabs.active {{ display:flex; }}
  .navrow {{
    max-width:1280px; margin:0 auto; padding:8px 22px;
    display:flex; align-items:center; gap:14px; flex-wrap:wrap;
  }}
  .navrow-label {{
    font-size:10px; color:var(--muted); letter-spacing:.18em; text-transform:uppercase;
  }}
  /* Report jump bar — shown below fund tabs in Por Fundo mode */
  #report-jump-bar {{
    display:none; gap:4px; flex-wrap:wrap; align-items:center;
    padding:6px 22px; max-width:1280px; margin:0 auto;
    border-top:1px solid rgba(255,255,255,0.04);
  }}
  body[data-mode="fund"] #report-jump-bar {{ display:flex; }}
  .jump-btn {{
    padding:4px 11px; font-size:11px; font-weight:600;
    color:var(--muted); background:transparent; border:1px solid var(--line);
    border-radius:7px; cursor:pointer; transition:all .15s ease;
    font-family:'Gadugi','Inter',system-ui,sans-serif; letter-spacing:.03em;
  }}
  .jump-btn:hover {{ color:var(--text); border-color:rgba(255,255,255,0.18); }}
  .jump-btn.active-jump {{ color:var(--accent-2); border-color:var(--accent-2); }}

  .controls {{ margin-left:auto; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
  .ctrl-group {{
    display:flex; align-items:center; gap:8px;
    background:var(--panel); border:1px solid var(--line);
    border-radius:10px; padding:6px 10px;
  }}
  .ctrl-group label {{
    font-size:10px; color:var(--muted); letter-spacing:.14em; text-transform:uppercase;
  }}
  .ctrl-group input[type=date] {{
    background:transparent; border:0; color:var(--text);
    font-family:'JetBrains Mono', monospace; font-size:12px;
    color-scheme:dark; outline:none;
  }}
  .btn-primary {{
    padding:8px 14px; border-radius:10px; border:1px solid var(--accent);
    background:linear-gradient(180deg, var(--accent-2), var(--accent));
    color:#fff; font-weight:600; font-size:11.5px; cursor:pointer;
    letter-spacing:.05em; text-transform:uppercase;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }}
  .btn-primary:hover {{ filter:brightness(1.08); }}
  .btn-accent {{
    padding:8px 14px; border-radius:10px; border:1px solid var(--accent);
    background:rgba(0,113,187,.12); color:var(--accent-2);
    font-weight:600; font-size:12px; cursor:pointer; letter-spacing:.04em;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }}
  .btn-accent:hover {{ background:rgba(0,113,187,.22); color:#fff; }}
  .btn-pdf {{
    padding:7px 12px; border-radius:9px; border:1px solid var(--line-2);
    background:var(--panel-2); color:var(--muted);
    font-weight:600; font-size:11px; cursor:pointer; letter-spacing:.04em;
    margin-left:6px;
  }}
  .btn-pdf:hover {{ border-color:var(--accent); color:var(--text); background:rgba(0,113,187,.10); }}

  /* Print / PDF export — remap CSS variables to a light palette so all
     inline `color:var(--up)` / backgrounds switch to paper-friendly tones. */
  @media print {{
    :root {{
      --bg:        #ffffff;
      --bg-2:      #ffffff;
      --panel:     #ffffff;
      --panel-2:   #f2f4f7;
      --line:      #d0d5dd;
      --line-2:    #b0b5c0;
      --text:      #111111;
      --muted:     #555555;
      --accent:    #003d5c;
      --accent-2:  #004a70;
      --up:        #0e7a32;
      --down:      #a8001a;
      --warn:      #8a6500;
    }}
    @page {{ size: A4 landscape; margin: 10mm 8mm; }}
    html, body {{
      background: #ffffff !important;
      color: #111111 !important;
      font-size: 11pt;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}
    .no-print, header .controls, header .mode-switcher,
    .navrow, .sub-tabs, #empty-state,
    .sn-switcher, .report-fund-switcher {{
      display: none !important;
    }}
    header {{
      padding: 4px 0 6px; border-bottom: 1px solid #000; margin-bottom: 6px;
      background: #ffffff !important;
    }}
    header h1 {{ font-size: 14pt !important; color: #000 !important; }}
    header p  {{ font-size: 9pt  !important; color: #333 !important; }}
    header .logo svg path {{ fill: #000 !important; }}
    main {{ padding: 0 !important; max-width: none !important; margin: 0 !important; }}

    .card, section.card {{
      background: #ffffff !important;
      border: 1px solid #999 !important;
      box-shadow: none !important;
      page-break-inside: avoid;
      margin-bottom: 8px !important;
      padding: 8px 10px !important;
    }}
    .card-head   {{ padding-bottom: 4px !important; margin-bottom: 6px !important; border-bottom: 1px solid #ddd !important; }}
    .card-title  {{ color: #000 !important; font-size: 11pt !important; letter-spacing: .1em !important; font-weight: 700 !important; }}
    .card-sub    {{ color: #444 !important; font-size: 9pt  !important; }}

    table {{ background: #ffffff !important; }}
    tbody tr {{ background: #ffffff !important; }}
    th {{
      font-size: 8.5pt !important;
      background: #f2f4f7 !important;
      color: #333 !important;
    }}
    td {{ font-size: 10pt !important; }}
    .mono {{ font-family: 'Courier New', monospace; }}

    /* Denser tables for paper */
    .summary-table td, .summary-table th,
    .summary-movers td, .summary-movers th,
    .pa-table td,      .pa-table th {{
      padding: 4px 7px !important;
      font-size: 9.5pt !important;
    }}
    .summary-table td.sum-fund {{ font-size: 10pt !important; color: #000 !important; font-weight: 700; }}

    .alert-item   {{ padding: 6px 8px !important; font-size: 9.5pt !important; background: #fff4d6 !important; border-left: 3px solid #a37500 !important; }}
    .comment-fund {{ padding: 6px 8px !important; font-size: 9.5pt !important; background: #fafbfc !important; border: 1px solid #d0d5dd !important; }}
    .comment-list li {{ font-size: 9pt !important; line-height: 1.5 !important; }}
    .comment-empty   {{ color: #777 !important; }}

    .section-wrap {{ page-break-inside: auto; }}
    .pa-sort-arrow {{ display: none !important; }}

    /* Keep heatmap very subtle so numbers remain readable */
    .pa-table td[style*="background:rgba(38,208"] {{ background: #e8f6ed !important; }}
    .pa-table td[style*="background:rgba(255,90"] {{ background: #fae9ec !important; }}

    .subtitle {{ font-size: 9pt !important; margin: 2px 0 6px !important; color: #333 !important; }}
  }}
  /* print-full: show all section-wraps even if filtered out by mode/fund/report */
  body.print-full .section-wrap {{ display: block !important; }}
  body.print-full .sn-switcher,
  body.print-full .report-fund-switcher {{ display: none !important; }}

  .date-hint {{ font-size:9px; color:var(--muted); margin-left:6px; }}
  .subtitle {{
    max-width:1280px; margin:10px auto 0; padding:0 22px;
    font-size:11px; color:var(--muted);
  }}

  /* Main */
  main {{ max-width:1280px; margin:0 auto; padding:18px 22px 40px; }}
  .section-wrap {{ display:block; }}

  /* ── Mobile responsive — viewport ≤ 768px ────────────────────────────── */
  @media (max-width: 768px) {{
    main {{ padding: 12px 10px 28px; }}
    .card {{ padding: 10px 12px; }}
    /* Wide tables: horizontal scroll instead of page overflow */
    .card table, .summary-table {{
      display: block; overflow-x: auto; white-space: nowrap;
      -webkit-overflow-scrolling: touch;
    }}
    .card-head {{ flex-wrap: wrap; gap: 6px; }}
    .card-title {{ font-size: 14px; }}
    .card-sub {{ font-size: 11px; }}
    /* Chip bars wrap to multiple lines */
    .fund-nav-chips, .mode-switcher, .sub-tabs {{ flex-wrap: wrap; }}
    /* Smaller monospace for dense tables */
    .mono {{ font-size: 11px; }}
    /* Hide decorative elements in header */
    header .brand p {{ display: none; }}
    header h1 {{ font-size: 14px; }}
  }}
  @media (max-width: 480px) {{
    main {{ padding: 8px 6px 24px; }}
    .card {{ padding: 8px 8px; border-radius: 6px; }}
    .card-title {{ font-size: 13px; }}
    .mono {{ font-size: 10px; }}
  }}
  .empty-view {{ padding:28px; text-align:center; color:var(--muted); font-size:12px; }}
  #empty-state {{
    padding:40px 16px; text-align:center; color:var(--muted); font-size:13px;
    border:1px dashed var(--line); border-radius:12px; margin-top:12px;
  }}

  /* legacy placeholder — report mode removed */
  body[data-mode="UNUSED"] #sections-container {{
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 12px; padding: 4px 18px; margin-top: 4px;
  }}
  body[data-mode="UNUSED"] #sections-container .section-wrap > .card {{
    background: transparent; border: 0;
    padding: 14px 0 16px; margin-bottom: 0;
    border-bottom: 1px solid var(--line);
    border-radius: 0;
  }}
  body[data-mode="UNUSED"] #sections-container .section-wrap:last-of-type > .card,
  body[data-mode="UNUSED"] #sections-container .section-wrap > .card:last-child {{
    border-bottom: 0;
  }}

  /* Distribution 252d table */
  .dist-table td, .dist-table th {{ vertical-align:middle; padding:7px 10px; }}
  .dist-table .dist-tag  {{ font-size:9px; font-weight:700; letter-spacing:1px; width:54px; }}
  .dist-table .dist-name {{ font-size:12px; color:var(--text); font-weight:500; }}
  .dist-table .dist-num  {{ font-size:12px; text-align:right; font-variant-numeric: tabular-nums; }}
  .dist-table .metric-row:hover {{ background:var(--panel-2); }}
  .dist-toggle {{ display:inline-flex; gap:2px; background:var(--panel-2); border:1px solid var(--line); border-radius:8px; padding:3px; }}
  .dist-btn, .dist-bench-btn {{ padding:5px 12px; font-size:11px; font-weight:600; color:var(--muted); background:transparent; border:0; border-radius:6px; cursor:pointer; letter-spacing:.04em; font-family:'Gadugi','Inter',system-ui,sans-serif; }}
  .dist-btn:hover, .dist-bench-btn:hover {{ color:var(--text); }}
  .dist-btn.active, .dist-bench-btn.active {{ color:#fff; background:linear-gradient(180deg,var(--accent-2),var(--accent)); }}
  .dist-bench-btn:disabled {{ opacity:.35; cursor:default; }}
  .dist-view {{ margin-top:10px; }}

  /* CSV export button (injected into each card-head) */
  .btn-csv {{
    margin-left: auto; padding: 4px 10px;
    font-size: 10px; font-weight: 600; letter-spacing: .06em;
    color: var(--muted); background: var(--panel-2);
    border: 1px solid var(--line); border-radius: 6px;
    cursor: pointer; font-family: 'Gadugi','Inter',system-ui,sans-serif;
    transition: all .15s ease;
  }}
  .btn-csv:hover {{ color: var(--text); border-color: var(--accent-2); background: rgba(0,113,187,.12); }}
  .card-head {{ gap: 8px; }}

  /* Stop table (Risk Budget Monitor) — widths tuned so the 300px SVG fits */
  .stop-table {{ table-layout: fixed; }}
  .stop-table td, .stop-table th {{ vertical-align: middle; white-space: nowrap; }}
  .stop-table th        {{ padding:6px 12px; }}
  .stop-table .pm-name   {{ font-size:12.5px; color:var(--text); padding:8px 12px; width:160px; font-weight:500; }}
  .stop-table .pm-margem {{ font-size:20px;   font-weight:700;  width:90px;  text-align:right; padding:8px 12px; font-variant-numeric: tabular-nums; }}
  .stop-table .bar-cell  {{ width:320px; padding:8px 10px; }}
  .stop-table .pm-hist   {{ font-size:11px;   width:170px; text-align:right; line-height:1.6; padding:8px 12px; font-variant-numeric: tabular-nums; }}
  .stop-table .pm-status {{ font-size:12px;   font-weight:600;  width:110px; padding:8px 12px; }}
  .stop-table .spark-cell {{ padding:2px 6px; width:auto; }}

  /* Cards */
  .card {{
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:18px 20px; margin-bottom:18px;
  }}
  .card-head {{
    display:flex; align-items:baseline; gap:8px;
    padding-bottom:10px; margin-bottom:10px;
    border-bottom:1px solid var(--line);
  }}
  .card-title {{
    font-size:11px; letter-spacing:.18em; text-transform:uppercase;
    color:var(--text); font-weight:700;
  }}
  .card-sub {{ font-size:11px; color:var(--muted); letter-spacing:.05em; }}
  .card-sub .fund-name {{
    color:var(--accent); font-weight:700; letter-spacing:.08em;
    padding:1px 6px; border-radius:4px;
    background:rgba(26,143,209,0.10); border:1px solid rgba(26,143,209,0.25);
  }}

  /* Utility: keep mixed-case inside uppercase-transformed parents (e.g. "VaR" in th) */
  .kc {{ text-transform:none !important; }}

  /* Frontier PA card — nested hierarchical PA (sub-tab) gets flush styling */
  .fpa-pa-nested > section.card {{
    background:transparent !important; border:none !important;
    padding:0 !important; margin:0 !important; box-shadow:none !important;
  }}
  .fpa-pa-nested > section.card > .card-head {{ padding:0 0 8px !important; }}

  /* Per-fund nav chips — only visible in "Por Report" mode.
     Click scrolls within the currently selected report to the chosen fund's
     section-wrap. In "Por Fundo" mode the user is already viewing one fund, so
     the chip bar would be redundant / confusing (and used to switch funds).
     Sticky below the header: as you scroll past one fund section the next
     section's chip bar naturally takes over (each bar highlights its own fund). */
  .fund-nav-chips {{
    position:sticky; top:var(--header-h, 72px); z-index:40;
    display:flex; flex-wrap:wrap; gap:6px;
    padding:8px 12px; margin-bottom:14px;
    background:rgba(17,20,26,.92); backdrop-filter:blur(8px);
    border:1px solid var(--line); border-radius:8px;
    font-size:11px; letter-spacing:.03em;
    box-shadow:0 4px 14px -8px rgba(0,0,0,.55);
  }}
  body:not([data-mode="report"]) .fund-nav-chips {{ display:none; }}
  .fund-nav-chips .chip-label {{
    color:var(--muted); text-transform:uppercase; letter-spacing:.12em;
    font-size:10px; padding:4px 6px; align-self:center;
  }}
  .fund-nav-chips .chip {{
    background:transparent; color:var(--muted);
    border:1px solid var(--line); border-radius:4px;
    padding:4px 10px; cursor:pointer; font-family:inherit;
    transition:all .12s ease;
  }}
  .fund-nav-chips .chip:hover {{ color:var(--text); border-color:var(--accent); }}
  .fund-nav-chips .chip.active {{
    background:var(--accent); color:#0b1220;
    border-color:var(--accent); font-weight:700;
  }}

  table {{ width:100%; border-collapse:collapse; }}
  .col-headers th {{
    font-size:10px; color:var(--muted); letter-spacing:1.5px; text-transform:uppercase;
    padding:6px 12px; text-align:left; border-bottom:1px solid var(--line);
    font-weight:500;
  }}
  .metric-row td {{ padding:6px 12px; vertical-align:middle; }}
  .metric-name {{ font-size:12px; color:var(--muted); width:120px; }}
  .value-cell {{ font-size:20px; font-weight:700; width:80px; text-align:right; }}
  .bar-cell {{ width:260px; padding:4px 16px; }}
  .util-cell {{ font-size:12px; color:var(--muted); width:90px; text-align:right; }}
  .spark-cell {{ width:180px; padding:2px 8px; }}
  .metric-row:hover {{ background:var(--panel-2); }}
  .bar-legend {{ margin-top:10px; font-size:10px; color:var(--muted); line-height:1.8; }}
  .tick {{ color:#fb923c; font-size:9px; }}

  .sum-movers-card .card-head {{ display:flex; flex-wrap:wrap; align-items:center; gap:12px; }}
  .sum-tgl {{ margin-left:auto; }}
  .rf-view-toggle {{ margin-left:auto; }}

  /* RF Exposure Map chart */
  .rf-expo-svg {{ display:block; margin:0 auto; }}
  .rf-expo-svg .rf-bar {{ stroke:none; opacity:.92; }}
  .rf-expo-svg .rf-real {{ fill:#f59e0b; }}          /* amber — real/IPCA */
  .rf-expo-svg .rf-nom  {{ fill:#14b8a6; }}          /* teal — nominal/pré */
  .rf-expo-svg .rf-benchbar {{ fill:#64748b; opacity:.9; }} /* slate — benchmark */
  .rf-expo-svg .rf-cum-bench {{ fill:none; stroke:#64748b; stroke-width:1.6; stroke-dasharray:4 3; }}
  .rf-expo-svg .rf-cum  {{ fill:none; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }}
  .rf-expo-svg .rf-cum-real {{ stroke:#b45309; stroke-dasharray:0; }}
  .rf-expo-svg .rf-cum-nom  {{ stroke:#0f766e; stroke-dasharray:0; }}
  .rf-expo-svg .rf-bench {{ fill:none; stroke:#64748b; stroke-width:1.4; stroke-dasharray:4 3; }}
  .rf-expo-svg .rf-gap {{ fill:none; stroke:#1a8fd1; stroke-width:2.2; }}
  .rf-expo-svg .rf-grid {{ stroke:var(--line); stroke-width:.7; opacity:.4; }}
  .rf-expo-svg .rf-zero {{ stroke:var(--muted); stroke-width:1; opacity:.6; }}
  .rf-expo-svg .rf-axis-lbl {{ fill:var(--muted); font-size:10.5px; font-family:'JetBrains Mono', monospace; }}
  .rf-expo-svg .rf-bench-marker {{ stroke:#f97316; stroke-width:2.2; stroke-dasharray:6 4; opacity:1; }}
  .rf-expo-svg .rf-bench-marker-lbl {{ fill:#f97316; font-weight:700; font-size:11px; }}
  .rf-expo-svg .rf-bench-marker-lbl-bg {{ fill:var(--bg); opacity:.85; }}

  .rf-legend {{ display:flex; flex-wrap:wrap; justify-content:center; gap:16px; align-items:center; }}
  .rf-legend-item {{ display:inline-flex; align-items:center; gap:5px; }}
  .rf-swatch {{ display:inline-block; width:12px; height:10px; border-radius:2px; vertical-align:middle; }}
  .rf-swatch.rf-real {{ background:#f59e0b; }}
  .rf-swatch.rf-nom  {{ background:#14b8a6; }}
  .rf-swatch.rf-benchbar {{ background:#64748b; }}
  .rf-swatch.rf-cum  {{ background:#b45309; height:2px; border-radius:0; }}
  .rf-swatch.rf-bench {{ background:transparent; border-top:2px dashed #64748b; height:0; width:14px; border-radius:0; }}
  .rf-swatch.rf-gap  {{ background:#1a8fd1; height:2px; border-radius:0; }}
  .rf-tbl-toggle {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:4px 10px; border-radius:6px; font-size:11px; cursor:pointer;
    font-family:'Inter', sans-serif; letter-spacing:.04em;
  }}
  .rf-tbl-toggle:hover {{ background:var(--panel-2); color:var(--text); }}

  /* Executive briefing (top of Summary) */
  .brief-card {{ border-left:3px solid var(--accent); }}
  .brief-headline {{
    font-size:16px; font-weight:600; color:var(--text);
    padding:10px 14px; margin:6px 0 12px; line-height:1.45;
    background:rgba(0,113,187,.06); border-radius:6px;
    border-left:2px solid var(--accent-2);
  }}
  .brief-benchmarks {{
    font-size:11px; color:var(--muted); margin-bottom:14px;
    letter-spacing:.04em; padding-left:4px;
  }}
  .brief-grid {{
    display:grid; grid-template-columns:1fr 1fr; gap:24px;
  }}
  @media (max-width:900px) {{ .brief-grid {{ grid-template-columns:1fr; gap:16px; }} }}
  .brief-col {{ min-width:0; }}
  .brief-section-title {{
    font-size:10px; letter-spacing:.14em; text-transform:uppercase;
    color:var(--accent-2); font-weight:700; margin:12px 0 6px;
    padding-bottom:4px; border-bottom:1px solid var(--line);
  }}
  .brief-list {{ list-style:none; margin:0; padding:0; font-size:12.5px; line-height:1.55; }}
  .brief-list li {{ padding:5px 0; border-bottom:1px dashed var(--line); }}
  .brief-list li:last-child {{ border-bottom:none; }}
  .brief-list li b {{ color:var(--text); font-weight:600; }}
  .brief-list li i {{ color:var(--muted); font-style:normal; }}
  .brief-annex {{
    margin-top:14px; padding-top:10px; border-top:1px solid var(--line);
    font-size:11px; color:var(--muted);
  }}
  .brief-annex a {{ color:var(--accent-2); text-decoration:none; border-bottom:1px dotted var(--accent-2); }}
  .brief-annex a:hover {{ color:var(--text); border-bottom-color:var(--text); }}
  .brief-commentary {{
    font-size:13px; color:var(--text); line-height:1.65;
    padding:10px 14px; margin:0 0 14px;
    background:rgba(26,143,209,.04); border-left:2px solid var(--accent-2);
    border-radius:6px;
  }}
  .brief-commentary b {{ color:var(--text); font-weight:600; }}

  /* PA Contribuições — Por Tamanho / Por Fundo grid (flows side-by-side) */
  .pa-alert-view {{
    display:grid;
    grid-template-columns:repeat(auto-fill, minmax(280px, 1fr));
    gap:10px;
  }}
  .pa-alert-view-hidden {{ display:none; }}

  /* Evolution Diversification — nested layer cards flattened inside unified card */
  .evo-div-body section.card {{
    border: 0 !important;
    background: transparent !important;
    padding: 4px 0 0 !important;
    box-shadow: none !important;
    margin-bottom: 0 !important;
  }}
  .evo-div-body .card-head {{
    border-bottom: 1px solid var(--line);
    padding-bottom: 8px;
    margin-bottom: 10px;
  }}

  /* Stop history modal — child window triggered from Risk Budget Monitor */
  .stop-modal {{
    position:fixed; inset:0; z-index:9999;
    display:flex; align-items:center; justify-content:center;
  }}
  .stop-modal-overlay {{
    position:absolute; inset:0; background:rgba(0,0,0,0.72);
    backdrop-filter: blur(2px);
  }}
  .stop-modal-box {{
    position:relative; z-index:1; max-width:860px; width:92%;
    max-height:85vh; overflow-y:auto;
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:18px 22px 16px;
    box-shadow:0 16px 48px rgba(0,0,0,0.6);
  }}
  .stop-modal-head {{
    display:flex; justify-content:space-between; align-items:center;
    border-bottom:1px solid var(--line); padding-bottom:10px; margin-bottom:14px;
  }}
  .stop-modal-head .modal-title {{
    font-size:15px; font-weight:700; color:var(--text);
  }}
  .stop-modal-close {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    width:28px; height:28px; border-radius:6px; font-size:14px; cursor:pointer;
  }}
  .stop-modal-close:hover {{ color:var(--text); border-color:var(--text); }}
  .stop-modal-tabs {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px; }}

  /* Top Movers split (per-fund) */
  .mov-split {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; }}
  .mov-col-title {{
    font-size:10px; letter-spacing:.15em; text-transform:uppercase;
    color:var(--muted); font-weight:700; margin-bottom:6px;
    padding-bottom:5px; border-bottom:1px solid var(--line);
  }}

  /* Comments / outliers block */
  .comments-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:12px; }}
  .comment-fund {{ background:var(--bg-2); border:1px solid var(--line); border-radius:8px; padding:12px 14px; }}
  .comment-title {{
    font-size:11px; color:var(--text); letter-spacing:.1em; text-transform:uppercase;
    font-weight:700; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid var(--line);
  }}
  .comment-empty {{ font-size:11.5px; color:var(--muted); font-style:italic; padding:4px 0; }}
  .comment-list  {{ list-style:none; padding:0; margin:0; font-size:11.5px; line-height:1.7; }}
  .comment-list li {{ padding:3px 0; border-bottom:1px dotted var(--line); }}
  .comment-list li:last-child {{ border-bottom:none; }}

  /* Summary page — fund grid + top movers */
  .summary-table {{
    width:100%; border-collapse:collapse; font-size:13px;
    background:var(--bg-2); border-radius:8px; overflow:hidden;
  }}
  .summary-table th {{
    color:var(--muted); font-size:10px; letter-spacing:.12em; text-transform:uppercase;
    padding:10px 12px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .summary-table td {{ padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:middle; }}
  .summary-table tr:last-child td {{ border-bottom:none; }}
  .summary-table tr:hover {{ background:rgba(26,143,209,.04); }}
  .summary-table td.sum-status {{ text-align:center; font-size:17px; width:60px; }}
  .summary-table td.sum-fund   {{ font-weight:700; color:var(--text); font-size:13.5px; }}
  .summary-table tr.bench-row:first-of-type td {{ border-top:2px solid var(--border); }}
  .summary-table tr.bench-row td {{ background:rgba(26,143,209,.03); }}
  .summary-table tr.bench-row td.sum-fund {{ font-weight:600; font-size:12.5px; }}

  .summary-movers {{
    width:100%; border-collapse:collapse; font-size:12px;
    background:var(--bg-2); border-radius:8px; overflow:hidden;
  }}
  .summary-movers th {{
    color:var(--muted); font-size:10px; letter-spacing:.12em; text-transform:uppercase;
    padding:10px 12px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .summary-movers td {{ padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:middle; }}
  .summary-movers tr:last-child td {{ border-bottom:none; }}
  .summary-movers td.sum-fund   {{ font-weight:700; color:var(--text); width:130px; }}
  .summary-movers td.sum-movers {{ font-family:'JetBrains Mono', monospace; font-size:11.5px; }}

  /* Single-name inline section */
  .sn-inline-stats {{
    color:var(--muted); font-size:12px;
    margin: 4px 2px 14px; padding-bottom:10px;
    border-bottom:1px solid var(--line);
  }}

  /* Performance attribution — hierarchical tree */
  .pa-card .card-head {{ display:flex; flex-wrap:wrap; align-items:center; gap:12px; }}
  .pa-toolbar {{ margin-left:auto; display:flex; gap:6px; align-items:center; }}
  .pa-search {{
    background:rgba(255,255,255,0.06); border:1px solid var(--line);
    border-radius:7px; color:var(--text); font-size:11.5px;
    padding:4px 10px; outline:none; width:150px;
    font-family:'JetBrains Mono',monospace;
  }}
  .pa-search:focus {{ border-color:var(--accent-2); background:rgba(255,255,255,0.1); }}
  .pa-search::placeholder {{ color:var(--muted); }}
  .pa-btn {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 10px; border-radius:6px; font-size:11px; cursor:pointer;
  }}
  .pa-btn:hover {{ color:var(--text); border-color:var(--line-2); }}
  .pa-view-toggle {{ display:flex; gap:4px; }}
  .pa-tgl {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 12px; border-radius:7px; font-size:11px; font-weight:600;
    letter-spacing:.06em; cursor:pointer;
  }}
  .pa-tgl:hover {{ color:var(--text); border-color:var(--line-2); }}
  .pa-tgl.active {{ background:var(--accent); border-color:var(--accent); color:#fff; }}

  /* Frontier Exposure toggles (IBOV/IBOD, Por Nome/Por Setor, ▼/▶ All).
     Same visual as .pa-tgl — dark bg, accent-blue active state. */
  .toggle-btn {{
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 12px; border-radius:7px; font-size:11px; font-weight:600;
    letter-spacing:.06em; cursor:pointer;
    font-family:'Inter', sans-serif;
  }}
  .toggle-btn:hover {{ color:var(--text); border-color:var(--line-2); background:rgba(0,113,187,.06); }}
  .toggle-btn.active {{ background:var(--accent); border-color:var(--accent); color:#fff; }}

  /* QUANT Exposure sortable column headers */
  .qexpo-sort-th {{ user-select:none; transition: color .12s ease; }}
  .qexpo-sort-th:hover {{ color:var(--accent-2); }}
  .qexpo-sort-th.qexpo-sort-active {{ color:var(--accent-2); }}
  .qexpo-sort-arrow {{ color:var(--accent-2); font-weight:700; margin-left:3px; }}

  .pa-table {{
    width:100%; background:var(--bg-2); border-radius:8px; overflow:hidden;
    font-size:12px; border-collapse:collapse;
  }}
  .pa-table th {{
    color:var(--muted); font-size:10px; letter-spacing:.1em; text-transform:uppercase;
    padding:9px 10px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .pa-table td.pa-name {{ padding:6px 10px; color:var(--text); font-weight:600; }}
  .pa-table td.t-num   {{ padding:6px 10px; text-align:right; }}
  .pa-table tbody tr   {{ border-bottom:1px solid var(--line); }}
  .pa-table tbody tr:last-child {{ border-bottom:none; }}
  .pa-table tbody tr.pa-has-children {{ cursor:pointer; }}
  .pa-table tbody tr.pa-has-children:hover {{ background:rgba(26,143,209,.06); }}
  .pa-table tbody tr.pa-l0 td.pa-name {{ padding-left:10px;  font-weight:700; }}
  .pa-table tbody tr.pa-l1 td.pa-name {{ padding-left:30px;  font-weight:600; color:var(--text); }}
  .pa-table tbody tr.pa-l2 td.pa-name {{ padding-left:50px;  font-weight:400; color:var(--muted); }}
  .pa-table tbody tr.pa-l1        {{ background:rgba(255,255,255,.012); }}
  .pa-table tbody tr.pa-l2        {{ background:rgba(255,255,255,.024); font-size:11.5px; }}
  .pa-table tbody tr.pa-pinned td.pa-name {{ color:var(--muted); font-style:italic; }}
  .pa-table tbody tr.pa-pinned    {{ border-top:1px dashed var(--line); }}
  .pa-exp {{
    display:inline-block; width:14px; color:var(--muted);
    font-size:10px; margin-right:4px; transition:transform .15s;
  }}
  .pa-exp-empty {{ color:transparent; }}
  .pa-has-children.expanded .pa-exp {{ transform:rotate(90deg); color:var(--accent-2); }}
  .pa-table tfoot tr.pa-total-row {{
    background:var(--panel-2); border-top:1px solid var(--line-2);
  }}
  .pa-table tfoot tr.pa-bench-row {{
    background:transparent; border-top:1px dashed var(--line);
  }}
  .pa-table tfoot tr.pa-bench-row td {{ font-style:italic; }}
  .pa-table tfoot tr.pa-nominal-row {{
    background:var(--panel-2); border-top:1px solid var(--line-2);
  }}
  .pa-table tfoot td.pa-name {{ padding:8px 10px; }}
  .pa-table th.pa-sortable {{ user-select:none; }}
  .pa-table th.pa-sortable:hover {{ color:var(--text); }}
  .pa-table th.pa-sort-active {{ color:var(--accent-2); }}
  .pa-sort-arrow {{ font-size:9px; color:var(--accent-2); margin-left:2px; }}
  .pa-pos {{ color:var(--muted); }}

  /* Single-name fund switcher (Por Report > Single-Name) */
  .sn-switcher {{
    display:flex; align-items:center; gap:8px;
    max-width: 1200px; margin: 0 auto 14px; padding: 8px 16px;
    background: var(--panel); border:1px solid var(--line);
    border-radius:10px;
  }}
  .sn-switcher-label {{
    font-size:10.5px; text-transform:uppercase; letter-spacing:.14em;
    color:var(--muted); margin-right:6px;
  }}
  .sn-switcher .tab {{
    background:transparent; border:1px solid var(--line);
    color:var(--muted); padding:6px 14px; border-radius:7px;
    font-size:12px; font-weight:600; letter-spacing:.06em;
    cursor:pointer;
  }}
  .sn-switcher .tab:hover {{ color:var(--text); border-color:var(--line-2); }}
  .sn-switcher .tab.active {{
    background:var(--accent); border-color:var(--accent); color:#fff;
  }}
  .sn-sides {{ display:flex; gap:18px; }}
  .sn-side {{ flex:1; }}
  .sn-side-head {{ font-size:11px; font-weight:700; letter-spacing:.1em; margin-bottom:6px; }}
  .sn-side-count {{ color:var(--muted); font-weight:400; }}
  .sn-table {{ background:var(--bg-2); border-radius:8px; overflow:hidden; font-size:11.5px; }}
  .sn-table th {{
    color:var(--muted); font-size:10px; padding:8px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }}
  .sn-table td.t-name {{ padding:6px 8px; color:var(--text); font-weight:700; }}
  .sn-table td.t-num  {{ padding:6px 8px; text-align:right; color:var(--muted); }}
  .sn-table tr {{ border-bottom:1px solid var(--line); }}
  .sn-table tr:last-child {{ border-bottom:none; }}
  .sn-table .t-empty {{ padding:14px; text-align:center; color:var(--muted); font-style:italic; }}
  /* Alerts */
  .alerts-section {{
    margin-top: 28px;
    border: 1px solid #fb923c44;
    border-radius: 8px;
    overflow: hidden;
  }}
  .alerts-header {{
    background: #1c1409;
    color: #fb923c;
    font-size: 11px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    padding: 10px 16px;
    border-bottom: 1px solid #fb923c33;
  }}
  .alert-item {{
    padding: 12px 16px;
    border-bottom: 1px solid #1e1e2e;
  }}
  .alert-item:last-child {{ border-bottom: none; }}
  .alert-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 6px;
  }}
  .alert-badge {{
    color: #fb923c;
    font-size: 14px;
  }}
  .alert-title {{
    color: #fb923c;
    font-weight: bold;
    font-size: 13px;
  }}
  .alert-stats {{
    color: #64748b;
    font-size: 11px;
    font-family: monospace;
  }}
  .alert-body {{
    color: #cbd5e1;
    font-size: 12px;
    line-height: 1.6;
    padding-left: 24px;
  }}
</style>
<script>
(function() {{
  function getBRT() {{
    var now = new Date();
    var hour = parseInt(new Intl.DateTimeFormat('en', {{
      timeZone: 'America/Sao_Paulo', hour: 'numeric', hour12: false
    }}).format(now));
    var fmt = new Intl.DateTimeFormat('en-CA', {{
      timeZone: 'America/Sao_Paulo',
      year: 'numeric', month: '2-digit', day: '2-digit'
    }});
    var todayStr = fmt.format(now);
    if (hour < 21) {{
      var d = new Date(now);
      d.setDate(d.getDate() - 1);
      var wd = new Intl.DateTimeFormat('en', {{timeZone:'America/Sao_Paulo', weekday:'short'}}).format(d);
      while (wd === 'Sat' || wd === 'Sun') {{
        d.setDate(d.getDate() - 1);
        wd = new Intl.DateTimeFormat('en', {{timeZone:'America/Sao_Paulo', weekday:'short'}}).format(d);
      }}
      return {{ def: fmt.format(d), today: todayStr, hour: hour }};
    }} else {{
      return {{ def: todayStr, today: todayStr, hour: hour }};
    }}
  }}
  // --- Navigation state: 3 modes (summary / fund / quality) driven by URL hash ---
  function parseHash() {{
    var h = (location.hash || '').slice(1);
    if (!h || h === 'summary') return {{ mode: 'summary' }};
    var m;
    if ((m = h.match(/^fund=(.*)$/)))   return {{ mode: 'fund',   sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^report=(.*)$/))) return {{ mode: 'report', sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^quality$/)))     return {{ mode: 'quality', sel: '' }};
    return {{ mode: 'summary' }};
  }}
  function setHash(mode, sel) {{
    var h = (mode === 'summary') ? '' : (sel ? (mode + '=' + encodeURIComponent(sel)) : mode);
    if (history.replaceState) history.replaceState(null, '', h ? ('#' + h) : location.pathname + location.search);
    else location.hash = h;
  }}

  var _FUND_REPORTS = {fund_reports_js};
  var _REPORT_LABELS = {report_labels_js};

  function updateJumpBar(fund) {{
    var bar = document.getElementById('report-jump-bar');
    if (!bar) return;
    var rids = _FUND_REPORTS[fund] || [];
    bar.innerHTML = '';
    rids.forEach(function(rid) {{
      var btn = document.createElement('button');
      btn.className = 'jump-btn';
      btn.dataset.rid = rid;
      btn.textContent = _REPORT_LABELS[rid] || rid;
      btn.onclick = function() {{ jumpTo('sec-' + fund + '-' + rid); }};
      bar.appendChild(btn);
    }});
  }}
  window.jumpTo = function(id) {{
    var el = document.getElementById(id);
    if (!el) return;
    // Account for sticky header height
    var hdr = document.querySelector('header');
    var offset = hdr ? hdr.offsetHeight : 0;
    var top = el.getBoundingClientRect().top + window.pageYOffset - offset - 8;
    window.scrollTo({{ top: top, behavior: 'smooth' }});
    // Highlight active jump button
    document.querySelectorAll('.jump-btn').forEach(function(b) {{ b.classList.remove('active-jump'); }});
    var rid = id.split('-').slice(2).join('-');
    document.querySelectorAll('.jump-btn[data-rid="' + rid + '"]').forEach(function(b) {{
      b.classList.add('active-jump');
    }});
  }};

  function syncHeaderH() {{
    var h = document.querySelector('header');
    if (!h) return;
    // rAF: header height depends on sub-tabs visibility, which is set earlier
    // in the same tick — let layout settle before reading offsetHeight.
    requestAnimationFrame(function() {{
      document.documentElement.style.setProperty('--header-h', h.offsetHeight + 'px');
    }});
  }}

  function applyState() {{
    var st = parseHash();
    var mode = st.mode, sel = st.sel;
    document.body.dataset.mode = mode;
    // Mode tabs
    document.querySelectorAll('.mode-tab').forEach(function(t) {{
      t.classList.toggle('active', t.dataset.mode === mode);
    }});
    // Sub-tab bars visibility
    document.querySelectorAll('.sub-tabs').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.for === mode);
    }});
    // Default selection per mode
    if (mode === 'fund' && !sel) {{
      var first = document.querySelector('.sub-tabs[data-for="fund"] .tab');
      if (first) sel = first.dataset.target;
    }}
    if (mode === 'report' && !sel) {{
      var first = document.querySelector('.sub-tabs[data-for="report"] .tab');
      if (first) sel = first.dataset.target;
    }}
    // Sub-tab active state
    document.querySelectorAll('.sub-tabs .tab').forEach(function(t) {{
      var bar = t.closest('.sub-tabs');
      t.classList.toggle('active', bar.dataset.for === mode && t.dataset.target === sel);
    }});
    // Update report jump bar (only in fund mode)
    if (mode === 'fund' && sel) updateJumpBar(sel);
    // Section visibility
    document.querySelectorAll('.section-wrap').forEach(function(el) {{
      var show = false;
      if (mode === 'summary')      show = el.dataset.view === 'summary';
      else if (mode === 'quality') show = el.dataset.view === 'quality';
      else if (mode === 'fund')    show = el.dataset.fund === sel;
      else if (mode === 'report')  show = el.dataset.report === sel;
      el.style.display = show ? '' : 'none';
    }});
    // Empty-state
    var anyVisible = Array.prototype.some.call(
      document.querySelectorAll('.section-wrap'),
      function(el) {{ return el.style.display !== 'none'; }}
    );
    var empty = document.getElementById('empty-state');
    if (empty) empty.style.display = anyVisible ? 'none' : '';
    // Recompute header height — it changes when #report-jump-bar shows/hides.
    syncHeaderH();
  }}
  window.selectMode = function(mode, sel) {{
    if (!sel) {{
      try {{
        if (mode === 'fund')   sel = sessionStorage.getItem('risk_monitor_fund')   || '';
        if (mode === 'report') sel = sessionStorage.getItem('risk_monitor_report') || '';
      }} catch (e) {{}}
    }}
    if (mode === 'fund' && !sel) {{
      var f = document.querySelector('.sub-tabs[data-for="fund"] .tab');
      if (f) sel = f.dataset.target;
    }}
    if (mode === 'report' && !sel) {{
      var r = document.querySelector('.sub-tabs[data-for="report"] .tab');
      if (r) sel = r.dataset.target;
    }}
    setHash(mode, sel);
    applyState();
  }};
  window.selectFund = function(name) {{
    try {{ sessionStorage.setItem('risk_monitor_fund', name); }} catch (e) {{}}
    setHash('fund', name);
    applyState();
  }};
  window.selectReport = function(name) {{
    try {{ sessionStorage.setItem('risk_monitor_report', name); }} catch (e) {{}}
    setHash('report', name);
    applyState();
  }};
  window.addEventListener('hashchange', applyState);
  window.addEventListener('DOMContentLoaded', function() {{
    var info   = getBRT();
    var picker = document.getElementById('date-picker');
    var hint   = document.getElementById('date-hint');
    var loaded = '{DATA_STR}';
    picker.value = loaded;
    if (info.hour >= 21 && info.def === info.today) {{
      hint.textContent = 'após 21h';
      hint.style.color = 'var(--warn)';
    }} else if (loaded !== info.def) {{
      hint.textContent = 'default ' + info.def;
      hint.style.color = 'var(--muted)';
    }}
    applyState();
    injectCsvButtons();
    attachUniversalSort();
    attachVrCaretToggle();
    highlightFundNames();
    injectFundNavChips();
    syncHeaderH();
    window.addEventListener('resize', syncHeaderH);
  }});
  // --- Wrap fund shortnames in card-sub elements with an accent chip ---
  function highlightFundNames() {{
    var SHORTS = {fund_shorts_js};
    // Sort by length desc so "MACRO_Q" matches before "MACRO"
    SHORTS.sort(function(a,b) {{ return b.length - a.length; }});
    var subs = document.querySelectorAll('.card-sub');
    subs.forEach(function(el) {{
      if (el.dataset.fundHighlighted) return;
      var html = el.innerHTML;
      for (var i = 0; i < SHORTS.length; i++) {{
        var s = SHORTS[i];
        // Word-boundary match; avoid wrapping inside existing tags
        var re = new RegExp('(^|[^A-Za-z0-9_>])(' + s + ')(?![A-Za-z0-9_])', 'g');
        html = html.replace(re, '$1<span class="fund-name">$2</span>');
      }}
      el.innerHTML = html;
      el.dataset.fundHighlighted = '1';
    }});
  }}
  // --- Fund-nav chip bar injected above each per-fund section ---
  function injectFundNavChips() {{
    var LABELS = {fund_labels_js};
    var ORDER  = {fund_order_js};
    // Collect available funds (those with at least one visible section-wrap)
    var available = ORDER.filter(function(s) {{
      return document.querySelector('.section-wrap[data-fund="' + s + '"]');
    }});
    if (available.length < 2) return;
    // Build one chip bar per fund — insert at the top of the first section-wrap
    // for each fund (user sees it once, highlighting the active fund).
    available.forEach(function(fs) {{
      var wrap = document.querySelector('.section-wrap[data-fund="' + fs + '"]');
      if (!wrap || wrap.querySelector('.fund-nav-chips')) return;
      var bar = document.createElement('div');
      bar.className = 'fund-nav-chips';
      var lbl = document.createElement('span');
      lbl.className = 'chip-label';
      lbl.textContent = 'Ir para:';
      bar.appendChild(lbl);
      available.forEach(function(s) {{
        var btn = document.createElement('button');
        btn.className = 'chip' + (s === fs ? ' active' : '');
        btn.textContent = LABELS[s] || s;
        // In "report" mode: stay in report, just scroll to that fund's section
        // for the currently selected report. In "fund" mode: switch fund.
        btn.onclick = function() {{
          var mode = document.body.dataset.mode || 'summary';
          if (mode === 'report') {{
            var currentReport = (location.hash.match(/report=([^&]+)/) || [])[1];
            var target = currentReport
              ? document.querySelector('#sec-' + s + '-' + currentReport)
              : document.querySelector('.section-wrap[data-fund="' + s + '"]');
            if (target) target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
          }} else {{
            window.selectFund(s);
          }}
        }};
        bar.appendChild(btn);
      }});
      wrap.insertBefore(bar, wrap.firstChild);
    }});
  }}
  // --- Vol Regime: expand/collapse books under a fund row ---
  function attachVrCaretToggle() {{
    document.querySelectorAll('.vr-caret').forEach(function(el) {{
      el.addEventListener('click', function(e) {{
        e.stopPropagation();
        var fs = el.getAttribute('data-fs');
        if (!fs) return;
        var rows = document.querySelectorAll('tr.vr-book[data-parent="' + fs + '"]');
        if (!rows.length) return;
        // Decide toggle direction from the first row's current visibility.
        // display may be '' (visible) or 'none' (hidden); decide once to keep
        // all sibling rows in sync.
        var shouldOpen = (rows[0].style.display === 'none');
        rows.forEach(function(r) {{
          r.style.display = shouldOpen ? '' : 'none';
        }});
        el.textContent = shouldOpen ? '▼' : '▶';
      }});
    }});
  }}
  window.goToDate = function(val) {{
    if (!val) return;
    var base = window.location.href.replace(/[^/\\\\]*$/, '').replace(/#.*$/, '');
    window.location.href = base + val + '_risk_monitor.html' + location.hash;
  }};
  // --- CSV export: scan visible tables inside a given element and download ---
  function escapeCSV(s) {{
    s = (s == null) ? '' : String(s).replace(/\\s+/g, ' ').trim();
    if (/[,";]/.test(s)) s = '"' + s.replace(/"/g, '""') + '"';
    return s;
  }}
  function tableToRows(tbl) {{
    var out = [];
    var rows = tbl.querySelectorAll('tr');
    for (var i = 0; i < rows.length; i++) {{
      var tr = rows[i];
      // skip hidden rows (drill-downs, toggled views)
      if (tr.offsetParent === null && tr.style.display !== '') continue;
      var cells = tr.querySelectorAll('th,td');
      if (!cells.length) continue;
      var line = [];
      for (var j = 0; j < cells.length; j++) {{
        var txt = cells[j].innerText || cells[j].textContent || '';
        line.push(escapeCSV(txt));
      }}
      out.push(line.join(','));
    }}
    return out;
  }}
  window.exportCardCSV = function(cardId, baseName) {{
    var el = document.getElementById(cardId);
    if (!el) return;
    var tables = el.querySelectorAll('table');
    var lines = [];
    tables.forEach(function(t, i) {{
      if (t.offsetParent === null) return;  // skip hidden tables
      if (i > 0 && lines.length) lines.push('');
      lines = lines.concat(tableToRows(t));
    }});
    if (!lines.length) return;
    var picker = document.getElementById('date-picker');
    var dt = (picker && picker.value) || '{DATA_STR}';
    var name = baseName + '_' + dt + '.csv';
    var blob = new Blob(['\\uFEFF' + lines.join('\\n')], {{ type: 'text/csv;charset=utf-8' }});
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a); a.click();
    setTimeout(function() {{ document.body.removeChild(a); URL.revokeObjectURL(a.href); }}, 100);
  }};
  // --- Universal table sort: clicking a <th> toggles asc/desc on that column ---
  function _cellKey(td) {{
    var dv = td.getAttribute('data-val');
    if (dv !== null && dv !== '') {{
      var n = parseFloat(dv);
      if (!isNaN(n)) return {{ n: n, s: dv }};
    }}
    var t = (td.innerText || td.textContent || '').trim();
    var clean = t.replace(/[%°\\s,+bps]/g, '').replace(/\\u2212/g, '-');
    var n2 = parseFloat(clean);
    if (!isNaN(n2)) return {{ n: n2, s: t }};
    return {{ n: NaN, s: t }};
  }}
  function sortTableByCol(table, colIdx, asc) {{
    var tbody = table.tBodies[0]; if (!tbody) return;
    var all   = Array.from(tbody.rows).filter(function(r) {{ return r.cells.length > colIdx; }});
    var pinned = all.filter(function(r) {{ return r.dataset.pinned === '1'; }});
    var rows   = all.filter(function(r) {{ return r.dataset.pinned !== '1'; }});
    rows.sort(function(a, b) {{
      var va = _cellKey(a.cells[colIdx]), vb = _cellKey(b.cells[colIdx]);
      if (!isNaN(va.n) && !isNaN(vb.n)) return asc ? va.n - vb.n : vb.n - va.n;
      if (!isNaN(va.n)) return -1;
      if (!isNaN(vb.n)) return  1;
      return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    }});
    rows.forEach(function(r) {{ tbody.appendChild(r); }});
    pinned.forEach(function(r) {{ tbody.appendChild(r); }});
  }}
  function attachUniversalSort() {{
    // Exclude tables that already have data-sort-col handlers (exposure section)
    document.querySelectorAll('table').forEach(function(table) {{
      if (table.dataset.sortAttached === '1') return;
      if (table.dataset.noSort === '1') return;
      var headers = table.querySelectorAll('thead th');
      if (!headers.length) return;
      // Skip tables that already use sortTable(...) handlers (have onclick with sortTable)
      var alreadyCustom = Array.from(headers).some(function(h) {{
        return (h.getAttribute('onclick') || '').indexOf('sortTable(') >= 0;
      }});
      if (alreadyCustom) return;
      table.dataset.sortAttached = '1';
      var sortState = {{}};
      headers.forEach(function(th, idx) {{
        th.style.cursor = 'pointer';
        th.style.userSelect = 'none';
        var ind = document.createElement('span');
        ind.textContent = ' ▲▼';
        ind.style.opacity = '0.3';
        ind.style.fontSize = '8px';
        ind.style.marginLeft = '3px';
        th.appendChild(ind);
        th.addEventListener('click', function() {{
          var asc = !sortState[idx];
          sortState = {{}}; sortState[idx] = asc;
          sortTableByCol(table, idx, asc);
          headers.forEach(function(h2, j) {{
            var sp = h2.lastChild;
            if (!sp || sp.nodeName !== 'SPAN') return;
            if (j === idx) {{ sp.textContent = asc ? ' ▲' : ' ▼'; sp.style.opacity = '0.85'; }}
            else           {{ sp.textContent = ' ▲▼'; sp.style.opacity = '0.3'; }}
          }});
        }});
      }});
    }});
  }}
  function injectCsvButtons() {{
    var cards = document.querySelectorAll('section.card, .modal');
    cards.forEach(function(card, idx) {{
      var head = card.querySelector('.card-head, .modal-head');
      if (!head || head.querySelector('.btn-csv')) return;
      if (!card.id) card.id = 'csv-card-' + idx;
      var title = card.querySelector('.card-title, .modal-title');
      var base = title ? title.textContent.trim().replace(/[^a-z0-9]+/gi, '_').toLowerCase() : 'table';
      var btn = document.createElement('button');
      btn.className = 'btn-csv';
      btn.textContent = '⤓ CSV';
      btn.setAttribute('type','button');
      btn.onclick = function(e) {{ e.stopPropagation(); window.exportCardCSV(card.id, base); }};
      head.appendChild(btn);
    }});
  }}
  window.setDistMode = function(cardId, mode) {{
    var card = document.getElementById(cardId);
    if (!card) return;
    card.querySelectorAll('.dist-btn[data-mode]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.mode === mode);
    }});
    card.querySelectorAll('.dist-view[data-mode]').forEach(function(v) {{
      v.style.display = (v.dataset.mode === mode) ? '' : 'none';
    }});
  }};
  window.setDistBench = function(cardId, bench) {{
    var card = document.getElementById(cardId);
    if (!card) return;
    card.querySelectorAll('.dist-bench-btn').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.bench === bench);
    }});
    card.querySelectorAll('[data-bench-section]').forEach(function(s) {{
      s.style.display = (s.dataset.benchSection === bench) ? '' : 'none';
    }});
  }};
  // ── PDF export ─────────────────────────────────────────────────────────
  // mode === 'current' → imprime só o que está visível (respeita mode/fund)
  // mode === 'full'    → expande todas as PA trees e mostra todas as seções
  window.exportPdf = function(mode) {{
    var body = document.body;
    if (mode === 'full') body.classList.add('print-full');
    else                 body.classList.add('print-current');
    // We do NOT auto-expand PA trees — that produces dozens of pages of tiny type.
    // User expands manually what they want before clicking; default is level-0 only.
    setTimeout(function() {{
      window.print();
      setTimeout(function() {{
        body.classList.remove('print-full', 'print-current');
      }}, 500);
    }}, 120);
  }};

  // Evolution Diversification — sub-tab toggle (C1 / C2 / C3 / Matriz Direcional)
  window.selectEvoDivView = function(btn, view) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('.evo-div-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.evoDiv === view);
    }});
    card.querySelectorAll('.evo-div-view').forEach(function(v) {{
      v.style.display = (v.dataset.evoDiv === view) ? '' : 'none';
    }});
  }};

  // IDKA exposure — expand/collapse per factor (click on header row)
  window.toggleIdkaFac = function(header, fac) {{
    var tbody = header.closest('tbody');
    if (!tbody) return;
    var children = tbody.querySelectorAll('tr[data-idka-child="' + fac + '"]');
    var caret = header.querySelector('.idka-fac-caret');
    var anyVisible = Array.from(children).some(function(tr) {{
      return tr.style.display !== 'none';
    }});
    children.forEach(function(tr) {{ tr.style.display = anyVisible ? 'none' : ''; }});
    if (caret) caret.textContent = anyVisible ? '▶' : '▼';
  }};
  window.idkaExpandAll = function(btn) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('tr[data-idka-child]').forEach(function(tr) {{ tr.style.display = ''; }});
    card.querySelectorAll('.idka-fac-caret').forEach(function(c) {{ c.textContent = '▼'; }});
  }};
  window.idkaCollapseAll = function(btn) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('tr[data-idka-child]').forEach(function(tr) {{ tr.style.display = 'none'; }});
    card.querySelectorAll('.idka-fac-caret').forEach(function(c) {{ c.textContent = '▶'; }});
  }};

  // IDKA exposure — Bruto / Líquido toggle (shows/hides synthetic rows + factor header spans)
  window.selectIdkaView = function(btn, view) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('.pa-view-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.idkaView === view);
    }});
    // Rows with data-idka-view only visible in their matching view
    card.querySelectorAll('tr[data-idka-view]').forEach(function(tr) {{
      tr.style.display = (tr.getAttribute('data-idka-view') === view) ? '' : 'none';
    }});
    // Factor header spans — each shows its mode's value (bruto / liq-benchmark / liq-replication)
    card.querySelectorAll('[data-idka-span]').forEach(function(el) {{
      el.style.display = (el.getAttribute('data-idka-span') === view) ? '' : 'none';
    }});
  }};

  // Stop history modal (child window) — opened from Risk Budget Monitor
  window.openStopHistory = function() {{
    var m = document.getElementById('stop-history-modal');
    if (m) m.style.display = 'flex';
  }};
  window.closeStopHistory = function() {{
    var m = document.getElementById('stop-history-modal');
    if (m) m.style.display = 'none';
  }};
  window.selectStopPm = function(btn, pm) {{
    var modal = btn.closest('.stop-modal-box');
    if (!modal) return;
    modal.querySelectorAll('.stop-modal-tabs [data-stop-pm]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.stopPm === pm);
    }});
    modal.querySelectorAll('.stop-pm-view').forEach(function(v) {{
      v.style.display = (v.dataset.stopPm === pm) ? '' : 'none';
    }});
  }};
  // ESC closes modal
  document.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') {{
      var m = document.getElementById('stop-history-modal');
      if (m && m.style.display !== 'none') m.style.display = 'none';
    }}
  }});

  // Summary > Top Movers view toggle (Por Livro / Por Classe)
  window.selectMoversView = function(btn, view) {{
    var card = btn.closest('.sum-movers-card');
    if (!card) return;
    card.querySelectorAll('.sum-tgl .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.movView === view);
    }});
    card.querySelectorAll('.mov-view').forEach(function(t) {{
      t.style.display = (t.dataset.movView === view) ? '' : 'none';
    }});
  }};
  // Top Posições — drill-down (Factor → Instrument → Fund)
  window.toggleTopPos = function(tr) {{
    var path = tr.getAttribute('data-tp-path');
    if (!path) return;
    var table = tr.closest('table'); if (!table) return;
    var direct = table.querySelectorAll('tr[data-tp-parent="' + path + '"]');
    var anyVisible = false;
    direct.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    direct.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    // Collapse grandchildren when collapsing
    if (!willOpen) {{
      var all = table.querySelectorAll('tr[data-tp-parent^="' + path + '|"]');
      all.forEach(function(r) {{ r.style.display = 'none'; }});
      // caret reset on closed children
      all.forEach(function(r) {{
        var c = r.querySelector('.tp-caret'); if (c) c.textContent = '▶';
      }});
    }}
    var caret = tr.querySelector('.tp-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // Breakdown por Fator — Líquido / Bruto toggle (net-of-bench vs gross)
  window.selectRfBrl = function(btn, mode) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-brl-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfBrl === mode);
    }});
    card.querySelectorAll('.rf-brl-body').forEach(function(v) {{
      v.style.display = (v.dataset.rfBrl === mode) ? '' : 'none';
    }});
  }};
  // Distribuição 252d — drill-down (fund row expands livro/rf children)
  window.toggleDistChildren = function(tr) {{
    var key = tr.getAttribute('data-dist-key');
    if (!key) return;
    var table = tr.closest('table'); if (!table) return;
    var children = table.querySelectorAll('tr.dist-row-child[data-dist-parent="' + key + '"]');
    var anyVisible = false;
    children.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    children.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    var caret = tr.querySelector('.dist-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // QUANT exposure — sort positions within each factor (respects hierarchy).
  // Clicking a header toggles direction on repeat click; clicking a different
  // header resets to its default direction.
  window.sortQuantExpoPositions = function(el, mode) {{
    var card = el.closest('.card');
    if (!card) return;
    // If the same header is clicked again, toggle direction between net_desc/asc,
    // or A-Z/Z-A, or gross desc/asc. For simplicity, we track "active" state;
    // repeat click on the active header flips direction where applicable.
    var wasActive = el.classList.contains('qexpo-sort-active');
    var newMode = mode;
    if (wasActive) {{
      if (mode === 'net_desc') newMode = 'net_asc';
      else if (mode === 'net_asc')  newMode = 'net_desc';
      else if (mode === 'gross')    newMode = 'gross_asc';
      else if (mode === 'gross_asc') newMode = 'gross';
      else if (mode === 'name')     newMode = 'name_desc';
      else if (mode === 'name_desc') newMode = 'name';
    }}
    // Clear active state across all headers; mark the clicked one.
    card.querySelectorAll('.qexpo-sort-th').forEach(function(th) {{
      th.classList.remove('qexpo-sort-active');
      var arrow = th.querySelector('.qexpo-sort-arrow');
      if (arrow) arrow.textContent = '';
    }});
    el.classList.add('qexpo-sort-active');
    var arrow = el.querySelector('.qexpo-sort-arrow');
    if (arrow) {{
      if (newMode.indexOf('asc') >= 0 || newMode === 'name_desc') arrow.textContent = '↑';
      else arrow.textContent = '↓';
    }}
    // Sort children
    var tbody = card.querySelector('.qexpo-view[data-qexpo-view="factor"] table tbody');
    if (!tbody) return;
    var parents = tbody.querySelectorAll('tr[data-qexpo-path]');
    parents.forEach(function(p) {{
      var path = p.getAttribute('data-qexpo-path');
      var children = Array.from(tbody.querySelectorAll('tr[data-qexpo-parent="' + path + '"]'));
      children.sort(function(a, b) {{
        var da = parseFloat(a.dataset.sortDelta || '0');
        var db = parseFloat(b.dataset.sortDelta || '0');
        var ga = parseFloat(a.dataset.sortAbs   || '0');
        var gb = parseFloat(b.dataset.sortAbs   || '0');
        if (newMode === 'gross')     return gb - ga;
        if (newMode === 'gross_asc') return ga - gb;
        if (newMode === 'net_desc')  return db - da;
        if (newMode === 'net_asc')   return da - db;
        if (newMode === 'name')      return (a.textContent || '').localeCompare(b.textContent || '');
        if (newMode === 'name_desc') return (b.textContent || '').localeCompare(a.textContent || '');
        return Math.abs(db) - Math.abs(da);  // abs_delta default
      }});
      var anchor = p;
      children.forEach(function(c) {{ anchor.after(c); anchor = c; }});
    }});
  }};
  // QUANT exposure — Por Fator drill-down (click factor to expand positions)
  window.toggleQuantExpoFactor = function(tr) {{
    var path = tr.getAttribute('data-qexpo-path');
    if (!path) return;
    var table = tr.closest('table'); if (!table) return;
    var children = table.querySelectorAll('tr[data-qexpo-parent="' + path + '"]');
    var anyVisible = false;
    children.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    children.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    var caret = tr.querySelector('.qexpo-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // QUANT exposure — Por Fator / Por Livro toggle
  window.selectQuantExpoView = function(btn, view) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.pa-view-toggle .pa-tgl').forEach(function(b) {{
      if (b.dataset.qexpoView)
        b.classList.toggle('active', b.dataset.qexpoView === view);
    }});
    card.querySelectorAll('.qexpo-view').forEach(function(v) {{
      v.style.display = (v.dataset.qexpoView === view) ? '' : 'none';
    }});
  }};
  // PA alerts — Por Tamanho / Por Fundo toggle (preserves grid layout)
  window.selectPaAlertSort = function(btn, mode) {{
    var container = btn.closest('.alerts-section') || document;
    container.querySelectorAll('.pa-alert-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.paSort === mode);
    }});
    container.querySelectorAll('.pa-alert-view').forEach(function(v) {{
      v.classList.toggle('pa-alert-view-hidden', v.dataset.paSort !== mode);
    }});
  }};
  // Exposure Map — Ambos/Real/Nominal factor filter. Bench bars always visible.
  window.selectRfView = function(btn, view) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-view-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfView === view);
    }});
    card.dataset.rfFactor = view;
    _rfApplyVisibility(card);
  }};
  // Exposure Map — Absoluto / Relativo mode toggle
  window.selectRfMode = function(btn, mode) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-mode-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfMode === mode);
    }});
    card.dataset.rfMode = mode;
    _rfApplyVisibility(card);
  }};
  function _rfApplyVisibility(card) {{
    var factor = card.dataset.rfFactor || 'both';
    var mode   = card.dataset.rfMode   || 'absoluto';
    card.querySelectorAll('.rf-expo-svg .rf-mode-group').forEach(function(g) {{
      g.style.display = (g.getAttribute('data-rf-mode') === mode) ? '' : 'none';
    }});
    card.querySelectorAll('.rf-expo-svg .rf-mode-group [data-factor]').forEach(function(el) {{
      var f = el.getAttribute('data-factor');
      if (f === 'bench') {{ el.style.display = ''; return; }}
      el.style.display = (factor === 'both' || f === factor) ? '' : 'none';
    }});
  }}
  // Expand/collapse the detail table under an Exposure Map card
  window.toggleRfTable = function(btn) {{
    var wrap = btn.nextElementSibling;
    if (!wrap) return;
    var open = wrap.style.display !== 'none';
    wrap.style.display = open ? 'none' : '';
    btn.textContent = (open ? '▸ Mostrar tabela' : '▾ Esconder tabela');
    btn.setAttribute('aria-expanded', open ? 'false' : 'true');
  }};
  // PA view toggle (Por Classe / Por Livro) inside a PA card
  window.selectPaView = function(btn, viewId) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    // Clear search when switching views
    var srch = card.querySelector('.pa-search');
    if (srch) srch.value = '';
    card.querySelectorAll('.pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.paView === viewId);
    }});
    card.querySelectorAll('.pa-view').forEach(function(v) {{
      v.style.display = (v.dataset.paView === viewId) ? '' : 'none';
    }});
  }};
  window.filterPa = function(input) {{
    var q = input.value.trim().toLowerCase();
    var card = input.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    if (!q) {{
      // Reset to root-only visible state
      view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
        tr.style.display = (parseInt(tr.dataset.level || '0') === 0) ? '' : 'none';
        tr.classList.remove('expanded');
      }});
      return;
    }}
    // Force-render all lazy children (expand any not-yet-rendered has-children)
    for (var guard = 0; guard < 50; guard++) {{
      var pending = Array.prototype.filter.call(
        view.querySelectorAll('tr.pa-has-children'),
        function(tr) {{ return tr.dataset.rendered !== '1'; }}
      );
      if (pending.length === 0) break;
      pending.forEach(function(tr) {{
        if (!tr.classList.contains('expanded')) window.togglePaRow(tr);
      }});
    }}
    // Build path → tr map for ancestor lookup
    var pathMap = {{}};
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      if (tr.dataset.path) pathMap[tr.dataset.path] = tr;
    }});
    // Find matching paths + all their ancestors
    var show = {{}};
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      var lbl = tr.querySelector('.pa-label');
      if (!lbl) return;
      if (lbl.textContent.trim().toLowerCase().indexOf(q) >= 0) {{
        var p = tr.dataset.path;
        while (p) {{
          show[p] = true;
          var anc = pathMap[p];
          p = anc ? anc.dataset.parent : '';
        }}
      }}
    }});
    // Apply visibility
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      tr.style.display = show[tr.dataset.path] ? '' : 'none';
    }});
  }};
  // ── PA lazy render helpers ─────────────────────────────────────────────
  function paDataFor(view) {{
    var id = view.dataset.paId;
    if (!id) return null;
    var cached = view._paData;
    if (cached) return cached;
    var s = document.getElementById(id);
    if (!s) return null;
    try {{ view._paData = JSON.parse(s.textContent); return view._paData; }}
    catch (e) {{ console.error('PA JSON parse failed', e); return null; }}
  }}
  function paEsc(s) {{
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }}
  function paPctCell(v, maxAbs) {{
    var pct = v / 100.0;
    if (Math.abs(pct) < 0.005) {{
      return '<td class="t-num mono" style="color:var(--muted)">—</td>';
    }}
    var color = v >= 0 ? 'var(--up)' : 'var(--down)';
    var rgb   = v >= 0 ? '38,208,124' : '255,90,106';
    var alpha = (maxAbs > 0) ? Math.min(Math.abs(v) / maxAbs, 1.0) * 0.14 : 0;
    var sign  = v >= 0 ? '+' : '';
    return '<td class="t-num mono" style="color:' + color
         + '; background:rgba(' + rgb + ',' + alpha.toFixed(2) + ')">'
         + sign + pct.toFixed(2) + '%</td>';
  }}
  function paPosCell(v) {{
    if (Math.abs(v) < 1e5) {{
      return '<td class="t-num mono pa-pos" style="color:var(--muted)">—</td>';
    }}
    var m = v / 1e6;
    var s = m.toLocaleString('pt-BR', {{minimumFractionDigits:1, maximumFractionDigits:1}});
    return '<td class="t-num mono pa-pos" style="color:var(--muted)">' + s + '</td>';
  }}
  function paRenderRow(node, maxAbs) {{
    var levelCls  = 'pa-l' + node.dp;
    var pinnedCls = node.pi ? ' pa-pinned' : '';
    var expander  = node.hc
      ? '<span class="pa-exp" aria-hidden="true">▸</span>'
      : '<span class="pa-exp pa-exp-empty" aria-hidden="true"></span>';
    var baseCls   = 'pa-row ' + levelCls + pinnedCls;
    var clsClick  = node.hc
      ? ' onclick="togglePaRow(this)" class="' + baseCls + ' pa-has-children"'
      : ' class="' + baseCls + '"';
    var pinAttr   = node.pi ? ' data-pinned="1"' : '';
    var attrs = ' data-level="' + node.dp + '" data-path="' + paEsc(node.pa)
              + '" data-parent="' + paEsc(node.pr) + '"' + pinAttr;
    var cells = '<td class="pa-name">' + expander
              + '<span class="pa-label">' + paEsc(node.d) + '</span></td>'
              + paPctCell(node.a[0], maxAbs[0])
              + paPctCell(node.a[1], maxAbs[1])
              + paPctCell(node.a[2], maxAbs[2])
              + paPctCell(node.a[3], maxAbs[3]);
    return '<tr' + attrs + clsClick + '>' + cells + '</tr>';
  }}
  function paRenderChildren(view, parentTr) {{
    if (parentTr.dataset.rendered === '1') return;
    var data = paDataFor(view);
    if (!data) return;
    var kids = (data.byParent || {{}})[parentTr.dataset.path] || [];
    var ordered = kids;
    // Only re-sort if user has explicitly clicked a sort header.
    // JSON default order already honours the server-side Excel-inspired order.
    if (view.dataset.userSorted === '1') {{
      var idx  = parseInt(view.dataset.sortIdx  || '2', 10);
      var desc = (view.dataset.sortDesc || '1') === '1';
      var reg = kids.filter(function(k) {{ return !k.pi; }});
      var pin = kids.filter(function(k) {{ return k.pi; }});
      reg.sort(function(a, b) {{
        var va = a.a[idx], vb = b.a[idx];
        var aZ = Math.abs(va) < 1e-6, bZ = Math.abs(vb) < 1e-6;
        if (aZ && bZ) return 0;
        if (aZ) return 1;
        if (bZ) return -1;
        return desc ? (vb - va) : (va - vb);
      }});
      ordered = reg.concat(pin);
    }}
    var html = ordered.map(function(k) {{ return paRenderRow(k, data.maxAbs); }}).join('');
    parentTr.insertAdjacentHTML('afterend', html);
    parentTr.dataset.rendered = '1';
  }}

  // PA tree expand/collapse (lazy)
  window.togglePaRow = function(tr) {{
    if (!tr) return;
    var view = tr.closest('.pa-view');
    if (!view) return;
    var path = tr.dataset.path;
    var willExpand = !tr.classList.contains('expanded');
    if (willExpand) paRenderChildren(view, tr);
    tr.classList.toggle('expanded', willExpand);
    var sel = 'tr[data-parent="' + (window.CSS && CSS.escape ? CSS.escape(path) : path) + '"]';
    view.querySelectorAll(sel).forEach(function(c) {{
      if (willExpand) {{
        c.style.display = '';
      }} else {{
        c.style.display = 'none';
        if (c.classList.contains('expanded')) window.togglePaRow(c);
      }}
    }});
  }};
  // Expand every parent in the currently-active view (recursive, lazy-renders as it goes)
  window.expandAllPa = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    // Loop: keep expanding until no collapsed parents remain visible
    for (var guard = 0; guard < 50; guard++) {{
      var pending = view.querySelectorAll('tr.pa-has-children:not(.expanded)');
      if (pending.length === 0) break;
      pending.forEach(function(tr) {{
        if (tr.style.display !== 'none') window.togglePaRow(tr);
      }});
    }}
  }};
  window.collapseAllPa = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    // Collapse top-level expanded rows; togglePaRow recursively collapses descendants.
    view.querySelectorAll('tr.pa-has-children.expanded[data-level="0"]').forEach(function(tr) {{
      window.togglePaRow(tr);
    }});
  }};
  // PA per-metric sort — preserves tree hierarchy (sorts siblings under each parent)
  window.sortPaMetric = function(th, idx) {{
    var view  = th.closest('.pa-view');
    if (!view) return;
    var tbody = view.querySelector('tbody');
    var curIdx  = parseInt(view.dataset.sortIdx || '2');
    var curDesc = (view.dataset.sortDesc || '1') === '1';
    var desc = (curIdx === idx) ? !curDesc : true;
    view.dataset.sortIdx   = String(idx);
    view.dataset.sortDesc  = desc ? '1' : '0';
    view.dataset.userSorted = '1';

    // Update arrow markers
    view.querySelectorAll('th.pa-sortable').forEach(function(h) {{
      h.classList.remove('pa-sort-active');
      var a = h.querySelector('.pa-sort-arrow');
      if (a) a.remove();
    }});
    th.classList.add('pa-sort-active');
    var arrow = document.createElement('span');
    arrow.className = 'pa-sort-arrow';
    arrow.textContent = desc ? ' ▾' : ' ▴';
    th.appendChild(arrow);

    function parseCell(tr, metricIdx) {{
      var cell = tr.children[1 + metricIdx];
      if (!cell) return 0;
      var t = cell.textContent.trim();
      if (t === '—' || t === '') return 0;
      return parseFloat(t.replace('+','').replace('%','').replace(',','.')) || 0;
    }}

    // group rows by parent path, separating pinned-bottom rows (Caixa/Custos/...)
    var byParentReg = {{}}, byParentPin = {{}};
    Array.prototype.forEach.call(tbody.children, function(tr) {{
      var p = tr.dataset.parent || '';
      if (tr.dataset.pinned === '1') {{
        (byParentPin[p] = byParentPin[p] || []).push(tr);
      }} else {{
        (byParentReg[p] = byParentReg[p] || []).push(tr);
      }}
    }});
    // sort regular siblings: positives → negatives → zeros, signed
    function paCmp(va, vb) {{
      var aZ = Math.abs(va) < 1e-6, bZ = Math.abs(vb) < 1e-6;
      if (aZ && bZ) return 0;
      if (aZ) return 1;   // zeros always last
      if (bZ) return -1;
      return desc ? (vb - va) : (va - vb);
    }}
    Object.keys(byParentReg).forEach(function(p) {{
      byParentReg[p].sort(function(a, b) {{
        return paCmp(parseCell(a, idx), parseCell(b, idx));
      }});
    }});
    // merge: regular first, pinned always last (not sorted)
    var byParent = {{}};
    var allKeys = new Set([].concat(Object.keys(byParentReg), Object.keys(byParentPin)));
    allKeys.forEach(function(p) {{
      byParent[p] = (byParentReg[p] || []).concat(byParentPin[p] || []);
    }});
    // DFS reconstruction
    var ordered = [];
    function visit(parentPath) {{
      (byParent[parentPath] || []).forEach(function(tr) {{
        ordered.push(tr);
        visit(tr.dataset.path);
      }});
    }}
    visit('');
    ordered.forEach(function(tr) {{ tbody.appendChild(tr); }});
  }};
}})();
</script>
</head>
<body>
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
    <div class="brand">
      <div class="logo"><svg viewBox="0 0 47 45" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M39.5781 37.6377C36.1678 40.8382 31.8394 42.6304 27.2486 42.6304H27.1174C17.5423 42.5024 9.67243 34.8213 9.67243 25.4758C9.54127 17.9227 14.6567 11.2657 21.8708 8.96135C22.2643 9.72947 22.5266 10.6256 22.6578 11.3937C20.428 12.0338 18.4605 13.186 16.7554 14.8502C14.6567 16.7705 13.3451 19.3309 12.6892 21.8913V22.0193C12.0334 24.5797 12.1646 27.1401 12.9516 29.4444C13.6074 31.6208 14.7879 33.6691 16.493 35.3333C18.4605 37.2536 20.8215 38.6618 23.3136 39.3019C23.4448 39.3019 23.7071 39.43 23.8383 39.43C23.9694 39.43 23.9694 39.43 24.1006 39.43C24.1006 39.43 24.4941 39.558 25.1499 39.558C25.6746 39.686 26.3304 39.686 26.8551 39.686C27.6421 39.686 28.4291 39.686 29.3472 39.558C32.364 39.1739 35.3808 37.8937 37.7418 35.7174C39.8404 33.6691 41.0209 31.3647 41.2833 30.4686C41.6768 29.3164 41.5456 28.0362 41.0209 26.8841C40.4963 25.7319 39.4469 24.8358 38.1353 24.4517C36.8236 24.0676 35.512 24.0676 34.3315 24.7077C31.8394 25.8599 30.79 28.8043 32.1017 31.2367C32.364 31.6208 32.8887 31.8768 33.2822 31.6208C33.6757 31.3647 33.938 30.8527 33.6757 30.4686C32.8887 28.9324 33.5445 27.1401 35.1185 26.372C35.9055 25.9879 36.6925 25.9879 37.4795 26.244C38.2665 26.5 38.9223 27.0121 39.3158 27.7802C39.7093 28.5483 39.7093 29.3164 39.4469 30.0845C39.3158 30.4686 39.1846 30.7246 38.9223 30.9807C34.8562 34.6932 28.4291 34.4372 24.4941 30.4686C24.1006 30.0845 23.5759 30.0845 23.1825 30.4686C22.789 30.8527 22.789 31.3647 23.1825 31.7488C26.5927 35.3333 31.8394 36.3575 36.299 34.6932C34.4627 36.3575 32.2329 37.3816 29.8719 37.7657C27.7732 38.0217 26.0681 36.9976 25.5434 36.7415C22.002 34.4372 20.2968 30.2126 21.3461 25.8599C22.6578 20.4831 28.1667 17.1546 33.6757 18.4348C34.0692 18.5628 34.4627 18.5628 34.725 18.6908C35.9055 19.0749 37.2171 18.6908 37.873 17.5386C38.2665 16.7705 38.3976 16.0024 38.0041 15.3623C38.0041 15.2343 36.6925 11.9058 32.8887 9.08937C39.9716 11.3937 44.9559 17.9227 44.9559 25.4758C44.8247 30.0845 42.9884 34.3092 39.5781 37.6377ZM30.79 6.78503C30.1342 6.65701 29.4784 7.04107 29.2161 7.68116C28.9537 8.32126 29.2161 8.96136 29.8719 9.34541C33.938 11.6498 35.6432 14.5942 36.0366 15.6184C36.1678 15.7464 36.1678 15.8744 36.1678 15.8744C36.299 16.3865 36.1678 16.6425 36.1678 16.7705C36.0366 16.8986 35.7743 17.2826 35.2497 17.1546C34.8562 17.0266 34.4627 16.8986 33.938 16.7705C27.5109 15.3623 20.9526 19.3309 19.3787 25.6039C18.1982 30.3406 20.1657 35.0773 23.9694 37.7657C22.9201 37.5097 21.7396 37.1256 20.6903 36.4855C20.6903 36.4855 20.6903 36.4855 20.5592 36.4855C20.428 36.4855 20.2968 36.3575 20.2968 36.3575C17.6735 34.6932 15.5749 32.2609 14.6567 29.3164C13.2139 24.8358 14.2632 19.715 17.9358 16.1304C19.3787 14.8502 21.0838 13.8261 22.9201 13.186C23.0513 13.9541 23.0513 14.8502 23.0513 15.6184C23.0513 16.1304 23.4448 16.5145 23.9694 16.5145C24.4941 16.5145 24.8876 16.1304 24.8876 15.6184C24.8876 14.3382 24.7564 13.058 24.4941 11.9058C24.2318 10.4976 23.7071 9.08937 23.1825 7.80918C23.0513 7.55314 22.789 7.2971 22.5266 7.16908C22.2643 7.04107 22.002 7.04107 21.7396 7.16908C13.6074 9.47343 7.70496 17.0266 7.70496 25.4758C7.70496 35.8454 16.3619 44.4227 26.9862 44.4227C27.1174 44.4227 27.1174 44.4227 27.1174 44.4227C32.2329 44.4227 37.086 42.5024 40.7586 38.9179C44.5624 35.3333 46.5299 30.4686 46.5299 25.4758C46.661 16.2585 40.1028 8.44928 30.79 6.78503Z" fill="white"/></svg></div>
      <div>
        <h1>Galapagos <span style="font-weight:400;opacity:.75">CAPITAL</span></h1>
        <p>Risk Monitor</p>
      </div>
    </div>
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
<div class="subtitle">Data-base: <span class="mono">{DATA_STR}</span> &nbsp;·&nbsp; gerado em <span class="mono">{date.today().isoformat()}</span></div>
<main>
  {summary_html}
  {quality_html}
  <div id="sections-container">
    {sections_html}
  </div>
  <div id="empty-state" style="display:none">Sem dados para essa combinação de fundo × report.</div>
  {alerts_html}
</main>
</body>
</html>"""
    return html

# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import time
    from concurrent.futures import ThreadPoolExecutor
    from glpg_fetch import read_sql

    t0 = time.time()
    print(f"Fetching data for {DATA_STR}...")
    d1_str = _prev_bday(DATA_STR)
    d2_str = _prev_bday(d1_str)

    # Fan out all independent DB queries to a thread pool.
    # Each fetch keeps its own connection; I/O-bound so GIL not an issue.
    with ThreadPoolExecutor(max_workers=12) as ex:
        fut_risk       = ex.submit(fetch_risk_history)
        fut_risk_raw   = ex.submit(fetch_risk_history_raw)
        fut_risk_idka  = ex.submit(fetch_risk_history_idka)
        fut_aum        = ex.submit(fetch_aum_history)
        fut_pm_pnl       = ex.submit(fetch_pm_pnl_history)
        fut_pm_book_var  = ex.submit(fetch_macro_pm_book_var,  DATA_STR)
        fut_expo       = ex.submit(fetch_macro_exposure, DATA_STR)
        fut_expo_d1    = ex.submit(fetch_macro_exposure, d1_str)
        fut_expo_d2    = ex.submit(fetch_macro_exposure, d2_str)
        fut_pnl_prod   = ex.submit(fetch_macro_pnl_products, DATA_STR)
        fut_pnl_prod_d1= ex.submit(fetch_macro_pnl_products, d1_str)
        fut_quant_sn   = ex.submit(fetch_quant_single_names, DATA_STR)
        fut_quant_sn_d1= ex.submit(fetch_quant_single_names, d1_str)
        fut_evo_sn     = ex.submit(fetch_evolution_single_names, DATA_STR)
        fut_evo_sn_d1  = ex.submit(fetch_evolution_single_names, d1_str)
        fut_evo_direct = ex.submit(fetch_evolution_direct_single_names, DATA_STR)
        fut_dist       = ex.submit(fetch_pnl_distribution, DATA_STR)
        fut_dist_prev  = ex.submit(fetch_pnl_distribution, d1_str)
        fut_dist_act   = ex.submit(fetch_pnl_actual_by_cut, DATA_STR)
        fut_pa         = ex.submit(fetch_pa_leaves, DATA_STR)
        fut_pa_daily   = ex.submit(fetch_pa_daily_per_product, DATA_STR)
        fut_cdi        = ex.submit(fetch_cdi_returns, DATA_STR)
        fut_ibov       = ex.submit(fetch_ibov_returns, DATA_STR)
        fut_idka_idx = {
            "IDKA_3Y":  ex.submit(fetch_idka_index_returns, "IDKA_IPCA_3A",  DATA_STR),
            "IDKA_10Y": ex.submit(fetch_idka_index_returns, "IDKA_IPCA_10A", DATA_STR),
        }
        fut_walb = {
            "IDKA_3Y":  ex.submit(fetch_idka_albatroz_weight, "IDKA IPCA 3Y FIRF",  DATA_STR),
            "IDKA_10Y": ex.submit(fetch_idka_albatroz_weight, "IDKA IPCA 10Y FIRF", DATA_STR),
        }
        fut_alb        = ex.submit(fetch_albatroz_exposure, DATA_STR)
        fut_quant_expo    = ex.submit(fetch_quant_exposure, DATA_STR)
        fut_quant_expo_d1 = ex.submit(fetch_quant_exposure, d1_str)
        fut_quant_var     = ex.submit(fetch_quant_var,      DATA_STR)
        fut_quant_var_d1  = ex.submit(fetch_quant_var,      d1_str)
        fut_evo_expo      = ex.submit(fetch_evolution_exposure, DATA_STR)
        fut_evo_expo_d1   = ex.submit(fetch_evolution_exposure, d1_str)
        fut_evo_var       = ex.submit(fetch_evolution_var,      DATA_STR)
        fut_evo_var_d1    = ex.submit(fetch_evolution_var,      d1_str)
        fut_evo_pnl_prod  = ex.submit(fetch_evolution_pnl_products, DATA_STR)
        fut_alb_d1     = ex.submit(fetch_albatroz_exposure, d1_str)
        fut_rf_expo = {
            "IDKA_3Y":   ex.submit(fetch_rf_exposure_map, "IDKA IPCA 3Y FIRF",  DATA_STR),
            "IDKA_10Y":  ex.submit(fetch_rf_exposure_map, "IDKA IPCA 10Y FIRF", DATA_STR),
            "ALBATROZ":  ex.submit(fetch_rf_exposure_map, "GALAPAGOS ALBATROZ FIRF LP", DATA_STR),
            "MACRO":     ex.submit(fetch_rf_exposure_map, "Galapagos Macro FIM", DATA_STR),
            "EVOLUTION": ex.submit(fetch_rf_exposure_map, "Galapagos Evolution FIC FIM CP",
                                   DATA_STR, True),
        }
        # Pre-warm NAV cache for today + D-1 so every _latest_nav() call hits memory.
        fut_navs    = ex.submit(fetch_all_latest_navs, DATA_STR)
        fut_navs_d1 = ex.submit(fetch_all_latest_navs, d1_str)
        fut_frontier   = ex.submit(fetch_frontier_mainboard, DATA_STR)
        fut_frontier_expo = ex.submit(fetch_frontier_exposure_data)
        fut_chg        = {
            short: ex.submit(fetch_fund_position_changes, short, DATA_STR, d1_str)
            for short in ("QUANT", "EVOLUTION", "MACRO_Q", "ALBATROZ", "FRONTIER")
        }

    # ── Resolve results (sequential, with per-task fallback) ──────────────
    df_risk     = fut_risk.result()
    df_risk_raw = fut_risk_raw.result()
    try:
        df_risk_idka = fut_risk_idka.result()
    except Exception as e:
        print(f"  IDKA risk fetch failed ({e})")
        df_risk_idka = None
    df_aum      = fut_aum.result()
    df_pm_pnl   = fut_pm_pnl.result()
    try:
        pm_book_var = fut_pm_book_var.result()
    except Exception as e:
        print(f"  PM book-report VaR failed ({e})")
        pm_book_var = {}
    series      = build_series(df_risk, df_aum, df_risk_raw, df_risk_idka)
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
        print(f"  Exposure missing for {DATA_STR} — using {d1_str}")
        expo_date_label = d1_str
        df_expo, df_var, macro_aum = _expo_d1_raw, _var_d1_raw, (_aum_raw or _latest_nav("Galapagos Macro FIM", d1_str) or 1.0)
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
    cur_mes = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
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

    # df_pa already resolved above (used to build df_today)

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
        df_quant_expo, quant_expo_nav = fut_quant_expo.result()
    except Exception as e:
        print(f"  QUANT exposure fetch failed ({e})")
        df_quant_expo, quant_expo_nav = None, None
    try:
        df_quant_expo_d1, _ = fut_quant_expo_d1.result()
    except Exception as e:
        print(f"  QUANT D-1 exposure fetch failed ({e})")
        df_quant_expo_d1 = None
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
        df_evo_expo_d1, _ = fut_evo_expo_d1.result()
    except Exception as e:
        print(f"  EVOLUTION D-1 exposure fetch failed ({e})")
        df_evo_expo_d1 = None
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
        frontier_bvar = compute_frontier_bvar_hs(df_frontier, DATA_STR) if df_frontier is not None else None
    except Exception as e:
        print(f"  Frontier BVaR (HS) failed ({e})")
        frontier_bvar = None

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

    # Resolve NAV pre-warms (side effect is populating _NAV_CACHE)
    try:
        fut_navs.result()
        fut_navs_d1.result()
    except Exception as e:
        print(f"  NAV warmup failed ({e})")

    print(f"  ...fetches done in {time.time()-t0:.1f}s")

    # ── Data manifest: what landed and what is stale/missing ─────────────────
    _var_dates = {}
    for td, cfg in ALL_FUNDS.items():
        s = series.get(td)
        if s is not None and not s.empty:
            s_avail = s[s["VAL_DATE"] <= DATA]
            if not s_avail.empty:
                _var_dates[cfg["short"]] = s_avail.iloc[-1]["VAL_DATE"]
    data_manifest = {
        "requested_date": DATA_STR,
        "d1_str":         d1_str,
        # PA / PnL
        "pa_ok":          df_pa is not None and not df_pa.empty,
        "pa_has_today":   (df_pa is not None and not df_pa.empty and
                           not df_pa[df_pa["dia_bps"].abs() > 1e-6].empty),
        # VaR / Stress
        "var_dates":      _var_dates,    # short → actual date used (may be D-1)
        # Exposure
        "expo_ok":        df_expo is not None and not df_expo.empty,
        "expo_date":      expo_date_label or DATA_STR,
        # Single-names
        "quant_sn_ok":    df_quant_sn is not None,
        "evo_sn_ok":      df_evo_sn is not None,
        # Distribution
        "dist_today_ok":  bool(dist_map),
        "dist_prev_ok":   bool(dist_map_prev),
        # ALBATROZ
        "alb_expo_ok":    df_alb_expo is not None and not df_alb_expo.empty,
        "quant_expo_ok":  df_quant_expo is not None and not df_quant_expo.empty,
        "evo_expo_ok":    df_evo_expo is not None and not df_evo_expo.empty,
        # Stop monitor
        "stop_ok":        bool(pm_margem),
        "stop_has_pnl":   any(abs(pm_margem.get(pm, STOP_BASE) - STOP_BASE) > 1 for pm in pm_margem),
        # Detail for quality tab
        "expo_rows":      len(df_expo) if df_expo is not None and not df_expo.empty else 0,
        "quant_sn_rows":  len(df_quant_sn) if df_quant_sn is not None else 0,
        "evo_sn_rows":    len(df_evo_sn)   if df_evo_sn   is not None else 0,
        "alb_expo_rows":  len(df_alb_expo) if df_alb_expo is not None and not df_alb_expo.empty else 0,
        "quant_expo_rows": len(df_quant_expo) if df_quant_expo is not None and not df_quant_expo.empty else 0,
        "evo_expo_rows":  len(df_evo_expo) if df_evo_expo is not None and not df_evo_expo.empty else 0,
        "stop_pms":       sorted(pm_margem.keys()) if pm_margem else [],
        "stop_pms_pnl":   [pm for pm, v in (pm_margem or {}).items() if abs(v - STOP_BASE) > 1],
    }

    html = build_html(series, stop_hist, df_today, df_expo, df_var, macro_aum, df_expo_d1, df_var_d1,
                      df_pnl_prod=df_pnl_prod, pm_margem=pm_margem,
                      df_quant_sn=df_quant_sn, quant_nav=quant_nav, quant_legs=quant_legs,
                      df_evo_sn=df_evo_sn, evo_nav=evo_nav, evo_legs=evo_legs,
                      df_evo_direct=df_evo_direct,
                      df_pa=df_pa, cdi=cdi, ibov=ibov, df_pa_daily=df_pa_daily,
                      idka_idx_ret=idka_idx_ret, walb=walb,
                      df_alb_expo=df_alb_expo, alb_nav=alb_nav,
                      df_quant_expo=df_quant_expo, quant_expo_nav=quant_expo_nav,
                      df_quant_expo_d1=df_quant_expo_d1,
                      df_quant_var=df_quant_var, df_quant_var_d1=df_quant_var_d1,
                      df_evo_expo=df_evo_expo, evo_expo_nav=evo_expo_nav,
                      df_evo_expo_d1=df_evo_expo_d1,
                      df_evo_var=df_evo_var, df_evo_var_d1=df_evo_var_d1,
                      df_evo_pnl_prod=df_evo_pnl_prod,
                      rf_expo_maps=rf_expo_maps,
                      df_frontier=df_frontier,
                      frontier_bvar=frontier_bvar,
                      df_frontier_ibov=df_frontier_ibov,
                      df_frontier_smll=df_frontier_smll,
                      df_frontier_sectors=df_frontier_sectors,
                      position_changes=position_changes,
                      dist_map=dist_map, dist_map_prev=dist_map_prev, dist_actuals=dist_actuals,
                      vol_regime_map=vol_regime_map,
                      pm_book_var=pm_book_var,
                      expo_date_label=expo_date_label, data_manifest=data_manifest)
    out  = OUT_DIR / f"{DATA_STR}_risk_monitor.html"
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out}")

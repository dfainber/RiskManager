"""
fund_renderers.py — per-fund builder cards (non-exposure, non-PA, non-EVO-div).

Covers everything that ends up as an HTML section but doesn't belong to
the exposure family (see expo_renderers), the PA family (see pa_renderers)
or the EVO diversification family (see evo_renderers):

  build_albatroz_risk_budget      ALBATROZ 150 bps/month stop card
  build_single_names_section      Single-name inline grid (QUANT / EVOLUTION)
  build_vol_regime_section        Per-fund Vol Regime card
  build_distribution_card         252d distribution (backward + forward views)
  build_stop_section              MACRO PM stop monitor grid
  build_pm_budget_vs_var_section  MACRO Budget vs. VaR-by-PM
  build_frontier_lo_section       Frontier Long Only positions + PA sub-tab
  build_data_quality_section      Qualidade tab (manifest-driven)
  _build_fund_mini_briefing       Per-fund bullet briefing
  _build_executive_briefing       House-level executive briefing

Plus the small helpers they need: _dist_entries, _kind_tag,
_build_backward_table, _build_forward_table, _build_stop_history_modal.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from risk_runtime import DATA, DATA_STR, fmt_br_num
_fmt_br_num = fmt_br_num  # backwards-compat alias for renderers below
from risk_config import (
    ALL_FUNDS,
    STOP_BASE, STOP_SEM, STOP_ANO, ALBATROZ_STOP_BPS,
    UTIL_WARN, UTIL_HARD,
    FUND_ORDER, FUND_LABELS,
    _FUND_PA_KEY,
    _DIST_PORTFOLIOS, _VR_PORTFOLIOS,
    ALERT_THRESHOLD, ALERT_COMMENTS,
)
from svg_renderers import make_sparkline, range_line_svg, stop_bar_svg, range_bar_svg, multi_line_chart_svg
from metrics import compute_distribution_stats, compute_pa_outliers, compute_top_windows
from pa_renderers import _pa_filter_alpha, _pa_render_name
from db_helpers import _prev_bday
from evo_renderers import build_evolution_diversification_section


def build_albatroz_risk_budget(df_pa: pd.DataFrame) -> str:
    """
    Risk Budget card for ALBATROZ — 150 bps/month stop.
    Reuses the stop_bar_svg helper from the MACRO stop monitor.
    Pulls MTD + YTD alpha from the already-fetched `df_pa` (REPORT_ALPHA_ATRIBUTION leaves).
    """
    if df_pa is None or df_pa.empty:
        return ""
    alb = df_pa[df_pa["FUNDO"] == "ALBATROZ"]
    if alb.empty:
        return ""

    mtd_bps = float(alb["mtd_bps"].sum())
    ytd_bps = float(alb["ytd_bps"].sum())
    dia_bps = float(alb["dia_bps"].sum())

    budget_abs = ALBATROZ_STOP_BPS
    consumed_bps = abs(mtd_bps) if mtd_bps < 0 else 0.0
    consumed_pct = consumed_bps / budget_abs * 100.0

    # Status semáforo
    if consumed_pct >= 100:
        status_label, status_color = "🔴 STOP", "var(--down)"
    elif consumed_pct >= 70:
        status_label, status_color = "🟡 ATENÇÃO", "var(--warn)"
    elif consumed_pct >= 50:
        status_label, status_color = "🟡 SOFT", "var(--warn)"
    else:
        status_label, status_color = "🟢 OK", "var(--up)"

    margem_bps = budget_abs + mtd_bps  # distance from stop (in bps, positive = room left)
    bar = stop_bar_svg(budget_abs, mtd_bps, budget_abs * 1.2, width=340, height=56)

    dia_color = "var(--up)" if dia_bps >= 0 else "var(--down)"
    mtd_color = "var(--up)" if mtd_bps >= 0 else "var(--down)"
    ytd_color = "var(--up)" if ytd_bps >= 0 else "var(--down)"

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risk Budget</span>
        <span class="card-sub">— ALBATROZ · stop mensal {budget_abs:.0f} bps · alpha vs. CDI</span>
      </div>
      <table class="metric-table" data-no-sort="1" style="width:100%">
        <thead>
          <tr class="col-headers">
            <th>Status</th>
            <th style="text-align:right">DIA</th>
            <th style="text-align:right">MTD</th>
            <th style="text-align:right">YTD</th>
            <th>Consumo do stop</th>
            <th style="text-align:right">Margem</th>
          </tr>
        </thead>
        <tbody>
          <tr class="metric-row">
            <td class="metric-name"><span style="color:{status_color}; font-weight:700">{status_label}</span></td>
            <td class="value-cell mono" style="color:{dia_color}">{dia_bps:+.1f} bps</td>
            <td class="value-cell mono" style="color:{mtd_color}; font-weight:700">{mtd_bps:+.1f} bps</td>
            <td class="value-cell mono" style="color:{ytd_color}">{ytd_bps:+.1f} bps</td>
            <td class="bar-cell">{bar}</td>
            <td class="util-cell mono" style="color:{status_color}; font-weight:700">{margem_bps:+.1f} bps</td>
          </tr>
        </tbody>
      </table>
      <div class="bar-legend">
        consumo: <span style="color:{status_color}; font-weight:600">{consumed_pct:.0f}%</span> do stop
        &nbsp;·&nbsp; stop em <span class="mono">-{budget_abs:.0f} bps</span>
        &nbsp;·&nbsp; <span style="color:var(--muted)">origem = início do mês</span>
      </div>
    </section>"""


def build_single_names_section(fund_short: str, sub_label: str,
                                df: pd.DataFrame, nav: float,
                                index_legs: dict) -> str:
    """
    Inline single-name L/S section for a given fund. Full detail (stats header + Top 8 L/S).
    `index_legs` is a dict of {leg_name: delta_in_brl} for each exploded index leg
    (WIN, BOVA11, SMAL11 …). Zero legs are hidden from the header.
    """
    if df is None or df.empty:
        return ""

    longs  = df[df["net"] > 0].sort_values("net", ascending=False).head(8)
    shorts = df[df["net"] < 0].sort_values("net", ascending=True).head(8)
    gross_l = df.loc[df["net"] > 0, "net"].sum()
    gross_s = df.loc[df["net"] < 0, "net"].sum()
    gross   = abs(gross_l) + abs(gross_s)
    net     = gross_l + gross_s
    n_total = len(df)
    n_long  = int((df["net"] > 0).sum())
    n_short = int((df["net"] < 0).sum())

    def fmt_row(row):
        net_pct    = row["net"]      * 100 / nav
        direct_pct = row["direct"]   * 100 / nav
        idx_pct    = row["from_idx"] * 100 / nav
        color      = "var(--up)" if row["net"] > 0 else "var(--down)"
        dir_disp   = f"{direct_pct:+.2f}%" if abs(row["direct"])   > 1 else "—"
        idx_disp   = f"{idx_pct:+.2f}%"    if abs(row["from_idx"]) > 1 else "—"
        return (
            f'<tr>'
            f'<td class="t-name">{row["ticker"]}</td>'
            f'<td class="t-num mono">{dir_disp}</td>'
            f'<td class="t-num mono">{idx_disp}</td>'
            f'<td class="t-num mono" style="color:{color}; font-weight:700">{net_pct:+.2f}%</td>'
            f'</tr>'
        )

    def side_table(title, color_var, rows_df, count):
        if rows_df.empty:
            body = '<tr><td colspan="4" class="t-empty">(sem posições)</td></tr>'
        else:
            body = "".join(fmt_row(r) for r in rows_df.to_dict("records"))
        return f"""
        <div class="sn-side">
          <div class="sn-side-head" style="color:{color_var}">
            {title} <span class="sn-side-count">· {count} names</span>
          </div>
          <table class="sn-table">
            <thead><tr>
              <th style="text-align:left">Ticker</th>
              <th style="text-align:right">Direct</th>
              <th style="text-align:right">From Idx</th>
              <th style="text-align:right">Net %NAV</th>
            </tr></thead>
            <tbody>{body}</tbody>
          </table>
        </div>"""

    # Breakdown of the index-explosion leg — only show non-zero sources
    leg_bits = []
    for leg_name, delta in (index_legs or {}).items():
        if nav and abs(delta) > 1:
            leg_bits.append(f"{leg_name} {delta*100/nav:+.2f}%")
    legs_html = (" &nbsp;|&nbsp; " + " · ".join(leg_bits)) if leg_bits else ""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Single-Name Exposure</span>
        <span class="card-sub">— {fund_short} · {sub_label}</span>
      </div>
      <div class="sn-inline-stats mono">
        Gross L <span style="color:var(--up)">{gross_l*100/nav:+.2f}%</span> &nbsp;|&nbsp;
        Gross S <span style="color:var(--down)">{gross_s*100/nav:+.2f}%</span> &nbsp;|&nbsp;
        Gross <span style="color:var(--text)">{gross*100/nav:.2f}%</span> &nbsp;|&nbsp;
        Net <span style="color:var(--text)">{net*100/nav:+.2f}%</span> &nbsp;|&nbsp;
        {n_total} names ({n_long}L / {n_short}S){legs_html}
      </div>
      <div class="sn-sides">
        {side_table("TOP 8 LONG",  "var(--up)",   longs,  n_long)}
        {side_table("TOP 8 SHORT", "var(--down)", shorts, n_short)}
      </div>
    </section>"""


def _dist_entries(fund_short):
    return [p for p in _DIST_PORTFOLIOS if p[4] == fund_short]


def _kind_tag(kind):
    label = {"fund":"FUND", "livro":"PM", "rf":"FATOR"}.get(kind, "")
    color = {"FUND":"var(--accent-2)", "PM":"var(--text)", "FATOR":"var(--muted)"}.get(label, "var(--muted)")
    return label, color


def build_vol_regime_section(fund_short: str, vol_regime_map: dict) -> str:
    """Per-fund Vol Regime card: fund total + per-book rows.
       Carteira atual × janela HS. std rolling 21d vs. std da janela completa.
       Primary: pct_rank (non-parametric). Ratio = vol 21d / vol full."""
    portfolios = _VR_PORTFOLIOS.get(fund_short, [])
    if not portfolios or not vol_regime_map:
        return ""

    regime_col = {
        "low": "var(--up)", "normal": "var(--muted)",
        "elevated": "var(--warn)", "stressed": "var(--down)", "—": "var(--muted)",
    }
    def _pct_col(p):
        if p is None: return "var(--muted)"
        if p >= 90: return "var(--down)"
        if p >= 70: return "var(--warn)"
        if p < 20:  return "var(--up)"
        return "var(--text)"
    def _ratio_col(r):
        if r is None: return "var(--muted)"
        if r >= 1.5: return "var(--down)"
        if r >= 1.2: return "var(--warn)"
        if r <= 0.7: return "var(--up)"
        return "var(--text)"

    def _z_col(z):
        if z is None:      return "var(--muted)"
        if abs(z) >= 2:    return "var(--down)"
        if abs(z) >= 1:    return "var(--warn)"
        return "var(--text)"

    rows = ""
    any_data = False
    any_book_with_data = False
    n_obs_ref = None
    fund_label = FUND_LABELS.get(fund_short, fund_short)
    for pkey, label, kind in portfolios:
        r = vol_regime_map.get(pkey)
        tag, tag_c = _kind_tag(kind)
        is_fund = (kind == "fund")
        # Parent fund row = pinned, never sorts away. Children = hidden by default,
        # toggled via the ▶ caret in the parent.
        if is_fund:
            caret = f'<span class="vr-caret" data-fs="{fund_short}" style="cursor:pointer;user-select:none;color:var(--accent-2);font-weight:700;margin-right:4px">▼</span>'
            row_cls = 'metric-row vr-row vr-fund'
            row_attr = f' data-pinned="1" data-fs="{fund_short}"'
        else:
            caret = ""
            row_cls = 'metric-row vr-row vr-book'
            row_attr = f' data-parent="{fund_short}"'

        if not r:
            continue
        any_data = True
        if not is_fund:
            any_book_with_data = True
        if n_obs_ref is None:
            n_obs_ref = r["n_obs"]
        vol_r = r["vol_recent_pct"]
        ratio = r["ratio"]
        pct_v = r["pct_rank"]
        z_v   = r["z"]
        regime = r["regime"]
        pct_s = f'{pct_v:.0f}°' if pct_v is not None else '—'
        z_s   = f'{z_v:+.2f}'   if z_v is not None   else '—'

        vol_range = range_line_svg(
            vol_r, r["vol_min_pct"], r["vol_max_pct"],
            v_p50=r["vol_p50_pct"], fmt="{:.2f}%",
        )
        z_range = range_line_svg(
            z_v, r["z_min"], r["z_max"],
            v_p50=r["z_p50"], fmt="{:+.1f}",
        ) if z_v is not None else ""

        rows += (
            f'<tr class="{row_cls}"{row_attr}>'
            f'<td class="dist-tag" style="color:{tag_c}">{caret}{tag}</td>'
            f'<td class="dist-name">{label}</td>'
            f'<td class="dist-num mono">{vol_r:.2f}%</td>'
            f'<td style="text-align:center;padding:4px 6px">{vol_range}</td>'
            f'<td class="dist-num mono" style="color:{_ratio_col(ratio)}; font-weight:700">{ratio:.2f}x</td>'
            f'<td style="text-align:center;padding:4px 6px">{z_range}</td>'
            f'<td class="dist-num mono" style="color:{_z_col(z_v)}; font-weight:700">{z_s}</td>'
            f'<td class="dist-num mono" style="color:{_pct_col(pct_v)}; font-weight:700">{pct_s}</td>'
            f'<td class="mono" style="color:{regime_col.get(regime,"var(--muted)")}; text-align:center; font-weight:700">{regime}</td>'
            "</tr>"
        )
    if not any_data:
        return ""
    sub = f"— {fund_short} · carteira atual × HS ({n_obs_ref}d)"
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Vol Regime</span>
        <span class="card-sub">{sub}</span>
      </div>
      <table class="metric-table dist-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left;width:70px">Tipo</th>
            <th style="text-align:left">Nome</th>
            <th style="text-align:right;width:80px">Vol 21d</th>
            <th style="text-align:center;width:230px">Range Vol</th>
            <th style="text-align:right;width:70px">Ratio</th>
            <th style="text-align:center;width:230px">Range z</th>
            <th style="text-align:right;width:70px">z</th>
            <th style="text-align:right;width:70px">Pct</th>
            <th style="text-align:center;width:90px">Regime</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        <b>▶</b> clique no caret para expandir books/PMs do fundo ·
        <b>Range Vol / Range z</b>: linha = [min, max] da janela HS; tick cinza = mediana; dot = atual ·
        <b>Ratio</b> = vol 21d / vol full (&gt;1.2 amarelo · &gt;1.5 vermelho · &lt;0.7 verde) ·
        <b>z</b> = (std 21d − média) / σ das rolling windows (direcional; N_eff baixo pela sobreposição) ·
        <b>Pct</b> = percentil da std(21d) ·
        <b>Regime</b>: low/normal/elevated/stressed via pct
      </div>
    </section>"""


def _build_top_windows_modal(modal_id: str, fund_short: str, sections: list) -> str:
    """Modal listing 5 worst + 5 best non-overlapping 21d windows.

    sections: list of (key, title, w_array). Each section pre-renders one
    table block (hidden except the first). Toggle at the top swaps which
    section is visible. Shown when the user clicks "Top 5 piores · 5 melhores"
    in the 21d view.
    """
    if not sections:
        return ""

    def _table_block(w, n_obs: int) -> str:
        ext = compute_top_windows(w, k=5, window_days=21)
        if not ext or (not ext.get("worst") and not ext.get("best")):
            return ""

        def _row(item, idx, color):
            return (
                f'<tr>'
                f'<td style="color:var(--muted);text-align:right">{idx+1}</td>'
                f'<td class="mono" style="text-align:right">{item["n_back"]}d atrás</td>'
                f'<td class="mono" style="text-align:right;color:{color};font-weight:700">{item["sum_bps"]:+.1f}</td>'
                f'<td class="mono" style="text-align:right">{item["mean_bps"]:+.2f}</td>'
                f'<td class="mono" style="text-align:right;color:var(--down)">{item["min_day"]:+.1f}</td>'
                f'<td class="mono" style="text-align:right;color:var(--up)">{item["max_day"]:+.1f}</td>'
                f'</tr>'
            )

        worst_rows = "".join(_row(it, i, "var(--down)") for i, it in enumerate(ext["worst"]))
        best_rows  = "".join(_row(it, i, "var(--up)")   for i, it in enumerate(ext["best"]))
        head = (
            '<thead><tr>'
            '<th style="text-align:right;width:34px">#</th>'
            '<th style="text-align:right">Encerra</th>'
            '<th style="text-align:right">Σ 21d (bps)</th>'
            '<th style="text-align:right">Média/dia</th>'
            '<th style="text-align:right">Pior dia</th>'
            '<th style="text-align:right">Melhor dia</th>'
            '</tr></thead>'
        )
        return (
            f'<div style="font-size:11px;color:var(--muted);margin-bottom:8px">{ext["n_obs"]} janelas · sem sobreposição · base HS</div>'
            f'<div style="color:var(--down);font-size:11px;font-weight:700;margin:8px 0 4px">5 PIORES</div>'
            f'<table class="metric-table" data-no-sort="1" style="width:100%">{head}<tbody>{worst_rows}</tbody></table>'
            f'<div style="color:var(--up);font-size:11px;font-weight:700;margin:14px 0 4px">5 MELHORES</div>'
            f'<table class="metric-table" data-no-sort="1" style="width:100%">{head}<tbody>{best_rows}</tbody></table>'
        )

    rendered = []
    for key, title, w in sections:
        block = _table_block(w, len(w) if w is not None else 0)
        if not block:
            continue
        rendered.append((key, title, block))
    if not rendered:
        return ""

    # Toggle: shown only if more than one section (e.g. IDKA cards).
    if len(rendered) > 1:
        btns = "".join(
            f'<button class="dist-btn dist-top-sec-btn{(" active" if i == 0 else "")}" '
            f'data-sec="{key}" onclick="setDistTopSection(\'{modal_id}\',\'{key}\')">{title}</button>'
            for i, (key, title, _) in enumerate(rendered)
        )
        toggle_html = f'<div class="dist-toggle" style="margin-bottom:12px">{btns}</div>'
    else:
        toggle_html = ""

    body = "".join(
        f'<div class="dist-top-sec{(" active" if i == 0 else "")}" data-sec="{key}">'
        f'<div style="font-weight:700;font-size:13px;margin-bottom:6px;color:var(--text)">{title}</div>'
        f'{block}</div>'
        for i, (key, title, block) in enumerate(rendered)
    )

    return f"""
    <div id="{modal_id}" class="dist-top-modal" style="display:none">
      <div class="dist-top-backdrop" onclick="closeDistTop('{modal_id}')"></div>
      <div class="dist-top-card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
          <div>
            <div style="font-size:14px;font-weight:700;color:var(--text)">Top janelas 21d — {fund_short}</div>
            <div style="font-size:11px;color:var(--muted)">Janelas de 21 dias úteis sem sobreposição · bps de NAV</div>
          </div>
          <button onclick="closeDistTop('{modal_id}')" style="background:transparent;border:1px solid var(--line);color:var(--muted);border-radius:4px;padding:4px 10px;cursor:pointer;font-size:13px">×</button>
        </div>
        {toggle_html}
        {body}
      </div>
    </div>"""


def build_distribution_card(fund_short: str, dist_map_now: dict, dist_map_prev: dict,
                            actuals: dict) -> str:
    """Distribuição card with toggles: Backward/Forward × 1d/21d (rolling sum).
       21d view = distribution of rolling 21-day cumulative returns.
       21d adds a "Top 5 piores / 5 melhores" button → modal with non-overlapping
       extremes.
    """
    # Build all 4 base variants (mode × window-days)
    bw252 = _build_backward_table(fund_short, dist_map_prev, actuals, 1)
    fw252 = _build_forward_table(fund_short, dist_map_now, 1)
    bw21  = _build_backward_table(fund_short, dist_map_prev, actuals, 21)
    fw21  = _build_forward_table(fund_short, dist_map_now, 21)
    if not (bw252 or fw252 or bw21 or fw21):
        return ""
    dck_id = f"dist-{fund_short.lower()}"
    _sub_map = {
        "FRONTIER": f"— {fund_short} · α vs IBOV · bps de NAV · realizado",
        "IDKA_3Y":  "— IDKA 3Y · HS active return · bps de NAV",
        "IDKA_10Y": "— IDKA 10Y · HS active return · bps de NAV",
        "ALBATROZ": "— ALBATROZ · HS · bps de NAV",
    }
    sub = _sub_map.get(fund_short, f"— {fund_short} · bps de NAV")

    _EMPTY_BW = '<div class="empty-view">Sem dados backward (D-1 sem simulação).</div>'
    _EMPTY_FW = '<div class="empty-view">Sem dados forward.</div>'

    # 1d/21d window toggle. Both buttons share the same .dist-btn styling so
    # the active state shows the canonical blue accent gradient.
    modal_id = f"{dck_id}-top21"
    window_toggle_html = (
        f'<div class="dist-toggle" style="margin-right:6px">'
        f'<button class="dist-btn active" data-window="1" '
        f'onclick="setDistWindow(\'{dck_id}\',\'1\')">1d</button>'
        f'<button class="dist-btn"        data-window="21" '
        f'onclick="setDistWindow(\'{dck_id}\',\'21\')">21d</button>'
        f'</div>'
        f'<button class="dist-btn dist-top21-btn" '
        f'onclick="openDistTop(\'{modal_id}\')" '
        f'style="display:none;background:linear-gradient(180deg,var(--accent-2),var(--accent));color:#fff;border:1px solid var(--line);margin-right:6px">'
        f'5 piores · 5 melhores</button>'
    )

    def _view(mode: str, window: str, html: str, default_active: bool) -> str:
        """One <div.dist-view> with both data-mode and data-window."""
        visible = default_active and mode == "forward" and window == "1"
        cls = "dist-view" + (" active" if visible else "")
        style = "" if visible else ' style="display:none"'
        return f'<div class="{cls}" data-mode="{mode}" data-window="{window}"{style}>{html}</div>'

    # ── IDKA: extra bench toggle (vs Benchmark / vs Replication / Comparação) ──
    if fund_short in {"IDKA_3Y", "IDKA_10Y"}:
        rep_short = f"{fund_short}_REP"
        cmp_short = f"{fund_short}_CMP"
        bw_rep_1   = _build_backward_table(rep_short, dist_map_prev, actuals, 1)
        fw_rep_1   = _build_forward_table(rep_short, dist_map_now, 1)
        bw_rep_21  = _build_backward_table(rep_short, dist_map_prev, actuals, 21)
        fw_rep_21  = _build_forward_table(rep_short, dist_map_now, 21)
        bw_cmp_1   = _build_backward_table(cmp_short, dist_map_prev, actuals, 1)
        fw_cmp_1   = _build_forward_table(cmp_short, dist_map_now, 1)
        bw_cmp_21  = _build_backward_table(cmp_short, dist_map_prev, actuals, 21)
        fw_cmp_21  = _build_forward_table(cmp_short, dist_map_now, 21)
        has_rep = bool(bw_rep_1 or fw_rep_1 or bw_rep_21 or fw_rep_21)
        has_cmp = bool(bw_cmp_1 or fw_cmp_1 or bw_cmp_21 or fw_cmp_21)

        def _bench_btn(bench, label, active=False, disabled=False):
            cls = "dist-bench-btn" + (" active" if active else "")
            dis = ' disabled title="Dados indisponíveis"' if disabled else ""
            return (f'<button class="{cls}" data-bench="{bench}"'
                    f' onclick="setDistBench(\'{dck_id}\',\'{bench}\')"{dis}>{label}</button>')

        # Default bench section: Comparação when available (IDKAs), else vs Benchmark.
        _cmp_default = bool(has_cmp)

        bench_toggle_html = (
            f'<div class="dist-toggle" style="margin-right:6px">'
            + _bench_btn("benchmark",  "vs Benchmark",  active=not _cmp_default)
            + _bench_btn("replication","vs Replication", disabled=not has_rep)
            + _bench_btn("comparison", "Comparação",    active=_cmp_default,
                          disabled=not has_cmp)
            + '</div>'
        )

        _bench_hidden = ' style="display:none"' if _cmp_default else ''
        sections_html = (
            f'<div data-bench-section="benchmark"{_bench_hidden}>'
            f'{_view("backward","1",  bw252 or _EMPTY_BW, not _cmp_default)}'
            f'{_view("forward","1",   fw252 or _EMPTY_FW, not _cmp_default)}'
            f'{_view("backward","21", bw21  or _EMPTY_BW, not _cmp_default)}'
            f'{_view("forward","21",  fw21  or _EMPTY_FW, not _cmp_default)}'
            f'</div>'
        )
        if has_rep:
            sections_html += (
                f'<div data-bench-section="replication" style="display:none">'
                f'{_view("backward","1",  bw_rep_1   or _EMPTY_BW, False)}'
                f'{_view("forward","1",   fw_rep_1   or _EMPTY_FW, False)}'
                f'{_view("backward","21", bw_rep_21  or _EMPTY_BW, False)}'
                f'{_view("forward","21",  fw_rep_21  or _EMPTY_FW, False)}'
                f'</div>'
            )
        if has_cmp:
            _cmp_note = (
                '<div class="bar-legend" style="margin-top:6px">'
                '<b>vs Benchmark</b> = retorno HS do fundo − retorno do índice IDKA · '
                '<b>vs Replication</b> = retorno HS do fundo − retorno da réplica NTN-B (DV-match) · '
                '<b>Repl − Bench (spread)</b> = retorno da réplica − retorno do índice: '
                'positivo = réplica superou o índice naquele cenário histórico'
                '</div>'
            )
            sections_html += (
                f'<div data-bench-section="comparison">'
                f'{_view("backward","1",  (bw_cmp_1  or _EMPTY_BW)+_cmp_note, True)}'
                f'{_view("forward","1",   (fw_cmp_1  or _EMPTY_FW)+_cmp_note, True)}'
                f'{_view("backward","21", (bw_cmp_21 or _EMPTY_BW)+_cmp_note, True)}'
                f'{_view("forward","21",  (fw_cmp_21 or _EMPTY_FW)+_cmp_note, True)}'
                f'</div>'
            )
        # Modal: 5 worst + 5 best 21d non-overlapping windows. 3 sections by series:
        #  - "vs Benchmark"  → fund_short series       (e.g. IDKA_10Y)
        #  - "vs Replication"→ rep_short series        (e.g. IDKA_10Y_REP)
        #  - "Repl − Bench"  → spread series           (e.g. IDKA_10Y_SPREAD)
        modal_sections = []
        bench_w  = dist_map_now.get(fund_short)
        rep_w    = dist_map_now.get(rep_short)
        spread_w = dist_map_now.get(f"{fund_short}_SPREAD")
        if bench_w is not None and len(bench_w) >= 21:
            modal_sections.append(("benchmark", "vs Benchmark", bench_w))
        if rep_w is not None and len(rep_w) >= 21:
            modal_sections.append(("replication", "vs Replication", rep_w))
        if spread_w is not None and len(spread_w) >= 21:
            modal_sections.append(("spread", "Repl − Bench (spread)", spread_w))
        modal_html = _build_top_windows_modal(modal_id, fund_short, modal_sections)
        return f"""
    <section class="card" id="{dck_id}" data-active-mode="forward" data-active-window="1">
      <div class="card-head" style="display:flex;align-items:center;justify-content:space-between">
        <div>
          <span class="card-title">Distribuição</span>
          <span class="card-sub">{sub}</span>
        </div>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          {bench_toggle_html}
          {window_toggle_html}
          <div class="dist-toggle">
            <button class="dist-btn"        data-mode="backward" onclick="setDistMode('{dck_id}','backward')">Backward</button>
            <button class="dist-btn active" data-mode="forward"  onclick="setDistMode('{dck_id}','forward')">Forward</button>
          </div>
        </div>
      </div>
      {sections_html}
      {modal_html}
    </section>"""

    # Non-IDKA path: single-bench card.
    primary_w = next((dist_map_now.get(p[0]) for p in _dist_entries(fund_short) if p[2] == "fund"), None)
    modal_html = ""
    if primary_w is not None and len(primary_w) >= 21:
        modal_html = _build_top_windows_modal(
            modal_id, fund_short, [("fund", fund_short, primary_w)]
        )
    return f"""
    <section class="card" id="{dck_id}" data-active-mode="forward" data-active-window="1">
      <div class="card-head" style="display:flex;align-items:center;justify-content:space-between">
        <div>
          <span class="card-title">Distribuição</span>
          <span class="card-sub">{sub}</span>
        </div>
        <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
          {window_toggle_html}
          <div class="dist-toggle">
            <button class="dist-btn"        data-mode="backward" onclick="setDistMode('{dck_id}','backward')">Backward</button>
            <button class="dist-btn active" data-mode="forward"  onclick="setDistMode('{dck_id}','forward')">Forward</button>
          </div>
        </div>
      </div>
      {_view("backward","1",  bw252 or _EMPTY_BW, True)}
      {_view("forward","1",   fw252 or _EMPTY_FW, True)}
      {_view("backward","21", bw21  or _EMPTY_BW, True)}
      {_view("forward","21",  fw21  or _EMPTY_FW, True)}
      {modal_html}
    </section>"""


def _to_rolling_sum(w, window: int):
    """Daily-return series → rolling cumulative-sum series (length n - window + 1)."""
    arr = np.asarray(w, dtype=float)
    arr = arr[~np.isnan(arr)]
    if len(arr) < window:
        return np.array([])
    if window <= 1:
        return arr
    csum = np.cumsum(arr)
    return np.concatenate(([csum[window - 1]], csum[window:] - csum[:-window]))


def _build_backward_table(fund_short: str, dist_map_prev: dict, actuals: dict,
                          window_days: int = 1) -> str:
    """Backward-looking: D-1 carteira distribution with today's realized DIA overlayed.

    window_days==1 (default): each obs = 1-day HS return; actual_bps = today's DIA.
    window_days>1: each obs = rolling sum of `window_days` consecutive 1-day HS
    returns. The overlay (actual_bps) is omitted because realized 21d returns
    aren't tracked here — the rolling-sum distribution alone is shown.
    """
    if not dist_map_prev:
        return ""
    rows = ""
    for portfolio_name, label, kind, key, fs in _dist_entries(fund_short):
        w_raw = dist_map_prev.get(portfolio_name)
        if w_raw is None or len(w_raw) < max(window_days * 2, 30):
            continue
        w = _to_rolling_sum(w_raw, window_days) if window_days > 1 else np.asarray(w_raw, dtype=float)
        if len(w) < 30:
            continue
        actual = actuals.get(f"{kind}:{key}") if window_days == 1 else None
        stats = compute_distribution_stats(w, actual)
        if stats is None or abs(stats["max"] - stats["min"]) < 1e-6:
            continue

        if "actual" not in stats:
            continue  # nothing to compare; skip backward row

        a   = stats["actual"]; pct = stats["percentile"]; nv = stats["nvols"]
        col = "var(--up)" if a > 0 else "var(--down)" if a < 0 else "var(--muted)"
        pct_col = "var(--down)" if pct < 10 else "var(--up)" if pct > 90 else "var(--text)"
        nv_s    = f"{nv:+.2f}" if nv is not None else "—"
        tag, tag_c = _kind_tag(kind)

        if kind == "fund":
            caret = f'<span class="dist-caret" data-dist-parent="{fund_short}">▼</span> '
            row_attrs = (f'class="metric-row dist-row-fund" data-dist-kind="fund" '
                         f'data-dist-key="{fund_short}" '
                         f'onclick="toggleDistChildren(this)" style="cursor:pointer"')
        else:
            caret = ''
            row_attrs = (f'class="metric-row dist-row-child" '
                         f'data-dist-kind="{kind}" data-dist-parent="{fund_short}"')

        rows += f"""
        <tr {row_attrs}>
          <td class="dist-tag" style="color:{tag_c}">{tag}</td>
          <td class="dist-name">{caret}{label}</td>
          <td class="dist-num mono" style="color:{col}; font-weight:700">{a:+.0f}</td>
          <td class="dist-num mono" style="color:{pct_col}; font-weight:600">{pct:.0f}°</td>
          <td class="dist-num mono">{nv_s}</td>
        </tr>"""

    if not rows:
        return ""
    return f"""
      <table class="metric-table dist-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left;width:54px">Tipo</th>
            <th style="text-align:left">Nome</th>
            <th style="text-align:right;width:70px">DIA</th>
            <th style="text-align:right;width:80px">Percentil</th>
            <th style="text-align:right;width:60px">#σ</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        <b>DIA</b> = PnL realizado hoje (bps NAV) · <b>Percentil</b> = posição ordinal de DIA na distribuição hipotética (5° e 95° são caudas) · <b>#σ</b> = DIA / σ histórico
      </div>"""


def _build_forward_table(fund_short: str, dist_map: dict, window_days: int = 1) -> str:
    """Forward-looking: today's carteira × historical scenarios. Describes expected P&L profile.

    window_days==1: each obs = 1-day HS return.
    window_days>1: each obs = rolling-sum of `window_days` consecutive 1-day HS returns.
    """
    if not dist_map:
        return ""
    rows = ""
    for portfolio_name, label, kind, key, fs in _dist_entries(fund_short):
        w_raw = dist_map.get(portfolio_name)
        if w_raw is None or len(w_raw) < max(window_days * 2, 30):
            continue
        w = _to_rolling_sum(w_raw, window_days) if window_days > 1 else np.asarray(w_raw, dtype=float)
        if len(w) < 30:
            continue
        stats = compute_distribution_stats(w, None)
        if stats is None or abs(stats["max"] - stats["min"]) < 1e-6:
            continue
        tag, tag_c = _kind_tag(kind)
        if kind == "fund":
            caret = f'<span class="dist-caret" data-dist-parent="{fund_short}">▼</span> '
            row_attrs = (f'class="metric-row dist-row-fund" data-dist-kind="fund" '
                         f'data-dist-key="{fund_short}" '
                         f'onclick="toggleDistChildren(this)" style="cursor:pointer"')
        else:
            caret = ''
            row_attrs = (f'class="metric-row dist-row-child" '
                         f'data-dist-kind="{kind}" data-dist-parent="{fund_short}"')
        rows += f"""
        <tr {row_attrs}>
          <td class="dist-tag" style="color:{tag_c}">{tag}</td>
          <td class="dist-name">{caret}{label}</td>
          <td class="dist-num mono" style="color:var(--down)">{stats['min']:.0f}</td>
          <td class="dist-num mono" style="color:var(--warn)">{stats['var95']:.0f}</td>
          <td class="dist-num mono" style="color:var(--muted)">{stats['mean']:+.1f}</td>
          <td class="dist-num mono" style="color:var(--accent-2)">{stats['var_p95']:+.0f}</td>
          <td class="dist-num mono" style="color:var(--up)">{stats['max']:+.0f}</td>
          <td class="dist-num mono" style="color:var(--muted)">{stats['sd']:.1f}</td>
        </tr>"""

    if not rows:
        return ""
    return f"""
      <table class="metric-table dist-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left;width:54px">Tipo</th>
            <th style="text-align:left">Nome</th>
            <th style="text-align:right;width:60px">Min</th>
            <th style="text-align:right;width:70px">p05</th>
            <th style="text-align:right;width:60px">Média</th>
            <th style="text-align:right;width:70px">p95</th>
            <th style="text-align:right;width:60px">Max</th>
            <th style="text-align:right;width:55px">σ</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        <b>Min/Max</b> = extremos dos 252 PnLs hipotéticos · <b>p05</b> = 5° percentil (cauda de perda, VaR 95%) · <b>p95</b> = 95° percentil (cauda de ganho) · <b>Média/σ</b> = expectativa diária e vol
      </div>"""


def _build_stop_history_modal(stop_history: dict[str, pd.DataFrame],
                               df_pm_book_pnl: pd.DataFrame | None = None) -> str:
    """Modal with month-by-month PnL + carry evolution per PM + overrides editor.
       Triggered by a button in the Risk Budget Monitor card head.
       df_pm_book_pnl: optional (mes, LIVRO, BOOK, pnl_mes_bps) — when present, each
       month row becomes expandable to show per-BOOK breakdown of that month's PnL."""
    PM_ORDER  = ["CI", "LF", "JD", "RJ"]
    PM_LABELS = {"CI": "CI (Comitê)", "LF": "Luiz Felipe", "JD": "Joca Dib",
                 "RJ": "Rodrigo Jafet"}
    LIVRO_OF  = {"CI": "CI", "LF": "Macro_LF", "JD": "Macro_JD", "RJ": "Macro_RJ"}

    # Index BOOK breakdown by (livro, mes) → list of (book, pnl) sorted by |pnl| desc.
    book_idx: dict[tuple[str, pd.Timestamp], list[tuple[str, float]]] = {}
    if df_pm_book_pnl is not None and not df_pm_book_pnl.empty:
        for (livro, mes), sub in df_pm_book_pnl.groupby(["LIVRO", "mes"]):
            items = [(str(r.BOOK), float(r.pnl_mes_bps)) for r in sub.itertuples(index=False)]
            items.sort(key=lambda x: -abs(x[1]))
            book_idx[(livro, pd.Timestamp(mes).normalize())] = items

    # Overrides ativos (para exibir no sub-tab Overrides)
    active_overrides = _load_risk_budget_overrides()
    ovr_rows_html = ""
    for (livro, mes), budget in sorted(active_overrides.items(), key=lambda x: (x[0][1], x[0][0])):
        ovr_rows_html += (
            f'<tr>'
            f'<td style="padding:5px 8px"><b>{livro}</b></td>'
            f'<td style="padding:5px 8px">{pd.Timestamp(mes).strftime("%Y-%m")}</td>'
            f'<td class="mono" style="text-align:right;padding:5px 8px;font-weight:700">{float(budget):.1f}</td>'
            f'</tr>'
        )
    if not ovr_rows_html:
        ovr_rows_html = '<tr><td colspan="3" style="padding:8px;color:var(--muted);text-align:center">— nenhum override ativo</td></tr>'

    # Options de PM e mês (próximos 3 meses a partir do atual)
    today = pd.Timestamp.today().normalize()
    month_opts = []
    for i in range(-1, 4):  # mês anterior + atual + próximos 3
        m = (today.to_period("M") + i).to_timestamp()
        month_opts.append(m.strftime("%Y-%m"))
    pm_options = "".join(f'<option value="{LIVRO_OF[pm]}">{LIVRO_OF[pm]} ({PM_LABELS[pm]})</option>' for pm in PM_ORDER)
    mes_options = "".join(f'<option value="{m}">{m}</option>' for m in month_opts)

    tabs_html   = ""
    tables_html = ""
    first_pm = None
    for pm in PM_ORDER:
        if pm not in stop_history or stop_history[pm].empty:
            continue
        hist = stop_history[pm].sort_values("mes")
        if first_pm is None:
            first_pm = pm
        active_cls = "active" if pm == first_pm else ""
        tabs_html += (
            f'<button class="pa-tgl {active_cls}" data-stop-pm="{pm}" '
            f'onclick="selectStopPm(this,\'{pm}\')">{PM_LABELS[pm]}</button>'
        )
        # Build rows — month-by-month (with optional BOOK drill)
        livro_for_pm = LIVRO_OF[pm]
        rows_html = ""
        for r in hist.itertuples(index=False):
            mes_lbl  = r.mes.strftime("%b/%y")
            pnl      = float(r.pnl)
            ytd      = float(r.ytd) + pnl   # pnl already added AFTER building row in build_stop_history;
                                             # ytd stored here is BEFORE the month's pnl → adjust for display
            budget   = float(r.budget_abs)
            base     = STOP_BASE if pm != "CI" else 233.0
            delta_c  = budget - base
            pnl_c = "var(--up)" if pnl >= 0 else "var(--down)"
            ytd_c = "var(--up)" if ytd >= 0 else "var(--down)"
            dc_c  = "var(--up)" if delta_c > 0.1 else ("var(--down)" if delta_c < -0.1 else "var(--muted)")

            mes_key = pd.Timestamp(r.mes).normalize()
            books = book_idx.get((livro_for_pm, mes_key), [])
            row_id = f"sh-{pm}-{mes_key.strftime('%Y%m')}"
            if books:
                caret = (f'<span class="sh-caret" style="cursor:pointer;color:var(--accent-2);'
                         f'font-weight:700;margin-right:4px;user-select:none">▶</span>')
                mes_cell = (f'<td style="padding:5px 8px;cursor:pointer" '
                            f'onclick="toggleStopHistRow(\'{row_id}\', this)">'
                            f'{caret}{mes_lbl}</td>')
            else:
                mes_cell = f'<td style="padding:5px 8px;color:var(--muted)">{mes_lbl}</td>'
            rows_html += (
                '<tr>'
                + mes_cell
                + f'<td class="mono" style="text-align:right;padding:5px 8px;color:{pnl_c}">{pnl:+.1f}</td>'
                + f'<td class="mono" style="text-align:right;padding:5px 8px;color:{ytd_c}">{ytd:+.1f}</td>'
                + f'<td class="mono" style="text-align:right;padding:5px 8px;color:var(--muted)">{base:.0f}</td>'
                + f'<td class="mono" style="text-align:right;padding:5px 8px;color:{dc_c}">{delta_c:+.1f}</td>'
                + f'<td class="mono" style="text-align:right;padding:5px 8px;font-weight:700">{budget:.1f}</td>'
                '</tr>'
            )

            if books:
                # Drill child row — book × pnl, sorted by |pnl| desc, sums to month pnl.
                book_rows = ""
                for bk, bpnl in books:
                    bc = "var(--up)" if bpnl >= 0 else "var(--down)"
                    book_rows += (
                        f'<tr>'
                        f'<td style="padding:3px 8px 3px 28px;font-size:11px;color:var(--muted)">{bk}</td>'
                        f'<td class="mono" style="text-align:right;padding:3px 8px;font-size:11px;color:{bc}">{bpnl:+.1f}</td>'
                        f'<td colspan="4"></td>'
                        f'</tr>'
                    )
                rows_html += (
                    f'<tr id="{row_id}" style="display:none;background:rgba(0,0,0,0.18)">'
                    f'<td colspan="6" style="padding:0">'
                    f'<table style="width:100%;border-collapse:collapse">'
                    f'<thead><tr>'
                    f'<th style="text-align:left;padding:4px 28px;font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Book</th>'
                    f'<th style="text-align:right;padding:4px 8px;font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">PnL (bps)</th>'
                    f'<th colspan="4"></th>'
                    f'</tr></thead>'
                    f'<tbody>{book_rows}</tbody>'
                    f'</table></td></tr>'
                )
        display_style = "" if pm == first_pm else 'style="display:none"'
        tables_html += (
            f'<div class="stop-pm-view" data-stop-pm="{pm}" {display_style}>'
            '<table class="summary-table"><thead><tr>'
            '<th style="text-align:left">Mês</th>'
            '<th style="text-align:right">PnL mês (bps)</th>'
            '<th style="text-align:right">PnL YTD</th>'
            '<th style="text-align:right">Base</th>'
            '<th style="text-align:right">Δ Carry</th>'
            '<th style="text-align:right">Budget total</th>'
            '</tr></thead>'
            f'<tbody>{rows_html}</tbody></table></div>'
        )
    if not first_pm:
        return ""

    # Overrides sub-tab — read existing, allow editing, generate JSON payload
    overrides_tab = f'<button class="pa-tgl" data-stop-pm="__overrides__" onclick="selectStopPm(this,\'__overrides__\')" style="margin-left:12px">⚙ Overrides</button>'

    overrides_view = f"""
    <div class="stop-pm-view" data-stop-pm="__overrides__" style="display:none">
      <div style="font-size:12px;color:var(--muted);margin-bottom:10px">
        Orçamentos manuais lidos de <code>data/mandatos/risk_budget_overrides.json</code>.
        Aplicam no INÍCIO do mês indicado (substitui o carry normal).
      </div>

      <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">
        Overrides ativos
      </div>
      <table class="summary-table" style="margin-bottom:14px">
        <thead><tr>
          <th style="text-align:left">LIVRO</th>
          <th style="text-align:left">Mês</th>
          <th style="text-align:right">Budget (bps)</th>
        </tr></thead>
        <tbody id="ovr-active-tbody">{ovr_rows_html}</tbody>
      </table>

      <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">
        Adicionar / editar override
      </div>
      <div style="display:grid;grid-template-columns:1.3fr 1fr 1fr 1.5fr auto;gap:8px;align-items:center;margin-bottom:10px">
        <select id="ovr-livro" class="mono" style="padding:6px 8px;background:var(--bg-2);color:var(--text);border:1px solid var(--line);border-radius:4px">
          {pm_options}
        </select>
        <select id="ovr-mes" class="mono" style="padding:6px 8px;background:var(--bg-2);color:var(--text);border:1px solid var(--line);border-radius:4px">
          {mes_options}
        </select>
        <input id="ovr-budget" type="number" step="0.1" placeholder="bps" class="mono"
               style="padding:6px 8px;background:var(--bg-2);color:var(--text);border:1px solid var(--line);border-radius:4px;text-align:right"/>
        <input id="ovr-note" type="text" placeholder="nota (ex: quem autorizou)"
               style="padding:6px 8px;background:var(--bg-2);color:var(--text);border:1px solid var(--line);border-radius:4px;font-size:12px"/>
        <button onclick="ovrAddEntry()" class="pa-tgl" style="padding:6px 14px">+ Adicionar</button>
      </div>

      <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">
        JSON gerado — copie e cole em <code>data/mandatos/risk_budget_overrides.json</code>
      </div>
      <textarea id="ovr-json-output" readonly rows="10" class="mono"
        style="width:100%;padding:8px;background:var(--bg-2);color:var(--text);border:1px solid var(--line);border-radius:4px;font-size:11.5px;resize:vertical"></textarea>
      <div style="display:flex;gap:8px;margin-top:8px;align-items:center">
        <button onclick="ovrCopyJson()" class="pa-tgl" style="padding:6px 14px">📋 Copiar JSON</button>
        <button onclick="ovrResetEdit()" class="pa-tgl" style="padding:6px 14px">↺ Resetar edição</button>
        <span id="ovr-status" style="font-size:12px;color:var(--muted)"></span>
      </div>

      <div class="bar-legend" style="margin-top:14px">
        <b>Fluxo:</b>
        (1) Adiciona/edita entries no form acima →
        (2) Copia o JSON →
        (3) Cola em <code>data/mandatos/risk_budget_overrides.json</code> →
        (4) Roda <code>run_report.bat</code> para regenerar.<br>
        Sem backend — o relatório é estático, então a persistência precisa de edit manual do arquivo.
      </div>
    </div>"""

    # Injetar o estado inicial de overrides no JS como JSON
    initial_ovr_json = json.dumps({
        "_description": "Overrides manuais de orçamento — editar via botão '⚙ Overrides' no modal.",
        "overrides": [
            {
                "livro": livro,
                "month": pd.Timestamp(mes).strftime("%Y-%m"),
                "budget_bps": float(budget),
                "note": ""
            }
            for (livro, mes), budget in sorted(active_overrides.items(), key=lambda x: (x[0][1], x[0][0]))
        ]
    }, indent=2, ensure_ascii=False)
    # Escape pra embutir em string JS
    initial_ovr_js = initial_ovr_json.replace("\\", "\\\\").replace("`", "\\`").replace("</", "<\\/")

    ovr_script = f"""
    <script>
    (function() {{
      window.__ovrInitial = {initial_ovr_json};
      window.__ovrState = JSON.parse(JSON.stringify(window.__ovrInitial));
      window.ovrRefresh = function() {{
        var ta = document.getElementById('ovr-json-output');
        if (ta) ta.value = JSON.stringify(window.__ovrState, null, 2);
      }};
      window.ovrAddEntry = function() {{
        var livro = document.getElementById('ovr-livro').value;
        var mes   = document.getElementById('ovr-mes').value;
        var bps   = parseFloat(document.getElementById('ovr-budget').value);
        var note  = (document.getElementById('ovr-note').value || '').trim();
        if (!livro || !mes || isNaN(bps)) {{
          document.getElementById('ovr-status').textContent = '⚠ Preencha LIVRO + Mês + Budget numérico';
          return;
        }}
        // Replace existing entry for same (livro, month) or append
        var arr = window.__ovrState.overrides || [];
        var idx = arr.findIndex(function(e) {{ return e.livro === livro && e.month === mes; }});
        var entry = {{livro: livro, month: mes, budget_bps: bps, note: note}};
        if (idx >= 0) arr[idx] = entry; else arr.push(entry);
        window.__ovrState.overrides = arr;
        window.ovrRefresh();
        document.getElementById('ovr-status').textContent = '✓ Adicionado (lembre de salvar e rodar o .bat)';
        document.getElementById('ovr-budget').value = '';
        document.getElementById('ovr-note').value = '';
      }};
      window.ovrResetEdit = function() {{
        window.__ovrState = JSON.parse(JSON.stringify(window.__ovrInitial));
        window.ovrRefresh();
        document.getElementById('ovr-status').textContent = '↺ Reset para o estado atual do arquivo';
      }};
      window.ovrCopyJson = function() {{
        var ta = document.getElementById('ovr-json-output');
        if (!ta) return;
        ta.select(); ta.setSelectionRange(0, 99999);
        try {{
          document.execCommand('copy');
          document.getElementById('ovr-status').textContent = '📋 Copiado! Cole em data/mandatos/risk_budget_overrides.json';
        }} catch(e) {{
          document.getElementById('ovr-status').textContent = '⚠ Copia manual: Ctrl+A / Ctrl+C no textarea';
        }}
      }};
      // Init on DOMContentLoaded (populates the textarea)
      document.addEventListener('DOMContentLoaded', function() {{
        if (document.getElementById('ovr-json-output')) window.ovrRefresh();
      }});
    }})();
    </script>"""

    return f"""
    <div id="stop-history-modal" class="stop-modal" style="display:none">
      <div class="stop-modal-overlay" onclick="closeStopHistory()"></div>
      <div class="stop-modal-box">
        <div class="stop-modal-head">
          <span class="modal-title">Histórico de Stop por PM — mês a mês</span>
          <button class="stop-modal-close" onclick="closeStopHistory()" title="Fechar">✕</button>
        </div>
        <div class="stop-modal-tabs">{tabs_html}{overrides_tab}</div>
        <div class="stop-modal-body">{tables_html}{overrides_view}</div>
        <div class="bar-legend" style="margin-top:14px">
          <b>Base</b>: stop fixo por mandato (63 bps p/ PMs · 233 bps CI hard stop).
          <b>Δ Carry</b>: ajuste em cima da base — positivo = bônus pelo ganho do mês anterior; negativo = base erodida por perda anterior.
          <b>Budget total</b> = Base + Δ Carry.<br>
          <b>Regras de carry (em vigor):</b>
          (1) <b>pnl positivo</b> → novo budget = 63 + 50% × ganho. Ex: ganho +60 → budget = 63 + 30 = 93.
          (2) <b>pnl negativo</b> → 3 camadas de penalty pro próximo mês:
              <span style="color:var(--up)">extra (acima de 63) = 25%</span> ·
              <span style="color:var(--warn)">base (até 63) = 50%</span> ·
              <span style="color:var(--down)">excedente (acima de B_t) = 100%</span>.
              Novo budget = <code>max(0, min(B_t, 63) − penalty)</code>. Carry extra não consumido evapora ("use it or lose it").
              Ex: B=88, perda=60 → 25%·25 + 50%·35 = 23.75 → 63−23.75 = <b>39.25</b>.
          (3) <b>Reset anual</b>: Janeiro reseta budget=63 (PMs) ou 233 (CI) e YTD=0, independente do carry de Dez.
          (4) <b>Cap semestral</b>: se STOP_SEM − |YTD| &lt; budget mensal, usa o semestral (mais apertado).
          (5) <b>Override</b>: entry no <code>risk_budget_overrides.json</code> substitui o budget calculado naquele mês.
          CI não tem carry (hard stop fixo 233 bps; soft mark 150 bps apenas alerta).
        </div>
      </div>
      {ovr_script}
    </div>"""


def build_stop_section(stop_history: dict[str, pd.DataFrame], df_pnl_today: pd.DataFrame,
                       df_pm_book_pnl: pd.DataFrame | None = None,
                       pm_has_position: dict[str, bool] | None = None) -> str:
    """Build the stop monitor HTML section.
       df_pm_book_pnl: optional (mes, LIVRO, BOOK, pnl_mes_bps) for modal drill-down.
       pm_has_position: optional {pm: bool} — when False and PM is in STOP territory,
       status downgrades from 🔴 STOP to ⚪ FLAT (stopped but no live exposure)."""
    PM_ORDER  = ["CI", "LF", "JD", "RJ"]
    PM_LABELS = {"CI": "CI (Comitê)", "LF": "Luiz Felipe", "JD": "Joca Dib",
                 "RJ": "Rodrigo Jafet"}
    LIVRO_MAP = {"CI": "CI", "LF": "Macro_LF", "JD": "Macro_JD",
                 "RJ": "Macro_RJ"}

    rows = ""
    for pm in PM_ORDER:
        if pm not in stop_history:
            continue
        hist = stop_history[pm]
        if hist.empty:
            continue

        # Current month row
        cur_mes   = pd.Timestamp(DATA_STR).to_period("M").to_timestamp()
        cur_row   = hist[hist["mes"] == cur_mes]
        budget    = cur_row["budget_abs"].iloc[0] if not cur_row.empty else STOP_BASE
        soft_mark = cur_row["soft_mark"].iloc[0] if not cur_row.empty and "soft_mark" in cur_row.columns else None

        # PnL MTD from df_pnl_today
        livro = LIVRO_MAP[pm]
        pnl_mtd_row = df_pnl_today[df_pnl_today["LIVRO"] == livro]
        pnl_mtd = float(pnl_mtd_row["mes_bps"].iloc[0]) if not pnl_mtd_row.empty else 0.0
        pnl_ytd = float(pnl_mtd_row["ytd_bps"].iloc[0]) if not pnl_mtd_row.empty else 0.0

        # Historical budget distribution
        bmin = hist["budget_abs"].min()
        bmax = hist["budget_abs"].max()
        bpct = (budget - bmin) / (bmax - bmin) * 100 if bmax != bmin else 50

        # Status
        CI_SOFT = 150.0
        has_pos = True if pm_has_position is None else bool(pm_has_position.get(pm, True))
        if budget == 0:
            status_label = "⚡ GANCHO"
            status_color = "#fb923c"
        elif pm == "CI":
            if pnl_mtd <= -budget:
                status_label, status_color = "🔴 STOP", "#f87171"
            elif pnl_mtd <= -CI_SOFT:
                status_label, status_color = "🟡 SOFT", "#facc15"
            else:
                status_label, status_color = "🟢 OK", "#4ade80"
        else:
            consumed = abs(pnl_mtd) / budget if budget > 0 and pnl_mtd < 0 else 0
            if consumed >= 1.0:
                status_label, status_color = "🔴 STOP", "#f87171"
            elif consumed >= 0.7:
                status_label, status_color = "🟡 ATENÇÃO", "#facc15"
            else:
                status_label, status_color = "🟢 OK", "#4ade80"
        # Downgrade STOP → FLAT when PM has zero live exposure (already closed positions).
        # Applies to non-CI PMs (CI is a committee with hard cap; semantics differ).
        if pm != "CI" and not has_pos and status_label == "🔴 STOP":
            status_label, status_color = "⚪ FLAT", "#94a3b8"

        # Absolute-margem secondary gate. The percent-consumption rule above
        # can show 🟢 OK when budget is small (post-carry penalty) even with
        # tiny absolute room — e.g. budget 10 bps / pnl −1 bps = 10% consumed.
        # A single bad day in a high-vol PM blows through that. Escalate
        # status by absolute remaining margem in bps.
        _margem_now = budget + pnl_mtd
        if pm != "CI" and budget > 0 and status_label not in ("🔴 STOP", "⚪ FLAT"):
            if _margem_now <= 0:
                status_label, status_color = "🔴 STOP", "#f87171"
            elif _margem_now < 10:
                status_label, status_color = "🟠 NEAR-BREACH", "#fb923c"
            elif _margem_now < 25 and status_label == "🟢 OK":
                status_label, status_color = "🟡 ATENÇÃO", "#facc15"

        bar = stop_bar_svg(budget, pnl_mtd, bmax, soft_mark=soft_mark if pd.notna(soft_mark) else None)
        spark = make_sparkline(hist.set_index("mes")["budget_abs"], "#60a5fa", width=140)

        ytd_color = "#4ade80" if pnl_ytd >= 0 else "#f87171"

        # Semestral / anual: compact inline flags (not full bars)
        sem_used  = abs(pnl_ytd) if pnl_ytd < 0 else 0
        ano_used  = abs(pnl_ytd) if pnl_ytd < 0 else 0
        sem_pct   = sem_used / STOP_SEM * 100
        ano_pct   = ano_used / STOP_ANO * 100
        def cap_chip(label, pct, limit):
            color = "#f87171" if pct >= 100 else "#facc15" if pct >= 70 else "#334155"
            return f'<span style="color:{color};font-size:9px">{label} {pct:.0f}%</span>'
        sem_chip = cap_chip("SEM", sem_pct, STOP_SEM) if pm != "CI" and sem_pct > 0 else ""
        ano_chip = cap_chip("ANO", ano_pct, STOP_ANO) if pm != "CI" and ano_pct > 0 else ""

        # Margem number color based on consumption
        if pm == "CI":
            ci_consumed = abs(pnl_mtd) / CI_SOFT if pnl_mtd < 0 else 0
            margem_color = "#f87171" if ci_consumed >= 1.0 else "#facc15" if ci_consumed >= 0.7 else "#60a5fa"
        elif budget == 0:
            margem_color = "#fb923c"  # gancho
        else:
            _c = abs(pnl_mtd) / budget if pnl_mtd < 0 else 0
            margem_color = "#f87171" if _c >= 1.0 else "#facc15" if _c >= 0.7 else "#60a5fa"

        margem_val = budget + pnl_mtd
        margem_str = str(int(round(margem_val))) if margem_val > 0 else ("–" if budget == 0 else str(int(round(margem_val))))

        rows += f"""
        <tr class="metric-row">
          <td class="pm-name">{PM_LABELS[pm]}</td>
          <td class="pm-margem mono" style="color:{margem_color}">{margem_str}</td>
          <td class="bar-cell">{bar}</td>
          <td class="pm-hist mono">
            <span style="color:{ytd_color}">YTD {pnl_ytd:+.0f}bps</span>
            &nbsp;{sem_chip}&nbsp;{ano_chip}
          </td>
          <td class="pm-status" style="color:{status_color}">{status_label}</td>
          <td class="spark-cell"><img src="data:image/png;base64,{spark}" height="34"/></td>
        </tr>"""

    modal_html = _build_stop_history_modal(stop_history, df_pm_book_pnl)
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risk Budget Monitor</span>
        <span class="card-sub">— MACRO · stop por PM</span>
        <button class="pa-tgl" style="margin-left:auto" onclick="openStopHistory()" title="Ver histórico mês a mês">📜 Histórico</button>
      </div>
      <table class="metric-table stop-table">
        <thead>
          <tr class="col-headers">
            <th style="text-align:left">PM</th>
            <th style="text-align:right">Margem (bps)</th>
            <th>Perf. MTD vs Stop</th>
            <th style="text-align:right">Histórico</th>
            <th>Status</th>
            <th>Budget trend</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <div class="bar-legend">
        Barra: <span style="color:var(--accent-2)">azul</span> = budget disponível · <span style="color:var(--up)">verde</span> = ganho MTD · <span style="color:var(--down)">vermelho</span> = consumo · Budget (bps) = base 63 + carrego
      </div>
    </section>
    {modal_html}"""


def build_pm_budget_vs_var_section(pm_book_var: dict[str, float],
                                    pm_hs_var: dict[str, dict],
                                    pm_margem: dict[str, float]) -> str:
    """Card: tabela PM × {Margem atual, VaR book-report, VaR paramétrico HS 21/63/252, pior dia}.
       Thresholds de cor (por linha) relativos à Margem atual de cada PM:
         · val ≥ 2σ (= Margem × 2/1.645)   → vermelho
         · val ≥ Margem (= 1 VaR)          → laranja
         · val ≥ 1σ (= Margem / 1.645)     → amarelo
    """
    pm_order = ["CI", "LF", "JD", "RJ"]
    pm_label = {"CI": "CI (Comitê)", "LF": "Luiz Felipe",
                "JD": "Joca Dib", "RJ": "Rodrigo Jafet"}

    pm_param_var = pm_book_var or {}
    pm_margem    = pm_margem or {}
    pm_hs_var    = pm_hs_var or {}

    def _bps(v, decimals=1):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        return f'<td class="mono" style="text-align:right">{_fmt_br_num(f"{v:,.{decimals}f}")}</td>'

    def _var_cell(v, ref):
        """Cell colored by vol-multiples of the PM's current Margem (ref).
           ref = Margem atual do PM (interpreted as 1 VaR = 1.645σ).
        """
        if v is None or (isinstance(v, float) and pd.isna(v)) or ref is None or ref <= 0:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        sigma = ref / 1.645
        two_s = 2 * sigma
        if   v >= two_s: col = "var(--down)"    # > 2σ
        elif v >= ref:   col = "#fb923c"        # > Margem (1 VaR)
        elif v >= sigma: col = "var(--warn)"    # > 1σ
        else:            col = "var(--text)"
        return f'<td class="mono" style="text-align:right;color:{col}">{_fmt_br_num(f"{v:,.1f}")}</td>'

    rows = ""
    for pm in pm_order:
        if pm not in pm_param_var and pm not in pm_hs_var:
            continue
        margem  = pm_margem.get(pm)
        v_param = pm_param_var.get(pm)
        h = pm_hs_var.get(pm, {})
        v_21, v_63, v_252 = h.get("v21"), h.get("v63"), h.get("v252")
        worst  = h.get("worst")
        n_obs  = h.get("n", 0)
        marg_s = _fmt_br_num(f'{margem:,.0f}') if margem is not None else '—'
        marg_html = f'<td class="mono" style="text-align:right;color:var(--accent-2);font-weight:700">{marg_s}</td>'
        rows += (
            f'<tr>'
            f'<td style="padding:5px 6px"><b>{pm}</b> '
            f'<span style="color:var(--muted);font-size:11px">· {pm_label[pm]}</span></td>'
            + marg_html
            + _var_cell(v_param, margem)
            + _var_cell(v_21,    margem)
            + _var_cell(v_63,    margem)
            + _var_cell(v_252,   margem)
            + _var_cell(worst,   margem)
            + f'<td class="mono" style="text-align:right;color:var(--muted);font-size:11px">{n_obs}</td>'
            + '</tr>'
        )

    if not rows:
        return ""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Budget vs VaR por PM — MACRO</span>
        <span class="card-sub">Margem atual × VaR paramétrico (Lote) × VaR hist 1d 95% (σ × 1.645)
        sobre posição atual × 21d/63d/252d · cor: múltiplos de vol vs Margem de cada PM</span>
      </div>

      <table class="summary-table">
        <thead>
          <tr>
            <th style="text-align:left">PM</th>
            <th style="text-align:right">Margem atual</th>
            <th style="text-align:right">VaR paramétrico</th>
            <th style="text-align:right">VaR hist 21d</th>
            <th style="text-align:right">VaR hist 63d</th>
            <th style="text-align:right">VaR hist 252d</th>
            <th style="text-align:right">Worst day pos.</th>
            <th style="text-align:right">obs</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>

      <div class="bar-legend" style="margin-top:10px">
        <b>Margem atual</b>: orçamento remanescente do PM no mês (base + carry − PnL MTD) — mesmo valor do Risk Budget Monitor.
        <b>VaR paramétrico</b>: valor da tabela Lote (<code>LOTE_FUND_STRESS_RPM</code> PARAMETRIC_VAR, TREE
        <code>Main_Macro_Ativos</code>, LEVEL=10) somado signed across os books do PM, magnitude final.
        Diversificado entre books do mesmo PM (hedges internos se cancelam) mas não diversificado entre PMs distintos.
        <b>VaR hist 21/63/252d</b>: <code>1.645 × σ</code> dos retornos da <b>posição atual</b> aplicada a
        21/63/252d de retornos históricos dos fatores (fonte: <code>PORTIFOLIO_DAILY_HISTORICAL_SIMULATION</code>, mesma
        engine da Distribuição 252d).
        <b>Worst day posição</b>: pior dia simulado na janela 252d (posição atual × piores fatores históricos).
        <b>Dias p/ esgotar</b>: Margem atual ÷ maior das 4 estimativas de VaR.
        <b>Cores por linha</b>: referência = Margem atual do PM (1 VaR = 1.645σ). <br>
        <span style="color:var(--text)">&lt; 1σ</span> ·
        <span style="color:var(--warn)">≥ 1σ</span> ·
        <span style="color:#fb923c">≥ VaR (Margem)</span> ·
        <span style="color:var(--down)">≥ 2σ</span>.
      </div>
    </section>"""


def build_frontier_lo_section(df: pd.DataFrame, date_str: str,
                               df_sectors: pd.DataFrame = None,
                               pa_hier_html: str = "") -> str:
    """Long Only position/attribution card for Frontier Ações FIC FI.
       df_sectors (optional): ticker → sector mapping for the Por Setor view.
       pa_hier_html (optional): pre-built hierarchical PA section (GFA rows from df_pa)
                                 rendered as a sub-tab. If empty, the sub-tab is omitted.
    """
    if df is None or df.empty:
        return ""

    val_date = str(df["VAL_DATE"].iloc[0])[:10]
    stale = val_date != date_str

    total_row = df[df["PRODUCT"] == "TOTAL"]
    if total_row.empty:
        return ""
    tot = total_row.iloc[0]

    gross    = tot["% Cash"]
    delta    = tot["DELTA"]
    nav_brl  = tot["R$"] / gross if gross else 0

    att_d  = tot["TOTAL_ATRIBUTION_DAY"]   * 100
    att_m  = tot["TOTAL_ATRIBUTION_MONTH"] * 100
    att_y  = tot["TOTAL_ATRIBUTION_YEAR"]  * 100
    er_d   = tot["TOTAL_IBOD_Benchmark_DAY"]   * 100
    er_m   = tot["TOTAL_IBOD_Benchmark_MONTH"] * 100
    er_y   = tot["TOTAL_IBOD_Benchmark_YEAR"]  * 100
    ibov_d = tot["TOTAL_IBVSP_DAY"]   * 100
    ibov_m = tot["TOTAL_IBVSP_MONTH"] * 100
    ibov_y = tot["TOTAL_IBVSP_YEAR"]  * 100

    stocks = df[~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"])]
    stocks_valid = stocks.dropna(subset=["BETA", "% Cash"])
    pct_sum = stocks_valid["% Cash"].sum()
    w_beta = (stocks_valid["% Cash"] * stocks_valid["BETA"]).sum() / pct_sum if pct_sum else None

    def pct(v, decimals=2):
        try:
            f = float(v)
            return f"{'+'if f>0 else ''}{f*100:.{decimals}f}%"
        except Exception:
            return "—"

    def color(v):
        try:
            return "var(--up)" if float(v) >= 0 else "var(--down)"
        except Exception:
            return "inherit"

    def fmt_brl(v):
        try:
            return f"R$ {_fmt_br_num(f'{float(v)/1e6:,.1f}')}M"
        except Exception:
            return "—"

    stale_badge = ' <span style="color:var(--warn);font-size:10px">D-1</span>' if stale else ""
    nav_fmt = (_fmt_br_num(f"{nav_brl/1e6:,.1f}") + "M") if nav_brl else "—"
    beta_fmt = f"{w_beta:.2f}" if w_beta is not None else "—"

    # ── Header metrics ─────────────────────────────────────────────────────────
    def metric_chip(label, val, c=None):
        col = c or color(val)
        return (f'<span class="sn-stat"><span class="sn-lbl">{label}</span>'
                f'<span class="sn-val mono" style="color:{col}">{pct(val)}</span></span>')

    header_metrics = f"""
      <div class="sn-inline-stats mono" style="margin-bottom:10px;flex-wrap:wrap;gap:6px 16px">
        <span class="sn-stat"><span class="sn-lbl">NAV</span><span class="sn-val mono">R$ {nav_fmt}</span></span>
        <span class="sn-stat"><span class="sn-lbl">Gross</span><span class="sn-val mono">{pct(gross)}</span></span>
        <span class="sn-stat"><span class="sn-lbl">Beta pond.</span><span class="sn-val mono">{beta_fmt}</span></span>
        <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
        {metric_chip("Attrib D", att_d/100)}
        {metric_chip("Attrib MTD", att_m/100)}
        {metric_chip("Attrib YTD", att_y/100)}
        <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
        {metric_chip("ER IBOD D", er_d/100)}
        {metric_chip("ER IBOD MTD", er_m/100)}
        {metric_chip("ER IBOD YTD", er_y/100)}
        <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
        {metric_chip("ER IBOV D", ibov_d/100)}
        {metric_chip("ER IBOV MTD", ibov_m/100)}
        {metric_chip("ER IBOV YTD", ibov_y/100)}
      </div>"""

    # ── Position table ─────────────────────────────────────────────────────────
    # ER columns (D/MTD/YTD) are bench-toggleable via the IBOV/IBOD buttons.
    # Each ER <td> carries data-ibov and data-ibod and the header text is rewritten
    # by fpaBmk() (inline JS below). Default bench = IBOV.
    col_headers = """
      <tr class="col-headers">
        <th style="text-align:left;min-width:80px">Ticker</th>
        <th style="text-align:right">% Cash</th>
        <th style="text-align:right">Delta</th>
        <th style="text-align:right">Beta</th>
        <th style="text-align:right">#ADTV</th>
        <th style="text-align:right">Ret D</th>
        <th style="text-align:right">Ret MTD</th>
        <th style="text-align:right">Ret YTD</th>
        <th style="text-align:right">Attrib D</th>
        <th style="text-align:right">Attrib MTD</th>
        <th style="text-align:right">Attrib YTD</th>
        <th style="text-align:right" class="fpa-th-er-d">ER IBOV D</th>
        <th style="text-align:right" class="fpa-th-er-m">ER IBOV MTD</th>
        <th style="text-align:right" class="fpa-th-er-y">ER IBOV YTD</th>
      </tr>"""

    def _num(x):
        try:
            return float(x)
        except Exception:
            return None

    def make_row(row, is_subtotal=False, is_total=False):
        ticker = row["PRODUCT"]
        book   = row.get("BOOK", "")
        label  = f"{ticker}" + (f" <span style='color:var(--muted);font-size:9px'>({book})</span>" if is_total else "")
        bg = "background:rgba(30,144,255,0.08)" if is_total else ("background:rgba(255,255,255,0.04)" if is_subtotal else "")
        fw = "font-weight:700" if (is_subtotal or is_total) else ""

        def cell(v, is_pct=True, decimals=2):
            try:
                f = float(v)
                txt = f"{'+'if f>0 else ''}{f*100:.{decimals}f}%" if is_pct else f"{f:.{decimals}f}"
                col = color(f) if is_pct else "inherit"
                return f'<td class="mono" style="text-align:right;color:{col}">{txt}</td>'
            except Exception:
                return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'

        def er_cell(v_ibov, v_ibod, v_cdi):
            # Default shows IBOV; data-ibod / data-cdi carry alternates.
            def _attr(v):
                f = _num(v)
                return "" if f is None else f"{f:.10f}"
            v = _num(v_ibov)
            if v is None:
                txt, col = "—", "var(--muted)"
            else:
                sign = "+" if v > 0 else ""
                txt  = f"{sign}{v*100:.2f}%"
                col  = "var(--up)" if v >= 0 else "var(--down)"
            return (f'<td class="mono fpa-er-cell" style="text-align:right;color:{col}" '
                    f'data-ibov="{_attr(v_ibov)}" data-ibod="{_attr(v_ibod)}" '
                    f'data-cdi="{_attr(v_cdi)}">{txt}</td>')

        adtv_cell = cell(row.get("#ADTV"), is_pct=False, decimals=2) if not (is_subtotal or is_total) else '<td></td>'
        beta_cell = cell(row.get("BETA"), is_pct=False, decimals=2) if not (is_subtotal or is_total) else '<td></td>'
        ret_d     = cell(row.get("RETURN_DAY"))    if not (is_subtotal or is_total) else '<td></td>'
        ret_m     = cell(row.get("RETURN_MONTH"))  if not (is_subtotal or is_total) else '<td></td>'
        ret_y     = cell(row.get("RETURN_YEAR"))   if not (is_subtotal or is_total) else '<td></td>'

        er_d = er_cell(row.get("TOTAL_IBVSP_DAY"),
                       row.get("TOTAL_IBOD_Benchmark_DAY"),
                       row.get("TOTAL_CDI_DAY"))
        er_m = er_cell(row.get("TOTAL_IBVSP_MONTH"),
                       row.get("TOTAL_IBOD_Benchmark_MONTH"),
                       row.get("TOTAL_CDI_MONTH"))
        er_y = er_cell(row.get("TOTAL_IBVSP_YEAR"),
                       row.get("TOTAL_IBOD_Benchmark_YEAR"),
                       row.get("TOTAL_CDI_YEAR"))

        return f"""<tr style="{bg};{fw}">
          <td style="padding-left:{'4px' if is_subtotal or is_total else '12px'};white-space:nowrap">{label}</td>
          {cell(row.get("% Cash"))}
          {cell(row.get("DELTA"))}
          {beta_cell}
          {adtv_cell}
          {ret_d}{ret_m}{ret_y}
          {cell(row.get("TOTAL_ATRIBUTION_DAY"))}
          {cell(row.get("TOTAL_ATRIBUTION_MONTH"))}
          {cell(row.get("TOTAL_ATRIBUTION_YEAR"))}
          {er_d}{er_m}{er_y}
        </tr>"""

    # ── By Name view (grouped by BOOK — default) ──────────────────────────────
    tbody = ""
    books = [b for b in df["BOOK"].unique() if b and b not in ("", None)]
    for book in books:
        book_rows = df[(df["BOOK"] == book) & (~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"]))]
        book_rows = book_rows.sort_values("% Cash", ascending=False, na_position="last")
        tbody += f'<tr><td colspan="14" style="padding:6px 4px 2px;font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;border-top:1px solid var(--border)">{book}</td></tr>'
        for _, r in book_rows.iterrows():
            tbody += make_row(r)
        sub = df[(df["PRODUCT"] == "SUBTOTAL") & (df["BOOK"] == book)]
        if not sub.empty:
            tbody += make_row(sub.iloc[0], is_subtotal=True)
    # Alienadas — aggregated P&L from stocks sold during the period (not in current portfolio)
    alien_row = df[df["PRODUCT"] == "AÇÕES ALIENADAS"]
    if not alien_row.empty:
        tbody += make_row(alien_row.iloc[0], is_subtotal=True)
    tbody += make_row(tot, is_total=True)

    by_name_html = f"""
      <div id="fpa-view-name" class="fpa-view">
        <div style="overflow-x:auto">
          <table class="metric-table" style="font-size:11px;min-width:900px">
            <thead>{col_headers}</thead>
            <tbody>{tbody}</tbody>
          </table>
        </div>
      </div>"""

    # ── By Sector view — sector headers with ▼/▶ toggle + aggregated ER ───────
    sector_html = ""
    if df_sectors is not None and not df_sectors.empty:
        sec_map = df_sectors.drop_duplicates("TICKER").set_index("TICKER")
        _stocks = df[(df["BOOK"].astype(str).str.strip() != "")
                     & (~df["PRODUCT"].isin(["TOTAL", "SUBTOTAL", "AÇÕES ALIENADAS"]))
                     & (df["% Cash"].notna())].copy()
        _stocks["sector"] = _stocks["PRODUCT"].map(sec_map["GLPG_SECTOR"]).fillna("Outros")

        # Numeric coerce for aggregation
        _agg_cols = ["% Cash", "DELTA",
                     "TOTAL_ATRIBUTION_DAY","TOTAL_ATRIBUTION_MONTH","TOTAL_ATRIBUTION_YEAR",
                     "TOTAL_IBVSP_DAY","TOTAL_IBVSP_MONTH","TOTAL_IBVSP_YEAR",
                     "TOTAL_IBOD_Benchmark_DAY","TOTAL_IBOD_Benchmark_MONTH","TOTAL_IBOD_Benchmark_YEAR",
                     "TOTAL_CDI_DAY","TOTAL_CDI_MONTH","TOTAL_CDI_YEAR"]
        for c in _agg_cols:
            if c in _stocks.columns:
                _stocks[c] = pd.to_numeric(_stocks[c], errors="coerce")

        # Order sectors by gross % Cash desc
        sec_order = (_stocks.groupby("sector")["% Cash"].sum()
                     .sort_values(ascending=False).index.tolist())

        sec_tbody = ""
        sec_idx = 0
        for sec in sec_order:
            grp = _stocks[_stocks["sector"] == sec].sort_values("% Cash", ascending=False)
            sec_id = f"fpa-sec-{sec_idx}"; sec_idx += 1

            def _s(col): return float(grp[col].sum()) if col in grp.columns else 0.0
            def _attr(v): return "" if v is None else f"{v:.10f}"

            # Sector header cells — mirror the column layout: 11 "left" cols + 3 ER cells
            pos_s   = _s("% Cash")
            delta_s = _s("DELTA")
            at_d = _s("TOTAL_ATRIBUTION_DAY");  at_m = _s("TOTAL_ATRIBUTION_MONTH");  at_y = _s("TOTAL_ATRIBUTION_YEAR")
            iv_d = _s("TOTAL_IBVSP_DAY");        iv_m = _s("TOTAL_IBVSP_MONTH");        iv_y = _s("TOTAL_IBVSP_YEAR")
            id_d = _s("TOTAL_IBOD_Benchmark_DAY"); id_m = _s("TOTAL_IBOD_Benchmark_MONTH"); id_y = _s("TOTAL_IBOD_Benchmark_YEAR")
            cd_d = _s("TOTAL_CDI_DAY");          cd_m = _s("TOTAL_CDI_MONTH");          cd_y = _s("TOTAL_CDI_YEAR")
            _b = grp.dropna(subset=["BETA"])
            wbeta = ((_b["% Cash"] * _b["BETA"]).sum() / _b["% Cash"].sum()
                     if not _b.empty and _b["% Cash"].sum() > 0 else None)

            def _pct(v):
                f = float(v); s = "+" if f > 0 else ""
                return f"{s}{f*100:.2f}%"
            def _col(v):
                return "var(--up)" if float(v) >= 0 else "var(--down)"

            def _sec_er_td(vi, vo, vc):
                sign = "+" if vi >= 0 else ""
                return (f'<td class="mono fpa-er-cell" style="text-align:right;color:{_col(vi)}" '
                        f'data-ibov="{_attr(vi)}" data-ibod="{_attr(vo)}" data-cdi="{_attr(vc)}">'
                        f'{sign}{vi*100:.2f}%</td>')

            beta_cell_sec = (f'<td class="mono" style="text-align:right">{wbeta:.2f}</td>'
                             if wbeta is not None else '<td></td>')
            sec_tbody += (
                f'<tr class="fpa-sector-row" data-sec-id="{sec_id}" '
                f'style="background:rgba(59,130,246,0.08);font-weight:700;cursor:pointer" '
                f'onclick="fpaToggleSector(this)">'
                f'<td style="padding:5px 8px;white-space:nowrap">'
                f'<span class="fpa-sector-arrow" style="font-size:9px;margin-right:6px">▼</span>{sec}'
                f' <span style="color:var(--muted);font-weight:400;font-size:9px">({len(grp)})</span></td>'
                f'<td class="mono" style="text-align:right">{_pct(pos_s)}</td>'
                f'<td class="mono" style="text-align:right">{_pct(delta_s)}</td>'
                f'{beta_cell_sec}'
                f'<td></td>'  # #ADTV
                f'<td></td><td></td><td></td>'  # Ret D/M/Y
                f'<td class="mono" style="text-align:right;color:{_col(at_d)}">{_pct(at_d)}</td>'
                f'<td class="mono" style="text-align:right;color:{_col(at_m)}">{_pct(at_m)}</td>'
                f'<td class="mono" style="text-align:right;color:{_col(at_y)}">{_pct(at_y)}</td>'
                f'{_sec_er_td(iv_d, id_d, cd_d)}'
                f'{_sec_er_td(iv_m, id_m, cd_m)}'
                f'{_sec_er_td(iv_y, id_y, cd_y)}'
                f'</tr>'
            )
            # Child rows under this sector
            for _, r in grp.iterrows():
                child_tr = make_row(r)  # starts with <tr ...>
                # Inject marker classes/attrs to identify as sector child
                child_tr = child_tr.replace(
                    '<tr style=',
                    f'<tr class="fpa-sector-child" data-sec-parent="{sec_id}" style=',
                    1)
                sec_tbody += child_tr

        # Alienadas — shown as a summary row above the grand total
        _alien = df[df["PRODUCT"] == "AÇÕES ALIENADAS"]
        if not _alien.empty:
            sec_tbody += make_row(_alien.iloc[0], is_subtotal=True)
        # Grand total row at the bottom
        sec_tbody += make_row(tot, is_total=True)

        sector_html = f"""
      <div id="fpa-view-sector" class="fpa-view" style="display:none">
        <div style="overflow-x:auto">
          <table class="metric-table" data-no-sort="1" style="font-size:11px;min-width:900px">
            <thead>{col_headers}</thead>
            <tbody>{sec_tbody}</tbody>
          </table>
        </div>
      </div>"""

    table_html = by_name_html + sector_html

    # ── Highlights strip: top-3 ↑ / top-3 ↓ — one div per bench, toggled by JS ──
    def _highlights_div(col_name: str, bench_label: str, visible: bool) -> str:
        try:
            _hs = df[(df["BOOK"].astype(str).str.strip() != "")
                     & (~df["PRODUCT"].isin(["TOTAL","SUBTOTAL","AÇÕES ALIENADAS"]))]
            _by = _hs.set_index("PRODUCT")[col_name].astype(float).fillna(0)
            _by = _by[_by.abs() > 0]
            # Fallback: if bench-relative column is all zero (data not populated
            # upstream for this bench), use absolute TOTAL_ATRIBUTION_DAY so the
            # banner still surfaces day's biggest movers.
            fallback_used = False
            if _by.empty and "TOTAL_ATRIBUTION_DAY" in _hs.columns:
                _by = _hs.set_index("PRODUCT")["TOTAL_ATRIBUTION_DAY"].astype(float).fillna(0)
                _by = _by[_by.abs() > 0]
                fallback_used = True
            _up = _by[_by > 0].sort_values(ascending=False).head(3)
            _dn = _by[_by < 0].sort_values().head(3)
            def _chip(t, v, up):
                col = "var(--up)" if up else "var(--down)"
                sign = "+" if up else ""
                return (f'<span class="mono" style="color:{col};font-weight:700;'
                        f'padding:2px 8px;border:1px solid {col};border-radius:4px;'
                        f'background:rgba(0,0,0,.15)">{t} {sign}{v*100:.2f}%</span>')
            if _up.empty and _dn.empty:
                return ""
            ups = " ".join(_chip(t, v, True)  for t, v in _up.items()) or '<span style="color:var(--muted)">—</span>'
            dns = " ".join(_chip(t, v, False) for t, v in _dn.items()) or '<span style="color:var(--muted)">—</span>'
            disp = "" if visible else "display:none"
            label_txt = (
                f"Highlights · contribuição absoluta hoje "
                f"<span style='color:var(--muted);font-size:9px;font-style:italic'>"
                f"(α vs {bench_label} sem dado upstream)</span>"
                if fallback_used
                else f"Highlights · α vs {bench_label} hoje"
            )
            return (
                f'<div class="fpa-highlights" data-bench="{bench_label.lower()}" '
                f'style="align-items:center;gap:12px;flex-wrap:wrap;padding:8px 12px;'
                f'margin:8px 0 12px;border:1px solid var(--line);border-radius:6px;'
                f'background:rgba(0,0,0,.15);display:{"flex" if visible else "none"}">'
                f'<span style="font-size:10px;color:var(--muted);letter-spacing:.12em;'
                f'text-transform:uppercase">{label_txt}</span>'
                f'<span style="color:var(--up);font-weight:700">↑</span> {ups}'
                '<span style="color:var(--line)">·</span>'
                f'<span style="color:var(--down);font-weight:700">↓</span> {dns}'
                '</div>'
            )
        except Exception:
            return ""

    highlights_html = (
        _highlights_div("TOTAL_IBVSP_DAY",          "IBOV", True)
      + _highlights_div("TOTAL_IBOD_Benchmark_DAY", "IBOD", False)
      + _highlights_div("TOTAL_CDI_DAY",            "CDI",  False)
    )

    # ── Toggle bar (bench IBOV/IBOD/CDI · view Por Nome/Por Setor) + JS ────────
    has_sector = (df_sectors is not None and not df_sectors.empty)
    view_buttons = ("" if not has_sector else """
      <div style="display:flex;gap:4px;margin-left:auto;align-items:center">
        <button class="toggle-btn active fpa-view-btn" data-view="name"
                onclick="fpaView('name')">Por Nome</button>
        <button class="toggle-btn fpa-view-btn" data-view="sector"
                onclick="fpaView('sector')">Por Setor</button>
        <span id="fpa-expand-btns" style="display:none;gap:4px;margin-left:4px">
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="fpaExpandAll()">▼ All</button>
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="fpaCollapseAll()">▶ All</button>
        </span>
      </div>""")
    toggle_bar = f"""
    <div style="display:flex;align-items:center;gap:8px;margin:0 0 10px;flex-wrap:wrap">
      <div style="display:flex;gap:4px">
        <button class="toggle-btn active fpa-btn" data-bench="ibov"
                onclick="fpaBmk('ibov')">IBOV</button>
        <button class="toggle-btn fpa-btn" data-bench="ibod"
                onclick="fpaBmk('ibod')">IBOD</button>
        <button class="toggle-btn fpa-btn" data-bench="cdi"
                onclick="fpaBmk('cdi')">CDI</button>
      </div>
      {view_buttons}
    </div>"""

    toggle_js = """
    <script>
    (function() {
      if (window.fpaBmk) return;  // only define once
      window.fpaBmk = function(bmk) {
        document.querySelectorAll('.fpa-btn').forEach(function(b) {
          b.classList.toggle('active', b.dataset.bench === bmk);
        });
        document.querySelectorAll('.fpa-highlights').forEach(function(d) {
          d.style.display = (d.dataset.bench === bmk) ? 'flex' : 'none';
        });
        var lbl = bmk.toUpperCase();
        document.querySelectorAll('.fpa-th-er-d').forEach(function(th) { th.textContent = 'ER '+lbl+' D'; });
        document.querySelectorAll('.fpa-th-er-m').forEach(function(th) { th.textContent = 'ER '+lbl+' MTD'; });
        document.querySelectorAll('.fpa-th-er-y').forEach(function(th) { th.textContent = 'ER '+lbl+' YTD'; });
        document.querySelectorAll('.fpa-er-cell').forEach(function(td) {
          var raw = td.dataset[bmk];
          if (raw === '' || raw === undefined) { td.textContent = '—'; td.style.color = 'var(--muted)'; return; }
          var v = parseFloat(raw);
          if (isNaN(v))                        { td.textContent = '—'; td.style.color = 'var(--muted)'; return; }
          td.textContent = (v >= 0 ? '+' : '') + (v*100).toFixed(2) + '%';
          td.style.color = v >= 0 ? 'var(--up)' : 'var(--down)';
        });
      };
      window.fpaView = function(view) {
        ['name','sector'].forEach(function(v) {
          var el = document.getElementById('fpa-view-'+v);
          if (el) el.style.display = (v === view) ? '' : 'none';
        });
        document.querySelectorAll('.fpa-view-btn').forEach(function(b) {
          b.classList.toggle('active', b.dataset.view === view);
        });
        var exp = document.getElementById('fpa-expand-btns');
        if (exp) exp.style.display = (view === 'sector') ? 'inline-flex' : 'none';
      };
      window.fpaToggleSector = function(tr) {
        var arrow = tr.querySelector('.fpa-sector-arrow');
        var open = arrow ? arrow.textContent.trim() === '▼' : true;
        if (arrow) arrow.textContent = open ? '▶' : '▼';
        var sid = tr.getAttribute('data-sec-id');
        if (!sid) return;
        document.querySelectorAll('.fpa-sector-child[data-sec-parent="'+sid+'"]').forEach(function(r) {
          r.style.display = open ? 'none' : '';
        });
      };
      window.fpaExpandAll = function() {
        document.querySelectorAll('.fpa-sector-row').forEach(function(tr) {
          var arrow = tr.querySelector('.fpa-sector-arrow');
          if (arrow && arrow.textContent.trim() === '▶') window.fpaToggleSector(tr);
        });
      };
      window.fpaCollapseAll = function() {
        document.querySelectorAll('.fpa-sector-row').forEach(function(tr) {
          var arrow = tr.querySelector('.fpa-sector-arrow');
          if (arrow && arrow.textContent.trim() === '▼') window.fpaToggleSector(tr);
        });
      };
      // Sub-tab switcher (Long Only / PA hierárquica)
      window.fpaTab = function(tab) {
        ['lo','pa'].forEach(function(t) {
          var el = document.getElementById('fpa-tab-'+t);
          if (el) el.style.display = (t === tab) ? '' : 'none';
        });
        document.querySelectorAll('.fpa-tab-btn').forEach(function(b) {
          b.classList.toggle('active', b.dataset.tab === tab);
        });
      };
    })();
    </script>"""

    # ── Sub-tabs (Long Only default · PA hierárquica if df_pa has GFA) ─────────
    if pa_hier_html:
        # Strip outer <section class="pa-card">…</section> so it nests cleanly inside the tab.
        # Simplest: render as-is; inner card becomes visually flush via CSS overrides.
        sub_tab_bar = """
    <div style="display:flex;gap:4px;margin:0 0 12px;border-bottom:1px solid var(--line);padding-bottom:6px">
      <button class="toggle-btn active fpa-tab-btn" data-tab="lo"
              onclick="fpaTab('lo')" style="font-weight:700">Long Only</button>
      <button class="toggle-btn fpa-tab-btn" data-tab="pa"
              onclick="fpaTab('pa')" style="font-weight:700">PA (hierárquica)</button>
    </div>"""
        lo_content = f"""<div id="fpa-tab-lo">{toggle_bar}{highlights_html}{table_html}</div>"""
        pa_content = (
            '<div id="fpa-tab-pa" style="display:none" class="fpa-pa-nested">'
            f'{pa_hier_html}'
            '</div>'
        )
        body = f"{sub_tab_bar}{lo_content}{pa_content}"
    else:
        body = f"{toggle_bar}{highlights_html}{table_html}"

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Performance Attribution</span>
        <span class="card-sub">— Frontier Ações · {val_date}{stale_badge} · NAV R$ {nav_fmt} · Beta {beta_fmt}</span>
      </div>
      {header_metrics}
      {body}
      {toggle_js}
    </section>"""


def build_data_quality_section(manifest: dict, series_map: dict, df_pa, df_pa_daily):
    """
    Returns (full_html, compact_html).
    full_html  — filterable detail table + sanity checks for the Qualidade tab.
    compact_html — compact status alert for the Summary page.
    """
    if manifest is None:
        return "", ""

    DATA_STR_  = manifest["requested_date"]
    d1_str_    = manifest["d1_str"]
    DATA_      = pd.Timestamp(DATA_STR_)
    expo_date  = manifest.get("expo_date", DATA_STR_)

    _NO_VAR_FUNDS: set = set()  # ALBATROZ/MACRO_Q now sourced from LOTE_FUND_STRESS

    # ── Build flat item list: one row per (fund, source) ──────────────────────
    # Each item: {fund_short, source_label, status, date_used, detail, problem}
    # status: "ok" | "stale" | "missing" | "na"
    items = []

    # Use the canonical mapping (includes IDKAIPCAY3 / IDKAIPCAY10).
    _PA_KEY = dict(_FUND_PA_KEY)
    _TD_BY_SHORT = {cfg["short"]: td_ for td_, cfg in ALL_FUNDS.items()}

    def _pa_item(short):
        pa_key = _PA_KEY.get(short)
        if df_pa is None or df_pa.empty or pa_key is None:
            return dict(status="missing", date="—", detail="PA não disponível")
        sub = df_pa[df_pa["FUNDO"] == pa_key]
        if sub.empty:
            return dict(status="missing", date="—", detail=f"FUNDO={pa_key} não encontrado no PA")
        dia = float(sub["dia_bps"].sum())
        mtd = float(sub["mtd_bps"].sum())
        n   = len(sub)
        has_dia = sub["dia_bps"].abs().sum() > 0.5
        status = "ok" if has_dia else "stale"
        return dict(status=status, date=DATA_STR_ if has_dia else d1_str_,
                    detail=f"{n} instrumentos · DIA {dia:+.1f} bps · MTD {mtd:+.1f} bps")

    def _var_item(short):
        td = _TD_BY_SHORT.get(short)
        if td is None or td not in series_map:
            src = "LOTE_FUND_STRESS" if short in _NO_VAR_FUNDS else "LOTE_FUND_STRESS_RPM"
            return dict(status="missing", date="—", detail=f"Sem dados em {src}")
        s = series_map[td]
        s_avail = s[s["VAL_DATE"] <= DATA_]
        if s_avail.empty:
            return dict(status="missing", date="—", detail="Nenhum registro até a data")
        row = s_avail.iloc[-1]
        last_date = str(row["VAL_DATE"])[:10]
        stale = pd.Timestamp(last_date) < DATA_
        cfg = ALL_FUNDS[td]
        var_val = abs(float(row["var_pct"]))
        soft, hard = cfg.get("var_soft", 999), cfg.get("var_hard", 999)
        stress_val = row.get("stress_pct", None)
        stress_txt = f" · Stress {float(stress_val):.1f}%" if stress_val is not None and not pd.isna(stress_val) else ""
        over = " ⚠ ACIMA SOFT" if var_val >= soft else ""
        detail = f"VaR {var_val:.2f}% (soft {soft:.2f}%, hard {hard:.2f}%){stress_txt}{over}"
        return dict(status="stale" if stale else "ok", date=last_date, detail=detail)

    def _expo_item(short):
        if short == "ALBATROZ":
            ok = manifest.get("alb_expo_ok", False)
            rows = manifest.get("alb_expo_rows", 0)
            if not ok:
                return dict(status="missing", date="—", detail="Exposição ALBATROZ não disponível")
            return dict(status="ok", date=DATA_STR_, detail=f"{rows} posições")
        if short == "QUANT":
            ok = manifest.get("quant_expo_ok", False)
            rows = manifest.get("quant_expo_rows", 0)
            if not ok:
                return dict(status="missing", date="—", detail="Exposição QUANT não disponível")
            return dict(status="ok", date=DATA_STR_, detail=f"{rows} posições")
        if short == "EVOLUTION":
            ok = manifest.get("evo_expo_ok", False)
            rows = manifest.get("evo_expo_rows", 0)
            if not ok:
                return dict(status="missing", date="—", detail="Exposição EVOLUTION não disponível")
            return dict(status="ok", date=DATA_STR_, detail=f"{rows} posições")
        ok = manifest.get("expo_ok", False)
        rows = manifest.get("expo_rows", 0)
        stale = expo_date != DATA_STR_
        if not ok:
            return dict(status="missing", date="—", detail="Exposição MACRO não disponível")
        return dict(status="stale" if stale else "ok", date=expo_date,
                    detail=f"{rows} posições" + (" (fallback D-1)" if stale else ""))

    def _sn_item(short):
        ok = manifest.get("quant_sn_ok" if short=="QUANT" else "evo_sn_ok", False)
        rows = manifest.get("quant_sn_rows" if short=="QUANT" else "evo_sn_rows", 0)
        if not ok:
            return dict(status="missing", date="—", detail="Single-Name não disponível")
        return dict(status="ok", date=DATA_STR_, detail=f"{rows} nomes")

    def _dist_item(short):
        today_ok = manifest.get("dist_today_ok", False)
        prev_ok  = manifest.get("dist_prev_ok", False)
        if today_ok:
            return dict(status="ok", date=DATA_STR_, detail="backward + forward disponíveis")
        if prev_ok:
            return dict(status="stale", date=d1_str_, detail="backward D-1 (fallback) · forward indisponível")
        return dict(status="missing", date="—", detail="Distribuição 252d não disponível")

    def _stop_item(short):
        if short == "ALBATROZ":
            ok = manifest.get("alb_expo_ok", False)
            return dict(status="ok" if ok else "missing", date=DATA_STR_ if ok else "—",
                        detail="Stop R$150k/mês (estimado via DV01)" if ok else "Exposure ALBATROZ não disponível")
        ok = manifest.get("stop_ok", False)
        has_pnl = manifest.get("stop_has_pnl", False)
        pms = manifest.get("stop_pms", [])
        pms_pnl = manifest.get("stop_pms_pnl", [])
        if not ok:
            return dict(status="missing", date="—", detail="Stop monitor não disponível")
        n_pms = len(pms)
        n_pnl = len(pms_pnl)
        detail = f"{n_pms} PMs · {n_pnl} com PnL MTD"
        if pms_pnl:
            detail += f" ({', '.join(pms_pnl)})"
        return dict(status="ok" if has_pnl else "stale", date=DATA_STR_,
                    detail=detail + ("" if has_pnl else " · PnL zero/ausente"))

    _SRC_DEFS = [
        ("PA / PnL",          ["MACRO","QUANT","EVOLUTION","MACRO_Q","ALBATROZ","BALTRA","IDKA_3Y","IDKA_10Y"], _pa_item),
        ("VaR / Stress",      ["MACRO","QUANT","EVOLUTION","MACRO_Q","ALBATROZ","BALTRA","IDKA_3Y","IDKA_10Y"], _var_item),
        ("Exposição",         ["MACRO","QUANT","EVOLUTION","ALBATROZ"],           _expo_item),
        ("Single-Name",       ["QUANT","EVOLUTION"],                              _sn_item),
        ("Distribuição 252d", ["MACRO","EVOLUTION"],                              _dist_item),
        ("Stop / Budget",     ["MACRO","ALBATROZ"],                               _stop_item),
    ]

    all_issues   = []
    n_known_gaps = 0
    for src_label, funds, fn in _SRC_DEFS:
        for short in FUND_ORDER:
            if short not in funds:
                continue
            it = fn(short)
            st = it["status"]
            problem = st in ("missing", "stale")  # D-1 is always a warning
            if st == "na":
                n_known_gaps += 1
            elif problem:
                all_issues.append((short, src_label, st == "stale"))
            items.append({
                "fund": short,
                "source": src_label,
                "status": st,
                "date": it["date"],
                "detail": it["detail"],
                "problem": problem,
            })

    # ── Render detail table ───────────────────────────────────────────────────
    _ST_COLOR = {"ok": "#4ade80", "stale": "#facc15", "missing": "#f87171", "na": "#94a3b8"}
    _ST_LABEL = {"ok": "TODAY", "stale": "D-1", "missing": "MISSING", "na": "N/A"}

    detail_rows = ""
    for it in items:
        st   = it["status"]
        col  = _ST_COLOR[st]
        lbl  = _ST_LABEL[st]
        prob = "1" if it["problem"] else "0"
        bg   = 'background:rgba(248,113,113,0.05)' if st == "missing" else (
               'background:rgba(250,204,21,0.04)' if st == "stale" else "")
        detail_rows += (
            f'<tr data-problem="{prob}" style="{bg}">'
            f'<td style="padding:5px 12px;font-weight:600;white-space:nowrap">'
            f'{FUND_LABELS.get(it["fund"], it["fund"])}</td>'
            f'<td style="padding:5px 12px;color:var(--muted)">{it["source"]}</td>'
            f'<td style="padding:5px 10px;text-align:center">'
            f'<span style="color:{col};font-weight:700;font-size:11px">{lbl}</span></td>'
            f'<td style="padding:5px 12px;font-family:var(--mono);font-size:11px;color:var(--muted)">'
            f'{it["date"]}</td>'
            f'<td style="padding:5px 12px;font-size:11.5px">{it["detail"]}</td>'
            f'</tr>'
        )

    # ── Sanity checks table ───────────────────────────────────────────────────
    sanity_rows = ""
    for td, cfg in ALL_FUNDS.items():
        short = cfg["short"]
        s = series_map.get(td)
        if s is None or s.empty: continue
        s_avail = s[s["VAL_DATE"] <= DATA_]
        if s_avail.empty: continue
        var_today = abs(s_avail.iloc[-1]["var_pct"])
        vmin, vmax = s["var_pct"].abs().min(), s["var_pct"].abs().max()
        pct = (var_today - vmin) / (vmax - vmin) * 100 if vmax != vmin else 0
        soft, hard = cfg.get("var_soft", 999), cfg.get("var_hard", 999)
        if var_today >= hard or pct >= 90:
            col, tag = "#f87171", "⚠ ACIMA DO LIMITE"
        elif var_today >= soft or pct >= 70:
            col, tag = "#facc15", "alerta"
        else:
            col, tag = "#4ade80", "normal"
        sanity_rows += (
            f'<tr><td style="padding:5px 12px;white-space:nowrap">{FUND_LABELS[short]}</td>'
            f'<td style="padding:5px 12px">VaR vs. mandato</td>'
            f'<td style="text-align:center;padding:5px 8px">'
            f'<span style="color:{col};font-weight:700;font-size:11px">{tag}</span></td>'
            f'<td style="padding:5px 12px;color:var(--muted);font-size:11.5px">'
            f'VaR {var_today:.2f}% · soft {soft:.2f}% · hard {hard:.2f}% · {pct:.0f}° pct 12M</td></tr>'
        )
    if df_pa is not None and not df_pa.empty and df_pa_daily is not None and not df_pa_daily.empty:
        for short in FUND_ORDER:
            pa_key = _PA_KEY.get(short)
            if pa_key is None: continue
            sub_today = df_pa[df_pa["FUNDO"] == pa_key]
            dia_val = float(sub_today["dia_bps"].sum()) if not sub_today.empty else 0.0
            hist = df_pa_daily[(df_pa_daily["FUNDO"] == pa_key) & (df_pa_daily["DATE"] < DATA_)]
            if hist.empty: continue
            sigma = float(hist.groupby("DATE")["dia_bps"].sum().std())
            if sigma < 0.1: continue
            z = abs(dia_val) / sigma
            col = "#4ade80" if z < 1.5 else "#facc15" if z < 2.5 else "#f87171"
            tag = f"z={z:.1f}"
            sanity_rows += (
                f'<tr><td style="padding:5px 12px;white-space:nowrap">{FUND_LABELS.get(short,short)}</td>'
                f'<td style="padding:5px 12px">DIA PnL vs. σ histórico</td>'
                f'<td style="text-align:center;padding:5px 8px">'
                f'<span style="color:{col};font-weight:700;font-size:11px">{tag}</span></td>'
                f'<td style="padding:5px 12px;color:var(--muted);font-size:11.5px">'
                f'DIA {dia_val:+.1f} bps · σ={sigma:.1f} bps</td></tr>'
            )

    # ≥1% NAV single-product contribution (alpha only — benchmark-tracking livros excluded)
    if df_pa is not None and not df_pa.empty:
        _pa_alpha_sc = _pa_filter_alpha(df_pa)
        _PA_TO_SHORT = {v: k for k, v in _PA_KEY.items()}
        for short in FUND_ORDER:
            pa_key = _PA_KEY.get(short)
            if pa_key is None: continue
            sub = _pa_alpha_sc[
                (_pa_alpha_sc["FUNDO"] == pa_key) &
                (~_pa_alpha_sc["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos", "Prev"})) &
                (~_pa_alpha_sc["CLASSE"].isin({"Caixa", "Custos"}))
            ]
            if sub.empty:
                continue
            hits = sub[sub["dia_bps"].abs() >= 100.0].sort_values(
                "dia_bps", key=lambda s: s.abs(), ascending=False
            )
            if hits.empty:
                continue
            top = hits.iloc[0]
            n = len(hits)
            extra = f" · +{n-1} outros" if n > 1 else ""
            _b = float(top["dia_bps"])
            sanity_rows += (
                f'<tr><td style="padding:5px 12px;white-space:nowrap">{FUND_LABELS.get(short,short)}</td>'
                f'<td style="padding:5px 12px">Contribuição PA ≥ 1% NAV</td>'
                f'<td style="text-align:center;padding:5px 8px">'
                f'<span style="color:#f87171;font-weight:700;font-size:11px">🚨 {_b/100:+.2f}%</span></td>'
                f'<td style="padding:5px 12px;color:var(--muted);font-size:11.5px">'
                f'{top["PRODUCT"]} ({top["LIVRO"]}) {_b:+.0f} bps{extra}</td></tr>'
            )

    # ── Full HTML ─────────────────────────────────────────────────────────────
    full_html = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Qualidade de Dados</span>
        <span class="card-sub">— disponibilidade e sanidade · {DATA_STR_}</span>
      </div>

      <div style="display:flex;align-items:center;gap:16px;margin-bottom:14px;flex-wrap:wrap">
        <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">
          Filtro:
        </div>
        <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
          <input type="radio" name="dq_filter" value="all" checked
                 onchange="dqFilter('all')" style="accent-color:#3b82f6"> Todos
        </label>
        <label style="display:flex;align-items:center;gap:6px;cursor:pointer;font-size:12px">
          <input type="radio" name="dq_filter" value="problems"
                 onchange="dqFilter('problems')" style="accent-color:#f87171"> Só problemas
        </label>
        <div style="margin-left:auto;font-size:10px;color:var(--muted)">
          <span style="color:#4ade80;font-weight:700">TODAY</span> dados de hoje &nbsp;·&nbsp;
          <span style="color:#facc15;font-weight:700">D-1</span> fallback &nbsp;·&nbsp;
          <span style="color:#f87171;font-weight:700">MISSING</span> indisponível &nbsp;·&nbsp;
          <span style="color:#94a3b8;font-weight:700">N/A</span> lacuna conhecida
        </div>
      </div>

      <div style="overflow-x:auto;margin-bottom:24px">
        <table id="dq-detail-table" style="border-collapse:collapse;width:100%;min-width:600px">
          <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Fundo</th>
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Fonte</th>
            <th style="text-align:center;padding:6px 10px;color:var(--muted);font-size:11px;text-transform:uppercase">Status</th>
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Data</th>
            <th style="text-align:left;padding:6px 12px;color:var(--muted);font-size:11px;text-transform:uppercase">Detalhe</th>
          </tr></thead>
          <tbody id="dq-detail-body">{detail_rows}</tbody>
        </table>
      </div>

      {'<div style="border-top:1px solid var(--border);padding-top:16px"><div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">Verificações de Sanidade</div><table style="border-collapse:collapse;width:100%"><thead><tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:5px 12px;color:var(--muted);font-size:11px">Fundo</th><th style="text-align:left;padding:5px 12px;color:var(--muted);font-size:11px">Verificação</th><th style="text-align:center;padding:5px 8px;color:var(--muted);font-size:11px">Status</th><th style="text-align:left;padding:5px 12px;color:var(--muted);font-size:11px">Detalhe</th></tr></thead><tbody>' + sanity_rows + '</tbody></table></div>' if sanity_rows else ''}
    </section>
    <script>
    window.dqFilter = function(val) {{
      var rows = document.querySelectorAll('#dq-detail-body tr');
      rows.forEach(function(r) {{
        if (val === 'all') r.style.display = '';
        else r.style.display = (r.dataset.problem === '1') ? '' : 'none';
      }});
    }};
    </script>"""

    # ── Compact alert for Summary page ────────────────────────────────────────
    n_missing = sum(1 for _, _, stale in all_issues if not stale)
    n_stale   = sum(1 for _, _, stale in all_issues if stale)
    gaps_note = (f' · <span style="color:#94a3b8">{n_known_gaps} lacuna(s) conhecida(s)</span>'
                 if n_known_gaps > 0 else "")
    if n_missing == 0 and n_stale == 0:
        status_dot = '<span style="color:#4ade80;font-size:16px">●</span>'
        status_txt = f'<span style="color:#4ade80">Todos os dados disponíveis para hoje</span>{gaps_note}'
    elif n_missing == 0:
        status_dot = '<span style="color:#facc15;font-size:16px">●</span>'
        status_txt = f'<span style="color:#facc15">{n_stale} fonte(s) usando dados de D-1</span>{gaps_note}'
    else:
        status_dot = '<span style="color:#f87171;font-size:16px">●</span>'
        status_txt = f'<span style="color:#f87171">{n_missing} fonte(s) indisponível · {n_stale} usando D-1</span>{gaps_note}'

    issue_lines = ""
    for short, source, stale in all_issues:
        c = "#facc15" if stale else "#f87171"
        t = "D-1" if stale else "MISSING"
        issue_lines += (f'<div style="display:flex;gap:8px;align-items:baseline;font-size:11px">'
                        f'<span style="color:{c};min-width:60px;font-weight:700">{t}</span>'
                        f'<span style="color:var(--muted)">{FUND_LABELS.get(short,short)} · {source}</span>'
                        f'</div>')

    compact_html = f"""
    <section class="card" style="margin-top:12px">
      <div class="card-head">
        <span class="card-title">Status dos Dados</span>
        <span class="card-sub">— {DATA_STR_}</span>
      </div>
      <div style="display:flex;gap:12px;align-items:center;margin-bottom:{'8px' if issue_lines else '0'}">
        {status_dot} {status_txt}
        <span style="color:var(--muted);font-size:11px;margin-left:auto">
          ver aba <em>Qualidade</em> para detalhe completo
        </span>
      </div>
      {('<div style="display:grid;grid-template-columns:1fr 1fr;gap:2px 16px;margin-top:4px">' + issue_lines + '</div>') if issue_lines else ''}
    </section>"""

    return full_html, compact_html


def _build_fund_mini_briefing(
    short: str, house_row: dict, series_map: dict, td_by_short: dict,
    df_pa, factor_matrix: dict, bench_matrix: dict,
    pm_margem=None, frontier_bvar=None, df_frontier=None,
) -> str:
    """Compact 1–2 min per-fund briefing card. Shows at top of each fund's
       reports as the "Briefing" tab.
    """
    if not house_row:
        return ""

    def _mm(v):
        try: return _fmt_br_num(f"{v/1e6:,.1f}M")
        except Exception: return "—"

    td = td_by_short.get(short)
    cfg = ALL_FUNDS.get(td, {}) if td else {}

    # Δ VaR D-1 (bps)
    dvar_bps = None
    s = series_map.get(td) if td else None
    if s is not None and not s.empty:
        s_avail = s[s["VAL_DATE"] <= DATA]
        if len(s_avail) >= 2:
            v_today = abs(float(s_avail.iloc[-1]["var_pct"]))
            v_prev  = abs(float(s_avail.iloc[-2]["var_pct"]))
            dvar_bps = (v_today - v_prev) * 100

    # Util (primary metric)
    util = None
    if not cfg.get("informative"):
        soft = cfg.get("var_soft", 0)
        v_pct = house_row["bvar_pct"] if cfg.get("primary") == "bvar" else house_row["var_pct"]
        util = v_pct / soft * 100 if soft else None

    # Top contributors / detractors today (for funds with PA)
    pa_key = _FUND_PA_KEY.get(short)
    top_contrib = []
    top_detract = []
    big_hits = []  # PA contributions ≥ 1% (100 bps) of NAV — flagged as alerts
    if pa_key and df_pa is not None and not df_pa.empty:
        sub = df_pa[
            (df_pa["FUNDO"] == pa_key) &
            (~df_pa["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos"})) &
            (~df_pa["CLASSE"].isin({"Caixa", "Custos"}))
        ].copy()
        if not sub.empty:
            sub_sorted = sub.sort_values("dia_bps", ascending=False)
            top_contrib = sub_sorted[sub_sorted["dia_bps"] > 0.5].head(2)
            top_detract = sub_sorted[sub_sorted["dia_bps"] < -0.5].tail(2)
            # ≥ 1% alert must exclude benchmark-tracking livros (IDKA CI tracks
            # the index; a 200 bp move there is tracking, not alpha).
            sub_alpha = _pa_filter_alpha(sub)
            _big = sub_alpha[sub_alpha["dia_bps"].abs() >= 100.0].sort_values(
                "dia_bps", key=lambda s: s.abs(), ascending=False
            )
            big_hits = list(_big.itertuples(index=False))

    # Fund-level factor dominance (net of bench) — only for funds in factor_matrix
    dominant_factor = None
    if factor_matrix and bench_matrix:
        nets = {}
        for fk, allocs in factor_matrix.items():
            v = allocs.get(short, 0.0) - bench_matrix.get(fk, {}).get(short, 0.0)
            if abs(v) >= 1_000:
                nets[fk] = v
        if nets:
            dominant_factor = max(nets.items(), key=lambda x: abs(x[1]))

    # Stop margem (MACRO only)
    stop_min = None
    if short == "MACRO" and pm_margem:
        try:
            stop_min = min((pm, m) for pm, m in pm_margem.items() if m is not None)
        except ValueError:
            stop_min = None

    # ── Risk budget alert: VaR/BVaR > 1.5× orçamento MTD disponível ─────────
    # Remaining MTD budget (in bps of NAV). Only funds with explicit monthly
    # loss budget: MACRO (sum of PM margens) and ALBATROZ (150 bps − consumed).
    budget_remaining_bps = None
    if short == "MACRO" and pm_margem:
        mlist = [max(0.0, m) for m in pm_margem.values() if m is not None]
        if mlist:
            budget_remaining_bps = float(sum(mlist))
    elif short == "ALBATROZ" and df_pa is not None and not df_pa.empty:
        alb = df_pa[df_pa["FUNDO"] == "ALBATROZ"]
        if not alb.empty:
            mtd = float(alb["mtd_bps"].sum())
            budget_remaining_bps = max(0.0, ALBATROZ_STOP_BPS + min(0.0, mtd))

    budget_alert = None  # (remaining_bps, max_var_bps, var_bps, bvar_bps)
    if budget_remaining_bps is not None and budget_remaining_bps > 0:
        var_bps_abs  = abs(house_row.get("var_pct")  or 0.0) * 100
        bvar_bps_abs = abs(house_row.get("bvar_pct") or 0.0) * 100
        max_var_bps  = max(var_bps_abs, bvar_bps_abs)
        if max_var_bps > 1.5 * budget_remaining_bps:
            budget_alert = (budget_remaining_bps, max_var_bps, var_bps_abs, bvar_bps_abs)

    # ── Stat chips ────────────────────────────────────────────────────────────
    is_bvar = cfg.get("primary") == "bvar"
    is_frontier = (short == "FRONTIER")
    var_label = "BVaR 95%" if is_bvar else ("HS BVaR" if is_frontier else "VaR 95%")
    var_val = house_row["bvar_pct"] if (is_bvar or is_frontier) else house_row["var_pct"]

    def _chip(lbl, val, color=None):
        col = f' style="color:{color}"' if color else ''
        return (f'<span class="sn-stat"><span class="sn-lbl">{lbl}</span>'
                f'<span class="sn-val mono"{col}>{val}</span></span>')

    chips = (
        _chip("NAV", f"R$ {_mm(house_row['nav'])}") +
        _chip(var_label, f"{var_val:.2f}%") +
        (_chip("Util", f"{util:.0f}%",
               "var(--down)" if util and util >= UTIL_HARD
               else "var(--warn)" if util and util >= UTIL_WARN else "var(--up)")
         if util is not None else "") +
        (_chip("Δ VaR D-1", f"{dvar_bps:+.0f} bps",
               "var(--down)" if dvar_bps and dvar_bps > 0 else "var(--up)")
         if dvar_bps is not None else "") +
        _chip("DIA",  f"{house_row['var_pct']*0 + house_row.get('var_pct',0) if False else 0}", None)  # placeholder
    )

    # Return chips from PA sums per window
    dia_bps = mtd_bps = ytd_bps = m12_bps = None
    if pa_key and df_pa is not None and not df_pa.empty:
        sub_all = df_pa[df_pa["FUNDO"] == pa_key]
        if not sub_all.empty:
            dia_bps = float(sub_all["dia_bps"].sum())
            mtd_bps = float(sub_all["mtd_bps"].sum())
            ytd_bps = float(sub_all["ytd_bps"].sum())
            m12_bps = float(sub_all["m12_bps"].sum())
    def _ret_chip(lbl, bps):
        if bps is None:
            return ""
        pct = bps / 100
        col = "var(--up)" if pct >= 0 else "var(--down)"
        return _chip(lbl, f"{'+' if pct>=0 else ''}{pct:.2f}%", col)

    chips = (
        _chip("NAV", f"R$ {_mm(house_row['nav'])}") +
        _chip(var_label, f"{var_val:.2f}%") +
        (_chip("Util", f"{util:.0f}%",
               "var(--down)" if util >= UTIL_HARD
               else "var(--warn)" if util >= UTIL_WARN else "var(--up)")
         if util is not None else "") +
        (_chip("Δ D-1",
               f"{dvar_bps:+.0f} bps",
               "var(--down)" if dvar_bps > 0 else "var(--up)" if dvar_bps < 0 else None)
         if dvar_bps is not None else "") +
        '<span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>' +
        _ret_chip("DIA",  dia_bps) +
        _ret_chip("MTD",  mtd_bps) +
        _ret_chip("YTD",  ytd_bps) +
        _ret_chip("12M",  m12_bps)
    )

    # ── Headline ──────────────────────────────────────────────────────────────
    headline_parts = []
    # Rule 0: single PA contribution ≥ 1% (100 bps) of NAV
    if big_hits:
        _h = big_hits[0]
        _b = float(_h.dia_bps)
        _icon = "🟢" if _b > 0 else "🚨"
        _col  = "var(--up)" if _b > 0 else "var(--down)"
        _extra = f" +{len(big_hits)-1}" if len(big_hits) > 1 else ""
        headline_parts.append(
            f'{_icon} <b>{_h.PRODUCT}</b> <span style="color:{_col}">{_b/100:+.2f}%</span> (≥ 1% NAV){_extra}'
        )
    if budget_alert is not None:
        _rem, _mx, _v, _b = budget_alert
        headline_parts.append(
            f'🚨 <b>VaR {_mx:.0f} bps</b> > 1,5× budget MTD ({_rem:.0f} bps)'
        )
    # Thresholds tightened 2026-05-01 session 2 per review §1.1: prior gates
    # (util≥85, |Δ VaR|≥10, |dia|≥5) let "tranquilo" boilerplate render on funds
    # that materially moved (8 of 19 cards). New gates: util≥70 (= UTIL_WARN),
    # |Δ VaR|≥5, |dia|≥3, plus MTD≥25 bps fallback so a chronically drifting
    # fund (FRONTIER YTD −10.83%) doesn't read as "tranquilo" on a small-DIA day.
    if util is not None and util >= 70:
        sev_icon = "🔴" if util >= 85 else "🟡"
        headline_parts.append(f'{sev_icon} <b>{util:.0f}% do soft limit</b>')
    elif dvar_bps is not None and abs(dvar_bps) >= 5:
        direction = "subiu" if dvar_bps > 0 else "caiu"
        headline_parts.append(f'VaR {direction} <b>{abs(dvar_bps):.0f} bps</b> vs D-1')
    if dia_bps is not None and abs(dia_bps) >= 3:
        sign = "+" if dia_bps > 0 else ""
        col = "var(--up)" if dia_bps > 0 else "var(--down)"
        headline_parts.append(f'alpha do dia <b style="color:{col}">{sign}{dia_bps/100:.2f}%</b>')
    if not headline_parts and mtd_bps is not None and abs(mtd_bps) >= 25:
        sign = "+" if mtd_bps > 0 else ""
        col = "var(--up)" if mtd_bps > 0 else "var(--down)"
        headline_parts.append(f'MTD <b style="color:{col}">{sign}{mtd_bps/100:.2f}%</b>')
    if not headline_parts:
        headline_parts.append("dia tranquilo — sem eventos materiais")
    headline = " · ".join(headline_parts)

    # ── Insights bullets ──────────────────────────────────────────────────────
    bullets = []
    if big_hits:
        _items = " · ".join(
            (f'<b>{h.PRODUCT}</b> '
             f'<span class="mono" style="color:{"var(--up)" if float(h.dia_bps) > 0 else "var(--down)"}">'
             f'{float(h.dia_bps)/100:+.2f}%</span>')
            for h in big_hits[:4]
        )
        _lead_icon = "🟢" if all(float(h.dia_bps) > 0 for h in big_hits) else "🚨"
        bullets.append(
            f'<li>{_lead_icon} <b style="color:var(--down)">Contribuição ≥ 1% NAV:</b> {_items}</li>'
        )
    if budget_alert is not None:
        _rem, _mx, _v, _b = budget_alert
        _var_label_bps = "VaR"
        if _b > 0 and _v > 0:
            _val_str = f'VaR {_v:.0f} bps · BVaR {_b:.0f} bps'
        elif _b > 0:
            _val_str = f'BVaR {_b:.0f} bps'
            _var_label_bps = "BVaR"
        else:
            _val_str = f'VaR {_v:.0f} bps'
        _ratio = _mx / _rem if _rem > 0 else float("inf")
        bullets.append(
            f'<li>🚨 <b style="color:var(--down)">Alerta:</b> {_val_str} · '
            f'orçamento MTD disponível <span class="mono">{_rem:.0f} bps</span> · '
            f'razão <b class="mono" style="color:var(--down)">{_ratio:.1f}×</b> '
            f'(> 1,5× — uma perda típica esgota o budget e sobra stress)</li>'
        )
    if len(top_contrib) > 0:
        items = " · ".join(
            f'<b>{r.PRODUCT}</b> {r.dia_bps/100:+.2f}%'
            for r in top_contrib.itertuples(index=False)
        )
        bullets.append(f'<li><span style="color:var(--up)">▲</span> <b>Contribuintes:</b> {items}</li>')
    if len(top_detract) > 0:
        items = " · ".join(
            f'<b>{r.PRODUCT}</b> {r.dia_bps/100:+.2f}%'
            for r in top_detract.itertuples(index=False)
        )
        bullets.append(f'<li><span style="color:var(--down)">▼</span> <b>Detratores:</b> {items}</li>')
    # FRONTIER — net vs IBOV bench is misleading (90% long + 10% cash reads as "10% short").
    # Report gross equity allocation + weighted portfolio beta from df_frontier instead.
    if short == "FRONTIER" and df_frontier is not None and not df_frontier.empty:
        _tot = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
        if not _tot.empty:
            _gross = float(_tot.iloc[0]["% Cash"]) if pd.notna(_tot.iloc[0]["% Cash"]) else None
            _cash  = max(0.0, 1.0 - _gross) if _gross is not None else None
            _stocks = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL","SUBTOTAL","AÇÕES ALIENADAS"])]
            _sv = _stocks.dropna(subset=["BETA","% Cash"])
            _psum = _sv["% Cash"].sum()
            _wbeta = float((_sv["% Cash"] * _sv["BETA"]).sum() / _psum) if _psum else None
            parts = []
            if _gross is not None:
                parts.append(f'<b class="mono" style="color:var(--up)">{_gross*100:.0f}%</b> equity')
            if _cash is not None:
                parts.append(f'<span class="mono" style="color:var(--muted)">{_cash*100:.0f}% caixa</span>')
            if _wbeta is not None:
                beta_col = ("var(--down)" if _wbeta > 1.10
                            else "var(--warn)" if _wbeta > 1.05
                            else "var(--text)")
                parts.append(
                    f'β ponderado <b class="mono" style="color:{beta_col}">{_wbeta:.2f}</b>'
                    f' <span style="color:var(--muted);font-size:10px">(IBOV β=1.00)</span>'
                )
            if parts:
                bullets.append(
                    '<li>📊 <b>Alocação:</b> ' + ' · '.join(parts) + '</li>'
                )
    elif dominant_factor:
        fk, v = dominant_factor
        is_rate = fk in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
        if is_rate:
            # Convenção DV01 (ground truth): tomado = DV01 > 0 (short bond / short DI1F),
            # dado = DV01 < 0 (long bond / long DI1F). factor_matrix já vem nessa convenção.
            direction = "tomado" if v > 0 else "dado"
            dir_color = "var(--down)" if v > 0 else "var(--up)"
        else:
            direction = "longo" if v > 0 else "curto"
            dir_color = "var(--up)" if v > 0 else "var(--down)"
        # Per-fund bullets use RELATIVE measures only: yr-equivalent for rate factors,
        # %NAV for everything else. Absolute BRL is cross-fund territory.
        is_rate_unit = fk in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
        fund_nav = house_row["nav"] if house_row else 0
        if is_rate_unit and fund_nav:
            yr_eq = v / fund_nav
            val_str = f'<span class="mono">{yr_eq:+.2f} yr</span>'
        elif fund_nav:
            pct_nav = v / fund_nav * 100
            val_str = f'<span class="mono">{pct_nav:+.1f}% NAV</span>'
        else:
            val_str = '<span class="mono" style="color:var(--muted)">—</span>'
        bullets.append(
            f'<li>🎯 <b>Posição líquida:</b> '
            f'<b class="mono" style="color:{dir_color} !important">{direction}</b> em '
            f'<b>{fk}</b> · {val_str}</li>'
        )
    if stop_min and stop_min[1] < 30:
        pm, m = stop_min
        bullets.append(
            f'<li>⚠ <b>Stop apertado:</b> {pm} com <span class="mono">{m:.0f} bps</span> de margem</li>'
        )
    if short == "FRONTIER" and frontier_bvar:
        bullets.append(
            f'<li>📊 <b>BVaR HS vs IBOV:</b> <span class="mono">{frontier_bvar["bvar_pct"]:.2f}%</span> · '
            f'TE anualizada <span class="mono">{frontier_bvar["std_er_pct"]*(252**0.5):.1f}%</span></li>'
        )
    if not bullets:
        bullets.append('<li><span style="color:var(--muted)">— sem destaques materiais hoje</span></li>')

    # ── Commentary (prose synthesis, 1–3 sentences) ───────────────────────────
    commentary_parts = []

    # Sentence 1 — day summary
    if dia_bps is None:
        commentary_parts.append(
            "Sem PA disponível no dia; olhar apenas VaR e exposições abaixo."
        )
    elif abs(dia_bps) < 5 and (dvar_bps is None or abs(dvar_bps) < 5):
        commentary_parts.append(
            "Dia neutro — alpha e risco estáveis, nada material para reportar."
        )
    else:
        parts1 = []
        if abs(dia_bps) >= 5:
            sign = "+" if dia_bps > 0 else ""
            parts1.append(f"alpha do dia em <b>{sign}{dia_bps/100:.2f}%</b>")
        if dvar_bps is not None and abs(dvar_bps) >= 5:
            parts1.append(
                f"VaR {'↑' if dvar_bps > 0 else '↓'} <b>{abs(dvar_bps):.0f} bps</b> vs D-1"
            )
        commentary_parts.append("Dia com " + " e ".join(parts1) + ".")

    # Sentence 2 — list top contributor/detractor by absolute size (no ratios).
    # Only mention when the contribution itself is material (|bps| ≥ 3), and
    # avoid mixing sides — just report what pulled up and what pulled down.
    flow_parts = []
    if len(top_contrib) > 0:
        top_c = top_contrib.iloc[0]
        if abs(float(top_c["dia_bps"])) >= 3:
            flow_parts.append(
                f"contrib <b>{top_c['PRODUCT']}</b> <span style=\"color:var(--up)\">+{float(top_c['dia_bps'])/100:.2f}%</span>"
            )
    if len(top_detract) > 0:
        top_d = top_detract.iloc[-1]  # most negative
        if abs(float(top_d["dia_bps"])) >= 3:
            flow_parts.append(
                f"detrator <b>{top_d['PRODUCT']}</b> <span style=\"color:var(--down)\">{float(top_d['dia_bps'])/100:.2f}%</span>"
            )
    if flow_parts:
        commentary_parts.append("Fluxo: " + " · ".join(flow_parts) + ".")

    # Sentence 3 — positioning / warnings
    pos_notes = []
    if util is not None:
        if util >= 85:
            pos_notes.append(f"util VaR em <b>{util:.0f}%</b> do soft — próximo do limite")
        elif util >= UTIL_WARN:
            pos_notes.append(f"util VaR em {util:.0f}% — vigilância")
    # FRONTIER — report gross equity allocation + weighted beta (not net vs IBOV).
    if short == "FRONTIER" and df_frontier is not None and not df_frontier.empty:
        _tot2 = df_frontier[df_frontier["PRODUCT"] == "TOTAL"]
        if not _tot2.empty and pd.notna(_tot2.iloc[0]["% Cash"]):
            _gross2 = float(_tot2.iloc[0]["% Cash"])
            _stk = df_frontier[~df_frontier["PRODUCT"].isin(["TOTAL","SUBTOTAL","AÇÕES ALIENADAS"])]
            _stkv = _stk.dropna(subset=["BETA","% Cash"])
            _ps = _stkv["% Cash"].sum()
            _wb = float((_stkv["% Cash"] * _stkv["BETA"]).sum() / _ps) if _ps else None
            _bit = f"alocação equity <b>{_gross2*100:.0f}%</b>"
            if _wb is not None:
                _bit += f", β ponderado <b>{_wb:.2f}</b>"
            pos_notes.append(_bit)
    elif dominant_factor:
        fk, v = dominant_factor
        is_rate = fk in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
        # Convenção DV01: tomado = DV01 > 0 (short); dado = DV01 < 0 (long, ex: NTN-B comprado).
        dir_word = (("tomado" if v > 0 else "dado") if is_rate
                    else ("longo" if v > 0 else "curto"))
        fund_nav_tmp = house_row["nav"] if house_row else 0
        if is_rate and fund_nav_tmp:
            pos_notes.append(f"{dir_word} em {fk} ({v/fund_nav_tmp:+.2f} yr eq)")
        elif fund_nav_tmp:
            pos_notes.append(f"{dir_word} em {fk} ({v/fund_nav_tmp*100:+.1f}% NAV)")
        else:
            pos_notes.append(f"{dir_word} em {fk}")
    if stop_min and stop_min[1] < 30:
        pos_notes.append(f"PM {stop_min[0]} com apenas {stop_min[1]:.0f} bps de stop")
    if pos_notes:
        commentary_parts.append("Posicionamento: " + "; ".join(pos_notes) + ".")

    commentary = " ".join(commentary_parts)

    return f"""
    <section class="card brief-card">
      <div class="card-head">
        <span class="card-title">Briefing — {FUND_LABELS.get(short, short)}</span>
        <span class="card-sub">— {DATA_STR} · resumo rápido 1–2 min</span>
      </div>
      <div class="brief-headline">{headline}</div>
      <p class="brief-commentary">{commentary}</p>
      <div class="sn-inline-stats mono" style="margin-bottom:14px; flex-wrap:wrap; gap:6px 14px">
        {chips}
      </div>
      <ul class="brief-list">{"".join(bullets)}</ul>
      <div class="brief-footnote" style="margin-top:10px; padding-top:8px; border-top:1px solid var(--line); font-size:10.5px; color:var(--muted-strong); font-weight:500; line-height:1.5">
        <b>Convenção de juros:</b>
        <span style="color:var(--up);font-weight:700">dado</span> = DV01 &lt; 0 (long bond · ex: NTN-B comprado, long DI1F) ·
        <span style="color:var(--down);font-weight:700">tomado</span> = DV01 &gt; 0 (short bond · ex: DI1F vendido).
      </div>
    </section>"""


def _build_executive_briefing(
    *,
    house_rows, factor_matrix, bench_matrix, agg_rows,
    position_changes, vol_regime_map, pm_margem, df_pa,
    frontier_bvar, series_map, td_by_short, ibov, cdi,
    usdbrl=None, di1_3y=None,
) -> str:
    """Curated 3–5 min briefing. Pulls from already-computed structures.
       Rotates headline category to avoid daily repetition; hides sections
       that have nothing material to report.
    """
    def _mm(v):
        try: return _fmt_br_num(f"{v/1e6:,.1f}M")
        except Exception: return "—"
    def _pct(v, sign=True):
        try:
            f = float(v)
            return f"{'+' if f>=0 and sign else ''}{f:.2f}%"
        except Exception: return "—"
    def _bps(v, sign=True):
        try:
            f = float(v)
            return f"{'+' if f>=0 and sign else ''}{f:.0f} bps"
        except Exception: return "—"

    parts_headline = []
    parts_risk   = []   # what moved
    parts_alpha  = []   # what pulled
    parts_insight = []  # non-obvious read
    parts_watch  = []   # attention bullets

    # ── Data summary numbers ──────────────────────────────────────────────────
    house_bvar_total = sum(r["bvar_brl"] for r in house_rows) if house_rows else 0
    house_var_total  = sum(r["var_brl"]  for r in house_rows) if house_rows else 0
    house_nav_total  = sum(r["nav"]      for r in house_rows) if house_rows else 0

    # Δ VaR D-1 per fund (bps of NAV)
    dvar_list = []  # (short, dvar_bps)
    for r in house_rows:
        td = td_by_short.get(r["short"])
        s = series_map.get(td) if td else None
        if s is None or s.empty:
            continue
        s_avail = s[s["VAL_DATE"] <= DATA]
        if len(s_avail) < 2:
            continue
        v_today = abs(float(s_avail.iloc[-1]["var_pct"]))
        v_prev  = abs(float(s_avail.iloc[-2]["var_pct"]))
        dvar_bps = (v_today - v_prev) * 100
        dvar_list.append((r["short"], dvar_bps, r["label"], v_today))
    dvar_list.sort(key=lambda x: abs(x[1]), reverse=True)

    # Top util VaR — use primary metric: BVaR for IDKAs (bvar-benchmarked),
    # absolute VaR for CDI-benchmarked funds. Matches the mandate soft limit.
    utils = []
    for r in house_rows:
        td = td_by_short.get(r["short"])
        cfg = ALL_FUNDS.get(td, {}) if td else {}
        if cfg.get("informative"):
            continue
        soft = cfg.get("var_soft", 99)
        v_pct = r["bvar_pct"] if cfg.get("primary") == "bvar" else r["var_pct"]
        util = v_pct / soft * 100 if soft else 0
        utils.append((r["short"], r["label"], util, v_pct, soft))
    utils.sort(key=lambda x: x[2], reverse=True)

    # ── HEADLINE ──────────────────────────────────────────────────────────────
    headline = None
    # Rule 0 (highest priority): any single PA contribution ≥ 1% (100 bps) do NAV.
    # Benchmark-tracking livros (IDKA CI) excluded — they move with the index, not alpha.
    if df_pa is not None and not df_pa.empty:
        _pa_alpha = _pa_filter_alpha(df_pa)
        _pa_hit = _pa_alpha[
            ~_pa_alpha["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos", "Prev"}) &
            ~_pa_alpha["CLASSE"].isin({"Caixa", "Custos"}) &
            (_pa_alpha["dia_bps"].abs() >= 100.0)
        ].sort_values("dia_bps", key=lambda s: s.abs(), ascending=False)
        if not _pa_hit.empty:
            _r0 = _pa_hit.iloc[0]
            _PA_TO_SHORT = {v: k for k, v in _FUND_PA_KEY.items()}
            _fund_lbl = FUND_LABELS.get(_PA_TO_SHORT.get(_r0["FUNDO"], _r0["FUNDO"]), _r0["FUNDO"])
            _bps0 = float(_r0["dia_bps"])
            _col = "var(--up)" if _bps0 > 0 else "var(--down)"
            _icon = "🟢" if _bps0 > 0 else "🚨"
            _extra = ""
            if len(_pa_hit) > 1:
                _extra = f' · mais {len(_pa_hit)-1} contribuição(ões) ≥ 1% em outros produtos'
            headline = (
                f'{_icon} <b>{_r0["PRODUCT"]}</b> ({_fund_lbl}) contribuiu '
                f'<span class="mono" style="color:{_col}">{_bps0/100:+.2f}%</span> do NAV hoje{_extra}.'
            )
    # Rule 1: pick the most urgent of (margem_inverse, util_VaR, |Δ VaR|).
    # Each kind has its own trigger; if multiple trigger, the highest-urgency
    # score wins. Per §1.2 of the code review: a 9 bps margem on an active PM
    # outranks a 14 bps Δ VaR on a comfortable-util IDKA.
    if not headline:
        _STOP_BASE_BPS  = 63.0   # MACRO monthly base
        _MARGEM_TRIGGER = 25.0   # < 25 bps margem → headline-worthy (one-bad-day risk)
        _UTIL_TRIGGER   = 85.0   # ≥ 85% util VaR → headline
        _DVAR_TRIGGER   = 10.0   # ≥ 10 bps Δ VaR vs D-1 → headline
        _candidates = []  # (score, kind, payload)
        if utils and utils[0][2] >= _UTIL_TRIGGER:
            _candidates.append((utils[0][2], "util", utils[0]))
        if dvar_list and abs(dvar_list[0][1]) >= _DVAR_TRIGGER:
            _candidates.append((abs(dvar_list[0][1]), "dvar", dvar_list[0]))
        if pm_margem:
            _pm_low, _m_val = min(pm_margem.items(), key=lambda x: x[1])
            if _m_val < _MARGEM_TRIGGER:
                _candidates.append((_STOP_BASE_BPS - _m_val, "marg", (_pm_low, _m_val)))
        if _candidates:
            _candidates.sort(key=lambda x: x[0], reverse=True)
            _, _kind, _payload = _candidates[0]
            if _kind == "util":
                s, lbl, u, v, soft = _payload
                headline = f'🔴 <b>{lbl}</b> em <span class="mono">{u:.0f}%</span> do soft limit VaR (<span class="mono">{v:.2f}%</span> vs soft {soft:.2f}%).'
            elif _kind == "dvar":
                s, dv, lbl, v_today = _payload
                direction = "subiu" if dv > 0 else "caiu"
                headline = f'<b>{lbl}</b> VaR {direction} <span class="mono">{abs(dv):.0f} bps</span> vs D-1 (agora <span class="mono">{v_today:.2f}%</span>).'
            elif _kind == "marg":
                _pm_name, _m_val = _payload
                headline = f'🟠 <b>MACRO · {_pm_name}</b> margem em <span class="mono">{_m_val:.0f} bps</span> — risco de breach intramês.'
    # Rule 3: concentration — top fund absorbs >40% of house BVaR
    if not headline and house_rows and house_bvar_total > 0:
        house_rows_sorted = sorted(house_rows, key=lambda r: r["bvar_brl"], reverse=True)
        top = house_rows_sorted[0]
        pct = top["bvar_brl"] / house_bvar_total * 100
        if pct >= 40:
            headline = f'<b>{top["label"]}</b> absorve <span class="mono">{pct:.0f}%</span> do risco ativo da casa (<span class="mono">{_mm(top["bvar_brl"])}</span> de <span class="mono">{_mm(house_bvar_total)}</span> BVaR).'
    if not headline:
        headline = "Dia sem mudanças materiais — carteira em regime normal. Monitorar rotação gradual."

    # ── RISCO · O QUE MUDOU ───────────────────────────────────────────────────
    for s, dv, lbl, v_today in dvar_list[:3]:
        if abs(dv) < 5:  # threshold for materiality
            continue
        direction = "↑" if dv > 0 else "↓"
        parts_risk.append(
            f'<li><span class="mono">{direction} {abs(dv):.0f} bps</span> · '
            f'<b>{lbl}</b> VaR agora {v_today:.2f}%</li>'
        )
    # Top 2 position_changes (fund-agnostic)
    if position_changes:
        all_changes = []
        for short, df_ch in (position_changes or {}).items():
            if df_ch is None or df_ch.empty:
                continue
            for _, r in df_ch.head(5).iterrows():
                all_changes.append((short, r))
        # Just flag if there's anything with |Δ| ≥ 5 pp
        big_changes = [
            (short, r) for short, r in all_changes
            if abs(float(r.get("delta_pp", 0))) >= 5
        ]
        big_changes.sort(key=lambda x: abs(float(x[1].get("delta_pp", 0))), reverse=True)
        for short, r in big_changes[:2]:
            lbl = FUND_LABELS.get(short, short)
            factor = r.get("rf", r.get("CLASSE", ""))
            dpp = float(r.get("delta_pp", 0))
            direction = "↑" if dpp > 0 else "↓"
            parts_risk.append(
                f'<li><span class="mono">{direction} {abs(dpp):.1f}%</span> · '
                f'<b>{lbl}</b> mudou exposição em <i>{factor}</i></li>'
            )

    # ── ALPHA · O QUE PUXOU ────────────────────────────────────────────────────
    # Cross-fund top contributors / detractors in BRL (dia_bps × NAV / 10000)
    if df_pa is not None and not df_pa.empty:
        _pa = df_pa.copy()
        _pa = _pa[~_pa["LIVRO"].isin({"Caixa", "Caixa USD", "Taxas e Custos", "Prev"})]
        _pa = _pa[~_pa["CLASSE"].isin({"Caixa", "Custos"})]
        # Convert to BRL via fund NAV
        nav_by_pa = {}
        _PA_TO_SHORT = {v: k for k, v in _FUND_PA_KEY.items()}
        for r in house_rows:
            short = r["short"]
            pa_key = _FUND_PA_KEY.get(short)
            if pa_key:
                nav_by_pa[pa_key] = r["nav"]
        _pa["nav"] = _pa["FUNDO"].map(nav_by_pa).fillna(0)
        _pa["brl_dia"] = _pa["dia_bps"] * _pa["nav"] / 10000
        _pa = _pa[_pa["brl_dia"].abs() > 50_000]
        contribs = _pa.sort_values("brl_dia", ascending=False).head(2)
        detract  = _pa.sort_values("brl_dia").head(2)
        for r in contribs.itertuples(index=False):
            fund_lbl = FUND_LABELS.get(_PA_TO_SHORT.get(r.FUNDO, r.FUNDO), r.FUNDO)
            parts_alpha.append(
                f'<li><span class="mono up">{r.dia_bps:+.1f} bps</span> · '
                f'<b>{r.PRODUCT}</b> ({fund_lbl})</li>'
            )
        for r in detract.itertuples(index=False):
            fund_lbl = FUND_LABELS.get(_PA_TO_SHORT.get(r.FUNDO, r.FUNDO), r.FUNDO)
            parts_alpha.append(
                f'<li><span class="mono down">{r.dia_bps:+.1f} bps</span> · '
                f'<b>{r.PRODUCT}</b> ({fund_lbl})</li>'
            )

    # ── LEITURA DO DIA (non-obvious insights) ─────────────────────────────────
    # (a) Top-3 concentration of house risk
    if agg_rows:
        from collections import defaultdict
        inst_totals = defaultdict(float)
        for r in agg_rows:
            key = (r["factor"], r["product"])
            inst_totals[key] += r["brl"]
        sorted_inst = sorted(inst_totals.items(), key=lambda x: abs(x[1]), reverse=True)
        top3 = sorted_inst[:3]
        if top3:
            labels = " + ".join(f'<b>{k[1]}</b>' for k, _ in top3)
            top3_sum = sum(abs(v) for _, v in top3)
            # Rough share of house — compare against sum of |all| for that unit category
            same_unit = [abs(v) for (f, _), v in inst_totals.items()
                         if agg_rows[0]["unit"] == next((r["unit"] for r in agg_rows if r["factor"] == f), None)]
            parts_insight.append(
                f'<li><b>Concentração:</b> {labels} somam '
                f'<span class="mono">{_mm(top3_sum)}</span> de exposição — '
                f'as 3 maiores posições da casa.</li>'
            )

    # (b) Dominant factor (largest |net| across house)
    if factor_matrix and bench_matrix:
        net_by_factor = {}
        for fk, allocs in factor_matrix.items():
            benches = bench_matrix.get(fk, {})
            total_net = sum(
                allocs.get(s, 0.0) - benches.get(s, 0.0)
                for s in set(allocs) | set(benches)
            )
            if abs(total_net) > 1_000:
                net_by_factor[fk] = total_net
        if net_by_factor:
            dom = max(net_by_factor.items(), key=lambda x: abs(x[1]))
            # Para fatores de juros use terminologia "tomado"/"dado" (convenção DV01):
            #   tomado = DV01 > 0 (short bond / short DI1F; ganha com alta de juros) → vermelho
            #   dado   = DV01 < 0 (long bond / long DI1F; ex: NTN-B comprado)        → verde
            # factor_matrix já está na convenção DV01 (raw DELTA para MACRO df_expo;
            # rf_expo_maps é negado ao popular factor_matrix para cancelar o flip).
            is_rate = dom[0] in ("Juros Reais (IPCA)", "Juros Nominais", "IPCA Idx")
            if is_rate:
                direction = "tomado" if dom[1] > 0 else "dado"
                dir_color = "var(--down)" if dom[1] > 0 else "var(--up)"
            else:
                direction = "longo" if dom[1] > 0 else "curto"
                dir_color = "var(--up)"   if dom[1] > 0 else "var(--down)"
            parts_insight.append(
                f'<li><b>Fator dominante:</b> casa está '
                f'<b class="mono" style="color:{dir_color} !important; font-weight:700">{direction}</b> em '
                f'<b>{dom[0]}</b> · <span class="mono">{_mm(dom[1])}</span> líquido do bench.</li>'
            )

    # (c) Vol regime shift
    if vol_regime_map:
        stressed = [(k, v) for k, v in vol_regime_map.items() if v and v.get("regime") == "stressed"]
        elevated = [(k, v) for k, v in vol_regime_map.items() if v and v.get("regime") == "elevated"]
        if stressed:
            names = ", ".join(k for k, _ in stressed)
            parts_insight.append(
                f'<li><b>Vol regime stressed:</b> {names} — pct rank ≥ 90. Histórico sugere cautela em tamanho.</li>'
            )
        elif elevated and len(elevated) >= 2:
            names = ", ".join(k for k, _ in elevated)
            parts_insight.append(
                f'<li><b>Vol regime elevated:</b> {names} acima do p70 — vigilância gradual.</li>'
            )

    # ── ATENÇÃO (max 3 bullets) ────────────────────────────────────────────────
    for s, lbl, u, v, soft in utils[:2]:
        if u >= 70:
            parts_watch.append(
                f'<li><b>{lbl}</b> util VaR em <span class="mono">{u:.0f}%</span> do soft — revisar tamanho se subir mais.</li>'
            )
    # MACRO stop flag
    if pm_margem:
        for pm, margem in pm_margem.items():
            if margem <= 20:  # low margin = red zone
                parts_watch.append(
                    f'<li><b>MACRO · {pm}</b> margem de stop em <span class="mono">{margem:.0f} bps</span> — espaço curto.</li>'
                )
                break
    parts_watch = parts_watch[:3]

    # ── Render ────────────────────────────────────────────────────────────────
    def _section(title, items, empty_msg=None):
        if not items:
            return f'<p style="color:var(--muted); font-style:italic; margin-top:8px">{empty_msg or "— nada material hoje"}</p>' if empty_msg else ""
        return (
            f'<div class="brief-section-title">{title}</div>'
            f'<ul class="brief-list">{"".join(items)}</ul>'
        )

    # Benchmarks para context
    bench_line = ""
    if ibov and cdi:
        def _fmt(v):
            pct = v / 100 if v else 0
            col = "up" if pct >= 0 else "down"
            return f'<span class="mono {col}">{pct:+.2f}%</span>'
        def _fmt_bps(v):
            try: f = float(v)
            except Exception: f = 0.0
            col = "up" if f <= 0 else "down"  # rate up = juros mais altos = vermelho
            return f'<span class="mono {col}">{f:+.0f} bps</span>'
        parts = [
            f'IBOV {_fmt(ibov.get("dia"))}',
            f'CDI {_fmt(cdi.get("dia"))}',
        ]
        if usdbrl is not None:
            parts.append(f'BRL {_fmt(usdbrl.get("dia"))}')
        if di1_3y is not None and di1_3y.get("rate") is not None:
            inst = di1_3y.get("instrument") or "DI1 3y"
            rate = di1_3y.get("rate")
            parts.append(
                f'{inst} <span class="mono">{rate:.2f}%</span> '
                f'({_fmt_bps(di1_3y.get("dia_bps"))})'
            )
        bench_line = (
            f'<div class="brief-benchmarks">Benchmarks hoje: '
            + " · ".join(parts) + '</div>'
        )

    return f"""
    <section class="card brief-card">
      <div class="card-head">
        <span class="card-title">Briefing Executivo</span>
        <span class="card-sub">— {DATA_STR} · 3–5 min · visão curada sobre risco e delta</span>
      </div>
      <div class="brief-headline">{headline}</div>
      {bench_line}
      {_section("Atenção", parts_watch) if parts_watch else ""}
      <div class="brief-grid">
        <div class="brief-col">
          {_section("Risco · o que mudou", parts_risk, "— VaR estável em todos os fundos vs D-1")}
          {_section("Alpha · o que puxou", parts_alpha, "— PnL do dia concentrado em ruído (nenhum contribuinte ≥ R$50k)")}
        </div>
        <div class="brief-col">
          {_section("Leitura do dia", parts_insight)}
        </div>
      </div>
      <div class="brief-annex">
        → Detalhe em: <a href="#" onclick="document.querySelector('.summary-table')?.scrollIntoView({{behavior:'smooth'}});return false">Status consolidado</a> ·
        <a href="#" onclick="document.querySelector('[class*=rf-brl-body]')?.closest('.card')?.scrollIntoView({{behavior:'smooth'}});return false">Breakdown por Fator</a> ·
        <a href="#" onclick="Array.from(document.querySelectorAll('.card-title')).find(function(x){{return x.textContent.indexOf('Top Posi')===0}})?.closest('.card')?.scrollIntoView({{behavior:'smooth'}});return false">Top Posições</a>
      </div>
      <div class="brief-footnote" style="margin-top:10px; padding-top:8px; border-top:1px solid var(--line); font-size:10.5px; color:var(--muted-strong); font-weight:500; line-height:1.5">
        <b>Convenção de juros:</b>
        <span style="color:var(--up);font-weight:700">dado</span> = DV01 &lt; 0 (long bond · ex: NTN-B comprado, long DI1F) ·
        <span style="color:var(--down);font-weight:700">tomado</span> = DV01 &gt; 0 (short bond · ex: DI1F vendido).
      </div>
    </section>"""


_RISK_BUDGET_OVERRIDES_PATH = Path(__file__).parent / "data" / "mandatos" / "risk_budget_overrides.json"


def carry_step(budget_abs: float, pnl: float, ytd: float) -> tuple[float, bool]:
    """Calcula budget do próximo mês e flag gancho.

    Regras:
      - pnl POSITIVO: novo budget = STOP_BASE + 50% × pnl. Bônus aplicado sempre
        que o mês fechou ganhando.
        Ex: pnl +60 → budget = 63 + 0.5·60 = 93.

      - pnl NEGATIVO: 3 camadas de penalty para o mês seguinte.
        · Extra (B_t > 63 → portion above base): 25% penalty
        · Base (até 63):                          50% penalty
        · Excess (acima de B_t):                  100% penalty
        Próximo budget = max(0, min(B_t, 63) − penalty). Carry extra não usado
        é "use it or lose it" — não rola pra o mês seguinte.
        Ex: B=88 (=63+25 extra), perda=60 → 25%·25 + 50%·35 = 23.75 → 63−23.75 = 39.25.

      - SEMESTRAL: se STOP_SEM − |YTD_após| < monthly budget, usa o semestral.

    Args:
        budget_abs: stop no início do mês (>= 0).
        pnl: realizado do mês (signed, bps).
        ytd: YTD já incluindo o pnl do mês (convention de build_stop_history).
    Returns:
        (next_month_budget, gancho_flag)
    """

    if pnl >= 0:
        next_abs = STOP_BASE + 0.5 * pnl
        gancho = False
    else:
        loss = abs(pnl)
        extra_avail = max(0.0, budget_abs - STOP_BASE)
        base_avail  = min(budget_abs, STOP_BASE)

        loss_in_extra = min(loss, extra_avail)
        remaining     = loss - loss_in_extra
        loss_in_base  = min(remaining, base_avail)
        remaining    -= loss_in_base
        loss_excess   = max(0.0, remaining)

        penalty = 0.25 * loss_in_extra + 0.50 * loss_in_base + 1.00 * loss_excess
        next_abs = max(0.0, min(budget_abs, STOP_BASE) - penalty)
        gancho = next_abs <= 0
        if gancho:
            next_abs = 0.0

    # Semestral cap (usa YTD após o mês — o que resta permitido no semestre)
    if ytd < 0:
        sem_cap = STOP_SEM - abs(ytd)
        if sem_cap < next_abs:
            next_abs = max(0.0, sem_cap)
            if next_abs <= 0:
                gancho = True
    return next_abs, gancho


def _load_risk_budget_overrides() -> dict[tuple[str, pd.Timestamp], float]:
    """Carrega overrides manuais de orçamento de {data/mandatos/risk_budget_overrides.json}.
       Formato do arquivo:
       {
         "overrides": [
           {"livro": "Macro_RJ", "month": "2026-04", "budget_bps": 63.0, "note": "..."}
         ]
       }
       Se o arquivo não existir, usa o override hardcoded histórico como fallback."""
    fallback = {("Macro_RJ", pd.Timestamp("2026-04-01")): 63.0}
    try:
        if not _RISK_BUDGET_OVERRIDES_PATH.exists():
            return fallback
        with open(_RISK_BUDGET_OVERRIDES_PATH, encoding="utf-8") as f:
            data = json.load(f)
        out: dict[tuple[str, pd.Timestamp], float] = {}
        for entry in data.get("overrides", []):
            livro = entry.get("livro")
            month_s = entry.get("month", "")
            budget = entry.get("budget_bps")
            if not livro or not month_s or budget is None:
                continue
            # Accept "YYYY-MM" or "YYYY-MM-DD"; normalize to first-of-month
            ts = pd.Timestamp(month_s + ("-01" if len(month_s) == 7 else ""))
            out[(livro, ts.normalize())] = float(budget)
        return out or fallback
    except Exception as e:
        print(f"  risk_budget_overrides load failed ({e}) — using fallback")
        return fallback


def build_stop_history(df_pnl: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Reconstruct monthly stop budget series per PM from PnL history."""
    overrides = _load_risk_budget_overrides()
    pm_map = {
        "CI":       "CI",
        "Macro_LF": "LF",
        "Macro_JD": "JD",
        "Macro_RJ": "RJ",
    }
    CI_HARD = 233.0   # CI (Comitê) hard stop — fixed, no carry
    CI_SOFT = 150.0   # CI soft mark (shown on bar)

    result = {}
    for livro, pm in pm_map.items():
        sub = df_pnl[df_pnl["LIVRO"] == livro].sort_values("mes").copy()
        if sub.empty:
            continue
        rows = []
        budget = CI_HARD if pm == "CI" else STOP_BASE
        ytd    = 0.0
        prev_year = None
        for r in sub.itertuples(index=False):
            mes = r.mes
            pnl = r.pnl_mes_bps
            # new calendar year → reset
            if prev_year is not None and mes.year != prev_year:
                budget = CI_HARD if pm == "CI" else STOP_BASE
                ytd    = 0.0
            prev_year = mes.year
            # apply management override for START of this month (PMs only)
            # normalize() strips any sub-day time component that can result from DB timezone drift
            key = (livro, mes.normalize())
            if key in overrides:
                budget = overrides[key]
            rows.append({"mes": mes, "budget_abs": budget, "pnl": pnl, "ytd": ytd,
                         "soft_mark": CI_SOFT if pm == "CI" else None})
            ytd += pnl
            if pm != "CI":
                budget, gancho = carry_step(budget, pnl, ytd)
                if gancho:
                    budget = 0.0
            # CI budget is fixed — no carry
        result[pm] = pd.DataFrame(rows)
    return result


# ── Análise sections (per-fund outliers / movers / position changes) ─────────
_ANALISE_MOVERS_EXCLUDE = {
    "LIVRO":  {"Taxas e Custos", "Caixa", "Caixa USD"},
    "CLASSE": {"Custos", "Caixa"},
}
_ANALISE_THRESHOLD_PP = 0.30


def _an_delta_pp(delta: float, pct_d0: float) -> str:
    pct_d1 = pct_d0 - delta
    reduces_risk = abs(pct_d0) < abs(pct_d1)
    color = "var(--up)" if reduces_risk else "var(--down)"
    sign = "+" if delta > 0 else ""
    return f'<span style="color:{color}; font-weight:700">{sign}{delta:.2f}%</span>'


def build_analise_sections(d, df_pa_daily) -> list:
    """Build per-fund Análise tab sections (outliers / top movers / position changes).

    Args:
        d: ReportData instance (accessed via d.df_pa, d.df_expo, etc.)
        df_pa_daily: historical PA data for z-score computation.

    Returns:
        list of (fund_short, "analise", html) tuples.
    """
    sections = []

    # Pre-compute per-fund outlier sets (single pass over the historical series).
    outliers_by_fund = {}
    if df_pa_daily is not None and not df_pa_daily.empty:
        try:
            _all_out = compute_pa_outliers(df_pa_daily, DATA_STR, z_min=2.0, bps_min=3.0, min_obs=20)
            if not _all_out.empty:
                for pa_key in _all_out["FUNDO"].unique():
                    outliers_by_fund[pa_key] = _all_out[_all_out["FUNDO"] == pa_key]
        except Exception as e:
            print(f"  per-fund outliers failed ({e})")

    def _an_outliers_card(short):
        pa_key = _FUND_PA_KEY.get(short)
        sub = outliers_by_fund.get(pa_key)
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
        if not pa_key or d.df_pa is None or d.df_pa.empty:
            return ""
        sub = d.df_pa[d.df_pa["FUNDO"] == pa_key]
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
        if d.df_frontier is None or d.df_frontier.empty:
            return None, None
        sub = d.df_frontier[~d.df_frontier["PRODUCT"].isin(["TOTAL", "SUBTOTAL"])].copy()
        sub = sub[sub["TOTAL_IBVSP_DAY"].notna()]
        if sub.empty:
            return None, None
        sub["alpha_pct"] = sub["TOTAL_IBVSP_DAY"].astype(float) * 100.0

        _THR = 0.01

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

        papel_pos = sub[sub["alpha_pct"] >=  _THR].nlargest(5, "alpha_pct")
        papel_neg = sub[sub["alpha_pct"] <= -_THR].nsmallest(5, "alpha_pct")
        papel_body = _split_html(papel_pos, papel_neg, "PRODUCT")

        setor_body = ""
        if d.df_frontier_sectors is not None and not d.df_frontier_sectors.empty:
            merged = sub.merge(
                d.df_frontier_sectors[["TICKER", "GLPG_SECTOR"]],
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
            if d.df_expo is None or d.df_expo_d1 is None:
                return ""
            d0 = d.df_expo.groupby(["pm", "rf"])["pct_nav"].sum().reset_index()
            d1 = d.df_expo_d1.groupby(["pm", "rf"])["pct_nav"].sum().reset_index().rename(
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
                <span class="card-sub">— MACRO · PM × fator · |Δ| ≥ {_ANALISE_THRESHOLD_PP:.2f}% · 🟢 reduz risco · 🔴 aumenta risco</span>
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
            if not d.position_changes:
                return ""
            df_chg = d.position_changes.get(short)
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
                <span class="card-sub">— {FUND_LABELS.get(short, short)} · por fator · |Δ| ≥ {_ANALISE_THRESHOLD_PP:.2f}% · 🟢 reduz risco · 🔴 aumenta risco</span>
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

    return sections



def build_per_fund_risk_monitor_sections(d, *, td_by_short, series_map):
    """Build per-fund Risk Monitor sections (the first big for-loop of build_html).

    Iterates FUND_ORDER and produces, for each fund with VaR/Stress data:
      - The Risk Monitor card (primary metric + secondary + sparklines + util pills)
      - A price-quality pill (ALBATROZ / BALTRA only — credit-bearing instruments)
      - Inline VaR Histórico chart (MACRO only — fund + per-PM lines)
      - Diversificação card (EVOLUTION only — also stashes Camada 4 state)

    Mutations are returned as 3 explicit outputs, no shared state with caller:
      sections           — list of (fund_short, report_id, html) to append
      alerts             — list of alert tuples (short, kind, range_pct, val, util, comment)
      evolution_c4_state — dict consumed later by the Summary tab's C4 alert

    Closure deps avoided: td_by_short and series_map come in as kwargs (computed
    in build_html from ReportData + ALL_FUNDS). util_color stays inline since it's
    only used here.
    """
    sections = []
    alerts = []
    evolution_c4_state = {}

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

        # Price-quality pill — ALBATROZ + BALTRA have direct credit-bearing
        # instruments (CRIs/debentures + Prev-book NTN-Bs). Flag if any PRICE is
        # null in LOTE_BOOK_OVERVIEW for D or D-1.
        pq_flags = None
        if short == "ALBATROZ":
            pq_flags = d.alb_pq_flags
        elif short == "BALTRA":
            pq_flags = d.baltra_pq_flags
        if pq_flags is not None:
            n = 0 if pq_flags.empty else len(pq_flags)
            if n == 0:
                pq_html = (
                    '<section class="card" style="border-color:rgba(38,208,124,.40);padding:8px 14px">'
                    '<span style="color:var(--up);font-weight:600">✓ Sanity Check de Preços</span> '
                    '<span class="card-sub">— todos os ativos não-cota com PRICE em D e D-1</span>'
                    '</section>'
                )
            else:
                _d1_str = str(_prev_bday(DATA_STR))
                rows = []
                for _, r in pq_flags.iterrows():
                    miss_t = bool(r["missing_today"]); miss_p = bool(r["missing_prev"])
                    why = []
                    if miss_t: why.append(f"D ({DATA_STR})")
                    if miss_p: why.append(f"D-1 ({_d1_str})")
                    pos_brl_str = fmt_br_num(f"{r['pos_brl']:,.0f}")
                    rows.append(
                        f'<tr><td>{r["produto"]}</td>'
                        f'<td style="color:var(--muted)">{r["product_class"] or "—"}</td>'
                        f'<td class="mono" style="text-align:right">{pos_brl_str}</td>'
                        f'<td style="color:var(--down);font-size:11px">{" · ".join(why)}</td></tr>'
                    )
                pq_html = (
                    '<section class="card" style="border-color:rgba(255,90,106,.40)">'
                    '<div class="card-head">'
                    f'<span class="card-title" style="color:var(--down)">⚠ Sanity Check de Preços — {n} ativo(s) sem preço</span>'
                    f'<span class="card-sub">— {short} · cotas/caixa/provisões isentas</span>'
                    '</div>'
                    '<table class="summary-table" data-no-sort="1">'
                    '<thead><tr><th>Produto</th><th>Tipo</th>'
                    '<th style="text-align:right">Posição (R$)</th><th>Falha</th></tr></thead>'
                    f'<tbody>{"".join(rows)}</tbody></table></section>'
                )
            risk_monitor_html = risk_monitor_html + pq_html

        sections.append((short, "risk-monitor", risk_monitor_html))

        # MACRO PM VaR history chart — inline below Risk Monitor (CI/LF/RJ/JD + fund total).
        if short == "MACRO" and d.macro_pm_var_hist is not None and not d.macro_pm_var_hist.empty:
            try:
                pm_hist = d.macro_pm_var_hist
                # Fund total series — pull last 121 obs from series_map (consistent with sparkline source)
                fund_s = series_map.get(td_by_short.get("MACRO"))
                if fund_s is not None and not fund_s.empty:
                    fund_s = fund_s[fund_s["VAL_DATE"] <= DATA].tail(len(pm_hist))
                # Units: tudo em bps de NAV.
                #   fund_s["var_pct"] está em pct (0.57 = 0.57%) → ×100 = bps.
                #   pm_hist[pm] já está em bps (× 10000 / NAV).
                fund_vals = (fund_s["var_pct"].abs() * 100).tolist() if fund_s is not None and not fund_s.empty else []
                dates = pm_hist["VAL_DATE"].tolist()
                if len(fund_vals) != len(dates):
                    if fund_s is not None and not fund_s.empty:
                        merged = pm_hist[["VAL_DATE"]].merge(
                            fund_s[["VAL_DATE", "var_pct"]], on="VAL_DATE", how="left"
                        )
                        merged["var_pct"] = merged["var_pct"].ffill().bfill()
                        fund_vals = (merged["var_pct"].abs() * 100).tolist()
                    else:
                        fund_vals = []
                series_payload = []
                if fund_vals:
                    series_payload.append({
                        "label": "Fund (total)", "values": fund_vals,
                        "color": "#facc15", "stroke": 2.4,
                    })
                # Brand-aligned palette: blue family for PMs, gold for fund total.
                pm_colors = {
                    "CI": "#5aa3e8",   # brand blue
                    "LF": "#1a8fd1",   # deep blue (sparkline VaR)
                    "RJ": "#22d3ee",   # cyan
                    "JD": "#a78bfa",   # purple
                }
                for pm in ("CI", "LF", "RJ", "JD"):
                    if pm in pm_hist.columns:
                        series_payload.append({
                            "label": pm,
                            "values": [float(v) for v in pm_hist[pm].tolist()],
                            "color": pm_colors[pm], "stroke": 1.4,
                        })
                chart_svg = multi_line_chart_svg(
                    dates, series_payload, width=820, height=320,
                    y_suffix=" bps",
                )
                # Append to the parent risk-monitor entry rather than as a new
                # section: lazy hydration's querySelector('template[data-fund=...]
                # [data-report=...]') matches only the first template, so a
                # duplicate (fund, report) pair never reaches the live DOM.
                _chart_html = f"""
                <section class="card">
                  <div class="card-head">
                    <span class="card-title">VaR Histórico</span>
                    <span class="card-sub">— MACRO · Fund + PMs (CI/LF/RJ/JD) · últimos {len(dates)}d úteis</span>
                  </div>
                  {chart_svg}
                  <div class="bar-legend" style="margin-top:8px">
                    Fund total via <code>LOTE_FUND_STRESS_RPM</code> (LEVEL=2). PMs via mesma tabela TREE='Main_Macro_Ativos' (LEVEL=10), agregando |Σ signed PARAMETRIC_VAR| por book do PM. Diversificado dentro do PM, não entre PMs.
                  </div>
                </section>"""
                _f, _r, _h = sections[-1]
                sections[-1] = (_f, _r, _h + _chart_html)
            except Exception as _e:
                print(f"  MACRO PM VaR history chart failed ({_e})")

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

    return sections, alerts, evolution_c4_state

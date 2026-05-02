"""
summary_renderers.py — cross-fund summary cards for the Summary view.

Covers card-level HTML blobs that appear in the Summary tab and are
independent of any single fund:

  build_vol_regime_card       Vol Regime table (MACRO / QUANT / EVOLUTION)
  build_movers_card           Top Movers DIA (por Livro / por Classe)
  build_changes_card          Significant position changes D-0 vs D-1
  build_comments_card         PA outlier comments by fund
  build_factor_breakdown_card Factor × Fund exposure breakdown (Líquido/Bruto toggle)
  build_top_positions_card    Top-15 consolidated positions drill-down
"""
from __future__ import annotations

from collections import defaultdict

import pandas as pd

from risk_runtime import DATA_STR, fmt_br_num as _fmt_br_num
from risk_config import FUND_ORDER, FUND_LABELS, _FUND_PA_KEY
from metrics import compute_pa_outliers
from pa_renderers import _pa_render_name


# ── Vol Regime ───────────────────────────────────────────────────────────────

_FUND_PORTFOLIO_KEY = {
    "MACRO":     "MACRO",
    "EVOLUTION": "EVOLUTION",
    "QUANT":     "SIST",
    "MACRO_Q":   "SIST_GLOBAL",
    "ALBATROZ":  "ALBATROZ",
}


def _regime_tag(regime: str) -> str:
    colors = {
        "low":      ("var(--up)",    "low"),
        "normal":   ("var(--muted)", "normal"),
        "elevated": ("var(--warn)",  "elevated"),
        "stressed": ("var(--down)",  "stressed"),
        "—":        ("var(--muted)", "—"),
    }
    c, lbl = colors.get(regime, ("var(--muted)", regime))
    return f'<span class="mono" style="color:{c}; font-weight:700">{lbl}</span>'


def _pct_color(p) -> str:
    if p is None or pd.isna(p):
        return "var(--muted)"
    if p >= 90: return "var(--down)"
    if p >= 70: return "var(--warn)"
    if p < 20:  return "var(--up)"
    return "var(--text)"


def build_vol_regime_card(vol_regime_map: dict) -> str:
    if not vol_regime_map or not any(vol_regime_map.values()):
        return ""
    vol_rows_html = ""
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
        vol_r = r["vol_recent_pct"]
        vol_f = r["vol_full_pct"]
        ratio = r["ratio"]
        pct_v = r["pct_rank"]
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
    if not vol_rows_html:
        return ""
    return f"""
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


# ── Top Movers ───────────────────────────────────────────────────────────────

_MOVERS_EXCLUDE = {
    "LIVRO":  {"Taxas e Custos", "Caixa", "Caixa USD"},
    "CLASSE": {"Custos", "Caixa"},
}


def _movers_rows(df_pa: pd.DataFrame, group_col: str,
                 exclude_fx: bool = False) -> str:
    """Build movers table rows. If exclude_fx=True, drop CLASSE in {BRLUSD, FX}
    so contributions reflect asset effect only (used by 'Por Classe sem FX')."""
    if df_pa is None or df_pa.empty:
        return ""
    exclude = _MOVERS_EXCLUDE.get(group_col, set())
    rows_html = ""
    for short in FUND_ORDER:
        pa_key = _FUND_PA_KEY.get(short)
        if not pa_key:
            continue
        sub = df_pa[df_pa["FUNDO"] == pa_key]
        if sub.empty:
            continue
        if exclude_fx and "CLASSE" in sub.columns:
            sub = sub[~sub["CLASSE"].isin({"BRLUSD", "FX"})]
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


def _movers_table(view_id: str, rows: str, active: bool) -> str:
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


def build_movers_card(df_pa: pd.DataFrame) -> str:
    livro_rows         = _movers_rows(df_pa, "LIVRO")
    classe_rows        = _movers_rows(df_pa, "CLASSE")
    classe_no_fx_rows  = _movers_rows(df_pa, "CLASSE", exclude_fx=True)
    if not livro_rows and not classe_rows and not classe_no_fx_rows:
        return ""
    return f"""
    <section class="card sum-movers-card">
      <div class="card-head">
        <span class="card-title">Top Movers — DIA</span>
        <span class="card-sub">— alpha do dia (drop &lt; 0.5 bps) · Taxas/Custos excluídos</span>
        <div class="pa-view-toggle sum-tgl">
          <button class="pa-tgl active" data-mov-view="livro"
                  onclick="selectMoversView(this,'livro')">Por Livro</button>
          <button class="pa-tgl" data-mov-view="classe"
                  onclick="selectMoversView(this,'classe')">Por Classe</button>
          <button class="pa-tgl" data-mov-view="classe_no_fx"
                  onclick="selectMoversView(this,'classe_no_fx')"
                  title="Agrupa por CLASSE excluindo BRLUSD e FX (só efeito-ativo)">Por Classe (sem FX)</button>
        </div>
      </div>
      {_movers_table("livro",         livro_rows,         True)}
      {_movers_table("classe",        classe_rows,        False)}
      {_movers_table("classe_no_fx",  classe_no_fx_rows,  False)}
    </section>"""


# ── Position Changes ─────────────────────────────────────────────────────────

_CHANGES_THRESHOLD_PP = 0.30


def _delta_pp(v: float) -> str:
    color = "var(--up)" if v > 0 else "var(--down)"
    sign = "+" if v > 0 else ""
    return f'<span style="color:{color}; font-weight:700">{sign}{v:.2f}%</span>'


def build_changes_card(df_expo, df_expo_d1, position_changes) -> str:
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
            chg = chg[chg["delta"].abs() >= _CHANGES_THRESHOLD_PP]
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
            big = df_chg[df_chg["delta"].abs() >= _CHANGES_THRESHOLD_PP]
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

    if not change_blocks:
        return ""
    return f"""
        <section class="card">
          <div class="card-head">
            <span class="card-title">Mudanças Significativas</span>
            <span class="card-sub">— exposição D-0 vs D-1 · |Δ| ≥ {_CHANGES_THRESHOLD_PP:.2f}% · MACRO por PM×fator, outros por fator agregado</span>
          </div>
          <div class="comments-grid">{''.join(change_blocks)}</div>
        </section>"""


# ── PA Outlier Comments ──────────────────────────────────────────────────────

_VAR_DELTA_MIN_BPS = 5.0  # minimum fund-level |Δ VaR| to emit a VaR bullet


_BVAR_KEYS = {"IDKA_3Y", "IDKA_10Y", "FRONTIER"}


def build_comments_card(
    df_pa_daily: pd.DataFrame,
    var_commentary: dict | None = None,
) -> str:
    """var_commentary: {short → {
        "delta": float, "driver": str, "driver_delta": float,
        "pos_eff": float|None, "marg_eff": float|None, "override": bool,
    }}"""
    if df_pa_daily is None or df_pa_daily.empty:
        return ""
    try:
        outliers = compute_pa_outliers(df_pa_daily, DATA_STR, z_min=2.0, bps_min=3.0, min_obs=20)
        body_chunks = []
        for short in FUND_ORDER:
            pa_key = _FUND_PA_KEY.get(short)
            if not pa_key:
                continue
            sub = outliers[outliers["FUNDO"] == pa_key] if not outliers.empty else outliers
            label = FUND_LABELS.get(short, short)

            items = ""
            # ── VaR delta bullet (top-1 driver + pos/marg + override flag) ───
            vc = (var_commentary or {}).get(short)
            if vc:
                delta    = vc["delta"]
                dd       = vc["driver_delta"]
                delta_c  = "var(--up)" if delta < 0 else "var(--down)"
                dd_c     = "var(--up)" if dd < 0 else "var(--down)"
                metric_lbl = "BVaR" if short in _BVAR_KEYS else "VaR"
                # Optional pos/marg decomp suffix (only when fund publishes per-row pos data)
                decomp = ""
                pe = vc.get("pos_eff"); me = vc.get("marg_eff")
                if pe is not None and me is not None:
                    pe_c = "var(--up)" if pe < 0 else "var(--down)"
                    me_c = "var(--up)" if me < 0 else "var(--down)"
                    decomp = (
                        f' <span style="color:var(--muted)">·</span> '
                        f'<span style="color:var(--muted)">[pos </span>'
                        f'<span class="mono" style="color:{pe_c}">{pe:+.1f}</span>'
                        f'<span style="color:var(--muted)"> / marg </span>'
                        f'<span class="mono" style="color:{me_c}">{me:+.1f}</span>'
                        f'<span style="color:var(--muted)">]</span>'
                    )
                # Override flag (engine artifact corrected — IDKA bench primitive, etc)
                override_tag = (
                    ' <span class="mono" style="color:#eab308" title="Override aplicado em primitive do passivo">⚠ override</span>'
                    if vc.get("override") else ""
                )
                items += (
                    f'<li style="border-left:2px solid var(--border);padding-left:6px">'
                    f'<span style="color:var(--muted)">{metric_lbl} </span>'
                    f'<span class="mono" style="color:{delta_c};font-weight:700">{delta:+.1f} bps</span>'
                    f'<span style="color:var(--muted)"> vs D-1 · driver: </span>'
                    f'<span class="mono">{vc["driver"]}</span>'
                    f' <span class="mono" style="color:{dd_c}">({dd:+.1f} bps)</span>'
                    + decomp + override_tag +
                    f'</li>'
                )
            # ── PA outlier bullets ────────────────────────────────────────────
            if sub is not None and not sub.empty:
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
            if not items:
                body_chunks.append(
                    f'<div class="comment-fund">'
                    f'<div class="comment-title">{label}</div>'
                    f'<div class="comment-empty">— sem eventos significativos</div>'
                    f'</div>'
                )
            else:
                body_chunks.append(
                    f'<div class="comment-fund">'
                    f'<div class="comment-title">{label}</div>'
                    f'<ul class="comment-list">{items}</ul>'
                    f'</div>'
                )
        return f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Comments — Outliers do dia</span>
                <span class="card-sub">— produtos com |z| ≥ 2σ (vs. 90d) e |contrib| ≥ 3 bps · VaR Δ ≥ {_VAR_DELTA_MIN_BPS:.0f} bps (BVaR IDKAs ≥ 2 bps) vs D-1</span>
              </div>
              <div class="comments-grid">{''.join(body_chunks)}</div>
            </section>"""
    except Exception as e:
        print(f"  comments block failed ({e})")
        return ""


# ── Factor Breakdown ─────────────────────────────────────────────────────────

_FACTOR_LIST = [
    "Juros Reais (IPCA)", "Juros Reais (IGPM)", "Juros Nominais",
    "IPCA Idx", "IGPM Idx",
    "Equity BR", "Equity DM", "Equity EM", "FX", "Commodities",
]

_FACTOR_UNIT = {
    "Juros Reais (IPCA)": "yrs",
    "Juros Reais (IGPM)": "yrs",
    "Juros Nominais":     "yrs",
    "IPCA Idx":           "pct",
    "IGPM Idx":           "pct",
    "Equity BR":          "pct",
    "Equity DM":          "pct",
    "Equity EM":          "pct",
    "FX":                 "pct",
    "Commodities":        "pct",
}


def _render_factor_rows(
    factor_matrix: dict,
    bench_matrix: dict,
    nav_by_short: dict,
    house_nav_tot: float,
    net_of_bench: bool,
) -> str:
    rows = ""
    for factor_key in _FACTOR_LIST:
        allocations = factor_matrix.get(factor_key, {})
        benches     = bench_matrix.get(factor_key, {}) if net_of_bench else {}
        shorts_with_data = set(allocations.keys()) | set(benches.keys())
        if not shorts_with_data:
            continue
        unit = _FACTOR_UNIT.get(factor_key, "pct")
        # DV01 convention: long bond = negative. factor_matrix/bench_matrix already
        # stored in DV01 conv — no flip needed here.
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


def build_factor_breakdown_card(factor_matrix: dict, bench_matrix: dict, nav_by_short: dict) -> str:
    fund_col_headers = "".join(
        f'<th style="text-align:right">{FUND_LABELS.get(f, f)}</th>' for f in FUND_ORDER
    )
    house_nav_tot       = sum(v for v in nav_by_short.values() if v)
    factor_rows_liquido = _render_factor_rows(factor_matrix, bench_matrix, nav_by_short, house_nav_tot, net_of_bench=True)
    factor_rows_bruto   = _render_factor_rows(factor_matrix, bench_matrix, nav_by_short, house_nav_tot, net_of_bench=False)
    return f"""
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


# ── Top Positions ────────────────────────────────────────────────────────────

def _mm(v: float) -> str:
    try:
        return _fmt_br_num(f"{v/1e6:,.1f}M")
    except Exception:
        return "—"


def _liquido_scale(
    fund_label: str,
    factor_label: str,
    bench_matrix: dict,
    fund_factor_gross: dict,
) -> float:
    short = next((s for s, lbl in FUND_LABELS.items() if lbl == fund_label), fund_label)
    bench = bench_matrix.get(factor_label, {}).get(short, 0.0)
    total = fund_factor_gross.get((fund_label, factor_label), 0.0)
    if abs(total) < 1e-6 or abs(bench) < 1e-6:
        return 1.0
    # bench_matrix usa convenção DV01 (long bond = negativo); agg_rows usa
    # ano_eq_brl (long bond = positivo). Usar |bench|/|total| para ser
    # agnóstico à convenção de sinal — ambos representam a mesma direção.
    # Residual Líquido > Bruto em posições opostas cross-fund é aceito.
    return max(0.0, min(1.0, 1.0 - abs(bench) / abs(total)))


def _render_top_rows(
    agg_rows: list,
    bench_matrix: dict,
    fund_factor_gross: dict,
    liquido: bool,
    mode_key: str,
) -> str:
    scaled = []
    for r in agg_rows:
        brl = r["brl"]
        if liquido:
            brl = brl * _liquido_scale(r["fund"], r["factor"], bench_matrix, fund_factor_gross)
        if abs(brl) < 1_000:
            continue
        rr = dict(r); rr["brl"] = brl
        scaled.append(rr)

    by_fi: dict = defaultdict(list)
    for r in scaled:
        by_fi[(r["factor"], r["product"])].append(r)

    instruments = []
    for (factor, product), rows in by_fi.items():
        total = sum(r["brl"] for r in rows)
        unit  = rows[0]["unit"]
        instruments.append({"factor": factor, "product": product, "total": total,
                            "unit": unit, "holders": rows})

    instruments.sort(key=lambda i: abs(i["total"]), reverse=True)
    instruments = instruments[:15]

    factor_totals: dict = defaultdict(float)
    factor_unit:   dict = {}
    for inst in instruments:
        factor_totals[inst["factor"]] += inst["total"]
        factor_unit[inst["factor"]]    = inst["unit"]
    factor_order = sorted(factor_totals.keys(), key=lambda f: abs(factor_totals[f]), reverse=True)

    html = ""
    for factor in factor_order:
        factor_insts = [i for i in instruments if i["factor"] == factor]
        factor_insts.sort(key=lambda i: abs(i["total"]), reverse=True)
        if not factor_insts:
            continue
        ftot     = factor_totals[factor]
        ftot_col = "var(--up)" if ftot >= 0 else "var(--down)"
        fpath    = f"{mode_key}|{factor}"
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
            col       = "var(--up)" if inst["total"] >= 0 else "var(--down)"
            ipath     = f"{mode_key}|{factor}|{inst['product']}"
            caret_html = '<span class="tp-caret">▶</span> ' if inst["holders"] else '  '
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
                pct  = (h["brl"] / inst["total"] * 100) if inst["total"] else 0
                html += (
                    f'<tr class="tp-row tp-lvl-2" data-tp-parent="{ipath}" style="display:none">'
                    f'<td class="sum-fund" style="padding-left:44px; font-size:11.5px; color:var(--muted)">{h["fund"]}</td>'
                    f'<td class="mono" style="color:var(--muted); font-size:10.5px" title="% do total do instrumento na casa (não do NAV do fundo)">{pct:+.0f}% do instrumento</td>'
                    f'<td class="mono" style="text-align:right; color:{hcol}; font-size:11.5px">{_mm(h["brl"])}</td>'
                    f'<td class="mono" style="color:var(--muted); font-size:10.5px">{h["unit"]}</td>'
                    "</tr>"
                )
    return html


def build_top_positions_card(agg_rows: list, bench_matrix: dict) -> str:
    fund_factor_gross: dict = {}
    for r in agg_rows:
        key = (r["fund"], r["factor"])
        fund_factor_gross[key] = fund_factor_gross.get(key, 0.0) + r["brl"]

    top_rows_liquido = _render_top_rows(agg_rows, bench_matrix, fund_factor_gross, liquido=True,  mode_key="liq")
    top_rows_bruto   = _render_top_rows(agg_rows, bench_matrix, fund_factor_gross, liquido=False, mode_key="bru")
    return f"""
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
          <th style="text-align:left" title="No nível Fundo: % do instrumento alocado naquele fundo">Fundo · share do instrumento</th>
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


# ── VaR / BVaR per-fund table ─────────────────────────────────────────────────

def build_var_bvar_card(house_rows: list) -> str:
    if not house_rows:
        return ""
    top5_abs = set(r["short"] for r in sorted(house_rows, key=lambda r: r["var_brl"],  reverse=True)[:5])
    top5_rel = set(r["short"] for r in sorted(house_rows, key=lambda r: r["bvar_brl"], reverse=True)[:5])

    def _delta_cell(today: float | None, d1: float | None) -> str:
        if d1 is None or today is None:
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        bps = (today - d1) * 100
        if abs(bps) < 0.5:
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        color = "var(--down)" if bps > 0 else "var(--up)"
        arrow = "▲" if bps > 0 else "▼"
        return f'<td class="mono" style="text-align:right; color:{color}">{arrow}{abs(bps):.0f}bp</td>'

    rows_html = ""
    for r in house_rows:
        rank_abs = ' <span style="color:#facc15;font-size:11px">★</span>' if r["short"] in top5_abs else ""
        rank_rel = ' <span style="color:#60a5fa;font-size:11px">◆</span>' if r["short"] in top5_rel else ""
        rows_html += (
            f'<tr onclick="selectFund(\'{r["short"]}\')" style="cursor:pointer">'
            f'<td class="sum-fund">{r["label"]}</td>'
            f'<td class="mono" style="text-align:right; color:var(--muted)">{_mm(r["nav"])}</td>'
            f'<td class="mono" style="text-align:right; font-weight:600">{r["var_pct"]:.2f}%{rank_abs}</td>'
            + _delta_cell(r["var_pct"], r.get("var_pct_d1"))
            + f'<td class="mono" style="text-align:right; font-weight:600">{r["bvar_pct"]:.2f}%{rank_rel}</td>'
            + _delta_cell(r["bvar_pct"], r.get("bvar_pct_d1"))
            + f'<td class="mono" style="text-align:center; color:var(--muted)">{r["bench"]}</td>'
            + "</tr>"
        )
    tot_nav      = sum(r["nav"]      for r in house_rows)
    tot_var_brl  = sum(r["var_brl"]  for r in house_rows)
    tot_bvar_brl = sum(r["bvar_brl"] for r in house_rows)
    tot_var_pct  = (tot_var_brl  / tot_nav * 100) if tot_nav else 0.0
    tot_bvar_pct = (tot_bvar_brl / tot_nav * 100) if tot_nav else 0.0
    _dash = '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
    total_row = (
        '<tr class="house-total-row">'
        '<td class="sum-fund" style="font-weight:700" title="VaR/BVaR % são médias ponderadas por NAV (não-diversificado). NAV/PnL são somas.">Total NAV-pond. (não-div.)</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{_mm(tot_nav)}</td>'
        f'<td class="mono" style="text-align:right; font-weight:700">{tot_var_pct:.2f}%</td>'
        + _dash
        + f'<td class="mono" style="text-align:right; font-weight:700">{tot_bvar_pct:.2f}%</td>'
        + _dash
        + '<td></td>'
        '</tr>'
    )
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Risco VaR e BVaR por fundo</span>
        <span class="card-sub">— {DATA_STR} · VaR 95% 1d absoluto e vs. benchmark · top-5 destacados (<span style="color:#facc15">★</span> absoluto · <span style="color:#60a5fa">◆</span> relativo)</span>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fundo</th>
          <th style="text-align:right">NAV</th>
          <th style="text-align:right"><span class="kc">VaR</span></th>
          <th style="text-align:right">Δ D-1</th>
          <th style="text-align:right"><span class="kc">BVaR</span></th>
          <th style="text-align:right">Δ D-1</th>
          <th style="text-align:center">Bench</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
        <tfoot>{total_row}</tfoot>
      </table>
      <div class="bar-legend" style="margin-top:10px">
        <span style="color:#facc15">★</span> top-5 risco absoluto (R$) &nbsp;·&nbsp;
        <span style="color:#60a5fa">◆</span> top-5 risco ativo vs. benchmark (R$) &nbsp;·&nbsp;
        <b>Δ D-1</b>: variação do VaR/BVaR vs. dia anterior em bps (▲ = risco subiu · ▼ = risco caiu) &nbsp;·&nbsp;
        <span style="color:var(--muted)">BVaR vs CDI ≈ VaR abs; Frontier usa HS BVaR vs. IBOV; IDKAs usam BVaR paramétrico. Total = soma ponderada por NAV.</span>
      </div>
    </section>"""


# ── Status Grid ───────────────────────────────────────────────────────────────

def build_status_grid(summary_rows_html: str, bench_rows_html: str) -> str:
    return f"""
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

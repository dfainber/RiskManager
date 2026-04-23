"""
summary_renderers.py — cross-fund summary cards for the Summary view.

Covers card-level HTML blobs that appear in the Summary tab and are
independent of any single fund:

  build_vol_regime_card     Vol Regime table (MACRO / QUANT / EVOLUTION)
  build_movers_card         Top Movers DIA (por Livro / por Classe)
  build_changes_card        Significant position changes D-0 vs D-1
  build_comments_card       PA outlier comments by fund
"""
from __future__ import annotations

import pandas as pd

from risk_runtime import DATA_STR
from risk_config import FUND_ORDER, FUND_LABELS, _FUND_PA_KEY
from metrics import compute_pa_outliers
from pa_renderers import _pa_render_name


# ── Vol Regime ───────────────────────────────────────────────────────────────

_FUND_PORTFOLIO_KEY = {
    "MACRO":     "MACRO",
    "EVOLUTION": "EVOLUTION",
    "QUANT":     "SIST",
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


def _movers_rows(df_pa: pd.DataFrame, group_col: str) -> str:
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
    livro_rows  = _movers_rows(df_pa, "LIVRO")
    classe_rows = _movers_rows(df_pa, "CLASSE")
    if not livro_rows and not classe_rows:
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
        </div>
      </div>
      {_movers_table("livro",  livro_rows,  True)}
      {_movers_table("classe", classe_rows, False)}
    </section>"""


# ── Position Changes ─────────────────────────────────────────────────────────

_CHANGES_THRESHOLD_PP = 0.30


def _delta_pp(v: float) -> str:
    color = "var(--up)" if v > 0 else "var(--down)"
    sign = "+" if v > 0 else ""
    return f'<span style="color:{color}; font-weight:700">{sign}{v:.2f} pp</span>'


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
            <span class="card-sub">— exposição D-0 vs D-1 · |Δ| ≥ {_CHANGES_THRESHOLD_PP:.2f} pp · MACRO por PM×fator, outros por fator agregado</span>
          </div>
          <div class="comments-grid">{''.join(change_blocks)}</div>
        </section>"""


# ── PA Outlier Comments ──────────────────────────────────────────────────────

def build_comments_card(df_pa_daily: pd.DataFrame) -> str:
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
        return f"""
            <section class="card">
              <div class="card-head">
                <span class="card-title">Comments — Outliers do dia</span>
                <span class="card-sub">— produtos com |z| ≥ 2σ (vs. 90d) e |contrib| ≥ 3 bps · ignora Caixa/Custos</span>
              </div>
              <div class="comments-grid">{''.join(body_chunks)}</div>
            </section>"""
    except Exception as e:
        print(f"  comments block failed ({e})")
        return ""

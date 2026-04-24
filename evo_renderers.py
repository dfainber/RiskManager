"""
evo_renderers.py — EVOLUTION diversification card renderers.

Render the 4 layers + directional matrix of the concentration/diversification
analysis for the Galapagos Evolution FIC FIM CP fund. Compute engine itself
lives in evolution_diversification_card.py; this module only consumes its
outputs and produces the HTML.

Consumers call `build_evolution_diversification_section(date_str)` which
returns (fragment_html, c4_state). c4_state is propagated to the Summary
page to decide whether to show the headline alert.
"""
from __future__ import annotations

import pandas as pd

from risk_config import _EVO_EXPECTED_STRATS
from risk_runtime import fmt_br_num as _fmt_br_num
from svg_renderers import evo_spark_svg as _evo_spark_svg
from evolution_diversification_card import (
    build_ratio_series          as _evo_build_ratio_series,
    compute_camada1             as _evo_compute_camada1,
    compute_camada3             as _evo_compute_camada3,
    fetch_direction_report      as _evo_fetch_direction_report,
    fetch_direction_report_history as _evo_fetch_direction_report_history,
    _compute_magnitude_p60_per_cat as _evo_compute_magnitude_p60,
    compute_camada_direcional   as _evo_compute_camada_direcional,
    compute_camada4             as _evo_compute_camada4,
)


def _evo_pct_color(pct: float) -> str:
    """Cor para percentil 'high=bad' usando CSS vars do tema."""
    if pd.isna(pct):        return "var(--muted)"
    if pct < 60:            return "var(--up)"
    if pct < 70:            return "var(--warn)"
    return "var(--down)"


def _evo_pct_badge(pct: float) -> str:
    if pd.isna(pct):   return "—"
    if pct < 60:       return "🟢"
    if pct < 70:       return "🟡"
    return "🔴"


def _evo_render_camada1(rows: list) -> str:
    # Excluir CAIXA, OUTROS e CREDITO (CAIXA e OUTROS são ruidosos; CREDITO tem
    # caveat de cotas júnior — análise direcional se concentra em MACRO/SIST/
    # FRONTIER/EVO_STRAT).
    _EXCLUDED_C1 = {"CAIXA", "OUTROS", "CREDITO"}
    rows = [r for r in rows if r["strat"] not in _EXCLUDED_C1]
    # Score: red (≥P70) = 1.0 · yellow (P60–69) = 0.5 · green (<P60) = 0
    score = sum(1.0 if (not pd.isna(r["pct"]) and r["pct"] >= 70) else
                0.5 if (not pd.isna(r["pct"]) and r["pct"] >= 60) else 0.0
                for r in rows)
    elevated = [r for r in rows if not pd.isna(r["pct"]) and r["pct"] >= 60]
    tr_html = ""
    for r in rows:
        c     = _evo_pct_color(r["pct"])
        vstr  = "—" if pd.isna(r["var_today"]) else _fmt_br_num(f"{r['var_today']:,.1f}")
        pstr  = "—" if pd.isna(r["pct"])       else f"P{r['pct']:.0f}"
        badge = _evo_pct_badge(r["pct"])
        tr_html += (
            f"<tr>"
            f"<td>{r['strat']}</td>"
            f"<td class='mono' style='text-align:right'>{vstr}</td>"
            f"<td class='mono' style='text-align:right;color:{c};"
            f"font-weight:600'>{pstr}</td>"
            f"<td style='text-align:center'>{badge}</td>"
            f"</tr>"
        )
    alert = ""
    if score >= 3:
        names = ", ".join(r["strat"] for r in elevated)
        score_detail = f"score={score:.1f}"
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(245,196,81,0.12);border-left:3px solid var(--warn);"
                 f"border-radius:4px;font-size:13px;color:var(--text)'>"
                 f"⚠️ Alerta C1 ({score_detail}) — {names} — sinal de carregamento agregado</div>")
    elif score >= 1:
        names = ", ".join(r["strat"] for r in elevated)
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(245,196,81,0.06);border-left:3px solid var(--muted);"
                 f"border-radius:4px;font-size:12px;color:var(--muted)'>"
                 f"score={score:.1f} · {names}</div>")
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Camada 1 — Utilização histórica por estratégia</span>
        <span class="card-sub">VaR em bps · percentil 252d próprio · 🟢&lt;P60 · 🟡 P60–69 (0.5 pt) · 🔴≥P70 (1 pt) · alerta score≥3</span>
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Estratégia</th>
          <th style="text-align:right"><span class="kc">VaR</span> (bps)</th>
          <th style="text-align:right">Percentil 252d</th>
          <th style="text-align:center">Estado</th>
        </tr></thead>
        <tbody>{tr_html}</tbody>
      </table>
      {alert}
      <div class="bar-legend" style="margin-top:10px">
        cada estratégia vs. próprio histórico 252d ·
        score ≥ 3 = carregamento agregado (🔴=1 pt, 🟡=0.5 pt) ·
        CAIXA / OUTROS / CREDITO excluídos
      </div>
    </section>"""


def _evo_render_camada2(d: dict) -> str:
    state_color = _evo_pct_color(d["ratio_wins_pct"])
    state_label = ("diversificação saudável" if d["ratio_wins_pct"] < 60 else
                   "abaixo da média histórica" if d["ratio_wins_pct"] < 80 else
                   "estratégias alinhadas" if d["ratio_wins_pct"] < 95 else
                   "sem diversificação efetiva")

    # Strategy breakdown rows — CREDITO omitido da tabela (caveat cotas júnior).
    # A Σ abaixo continua exibindo raw e winsorizada (que inclui CREDITO clipado).
    rows = []
    for s in _EVO_EXPECTED_STRATS:
        if s == "CREDITO":
            continue
        v = d["strat_today_bps"].get(s)
        rows.append((s, float(v) if v is not None else None))
    if "OUTROS" in d["strat_today_bps"]:
        rows.append(("OUTROS", float(d["strat_today_bps"]["OUTROS"])))

    strat_rows_html = ""
    for s, v in rows:
        vstr  = "—" if v is None else _fmt_br_num(f"{v:,.1f}")
        share = "—" if (v is None or d["var_soma_bps"] == 0) \
                     else f"{v/d['var_soma_bps']*100:.0f}%"
        strat_rows_html += (
            f"<tr><td>{s}</td>"
            f"<td class='mono' style='text-align:right'>{vstr}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>{share}</td></tr>"
        )
    _soma_raw_s  = _fmt_br_num(f"{d['var_soma_bps']:,.1f}")
    strat_rows_html += (
        "<tr style='border-top:1.5px solid var(--line-2);font-weight:600'>"
        f"<td>Σ VaR estratégias <span style='color:var(--muted);font-weight:400'>(raw)</span></td>"
        f"<td class='mono' style='text-align:right'>{_soma_raw_s}</td>"
        f"<td class='mono' style='text-align:right;color:var(--muted)'>100%</td></tr>"
    )
    if abs(d['var_soma_bps'] - d['var_soma_wins_bps']) > 0.01:
        _soma_wins_s = _fmt_br_num(f"{d['var_soma_wins_bps']:,.1f}")
        strat_rows_html += (
            "<tr style='font-weight:700'>"
            f"<td>Σ VaR estratégias <span style='color:var(--up);font-weight:400'>(winsorizado)</span></td>"
            f"<td class='mono' style='text-align:right;color:var(--up)'>{_soma_wins_s}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>—</td></tr>"
        )

    saving_pct = (1.0 - d["ratio_wins"]) * 100
    cr_share    = d["credito_share_raw"]  * 100 if not pd.isna(d["credito_share_raw"])  else float("nan")
    cr_share_w  = d["credito_share_wins"] * 100 if not pd.isna(d["credito_share_wins"]) else float("nan")
    cr_color = ("var(--down)" if cr_share > 40
                else "var(--warn)" if cr_share > 25
                else "var(--muted)")

    n_wins = len(d["credito_wins_dates"])
    wins_note = ""
    if n_wins > 0:
        recent_wins = [pd.Timestamp(x).strftime("%Y-%m-%d")
                       for x in d["credito_wins_dates"][-3:]]
        wins_note = (
            f"<div style='margin-top:10px;padding:8px 12px;"
            f"background:rgba(245,196,81,0.12);border-left:3px solid var(--warn);"
            f"border-radius:4px;font-size:12px;color:var(--text);line-height:1.5'>"
            f"⚠️ CREDITO clipado em <b>{n_wins}</b> dias na janela 252d "
            f"(spike de cotas júnior). Últimos: {', '.join(recent_wins)}. "
            f"Ratio principal usa Σ winsorizado.</div>"
        )

    spark = _evo_spark_svg(d["ratio_wins_series"], d["ratio_wins"])

    pct_fmt    = "—" if pd.isna(d['ratio_wins_pct']) else f"{d['ratio_wins_pct']:.0f}"
    raw_pctfmt = "—" if pd.isna(d['ratio_pct'])      else f"{d['ratio_pct']:.0f}"

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Camada 2 — Diversification Benefit</span>
        <span class="card-sub">CREDITO winsorizado (63d MAD, 3σ, causal) · ratio principal = Σ winsorizado</span>
      </div>

      <div style="display:grid;grid-template-columns:1.2fr 1fr;gap:24px;align-items:start">
        <div>
          <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                      letter-spacing:.5px;margin-bottom:6px">
            VaR por estratégia (bps) — soma linear, corr=1
          </div>
          <table class="summary-table">
            <tbody>{strat_rows_html}</tbody>
          </table>
        </div>

        <div>
          <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                      letter-spacing:.5px;margin-bottom:6px">
            Ratio = VaR_real / Σ VaR_estratégias
          </div>
          <div style="font-size:44px;font-weight:700;line-height:1;
                      color:{state_color};font-family:'JetBrains Mono',monospace">
            {d['ratio_wins']:.2f}
          </div>
          <div style="color:{state_color};font-size:13px;margin-top:4px;font-weight:600">
            {state_label} · P{pct_fmt}
          </div>
          <div style="color:var(--muted);font-size:11px;margin-top:2px">
            (raw sem winsor.: {d['ratio']:.2f} / P{raw_pctfmt})
          </div>

          <div style="margin-top:16px;font-size:13px;line-height:1.7">
            <div><b>VaR real (fundo):</b>
              <span class="mono">{_fmt_br_num(f"{d['var_real_bps']:,.1f}")} bps</span></div>
            <div><b>Σ estratégias (winsor.):</b>
              <span class="mono">{_fmt_br_num(f"{d['var_soma_wins_bps']:,.1f}")} bps</span></div>
            <div><b>Benefício:</b>
              <span class="mono">
                −{_fmt_br_num(f"{d['var_soma_wins_bps'] - d['var_real_bps']:,.1f}")} bps
                ({saving_pct:.0f}% redução)
              </span></div>
            <div style="margin-top:6px"><b>Share CREDITO na Σ:</b>
              <span class="mono" style="color:{cr_color};font-weight:600">{cr_share:.0f}%</span>
              <span style="color:var(--muted)">raw</span> ·
              <span class="mono" style="color:var(--muted)">{cr_share_w:.0f}%</span>
              <span style="color:var(--muted)">winsor.</span>
            </div>
            <div style="margin-top:6px"><b>Percentil 252d:</b>
              <span class="mono">P{pct_fmt}</span>
              · média {d['ratio_wins_mean']:.2f}
              · range [{d['ratio_wins_min']:.2f}, {d['ratio_wins_max']:.2f}]
            </div>
          </div>
        </div>
      </div>

      {wins_note}

      <div style="margin-top:20px">
        <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                    letter-spacing:.5px;margin-bottom:4px">
          Ratio histórico (winsorizado) — últimos 252d · traço = média · dot = hoje
        </div>
        {spark}
      </div>

      <div class="bar-legend" style="margin-top:16px">
        ratio baixo → diversificação reduzindo VaR linear · acima do P80 = alinhamento atípico ·
        tratamento CREDITO: <a href="../../docs/CREDITO_TREATMENT.md" style="color:var(--accent-2)">docs/CREDITO_TREATMENT.md</a>
      </div>
    </section>"""


def _evo_render_camada3(c3: dict, c1_rows: list | None = None) -> str:
    if c3["corr_63d"] is None or not c3["pairs"]:
        return """
        <section class="card">
          <div class="card-head">
            <span class="card-title">Camada 3 — Correlação realizada</span>
            <span class="card-sub">dados insuficientes de PnL por LIVRO</span>
          </div>
        </section>"""

    # Significance filter: a pair only counts as "aligned" when BOTH strategies
    # are simultaneously ≥ P70 in Camada 1 *and* the pair's 63d correlation is
    # ≥ P85. This filters out high correlation between strategies that happen
    # to be small/idle (where correlation is mathematically unstable and
    # economically meaningless).
    c1_pct = {r["strat"]: r["pct"] for r in (c1_rows or [])
              if r.get("pct") is not None and not pd.isna(r.get("pct"))}

    corr = c3["corr_63d"]
    cols = list(corr.columns)

    rows_html = ""
    for i, a in enumerate(cols):
        cells = f"<td style='padding:6px 8px;font-weight:600'>{a}</td>"
        for j, b in enumerate(cols):
            if j < i:
                cells += "<td style='padding:6px 8px;color:var(--line-2)'>·</td>"
            elif j == i:
                cells += "<td style='padding:6px 8px;color:var(--line-2)'>—</td>"
            else:
                pair = next((p for p in c3["pairs"]
                             if p["a"] == a and p["b"] == b), None)
                if pair is None:
                    cells += "<td>—</td>"
                else:
                    color = _evo_pct_color(pair["c63_pct"])
                    pstr = "—" if pd.isna(pair['c63_pct']) else f"P{pair['c63_pct']:.0f}"
                    cells += (
                        f"<td style='padding:6px 8px;text-align:center' class='mono'>"
                        f"<span style='font-weight:600;color:{color}'>{pair['c63']:+.2f}</span>"
                        f"<br><span style='font-size:11px;color:var(--muted)'>{pstr}</span>"
                        f"</td>"
                    )
        rows_html += f"<tr>{cells}</tr>"

    header_cells = (
        "<th></th>"
        + "".join(f"<th style='color:var(--muted)'>{c}</th>" for c in cols)
    )

    pairs_sorted = sorted(c3["pairs"], key=lambda p: -abs(p["c63"]))
    pair_rows = ""
    for p in pairs_sorted:
        color = _evo_pct_color(p["c63_pct"])
        pstr = "—" if pd.isna(p["c63_pct"]) else f"P{p['c63_pct']:.0f}"
        pair_rows += (
            f"<tr>"
            f"<td>{p['a']} × {p['b']}</td>"
            f"<td class='mono' style='text-align:right'>{p['c21']:+.2f}</td>"
            f"<td class='mono' style='text-align:right;color:{color};font-weight:600'>{p['c63']:+.2f}</td>"
            f"<td class='mono' style='text-align:right;color:{color}'>{pstr}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>{p['c63_mean']:+.2f}</td>"
            f"<td class='mono' style='text-align:right;color:var(--muted)'>[{p['c63_min']:+.2f}, {p['c63_max']:+.2f}]</td>"
            f"</tr>"
        )

    # Raw flagged = any pair with c63_pct ≥ 85
    # Significant flagged = raw AND both strategies ≥ P70 in Camada 1
    flagged_raw = [p for p in c3["pairs"]
                   if not pd.isna(p["c63_pct"]) and p["c63_pct"] >= 85]
    def _both_loaded(p):
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        return (pa is not None and pa >= 70 and pb is not None and pb >= 70)
    flagged_sig = [p for p in flagged_raw if _both_loaded(p)]
    flagged_quiet = [p for p in flagged_raw if not _both_loaded(p)]

    def _pair_str_with_c1(p):
        pa, pb = c1_pct.get(p["a"]), c1_pct.get(p["b"])
        sa = f"P{pa:.0f}" if pa is not None else "—"
        sb = f"P{pb:.0f}" if pb is not None else "—"
        return f"{p['a']}×{p['b']} (corr P{p['c63_pct']:.0f} · {p['a']} C1 {sa}, {p['b']} C1 {sb})"

    alert = ""
    if flagged_sig:
        names = ", ".join(_pair_str_with_c1(p) for p in flagged_sig)
        alert += (f"<div style='margin-top:10px;padding:8px 12px;"
                  f"background:rgba(255,90,106,0.12);border-left:3px solid var(--down);"
                  f"border-radius:4px;font-size:13px;color:var(--text)'>"
                  f"🚨 Alinhamento relevante (corr ≥ P85 · ambas estratégias C1 ≥ P70): {names}</div>")
    if flagged_quiet:
        names = ", ".join(_pair_str_with_c1(p) for p in flagged_quiet)
        alert += (f"<div style='margin-top:8px;padding:8px 12px;"
                  f"background:rgba(184,135,0,0.10);border-left:3px solid var(--warn);"
                  f"border-radius:4px;font-size:13px;color:var(--muted)'>"
                  f"🟡 Correlação alta mas estratégia(s) abaixo de C1 P70 (sinal desconsiderado): {names}</div>")
    if not flagged_raw:
        alert = ("<div style='margin-top:10px;padding:8px 12px;"
                 "background:rgba(74,222,128,0.08);border-left:3px solid var(--up);"
                 "border-radius:4px;font-size:13px;color:var(--muted)'>"
                 "✓ Nenhum par em alinhamento atípico (corr 63d &lt; P85).</div>")

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Camada 3 — Correlação realizada entre estratégias</span>
        <span class="card-sub">PnL diário · janela 63d · percentil 252d de rolling 63d · {c3['n_obs']} obs</span>
      </div>

      <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                  letter-spacing:.5px;margin-bottom:6px">
        Matriz 63d (percentil 252d abaixo)
      </div>
      <table class="summary-table" style="width:auto;margin-bottom:18px">
        <thead><tr>{header_cells}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>

      <div style="font-size:12px;color:var(--muted);text-transform:uppercase;
                  letter-spacing:.5px;margin-bottom:6px">
        Pares — 21d · 63d · P252d · média · range
      </div>
      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Par</th>
          <th style="text-align:right">21d</th>
          <th style="text-align:right">63d</th>
          <th style="text-align:right">P252d</th>
          <th style="text-align:right">média</th>
          <th style="text-align:right">range</th>
        </tr></thead>
        <tbody>{pair_rows}</tbody>
      </table>

      {alert}

      <div class="bar-legend" style="margin-top:16px">
        correlação positiva entre direcionais reduz diversificação ·
        P &gt; 85 = alinhamento atípico ·
        21d sensível · 63d baseline ·
        <b>filtro de significância</b>: par só conta como alinhamento relevante se <u>ambas</u>
        estratégias estão ≥ P70 na Camada 1 (caso contrário a correlação alta pode ser
        artefato de uma estratégia ociosa)
      </div>
    </section>"""


def _evo_render_camada_direcional(c_dir: dict) -> str:
    """Render da Camada Direcional — DELTA_SIST × DELTA_DISC por CATEGORIA.
       Mostra tabela com sinal de cada perna + badge do estado."""
    rows = c_dir.get("rows", [])
    if not rows:
        return ""

    state_badge = {
        "same-sign":  ("🟥 mesmo sinal",    "var(--down)"),
        "opposite":   ("🟩 sinais opostos", "var(--up)"),
        "only-sist":  ("⚪ só sistemático", "var(--muted)"),
        "only-disc":  ("⚪ só discricion.", "var(--muted)"),
        "dust":       ("— irrelevante",     "var(--line-2)"),
    }

    def _cell_bps(v, material=True):
        if abs(v) < 0.1:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        col = "var(--up)" if v >= 0 else "var(--down)"
        sign = "+" if v >= 0 else ""
        opacity = "" if material else "opacity:0.6;"
        return f'<td class="mono" style="text-align:right;color:{col};{opacity}">{sign}{v:.0f}</td>'

    def _cell_pct(v, material=True):
        vp = v * 100  # fraction → %
        col = "var(--up)" if vp >= 0 else "var(--down)"
        sign = "+" if vp >= 0 else ""
        weight = "font-weight:700;" if material else ""
        return f'<td class="mono" style="text-align:right;color:{col};{weight}">{sign}{vp:.1f}%</td>'

    body = ""
    for r in rows:
        label, color = state_badge.get(r["state"], ("—", "var(--muted)"))
        dim_parts = []
        if not r["material"]:
            dim_parts.append("abaixo de 1% PL")
        if r["state"] == "same-sign" and not r.get("mag_passes", True):
            dim_parts.append("magnitude < P60 hist")
        dim = f" ({'; '.join(dim_parts)})" if dim_parts else ""
        cat_style = "color:var(--text)" if r["material"] and r.get("mag_passes", True) else "color:var(--muted)"
        # State label: degrade same-sign display if mag_passes fails
        if r["state"] == "same-sign" and not r.get("mag_passes", True):
            label = "🟡 nominal (mag. baixa)"
            color = "var(--warn)"
        body += (
            f'<tr>'
            f'<td style="{cat_style};padding:5px 4px"><b>{r["categoria"]}</b> '
            f'<span style="color:var(--muted);font-size:11px">· {r["tipo"]}{dim}</span></td>'
            + _cell_bps(r["sist_bps"], r["material"])
            + _cell_bps(r["disc_bps"], r["material"])
            + _cell_pct(r["pct_pl"],  r["material"])
            + f'<td style="text-align:center;padding:5px 4px;color:{color};font-size:12px">{label}</td>'
            + '</tr>'
        )

    same_cnt = c_dir.get("same_sign_count", 0)
    same_cats = c_dir.get("same_sign_categorias", [])
    nominal_cats = c_dir.get("same_sign_nominal_only", [])
    th = c_dir.get("thresholds", {})

    if same_cnt >= 3:
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(255,90,106,0.12);border-left:3px solid var(--down);"
                 f"border-radius:4px;font-size:13px;color:var(--text)'>"
                 f"🚨 <b>{same_cnt} categorias</b> com sistemática e discricionária "
                 f"alinhadas no mesmo sinal (e magnitude conjunta ≥ P60 hist): {', '.join(same_cats)}</div>")
    elif same_cnt >= 1:
        alert = (f"<div style='margin-top:10px;padding:8px 12px;"
                 f"background:rgba(184,135,0,0.10);border-left:3px solid var(--warn);"
                 f"border-radius:4px;font-size:13px;color:var(--text)'>"
                 f"🟡 {same_cnt} categoria(s) com mesmo sinal relevante: {', '.join(same_cats)} "
                 f"(abaixo do gatilho de ≥3 da Camada 4)</div>")
    else:
        alert = ("<div style='margin-top:10px;padding:8px 12px;"
                 "background:rgba(74,222,128,0.08);border-left:3px solid var(--up);"
                 "border-radius:4px;font-size:13px;color:var(--muted)'>"
                 "✓ Nenhuma categoria com alinhamento relevante (same-sign + magnitude ≥ P60 hist).</div>")
    # Aux info: nominal-only alignments (sinal bate mas mag < P60)
    if nominal_cats:
        alert += (f"<div style='margin-top:6px;padding:6px 12px;"
                  f"background:rgba(184,135,0,0.06);border-left:2px solid var(--line-2);"
                  f"border-radius:4px;font-size:12px;color:var(--muted)'>"
                  f"Alinhamento nominal (desconsiderado por magnitude &lt; P60 histórico): "
                  f"{', '.join(nominal_cats)}</div>")

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Matriz Direcional — SIST × DISC por categoria</span>
        <span class="card-sub">DELTA_SIST × DELTA_DISC · fonte <code>RISK_DIRECTION_REPORT</code> ·
        filtros: cada perna ≥ {th.get('min_leg_bps', 5):.0f} bps, categoria ≥ {th.get('min_cat_pct', 1):.0f}% PL,
        magnitude conjunta (|sist|+|disc|) ≥ P{th.get('mag_pct', 60):.0f} histórico 252d</span>
      </div>

      <table class="summary-table">
        <thead><tr>
          <th style="text-align:left">Categoria</th>
          <th style="text-align:right">Sist (bps)</th>
          <th style="text-align:right">Disc (bps)</th>
          <th style="text-align:right">% PL</th>
          <th style="text-align:center">Estado</th>
        </tr></thead>
        <tbody>{body}</tbody>
      </table>

      {alert}

      <div class="bar-legend" style="margin-top:14px">
        a <b>smoking gun</b> do alinhamento: se sistemática e discricionária estão long/short
        na mesma classe de ativo, a descorrelação esperada entre as duas metades do fundo desaparece.
        ≥3 categorias com mesmo sinal <b>E magnitude conjunta historicamente relevante (≥ P60)</b>
        → condição 5 da Camada 4 (bull market alignment). "Sinais opostos" (🟩) é o estado saudável.
        <b>Por que o filtro P60:</b> antes, uma categoria contava como same-sign mesmo com SIST tiny
        (+3 bps) e DISC carregando tudo — alinhamento nominal, não real. Agora exige que o tamanho
        conjunto esteja acima do seu próprio histórico recente.
      </div>
    </section>"""


def _evo_render_camada4_alert(c4: dict) -> str:
    """Caixa de alerta Camada 4 — exibida no TOPO do tab Diversificação do Evolution.
       Mostra quantas condições acenderam e detalhe de cada uma."""
    if not c4:
        return ""
    n_lit = c4.get("n_lit", 0)
    alert_fired = c4.get("alert", False)
    buckets = c4.get("buckets_pct", {})

    # Bucket summary bar
    bucket_html = " · ".join(
        f'<b>{k}</b> P{v:.0f}' for k, v in buckets.items()
    )

    cond_rows = ""
    for c in c4.get("conditions", []):
        mark  = "🔴" if c["lit"] else "⚪"
        label = f"<b>{c['name']}</b>"
        if c["id"] == 1 and c["detail"]:
            det = ", ".join(f"{k} P{v:.0f}" for k, v in c["detail"].items())
            det_html = f" · {det}" if det else ""
        elif c["id"] == 2 and c["detail"]:
            det = ", ".join(f"{k} P{v:.0f}" for k, v in c["detail"].items())
            det_html = f" · {det}" if det else ""
        elif c["id"] == 3 and c["detail"] is not None and not pd.isna(c["detail"]):
            det_html = f" · ratio em P{c['detail']:.0f}"
        elif c["id"] == 4 and c["detail"]:
            det = ", ".join(f"{p['a']}×{p['b']} P{p['c63_pct']:.0f}" for p in c["detail"])
            det_html = f" · {det}"
        elif c["id"] == 5 and c["detail"]:
            det_html = f" · {', '.join(c['detail'])}"
        else:
            det_html = ""
        cond_rows += (
            f'<li style="margin:4px 0;color:'
            f'{"var(--text)" if c["lit"] else "var(--muted)"}">{mark} {label}{det_html}</li>'
        )

    if alert_fired:
        title = "🚨 BULL MARKET ALIGNMENT — alerta disparado"
        subtitle = f"{n_lit} de 5 condições acesas (gatilho ≥ 3)"
        bg = "rgba(255,90,106,0.14)"; border = "var(--down)"
    elif n_lit >= 1:
        title = "🟡 Alinhamento parcial"
        subtitle = f"{n_lit} de 5 condições acesas (gatilho em 3)"
        bg = "rgba(184,135,0,0.10)"; border = "var(--warn)"
    else:
        title = "✓ Sem sinais de bull market alignment"
        subtitle = "0 de 5 condições acesas"
        bg = "rgba(74,222,128,0.06)"; border = "var(--up)"

    return f"""
    <section class="card" style="background:{bg};border-left:4px solid {border}">
      <div class="card-head">
        <span class="card-title">Camada 4 — {title}</span>
        <span class="card-sub">{subtitle}</span>
      </div>
      <div style="padding:4px 8px 8px;font-size:12px;color:var(--muted)">
        <b>Buckets direcionais:</b> {bucket_html or '—'}
      </div>
      <ul style="margin:0;padding:8px 24px 14px;font-size:13px;line-height:1.7;list-style:none">
        {cond_rows}
      </ul>
      <div class="bar-legend" style="margin-top:4px;padding:0 14px 12px">
        "Bull market alignment" = cenário onde as estratégias do Evolution ficam direcionais
        na mesma direção, a diversificação esperada desaparece, e o risco efetivo do fundo
        fica maior que o VaR linear sugere. Buckets unidos: FRONTIER+EVO_STRAT (pacote tático).
      </div>
    </section>"""


def build_evolution_diversification_section(date_str: str) -> tuple[str, dict]:
    """Retorna (fragmento HTML, c4_state).
       c4_state é usado pelo Summary pra headline quando o alerta acende."""
    try:
        d       = _evo_build_ratio_series(date_str)
        c1_rows = _evo_compute_camada1(d["strat_pivot"], d["effective_date"])
        c3      = _evo_compute_camada3(date_str, d["effective_date"])
        # Matriz direcional (nova)
        try:
            df_dir      = _evo_fetch_direction_report(date_str)
            df_dir_hist = _evo_fetch_direction_report_history(date_str, lookback_days=400)
            mag_p60     = _evo_compute_magnitude_p60(df_dir_hist, pct_threshold=60.0, lookback=252)
            c_dir       = _evo_compute_camada_direcional(df_dir, d["nav"],
                                                          mag_p60_by_cat=mag_p60)
        except Exception as ee:
            print(f"  [Evolution] direction report fetch failed: {ee}")
            c_dir = {"rows": [], "same_sign_count": 0, "same_sign_categorias": [],
                     "thresholds": {"min_leg_bps": 5, "min_cat_pct": 1}}
        # Camada 4 (agregada)
        c4 = _evo_compute_camada4(c1_rows, d, c3, c_dir,
                                  d["strat_pivot"], d["effective_date"])
    except Exception as e:
        err_html = (f"<section class='card'><div class='card-head'>"
                    f"<span class='card-title'>Diversificação</span></div>"
                    f"<div style='color:var(--muted);padding:12px'>"
                    f"Sem dados para {date_str}: {e}</div></section>")
        return err_html, {}

    # Unified diversification card: Camada 4 alert (summary) no topo,
    # depois 1 card com sub-tabs pra C1 / C2 / C3 / Matriz Direcional.
    c1_html  = _evo_render_camada1(c1_rows)
    c2_html  = _evo_render_camada2(d)
    c3_html  = _evo_render_camada3(c3, c1_rows)
    dir_html = _evo_render_camada_direcional(c_dir)

    c4_html  = _evo_render_camada4_alert(c4)

    unified = f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Diversificação — Evolution</span>
        <span class="card-sub">4 camadas complementares · abre em Resumo (Camada 4)</span>
        <div class="pa-view-toggle evo-div-toggle" style="margin-left:auto;flex-wrap:wrap">
          <button class="pa-tgl active" data-evo-div="resumo"
                  onclick="selectEvoDivView(this,'resumo')">Resumo</button>
          <button class="pa-tgl" data-evo-div="camada1"
                  onclick="selectEvoDivView(this,'camada1')">Camada 1 · Utilização</button>
          <button class="pa-tgl" data-evo-div="camada2"
                  onclick="selectEvoDivView(this,'camada2')">Camada 2 · Ratio</button>
          <button class="pa-tgl" data-evo-div="camada3"
                  onclick="selectEvoDivView(this,'camada3')">Camada 3 · Correlação</button>
          <button class="pa-tgl" data-evo-div="direcional"
                  onclick="selectEvoDivView(this,'direcional')">Matriz Direcional</button>
        </div>
      </div>
      <div class="evo-div-body">
        <div class="evo-div-view" data-evo-div="resumo">{c4_html}</div>
        <div class="evo-div-view" data-evo-div="camada1" style="display:none">{c1_html}</div>
        <div class="evo-div-view" data-evo-div="camada2" style="display:none">{c2_html}</div>
        <div class="evo-div-view" data-evo-div="camada3" style="display:none">{c3_html}</div>
        <div class="evo-div-view" data-evo-div="direcional" style="display:none">{dir_html}</div>
      </div>
    </section>"""

    return unified, c4

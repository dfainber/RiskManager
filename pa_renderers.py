"""
pa_renderers.py — Performance Attribution (PA) section renderers.

12 helpers that turn the PA leaves dataframe (q_models.REPORT_ALPHA_ATRIBUTION)
into a hierarchical HTML card with lazy-loaded descendants, heatmaps, filtering,
and an optional benchmark-decomposition view for IDKA funds.

Pure HTML rendering — no DB access. Consumers pass in pre-fetched dataframes.
"""
from __future__ import annotations

import json

import pandas as pd

from risk_runtime import fmt_br_num as _fmt_br_num
from risk_config import (
    _PA_LIVRO_RENAMES,
    _PA_ORDER_BY_LEVEL,
    _PA_PINNED_BOTTOM,
    _PA_AGG_LEN,
    _FUND_PA_KEY,
    _PA_BENCH_LIVROS,
)


def _pa_escape(v) -> str:
    """Sanitize a tree key for use inside data-path attributes."""
    s = str(v) if v is not None and str(v) != "nan" else "—"
    return s.replace("|", "¦").replace('"', "'")


def _pa_render_name(raw: str) -> str:
    """Map raw tree key to display label (e.g., Macro_JD → JD)."""
    return _PA_LIVRO_RENAMES.get(raw, raw)


def _evo_strategy(livro: str) -> str:
    """Bucket a LIVRO into a high-level strategy for the EVOLUTION Livro view."""
    if livro == "CI" or livro.startswith("Macro_"):
        return "Macro"
    if livro in {"Bracco", "Quant_PA", "HEDGE_SISTEMATICO"} or livro.startswith("SIST_"):
        return "Quant"
    if livro in {"FMN", "FCO", "FLO", "GIS FI FUNDO IMOBILIÁRIO", "RV BZ"}:
        return "Equities"
    if livro in {"Crédito", "CRÉDITO", "CredEstr"}:
        return "Crédito"
    return "Caixa & Custos"


def _pa_bp_cell(v: float, bold: bool = False, heat_max: float = 0.0) -> str:
    """
    Render a bps value as a percentage with 2 decimals (1 bp = 0.01%).
    If `heat_max` > 0, apply a subtle green/red background proportional to |v|/heat_max.
    """
    pct = v / 100.0
    if abs(pct) < 0.005:  # rounds to 0.00%
        return '<td class="t-num mono" style="color:var(--muted)">—</td>'
    color = "var(--up)" if v >= 0 else "var(--down)"
    weight = "font-weight:700;" if bold else ""
    bg = ""
    if heat_max and heat_max > 0:
        rgb = "38,208,124" if v >= 0 else "255,90,106"
        alpha = min(abs(v) / heat_max, 1.0) * 0.14  # subtle so the text stays legible
        bg = f" background:rgba({rgb},{alpha:.2f});"
    return f'<td class="t-num mono" style="color:{color};{weight}{bg}">{pct:+.2f}%</td>'


def _pa_pos_cell(v: float) -> str:
    """Position cell — BRL in millions, hide dust."""
    if abs(v) < 1e5:  # <100k reais
        return '<td class="t-num mono pa-pos" style="color:var(--muted)">—</td>'
    m = v / 1e6
    return f'<td class="t-num mono pa-pos" style="color:var(--muted)">{_fmt_br_num(f"{m:,.1f}")}</td>'


def _build_pa_tree(df: pd.DataFrame, levels: list) -> dict:
    """Build a nested dict tree from leaf rows, aggregating bps metrics + position up each level."""
    root = {"_children": {}, "_agg": [0.0] * _PA_AGG_LEN}
    val_cols = ["dia_bps","mtd_bps","ytd_bps","m12_bps","position_brl"]
    for r in df.itertuples(index=False):
        vals = [getattr(r, c) for c in val_cols]
        node = root
        node["_agg"] = [a + v for a, v in zip(node["_agg"], vals)]
        for lv in levels:
            key = _pa_escape(getattr(r, lv))
            if key not in node["_children"]:
                node["_children"][key] = {"_children": {}, "_agg": [0.0] * _PA_AGG_LEN}
            node = node["_children"][key]
            node["_agg"] = [a + v for a, v in zip(node["_agg"], vals)]
    return root


def _render_pa_tree_rows(node: dict, path: list, depth: int, levels: list, out: list):
    """
    Depth-first traversal. Non-pinned children sorted by declared order (fixed list
    inspired by Excel PA report) with |YTD| desc as tiebreak. Pinned children
    (Caixa/Custos/…) always appended at the end.
    """
    max_depth = len(levels)
    level_name = levels[depth] if depth < max_depth else ""
    order_dict = _PA_ORDER_BY_LEVEL.get(level_name, {})
    items = list(node["_children"].items())
    pinned = [kv for kv in items if kv[0] in _PA_PINNED_BOTTOM]
    regular = [kv for kv in items if kv[0] not in _PA_PINNED_BOTTOM]
    regular.sort(key=lambda kv: (order_dict.get(kv[0], 10_000), -abs(kv[1]["_agg"][2])))
    pinned.sort(key=lambda kv: kv[0])
    ordered = regular + pinned
    for name, child in ordered:
        cur_path = path + [name]
        has_children = bool(child["_children"]) and depth < max_depth - 1
        out.append({
            "name": name,
            "display": _pa_render_name(name),
            "depth": depth,
            "path": "|".join(cur_path),
            "parent": "|".join(path) if path else "",
            "has_children": has_children,
            "pinned": name in _PA_PINNED_BOTTOM,
            "agg": child["_agg"],
        })
        if has_children:
            _render_pa_tree_rows(child, cur_path, depth + 1, levels, out)


def _render_pa_row_html(r: dict, max_abs: list) -> str:
    """Server-side render of a single PA row (used for depth-0 rows only with lazy render)."""
    level_cls = f"pa-l{r['depth']}"
    pinned_cls = " pa-pinned" if r["pinned"] else ""
    expander = (
        '<span class="pa-exp" aria-hidden="true">▸</span>'
        if r["has_children"] else
        '<span class="pa-exp pa-exp-empty" aria-hidden="true"></span>'
    )
    base_cls = f"pa-row {level_cls}{pinned_cls}"
    if r["has_children"]:
        cls_click = f' onclick="togglePaRow(this)" class="{base_cls} pa-has-children"'
    else:
        cls_click = f' class="{base_cls}"'
    pinned_attr = ' data-pinned="1"' if r["pinned"] else ""
    row_attrs = (
        f' data-level="{r["depth"]}"'
        f' data-path="{r["path"]}"'
        f' data-parent="{r["parent"]}"'
        f'{pinned_attr}'
    )
    agg = r["agg"]
    cells = (
        f'<td class="pa-name">{expander}<span class="pa-label">{r["display"]}</span></td>'
        + _pa_bp_cell(agg[0], heat_max=max_abs[0])
        + _pa_bp_cell(agg[1], heat_max=max_abs[1])
        + _pa_bp_cell(agg[2], heat_max=max_abs[2])
        + _pa_bp_cell(agg[3], heat_max=max_abs[3])
    )
    return f'<tr{row_attrs}{cls_click}>{cells}</tr>'


def _build_pa_view(fund_short: str, df: pd.DataFrame, view_id: str,
                    levels: list, first_col_label: str, active: bool,
                    cdi: dict) -> str:
    """
    Render one PA hierarchical view with lazy-loaded descendants.
    - Only depth-0 rows are pre-rendered as HTML.
    - Deeper rows are embedded as compact JSON and instantiated on expand.
    - Heatmap: each metric cell gets a background tint proportional to |v|/col_max.
    """
    if df is None or df.empty:
        return ""
    tree = _build_pa_tree(df, levels)
    rows = []
    _render_pa_tree_rows(tree, [], 0, levels, rows)

    # Max absolute per column (DIA/MTD/YTD/12M/POS) for heatmap scaling
    max_abs = [0.0] * _PA_AGG_LEN
    for r in rows:
        for i, v in enumerate(r["agg"]):
            if abs(v) > max_abs[i]:
                max_abs[i] = abs(v)

    # Group rows by parent path for lazy rendering
    by_parent: dict = {}
    for r in rows:
        by_parent.setdefault(r["parent"], []).append({
            "n":  r["name"],
            "d":  r["display"],
            "pa": r["path"],
            "pr": r["parent"],
            "a":  [round(x, 3) for x in r["agg"]],
            "hc": 1 if r["has_children"] else 0,
            "pi": 1 if r["pinned"] else 0,
            "dp": r["depth"],
        })

    root_rows = by_parent.get("", [])
    # Server-render only depth-0 rows
    tbody_rows = []
    for child in root_rows:
        tbody_rows.append(_render_pa_row_html({
            "name": child["n"], "display": child["d"],
            "depth": child["dp"], "path": child["pa"], "parent": child["pr"],
            "has_children": bool(child["hc"]), "pinned": bool(child["pi"]),
            "agg": child["a"],
        }, max_abs))

    # Compact JSON: keep all levels (including root) so filter/expand-all can work uniformly
    data_id = f"pa-data-{fund_short}-{view_id}"
    json_blob = json.dumps(
        {"maxAbs": [round(x, 3) for x in max_abs], "byParent": by_parent},
        separators=(",", ":"), ensure_ascii=False,
    ).replace("</", "<\\/")

    t = tree["_agg"]
    total_row = (
        '<tr class="pa-total-row">'
        '<td class="pa-name" style="font-weight:700">Total Alpha</td>'
        + _pa_bp_cell(t[0], bold=True)
        + _pa_bp_cell(t[1], bold=True)
        + _pa_bp_cell(t[2], bold=True)
        + _pa_bp_cell(t[3], bold=True)
        + "</tr>"
    )
    cdi_row = (
        '<tr class="pa-bench-row">'
        '<td class="pa-name" style="color:var(--muted); font-style:italic">Benchmark (CDI)</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["dia"]/100:+.2f}%</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["mtd"]/100:+.2f}%</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["ytd"]/100:+.2f}%</td>'
        f'<td class="t-num mono" style="color:var(--muted)">{cdi["m12"]/100:+.2f}%</td>'
        '</tr>'
    )
    nominal_row = (
        '<tr class="pa-nominal-row">'
        '<td class="pa-name" style="font-weight:700">Retorno Nominal</td>'
        + _pa_bp_cell(t[0] + cdi["dia"], bold=True)
        + _pa_bp_cell(t[1] + cdi["mtd"], bold=True)
        + _pa_bp_cell(t[2] + cdi["ytd"], bold=True)
        + _pa_bp_cell(t[3] + cdi["m12"], bold=True)
        + "</tr>"
    )

    # Sort arrow: default YTD desc (tree is already server-sorted that way)
    def th_sort(idx: int, label: str, active_idx: int = 2) -> str:
        arrow = ' <span class="pa-sort-arrow">▾</span>' if idx == active_idx else ''
        extra = ' pa-sort-active' if idx == active_idx else ''
        return (
            f'<th class="pa-sortable{extra}" data-pa-metric="{idx}"'
            f' onclick="sortPaMetric(this,{idx})" style="text-align:right; cursor:pointer">'
            f'{label}{arrow}</th>'
        )

    active_style = "" if active else ' style="display:none"'
    return f"""
    <div class="pa-view" data-pa-view="{view_id}" data-pa-id="{data_id}"
         data-sort-idx="2" data-sort-desc="1"{active_style}>
      <script type="application/json" id="{data_id}">{json_blob}</script>
      <table class="pa-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">{first_col_label}</th>
          {th_sort(0,'DIA')}
          {th_sort(1,'MTD')}
          {th_sort(2,'YTD')}
          {th_sort(3,'12M')}
        </tr></thead>
        <tbody>{''.join(tbody_rows)}</tbody>
        <tfoot>{total_row}{cdi_row}{nominal_row}</tfoot>
      </table>
    </div>"""


def _build_pa_bench_decomp_view(fund_short: str, df: pd.DataFrame, cdi: dict,
                                  idka_index_ret: dict, w_alb: float,
                                  albatroz_pa_sum: dict, ibov: dict = None) -> str:
    """3-line bench decomposition table for IDKA PA (as a 3rd view).

    Engine stores each fund's PA vs. its own benchmark:
      - IDKA PA rows are already α vs. IDKA_index
      - Albatroz PA rows are already α vs. CDI

    Decomposition (per window):
      Total     = sum(IDKA_PA_bps)                       (α vs. IDKA_index directly)
      Via Alb   = w_alb × Albatroz_α_vs_CDI              (Albatroz's own α, scaled)
      Swap leg  = w_alb × (CDI − IDKA_index)             (bench-cross adjustment)
      Direct α  = Total − Via Alb − Swap                 (residual)

    Rationale: the Albatroz slice earns Albatroz_return = CDI + Albatroz_α.
    IDKA evaluates everything vs. IDKA_index, so the Albatroz slice's
    contribution to IDKA α is w_alb × (Albatroz_return − IDKA_index)
    = Via Alb + Swap leg. Direct α is what the fund's own direct holdings
    added beyond that.
    """
    windows = ["dia", "mtd", "ytd", "m12"]

    # IDKA PA rows are already α vs. IDKA_index (confirmed per Diego 2026-04-19)
    sum_idka_pa = {w: 0.0 for w in windows}
    for w in windows:
        col = f"{w}_bps"
        sum_idka_pa[w] = float(df[col].sum()) if col in df.columns else 0.0

    def _val(d, key):
        return float(d.get(key, 0.0)) if d else 0.0

    total = {}    # α vs. IDKA_index = sum_pa directly
    via_alb = {}  # w_alb × Albatroz α vs. CDI
    swap = {}     # w_alb × (CDI − idka_idx)
    direct = {}   # residual
    for w in windows:
        cdi_w = _val(cdi, w)
        idx_w = _val(idka_index_ret, w)
        alb_w = _val(albatroz_pa_sum, w)
        total[w]   = sum_idka_pa[w]
        via_alb[w] = w_alb * alb_w
        swap[w]    = w_alb * (cdi_w - idx_w)
        direct[w]  = total[w] - via_alb[w] - swap[w]

    def _cell(bps, bold=False):
        pct = bps / 100.0  # bps → %
        if abs(pct) < 0.005:
            return '<td class="pa-val mono" style="color:var(--muted); text-align:right">—</td>'
        col = "var(--up)" if bps >= 0 else "var(--down)"
        weight = "font-weight:700;" if bold else ""
        return f'<td class="pa-val mono" style="color:{col}; text-align:right; {weight}">{pct:+.2f}%</td>'

    def _row(label, vals, bold=False, sub=""):
        weight = "font-weight:700" if bold else ""
        sub_html = f' <span style="color:var(--muted); font-size:10px">{sub}</span>' if sub else ''
        return (
            f'<tr class="pa-row" style="{weight}">'
            f'<td class="pa-name" style="{weight}">{label}{sub_html}</td>'
            + _cell(vals["dia"], bold)
            + _cell(vals["mtd"], bold)
            + _cell(vals["ytd"], bold)
            + _cell(vals["m12"], bold)
            + '</tr>'
        )

    # Direct positions subtotal = Direct α (replica) + Swap leg (bench cross effect
    # caused by the decision to allocate to Albatroz). Swap is a consequence of the
    # allocation choice, so it belongs in the "Direct positions" block.
    direct_subtotal = {w: direct[w] + swap[w] for w in windows}

    rows = ""
    # Direct positions block (header + 2 sub-rows + subtotal)
    rows += (
        '<tr class="pa-row pa-group-header">'
        '<td class="pa-name" style="font-weight:700; color:var(--accent-2); '
        'text-transform:uppercase; letter-spacing:.05em; font-size:11px; padding-top:10px">Direct Positions</td>'
        '<td></td><td></td><td></td><td></td></tr>'
    )
    rows += _row(
        "&nbsp;&nbsp;↳ Direct α",
        direct, sub="(réplica vs. IDKA index, parcela direta)",
    )
    rows += _row(
        "&nbsp;&nbsp;↳ Swap leg",
        swap, sub="(CDI − IDKA_index) × w_alb — ajuste de bench pela alocação em Albatroz",
    )
    rows += _row(
        "<b>Direct subtotal</b>",
        direct_subtotal, bold=True, sub="soma Direct α + Swap",
    )
    # Via Albatroz (standalone)
    rows += (
        '<tr class="pa-row pa-group-header">'
        '<td class="pa-name" style="font-weight:700; color:var(--accent-2); '
        'text-transform:uppercase; letter-spacing:.05em; font-size:11px; padding-top:10px">Via Albatroz</td>'
        '<td></td><td></td><td></td><td></td></tr>'
    )
    rows += _row(
        "&nbsp;&nbsp;↳ Albatroz α",
        via_alb, sub=f"(Albatroz α vs. CDI × w_alb {w_alb:.1%})",
    )
    # Grand total
    rows += _row("<b>Total vs. IDKA benchmark</b>", total, bold=True)

    # ── Referência — retorno absoluto do fundo e dos benchmarks principais
    # (contexto: igual ao bloco "RETORNO ABSOLUTO / CDI / IBOV / IDKA_IPCA_3A"
    # do xlsx oficial da Controle)
    idx_label = "IDKA_IPCA_3A" if fund_short == "IDKA_3Y" else "IDKA_IPCA_10A"
    retorno_abs = {w: total[w] + float((idka_index_ret or {}).get(w, 0.0)) for w in windows}
    rows += (
        '<tr class="pa-row pa-group-header">'
        '<td class="pa-name" style="font-weight:700; color:var(--muted); '
        'text-transform:uppercase; letter-spacing:.05em; font-size:10.5px; padding-top:14px">Referência</td>'
        '<td></td><td></td><td></td><td></td></tr>'
    )
    rows += _row("&nbsp;&nbsp;Retorno Absoluto fund", retorno_abs, sub="= Total α + IDKA_index")
    rows += _row(f"&nbsp;&nbsp;{idx_label}", idka_index_ret or {}, sub="bench")
    rows += _row("&nbsp;&nbsp;CDI", cdi or {}, sub="ref. juros")

    return f"""
    <div class="pa-view" data-pa-view="bench" style="display:none">
      <div style="padding:8px 4px; font-size:11px; color:var(--muted); line-height:1.5">
        Decomposição α vs. IDKA benchmark · w_alb snapshot atual ({w_alb:.1%}) aplicado a todos os windows ·
        Swap leg compensa o fato de Albatroz ser benchmarkeado a CDI dentro de IDKA (bench IDKA_index).
      </div>
      <table class="pa-table" data-no-sort="1" style="margin-top:6px">
        <thead><tr>
          <th style="text-align:left">Componente</th>
          <th style="text-align:right">DIA</th>
          <th style="text-align:right">MTD</th>
          <th style="text-align:right">YTD</th>
          <th style="text-align:right">12M</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""


def _apply_fx_split_remap(df: pd.DataFrame) -> pd.DataFrame:
    """Reagrupa CLASSE='BRLUSD' e CLASSE='FX' em "FX Basis Risk & Carry" antes de
    construir a árvore PA. Linhas BRLUSD com GRUPO específico (Commodities / RV
    Intl / RF Intl) ganham GRUPO='FX em <X>' pra preservar drill-down.
    Demais linhas — unchanged. Total preservado (pura recategorização)."""
    if df.empty or "CLASSE" not in df.columns:
        return df
    out = df.copy()
    is_brlusd = out["CLASSE"] == "BRLUSD"
    is_fx     = out["CLASSE"] == "FX"
    if not (is_brlusd.any() or is_fx.any()):
        return out
    # Map GRUPO sub-bucket only for BRLUSD rows (FX cross stays "FX Spot & Futuros")
    if "GRUPO" in out.columns:
        grp_map = {
            "Commodities": "FX em Commodities",
            "RV Intl":     "FX em RV Intl",
            "RF Intl":     "FX em RF Intl",
        }
        new_grupo = out.loc[is_brlusd, "GRUPO"].map(grp_map).fillna("FX Spot & Futuros")
        out.loc[is_brlusd, "GRUPO"] = new_grupo
        out.loc[is_fx,     "GRUPO"] = "FX Spot & Futuros"
    out.loc[is_brlusd | is_fx, "CLASSE"] = "FX Basis Risk & Carry"
    return out


def build_pa_section_hier(fund_short: str, df_pa: pd.DataFrame, cdi: dict,
                           idka_index_ret: dict = None, w_alb: float = None,
                           albatroz_pa_sum: dict = None, ibov: dict = None,
                           pmovers_trigger: str = "") -> str:
    """
    PA card with hierarchical views (Por Classe / Por Livro).
    For IDKA funds, adds a 3rd view "Por Bench" with bench decomposition
    (Direct α / Via Albatroz / Swap leg / Total vs. IDKA index).

    FX-split sempre aplicado: CLASSE='BRLUSD' e CLASSE='FX' viram
    "FX Basis Risk & Carry" antes do tree builder. Substitui a árvore canônica
    pela versão FX-split (decisão 2026-04-28). Para fundos sem FX, no-op.
    """
    pa_key = _FUND_PA_KEY.get(fund_short)
    if pa_key is None or df_pa is None or df_pa.empty:
        return ""
    df = df_pa[df_pa["FUNDO"] == pa_key].copy()
    if df.empty:
        return ""
    df = _apply_fx_split_remap(df)

    # For IDKAs, the "Por Bench" decomposition is the default active view —
    # it's the most useful lens for a benchmarked RF fund.
    is_idka = fund_short in ("IDKA_3Y", "IDKA_10Y")
    bench_enabled = (is_idka and idka_index_ret and w_alb is not None
                     and albatroz_pa_sum is not None)

    # EVOLUTION defaults to "Por Livro" (Strategy hierarchy is more meaningful);
    # other funds default to "Por Classe" (when no bench view is shown).
    livro_default = (fund_short == "EVOLUTION")
    classe_default = (not bench_enabled) and not livro_default

    view_classe = _build_pa_view(
        fund_short, df, "classe",
        ["CLASSE", "PRODUCT"], "Classe / Produto",
        active=classe_default,
        cdi=cdi,
    )

    if fund_short == "EVOLUTION":
        df_evo = df.copy()
        df_evo["STRATEGY"] = df_evo["LIVRO"].map(_evo_strategy)
        view_livro = _build_pa_view(
            fund_short, df_evo, "livro",
            ["STRATEGY", "LIVRO", "PRODUCT"], "Strategy / Livro / Produto",
            active=livro_default, cdi=cdi,
        )
    else:
        view_livro = _build_pa_view(
            fund_short, df, "livro",
            ["LIVRO", "PRODUCT"], "Livro / Produto", active=False, cdi=cdi,
        )

    # 3rd view: bench decomposition — IDKA default
    view_bench = ""
    bench_toggle_btn = ""
    classe_active_cls = "active" if classe_default else ""
    livro_active_cls  = "active" if livro_default  else ""
    if bench_enabled:
        view_bench = _build_pa_bench_decomp_view(
            fund_short, df, cdi, idka_index_ret, w_alb, albatroz_pa_sum, ibov=ibov
        )
        # Force-display the bench view as active (override the default hidden)
        view_bench = view_bench.replace(
            'data-pa-view="bench" style="display:none"',
            'data-pa-view="bench" style="display:block"',
        )
        bench_toggle_btn = (
            '<button class="pa-tgl active" data-pa-view="bench" '
            'onclick="selectPaView(this,\'bench\')">Por Bench</button>'
        )

    return f"""
    <section class="card pa-card" data-pa-fund="{fund_short}">
      <div class="card-head">
        <span class="card-title">Performance Attribution</span>
        <span class="card-sub">— {fund_short} · alpha (%) vs. benchmark · click p/ drill-down</span>
        {pmovers_trigger}
        <div class="pa-toolbar">
          <input class="pa-search" type="search" placeholder="🔍 buscar..." oninput="filterPa(this)" title="Filtrar por nome (busca parcial)"/>
          <button class="pa-btn" onclick="expandAllPa(this)"    title="Expandir tudo">⤢ Expandir</button>
          <button class="pa-btn" onclick="collapseAllPa(this)"  title="Colapsar tudo">⤡ Colapsar</button>
          <button class="pa-btn" onclick="resetPaSort(this)"    title="Voltar à ordem padrão (YTD desc)">↺ Reset</button>
        </div>
        <div class="pa-view-toggle">
          <button class="pa-tgl {classe_active_cls}" data-pa-view="classe"
                  onclick="selectPaView(this,'classe')">Por Classe</button>
          <button class="pa-tgl {livro_active_cls}" data-pa-view="livro"
                  onclick="selectPaView(this,'livro')">Por Livro</button>
          {bench_toggle_btn}
        </div>
      </div>
      {view_classe}
      {view_livro}
      {view_bench}
    </section>"""


def _pa_filter_alpha(df):
    """Drop PA rows from benchmark-tracking livros; keep only alpha-bearing contributions."""
    if df is None or df.empty:
        return df
    mask = True
    for pa_key, livros in _PA_BENCH_LIVROS.items():
        mask = mask & ~((df["FUNDO"] == pa_key) & (df["LIVRO"].isin(livros)))
    return df[mask]

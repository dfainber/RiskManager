"""QUANT Performance Attribution — FX-segregated view.

Mirrors generate_evolution_pa_fx_split.py for QUANT; FX-split is applied
one level below LIVRO:

  LIVRO → CLASSE_NEW (FX-split here) → GRUPO_NEW → PRODUCT

Total PnL preserved. The FX-bucketing rule lives in `pa_renderers.fx_split_classify`.

Output: data/morning-calls/<date>_quant_pa_fx_split.html
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from glpg_fetch import read_sql
from pa_renderers import (
    _FX_SPLIT_CLASSES as _FX_CLASSES,
    _pa_fx_bps_cell as _bps_cell,
    _pa_fx_bps_color as _bps_color,
    _pa_fx_esc as _esc,
    fx_split_classify,
    PA_FX_SPLIT_CSS_BASE,
    PA_FX_SPLIT_CSS_TOOLBAR,
    PA_FX_SPLIT_JS_TOGGLE,
)
from risk_runtime import DATA_STR, OUT_DIR


WINDOWS = [("dia", "DIA"), ("mtd", "MTD"), ("ytd", "YTD"), ("m12", "12M")]
LIVRO_PINNED_BOTTOM = {"Caixa", "Caixa USD", "Taxas e Custos", "Custos"}


def _fetch_quant_pa(date_str: str) -> pd.DataFrame:
    q = f"""
    SELECT
      "CLASSE", "GRUPO", "SUBCLASSE", "LIVRO", "BOOK", "PRODUCT",
      SUM(CASE WHEN "DATE" = DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS dia_bps,
      SUM(CASE WHEN "DATE" >= DATE_TRUNC('month', DATE '{date_str}')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS mtd_bps,
      SUM(CASE WHEN "DATE" >= DATE_TRUNC('year', DATE '{date_str}')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS ytd_bps,
      SUM(CASE WHEN "DATE" >= (DATE '{date_str}' - INTERVAL '12 months')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS m12_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'QUANT'
      AND "DATE" >= (DATE '{date_str}' - INTERVAL '12 months')
      AND "DATE" <= DATE '{date_str}'
    GROUP BY "CLASSE", "GRUPO", "SUBCLASSE", "LIVRO", "BOOK", "PRODUCT"
    HAVING ABS(SUM(CASE WHEN "DATE" = DATE '{date_str}' THEN "DIA" ELSE 0 END)) > 1e-9
        OR ABS(SUM("DIA")) > 1e-9
    """
    df = read_sql(q)
    for c in ("dia_bps", "mtd_bps", "ytd_bps", "m12_bps"):
        df[c] = df[c].astype(float).fillna(0.0)
    return df


def _apply_remap(df: pd.DataFrame) -> pd.DataFrame:
    """Add CLASSE_NEW / GRUPO_NEW columns; original columns preserved.
    Source-of-truth classifier lives in `pa_renderers.fx_split_classify`."""
    out = df.copy()
    new = out.apply(lambda r: fx_split_classify(r["CLASSE"], r["GRUPO"]), axis=1)
    out["CLASSE_NEW"] = [t[0] for t in new]
    out["GRUPO_NEW"]  = [t[1] for t in new]
    return out


def _build_tree_table(df: pd.DataFrame) -> str:
    """Hierarchy: LIVRO → CLASSE_NEW → GRUPO_NEW → PRODUCT."""
    if df.empty:
        return '<div style="color:#888">Sem dados.</div>'

    classe_order_default = [
        "RF BZ IPCA", "RF BZ Pré-fixado", "RF BZ", "RF Intl",
        "RV BZ", "RV Intl", "Commodities", "ETF Options", "FX Basis Risk & Carry",
        "Caixa", "Custos",
    ]
    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    rows_html: list[str] = []

    # Level-0: LIVRO — sort by |12M| desc, with Caixa/Custos pinned at bottom
    lv_tot = df.groupby("LIVRO", as_index=False)[cols].sum()
    lv_tot["_pin"] = lv_tot["LIVRO"].apply(lambda x: 1 if x in LIVRO_PINNED_BOTTOM else 0)
    lv_tot["_abs12m"] = lv_tot["m12_bps"].abs()
    lv_tot = lv_tot.sort_values(["_pin", "_abs12m"], ascending=[True, False])

    for _, lr in lv_tot.iterrows():
        liv = "—" if pd.isna(lr["LIVRO"]) else (lr["LIVRO"] or "—")
        lv_id = "L_" + str(liv).replace(" ", "_").replace("/", "_").replace("&", "and")
        rows_html.append(
            f'<tr class="lvl0" data-row-id="{lv_id}" onclick="paToggle(this)" '
            f'style="cursor:pointer;background:#1a2030">'
            f'<td><span class="caret">▶</span> '
            f'<span style="color:#e6e6e6;font-weight:700">{_esc(liv)}</span></td>'
            f'{_bps_cell(lr["dia_bps"], True)}{_bps_cell(lr["mtd_bps"], True)}'
            f'{_bps_cell(lr["ytd_bps"], True)}{_bps_cell(lr["m12_bps"], True)}'
            f'</tr>'
        )

        # Level-1: CLASSE_NEW within LIVRO
        cl_tot = (df[df["LIVRO"] == lr["LIVRO"]]
                  .groupby("CLASSE_NEW", as_index=False)[cols].sum())
        cl_tot["_ord"] = cl_tot["CLASSE_NEW"].apply(
            lambda c: classe_order_default.index(c) if c in classe_order_default else 99
        )
        cl_tot = cl_tot.sort_values(["_ord", "ytd_bps"],
                                    ascending=[True, False],
                                    key=lambda s: s.abs() if s.name == "ytd_bps" else s)
        for _, cr in cl_tot.iterrows():
            cls = cr["CLASSE_NEW"]
            is_fx = cls == "FX Basis Risk & Carry"
            cls_color = "#5aa3e8" if is_fx else "#cfd6e0"
            cls_id = f"{lv_id}__C_{cls.replace(' ', '_').replace('&', 'and')}"
            rows_html.append(
                f'<tr class="lvl1" data-row-parent="{lv_id}" data-row-id="{cls_id}" '
                f'onclick="paToggle(this)" style="cursor:pointer;display:none">'
                f'<td style="padding-left:24px"><span class="caret">▶</span> '
                f'<span style="color:{cls_color};font-weight:600">{_esc(cls)}</span></td>'
                f'{_bps_cell(cr["dia_bps"])}{_bps_cell(cr["mtd_bps"])}'
                f'{_bps_cell(cr["ytd_bps"])}{_bps_cell(cr["m12_bps"])}'
                f'</tr>'
            )

            # Level-2: GRUPO_NEW
            gr_tot = (df[(df["LIVRO"] == lr["LIVRO"]) & (df["CLASSE_NEW"] == cls)]
                      .groupby("GRUPO_NEW", as_index=False)[cols].sum()
                      .sort_values("ytd_bps", key=lambda s: s.abs(), ascending=False))
            for _, gr in gr_tot.iterrows():
                grp = gr["GRUPO_NEW"]
                grp_id = f"{cls_id}__G_{grp.replace(' ', '_').replace('&', 'and')}"
                rows_html.append(
                    f'<tr class="lvl2" data-row-parent="{cls_id}" data-row-id="{grp_id}" '
                    f'onclick="paToggle(this)" style="cursor:pointer;display:none">'
                    f'<td style="padding-left:48px"><span class="caret">▶</span> '
                    f'<span style="color:#9aa3b2">{_esc(grp)}</span></td>'
                    f'{_bps_cell(gr["dia_bps"])}{_bps_cell(gr["mtd_bps"])}'
                    f'{_bps_cell(gr["ytd_bps"])}{_bps_cell(gr["m12_bps"])}'
                    f'</tr>'
                )

                # Level-3: PRODUCT
                pr = (df[(df["LIVRO"] == lr["LIVRO"]) & (df["CLASSE_NEW"] == cls)
                         & (df["GRUPO_NEW"] == grp)]
                      .groupby("PRODUCT", as_index=False)[cols].sum()
                      .sort_values("ytd_bps", key=lambda s: s.abs(), ascending=False))
                for _, pp in pr.iterrows():
                    rows_html.append(
                        f'<tr class="lvl3" data-row-parent="{grp_id}" '
                        f'style="display:none">'
                        f'<td style="padding-left:72px;color:#7a8290;font-size:11px">{_esc(pp["PRODUCT"])}</td>'
                        f'{_bps_cell(pp["dia_bps"])}{_bps_cell(pp["mtd_bps"])}'
                        f'{_bps_cell(pp["ytd_bps"])}{_bps_cell(pp["m12_bps"])}'
                        f'</tr>'
                    )

    tot = df[cols].sum()
    rows_html.append(
        f'<tr class="total" style="border-top:2px solid #555;font-weight:700;background:#0d1626">'
        f'<td style="padding-top:8px">TOTAL QUANT</td>'
        f'{_bps_cell(tot["dia_bps"], True)}{_bps_cell(tot["mtd_bps"], True)}'
        f'{_bps_cell(tot["ytd_bps"], True)}{_bps_cell(tot["m12_bps"], True)}'
        f'</tr>'
    )

    head = (
        '<thead><tr style="border-bottom:1px solid #333;color:#9aa3b2;font-size:11px">'
        '<th style="text-align:left;padding:6px 10px">CATEGORIA</th>'
        '<th class="num sortable" onclick="evoSortBy(1)">DIA</th>'
        '<th class="num sortable" onclick="evoSortBy(2)">MTD</th>'
        '<th class="num sortable" onclick="evoSortBy(3)">YTD</th>'
        '<th class="num sortable" onclick="evoSortBy(4)">12M</th>'
        '</tr></thead>'
    )
    body = "<tbody>" + "".join(rows_html) + "</tbody>"
    toolbar = (
        '<div class="pa-toolbar-mini">'
        '<button class="pa-btn" onclick="evoExpandAll()" title="Expandir tudo">⤢ Expandir</button>'
        '<button class="pa-btn" onclick="evoCollapseAll()" title="Colapsar tudo">⤡ Colapsar</button>'
        '<button class="pa-btn" onclick="evoResetSort()" title="Voltar à ordem padrão (hierarquia)">↺ Reset</button>'
        '</div>'
    )
    return f'{toolbar}<table class="pa-tree" id="evo-pa-tree">{head}{body}</table>'


def _build_top_block(df: pd.DataFrame, win: str, label: str) -> str:
    col = f"{win}_bps"
    df_asset = df[df["CLASSE_NEW"] != "FX Basis Risk & Carry"]
    g = (df_asset.groupby(["LIVRO", "CLASSE_NEW", "PRODUCT"], as_index=False)
                 .agg(v=(col, "sum"))
                 .query("abs(v) > 0.05")
                 .sort_values("v"))
    if g.empty:
        return f'<div class="top-block"><div class="top-title">{label}</div>' \
               f'<div style="color:#666">Sem movimentos materiais.</div></div>'

    worst = g.head(5)
    best  = g.tail(5).iloc[::-1]

    def _row(r, color):
        return (
            f'<tr><td style="color:#cfd6e0;font-size:11px">'
            f'<span style="color:#888">[{_esc(str(r["LIVRO"])[:7])}/{_esc(r["CLASSE_NEW"][:7])}]</span> '
            f'{_esc(r["PRODUCT"])}'
            f'</td><td class="num" style="color:{color};font-weight:600">{r["v"]/100:+.2f}%</td></tr>'
        )

    worst_html = "".join(_row(r, "#e74c3c") for _, r in worst.iterrows())
    best_html  = "".join(_row(r, "#26a65b") for _, r in best.iterrows())

    return (
        f'<div class="top-block">'
        f'<div class="top-title">{label}</div>'
        f'<div class="top-sub" style="color:#e74c3c">5 PIORES</div>'
        f'<table class="top-tbl"><tbody>{worst_html}</tbody></table>'
        f'<div class="top-sub" style="color:#26a65b;margin-top:8px">5 MELHORES</div>'
        f'<table class="top-tbl"><tbody>{best_html}</tbody></table>'
        f'</div>'
    )


def _build_verification_block(df: pd.DataFrame) -> str:
    """Verifica: total, Commodities/RV Intl/RF Intl preservados (global e por LIVRO),
    FX Basis (novo) = BRLUSD+FX (antigo) globalmente."""
    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    new_classe_global = df.groupby("CLASSE_NEW")[cols].sum()
    old_classe_global = df.groupby("CLASSE")[cols].sum()
    fx_old_global = old_classe_global.loc[old_classe_global.index.isin(["BRLUSD", "FX"])][cols].sum()

    rows = [("TOTAL QUANT", df[cols].sum(), df[cols].sum())]
    for bucket in ("Commodities", "RV Intl", "RF Intl"):
        if bucket in old_classe_global.index and bucket in new_classe_global.index:
            rows.append((f"{bucket} (preservado)",
                         old_classe_global.loc[bucket],
                         new_classe_global.loc[bucket]))
    if "FX Basis Risk & Carry" in new_classe_global.index:
        rows.append(("FX Basis Risk & Carry (= antigo BRLUSD + FX)",
                     fx_old_global, new_classe_global.loc["FX Basis Risk & Carry"]))

    # Per-LIVRO: only Commodities/RV Intl/RF Intl preservados (where present)
    for liv in sorted(df["LIVRO"].dropna().unique().tolist()):
        sub = df[df["LIVRO"] == liv]
        if sub.empty:
            continue
        old_l = sub.groupby("CLASSE")[cols].sum()
        new_l = sub.groupby("CLASSE_NEW")[cols].sum()
        for bucket in ("Commodities", "RV Intl", "RF Intl"):
            if bucket in old_l.index and bucket in new_l.index:
                ov = old_l.loc[bucket]
                nv = new_l.loc[bucket]
                if any(abs(float(ov[c])) > 0.05 or abs(float(nv[c])) > 0.05 for c in cols):
                    rows.append((f"  └─ {liv} · {bucket}", ov, nv))

    head = (
        '<thead><tr style="color:#9aa3b2;font-size:11px">'
        '<th style="text-align:left;padding:6px 10px">Bucket</th>'
        '<th class="num">DIA orig.</th><th class="num">DIA novo</th>'
        '<th class="num">MTD orig.</th><th class="num">MTD novo</th>'
        '<th class="num">YTD orig.</th><th class="num">YTD novo</th>'
        '<th class="num">12M orig.</th><th class="num">12M novo</th>'
        '<th class="num">Δ</th></tr></thead>'
    )

    def _fmt(v: float) -> str:
        return f"{v/100:+.2f}%" if abs(v) > 0.05 else "—"

    body_rows = []
    for label, old, new in rows:
        diffs = [abs(float(new[c]) - float(old[c])) for c in cols]
        max_diff = max(diffs)
        ok = max_diff < 0.05
        marker = ('<span style="color:#26a65b">✓</span>' if ok
                  else f'<span style="color:#e74c3c">⚠ {max_diff:.2f} bps</span>')
        cells = ""
        for c in cols:
            ov, nv = float(old[c]), float(new[c])
            ok_pair = abs(ov - nv) < 0.05
            col = "#cfd6e0" if ok_pair else "#e74c3c"
            cells += f'<td class="num" style="color:{col}">{_fmt(ov)}</td>'
            cells += f'<td class="num" style="color:{col};font-weight:600">{_fmt(nv)}</td>'
        body_rows.append(
            f'<tr><td style="padding:5px 10px;font-size:11px;color:#cfd6e0">{_esc(label)}</td>'
            f'{cells}<td class="num">{marker}</td></tr>'
        )

    return f'<table class="pa-tree">{head}<tbody>{"".join(body_rows)}</tbody></table>'


CSS = PA_FX_SPLIT_CSS_BASE + PA_FX_SPLIT_CSS_TOOLBAR

JS = PA_FX_SPLIT_JS_TOGGLE + """
var _evoSort = { col: null, asc: false };
var _evoOriginal = null;

function _evoCacheOriginal() {
  if (_evoOriginal) return;
  var tbody = document.querySelector('#evo-pa-tree tbody');
  if (!tbody) return;
  _evoOriginal = Array.prototype.slice.call(tbody.children);
}

function _evoCellNum(td, asc) {
  var t = (td && td.textContent || '').trim();
  if (!t || t === '—') return asc ? Infinity : -Infinity;
  return parseFloat(t.replace('%', '').replace('+', '').replace(',', '.')) || 0;
}

function evoSortBy(colIdx) {
  _evoCacheOriginal();
  var tbody = document.querySelector('#evo-pa-tree tbody');
  if (!tbody) return;
  if (_evoSort.col === colIdx) _evoSort.asc = !_evoSort.asc;
  else { _evoSort.col = colIdx; _evoSort.asc = false; }

  var all = Array.prototype.slice.call(tbody.children);
  var totalRow = all.filter(function(r) { return r.classList.contains('total'); })[0];
  var data = all.filter(function(r) { return !r.classList.contains('total'); });

  var byParent = {};
  data.forEach(function(r) {
    var p = r.dataset.rowParent || '__root__';
    (byParent[p] = byParent[p] || []).push(r);
  });
  Object.keys(byParent).forEach(function(p) {
    byParent[p].sort(function(a, b) {
      var va = _evoCellNum(a.cells[colIdx], _evoSort.asc);
      var vb = _evoCellNum(b.cells[colIdx], _evoSort.asc);
      return _evoSort.asc ? va - vb : vb - va;
    });
  });

  var result = [];
  function dfs(parentId) {
    (byParent[parentId] || []).forEach(function(k) {
      result.push(k);
      var sub = k.dataset.rowId;
      if (sub) dfs(sub);
    });
  }
  dfs('__root__');
  result.forEach(function(r) { tbody.appendChild(r); });
  if (totalRow) tbody.appendChild(totalRow);

  document.querySelectorAll('#evo-pa-tree th').forEach(function(th) {
    th.classList.remove('sort-asc', 'sort-desc');
  });
  var ths = document.querySelectorAll('#evo-pa-tree th');
  if (ths[colIdx]) ths[colIdx].classList.add(_evoSort.asc ? 'sort-asc' : 'sort-desc');
}

function evoResetSort() {
  _evoCacheOriginal();
  var tbody = document.querySelector('#evo-pa-tree tbody');
  if (!tbody || !_evoOriginal) return;
  _evoOriginal.forEach(function(r) { tbody.appendChild(r); });
  _evoSort = { col: null, asc: false };
  document.querySelectorAll('#evo-pa-tree th').forEach(function(th) {
    th.classList.remove('sort-asc', 'sort-desc');
  });
}

function evoExpandAll() {
  var tbody = document.querySelector('#evo-pa-tree tbody');
  if (!tbody) return;
  Array.prototype.forEach.call(tbody.children, function(r) {
    if (r.classList.contains('total')) return;
    r.style.display = '';
    var c = r.querySelector('.caret');
    if (c && r.dataset.rowId) c.classList.add('open');
  });
}

function evoCollapseAll() {
  var tbody = document.querySelector('#evo-pa-tree tbody');
  if (!tbody) return;
  Array.prototype.forEach.call(tbody.children, function(r) {
    if (r.classList.contains('total')) return;
    if (r.dataset.rowParent) r.style.display = 'none';
    var c = r.querySelector('.caret');
    if (c) c.classList.remove('open');
  });
}

document.addEventListener('DOMContentLoaded', _evoCacheOriginal);
"""


def _build_html(df: pd.DataFrame, date_str: str) -> str:
    tree_html = _build_tree_table(df)
    top_blocks = "".join(
        _build_top_block(df, win, label) for win, label in WINDOWS[:3]
    )
    verif_html = _build_verification_block(df)

    return f"""<!doctype html>
<html lang="pt-BR"><head>
<meta charset="utf-8">
<title>QUANT PA — FX Split — {date_str}</title>
<style>{CSS}</style>
</head><body>
<h1>QUANT Performance Attribution — FX-segregated view</h1>
<div class="sub">{date_str} · q_models.REPORT_ALPHA_ATRIBUTION · FX-split aplicado dentro de cada LIVRO · total idêntico ao PA canônico</div>

<div class="legend" style="margin:0 0 16px">
  Hierarquia: <b>LIVRO</b> → <b>CLASSE</b> (com FX-split) → GRUPO → PRODUCT.
  <b>FX Basis Risk &amp; Carry</b> agrega <i>FX em Commodities/RV Intl/RF Intl</i> (efeito do câmbio sobre exposições USD)
  + <i>FX Spot &amp; Futuros</i> (USD Brasil hedge + posições FX direcionais + cross-FX EUR/JPY/CAD + custos cambiais).
  <b>Total por LIVRO = idêntico ao PA original</b>; reordenação categórica, não recálculo.
</div>

{tree_html}

<div class="sub" style="margin-top:24px">Verificação — totais novos vs originais (devem bater, é só reagrupamento)</div>
{verif_html}

<div class="sub" style="margin-top:24px">Top contribuintes &amp; detratores por janela — <b>excluindo FX Basis Risk &amp; Carry</b> (só efeito-ativo puro)</div>
<div class="cards">
{top_blocks}
</div>

<div class="legend">
  <b>DIA</b>: PnL realizado hoje · <b>MTD</b>: mês corrente · <b>YTD</b>: ano corrente · <b>12M</b>: últimos 12 meses · todos em % de NAV.<br>
  Sanity: TOTAL QUANT desta tabela = soma de todas as linhas DIA/MTD/YTD/12M originais — a recategorização só renomeia &quot;BRLUSD&quot;+&quot;FX&quot; em &quot;FX Basis Risk &amp; Carry&quot; dentro de cada LIVRO.
</div>

<script>{JS}</script>
</body></html>"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=DATA_STR, help="Reference date YYYY-MM-DD")
    p.add_argument("--out", default=None, help="Output html path")
    args = p.parse_args()

    print(f"Fetching QUANT PA for {args.date}...")
    df = _fetch_quant_pa(args.date)
    if df.empty:
        print("No data — nothing written.")
        return 1
    df = _apply_remap(df)

    out_path = Path(args.out) if args.out else OUT_DIR / f"{args.date}_quant_pa_fx_split.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    html = _build_html(df, args.date)
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved: {out_path}")

    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    tot = df[cols].sum()
    print(f"\nTotal QUANT (bps):  DIA={tot['dia_bps']:+.1f}  MTD={tot['mtd_bps']:+.1f}  "
          f"YTD={tot['ytd_bps']:+.1f}  12M={tot['m12_bps']:+.1f}")
    fx = df[df["CLASSE_NEW"] == "FX Basis Risk & Carry"][cols].sum()
    print(f"FX Basis Risk & Carry:  DIA={fx['dia_bps']:+.1f}  MTD={fx['mtd_bps']:+.1f}  "
          f"YTD={fx['ytd_bps']:+.1f}  12M={fx['m12_bps']:+.1f}")
    print("\nPer-LIVRO totals (12M bps):")
    for liv, v in df.groupby("LIVRO")["m12_bps"].sum().sort_values(ascending=False).items():
        print(f"  {str(liv):20s} {v:+8.1f} bps")
    return 0


if __name__ == "__main__":
    sys.exit(main())

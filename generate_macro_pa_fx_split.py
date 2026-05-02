"""MACRO Performance Attribution — FX-segregated view.

Re-orders the canonical PA (q_models.REPORT_ALPHA_ATRIBUTION) so asset
effect and FX effect render in separate buckets. Total PnL is preserved.

The FX-bucketing rule lives in `pa_renderers.fx_split_classify`.

Output: data/morning-calls/<date>_macro_pa_fx_split.html
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
    PA_FX_SPLIT_JS_TOGGLE,
)
from risk_runtime import DATA_STR, OUT_DIR


WINDOWS = [("dia", "DIA"), ("mtd", "MTD"), ("ytd", "YTD"), ("m12", "12M")]


def _fetch_macro_pa(date_str: str) -> pd.DataFrame:
    """Pull MACRO PA leaves with FX classification columns + 4 windows in bps."""
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
    WHERE "FUNDO" = 'MACRO'
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
    """Hierarchical PA: CLASSE_NEW → GRUPO_NEW → LIVRO → PRODUCT."""
    if df.empty:
        return '<div style="color:#888">Sem dados.</div>'

    classe_order_default = [
        "RF BZ IPCA", "RF BZ Pré-fixado", "RF BZ", "RF Intl",
        "RV BZ", "RV Intl", "Commodities", "ETF Options", "FX Basis Risk & Carry",
        "Caixa", "Custos",
    ]

    rows_html: list[str] = []

    # Level-0: CLASSE_NEW totals, ordered by canonical list then by |YTD|
    cl_tot = df.groupby("CLASSE_NEW", as_index=False).agg(
        dia=("dia_bps", "sum"), mtd=("mtd_bps", "sum"),
        ytd=("ytd_bps", "sum"), m12=("m12_bps", "sum"),
    )
    cl_tot["_ord"] = cl_tot["CLASSE_NEW"].apply(
        lambda c: classe_order_default.index(c) if c in classe_order_default else 99
    )
    cl_tot = cl_tot.sort_values(["_ord", "ytd"],
                                ascending=[True, False],
                                key=lambda s: s.abs() if s.name == "ytd" else s)

    for _, cr in cl_tot.iterrows():
        cls = cr["CLASSE_NEW"]
        cls_id = cls.replace(" ", "_").replace("/", "_").replace("&", "and")
        is_fx = cls == "FX Basis Risk & Carry"
        cls_color = "#5aa3e8" if is_fx else "#e6e6e6"
        rows_html.append(
            f'<tr class="lvl0" data-row-id="{cls_id}" onclick="paToggle(this)" '
            f'style="cursor:pointer;background:#1a2030">'
            f'<td><span class="caret">▶</span> '
            f'<span style="color:{cls_color};font-weight:700">{_esc(cls)}</span></td>'
            f'{_bps_cell(cr["dia"], True)}{_bps_cell(cr["mtd"], True)}'
            f'{_bps_cell(cr["ytd"], True)}{_bps_cell(cr["m12"], True)}'
            f'</tr>'
        )

        # Level-1: GRUPO_NEW under this CLASSE
        gr_tot = (df[df["CLASSE_NEW"] == cls]
                  .groupby("GRUPO_NEW", as_index=False)
                  .agg(dia=("dia_bps", "sum"), mtd=("mtd_bps", "sum"),
                       ytd=("ytd_bps", "sum"), m12=("m12_bps", "sum"))
                  .sort_values("ytd", key=lambda s: s.abs(), ascending=False))
        for _, gr in gr_tot.iterrows():
            grp = gr["GRUPO_NEW"]
            grp_id = f"{cls_id}__{grp.replace(' ', '_').replace('/', '_').replace('&', 'and')}"
            rows_html.append(
                f'<tr class="lvl1" data-row-parent="{cls_id}" data-row-id="{grp_id}" '
                f'onclick="paToggle(this)" style="cursor:pointer;display:none">'
                f'<td style="padding-left:24px"><span class="caret">▶</span> '
                f'<span style="color:#cfd6e0">{_esc(grp)}</span></td>'
                f'{_bps_cell(gr["dia"])}{_bps_cell(gr["mtd"])}'
                f'{_bps_cell(gr["ytd"])}{_bps_cell(gr["m12"])}'
                f'</tr>'
            )

            # Level-2: LIVRO
            lv_tot = (df[(df["CLASSE_NEW"] == cls) & (df["GRUPO_NEW"] == grp)]
                      .groupby("LIVRO", as_index=False)
                      .agg(dia=("dia_bps", "sum"), mtd=("mtd_bps", "sum"),
                           ytd=("ytd_bps", "sum"), m12=("m12_bps", "sum"))
                      .sort_values("ytd", key=lambda s: s.abs(), ascending=False))
            for _, lv in lv_tot.iterrows():
                liv = lv["LIVRO"]
                liv_id = f"{grp_id}__{(liv or 'NA').replace(' ', '_')}"
                rows_html.append(
                    f'<tr class="lvl2" data-row-parent="{grp_id}" data-row-id="{liv_id}" '
                    f'onclick="paToggle(this)" style="cursor:pointer;display:none">'
                    f'<td style="padding-left:48px"><span class="caret">▶</span> '
                    f'<span style="color:#9aa3b2">{_esc(liv)}</span></td>'
                    f'{_bps_cell(lv["dia"])}{_bps_cell(lv["mtd"])}'
                    f'{_bps_cell(lv["ytd"])}{_bps_cell(lv["m12"])}'
                    f'</tr>'
                )

                # Level-3: PRODUCT (leaves)
                pr = (df[(df["CLASSE_NEW"] == cls) & (df["GRUPO_NEW"] == grp) & (df["LIVRO"] == liv)]
                      .groupby("PRODUCT", as_index=False)
                      .agg(dia=("dia_bps", "sum"), mtd=("mtd_bps", "sum"),
                           ytd=("ytd_bps", "sum"), m12=("m12_bps", "sum"))
                      .sort_values("ytd", key=lambda s: s.abs(), ascending=False))
                for _, pp in pr.iterrows():
                    rows_html.append(
                        f'<tr class="lvl3" data-row-parent="{liv_id}" '
                        f'style="display:none">'
                        f'<td style="padding-left:72px;color:#7a8290;font-size:11px">{_esc(pp["PRODUCT"])}</td>'
                        f'{_bps_cell(pp["dia"])}{_bps_cell(pp["mtd"])}'
                        f'{_bps_cell(pp["ytd"])}{_bps_cell(pp["m12"])}'
                        f'</tr>'
                    )

    # Total row (verification — should match canonical PA total)
    tot = df[["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]].sum()
    rows_html.append(
        f'<tr class="total" style="border-top:2px solid #555;font-weight:700;background:#0d1626">'
        f'<td style="padding-top:8px">TOTAL MACRO</td>'
        f'{_bps_cell(tot["dia_bps"], True)}{_bps_cell(tot["mtd_bps"], True)}'
        f'{_bps_cell(tot["ytd_bps"], True)}{_bps_cell(tot["m12_bps"], True)}'
        f'</tr>'
    )

    head = (
        '<thead><tr style="border-bottom:1px solid #333;color:#9aa3b2;font-size:11px">'
        '<th style="text-align:left;padding:6px 10px">CATEGORIA</th>'
        '<th class="num">DIA</th><th class="num">MTD</th>'
        '<th class="num">YTD</th><th class="num">12M</th>'
        '</tr></thead>'
    )
    body = "<tbody>" + "".join(rows_html) + "</tbody>"
    return f'<table class="pa-tree">{head}{body}</table>'


def _build_top_block(df: pd.DataFrame, win: str, label: str) -> str:
    """5 piores + 5 melhores por (CLASSE_NEW, GRUPO_NEW, PRODUCT) numa janela.
       Exclui linhas de 'FX Basis Risk & Carry' (destaques puramente de efeito-ativo).
    """
    col = f"{win}_bps"
    df_asset = df[df["CLASSE_NEW"] != "FX Basis Risk & Carry"]
    g = (df_asset.groupby(["CLASSE_NEW", "GRUPO_NEW", "PRODUCT"], as_index=False)
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
            f'<span style="color:#888">[{_esc(r["CLASSE_NEW"][:3])}/{_esc(r["GRUPO_NEW"][:8])}]</span> '
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


CSS = PA_FX_SPLIT_CSS_BASE
JS = PA_FX_SPLIT_JS_TOGGLE


def _build_verification_block(df: pd.DataFrame) -> str:
    """Sanity table: confirm each new top-level CLASSE_NEW total equals the
    sum of corresponding *original* rows (since this is pure regrouping)."""
    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]

    # Per-CLASSE_NEW totals from new view
    new_tot = df.groupby("CLASSE_NEW")[cols].sum()

    # Original CLASSE totals (canonical PA — what each row WAS labeled as).
    # Includes the DB-native 'FX Carry & Bases Risk' (since 2026-04-24) plus
    # legacy 'BRLUSD'/'FX' rows kept for pre-cutover history.
    old_tot = df.groupby("CLASSE")[cols].sum()
    fx_old  = old_tot.loc[old_tot.index.isin(_FX_CLASSES)][cols].sum()

    rows = []
    rows.append((
        "Total MACRO (todas as linhas)",
        df[cols].sum(),
        df[cols].sum(),  # tautological — both views must equal
    ))
    # Commodities: should match exactly (no row moved in/out)
    if "Commodities" in old_tot.index and "Commodities" in new_tot.index:
        rows.append(("Commodities (CLASSE='Commodities' preservado)",
                     old_tot.loc["Commodities"], new_tot.loc["Commodities"]))
    # RV Intl
    if "RV Intl" in old_tot.index and "RV Intl" in new_tot.index:
        rows.append(("RV Intl (preservado)",
                     old_tot.loc["RV Intl"], new_tot.loc["RV Intl"]))
    # RF Intl
    if "RF Intl" in old_tot.index and "RF Intl" in new_tot.index:
        rows.append(("RF Intl (preservado)",
                     old_tot.loc["RF Intl"], new_tot.loc["RF Intl"]))
    # FX Basis = old BRLUSD + FX + DB-native 'FX Carry & Bases Risk'
    if "FX Basis Risk & Carry" in new_tot.index:
        rows.append(("FX Basis Risk & Carry (= BRLUSD + FX + FX Carry & Bases Risk)",
                     fx_old, new_tot.loc["FX Basis Risk & Carry"]))

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
        ok = max_diff < 0.05  # less than 0.05 bps tolerance
        marker = '<span style="color:#26a65b">✓</span>' if ok else f'<span style="color:#e74c3c">⚠ {max_diff:.2f} bps</span>'
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


def _build_html(df: pd.DataFrame, date_str: str) -> str:
    tree_html = _build_tree_table(df)
    top_blocks = "".join(
        _build_top_block(df, win, label) for win, label in WINDOWS[:3]
    )
    verif_html = _build_verification_block(df)
    tot = df[["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]].sum()

    return f"""<!doctype html>
<html lang="pt-BR"><head>
<meta charset="utf-8">
<title>MACRO PA — FX Split — {date_str}</title>
<style>{CSS}</style>
</head><body>
<h1>MACRO Performance Attribution — FX-segregated view</h1>
<div class="sub">{date_str} · q_models.REPORT_ALPHA_ATRIBUTION · re-categorizado · total idêntico ao PA canônico</div>

<div class="legend" style="margin:0 0 16px">
  <b>FX Basis Risk &amp; Carry</b> agrega:
  <i>FX em Commodities/RV Intl/RF Intl</i> (efeito do câmbio sobre exposições USD-denominadas) +
  <i>FX Spot &amp; Futuros</i> (USD Brasil hedge + posições FX direcionais + cross-FX EUR/JPY/CAD + custos cambiais).
  Demais categorias mantêm o efeito-ativo natural (commodity em BRL, equity intl em BRL, etc.).
  <b>Total = idêntico ao PA original</b> — esta é uma reordenação, não recálculo.
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
  Sanity: TOTAL desta tabela = soma de todas as linhas DIA/MTD/YTD/12M originais (CLASSE×GRUPO×PRODUTO) — a recategorização só renomeia top-level &quot;BRLUSD&quot;+&quot;FX&quot;+&quot;FX Carry &amp; Bases Risk&quot; (DB nativo desde 2026-04-24) em &quot;FX Basis Risk &amp; Carry&quot;.
</div>

<script>{JS}</script>
</body></html>"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=DATA_STR, help="Reference date YYYY-MM-DD")
    p.add_argument("--out", default=None, help="Output html path")
    args = p.parse_args()

    print(f"Fetching MACRO PA for {args.date}...")
    df = _fetch_macro_pa(args.date)
    if df.empty:
        print("No data — nothing written.")
        return 1
    df = _apply_remap(df)

    out_path = Path(args.out) if args.out else OUT_DIR / f"{args.date}_macro_pa_fx_split.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    html = _build_html(df, args.date)
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved: {out_path}")

    # Sanity numbers to console
    tot = df[["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]].sum()
    print(f"\nTotals (bps):  DIA={tot['dia_bps']:+.1f}  MTD={tot['mtd_bps']:+.1f}  "
          f"YTD={tot['ytd_bps']:+.1f}  12M={tot['m12_bps']:+.1f}")
    fx = df[df["CLASSE_NEW"] == "FX Basis Risk & Carry"][["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]].sum()
    print(f"FX Basis (bps): DIA={fx['dia_bps']:+.1f}  MTD={fx['mtd_bps']:+.1f}  "
          f"YTD={fx['ytd_bps']:+.1f}  12M={fx['m12_bps']:+.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

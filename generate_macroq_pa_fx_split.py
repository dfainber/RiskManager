"""MACRO_Q (Galapagos Global Macro Q) Performance Attribution — FX-segregated view.

Two side-by-side views, switched via the "FX Consolidado" toggle:

  • Default — FX-split nested per LIVRO (same shape as QUANT/EVOLUTION/MACRO):
       LIVRO → CLASSE_NEW (BRLUSD/FX → "FX Basis Risk & Carry")
             → GRUPO_NEW → PRODUCT

  • FX Consolidado — every BRLUSD/FX row across all LIVROs is lifted into a
    single top-level row "FX Basis Risk & Carry" placed above Caixa / Taxas
    e Custos. Other LIVROs show only their asset-effect rows. The total of
    both views is identical (pure regrouping, no recalculation).

In REPORT_ALPHA_ATRIBUTION, MACRO_Q lives under FUNDO='GLOBAL'.

Output: data/morning-calls/<date>_macroq_pa_fx_split.html
"""
from __future__ import annotations

import argparse
import html as html_lib
import sys
from pathlib import Path

import pandas as pd

from glpg_fetch import read_sql
from risk_runtime import DATA_STR, OUT_DIR


WINDOWS = [("dia", "DIA"), ("mtd", "MTD"), ("ytd", "YTD"), ("m12", "12M")]
LIVRO_PINNED_BOTTOM = {"Caixa", "Caixa USD", "Taxas e Custos", "Custos"}
FX_BASIS_LABEL = "FX Basis Risk & Carry"


def _fetch_macroq_pa(date_str: str) -> pd.DataFrame:
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
      SUM(CASE WHEN "DATE" >  (DATE '{date_str}' - INTERVAL '12 months')
                AND "DATE" <= DATE '{date_str}'
               THEN "DIA" ELSE 0 END) * 10000 AS m12_bps
    FROM q_models."REPORT_ALPHA_ATRIBUTION"
    WHERE "FUNDO" = 'GLOBAL'
      AND "DATE" >  (DATE '{date_str}' - INTERVAL '12 months')
      AND "DATE" <= DATE '{date_str}'
    GROUP BY "CLASSE", "GRUPO", "SUBCLASSE", "LIVRO", "BOOK", "PRODUCT"
    HAVING ABS(SUM(CASE WHEN "DATE" = DATE '{date_str}' THEN "DIA" ELSE 0 END)) > 1e-9
        OR ABS(SUM("DIA")) > 1e-9
    """
    df = read_sql(q)
    for c in ("dia_bps", "mtd_bps", "ytd_bps", "m12_bps"):
        df[c] = df[c].astype(float).fillna(0.0)
    return df


_FX_CLASSES = ("BRLUSD", "FX", "FX Carry & Bases Risk")
_FX_GRUPO_MAP = {
    "Commodities":     "FX em Commodities",
    "Precious Metals": "FX em Precious Metals",
    "RV Intl":         "FX em RV Intl",
    "RF Intl":         "FX em RF Intl",
}


def _remap_classe(classe: str, grupo: str) -> tuple[str, str]:
    """Fold legacy ('BRLUSD'/'FX') and DB-native ('FX Carry & Bases Risk',
    emitted since 2026-04-24) into a single FX_BASIS_LABEL bucket."""
    if classe in _FX_CLASSES:
        grupo_clean = (grupo or "").strip()
        return (FX_BASIS_LABEL,
                _FX_GRUPO_MAP.get(grupo_clean, "FX Spot & Futuros"))
    return (classe, grupo)


def _apply_remap(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    new = out.apply(lambda r: _remap_classe(r["CLASSE"], r["GRUPO"]), axis=1)
    out["CLASSE_NEW"] = [t[0] for t in new]
    out["GRUPO_NEW"]  = [t[1] for t in new]
    return out


# ─── Render helpers ──────────────────────────────────────────────────────────
def _bps_color(v: float) -> str:
    if v > 0.5:  return "#26a65b"
    if v < -0.5: return "#e74c3c"
    return "#9aa3b2"


def _bps_cell(v: float, bold: bool = False) -> str:
    if abs(v) < 0.05:
        return '<td class="num" style="color:#666">—</td>'
    col = _bps_color(v)
    pct = v / 100.0
    weight = "font-weight:700;" if bold else ""
    return f'<td class="num" style="color:{col};{weight}">{pct:+.2f}%</td>'


def _esc(s: str) -> str:
    return html_lib.escape(str(s) if s is not None else "")


def _classe_order_key(c: str) -> int:
    order = [
        "RF BZ IPCA", "RF BZ Pré-fixado", "RF BZ", "RF Intl",
        "RV BZ", "RV Intl", "Commodities", "ETF Options", FX_BASIS_LABEL,
        "Caixa", "Custos",
    ]
    return order.index(c) if c in order else 99


# ─── DEFAULT VIEW: LIVRO → CLASSE_NEW → GRUPO_NEW → PRODUCT ──────────────
def _build_default_tree(df: pd.DataFrame, table_id: str) -> str:
    if df.empty:
        return '<div style="color:#888">Sem dados.</div>'
    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    rows: list[str] = []

    # Level-0: LIVRO
    lv_tot = df.groupby("LIVRO", as_index=False)[cols].sum()
    lv_tot["_pin"] = lv_tot["LIVRO"].apply(lambda x: 1 if x in LIVRO_PINNED_BOTTOM else 0)
    lv_tot["_abs12m"] = lv_tot["m12_bps"].abs()
    lv_tot = lv_tot.sort_values(["_pin", "_abs12m"], ascending=[True, False])

    for _, lr in lv_tot.iterrows():
        liv = lr["LIVRO"] or "—"
        lv_id = "L_" + str(liv).replace(" ", "_").replace("/", "_").replace("&", "and")
        rows.append(_lvl0_row(lv_id, _esc(liv), lr, "#e6e6e6"))

        cl_tot = (df[df["LIVRO"] == lr["LIVRO"]]
                  .groupby("CLASSE_NEW", as_index=False)[cols].sum())
        cl_tot["_ord"] = cl_tot["CLASSE_NEW"].apply(_classe_order_key)
        cl_tot = cl_tot.sort_values(["_ord", "ytd_bps"],
                                    ascending=[True, False],
                                    key=lambda s: s.abs() if s.name == "ytd_bps" else s)
        for _, cr in cl_tot.iterrows():
            cls = cr["CLASSE_NEW"]
            is_fx = cls == FX_BASIS_LABEL
            cls_color = "#5aa3e8" if is_fx else "#cfd6e0"
            cls_id = f"{lv_id}__C_{cls.replace(' ', '_').replace('&', 'and')}"
            rows.append(_lvl1_row(lv_id, cls_id, _esc(cls), cls_color, cr))

            gr_tot = (df[(df["LIVRO"] == lr["LIVRO"]) & (df["CLASSE_NEW"] == cls)]
                      .groupby("GRUPO_NEW", as_index=False)[cols].sum()
                      .sort_values("ytd_bps", key=lambda s: s.abs(), ascending=False))
            for _, gr in gr_tot.iterrows():
                grp = gr["GRUPO_NEW"]
                grp_id = f"{cls_id}__G_{grp.replace(' ', '_').replace('&', 'and')}"
                rows.append(_lvl2_row(cls_id, grp_id, _esc(grp), gr))
                pr = (df[(df["LIVRO"] == lr["LIVRO"]) & (df["CLASSE_NEW"] == cls)
                         & (df["GRUPO_NEW"] == grp)]
                      .groupby("PRODUCT", as_index=False)[cols].sum()
                      .sort_values("ytd_bps", key=lambda s: s.abs(), ascending=False))
                for _, pp in pr.iterrows():
                    rows.append(_lvl3_row(grp_id, _esc(pp["PRODUCT"]), pp))

    rows.append(_total_row("TOTAL MACRO_Q", df[cols].sum()))
    return _wrap_table(table_id, rows)


# ─── CONSOLIDATED VIEW: FX Basis lifted to top-level ───────────────────────
def _build_consolidated_tree(df: pd.DataFrame, table_id: str) -> str:
    """Same hierarchy except CLASSE='BRLUSD'/'FX' rows are pulled out of their
    LIVROs into a single top-level 'FX Basis Risk & Carry' row (no children),
    placed above Caixa/Caixa USD/Taxas e Custos."""
    if df.empty:
        return '<div style="color:#888">Sem dados.</div>'
    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    rows: list[str] = []

    # Split: asset-effect rows vs FX-basis rows
    is_fx = df["CLASSE_NEW"] == FX_BASIS_LABEL
    df_asset = df[~is_fx]
    df_fx    = df[is_fx]

    # Order: asset LIVROs (by |12M|) → FX Basis line → pinned LIVROs at the bottom
    if df_asset.empty:
        asset_lvs = pd.DataFrame(columns=["LIVRO"] + cols)
    else:
        lv_tot = df_asset.groupby("LIVRO", as_index=False)[cols].sum()
        lv_tot["_pin"] = lv_tot["LIVRO"].apply(lambda x: 1 if x in LIVRO_PINNED_BOTTOM else 0)
        lv_tot["_abs12m"] = lv_tot["m12_bps"].abs()
        lv_tot = lv_tot.sort_values(["_pin", "_abs12m"], ascending=[True, False])
        asset_lvs = lv_tot

    asset_lvs_top = asset_lvs[asset_lvs["_pin"] == 0] if "_pin" in asset_lvs.columns else asset_lvs
    asset_lvs_pin = asset_lvs[asset_lvs["_pin"] == 1] if "_pin" in asset_lvs.columns else pd.DataFrame()

    # 1) Top (non-pinned) asset LIVROs
    for _, lr in asset_lvs_top.iterrows():
        rows.extend(_render_livro_subtree(df_asset, lr, cols))

    # 2) FX Basis Risk & Carry (single top-level line, no children)
    if not df_fx.empty:
        fx_tot = df_fx[cols].sum()
        rows.append(
            f'<tr class="lvl0 fx-consol-row" '
            f'style="background:#1a2030;border-left:3px solid #5aa3e8">'
            f'<td><span style="color:#5aa3e8;font-weight:700">'
            f'≡ {_esc(FX_BASIS_LABEL)}</span> '
            f'<span style="color:#7a8290;font-size:10px">'
            f'(consolidado · BRLUSD + FX de todos os LIVROs)</span></td>'
            f'{_bps_cell(fx_tot["dia_bps"], True)}{_bps_cell(fx_tot["mtd_bps"], True)}'
            f'{_bps_cell(fx_tot["ytd_bps"], True)}{_bps_cell(fx_tot["m12_bps"], True)}'
            f'</tr>'
        )

    # 3) Pinned (Caixa / Custos) asset LIVROs
    for _, lr in asset_lvs_pin.iterrows():
        rows.extend(_render_livro_subtree(df_asset, lr, cols))

    rows.append(_total_row("TOTAL MACRO_Q", df[cols].sum()))
    return _wrap_table(table_id, rows)


def _render_livro_subtree(df_asset: pd.DataFrame, lr: pd.Series,
                          cols: list[str]) -> list[str]:
    """Render LIVRO → CLASSE_NEW → GRUPO_NEW → PRODUCT, asset-only."""
    out: list[str] = []
    liv = lr["LIVRO"] or "—"
    lv_id = "AL_" + str(liv).replace(" ", "_").replace("/", "_").replace("&", "and")
    out.append(_lvl0_row(lv_id, _esc(liv), lr, "#e6e6e6"))

    cl_tot = (df_asset[df_asset["LIVRO"] == lr["LIVRO"]]
              .groupby("CLASSE_NEW", as_index=False)[cols].sum())
    if cl_tot.empty:
        return out
    cl_tot["_ord"] = cl_tot["CLASSE_NEW"].apply(_classe_order_key)
    cl_tot = cl_tot.sort_values(["_ord", "ytd_bps"],
                                ascending=[True, False],
                                key=lambda s: s.abs() if s.name == "ytd_bps" else s)
    for _, cr in cl_tot.iterrows():
        cls = cr["CLASSE_NEW"]
        cls_id = f"{lv_id}__C_{cls.replace(' ', '_').replace('&', 'and')}"
        out.append(_lvl1_row(lv_id, cls_id, _esc(cls), "#cfd6e0", cr))
        gr_tot = (df_asset[(df_asset["LIVRO"] == lr["LIVRO"])
                           & (df_asset["CLASSE_NEW"] == cls)]
                  .groupby("GRUPO_NEW", as_index=False)[cols].sum()
                  .sort_values("ytd_bps", key=lambda s: s.abs(), ascending=False))
        for _, gr in gr_tot.iterrows():
            grp = gr["GRUPO_NEW"]
            grp_id = f"{cls_id}__G_{grp.replace(' ', '_').replace('&', 'and')}"
            out.append(_lvl2_row(cls_id, grp_id, _esc(grp), gr))
            pr = (df_asset[(df_asset["LIVRO"] == lr["LIVRO"])
                           & (df_asset["CLASSE_NEW"] == cls)
                           & (df_asset["GRUPO_NEW"] == grp)]
                  .groupby("PRODUCT", as_index=False)[cols].sum()
                  .sort_values("ytd_bps", key=lambda s: s.abs(), ascending=False))
            for _, pp in pr.iterrows():
                out.append(_lvl3_row(grp_id, _esc(pp["PRODUCT"]), pp))
    return out


# ─── Row builders ──────────────────────────────────────────────────────────
def _lvl0_row(lv_id: str, label: str, r: pd.Series, color: str) -> str:
    return (f'<tr class="lvl0" data-row-id="{lv_id}" onclick="paToggle(this)" '
            f'style="cursor:pointer;background:#1a2030">'
            f'<td><span class="caret">▶</span> '
            f'<span style="color:{color};font-weight:700">{label}</span></td>'
            f'{_bps_cell(r["dia_bps"], True)}{_bps_cell(r["mtd_bps"], True)}'
            f'{_bps_cell(r["ytd_bps"], True)}{_bps_cell(r["m12_bps"], True)}'
            f'</tr>')


def _lvl1_row(parent: str, cid: str, label: str, color: str, r: pd.Series) -> str:
    return (f'<tr class="lvl1" data-row-parent="{parent}" data-row-id="{cid}" '
            f'onclick="paToggle(this)" style="cursor:pointer;display:none">'
            f'<td style="padding-left:24px"><span class="caret">▶</span> '
            f'<span style="color:{color};font-weight:600">{label}</span></td>'
            f'{_bps_cell(r["dia_bps"])}{_bps_cell(r["mtd_bps"])}'
            f'{_bps_cell(r["ytd_bps"])}{_bps_cell(r["m12_bps"])}'
            f'</tr>')


def _lvl2_row(parent: str, gid: str, label: str, r: pd.Series) -> str:
    return (f'<tr class="lvl2" data-row-parent="{parent}" data-row-id="{gid}" '
            f'onclick="paToggle(this)" style="cursor:pointer;display:none">'
            f'<td style="padding-left:48px"><span class="caret">▶</span> '
            f'<span style="color:#9aa3b2">{label}</span></td>'
            f'{_bps_cell(r["dia_bps"])}{_bps_cell(r["mtd_bps"])}'
            f'{_bps_cell(r["ytd_bps"])}{_bps_cell(r["m12_bps"])}'
            f'</tr>')


def _lvl3_row(parent: str, label: str, r: pd.Series) -> str:
    return (f'<tr class="lvl3" data-row-parent="{parent}" '
            f'style="display:none">'
            f'<td style="padding-left:72px;color:#7a8290;font-size:11px">{label}</td>'
            f'{_bps_cell(r["dia_bps"])}{_bps_cell(r["mtd_bps"])}'
            f'{_bps_cell(r["ytd_bps"])}{_bps_cell(r["m12_bps"])}'
            f'</tr>')


def _total_row(label: str, tot: pd.Series) -> str:
    return (f'<tr class="total" style="border-top:2px solid #555;font-weight:700;background:#0d1626">'
            f'<td style="padding-top:8px">{label}</td>'
            f'{_bps_cell(tot["dia_bps"], True)}{_bps_cell(tot["mtd_bps"], True)}'
            f'{_bps_cell(tot["ytd_bps"], True)}{_bps_cell(tot["m12_bps"], True)}'
            f'</tr>')


def _wrap_table(table_id: str, rows: list[str]) -> str:
    head = (
        '<thead><tr style="border-bottom:1px solid #333;color:#9aa3b2;font-size:11px">'
        '<th style="text-align:left;padding:6px 10px">CATEGORIA</th>'
        f'<th class="num sortable" onclick="evoSortBy(\'{table_id}\',1)">DIA</th>'
        f'<th class="num sortable" onclick="evoSortBy(\'{table_id}\',2)">MTD</th>'
        f'<th class="num sortable" onclick="evoSortBy(\'{table_id}\',3)">YTD</th>'
        f'<th class="num sortable" onclick="evoSortBy(\'{table_id}\',4)">12M</th>'
        '</tr></thead>'
    )
    body = "<tbody>" + "".join(rows) + "</tbody>"
    return f'<table class="pa-tree" id="{table_id}">{head}{body}</table>'


# ─── Top contributors block ─────────────────────────────────────────────────
def _build_top_block(df: pd.DataFrame, win: str, label: str) -> str:
    col = f"{win}_bps"
    df_asset = df[df["CLASSE_NEW"] != FX_BASIS_LABEL]
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
    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    new_classe_global = df.groupby("CLASSE_NEW")[cols].sum()
    old_classe_global = df.groupby("CLASSE")[cols].sum()
    fx_old_global = old_classe_global.loc[old_classe_global.index.isin(["BRLUSD", "FX"])][cols].sum()

    rows = [("TOTAL MACRO_Q", df[cols].sum(), df[cols].sum())]
    for bucket in ("Commodities", "RV Intl", "RF Intl"):
        if bucket in old_classe_global.index and bucket in new_classe_global.index:
            rows.append((f"{bucket} (preservado)",
                         old_classe_global.loc[bucket],
                         new_classe_global.loc[bucket]))
    if FX_BASIS_LABEL in new_classe_global.index:
        rows.append(("FX Basis Risk & Carry (= antigo BRLUSD + FX)",
                     fx_old_global, new_classe_global.loc[FX_BASIS_LABEL]))
    for liv in sorted(df["LIVRO"].dropna().unique().tolist()):
        sub = df[df["LIVRO"] == liv]
        if sub.empty:
            continue
        old_l = sub.groupby("CLASSE")[cols].sum()
        new_l = sub.groupby("CLASSE_NEW")[cols].sum()
        for bucket in ("Commodities", "RV Intl", "RF Intl"):
            if bucket in old_l.index and bucket in new_l.index:
                ov = old_l.loc[bucket]; nv = new_l.loc[bucket]
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
        max_diff = max(diffs); ok = max_diff < 0.05
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


CSS = """
* { box-sizing:border-box }
body { background:#0a0f1a; color:#e6e6e6; font-family:'Segoe UI',system-ui,sans-serif; margin:0; padding:24px; }
h1 { font-size:18px; margin:0 0 4px; color:#5aa3e8 }
.sub { color:#888; font-size:12px; margin-bottom:16px }
table.pa-tree { width:100%; border-collapse:collapse; background:#0d1626; border:1px solid #1f2940; border-radius:8px; overflow:hidden }
table.pa-tree th, table.pa-tree td { padding:6px 10px; font-size:12px }
table.pa-tree td.num, table.pa-tree th.num { text-align:right; font-variant-numeric:tabular-nums; min-width:70px }
table.pa-tree tr:hover { background:#15203a }
.caret { display:inline-block; transition:transform 0.15s; font-size:9px; color:#5aa3e8; width:10px }
.caret.open { transform:rotate(90deg) }
.cards { display:grid; grid-template-columns:1fr 1fr 1fr; gap:14px; margin-top:20px }
.top-block { background:#0d1626; border:1px solid #1f2940; border-radius:8px; padding:12px 14px }
.top-title { font-weight:700; color:#5aa3e8; font-size:13px; margin-bottom:8px }
.top-sub { font-size:10px; font-weight:700; letter-spacing:0.05em; margin-bottom:4px }
.top-tbl { width:100%; border-collapse:collapse }
.top-tbl td { padding:3px 6px; font-size:11px; border-bottom:1px solid #1a1f30 }
.top-tbl td.num { text-align:right; font-variant-numeric:tabular-nums }
.legend { color:#888; font-size:11px; margin-top:18px; line-height:1.5 }
.legend b { color:#cfd6e0 }
.pa-toolbar-mini { display:flex; justify-content:flex-end; gap:8px; margin-bottom:6px; align-items:center }
.pa-btn { background:#1a2030; border:1px solid #2a3550; color:#cfd6e0; padding:4px 10px; font-size:11px; border-radius:4px; cursor:pointer; font-family:inherit }
.pa-btn:hover { background:#2a3550 }
.pa-btn.active { background:#1f4068; border-color:#5aa3e8; color:#fff }
th.sortable { cursor:pointer; user-select:none }
th.sortable:hover { color:#cfd6e0 }
th.sort-asc::after { content:' ▲'; color:#5aa3e8; font-size:9px }
th.sort-desc::after { content:' ▼'; color:#5aa3e8; font-size:9px }
.fx-consol-row td { padding-top:8px; padding-bottom:8px }
"""

JS = """
function paToggle(tr) {
  var id = tr.dataset.rowId;
  if (!id) return;
  var caret = tr.querySelector('.caret');
  // restrict to the same containing table
  var tbody = tr.parentElement;
  var rows = tbody.querySelectorAll('tr[data-row-parent="'+id+'"]');
  rows.forEach(function(r) {
    if (r.style.display === 'none') {
      r.style.display = '';
    } else {
      r.style.display = 'none';
      var subId = r.dataset.rowId;
      if (subId) {
        tbody.querySelectorAll('tr[data-row-parent^="'+subId+'"]').forEach(function(d) {
          d.style.display = 'none';
          var dc = d.querySelector('.caret');
          if (dc) dc.classList.remove('open');
        });
        var c = r.querySelector('.caret');
        if (c) c.classList.remove('open');
      }
    }
  });
  var anyVisible = Array.prototype.some.call(rows, function(r) { return r.style.display !== 'none'; });
  if (caret) caret.classList.toggle('open', anyVisible);
}

// Sort state per table
var _evoSort = {};        // tableId -> {col, asc}
var _evoOriginal = {};    // tableId -> [rows]

function _evoCacheOriginal(tableId) {
  if (_evoOriginal[tableId]) return;
  var tbody = document.querySelector('#'+tableId+' tbody');
  if (!tbody) return;
  _evoOriginal[tableId] = Array.prototype.slice.call(tbody.children);
}

function _evoCellNum(td, asc) {
  var t = (td && td.textContent || '').trim();
  if (!t || t === '—') return asc ? Infinity : -Infinity;
  return parseFloat(t.replace('%', '').replace('+', '').replace(',', '.')) || 0;
}

function evoSortBy(tableId, colIdx) {
  _evoCacheOriginal(tableId);
  var tbody = document.querySelector('#'+tableId+' tbody');
  if (!tbody) return;
  var st = _evoSort[tableId] || { col: null, asc: false };
  if (st.col === colIdx) st.asc = !st.asc;
  else { st.col = colIdx; st.asc = false; }
  _evoSort[tableId] = st;

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
      var va = _evoCellNum(a.cells[colIdx], st.asc);
      var vb = _evoCellNum(b.cells[colIdx], st.asc);
      return st.asc ? va - vb : vb - va;
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

  document.querySelectorAll('#'+tableId+' th').forEach(function(th) {
    th.classList.remove('sort-asc', 'sort-desc');
  });
  var ths = document.querySelectorAll('#'+tableId+' th');
  if (ths[colIdx]) ths[colIdx].classList.add(st.asc ? 'sort-asc' : 'sort-desc');
}

function evoResetSort(tableId) {
  _evoCacheOriginal(tableId);
  var tbody = document.querySelector('#'+tableId+' tbody');
  var orig = _evoOriginal[tableId];
  if (!tbody || !orig) return;
  orig.forEach(function(r) { tbody.appendChild(r); });
  _evoSort[tableId] = { col: null, asc: false };
  document.querySelectorAll('#'+tableId+' th').forEach(function(th) {
    th.classList.remove('sort-asc', 'sort-desc');
  });
}

function evoExpandAll(tableId) {
  var tbody = document.querySelector('#'+tableId+' tbody');
  if (!tbody) return;
  Array.prototype.forEach.call(tbody.children, function(r) {
    if (r.classList.contains('total')) return;
    r.style.display = '';
    var c = r.querySelector('.caret');
    if (c && r.dataset.rowId) c.classList.add('open');
  });
}

function evoCollapseAll(tableId) {
  var tbody = document.querySelector('#'+tableId+' tbody');
  if (!tbody) return;
  Array.prototype.forEach.call(tbody.children, function(r) {
    if (r.classList.contains('total')) return;
    if (r.dataset.rowParent) r.style.display = 'none';
    var c = r.querySelector('.caret');
    if (c) c.classList.remove('open');
  });
}

// View toggle: 'default' vs 'consolidated'
function evoSetView(view) {
  var d = document.getElementById('view-default');
  var c = document.getElementById('view-consolidated');
  var bd = document.getElementById('btn-view-default');
  var bc = document.getElementById('btn-view-consolidated');
  if (view === 'consolidated') {
    if (d) d.style.display = 'none';
    if (c) c.style.display = 'block';
    if (bd) bd.classList.remove('active');
    if (bc) bc.classList.add('active');
  } else {
    if (d) d.style.display = 'block';
    if (c) c.style.display = 'none';
    if (bd) bd.classList.add('active');
    if (bc) bc.classList.remove('active');
  }
}

document.addEventListener('DOMContentLoaded', function() {
  _evoCacheOriginal('macroq-default');
  _evoCacheOriginal('macroq-consolidated');
});
"""


def _build_html(df: pd.DataFrame, date_str: str) -> str:
    default_tree = _build_default_tree(df, "macroq-default")
    consol_tree = _build_consolidated_tree(df, "macroq-consolidated")
    top_blocks = "".join(_build_top_block(df, win, label) for win, label in WINDOWS[:3])
    verif_html = _build_verification_block(df)

    toolbar_default = (
        '<div class="pa-toolbar-mini">'
        '<button class="pa-btn" onclick="evoExpandAll(\'macroq-default\')">⤢ Expandir</button>'
        '<button class="pa-btn" onclick="evoCollapseAll(\'macroq-default\')">⤡ Colapsar</button>'
        '<button class="pa-btn" onclick="evoResetSort(\'macroq-default\')">↺ Reset</button>'
        '</div>'
    )
    toolbar_consol = (
        '<div class="pa-toolbar-mini">'
        '<button class="pa-btn" onclick="evoExpandAll(\'macroq-consolidated\')">⤢ Expandir</button>'
        '<button class="pa-btn" onclick="evoCollapseAll(\'macroq-consolidated\')">⤡ Colapsar</button>'
        '<button class="pa-btn" onclick="evoResetSort(\'macroq-consolidated\')">↺ Reset</button>'
        '</div>'
    )
    view_toggle = (
        '<div class="pa-toolbar-mini" style="justify-content:flex-start;margin-bottom:14px">'
        '<span style="color:#9aa3b2;font-size:11px;margin-right:6px">View:</span>'
        '<button class="pa-btn active" id="btn-view-default" '
        'onclick="evoSetView(\'default\')">FX Detalhado</button>'
        '<button class="pa-btn" id="btn-view-consolidated" '
        'onclick="evoSetView(\'consolidated\')">FX Consolidado</button>'
        '</div>'
    )

    return f"""<!doctype html>
<html lang="pt-BR"><head>
<meta charset="utf-8">
<title>MACRO_Q PA — FX Split — {date_str}</title>
<style>{CSS}</style>
</head><body>
<h1>MACRO_Q (Galapagos Global Macro Q) Performance Attribution — FX-segregated view</h1>
<div class="sub">{date_str} · q_models.REPORT_ALPHA_ATRIBUTION (FUNDO='GLOBAL') · total idêntico ao PA canônico</div>

<div class="legend" style="margin:0 0 16px">
  <b>FX Detalhado</b>: hierarquia LIVRO → CLASSE (com FX-split nested) → GRUPO → PRODUCT.<br>
  <b>FX Consolidado</b>: BRLUSD + FX de <i>todos</i> os LIVROs unificados em uma única linha top-level
  &quot;FX Basis Risk &amp; Carry&quot;, posicionada acima de Caixa/Caixa USD/Taxas e Custos.
  Nas demais linhas top-level só aparece o efeito-ativo (não-FX). <b>Totais idênticos.</b>
</div>

{view_toggle}

<div id="view-default" style="display:block">
{toolbar_default}
{default_tree}
</div>

<div id="view-consolidated" style="display:none">
{toolbar_consol}
{consol_tree}
</div>

<div class="sub" style="margin-top:24px">Verificação — totais novos vs originais (devem bater, é só reagrupamento)</div>
{verif_html}

<div class="sub" style="margin-top:24px">Top contribuintes &amp; detratores por janela — <b>excluindo FX Basis Risk &amp; Carry</b> (só efeito-ativo puro)</div>
<div class="cards">
{top_blocks}
</div>

<div class="legend">
  <b>DIA</b>: PnL realizado hoje · <b>MTD</b>: mês corrente · <b>YTD</b>: ano corrente · <b>12M</b>: últimos 12 meses · todos em % de NAV.<br>
  Sanity: TOTAL MACRO_Q desta tabela = soma de todas as linhas DIA/MTD/YTD/12M originais.
</div>

<script>{JS}</script>
</body></html>"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=DATA_STR, help="Reference date YYYY-MM-DD")
    p.add_argument("--out", default=None, help="Output html path")
    args = p.parse_args()

    print(f"Fetching MACRO_Q (GLOBAL) PA for {args.date}...")
    df = _fetch_macroq_pa(args.date)
    if df.empty:
        print("No data — nothing written.")
        return 1
    df = _apply_remap(df)

    out_path = Path(args.out) if args.out else OUT_DIR / f"{args.date}_macroq_pa_fx_split.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    html = _build_html(df, args.date)
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved: {out_path}")

    cols = ["dia_bps", "mtd_bps", "ytd_bps", "m12_bps"]
    tot = df[cols].sum()
    print(f"\nTotal MACRO_Q (bps):  DIA={tot['dia_bps']:+.1f}  MTD={tot['mtd_bps']:+.1f}  "
          f"YTD={tot['ytd_bps']:+.1f}  12M={tot['m12_bps']:+.1f}")
    fx = df[df["CLASSE_NEW"] == FX_BASIS_LABEL][cols].sum()
    print(f"FX Basis Risk & Carry:  DIA={fx['dia_bps']:+.1f}  MTD={fx['mtd_bps']:+.1f}  "
          f"YTD={fx['ytd_bps']:+.1f}  12M={fx['m12_bps']:+.1f}")
    print("\nPer-LIVRO totals (12M bps):")
    for liv, v in df.groupby("LIVRO")["m12_bps"].sum().sort_values(ascending=False).items():
        print(f"  {str(liv):20s} {v:+8.1f} bps")
    return 0


if __name__ == "__main__":
    sys.exit(main())

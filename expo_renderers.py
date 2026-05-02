"""
expo_renderers.py — Exposure-section renderers across every fund family.

One module aggregating the HTML section builders that turn exposure
dataframes (LOTE_PRODUCT_EXPO, VaR book maps, RF exposure maps, etc.)
into Morning-Call cards:

  _build_expo_unified_table       Shared factor × product unified table
  _prepare_quant_var_for_unified  QUANT VaR prep for the unified table
  build_quant_exposure_section    QUANT card (uses unified table)
  build_evolution_exposure_section EVOLUTION card (Strategy → Livro → Instr)
  build_idka_exposure_section     IDKA 3Y / 10Y card (3-way bench toggle)
  build_rf_exposure_map_section   Generic RF exposure map (IDKAs, ALBATROZ,
                                   MACRO look-through, EVOLUTION look-through)
  build_albatroz_exposure         ALBATROZ RF card with DV01 drill-down
  build_exposure_section          MACRO exposure (POSIÇÕES × PM VaR)
  build_frontier_exposure_section FRONTIER Ações card (vs IBOV/IBOD)

Pure HTML rendering. DB fetches stay in data_fetch. The only fetch these
helpers make themselves is fetch_rf_exposure_map (inside RF map card)
and _compute_idka_bench_replication (inside IDKA card) — both imported
from data_fetch.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from risk_runtime import fmt_br_num as _fmt_br_num
from risk_config import (
    _RF_ORDER, _RF_BUCKETS,
    _PM_LIVRO,
    _QUANT_FACTOR_ORDER, _QUANT_VAR_BOOK_FACTOR,
    _EVO_STRATEGY_ORDER,
)
from html_assets import UEXPO_JS as _UEXPO_JS
from data_fetch import _compute_idka_bench_replication
from vardod_renderers import build_vardod_trigger


def _build_expo_unified_table(
    fund_key: str,
    nav: float,
    df: pd.DataFrame,
    df_d1: pd.DataFrame | None,
    df_var: pd.DataFrame | None,
    df_var_d1: pd.DataFrame | None,
    factor_order: list,
    table_id: str | None = None,
    diversified_var_bps: float | None = None,
    diversified_var_bps_d1: float | None = None,
) -> str:
    """Render a unified Exposure factor × product table.
       Expected df columns: factor, PRODUCT, PRODUCT_CLASS, delta, pct_nav, sigma.
       Expected df_var columns: factor, PRODUCT, PRODUCT_CLASS, var_pct (bps, positive=loss).
       factor_order: ordered list of factor keys for initial display (default sort |Net| desc).
    """
    if df is None or df.empty:
        return ""
    tbl_id = table_id or f"tbl-uexpo-{fund_key}"

    # ── Aggregate to (factor, PRODUCT, PRODUCT_CLASS) at product level ────────
    prod = (df.assign(_abs=df["delta"].abs())
              .groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
              .agg(delta=("delta", "sum"),
                   gross_brl=("_abs", "sum"),
                   sigma=("sigma", "mean")))
    prod["net_pct"]   = prod["delta"]     * 100 / nav
    prod["gross_pct"] = prod["gross_brl"] * 100 / nav

    # D-1 product lookup
    d1_prod = {}
    if df_d1 is not None and not df_d1.empty:
        p1 = (df_d1.assign(_abs=df_d1["delta"].abs())
                    .groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                    .agg(delta=("delta", "sum")))
        p1["net_pct"] = p1["delta"] * 100 / nav
        for r in p1.itertuples(index=False):
            d1_prod[(r.factor, r.PRODUCT, r.PRODUCT_CLASS)] = r.net_pct

    # VaR at product level
    v_prod = {}
    if df_var is not None and not df_var.empty:
        vp = (df_var.groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                    .agg(var_pct=("var_pct", "sum")))
        for r in vp.itertuples(index=False):
            v_prod[(r.factor, r.PRODUCT, r.PRODUCT_CLASS)] = float(r.var_pct)

    v1_prod = {}
    if df_var_d1 is not None and not df_var_d1.empty:
        vp1 = (df_var_d1.groupby(["factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                         .agg(var_pct=("var_pct", "sum")))
        for r in vp1.itertuples(index=False):
            v1_prod[(r.factor, r.PRODUCT, r.PRODUCT_CLASS)] = float(r.var_pct)

    # ── Factor-level aggregates ───────────────────────────────────────────────
    def _sigma_weighted(sub: pd.DataFrame) -> float | None:
        s = sub.dropna(subset=["sigma"])
        w = s["net_pct"].abs()
        tot = w.sum()
        if tot <= 0:
            return None
        return float((w * s["sigma"]).sum() / tot)

    factors = []
    for f in prod["factor"].unique():
        sub = prod[prod["factor"] == f]
        net_pct   = float(sub["net_pct"].sum())
        gross_pct = float(sub["gross_pct"].sum())
        net_brl   = float(sub["delta"].sum())
        gross_brl = float(sub["gross_brl"].sum())
        sig       = _sigma_weighted(sub)
        var_pct   = None
        if v_prod:
            var_pct = sum(v_prod.get((f, r.PRODUCT, r.PRODUCT_CLASS), 0.0)
                          for r in sub.itertuples(index=False))
        # D-1 factor-level
        d1_net_pct = None
        if df_d1 is not None and not df_d1.empty:
            sub1 = df_d1[df_d1["factor"] == f]
            if not sub1.empty:
                d1_net_pct = float(sub1["delta"].sum()) * 100 / nav
        d1_var_pct = None
        if v1_prod:
            d1_var_pct = sum(v1_prod.get((f, r.PRODUCT, r.PRODUCT_CLASS), 0.0)
                             for r in sub.itertuples(index=False))
        factors.append(dict(
            factor=f, net_pct=net_pct, net_brl=net_brl,
            gross_pct=gross_pct, gross_brl=gross_brl,
            sigma=sig, var_pct=var_pct,
            d1_var_pct=d1_var_pct,
            d_expo=(net_pct - d1_net_pct) if d1_net_pct is not None else None,
            d_var=(var_pct - d1_var_pct) if (var_pct is not None and d1_var_pct is not None) else None,
            n_prods=len(sub),
        ))

    # Default sort: |Net| desc, with factor_order as tiebreaker
    order_idx = {f: i for i, f in enumerate(factor_order or [])}
    factors.sort(key=lambda r: (-abs(r["net_pct"]), order_idx.get(r["factor"], 99)))

    # ── Format helpers ────────────────────────────────────────────────────────
    def _num(v, n=1):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—", "var(--muted)"
        return f'{v:+.{n}f}', ("var(--up)" if v >= 0 else "var(--down)")

    def _abs_num(v, n=2, unit="%"):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        return f'{v:.{n}f}{unit}'

    def _money(v_brl):
        if v_brl is None or (isinstance(v_brl, float) and pd.isna(v_brl)):
            return "—"
        return _fmt_br_num(f'{v_brl/1e6:+,.1f}M')

    def _money_abs(v_brl):
        if v_brl is None or (isinstance(v_brl, float) and pd.isna(v_brl)):
            return "—"
        return _fmt_br_num(f'{v_brl/1e6:,.1f}M')

    def _dv(v):
        return "" if (v is None or (isinstance(v, float) and pd.isna(v))) else f'{v:.6f}'

    def _cell(txt, color=None, extra=""):
        col = f'color:{color};' if color else ''
        return (f'<td class="mono" style="text-align:right;font-size:11.5px;{col}{extra}" '
                f'>{txt}</td>')

    # ── Render factor rows + product children ────────────────────────────────
    body = ""
    tot_net = 0.0; tot_gross = 0.0; tot_net_brl = 0.0; tot_gross_brl = 0.0
    tot_var_abs = 0.0; any_var = False  # undiversified: Σ |VaR fator|
    tot_var_abs_d1 = 0.0; any_var_d1 = False  # for Δ vs D-1
    for _default_idx, g in enumerate(factors):
        f = g["factor"]
        tot_net       += g["net_pct"]
        tot_gross     += g["gross_pct"]
        tot_net_brl   += g["net_brl"]
        tot_gross_brl += g["gross_brl"]
        path = f'{fund_key}-' + f.replace(' ', '_').replace('(', '').replace(')', '')

        net_s, net_c = _num(g["net_pct"], 2)
        gross_s = _abs_num(g["gross_pct"], 1)
        dexp_s, dexp_c = _num(g["d_expo"], 2)
        sig_s = _abs_num(g["sigma"], 1, unit="")
        var_raw = g["var_pct"]
        if var_raw is None:
            var_s, var_c = "—", "var(--muted)"
        else:
            # Sign: dado (long, net≥0) → negative/green; tomado (short, net<0) → positive/red
            var_signed = var_raw * (-1 if g["net_pct"] >= 0 else 1)
            var_s = f'{var_signed:+.1f}'
            var_c = "var(--up)" if var_signed < 0 else "var(--down)"
            tot_var_abs += abs(var_raw); any_var = True
        if g.get("d1_var_pct") is not None:
            tot_var_abs_d1 += abs(g["d1_var_pct"]); any_var_d1 = True
        dvar_s, dvar_c = _num(g["d_var"], 1)

        body += (
            f'<tr class="uexpo-row" data-expo-lvl="0" data-expo-path="{path}" '
            f'onclick="uexpoToggle(this)" style="cursor:pointer; border-top:1px solid var(--border)" '
            f'data-default-order="{_default_idx}" '
            f'data-sort-name="{f}" '
            f'data-sort-net="{-abs(g["net_pct"]):.6f}" '
            f'data-sort-dexp="{_dv(g["d_expo"])}" '
            f'data-sort-gross="{-g["gross_pct"]:.6f}" '
            f'data-sort-sigma="{_dv(g["sigma"])}" '
            f'data-sort-var="{_dv(var_signed if var_raw is not None else None)}" '
            f'data-sort-dvar="{_dv(g["d_var"])}">'
            f'<td class="sum-fund" style="font-weight:700"><span class="uexpo-caret">▶</span> {f} '
            f'<span style="color:var(--muted);font-size:10px;font-weight:400">· {g["n_prods"]}</span></td>'
            + _cell(f'{net_s}%', net_c, "font-weight:700")
            + _cell(f'{dexp_s}%' if dexp_s != '—' else '—', dexp_c)
            + _cell(gross_s, "var(--muted)")
            + _cell(sig_s, "var(--muted)")
            + _cell(var_s, var_c)
            + _cell(f'{dvar_s}' if dvar_s != '—' else '—', dvar_c)
            + '</tr>'
        )

        # Product children — sort by |net_pct| desc
        sub_prods = prod[prod["factor"] == f].copy()
        sub_prods["_abs"] = sub_prods["net_pct"].abs()
        sub_prods = sub_prods.sort_values("_abs", ascending=False)
        for p in sub_prods.itertuples(index=False):
            key = (f, p.PRODUCT, p.PRODUCT_CLASS)
            p_net = float(p.net_pct)
            p_net_brl = float(p.delta)
            p_gross = float(p.gross_pct)
            p_gross_brl = float(p.gross_brl)
            p_sig = float(p.sigma) if pd.notna(p.sigma) else None
            p_var = v_prod.get(key)
            p_d1 = d1_prod.get(key)
            # Δ Expo: if D-1 data exists globally, a missing product key means a
            # NEW position today → delta = today − 0 = today. Without this, new
            # positions rendered "—" and children stopped summing to factor total.
            if df_d1 is not None and not df_d1.empty:
                p_dexp = p_net - (p_d1 if p_d1 is not None else 0.0)
            else:
                p_dexp = None
            # Same logic for Δ VaR: treat missing D-1 as 0 when D-1 data exists.
            if p_var is not None and df_var_d1 is not None and not df_var_d1.empty:
                p_dvar = p_var - v1_prod.get(key, 0.0)
            else:
                p_dvar = None

            net_col = "var(--up)" if p_net >= 0 else "var(--down)"
            if p_var is None:
                pvar_s, pvar_c = "—", "var(--muted)"
            else:
                p_var_signed = p_var * (-1 if p_net >= 0 else 1)
                pvar_s = f'{p_var_signed:+.1f}'
                pvar_c = "var(--up)" if p_var_signed < 0 else "var(--down)"
            pdexp_s, pdexp_c = _num(p_dexp, 2)
            pdvar_s, pdvar_c = _num(p_dvar, 1)
            p_sig_s = _abs_num(p_sig, 1, unit="")
            body += (
                f'<tr class="uexpo-row" data-expo-lvl="1" data-expo-parent="{path}" '
                f'style="display:none; background:var(--bg-alt, rgba(0,0,0,0.12))">'
                f'<td style="padding-left:28px; font-size:11px; color:var(--muted)">'
                f'  <span style="color:var(--text); font-weight:600">{p.PRODUCT}</span> '
                f'<span style="color:var(--muted); font-size:10px">({p.PRODUCT_CLASS})</span>'
                f'</td>'
                + _cell(f'{p_net:+.2f}%', net_col)
                + _cell(f'{pdexp_s}%' if pdexp_s != '—' else '—', pdexp_c)
                + _cell(f'{p_gross:.2f}%', "var(--muted)")
                + _cell(p_sig_s, "var(--muted)")
                + _cell(pvar_s, pvar_c)
                + _cell(pdvar_s if pdvar_s != '—' else '—', pdvar_c)
                + '</tr>'
            )

    # ── Total row (pinned) ────────────────────────────────────────────────────
    # Net %NAV total omitted (user: net can cross long/short and aggregate is
    # not meaningful). Gross total kept — sum of |exposures| is the book size.
    # VaR total = Σ |VaR fator| (undiversified). Diversification benefit shown
    # in the next pinned row when diversified_var_bps is provided.
    # Delta non-diversified (D-vs-D-1)
    d_tot_var = (tot_var_abs - tot_var_abs_d1) if (any_var and any_var_d1) else None
    d_tot_var_s, d_tot_var_c = _num(d_tot_var, 1)

    body += (
        '<tr class="pa-total-row" data-pinned="1" '
        'style="border-top:2px solid var(--border); font-weight:700">'
        '<td class="sum-fund" style="font-weight:700">Total <span style="color:var(--muted);font-size:10px;font-weight:400">não-diversificado</span></td>'
        + '<td></td>'
        + '<td></td>'
        + _cell(f'{tot_gross:.1f}%', "var(--text)", "font-weight:700")
        + '<td></td>'
        + _cell(f'{tot_var_abs:.1f}' if any_var else '—',
                "var(--text)" if any_var else "var(--muted)",
                "font-weight:700")
        + _cell(d_tot_var_s, d_tot_var_c, "font-weight:700")
        + '</tr>'
    )

    # ── Diversificado (HS portfolio VaR) — pinned row when caller passes it ──
    # Shows the fund's full-portfolio HS VaR alongside the undiversified sum,
    # so the diversification benefit is visible at a glance.
    if diversified_var_bps is not None and any_var:
        div_bps = abs(float(diversified_var_bps))
        benefit = tot_var_abs - div_bps  # positive = correlation reduces risk
        bf_s = f' <span style="color:var(--muted);font-size:10px;font-weight:400">· benefício {benefit:+.1f} bps</span>'
        # Delta diversified vs D-1
        d_div = None
        if diversified_var_bps_d1 is not None:
            d_div = div_bps - abs(float(diversified_var_bps_d1))
        d_div_s, d_div_c = _num(d_div, 1)
        body += (
            '<tr class="pa-total-row" data-pinned="1" '
            'style="font-weight:700">'
            '<td class="sum-fund" style="font-weight:700">Diversificado <span style="color:var(--muted);font-size:10px;font-weight:400">HS portfolio</span></td>'
            + '<td></td>'
            + '<td></td>'
            + '<td></td>'
            + '<td></td>'
            + _cell(f'{div_bps:.1f}{bf_s}', "var(--text)", "font-weight:700")
            + _cell(d_div_s, d_div_c, "font-weight:700")
            + '</tr>'
        )

    def _th(label, sort_key, active=False, align="right"):
        arrow = '↓' if active else ''
        act_cls = ' uexpo-sort-active' if active else ''
        return (f'<th class="uexpo-sort-th{act_cls}" data-expo-sort="{sort_key}" '
                f'style="text-align:{align}; cursor:pointer; user-select:none" '
                f'onclick="uexpoSort(this,\'{sort_key}\')">'
                f'{label} <span class="uexpo-arrow" '
                f'style="font-size:9px;opacity:{0.9 if active else 0}">{arrow}</span></th>')

    header = (
        _th("Fator",      "name",  active=False, align="left")
        + _th("Net %NAV",   "net",   active=True)
        + _th("Δ Expo",     "dexp",  active=False)
        + _th("Gross %NAV", "gross", active=False)
        + _th("σ (bps)",    "sigma", active=False)
        + _th("VaR (bps)",  "var",   active=False)
        + _th("Δ VaR",      "dvar",  active=False)
    )

    return (
        f'<table id="{tbl_id}" class="summary-table" data-no-sort="1">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{body}</tbody>'
        f'</table>'
    )


def _prepare_quant_var_for_unified(df_var: pd.DataFrame | None) -> pd.DataFrame | None:
    """Tag BOOK-level QUANT VaR with a factor column matching the Por Fator view."""
    if df_var is None or df_var.empty:
        return df_var
    out = df_var.copy()
    out["factor"] = out["BOOK"].map(_QUANT_VAR_BOOK_FACTOR).fillna("Outros")
    return out


def build_quant_exposure_section(df: pd.DataFrame, nav: float,
                                   df_d1: pd.DataFrame = None,
                                   df_var: pd.DataFrame = None,
                                   df_var_d1: pd.DataFrame = None,
                                   diversified_var_bps: float | None = None,
                                   diversified_var_bps_d1: float | None = None) -> str:
    """QUANT exposure card — two views:
       1. Por Fator: unified factor × product table (Net/Gross + σ + VaR + Δs).
       2. Por Livro: factor × livro matrix (which livro drives each factor).
    """
    if df is None or df.empty or not nav:
        return ""

    df_var_tagged    = _prepare_quant_var_for_unified(df_var)
    df_var_d1_tagged = _prepare_quant_var_for_unified(df_var_d1)
    if df_d1 is not None and not df_d1.empty:
        if "pct_nav" not in df_d1.columns:
            df_d1 = df_d1.copy()
            df_d1["pct_nav"] = df_d1["delta"] * 100 / nav
    if "pct_nav" not in df.columns:
        df = df.copy()
        df["pct_nav"] = df["delta"] * 100 / nav

    factor_table = _build_expo_unified_table(
        fund_key="quant",
        nav=nav,
        df=df,
        df_d1=df_d1,
        df_var=df_var_tagged,
        df_var_d1=df_var_d1_tagged,
        factor_order=_QUANT_FACTOR_ORDER,
        diversified_var_bps=diversified_var_bps,
        diversified_var_bps_d1=diversified_var_bps_d1,
    )

    # ── Por Livro summary (livro-level: Net, Δ Expo, σ, VaR signed, Δ VaR) ────
    # Aggregate exposure per BOOK
    livro_agg = (df.assign(_abs=df["delta"].abs())
                   .groupby("BOOK", as_index=False)
                   .agg(delta=("delta", "sum"),
                        gross_brl=("_abs", "sum"),
                        sigma=("sigma", "mean")))
    livro_agg["net_pct"]   = livro_agg["delta"]     * 100 / nav
    livro_agg["gross_pct"] = livro_agg["gross_brl"] * 100 / nav

    # Weighted-average σ per livro (weight = |net_pct|)
    def _livro_sigma(book):
        sub = df[df["BOOK"] == book].copy()
        sub = sub.dropna(subset=["sigma"])
        w = sub["delta"].abs()
        return float((w * sub["sigma"]).sum() / w.sum()) if w.sum() > 0 else None

    # VaR per livro from df_var_tagged (already has factor column; sum by BOOK)
    v_livro = {}
    if df_var_tagged is not None and not df_var_tagged.empty and "BOOK" in df_var_tagged.columns:
        vl = df_var_tagged.groupby("BOOK", as_index=False).agg(var_pct=("var_pct", "sum"))
        v_livro = dict(zip(vl["BOOK"], vl["var_pct"]))

    # D-1 per livro
    d1_livro_net = {}
    v1_livro = {}
    if df_d1 is not None and not df_d1.empty:
        d1g = (df_d1.groupby("BOOK", as_index=False).agg(delta=("delta", "sum")))
        d1_livro_net = {r.BOOK: r.delta * 100 / nav for r in d1g.itertuples(index=False)}
    if df_var_d1_tagged is not None and not df_var_d1_tagged.empty and "BOOK" in df_var_d1_tagged.columns:
        v1l = df_var_d1_tagged.groupby("BOOK", as_index=False).agg(var_pct=("var_pct", "sum"))
        v1_livro = dict(zip(v1l["BOOK"], v1l["var_pct"]))

    livro_agg = livro_agg.sort_values("net_pct", key=lambda s: s.abs(), ascending=False)

    def _c(txt, col, extra=""):
        return (f'<td class="mono" style="text-align:right;font-size:11.5px;color:{col};{extra}">'
                f'{txt}</td>')

    livro_rows = ""
    tot_net_l = tot_var_l = 0.0; any_var_l = False
    for r in livro_agg.itertuples(index=False):
        net_c   = "var(--up)" if r.net_pct >= 0 else "var(--down)"
        sig_val = _livro_sigma(r.BOOK)
        sig_s   = f'{sig_val:.1f}' if sig_val is not None else "—"
        # Δ Expo
        d1n = d1_livro_net.get(r.BOOK)
        dexp_raw = (r.net_pct - d1n) if d1n is not None else None
        dexp_s = f'{dexp_raw:+.2f}%' if dexp_raw is not None else "—"
        dexp_c = "var(--up)" if (dexp_raw is not None and dexp_raw >= 0) else "var(--down)" if dexp_raw is not None else "var(--muted)"
        # VaR signed
        var_raw_l = v_livro.get(r.BOOK)
        if var_raw_l is None:
            var_s_l, var_c_l = "—", "var(--muted)"
        else:
            var_signed_l = var_raw_l * (-1 if r.net_pct >= 0 else 1)
            var_s_l = f'{var_signed_l:+.1f}'
            var_c_l = "var(--up)" if var_signed_l < 0 else "var(--down)"
            tot_var_l += var_signed_l; any_var_l = True
        # Δ VaR
        d1v_l = v1_livro.get(r.BOOK)
        if var_raw_l is not None and d1v_l is not None:
            dvar_raw_l = var_raw_l - d1v_l
            dvar_s_l = f'{dvar_raw_l:+.1f}'
            dvar_c_l = "var(--up)" if dvar_raw_l < 0 else "var(--down)"
        else:
            dvar_s_l, dvar_c_l = "—", "var(--muted)"
        tot_net_l += r.net_pct
        livro_rows += (
            f'<tr>'
            f'<td class="sum-fund">{r.BOOK}</td>'
            + _c(f'{r.net_pct:+.2f}%', net_c, "font-weight:600")
            + _c(dexp_s, dexp_c)
            + _c(sig_s, "var(--muted)")
            + _c(var_s_l, var_c_l)
            + _c(dvar_s_l, dvar_c_l)
            + '</tr>'
        )
    # Total row
    tot_net_c_l = "var(--up)" if tot_net_l >= 0 else "var(--down)"
    tot_var_c_l = ("var(--up)" if tot_var_l < 0 else "var(--down)") if any_var_l else "var(--muted)"
    livro_rows += (
        '<tr class="pa-total-row">'
        '<td class="sum-fund" style="font-weight:700">Total</td>'
        + _c(f'{tot_net_l:+.2f}%', tot_net_c_l, "font-weight:700")
        + '<td></td>'
        + '<td></td>'
        + _c(f'{tot_var_l:+.1f}' if any_var_l else '—', tot_var_c_l, "font-weight:700")
        + '<td></td>'
        + '</tr>'
    )
    livro_header = (
        '<th style="text-align:left">Livro</th>'
        '<th style="text-align:right">Net %NAV</th>'
        '<th style="text-align:right">Δ Expo</th>'
        '<th style="text-align:right">σ (bps)</th>'
        '<th style="text-align:right">VaR (bps)</th>'
        '<th style="text-align:right">Δ VaR</th>'
    )

    nav_fmt = _fmt_br_num(f"{nav/1e6:,.1f}")

    return f"""
    {_UEXPO_JS}
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — QUANT</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · por fator de risco e por livro</span>
        {build_vardod_trigger("QUANT")}
        <div class="pa-view-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-qexpo-view="factor"
                  onclick="selectQuantExpoView(this,'factor')">Por Fator</button>
          <button class="pa-tgl" data-qexpo-view="livro"
                  onclick="selectQuantExpoView(this,'livro')">Por Livro</button>
        </div>
      </div>

      <div class="qexpo-view" data-qexpo-view="factor">{factor_table}</div>

      <div class="qexpo-view" data-qexpo-view="livro" style="display:none">
        <table class="summary-table" data-no-sort="1">
          <thead><tr>{livro_header}</tr></thead>
          <tbody>{livro_rows}</tbody>
        </table>
      </div>

      <div class="bar-legend" style="margin-top:10px">
        <b>Por Fator</b>: factor × produto — Net/Gross %NAV, Δ Expo, σ (bps), VaR signed (dado=−/verde · tomado=+/vermelho).
        <b>Por Livro</b>: Net %NAV, Δ Expo, σ, VaR signed e Δ VaR por livro. Fonte: LOTE_PRODUCT_EXPO + LOTE_FUND_STRESS_RPM LEVEL=3.
      </div>
    </section>"""


def build_evolution_exposure_section(df: pd.DataFrame, nav: float,
                                      df_d1: pd.DataFrame = None,
                                      df_var: pd.DataFrame = None,
                                      df_var_d1: pd.DataFrame = None,
                                      df_pnl_prod: pd.DataFrame = None,
                                      diversified_var_bps: float | None = None,
                                      diversified_var_bps_d1: float | None = None) -> str:
    """EVOLUTION exposure — full look-through card with two views:
       1. POR STRATEGY (default): Strategy → LIVRO → Instrumento, 3-level drill.
       2. POR FATOR: factor × product using the shared _build_expo_unified_table.
       Columns mirror MACRO: %NAV, σ, Δ Expo, VaR(bps), Δ VaR, DIA.
    """
    if df is None or df.empty or not nav:
        return ""

    # ── Build the Por Fator view (reuse MACRO helper) ────────────────────────
    _expo_u    = df.rename(columns={"factor": "factor"}).copy()  # already tagged
    _var_u     = df_var.rename(columns={"rf": "factor"}).copy() if df_var is not None and not df_var.empty else None
    _expo_d1_u = df_d1.rename(columns={"factor": "factor"}).copy() if df_d1 is not None and not df_d1.empty else None
    _var_d1_u  = df_var_d1.rename(columns={"rf": "factor"}).copy() if df_var_d1 is not None and not df_var_d1.empty else None
    factor_table = _build_expo_unified_table(
        fund_key="evolution",
        nav=nav,
        df=_expo_u,
        df_d1=_expo_d1_u,
        df_var=_var_u,
        df_var_d1=_var_d1_u,
        factor_order=_RF_ORDER,
        diversified_var_bps=diversified_var_bps,
        diversified_var_bps_d1=diversified_var_bps_d1,
    )

    # ── Build the Por Strategy view (Strategy → LIVRO → Instrumento) ────────
    # VaR attribution: at the product level, allocate the factor's total VaR
    # proportionally to each product's delta share within that factor.
    rf_delta_tot = df.groupby("factor").agg(rf_delta=("delta", "sum")).reset_index()
    rf_var_tot = (df_var.groupby("rf").agg(var_pct=("var_pct", "sum")).reset_index()
                  if df_var is not None and not df_var.empty else pd.DataFrame(columns=["rf", "var_pct"]))
    rf_var_map = dict(zip(rf_var_tot["rf"], rf_var_tot["var_pct"])) if not rf_var_tot.empty else {}

    # Product-level agg: sum delta across primitives for the same instrument within a livro
    prod_agg = (df.groupby(["strategy", "livro", "factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                  .agg(delta=("delta", "sum"), sigma=("sigma", "mean")))
    prod_agg = prod_agg.merge(rf_delta_tot, on="factor", how="left")
    prod_agg["pct_nav"] = prod_agg["delta"] * 100 / nav
    prod_agg["prod_var_pct"] = prod_agg.apply(
        lambda r: (r["delta"] / r["rf_delta"] * rf_var_map.get(r["factor"], 0.0))
                  if r["rf_delta"] else 0.0,
        axis=1,
    )

    # D-1 product lookup (for Δ Expo / Δ VaR at instrument level)
    d1_prod = {}
    d1_prod_var = {}
    if df_d1 is not None and not df_d1.empty:
        rf_delta_tot_d1 = df_d1.groupby("factor").agg(rf_delta=("delta", "sum")).reset_index()
        rf_var_tot_d1 = (df_var_d1.groupby("rf").agg(var_pct=("var_pct", "sum")).reset_index()
                         if df_var_d1 is not None and not df_var_d1.empty
                         else pd.DataFrame(columns=["rf", "var_pct"]))
        rf_var_map_d1 = dict(zip(rf_var_tot_d1["rf"], rf_var_tot_d1["var_pct"])) if not rf_var_tot_d1.empty else {}
        p1 = (df_d1.groupby(["strategy", "livro", "factor", "PRODUCT", "PRODUCT_CLASS"], as_index=False)
                    .agg(delta=("delta", "sum")))
        p1 = p1.merge(rf_delta_tot_d1, on="factor", how="left")
        p1["pct_nav"] = p1["delta"] * 100 / nav
        p1["prod_var_pct"] = p1.apply(
            lambda r: (r["delta"] / r["rf_delta"] * rf_var_map_d1.get(r["factor"], 0.0))
                      if r["rf_delta"] else 0.0,
            axis=1,
        )
        for r in p1.itertuples(index=False):
            k = (r.strategy, r.livro, r.factor, r.PRODUCT, r.PRODUCT_CLASS)
            d1_prod[k] = r.pct_nav
            d1_prod_var[k] = r.prod_var_pct

    # DIA lookup per (LIVRO, PRODUCT)
    dia_lookup = {}
    if df_pnl_prod is not None and not df_pnl_prod.empty:
        for r in df_pnl_prod.itertuples(index=False):
            dia_lookup[(r.LIVRO, r.PRODUCT)] = float(r.dia_bps)

    def _num(v, n=1):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—", "var(--muted)"
        return f'{v:+.{n}f}', ("var(--up)" if v >= 0 else "var(--down)")

    def _abs_num(v, n=2, unit="%"):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        return f'{v:.{n}f}{unit}'

    def _dv(v):
        return "" if (v is None or (isinstance(v, float) and pd.isna(v))) else f'{v:.6f}'

    def _cell(txt, color=None, extra=""):
        col = f'color:{color};' if color else ''
        return (f'<td class="mono" style="text-align:right;font-size:11.5px;{col}{extra}">{txt}</td>')

    # Rollups per (strategy, livro) and per strategy
    livro_sum = (prod_agg.groupby(["strategy", "livro"], as_index=False)
                          .agg(pct_nav=("pct_nav", "sum"),
                               prod_var_pct=("prod_var_pct", "sum"),
                               gross_brl=("delta", lambda s: s.abs().sum())))
    strat_sum = (prod_agg.groupby("strategy", as_index=False)
                          .agg(pct_nav=("pct_nav", "sum"),
                               prod_var_pct=("prod_var_pct", "sum"),
                               gross_brl=("delta", lambda s: s.abs().sum())))

    # Weighted σ per (strategy, livro) and per strategy
    def _sigma_w(sub: pd.DataFrame) -> float | None:
        s = sub.dropna(subset=["sigma"])
        w = s["pct_nav"].abs()
        tot = w.sum()
        if tot <= 0:
            return None
        return float((w * s["sigma"]).sum() / tot)
    livro_sigma = {(r.strategy, r.livro): _sigma_w(prod_agg[(prod_agg["strategy"] == r.strategy) & (prod_agg["livro"] == r.livro)])
                   for r in livro_sum.itertuples(index=False)}
    strat_sigma = {r.strategy: _sigma_w(prod_agg[prod_agg["strategy"] == r.strategy])
                   for r in strat_sum.itertuples(index=False)}

    # D-1 rollups
    d1_livro = {}
    d1_strat = {}
    d1_livro_var = {}
    d1_strat_var = {}
    if df_d1 is not None and not df_d1.empty:
        p1_tmp = pd.DataFrame([
            dict(strategy=k[0], livro=k[1], factor=k[2], PRODUCT=k[3], PRODUCT_CLASS=k[4],
                 pct_nav=v, prod_var_pct=d1_prod_var.get(k, 0.0))
            for k, v in d1_prod.items()
        ])
        if not p1_tmp.empty:
            _l = p1_tmp.groupby(["strategy", "livro"]).agg(pct_nav=("pct_nav", "sum"),
                                                          prod_var_pct=("prod_var_pct", "sum"))
            for k, r in _l.iterrows():  # k = (strategy, livro) multi-index — index needed
                d1_livro[k] = r["pct_nav"]; d1_livro_var[k] = r["prod_var_pct"]
            _s = p1_tmp.groupby("strategy").agg(pct_nav=("pct_nav", "sum"),
                                               prod_var_pct=("prod_var_pct", "sum"))
            for k, r in _s.iterrows():  # k = strategy index — index needed
                d1_strat[k] = r["pct_nav"]; d1_strat_var[k] = r["prod_var_pct"]

    # ── Render rows ──────────────────────────────────────────────────────────
    body = ""
    strategies_sorted = sorted(
        strat_sum["strategy"].tolist(),
        key=lambda s: (_EVO_STRATEGY_ORDER.index(s) if s in _EVO_STRATEGY_ORDER else 99)
    )
    for strat in strategies_sorted:
        s_row = strat_sum[strat_sum["strategy"] == strat].iloc[0]
        s_pct  = float(s_row["pct_nav"])
        s_var  = float(s_row["prod_var_pct"])
        s_gross = float(s_row["gross_brl"]) * 100 / nav
        s_sig = strat_sigma.get(strat)
        s_dexp = (s_pct - d1_strat[strat]) if strat in d1_strat else None
        s_dvar = (s_var - d1_strat_var[strat]) if strat in d1_strat_var else None
        # Uniform styling (sem cor específica por strategy — consistente com Por Fator)
        s_color = "var(--text)"

        s_net_s, s_net_c   = _num(s_pct, 2)
        s_dexp_s, s_dexp_c = _num(s_dexp, 2)
        s_var_s = f'{s_var:.1f}' if s_var else "—"
        s_var_c = "var(--down)" if s_var > 0 else "var(--muted)"
        s_dvar_s, s_dvar_c = _num(s_dvar, 1)
        s_sig_s = _abs_num(s_sig, 1, unit="")
        strat_path = f'evo-strat-{strat}'

        # DIA at strategy level: sum across all livros in the strategy
        s_dia = 0.0
        for lv in prod_agg[prod_agg["strategy"] == strat]["livro"].unique():
            for p in prod_agg[(prod_agg["strategy"] == strat) & (prod_agg["livro"] == lv)].itertuples(index=False):
                s_dia += dia_lookup.get((lv, p.PRODUCT), 0.0)
        s_dia_s = f'{s_dia:+.0f}' if abs(s_dia) > 0.05 else "—"
        s_dia_c = "var(--down)" if s_dia < 0 else "var(--up)" if s_dia > 0 else "var(--muted)"

        body += (
            f'<tr class="uexpo-row" data-expo-lvl="0" data-expo-path="{strat_path}" '
            f'onclick="uexpoToggle(this)" style="cursor:pointer; border-top:1px solid var(--border)" '
            f'data-default-order="{strategies_sorted.index(strat)}" '
            f'data-sort-name="{strat}" '
            f'data-sort-net="{-abs(s_pct):.6f}" '
            f'data-sort-dexp="{_dv(s_dexp)}" '
            f'data-sort-gross="{-s_gross:.6f}" '
            f'data-sort-sigma="{_dv(s_sig)}" '
            f'data-sort-var="{_dv(s_var)}" '
            f'data-sort-dvar="{_dv(s_dvar)}" '
            f'data-sort-dia="{_dv(s_dia if abs(s_dia) > 0.05 else None)}">'
            f'<td class="sum-fund" style="font-weight:700;color:{s_color}"><span class="uexpo-caret">▶</span> {strat}</td>'
            + _cell(f'{s_net_s}%', s_net_c, "font-weight:700")
            + _cell(f'{s_dexp_s}%' if s_dexp_s != '—' else '—', s_dexp_c)
            + _cell(s_sig_s, "var(--muted)")
            + _cell(s_var_s, s_var_c)
            + _cell(f'{s_dvar_s}' if s_dvar_s != '—' else '—', s_dvar_c)
            + _cell(s_dia_s, s_dia_c)
            + '</tr>'
        )

        # Level 1: LIVROs within strategy
        livros_here = livro_sum[livro_sum["strategy"] == strat].sort_values(
            "pct_nav", key=lambda s: s.abs(), ascending=False
        )
        for lrow in livros_here.itertuples(index=False):
            lv = lrow.livro
            l_pct = float(lrow.pct_nav)
            l_var = float(lrow.prod_var_pct)
            l_gross = float(lrow.gross_brl) * 100 / nav
            l_sig = livro_sigma.get((strat, lv))
            k_l = (strat, lv)
            l_dexp = (l_pct - d1_livro[k_l]) if k_l in d1_livro else None
            l_dvar = (l_var - d1_livro_var[k_l]) if k_l in d1_livro_var else None

            l_net_s, l_net_c   = _num(l_pct, 2)
            l_dexp_s, l_dexp_c = _num(l_dexp, 2)
            l_var_s = f'{l_var:.1f}' if l_var else "—"
            l_var_c = "var(--down)" if l_var > 0 else "var(--muted)"
            l_dvar_s, l_dvar_c = _num(l_dvar, 1)
            l_sig_s = _abs_num(l_sig, 1, unit="")
            livro_path = f'{strat_path}-{lv}'

            # DIA for LIVRO: sum dia_bps across instruments
            l_dia = sum(dia_lookup.get((lv, p.PRODUCT), 0.0)
                        for p in prod_agg[(prod_agg["strategy"] == strat) & (prod_agg["livro"] == lv)].itertuples(index=False))
            l_dia_s = f'{l_dia:+.0f}' if abs(l_dia) > 0.05 else "—"
            l_dia_c = "var(--down)" if l_dia < 0 else "var(--up)" if l_dia > 0 else "var(--muted)"

            l_var_sort = _dv(l_var) if l_var else ""
            body += (
                f'<tr class="uexpo-row" data-expo-lvl="1" data-expo-parent="{strat_path}" '
                f'data-expo-path="{livro_path}" onclick="uexpoToggle(this)" '
                f'style="cursor:pointer; display:none; background:rgba(96,165,250,0.04)" '
                f'data-sort-name="{lv}" '
                f'data-sort-net="{-abs(l_pct):.6f}" '
                f'data-sort-dexp="{_dv(l_dexp)}" '
                f'data-sort-sigma="{_dv(l_sig)}" '
                f'data-sort-var="{l_var_sort}" '
                f'data-sort-dvar="{_dv(l_dvar)}" '
                f'data-sort-dia="{_dv(l_dia if abs(l_dia) > 0.05 else None)}">'
                f'<td style="padding-left:24px; font-weight:600; color:var(--text)">'
                f'<span class="uexpo-caret">▶</span> {lv}</td>'
                + _cell(f'{l_net_s}%', l_net_c)
                + _cell(f'{l_dexp_s}%' if l_dexp_s != '—' else '—', l_dexp_c)
                + _cell(l_sig_s, "var(--muted)")
                + _cell(l_var_s, l_var_c)
                + _cell(f'{l_dvar_s}' if l_dvar_s != '—' else '—', l_dvar_c)
                + _cell(l_dia_s, l_dia_c)
                + '</tr>'
            )

            # Level 2: Instruments within (strategy, livro)
            inst_rows = prod_agg[(prod_agg["strategy"] == strat) & (prod_agg["livro"] == lv)].copy()
            inst_rows["_abs"] = inst_rows["pct_nav"].abs()
            inst_rows = inst_rows.sort_values("_abs", ascending=False)
            for p in inst_rows.itertuples(index=False):
                p_net = float(p.pct_nav)
                p_var = float(p.prod_var_pct)
                p_sig = float(p.sigma) if pd.notna(p.sigma) else None
                key = (strat, lv, p.factor, p.PRODUCT, p.PRODUCT_CLASS)
                p_d1 = d1_prod.get(key)
                if df_d1 is not None and not df_d1.empty:
                    p_dexp = p_net - (p_d1 if p_d1 is not None else 0.0)
                else:
                    p_dexp = None
                p_dvar = None
                if df_var_d1 is not None and not df_var_d1.empty:
                    p_dvar = p_var - d1_prod_var.get(key, 0.0)
                p_dia = dia_lookup.get((lv, p.PRODUCT))

                p_net_s, p_net_c = _num(p_net, 2)
                p_dexp_s, p_dexp_c = _num(p_dexp, 2)
                p_var_s = f'{p_var:.1f}' if abs(p_var) > 0.05 else "—"
                p_var_c = "var(--down)" if p_var > 0 else "var(--muted)"
                p_dvar_s, p_dvar_c = _num(p_dvar, 1)
                p_sig_s = _abs_num(p_sig, 1, unit="")
                p_dia_s = f'{p_dia:+.0f}' if (p_dia is not None and abs(p_dia) > 0.05) else "—"
                p_dia_c = "var(--down)" if (p_dia is not None and p_dia < 0) else "var(--up)" if (p_dia is not None and p_dia > 0) else "var(--muted)"
                p_var_sort = _dv(p_var) if abs(p_var) > 0.05 else ""
                p_dia_sort = _dv(p_dia) if (p_dia is not None and abs(p_dia) > 0.05) else ""
                body += (
                    f'<tr class="uexpo-row" data-expo-lvl="2" data-expo-parent="{livro_path}" '
                    f'style="display:none; background:rgba(0,0,0,0.18)" '
                    f'data-sort-name="{p.PRODUCT}" '
                    f'data-sort-net="{-abs(p_net):.6f}" '
                    f'data-sort-dexp="{_dv(p_dexp)}" '
                    f'data-sort-sigma="{_dv(p_sig)}" '
                    f'data-sort-var="{p_var_sort}" '
                    f'data-sort-dvar="{_dv(p_dvar)}" '
                    f'data-sort-dia="{p_dia_sort}">'
                    f'<td style="padding-left:44px; font-size:11px; color:var(--muted)">'
                    f'<span style="color:var(--text); font-weight:500">{p.PRODUCT}</span> '
                    f'<span style="color:var(--muted); font-size:10px">({p.PRODUCT_CLASS} · {p.factor})</span>'
                    f'</td>'
                    + _cell(f'{p_net_s}%', p_net_c)
                    + _cell(f'{p_dexp_s}%' if p_dexp_s != '—' else '—', p_dexp_c)
                    + _cell(p_sig_s, "var(--muted)")
                    + _cell(p_var_s, p_var_c)
                    + _cell(f'{p_dvar_s}' if p_dvar_s != '—' else '—', p_dvar_c)
                    + _cell(p_dia_s, p_dia_c)
                    + '</tr>'
                )

    # Total row (pinned)
    tot_gross = (prod_agg["delta"].abs().sum()) * 100 / nav
    tot_var = sum(v for v in rf_var_map.values())
    body += (
        '<tr class="pa-total-row" data-pinned="1" '
        'style="border-top:2px solid var(--border); font-weight:700">'
        '<td class="sum-fund" style="font-weight:700">Total</td>'
        + '<td></td><td></td>'
        + '<td></td>'  # σ empty at total
        + _cell(f'{tot_var:.1f}' if tot_var else '—',
                "var(--down)" if tot_var > 0 else "var(--muted)",
                "font-weight:700")
        + '<td></td><td></td>'
        + '</tr>'
    )

    def _th(label, sort_key, active=False, align="right"):
        arrow = '↓' if active else ''
        act_cls = ' uexpo-sort-active' if active else ''
        return (f'<th class="uexpo-sort-th{act_cls}" data-expo-sort="{sort_key}" '
                f'style="text-align:{align}; cursor:pointer; user-select:none" '
                f'onclick="evoExpoSort(this,\'{sort_key}\')">'
                f'{label} <span class="uexpo-arrow" '
                f'style="font-size:9px;opacity:{0.9 if active else 0}">{arrow}</span></th>')

    header = (
        _th("Strategy / LIVRO / Instrumento", "name",  active=False, align="left")
        + _th("Net %NAV",   "net",   active=True)
        + _th("Δ Expo",     "dexp",  active=False)
        + _th("σ (bps)",    "sigma", active=False)
        + _th("VaR (bps)",  "var",   active=False)
        + _th("Δ VaR",      "dvar",  active=False)
        + _th("DIA (bps)",  "dia",   active=False)
    )

    strategy_table = (
        f'<table id="tbl-uexpo-evolution-strat" class="summary-table" data-no-sort="1">'
        f'<thead><tr>{header}</tr></thead>'
        f'<tbody>{body}</tbody>'
        f'</table>'
    )

    nav_fmt = _fmt_br_num(f"{nav/1e6:,.1f}")

    view_toggle_js = r"""<script>
if (!window.__evoExpoJSLoaded) {
  window.__evoExpoJSLoaded = true;
  window.selectEvoExpoView = function(btn, view) {
    var head = btn.closest('.card-head');
    if (!head) return;
    head.querySelectorAll('.pa-tgl').forEach(function(b) {
      b.classList.toggle('active', b === btn);
    });
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('.evo-expo-view').forEach(function(div) {
      div.style.display = div.getAttribute('data-evo-view') === view ? '' : 'none';
    });
  };

  // Cascading sort — sorts strategies, then LIVROs within each strategy,
  // then instruments within each LIVRO, by the same column + direction.
  window.evoExpoSort = function(th, sortKey) {
    var table = th.closest('table'); if (!table) return;
    var tbody = table.tBodies[0];    if (!tbody) return;
    if (!window.__uexpoSortState) window.__uexpoSortState = {};
    var key = (table.id || '') + ':' + sortKey;
    var prev = window.__uexpoSortState[key];
    var state;
    if (sortKey === 'name') {
      state = (prev === 'asc') ? 'desc' : (prev === 'desc') ? 'default' : 'asc';
    } else {
      state = (prev === 'asc') ? 'desc' : 'asc';
    }
    window.__uexpoSortState = {}; window.__uexpoSortState[key] = state;

    // Build nested tree: strategies → livros → instruments
    var strategies = [];
    var totalRow = null;
    var curStrat = null, curLivro = null;
    Array.from(tbody.rows).forEach(function(r){
      if (r.classList.contains('pa-total-row')) { totalRow = r; return; }
      var lvl = r.getAttribute('data-expo-lvl');
      if (lvl === '0') {
        curStrat = { head: r, livros: [] };
        strategies.push(curStrat);
        curLivro = null;
      } else if (lvl === '1' && curStrat) {
        curLivro = { head: r, instruments: [] };
        curStrat.livros.push(curLivro);
      } else if (lvl === '2' && curLivro) {
        curLivro.instruments.push(r);
      }
    });

    function val(r, k) {
      if (k === 'name') return r.getAttribute('data-sort-name') || '';
      var raw = r.getAttribute('data-sort-' + k);
      if (raw === null || raw === '') return null;
      var n = parseFloat(raw);
      return isNaN(n) ? null : n;
    }
    var asc = (state === 'asc');
    function cmp(a, b) {
      if (sortKey === 'name') {
        var sa = val(a, 'name'), sb = val(b, 'name');
        return asc ? sa.localeCompare(sb) : sb.localeCompare(sa);
      }
      var va = val(a, sortKey), vb = val(b, sortKey);
      var aN = (va === null), bN = (vb === null);
      if (aN && bN) {
        var na = val(a, 'name'), nb = val(b, 'name');
        return asc ? na.localeCompare(nb) : nb.localeCompare(na);
      }
      if (aN) return 1;
      if (bN) return -1;
      return asc ? va - vb : vb - va;
    }

    if (state === 'default') {
      strategies.sort(function(a, b){
        var va = parseInt(a.head.getAttribute('data-default-order')||'0', 10);
        var vb = parseInt(b.head.getAttribute('data-default-order')||'0', 10);
        return va - vb;
      });
    } else {
      strategies.sort(function(a, b){ return cmp(a.head, b.head); });
      strategies.forEach(function(s){
        s.livros.sort(function(a, b){ return cmp(a.head, b.head); });
        s.livros.forEach(function(l){
          l.instruments.sort(function(a, b){ return cmp(a, b); });
        });
      });
    }

    strategies.forEach(function(s){
      tbody.appendChild(s.head);
      s.livros.forEach(function(l){
        tbody.appendChild(l.head);
        l.instruments.forEach(function(i){ tbody.appendChild(i); });
      });
    });
    if (totalRow) tbody.appendChild(totalRow);

    table.querySelectorAll('th[data-expo-sort]').forEach(function(h){
      var arrow = h.querySelector('.uexpo-arrow');
      if (!arrow) return;
      var active = (h === th) && (state !== 'default');
      var glyph  = state === 'asc' ? '↑' : state === 'desc' ? '↓' : '';
      arrow.textContent = (h === th) ? glyph : '';
      arrow.style.opacity = active ? '0.9' : '0';
      h.classList.toggle('uexpo-sort-active', active);
    });
  };
}
</script>"""

    return f"""
    {_UEXPO_JS}
    {view_toggle_js}
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — EVOLUTION</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · look-through completo (MACRO + SIST + FRONTIER + CREDITO + EVO_STRAT)</span>
        {build_vardod_trigger("EVOLUTION")}
        <div class="pa-view-toggle" style="margin-left:auto">
          <button class="pa-tgl active" data-evo-view="strat"
                  onclick="selectEvoExpoView(this,'strat')">Por Strategy</button>
          <button class="pa-tgl" data-evo-view="factor"
                  onclick="selectEvoExpoView(this,'factor')">Por Fator</button>
        </div>
      </div>

      <div class="evo-expo-view" data-evo-view="strat">{strategy_table}</div>
      <div class="evo-expo-view" data-evo-view="factor" style="display:none">{factor_table}</div>

      <div class="bar-legend" style="margin-top:10px">
        <b>Por Strategy</b>: Strategy → LIVRO → Instrumento (3 níveis, click ▶ para expandir).
        VaR alocado proporcionalmente ao Δ do instrumento dentro do fator. DIA em bps via <code>REPORT_ALPHA_ATRIBUTION</code> (FUNDO='EVOLUTION').
        <b>Por Fator</b>: taxonomia RF-* (idem MACRO) — fator × instrumento. Fonte: <code>LOTE_PRODUCT_EXPO</code>
        (TRADING_DESK_SHARE_SOURCE = Evolution) + <code>LOTE_BOOK_STRESS_RPM</code> LEVEL=3.
        <b>σ:</b> proxy via <code>STANDARD_DEVIATION_ASSETS</code> (BOOK='MACRO').
      </div>
    </section>"""


def build_idka_exposure_section(short: str, df: pd.DataFrame, nav: float,
                                  bench_dur: float, bench_label: str,
                                  date_str: str = None) -> str:
    """IDKA exposure table — positions por fator com toggle Bruto/Líquido.
       Líquido = Bruto − bench teórico DV-matched (NTN-Bs que replicam IDKA index
       via weighted modified-duration matching — metodologia IDKA_TABLES_GRAPHS.py).
       Dados vêm de rf_expo_maps[short] (fetch_rf_exposure_map)."""
    if df is None or df.empty or not nav:
        return ""

    FACTOR_ORDER = ["real", "ipca_idx", "real_igpm", "igpm_idx", "nominal", "cdi", "other"]
    FACTOR_LABEL = {
        "real":      "Duration Real (IPCA Coupon)",
        "ipca_idx":  "Indexação IPCA (carry)",
        "real_igpm": "Duration Real (IGPM Coupon)",
        "igpm_idx":  "Indexação IGPM (carry)",
        "nominal":   "Juros Nominais",
        "cdi":       "CDI / LFT",
        "other":     "Outros",
    }

    df = df.copy()
    df["yrs_to_mat"] = pd.to_numeric(df.get("yrs_to_mat", 0), errors="coerce").fillna(0.0)
    df["mod_dur"]    = pd.to_numeric(df.get("mod_dur",    0), errors="coerce").fillna(0.0)
    # DV01 convention para duration factors (real + real_igpm + nominal): long bond = negative.
    # ipca_idx / igpm_idx / cdi / other são face-value notional (%NAV), NÃO duration — não flipar
    # (e não vão no total de yrs).
    _DUR_FAC = {"real", "real_igpm", "nominal"}
    df["ano_eq_brl"] = df.apply(
        lambda r: -r["ano_eq_brl"] if r["factor"] in _DUR_FAC else r["ano_eq_brl"],
        axis=1,
    )
    df["dv01_brl"] = df.apply(
        lambda r: r["ano_eq_brl"] * 0.0001 if r["factor"] in _DUR_FAC else 0.0,
        axis=1,
    )

    # Aggregate per (factor, PRODUCT, expiry, via)
    agg = (df.groupby(["factor", "PRODUCT", "PRODUCT_CLASS", "via", "yrs_to_mat"],
                      as_index=False)
             .agg(ano_eq_brl=("ano_eq_brl", "sum"),
                  dv01_brl=("dv01_brl",   "sum"),
                  delta_brl=("delta_brl",   "sum"),
                  position_brl=("position_brl", "sum"),
                  mod_dur=("mod_dur", "max")))
    agg["_abs"] = agg["ano_eq_brl"].abs()

    def _yrs_cell(v):
        if v is None or pd.isna(v) or abs(v) < 0.001:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        col = "var(--up)" if v >= 0 else "var(--down)"
        return f'<td class="mono" style="text-align:right;color:{col}">{v:+.2f} yrs</td>'

    def _pct_cell(v_brl):
        if v_brl is None or pd.isna(v_brl) or abs(v_brl) < 1_000:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        pct = v_brl * 100 / nav
        col = "var(--up)" if v_brl >= 0 else "var(--down)"
        return f'<td class="mono" style="text-align:right;color:{col}">{pct:+.1f}%</td>'

    def _dv01_cell(v_brl):
        if v_brl is None or pd.isna(v_brl) or abs(v_brl) < 10:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        col = "var(--up)" if v_brl >= 0 else "var(--down)"
        return f'<td class="mono" style="text-align:right;color:{col}">{_fmt_br_num(f"{v_brl/1e3:+,.1f}")}k</td>'

    def _dur_cell(d, y):
        # If we have mod_dur use it; else show yrs_to_mat as proxy
        val = d if d and d > 0.01 else y
        if not val or abs(val) < 0.01:
            return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
        return f'<td class="mono" style="text-align:right;color:var(--muted)">{val:.2f}y</td>'

    body = ""
    # Duration totals in yrs — SOMENTE fatores 'real' + 'real_igpm' + 'nominal'.
    # ipca_idx / igpm_idx (carry) e cdi são notional (BRL face value) — mostrados como %NAV,
    # EXCLUÍDOS dos totais de yrs pra não misturar unidades.
    _DUR_FACTORS = {"real", "real_igpm", "nominal"}
    total_ano_eq        = 0.0
    total_ano_eq_via    = 0.0
    total_ano_eq_direct = 0.0

    # Bench computation moved here so target_dm is available for factor header spans
    tenour_du = {3: 756, 10: 2520}.get(int(bench_dur), 756)
    bench_replication = _compute_idka_bench_replication(date_str, int(bench_dur), tenour_du) if date_str else pd.DataFrame()
    target_dm = float(bench_dur)
    y_tenor   = None
    bench_ano_eq_total = 0.0
    if not bench_replication.empty:
        target_dm = float(bench_replication["TARGET_DM"].iloc[0])
        y_tenor   = float(bench_replication["YIELD_TEN"].iloc[0])
        bench_ano_eq_total = float(bench_replication["ANO_EQ_BENCH"].sum())
    else:
        bench_ano_eq_total = target_dm   # fallback

    def _fae_yrs(val):
        col = "var(--up)" if val >= 0 else "var(--down)"
        return f'<span style="color:{col};font-weight:700">{val:+.2f} yrs</span>'

    for f in FACTOR_ORDER:
        sub = agg[agg["factor"] == f].sort_values("_abs", ascending=False)
        if sub.empty:
            continue
        is_dur = f in _DUR_FACTORS
        fac_ano = float(sub["ano_eq_brl"].sum())
        fac_pos = float(sub["position_brl"].sum())
        fac_ano_direct = float(sub[sub["via"] == "direct"]["ano_eq_brl"].sum()) if is_dur else 0.0
        if is_dur:
            total_ano_eq        += fac_ano
            total_ano_eq_via    += float(sub[sub["via"] == "via_albatroz"]["ano_eq_brl"].sum())
            total_ano_eq_direct += fac_ano_direct
        fac_pct = fac_pos * 100 / nav
        n_pos = len(sub)
        # Factor header ANO-EQ cell: duration factors get 3 mode-aware spans so the
        # header updates when the toggle changes (bruto / líq-benchmark / líq-replication).
        # Benchmark only affects the "real" factor for IDKAs (NTN-B coupon duration).
        if is_dur:
            bench_adj_b = target_dm          if f == "real" else 0.0
            bench_adj_r = bench_ano_eq_total if f == "real" else 0.0
            # Líquido usa total bruto do fator (sem swap plug): total - bench
            liq_b_val   = fac_ano / nav + bench_adj_b
            liq_r_val   = fac_ano / nav + bench_adj_r
            fac_ae_cell = (
                f'<td class="mono" style="text-align:right;font-weight:700">'
                f'<span data-idka-span="bruto" style="display:none">{_fae_yrs(fac_ano / nav)}</span>'
                f'<span data-idka-span="liq-benchmark">{_fae_yrs(liq_b_val)}</span>'
                f'<span data-idka-span="liq-replication" style="display:none">{_fae_yrs(liq_r_val)}</span>'
                f'</td>'
            )
        else:
            fac_ae_val  = fac_ano * 100 / nav
            fac_ae_col  = "var(--up)" if fac_ae_val >= 0 else "var(--down)"
            fac_ae_cell = f'<td class="mono" style="text-align:right;font-weight:700;color:{fac_ae_col}">{fac_ae_val:+.1f}%</td>'
        body += (
            f'<tr class="idka-fac-head" data-idka-fac="{f}" '
            f'style="background:rgba(96,165,250,0.08);font-weight:600;cursor:pointer" '
            f'onclick="toggleIdkaFac(this,\'{f}\')">'
            f'<td style="padding:6px 8px" colspan="2">'
            f'<span class="idka-fac-caret" style="color:var(--accent-2);margin-right:5px;'
            f'display:inline-block;transition:transform .15s">▶</span>'
            f'<b>{FACTOR_LABEL[f]}</b> '
            f'<span style="color:var(--muted);font-weight:400;font-size:11px">· {n_pos} pos</span></td>'
            f'<td></td><td></td>'
            + fac_ae_cell
            + f'<td class="mono" style="text-align:right;color:var(--muted)">{fac_pct:+.1f}%</td>'
            + f'<td></td>'
            + '</tr>'
        )
        for r in sub.itertuples(index=False):
            via_tag = "direct" if r.via == "direct" else ("via Albatroz" if r.via == "via_albatroz" else r.via)
            # Per-row: yrs for duration factors, %NAV (notional face) for others
            if is_dur:
                ae_cell = _yrs_cell(r.ano_eq_brl / nav)
            else:
                p = r.ano_eq_brl * 100 / nav
                if abs(p) < 0.05:
                    ae_cell = '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'
                else:
                    col = "var(--up)" if p >= 0 else "var(--down)"
                    ae_cell = f'<td class="mono" style="text-align:right;color:{col}">{p:+.1f}%</td>'
            body += (
                f'<tr class="idka-pos-row" data-idka-child="{f}" style="display:none;font-size:11.5px">'
                f'<td style="padding:4px 20px;color:var(--muted)">{r.PRODUCT}</td>'
                f'<td class="mono" style="color:var(--muted);font-size:10.5px">{r.PRODUCT_CLASS}</td>'
                + _dur_cell(r.mod_dur, r.yrs_to_mat)
                + (_dv01_cell(r.dv01_brl) if is_dur else '<td class="mono" style="text-align:right;color:var(--muted)">—</td>')
                + ae_cell
                + _pct_cell(r.position_brl)
                + f'<td class="mono" style="color:var(--muted);font-size:10.5px">{via_tag}</td>'
                + '</tr>'
            )

    # Total BRUTO row — só aparece no modo 'bruto' (hidden por default)
    total_yrs_bruto = total_ano_eq / nav
    body_bruto_total = (
        f'<tr class="pa-total-row" data-idka-view="bruto" '
        f'style="display:none;border-top:2px solid var(--border);font-weight:700;background:rgba(0,0,0,0.25)">'
        f'<td style="padding:6px 8px" colspan="2"><b>TOTAL BRUTO — Duration</b> '
        f'<span style="color:var(--muted);font-weight:400;font-size:11px">(só fatores real + nominal)</span></td>'
        f'<td></td><td></td>'
        f'<td class="mono" style="text-align:right;color:var(--text);font-weight:700">{total_yrs_bruto:+.2f} yrs</td>'
        f'<td></td><td></td>'
        f'</tr>'
    )

    # bench_replication, target_dm, bench_ano_eq_total already computed above (before loop)
    via_yrs_disp = total_ano_eq_via / nav

    # 3 modos de view com data-idka-view:
    #   'bruto'           — só positions + TOTAL BRUTO
    #   'liq-benchmark'   — positions + Benchmark row + TOTAL LÍQUIDO vs Bench (DEFAULT)
    #   'liq-replication' — positions + Replication rows + TOTAL LÍQUIDO vs Replication
    # Sem swap plug — comparação é bruto total vs benchmark (inclui via_albatroz)
    _DEFAULT_VIEW = "liq-benchmark"
    def _disp(view_tag):
        return "" if view_tag == _DEFAULT_VIEW else "display:none;"

    swap_row = ""  # removido: swap plug não faz sentido ao comparar tudo vs IDKA

    # Benchmark point row (liq-benchmark mode only; default → visible)
    bench_point_row = (
        f'<tr class="idka-bench-point" data-idka-view="liq-benchmark" '
        f'style="{_disp("liq-benchmark")}background:rgba(184,135,0,0.10);color:var(--warn);font-weight:700">'
        f'<td style="padding:6px 8px" colspan="2">− <b>Benchmark ({bench_label})</b> '
        f'<span style="color:var(--muted);font-weight:400;font-size:11px">'
        f'· target MD = {target_dm:.3f}y'
        + (f' (TIR tenor {y_tenor:.2f}%)' if y_tenor is not None else f' (fallback: {int(bench_dur)}y nominal)')
        + f'</span></td>'
        f'<td class="mono" style="text-align:right;color:var(--muted)">{target_dm:.2f}y</td>'
        f'<td></td>'
        f'<td class="mono" style="text-align:right">−{target_dm:.3f} yrs</td>'
        f'<td class="mono" style="text-align:right;color:var(--muted)">100% NAV</td>'
        f'<td class="mono" style="color:var(--muted);font-size:10.5px">índice</td>'
        f'</tr>'
    )

    # Replication row (liq-replication mode only) — uma única linha de subtração, sem legs
    if not bench_replication.empty:
        legs_detail = " · ".join(
            f'{r.INSTRUMENT} w={float(r.W)*100:.0f}% MD={float(r.MD):.2f}y'
            for r in bench_replication.itertuples(index=False)
        )
        replication_rows = (
            f'<tr class="idka-bench-repl-head" data-idka-view="liq-replication" '
            f'style="display:none;background:rgba(184,135,0,0.10);color:var(--warn);font-weight:700">'
            f'<td style="padding:6px 8px" colspan="2">− <b>Replication</b> '
            f'<span style="color:var(--muted);font-weight:400;font-size:11px">'
            f'· DV-match: {legs_detail}</span></td>'
            f'<td class="mono" style="text-align:right;color:var(--muted)">{target_dm:.2f}y</td>'
            f'<td></td>'
            f'<td class="mono" style="text-align:right">−{bench_ano_eq_total:.3f} yrs</td>'
            f'<td></td><td></td>'
            f'</tr>'
        )
    else:
        replication_rows = (
            f'<tr class="idka-bench-repl-head" data-idka-view="liq-replication" '
            f'style="display:none;background:rgba(184,135,0,0.10);color:var(--muted);font-weight:600">'
            f'<td style="padding:6px 8px" colspan="7">⚠ Replication DV-match indisponível '
            f'(fallback: usa target MD {target_dm:.2f}y). '
            f'Verifique q_models.BR_YIELDS e PRICES_ANBIMA_BR_PUBLIC_BONDS pra {date_str}.</td></tr>'
        )

    bench_rows_html = bench_point_row + replication_rows

    # TOTAL LÍQUIDO = bruto total − bench (inclui via_albatroz; sem swap plug)
    # + target_dm porque bench é long bond (negativo na convenção) → somar positivo = subtrair
    liq_vs_bench_yrs = total_ano_eq / nav + target_dm
    liq_vs_repl_yrs  = total_ano_eq / nav + bench_ano_eq_total

    def _liq_row(view_tag, label, yrs):
        col = "var(--up)" if yrs >= 0 else "var(--down)"
        return (
            f'<tr class="idka-liq-total" data-idka-view="{view_tag}" '
            f'style="{_disp(view_tag)}border-top:2px solid var(--border);font-weight:700;'
            f'background:rgba(74,222,128,0.08)">'
            f'<td style="padding:6px 8px" colspan="2"><b>{label}</b> '
            f'<span style="color:var(--muted);font-weight:400;font-size:11px">'
            f'· duration total − bench (real + nominal + via Albatroz)</span></td>'
            f'<td></td><td></td>'
            f'<td class="mono" style="text-align:right;color:{col};font-weight:700">{yrs:+.2f} yrs</td>'
            f'<td></td><td></td>'
            f'</tr>'
        )
    liq_row = (
        _liq_row("liq-benchmark",   "TOTAL LÍQUIDO vs Benchmark",   liq_vs_bench_yrs) +
        _liq_row("liq-replication", "TOTAL LÍQUIDO vs Replication", liq_vs_repl_yrs)
    )

    nav_fmt = _fmt_br_num(f"{nav/1e6:,.1f}")
    short_label = "IDKA 3Y" if short == "IDKA_3Y" else ("IDKA 10Y" if short == "IDKA_10Y" else short)
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — {short_label}</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · posições por fator · bench = {bench_label} ({bench_dur:.0f}y real + IPCA carry)</span>
        {build_vardod_trigger(short)}
        <div style="display:flex;align-items:center;gap:6px;margin-left:auto;flex-wrap:wrap">
          <button class="pa-tgl" style="font-size:11px;padding:3px 8px"
                  onclick="idkaExpandAll(this)">▼ All</button>
          <button class="pa-tgl" style="font-size:11px;padding:3px 8px"
                  onclick="idkaCollapseAll(this)">▶ All</button>
          <div class="pa-view-toggle" style="flex-wrap:wrap">
            <button class="pa-tgl" data-idka-view="bruto"
                    onclick="selectIdkaView(this,'bruto')">Bruto</button>
            <button class="pa-tgl active" data-idka-view="liq-benchmark"
                    onclick="selectIdkaView(this,'liq-benchmark')">Líquido vs Benchmark</button>
            <button class="pa-tgl" data-idka-view="liq-replication"
                    onclick="selectIdkaView(this,'liq-replication')">Líquido vs Replication</button>
          </div>
        </div>
      </div>
      <table class="summary-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Fator / Produto</th>
          <th style="text-align:left">Classe</th>
          <th style="text-align:right">Duration</th>
          <th style="text-align:right">DV01 (R$)</th>
          <th style="text-align:right">Ano-Eq (yrs)</th>
          <th style="text-align:right">%NAV (face)</th>
          <th style="text-align:left">Via</th>
        </tr></thead>
        <tbody>{body}{body_bruto_total}{swap_row}{bench_rows_html}{liq_row}</tbody>
      </table>
      <div class="bar-legend" style="margin-top:10px">
        <b>Duration</b>: modified duration ou anos até o vencimento (proxy).
        <b>DV01</b>: BRL perdido por 1bp de alta de yield (= Ano-Eq × 0.0001).
        <b>Ano-Eq</b>: exposição em <i>anos de duration × NAV</i> (BRL-yr) convertida em anos por NAV.
        <b>%NAV</b>: posição nominal ÷ NAV.
        <b>Bruto</b>: todas as posições como estão (direct + via Albatroz).
        <b>Líquido vs Benchmark</b>: subtrai o <b>índice IDKA teórico no ponto</b> (target MD = anos / (1+y)),
        referência abstrata.
        <b>Líquido vs Replication</b>: subtrai a <b>melhor carteira NTN-B que replica o benchmark</b>
        (DV-match — metodologia <code>IDKA_TABLES_GRAPHS.py</code>), composição concreta.
        Os dois totais coincidem hoje (por construção, soma replication = target MD); a diferença
        aparece em <b>historical spread</b> — bench point vs replication divergem ao longo do tempo
        conforme os preços ANBIMA × target móvel. Overweight de duration = TOTAL LÍQUIDO &lt; 0 (DV01 conv).
      </div>
    </section>"""


def build_rf_exposure_map_section(short: str, df: pd.DataFrame, nav: float,
                                   bench_dur_yrs: float, bench_label: str) -> str:
    """Grouped bar chart of ANO_EQ (% NAV) by maturity bucket × factor,
       with Fund/Bench/Relative toggle and cumulative lines.
    """
    if df is None or df.empty or not nav:
        return ""

    rel = df[df["factor"].isin(["real", "real_igpm", "nominal"])].copy()
    if rel.empty:
        return ""
    # Express ANO_EQ in years (ano_eq_brl / NAV).
    # DV01 convention: long bond = negative (bar goes DOWN from zero).
    # ano_eq_brl is long=positive in fetch_rf_exposure_map; flip sign here.
    rel["yr"] = -rel["ano_eq_brl"] / nav
    pivot = (rel.pivot_table(index="bucket", columns="factor", values="yr",
                             aggfunc="sum", fill_value=0.0))
    bucket_order = [b[0] for b in _RF_BUCKETS]
    pivot = pivot.reindex(index=bucket_order, columns=["real", "real_igpm", "nominal"], fill_value=0.0)

    # Per-bucket arrays in years-equivalent (DV01 conv: long bond = negative).
    # Real factor = IPCA real + IGPM real (visually combined; broken out in stat row + table).
    fund_real_ipca_b = pivot["real"].tolist()
    fund_real_igpm_b = pivot["real_igpm"].tolist()
    fund_real_b = [a + b for a, b in zip(fund_real_ipca_b, fund_real_igpm_b)]
    fund_nom_b  = pivot["nominal"].tolist()

    cdi_bench = (bench_dur_yrs == 0)

    def bench_bucket_idx(bench_dur: float) -> int:
        for i, (_, lo, hi) in enumerate(_RF_BUCKETS):
            if lo <= bench_dur < hi:
                return i
        return len(_RF_BUCKETS) - 1

    # Benchmark per-bucket: IDKAs = 100% NAV long at bench_dur → DV01 conv negative
    bench_real_b = [0.0] * len(bucket_order)
    bench_nom_b  = [0.0] * len(bucket_order)
    if not cdi_bench:
        bench_real_b[bench_bucket_idx(bench_dur_yrs)] = -bench_dur_yrs

    # Relative = fund - bench per bucket
    rel_real_b = [fund_real_b[i] - bench_real_b[i] for i in range(len(bucket_order))]
    rel_nom_b  = [fund_nom_b[i]  - bench_nom_b[i]  for i in range(len(bucket_order))]

    # Cumulative series
    def cumsum(arr):
        out, s = [], 0.0
        for v in arr:
            s += v
            out.append(s)
        return out

    cum_real = cumsum(fund_real_b)
    cum_nom  = cumsum(fund_nom_b)
    bench_cum_real = cumsum(bench_real_b)
    bench_cum_nom  = cumsum(bench_nom_b)
    rel_cum_real   = cumsum(rel_real_b)
    rel_cum_nom    = cumsum(rel_nom_b)

    # Legacy names kept for the stat-row / legend blocks below
    bench_real = bench_cum_real
    bench_nom  = bench_cum_nom

    # Via-Albatroz breakdown
    via_df = df[df["factor"].isin(["real", "nominal"])]
    via_alb_total = float(via_df[via_df["via"] == "via_albatroz"]["ano_eq_brl"].sum() / nav) if nav else 0.0

    # SVG geometry — absoluto uses 3 bars/bucket; relativo uses 2 bars/bucket
    W, H = 760, 300
    pad_l, pad_r, pad_t, pad_b = 46, 26, 26, 38
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b
    n_buckets = len(bucket_order)
    band_w = plot_w / n_buckets
    inter_bucket = 4          # gap between buckets
    bar_w_abs = (band_w - inter_bucket) / 3   # absoluto: 3 bars flush
    bar_w_rel = (band_w - inter_bucket) / 2   # relativo: 2 bars flush

    # Y scale — fit both absoluto and relativo series.
    # If positions are tiny (max abs < 0.5 yr), snap to a fixed ±0.5 yr window
    # so the chart actually shows the movements instead of looking empty.
    all_vals = (fund_real_b + fund_nom_b + bench_real_b + bench_nom_b +
                rel_real_b + rel_nom_b +
                cum_real + cum_nom + bench_cum_real + rel_cum_real + rel_cum_nom)
    max_abs = max([abs(v) for v in all_vals] + [0.0])
    if max_abs < 0.5:
        y_max, y_min = 0.5, -0.5
    else:
        y_max = max_abs * 1.15
        y_min = min([0.0] + [v for v in all_vals]) * 1.15
        if y_min >= 0:
            y_min = -y_max * 0.10

    def y_scale(v):
        return pad_t + plot_h * (1.0 - (v - y_min) / (y_max - y_min))

    def x_band(i):
        return pad_l + i * band_w

    def bar_rect(x0, v, cls, factor, bw):
        y_top = y_scale(max(v, 0))
        y_bot = y_scale(min(v, 0))
        return (f'<rect class="rf-bar {cls}" data-factor="{factor}" '
                f'x="{x0:.1f}" y="{y_top:.1f}" width="{bw:.1f}" height="{max(y_bot - y_top, 0.5):.1f}"/>')

    # Absoluto: Fund Real | Fund Nominal | Bench (flush within bucket).
    # Bench bar's data-factor reflects its composition so the factor filter
    # (Real/Nominal) hides it appropriately — e.g., IDKA bench is 100% Real,
    # so clicking Nominal filter should hide it.
    abs_bars = []
    for i in range(n_buckets):
        x0 = x_band(i) + inter_bucket / 2
        x1 = x0 + bar_w_abs
        x2 = x1 + bar_w_abs
        bench_total = bench_real_b[i] + bench_nom_b[i]
        # Determine bench factor tag from its composition
        if bench_real_b[i] != 0 and bench_nom_b[i] == 0:
            bench_tag = "real"
        elif bench_nom_b[i] != 0 and bench_real_b[i] == 0:
            bench_tag = "nominal"
        else:
            bench_tag = "bench"  # mixed or zero — always visible
        abs_bars.append(bar_rect(x0, fund_real_b[i], "rf-real", "real", bar_w_abs))
        abs_bars.append(bar_rect(x1, fund_nom_b[i],  "rf-nom",  "nominal", bar_w_abs))
        abs_bars.append(bar_rect(x2, bench_total, "rf-benchbar", bench_tag, bar_w_abs))
    abs_bars_svg = "".join(abs_bars)

    # Relativo: Relative Real | Relative Nominal (fund − bench per bucket, flush)
    rel_bars = []
    for i in range(n_buckets):
        x0 = x_band(i) + inter_bucket / 2
        x1 = x0 + bar_w_rel
        rel_bars.append(bar_rect(x0, rel_real_b[i], "rf-real", "real", bar_w_rel))
        rel_bars.append(bar_rect(x1, rel_nom_b[i],  "rf-nom",  "nominal", bar_w_rel))
    rel_bars_svg = "".join(rel_bars)

    def poly_points(values):
        pts = []
        for i, v in enumerate(values):
            cx = x_band(i) + band_w / 2
            pts.append(f"{cx:.1f},{y_scale(v):.1f}")
        return " ".join(pts)

    abs_cum_r = f'<polyline class="rf-cum rf-cum-real" data-factor="real"    points="{poly_points(cum_real)}"/>'
    abs_cum_n = f'<polyline class="rf-cum rf-cum-nom"  data-factor="nominal" points="{poly_points(cum_nom)}"/>'
    abs_cum_b = f'<polyline class="rf-cum rf-cum-bench" data-factor="bench" points="{poly_points([a + b for a, b in zip(bench_cum_real, bench_cum_nom)])}"/>'
    rel_cum_r = f'<polyline class="rf-cum rf-cum-real" data-factor="real"    points="{poly_points(rel_cum_real)}"/>'
    rel_cum_n = f'<polyline class="rf-cum rf-cum-nom"  data-factor="nominal" points="{poly_points(rel_cum_nom)}"/>'

    # Zero line
    y0 = y_scale(0)
    zero_line = f'<line class="rf-zero" x1="{pad_l}" y1="{y0:.1f}" x2="{W - pad_r}" y2="{y0:.1f}"/>'

    # Y-axis ticks (5 ticks) — values are in years
    y_axis = ""
    for k in range(5):
        t = y_min + (y_max - y_min) * k / 4
        y = y_scale(t)
        y_axis += f'<line class="rf-grid" x1="{pad_l}" y1="{y:.1f}" x2="{W - pad_r}" y2="{y:.1f}"/>'
        y_axis += f'<text class="rf-axis-lbl" x="{pad_l - 6:.1f}" y="{y + 3:.1f}" text-anchor="end">{t:+.1f}y</text>'

    # X-axis labels
    x_axis = ""
    for i, b in enumerate(bucket_order):
        cx = x_band(i) + band_w / 2
        x_axis += f'<text class="rf-axis-lbl" x="{cx:.1f}" y="{H - 12:.1f}" text-anchor="middle">{b}</text>'

    # Single chart with two mode groups — JS toggles between Absoluto / Relativo
    svg = f"""
    <svg class="rf-expo-svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
      {y_axis}
      {zero_line}
      <g class="rf-mode-group" data-rf-mode="absoluto">{abs_bars_svg}{abs_cum_r}{abs_cum_n}{abs_cum_b}</g>
      <g class="rf-mode-group" data-rf-mode="relativo" style="display:none">{rel_bars_svg}{rel_cum_r}{rel_cum_n}</g>
      {x_axis}
    </svg>
    """

    # ── Summary stats ─────────────────────────────────────────────────────────
    dur_real_ipca = sum(fund_real_ipca_b)
    dur_real_igpm = sum(fund_real_igpm_b)
    dur_real      = dur_real_ipca + dur_real_igpm   # combined (used in chart + Total)
    dur_nom       = sum(fund_nom_b)
    dur_total     = dur_real + dur_nom
    bench_total   = bench_dur_yrs
    gap_total     = dur_total - bench_total
    cdi_weight = float(df[df["factor"] == "cdi"]["delta_brl"].sum() / nav * 100) if nav else 0.0

    # Table: one row per bucket, showing Fund / Bench / Relative totals (sum real+nominal).
    def pct_cell(v, color_bias=True):
        if abs(v) < 0.005:
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        col = ("var(--up)" if v >= 0 else "var(--down)") if color_bias else "var(--text)"
        return f'<td class="mono" style="text-align:right; color:{col}">{v:+.2f}</td>'

    tbl_rows = ""
    for i, b in enumerate(bucket_order):
        f_r = fund_real_b[i]; f_n = fund_nom_b[i]; f_t = f_r + f_n
        b_r = bench_real_b[i]; b_n = bench_nom_b[i]; b_t = b_r + b_n
        r_t = f_t - b_t
        tbl_rows += (
            "<tr>"
            f'<td class="pa-name" style="font-weight:600">{b}</td>'
            + pct_cell(f_r) + pct_cell(f_n) + pct_cell(f_t)
            + pct_cell(b_t, color_bias=False)
            + pct_cell(r_t)
            + "</tr>"
        )
    tbl_footer = (
        '<tr class="pa-total-row">'
        '<td class="pa-name" style="font-weight:700">Total (yr eq.)</td>'
        + pct_cell(sum(fund_real_b)) + pct_cell(sum(fund_nom_b)) + pct_cell(sum(fund_real_b) + sum(fund_nom_b))
        + pct_cell(sum(bench_real_b) + sum(bench_nom_b), color_bias=False)
        + pct_cell((sum(fund_real_b) + sum(fund_nom_b)) - (sum(bench_real_b) + sum(bench_nom_b)))
        + "</tr>"
    )

    bench_note = (f"benchmark {bench_label} · constant-maturity {bench_total:.1f}yr, 100% NAV"
                  if not cdi_bench else f"benchmark {bench_label} · duração zero")
    nav_fmt = _fmt_br_num(f"{nav/1e6:,.1f}")
    via_chip = (f'<span class="sn-stat"><span class="sn-lbl">via Albatroz</span>'
                f'<span class="sn-val mono">{via_alb_total:+.2f}yr</span></span>'
                if abs(via_alb_total) > 0.005 and short.startswith("IDKA") else "")
    igpm_chip = (
        f'<span class="sn-stat"><span class="sn-lbl">Juros Reais (IGPM)</span>'
        f'<span class="sn-val mono">{dur_real_igpm:+.2f}yr</span></span>'
        if abs(dur_real_igpm) > 0.005 else ""
    )
    stat_row = (
        '<div class="sn-inline-stats mono" style="margin-bottom:12px; flex-wrap:wrap; gap:6px 18px">'
        f'<span class="sn-stat"><span class="sn-lbl">NAV</span><span class="sn-val mono">R$ {nav_fmt}M</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Juros Reais (IPCA)</span><span class="sn-val mono">{dur_real_ipca:+.2f}yr</span></span>'
        + igpm_chip +
        f'<span class="sn-stat"><span class="sn-lbl">Juros Nominais</span><span class="sn-val mono">{dur_nom:+.2f}yr</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Total Fund</span><span class="sn-val mono">{dur_total:+.2f}yr</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Bench</span><span class="sn-val mono">{bench_total:+.2f}yr</span></span>'
        '<span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>'
        f'<span class="sn-stat"><span class="sn-lbl">CDI</span><span class="sn-val mono">{cdi_weight:+.1f}%NAV</span></span>'
        f'<span class="sn-stat"><span class="sn-lbl">Gap (Fund − Bench)</span><span class="sn-val mono" style="color:{"var(--down)" if abs(gap_total)>0.5 else "var(--text)"}">{gap_total:+.2f}yr</span></span>'
        + via_chip +
        '</div>'
    )

    # ── Position-level table (by asset) ───────────────────────────────────────
    pos = df[df["factor"].isin(["real", "real_igpm", "nominal", "ipca_idx", "igpm_idx"])].copy()
    pos["ano_eq_yr"] = pos["ano_eq_brl"] / nav if nav else 0.0
    pos["pct_nav"]   = pos["position_brl"] / nav * 100 if nav else 0.0
    pos = pos.sort_values("ano_eq_yr", key=lambda s: s.abs(), ascending=False)

    def _p_cell(v, fmt, color_bias=False):
        if v is None or v != v or (isinstance(v, (int, float)) and abs(v) < 1e-9):
            return '<td class="mono" style="text-align:right; color:var(--muted)">—</td>'
        col = "var(--text)"
        if color_bias:
            col = "var(--up)" if v >= 0 else "var(--down)"
        return f'<td class="mono" style="text-align:right; color:{col}">{fmt.format(v)}</td>'

    pos_rows = ""
    for _, r in pos.iterrows():
        via_tag = '<span style="color:var(--muted); font-size:10px">via Albatroz</span>' if r["via"] == "via_albatroz" else ""
        pos_rows += (
            "<tr>"
            f'<td class="pa-name">{r["PRODUCT"]} {via_tag}</td>'
            f'<td class="mono" style="color:var(--muted); font-size:11px">{r["BOOK"]}</td>'
            f'<td class="mono" style="color:var(--muted); font-size:11px">{r["factor"]}</td>'
            + _p_cell(r.get("yrs_to_mat"), "{:.2f}y")
            + _p_cell(r.get("mod_dur"),    "{:.2f}")
            + _p_cell(r.get("pct_nav"),    "{:+.2f}%")
            + _p_cell(r.get("ano_eq_yr"),  "{:+.3f}", color_bias=True)
            + "</tr>"
        )
    positions_table_html = f"""
      <table class="pa-table" style="margin-top:8px; font-size:11px">
        <thead><tr>
          <th style="text-align:left">Produto</th>
          <th style="text-align:left">Book</th>
          <th style="text-align:left">Fator</th>
          <th style="text-align:right">Maturidade</th>
          <th style="text-align:right">Duration</th>
          <th style="text-align:right">Position (%NAV)</th>
          <th style="text-align:right">ANO_EQ (yr)</th>
        </tr></thead>
        <tbody>{pos_rows}</tbody>
      </table>"""

    # Absoluto / Relativo mode + Ambos/Real/Nominal factor filter
    toggle_html = (
        '<div class="rf-toggles" style="display:flex; gap:10px; margin-left:auto; flex-wrap:wrap">'
        f'<div class="pa-view-toggle rf-mode-toggle">'
        f'<button class="pa-tgl active" data-rf-mode="absoluto" onclick="selectRfMode(this,\'absoluto\')">Absoluto</button>'
        f'<button class="pa-tgl"        data-rf-mode="relativo" onclick="selectRfMode(this,\'relativo\')">Relativo</button>'
        '</div>'
        f'<div class="pa-view-toggle rf-view-toggle">'
        f'<button class="pa-tgl active" data-rf-view="both"    onclick="selectRfView(this,\'both\')">Ambos</button>'
        f'<button class="pa-tgl"        data-rf-view="real"    onclick="selectRfView(this,\'real\')">Juros Reais</button>'
        f'<button class="pa-tgl"        data-rf-view="nominal" onclick="selectRfView(this,\'nominal\')">Juros Nominais</button>'
        '</div>'
        '</div>'
    )

    return f"""
    <section class="card" id="rf-expo-{short}">
      <div class="card-head">
        <span class="card-title">Exposure Map — fatores RF</span>
        <span class="card-sub">— {short} · ANO_EQ (%NAV) por bucket de maturidade · {bench_note}</span>
        {toggle_html}
      </div>
      {stat_row}
      <div style="overflow-x:auto">{svg}</div>
      <div class="rf-legend mono" style="margin-top:6px; font-size:10.5px; color:var(--muted); text-align:center">
        <span class="rf-legend-item"><span class="rf-swatch rf-real"></span> Fund Juros Reais (IPCA)</span>
        <span class="rf-legend-item"><span class="rf-swatch rf-nom"></span> Fund Juros Nominais (Pré)</span>
        <span class="rf-legend-item"><span class="rf-swatch rf-benchbar"></span> Benchmark</span>
        <span class="rf-legend-item"><span class="rf-swatch rf-cum"></span> Cumulativo (Fund)</span>
      </div>
      <div style="margin-top:14px">
        <button class="rf-tbl-toggle" onclick="toggleRfTable(this)"
                aria-expanded="false">▸ Mostrar tabela (por bucket)</button>
        <div class="rf-tbl-wrap" style="display:none">
          <table class="pa-table" style="margin-top:8px" data-no-sort="1">
            <thead><tr>
              <th style="text-align:left">Bucket</th>
              <th style="text-align:right">Juros Reais (yr)</th>
              <th style="text-align:right">Juros Nominais (yr)</th>
              <th style="text-align:right">Fund Total</th>
              <th style="text-align:right">Bench</th>
              <th style="text-align:right">Relative</th>
            </tr></thead>
            <tbody>{tbl_rows}</tbody>
            <tfoot>{tbl_footer}</tfoot>
          </table>
        </div>
      </div>
      <div style="margin-top:10px">
        <button class="rf-tbl-toggle" onclick="toggleRfTable(this)"
                aria-expanded="false">▸ Mostrar posições (por ativo)</button>
        <div class="rf-tbl-wrap" style="display:none">
          {positions_table_html}
        </div>
      </div>
    </section>"""


def build_albatroz_exposure(df: pd.DataFrame, nav: float,
                            fund_label: str = "ALBATROZ") -> str:
    """RF exposure card for an RF fund — summary by indexador + top positions by |DV01|.

    CDI rows are excluded from the breakdown and totals — LFTs are floating-rate
    (duration ≈ 0) and only inflate the gross / net %NAV columns without
    representing actual rate risk. Surfaced via Σ DV01 elsewhere if needed.

    Default label = ALBATROZ; pass `fund_label="BALTRA"` to reuse for BALTRA.
    """
    if df is None or df.empty or not nav:
        return ""

    df = df[df["indexador"] != "CDI"].copy()
    # Drop rows with no rate sensitivity (mod_dur ≈ 0): Equity, IBOVSPFuture
    # noise, USDBRLFuture, FIDCs (Funds BR), Corn Futures, etc. These appear
    # as "Outros" in the by-indexador breakdown and inflate %NAV / Gross /
    # Top Posições without representing actual RF risk. CRIs (mod_dur 2-4y)
    # are preserved.
    df = df[df["mod_dur"].astype(float) > 0.01].copy()
    if df.empty:
        return ""

    # Position-weighted modified duration. delta_brl is already POSITION × MOD_DUR
    # (CLAUDE.md §8: "DELTA já é duration-weighted"). The naive `Σ(|delta|×MD)/Σ|delta|`
    # would be Σ(|POS|×MD²)/Σ(|POS|×MD) — biased toward longest-MD instruments.
    # Recovered as `Σ|delta| / Σ(|delta|/MD) = Σ(|POS|×MD) / Σ|POS|`.
    abs_delta = df["delta_brl"].abs().sum()
    abs_pos   = (df["delta_brl"].abs() / df["mod_dur"]).sum()
    dur_w = abs_delta / abs_pos if abs_pos else 0.0
    total_dv01 = df["dv01_brl"].sum()
    total_delta = df["delta_brl"].sum()

    # ── By Indexador summary ───────────────────────────────────────────
    idx_order = ["Pré", "IPCA", "IGP-M", "Outros"]
    by_idx = df.groupby("indexador").agg(
        delta_brl=("delta_brl", "sum"),
        dv01_brl=("dv01_brl", "sum"),
        gross_brl=("delta_brl", lambda s: s.abs().sum()),
        # Same fix as the aggregate dur_w above — position-weighted MD.
        dur_w=("delta_brl", lambda s: (s.abs().sum() / (s.abs() / df.loc[s.index, "mod_dur"]).sum())
                                     if (s.abs() / df.loc[s.index, "mod_dur"]).sum() else 0.0),
    ).reset_index()
    by_idx = by_idx.set_index("indexador").reindex(idx_order).reset_index().dropna(how="all", subset=["delta_brl"])

    def bp_pct(v_brl):
        pct = v_brl * 100 / nav
        color = "var(--up)" if v_brl >= 0 else "var(--down)"
        return f'<td class="t-num mono" style="color:{color}">{pct:+.2f}%</td>'

    def yr_cell(v_brl):
        # delta_brl is already duration-weighted (POSITION × MOD_DURATION) for
        # the rate primitive (post-fix in fetch_albatroz_exposure). Therefore
        # delta_brl / NAV gives duration in yrs directly. Long bond = positive
        # yrs in the engine's sign convention (DV01 negative = long).
        if v_brl is None or pd.isna(v_brl):
            return '<td class="t-num mono" style="color:var(--muted)">—</td>'
        yrs = v_brl / nav if nav else 0.0
        color = "var(--up)" if v_brl >= 0 else "var(--down)"
        return f'<td class="t-num mono" style="color:{color}">{yrs:+.2f}yr</td>'

    def mm(v):
        return _fmt_br_num(f"{v/1e6:,.1f}")

    def mm_cell(v):
        return f'<td class="t-num mono" style="color:var(--muted)">{mm(v)}</td>'

    def dv01_cell(v):
        if abs(v) < 1:
            return '<td class="t-num mono" style="color:var(--muted)">—</td>'
        color = "var(--up)" if v >= 0 else "var(--down)"
        return f'<td class="t-num mono" style="color:{color}">{_fmt_br_num(f"{v/1e3:+,.1f}")}</td>'

    def dur_cell(v):
        if v is None or abs(v) < 0.01:
            return '<td class="t-num mono" style="color:var(--muted)">—</td>'
        return f'<td class="t-num mono" style="color:var(--muted)">{v:.2f}</td>'

    # Indexador rows with drill-down: each parent row expands to show its instruments.
    idx_rows = ""
    for _i, r in enumerate(by_idx.itertuples(index=False)):
        idx_id = f"alb-idx-{_i}"
        # Parent (clickable) row
        idx_rows += (
            f'<tr class="alb-idx-row" data-idx-id="{idx_id}" '
            f'style="cursor:pointer" onclick="albToggleIdx(this)">'
            f'<td class="pa-name" style="font-weight:600">'
            f'<span class="alb-idx-arrow" style="font-size:9px;margin-right:6px;color:var(--accent-2)">▶</span>'
            f'{r.indexador}</td>'
            + yr_cell(r.delta_brl)
            + mm_cell(r.gross_brl)
            + dur_cell(r.dur_w)
            + dv01_cell(r.dv01_brl)
            + "</tr>"
        )
        # Child rows: all instruments in this indexador, sorted by |DV01| desc
        _kids = df[df["indexador"] == r.indexador].copy()
        _kids["_abs_dv01"] = _kids["dv01_brl"].abs()
        _kids = _kids.sort_values("_abs_dv01", ascending=False)
        for c in _kids.itertuples(index=False):
            _gross_c = abs(c.delta_brl)
            idx_rows += (
                f'<tr class="alb-idx-child" data-idx-parent="{idx_id}" style="display:none">'
                f'<td class="pa-name" style="padding-left:28px">'
                f'{c.PRODUCT} '
                f'<span style="color:var(--muted); font-size:10px">({c.BOOK})</span></td>'
                + yr_cell(c.delta_brl)
                + mm_cell(_gross_c)
                + dur_cell(c.mod_dur)
                + dv01_cell(c.dv01_brl)
                + "</tr>"
            )
    idx_total_row = (
        '<tr class="pa-total-row">'
        '<td class="pa-name" style="font-weight:700">Total</td>'
        + yr_cell(total_delta)
        + f'<td class="t-num mono" style="color:var(--muted); font-weight:700">{mm(abs_delta)}</td>'
        + (f'<td class="t-num mono" style="color:var(--muted); font-weight:700">{dur_w:.2f}</td>'
           if dur_w else '<td class="t-num mono" style="color:var(--muted)">—</td>')
        + (f'<td class="t-num mono" style="font-weight:700; color:{"var(--up)" if total_dv01>=0 else "var(--down)"}">{_fmt_br_num(f"{total_dv01/1e3:+,.1f}")}</td>'
           if abs(total_dv01) >= 1 else '<td class="t-num mono" style="color:var(--muted)">—</td>')
        + "</tr>"
    )

    # ── Top positions by |DV01| ────────────────────────────────────────
    top = df.copy()
    top["abs_dv01"] = top["dv01_brl"].abs()
    top = top.sort_values("abs_dv01", ascending=False).head(15)
    pos_rows = "".join(
        f'<tr>'
        f'<td class="pa-name">{r.PRODUCT}</td>'
        f'<td class="pa-name" style="color:var(--muted); font-size:11px">{r.indexador}</td>'
        f'<td class="pa-name" style="color:var(--muted); font-size:11px">{r.BOOK}</td>'
        + yr_cell(r.delta_brl)
        + dur_cell(r.mod_dur)
        + dv01_cell(r.dv01_brl)
        + "</tr>"
        for r in top.itertuples(index=False)
    )

    nav_fmt = _fmt_br_num(f"{nav/1e6:,.1f}")
    dv01_fmt = _fmt_br_num(f"{total_dv01/1e3:+,.1f}")
    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposure RF</span>
        <span class="card-sub">— {fund_label} · NAV R$ {nav_fmt}M · Duration agregada {dur_w:.2f}y · DV01 {dv01_fmt}k R$/bp</span>
        {build_vardod_trigger(fund_label)}
      </div>

      <div class="sn-inline-stats mono" style="margin-bottom:8px; display:flex; align-items:center; gap:10px">
        <span style="color:var(--muted); font-size:10.5px; letter-spacing:.12em; text-transform:uppercase">Por Indexador</span>
        <span style="color:var(--muted); font-size:9.5px">· click pra expandir</span>
        <span style="margin-left:auto; display:flex; gap:4px">
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="albExpandAllIdx()">▼ All</button>
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="albCollapseAllIdx()">▶ All</button>
        </span>
      </div>
      <table class="pa-table" data-no-sort="1">
        <thead><tr>
          <th style="text-align:left">Indexador</th>
          <th style="text-align:right">Net (yrs)</th>
          <th style="text-align:right">Gross (R$M)</th>
          <th style="text-align:right">Mod Dur (y)</th>
          <th style="text-align:right">DV01 (kR$/bp)</th>
        </tr></thead>
        <tbody>{idx_rows}</tbody>
        <tfoot>{idx_total_row}</tfoot>
      </table>
      <script>
      (function() {{
        if (window.albToggleIdx) return;
        window.albToggleIdx = function(tr) {{
          var arrow = tr.querySelector('.alb-idx-arrow');
          var open = arrow && arrow.textContent.trim() === '▼';
          if (arrow) arrow.textContent = open ? '▶' : '▼';
          var id = tr.getAttribute('data-idx-id');
          if (!id) return;
          document.querySelectorAll('.alb-idx-child[data-idx-parent="'+id+'"]').forEach(function(r) {{
            r.style.display = open ? 'none' : '';
          }});
        }};
        window.albExpandAllIdx = function() {{
          document.querySelectorAll('.alb-idx-row').forEach(function(tr) {{
            var a = tr.querySelector('.alb-idx-arrow');
            if (a && a.textContent.trim() === '▶') window.albToggleIdx(tr);
          }});
        }};
        window.albCollapseAllIdx = function() {{
          document.querySelectorAll('.alb-idx-row').forEach(function(tr) {{
            var a = tr.querySelector('.alb-idx-arrow');
            if (a && a.textContent.trim() === '▼') window.albToggleIdx(tr);
          }});
        }};
      }})();
      </script>

      <div class="sn-inline-stats mono" style="margin:16px 0 8px">
        <span style="color:var(--muted); font-size:10.5px; letter-spacing:.12em; text-transform:uppercase">Top 15 Posições — por |DV01|</span>
      </div>
      <table class="pa-table">
        <thead><tr>
          <th style="text-align:left">Produto</th>
          <th style="text-align:left">Idx</th>
          <th style="text-align:left">Book</th>
          <th style="text-align:right">Net (yrs)</th>
          <th style="text-align:right">Mod Dur (y)</th>
          <th style="text-align:right">DV01 (kR$/bp)</th>
        </tr></thead>
        <tbody>{pos_rows}</tbody>
      </table>
    </section>"""


def build_exposure_section(df_expo: pd.DataFrame, df_var: pd.DataFrame, aum: float,
                           df_expo_d1: pd.DataFrame = None, df_var_d1: pd.DataFrame = None,
                           df_pnl_prod: pd.DataFrame = None, pm_margem: dict = None,
                           diversified_var_bps: float | None = None,
                           diversified_var_bps_d1: float | None = None) -> str:
    """POSIÇÕES (unified factor × produto table) + PM VaR toggle."""

    def delta_str(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "—", "#475569"
        c = "#f87171" if val < 0 else "#4ade80"
        return f'{val:+.1f}', c

    def dv(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return ""
        return f'{val:.6f}'

    def sort_th(label, tbody_id, col_idx, paired, align="right", extra_style=""):
        paired_js = "true" if paired else "false"
        base = (f'font-size:8px;color:#475569;padding:2px 8px;cursor:pointer;'
                f'text-align:{align};user-select:none;{extra_style}')
        return (f'<th style="{base}" data-sort-col="{col_idx}" '
                f'onclick="sortTable(\'{tbody_id}\',{col_idx},{paired_js})">'
                f'{label}<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>')

    # ── D-1 lookup tables (for PM VaR view) ──────────────────────────────────
    d1_prod_expo = {}
    if df_expo_d1 is not None and not df_expo_d1.empty:
        for r in (df_expo_d1.groupby(["rf","PRODUCT","PRODUCT_CLASS"])
                             .agg(pct_nav=("pct_nav","sum")).reset_index()).itertuples(index=False):
            d1_prod_expo[(r.rf, r.PRODUCT, r.PRODUCT_CLASS)] = r.pct_nav

    dia_lookup = {}
    if df_pnl_prod is not None and not df_pnl_prod.empty:
        for r in df_pnl_prod.itertuples(index=False):
            dia_lookup[(r.LIVRO, r.PRODUCT)] = float(r.dia_bps)

    rf_var  = df_var.groupby("rf").agg(var_pct=("var_pct","sum")).reset_index()

    # ── PM × product breakdown for grouped PM VaR view ───────────────────────
    pm_prod = (df_expo.groupby(["rf","pm","PRODUCT","PRODUCT_CLASS"])
                      .agg(pct_nav=("pct_nav","sum"), delta=("delta","sum"),
                           sigma=("sigma","mean"))
                      .reset_index())
    rf_delta_tot = df_expo.groupby("rf").agg(rf_delta=("delta","sum")).reset_index()
    pm_prod = pm_prod.merge(rf_delta_tot, on="rf").merge(rf_var[["rf","var_pct"]], on="rf", how="left")
    pm_prod["prod_var_pct"] = np.where(
        pm_prod["rf_delta"] != 0,
        pm_prod["delta"] / pm_prod["rf_delta"] * pm_prod["var_pct"],
        0.0
    )

    # ── POSIÇÕES: unified factor × produto table ─────────────────────────────
    # Rename rf → factor for the shared helper.
    _expo_u    = df_expo.rename(columns={"rf": "factor"})
    _var_u     = df_var.rename(columns={"rf": "factor"})
    _expo_d1_u = df_expo_d1.rename(columns={"rf": "factor"}) if df_expo_d1 is not None and not df_expo_d1.empty else None
    _var_d1_u  = df_var_d1.rename(columns={"rf": "factor"})  if df_var_d1  is not None and not df_var_d1.empty  else None
    rf_view_table = _build_expo_unified_table(
        fund_key="macro",
        nav=aum,
        df=_expo_u,
        df_d1=_expo_d1_u,
        df_var=_var_u,
        df_var_d1=_var_d1_u,
        factor_order=_RF_ORDER,
        diversified_var_bps=diversified_var_bps,
        diversified_var_bps_d1=diversified_var_bps_d1,
    )

    # ── D-1 PM totals (expo + VaR attribution) ───────────────────────────────
    d1_pm_expo_tot = {}
    d1_pm_var_tot  = {}
    d1_pm_prod_var = {}   # keyed by (pm, rf, PRODUCT, PRODUCT_CLASS)
    if df_expo_d1 is not None and not df_expo_d1.empty:
        _pm_d1 = df_expo_d1.groupby("pm").agg(pct_nav=("pct_nav","sum")).reset_index()
        d1_pm_expo_tot = dict(zip(_pm_d1["pm"], _pm_d1["pct_nav"]))
        if df_var_d1 is not None and not df_var_d1.empty:
            _rfd_d1   = df_expo_d1.groupby("rf").agg(rf_delta=("delta","sum")).reset_index()
            _pmrf_d1  = (df_expo_d1.groupby(["rf","pm","PRODUCT","PRODUCT_CLASS"])
                                   .agg(delta=("delta","sum")).reset_index())
            _rfvar_d1 = df_var_d1.groupby("rf").agg(var_pct=("var_pct","sum")).reset_index()
            _pmrf_d1  = _pmrf_d1.merge(_rfd_d1, on="rf").merge(_rfvar_d1, on="rf", how="left")
            _pmrf_d1["pv"] = np.where(_pmrf_d1["rf_delta"] != 0,
                                      _pmrf_d1["delta"] / _pmrf_d1["rf_delta"] * _pmrf_d1["var_pct"], 0.0)
            # PM total VaR D-1
            _pm_var_d1 = _pmrf_d1.groupby("pm").agg(pv=("pv","sum")).reset_index()
            d1_pm_var_tot = dict(zip(_pm_var_d1["pm"], _pm_var_d1["pv"]))
            # product-level VaR D-1
            for r in _pmrf_d1.itertuples(index=False):
                d1_pm_prod_var[(r.pm, r.rf, r.PRODUCT, r.PRODUCT_CLASS)] = r.pv

    # ── PM total VaR today (sum of prod_var_pct across all RF) ───────────────
    pm_var_today = pm_prod.groupby("pm").agg(var_tot=("prod_var_pct","sum")).reset_index()
    pm_var_today = dict(zip(pm_var_today["pm"], pm_var_today["var_tot"]))

    # ── Global PM VaR table — expandable, same structure as POSIÇÕES ─────────
    PM_ORDER_LIST = ["CI", "LF", "JD", "RJ"]
    pm_summary_rows = ""
    for pm_name in PM_ORDER_LIST:
        prod_sub = pm_prod[pm_prod["pm"] == pm_name].sort_values("pct_nav")
        if prod_sub.empty:
            continue
        pm_pct   = prod_sub["pct_nav"].sum()
        pm_delta = prod_sub["delta"].sum()
        pm_var   = pm_var_today.get(pm_name, 0.0)
        pm_color = "#60a5fa"
        vc    = "#f87171" if pm_var    > 0 else "#94a3b8"
        # weighted-average σ for PM (weight = |pct_nav|)
        _vs = prod_sub.dropna(subset=["sigma"])
        _w  = _vs["pct_nav"].abs()
        pm_sigma = (_w * _vs["sigma"]).sum() / _w.sum() if _w.sum() > 0 else None
        sig_s_pm = f'{pm_sigma:.1f}' if pm_sigma is not None else "—"

        # Δ PM
        d1e = d1_pm_expo_tot.get(pm_name)
        d1v = d1_pm_var_tot.get(pm_name)
        dexp_raw_pm = (pm_pct - d1e) if d1e is not None else None
        dvar_raw_pm = (pm_var - d1v) if d1v is not None else None
        dexp_s, dexp_c = delta_str(dexp_raw_pm)
        dvar_s, dvar_c = delta_str(dvar_raw_pm)
        _marg_val = (pm_margem or {}).get(pm_name)
        if _marg_val is None:
            _marg_s, _marg_c = "—", "#475569"
        else:
            _marg_s = f'{_marg_val:+.0f}'
            _marg_c = "#f87171" if _marg_val <= 0 else "#facc15" if _marg_val < 20 else "#4ade80"

        # DIA: sum of PnL DIA (bps) for all products of this PM
        livro_key = _PM_LIVRO.get(pm_name, pm_name)
        pm_dia = sum(
            dia_lookup.get((livro_key, p.PRODUCT), 0.0)
            for p in prod_sub.itertuples(index=False)
        )
        pm_dia_s = f'{pm_dia:+.0f}' if pm_dia != 0 else "—"
        pm_dia_c = "#f87171" if pm_dia < 0 else "#4ade80" if pm_dia > 0 else "#475569"

        # ── instrument drill rows ─────────────────────────────────────────────
        inst_tbody_id = f"tbody-inst-{pm_name}"
        inst_rows = ""
        for _, p in prod_sub.iterrows():
            ppv_c = "#f87171" if p["pct_nav"] < 0 else "#4ade80"
            pvc2  = "#f87171" if p["prod_var_pct"] > 0 else "#64748b"

            key_expo = (p["rf"], p["PRODUCT"], p["PRODUCT_CLASS"])
            key_var  = (pm_name, p["rf"], p["PRODUCT"], p["PRODUCT_CLASS"])
            de_raw_i = (p["pct_nav"] - d1_prod_expo[key_expo]) if key_expo in d1_prod_expo else None
            dv_raw_i = (p["prod_var_pct"] - d1_pm_prod_var[key_var]) if key_var in d1_pm_prod_var else None
            de_s, de_c = delta_str(de_raw_i)
            dv_s, dv_c = delta_str(dv_raw_i)

            isig_raw = p["sigma"] if pd.notna(p.get("sigma")) else None
            isig_s = f'{isig_raw:.1f}' if isig_raw is not None else "—"
            livro_key = _PM_LIVRO.get(pm_name, pm_name)
            dia_raw = dia_lookup.get((livro_key, p["PRODUCT"]))
            dia_s = f'{dia_raw:+.1f}' if dia_raw is not None else "—"
            dia_c = "#f87171" if (dia_raw is not None and dia_raw < 0) else "#4ade80" if (dia_raw is not None and dia_raw > 0) else "#475569"
            inst_rows += (
                f'<tr style="border-top:1px solid #0f172a;background:#0a0f18">'
                f'<td style="padding:2px 24px;font-size:10px;color:#94a3b8" colspan="2" data-val="{p["PRODUCT"]}">'
                f'  {p["PRODUCT"]} <span style="color:#334155;font-size:9px">{p["PRODUCT_CLASS"]}</span></td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{ppv_c};text-align:right" data-val="{p["pct_nav"]:.6f}">{p["pct_nav"]:+.1f}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:#94a3b8;text-align:right" data-val="{dv(isig_raw)}">{isig_s}</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{de_c};text-align:right" data-val="{dv(de_raw_i)}">{de_s}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{pvc2};text-align:right" data-val="{p["prod_var_pct"]:.6f}">{p["prod_var_pct"]:.1f}</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{dv_c};text-align:right" data-val="{dv(dv_raw_i)}">{dv_s}%</td>'
                f'<td style="padding:2px 8px;font-size:10px;font-family:monospace;color:{dia_c};text-align:right" data-val="{dv(dia_raw)}">{dia_s}</td>'
                f'</tr>'
            )

        drill_id = f"pmv-{pm_name}"
        pm_summary_rows += f"""
        <tr class="metric-row" style="cursor:pointer" onclick="toggleDrillPM('{drill_id}')">
          <td style="font-size:12px;font-weight:bold;color:{pm_color};padding:5px 12px;width:50px" colspan="2" data-val="{pm_name}"><span class="pmv-caret">▶</span> {pm_name}</td>
          <td style="font-size:13px;font-family:monospace;font-weight:bold;color:#94a3b8;text-align:right;width:58px" data-val="{dv(pm_sigma)}">{sig_s_pm}</td>
          <td style="font-size:10px;font-family:monospace;color:{dexp_c};text-align:right;width:48px" data-val="{dv(dexp_raw_pm)}">{dexp_s}%</td>
          <td style="font-size:12px;font-family:monospace;color:{vc};text-align:right;width:58px" data-val="{pm_var:.6f}">{pm_var:.1f}</td>
          <td style="font-size:10px;font-family:monospace;color:{dvar_c};text-align:right;width:48px" data-val="{dv(dvar_raw_pm)}">{dvar_s}%</td>
          <td style="font-size:12px;font-family:monospace;font-weight:bold;color:{_marg_c};text-align:right;width:60px" data-val="{dv(_marg_val)}">{_marg_s}</td>
          <td style="font-size:12px;font-family:monospace;color:{pm_dia_c};text-align:right;width:58px" data-val="{pm_dia:.2f}">{pm_dia_s}</td>
        </tr>
        <tr id="{drill_id}" style="display:none">
          <td colspan="8" style="padding:0">
            <table style="width:100%;border-collapse:collapse">
              <thead><tr>
                <th colspan="2" style="font-size:8px;color:#475569;padding:2px 8px;text-align:left;cursor:pointer;user-select:none"
                    data-sort-col="0" onclick="sortTable('{inst_tbody_id}',0,false)">Instrumento<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
                {sort_th("Expo%", inst_tbody_id, 1, False)}
                {sort_th("σ (bps)", inst_tbody_id, 2, False)}
                {sort_th("ΔExpo", inst_tbody_id, 3, False)}
                {sort_th("VaR(bps)",  inst_tbody_id, 4, False)}
                {sort_th("ΔVaR",  inst_tbody_id, 5, False)}
                {sort_th("DIA",   inst_tbody_id, 6, False)}
              </tr></thead>
              <tbody id="{inst_tbody_id}">{inst_rows}</tbody>
            </table>
          </td>
        </tr>"""

    global_pm_table = f"""
    <table style="width:100%;border-collapse:collapse;margin-bottom:6px">
      <thead><tr>
        <th colspan="2" style="font-size:8px;color:#475569;padding:2px 8px;text-align:left;cursor:pointer;user-select:none"
            data-sort-col="0" onclick="sortTable('tbody-pmv',0,true)">PM<span class="sort-ind" style="opacity:0.3"> ▲▼</span></th>
        {sort_th("σ (bps)", "tbody-pmv", 1, True)}
        {sort_th("ΔExpo",  "tbody-pmv", 2, True)}
        {sort_th("VaR(bps)",   "tbody-pmv", 3, True)}
        {sort_th("ΔVaR",   "tbody-pmv", 4, True)}
        {sort_th("Margem", "tbody-pmv", 5, True)}
        {sort_th("DIA",    "tbody-pmv", 6, True)}
      </tr></thead>
      <tbody id="tbody-pmv">{pm_summary_rows}</tbody>
    </table>"""

    toggle_btns = (
        '<div style="display:inline-flex;gap:2px;margin-left:12px;vertical-align:middle">'
        '<button id="mbtn-pos" onclick="setMacroView(\'pos\')"'
        ' style="background:#1e3a5f;color:#60a5fa;border:none;border-radius:3px;padding:2px 8px;font-size:9px;cursor:pointer;letter-spacing:1px">POSIÇÕES</button>'
        '<button id="mbtn-pmv" onclick="setMacroView(\'pmv\')"'
        ' style="background:transparent;color:#475569;border:none;border-radius:3px;padding:2px 8px;font-size:9px;cursor:pointer;letter-spacing:1px">PM VaR</button>'
        '</div>'
    )

    js = """<script>
var _macroSort = {};
function _getVal(row, colIdx) {
    var td = row.cells ? row.cells[colIdx] : null;
    if (!td) return {n: NaN, s: ''};
    var v = td.getAttribute('data-val');
    if (v === null || v === '') return {n: NaN, s: ''};
    var f = parseFloat(v);
    return {n: f, s: v};
}
function _sortBodyByCol(tbody, colIdx, asc) {
    if (!tbody) return;
    var rows = Array.from(tbody.rows);
    rows.sort(function(a, b) {
        var va = _getVal(a, colIdx), vb = _getVal(b, colIdx);
        var na = va.n, nb = vb.n;
        if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
        if (!isNaN(na)) return -1;
        if (!isNaN(nb)) return  1;
        return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    });
    rows.forEach(function(r){ tbody.appendChild(r); });
}
// PM-level colIdx → instrument-level colIdx (instrument tables have extra "Expo%" col).
// PM:   0=PM  1=σ  2=ΔExpo  3=VaR  4=ΔVaR  5=Margem  6=DIA
// Inst: 0=Ins 1=Expo% 2=σ 3=ΔExpo 4=VaR 5=ΔVaR 6=DIA
var _PMV_TO_INST_COL = {0: 0, 1: 2, 2: 3, 3: 4, 4: 5, 6: 6};  // 5 (Margem) has no instrument counterpart
function sortTable(tbodyId, colIdx, paired) {
    var key = tbodyId + ':' + colIdx;
    var asc = _macroSort[key] !== true;
    _macroSort[key] = asc;
    var tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    var rows = Array.from(tbody.rows);
    function cmp(a, b) {
        var va = _getVal(a, colIdx), vb = _getVal(b, colIdx);
        var na = va.n, nb = vb.n;
        if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
        if (!isNaN(na)) return -1;
        if (!isNaN(nb)) return  1;
        return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    }
    if (paired) {
        var pairs = [];
        for (var i = 0; i + 1 < rows.length; i += 2) pairs.push([rows[i], rows[i+1]]);
        pairs.sort(function(a,b){ return cmp(a[0], b[0]); });
        pairs.forEach(function(p){ tbody.appendChild(p[0]); tbody.appendChild(p[1]); });
    } else {
        rows.sort(cmp);
        rows.forEach(function(r){ tbody.appendChild(r); });
    }
    // Cascade: when sorting the PM-level table, sort each PM's instrument table by the same key.
    if (tbodyId === 'tbody-pmv' && colIdx in _PMV_TO_INST_COL) {
        var instCol = _PMV_TO_INST_COL[colIdx];
        document.querySelectorAll('tbody[id^="tbody-inst-"]').forEach(function(tb){
            _sortBodyByCol(tb, instCol, asc);
        });
    }
    var table = tbody.closest('table');
    if (!table) return;
    var thead = table.querySelector('thead');
    if (!thead) return;
    thead.querySelectorAll('.sort-ind').forEach(function(el){ el.textContent=' ▲▼'; el.style.opacity='0.3'; });
    var act = thead.querySelector('[data-sort-col="' + colIdx + '"] .sort-ind');
    if (act) { act.textContent = asc ? ' ▲' : ' ▼'; act.style.opacity = '1'; }
}
function setMacroView(v) {
    ['pos','pmv'].forEach(function(b) {
        var btn = document.getElementById('mbtn-' + b);
        if (btn) btn.classList.toggle('active', b===v);
    });
    var rfv = document.getElementById('macro-rf-view');
    var pmv = document.getElementById('macro-pm-view');
    if (rfv) rfv.style.display = v==='pos' ? '' : 'none';
    if (pmv) pmv.style.display = v==='pmv' ? '' : 'none';
}
function toggleDrillPM(id) {
    var row = document.getElementById(id);
    if (!row) return;
    var opening = row.style.display === 'none';
    row.style.display = opening ? '' : 'none';
    var parent = row.previousElementSibling;
    if (parent) {
        var caret = parent.querySelector('.pmv-caret');
        if (caret) caret.textContent = opening ? '▼' : '▶';
    }
}
</script>"""

    nav_fmt = _fmt_br_num(f"{aum/1e6:,.1f}")

    return f"""
    {_UEXPO_JS}
    {js}
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição — MACRO</span>
        <span class="card-sub">— NAV R$ {nav_fmt}M · por fator de risco e por PM</span>
        {build_vardod_trigger("MACRO")}
        <div class="pa-view-toggle" style="margin-left:auto">
          <button id="mbtn-pos" class="pa-tgl active" onclick="setMacroView('pos')">POSIÇÕES</button>
          <button id="mbtn-pmv" class="pa-tgl"        onclick="setMacroView('pmv')">PM VaR</button>
        </div>
      </div>
      <div id="macro-rf-view">{rf_view_table}</div>
      <div id="macro-pm-view" style="display:none">{global_pm_table}</div>
    </section>"""


def build_frontier_exposure_section(df_lo: pd.DataFrame,
                                    df_ibov: pd.DataFrame,
                                    df_smll: pd.DataFrame,
                                    df_sectors: pd.DataFrame) -> str:
    """Active-weight exposure card for Frontier Long Only vs IBOV / IBOD."""
    if df_lo is None or df_lo.empty:
        return ""
    if df_ibov is None or df_ibov.empty:
        return ""

    # ── Positions ──────────────────────────────────────────────────────────────
    EXCLUDE = {"TOTAL", "SUBTOTAL"}
    # Chain filters once; copy once at the end via sort_values/reset_index.
    _mask = (df_lo["BOOK"].astype(str).str.strip() != "") & \
            (~df_lo["PRODUCT"].isin(EXCLUDE)) & \
            (df_lo["% Cash"].notna())
    stocks = df_lo[_mask].sort_values("% Cash", ascending=False).reset_index(drop=True)

    if stocks.empty:
        return ""

    total_row = df_lo[df_lo["PRODUCT"] == "TOTAL"]
    gross = float(total_row.iloc[0]["% Cash"]) if not total_row.empty else stocks["% Cash"].sum()
    cash_pct = max(0.0, 1.0 - gross)

    # ── Index weights ──────────────────────────────────────────────────────────
    ibov_wt = df_ibov.set_index("INSTRUMENT")["weight"].to_dict() if not df_ibov.empty else {}
    smll_wt = df_smll.set_index("INSTRUMENT")["weight"].to_dict() if not df_smll.empty else {}

    stocks["ibov_w"] = stocks["PRODUCT"].map(ibov_wt).fillna(0.0)
    stocks["smll_w"] = stocks["PRODUCT"].map(smll_wt).fillna(0.0)
    stocks["ibod_w"] = 0.5 * stocks["ibov_w"] + 0.5 * stocks["smll_w"]
    stocks["ibov_act"] = stocks["% Cash"] - stocks["ibov_w"]
    stocks["ibod_act"] = stocks["% Cash"] - stocks["ibod_w"]

    # ── Sector mapping ─────────────────────────────────────────────────────────
    if not df_sectors.empty:
        sec_map  = df_sectors.drop_duplicates("TICKER").set_index("TICKER")
        stocks["sector"] = stocks["PRODUCT"].map(sec_map["GLPG_SECTOR"]).fillna("Outros")
        stocks["macro"]  = stocks["PRODUCT"].map(sec_map["GLPG_MACRO_CLASSIFICATION"]).fillna("—")
    else:
        stocks["sector"] = "Outros"
        stocks["macro"]  = "—"

    # Ensure ER columns exist and are numeric (fill NaN with 0 for safe aggregation)
    for _col_name in ["TOTAL_IBVSP_DAY", "TOTAL_IBVSP_MONTH", "TOTAL_IBVSP_YEAR",
                      "TOTAL_IBOD_Benchmark_DAY", "TOTAL_IBOD_Benchmark_MONTH", "TOTAL_IBOD_Benchmark_YEAR"]:
        if _col_name not in stocks.columns:
            stocks[_col_name] = 0.0
        else:
            stocks[_col_name] = pd.to_numeric(stocks[_col_name], errors="coerce").fillna(0.0)

    # ── Header stats ───────────────────────────────────────────────────────────
    w_beta_num = (stocks["% Cash"] * stocks["BETA"].fillna(0)).sum()
    w_beta = w_beta_num / gross if gross > 0 else None

    # Ex-ante TE (beta mismatch only, σ_IBOV ≈ 20% annualized)
    _SIGMA_IBOV = 0.20
    te_ibov = abs((w_beta or 1.0) - 1.0) * _SIGMA_IBOV * 100  # in %
    te_ibod = te_ibov * 0.75  # IBOD has ~75% IBOV correlation

    def _pf(v, decimals=2, sign=False):
        try:
            f = float(v)
            s = "+" if (sign and f > 0) else ""
            return f"{s}{f*100:.{decimals}f}%"
        except Exception:
            return "—"

    def _col(v):
        try:
            return "var(--up)" if float(v) >= 0 else "var(--down)"
        except Exception:
            return "inherit"

    beta_fmt = f"{w_beta:.2f}" if w_beta is not None else "—"

    stats_bar = f"""
    <div class="sn-inline-stats mono" style="margin-bottom:12px;flex-wrap:wrap;gap:6px 16px">
      <span class="sn-stat"><span class="sn-lbl">Gross</span>
        <span class="sn-val mono">{_pf(gross)}</span></span>
      <span class="sn-stat"><span class="sn-lbl">Caixa</span>
        <span class="sn-val mono" style="color:var(--muted)">{_pf(cash_pct)}</span></span>
      <span class="sn-stat"><span class="sn-lbl">Beta pond.</span>
        <span class="sn-val mono">{beta_fmt}</span></span>
      <span style="width:1px;background:var(--border);margin:0 4px;align-self:stretch"></span>
      <span class="sn-stat"><span class="sn-lbl">TE aprox vs IBOV</span>
        <span class="sn-val mono" style="color:var(--warn)">{te_ibov:.1f}%</span></span>
      <span class="sn-stat"><span class="sn-lbl">TE aprox vs IBOD</span>
        <span class="sn-val mono" style="color:var(--warn)">{te_ibod:.1f}%</span></span>
      <span style="font-size:9px;color:var(--muted);align-self:center">(TE estimado via β; σ<sub>IBOV</sub>=20%)</span>
    </div>"""

    # ── Toggle buttons ─────────────────────────────────────────────────────────
    uid = "loexpo"
    toggle_bar = f"""
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap">
      <div style="display:flex;gap:4px">
        <button id="{uid}-ibov-btn" class="toggle-btn active"
                onclick="loExpoBmk('{uid}','ibov')">IBOV</button>
        <button id="{uid}-ibod-btn" class="toggle-btn"
                onclick="loExpoBmk('{uid}','ibod')">IBOD</button>
      </div>
      <div style="display:flex;gap:4px;margin-left:auto">
        <button id="{uid}-name-btn" class="toggle-btn active"
                onclick="loExpoView('{uid}','name')">Por Nome</button>
        <button id="{uid}-sector-btn" class="toggle-btn"
                onclick="loExpoView('{uid}','sector')">Por Setor</button>
        <span id="{uid}-expand-btns" style="display:none;gap:4px;margin-left:4px;display:none">
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="loExpoExpandAll('{uid}')">▼ All</button>
          <button class="toggle-btn" style="font-size:10px;padding:2px 7px"
                  onclick="loExpoCollapseAll('{uid}')">▶ All</button>
        </span>
      </div>
    </div>"""

    # ── Table header ───────────────────────────────────────────────────────────
    th = """<thead><tr>
      <th style="text-align:left;min-width:70px">Ticker</th>
      <th style="text-align:right">Posição</th>
      <th style="text-align:right" id="{uid}-th-bmk">Bench</th>
      <th style="text-align:right" id="{uid}-th-act">Ativo</th>
      <th style="text-align:right">Beta</th>
      <th style="text-align:right">ER D</th>
      <th style="text-align:right">ER MTD</th>
      <th style="text-align:right">ER YTD</th>
    </tr></thead>""".replace("{uid}", uid)

    def _tr(row, indent=False, is_sector=False, colspan=None):
        if is_sector:
            s = row["sector"]
            pos_tot    = row["pos_tot"]
            ibov_tot   = row["ibov_tot"]
            ibod_tot   = row["ibod_tot"]
            ibov_a_tot = row["ibov_act_tot"]
            ibod_a_tot = row["ibod_act_tot"]
            er_ibov_d  = row.get("er_ibov_d_tot", 0.0) or 0.0
            er_ibov_m  = row.get("er_ibov_m_tot", 0.0) or 0.0
            er_ibov_y  = row.get("er_ibov_y_tot", 0.0) or 0.0
            er_ibod_d  = row.get("er_ibod_d_tot", 0.0) or 0.0
            er_ibod_m  = row.get("er_ibod_m_tot", 0.0) or 0.0
            er_ibod_y  = row.get("er_ibod_y_tot", 0.0) or 0.0
            wbeta      = row.get("w_beta")

            def _sec_er(vi, vo):
                col = _col(vo)
                s = "+" if vo >= 0 else ""
                return (f'<td class="mono {uid}-er-cell" style="text-align:right;color:{col}"'
                        f' data-ibov="{vi:.6f}" data-ibod="{vo:.6f}">{s}{vo*100:.2f}%</td>')

            beta_td_s = (f'<td class="mono" style="text-align:right">{wbeta:.2f}</td>'
                         if wbeta is not None else '<td></td>')
            return (f'<tr class="{uid}-sector-row" style="background:rgba(59,130,246,0.07);'
                    f'font-weight:700;cursor:pointer" onclick="loExpoToggleSector(this)">'
                    f'<td style="padding:5px 4px;white-space:nowrap">'
                    f'<span class="{uid}-sector-arrow" style="font-size:9px;margin-right:4px">▼</span>{s}</td>'
                    f'<td class="mono" style="text-align:right">{_pf(pos_tot)}</td>'
                    f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                    f'  data-ibov="{ibov_tot:.6f}" data-ibod="{ibod_tot:.6f}">{_pf(ibov_tot)}</td>'
                    f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(ibov_a_tot)}"'
                    f'  data-ibov="{ibov_a_tot:.6f}" data-ibod="{ibod_a_tot:.6f}">{_pf(ibov_a_tot, sign=True)}</td>'
                    f'{beta_td_s}'
                    f'{_sec_er(er_ibov_d, er_ibod_d)}'
                    f'{_sec_er(er_ibov_m, er_ibod_m)}'
                    f'{_sec_er(er_ibov_y, er_ibod_y)}'
                    f'</tr>')

        tk     = row["PRODUCT"]
        pos    = row["% Cash"]
        ibov_b = row["ibov_w"]
        ibod_b = row["ibod_w"]
        ibov_a = row["ibov_act"]
        ibod_a = row["ibod_act"]
        beta   = row.get("BETA")
        # IBOD ER (primary) and IBOV ER (for toggle)
        er_ibod_d_s = row.get("TOTAL_IBOD_Benchmark_DAY")
        er_ibod_m_s = row.get("TOTAL_IBOD_Benchmark_MONTH")
        er_ibod_y_s = row.get("TOTAL_IBOD_Benchmark_YEAR")
        er_ibov_d_s = row.get("TOTAL_IBVSP_DAY")
        er_ibov_m_s = row.get("TOTAL_IBVSP_MONTH")
        er_ibov_y_s = row.get("TOTAL_IBVSP_YEAR")
        pad  = "padding-left:20px" if indent else "padding-left:4px"
        beta_td = (f'<td class="mono" style="text-align:right">{float(beta):.2f}</td>'
                   if pd.notna(beta) else '<td class="mono" style="text-align:right;color:var(--muted)">—</td>')

        def _ertd(vi, vo):
            try:
                fi = float(vo)  # default: ibod
                fv = float(vi)
                s = "+" if fi >= 0 else ""
                return (f'<td class="mono {uid}-er-cell" style="text-align:right;color:{_col(fi)}"'
                        f' data-ibov="{fv:.6f}" data-ibod="{fi:.6f}">'
                        f'{s}{fi*100:.2f}%</td>')
            except Exception:
                return '<td class="mono" style="text-align:right;color:var(--muted)">—</td>'

        return (f'<tr>'
                f'<td style="{pad};white-space:nowrap">{tk}</td>'
                f'<td class="mono" style="text-align:right">{_pf(pos)}</td>'
                f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                f'  data-ibov="{ibov_b:.6f}" data-ibod="{ibod_b:.6f}">{_pf(ibov_b)}</td>'
                f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(ibov_a)}"'
                f'  data-ibov="{ibov_a:.6f}" data-ibod="{ibod_a:.6f}">{_pf(ibov_a, sign=True)}</td>'
                f'{beta_td}'
                f'{_ertd(er_ibov_d_s, er_ibod_d_s)}'
                f'{_ertd(er_ibov_m_s, er_ibod_m_s)}'
                f'{_ertd(er_ibov_y_s, er_ibod_y_s)}'
                f'</tr>')

    # ── By Name table ──────────────────────────────────────────────────────────
    name_rows = "".join(_tr(r) for _, r in stocks.iterrows())
    # Totals row
    tot_ibov_act = stocks["ibov_act"].sum()
    tot_ibod_act = stocks["ibod_act"].sum()
    tot_ibov_bmk = stocks["ibov_w"].sum()
    tot_ibod_bmk = stocks["ibod_w"].sum()
    tot_er_ibov_d = stocks["TOTAL_IBVSP_DAY"].sum()
    tot_er_ibov_m = stocks["TOTAL_IBVSP_MONTH"].sum()
    tot_er_ibov_y = stocks["TOTAL_IBVSP_YEAR"].sum()
    tot_er_ibod_d = stocks["TOTAL_IBOD_Benchmark_DAY"].sum()
    tot_er_ibod_m = stocks["TOTAL_IBOD_Benchmark_MONTH"].sum()
    tot_er_ibod_y = stocks["TOTAL_IBOD_Benchmark_YEAR"].sum()

    def _tot_er(vi, vo):
        s = "+" if vo >= 0 else ""
        return (f'<td class="mono {uid}-er-cell" style="text-align:right;font-weight:700;color:{_col(vo)}"'
                f' data-ibov="{vi:.6f}" data-ibod="{vo:.6f}">{s}{vo*100:.2f}%</td>')

    name_rows += (f'<tr data-pinned="1" style="font-weight:700;border-top:2px solid var(--border)">'
                  f'<td>TOTAL</td>'
                  f'<td class="mono" style="text-align:right">{_pf(gross)}</td>'
                  f'<td class="mono {uid}-bmk-cell" style="text-align:right"'
                  f'  data-ibov="{tot_ibov_bmk:.6f}" data-ibod="{tot_ibod_bmk:.6f}">{_pf(tot_ibov_bmk)}</td>'
                  f'<td class="mono {uid}-act-cell" style="text-align:right;color:{_col(tot_ibov_act)}"'
                  f'  data-ibov="{tot_ibov_act:.6f}" data-ibod="{tot_ibod_act:.6f}">{_pf(tot_ibov_act, sign=True)}</td>'
                  f'<td></td>'
                  f'{_tot_er(tot_er_ibov_d, tot_er_ibod_d)}'
                  f'{_tot_er(tot_er_ibov_m, tot_er_ibod_m)}'
                  f'{_tot_er(tot_er_ibov_y, tot_er_ibod_y)}'
                  f'</tr>')
    name_rows += (f'<tr data-pinned="1" style="color:var(--muted)">'
                  f'<td style="padding-left:4px;font-style:italic">Caixa</td>'
                  f'<td class="mono" style="text-align:right">{_pf(cash_pct)}</td>'
                  f'<td colspan="6"></td></tr>')

    by_name_html = f"""
    <div id="{uid}-view-name" class="{uid}-view">
      <div style="overflow-x:auto">
        <table class="metric-table" style="font-size:11px;min-width:620px">
          {th}<tbody>{name_rows}</tbody>
        </table>
      </div>
    </div>"""

    # ── By Sector table ────────────────────────────────────────────────────────
    sector_order = (stocks.groupby("sector")["% Cash"].sum()
                    .sort_values(ascending=False).index.tolist())
    sector_rows = ""
    for sec in sector_order:
        grp = stocks[stocks["sector"] == sec].sort_values("% Cash", ascending=False)
        sec_pos    = grp["% Cash"].sum()
        sec_ibov   = grp["ibov_w"].sum()
        sec_ibod   = grp["ibod_w"].sum()
        sec_ibov_a = grp["ibov_act"].sum()
        sec_ibod_a = grp["ibod_act"].sum()
        sec_er_ibov_d = grp["TOTAL_IBVSP_DAY"].sum()
        sec_er_ibov_m = grp["TOTAL_IBVSP_MONTH"].sum()
        sec_er_ibov_y = grp["TOTAL_IBVSP_YEAR"].sum()
        sec_er_ibod_d = grp["TOTAL_IBOD_Benchmark_DAY"].sum()
        sec_er_ibod_m = grp["TOTAL_IBOD_Benchmark_MONTH"].sum()
        sec_er_ibod_y = grp["TOTAL_IBOD_Benchmark_YEAR"].sum()
        # Weighted beta: Σ(pos_i × beta_i) / Σ(pos_i)
        _b = grp.dropna(subset=["BETA"])
        sec_wbeta = ((_b["% Cash"] * _b["BETA"]).sum() / _b["% Cash"].sum()
                     if not _b.empty and _b["% Cash"].sum() > 0 else None)
        macro_c = grp.iloc[0]["macro"] if not grp.empty else "—"
        sec_data = {
            "sector":       f"{sec} <span style='font-size:9px;color:var(--muted);font-weight:400'>({macro_c})</span>",
            "pos_tot":      sec_pos,
            "ibov_tot":     sec_ibov,   "ibod_tot":     sec_ibod,
            "ibov_act_tot": sec_ibov_a, "ibod_act_tot": sec_ibod_a,
            "er_ibov_d_tot": sec_er_ibov_d, "er_ibod_d_tot": sec_er_ibod_d,
            "er_ibov_m_tot": sec_er_ibov_m, "er_ibod_m_tot": sec_er_ibod_m,
            "er_ibov_y_tot": sec_er_ibov_y, "er_ibod_y_tot": sec_er_ibod_y,
            "w_beta":       sec_wbeta,
        }
        sector_rows += _tr(sec_data, is_sector=True)
        for _, r in grp.iterrows():
            sector_rows += (f'<tr class="{uid}-child-row">' +
                            _tr(r, indent=True)[4:])  # strip leading <tr>

    by_sector_html = f"""
    <div id="{uid}-view-sector" class="{uid}-view" style="display:none">
      <div style="overflow-x:auto">
        <table class="metric-table" data-no-sort="1" style="font-size:11px;min-width:620px">
          {th}<tbody>{sector_rows}</tbody>
        </table>
      </div>
    </div>"""

    # ── JavaScript ─────────────────────────────────────────────────────────────
    js = f"""<script>
(function() {{
  var _bmk = 'ibov';
  window.loExpoBmk = function(uid, bmk) {{
    _bmk = bmk;
    ['ibov','ibod'].forEach(function(b) {{
      var btn = document.getElementById(uid+'-'+b+'-btn');
      if (btn) btn.classList.toggle('active', b === bmk);
    }});
    document.querySelectorAll('.'+uid+'-bmk-cell').forEach(function(td) {{
      var v = parseFloat(td.dataset[bmk]);
      td.textContent = isNaN(v) ? '—' : (v*100).toFixed(2)+'%';
    }});
    document.querySelectorAll('.'+uid+'-act-cell').forEach(function(td) {{
      var v = parseFloat(td.dataset[bmk]);
      if (isNaN(v)) {{ td.textContent = '—'; return; }}
      td.textContent = (v >= 0 ? '+' : '') + (v*100).toFixed(2) + '%';
      td.style.color = v >= 0 ? 'var(--up)' : 'var(--down)';
    }});
    document.querySelectorAll('.'+uid+'-er-cell').forEach(function(td) {{
      var v = parseFloat(td.dataset[bmk]);
      if (isNaN(v)) {{ td.textContent = '—'; return; }}
      td.textContent = (v >= 0 ? '+' : '') + (v*100).toFixed(2) + '%';
      td.style.color = v >= 0 ? 'var(--up)' : 'var(--down)';
    }});
  }};
  window.loExpoView = function(uid, view) {{
    ['name','sector'].forEach(function(v) {{
      var el = document.getElementById(uid+'-view-'+v);
      if (el) el.style.display = (v === view) ? '' : 'none';
      var btn = document.getElementById(uid+'-'+v+'-btn');
      if (btn) btn.classList.toggle('active', v === view);
    }});
    var expBtns = document.getElementById(uid+'-expand-btns');
    if (expBtns) expBtns.style.display = (view === 'sector') ? 'flex' : 'none';
  }};
  window.loExpoToggleSector = function(tr) {{
    var arrow = tr.querySelector('.{uid}-sector-arrow');
    var open = arrow ? arrow.textContent.trim() === '▼' : true;
    if (arrow) arrow.textContent = open ? '▶' : '▼';
    var sib = tr.nextElementSibling;
    while (sib && !sib.classList.contains('{uid}-sector-row')) {{
      sib.style.display = open ? 'none' : '';
      sib = sib.nextElementSibling;
    }}
  }};
  window.loExpoExpandAll = function(uid) {{
    document.querySelectorAll('.'+uid+'-sector-row').forEach(function(tr) {{
      var arrow = tr.querySelector('.'+uid+'-sector-arrow');
      if (arrow && arrow.textContent.trim() === '▶') window.loExpoToggleSector(tr);
    }});
  }};
  window.loExpoCollapseAll = function(uid) {{
    document.querySelectorAll('.'+uid+'-sector-row').forEach(function(tr) {{
      var arrow = tr.querySelector('.'+uid+'-sector-arrow');
      if (arrow && arrow.textContent.trim() === '▼') window.loExpoToggleSector(tr);
    }});
  }};
}})();
</script>"""

    return f"""
    <section class="card">
      <div class="card-head">
        <span class="card-title">Exposição vs Benchmark</span>
        <span class="card-sub">— Frontier Ações · Active Weight por nome e setor</span>
        {build_vardod_trigger("FRONTIER")}
      </div>
      {stats_bar}
      {toggle_bar}
      {by_name_html}
      {by_sector_html}
      {js}
    </section>"""

"""
generate_credit_report.py
=========================
Standalone credit-fund daily report. v1 = Sea Lion only.

Usage:
    python generate_credit_report.py                  # uses latest available date
    python generate_credit_report.py --date 2026-04-28
    python generate_credit_report.py --fund SEA_LION --date 2026-04-28

Output: data/credit-reports/{date}_{fund}_credit.html
"""
from __future__ import annotations

import argparse
import math
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from credit.credit_config import (
    CREDIT_FUNDS,
    INDEX_MAP,
    CREDIT_CURVE_CHAINS,
    TIPO_GROUPS,
    RISK_MONITOR_LOGO_SVG,
    POWERED_BY_FOOTER,
)
from credit.credit_data import (
    fetch_nav_history,
    fetch_nav_at,
    fetch_positions_snapshot,
    fetch_index_panel,
    fetch_credit_curves,
    fetch_issuer_limits,
    fetch_cdi_annual_rate,
    fetch_ipca_12m,
    fetch_price_quality_flags,
)


# ────────────────────────────────────────────────────────────────────────────
# helpers
# ────────────────────────────────────────────────────────────────────────────

def _prev_business_day(dt: date) -> date:
    d = dt - timedelta(days=1)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d


def fmt_brl(v: Optional[float], digits: int = 2) -> str:
    if v is None or pd.isna(v):
        return "—"
    s = f"{v:,.{digits}f}"
    # Brazilian style: , for decimal, . for thousands
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_brl_mm(v: Optional[float], digits: int = 2) -> str:
    """Compact BRL formatter in millions (e.g., R$ 64.39 MM). Used in the
    Alocação table to keep wide columns from blowing up on big positions."""
    if v is None or pd.isna(v):
        return "—"
    s = f"{v / 1_000_000:,.{digits}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".") + " MM"


def fmt_pct(v: Optional[float], digits: int = 2, signed: bool = False) -> str:
    if v is None or pd.isna(v):
        return "—"
    sign = "+" if (signed and v > 0) else ""
    return f"{sign}{v * 100:.{digits}f}%"


def fmt_yr(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{v:.2f}y"


def _csv_encode(rows: list[list]) -> str:
    """Encode rows as CSV (UTF-8 BOM added by JS)."""
    out = []
    for row in rows:
        cells = []
        for c in row:
            s = "" if c is None else str(c)
            s = s.replace('"', '""')
            cells.append(f'"{s}"')
        out.append(",".join(cells))
    return "\n".join(out)


def _csv_btn(filename: str) -> str:
    return (f'<button class="csv-btn" data-filename="{filename}" '
            f'onclick="exportCsv(this)">↓ CSV</button>')


# Credit rating quality rank — lower = better quality. Used as data-sort attribute
# on rating cells so the universal column sort sorts by quality, not alphabetically.
# "Soberano" sits above AAA: BRL Treasury debt has no corporate credit risk, only
# the same currency/inflation risk you'd already be running anyway.
RATING_ORDER = {
    "Soberano": 0,
    "AAA":  1,
    "AA+":  2, "AA":  3, "AA-":  4,
    "A+":   5, "A":   6, "A-":   7,
    "BBB+": 8, "BBB": 9, "BBB-": 10,
    "BB+":  11,"BB":  12,"BB-":  13,
    "B+":   14,"B":   15,"B-":   16,
    "CCC+": 17,"CCC": 18,"CCC-": 19,
    "CC":   20,"C":   21,"D":    22,
}


# ────────────────────────────────────────────────────────────────────────────
# metrics
# ────────────────────────────────────────────────────────────────────────────

def compute_period_returns(nav_share: pd.Series, dt: pd.Timestamp) -> dict[str, Optional[float]]:
    """Period returns (in decimal) for the fund: dia, mes, ano, 12m, 24m, 36m, since inception."""
    # snap dt to nearest available date <= dt
    s = nav_share.sort_index()
    s = s[s.index <= dt]
    if len(s) < 2:
        return {k: None for k in ("dia", "mes", "ano", "m12", "m24", "m36", "inicio")}

    last = s.iloc[-1]
    today = s.index[-1]

    def ret_to(target_dt):
        prior = s[s.index <= target_dt]
        if len(prior) == 0:
            return None
        return (last / prior.iloc[-1]) - 1.0

    # Day-1
    dia = (s.iloc[-1] / s.iloc[-2]) - 1.0 if len(s) >= 2 else None
    # MTD = since end of prior month
    mes = ret_to(today.replace(day=1) - timedelta(days=1))
    # YTD = since end of prior year
    ano = ret_to(today.replace(month=1, day=1) - timedelta(days=1))
    m12 = ret_to(today - pd.DateOffset(months=12))
    m24 = ret_to(today - pd.DateOffset(months=24))
    m36 = ret_to(today - pd.DateOffset(months=36))
    inicio = (last / s.iloc[0]) - 1.0
    return dict(dia=dia, mes=mes, ano=ano, m12=m12, m24=m24, m36=m36, inicio=inicio)


def compute_index_period_returns(idx: pd.Series, dt: pd.Timestamp) -> dict[str, Optional[float]]:
    return compute_period_returns(idx, dt)


def weighted(values: pd.Series, weights: pd.Series) -> Optional[float]:
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return None
    return float((values[mask] * weights[mask]).sum() / weights[mask].sum())


def compute_monthly_returns_grid(nav_share: pd.Series, cdi_idx: pd.Series) -> pd.DataFrame:
    """Returns long DataFrame: (year, month, fund, cdi, pct_cdi).
       month ∈ 1..12 = monthly returns; month == 13 = year total (compound of months
       that exist for that year — handles partial inception year correctly).
       CDI is clipped to fund's existence window so we don't show CDI rows for
       periods where the fund didn't exist."""
    if nav_share.empty:
        return pd.DataFrame(columns=["year","month","fund","cdi","pct_cdi"])

    fund_start = nav_share.index.min()
    fund_end = nav_share.index.max()
    nav = nav_share.sort_index()
    cdi = cdi_idx[(cdi_idx.index >= fund_start) & (cdi_idx.index <= fund_end)].sort_index()

    nav_last = nav.resample("ME").last()
    nav_first = nav.resample("ME").first()
    cdi_last = cdi.resample("ME").last()
    cdi_first = cdi.resample("ME").first()

    nav_ret = nav_last.pct_change()
    cdi_ret = cdi_last.pct_change()
    # First-month fix: use first→last within the month so partial inception month is captured
    if len(nav_last) and pd.notna(nav_first.iloc[0]) and nav_first.iloc[0] > 0:
        nav_ret.iloc[0] = (nav_last.iloc[0] / nav_first.iloc[0]) - 1
    if len(cdi_last) and pd.notna(cdi_first.iloc[0]) and cdi_first.iloc[0] > 0:
        cdi_ret.iloc[0] = (cdi_last.iloc[0] / cdi_first.iloc[0]) - 1

    df = pd.DataFrame({"fund": nav_ret, "cdi": cdi_ret})
    df["year"] = df.index.year
    df["month"] = df.index.month
    df["pct_cdi"] = df["fund"] / df["cdi"].where(df["cdi"] != 0, np.nan)

    # Months
    monthly_rows = [{
        "year": int(r["year"]), "month": int(r["month"]),
        "fund": r["fund"], "cdi": r["cdi"], "pct_cdi": r["pct_cdi"]
    } for _, r in df.iterrows()]

    # Annual totals (month=13). Compound only available months (handles partial yrs).
    annual_rows = []
    for yr, sub in df.groupby("year"):
        f_vals = sub["fund"].dropna()
        c_vals = sub["cdi"].dropna()
        f_year = float((1 + f_vals).prod() - 1) if len(f_vals) else None
        c_year = float((1 + c_vals).prod() - 1) if len(c_vals) else None
        pc_year = (f_year / c_year) if (f_year is not None and c_year is not None and c_year > 0) else None
        annual_rows.append({"year": int(yr), "month": 13,
                            "fund": f_year, "cdi": c_year, "pct_cdi": pc_year})

    return pd.DataFrame(monthly_rows + annual_rows)


def compute_position_carry(positions: pd.DataFrame,
                            cdi_annual: float,
                            ipca_annual: float) -> pd.Series:
    """Per-instrument annualized carry estimate based on indexador + spread.
       DI+ → CDI + spread; IPCA+ → IPCA + spread; Selic/LFT → CDI;
       Pré → spread (already total); else NaN."""
    cdi = cdi_annual or 0.0
    ipca = ipca_annual or 0.0
    out = []
    for _, r in positions.iterrows():
        idx_raw = r.get("indexador")
        idx = (str(idx_raw).strip() if pd.notna(idx_raw) else "")
        sp = r.get("spread")
        sp = float(sp) if pd.notna(sp) else 0.0
        if idx == "DI+":
            out.append(cdi + sp)
        elif idx == "IPCA+":
            out.append(ipca + sp)
        elif idx == "Selic" or (r.get("tipo_ativo") == "LFT"):
            out.append(cdi)
        elif idx == "%DI" or idx == "% DI":
            # Spread in this case is multiplier - 1; if spread blank assume 1.0×CDI
            mult = 1.0 + sp
            out.append(cdi * mult)
        elif idx == "Pré" or idx == "Pre":
            # taxa_emissao or spread is the absolute rate
            taxa = r.get("taxa_emissao")
            out.append(float(taxa) if pd.notna(taxa) else sp)
        else:
            out.append(np.nan)
    return pd.Series(out, index=positions.index, name="carry_anual")


def compute_portfolio_carry(positions: pd.DataFrame, nav: float) -> tuple[float, float]:
    """Returns (carry_bruto_dc, carry_liquido_total).
       Bruto DC: weighted avg spread on FIDC/CRA/CRI/Debenture rows only.
       Líquido total: weighted avg spread on full portfolio (incl LFT/Funds BR with 0 spread)."""
    p = positions.copy()
    p["spread_filled"] = p["spread"].fillna(0.0)
    # DC subset
    dc_mask = p["tipo_ativo"].isin(["FIDC", "FIDC NP", "Debenture", "Debenture Infra", "CRA", "CRI"])
    dc = p[dc_mask]
    if len(dc) and dc["pos_brl"].sum() > 0:
        carry_bruto = float((dc["spread_filled"] * dc["pos_brl"]).sum() / dc["pos_brl"].sum())
    else:
        carry_bruto = 0.0
    # Full portfolio
    if p["pos_brl"].sum() > 0:
        carry_liquido = float((p["spread_filled"] * p["pos_brl"]).sum() / p["pos_brl"].sum())
    else:
        carry_liquido = 0.0
    return carry_bruto, carry_liquido


def _approx_duration_yrs(row, ref_dt: date) -> Optional[float]:
    """If asset_master.duration is missing, approximate from data_vencimento."""
    dur = row.get("am_duration")
    if pd.notna(dur) and dur > 0:
        return float(dur)
    venc = row.get("data_vencimento")
    if pd.isna(venc) or venc is None:
        return None
    if isinstance(venc, str):
        try:
            venc = datetime.strptime(venc, "%Y-%m-%d").date()
        except ValueError:
            return None
    days = (venc - ref_dt).days
    return max(days / 365.25, 0.0) if days > 0 else 0.0


# ────────────────────────────────────────────────────────────────────────────
# CSS (compact dark theme — palette aligned with main risk monitor)
# ────────────────────────────────────────────────────────────────────────────

CSS = """
:root {
  --bg:#0b0d10; --bg-2:#111418; --panel:#14181d; --panel-2:#181d24;
  --line:#232a33; --line-2:#2d3540;
  --text:#e7ecf2; --muted:#a8b3c2; --muted-soft:#6b7480;
  --accent:#0071BB; --accent-2:#1a8fd1; --accent-deep:#183C80;
  --up:#26d07c; --down:#ff5a6a; --warn:#f5c451;
}
* { box-sizing:border-box; margin:0; padding:0; }
html, body { background:var(--bg); color:var(--text);
             font-family:'Inter', system-ui, sans-serif; -webkit-font-smoothing:antialiased; }
body {
  min-height:100vh;
  background:
    radial-gradient(1200px 600px at 10% -10%, rgba(0,113,187,.06), transparent 60%),
    radial-gradient(900px 500px at 110% 10%, rgba(26,143,209,.04), transparent 60%),
    var(--bg);
}
.mono { font-family:'JetBrains Mono', ui-monospace, monospace; font-variant-numeric:tabular-nums; }
.wrap { max-width:1280px; margin:0 auto; padding:18px 22px 60px; }

/* Top brand strip — same Risk Monitor icon used across all reports */
.report-brand-hd {
  display:flex; align-items:center; gap:14px;
  padding:10px 22px 14px; margin:-4px 0 14px;
  border-bottom:1px solid var(--line);
}
.report-brand-hd .brand { display:flex; align-items:center; gap:14px; }
.report-brand-hd .rm-logo { width:48px; height:48px; flex:0 0 auto; }
.report-brand-hd .brand-titles { display:flex; flex-direction:column; }
.report-brand-hd .brand-eyebrow {
  font-size:11px; color:var(--muted); text-transform:uppercase;
  letter-spacing:1.5px; font-weight:600;
}
.report-brand-hd .brand-title {
  font-size:20px; font-weight:700; letter-spacing:.3px;
  font-family:'Gadugi','Inter',system-ui,sans-serif;
}
.report-brand-hd .brand-meta {
  margin-left:auto; font-size:12px; color:var(--muted);
  text-align:right;
}

/* Powered by Galápagos footer */
.powered-by {
  display:flex; justify-content:flex-end; align-items:center; gap:10px;
  padding:18px 22px 26px; max-width:1280px; margin:24px auto 0;
  border-top:1px solid var(--line);
  color:var(--muted-soft); font-size:11px; letter-spacing:.4px;
  text-transform:uppercase;
}
.powered-by img { height:22px; width:auto; opacity:.85; }

/* Header */
header.fund-hd {
  display:flex; align-items:center; gap:24px; flex-wrap:wrap;
  padding:18px 22px; border:1px solid var(--line);
  border-radius:14px; background:var(--panel); margin-bottom:18px;
}
.fund-hd .name { font-size:20px; font-weight:700; letter-spacing:.2px; }
.fund-hd .sub  { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:1.5px; }
.fund-hd .stat { display:flex; flex-direction:column; gap:2px; }
.fund-hd .stat-v { font-size:18px; font-weight:600; }
.fund-hd .stat-l { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:1px; }

.up   { color:var(--up); }
.down { color:var(--down); }
.warn { color:var(--warn); }

/* Cards */
.card {
  border:1px solid var(--line); border-radius:14px; background:var(--panel);
  margin-bottom:18px; overflow:hidden;
}
.card-hd {
  padding:14px 18px; border-bottom:1px solid var(--line);
  display:flex; align-items:baseline; gap:14px; justify-content:space-between;
}
.card-title { font-size:14px; font-weight:700; letter-spacing:.4px; text-transform:uppercase; }
.card-sub { font-size:11.5px; color:var(--muted); }
.card-body { padding:14px 18px; }

/* Tables */
table { width:100%; border-collapse:collapse; font-size:12.5px; }
th, td { padding:7px 10px; text-align:right; border-bottom:1px solid var(--line); }
th { font-weight:600; color:var(--muted); font-size:11px;
     text-transform:uppercase; letter-spacing:.6px; background:var(--panel-2);
     user-select:none; }
th:first-child, td:first-child { text-align:left; }
th[data-sortable="1"] { cursor:pointer; transition:background .12s, color .12s; }
th[data-sortable="1"]:hover { background:var(--line); color:var(--text); }
th .sort-arrow { font-size:9px; margin-left:5px; opacity:.85;
                 display:inline-block; vertical-align:middle; }
th[data-sort-state="0"] .sort-arrow { color:var(--muted-soft); content:"\\2195"; }
th[data-sort-state="1"] .sort-arrow,
th[data-sort-state="2"] .sort-arrow { color:var(--accent-2); }
tr.group { background:var(--panel-2); font-weight:600; }
tr.total { background:var(--bg-2); border-top:2px solid var(--line-2); font-weight:700; }
tr.total td { color:var(--text); }

/* Alocação drill-down */
tr.alc-parent:hover { background:var(--line); }
.alc-pane tr.alc-hidden { display:none !important; }
.alc-caret { display:inline-block; width:11px; color:var(--accent-2); font-size:10px;
             margin-right:4px; transition:color .12s; }
tr.alc-parent:hover .alc-caret { color:var(--text); }
.alc-count { color:var(--muted-soft); font-size:10.5px; font-weight:500; margin-left:4px; }
.alc-toggle { display:inline-flex; gap:2px; padding:2px;
              background:var(--panel-2); border:1px solid var(--line); border-radius:7px; }
.alc-tab { padding:4px 10px; border-radius:5px; cursor:pointer;
           font-size:11px; font-weight:600; color:var(--muted);
           border:1px solid transparent; background:transparent;
           font-family:inherit; letter-spacing:.3px; transition:all .12s; }
.alc-tab:hover { color:var(--text); }
.alc-tab.active { background:var(--accent); color:#fff; border-color:var(--accent); }
.alc-cred-chip {
  display:inline-flex; align-items:baseline; gap:8px;
  background:rgba(0,113,187,.10); border:1px solid rgba(0,113,187,.32);
  border-radius:8px; padding:5px 10px;
}
.alc-cred-lbl { font-size:10px; color:var(--accent-2); font-weight:700;
                text-transform:uppercase; letter-spacing:.6px; }
.alc-cred-val { font-size:13px; color:var(--text); font-weight:700;
                font-family:'JetBrains Mono'; }
.alc-cred-pct { font-size:10.5px; color:var(--muted); font-family:'JetBrains Mono'; }

.tag { display:inline-block; padding:2px 7px; border-radius:6px; font-size:10.5px;
       font-weight:600; letter-spacing:.4px; }
/* Sovereign — sits above AAA, distinct deep-navy chip */
.tag-soberano { background:rgba(13,44,90,.65); color:#fff; font-weight:700;
                border:1px solid rgba(124,200,232,.35); }
/* Tranche subordinação tags — Senior green, Mezanino amber, Junior red */
.tag-tr-senior   { background:rgba(38,208,124,.16); color:var(--up); }
.tag-tr-mezanino { background:rgba(245,196,81,.16); color:var(--warn); }
.tag-tr-junior   { background:rgba(255,90,106,.16); color:var(--down); }
/* Investment grade tiers: green → blue → light blue (top to lowest IG) */
.tag-aaa { background:rgba(38,208,124,.14); color:var(--up); }
.tag-aa  { background:rgba(26,143,209,.14); color:var(--accent-2); }
.tag-a   { background:rgba(124,200,232,.14); color:#7cc8e8; }
/* BBB = lowest investment grade — amber */
.tag-bbb { background:rgba(245,196,81,.16); color:var(--warn); }
/* BB and below = speculative / high-yield — red */
.tag-bb  { background:rgba(255,138,90,.14); color:#ff8a5a; }
.tag-b   { background:rgba(255,90,106,.14); color:var(--down); }
.tag-ccc { background:rgba(190,40,55,.20); color:#ff5a6a; font-style:italic; }
.tag-na  { background:rgba(168,179,194,.10); color:var(--muted); }

/* Two-column grid for cards */
.grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
@media (max-width:880px){ .grid-2{grid-template-columns:1fr;} }

/* Chart container */
.chart-box { background:var(--panel-2); border:1px solid var(--line); border-radius:10px;
             padding:8px; overflow:hidden; }
.chart-box svg { display:block; max-width:100%; height:auto; }

/* Concentration breach indicator */
.bar-bg   { width:100%; height:6px; background:var(--line); border-radius:3px; overflow:hidden; }
.bar-fill { height:100%; border-radius:3px; }

/* Donut legend (HTML, below SVG) */
.donut-legend {
  display:grid; grid-template-columns:14px minmax(0,1fr) 56px; gap:4px 10px;
  padding:10px 12px 4px; font-size:11.5px; color:var(--text);
  max-height:200px; overflow-y:auto;
}
.donut-legend .dl-row { display:contents; }
.donut-legend .dl-dot { width:10px; height:10px; border-radius:2px; align-self:center; }
.donut-legend .dl-lbl { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; align-self:center; }
.donut-legend .dl-val { color:var(--muted); font-family:'JetBrains Mono'; font-variant-numeric:tabular-nums; text-align:right; align-self:center; white-space:nowrap; }

/* CSV export button */
.csv-btn {
  background:transparent; border:1px solid var(--line); color:var(--muted);
  font-size:10.5px; padding:4px 9px; border-radius:6px; cursor:pointer;
  font-family:'Inter', sans-serif; letter-spacing:.3px; transition:all .12s;
}
.csv-btn:hover { color:var(--text); border-color:var(--line-2); background:var(--panel-2); }
.card-hd-actions { display:flex; gap:8px; align-items:center; }

/* Fund-switcher tabs (consolidated report) */
.fund-tabs {
  display:flex; gap:4px; flex-wrap:wrap; padding:6px;
  background:var(--panel); border:1px solid var(--line);
  border-radius:14px; margin-bottom:18px;
  position:sticky; top:0; z-index:50;
  backdrop-filter:blur(14px);
}
.fund-tab {
  padding:9px 16px; border-radius:10px; cursor:pointer;
  font-size:13px; font-weight:600; color:var(--muted);
  border:1px solid transparent; transition:all .15s;
  font-family:inherit; background:transparent;
}
.fund-tab:hover { color:var(--text); background:var(--panel-2); }
.fund-tab.active {
  background:var(--accent); color:#fff;
  border-color:var(--accent); box-shadow:0 0 0 1px rgba(0,113,187,.30);
}
.fund-tab .tab-meta {
  font-size:10.5px; font-weight:500; color:rgba(255,255,255,.78);
  margin-left:7px; padding:2px 6px; background:rgba(255,255,255,.10);
  border-radius:5px;
}
.fund-tab:not(.active) .tab-meta { color:var(--muted-soft); background:transparent; }
.fund-pane { display:none; }
.fund-pane.active { display:block; }

/* Footer */
footer { text-align:center; color:var(--muted-soft); font-size:11px; margin-top:30px; }
"""


# ────────────────────────────────────────────────────────────────────────────
# Universal table sort — vanilla JS, no deps
#
# Behaviour: click any header in any table with >=3 data rows to sort.
# Three-state cycle: none → asc → desc → none. On reset, original order is
# restored (incl. group rows). When sorted, group rows are hidden so the
# whole instrument list flattens into one comparable list. Total row stays
# pinned to the bottom. Numeric values are detected (handles BR format
# "1.234,56", "%", "y" suffix). Empty / "—" sort to the bottom regardless
# of direction.
# ────────────────────────────────────────────────────────────────────────────

TAB_JS = r"""
(function(){
  window.selectFund = function(key){
    document.querySelectorAll('.fund-pane').forEach(function(p){
      p.classList.toggle('active', p.dataset.fund === key);
    });
    document.querySelectorAll('.fund-tab').forEach(function(t){
      t.classList.toggle('active', t.dataset.fund === key);
    });
    if(key) history.replaceState(null, '', '#' + key);
    window.scrollTo({top:0});
  };
  document.addEventListener('DOMContentLoaded', function(){
    var first = document.querySelector('.fund-pane');
    var hash = (location.hash || '').replace('#','');
    var initial = hash || (first && first.dataset.fund);
    if(initial) selectFund(initial);
  });
})();
"""

CSV_JS = r"""
(function(){
  window.exportCsv = function(btn){
    var card = btn.closest('.card');
    if(!card) return;
    var csv = card.dataset.csv;
    if(!csv){
      // Fallback: try to build from first table
      var t = card.querySelector('table');
      if(!t){ alert('Sem dados para exportar'); return; }
      var lines = [];
      Array.prototype.forEach.call(t.querySelectorAll('tr'), function(tr){
        var cells = [];
        Array.prototype.forEach.call(tr.cells, function(c){
          var s = (c.textContent || '').replace(/\s+/g,' ').trim();
          cells.push('"' + s.replace(/"/g,'""') + '"');
        });
        lines.push(cells.join(','));
      });
      csv = lines.join('\n');
    }
    var name = btn.dataset.filename || 'export.csv';
    var blob = new Blob(['﻿' + csv], {type:'text/csv;charset=utf-8'});
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url; a.download = name;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };
})();
"""

ALOC_JS = r"""
(function(){
  // Drill-down on the Alocação card. Parent groupings (Tipo / Grupo /
  // Subordinação / Rating) are pre-rendered as parallel <div class="alc-pane">
  // elements; only the active one is visible. Each parent row toggles its
  // children via the caret. Card also supports Expand-all / Collapse-all and
  // per-column sorting on every th in the active pane's table.

  function _setExpand(parent, expanded){
    var card = parent.closest('.card'); if(!card) return;
    var key = parent.dataset.alcKey;
    var mode = parent.dataset.alcMode;
    var caret = parent.querySelector('.alc-caret');
    if(expanded){
      parent.classList.remove('alc-collapsed');
      if(caret) caret.textContent = '▼';
    } else {
      parent.classList.add('alc-collapsed');
      if(caret) caret.textContent = '▶';
    }
    card.querySelectorAll(
      'tr.alc-child[data-parent="'+key+'"][data-alc-mode="'+mode+'"]'
    ).forEach(function(r){
      r.classList.toggle('alc-hidden', !expanded);
      // Defensive — clear any leftover inline display from older renders
      if(expanded) r.style.removeProperty('display');
    });
  }

  window.toggleAlcRow = function(parent){
    var collapsed = parent.classList.contains('alc-collapsed');
    _setExpand(parent, collapsed);  // toggle: expand if currently collapsed
  };

  window.setAlcGroup = function(btn, mode){
    var card = btn.closest('.card'); if(!card) return;
    card.querySelectorAll('.alc-pane').forEach(function(p){
      p.style.display = (p.dataset.alcMode === mode) ? '' : 'none';
    });
    card.querySelectorAll('.alc-tab').forEach(function(t){
      t.classList.toggle('active', t.dataset.alcMode === mode);
    });
  };

  // Expand-all / collapse-all — operate on the *visible* pane only so each
  // grouping mode keeps its own state.
  function _activePane(card){
    var panes = card.querySelectorAll('.alc-pane');
    for(var i=0;i<panes.length;i++){
      if(panes[i].style.display !== 'none') return panes[i];
    }
    return panes[0];
  }
  window.alcExpandAll = function(btn){
    var card = btn.closest('.card'); if(!card) return;
    var pane = _activePane(card); if(!pane) return;
    pane.querySelectorAll('tr.alc-parent').forEach(function(p){ _setExpand(p, true); });
  };
  window.alcCollapseAll = function(btn){
    var card = btn.closest('.card'); if(!card) return;
    var pane = _activePane(card); if(!pane) return;
    pane.querySelectorAll('tr.alc-parent').forEach(function(p){ _setExpand(p, false); });
  };

  // ─── Per-column sort for alc-pane tables ───────────────────────────────
  // Scoped to '.alc-pane table' so it doesn't double-attach with the host
  // report's own sort. Three-state cycle: none → asc → desc → none. Group
  // (parent) rows hide when sorted; child rows are flattened. Total row stays
  // pinned at the bottom.
  function _cellValue(cell){
    if(!cell) return null;
    if(cell.dataset && cell.dataset.sort){
      var n = parseFloat(cell.dataset.sort);
      if(!isNaN(n)) return n;
    }
    var s = (cell.textContent || '').trim();
    if(!s || s === '—' || s === '-') return null;
    var num = parseFloat(s.replace(/\./g,'').replace(',','.').replace(/[%y\s]/g,'').replace('MM','').trim());
    if(!isNaN(num)) return num;
    return s.toLowerCase();
  }
  function _cmp(a, b, dir){
    if(a === null && b === null) return 0;
    if(a === null) return 1;
    if(b === null) return -1;
    if(a < b) return dir === 'asc' ? -1 : 1;
    if(a > b) return dir === 'asc' ? 1 : -1;
    return 0;
  }
  function _attachAlcSort(table){
    if(table.dataset.alcSorted === '1') return;
    table.dataset.alcSorted = '1';
    var rows = Array.prototype.slice.call(table.querySelectorAll('tr'));
    if(rows.length < 3) return;
    var headerRow = rows[0];
    var ths = Array.prototype.slice.call(headerRow.querySelectorAll('th'));
    var dataRows = [], groupRows = [], totalRow = null;
    rows.slice(1).forEach(function(r){
      if(r.classList.contains('group')) groupRows.push(r);
      else if(r.classList.contains('total')) totalRow = r;
      else dataRows.push(r);
    });
    if(dataRows.length < 2) return;
    var originalOrder = rows.slice(1);
    ths.forEach(function(th, idx){
      th.dataset.sortable = '1';
      th.dataset.sortState = '0';
      var arrow = document.createElement('span');
      arrow.className = 'sort-arrow';
      arrow.textContent = '↕';
      th.appendChild(arrow);
      th.style.cursor = 'pointer';
      th.addEventListener('click', function(){
        var cur = parseInt(th.dataset.sortState, 10) || 0;
        var next = (cur + 1) % 3;
        ths.forEach(function(t){
          if(t !== th){ t.dataset.sortState = '0'; var a = t.querySelector('.sort-arrow'); if(a) a.textContent = '↕'; }
        });
        th.dataset.sortState = String(next);
        th.querySelector('.sort-arrow').textContent = next === 1 ? '▲' : next === 2 ? '▼' : '↕';
        var parent = dataRows[0].parentNode;
        if(next === 0){
          // Reset: restore original DOM order and re-apply each parent's
          // collapsed/expanded state (including the default-collapsed setup).
          originalOrder.forEach(function(r){ parent.appendChild(r); });
          groupRows.forEach(function(r){ r.style.display = ''; });
          // Re-hide children whose parent is collapsed; show others
          dataRows.forEach(function(r){
            var pkey = r.getAttribute('data-parent');
            var pmode = r.getAttribute('data-alc-mode');
            var owner = parent.querySelector(
              'tr.alc-parent[data-alc-key="'+pkey+'"][data-alc-mode="'+pmode+'"]'
            );
            var collapsed = owner && owner.classList.contains('alc-collapsed');
            r.classList.toggle('alc-hidden', !!collapsed);
            r.style.removeProperty('display');
          });
          return;
        }
        var dir = next === 1 ? 'asc' : 'desc';
        var sorted = dataRows.slice().sort(function(a, b){
          return _cmp(_cellValue(a.cells[idx]), _cellValue(b.cells[idx]), dir);
        });
        // Sort flattens — hide group rows, force-show all data rows regardless
        // of their parent's collapsed state (so the user sees everything sorted).
        groupRows.forEach(function(r){ r.style.display = 'none'; });
        dataRows.forEach(function(r){
          r.classList.remove('alc-hidden');
          r.style.removeProperty('display');
        });
        sorted.forEach(function(r){ parent.appendChild(r); });
        if(totalRow) parent.appendChild(totalRow);
      });
    });
  }
  function _initAlcSort(){
    document.querySelectorAll('.alc-pane table').forEach(_attachAlcSort);
  }
  // Expose so host pages can re-run after lazy DOM injection (templates
  // hydrated into the page after initial load).
  window.initAlcSort = _initAlcSort;
  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', _initAlcSort);
  } else {
    _initAlcSort();
  }
})();
"""

SORT_JS = r"""
(function(){
  // Cell value resolver: prefers explicit data-sort (numeric rank), then numeric
  // parse from text content, then lowercase string. Used by the column sorter
  // so columns like "Rating" sort by quality rank (AAA=1 ... D=22) instead of
  // alphabetically (where BBB > AAA).
  function cellValue(cell){
    if(!cell) return null;
    if(cell.dataset && cell.dataset.sort){
      var n = parseFloat(cell.dataset.sort);
      if(!isNaN(n)) return n;
    }
    return parseValue(cell.textContent);
  }
  function parseValue(s){
    if(!s) return null;
    s = s.trim();
    if(!s || s === '—' || s === '-') return null;
    // BR number: 1.234.567,89%  → 1234567.89
    var num = parseFloat(s.replace(/\./g,'').replace(',','.').replace(/[%y\s]/g,''));
    if(!isNaN(num)) return num;
    return s.toLowerCase();
  }
  function cmp(a, b, dir){
    if(a === null && b === null) return 0;
    if(a === null) return 1;   // nulls always last
    if(b === null) return -1;
    if(a < b) return dir === 'asc' ? -1 : 1;
    if(a > b) return dir === 'asc' ? 1 : -1;
    return 0;
  }
  function attach(table){
    var rows = Array.prototype.slice.call(table.querySelectorAll('tr'));
    if(rows.length < 4) return;  // need header + ≥3 data rows
    var headerRow = rows[0];
    var ths = Array.prototype.slice.call(headerRow.querySelectorAll('th'));
    if(ths.length === 0) return;
    var dataRows = [];
    var groupRows = [];
    var totalRow = null;
    rows.slice(1).forEach(function(r){
      if(r.classList.contains('group')) groupRows.push(r);
      else if(r.classList.contains('total')) totalRow = r;
      else dataRows.push(r);
    });
    if(dataRows.length < 2) return;
    var originalOrder = rows.slice(1);

    ths.forEach(function(th, idx){
      th.dataset.sortable = '1';
      th.dataset.sortState = '0';
      var arrow = document.createElement('span');
      arrow.className = 'sort-arrow';
      arrow.textContent = '↕';
      th.appendChild(arrow);
      th.addEventListener('click', function(){
        var cur = parseInt(th.dataset.sortState, 10) || 0;
        var next = (cur + 1) % 3;  // 0=none, 1=asc, 2=desc
        ths.forEach(function(t){
          if(t !== th){ t.dataset.sortState='0'; t.querySelector('.sort-arrow').textContent='↕'; }
        });
        th.dataset.sortState = String(next);
        th.querySelector('.sort-arrow').textContent = next === 1 ? '▲' : next === 2 ? '▼' : '↕';

        var parent = dataRows[0].parentNode;
        if(next === 0){
          // Restore original order, show group rows
          originalOrder.forEach(function(r){ parent.appendChild(r); });
          groupRows.forEach(function(r){ r.style.display=''; });
          return;
        }
        var dir = next === 1 ? 'asc' : 'desc';
        var sorted = dataRows.slice().sort(function(a,b){
          var va = cellValue(a.cells[idx]);
          var vb = cellValue(b.cells[idx]);
          return cmp(va, vb, dir);
        });
        groupRows.forEach(function(r){ r.style.display='none'; });
        sorted.forEach(function(r){ parent.appendChild(r); });
        if(totalRow) parent.appendChild(totalRow);
      });
    });
  }
  document.querySelectorAll('table').forEach(attach);
})();
"""


# ────────────────────────────────────────────────────────────────────────────
# SVG primitives
# ────────────────────────────────────────────────────────────────────────────

def _svg_line_chart(series_dict: dict[str, pd.Series], width: int = 760, height: int = 240,
                    palette: dict[str, str] | None = None,
                    rebase_to_zero: bool = True) -> str:
    """Multi-line chart. series_dict: label -> series indexed by date."""
    if not series_dict:
        return f'<svg width="{width}" height="{height}"></svg>'
    palette = palette or {}
    default_colors = ["#0071BB", "#1a8fd1", "#26d07c", "#f5c451", "#ff5a6a", "#a890ff"]

    # Align dates (intersect)
    df = pd.concat(series_dict.values(), axis=1).ffill().dropna(how="all")
    df.columns = list(series_dict.keys())
    if len(df) == 0:
        return f'<svg width="{width}" height="{height}"></svg>'

    if rebase_to_zero:
        df = (df / df.iloc[0]) - 1.0

    # Layout
    pad_l, pad_r, pad_t, pad_b = 50, 16, 14, 28
    plot_w, plot_h = width - pad_l - pad_r, height - pad_t - pad_b
    n = len(df)
    xs = np.linspace(pad_l, pad_l + plot_w, n)
    y_min = df.min().min()
    y_max = df.max().max()
    if y_min == y_max:
        y_min -= 0.005; y_max += 0.005
    pad = (y_max - y_min) * 0.10
    y_min -= pad; y_max += pad

    def y_to_px(y):
        return pad_t + plot_h * (1 - (y - y_min) / (y_max - y_min))

    # Gridlines (5 horizontal)
    grid = []
    for i in range(5):
        gy = y_min + (y_max - y_min) * i / 4
        py = y_to_px(gy)
        grid.append(f'<line x1="{pad_l}" y1="{py:.1f}" x2="{pad_l+plot_w}" y2="{py:.1f}" stroke="#232a33" stroke-width="0.5"/>')
        grid.append(f'<text x="{pad_l-6}" y="{py+3:.1f}" text-anchor="end" font-size="9.5" fill="#a8b3c2" font-family="JetBrains Mono">{gy*100:+.1f}%</text>')

    # Lines
    paths = []
    legend_parts = []
    for i, col in enumerate(df.columns):
        color = palette.get(col, default_colors[i % len(default_colors)])
        pts = " ".join(f"{xs[k]:.1f},{y_to_px(df[col].iloc[k]):.1f}" for k in range(n) if pd.notna(df[col].iloc[k]))
        paths.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="1.6"/>')
        legend_parts.append(f'<span style="color:{color};font-weight:600">●</span> <span style="color:#e7ecf2">{col}</span>')

    # X axis date ticks (4)
    xticks = []
    for k in (0, n // 3, 2 * n // 3, n - 1):
        if k < 0 or k >= n: continue
        d = df.index[k]
        label = d.strftime("%b-%y")
        xticks.append(f'<text x="{xs[k]:.1f}" y="{height-8}" text-anchor="middle" font-size="9.5" fill="#a8b3c2" font-family="JetBrains Mono">{label}</text>')

    svg = (f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">'
           f'<rect width="100%" height="100%" fill="#181d24"/>'
           + "".join(grid) + "".join(paths) + "".join(xticks)
           + '</svg>')
    legend = '<div style="display:flex;gap:18px;flex-wrap:wrap;margin-top:6px;font-size:11px">' + " · ".join(legend_parts) + "</div>"
    return svg + legend


def _svg_donut(items: list[tuple[str, float]], width: int = 260, height: int = 220,
                palette: list[str] | None = None, label_fmt=None,
                tooltip_fmt=None) -> str:
    """Donut chart with HTML legend below (always fits regardless of container width).
       Each slice has a hover-friendly <title>. ``tooltip_fmt(lbl, v, frac)`` overrides
       the default ``"<label> · X.X%"`` tooltip — used e.g. for the Subordinação donut
       where the slice label is % of credit but the tooltip shows % of NAV.
       Returns the SVG + a <div class="donut-legend"> beneath it."""
    if not items:
        return f'<svg width="{width}" height="{height}"></svg>'
    palette = palette or [
        "#0d2c5a", "#1a8fd1", "#7cc8e8", "#0071BB", "#26d07c",
        "#f5c451", "#ff5a6a", "#a890ff", "#5a8eb0", "#7a8a99",
        "#c9d1dd", "#3d4858",
    ]
    total = sum(v for _, v in items if v and v > 0)
    if total <= 0:
        return f'<svg width="{width}" height="{height}"></svg>'

    cx, cy = width / 2, height / 2
    r_out, r_in = min(width, height) / 2 - 30, min(width, height) / 2 - 60
    parts = [f'<rect width="100%" height="100%" fill="#181d24"/>']

    angle = -math.pi / 2
    legend_items = []
    label_fmt = label_fmt or (lambda v: f"{v/total*100:.1f}%")

    for i, (lbl, v) in enumerate(items):
        if not v or v <= 0:
            continue
        frac = v / total
        a2 = angle + frac * 2 * math.pi
        large = 1 if frac > 0.5 else 0
        x1, y1 = cx + r_out * math.cos(angle), cy + r_out * math.sin(angle)
        x2, y2 = cx + r_out * math.cos(a2),    cy + r_out * math.sin(a2)
        x3, y3 = cx + r_in  * math.cos(a2),    cy + r_in  * math.sin(a2)
        x4, y4 = cx + r_in  * math.cos(angle), cy + r_in  * math.sin(angle)
        color = palette[i % len(palette)]
        d = (f'M {x1:.2f} {y1:.2f} A {r_out} {r_out} 0 {large} 1 {x2:.2f} {y2:.2f} '
             f'L {x3:.2f} {y3:.2f} A {r_in} {r_in} 0 {large} 0 {x4:.2f} {y4:.2f} Z')
        lbl_str = str(lbl) if lbl is not None and not (isinstance(lbl, float) and pd.isna(lbl)) else "—"
        title = tooltip_fmt(lbl_str, v, frac) if tooltip_fmt else f'{lbl_str} · {frac*100:.1f}%'
        parts.append(
            f'<path d="{d}" fill="{color}" stroke="#181d24" stroke-width="1.5"><title>{title}</title></path>'
        )
        # Inside-slice % label only for big slices (>= 7%)
        if frac >= 0.07:
            mid = (angle + a2) / 2
            r_mid = (r_out + r_in) / 2
            lx = cx + r_mid * math.cos(mid)
            ly = cy + r_mid * math.sin(mid) + 3
            parts.append(
                f'<text x="{lx:.1f}" y="{ly:.1f}" text-anchor="middle" '
                f'font-size="10.5" fill="#ffffff" font-weight="600" font-family="JetBrains Mono" '
                f'pointer-events="none">{label_fmt(v)}</text>'
            )
        legend_items.append((color, lbl_str, frac, v))
        angle = a2

    svg = (f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
           f'xmlns="http://www.w3.org/2000/svg">' + "".join(parts) + '</svg>')

    legend_rows = []
    for color, lbl, frac, _ in legend_items:
        legend_rows.append(
            f'<div class="dl-row">'
            f'<span class="dl-dot" style="background:{color}"></span>'
            f'<span class="dl-lbl" title="{lbl}">{lbl}</span>'
            f'<span class="dl-val">{frac*100:.1f}%</span>'
            f'</div>'
        )
    legend_html = '<div class="donut-legend">' + "".join(legend_rows) + '</div>'

    return svg + legend_html


def _svg_hbar(items: list[tuple[str, float]], width: int = 460, row_h: int = 22,
               max_rows: int = 12, fmt=None, color: str = "#0071BB") -> str:
    """Horizontal bar chart with full labels (no truncation), value at right + tooltip."""
    items = sorted([(l, v) for l, v in items if v is not None], key=lambda x: -x[1])[:max_rows]
    if not items:
        return f'<svg width="{width}" height="40"></svg>'
    fmt = fmt or (lambda v: f"{v*100:.2f}%")
    max_v = max(v for _, v in items) or 1
    height = row_h * len(items) + 10
    label_w = 200
    bar_x0 = label_w + 6
    bar_max = width - bar_x0 - 60
    parts = [f'<rect width="100%" height="100%" fill="#181d24"/>']
    for i, (lbl, v) in enumerate(items):
        y = i * row_h + row_h - 6
        bar_w = (v / max_v) * bar_max
        full = lbl or "—"
        title = f'{full} · {fmt(v)}'
        # Label with foreignObject would be ideal but SVG <text> + clipPath also works.
        parts.append(
            f'<text x="6" y="{y+1}" font-size="11" fill="#e7ecf2" font-family="Inter">'
            f'<title>{title}</title>{full}</text>'
        )
        parts.append(
            f'<rect x="{bar_x0}" y="{y-12}" width="{bar_w:.1f}" height="14" rx="2" '
            f'fill="{color}" opacity="0.85"><title>{title}</title></rect>'
        )
        parts.append(f'<text x="{width-6}" y="{y+1}" text-anchor="end" font-size="11" fill="#e7ecf2" font-family="JetBrains Mono">{fmt(v)}</text>')
    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">' + "".join(parts) + '</svg>'


def _svg_vbar(items: list[tuple[str, float]], width: int = 540, height: int = 320,
               color: str = "#0071BB", max_bars: int = 16, fmt=None) -> str:
    """Vertical bar chart with full rotated x-labels (no truncation).
       Height is sized so longest label fits within pad_b at -45° rotation."""
    items = sorted([(l, v) for l, v in items if v is not None], key=lambda x: -x[1])[:max_bars]
    if not items:
        return f'<svg width="{width}" height="{height}"></svg>'
    fmt = fmt or (lambda v: f"{v*100:.2f}%")

    # Estimate pad_b from longest label: ~5px per char × cos(45°) ≈ 3.5px per char
    longest = max((len(str(l or "")) for l, _ in items), default=12)
    pad_b = max(80, int(longest * 4.4) + 18)

    pad_l, pad_r, pad_t = 18, 14, 24
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    n = len(items)
    bar_w = plot_w / max(n, 1) * 0.72
    gap = (plot_w - bar_w * n) / max(n - 1, 1) if n > 1 else 0
    max_v = max(v for _, v in items) or 1
    parts = [f'<rect width="100%" height="100%" fill="#181d24"/>']
    for i, (lbl, v) in enumerate(items):
        x = pad_l + i * (bar_w + gap)
        h = (v / max_v) * plot_h
        y = pad_t + (plot_h - h)
        title = f'{lbl} · {fmt(v)}'
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" rx="2" '
            f'fill="{color}" opacity="0.88"><title>{title}</title></rect>'
        )
        parts.append(f'<text x="{x+bar_w/2:.1f}" y="{y-4:.1f}" text-anchor="middle" font-size="10" fill="#e7ecf2" font-family="JetBrains Mono">{fmt(v)}</text>')
        cx = x + bar_w / 2
        cy_lbl = pad_t + plot_h + 10
        full = lbl or "—"
        parts.append(
            f'<text x="{cx:.1f}" y="{cy_lbl:.1f}" text-anchor="end" font-size="10" fill="#a8b3c2" '
            f'font-family="Inter" transform="rotate(-45 {cx:.1f} {cy_lbl:.1f})">{full}</text>'
        )
    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">' + "".join(parts) + '</svg>'


def _svg_aum_chart(nav_h: pd.DataFrame, width: int = 760, height: int = 240) -> str:
    """NAV history line chart with annotated peak / trough / latest."""
    s = nav_h["NAV"].astype(float).sort_index()
    if len(s) < 5:
        return f'<svg width="{width}" height="{height}"></svg>'
    pad_l, pad_r, pad_t, pad_b = 50, 18, 14, 28
    plot_w, plot_h = width - pad_l - pad_r, height - pad_t - pad_b
    n = len(s)
    xs = np.linspace(pad_l, pad_l + plot_w, n)
    y_min = float(s.min())
    y_max = float(s.max())
    pad = (y_max - y_min) * 0.10 or y_max * 0.05
    y_min -= pad; y_max += pad
    def y2px(y): return pad_t + plot_h * (1 - (y - y_min) / (y_max - y_min))

    parts = [f'<rect width="100%" height="100%" fill="#181d24"/>']
    # Gridlines
    for i in range(5):
        gy = y_min + (y_max - y_min) * i / 4
        py = y2px(gy)
        parts.append(f'<line x1="{pad_l}" y1="{py:.1f}" x2="{pad_l+plot_w}" y2="{py:.1f}" stroke="#232a33" stroke-width="0.5"/>')
        parts.append(f'<text x="{pad_l-6}" y="{py+3:.1f}" text-anchor="end" font-size="9.5" fill="#a8b3c2" font-family="JetBrains Mono">{gy/1e6:.0f}M</text>')

    pts = " ".join(f"{xs[k]:.1f},{y2px(s.iloc[k]):.1f}" for k in range(n))
    parts.append(f'<polyline points="{pts}" fill="none" stroke="#0071BB" stroke-width="1.8"/>')
    # Fill below
    pts_fill = pts + f" {xs[-1]:.1f},{pad_t+plot_h:.1f} {xs[0]:.1f},{pad_t+plot_h:.1f}"
    parts.append(f'<polygon points="{pts_fill}" fill="#0071BB" fill-opacity="0.12"/>')

    # Annotate peak / trough / latest
    peak_idx = int(s.values.argmax())
    trough_idx = int(s.values.argmin())
    last_idx = n - 1
    for idx, label in [(peak_idx, "peak"), (trough_idx, "trough"), (last_idx, "atual")]:
        v = float(s.iloc[idx])
        x_, y_ = xs[idx], y2px(v)
        color = "#26d07c" if label == "peak" else "#ff5a6a" if label == "trough" else "#e7ecf2"
        parts.append(f'<circle cx="{x_:.1f}" cy="{y_:.1f}" r="3.5" fill="{color}"/>')
        parts.append(f'<text x="{x_:.1f}" y="{y_-7:.1f}" text-anchor="middle" font-size="10" fill="{color}" font-family="JetBrains Mono">{v/1e6:.0f}M</text>')

    # X-axis labels (years)
    ts_idx = pd.DatetimeIndex(s.index)
    years = sorted(ts_idx.year.unique())
    for yr in years:
        # Find first index of this year
        mask = ts_idx.year == yr
        if mask.any():
            i = mask.argmax()
            parts.append(f'<text x="{xs[i]:.1f}" y="{height-8}" font-size="9.5" fill="#a8b3c2" font-family="JetBrains Mono">{yr}</text>')

    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">' + "".join(parts) + '</svg>'


def _svg_curves(curves: dict[str, pd.DataFrame], width: int = 520, height: int = 240) -> str:
    """Render AAA/AA/A curves on the same axes. tenor_bdays on x, rate on y."""
    palette = {"AAA": "#0d2c5a", "AA": "#1a8fd1", "A": "#7cc8e8"}
    pad_l, pad_r, pad_t, pad_b = 44, 16, 14, 30
    plot_w, plot_h = width - pad_l - pad_r, height - pad_t - pad_b

    # Combine all tenors + rates to set axis bounds
    all_tenors = []
    all_rates = []
    for r, df in curves.items():
        if df.empty: continue
        all_tenors.extend(df["tenor_bdays"].tolist())
        all_rates.extend(df["rate"].dropna().tolist())
        if "rate_prev" in df.columns:
            all_rates.extend(df["rate_prev"].dropna().tolist())
    if not all_tenors or not all_rates:
        return f'<svg width="{width}" height="{height}"></svg>'

    x_min, x_max = min(all_tenors), max(all_tenors)
    y_min, y_max = min(all_rates), max(all_rates)
    if y_min == y_max: y_min -= 0.1; y_max += 0.1
    pad_y = (y_max - y_min) * 0.10
    y_min -= pad_y; y_max += pad_y

    def x2px(x): return pad_l + plot_w * (x - x_min) / (x_max - x_min)
    def y2px(y): return pad_t + plot_h * (1 - (y - y_min) / (y_max - y_min))

    # Gridlines
    parts = ['<rect width="100%" height="100%" fill="#181d24"/>']
    for i in range(5):
        gy = y_min + (y_max - y_min) * i / 4
        py = y2px(gy)
        parts.append(f'<line x1="{pad_l}" y1="{py:.1f}" x2="{pad_l+plot_w}" y2="{py:.1f}" stroke="#232a33" stroke-width="0.5"/>')
        parts.append(f'<text x="{pad_l-6}" y="{py+3:.1f}" text-anchor="end" font-size="9.5" fill="#a8b3c2" font-family="JetBrains Mono">{gy:.2f}</text>')

    # Curves
    legend = []
    for r, df in curves.items():
        df = df.sort_values("tenor_bdays")
        if df.empty: continue
        color = palette.get(r, "#1a8fd1")
        pts = " ".join(f"{x2px(t):.1f},{y2px(rt):.1f}" for t, rt in zip(df['tenor_bdays'], df['rate']) if pd.notna(rt))
        if pts:
            parts.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/>')
        if "rate_prev" in df.columns and df["rate_prev"].notna().any():
            pts_p = " ".join(f"{x2px(t):.1f},{y2px(rt):.1f}" for t, rt in zip(df['tenor_bdays'], df['rate_prev']) if pd.notna(rt))
            if pts_p:
                parts.append(f'<polyline points="{pts_p}" fill="none" stroke="{color}" stroke-width="1.4" stroke-dasharray="4,3" opacity="0.7"/>')
        legend.append(f'<span style="color:{color};font-weight:600">●</span> <span style="color:#e7ecf2">{r}</span>')

    # X-axis ticks (5)
    n_ticks = 5
    for i in range(n_ticks):
        x = x_min + (x_max - x_min) * i / (n_ticks - 1)
        years = x / 252
        parts.append(f'<text x="{x2px(x):.1f}" y="{height-10}" text-anchor="middle" font-size="9.5" fill="#a8b3c2" font-family="JetBrains Mono">{years:.0f}y</text>')

    svg = f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">' + "".join(parts) + '</svg>'
    legend_html = '<div style="display:flex;gap:18px;margin-top:6px;font-size:11px">' + " · ".join(legend) + ' · <span style="color:#a8b3c2">tracejado = D-1</span></div>'
    return svg + legend_html


# ────────────────────────────────────────────────────────────────────────────
# Cards
# ────────────────────────────────────────────────────────────────────────────

def render_monthly_heatmap_html(grid: pd.DataFrame) -> str:
    """Heatmap: year rows × (Fundo/CDI/% do CDI) sub-rows × Jan-Dec + Total column.
       Drops years where the fund had no observations to keep CDI from showing for
       pre-inception periods."""
    months_pt = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    if grid.empty:
        return '<div style="padding:14px;color:var(--muted)">Sem dados mensais.</div>'

    # Only show years where the fund actually had data (CDI alone shouldn't appear)
    fund_years = grid.loc[grid["fund"].notna(), "year"].unique()
    if len(fund_years) == 0:
        return '<div style="padding:14px;color:var(--muted)">Sem dados mensais.</div>'
    years = sorted(fund_years)

    head = ('<tr><th>Ano</th><th></th>'
            + "".join(f'<th>{m}</th>' for m in months_pt)
            + '<th style="background:var(--line)">Total</th></tr>')

    def cell_color_pct(v):
        # %CDI: ≥100% dark green, 0-100% light green, <0 red
        if v is None or pd.isna(v): return "transparent"
        if v < 0:    return "rgba(255,90,106,.22)"
        if v >= 1.0: return "rgba(20,140,70,.42)"
        return "rgba(38,208,124,.14)"

    def cell_color_ret(v):
        if v is None or pd.isna(v): return "transparent"
        return "rgba(38,208,124,.10)" if v > 0 else "rgba(255,90,106,.10)" if v < 0 else "transparent"

    fmt_pct_loc = lambda v: fmt_pct(v)
    fmt_pct_cdi = lambda v: f"{v*100:.0f}%" if pd.notna(v) else "—"

    rows = []
    for yr in years:
        sub = grid[grid["year"] == yr].set_index("month")
        for kind, label, formatter, color_fn in [
            ("fund",    "Fundo",    fmt_pct_loc, cell_color_ret),
            ("cdi",     "CDI",      fmt_pct_loc, cell_color_ret),
            ("pct_cdi", "% do CDI", fmt_pct_cdi, cell_color_pct),
        ]:
            cells = []
            for m in range(1, 13):
                if m in sub.index and pd.notna(sub.loc[m, kind]):
                    val = sub.loc[m, kind]
                    cells.append(f'<td class="mono" style="background:{color_fn(val)}">{formatter(val)}</td>')
                else:
                    cells.append('<td class="mono" style="color:var(--muted-soft)">—</td>')
            # Total column (month==13)
            if 13 in sub.index and pd.notna(sub.loc[13, kind]):
                tval = sub.loc[13, kind]
                total_cell = (
                    f'<td class="mono" style="background:{color_fn(tval)};'
                    f'border-left:2px solid var(--line-2);font-weight:700">{formatter(tval)}</td>'
                )
            else:
                total_cell = '<td class="mono" style="border-left:2px solid var(--line-2);color:var(--muted-soft)">—</td>'
            year_cell = f'<td rowspan="3" style="vertical-align:middle;font-weight:700">{yr}</td>' if kind == "fund" else ""
            rows.append(f'<tr>{year_cell}<td style="color:var(--muted)">{label}</td>{"".join(cells)}{total_cell}</tr>')

    return (
        '<div style="overflow-x:auto"><table data-no-sort="1">'
        + head + "".join(rows) + '</table></div>'
    )


def render_header(fund_label: str, dt: date, nav: float,
                  fund_rets: dict, bench_rets: dict, bench_label: str,
                  carry_bruto: float, carry_liquido: float,
                  pct_cdi_total: Optional[float]) -> str:
    def stat(label, val, color_class=""):
        return (f'<div class="stat"><div class="stat-v {color_class}">{val}</div>'
                f'<div class="stat-l">{label}</div></div>')

    def colored(v):
        if v is None or pd.isna(v): return ""
        return "up" if v > 0 else "down" if v < 0 else ""

    return (
        f'<header class="fund-hd">'
        f'  <div><div class="sub">CRÉDITO</div><div class="name">{fund_label}</div></div>'
        f'  {stat("NAV", "R$ " + fmt_brl(nav))}'
        f'  {stat("Rent. 12M", fmt_pct(fund_rets["m12"], 2, signed=True), colored(fund_rets["m12"]))}'
        f'  {stat("Carry Bruto DC", fmt_pct(carry_bruto, 2))}'
        f'  {stat("Carry Líquido", fmt_pct(carry_liquido, 2))}'
        f'  {stat("% do CDI Total", f"{pct_cdi_total*100:.0f}%" if pct_cdi_total is not None else "—")}'
        f'  {stat("MTD", fmt_pct(fund_rets["mes"], 2, signed=True), colored(fund_rets["mes"]))}'
        f'  {stat("YTD", fmt_pct(fund_rets["ano"], 2, signed=True), colored(fund_rets["ano"]))}'
        f'  {stat(f"{bench_label} MTD", fmt_pct(bench_rets["mes"], 2, signed=True))}'
        f'  {stat("Data", dt.strftime("%d/%m/%Y"))}'
        f'</header>'
    )


def render_performance_card(fund_label: str, fund_rets: dict, bench_rets: dict,
                            bench_label: str, nav_share: pd.Series,
                            bench_idx: pd.Series, monthly_grid: pd.DataFrame) -> str:
    rows = [
        ("% Mês",       "mes"),
        ("% Ano",       "ano"),
        ("% 12 meses",  "m12"),
        ("% 24 meses",  "m24"),
        ("% 36 meses",  "m36"),
        ("% Início",    "inicio"),
    ]
    head = '<tr><th>Performance</th>' + "".join(f'<th>{r[0]}</th>' for r in rows) + "</tr>"
    body_rows = []
    body_rows.append(
        f'<tr><td>{fund_label}</td>' +
        "".join(f'<td class="mono">{fmt_pct(fund_rets[r[1]])}</td>' for r in rows) +
        '</tr>'
    )
    body_rows.append(
        f'<tr><td>{bench_label}</td>' +
        "".join(f'<td class="mono">{fmt_pct(bench_rets[r[1]])}</td>' for r in rows) +
        '</tr>'
    )

    # Cumulative chart over last 24m for two series, rebased
    end_dt = nav_share.index[-1]
    start_dt = end_dt - pd.DateOffset(months=24)
    fund_slice = nav_share[nav_share.index >= start_dt]
    bench_slice = bench_idx[bench_idx.index >= start_dt]
    chart_html = _svg_line_chart(
        {fund_label: fund_slice, bench_label: bench_slice},
        width=760, height=240,
        palette={fund_label: "#0071BB", bench_label: "#a8b3c2"},
    )

    monthly_html = render_monthly_heatmap_html(monthly_grid)

    # CSV: monthly grid (long format)
    csv_rows = [["Ano", "Mes", "Fundo", "CDI", "% do CDI"]]
    for _, r in monthly_grid.sort_values(["year", "month"]).iterrows():
        m = "Total" if r["month"] == 13 else int(r["month"])
        f_ = f"{r['fund']*100:.4f}%" if pd.notna(r["fund"]) else ""
        c_ = f"{r['cdi']*100:.4f}%" if pd.notna(r["cdi"]) else ""
        p_ = f"{r['pct_cdi']*100:.2f}%" if pd.notna(r["pct_cdi"]) else ""
        csv_rows.append([int(r["year"]), m, f_, c_, p_])
    csv = _csv_encode(csv_rows)

    return (
        f'<div class="card" data-csv="{csv.replace(chr(34), "&quot;")}"><div class="card-hd">'
        '<div class="card-title">Retorno</div>'
        '<div class="card-hd-actions">'
        f'<div class="card-sub">vs {bench_label} · D-2 · 24m rebased + heatmap mensal (com Total)</div>'
        f'  {_csv_btn("sealion_retorno_mensal.csv")}'
        '</div></div><div class="card-body">'
        '<div class="grid-2">'
        f'  <div><table>{head}<tbody>{"".join(body_rows)}</tbody></table></div>'
        f'  <div class="chart-box">{chart_html}</div>'
        '</div>'
        '<div style="margin-top:14px"><div style="font-size:11.5px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px">Retorno Mensal &amp; Anual</div>'
        f'  {monthly_html}</div>'
        '</div></div>'
    )


def render_quality_card(flags: pd.DataFrame, ref_dt: date, prev_dt: date) -> str:
    """Quality flag for missing prices on D and D-1 (excluding 3rd-party funds,
    cash, provisions). Renders ✓ if clean, ⚠ + asset table if any flag fires."""
    fmt_date = lambda d: d.strftime("%d/%m/%Y")
    if flags.empty:
        return (
            '<div class="card" style="border-color:rgba(38,208,124,.40)">'
            '<div class="card-hd" style="border-bottom:none">'
            '<div class="card-title" style="color:var(--up)">✓ Sanity Check de Preços</div>'
            '<div class="card-hd-actions">'
            f'<div class="card-sub">Todos os ativos não-cotas precificados em {fmt_date(prev_dt)} e {fmt_date(ref_dt)}</div>'
            '</div></div></div>'
        )

    rows_html = ['<tr><th>Produto</th><th>Tipo</th><th>Posição (R$)</th>'
                 f'<th>Preço {fmt_date(prev_dt)}</th><th>Preço {fmt_date(ref_dt)}</th><th>Falha</th></tr>']
    for _, r in flags.iterrows():
        miss_t = bool(r["missing_today"])
        miss_p = bool(r["missing_prev"])
        flags_str = []
        if miss_t: flags_str.append(f'preço {fmt_date(ref_dt)}')
        if miss_p: flags_str.append(f'preço {fmt_date(prev_dt)}')
        flag_label = " · ".join(flags_str)
        pt_disp = fmt_brl(r["price_today"], 4) if pd.notna(r["price_today"]) else "—"
        pp_disp = fmt_brl(r["price_prev"], 4)  if pd.notna(r["price_prev"])  else "—"
        rows_html.append(
            "<tr>"
            f'<td>{r["produto"]}</td>'
            f'<td>{r["product_class"] or "—"}</td>'
            f'<td class="mono">{fmt_brl(r["pos_brl"])}</td>'
            f'<td class="mono" style="{"color:var(--down)" if miss_p else ""}">{pp_disp}</td>'
            f'<td class="mono" style="{"color:var(--down)" if miss_t else ""}">{pt_disp}</td>'
            f'<td style="color:var(--down)">{flag_label}</td>'
            "</tr>"
        )

    csv_rows = [["Produto", "Tipo", "Posicao_BRL",
                 f"Preco_{prev_dt.isoformat()}", f"Preco_{ref_dt.isoformat()}", "Falha"]]
    for _, r in flags.iterrows():
        miss_t = bool(r["missing_today"]); miss_p = bool(r["missing_prev"])
        why = []
        if miss_t: why.append(f"missing {ref_dt.isoformat()}")
        if miss_p: why.append(f"missing {prev_dt.isoformat()}")
        csv_rows.append([
            r["produto"], r["product_class"] or "",
            f"{r['pos_brl']:.2f}",
            f"{r['price_prev']:.4f}" if pd.notna(r["price_prev"]) else "",
            f"{r['price_today']:.4f}" if pd.notna(r["price_today"]) else "",
            "; ".join(why),
        ])
    csv = _csv_encode(csv_rows)

    return (
        f'<div class="card" data-csv="{csv.replace(chr(34), "&quot;")}" '
        f'style="border-color:rgba(255,90,106,.40)">'
        '<div class="card-hd">'
        f'<div class="card-title" style="color:var(--down)">⚠ Sanity Check de Preços — {len(flags)} ativo(s) sem preço</div>'
        '<div class="card-hd-actions">'
        f'<div class="card-sub">Comparação D ({fmt_date(ref_dt)}) vs D-1 ({fmt_date(prev_dt)}) · cotas de fundos / caixa / provisões isentas</div>'
        f'  {_csv_btn("sealion_quality_flags.csv")}'
        '</div></div><div class="card-body">'
        f'<table>{"".join(rows_html)}</table>'
        '<div style="font-size:11px;color:var(--muted-soft);margin-top:8px">'
        'Critério: PRICE não-nulo em LOTE_BOOK_OVERVIEW para D e D-1. '
        'Isentos: PRODUCT_CLASS ∈ {Funds BR, Cash, Provisions and Costs}.'
        '</div>'
        '</div></div>'
    )


def render_aum_card(nav_h: pd.DataFrame) -> str:
    chart = _svg_aum_chart(nav_h, width=1180, height=240)
    csv_rows = [["Data", "NAV", "SHARE"]]
    for dt, r in nav_h.iterrows():
        csv_rows.append([dt.strftime("%Y-%m-%d"), f"{r['NAV']:.2f}", f"{r['SHARE']:.6f}"])
    csv = _csv_encode(csv_rows)
    return (
        f'<div class="card" data-csv="{csv.replace(chr(34), "&quot;")}"><div class="card-hd">'
        '<div class="card-title">AUM Histórica</div>'
        '<div class="card-hd-actions">'
        '<div class="card-sub">NAV diário · marcado peak / trough / atual</div>'
        f'  {_csv_btn("sealion_aum_historico.csv")}'
        '</div></div><div class="card-body">'
        f'<div class="chart-box">{chart}</div>'
        '</div></div>'
    )


def render_distribuicao_card(positions: pd.DataFrame, nav: float,
                              denom_nav: bool = False,
                              exclude_sovereign: bool = False) -> str:
    """Side-by-side donuts/bars: PL por Tipo Ativo · Setores · Rating · Subordinação.

    ``denom_nav`` switches Tipo/Setor/Rating slice labels from "% of card total"
    to "% of NAV" — useful for inline credit sections of mixed-mandate funds.
    ``exclude_sovereign`` drops Soberano / Títulos Públicos rows from the
    Tipo/Setor/Rating donuts (and from the Subordinação donut), since for
    a Crédito look-through view sovereign isn't meaningful credit risk.
    The Subordinação donut always uses % of credit on the slice label and
    shows % of NAV in the tooltip when ``denom_nav=True``."""
    if positions.empty:
        return ""

    base = positions.copy()
    base["rating_eff"] = base.apply(_effective_rating, axis=1)
    if exclude_sovereign:
        sov_mask = (base["rating_eff"] == "Soberano") | \
                   (base["tipo_ativo"].astype(str).str.strip() == "Títulos Públicos") | \
                   (base["grupo_economico"].astype(str).str.strip().str.lower() == "tesouro nacional")
        base_credit = base[~sov_mask].copy()
    else:
        base_credit = base

    if base_credit.empty:
        return ""

    # Denominator: NAV (when denom_nav) or sum of (filtered) positions
    total_credit = float(base_credit["pos_brl"].sum())
    denom = nav if (denom_nav and nav) else total_credit
    pct_label = "% NAV" if denom_nav else "%"

    # Tooltip when in NAV mode shows both the slice value and what % of credit
    # it represents — and the % NAV label.
    def _tt_nav(lbl, v, frac):
        of_nav = (v / nav * 100) if nav else 0.0
        of_credit = (v / total_credit * 100) if total_credit else 0.0
        return f'{lbl} · {of_nav:.2f}% NAV · {of_credit:.1f}% crédito'

    def _slice_pct(v, denom_val):
        return (v / denom_val) if denom_val else 0.0

    # By Tipo Ativo
    tipo = base_credit.groupby("tipo_ativo", dropna=False)["pos_brl"].sum().reset_index()
    tipo = tipo[tipo["pos_brl"] > 0].sort_values("pos_brl", ascending=False)
    tipo_items = [(r["tipo_ativo"] or "Outros", r["pos_brl"]) for _, r in tipo.iterrows()]
    tipo_donut = _svg_donut(
        tipo_items, width=320, height=240,
        label_fmt=lambda v: f"{_slice_pct(v, denom)*100:.1f}%",
        tooltip_fmt=(_tt_nav if denom_nav else None),
    )

    # By Setor
    setor = base_credit.groupby("setor", dropna=False)["pos_brl"].sum().reset_index()
    setor = setor[setor["pos_brl"] > 0].sort_values("pos_brl", ascending=False)
    setor_items = [(r["setor"] or "—", _slice_pct(r["pos_brl"], denom)) for _, r in setor.iterrows()]
    setor_bars = _svg_vbar(setor_items, width=540, height=300, max_bars=14)

    # By Rating — apply Tesouro Nacional → 'Soberano' override before grouping
    rating = base_credit.groupby("rating_eff", dropna=False)["pos_brl"].sum().reset_index()
    rating = rating[rating["pos_brl"] > 0].sort_values("pos_brl", ascending=False)
    # Pin "Soberano" to the first slot so it always picks the navy palette colour
    rating_items_raw = [(r["rating_eff"] or "Sem Rating", r["pos_brl"]) for _, r in rating.iterrows()]
    soberano = [it for it in rating_items_raw if it[0] == "Soberano"]
    others = [it for it in rating_items_raw if it[0] != "Soberano"]
    rating_items = soberano + others
    rating_palette = ["#0d2c5a", "#26d07c", "#1a8fd1", "#7cc8e8", "#f5c451", "#ff5a6a",
                     "#a890ff", "#5a8eb0", "#7a8a99", "#c9d1dd"]
    rating_donut = _svg_donut(
        rating_items, width=320, height=240, palette=rating_palette,
        label_fmt=lambda v: f"{_slice_pct(v, denom)*100:.1f}%",
        tooltip_fmt=(_tt_nav if denom_nav else None),
    )

    # By Subordinação (only render the donut when there's signal — i.e.,
    # at least one Senior/Mezanino/Junior bucket present, not 100% N/A).
    # Always uses % of credit on the slice label; tooltip shows % NAV when
    # denom_nav=True so the user can read both off the same chart.
    base_credit["tranche"] = base_credit.apply(_tranche_bucket, axis=1)
    tranche = base_credit.groupby("tranche", dropna=False)["pos_brl"].sum().reset_index()
    tranche = tranche[tranche["pos_brl"] > 0]
    has_tranche_signal = (tranche["tranche"] != "N/A").any()
    tranche_donut_html = ""
    if has_tranche_signal:
        # Sort Senior → Mezanino → Junior → N/A so palette colours align
        tranche["sk"] = tranche["tranche"].map(lambda k: _TRANCHE_ORDER.get(k, 99))
        tranche = tranche.sort_values("sk")
        tranche_total = float(tranche["pos_brl"].sum())
        tranche_items = [(r["tranche"], r["pos_brl"]) for _, r in tranche.iterrows()]
        # Senior=green, Mezanino=amber, Junior=red, Sem Subordinação=muted
        tranche_palette = ["#26d07c", "#f5c451", "#ff5a6a", "#7a8a99"]

        def _tt_tranche(lbl, v, frac):
            of_credit = (v / tranche_total * 100) if tranche_total else 0.0
            of_nav = (v / nav * 100) if nav else 0.0
            return f'{lbl} · {of_credit:.1f}% crédito · {of_nav:.2f}% NAV'

        tranche_donut = _svg_donut(
            tranche_items, width=320, height=240, palette=tranche_palette,
            label_fmt=lambda v: f"{(v / tranche_total * 100):.1f}%",
            tooltip_fmt=(_tt_tranche if denom_nav else None),
        )
        tranche_donut_html = (
            f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 8px;font-weight:600">'
            f'PL por Subordinação <span style="color:var(--muted-soft);font-weight:500">(% crédito)</span>'
            f'</div>{tranche_donut}</div>'
        )

    # CSV: combined long format. Always include both % NAV and % crédito so
    # the export carries the same information as both label + tooltip.
    csv_rows = [["Categoria", "Item", "Posicao_BRL", "Pct_NAV", "Pct_Credit"]]
    def _csv_row(cat, item, pos):
        pct_nav = (pos / nav * 100) if nav else 0.0
        pct_cr = (pos / total_credit * 100) if total_credit else 0.0
        return [cat, item, f"{pos:.2f}", f"{pct_nav:.4f}%", f"{pct_cr:.4f}%"]
    for _, r in tipo.iterrows():
        csv_rows.append(_csv_row("Tipo de Ativo", r["tipo_ativo"] or "—", r["pos_brl"]))
    for _, r in setor.iterrows():
        csv_rows.append(_csv_row("Setor", r["setor"] or "—", r["pos_brl"]))
    for _, r in rating.iterrows():
        csv_rows.append(_csv_row("Rating", r["rating_eff"] or "Sem Rating", r["pos_brl"]))
    if has_tranche_signal:
        for _, r in tranche.iterrows():
            csv_rows.append(_csv_row("Subordinação", r["tranche"], r["pos_brl"]))
    csv = _csv_encode(csv_rows)

    # Layout: 2×2 grid when tranche donut is present (one row per pair) so each
    # chart has more breathing room; fallback to 3 columns when no Subordinação.
    sub_label = " · PL por Subordinação" if has_tranche_signal else ""
    grid_cols = "1fr 1fr" if has_tranche_signal else "1fr 1.6fr 1fr"

    # Sub-line wording — NAV-based vs card-total semantics
    nav_qualifier = " (% NAV, ex-Soberano)" if (denom_nav and exclude_sovereign) else \
                    " (% NAV)" if denom_nav else ""

    return (
        f'<div class="card" data-csv="{csv.replace(chr(34), "&quot;")}"><div class="card-hd">'
        '<div class="card-title">Distribuição</div>'
        '<div class="card-hd-actions">'
        f'<div class="card-sub">Tipo · Setores · Rating{sub_label}{nav_qualifier}</div>'
        f'  {_csv_btn("sealion_distribuicao.csv")}'
        '</div></div><div class="card-body">'
        f'<div style="display:grid;grid-template-columns:{grid_cols};gap:14px;align-items:start">'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 8px;font-weight:600">PL por Tipo de Ativo</div>{tipo_donut}</div>'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 8px;font-weight:600">Setores por AUM</div>{setor_bars}</div>'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 8px;font-weight:600">PL por Rating</div>{rating_donut}</div>'
        f'{tranche_donut_html}'
        '</div></div></div>'
    )


def _rating_class(r: str) -> str:
    if not r: return "na"
    r = str(r).strip()
    if r == "Soberano":         return "soberano"
    if r == "AAA":              return "aaa"
    if r.startswith("AA"):      return "aa"
    if r.startswith("A"):       return "a"
    if r.startswith("BBB"):     return "bbb"
    if r.startswith("BB"):      return "bb"
    if r.startswith("B"):       return "b"
    if r.startswith("CC") or r.startswith("C") or r.startswith("D"):
        return "ccc"
    return "na"


# Subordinação tranche detection. Tranche labels live in the product name
# (e.g. "FIDC Estanho – Senior 1", "FIDC ABC MEZ2", "Yaaleh FIDC - Sênior 4",
# "SB CREDITO FIDC MULTISSETORIAL CLASSE UNICA SN").
# Order matters: Monocota / Classe Única first (since "Classe Única SN" would
# otherwise match Senior), then Mezanino/Junior before Senior to avoid the
# "Sr" 2-letter fragment misclassifying names like "FIDC ABC Sr Mez 1".
# Monocota and any non-tranched instrument (Debentures, CRIs, etc.) all roll
# up into the same "Sem Subordinação" bucket — they have no seniority ladder
# (the credit team treats them as flat exposure).
_TRANCHE_PATTERNS: list[tuple[str, "re.Pattern"]] = [
    ("Sem Subordinação", re.compile(r"\b(monocota|classe\s+[úu]nica|cota\s+[úu]nica)\b", re.IGNORECASE)),
    ("Mezanino", re.compile(r"\b(mezanino|mez\d*|mz\d+)\b", re.IGNORECASE)),
    ("Junior",   re.compile(r"\b(junior|j[uú]nior|subordinad[ao])\b", re.IGNORECASE)),
    ("Senior",   re.compile(r"\b(senior|s[eê]nior|snr|sr)\b", re.IGNORECASE)),
]
_TRANCHE_ORDER = {"Senior": 0, "Mezanino": 1, "Junior": 2, "Sem Subordinação": 3}


def _tranche_bucket(row) -> str:
    """Classify a holding as Senior / Mezanino / Junior / Sem Subordinação.

    1) Search the product name for an explicit tranche label.
    2) Fallback for FIDCs without a label: numeric `subordinacao` value implies
       it is the Senior tranche (the subordinacao field on asset_master is the
       cushion *below* this tranche, only meaningful for the senior).
    3) Anything else (Monocota / Classe Única, Debenture, CRI, CRA, NTN-*, etc.)
       falls into "Sem Subordinação" — flat credit exposure with no seniority.
    """
    name = str(row.get("produto") or "")
    for label, pat in _TRANCHE_PATTERNS:
        if pat.search(name):
            return label
    tipo = row.get("tipo_ativo")
    if tipo in ("FIDC", "FIDC NP") and pd.notna(row.get("subordinacao")):
        return "Senior"
    return "Sem Subordinação"


def _effective_rating(row) -> str:
    """Sovereign override: Tesouro Nacional issuer or NTN-*/LFT/LTN tipo → 'Soberano'.
    Treasury debt isn't rated by agencies against itself — it's its own bucket,
    sitting above AAA in the quality scale."""
    grp = row.get("grupo_economico")
    if isinstance(grp, str) and grp.strip().lower() == "tesouro nacional":
        return "Soberano"
    tipo = row.get("tipo_ativo")
    if tipo in ("NTN-B", "LFT", "NTN-F", "LTN", "NTN-C"):
        return "Soberano"
    return row.get("rating") or ""


def render_alocacao_card(positions: pd.DataFrame, nav: float, ref_dt: date,
                          default_mode: str = "tipo",
                          default_collapsed: bool = False,
                          mm_mode: bool = False) -> str:
    """Drill-down by Tipo / Subordinação / Rating / Grupo Econômico.

    Args:
      default_mode: which grouping pane is visible on load. One of
        'tipo' (default for credit reports), 'subordinacao' (Dragon),
        'rating', or 'grupo' (BALTRA/EVO Crédito section in main report).
      default_collapsed: when True, all parent rows render with caret ▶
        (children hidden). Click to expand. Useful for big inline cards.
      mm_mode: when True, all monetary cells (Posição, group totals,
        Total) render in R$ MM with 2 decimals via fmt_brl_mm.
    """
    if positions.empty:
        return ""
    p = positions.copy()
    p["tipo_norm"] = p["tipo_ativo"].fillna("Outros").map(lambda t: TIPO_GROUPS.get(t, t or "Outros"))
    p["dur_yrs"] = p.apply(lambda r: _approx_duration_yrs(r, ref_dt), axis=1)
    p["pct_pl"] = p["pos_brl"] / nav
    p["rating_eff"] = p.apply(_effective_rating, axis=1)
    p["tranche"] = p.apply(_tranche_bucket, axis=1)
    p["grupo_eff"] = p["grupo_economico"].astype(object).where(
        p["grupo_economico"].astype(object).notna() & (p["grupo_economico"].astype(str).str.strip() != ""),
        "Sem Grupo")

    group_order = ["Crédito Infra", "Crédito", "Estruturados", "Títulos Públicos", "Caixa", "Outros", "Custos & Provisões"]
    p["group_order"] = p["tipo_norm"].map(lambda g: group_order.index(g) if g in group_order else 99)
    p = p.sort_values(["group_order", "pos_brl"], ascending=[True, False])

    money = fmt_brl_mm if mm_mode else fmt_brl
    headers = ('<tr><th>Produto</th><th>Tipo</th><th>Subord.</th><th>Indexador</th>'
               f'<th>Posição ({"R$ MM" if mm_mode else "R$"})</th><th>%PL</th><th>Spread</th>'
               '<th>Carry Anual</th><th>Duration</th>'
               '<th>Rating</th><th>Setor</th><th>Grupo Econômico</th></tr>')

    def _child_row(r) -> str:
        rating_val = r.get("rating_eff") or ""
        tag = _rating_class(rating_val)
        tag_html = f'<span class="tag tag-{tag}">{rating_val or "—"}</span>'
        rating_rank = RATING_ORDER.get(str(rating_val).strip(), 99)
        tr_val = r.get("tranche") or "Sem Subordinação"
        tr_rank = _TRANCHE_ORDER.get(tr_val, 9)
        if tr_val in ("Senior", "Mezanino", "Junior"):
            tr_html = f'<span class="tag tag-tr-{tr_val.lower()}">{tr_val}</span>'
        else:
            # Sem Subordinação / non-tranched — keep the cell compact with "—"
            tr_html = '<span class="tag tag-na">—</span>'
        return (
            f'<td>{r["produto"]}</td>'
            f'<td>{r.get("tipo_ativo") or "—"}</td>'
            f'<td data-sort="{tr_rank}">{tr_html}</td>'
            f'<td>{r.get("indexador") or "—"}</td>'
            f'<td class="mono">{money(r["pos_brl"])}</td>'
            f'<td class="mono">{fmt_pct(r["pct_pl"])}</td>'
            f'<td class="mono">{fmt_pct(r["spread"]) if pd.notna(r["spread"]) else "—"}</td>'
            f'<td class="mono">{fmt_pct(r.get("carry_anual")) if pd.notna(r.get("carry_anual")) else "—"}</td>'
            f'<td class="mono">{fmt_yr(r["dur_yrs"])}</td>'
            f'<td data-sort="{rating_rank}">{tag_html}</td>'
            f'<td>{r.get("setor") or "—"}</td>'
            f'<td>{r.get("grupo_eff") or "—"}</td>'
        )

    def _build_pane(group_col: str, key_order: list, mode: str, default_visible: bool) -> tuple[str, float, float, float, float, float, float, float, float]:
        """Render one full <table> grouped by `group_col` (e.g. 'tipo_norm' or 'rating_eff').
        `key_order` defines render order of group keys (missing groups skipped).
        Returns (table_html, grand_pos, grand_pct, spread_acc, spread_w, dur_acc, dur_w, carry_acc, carry_w)."""
        rows = [headers]
        gp = 0.0; gpct = 0.0
        sa = 0.0; sw = 0.0; da = 0.0; dw = 0.0; ca = 0.0; cw = 0.0
        # Anchor the iteration on key_order, then append any unknown groups at the end
        seen = set()
        ordered_keys = [k for k in key_order if k in p[group_col].values]
        ordered_keys += [k for k in p[group_col].dropna().unique() if k not in key_order and k not in seen]
        for key in ordered_keys:
            sub = p[p[group_col] == key].copy()
            if sub.empty:
                continue
            sub = sub.sort_values("pos_brl", ascending=False)
            sub_pos = sub["pos_brl"].sum()
            sub_pct = sub["pct_pl"].sum()
            sub_spread = weighted(sub["spread"], sub["pos_brl"])
            sub_dur = weighted(sub["dur_yrs"], sub["pos_brl"])
            sub_carry = weighted(sub["carry_anual"], sub["pos_brl"]) if "carry_anual" in sub.columns else None

            key_id = f"{mode}-{str(key).replace(' ', '_').replace('&', 'and').replace('+', 'p').replace('—','na')}"
            n_children = len(sub)
            # Parent row — caret state + child visibility depend on default_collapsed
            caret_glyph = "▶" if default_collapsed else "▼"
            parent_extra_cls = " alc-collapsed" if default_collapsed else ""
            parent_label = f'<span class="alc-caret">{caret_glyph}</span> {key} <span class="alc-count">({n_children})</span>'
            rows.append(
                f'<tr class="group alc-parent{parent_extra_cls}" data-alc-key="{key_id}" data-alc-mode="{mode}" '
                f'onclick="toggleAlcRow(this)" style="cursor:pointer">'
                f'<td>{parent_label}</td><td></td><td></td><td></td>'
                f'<td class="mono">{money(sub_pos)}</td>'
                f'<td class="mono">{fmt_pct(sub_pct)}</td>'
                f'<td class="mono">{fmt_pct(sub_spread) if sub_spread is not None else "—"}</td>'
                f'<td class="mono">{fmt_pct(sub_carry) if sub_carry is not None else "—"}</td>'
                f'<td class="mono">{fmt_yr(sub_dur)}</td>'
                f'<td colspan="3"></td></tr>'
            )
            extra_child_cls = " alc-hidden" if default_collapsed else ""
            for _, r in sub.iterrows():
                rows.append(
                    f'<tr class="alc-child{extra_child_cls}" data-parent="{key_id}" data-alc-mode="{mode}">'
                    + _child_row(r) + '</tr>'
                )
            gp += sub_pos; gpct += sub_pct
            if sub_spread is not None: sa += sub_spread * sub_pos; sw += sub_pos
            if sub_dur is not None: da += sub_dur * sub_pos; dw += sub_pos
            if sub_carry is not None: ca += sub_carry * sub_pos; cw += sub_pos

        overall_spread = sa / sw if sw > 0 else None
        overall_dur = da / dw if dw > 0 else None
        overall_carry = ca / cw if cw > 0 else None
        rows.append(
            '<tr class="total">'
            f'<td>Total</td><td></td><td></td><td></td>'
            f'<td class="mono">{money(gp)}</td>'
            f'<td class="mono">{fmt_pct(gpct)}</td>'
            f'<td class="mono">{fmt_pct(overall_spread) if overall_spread is not None else "—"}</td>'
            f'<td class="mono">{fmt_pct(overall_carry) if overall_carry is not None else "—"}</td>'
            f'<td class="mono">{fmt_yr(overall_dur)}</td>'
            f'<td colspan="3"></td></tr>'
        )
        style = "" if default_visible else "display:none"
        pane = (
            f'<div class="alc-pane" data-alc-mode="{mode}" style="{style}">'
            f'<table>{"".join(rows)}</table></div>'
        )
        return pane, gp, gpct, sa, sw, da, dw, ca, cw

    # Pane 1 — drill-down by Tipo
    tipo_pane, grand_pos, grand_pct, *_ = _build_pane(
        "tipo_norm", group_order, mode="tipo", default_visible=(default_mode == "tipo")
    )

    # "Total Crédito" — sum of credit-risk buckets only (excludes Títulos Públicos,
    # Caixa, Custos & Provisões). This is what matters for the credit team.
    credit_buckets = ("Crédito Infra", "Crédito", "Estruturados")
    cred = p[p["tipo_norm"].isin(credit_buckets)]
    cred_pos = cred["pos_brl"].sum()
    cred_pct = cred["pct_pl"].sum()

    # Pane 2 — drill-down by Rating
    rating_keys_present = [k for k in p["rating_eff"].dropna().unique() if k]
    rating_order_keys = sorted(
        [k for k in rating_keys_present if str(k).strip() in RATING_ORDER],
        key=lambda k: RATING_ORDER[str(k).strip()]
    )
    other = [k for k in rating_keys_present if k not in rating_order_keys]
    rating_order_keys += other
    # Also include rows with empty rating_eff under "Sem Rating"
    p["rating_eff_lbl"] = p["rating_eff"].where(p["rating_eff"].astype(bool), "Sem Rating")
    rating_keys_present_lbl = [k for k in p["rating_eff_lbl"].unique() if k]
    rating_order_keys_lbl = sorted(
        [k for k in rating_keys_present_lbl if str(k).strip() in RATING_ORDER],
        key=lambda k: RATING_ORDER[str(k).strip()]
    )
    rating_order_keys_lbl += [k for k in rating_keys_present_lbl if k not in rating_order_keys_lbl]
    rating_pane, *_ = _build_pane(
        "rating_eff_lbl", rating_order_keys_lbl, mode="rating",
        default_visible=(default_mode == "rating")
    )

    # Pane 3 — drill-down by Subordinação (Senior / Mezanino / Junior / N/A)
    sub_order_keys = sorted(
        [k for k in p["tranche"].unique() if k],
        key=lambda k: _TRANCHE_ORDER.get(k, 99)
    )
    sub_pane, *_ = _build_pane(
        "tranche", sub_order_keys, mode="subordinacao",
        default_visible=(default_mode == "subordinacao")
    )

    # Pane 4 — drill-down by Grupo Econômico (alphabetical, "Sem Grupo" pinned last)
    all_grupos = [str(k) for k in p["grupo_eff"].unique() if k is not None and str(k).strip()]
    grupo_keys = sorted([g for g in all_grupos if g != "Sem Grupo"], key=lambda s: s.lower())
    if "Sem Grupo" in all_grupos:
        grupo_keys.append("Sem Grupo")
    grupo_pane, *_ = _build_pane(
        "grupo_eff", grupo_keys, mode="grupo",
        default_visible=(default_mode == "grupo")
    )

    # Toggle pills — four modes; the active flag follows default_mode
    def _btn(mode_key: str, label: str) -> str:
        active = " active" if mode_key == default_mode else ""
        return (f'<button type="button" class="alc-tab{active}" data-alc-mode="{mode_key}" '
                f"onclick=\"setAlcGroup(this, '{mode_key}')\">{label}</button>")
    toggle = (
        '<div class="alc-toggle">'
        + _btn("tipo", "Por Tipo")
        + _btn("grupo", "Por Grupo")
        + _btn("subordinacao", "Por Subordinação")
        + _btn("rating", "Por Rating")
        + '</div>'
    )

    cred_chip = (
        '<div class="alc-cred-chip" title="Soma de Crédito Infra + Crédito + Estruturados">'
        f'<span class="alc-cred-lbl">Total Crédito</span>'
        f'<span class="alc-cred-val">{money(cred_pos)}</span>'
        f'<span class="alc-cred-pct">{fmt_pct(cred_pct)} do PL</span>'
        '</div>'
    )

    expand_collapse = (
        '<div class="alc-expand-toggle">'
        '<button type="button" class="alc-xc-btn" onclick="alcExpandAll(this)" '
        'title="Expandir todos os grupos">▼ Expand all</button>'
        '<button type="button" class="alc-xc-btn" onclick="alcCollapseAll(this)" '
        'title="Colapsar todos os grupos">▶ Collapse all</button>'
        '</div>'
    )

    return (
        '<div class="card"><div class="card-hd">'
        '<div class="card-title">Alocação</div>'
        '<div class="card-hd-actions">'
        f'{cred_chip}'
        f'{toggle}'
        f'{expand_collapse}'
        f'<div class="card-sub">{len(p)} posições · Σ {money(grand_pos)} · {fmt_pct(grand_pct)} do PL</div>'
        f'  {_csv_btn("sealion_alocacao.csv")}'
        '</div></div><div class="card-body" style="overflow-x:auto">'
        f'{tipo_pane}{grupo_pane}{sub_pane}{rating_pane}'
        '</div></div>'
    )


def render_concentracao_card(positions: pd.DataFrame, limits: pd.DataFrame, nav: float) -> str:
    if positions.empty:
        return ""
    p = positions.copy()
    p["pct_pl"] = p["pos_brl"] / nav

    # Counts
    n_emissores = p["apelido_emissor"].dropna().nunique() or p["nome_emissor"].dropna().nunique()
    n_grupos = p["grupo_economico"].dropna().nunique()

    # Aggregate by Grupo Econômico (with limit join)
    grp = (p.groupby("grupo_economico", dropna=False)
            .agg(pos=("pos_brl", "sum"), n=("produto", "count"))
            .reset_index())
    grp["pct"] = grp["pos"] / nav
    grp = grp[grp["pos"] > 0].sort_values("pos", ascending=False)

    lim_map = {row["emissor"]: (row["limit_pct"], row["limit_text"]) for _, row in limits.iterrows()}

    def find_limit(grupo):
        if grupo is None or pd.isna(grupo) or not isinstance(grupo, str): return (None, None)
        if grupo in lim_map: return lim_map[grupo]
        gl = grupo.lower()
        for k, v in lim_map.items():
            if isinstance(k, str) and k.lower() == gl:
                return v
        return (None, None)

    grp["limit_pct"], grp["limit_text"] = zip(*grp["grupo_economico"].map(find_limit))

    def util_color(util):
        if util is None: return "var(--muted)"
        if util > 1.0: return "var(--down)"
        if util > 0.8: return "var(--warn)"
        return "var(--up)"

    # Limit-breach table (kept as before — actionable)
    rows_html = ['<tr><th>Grupo Econômico</th><th>Posições</th><th>%PL</th><th>Limite</th><th>Utilização</th><th></th></tr>']
    for _, r in grp.iterrows():
        if pd.isna(r["grupo_economico"]) or r["pct"] < 0.001:
            continue
        lim = r["limit_pct"]
        util = (r["pct"] / lim) if (lim and lim > 0) else None
        util_str = f'{util*100:.0f}%' if util is not None else (r["limit_text"] or "—")
        bar_pct = min(util, 1.5) if util is not None else 0
        bar_html = ""
        if util is not None:
            bar_html = (
                f'<div class="bar-bg"><div class="bar-fill" '
                f'style="width:{min(bar_pct*100,100):.0f}%; background:{util_color(util)}"></div></div>'
            )
        rows_html.append(
            "<tr>"
            f'<td>{r["grupo_economico"] or "—"}</td>'
            f'<td class="mono">{int(r["n"])}</td>'
            f'<td class="mono">{fmt_pct(r["pct"])}</td>'
            f'<td class="mono">{fmt_pct(lim) if lim else (r["limit_text"] or "—")}</td>'
            f'<td class="mono" style="color:{util_color(util)}">{util_str}</td>'
            f'<td style="width:140px">{bar_html}</td>'
            "</tr>"
        )

    # Top emissores horizontal bars
    emissor_grp = (p.groupby("apelido_emissor", dropna=False)["pos_brl"].sum() / nav).reset_index()
    emissor_grp = emissor_grp[emissor_grp["pos_brl"] > 0.001].sort_values("pos_brl", ascending=False)
    emissor_items = [(r["apelido_emissor"] or "—", r["pos_brl"]) for _, r in emissor_grp.iterrows()]
    emissor_bars = _svg_hbar(emissor_items, width=540, max_rows=14)

    # Top grupos horizontal bars
    grupo_items = [(r["grupo_economico"] or "—", r["pct"]) for _, r in grp.iterrows() if r["pct"] > 0.001]
    grupo_bars = _svg_hbar(grupo_items, width=540, max_rows=14, color="#1a8fd1")

    # Tile blocks
    tile = lambda label, val: (
        f'<div style="background:var(--panel-2);border:1px solid var(--line);'
        f'border-radius:10px;padding:18px;text-align:center">'
        f'<div style="font-size:30px;font-weight:700;color:var(--text)">{val}</div>'
        f'<div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-top:4px">{label}</div>'
        f'</div>'
    )

    csv_rows = [["Tipo", "Item", "Posicoes", "Pct_PL", "Limite", "Utilizacao"]]
    for lbl, frac in emissor_items:
        csv_rows.append(["Emissor", lbl, "", f"{frac*100:.4f}%", "", ""])
    for _, r in grp.iterrows():
        if pd.isna(r["grupo_economico"]) or r["pct"] < 0.001: continue
        lim = r["limit_pct"]
        util = (r["pct"] / lim) if (lim and lim > 0) else None
        csv_rows.append([
            "Grupo Economico", r["grupo_economico"] or "—",
            int(r["n"]), f"{r['pct']*100:.4f}%",
            f"{lim*100:.2f}%" if lim else (r["limit_text"] or "—"),
            f"{util*100:.0f}%" if util is not None else "",
        ])
    csv = _csv_encode(csv_rows)

    return (
        f'<div class="card" data-csv="{csv.replace(chr(34), "&quot;")}"><div class="card-hd">'
        '<div class="card-title">Concentração</div>'
        '<div class="card-hd-actions">'
        '<div class="card-sub">Limites · Top Emissores · Top Grupos Econômicos</div>'
        f'  {_csv_btn("sealion_concentracao.csv")}'
        '</div></div><div class="card-body">'
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">'
        f'  {tile("# Emissores", n_emissores)}'
        f'  {tile("# Grupos Econômicos", n_grupos)}'
        '</div>'
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 6px">Top Emissores · %PL</div>{emissor_bars}</div>'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 6px">Top Grupos Econômicos · %PL</div>{grupo_bars}</div>'
        '</div>'
        '<div style="font-size:11px;color:var(--muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px">Limites por Grupo Econômico</div>'
        f'<table>{"".join(rows_html)}</table>'
        '</div></div>'
    )


def render_indices_card(idx_panel: pd.DataFrame, curves: dict[str, pd.DataFrame],
                        ref_dt: date) -> str:
    # MTD slice for indices
    mtd_start = pd.Timestamp(ref_dt.replace(day=1)) - pd.Timedelta(days=1)
    ytd_start = pd.Timestamp(ref_dt.replace(month=1, day=1)) - pd.Timedelta(days=1)

    def slice_(start_ts):
        sub = idx_panel[idx_panel.index >= start_ts]
        return {col: sub[col] for col in sub.columns}

    mtd_chart = _svg_line_chart(
        slice_(mtd_start), width=520, height=240,
        palette={"CDI": "#183C80", "IMA-B": "#1a8fd1",
                 "IDA-Infra": "#26d07c", "IDA-Ex-Infra": "#f5c451"},
    )
    ytd_chart = _svg_line_chart(
        slice_(ytd_start), width=520, height=240,
        palette={"CDI": "#183C80", "IMA-B": "#1a8fd1",
                 "IDA-Infra": "#26d07c", "IDA-Ex-Infra": "#f5c451"},
    )
    curves_chart = _svg_curves(curves, width=520, height=240)

    # CSV: today's curves + indices last value
    csv_rows = [["Tipo", "Item", "Tenor_BDays", "Valor"]]
    for r, df in curves.items():
        for _, row in df.iterrows():
            csv_rows.append(["Curva ANBIMA", r, int(row["tenor_bdays"]),
                             f"{row['rate']:.4f}" if pd.notna(row["rate"]) else ""])
    if not idx_panel.empty:
        last_dt = idx_panel.index[-1].strftime("%Y-%m-%d")
        for col in idx_panel.columns:
            csv_rows.append(["Indice (cumulativo até)", col, last_dt, f"{idx_panel[col].iloc[-1]:.6f}"])
    csv = _csv_encode(csv_rows)

    return (
        f'<div class="card" data-csv="{csv.replace(chr(34), "&quot;")}"><div class="card-hd">'
        '<div class="card-title">Mercado de Crédito</div>'
        '<div class="card-hd-actions">'
        '<div class="card-sub">Índices BR · Curvas ANBIMA por Rating</div>'
        f'  {_csv_btn("sealion_mercado_credito.csv")}'
        '</div></div><div class="card-body">'
        '<div class="grid-2">'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 6px">Índices RF/Crédito · MTD</div>{mtd_chart}</div>'
        f'  <div class="chart-box"><div style="font-size:11px;color:var(--muted);margin:2px 4px 6px">Índices RF/Crédito · YTD</div>{ytd_chart}</div>'
        '</div>'
        f'<div class="chart-box" style="margin-top:14px"><div style="font-size:11px;color:var(--muted);margin:2px 4px 6px">Curvas de Crédito ANBIMA · {ref_dt.strftime("%d/%m/%Y")}</div>{curves_chart}</div>'
        '</div></div>'
    )


# ────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ────────────────────────────────────────────────────────────────────────────

def _build_fund_body(fund_key: str, ref_dt: date) -> tuple[str, dict]:
    """Render one fund's set of cards as inner HTML (no <html>/<body> wrapper).
       Returns (inner_html, meta) where meta has nav, mtd, ytd, m12, label
       used for the tab strip in the consolidated report."""
    cfg = CREDIT_FUNDS[fund_key]
    trading_desk = cfg["trading_desk"]
    bench_label = cfg["benchmark"]
    fund_label = cfg["label"]

    print(f"[{fund_label}] {ref_dt}: fetching data…")

    # NAV history (since inception or 3y back)
    nav_h = fetch_nav_history(trading_desk, "2022-01-01")
    nav_share_series = nav_h["SHARE"].astype(float)
    nav_at = float(nav_h.loc[pd.Timestamp(ref_dt), "NAV"]) if pd.Timestamp(ref_dt) in nav_h.index else None
    if nav_at is None:
        nav_at = fetch_nav_at(trading_desk, ref_dt.isoformat()) or 0.0

    # Positions
    pos = fetch_positions_snapshot(trading_desk, ref_dt.isoformat(),
                                    include_cash_provisions=False)

    # Indices
    idx_start = (ref_dt - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    idx_panel = fetch_index_panel(["CDI", "IMA-B", "IDA-Infra", "IDA-Ex-Infra"], idx_start)

    # Curves (D-2 + previous business day)
    prev_bd = _prev_business_day(ref_dt)
    curves = fetch_credit_curves(ref_dt.isoformat(), prev_bd.isoformat())

    # Limits
    limits = fetch_issuer_limits(trading_desk)

    # Price sanity check: every non-fund, non-cash position must have PRICE on D and D-1
    quality_flags = fetch_price_quality_flags(trading_desk,
                                               ref_dt.isoformat(),
                                               prev_bd.isoformat())

    # Period returns
    ts = pd.Timestamp(ref_dt)
    fund_rets = compute_period_returns(nav_share_series, ts)

    # Bench: build cumulative for the chosen index
    bench_idx = idx_panel[bench_label] if bench_label in idx_panel.columns else None
    bench_rets = compute_index_period_returns(bench_idx, ts) if bench_idx is not None else \
                 {k: None for k in ("dia","mes","ano","m12","m24","m36","inicio")}

    # Rates today
    cdi_annual = fetch_cdi_annual_rate(ref_dt.isoformat()) or 0.0
    ipca_annual = fetch_ipca_12m(ref_dt.isoformat()) or 0.0

    # Position-level carry + portfolio carry
    pos["carry_anual"] = compute_position_carry(pos, cdi_annual, ipca_annual)
    carry_bruto, carry_liquido = compute_portfolio_carry(pos, nav_at)

    # % do CDI total (since-inception fund / since-inception CDI)
    cdi_full = idx_panel["CDI"] if "CDI" in idx_panel.columns else None
    pct_cdi_total = None
    if cdi_full is not None and len(nav_share_series) and len(cdi_full):
        nav_total_ret = fund_rets.get("inicio")
        # Align CDI to fund's inception window
        nav_start = nav_share_series.index[0]
        cdi_window = cdi_full[cdi_full.index >= nav_start]
        if len(cdi_window) >= 2 and nav_total_ret:
            cdi_total_ret = cdi_window.iloc[-1] / cdi_window.iloc[0] - 1
            if cdi_total_ret > 0:
                pct_cdi_total = nav_total_ret / cdi_total_ret

    # Monthly grid (includes annual Total column; clipped to fund inception)
    cdi_idx_for_returns = cdi_full if cdi_full is not None else nav_share_series  # fallback
    monthly_grid = compute_monthly_returns_grid(nav_share_series, cdi_idx_for_returns)

    print(f"[{fund_label}] rendering body…")

    parts = []
    parts.append(render_header(fund_label, ref_dt, nav_at, fund_rets, bench_rets, bench_label,
                               carry_bruto, carry_liquido, pct_cdi_total))
    parts.append(render_quality_card(quality_flags, ref_dt, prev_bd))
    parts.append(render_distribuicao_card(pos, nav_at))
    parts.append(render_concentracao_card(pos, limits, nav_at))
    # Dragon is FIDC-heavy → default the drill-down to "Por Subordinação"
    alc_default_mode = "subordinacao" if fund_key == "DRAGON" else "tipo"
    parts.append(render_alocacao_card(pos, nav_at, ref_dt, default_mode=alc_default_mode))
    parts.append(render_indices_card(idx_panel, curves, ref_dt))
    parts.append(render_aum_card(nav_h))
    parts.append(render_performance_card(fund_label, fund_rets, bench_rets, bench_label,
                                         nav_share_series, bench_idx, monthly_grid))

    meta = {
        "label": fund_label,
        "nav": nav_at,
        "mtd": fund_rets.get("mes"),
        "ytd": fund_rets.get("ano"),
        "m12": fund_rets.get("m12"),
        "n_flags": len(quality_flags),
    }
    return "".join(parts), meta


def _brand_strip(eyebrow: str, title: str, meta: str) -> str:
    return (
        f'<header class="report-brand-hd">'
        f'  <div class="brand">{RISK_MONITOR_LOGO_SVG}'
        f'    <div class="brand-titles">'
        f'      <span class="brand-eyebrow">{eyebrow}</span>'
        f'      <span class="brand-title">{title}</span>'
        f'    </div></div>'
        f'  <div class="brand-meta">{meta}</div>'
        f'</header>'
    )


def build_report(fund_key: str, ref_dt: date) -> str:
    """Single-fund report (one HTML page for one fund)."""
    body, _ = _build_fund_body(fund_key, ref_dt)
    cfg = CREDIT_FUNDS[fund_key]
    brand = _brand_strip(
        eyebrow="GALÁPAGOS · CRÉDITO · RISK MONITOR",
        title=cfg["label"],
        meta=f'D-2 · {ref_dt.strftime("%d/%m/%Y")}',
    )
    return (
        f'<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8">'
        f'<title>{cfg["label"]} · Crédito · {ref_dt.strftime("%d/%m/%Y")}</title>'
        f'<style>{CSS}</style></head><body><div class="wrap">'
        + brand + body
        + '<footer style="text-align:center;color:var(--muted-soft);font-size:11px;margin-top:30px">'
          'Galápagos Capital · gerado por generate_credit_report.py · '
          'fonte posições: LOTE_BOOK_OVERVIEW · curvas: ANBIMA · asset master: credit.asset_master</footer>'
        + POWERED_BY_FOOTER
        + f'<script>{CSV_JS}</script>'
        + f'<script>{SORT_JS}</script>'
        + f'<script>{ALOC_JS}</script>'
        + f'<script>{TAB_JS}</script>'
        + '</div></body></html>'
    )


def build_consolidated(ref_dt: date, fund_keys: list[str] | None = None) -> str:
    """All-funds report: tab strip on top, one pane per fund. Single HTML file."""
    fund_keys = fund_keys or list(CREDIT_FUNDS.keys())
    panes_html = []
    tab_meta: list[tuple[str, dict]] = []
    for k in fund_keys:
        body, meta = _build_fund_body(k, ref_dt)
        panes_html.append(f'<div class="fund-pane" data-fund="{k}">{body}</div>')
        tab_meta.append((k, meta))

    # Tab strip
    tabs = []
    for k, meta in tab_meta:
        mtd_str = ""
        if meta["mtd"] is not None and not pd.isna(meta["mtd"]):
            mtd_str = f'<span class="tab-meta">{meta["mtd"]*100:+.2f}% mês</span>'
        flag_chip = ""
        if meta["n_flags"] > 0:
            flag_chip = f'<span class="tab-meta" style="color:var(--down)">⚠ {meta["n_flags"]}</span>'
        tabs.append(
            f'<button type="button" class="fund-tab" data-fund="{k}" '
            f'onclick="selectFund(this.dataset.fund)">{meta["label"]}{mtd_str}{flag_chip}</button>'
        )
    tab_strip = f'<div class="fund-tabs">{"".join(tabs)}</div>'

    brand = _brand_strip(
        eyebrow="GALÁPAGOS · CRÉDITO · RISK MONITOR",
        title="Painel Consolidado",
        meta=f'D-2 · {ref_dt.strftime("%d/%m/%Y")} · {len(tab_meta)} fundos',
    )

    return (
        f'<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8">'
        f'<title>Crédito · Consolidado · {ref_dt.strftime("%d/%m/%Y")}</title>'
        f'<style>{CSS}</style></head><body><div class="wrap">'
        + brand + tab_strip + "".join(panes_html)
        + '<footer style="text-align:center;color:var(--muted-soft);font-size:11px;margin-top:30px">'
          'Galápagos Capital · gerado por generate_credit_report.py · '
          'fonte posições: LOTE_BOOK_OVERVIEW · curvas: ANBIMA · asset master: credit.asset_master</footer>'
        + POWERED_BY_FOOTER
        + f'<script>{CSV_JS}</script>'
        + f'<script>{SORT_JS}</script>'
        + f'<script>{ALOC_JS}</script>'
        + f'<script>{TAB_JS}</script>'
        + '</div></body></html>'
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fund", default=None, choices=list(CREDIT_FUNDS.keys()),
                    help="If set, render only this fund. Otherwise render the consolidated multi-fund report.")
    ap.add_argument("--date", default=None,
                    help="Reference date YYYY-MM-DD (default: D-2 from today)")
    ap.add_argument("--out-dir", default="data/credit-reports")
    args = ap.parse_args()

    if args.date:
        ref_dt = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        d = date.today() - timedelta(days=2)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        ref_dt = d

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.fund:
        html = build_report(args.fund, ref_dt)
        out_path = out_dir / f'{ref_dt.isoformat()}_{args.fund.lower()}_credit.html'
    else:
        html = build_consolidated(ref_dt)
        out_path = out_dir / f'{ref_dt.isoformat()}_credito_consolidado.html'

    out_path.write_text(html, encoding="utf-8")
    print(f"saved -> {out_path}")


if __name__ == "__main__":
    main()

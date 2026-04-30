"""
credit_card_renderers.py
========================
Inline 'Crédito · Look-through' section for the main risk monitor.
Used for funds that hold credit instruments via cota look-through but aren't
primary credit funds (BALTRA, EVOLUTION).

Reuses the renderers from ``generate_credit_report`` so the visual treatment
matches the standalone credit reports — same Tipo/Subordinação/Rating
drill-down, same donut palettes, same Soberano bucket on top of the rating
ladder.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

# The credit report module owns the rendering primitives + classification
# helpers. Importing them keeps a single source of truth for tag styles,
# tranche detection, sovereign override, and carry computation.
from generate_credit_report import (
    render_distribuicao_card,
    render_alocacao_card,
    compute_position_carry,
    CSS as _CREDIT_CSS,
    ALOC_JS as CREDIT_ALOC_JS,
    SORT_JS as CREDIT_SORT_JS,
    CSV_JS as CREDIT_CSV_JS,
)


def _fmt_brl(v: float) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"R$ {v:,.0f}".replace(",", ".")


def _fmt_pct(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{v*100:.2f}%"


def _fmt_pct_pl(v_brl: float, nav: float) -> str:
    if not nav or pd.isna(nav):
        return "—"
    return f"{(v_brl / nav) * 100:.1f}%"


def build_credit_section(positions: pd.DataFrame, nav: float, fund_label: str,
                          ref_dt: date, cdi_annual: Optional[float],
                          ipca_annual: Optional[float],
                          default_mode: str = "tipo") -> str:
    """Render the Crédito section for one fund (BALTRA or EVOLUTION).
    positions = output of data_fetch.fetch_fund_credit_positions.
    Returns "" if there's nothing to show."""
    if positions is None or positions.empty or not nav:
        return ""

    p = positions.copy()
    # Normalize NaN → "" for string-bearing columns so downstream `.strip()` /
    # str equality calls don't trip on float NaN. The credit-report fetcher
    # returns these as proper None (xlsm path) but the LOTE_PRODUCT_EXPO join
    # leaves missing asset_master fields as NaN. Use astype(object).fillna("")
    # since `.where(notna, None)` keeps the float dtype on numeric-empty cols.
    for col in ("indexador", "tipo_ativo", "classe", "rating", "setor",
                "grupo_economico", "apelido_emissor", "produto", "book",
                "product_class"):
        if col in p.columns:
            p[col] = p[col].astype(object).where(p[col].notna(), "")
    # Drop sovereign cash-equivalents and minor sovereign tipos from the
    # Crédito look-through view — LFTs are essentially CDI exposure (no real
    # rate risk), and NTN-C/NTN-F are immaterial. NTN-Bs stay because they
    # carry meaningful real-rate / inflation exposure.
    drop_tipos = {"LFT", "NTN-C", "NTN-F"}
    drop_pclass = {"LFT", "NTN-C", "NTN-F"}
    drop_mask = p["tipo_ativo"].isin(drop_tipos) | p["product_class"].isin(drop_pclass)
    p = p[~drop_mask].reset_index(drop=True)
    if p.empty:
        return ""
    p["carry_anual"] = compute_position_carry(p, cdi_annual or 0.0, ipca_annual or 0.0)

    # Header tiles — Total Crédito Look-through · Soberano · Corp Credit · Carry
    sov_mask = p["grupo_economico"].astype(str).str.lower().eq("tesouro nacional") | \
               p["tipo_ativo"].isin(["NTN-B", "NTN-C", "NTN-F", "LFT", "LTN", "Títulos Públicos"])
    sov_pos = float(p.loc[sov_mask, "pos_brl"].sum())
    corp_pos = float(p.loc[~sov_mask, "pos_brl"].sum())
    total_pos = sov_pos + corp_pos

    # Carry weighted on corp only (sovereign carry isn't a discretionary call)
    corp_df = p.loc[~sov_mask].copy()
    corp_df = corp_df[corp_df["carry_anual"].notna()]
    if len(corp_df) and corp_df["pos_brl"].sum() > 0:
        carry_med = float((corp_df["carry_anual"] * corp_df["pos_brl"]).sum()
                          / corp_df["pos_brl"].sum())
    else:
        carry_med = None

    def _tile(label: str, value: str, sub: str = "") -> str:
        sub_html = f'<div class="cs-tile-sub">{sub}</div>' if sub else ""
        return (
            f'<div class="cs-tile">'
            f'<div class="cs-tile-lbl">{label}</div>'
            f'<div class="cs-tile-val">{value}</div>'
            f'{sub_html}</div>'
        )

    tiles_html = (
        '<div class="cs-tiles">'
        + _tile("Crédito Look-through (Total)", _fmt_brl(total_pos),
                _fmt_pct_pl(total_pos, nav) + " do PL")
        + _tile("Soberano", _fmt_brl(sov_pos),
                _fmt_pct_pl(sov_pos, nav) + " do PL")
        + _tile("Corp Credit", _fmt_brl(corp_pos),
                _fmt_pct_pl(corp_pos, nav) + " do PL")
        + _tile("Carry médio (corp)", _fmt_pct(carry_med),
                "ponderado por posição" if carry_med is not None else "")
        + '</div>'
    )

    # Reuse the credit-report card renderers wholesale, with NAV-denominated
    # donuts (ex-Soberano) and a collapsed MM-formatted Alocação table.
    distrib = render_distribuicao_card(p, nav, denom_nav=True, exclude_sovereign=True)
    alocacao = render_alocacao_card(
        p, nav, ref_dt,
        default_mode=default_mode,
        default_collapsed=True,
        mm_mode=True,
    )

    return (
        f'<div class="card credit-section-wrap"><div class="card-hd">'
        f'<div class="card-title">Crédito · Look-through · {fund_label}</div>'
        f'<div class="card-hd-actions">'
        f'<div class="card-sub">'
        f'Posições via TRADING_DESK_SHARE_SOURCE — direto + cotas (Albatroz / IDKAs / CI strategies). '
        f'asset_master cobre instrumentos do kit de crédito; demais aparecem como "Sem Rating".'
        f'</div>'
        f'</div></div><div class="card-body">'
        f'{tiles_html}'
        f'{distrib}'
        f'{alocacao}'
        f'</div></div>'
    )


# CSS additions strictly needed by the imported renderers — extracted from
# generate_credit_report.CSS but trimmed to the classes the credit functions
# emit (.tag-*, .alc-*, .chart-box, .donut-legend, etc.). The main report
# already defines :root vars + .card / .card-hd / .csv-btn / etc., so we
# only add what's missing.
CREDIT_SECTION_CSS = r"""
/* Inline Crédito section — header tiles */
.credit-section-wrap .cs-tiles {
  display:grid; grid-template-columns:repeat(4, 1fr); gap:10px;
  margin-bottom:14px;
}
.credit-section-wrap .cs-tile {
  background:var(--panel-2); border:1px solid var(--line);
  border-radius:10px; padding:12px 14px;
}
.credit-section-wrap .cs-tile-lbl {
  font-size:10px; font-weight:700; color:var(--accent-2);
  text-transform:uppercase; letter-spacing:.6px;
}
.credit-section-wrap .cs-tile-val {
  font-size:18px; font-weight:700; color:var(--text);
  font-family:'JetBrains Mono', monospace; margin-top:4px;
}
.credit-section-wrap .cs-tile-sub {
  font-size:10.5px; color:var(--muted); margin-top:2px;
}

/* Rating tags */
.tag { display:inline-block; padding:2px 7px; border-radius:6px; font-size:10.5px;
       font-weight:600; letter-spacing:.4px; }
.tag-soberano { background:rgba(13,44,90,.65); color:#fff; font-weight:700;
                border:1px solid rgba(124,200,232,.35); }
.tag-aaa { background:rgba(38,208,124,.14); color:var(--up); }
.tag-aa  { background:rgba(26,143,209,.14); color:var(--accent-2); }
.tag-a   { background:rgba(124,200,232,.14); color:#7cc8e8; }
.tag-bbb { background:rgba(245,196,81,.16); color:var(--warn); }
.tag-bb  { background:rgba(255,138,90,.14); color:#ff8a5a; }
.tag-b   { background:rgba(255,90,106,.14); color:var(--down); }
.tag-ccc { background:rgba(190,40,55,.20); color:#ff5a6a; font-style:italic; }
.tag-na  { background:rgba(168,179,194,.10); color:var(--muted); }

/* Tranche tags */
.tag-tr-senior   { background:rgba(38,208,124,.16); color:var(--up); }
.tag-tr-mezanino { background:rgba(245,196,81,.16); color:var(--warn); }
.tag-tr-junior   { background:rgba(255,90,106,.16); color:var(--down); }

/* Alocação drill-down — toggle, parent rows, caret */
tr.alc-parent:hover { background:var(--line); }
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

/* Expand-all / Collapse-all buttons */
.alc-expand-toggle { display:inline-flex; gap:4px; }
.alc-xc-btn {
  background:transparent; border:1px solid var(--line); color:var(--muted);
  font-size:10.5px; padding:4px 9px; border-radius:6px; cursor:pointer;
  font-family:'Inter', sans-serif; letter-spacing:.3px; transition:all .12s;
}
.alc-xc-btn:hover { color:var(--text); border-color:var(--line-2); background:var(--panel-2); }

/* Default-collapsed children — class-based so JS can clear deterministically */
.alc-pane tr.alc-hidden { display:none !important; }

/* Sort arrows on alc-pane table headers */
.alc-pane th { position:relative; }
.alc-pane th .sort-arrow {
  font-size:9px; margin-left:5px; opacity:.85;
  display:inline-block; vertical-align:middle; color:var(--muted-soft);
}
.alc-pane th[data-sort-state="1"] .sort-arrow,
.alc-pane th[data-sort-state="2"] .sort-arrow { color:var(--accent-2); }
.alc-pane th[data-sortable="1"]:hover { background:var(--line); color:var(--text); }

/* Donut chart container + legend (used by render_distribuicao_card) */
.chart-box { background:var(--panel-2); border:1px solid var(--line); border-radius:10px;
             padding:8px; overflow:hidden; }
.chart-box svg { display:block; max-width:100%; height:auto; }
.donut-legend {
  display:grid; grid-template-columns:14px minmax(0,1fr) 56px; gap:4px 10px;
  padding:10px 12px 4px; font-size:11.5px; color:var(--text);
  max-height:200px; overflow-y:auto;
}
.donut-legend .dl-row { display:contents; }
.donut-legend .dl-dot { width:10px; height:10px; border-radius:2px; align-self:center; }
.donut-legend .dl-lbl { white-space:nowrap; overflow:hidden; text-overflow:ellipsis; align-self:center; }
.donut-legend .dl-val { color:var(--muted); font-family:'JetBrains Mono'; font-variant-numeric:tabular-nums; text-align:right; align-self:center; white-space:nowrap; }
"""

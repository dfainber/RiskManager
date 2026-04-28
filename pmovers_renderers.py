"""pmovers_renderers.py — Product-level Top Movers modal.

Per-fund popup showing 5 worst + 5 best instruments for periods
DIA / MTD / YTD / 12M, sourced from REPORT_ALPHA_ATRIBUTION.

Complements the CLASSE-level Top Movers in the Summary card:
that one answers "where is the move", this one answers "which
instrument drove it".

Public API:
  build_pmovers_data_payload(df_pa)        → (script_html, set[funds_with_data])
  build_pmovers_trigger(fund_short)        → small clickable button
  build_pmovers_modal_scaffold()           → single global modal HTML
  PMOVERS_CSS, PMOVERS_JS                  → strings to inline in report
"""
from __future__ import annotations

import json
import math

import pandas as pd

from risk_config import FUND_ORDER, FUND_LABELS, _FUND_PA_KEY


# Periods to render: (period_key, df_column, header_label)
# df_pa columns are already in bps of NAV (dia_bps/mtd_bps/ytd_bps/m12_bps).
_PERIODS = [
    ("DIA",  "dia_bps",  "DIA"),
    ("MTD",  "mtd_bps",  "MTD"),
    ("YTD",  "ytd_bps",  "YTD"),
    ("12M",  "m12_bps",  "12M"),
]

# Filter: CLASSE strictly in this set → exclude
_EXCLUDE_CLASSES = {"Caixa", "Custos"}

# CLASSE → compact tag display label
_TAG_LABEL = {
    "RV BZ":                 "RV BZ",
    "RV Intl":               "RV Intl",
    "RF BZ":                 "RF BZ",
    "RF BZ IPCA":            "RF IPCA",
    "RF BZ IGP-M":           "RF IGP-M",
    "RF Intl":               "RF Intl",
    "Commodities":           "Commodit",
    "FX":                    "FX",
    "BRLUSD":                "BRLUSD",
    "FX Carry & Bases Risk": "FX Carry",
    "ETF Options":           "ETF Opt",
    "Credito":               "Crédito",
    "CredEstr":              "CredEstr",
}


def _fmt_pct(v: float) -> str:
    return f"{v:+.2f}%"


def _compact_tag(classe: str) -> str:
    return _TAG_LABEL.get((classe or "").strip(), (classe or "?")[:9])


def _fund_movers(df_pa: pd.DataFrame, pa_key: str, n: int = 5) -> dict | None:
    """Compute top-n worst + best per period for a single fund.
    Returns dict {period_key: {"worst":[...], "best":[...]}} or None when no data.
    """
    if df_pa is None or df_pa.empty:
        return None
    sub = df_pa[df_pa["FUNDO"] == pa_key].copy()
    if sub.empty:
        return None

    # Filter accounting/cash/cost rows
    sub = sub[~sub["CLASSE"].isin(_EXCLUDE_CLASSES)]
    sub = sub[~sub["PRODUCT"].astype(str).str.startswith("Provision")]
    if sub.empty:
        return None

    out = {}
    for period_key, col_name, _hdr in _PERIODS:
        if col_name not in sub.columns:
            continue
        # Aggregate per (CLASSE, PRODUCT) — same product can appear in
        # multiple LIVROs; sum to get fund-level instrument PnL.
        agg = (sub.groupby(["CLASSE", "PRODUCT"], as_index=False)[col_name]
                  .sum()
                  .rename(columns={col_name: "v"}))
        # df_pa is already in bps of NAV → divide by 100 to get %.
        agg["v"] = agg["v"].astype(float) / 100.0
        # Drop zero-rows to avoid noise (|v| ≥ 0.005% = 0.5 bps)
        agg = agg[agg["v"].abs() >= 0.005]
        if agg.empty:
            out[period_key] = {"worst": [], "best": []}
            continue

        worst = agg.nsmallest(n, "v")
        best  = agg.nlargest(n,  "v")

        def _to_rows(d):
            return [
                {
                    "tag":     _compact_tag(r.CLASSE),
                    "classe":  str(r.CLASSE),
                    "product": str(r.PRODUCT),
                    "v":       round(float(r.v), 4),
                }
                for r in d.itertuples(index=False)
            ]

        out[period_key] = {"worst": _to_rows(worst), "best": _to_rows(best)}
    return out if any(out.get(p, {}).get("worst") or out.get(p, {}).get("best")
                       for p in [k[2] for k in _PERIODS]) else None


def build_pmovers_data_payload(df_pa: pd.DataFrame) -> tuple[str, set[str]]:
    payload: dict[str, dict] = {}
    funds_with_data: set[str] = set()
    for short in FUND_ORDER:
        pa_key = _FUND_PA_KEY.get(short)
        if not pa_key:
            continue
        d = _fund_movers(df_pa, pa_key)
        if not d:
            continue
        payload[short] = {
            "fund_label": FUND_LABELS.get(short, short),
            "periods":    d,
        }
        funds_with_data.add(short)
    js_obj = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    script = f'<script>window.__PMOVERS_DATA = {js_obj};</script>'
    return script, funds_with_data


def build_pmovers_trigger(fund_short: str, has_data: bool = True) -> str:
    if not has_data:
        return ""
    return (
        f'<span class="pmovers-trigger" onclick="openPMovers(\'{fund_short}\')" '
        f'title="Top movers por instrumento (DIA · MTD · YTD · 12M)">'
        f'Top Movers Produto →</span>'
    )


def build_pmovers_modal_scaffold() -> str:
    return """
<div id="pmovers-backdrop" class="pmovers-backdrop" onclick="closePMovers()"></div>
<div id="pmovers-modal" class="pmovers-modal" role="dialog" aria-hidden="true">
  <div class="pmovers-head">
    <span id="pmovers-title" class="pmovers-title">Top Movers Produto</span>
    <span class="pmovers-close" onclick="closePMovers()" title="Fechar (Esc)">×</span>
  </div>
  <div id="pmovers-body" class="pmovers-body"></div>
  <div class="pmovers-footnote">
    5 piores + 5 melhores instrumentos por período · agregação por (CLASSE, PRODUCT)
    · filtra Caixa / Custos / Provisões · |contrib| ≥ 0,005%.
  </div>
</div>
""".strip()


PMOVERS_CSS = r"""
<style>
.pmovers-trigger {
  display: inline-block;
  margin-left: 12px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.04em;
  color: var(--accent, #60a5fa);
  cursor: pointer;
  user-select: none;
  border: 1px solid var(--accent, #60a5fa);
  border-radius: 7px;
  padding: 4px 11px;
  background: rgba(96, 165, 250, 0.08);
  transition: background 0.12s, color 0.12s, border-color 0.12s;
  font-family: 'Inter', sans-serif;
}
.pmovers-trigger:hover {
  background: var(--accent, #60a5fa);
  color: #fff;
}

.pmovers-backdrop {
  display: none;
  position: fixed; top:0; left:0; right:0; bottom:0;
  background: rgba(0,0,0,0.55);
  z-index: 9998;
}
.pmovers-backdrop.show { display: block; }

.pmovers-modal {
  display: none;
  position: fixed;
  top: 50%; left: 50%;
  transform: translate(-50%, -50%);
  width: min(1100px, calc(100vw - 40px));
  max-height: calc(100vh - 80px);
  overflow: auto;
  background: var(--bg, #0f1422);
  border: 1px solid var(--border, #2a3447);
  border-radius: 6px;
  box-shadow: 0 8px 32px rgba(0,0,0,0.5);
  z-index: 9999;
  padding: 16px 18px 14px;
  color: var(--text);
}
.pmovers-modal.show { display: block; }

.pmovers-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--border, #2a3447);
}
.pmovers-title {
  font-size: 14px;
  font-weight: 600;
}
.pmovers-close {
  font-size: 22px;
  line-height: 1;
  cursor: pointer;
  color: var(--muted);
  padding: 0 4px;
}
.pmovers-close:hover { color: var(--text); }

.pmovers-body {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 14px;
}
.pmovers-col {
  border: 1px solid var(--border-soft, rgba(168, 179, 194, 0.15));
  border-radius: 4px;
  padding: 8px 10px;
  font-size: 11.5px;
}
.pmovers-col-title {
  font-weight: 700;
  font-size: 12px;
  margin-bottom: 6px;
  letter-spacing: 0.05em;
  color: var(--accent, #60a5fa);
}
.pmovers-section {
  margin-top: 6px;
}
.pmovers-section-title {
  font-size: 10.5px;
  font-weight: 600;
  margin-bottom: 4px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.pmovers-section-title.worst { color: var(--down, #ef4444); }
.pmovers-section-title.best  { color: var(--up,   #22c55e); }
.pmovers-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  padding: 3px 0;
  border-bottom: 1px solid var(--border-soft, rgba(168, 179, 194, 0.08));
  gap: 6px;
}
.pmovers-row:last-child { border-bottom: none; }
.pmovers-row-left {
  display: flex;
  align-items: baseline;
  gap: 6px;
  overflow: hidden;
  flex: 1 1 auto;
  min-width: 0;
}
.pmovers-tag {
  font-size: 9.5px;
  font-weight: 600;
  color: var(--muted-strong, #c9d1dd);
  background: rgba(96, 165, 250, 0.10);
  border-radius: 3px;
  padding: 1px 5px;
  white-space: nowrap;
}
.pmovers-product {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-family: monospace;
  font-size: 11px;
}
.pmovers-val {
  font-family: monospace;
  font-size: 11px;
  font-weight: 600;
  white-space: nowrap;
}
.pmovers-val.pos { color: var(--up,   #22c55e); }
.pmovers-val.neg { color: var(--down, #ef4444); }
.pmovers-empty {
  font-size: 10.5px;
  color: var(--muted);
  font-style: italic;
  padding: 4px 0;
}

.pmovers-footnote {
  margin-top: 12px;
  padding-top: 8px;
  border-top: 1px solid var(--border, #2a3447);
  font-size: 10.5px;
  color: var(--muted);
}

@media (max-width: 900px) {
  .pmovers-body { grid-template-columns: repeat(2, 1fr); }
}
</style>
""".strip()


PMOVERS_JS = r"""
<script>
(function() {
  const PERIODS = [
    {key: "DIA", title: "DIA"},
    {key: "MTD", title: "MTD"},
    {key: "YTD", title: "YTD"},
    {key: "12M", title: "12M"},
  ];

  function _fmtPct(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return "—";
    var s = v >= 0 ? "+" : "";
    return s + v.toFixed(2) + "%";
  }

  function _renderRow(r) {
    var cls = (r.v >= 0) ? "pos" : "neg";
    return (
      '<div class="pmovers-row">' +
        '<div class="pmovers-row-left">' +
          '<span class="pmovers-tag">' + _esc(r.tag) + '</span>' +
          '<span class="pmovers-product" title="' + _esc(r.classe) + ' · ' + _esc(r.product) + '">' + _esc(r.product) + '</span>' +
        '</div>' +
        '<span class="pmovers-val ' + cls + '">' + _fmtPct(r.v) + '</span>' +
      '</div>'
    );
  }

  function _esc(s) {
    if (s === null || s === undefined) return "";
    return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  }

  function _renderColumn(periodCfg, periodData) {
    var worst = (periodData && periodData.worst) || [];
    var best  = (periodData && periodData.best)  || [];
    var worstHtml = worst.length ? worst.map(_renderRow).join("") : '<div class="pmovers-empty">— sem movimentos materiais —</div>';
    var bestHtml  = best.length  ? best.map(_renderRow).join("")  : '<div class="pmovers-empty">— sem movimentos materiais —</div>';
    return (
      '<div class="pmovers-col">' +
        '<div class="pmovers-col-title">' + periodCfg.title + '</div>' +
        '<div class="pmovers-section">' +
          '<div class="pmovers-section-title worst">5 PIORES</div>' +
          worstHtml +
        '</div>' +
        '<div class="pmovers-section" style="margin-top:10px">' +
          '<div class="pmovers-section-title best">5 MELHORES</div>' +
          bestHtml +
        '</div>' +
      '</div>'
    );
  }

  window.openPMovers = function(fundShort) {
    var d = window.__PMOVERS_DATA && window.__PMOVERS_DATA[fundShort];
    if (!d) {
      alert("Sem dados de movers para " + fundShort);
      return;
    }
    document.getElementById("pmovers-title").textContent =
      d.fund_label + " · Top Movers Produto";
    var body = document.getElementById("pmovers-body");
    body.innerHTML = PERIODS.map(function(p) {
      return _renderColumn(p, d.periods[p.key]);
    }).join("");
    document.getElementById("pmovers-backdrop").classList.add("show");
    document.getElementById("pmovers-modal").classList.add("show");
    document.getElementById("pmovers-modal").setAttribute("aria-hidden", "false");
  };

  window.closePMovers = function() {
    document.getElementById("pmovers-backdrop").classList.remove("show");
    document.getElementById("pmovers-modal").classList.remove("show");
    document.getElementById("pmovers-modal").setAttribute("aria-hidden", "true");
  };

  document.addEventListener("keydown", function(e) {
    if (e.key === "Escape") {
      var m = document.getElementById("pmovers-modal");
      if (m && m.classList.contains("show")) closePMovers();
    }
  });
})();
</script>
""".strip()

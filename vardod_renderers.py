"""vardod_renderers.py — VaR DoD attribution modal (Phase 2 of VaR DoD feature).

Provides:
  - build_vardod_trigger(fund_key, has_data)  → small clickable link for section header
  - build_vardod_modal_scaffold()             → single global modal HTML
  - build_vardod_data_payload(date_str)       → <script>window.__VAR_DOD_DATA = {...}</script>
  - VARDOD_CSS, VARDOD_JS                      → strings to inline into report

Modal opens via openVarDoD(fundKey) — populates from window.__VAR_DOD_DATA[fundKey].
Closes on backdrop click, X button, or ESC. Sortable table by clicking headers.
"""
from __future__ import annotations

import json
import math

import pandas as pd

from data_fetch import fetch_var_dod_decomposition, _VAR_DOD_DISPATCH


_FUND_LABEL = {
    "IDKA_3Y":   "IDKA 3Y",
    "IDKA_10Y":  "IDKA 10Y",
    "MACRO":     "MACRO",
    "QUANT":     "QUANT",
    "EVOLUTION": "EVOLUTION",
    "ALBATROZ":  "ALBATROZ",
    "MACRO_Q":   "MACRO_Q",
    "BALTRA":    "BALTRA",
    "FRONTIER":  "FRONTIER",
}


def build_vardod_trigger(fund_key: str, has_data: bool = True) -> str:
    """Small clickable text-link for the exposure section header.
    Returns empty string when no data available (trigger hidden)."""
    if not has_data or fund_key not in _VAR_DOD_DISPATCH:
        return ""
    return (
        f'<span class="vardod-trigger" onclick="openVarDoD(\'{fund_key}\')" '
        f'title="Decomposição da mudança de VaR/BVaR vs D-1">'
        f'VaR DoD →</span>'
    )


def build_vardod_modal_scaffold() -> str:
    """Single global modal HTML — placed once near end of <body>."""
    return """
<div id="vardod-backdrop" class="vardod-backdrop" onclick="closeVarDoD()"></div>
<div id="vardod-modal" class="vardod-modal modal" role="dialog" aria-hidden="true">
  <div class="vardod-head modal-head">
    <span id="vardod-title" class="vardod-title modal-title">VaR DoD</span>
    <span class="vardod-close" onclick="closeVarDoD()" title="Fechar (Esc)">×</span>
  </div>
  <div id="vardod-headline" class="vardod-headline"></div>
  <div id="vardod-warning" class="vardod-warning" style="display:none"></div>
  <div class="vardod-table-wrap">
    <table id="vardod-table" class="vardod-table">
      <thead>
        <tr id="vardod-thead-row"></tr>
      </thead>
      <tbody id="vardod-tbody"></tbody>
    </table>
  </div>
  <div id="vardod-footnote" class="vardod-footnote"></div>
</div>
""".strip()


def _df_to_payload(df: pd.DataFrame, fund_key: str) -> dict:
    """Convert decomposition DataFrame to JSON-friendly dict.
    Filters out rows where both D-1 and D contribs are immaterial (< 0.05 bps)."""
    if df is None or df.empty:
        return {}

    mask = (df["contrib_d1_bps"].abs() >= 0.05) | (df["contrib_d_bps"].abs() >= 0.05)
    df = df[mask].copy()
    if df.empty:
        return {}

    has_decomp    = bool(df["pos_effect_bps"].notna().any())
    has_vol_proxy = bool(df["d_vol_bps"].notna().any())
    has_pos_info  = bool(df["d_pos_pct"].notna().any())

    rows = []
    for r in df.itertuples(index=False):
        def _f(v):
            if v is None:
                return None
            try:
                fv = float(v)
                if math.isnan(fv) or math.isinf(fv):
                    return None
                return round(fv, 4)
            except (TypeError, ValueError):
                return None

        # Serialize children (look-through explosion) if present
        children_raw = getattr(r, "children", None)
        children = None
        if isinstance(children_raw, list) and len(children_raw) > 0:
            children = []
            for c in children_raw:
                children.append({
                    "label":          str(c.get("label", "")),
                    "group":          (None if c.get("group") is None else str(c.get("group"))),
                    "contrib_d1_bps": _f(c.get("contrib_d1_bps")),
                    "contrib_d_bps":  _f(c.get("contrib_d_bps")),
                    "delta_bps":      _f(c.get("delta_bps")),
                    "pos_d1":         _f(c.get("pos_d1")),
                    "pos_d":          _f(c.get("pos_d")),
                    "d_pos_pct":      _f(c.get("d_pos_pct")),
                    "vol_d1_bps":     _f(c.get("vol_d1_bps")),
                    "vol_d_bps":      _f(c.get("vol_d_bps")),
                    "d_vol_bps":      _f(c.get("d_vol_bps")),
                    "pos_effect_bps": _f(c.get("pos_effect_bps")),
                    "vol_effect_bps": _f(c.get("vol_effect_bps")),
                    "sign":           str(c.get("sign", "")),
                    "override":       str(c.get("override_note", "") or ""),
                })

        rows.append({
            "label": str(r.label),
            "group": (None if pd.isna(r.group) else str(r.group)),
            "contrib_d1_bps": _f(r.contrib_d1_bps),
            "contrib_d_bps":  _f(r.contrib_d_bps),
            "delta_bps":      _f(r.delta_bps),
            "pos_d1":         _f(r.pos_d1),
            "pos_d":          _f(r.pos_d),
            "d_pos_pct":      _f(r.d_pos_pct),
            "vol_d1_bps":     _f(r.vol_d1_bps),
            "vol_d_bps":      _f(r.vol_d_bps),
            "d_vol_bps":      _f(r.d_vol_bps),
            "pos_effect_bps": _f(r.pos_effect_bps),
            "vol_effect_bps": _f(r.vol_effect_bps),
            "sign":           str(r.sign),
            "override":       str(r.override_note or ""),
            "children":       children,
        })

    cfg = _VAR_DOD_DISPATCH.get(fund_key, (None, None, "VaR", None))
    metric = cfg[2]

    def _r(v, n=3):
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return round(f, n)
        except (TypeError, ValueError):
            return None

    pe_sum = _r(float(df["pos_effect_bps"].sum())) if has_decomp else None
    ve_sum = _r(float(df["vol_effect_bps"].sum())) if has_decomp else None

    return {
        "fund_label": _FUND_LABEL.get(fund_key, fund_key),
        "metric": metric,
        "delta_total_bps":      _r(float(df["delta_bps"].sum())),
        "pos_effect_total_bps": pe_sum,
        "vol_effect_total_bps": ve_sum,
        "has_decomp": has_decomp,
        "has_vol_proxy": has_vol_proxy,
        "has_pos_info": has_pos_info,
        "modal_note": str(df.attrs.get("modal_note", "")) or None,
        "rows": rows,
    }


def build_vardod_data_payload(date_str: str,
                              prefetched_dfs: dict | None = None) -> tuple[str, set[str]]:
    """Fetch decomposition for all supported funds, return:
       (script_html, funds_with_data) — script_html is the <script>...</script>
       block; funds_with_data is the set of fund_keys with non-empty data.

       prefetched_dfs: optional {fund_key → DataFrame} cache to skip the DB
       round-trips (useful when the caller already pulled DoD data for
       Comments / commentary upstream).
    """
    payload: dict[str, dict] = {}
    funds_with_data: set[str] = set()
    for fund_key in _VAR_DOD_DISPATCH:
        if prefetched_dfs is not None and fund_key in prefetched_dfs:
            df = prefetched_dfs[fund_key]
        else:
            try:
                df = fetch_var_dod_decomposition(fund_key, date_str)
            except Exception as e:
                print(f"  [vardod] {fund_key} failed: {e}")
                continue
        if df is None or df.empty:
            continue
        d = _df_to_payload(df, fund_key)
        if not d.get("rows"):
            continue
        payload[fund_key] = d
        funds_with_data.add(fund_key)

    js_obj = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    script = f'<script>window.__VAR_DOD_DATA = {js_obj};</script>'
    return script, funds_with_data


VARDOD_CSS = r"""
<style>
.vardod-trigger {
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
.vardod-trigger:hover {
  background: var(--accent, #60a5fa);
  color: #fff;
  border-color: var(--accent, #60a5fa);
}

.vardod-backdrop {
  display: none;
  position: fixed; top:0; left:0; right:0; bottom:0;
  background: rgba(0,0,0,0.55);
  z-index: 9998;
}
.vardod-backdrop.show { display: block; }

.vardod-modal {
  display: none;
  position: fixed;
  top: 50%; left: 50%;
  transform: translate(-50%, -50%);
  width: min(820px, calc(100vw - 40px));
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
.vardod-modal.show { display: block; }

.vardod-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--border, #2a3447);
}
.vardod-title {
  font-size: 14px;
  font-weight: 600;
  letter-spacing: 0.2px;
}
.vardod-close {
  font-size: 22px;
  line-height: 1;
  cursor: pointer;
  color: var(--muted);
  padding: 0 4px;
}
.vardod-close:hover { color: var(--text); }

.vardod-headline {
  font-size: 13px;
  margin-bottom: 8px;
  font-weight: 500;
}
.vardod-headline .delta-up   { color: var(--down, #ef4444); }
.vardod-headline .delta-down { color: var(--up,   #22c55e); }
.vardod-headline .delta-flat { color: var(--muted); }

.vardod-warning {
  font-size: 11px;
  background: rgba(234, 179, 8, 0.12);
  border: 1px solid rgba(234, 179, 8, 0.4);
  border-radius: 4px;
  padding: 6px 10px;
  margin-bottom: 8px;
  color: #eab308;
}

.vardod-table-wrap {
  margin-top: 6px;
  overflow-x: auto;
}
.vardod-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11.5px;
}
.vardod-table th, .vardod-table td {
  padding: 5px 7px;
  text-align: right;
  border-bottom: 1px solid var(--border-soft, rgba(168, 179, 194, 0.10));
  white-space: nowrap;
}
.vardod-table th:first-child, .vardod-table td:first-child {
  text-align: left;
  white-space: nowrap;
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.vardod-table th {
  font-weight: 600;
  color: var(--muted-strong, #c9d1dd);
  cursor: pointer;
  user-select: none;
  position: sticky; top: 0;
  background: var(--bg, #0f1422);
}
.vardod-table th:hover { color: var(--text); }
.vardod-table tr.row-override td { background: rgba(234, 179, 8, 0.06); }
.vardod-table tr.row-child td { background: rgba(96, 165, 250, 0.04); padding-top: 3px; padding-bottom: 3px; }
.vardod-table tr.row-child td:first-child { padding-left: 22px; color: var(--muted-strong, #c9d1dd); font-size: 11px; }
.vardod-table td.vardod-expandable { font-weight: 500; }
.vardod-table .vardod-caret { display: inline-block; width: 12px; color: var(--accent, #60a5fa); font-size: 9px; }
.vardod-table .pos { color: var(--up,   #22c55e); }
.vardod-table .neg { color: var(--down, #ef4444); }
.vardod-table .muted { color: var(--muted); }

.vardod-footnote {
  margin-top: 12px;
  padding-top: 8px;
  border-top: 1px solid var(--border, #2a3447);
  font-size: 11px;
  color: var(--muted);
  line-height: 1.45;
}
</style>
""".strip()


VARDOD_JS = r"""
<script>
(function() {
  let _vd_state = { fundKey: null, sortCol: 'delta_bps', sortDir: -1 };

  function _fmtBps(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    var sign = v >= 0 ? '+' : '';
    return sign + v.toFixed(2);
  }
  function _fmtPct(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    var sign = v >= 0 ? '+' : '';
    return sign + v.toFixed(1) + '%';
  }
  function _signClass(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return 'muted';
    if (Math.abs(v) < 0.05) return 'muted';
    return v > 0 ? 'pos' : 'neg';
  }

  function _renderHeader(d) {
    var cols = [
      ['label',          'Fator',       'left'],
      ['contrib_d1_bps', 'D-1',         'right'],
      ['contrib_d_bps',  'D',           'right'],
      ['delta_bps',      'Δ',           'right']
    ];
    if (d.has_decomp) {
      cols.push(['pos_effect_bps', 'Pos eff', 'right']);
      cols.push(['vol_effect_bps', 'Marg eff', 'right']);
    }
    if (d.has_pos_info) cols.push(['d_pos_pct', 'Δ pos %', 'right']);
    if (d.has_vol_proxy || d.has_decomp) cols.push(['d_vol_bps', 'Δ vol', 'right']);
    var tr = document.getElementById('vardod-thead-row');
    tr.innerHTML = '';
    cols.forEach(function(c) {
      var th = document.createElement('th');
      th.textContent = c[1];
      th.dataset.col = c[0];
      th.style.textAlign = c[2];
      th.onclick = function() { _sortAndRender(c[0]); };
      tr.appendChild(th);
    });
  }

  function _renderRow(r, d, isChild) {
    var tr = document.createElement('tr');
    if (r.override && r.override.length) tr.className = 'row-override';
    if (isChild) tr.className = (tr.className ? tr.className + ' ' : '') + 'row-child';

    function td(text, cls, isHTML) {
      var el = document.createElement('td');
      if (isHTML) el.innerHTML = text;
      else el.textContent = text;
      if (cls) el.className = cls;
      return el;
    }

    var hasKids = !isChild && Array.isArray(r.children) && r.children.length > 0;
    var labelText = (isChild ? '↳ ' : '') + (r.label || '—');
    if (hasKids) {
      var caret = '<span class="vardod-caret">▶</span> ';
      tr.appendChild(td(caret + labelText, 'vardod-expandable', true));
      tr.style.cursor = 'pointer';
      tr.onclick = function(ev) {
        if (ev.target && ev.target.tagName === 'A') return;
        _toggleChildren(tr, r, d);
      };
    } else {
      tr.appendChild(td(labelText));
    }
    tr.appendChild(td(_fmtBps(r.contrib_d1_bps), _signClass(r.contrib_d1_bps)));
    tr.appendChild(td(_fmtBps(r.contrib_d_bps),  _signClass(r.contrib_d_bps)));
    tr.appendChild(td(_fmtBps(r.delta_bps),      _signClass(r.delta_bps)));
    if (d.has_decomp) {
      tr.appendChild(td(_fmtBps(r.pos_effect_bps), _signClass(r.pos_effect_bps)));
      tr.appendChild(td(_fmtBps(r.vol_effect_bps), _signClass(r.vol_effect_bps)));
    }
    if (d.has_pos_info) tr.appendChild(td(_fmtPct(r.d_pos_pct), 'muted'));
    if (d.has_vol_proxy || d.has_decomp) tr.appendChild(td(_fmtBps(r.d_vol_bps), 'muted'));
    return tr;
  }

  function _toggleChildren(parentTr, parentRow, d) {
    var open = parentTr.classList.toggle('expanded');
    var caret = parentTr.querySelector('.vardod-caret');
    if (caret) caret.textContent = open ? '▼' : '▶';
    if (open) {
      // Insert child rows immediately after the parent
      var anchor = parentTr;
      parentRow.children.forEach(function(c) {
        var ctr = _renderRow(c, d, true);
        ctr.dataset.childOf = parentRow.label;
        anchor.parentNode.insertBefore(ctr, anchor.nextSibling);
        anchor = ctr;
      });
    } else {
      // Remove child rows
      var rows = parentTr.parentNode.querySelectorAll('tr[data-child-of]');
      rows.forEach(function(rr) {
        if (rr.dataset.childOf === parentRow.label) rr.remove();
      });
    }
  }

  function _renderRows(rows, d) {
    var tbody = document.getElementById('vardod-tbody');
    tbody.innerHTML = '';
    rows.forEach(function(r) {
      tbody.appendChild(_renderRow(r, d, false));
    });
  }

  function _sortAndRender(col) {
    var d = window.__VAR_DOD_DATA && window.__VAR_DOD_DATA[_vd_state.fundKey];
    if (!d) return;
    var dir = (_vd_state.sortCol === col) ? -_vd_state.sortDir : -1;
    _vd_state.sortCol = col;
    _vd_state.sortDir = dir;
    var rows = d.rows.slice();
    rows.sort(function(a, b) {
      var av = a[col], bv = b[col];
      if (col === 'label' || col === 'group') {
        av = (av || ''); bv = (bv || '');
        return av < bv ? -dir : av > bv ? dir : 0;
      }
      // sort by absolute value for delta_bps and effect/Δ columns
      var absoluteCols = ['delta_bps','pos_effect_bps','vol_effect_bps','d_vol_bps','d_pos_pct'];
      if (absoluteCols.indexOf(col) >= 0) {
        av = Math.abs(av || 0); bv = Math.abs(bv || 0);
      } else {
        av = (av === null || av === undefined) ? -Infinity : av;
        bv = (bv === null || bv === undefined) ? -Infinity : bv;
      }
      return av < bv ? -dir : av > bv ? dir : 0;
    });
    _renderRows(rows, d);
  }

  window.openVarDoD = function(fundKey) {
    var d = window.__VAR_DOD_DATA && window.__VAR_DOD_DATA[fundKey];
    if (!d) {
      alert('Sem dados de VaR DoD para este fundo (D-1 indisponível).');
      return;
    }
    _vd_state.fundKey = fundKey;
    _vd_state.sortCol = 'delta_bps';
    _vd_state.sortDir = -1;

    document.getElementById('vardod-title').textContent =
      d.fund_label + ' · ' + d.metric + ' DoD';

    var dt = d.delta_total_bps;
    var dtCls = (Math.abs(dt) < 0.5) ? 'delta-flat' : (dt > 0 ? 'delta-up' : 'delta-down');
    var headline = 'Δ' + d.metric + ' = <span class="' + dtCls + '">' + _fmtBps(dt) + ' bps</span>';
    if (d.has_decomp && d.pos_effect_total_bps !== null && d.vol_effect_total_bps !== null) {
      headline += '  &nbsp;·&nbsp;  posição: ' + _fmtBps(d.pos_effect_total_bps) +
                  ' bps  ·  marginal (Δg): ' + _fmtBps(d.vol_effect_total_bps) + ' bps';
    }
    document.getElementById('vardod-headline').innerHTML = headline;

    // Warning bar: modal-level note (e.g. FRONTIER caveat) OR row-level overrides (IDKA passivo)
    var w = document.getElementById('vardod-warning');
    var msgs = [];
    if (d.modal_note) {
      msgs.push('ℹ ' + d.modal_note);
    }
    var hasOverride = d.rows.some(function(r) { return r.override && r.override.length; });
    if (hasOverride) {
      var notes = d.rows.filter(function(r) { return r.override; })
                        .map(function(r) { return r.label + ': ' + r.override; })
                        .join(' · ');
      msgs.push('⚠ Override aplicado no passivo: ' + notes);
    }
    if (msgs.length) {
      w.innerHTML = msgs.join('<br>');
      w.style.display = 'block';
    } else {
      w.style.display = 'none';
      w.innerHTML = '';
    }

    // Footnote
    var fn = '';
    if (d.has_decomp) {
      fn = 'Decomposição: g = contrib/pos é a contribuição marginal de VaR por BRL de exposição ' +
           '(absorve mudanças de vol E correlação — engine não isola σ). ' +
           'Pos eff = Δpos × g_(D-1) (mudou tamanho da posição na vol/correlação de ontem). ' +
           'Marg eff = pos_D × Δg (mesma posição, novo regime de risco). ' +
           'Soma exata: Pos eff + Marg eff = Δ.';
    } else {
      fn = 'Engine deste fundo não publica vol/posição por linha — só ΔContribuição é mostrado. ' +
           'Δ = D - D-1 em bps de NAV.';
    }
    fn += ' Linhas em destaque amarelo = override aplicado no passivo (engine artifact).';
    document.getElementById('vardod-footnote').textContent = fn;

    _renderHeader(d);
    _sortAndRender('delta_bps');

    document.getElementById('vardod-backdrop').classList.add('show');
    document.getElementById('vardod-modal').classList.add('show');
    document.getElementById('vardod-modal').setAttribute('aria-hidden', 'false');
  };

  window.closeVarDoD = function() {
    document.getElementById('vardod-backdrop').classList.remove('show');
    document.getElementById('vardod-modal').classList.remove('show');
    document.getElementById('vardod-modal').setAttribute('aria-hidden', 'true');
  };

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      var m = document.getElementById('vardod-modal');
      if (m && m.classList.contains('show')) closeVarDoD();
    }
  });
})();
</script>
""".strip()

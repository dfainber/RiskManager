"""HTML renderers for the Market States section + summary commentary strip.

Two public functions:
  - build_market_states_section(snap)  → full Market-tab "Estados" view
  - build_market_states_summary_strip(snap) → compact colored-pill strip for Resumo

Both consume the dict returned by `market_states_fetch.fetch_market_states_snapshot`.
Return "" when snap is empty (graceful degrade — no section / no strip rendered).
"""
from __future__ import annotations

from typing import Any

# ── Color palettes ─────────────────────────────────────────────────────────
_RISK_COLORS = {
    "EXTREME_FEAR":   "#9b1c2c",   # dark red
    "FEAR":           "#d35400",   # orange-red
    "NEUTRAL":        "#7f8c8d",   # gray
    "GREED":          "#27ae60",   # green
    "EXTREME_GREED":  "#0e7d4f",   # dark green
}
_RISK_LABEL_PT = {
    "EXTREME_FEAR":   "Medo Extremo",
    "FEAR":           "Medo",
    "NEUTRAL":        "Neutro",
    "GREED":          "Ganância",
    "EXTREME_GREED":  "Ganância Extrema",
}
_RISK_ORDER = ["EXTREME_FEAR", "FEAR", "NEUTRAL", "GREED", "EXTREME_GREED"]

_QUAD_COLORS = {
    "Q1": "#27ae60",   # Goldilocks — green (growth↑, inflation↓)
    "Q2": "#2980b9",   # Reflation  — blue (both↑)
    "Q3": "#c0392b",   # Stagflation — red (growth↓, inflation↑)
    "Q4": "#8e44ad",   # Deflation  — purple (both↓)
}
_QUAD_LABEL = {
    "Q1": "Goldilocks",
    "Q2": "Reflation",
    "Q3": "Stagflation",
    "Q4": "Deflation",
}
_QUAD_AXES = {
    "Q1": ("Crescimento ↑", "Inflação ↓"),
    "Q2": ("Crescimento ↑", "Inflação ↑"),
    "Q3": ("Crescimento ↓", "Inflação ↑"),
    "Q4": ("Crescimento ↓", "Inflação ↓"),
}


def _pill(text: str, bg: str, fg: str = "#fff", *, big: bool = False, title: str = "") -> str:
    """Render a colored pill / badge."""
    pad = "4px 10px" if not big else "6px 14px"
    fs  = "11px"     if not big else "13px"
    weight = 700 if big else 600
    title_attr = f' title="{title}"' if title else ""
    return (
        f'<span{title_attr} style="display:inline-block;background:{bg};color:{fg};'
        f'padding:{pad};border-radius:12px;font-size:{fs};font-weight:{weight};'
        f'letter-spacing:0.3px;line-height:1.2">{text}</span>'
    )


def _risk_pill(state: str, z: float, *, big: bool = False) -> str:
    bg = _RISK_COLORS.get(state, "#7f8c8d")
    label_pt = _RISK_LABEL_PT.get(state, state)
    text = f'{label_pt} · z {z:+.2f}'
    return _pill(text, bg, big=big, title=f"Risk State (CNN F&G-style): {state}, z-score {z:+.3f}")


def _quad_pill(q: str, *, label: str = "", big: bool = False, title: str = "") -> str:
    bg = _QUAD_COLORS.get(q, "#7f8c8d")
    name = _QUAD_LABEL.get(q, q)
    text = f'{q} · {name}' if not label else f'{label}: {q}'
    return _pill(text, bg, big=big, title=title or f"{q} = {name}")


# ── Compact strip for the Resumo (summary) view ────────────────────────────
def build_market_states_summary_strip(snap: dict[str, Any]) -> str:
    """One-line colored strip placed at the top of the Resumo view.

    Renders: Risk State pill + master Quadrant pill + 3 per-duration mini pills
    + confluence indicator + asof. Designed to scan in <2 seconds.
    """
    if not snap:
        return ""

    risk = snap.get("risk_state", "NEUTRAL")
    z    = float(snap.get("ror_z", 0.0))
    regime = snap.get("regime", "Q?")
    conf   = float(snap.get("confidence", 0.0))
    by_dur = snap.get("by_duration", {})
    confluence = int(snap.get("confluence", 0))
    asof = snap.get("asof", "")

    risk_html = _risk_pill(risk, z, big=True)
    master_html = _quad_pill(
        regime, big=True,
        title=f"Master regime (modal across markets): {regime} · confidence {conf*100:.0f}%"
    )
    dur_pills = "".join(
        _quad_pill(by_dur.get(d, "?"), label=d, title=f"{d} ({snap.get('duration_days',{}).get(d,'?')}d)")
        for d in ("Trade", "Trend", "Tail")
    )

    # Confluence dots — 3 = all aligned, 1 = total flux
    dots = "".join(
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        f'margin:0 1px;background:{"#26a65b" if i < confluence else "rgba(255,255,255,0.18)"}"></span>'
        for i in range(3)
    )

    return f"""
    <section class="card" style="margin-bottom:14px;padding:12px 16px;background:rgba(255,255,255,0.02);
                                  border-left:4px solid {_RISK_COLORS.get(risk, '#7f8c8d')}">
      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px 14px;font-size:12px">
        <span style="font-weight:700;color:var(--text)">Estados de Mercado</span>
        <span style="color:var(--muted)">— {asof}</span>
        {risk_html}
        {master_html}
        <span style="color:var(--muted);margin-left:6px">por duração:</span>
        <span style="display:inline-flex;gap:4px">{dur_pills}</span>
        <span style="color:var(--muted);margin-left:6px">confluência</span>
        <span style="display:inline-flex;align-items:center">{dots}</span>
        <span style="color:var(--muted);font-size:11px">{confluence}/3</span>
      </div>
    </section>"""


# ── Full section for the Market tab "Estados" sub-tab ──────────────────────
def _risk_gauge(state: str, z: float) -> str:
    """5-segment horizontal gauge with marker at the current bucket."""
    segs = ""
    for s in _RISK_ORDER:
        active = (s == state)
        bg = _RISK_COLORS[s]
        opacity = "1" if active else "0.32"
        border = "border:2px solid #fff" if active else "border:2px solid transparent"
        label_pt = _RISK_LABEL_PT[s]
        segs += (
            f'<div style="flex:1;background:{bg};opacity:{opacity};{border};'
            f'padding:8px 4px;text-align:center;font-size:10px;font-weight:600;color:#fff;'
            f'letter-spacing:0.3px;border-radius:4px;margin:0 1px">'
            f'{label_pt}</div>'
        )
    return (
        f'<div>'
        f'<div style="display:flex;align-items:stretch;height:46px">{segs}</div>'
        f'<div style="margin-top:6px;font-size:11px;color:var(--muted);text-align:right">'
        f'RoR z-score: <b style="color:var(--text)">{z:+.2f}</b></div>'
        f'</div>'
    )


def _quad_grid(snap: dict[str, Any]) -> str:
    """2×2 quadrant grid with per-duration markers placed in their cells.

    Layout (Hedgeye-style):
                    Inflation ↓        Inflation ↑
      Growth ↑      Q1 Goldilocks      Q2 Reflation
      Growth ↓      Q4 Deflation       Q3 Stagflation
    """
    by_dur = snap.get("by_duration", {})
    master = snap.get("regime", "")
    scores = snap.get("scores", {})

    def cell(q: str) -> str:
        bg = _QUAD_COLORS[q]
        name = _QUAD_LABEL[q]
        score = scores.get(q, 0.0)
        # Markers for any duration whose regime == this q
        dur_marks = ""
        for d in ("Trade", "Trend", "Tail"):
            if by_dur.get(d) == q:
                dur_marks += (
                    f'<span style="display:inline-block;background:rgba(255,255,255,0.92);'
                    f'color:{bg};padding:2px 7px;border-radius:8px;font-size:10px;'
                    f'font-weight:700;margin:2px">{d}</span>'
                )
        master_mark = ""
        if master == q:
            master_mark = (
                f'<div style="position:absolute;top:6px;right:8px;background:#fff;color:{bg};'
                f'font-size:9px;font-weight:800;padding:2px 6px;border-radius:8px;letter-spacing:0.3px">'
                f'★ MASTER</div>'
            )
        empty_mark = '<span style="opacity:0.5;font-size:10px">—</span>'
        body_marks = dur_marks if dur_marks else empty_mark
        return (
            f'<div style="position:relative;background:{bg};color:#fff;padding:14px 12px;'
            f'border-radius:8px;min-height:110px">'
            f'{master_mark}'
            f'<div style="font-size:18px;font-weight:800;letter-spacing:0.5px">{q}</div>'
            f'<div style="font-size:11px;opacity:0.9;margin-bottom:8px">{name}</div>'
            f'<div style="font-size:10px;opacity:0.85;margin-bottom:6px">votos: {score:+.1f}</div>'
            f'<div>{body_marks}</div>'
            f'</div>'
        )

    return (
        f'<div style="display:grid;grid-template-columns:90px 1fr 1fr;grid-template-rows:auto auto auto;'
        f'gap:8px;align-items:stretch">'
        # Header row
        f'<div></div>'
        f'<div style="text-align:center;font-size:11px;color:var(--muted);font-weight:600">Inflação ↓</div>'
        f'<div style="text-align:center;font-size:11px;color:var(--muted);font-weight:600">Inflação ↑</div>'
        # Row 1: Growth ↑
        f'<div style="display:flex;align-items:center;justify-content:flex-end;font-size:11px;color:var(--muted);font-weight:600">Crescimento ↑</div>'
        f'{cell("Q1")}{cell("Q2")}'
        # Row 2: Growth ↓
        f'<div style="display:flex;align-items:center;justify-content:flex-end;font-size:11px;color:var(--muted);font-weight:600">Crescimento ↓</div>'
        f'{cell("Q4")}{cell("Q3")}'
        f'</div>'
    )


def _ror_sparkline(history: list[dict]) -> str:
    """Inline SVG sparkline of RoR z over the recent window with band lines at ±0.5 / ±1.5."""
    if not history or len(history) < 2:
        return ""
    vals = [pt["z"] for pt in history]
    n = len(vals)
    # Fixed y-range so the band lines anchor consistently
    y_min, y_max = -2.5, 2.5
    w, h = 360, 64
    pad_l, pad_r, pad_t, pad_b = 6, 6, 4, 4
    inner_w = w - pad_l - pad_r
    inner_h = h - pad_t - pad_b

    def x(i): return pad_l + (i / max(n - 1, 1)) * inner_w
    def y(v): return pad_t + (1 - (v - y_min) / (y_max - y_min)) * inner_h

    points = " ".join(f"{x(i):.1f},{y(v):.2f}" for i, v in enumerate(vals))
    last_v = vals[-1]
    last_color = _RISK_COLORS.get(
        "EXTREME_FEAR" if last_v < -1.5 else "FEAR" if last_v < -0.5 else "NEUTRAL"
        if last_v <= 0.5 else "GREED" if last_v <= 1.5 else "EXTREME_GREED",
        "#7f8c8d",
    )

    bands = ""
    for v, c in [(-1.5, "#9b1c2c"), (-0.5, "#d35400"), (0.5, "#27ae60"), (1.5, "#0e7d4f")]:
        bands += f'<line x1="{pad_l}" x2="{w - pad_r}" y1="{y(v):.2f}" y2="{y(v):.2f}" stroke="{c}" stroke-width="1" stroke-dasharray="2,3" opacity="0.4"/>'
    zero = f'<line x1="{pad_l}" x2="{w - pad_r}" y1="{y(0):.2f}" y2="{y(0):.2f}" stroke="rgba(255,255,255,0.3)" stroke-width="1"/>'

    return (
        f'<svg viewBox="0 0 {w} {h}" style="width:100%;max-width:{w}px;height:{h}px">'
        f'{bands}{zero}'
        f'<polyline points="{points}" fill="none" stroke="{last_color}" stroke-width="1.6"/>'
        f'<circle cx="{x(n-1):.1f}" cy="{y(last_v):.2f}" r="3" fill="{last_color}"/>'
        f'</svg>'
    )


def build_market_states_section(snap: dict[str, Any]) -> str:
    """Render the full 'Estados' sub-tab content for the Market section."""
    if not snap:
        return ""

    risk = snap.get("risk_state", "NEUTRAL")
    z    = float(snap.get("ror_z", 0.0))
    regime = snap.get("regime", "Q?")
    conf   = float(snap.get("confidence", 0.0))
    by_dur = snap.get("by_duration", {})
    durations_d = snap.get("duration_days", {})
    confluence = int(snap.get("confluence", 0))
    asof = snap.get("asof", "")
    stale_days = int(snap.get("stale_days", 0))

    stale_note = ""
    if stale_days >= 2:
        stale_note = (
            f'<span style="color:#d35400;font-size:11px;margin-left:8px">'
            f'⚠ dados de {stale_days}d atrás (batch diário externo)</span>'
        )

    # Duration legend strip
    dur_legend = "".join(
        f'<div style="display:flex;align-items:center;gap:6px">'
        f'<span style="color:var(--muted);font-size:11px;min-width:55px">{d} ({durations_d.get(d, "?")}d)</span>'
        f'{_quad_pill(by_dur.get(d, "?"))}'
        f'</div>'
        for d in ("Trade", "Trend", "Tail")
    )

    return f"""
    <div style="padding:10px 0">
      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px;margin-bottom:14px">
        <span style="font-size:13px;font-weight:700;color:var(--text)">Risk State (RoR Composite)</span>
        <span style="color:var(--muted);font-size:11px">CNN Fear & Greed-style · 12 inputs · 252d z-score</span>
        {stale_note}
      </div>
      <div style="margin-bottom:18px">
        {_risk_gauge(risk, z)}
        <div style="margin-top:8px">{_ror_sparkline(snap.get("ror_history", []))}</div>
        <div style="font-size:11px;color:var(--muted);margin-top:4px">
          Linhas tracejadas: ±0.5 (Neutro/Medo/Ganância) e ±1.5 (Extremos). Últimos 63 dias.
        </div>
      </div>

      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px;margin:18px 0 10px 0">
        <span style="font-size:13px;font-weight:700;color:var(--text)">Quadrante de Mercado (Growth × Inflation)</span>
        <span style="color:var(--muted);font-size:11px">VAMS engine · master regime + per-duration nesting</span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin-bottom:10px">
        <span style="color:var(--muted);font-size:11px">Master:</span>
        {_quad_pill(regime, big=True, title=f"Confiança {conf*100:.0f}%")}
        <span style="color:var(--muted);font-size:11px">·  conf {conf*100:.0f}%  ·  confluência {confluence}/3</span>
      </div>
      {_quad_grid(snap)}
      <div style="display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin-top:12px">
        {dur_legend}
      </div>
      <div style="font-size:10px;color:var(--muted);margin-top:10px;line-height:1.4">
        Calculado por projeto separado <code>Markets states/regime_panel</code> (rotina diária externa).
        Kit consome <code>artifacts_glpg.pkl</code> read-only — se ausente ou &gt;7d defasado, a seção é ocultada.
      </div>
    </div>"""

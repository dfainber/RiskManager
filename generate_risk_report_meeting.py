"""
generate_risk_report_meeting.py — Versão "Reunião" do Morning Call.

Versão exclusiva pra exibição na telona da sala do Morning Call. Não altera
`generate_risk_report.py` — pós-processa o HTML do report dark (lê de
`data/morning-calls/{DATE}_risk_monitor.html`, ou roda o gerador se faltando):

  • Paleta light brand-aligned (#183B80 navy + #2AADF5 azul, fundo #f5f7fa)
  • Tipografia ampliada (always-on) pra leitura à distância
  • Headers / cards / tabelas no padrão "relatório de gestora"

Output:
  data/morning-calls-meeting/{DATE}_risk_monitor_meeting.html
  F:\\Bloomberg\\Risk_Manager\\Data\\Morningcall\\ultimo_risk_monitor_meeting.html

Uso:
  python generate_risk_report_meeting.py                 # latest bday no DB
  python generate_risk_report_meeting.py 2026-04-29
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT        = Path(__file__).parent
DARK_DIR    = ROOT / "data" / "morning-calls"
MEETING_DIR = ROOT / "data" / "morning-calls-meeting"
MIRROR      = Path(r"F:\Bloomberg\Risk_Manager\Data\Morningcall")


# ─────────────────────────────────────────────────────────────────────────────
# Light theme overrides — replace the dark :root block + selected hardcoded
# colors. Brand palette (#183B80 navy + #2AADF5 blue) + white cards on
# subtle blue-gray background. Bumped font sizes for big-screen viewing.
# ─────────────────────────────────────────────────────────────────────────────

_LIGHT_ROOT = """
  :root {
    --bg:            #f5f7fa;
    --bg-2:          #ffffff;
    --panel:         #ffffff;
    --panel-2:       #f0f3f8;
    --line:          #d6dde7;
    --line-2:        #b8c2d0;
    --text:          #0c1e3e;
    --muted:         #4a5a72;
    --muted-strong:  #1f2d44;
    --muted-soft:    #6b7a8e;
    --accent:        #183B80;
    --accent-2:      #2AADF5;
    --accent-deep:   #0c2048;
    --up:            #1E8C45;
    --down:          #C0392B;
    --warn:          #E67E22;
  }
"""

# Extra screen-only rules to make it pop on the big screen and read like a
# bank deck. Appended to the existing <style> block.
_MEETING_OVERRIDES = """
  /* ── meeting theme overrides (post-processed, always-on) ─────────────── */
  html, body { font-size: 17px; }
  body {
    background:
      radial-gradient(1100px 520px at 8% -8%, rgba(42,173,245,0.08), transparent 60%),
      radial-gradient(900px 480px at 105% 5%, rgba(24,59,128,0.06),  transparent 60%),
      var(--bg) !important;
    color: var(--text);
  }
  header {
    background: rgba(255,255,255,0.92) !important;
    border-bottom: 1px solid var(--line) !important;
    box-shadow: 0 1px 6px rgba(12,30,80,0.06);
  }
  /* Logo: o novo Risk Monitor é um SVG inline com gradient próprio — não
     precisa de filtro pra contraste. Wordmark em navy. */
  header .brand .rm-logo { width: 52px !important; height: 52px !important; }
  header .brand .rm-wordmark {
    color: var(--accent) !important;
    font-size: 24px !important;
    font-weight: 700 !important;
  }
  /* Legacy <img> path (caso o report dark ainda tenha PNG): manter filtro. */
  header .brand img {
    filter: brightness(0) saturate(100%) invert(13%) sepia(60%) saturate(2200%) hue-rotate(213deg) contrast(0.95) !important;
    height: 30px !important; width: auto !important; max-width: 180px !important;
  }

  /* "Powered by Galápagos" footer — recolor PNG para navy no fundo branco */
  .powered-by {
    border-top: 1px solid var(--line) !important;
    color: var(--muted-soft) !important;
  }
  .powered-by img {
    filter: brightness(0) saturate(100%) invert(13%) sepia(60%) saturate(2200%) hue-rotate(213deg) contrast(0.95) !important;
    opacity: 1 !important;
    height: 24px !important;
  }
  .subtitle { color: var(--muted-strong); font-size: 12px; font-weight: 500; }

  /* Tabs / nav: navy chip bar on white */
  .tabs, .sub-tabs, .mode-switcher, .ctrl-group {
    background: #ffffff !important;
    border: 1px solid var(--line) !important;
    box-shadow: 0 1px 3px rgba(12,30,80,0.05);
  }
  .tab, .mode-tab { color: var(--muted-strong); font-size: 15px; padding: 9px 16px; }
  .tab:hover, .mode-tab:hover { color: var(--accent); background: rgba(24,59,128,0.05); }
  .tab.active, .mode-tab.active {
    color: #ffffff !important;
    background: linear-gradient(180deg, var(--accent), var(--accent-deep)) !important;
    box-shadow: 0 1px 4px rgba(12,30,80,0.18);
  }
  .navrow-label { color: var(--accent); font-weight: 700; font-size: 11px; }

  /* Jump bar */
  #report-jump-bar { border-top: 1px solid var(--line); }
  .jump-btn {
    color: var(--muted-strong); border: 1px solid var(--line);
    background: #ffffff;
  }
  .jump-btn:hover { color: var(--accent); border-color: var(--accent-2); }
  .jump-btn.active-jump {
    color: #ffffff; background: var(--accent); border-color: var(--accent);
  }

  /* Cards: white with subtle shadow + navy top border */
  .card, section.card {
    background: #ffffff !important;
    border: 1px solid var(--line) !important;
    border-top: 3px solid var(--accent) !important;
    box-shadow: 0 1px 6px rgba(12,30,80,0.05) !important;
  }
  .card-title {
    color: var(--accent-deep) !important;
    font-weight: 700;
    letter-spacing: .04em;
    font-size: 19px;
  }
  .card-sub { color: var(--muted); font-size: 14px; }

  /* Tables: navy headers, alternating rows, hover */
  table th {
    background: var(--accent) !important;
    color: #ffffff !important;
    font-weight: 600;
    letter-spacing: .04em;
    border-bottom: 0 !important;
  }
  table th:hover { background: var(--accent-deep) !important; }
  table td { border-bottom: 1px solid #eef2f8 !important; }
  table tbody tr:nth-child(even) td { background: #fafbfd; }
  table tbody tr:hover td { background: #eef4fc !important; }

  /* Buttons */
  .btn-primary {
    background: linear-gradient(180deg, var(--accent-2), var(--accent)) !important;
    border: 1px solid var(--accent) !important;
    color: #ffffff !important;
    box-shadow: 0 1px 3px rgba(12,30,80,0.15);
  }
  .btn-primary:hover { filter: brightness(1.06); }
  .btn-accent {
    background: rgba(24,59,128,0.08) !important;
    color: var(--accent) !important;
    border: 1px solid var(--accent) !important;
    font-weight: 600;
  }
  .btn-accent:hover { background: var(--accent) !important; color: #ffffff !important; }
  .btn-pdf {
    background: #ffffff !important;
    border: 1px solid var(--line-2) !important;
    color: var(--muted-strong) !important;
  }
  .btn-pdf:hover { color: var(--accent); border-color: var(--accent); background: rgba(24,59,128,0.04) !important; }

  /* Date input: light color-scheme */
  .ctrl-group input[type=date] {
    color: var(--text) !important;
    color-scheme: light !important;
  }

  /* Pa toggle pills */
  .pa-tgl, .pa-tgl.active {
    border: 1px solid var(--line);
    background: #ffffff;
    color: var(--muted-strong);
  }
  .pa-tgl.active {
    background: var(--accent) !important;
    color: #ffffff !important;
    border-color: var(--accent) !important;
  }

  /* Modals */
  .modal-overlay, .vardod-overlay, .pmovers-overlay {
    background: rgba(12,30,80,0.45) !important;
  }
  .modal, .vardod-modal, .pmovers-modal {
    background: #ffffff !important;
    border: 1px solid var(--line) !important;
    box-shadow: 0 8px 32px rgba(12,30,80,0.25) !important;
    color: var(--text) !important;
  }

  /* Warning / alert banners — replace dark warning palette w/ light invest-bank look */
  [style*="background:#7c2d12"] { background: #FADBD8 !important; }
  [style*="color:#fca5a5"]      { color: #C0392B !important; }
  [style*="background:#1e293b"] { background: #e9eef7 !important; }
  [style*="background:#0f172a"] { background: #ffffff !important; }
  [style*="background:#111827"] { background: #ffffff !important; }

  /* Big screen: thicker bar dividers / strip lines */
  .summary-table tr.summary-sep td { border-top: 2px solid var(--line) !important; }

  /* Footer print badge / subtle elements */
  .date-hint { color: var(--muted-soft); font-size: 11px; }

  /* ── Tabelas: fontes ampliadas pra leitura na telona ──────────────────── */
  .summary-table { font-size: 16px !important; }
  .summary-table th { font-size: 13px !important; padding: 12px 14px !important; color: #ffffff !important; }
  .summary-table td { padding: 12px 14px !important; }
  .summary-table td.sum-fund { font-size: 17px !important; font-weight: 700 !important; }
  .summary-table td.sum-status { font-size: 22px !important; }

  .summary-movers { font-size: 15px !important; }
  .summary-movers th { font-size: 13px !important; padding: 12px 14px !important; color: #ffffff !important; }
  .summary-movers td { padding: 11px 14px !important; }
  .summary-movers td.sum-movers { font-size: 15px !important; }

  /* Inside narrower "comment-fund" grid cards (Mudanças Significativas):
     compact the table so 4-5 columns fit without horizontal clipping. */
  .comment-fund { overflow-x: auto !important; padding: 14px !important; }
  .comment-fund .summary-movers { font-size: 13px !important; }
  .comment-fund .summary-movers th {
    font-size: 11px !important; padding: 8px 8px !important;
    letter-spacing: .02em !important;
  }
  .comment-fund .summary-movers td { padding: 7px 8px !important; }
  .comment-fund .comment-title {
    font-size: 13px !important; font-weight: 700 !important;
    color: var(--accent-deep) !important; letter-spacing: .06em !important;
    margin-bottom: 8px;
  }

  .pa-table { font-size: 15px !important; }
  .pa-table th { font-size: 13px !important; padding: 11px 12px !important; color: #ffffff !important; }
  .pa-table td.pa-name, .pa-table td.t-num { padding: 9px 12px !important; }
  .pa-table tbody tr.pa-l2 { font-size: 14.5px !important; }
  .pa-table tbody tr.pa-l2 td.pa-name { color: var(--muted-strong) !important; }

  table th { font-size: 13px !important; padding: 10px 13px !important; }
  table td { padding: 9px 13px !important; }
  .mono { font-size: 14px !important; }

  /* Pills / toggles */
  .pa-tgl, .pa-btn, .toggle-btn { font-size: 13.5px !important; padding: 7px 14px !important; }
  .jump-btn { font-size: 13px !important; padding: 6px 14px !important; }
  .pa-search { font-size: 13.5px !important; padding: 6px 12px !important; }

  /* Markets tab */
  .mkt-tbl { font-size: 14px !important; }
  .mkt-tbl th { font-size: 12px !important; }
  .mkt-tbl td { padding: 8px 11px !important; }
  .mkt-panel-hdr { font-size: 13px !important; padding: 10px 14px !important; }

  /* Briefing / comments / alerts */
  .comment-list li { font-size: 15px !important; line-height: 1.6 !important; }
  .comment-fund   { font-size: 15px !important; }
  .alert-item     { font-size: 15px !important; }

  /* Stop monitor / Risk Budget */
  .stop-table th, .stop-table td { font-size: 14.5px !important; padding: 10px 12px !important; }

  /* Wide tables (Frontier 14-col, Vol Regime, Distribuição 9-col, Markets):
     comprimir padding pra caber sem horizontal scroll ou clip. */
  .metric-table, .dist-table { font-size: 13px !important; }
  .metric-table th, .dist-table th { font-size: 11px !important; padding: 8px 8px !important; color: #ffffff !important; letter-spacing: .03em; }
  .metric-table td, .dist-table td { padding: 7px 8px !important; }
  .mkt-full-tbl { font-size: 13px !important; }
  .mkt-full-tbl th { font-size: 11px !important; padding: 8px 9px !important; color: #ffffff !important; }
  .mkt-full-tbl td { padding: 7px 9px !important; }

  /* Cards: defensive horizontal scroll quando tabela ainda é mais larga
     que o card (preferível a clipping silencioso). */
  .card, section.card { overflow-x: auto !important; }

  /* Stats bar inline (Frontier "NAV / Gross / Beta / Attrib / ER ..."): fonte
     pequena demais e cramada. Reagrupa com gap maior e fonte legível. */
  .sn-inline-stats {
    font-size: 14px !important;
    color: var(--muted-strong) !important;
    display: flex !important; flex-wrap: wrap !important;
    gap: 6px 22px !important;
    padding: 10px 4px !important;
    border-bottom: 1px solid var(--line) !important;
    margin-bottom: 14px !important;
  }
  .sn-stat { display: inline-flex !important; align-items: baseline; gap: 6px; }
  .sn-lbl  { color: var(--muted) !important; font-size: 11.5px !important;
             text-transform: uppercase; letter-spacing: .06em; font-weight: 600; }
  .sn-val  { color: var(--text) !important; font-weight: 700 !important;
             font-size: 14px !important; }

  /* Modal overlays — substituir overlays muito escuros por navy semi-transparente */
  [style*="background:rgba(0,0,0,0.72)"]    { background: rgba(12,30,80,0.55) !important; }
  [style*="background:rgba(0,0,0,0.55)"]    { background: rgba(12,30,80,0.45) !important; }
  [style*="background:rgba(15,23,42,0.96)"] { background: rgba(255,255,255,0.98) !important; color: var(--text) !important; }
  [style*="background:rgba(11,13,16,.78)"]  { background: rgba(255,255,255,0.92) !important; }
  [style*="background:rgba(17,20,26,.92)"]  { background: rgba(255,255,255,0.96) !important; color: var(--text) !important; }

  /* Esconder controles secundários (foco em números na sala de reunião) */
  .navrow-label,
  .ctrl-group label,
  .date-hint { display: none !important; }
  header .controls { gap: 8px !important; }
  .ctrl-group { padding: 5px 10px !important; }

  /* Bordas mais grossas pros cards (visibilidade à distância) */
  .card, section.card {
    border-top-width: 4px !important;
  }
  table tbody tr:hover td { background: #dfeafa !important; }
"""


# ─────────────────────────────────────────────────────────────────────────────
# Hardcoded dark hex codes used in inline `style="background:#XXX"` across the
# daily renderers. CSS variables alone don't reach these — substitute them
# with light equivalents at the HTML string level. Only `background:#XXX`
# pattern is targeted (semantic colors like #22c55e for positive, #f87171
# for borders, etc are preserved).
# ─────────────────────────────────────────────────────────────────────────────
_BG_HEX_MAP = {
    "#0f2d1a": "#e8f6ed",   # Análise tile bg (positive)
    "#2d0f0f": "#fae9ec",   # Análise tile bg (negative)
    "#1e293b": "#e1e7ef",   # progress bar / stop bar track
    "#0a0f18": "#f5f7fa",   # ALBATROZ expand row bg
    "#0f172a": "#dde3ed",   # border / panel
    "#1e3a5f": "#dde9f8",   # button bg in expo
    "#0b0d10": "#f5f7fa",
    "#111418": "#ffffff",
    "#14181d": "#ffffff",
    "#181d24": "#f8fafc",
    "#232a33": "#d6dde7",
    "#2d3540": "#b8c2d0",
    "#0b1220": "#f5f7fa",
    "#0f1422": "#f0f3f8",
    "#1e1e2e": "#f0f3f8",
    "#2d2d3d": "#dde3ed",
    "#14241a": "#e8f6ed",   # gain-side tint (stop bar)
    "#1e4976": "#a8c4e8",   # blue budget-available (stop bar) — visible on white
    "#1c1409": "#f5f3eb",   # warning bg
    "#7c2d12": "#FADBD8",   # warn bg (already in CSS overrides but covers inline)
    "#fca5a5": "#C0392B",   # warn text
    "#1a2030": "#eef2f8",   # PA FX-split tree bg
    "#0d1626": "#ffffff",   # PA FX-split table bg
    "#15203a": "#eef4fc",   # row hover
    "#1f2940": "#d6dde7",   # border
    "#2a3550": "#c8d0dc",
    "#cfd6e0": "#1f2d44",   # secondary text on dark
    "#e6e6e6": "#0c1e3e",   # body text on dark
    "#0a0f1a": "#f5f7fa",   # FX-split body bg
    "#11141a": "#ffffff",
    "#1a1f26": "#ffffff",
}


def _apply_bg_swap(html: str) -> str:
    """Replace dark hex codes wherever they're used as a CSS value or SVG fill.
    Limited blast — only matches `:#XXX`, `: #XXX`, `fill="#XXX"`, `fill='#XXX'`,
    `stroke="#XXX"` and `stroke='#XXX'`, never bare hex in text."""
    for dark, light in _BG_HEX_MAP.items():
        for pat, repl in (
            (f":{dark}",         f":{light}"),
            (f": {dark}",        f": {light}"),
            (f'fill="{dark}"',   f'fill="{light}"'),
            (f"fill='{dark}'",   f"fill='{light}'"),
            (f'stroke="{dark}"', f'stroke="{light}"'),
            (f"stroke='{dark}'", f"stroke='{light}'"),
        ):
            html = html.replace(pat, repl)
    return html


# ─────────────────────────────────────────────────────────────────────────────
# Light text colors that have poor contrast on white. Darken to improve
# readability on the meeting big screen. Targets `color:#XXX` (CSS) and
# `fill="#XXX"` (SVG text).
# ─────────────────────────────────────────────────────────────────────────────
_TEXT_COLOR_MAP = {
    "#94a3b8": "#475569",  # slate-400 → slate-600 (most common light text)
    "#cbd5e1": "#475569",  # slate-300
    "#e2e8f0": "#475569",  # slate-200
    "#facc15": "#a16207",  # yellow-400 → yellow-700 (soft mark, etc)
    "#fbbf24": "#a16207",  # amber-400
    "#eab308": "#854d0e",  # yellow-500
    "#fb923c": "#c2410c",  # orange-400 → orange-700 (GANCHO label)
    "#60a5fa": "#1d4ed8",  # blue-400 → blue-700 (base-63 marker)
    "#64748b": "#334155",  # slate-500 → slate-700
    "#f87171": "#dc2626",  # red-400 → red-600 (improve negative-text contrast)
    "#888888": "#555555",  # gray text
    "#aaaaaa": "#666666",
    "#bbbbbb": "#666666",
    "#777":    "#555",     # short hex muted gray
    "#777777": "#555555",
    # Multi-line chart palette (VaR Histórico — Fund + PMs) and accent strokes.
    # Original mid-saturation hues read fine on dark but soften on white;
    # darken to ~5:1+ contrast for the meeting big screen.
    "#1a8fd1": "#0e5a8a",  # BVaR sparkline blue / LF line
    "#5aa3e8": "#1d4ed8",  # CI line (brand blue)
    "#22d3ee": "#0e7490",  # RJ cyan
    "#a78bfa": "#6d28d9",  # JD purple
    "#f472b6": "#be185d",  # Stress sparkline pink
}


def _apply_text_color_swap(html: str) -> str:
    """Darken low-contrast text colors on white. Targets `color:#XXX` plus
    `fill="#XXX"` and `fill='#XXX'` (SVG text uses fill for color)."""
    for src, dst in _TEXT_COLOR_MAP.items():
        for pat, repl in (
            (f"color:{src}",     f"color:{dst}"),
            (f"color: {src}",    f"color: {dst}"),
            (f'fill="{src}"',    f'fill="{dst}"'),
            (f"fill='{src}'",    f"fill='{dst}'"),
            (f'stroke="{src}"',  f'stroke="{dst}"'),
            (f"stroke='{src}'",  f"stroke='{dst}'"),
        ):
            html = html.replace(pat, repl)
    return html


# ─────────────────────────────────────────────────────────────────────────────
# Inline font-size bumps. Renderers have many inline `font-size:NNpx` (especially
# in expo / pa / vardod / pmovers). These don't pick up CSS class rules — so
# replace them at the string level. Conservative bump (~+1.5px) to keep
# layouts intact while improving big-screen readability.
# ─────────────────────────────────────────────────────────────────────────────
_FONT_SIZE_MAP = {
    "font-size:9px":     "font-size:10px",     # footnote tier — minimal bump
    "font-size:9.5px":   "font-size:10.5px",
    "font-size:10px":    "font-size:11.5px",
    "font-size:10.5px":  "font-size:12px",
    "font-size:11px":    "font-size:12.5px",
    "font-size:11.5px":  "font-size:13px",
    "font-size:12px":    "font-size:13.5px",
    "font-size:12.5px":  "font-size:14px",
}


def _apply_font_bump(html: str) -> str:
    """Bump inline font-sizes (replacing larger first to avoid double-replace)."""
    for src in sorted(_FONT_SIZE_MAP.keys(),
                      key=lambda s: -float(s.split(":")[1].rstrip("px"))):
        html = html.replace(src, _FONT_SIZE_MAP[src])
    return html


# Replace the FIRST `:root { ... }` block (the screen one).
# Don't touch the @media print block (already light, harmless).
_ROOT_RE = re.compile(r":root\s*\{[^}]*?\}", re.DOTALL)


def transform_to_meeting(html: str) -> str:
    """Apply the meeting-theme post-processing to a rendered dark report HTML."""
    new_html, n = _ROOT_RE.subn(_LIGHT_ROOT.strip(), html, count=1)
    if n == 0:
        raise RuntimeError("Meeting theme: did not find :root block in dark HTML")
    # Swap hardcoded dark backgrounds (CSS values + SVG fill/stroke) for light
    new_html = _apply_bg_swap(new_html)
    # Darken low-contrast text colors that don't read well on white
    new_html = _apply_text_color_swap(new_html)
    # Bump inline font-sizes for big-screen readability
    new_html = _apply_font_bump(new_html)
    # Append our CSS overrides at the end of the main <style> block
    new_html = new_html.replace("</style>", _MEETING_OVERRIDES + "\n</style>", 1)
    return new_html


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_date(arg: str | None) -> str:
    """Return YYYY-MM-DD. If arg given, validate; else call latest_bday.py."""
    if arg:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", arg):
            sys.exit(f"Formato inválido: '{arg}'. Use YYYY-MM-DD.")
        return arg
    res = subprocess.run(
        [sys.executable, str(ROOT / "latest_bday.py")],
        capture_output=True, text=True, cwd=ROOT,
    )
    if res.returncode != 0 or not res.stdout.strip():
        sys.exit("latest_bday.py falhou e nenhuma data foi passada.")
    return res.stdout.strip().splitlines()[-1].strip()


def _ensure_dark_report(date_str: str) -> Path:
    src = DARK_DIR / f"{date_str}_risk_monitor.html"
    if src.exists():
        return src
    print(f"Dark report ausente — gerando {date_str}...")
    res = subprocess.run(
        [sys.executable, str(ROOT / "generate_risk_report.py"), date_str],
        cwd=ROOT,
    )
    if res.returncode != 0 or not src.exists():
        sys.exit(f"Falha ao gerar dark report pra {date_str}.")
    return src


def main() -> Path:
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    date_str = _resolve_date(arg)
    print(f">> Meeting theme - data {date_str}")

    src = _ensure_dark_report(date_str)
    print(f"   Source: {src}")

    html = src.read_text(encoding="utf-8")
    html_meeting = transform_to_meeting(html)

    # Single "ultimo" output — sem arquivo datado, sempre sobrescreve.
    # Nome: ultimo_morning_call_meeting.html (alinhado com a entrega
    # consumida na telona da sala de reunião).
    fname = "ultimo_morning_call_meeting.html"

    MEETING_DIR.mkdir(parents=True, exist_ok=True)
    out = MEETING_DIR / fname
    out.write_text(html_meeting, encoding="utf-8")
    print(f"   -> {out}")

    try:
        MIRROR.mkdir(parents=True, exist_ok=True)
        mirror = MIRROR / fname
        mirror.write_text(html_meeting, encoding="utf-8")
        print(f"   -> mirror: {mirror}")
    except Exception as exc:
        print(f"   ! mirror falhou: {exc}")

    print(f"\n[OK] Meeting report {date_str} pronto\n")
    return out


if __name__ == "__main__":
    main()

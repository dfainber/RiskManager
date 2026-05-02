"""
html_assets.py — large inline CSS/JS blobs used by the generated HTML reports.

Moved out of generate_risk_report.py to keep the orchestrator focused on
Python orchestration instead of kilobyte-long asset strings. Consumers just
import the blob they need and interpolate it into their section's HTML.
"""

# ── Unified exposure table: collapse/expand + sort ────────────────────────────
# Used by: build_quant_exposure_section, build_evolution_exposure_section,
#          build_exposure_section (MACRO).
# Default sort: |Net| desc. Headers clickable (↑/↓). Drill children collapse
# with parent. Idempotent via window.__uexpoJSLoaded guard.
UEXPO_JS = r"""<script>
if (!window.__uexpoJSLoaded) {
  window.__uexpoJSLoaded = true;
  window.__uexpoSortState = {};

  window.uexpoToggle = function(row) {
    var path  = row.getAttribute('data-expo-path');
    var caret = row.querySelector('.uexpo-caret');
    var table = row.closest('table');
    if (!table || !path) return;
    var sel   = 'tr[data-expo-parent="' + path.replace(/"/g,'\\"') + '"]';
    var kids  = table.querySelectorAll(sel);
    if (!kids.length) return;
    var opening = kids[0].style.display === 'none';
    kids.forEach(function(c){ c.style.display = opening ? '' : 'none'; });
    if (caret) caret.textContent = opening ? '▼' : '▶';
  };

  window.uexpoSort = function(th, sortKey) {
    var table = th.closest('table'); if (!table) return;
    var tbody = table.tBodies[0];   if (!tbody) return;
    var key   = (table.id || '') + ':' + sortKey;

    // The name column cycles through 3 states: A→Z, Z→A, default.
    // Numeric columns toggle asc/desc.
    var prev = window.__uexpoSortState[key];
    var state;
    if (sortKey === 'name') {
      state = (prev === 'asc') ? 'desc' : (prev === 'desc') ? 'default' : 'asc';
    } else {
      state = (prev === 'asc') ? 'desc' : 'asc';
    }
    window.__uexpoSortState = {}; window.__uexpoSortState[key] = state;

    // Group factor-level rows with their children (level=1 rows follow their parent).
    var groups = [];
    var cur = null;
    Array.from(tbody.rows).forEach(function(r){
      if (r.getAttribute('data-expo-lvl') === '0' || r.classList.contains('pa-total-row')) {
        cur = { head: r, kids: [], total: r.classList.contains('pa-total-row') };
        groups.push(cur);
      } else if (cur) {
        cur.kids.push(r);
      }
    });
    var totals = groups.filter(function(g){ return g.total; });
    var sortable = groups.filter(function(g){ return !g.total; });

    if (state === 'default') {
      sortable.sort(function(a, b){
        var va = parseInt(a.head.getAttribute('data-default-order'), 10);
        var vb = parseInt(b.head.getAttribute('data-default-order'), 10);
        return va - vb;
      });
    } else {
      var asc = (state === 'asc');
      sortable.sort(function(a, b){
        if (sortKey === 'name') {
          var na = a.head.getAttribute('data-sort-name') || '';
          var nb = b.head.getAttribute('data-sort-name') || '';
          return asc ? na.localeCompare(nb) : nb.localeCompare(na);
        }
        var va = parseFloat(a.head.getAttribute('data-sort-' + sortKey));
        var vb = parseFloat(b.head.getAttribute('data-sort-' + sortKey));
        var aNaN = isNaN(va), bNaN = isNaN(vb);
        if (aNaN && bNaN) {
          var na2 = a.head.getAttribute('data-sort-name') || '';
          var nb2 = b.head.getAttribute('data-sort-name') || '';
          return asc ? na2.localeCompare(nb2) : nb2.localeCompare(na2);
        }
        if (aNaN) return 1;
        if (bNaN) return -1;
        return asc ? va - vb : vb - va;
      });
    }

    sortable.forEach(function(g){
      tbody.appendChild(g.head);
      g.kids.forEach(function(k){ tbody.appendChild(k); });
    });
    totals.forEach(function(g){
      tbody.appendChild(g.head);
      g.kids.forEach(function(k){ tbody.appendChild(k); });
    });

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


# ── Main report CSS (extracted from generate_risk_report.py 1959-2786) ────
# Static stylesheet — no Python interpolations. Interpolated as {MAIN_CSS}
# inside the build_html f-string between literal <style>...</style> tags.
MAIN_CSS = """
  :root {
    --bg:#0b0d10;
    --bg-2:#111418;
    --panel:#14181d;
    --panel-2:#181d24;
    --line:#232a33;
    --line-2:#2d3540;
    --text:#e7ecf2;
    --muted:#a8b3c2;
    --muted-strong:#c9d1dd;
    --muted-soft:#6b7480;
    --accent:#0071BB;
    --accent-2:#1a8fd1;
    --accent-deep:#183C80;
    --up:#26d07c;
    --down:#ff5a6a;
    --warn:#f5c451;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body {
    background: var(--bg); color: var(--text);
    font-family: 'Inter', system-ui, sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  h1, h2, .card-title, .modal-title {
    font-family: 'Gadugi', 'Inter', system-ui, sans-serif;
  }
  body {
    min-height:100vh;
    background:
      radial-gradient(1200px 600px at 10% -10%, rgba(0,113,187,.06), transparent 60%),
      radial-gradient(900px 500px at 110% 10%, rgba(26,143,209,.04), transparent 60%),
      var(--bg);
  }
  .mono { font-family:'JetBrains Mono', ui-monospace, monospace; font-variant-numeric: tabular-nums; }

  /* Header */
  header {
    position:sticky; top:0; z-index:50;
    backdrop-filter:blur(14px);
    background:rgba(11,13,16,.78);
    border-bottom:1px solid var(--line);
  }
  .hwrap {
    max-width:1280px; margin:0 auto; padding:14px 22px;
    display:flex; align-items:center; gap:22px; flex-wrap:wrap;
  }
  .brand { display:flex; align-items:center; gap:12px; }
  .brand .rm-logo { width:48px; height:48px; flex:0 0 auto; }
  .brand .rm-wordmark {
    font-family:'Gadugi', 'Inter', system-ui, sans-serif;
    font-size:22px; font-weight:700; letter-spacing:.4px;
    color:var(--text); white-space:nowrap;
  }

  /* "Powered by Galápagos" footer */
  .powered-by {
    display:flex; justify-content:flex-end; align-items:center; gap:10px;
    padding:18px 22px 26px; max-width:1280px; margin:24px auto 0;
    border-top:1px solid var(--line);
    color:var(--muted-soft); font-size:11px; letter-spacing:.4px; text-transform:uppercase;
  }
  .powered-by img { height:22px; width:auto; opacity:.85; }

  .tabs, .sub-tabs {
    display:flex; gap:4px;
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:4px;
  }
  .tab, .mode-tab {
    padding:7px 14px; font-size:12.5px; font-weight:600;
    color:var(--muted); background:transparent; border:0; border-radius:9px;
    cursor:pointer; transition:all .18s ease; letter-spacing:.05em;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }
  .tab:hover, .mode-tab:hover { color:var(--text); }
  .tab.active, .mode-tab.active { color:#fff; background:linear-gradient(180deg, var(--accent-2), var(--accent)); }
  .mode-switcher {
    display:flex; gap:4px;
    background:var(--panel-2); border:1px solid var(--line);
    border-radius:12px; padding:4px;
  }
  .sub-tabs { display:none; }
  .sub-tabs.active { display:flex; }
  .navrow {
    max-width:1280px; margin:0 auto; padding:8px 22px;
    display:flex; align-items:center; gap:14px; flex-wrap:wrap;
  }
  .navrow-label {
    font-size:10px; color:var(--muted); letter-spacing:.18em; text-transform:uppercase;
  }
  /* Report jump bar — shown below fund tabs in Por Fundo mode */
  #report-jump-bar {
    display:none; gap:4px; flex-wrap:wrap; align-items:center;
    padding:6px 22px; max-width:1280px; margin:0 auto;
    border-top:1px solid rgba(255,255,255,0.04);
  }
  body[data-mode="fund"] #report-jump-bar { display:flex; }
  .jump-btn {
    padding:4px 11px; font-size:11px; font-weight:600;
    color:var(--muted); background:transparent; border:1px solid var(--line);
    border-radius:7px; cursor:pointer; transition:all .15s ease;
    font-family:'Gadugi','Inter',system-ui,sans-serif; letter-spacing:.03em;
  }
  .jump-btn:hover { color:var(--text); border-color:rgba(255,255,255,0.18); }
  .jump-btn.active-jump { color:var(--accent-2); border-color:var(--accent-2); }

  .controls { margin-left:auto; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .ctrl-group {
    display:flex; align-items:center; gap:8px;
    background:var(--panel); border:1px solid var(--line);
    border-radius:10px; padding:6px 10px;
  }
  .ctrl-group label {
    font-size:10px; color:var(--muted); letter-spacing:.14em; text-transform:uppercase;
  }
  .ctrl-group input[type=date] {
    background:transparent; border:0; color:var(--text);
    font-family:'JetBrains Mono', monospace; font-size:12px;
    color-scheme:dark; outline:none;
  }
  .btn-primary {
    padding:8px 14px; border-radius:10px; border:1px solid var(--accent);
    background:linear-gradient(180deg, var(--accent-2), var(--accent));
    color:#fff; font-weight:600; font-size:11.5px; cursor:pointer;
    letter-spacing:.05em; text-transform:uppercase;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }
  .btn-primary:hover { filter:brightness(1.08); }
  .btn-accent {
    padding:8px 14px; border-radius:10px; border:1px solid var(--accent);
    background:rgba(0,113,187,.12); color:var(--accent-2);
    font-weight:600; font-size:12px; cursor:pointer; letter-spacing:.04em;
    font-family:'Gadugi','Inter',system-ui,sans-serif;
  }
  .btn-accent:hover { background:rgba(0,113,187,.22); color:#fff; }
  .btn-pdf {
    padding:7px 12px; border-radius:9px; border:1px solid var(--line-2);
    background:var(--panel-2); color:var(--muted);
    font-weight:600; font-size:11px; cursor:pointer; letter-spacing:.04em;
    margin-left:6px;
  }
  .btn-pdf:hover { border-color:var(--accent); color:var(--text); background:rgba(0,113,187,.10); }

  /* Print / PDF export — remap CSS variables to a light palette so all
     inline `color:var(--up)` / backgrounds switch to paper-friendly tones. */
  @media print {
    :root {
      --bg:        #ffffff;
      --bg-2:      #ffffff;
      --panel:     #ffffff;
      --panel-2:   #f2f4f7;
      --line:      #d0d5dd;
      --line-2:    #b0b5c0;
      --text:      #111111;
      --muted:     #555555;
      --muted-strong: #333333;
      --muted-soft:   #888888;
      --accent:    #003d5c;
      --accent-2:  #004a70;
      --up:        #0e7a32;
      --down:      #a8001a;
      --warn:      #8a6500;
    }
    @page { size: A4 landscape; margin: 10mm 8mm; }
    html, body {
      background: #ffffff !important;
      color: #111111 !important;
      font-size: 11pt;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }
    .no-print, header .controls, header .mode-switcher,
    .navrow, .sub-tabs, #empty-state,
    .sn-switcher, .report-fund-switcher {
      display: none !important;
    }
    header {
      padding: 4px 0 6px; border-bottom: 1px solid #000; margin-bottom: 6px;
      background: #ffffff !important;
    }
    header .brand img { filter: brightness(0); }
    main { padding: 0 !important; max-width: none !important; margin: 0 !important; }

    .card, section.card {
      background: #ffffff !important;
      border: 1px solid #999 !important;
      box-shadow: none !important;
      page-break-inside: avoid;
      margin-bottom: 8px !important;
      padding: 8px 10px !important;
    }
    .card-head   { padding-bottom: 4px !important; margin-bottom: 6px !important; border-bottom: 1px solid #ddd !important; }
    .card-title  { color: #000 !important; font-size: 11pt !important; letter-spacing: .1em !important; font-weight: 700 !important; }
    .card-sub    { color: #444 !important; font-size: 9pt  !important; }

    table { background: #ffffff !important; }
    tbody tr { background: #ffffff !important; }
    th {
      font-size: 8.5pt !important;
      background: #f2f4f7 !important;
      color: #333 !important;
    }
    td { font-size: 10pt !important; }
    .mono { font-family: 'Courier New', monospace; }

    /* Denser tables for paper */
    .summary-table td, .summary-table th,
    .summary-movers td, .summary-movers th,
    .pa-table td,      .pa-table th {
      padding: 4px 7px !important;
      font-size: 9.5pt !important;
    }
    .summary-table td.sum-fund { font-size: 10pt !important; color: #000 !important; font-weight: 700; }

    .alert-item   { padding: 6px 8px !important; font-size: 9.5pt !important; background: #fff4d6 !important; border-left: 3px solid #a37500 !important; }
    .comment-fund { padding: 6px 8px !important; font-size: 9.5pt !important; background: #fafbfc !important; border: 1px solid #d0d5dd !important; }
    .comment-list li { font-size: 9pt !important; line-height: 1.5 !important; }
    .comment-empty   { color: #777 !important; }

    .section-wrap { page-break-inside: auto; }
    .pa-sort-arrow { display: none !important; }

    /* Keep heatmap very subtle so numbers remain readable */
    .pa-table td[style*="background:rgba(38,208"] { background: #e8f6ed !important; }
    .pa-table td[style*="background:rgba(255,90"] { background: #fae9ec !important; }

    .subtitle { font-size: 9pt !important; margin: 2px 0 6px !important; color: #333 !important; }
  }
  /* print-full: show all section-wraps even if filtered out by mode/fund/report */
  body.print-full .section-wrap { display: block !important; }
  body.print-full .sn-switcher,
  body.print-full .report-fund-switcher { display: none !important; }

  .date-hint { font-size:9px; color:var(--muted); margin-left:6px; }
  .subtitle {
    max-width:1280px; margin:10px auto 0; padding:0 22px;
    font-size:11px; color:var(--muted);
  }

  /* Main */
  main { max-width:1280px; margin:0 auto; padding:18px 22px 40px; }
  .section-wrap { display:block; }

  /* ── Mobile responsive — viewport ≤ 768px ────────────────────────────── */
  @media (max-width: 768px) {
    main { padding: 12px 10px 28px; }
    .card { padding: 10px 12px; }
    /* Wide tables: horizontal scroll instead of page overflow */
    .card table, .summary-table {
      display: block; overflow-x: auto; white-space: nowrap;
      -webkit-overflow-scrolling: touch;
    }
    .card-head { flex-wrap: wrap; gap: 6px; }
    .card-title { font-size: 14px; }
    .card-sub { font-size: 11px; }
    /* Chip bars wrap to multiple lines */
    .fund-nav-chips, .mode-switcher, .sub-tabs { flex-wrap: wrap; }
    /* Smaller monospace for dense tables */
    .mono { font-size: 11px; }
    header .brand img { height: 28px; }
  }
  @media (max-width: 480px) {
    main { padding: 8px 6px 24px; }
    .card { padding: 8px 8px; border-radius: 6px; }
    .card-title { font-size: 13px; }
    .mono { font-size: 10px; }
  }
  /* ── Market tab ── */
  .mkt-subtabs { display:flex; gap:4px; }
  .mkt-stab {
    background:none; border:1px solid var(--line); border-radius:5px;
    color:var(--muted); font-size:11px; padding:3px 10px; cursor:pointer;
    transition:color .15s,border-color .15s,background .15s;
  }
  .mkt-stab:hover { color:var(--text); border-color:var(--accent); }
  .mkt-stab.active { color:var(--text); background:var(--panel-2); border-color:var(--accent); }
  .mkt-view { }
  .mkt-win-bar { display:flex; align-items:center; gap:4px; padding:10px 14px 0; }
  .mkt-win-lbl { font-size:10px; color:var(--muted); margin-right:4px; }
  .mkt-win-btn {
    background:none; border:1px solid var(--line); border-radius:4px;
    color:var(--muted); font-size:10px; padding:2px 8px; cursor:pointer;
    transition:color .12s,border-color .12s,background .12s;
  }
  .mkt-win-btn.active { color:var(--text); background:var(--panel-2); border-color:var(--accent); }
  .mkt-panels-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; padding:10px 14px 14px; }
  .mkt-panel { border:1px solid var(--line); border-radius:8px; overflow:hidden; }
  .mkt-panel-hdr { padding:7px 12px; background:var(--panel-2); font-size:10px; font-weight:700;
                    letter-spacing:.8px; text-transform:uppercase; color:var(--muted);
                    border-bottom:1px solid var(--line); }
  .mkt-tbl { width:100%; border-collapse:collapse; font-size:12px; }
  .mkt-tbl th { background:var(--panel-2); padding:4px 8px; text-align:right;
                 border-bottom:1px solid var(--line); font-size:10px; color:var(--muted);
                 font-weight:500; white-space:nowrap; cursor:pointer; user-select:none; }
  .mkt-tbl th:first-child { text-align:left; }
  .mkt-tbl td { padding:5px 8px; border-bottom:1px solid var(--line); white-space:nowrap; vertical-align:middle; }
  .mkt-tbl tr:last-child td { border-bottom:none; }
  .mkt-tbl tr:hover td { background:rgba(255,255,255,.03); }
  .mkt-full-tbl { padding:0 4px; }
  .mkt-lbl { color:var(--text); font-weight:500; min-width:100px; }
  .mkt-val { text-align:right; font-variant-numeric:tabular-nums; min-width:72px; }
  .mkt-chg { text-align:right; min-width:72px; font-variant-numeric:tabular-nums; }
  .mkt-arr { text-align:center; width:18px; padding:5px 2px; }
  .mkt-spark { text-align:right; width:84px; padding-right:10px; }
  .mkt-unit { font-size:9px; color:var(--muted); margin-left:3px; }
  .mkt-null { color:var(--muted); }
  @media (max-width:900px) { .mkt-panels-grid { grid-template-columns:1fr; } }

  .empty-view { padding:28px; text-align:center; color:var(--muted); font-size:12px; }
  #empty-state {
    padding:40px 16px; text-align:center; color:var(--muted); font-size:13px;
    border:1px dashed var(--line); border-radius:12px; margin-top:12px;
  }

  /* legacy placeholder — report mode removed */
  body[data-mode="UNUSED"] #sections-container {
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 12px; padding: 4px 18px; margin-top: 4px;
  }
  body[data-mode="UNUSED"] #sections-container .section-wrap > .card {
    background: transparent; border: 0;
    padding: 14px 0 16px; margin-bottom: 0;
    border-bottom: 1px solid var(--line);
    border-radius: 0;
  }
  body[data-mode="UNUSED"] #sections-container .section-wrap:last-of-type > .card,
  body[data-mode="UNUSED"] #sections-container .section-wrap > .card:last-child {
    border-bottom: 0;
  }

  /* Distribution 252d table */
  .dist-table td, .dist-table th { vertical-align:middle; padding:7px 10px; }
  .dist-table .dist-tag  { font-size:9px; font-weight:700; letter-spacing:1px; width:54px; }
  .dist-table .dist-name { font-size:12px; color:var(--text); font-weight:500; }
  .dist-table .dist-num  { font-size:12px; text-align:right; font-variant-numeric: tabular-nums; }
  .dist-table .metric-row:hover { background:var(--panel-2); }
  .dist-toggle { display:inline-flex; gap:2px; background:var(--panel-2); border:1px solid var(--line); border-radius:8px; padding:3px; }
  .dist-btn, .dist-bench-btn { padding:5px 12px; font-size:11px; font-weight:600; color:var(--muted); background:transparent; border:0; border-radius:6px; cursor:pointer; letter-spacing:.04em; font-family:'Gadugi','Inter',system-ui,sans-serif; }
  .dist-btn:hover, .dist-bench-btn:hover { color:var(--text); }
  .dist-btn.active, .dist-bench-btn.active { color:#fff; background:linear-gradient(180deg,var(--accent-2),var(--accent)); }
  .dist-bench-btn:disabled { opacity:.35; cursor:default; }
  .dist-btn.dist-btn-disabled { color:var(--muted); opacity:.35; cursor:not-allowed; background:transparent !important; }
  .dist-btn.dist-btn-disabled:hover { color:var(--muted); }
  .dist-view { margin-top:10px; }
  .dist-top-modal { position:fixed; top:0; left:0; right:0; bottom:0; z-index:9998; }
  .dist-top-backdrop { position:absolute; inset:0; background:rgba(0,0,0,0.55); }
  .dist-top-card { position:relative; max-width:780px; margin:5vh auto; max-height:85vh; overflow-y:auto;
                    background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px 20px;
                    box-shadow:0 12px 40px rgba(0,0,0,0.6); }
  .dist-top-sec { display:none; }
  .dist-top-sec.active { display:block; }

  /* CSV export button (injected into each card-head) */
  .btn-csv {
    margin-left: auto; padding: 4px 10px;
    font-size: 10px; font-weight: 600; letter-spacing: .06em;
    color: var(--muted); background: var(--panel-2);
    border: 1px solid var(--line); border-radius: 6px;
    cursor: pointer; font-family: 'Gadugi','Inter',system-ui,sans-serif;
    transition: all .15s ease;
  }
  .btn-csv:hover { color: var(--text); border-color: var(--accent-2); background: rgba(0,113,187,.12); }
  .card-head { gap: 8px; }

  /* Stop table (Risk Budget Monitor) — widths tuned so the 300px SVG fits */
  .stop-table { table-layout: fixed; }
  .stop-table td, .stop-table th { vertical-align: middle; white-space: nowrap; }
  .stop-table th        { padding:6px 12px; }
  .stop-table .pm-name   { font-size:12.5px; color:var(--text); padding:8px 12px; width:160px; font-weight:500; }
  .stop-table .pm-margem { font-size:20px;   font-weight:700;  width:90px;  text-align:right; padding:8px 12px; font-variant-numeric: tabular-nums; }
  .stop-table .bar-cell  { width:320px; padding:8px 10px; }
  .stop-table .pm-hist   { font-size:11px;   width:170px; text-align:right; line-height:1.6; padding:8px 12px; font-variant-numeric: tabular-nums; }
  .stop-table .pm-status { font-size:12px;   font-weight:600;  width:110px; padding:8px 12px; }
  .stop-table .spark-cell { padding:2px 6px; width:auto; }

  /* Cards */
  .card {
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:18px 20px; margin-bottom:18px;
  }
  .card-head {
    display:flex; align-items:baseline; gap:8px;
    padding-bottom:10px; margin-bottom:10px;
    border-bottom:1px solid var(--line);
  }
  .card-title {
    font-size:11px; letter-spacing:.18em; text-transform:uppercase;
    color:var(--text); font-weight:700;
  }
  .card-sub { font-size:11px; color:var(--muted-strong); letter-spacing:.05em; }
  .card-sub .fund-name {
    color:var(--accent); font-weight:700; letter-spacing:.08em;
    padding:1px 6px; border-radius:4px;
    background:rgba(26,143,209,0.10); border:1px solid rgba(26,143,209,0.25);
  }

  /* Utility: keep mixed-case inside uppercase-transformed parents (e.g. "VaR" in th) */
  .kc { text-transform:none !important; }

  /* Frontier PA card — nested hierarchical PA (sub-tab) gets flush styling */
  .fpa-pa-nested > section.card {
    background:transparent !important; border:none !important;
    padding:0 !important; margin:0 !important; box-shadow:none !important;
  }
  .fpa-pa-nested > section.card > .card-head { padding:0 0 8px !important; }

  /* Per-fund nav chips — only visible in "Por Report" mode.
     Click scrolls within the currently selected report to the chosen fund's
     section-wrap. In "Por Fundo" mode the user is already viewing one fund, so
     the chip bar would be redundant / confusing (and used to switch funds).
     Sticky below the header: as you scroll past one fund section the next
     section's chip bar naturally takes over (each bar highlights its own fund). */
  .fund-nav-chips {
    position:sticky; top:var(--header-h, 72px); z-index:40;
    display:flex; flex-wrap:wrap; gap:6px;
    padding:8px 12px; margin-bottom:14px;
    background:rgba(17,20,26,.92); backdrop-filter:blur(8px);
    border:1px solid var(--line); border-radius:8px;
    font-size:11px; letter-spacing:.03em;
    box-shadow:0 4px 14px -8px rgba(0,0,0,.55);
  }
  body:not([data-mode="report"]) .fund-nav-chips { display:none; }
  .fund-nav-chips .chip-label {
    color:var(--muted); text-transform:uppercase; letter-spacing:.12em;
    font-size:10px; padding:4px 6px; align-self:center;
  }
  .fund-nav-chips .chip {
    background:transparent; color:var(--muted);
    border:1px solid var(--line); border-radius:4px;
    padding:4px 10px; cursor:pointer; font-family:inherit;
    transition:all .12s ease;
  }
  .fund-nav-chips .chip:hover { color:var(--text); border-color:var(--accent); }
  .fund-nav-chips .chip.active {
    background:var(--accent); color:#0b1220;
    border-color:var(--accent); font-weight:700;
  }

  table { width:100%; border-collapse:collapse; }
  .col-headers th {
    font-size:10px; color:var(--muted); letter-spacing:1.5px; text-transform:uppercase;
    padding:6px 12px; text-align:left; border-bottom:1px solid var(--line);
    font-weight:500;
  }
  .metric-row td { padding:6px 12px; vertical-align:middle; }
  .metric-name { font-size:12px; color:var(--muted); width:120px; }
  .value-cell { font-size:20px; font-weight:700; width:80px; text-align:right; }
  .bar-cell { width:260px; padding:4px 16px; }
  .util-cell { font-size:12px; color:var(--muted); width:90px; text-align:right; }
  .spark-cell { width:180px; padding:2px 8px; }
  .metric-row:hover { background:var(--panel-2); }
  .bar-legend { margin-top:10px; font-size:10px; color:var(--muted-strong); font-weight:500; line-height:1.8; }
  .tick { color:#fb923c; font-size:9px; }

  .sum-movers-card .card-head { display:flex; flex-wrap:wrap; align-items:center; gap:12px; }
  .sum-tgl { margin-left:auto; }
  .rf-view-toggle { margin-left:auto; }

  /* RF Exposure Map chart */
  .rf-expo-svg { display:block; margin:0 auto; }
  .rf-expo-svg .rf-bar { stroke:none; opacity:.92; }
  .rf-expo-svg .rf-real { fill:#f59e0b; }          /* amber — real/IPCA */
  .rf-expo-svg .rf-nom  { fill:#14b8a6; }          /* teal — nominal/pré */
  .rf-expo-svg .rf-benchbar { fill:#64748b; opacity:.9; } /* slate — benchmark */
  .rf-expo-svg .rf-cum-bench { fill:none; stroke:#64748b; stroke-width:1.6; stroke-dasharray:4 3; }
  .rf-expo-svg .rf-cum  { fill:none; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }
  .rf-expo-svg .rf-cum-real { stroke:#b45309; stroke-dasharray:0; }
  .rf-expo-svg .rf-cum-nom  { stroke:#0f766e; stroke-dasharray:0; }
  .rf-expo-svg .rf-bench { fill:none; stroke:#64748b; stroke-width:1.4; stroke-dasharray:4 3; }
  .rf-expo-svg .rf-gap { fill:none; stroke:#1a8fd1; stroke-width:2.2; }
  .rf-expo-svg .rf-grid { stroke:var(--line); stroke-width:.7; opacity:.4; }
  .rf-expo-svg .rf-zero { stroke:var(--muted); stroke-width:1; opacity:.6; }
  .rf-expo-svg .rf-axis-lbl { fill:var(--muted); font-size:10.5px; font-family:'JetBrains Mono', monospace; }
  .rf-expo-svg .rf-bench-marker { stroke:#f97316; stroke-width:2.2; stroke-dasharray:6 4; opacity:1; }
  .rf-expo-svg .rf-bench-marker-lbl { fill:#f97316; font-weight:700; font-size:11px; }
  .rf-expo-svg .rf-bench-marker-lbl-bg { fill:var(--bg); opacity:.85; }

  .rf-legend { display:flex; flex-wrap:wrap; justify-content:center; gap:16px; align-items:center; }
  .rf-legend-item { display:inline-flex; align-items:center; gap:5px; }
  .rf-swatch { display:inline-block; width:12px; height:10px; border-radius:2px; vertical-align:middle; }
  .rf-swatch.rf-real { background:#f59e0b; }
  .rf-swatch.rf-nom  { background:#14b8a6; }
  .rf-swatch.rf-benchbar { background:#64748b; }
  .rf-swatch.rf-cum  { background:#b45309; height:2px; border-radius:0; }
  .rf-swatch.rf-bench { background:transparent; border-top:2px dashed #64748b; height:0; width:14px; border-radius:0; }
  .rf-swatch.rf-gap  { background:#1a8fd1; height:2px; border-radius:0; }
  .rf-tbl-toggle {
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:4px 10px; border-radius:6px; font-size:11px; cursor:pointer;
    font-family:'Inter', sans-serif; letter-spacing:.04em;
  }
  .rf-tbl-toggle:hover { background:var(--panel-2); color:var(--text); }

  /* Executive briefing (top of Summary) */
  .brief-card { border-left:3px solid var(--accent); }
  .brief-headline {
    font-size:16px; font-weight:600; color:var(--text);
    padding:10px 14px; margin:6px 0 12px; line-height:1.45;
    background:rgba(0,113,187,.06); border-radius:6px;
    border-left:2px solid var(--accent-2);
  }
  .brief-benchmarks {
    font-size:11px; color:var(--muted); margin-bottom:14px;
    letter-spacing:.04em; padding-left:4px;
  }
  .brief-grid {
    display:grid; grid-template-columns:1fr 1fr; gap:24px;
  }
  @media (max-width:900px) { .brief-grid { grid-template-columns:1fr; gap:16px; } }
  .brief-col { min-width:0; }
  .brief-section-title {
    font-size:10px; letter-spacing:.14em; text-transform:uppercase;
    color:var(--accent-2); font-weight:700; margin:12px 0 6px;
    padding-bottom:4px; border-bottom:1px solid var(--line);
  }
  .brief-list { list-style:none; margin:0; padding:0; font-size:12.5px; line-height:1.55; }
  .brief-list li { padding:5px 0; border-bottom:1px dashed var(--line); }
  .brief-list li:last-child { border-bottom:none; }
  .brief-list li b { color:var(--text); font-weight:600; }
  .brief-list li i { color:var(--muted); font-style:normal; }
  .brief-annex {
    margin-top:14px; padding-top:10px; border-top:1px solid var(--line);
    font-size:11px; color:var(--muted);
  }
  .brief-annex a { color:var(--accent-2); text-decoration:none; border-bottom:1px dotted var(--accent-2); }
  .brief-annex a:hover { color:var(--text); border-bottom-color:var(--text); }
  .brief-commentary {
    font-size:13px; color:var(--text); line-height:1.65;
    padding:10px 14px; margin:0 0 14px;
    background:rgba(26,143,209,.04); border-left:2px solid var(--accent-2);
    border-radius:6px;
  }
  .brief-commentary b { color:var(--text); font-weight:600; }

  /* PA Contribuições — Por Tamanho / Por Fundo grid (flows side-by-side) */
  .pa-alert-view {
    display:grid;
    grid-template-columns:repeat(auto-fill, minmax(280px, 1fr));
    gap:10px;
  }
  .pa-alert-view-hidden { display:none; }

  /* Evolution Diversification — nested layer cards flattened inside unified card */
  .evo-div-body section.card {
    border: 0 !important;
    background: transparent !important;
    padding: 4px 0 0 !important;
    box-shadow: none !important;
    margin-bottom: 0 !important;
  }
  .evo-div-body .card-head {
    border-bottom: 1px solid var(--line);
    padding-bottom: 8px;
    margin-bottom: 10px;
  }

  /* Stop history modal — child window triggered from Risk Budget Monitor */
  .stop-modal {
    position:fixed; inset:0; z-index:9999;
    display:flex; align-items:center; justify-content:center;
  }
  .stop-modal-overlay {
    position:absolute; inset:0; background:rgba(0,0,0,0.72);
    backdrop-filter: blur(2px);
  }
  .stop-modal-box {
    position:relative; z-index:1; max-width:860px; width:92%;
    max-height:85vh; overflow-y:auto;
    background:var(--panel); border:1px solid var(--line);
    border-radius:12px; padding:18px 22px 16px;
    box-shadow:0 16px 48px rgba(0,0,0,0.6);
  }
  .stop-modal-head {
    display:flex; justify-content:space-between; align-items:center;
    border-bottom:1px solid var(--line); padding-bottom:10px; margin-bottom:14px;
  }
  .stop-modal-head .modal-title {
    font-size:15px; font-weight:700; color:var(--text);
  }
  .stop-modal-close {
    background:transparent; border:1px solid var(--line); color:var(--muted);
    width:28px; height:28px; border-radius:6px; font-size:14px; cursor:pointer;
  }
  .stop-modal-close:hover { color:var(--text); border-color:var(--text); }
  .stop-modal-tabs { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px; }

  /* Top Movers split (per-fund) */
  .mov-split { display:grid; grid-template-columns:1fr 1fr; gap:24px; }
  .mov-col-title {
    font-size:10px; letter-spacing:.15em; text-transform:uppercase;
    color:var(--muted); font-weight:700; margin-bottom:6px;
    padding-bottom:5px; border-bottom:1px solid var(--line);
  }

  /* Comments / outliers block */
  .comments-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:12px; }
  .comment-fund { background:var(--bg-2); border:1px solid var(--line); border-radius:8px; padding:12px 14px; }
  .comment-title {
    font-size:11px; color:var(--text); letter-spacing:.1em; text-transform:uppercase;
    font-weight:700; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid var(--line);
  }
  .comment-empty { font-size:11.5px; color:var(--muted-strong); font-style:italic; padding:4px 0; }
  .comment-list  { list-style:none; padding:0; margin:0; font-size:11.5px; line-height:1.7; }
  .comment-list li { padding:3px 0; border-bottom:1px dotted var(--line); }
  .comment-list li:last-child { border-bottom:none; }

  /* Summary page — fund grid + top movers */
  .summary-table {
    width:100%; border-collapse:collapse; font-size:13px;
    background:var(--bg-2); border-radius:8px; overflow:hidden;
  }
  .summary-table th {
    color:var(--muted); font-size:10px; letter-spacing:.12em; text-transform:uppercase;
    padding:10px 12px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }
  .summary-table td { padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:middle; }
  .summary-table tr:last-child td { border-bottom:none; }
  .summary-table tr:hover { background:rgba(26,143,209,.04); }
  .summary-table td.sum-status { text-align:center; font-size:17px; width:60px; }
  .summary-table td.sum-fund   { font-weight:700; color:var(--text); font-size:13.5px; }
  .summary-table tr.bench-row:first-of-type td { border-top:2px solid var(--border); }
  .summary-table tr.bench-row td { background:rgba(26,143,209,.03); }
  .summary-table tr.bench-row td.sum-fund { font-weight:600; font-size:12.5px; }

  .summary-movers {
    width:100%; border-collapse:collapse; font-size:12px;
    background:var(--bg-2); border-radius:8px; overflow:hidden;
  }
  .summary-movers th {
    color:var(--muted); font-size:10px; letter-spacing:.12em; text-transform:uppercase;
    padding:10px 12px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }
  .summary-movers td { padding:10px 12px; border-bottom:1px solid var(--line); vertical-align:middle; }
  .summary-movers tr:last-child td { border-bottom:none; }
  .summary-movers td.sum-fund   { font-weight:700; color:var(--text); width:130px; }
  .summary-movers td.sum-movers { font-family:'JetBrains Mono', monospace; font-size:11.5px; }

  /* Single-name inline section */
  .sn-inline-stats {
    color:var(--muted); font-size:12px;
    margin: 4px 2px 14px; padding-bottom:10px;
    border-bottom:1px solid var(--line);
  }

  /* Performance attribution — hierarchical tree */
  .pa-card .card-head { display:flex; flex-wrap:wrap; align-items:center; gap:12px; }
  .pa-toolbar { margin-left:auto; display:flex; gap:6px; align-items:center; }
  .pa-search {
    background:rgba(255,255,255,0.06); border:1px solid var(--line);
    border-radius:7px; color:var(--text); font-size:11.5px;
    padding:4px 10px; outline:none; width:150px;
    font-family:'JetBrains Mono',monospace;
  }
  .pa-search:focus { border-color:var(--accent-2); background:rgba(255,255,255,0.1); }
  .pa-search::placeholder { color:var(--muted); }
  .pa-btn {
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 10px; border-radius:6px; font-size:11px; cursor:pointer;
  }
  .pa-btn:hover { color:var(--text); border-color:var(--line-2); }
  .pa-view-toggle { display:flex; gap:4px; }
  .pa-tgl {
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 12px; border-radius:7px; font-size:11px; font-weight:600;
    letter-spacing:.06em; cursor:pointer;
  }
  .pa-tgl:hover { color:var(--text); border-color:var(--line-2); }
  .pa-tgl.active { background:var(--accent); border-color:var(--accent); color:#fff; }

  /* Frontier Exposure toggles (IBOV/IBOD, Por Nome/Por Setor, ▼/▶ All).
     Same visual as .pa-tgl — dark bg, accent-blue active state. */
  .toggle-btn {
    background:transparent; border:1px solid var(--line); color:var(--muted);
    padding:5px 12px; border-radius:7px; font-size:11px; font-weight:600;
    letter-spacing:.06em; cursor:pointer;
    font-family:'Inter', sans-serif;
  }
  .toggle-btn:hover { color:var(--text); border-color:var(--line-2); background:rgba(0,113,187,.06); }
  .toggle-btn.active { background:var(--accent); border-color:var(--accent); color:#fff; }

  /* QUANT Exposure sortable column headers */
  .qexpo-sort-th { user-select:none; transition: color .12s ease; }
  .qexpo-sort-th:hover { color:var(--accent-2); }
  .qexpo-sort-th.qexpo-sort-active { color:var(--accent-2); }
  .qexpo-sort-arrow { color:var(--accent-2); font-weight:700; margin-left:3px; }

  .pa-table {
    width:100%; background:var(--bg-2); border-radius:8px; overflow:hidden;
    font-size:12px; border-collapse:collapse;
  }
  .pa-table th {
    color:var(--muted); font-size:10px; letter-spacing:.1em; text-transform:uppercase;
    padding:9px 10px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }
  .pa-table td.pa-name { padding:6px 10px; color:var(--text); font-weight:600; }
  .pa-table td.t-num   { padding:6px 10px; text-align:right; }
  .pa-table tbody tr   { border-bottom:1px solid var(--line); }
  .pa-table tbody tr:last-child { border-bottom:none; }
  .pa-table tbody tr.pa-has-children { cursor:pointer; }
  .pa-table tbody tr.pa-has-children:hover { background:rgba(26,143,209,.06); }
  .pa-table tbody tr.pa-l0 td.pa-name { padding-left:10px;  font-weight:700; }
  .pa-table tbody tr.pa-l1 td.pa-name { padding-left:30px;  font-weight:600; color:var(--text); }
  .pa-table tbody tr.pa-l2 td.pa-name { padding-left:50px;  font-weight:400; color:var(--muted); }
  .pa-table tbody tr.pa-l1        { background:rgba(255,255,255,.012); }
  .pa-table tbody tr.pa-l2        { background:rgba(255,255,255,.024); font-size:11.5px; }
  .pa-table tbody tr.pa-pinned td.pa-name { color:var(--muted); font-style:italic; }
  .pa-table tbody tr.pa-pinned    { border-top:1px dashed var(--line); }
  .pa-exp {
    display:inline-block; width:14px; color:var(--muted);
    font-size:10px; margin-right:4px; transition:transform .15s;
  }
  .pa-exp-empty { color:transparent; }
  .pa-has-children.expanded .pa-exp { transform:rotate(90deg); color:var(--accent-2); }
  .pa-table tfoot tr.pa-total-row {
    background:var(--panel-2); border-top:1px solid var(--line-2);
  }
  .pa-table tfoot tr.pa-bench-row {
    background:transparent; border-top:1px dashed var(--line);
  }
  .pa-table tfoot tr.pa-bench-row td { font-style:italic; }
  .pa-table tfoot tr.pa-nominal-row {
    background:var(--panel-2); border-top:1px solid var(--line-2);
  }
  .pa-table tfoot td.pa-name { padding:8px 10px; }
  .pa-table th.pa-sortable { user-select:none; }
  .pa-table th.pa-sortable:hover { color:var(--text); }
  .pa-table th.pa-sort-active { color:var(--accent-2); border-left:1px solid rgba(90,163,232,0.35); border-right:1px solid rgba(90,163,232,0.35); }
  .pa-sort-arrow { font-size:9px; color:var(--accent-2); margin-left:2px; }
  /* Highlight da coluna ativa do sort no PA — fundo azul sutil + bordas laterais
     no header. nth-child(2..5) = colunas DIA/MTD/YTD/12M (col 1 = nome). */
  .pa-view[data-sort-idx="0"] .pa-table tbody td:nth-child(2),
  .pa-view[data-sort-idx="0"] .pa-table tfoot td:nth-child(2) { background:rgba(90,163,232,0.06); }
  .pa-view[data-sort-idx="1"] .pa-table tbody td:nth-child(3),
  .pa-view[data-sort-idx="1"] .pa-table tfoot td:nth-child(3) { background:rgba(90,163,232,0.06); }
  .pa-view[data-sort-idx="2"] .pa-table tbody td:nth-child(4),
  .pa-view[data-sort-idx="2"] .pa-table tfoot td:nth-child(4) { background:rgba(90,163,232,0.06); }
  .pa-view[data-sort-idx="3"] .pa-table tbody td:nth-child(5),
  .pa-view[data-sort-idx="3"] .pa-table tfoot td:nth-child(5) { background:rgba(90,163,232,0.06); }
  .pa-pos { color:var(--muted); }

  /* Single-name fund switcher (Por Report > Single-Name) */
  .sn-switcher {
    display:flex; align-items:center; gap:8px;
    max-width: 1200px; margin: 0 auto 14px; padding: 8px 16px;
    background: var(--panel); border:1px solid var(--line);
    border-radius:10px;
  }
  .sn-switcher-label {
    font-size:10.5px; text-transform:uppercase; letter-spacing:.14em;
    color:var(--muted); margin-right:6px;
  }
  .sn-switcher .tab {
    background:transparent; border:1px solid var(--line);
    color:var(--muted); padding:6px 14px; border-radius:7px;
    font-size:12px; font-weight:600; letter-spacing:.06em;
    cursor:pointer;
  }
  .sn-switcher .tab:hover { color:var(--text); border-color:var(--line-2); }
  .sn-switcher .tab.active {
    background:var(--accent); border-color:var(--accent); color:#fff;
  }
  .sn-sides { display:flex; gap:18px; }
  .sn-side { flex:1; }
  .sn-side-head { font-size:11px; font-weight:700; letter-spacing:.1em; margin-bottom:6px; }
  .sn-side-count { color:var(--muted); font-weight:400; }
  .sn-table { background:var(--bg-2); border-radius:8px; overflow:hidden; font-size:11.5px; }
  .sn-table th {
    color:var(--muted); font-size:10px; padding:8px; background:var(--panel-2);
    border-bottom:1px solid var(--line); font-weight:500;
  }
  .sn-table td.t-name { padding:6px 8px; color:var(--text); font-weight:700; }
  .sn-table td.t-num  { padding:6px 8px; text-align:right; color:var(--muted); }
  .sn-table tr { border-bottom:1px solid var(--line); }
  .sn-table tr:last-child { border-bottom:none; }
  .sn-table .t-empty { padding:14px; text-align:center; color:var(--muted); font-style:italic; }
  /* Alerts */
  .alerts-section {
    margin-top: 28px;
    border: 1px solid #fb923c44;
    border-radius: 8px;
    overflow: hidden;
  }
  .alerts-header {
    background: #1c1409;
    color: #fb923c;
    font-size: 11px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    padding: 10px 16px;
    border-bottom: 1px solid #fb923c33;
  }
  .alert-item {
    padding: 12px 16px;
    border-bottom: 1px solid #1e1e2e;
  }
  .alert-item:last-child { border-bottom: none; }
  .alert-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 6px;
  }
  .alert-badge {
    color: #fb923c;
    font-size: 14px;
  }
  .alert-title {
    color: #fb923c;
    font-weight: bold;
    font-size: 13px;
  }
  .alert-stats {
    color: #64748b;
    font-size: 11px;
    font-family: monospace;
  }
  .alert-body {
    color: #cbd5e1;
    font-size: 12px;
    line-height: 1.6;
    padding-left: 24px;
  }
"""


# ── iOS touch polyfill JS (extracted from generate_risk_report.py) ────────
# Static script body — no Python interpolations. Interpolated as
# {IOS_POLYFILL_JS} between literal <script>...</script> tags.
IOS_POLYFILL_JS = """/* ── iOS touch polyfill ──────────────────────────────────────────────────────
 * Safari Mobile has two quirks that break the report on iPhone/iPad:
 *   1. onclick on non-semantic elements (div/span/tr/th) often fails to fire
 *      on first tap unless the element has role="button" + tabindex + cursor
 *   2. touch events need keyboard-equivalent handlers for accessibility
 *
 * This polyfill runs once after DOMContentLoaded and patches every element
 * with an inline onclick attribute:
 *   - adds role="button" + tabindex="0" if missing
 *   - adds cursor:pointer if missing
 *   - adds keydown (Enter/Space) that triggers the onclick
 *
 * Also toggles the #ios-banner visibility based on user agent so desktop
 * users don't see the mobile hint.
 * ─────────────────────────────────────────────────────────────────────────── */
(function() {
  var isIOS = /iPhone|iPad|iPod/.test(navigator.userAgent)
              || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);

  function patchClickables() {
    var els = document.querySelectorAll('[onclick]');
    els.forEach(function(el) {
      var tag = el.tagName.toLowerCase();
      // <button> and <a> already work fine on Safari — skip
      if (tag === 'button' || tag === 'a') return;

      if (!el.hasAttribute('role'))     el.setAttribute('role', 'button');
      if (!el.hasAttribute('tabindex')) el.setAttribute('tabindex', '0');
      if (!el.style.cursor)             el.style.cursor = 'pointer';

      // Keyboard equivalents (Enter/Space triggers click) — also helps iOS
      // VoiceOver users and fixes some focus-driven tap flows.
      if (!el.dataset.iosPatched) {
        el.dataset.iosPatched = '1';
        el.addEventListener('keydown', function(e) {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            el.click();
          }
        });
      }
    });
  }

  function run() {
    if (isIOS) {
      var banner = document.getElementById('ios-banner');
      if (banner) banner.style.display = 'block';
    }
    patchClickables();
    // Observe DOM mutations — some cards lazy-render children on expand,
    // those need the same patching when they appear.
    if (window.MutationObserver) {
      var mo = new MutationObserver(function() { patchClickables(); });
      mo.observe(document.body, { childList: true, subtree: true });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', run);
  } else {
    run();
  }
})();
"""


# ── Main navigation + tab JS (extracted from generate_risk_report.py) ────
# Wrapped as a builder function: same f-string mechanics as the original
# inline block, just relocated. The {{ }} JS literal-brace escapes and
# {var} Python interpolations are preserved verbatim from the source.
def main_navigation_js(*, fund_reports_js, report_labels_js, DATA_STR,
                       fund_shorts_js, fund_labels_js, fund_order_js):
    return f"""<script>
(function() {{
  function getBRT() {{
    var now = new Date();
    var hour = parseInt(new Intl.DateTimeFormat('en', {{
      timeZone: 'America/Sao_Paulo', hour: 'numeric', hour12: false
    }}).format(now));
    var fmt = new Intl.DateTimeFormat('en-CA', {{
      timeZone: 'America/Sao_Paulo',
      year: 'numeric', month: '2-digit', day: '2-digit'
    }});
    var todayStr = fmt.format(now);
    if (hour < 21) {{
      var d = new Date(now);
      d.setDate(d.getDate() - 1);
      var wd = new Intl.DateTimeFormat('en', {{timeZone:'America/Sao_Paulo', weekday:'short'}}).format(d);
      while (wd === 'Sat' || wd === 'Sun') {{
        d.setDate(d.getDate() - 1);
        wd = new Intl.DateTimeFormat('en', {{timeZone:'America/Sao_Paulo', weekday:'short'}}).format(d);
      }}
      return {{ def: fmt.format(d), today: todayStr, hour: hour }};
    }} else {{
      return {{ def: todayStr, today: todayStr, hour: hour }};
    }}
  }}
  // --- Navigation state: 3 modes (summary / fund / quality) driven by URL hash ---
  function parseHash() {{
    var h = (location.hash || '').slice(1);
    if (!h || h === 'summary') return {{ mode: 'summary' }};
    var m;
    if ((m = h.match(/^fund=(.*)$/)))   return {{ mode: 'fund',   sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^report=(.*)$/))) return {{ mode: 'report', sel: m[1] ? decodeURIComponent(m[1]) : '' }};
    if ((m = h.match(/^quality$/)))     return {{ mode: 'quality', sel: '' }};
    if ((m = h.match(/^pnl$/)))         return {{ mode: 'pnl',     sel: '' }};
    if ((m = h.match(/^peers$/)))       return {{ mode: 'peers',   sel: '' }};
    if ((m = h.match(/^market$/)))      return {{ mode: 'market',  sel: '' }};
    return {{ mode: 'summary' }};
  }}
  function setHash(mode, sel) {{
    var h = (mode === 'summary') ? '' : (sel ? (mode + '=' + encodeURIComponent(sel)) : mode);
    if (history.replaceState) history.replaceState(null, '', h ? ('#' + h) : location.pathname + location.search);
    else location.hash = h;
  }}

  var _FUND_REPORTS = {fund_reports_js};
  var _REPORT_LABELS = {report_labels_js};

  function updateJumpBar(fund) {{
    var bar = document.getElementById('report-jump-bar');
    if (!bar) return;
    var rids = _FUND_REPORTS[fund] || [];
    bar.innerHTML = '';
    rids.forEach(function(rid) {{
      var btn = document.createElement('button');
      btn.className = 'jump-btn';
      btn.dataset.rid = rid;
      btn.textContent = _REPORT_LABELS[rid] || rid;
      btn.onclick = function() {{ jumpTo('sec-' + fund + '-' + rid); }};
      bar.appendChild(btn);
    }});
  }}
  window.jumpTo = function(id) {{
    var el = document.getElementById(id);
    if (!el) return;
    // Account for sticky header height
    var hdr = document.querySelector('header');
    var offset = hdr ? hdr.offsetHeight : 0;
    var top = el.getBoundingClientRect().top + window.pageYOffset - offset - 8;
    window.scrollTo({{ top: top, behavior: 'smooth' }});
    // Highlight active jump button
    document.querySelectorAll('.jump-btn').forEach(function(b) {{ b.classList.remove('active-jump'); }});
    var rid = id.split('-').slice(2).join('-');
    document.querySelectorAll('.jump-btn[data-rid="' + rid + '"]').forEach(function(b) {{
      b.classList.add('active-jump');
    }});
  }};

  function syncHeaderH() {{
    var h = document.querySelector('header');
    if (!h) return;
    // rAF: header height depends on sub-tabs visibility, which is set earlier
    // in the same tick — let layout settle before reading offsetHeight.
    requestAnimationFrame(function() {{
      document.documentElement.style.setProperty('--header-h', h.offsetHeight + 'px');
    }});
  }}

  // ─── Lazy hydration of per-fund / per-report sections ──────────────────
  // At load, every (fund, report) block lives inside a <template> and is NOT
  // in the live DOM. Hydrating clones the template into #sections-container
  // the first time the user enters Per-Fundo / Per-Report mode for that
  // fund/report. Subsequent visits just toggle visibility on existing nodes.
  var _sectionsHydrated = Object.create(null);   // "fund:report" → true
  function _hydrateSection(fund, report) {{
    var key = fund + ':' + report;
    if (_sectionsHydrated[key]) return;
    var tpl = document.querySelector(
      'template.tpl-section[data-fund="' + fund + '"][data-report="' + report + '"]'
    );
    if (!tpl) {{ _sectionsHydrated[key] = true; return; }}
    var host = document.getElementById('sections-container');
    if (!host) return;
    // Hide before append so idle-hydration in summary/quality/etc. mode never
    // flashes the section visible — applyState's visibility loop only runs on
    // mode/sel changes, not after lazy clones.
    var frag = tpl.content.cloneNode(true);
    Array.prototype.forEach.call(frag.querySelectorAll('.section-wrap'), function(el) {{
      el.style.display = 'none';
    }});
    host.appendChild(frag);
    _sectionsHydrated[key] = true;
    // If the just-hydrated section matches the current mode/sel, reveal it.
    var mode = document.body.dataset.mode || '';
    if (mode === 'fund' || mode === 'report') {{
      var bar = document.querySelector('.sub-tabs[data-for="' + mode + '"]');
      var active = bar ? bar.querySelector('.tab.active') : null;
      var sel = active ? active.dataset.target : '';
      var match = (mode === 'fund' && sel === fund) || (mode === 'report' && sel === report);
      if (match) {{
        host.querySelectorAll('.section-wrap[data-fund="' + fund + '"][data-report="' + report + '"]')
          .forEach(function(el) {{ el.style.display = ''; }});
      }}
    }}
    // Re-run handlers that walk the DOM at startup so they pick up newly
    // injected nodes (credit-section sort, vol-regime caret toggle, peers
    // tables, CSV buttons, fund-name highlighting).
    if (typeof window.initAlcSort === 'function') window.initAlcSort();
    if (typeof attachVrCaretToggle === 'function') attachVrCaretToggle();
    if (typeof injectCsvButtons === 'function') injectCsvButtons();
    if (typeof highlightFundNames === 'function') highlightFundNames();
    if (typeof initRptPeers === 'function') initRptPeers();
  }}
  function _hydrateFund(fund) {{
    if (!fund) return;
    var rids = (_FUND_REPORTS && _FUND_REPORTS[fund]) || [];
    rids.forEach(function(r) {{ _hydrateSection(fund, r); }});
  }}
  function _hydrateReport(report) {{
    if (!report) return;
    Object.keys(_FUND_REPORTS || {{}}).forEach(function(fund) {{
      var rids = _FUND_REPORTS[fund] || [];
      if (rids.indexOf(report) >= 0) _hydrateSection(fund, report);
    }});
  }}
  // Pre-warm in the background (during browser idle time) once the page is
  // settled, so by the time the user clicks a fund tab everything's ready.
  function _idleHydrateAll() {{
    var pairs = [];
    Object.keys(_FUND_REPORTS || {{}}).forEach(function(fund) {{
      (_FUND_REPORTS[fund] || []).forEach(function(r) {{ pairs.push([fund, r]); }});
    }});
    function step(deadline) {{
      while (pairs.length && (!deadline || deadline.timeRemaining() > 4)) {{
        var p = pairs.shift();
        _hydrateSection(p[0], p[1]);
      }}
      if (pairs.length) {{
        if (window.requestIdleCallback) requestIdleCallback(step);
        else setTimeout(step, 50);
      }}
    }}
    if (window.requestIdleCallback) requestIdleCallback(step);
    else setTimeout(step, 200);
  }}

  function applyState() {{
    var st = parseHash();
    var mode = st.mode, sel = st.sel;
    document.body.dataset.mode = mode;
    // Mode tabs
    document.querySelectorAll('.mode-tab').forEach(function(t) {{
      t.classList.toggle('active', t.dataset.mode === mode);
    }});
    // Sub-tab bars visibility
    document.querySelectorAll('.sub-tabs').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.for === mode);
    }});
    // Default selection per mode
    if (mode === 'fund' && !sel) {{
      var first = document.querySelector('.sub-tabs[data-for="fund"] .tab');
      if (first) sel = first.dataset.target;
    }}
    if (mode === 'report' && !sel) {{
      var first = document.querySelector('.sub-tabs[data-for="report"] .tab');
      if (first) sel = first.dataset.target;
    }}
    // Sub-tab active state
    document.querySelectorAll('.sub-tabs .tab').forEach(function(t) {{
      var bar = t.closest('.sub-tabs');
      t.classList.toggle('active', bar.dataset.for === mode && t.dataset.target === sel);
    }});
    // Update report jump bar (only in fund mode)
    if (mode === 'fund' && sel) updateJumpBar(sel);
    // Hydrate the sections we need before toggling visibility.
    if (mode === 'fund')   _hydrateFund(sel);
    if (mode === 'report') _hydrateReport(sel);
    // Section visibility
    document.querySelectorAll('.section-wrap').forEach(function(el) {{
      var show = false;
      if (mode === 'summary')      show = el.dataset.view === 'summary';
      else if (mode === 'quality') show = el.dataset.view === 'quality';
      else if (mode === 'pnl')     show = el.dataset.view === 'pnl';
      else if (mode === 'peers')   show = el.dataset.view === 'peers';
      else if (mode === 'market')  show = el.dataset.view === 'market';
      else if (mode === 'fund')    show = el.dataset.fund === sel;
      else if (mode === 'report')  show = el.dataset.report === sel;
      el.style.display = show ? '' : 'none';
    }});
    // Empty-state
    var anyVisible = Array.prototype.some.call(
      document.querySelectorAll('.section-wrap'),
      function(el) {{ return el.style.display !== 'none'; }}
    );
    var empty = document.getElementById('empty-state');
    if (empty) {{
      empty.style.display = anyVisible ? 'none' : '';
      if (!anyVisible) {{
        var activeTab = document.querySelector('.sub-tabs[data-for="' + mode + '"] .tab.active');
        var label = activeTab ? activeTab.textContent.trim() : (sel || '');
        if (mode === 'fund' && label) {{
          empty.textContent = 'Nenhum dado disponível para o fundo ' + label + ' nesta data.';
        }} else if (mode === 'report' && label) {{
          empty.textContent = 'Nenhum fundo tem dados para o report ' + label + ' nesta data.';
        }} else {{
          empty.textContent = 'Sem dados para essa combinação de fundo × report.';
        }}
      }}
    }}
    // Recompute header height — it changes when #report-jump-bar shows/hides.
    syncHeaderH();
  }}
  window.selectMode = function(mode, sel) {{
    if (!sel) {{
      try {{
        if (mode === 'fund')   sel = sessionStorage.getItem('risk_monitor_fund')   || '';
        if (mode === 'report') sel = sessionStorage.getItem('risk_monitor_report') || '';
      }} catch (e) {{}}
    }}
    if (mode === 'fund' && !sel) {{
      var f = document.querySelector('.sub-tabs[data-for="fund"] .tab');
      if (f) sel = f.dataset.target;
    }}
    if (mode === 'report' && !sel) {{
      var r = document.querySelector('.sub-tabs[data-for="report"] .tab');
      if (r) sel = r.dataset.target;
    }}
    setHash(mode, sel);
    applyState();
  }};
  window.selectFund = function(name) {{
    try {{ sessionStorage.setItem('risk_monitor_fund', name); }} catch (e) {{}}
    setHash('fund', name);
    applyState();
    // Always start at top of the page when switching funds
    window.scrollTo({{ top: 0, behavior: 'instant' }});
  }};
  window.selectReport = function(name) {{
    try {{ sessionStorage.setItem('risk_monitor_report', name); }} catch (e) {{}}
    setHash('report', name);
    applyState();
  }};
  window.addEventListener('hashchange', applyState);
  window.addEventListener('DOMContentLoaded', function() {{
    var info   = getBRT();
    var picker = document.getElementById('date-picker');
    var hint   = document.getElementById('date-hint');
    var loaded = '{DATA_STR}';
    picker.value = loaded;
    if (info.hour >= 21 && info.def === info.today) {{
      hint.textContent = 'após 21h';
      hint.style.color = 'var(--warn)';
    }} else if (loaded !== info.def) {{
      hint.textContent = 'default ' + info.def;
      hint.style.color = 'var(--muted)';
    }}
    applyState();
    injectCsvButtons();
    attachUniversalSort();
    attachVrCaretToggle();
    highlightFundNames();
    injectFundNavChips();
    syncHeaderH();
    window.addEventListener('resize', syncHeaderH);
    if (typeof initRptPnl   === 'function') initRptPnl();
    if (typeof initRptPeers === 'function') initRptPeers();
    // After first render, pre-warm remaining (fund, report) sections in
    // browser idle time so subsequent tab switches feel instant.
    _idleHydrateAll();
  }});
  // --- Wrap fund shortnames in card-sub elements with an accent chip ---
  function highlightFundNames() {{
    var SHORTS = {fund_shorts_js};
    // Sort by length desc so "MACRO_Q" matches before "MACRO"
    SHORTS.sort(function(a,b) {{ return b.length - a.length; }});
    var subs = document.querySelectorAll('.card-sub');
    subs.forEach(function(el) {{
      if (el.dataset.fundHighlighted) return;
      var html = el.innerHTML;
      for (var i = 0; i < SHORTS.length; i++) {{
        var s = SHORTS[i];
        // Word-boundary match; avoid wrapping inside existing tags
        var re = new RegExp('(^|[^A-Za-z0-9_>])(' + s + ')(?![A-Za-z0-9_])', 'g');
        html = html.replace(re, '$1<span class="fund-name">$2</span>');
      }}
      el.innerHTML = html;
      el.dataset.fundHighlighted = '1';
    }});
  }}
  // --- Fund-nav chip bar injected above each per-fund section ---
  function injectFundNavChips() {{
    var LABELS = {fund_labels_js};
    var ORDER  = {fund_order_js};
    // Collect available funds (those with at least one visible section-wrap)
    var available = ORDER.filter(function(s) {{
      return document.querySelector('.section-wrap[data-fund="' + s + '"]');
    }});
    if (available.length < 2) return;
    // Build one chip bar per fund — insert at the top of the first section-wrap
    // for each fund (user sees it once, highlighting the active fund).
    available.forEach(function(fs) {{
      var wrap = document.querySelector('.section-wrap[data-fund="' + fs + '"]');
      if (!wrap || wrap.querySelector('.fund-nav-chips')) return;
      var bar = document.createElement('div');
      bar.className = 'fund-nav-chips';
      var lbl = document.createElement('span');
      lbl.className = 'chip-label';
      lbl.textContent = 'Ir para:';
      bar.appendChild(lbl);
      available.forEach(function(s) {{
        var btn = document.createElement('button');
        btn.className = 'chip' + (s === fs ? ' active' : '');
        btn.textContent = LABELS[s] || s;
        // In "report" mode: stay in report, just scroll to that fund's section
        // for the currently selected report. In "fund" mode: switch fund.
        btn.onclick = function() {{
          var mode = document.body.dataset.mode || 'summary';
          if (mode === 'report') {{
            var currentReport = (location.hash.match(/report=([^&]+)/) || [])[1];
            var target = currentReport
              ? document.querySelector('#sec-' + s + '-' + currentReport)
              : document.querySelector('.section-wrap[data-fund="' + s + '"]');
            if (target) target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
          }} else {{
            window.selectFund(s);
          }}
        }};
        bar.appendChild(btn);
      }});
      wrap.insertBefore(bar, wrap.firstChild);
    }});
  }}
  // --- Vol Regime: expand/collapse books under a fund row ---
  function attachVrCaretToggle() {{
    document.querySelectorAll('.vr-caret').forEach(function(el) {{
      el.addEventListener('click', function(e) {{
        e.stopPropagation();
        var fs = el.getAttribute('data-fs');
        if (!fs) return;
        var rows = document.querySelectorAll('tr.vr-book[data-parent="' + fs + '"]');
        if (!rows.length) return;
        // Decide toggle direction from the first row's current visibility.
        // display may be '' (visible) or 'none' (hidden); decide once to keep
        // all sibling rows in sync.
        var shouldOpen = (rows[0].style.display === 'none');
        rows.forEach(function(r) {{
          r.style.display = shouldOpen ? '' : 'none';
        }});
        el.textContent = shouldOpen ? '▼' : '▶';
      }});
    }});
  }}
  window.goToDate = function(val) {{
    if (!val) return;
    var base = window.location.href.replace(/[^/\\\\]*$/, '').replace(/#.*$/, '');
    window.location.href = base + val + '_risk_monitor.html' + location.hash;
  }};
  // --- CSV export: scan visible tables inside a given element and download ---
  function escapeCSV(s) {{
    s = (s == null) ? '' : String(s).replace(/\\s+/g, ' ').trim();
    if (/[,";]/.test(s)) s = '"' + s.replace(/"/g, '""') + '"';
    return s;
  }}
  function tableToRows(tbl) {{
    var out = [];
    var rows = tbl.querySelectorAll('tr');
    for (var i = 0; i < rows.length; i++) {{
      var tr = rows[i];
      // skip hidden rows (drill-downs, toggled views)
      if (tr.offsetParent === null && tr.style.display !== '') continue;
      var cells = tr.querySelectorAll('th,td');
      if (!cells.length) continue;
      var line = [];
      for (var j = 0; j < cells.length; j++) {{
        var txt = cells[j].innerText || cells[j].textContent || '';
        line.push(escapeCSV(txt));
      }}
      out.push(line.join(','));
    }}
    return out;
  }}
  window.exportCardCSV = function(cardId, baseName) {{
    var el = document.getElementById(cardId);
    if (!el) return;
    var tables = el.querySelectorAll('table');
    var lines = [];
    tables.forEach(function(t, i) {{
      if (t.offsetParent === null) return;  // skip hidden tables
      if (i > 0 && lines.length) lines.push('');
      lines = lines.concat(tableToRows(t));
    }});
    if (!lines.length) return;
    var picker = document.getElementById('date-picker');
    var dt = (picker && picker.value) || '{DATA_STR}';
    var name = baseName + '_' + dt + '.csv';
    var blob = new Blob(['\\uFEFF' + lines.join('\\n')], {{ type: 'text/csv;charset=utf-8' }});
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a); a.click();
    setTimeout(function() {{ document.body.removeChild(a); URL.revokeObjectURL(a.href); }}, 100);
  }};
  // --- Universal table sort: clicking a <th> toggles asc/desc on that column ---
  function _cellKey(td) {{
    var dv = td.getAttribute('data-val');
    if (dv !== null && dv !== '') {{
      var n = parseFloat(dv);
      if (!isNaN(n)) return {{ n: n, s: dv }};
    }}
    var t = (td.innerText || td.textContent || '').trim();
    var clean = t.replace(/[%°\\s,+bps]/g, '').replace(/\\u2212/g, '-');
    var n2 = parseFloat(clean);
    if (!isNaN(n2)) return {{ n: n2, s: t }};
    return {{ n: NaN, s: t }};
  }}
  function sortTableByCol(table, colIdx, asc) {{
    var tbody = table.tBodies[0]; if (!tbody) return;
    var all   = Array.from(tbody.rows).filter(function(r) {{ return r.cells.length > colIdx; }});
    var pinned = all.filter(function(r) {{ return r.dataset.pinned === '1'; }});
    var rows   = all.filter(function(r) {{ return r.dataset.pinned !== '1'; }});
    rows.sort(function(a, b) {{
      var va = _cellKey(a.cells[colIdx]), vb = _cellKey(b.cells[colIdx]);
      if (!isNaN(va.n) && !isNaN(vb.n)) return asc ? va.n - vb.n : vb.n - va.n;
      if (!isNaN(va.n)) return -1;
      if (!isNaN(vb.n)) return  1;
      return asc ? va.s.localeCompare(vb.s) : vb.s.localeCompare(va.s);
    }});
    rows.forEach(function(r) {{ tbody.appendChild(r); }});
    pinned.forEach(function(r) {{ tbody.appendChild(r); }});
  }}
  // Universal sort — single delegated listener instead of attaching per-th
  // listeners on every table at load. The DOMContentLoaded walk is gone;
  // each table gets initialised lazily on first interaction (hover or click).
  // Saves thousands of addEventListener calls at load time.
  function _initSortableTable(table) {{
    if (table.dataset.sortAttached === '1') return true;  // already done
    if (table.dataset.sortAttached === '0') return false; // marked as not sortable
    if (table.dataset.noSort === '1') {{
      table.dataset.sortAttached = '0'; return false;
    }}
    var headers = table.querySelectorAll('thead th');
    if (!headers.length) {{
      table.dataset.sortAttached = '0'; return false;
    }}
    // Skip tables that already use sortTable(...) handlers (custom onclick)
    var alreadyCustom = Array.from(headers).some(function(h) {{
      return (h.getAttribute('onclick') || '').indexOf('sortTable(') >= 0;
    }});
    if (alreadyCustom) {{
      table.dataset.sortAttached = '0'; return false;
    }}
    headers.forEach(function(th, idx) {{
      th.style.cursor = 'pointer';
      th.style.userSelect = 'none';
      th.dataset.usortColIdx = String(idx);
      var ind = document.createElement('span');
      ind.className = 'usort-ind';
      ind.textContent = ' ▲▼';
      ind.style.opacity = '0.3';
      ind.style.fontSize = '8px';
      ind.style.marginLeft = '3px';
      th.appendChild(ind);
    }});
    table.dataset.sortAttached = '1';
    return true;
  }}
  // Single document-level click listener handles sort for any table that's
  // either already initialised or initialisable on demand.
  function _delegatedSortClick(ev) {{
    var th = ev.target && ev.target.closest && ev.target.closest('th');
    if (!th) return;
    var table = th.closest('table');
    if (!table) return;
    if (!_initSortableTable(table)) return;        // not sortable → ignore
    if (!th.dataset.usortColIdx) return;            // header not in <thead>
    var idx = parseInt(th.dataset.usortColIdx, 10);
    var prev = parseInt(table.dataset.usortLast || '-1', 10);
    var asc = (prev === idx) ? !(table.dataset.usortAsc === '1') : true;
    sortTableByCol(table, idx, asc);
    table.dataset.usortLast = String(idx);
    table.dataset.usortAsc = asc ? '1' : '0';
    table.querySelectorAll('thead th .usort-ind').forEach(function(s, j) {{
      if (j === idx) {{ s.textContent = asc ? ' ▲' : ' ▼'; s.style.opacity = '0.85'; }}
      else           {{ s.textContent = ' ▲▼'; s.style.opacity = '0.3'; }}
    }});
  }}
  // Mouseover initialises the table the first time a user hovers a header,
  // so the ▲▼ indicators appear before the user clicks (preserves affordance).
  function _delegatedSortHover(ev) {{
    var th = ev.target && ev.target.closest && ev.target.closest('th');
    if (!th) return;
    var table = th.closest('table');
    if (table) _initSortableTable(table);
  }}
  function attachUniversalSort() {{
    document.addEventListener('click', _delegatedSortClick);
    document.addEventListener('mouseover', _delegatedSortHover);
  }}
  function injectCsvButtons() {{
    var cards = document.querySelectorAll('section.card, .modal');
    cards.forEach(function(card, idx) {{
      var head = card.querySelector('.card-head, .modal-head');
      if (!head || head.querySelector('.btn-csv')) return;
      if (!card.id) card.id = 'csv-card-' + idx;
      var title = card.querySelector('.card-title, .modal-title');
      var base = title ? title.textContent.trim().replace(/[^a-z0-9]+/gi, '_').toLowerCase() : 'table';
      var btn = document.createElement('button');
      btn.className = 'btn-csv';
      btn.textContent = '⤓ CSV';
      btn.setAttribute('type','button');
      btn.onclick = function(e) {{ e.stopPropagation(); window.exportCardCSV(card.id, base); }};
      head.appendChild(btn);
    }});
  }}
  function _applyDistVisibility(card) {{
    var mode = card.dataset.activeMode   || 'forward';
    var win  = card.dataset.activeWindow || '1';
    card.querySelectorAll('.dist-view[data-mode]').forEach(function(v) {{
      var match = (v.dataset.mode === mode) && ((v.dataset.window || '1') === win);
      v.style.display = match ? '' : 'none';
    }});
    // Show/hide the "5 piores · 5 melhores" button — only relevant on 21d
    card.querySelectorAll('.dist-top21-btn').forEach(function(b) {{
      b.style.display = (win === '21') ? '' : 'none';
    }});
    // Gray-out the Backward button on 21d (combo not relevant — overlay omitted)
    card.querySelectorAll('.dist-btn[data-mode="backward"]').forEach(function(b) {{
      b.classList.toggle('dist-btn-disabled', win === '21');
    }});
  }}
  window.setDistMode = function(cardId, mode) {{
    var card = document.getElementById(cardId);
    if (!card) return;
    // Block backward when window=21
    if (mode === 'backward' && (card.dataset.activeWindow || '1') === '21') return;
    card.dataset.activeMode = mode;
    card.querySelectorAll('.dist-btn[data-mode]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.mode === mode);
    }});
    _applyDistVisibility(card);
  }};
  window.setDistWindow = function(cardId, win) {{
    var card = document.getElementById(cardId);
    if (!card) return;
    card.dataset.activeWindow = win;
    card.querySelectorAll('.dist-btn[data-window]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.window === win);
    }});
    _applyDistVisibility(card);
  }};
  window.openDistTop = function(modalId) {{
    var m = document.getElementById(modalId);
    if (m) m.style.display = '';
  }};
  window.closeDistTop = function(modalId) {{
    var m = document.getElementById(modalId);
    if (m) m.style.display = 'none';
  }};
  window.setDistTopSection = function(modalId, sec) {{
    var m = document.getElementById(modalId);
    if (!m) return;
    m.querySelectorAll('.dist-top-sec-btn').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.sec === sec);
    }});
    m.querySelectorAll('.dist-top-sec').forEach(function(s) {{
      s.classList.toggle('active', s.dataset.sec === sec);
    }});
  }};
  window.setDistBench = function(cardId, bench) {{
    var card = document.getElementById(cardId);
    if (!card) return;
    card.querySelectorAll('.dist-bench-btn').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.bench === bench);
    }});
    card.querySelectorAll('[data-bench-section]').forEach(function(s) {{
      s.style.display = (s.dataset.benchSection === bench) ? '' : 'none';
    }});
    // Reset mode to Forward when switching bench tab — Backward shows realized
    // 252d which doesn't always exist for the comparison/replication views, so
    // Forward is a safer default (avoids landing on an empty / gray-out view).
    card.dataset.activeMode = 'forward';
    card.querySelectorAll('.dist-btn[data-mode]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.mode === 'forward');
    }});
    // Re-apply mode/window visibility so views inside the new bench section
    // show the right Backward/Forward × 1d/21d combo (otherwise all 4 stay hidden).
    _applyDistVisibility(card);
  }};
  // ── PDF export ─────────────────────────────────────────────────────────
  // mode === 'current' → imprime só o que está visível (respeita mode/fund)
  // mode === 'full'    → expande todas as PA trees e mostra todas as seções
  window.exportPdf = function(mode) {{
    var body = document.body;
    if (mode === 'full') body.classList.add('print-full');
    else                 body.classList.add('print-current');
    // We do NOT auto-expand PA trees — that produces dozens of pages of tiny type.
    // User expands manually what they want before clicking; default is level-0 only.
    setTimeout(function() {{
      window.print();
      setTimeout(function() {{
        body.classList.remove('print-full', 'print-current');
      }}, 500);
    }}, 120);
  }};

  // Evolution Diversification — sub-tab toggle (C1 / C2 / C3 / Matriz Direcional)
  window.selectEvoDivView = function(btn, view) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('.evo-div-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.evoDiv === view);
    }});
    card.querySelectorAll('.evo-div-view').forEach(function(v) {{
      v.style.display = (v.dataset.evoDiv === view) ? '' : 'none';
    }});
  }};

  // IDKA exposure — expand/collapse per factor (click on header row)
  window.toggleIdkaFac = function(header, fac) {{
    var tbody = header.closest('tbody');
    if (!tbody) return;
    var children = tbody.querySelectorAll('tr[data-idka-child="' + fac + '"]');
    var caret = header.querySelector('.idka-fac-caret');
    var anyVisible = Array.from(children).some(function(tr) {{
      return tr.style.display !== 'none';
    }});
    children.forEach(function(tr) {{ tr.style.display = anyVisible ? 'none' : ''; }});
    if (caret) caret.textContent = anyVisible ? '▶' : '▼';
  }};
  window.idkaExpandAll = function(btn) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('tr[data-idka-child]').forEach(function(tr) {{ tr.style.display = ''; }});
    card.querySelectorAll('.idka-fac-caret').forEach(function(c) {{ c.textContent = '▼'; }});
  }};
  window.idkaCollapseAll = function(btn) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('tr[data-idka-child]').forEach(function(tr) {{ tr.style.display = 'none'; }});
    card.querySelectorAll('.idka-fac-caret').forEach(function(c) {{ c.textContent = '▶'; }});
  }};

  // IDKA exposure — Bruto / Líquido toggle (shows/hides synthetic rows + factor header spans)
  window.selectIdkaView = function(btn, view) {{
    var card = btn.closest('section.card');
    if (!card) return;
    card.querySelectorAll('.pa-view-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.idkaView === view);
    }});
    // Rows with data-idka-view only visible in their matching view
    card.querySelectorAll('tr[data-idka-view]').forEach(function(tr) {{
      tr.style.display = (tr.getAttribute('data-idka-view') === view) ? '' : 'none';
    }});
    // Factor header spans — each shows its mode's value (bruto / liq-benchmark / liq-replication)
    card.querySelectorAll('[data-idka-span]').forEach(function(el) {{
      el.style.display = (el.getAttribute('data-idka-span') === view) ? '' : 'none';
    }});
  }};

  // Stop history modal (child window) — opened from Risk Budget Monitor
  window.openStopHistory = function() {{
    var m = document.getElementById('stop-history-modal');
    if (m) m.style.display = 'flex';
  }};
  window.closeStopHistory = function() {{
    var m = document.getElementById('stop-history-modal');
    if (m) m.style.display = 'none';
  }};
  window.selectStopPm = function(btn, pm) {{
    var modal = btn.closest('.stop-modal-box');
    if (!modal) return;
    modal.querySelectorAll('.stop-modal-tabs [data-stop-pm]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.stopPm === pm);
    }});
    modal.querySelectorAll('.stop-pm-view').forEach(function(v) {{
      v.style.display = (v.dataset.stopPm === pm) ? '' : 'none';
    }});
  }};
  // Stop history: expand/collapse BOOK-level breakdown for a month row
  window.toggleStopHistRow = function(rowId, cell) {{
    var row = document.getElementById(rowId);
    if (!row) return;
    var opening = row.style.display === 'none';
    row.style.display = opening ? '' : 'none';
    if (cell) {{
      var caret = cell.querySelector('.sh-caret');
      if (caret) caret.textContent = opening ? '▼' : '▶';
    }}
  }};
  // ESC closes modal
  document.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') {{
      var m = document.getElementById('stop-history-modal');
      if (m && m.style.display !== 'none') m.style.display = 'none';
    }}
  }});

  // Summary > Top Movers view toggle (Por Livro / Por Classe)
  window.selectMoversView = function(btn, view) {{
    var card = btn.closest('.sum-movers-card');
    if (!card) return;
    card.querySelectorAll('.sum-tgl .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.movView === view);
    }});
    card.querySelectorAll('.mov-view').forEach(function(t) {{
      t.style.display = (t.dataset.movView === view) ? '' : 'none';
    }});
  }};
  // Top Posições — drill-down (Factor → Instrument → Fund)
  window.toggleTopPos = function(tr) {{
    var path = tr.getAttribute('data-tp-path');
    if (!path) return;
    var table = tr.closest('table'); if (!table) return;
    var direct = table.querySelectorAll('tr[data-tp-parent="' + path + '"]');
    var anyVisible = false;
    direct.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    direct.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    // Collapse grandchildren when collapsing
    if (!willOpen) {{
      var all = table.querySelectorAll('tr[data-tp-parent^="' + path + '|"]');
      all.forEach(function(r) {{ r.style.display = 'none'; }});
      // caret reset on closed children
      all.forEach(function(r) {{
        var c = r.querySelector('.tp-caret'); if (c) c.textContent = '▶';
      }});
    }}
    var caret = tr.querySelector('.tp-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // Breakdown por Fator — Líquido / Bruto toggle (net-of-bench vs gross)
  window.selectRfBrl = function(btn, mode) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-brl-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfBrl === mode);
    }});
    card.querySelectorAll('.rf-brl-body').forEach(function(v) {{
      v.style.display = (v.dataset.rfBrl === mode) ? '' : 'none';
    }});
  }};
  // Distribuição 252d — drill-down (fund row expands livro/rf children)
  window.toggleDistChildren = function(tr) {{
    var key = tr.getAttribute('data-dist-key');
    if (!key) return;
    var table = tr.closest('table'); if (!table) return;
    var children = table.querySelectorAll('tr.dist-row-child[data-dist-parent="' + key + '"]');
    var anyVisible = false;
    children.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    children.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    var caret = tr.querySelector('.dist-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // QUANT exposure — sort positions within each factor (respects hierarchy).
  // Clicking a header toggles direction on repeat click; clicking a different
  // header resets to its default direction.
  window.sortQuantExpoPositions = function(el, mode) {{
    var card = el.closest('.card');
    if (!card) return;
    // If the same header is clicked again, toggle direction between net_desc/asc,
    // or A-Z/Z-A, or gross desc/asc. For simplicity, we track "active" state;
    // repeat click on the active header flips direction where applicable.
    var wasActive = el.classList.contains('qexpo-sort-active');
    var newMode = mode;
    if (wasActive) {{
      if (mode === 'net_desc') newMode = 'net_asc';
      else if (mode === 'net_asc')  newMode = 'net_desc';
      else if (mode === 'gross')    newMode = 'gross_asc';
      else if (mode === 'gross_asc') newMode = 'gross';
      else if (mode === 'name')     newMode = 'name_desc';
      else if (mode === 'name_desc') newMode = 'name';
    }}
    // Clear active state across all headers; mark the clicked one.
    card.querySelectorAll('.qexpo-sort-th').forEach(function(th) {{
      th.classList.remove('qexpo-sort-active');
      var arrow = th.querySelector('.qexpo-sort-arrow');
      if (arrow) arrow.textContent = '';
    }});
    el.classList.add('qexpo-sort-active');
    var arrow = el.querySelector('.qexpo-sort-arrow');
    if (arrow) {{
      if (newMode.indexOf('asc') >= 0 || newMode === 'name_desc') arrow.textContent = '↑';
      else arrow.textContent = '↓';
    }}
    // Sort children
    var tbody = card.querySelector('.qexpo-view[data-qexpo-view="factor"] table tbody');
    if (!tbody) return;
    var parents = tbody.querySelectorAll('tr[data-qexpo-path]');
    parents.forEach(function(p) {{
      var path = p.getAttribute('data-qexpo-path');
      var children = Array.from(tbody.querySelectorAll('tr[data-qexpo-parent="' + path + '"]'));
      children.sort(function(a, b) {{
        var da = parseFloat(a.dataset.sortDelta || '0');
        var db = parseFloat(b.dataset.sortDelta || '0');
        var ga = parseFloat(a.dataset.sortAbs   || '0');
        var gb = parseFloat(b.dataset.sortAbs   || '0');
        if (newMode === 'gross')     return gb - ga;
        if (newMode === 'gross_asc') return ga - gb;
        if (newMode === 'net_desc')  return db - da;
        if (newMode === 'net_asc')   return da - db;
        if (newMode === 'name')      return (a.textContent || '').localeCompare(b.textContent || '');
        if (newMode === 'name_desc') return (b.textContent || '').localeCompare(a.textContent || '');
        return Math.abs(db) - Math.abs(da);  // abs_delta default
      }});
      var anchor = p;
      children.forEach(function(c) {{ anchor.after(c); anchor = c; }});
    }});
  }};
  // QUANT exposure — Por Fator drill-down (click factor to expand positions)
  window.toggleQuantExpoFactor = function(tr) {{
    var path = tr.getAttribute('data-qexpo-path');
    if (!path) return;
    var table = tr.closest('table'); if (!table) return;
    var children = table.querySelectorAll('tr[data-qexpo-parent="' + path + '"]');
    var anyVisible = false;
    children.forEach(function(r) {{ if (r.style.display !== 'none') anyVisible = true; }});
    var willOpen = !anyVisible;
    children.forEach(function(r) {{ r.style.display = willOpen ? '' : 'none'; }});
    var caret = tr.querySelector('.qexpo-caret');
    if (caret) caret.textContent = willOpen ? '▼' : '▶';
  }};
  // QUANT exposure — Por Fator / Por Livro toggle
  window.selectQuantExpoView = function(btn, view) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.pa-view-toggle .pa-tgl').forEach(function(b) {{
      if (b.dataset.qexpoView)
        b.classList.toggle('active', b.dataset.qexpoView === view);
    }});
    card.querySelectorAll('.qexpo-view').forEach(function(v) {{
      v.style.display = (v.dataset.qexpoView === view) ? '' : 'none';
    }});
  }};
  // PA alerts — Por Tamanho / Por Fundo toggle (preserves grid layout)
  window.selectPaAlertSort = function(btn, mode) {{
    var container = btn.closest('.alerts-section') || document;
    container.querySelectorAll('.pa-alert-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.paSort === mode);
    }});
    container.querySelectorAll('.pa-alert-view').forEach(function(v) {{
      v.classList.toggle('pa-alert-view-hidden', v.dataset.paSort !== mode);
    }});
  }};
  // Exposure Map — Ambos/Real/Nominal factor filter. Bench bars always visible.
  window.selectRfView = function(btn, view) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-view-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfView === view);
    }});
    card.dataset.rfFactor = view;
    _rfApplyVisibility(card);
  }};
  // Exposure Map — Absoluto / Relativo mode toggle
  window.selectRfMode = function(btn, mode) {{
    var card = btn.closest('.card');
    if (!card) return;
    card.querySelectorAll('.rf-mode-toggle .pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.rfMode === mode);
    }});
    card.dataset.rfMode = mode;
    _rfApplyVisibility(card);
  }};
  function _rfApplyVisibility(card) {{
    var factor = card.dataset.rfFactor || 'both';
    var mode   = card.dataset.rfMode   || 'absoluto';
    card.querySelectorAll('.rf-expo-svg .rf-mode-group').forEach(function(g) {{
      g.style.display = (g.getAttribute('data-rf-mode') === mode) ? '' : 'none';
    }});
    card.querySelectorAll('.rf-expo-svg .rf-mode-group [data-factor]').forEach(function(el) {{
      var f = el.getAttribute('data-factor');
      if (f === 'bench') {{ el.style.display = ''; return; }}
      el.style.display = (factor === 'both' || f === factor) ? '' : 'none';
    }});
  }}
  // Expand/collapse the detail table under an Exposure Map card
  window.toggleRfTable = function(btn) {{
    var wrap = btn.nextElementSibling;
    if (!wrap) return;
    var open = wrap.style.display !== 'none';
    wrap.style.display = open ? 'none' : '';
    btn.textContent = (open ? '▸ Mostrar tabela' : '▾ Esconder tabela');
    btn.setAttribute('aria-expanded', open ? 'false' : 'true');
  }};
  // PA view toggle (Por Classe / Por Livro) inside a PA card
  window.selectPaView = function(btn, viewId) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    // Clear search when switching views
    var srch = card.querySelector('.pa-search');
    if (srch) srch.value = '';
    card.querySelectorAll('.pa-tgl').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.paView === viewId);
    }});
    card.querySelectorAll('.pa-view').forEach(function(v) {{
      v.style.display = (v.dataset.paView === viewId) ? '' : 'none';
    }});
  }};
  window.filterPa = function(input) {{
    var q = input.value.trim().toLowerCase();
    var card = input.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    if (!q) {{
      // Reset to root-only visible state
      view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
        tr.style.display = (parseInt(tr.dataset.level || '0') === 0) ? '' : 'none';
        tr.classList.remove('expanded');
      }});
      return;
    }}
    // Force-render all lazy children (expand any not-yet-rendered has-children)
    for (var guard = 0; guard < 50; guard++) {{
      var pending = Array.prototype.filter.call(
        view.querySelectorAll('tr.pa-has-children'),
        function(tr) {{ return tr.dataset.rendered !== '1'; }}
      );
      if (pending.length === 0) break;
      pending.forEach(function(tr) {{
        if (!tr.classList.contains('expanded')) window.togglePaRow(tr);
      }});
    }}
    // Build path → tr map for ancestor lookup
    var pathMap = {{}};
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      if (tr.dataset.path) pathMap[tr.dataset.path] = tr;
    }});
    // Find matching paths + all their ancestors
    var show = {{}};
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      var lbl = tr.querySelector('.pa-label');
      if (!lbl) return;
      if (lbl.textContent.trim().toLowerCase().indexOf(q) >= 0) {{
        var p = tr.dataset.path;
        while (p) {{
          show[p] = true;
          var anc = pathMap[p];
          p = anc ? anc.dataset.parent : '';
        }}
      }}
    }});
    // Apply visibility
    view.querySelectorAll('tr.pa-row').forEach(function(tr) {{
      tr.style.display = show[tr.dataset.path] ? '' : 'none';
    }});
  }};
  // ── PA lazy render helpers ─────────────────────────────────────────────
  function paDataFor(view) {{
    var id = view.dataset.paId;
    if (!id) return null;
    var cached = view._paData;
    if (cached) return cached;
    var s = document.getElementById(id);
    if (!s) return null;
    try {{ view._paData = JSON.parse(s.textContent); return view._paData; }}
    catch (e) {{ console.error('PA JSON parse failed', e); return null; }}
  }}
  function paEsc(s) {{
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }}
  function paPctCell(v, maxAbs) {{
    var pct = v / 100.0;
    if (Math.abs(pct) < 0.005) {{
      return '<td class="t-num mono" style="color:var(--muted)">—</td>';
    }}
    var color = v >= 0 ? 'var(--up)' : 'var(--down)';
    var rgb   = v >= 0 ? '38,208,124' : '255,90,106';
    var alpha = (maxAbs > 0) ? Math.min(Math.abs(v) / maxAbs, 1.0) * 0.14 : 0;
    var sign  = v >= 0 ? '+' : '';
    return '<td class="t-num mono" style="color:' + color
         + '; background:rgba(' + rgb + ',' + alpha.toFixed(2) + ')">'
         + sign + pct.toFixed(2) + '%</td>';
  }}
  function paPosCell(v) {{
    if (Math.abs(v) < 1e5) {{
      return '<td class="t-num mono pa-pos" style="color:var(--muted)">—</td>';
    }}
    var m = v / 1e6;
    var s = m.toLocaleString('pt-BR', {{minimumFractionDigits:1, maximumFractionDigits:1}});
    return '<td class="t-num mono pa-pos" style="color:var(--muted)">' + s + '</td>';
  }}
  function paRenderRow(node, maxAbs) {{
    var levelCls  = 'pa-l' + node.dp;
    var pinnedCls = node.pi ? ' pa-pinned' : '';
    var expander  = node.hc
      ? '<span class="pa-exp" aria-hidden="true">▸</span>'
      : '<span class="pa-exp pa-exp-empty" aria-hidden="true"></span>';
    var baseCls   = 'pa-row ' + levelCls + pinnedCls;
    var clsClick  = node.hc
      ? ' onclick="togglePaRow(this)" class="' + baseCls + ' pa-has-children"'
      : ' class="' + baseCls + '"';
    var pinAttr   = node.pi ? ' data-pinned="1"' : '';
    var attrs = ' data-level="' + node.dp + '" data-path="' + paEsc(node.pa)
              + '" data-parent="' + paEsc(node.pr) + '"' + pinAttr;
    var cells = '<td class="pa-name">' + expander
              + '<span class="pa-label">' + paEsc(node.d) + '</span></td>'
              + paPctCell(node.a[0], maxAbs[0])
              + paPctCell(node.a[1], maxAbs[1])
              + paPctCell(node.a[2], maxAbs[2])
              + paPctCell(node.a[3], maxAbs[3]);
    return '<tr' + attrs + clsClick + '>' + cells + '</tr>';
  }}
  function paRenderChildren(view, parentTr) {{
    if (parentTr.dataset.rendered === '1') return;
    var data = paDataFor(view);
    if (!data) return;
    var kids = (data.byParent || {{}})[parentTr.dataset.path] || [];
    var ordered = kids;
    // Only re-sort if user has explicitly clicked a sort header.
    // JSON default order already honours the server-side Excel-inspired order.
    if (view.dataset.userSorted === '1') {{
      var idx  = parseInt(view.dataset.sortIdx  || '2', 10);
      var desc = (view.dataset.sortDesc || '1') === '1';
      var reg = kids.filter(function(k) {{ return !k.pi; }});
      var pin = kids.filter(function(k) {{ return k.pi; }});
      reg.sort(function(a, b) {{
        var va = a.a[idx], vb = b.a[idx];
        var aZ = Math.abs(va) < 1e-6, bZ = Math.abs(vb) < 1e-6;
        if (aZ && bZ) return 0;
        if (aZ) return 1;
        if (bZ) return -1;
        return desc ? (vb - va) : (va - vb);
      }});
      ordered = reg.concat(pin);
    }}
    var html = ordered.map(function(k) {{ return paRenderRow(k, data.maxAbs); }}).join('');
    parentTr.insertAdjacentHTML('afterend', html);
    parentTr.dataset.rendered = '1';
  }}

  // PA tree expand/collapse (lazy)
  window.togglePaRow = function(tr) {{
    if (!tr) return;
    var view = tr.closest('.pa-view');
    if (!view) return;
    var path = tr.dataset.path;
    var willExpand = !tr.classList.contains('expanded');
    if (willExpand) paRenderChildren(view, tr);
    tr.classList.toggle('expanded', willExpand);
    var sel = 'tr[data-parent="' + (window.CSS && CSS.escape ? CSS.escape(path) : path) + '"]';
    view.querySelectorAll(sel).forEach(function(c) {{
      if (willExpand) {{
        c.style.display = '';
      }} else {{
        c.style.display = 'none';
        if (c.classList.contains('expanded')) window.togglePaRow(c);
      }}
    }});
  }};
  // Expand every parent in the currently-active view (recursive, lazy-renders as it goes)
  window.expandAllPa = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    // Loop: keep expanding until no collapsed parents remain visible
    for (var guard = 0; guard < 50; guard++) {{
      var pending = view.querySelectorAll('tr.pa-has-children:not(.expanded)');
      if (pending.length === 0) break;
      pending.forEach(function(tr) {{
        if (tr.style.display !== 'none') window.togglePaRow(tr);
      }});
    }}
  }};
  window.collapseAllPa = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    // Collapse top-level expanded rows; togglePaRow recursively collapses descendants.
    view.querySelectorAll('tr.pa-has-children.expanded[data-level="0"]').forEach(function(tr) {{
      window.togglePaRow(tr);
    }});
  }};
  // PA per-metric sort — preserves tree hierarchy (sorts siblings under each parent)
  window.sortPaMetric = function(th, idx) {{
    var view  = th.closest('.pa-view');
    if (!view) return;
    var tbody = view.querySelector('tbody');
    var curIdx  = parseInt(view.dataset.sortIdx || '2');
    var curDesc = (view.dataset.sortDesc || '1') === '1';
    var desc = (curIdx === idx) ? !curDesc : true;
    view.dataset.sortIdx   = String(idx);
    view.dataset.sortDesc  = desc ? '1' : '0';
    view.dataset.userSorted = '1';

    // Update arrow markers
    view.querySelectorAll('th.pa-sortable').forEach(function(h) {{
      h.classList.remove('pa-sort-active');
      var a = h.querySelector('.pa-sort-arrow');
      if (a) a.remove();
    }});
    th.classList.add('pa-sort-active');
    var arrow = document.createElement('span');
    arrow.className = 'pa-sort-arrow';
    arrow.textContent = desc ? ' ▾' : ' ▴';
    th.appendChild(arrow);

    function parseCell(tr, metricIdx) {{
      var cell = tr.children[1 + metricIdx];
      if (!cell) return 0;
      var t = cell.textContent.trim();
      if (t === '—' || t === '') return 0;
      return parseFloat(t.replace('+','').replace('%','').replace(',','.')) || 0;
    }}

    // group rows by parent path, separating pinned-bottom rows (Caixa/Custos/...)
    var byParentReg = {{}}, byParentPin = {{}};
    Array.prototype.forEach.call(tbody.children, function(tr) {{
      var p = tr.dataset.parent || '';
      if (tr.dataset.pinned === '1') {{
        (byParentPin[p] = byParentPin[p] || []).push(tr);
      }} else {{
        (byParentReg[p] = byParentReg[p] || []).push(tr);
      }}
    }});
    // sort regular siblings: positives → negatives → zeros, signed
    function paCmp(va, vb) {{
      var aZ = Math.abs(va) < 1e-6, bZ = Math.abs(vb) < 1e-6;
      if (aZ && bZ) return 0;
      if (aZ) return 1;   // zeros always last
      if (bZ) return -1;
      return desc ? (vb - va) : (va - vb);
    }}
    Object.keys(byParentReg).forEach(function(p) {{
      byParentReg[p].sort(function(a, b) {{
        return paCmp(parseCell(a, idx), parseCell(b, idx));
      }});
    }});
    // merge: regular first, pinned always last (not sorted)
    var byParent = {{}};
    var allKeys = new Set([].concat(Object.keys(byParentReg), Object.keys(byParentPin)));
    allKeys.forEach(function(p) {{
      byParent[p] = (byParentReg[p] || []).concat(byParentPin[p] || []);
    }});
    // DFS reconstruction
    var ordered = [];
    function visit(parentPath) {{
      (byParent[parentPath] || []).forEach(function(tr) {{
        ordered.push(tr);
        visit(tr.dataset.path);
      }});
    }}
    visit('');
    ordered.forEach(function(tr) {{ tbody.appendChild(tr); }});
  }};

  // PA sort reset — restores server-side default order (YTD desc, Excel-inspired)
  window.resetPaSort = function(btn) {{
    var card = btn.closest('.pa-card');
    if (!card) return;
    var view = Array.prototype.find.call(card.querySelectorAll('.pa-view'),
      function(v) {{ return v.style.display !== 'none'; }});
    if (!view) return;
    var tbody = view.querySelector('tbody');
    if (!tbody) return;
    var data = paDataFor(view);
    if (!data || !data.byParent) return;

    // Reset state markers
    view.dataset.userSorted = '0';
    view.dataset.sortIdx    = '2';
    view.dataset.sortDesc   = '1';

    // Reset headers: clear all arrows + active class, then mark YTD (idx=2) as default
    view.querySelectorAll('th.pa-sortable').forEach(function(h) {{
      h.classList.remove('pa-sort-active');
      var a = h.querySelector('.pa-sort-arrow');
      if (a) a.remove();
    }});
    var defTh = view.querySelector('th.pa-sortable[data-pa-metric="2"]');
    if (defTh) {{
      defTh.classList.add('pa-sort-active');
      var arrow = document.createElement('span');
      arrow.className = 'pa-sort-arrow';
      arrow.textContent = ' ▾';
      defTh.appendChild(arrow);
    }}

    // Walk the JSON tree DFS and reorder existing rendered rows
    // (non-rendered descendants don't exist in DOM yet — no action needed).
    var rowByPath = {{}};
    Array.prototype.forEach.call(tbody.children, function(tr) {{
      rowByPath[tr.dataset.path] = tr;
    }});
    var ordered = [];
    function visitDef(parentPath) {{
      var kids = data.byParent[parentPath] || [];
      kids.forEach(function(k) {{
        var tr = rowByPath[k.pa];
        if (tr) {{
          ordered.push(tr);
          visitDef(k.pa);
        }}
      }});
    }}
    visitDef('');
    ordered.forEach(function(tr) {{ tbody.appendChild(tr); }});
  }};
}})();
</script>"""


# ── Cards data JS (PnL + peers; extracted from generate_risk_report.py) ──
# Builder function that injects three JSON blobs as JS constants.
def cards_data_js(*, _book_pnl_json, _peers_json, _peers_eopm_json):
    return f"""<script>
// ── P&L + Peers data (baked at report generation time) ──────────────────────
const _RPT_PNL        = {_book_pnl_json};
// Peers tem dois snapshots: 'current' = data do relatório (closest ≤ DATA_STR);
// 'eopm' = fim do mês anterior (apples-to-apples, peers reportam mensalmente).
// Toggle no UI alterna qual está ativo via window._RPT_PEERS = ...
const _RPT_PEERS_CURRENT = {_peers_json};
const _RPT_PEERS_EOPM    = {_peers_eopm_json};
let   _RPT_PEERS         = _RPT_PEERS_CURRENT;
window._RPT_PEERS_CURRENT = _RPT_PEERS_CURRENT;
window._RPT_PEERS_EOPM    = _RPT_PEERS_EOPM;
window._RPT_PEERS         = _RPT_PEERS;

// ── PnL helpers ──────────────────────────────────────────────────────────────
(function() {{
  function fmtPnl(v) {{
    if (v == null || isNaN(v)) return '<span style="color:var(--muted)">—</span>';
    var cls = v > 0 ? 'var(--up)' : v < 0 ? 'var(--down)' : 'var(--muted)';
    return '<span style="color:' + cls + ';font-family:monospace">' + (v>=0?'+':'') +
           v.toLocaleString('pt-BR',{{minimumFractionDigits:0,maximumFractionDigits:0}}) + '</span>';
  }}
  function fmtBps(pct) {{
    if (pct == null || isNaN(pct)) return '<span style="color:var(--muted)">—</span>';
    var bps = pct * 10000;
    var cls = bps > 0 ? 'var(--up)' : bps < 0 ? 'var(--down)' : 'var(--muted)';
    return '<span style="color:' + cls + ';font-family:monospace">' + (bps>=0?'+':'') + bps.toFixed(1) + 'bp</span>';
  }}
  var BOOK_GROUPS = [
    {{id:'RJ',    label:'RJ',         pfx:'RJ'}},
    {{id:'JD',    label:'JD',         pfx:'JD'}},
    {{id:'LF',    label:'LF',         pfx:'LF'}},
    {{id:'CI',    label:'CI',         pfx:'CI'}},
    {{id:'quant', label:'Quant',      pfx:'SIST', names:['BRACCO','QUANT_PA']}},
    {{id:'eqbr',  label:'Equities BR',names:['FLO'], contains:['AÇÕES','ACOES','FMN']}},
    {{id:'credit',label:'Credit',     contains:['CRED']}},
  ];
  function bookGroup(name) {{
    if (!name) return null;
    var up = name.toUpperCase();
    for (var i=0; i<BOOK_GROUPS.length; i++) {{
      var d = BOOK_GROUPS[i];
      if (d.pfx      && up.startsWith(d.pfx))                return d;
      if (d.names    && d.names.some(function(n){{return up===n;}}))           return d;
      if (d.contains && d.contains.some(function(k){{return up.includes(k);}})) return d;
    }}
    return null;
  }}
  var FUND_ORDER_PNL = ['Macro','Evolution','Albatroz','Quantitativo','Macro Q','Frontier','Frontier LB'];
  var _pnlState = {{}};

  function renderPnlFund(container, fk, fd) {{
    var st = _pnlState[fk] || (_pnlState[fk] = {{open:false, books:{{}}, bgroups:{{}}}});
    var fDiv = document.createElement('div');
    fDiv.style.cssText = 'margin-bottom:10px;border-radius:6px;overflow:hidden;border:1px solid var(--line)';
    var hdr = document.createElement('div');
    hdr.style.cssText = 'display:flex;align-items:center;gap:10px;padding:10px 14px;cursor:pointer;background:var(--panel);user-select:none';
    hdr.innerHTML = '<span style="font-size:10px;color:var(--muted);transition:transform .15s">' + (st.open?'▼':'▶') + '</span>' +
      '<span style="font-weight:600;flex:1">' + fk + '</span>' +
      '<span style="font-size:11px;color:var(--muted)">' + fmtPnl(fd.total_pl) + ' &nbsp;|&nbsp; ' + fmtBps(fd.total_pl_pct) + '</span>';
    hdr.onclick = function() {{
      st.open = !st.open;
      hdr.querySelector('span').textContent = st.open ? '▼' : '▶';
      body.style.display = st.open ? '' : 'none';
    }};
    var body = document.createElement('div');
    body.style.display = st.open ? '' : 'none';
    var tbl = document.createElement('table');
    tbl.style.cssText = 'width:100%;border-collapse:collapse;font-size:12px';
    tbl.innerHTML = '<thead><tr style="background:var(--bg-2)">' +
      '<th style="text-align:left;padding:6px 10px;border-bottom:1px solid var(--line);font-size:11px;color:var(--muted);font-weight:500;min-width:220px">Book / Classe / Posição</th>' +
      '<th style="padding:6px 10px;border-bottom:1px solid var(--line);font-size:11px;color:var(--muted);font-weight:500;text-align:right">P&L (BRL)</th>' +
      '<th style="padding:6px 10px;border-bottom:1px solid var(--line);font-size:11px;color:var(--muted);font-weight:500;text-align:right">P&L (bp)</th>' +
      '<th style="padding:6px 10px;border-bottom:1px solid var(--line);font-size:11px;color:var(--muted);font-weight:500;text-align:right">Pos P&L</th>' +
      '<th style="padding:6px 10px;border-bottom:1px solid var(--line);font-size:11px;color:var(--muted);font-weight:500;text-align:right">Trade P&L</th>' +
      '</tr></thead>';
    var tbody = document.createElement('tbody');
    tbl.appendChild(tbody);
    var books = fd.books || [];
    var grpMap = {{}}, ungrouped = [];
    books.filter(function(b){{return b.pl!==0&&b.pl!=null;}}).forEach(function(b) {{
      var g = bookGroup(b.book);
      if (g) {{ if (!grpMap[g.id]) grpMap[g.id]={{def:g,books:[]}}; grpMap[g.id].books.push(b); }}
      else ungrouped.push(b);
    }});
    function tdStyle(extra) {{ return 'style="padding:5px 10px;border-bottom:1px solid var(--line);' + (extra||'') + '"'; }}
    function appendBook(b, grpOpen) {{
      var bSt = st.books[b.book] || (st.books[b.book] = {{open:false,classes:{{}}}});
      var bRow = document.createElement('tr');
      bRow.style.cursor = 'pointer';
      bRow.style.display = (grpOpen===false) ? 'none' : '';
      bRow.dataset.bookKey = fk+'|'+b.book;
      bRow.innerHTML = '<td '+tdStyle()+'><span style="display:inline-block;width:12px;font-size:10px;color:var(--muted)">' + (bSt.open?'▼':'▶') + '</span> <strong>' + b.book + '</strong></td>' +
        '<td '+tdStyle('text-align:right')+'>' + fmtPnl(b.pl) + '</td>' +
        '<td '+tdStyle('text-align:right')+'>' + fmtBps(b.pl_pct) + '</td>' +
        '<td '+tdStyle('text-align:right;color:var(--muted)')+'> — </td><td '+tdStyle('text-align:right;color:var(--muted)')+'> — </td>';
      bRow.onclick = function() {{
        bSt.open = !bSt.open;
        bRow.querySelector('span').textContent = bSt.open ? '▼' : '▶';
        tbody.querySelectorAll('[data-parent-book="' + fk+'|'+b.book + '"]').forEach(function(r) {{
          r.style.display = bSt.open ? '' : 'none';
          if (!bSt.open && r.classList.contains('cls-row')) {{
            r.querySelector('span').textContent = '▶';
            var ck = r.dataset.classKey;
            if (ck) {{ bSt.classes[ck]=false; tbody.querySelectorAll('[data-parent-class="'+ck+'"]').forEach(function(pr){{pr.style.display='none';}}); }}
          }}
        }});
      }};
      tbody.appendChild(bRow);
      (b.classes||[]).forEach(function(cls,ci) {{
        var ck = fk+'|'+b.book+'|'+ci;
        var cOpen = bSt.classes[ck]||false;
        var cRow = document.createElement('tr');
        cRow.className = 'cls-row';
        cRow.dataset.parentBook = fk+'|'+b.book;
        cRow.dataset.classKey   = ck;
        cRow.style.cssText = 'cursor:pointer;display:' + (bSt.open?(grpOpen===false?'none':''):'none');
        cRow.innerHTML = '<td '+tdStyle('padding-left:28px')+'><span style="display:inline-block;width:12px;font-size:10px;color:var(--muted)">'+(cOpen?'▼':'▶')+'</span> '+cls['class']+'</td>' +
          '<td '+tdStyle('text-align:right')+'>' + fmtPnl(cls.pl) + '</td>' +
          '<td '+tdStyle('text-align:right')+'>' + fmtBps(cls.pl_pct) + '</td>' +
          '<td '+tdStyle('text-align:right;color:var(--muted)')+'> — </td><td '+tdStyle('text-align:right;color:var(--muted)')+'> — </td>';
        cRow.onclick = function() {{
          bSt.classes[ck] = !bSt.classes[ck];
          cRow.querySelector('span').textContent = bSt.classes[ck] ? '▼' : '▶';
          tbody.querySelectorAll('[data-parent-class="'+ck+'"]').forEach(function(pr) {{ pr.style.display = bSt.classes[ck] ? '' : 'none'; }});
        }};
        tbody.appendChild(cRow);
        (cls.positions||[]).forEach(function(pos) {{
          var pRow = document.createElement('tr');
          pRow.dataset.parentClass = ck;
          pRow.dataset.parentBook  = fk+'|'+b.book;
          pRow.style.display = 'none';
          pRow.innerHTML = '<td '+tdStyle('padding-left:42px;color:var(--muted);font-size:11px')+' title="'+pos.desc+'">'+(pos.desc.length>45?pos.desc.slice(0,44)+'…':pos.desc)+'</td>' +
            '<td '+tdStyle('text-align:right')+'>' + fmtPnl(pos.pl) + '</td>' +
            '<td '+tdStyle('text-align:right')+'>' + fmtBps(pos.pl_pct) + '</td>' +
            '<td '+tdStyle('text-align:right')+'>' + fmtPnl(pos.pos_pl) + '</td>' +
            '<td '+tdStyle('text-align:right')+'>' + fmtPnl(pos.trade_pl) + '</td>';
          tbody.appendChild(pRow);
        }});
      }});
    }}
    BOOK_GROUPS.forEach(function(def) {{
      var bucket = grpMap[def.id];
      if (!bucket||!bucket.books.length) return;
      var gOpen = st.bgroups[def.id]!==undefined ? st.bgroups[def.id] : false;
      var grpPl = bucket.books.reduce(function(s,b){{return s+(b.pl||0);}},0);
      // Sum (not average) — pl_pct is PL/fund_NAV per book, additive across books.
      var grpPct = bucket.books.reduce(function(s,b){{return s+(b.pl_pct||0);}},0);
      var gRow = document.createElement('tr');
      gRow.style.cssText = 'cursor:pointer;background:var(--bg-2)';
      gRow.innerHTML = '<td style="padding:5px 10px;border-top:1px solid var(--line);border-bottom:1px solid var(--line)">' +
        '<span style="display:inline-block;width:12px;font-size:10px;color:var(--muted)">'+(gOpen?'▼':'▶')+'</span> <b>'+def.label+'</b> ' +
        '<span style="font-size:10px;opacity:.5;margin-left:4px">'+bucket.books.length+' books</span></td>' +
        '<td style="padding:5px 10px;border-top:1px solid var(--line);border-bottom:1px solid var(--line);text-align:right">' + fmtPnl(grpPl) + '</td>' +
        '<td style="padding:5px 10px;border-top:1px solid var(--line);border-bottom:1px solid var(--line);text-align:right">' + fmtBps(grpPct) + '</td>' +
        '<td></td><td></td>';
      gRow.onclick = function() {{
        st.bgroups[def.id] = !gOpen;
        // re-render this fund block
        var ctr = document.getElementById('rpt-pnl-container');
        var oldDiv = ctr && ctr.querySelector('[data-fund-pnl="'+fk+'"]');
        if (oldDiv && _RPT_PNL.funds && _RPT_PNL.funds[fk]) {{
          var tmp = document.createElement('div'); renderPnlFund(tmp,fk,_RPT_PNL.funds[fk]); oldDiv.replaceWith(tmp.firstChild);
        }}
      }};
      tbody.appendChild(gRow);
      bucket.books.forEach(function(b) {{ appendBook(b, gOpen); }});
    }});
    ungrouped.forEach(function(b) {{ appendBook(b, true); }});
    body.appendChild(tbl);
    fDiv.dataset.fundPnl = fk;
    fDiv.appendChild(hdr);
    fDiv.appendChild(body);
    container.appendChild(fDiv);
  }}

  function renderAll(data) {{
    var ctr = document.getElementById('rpt-pnl-container');
    if (!ctr) return;
    var funds = (data && data.funds) || {{}};
    if (!Object.keys(funds).length) {{
      ctr.innerHTML = '<div style="padding:24px;color:var(--muted)">Sem dados P&L.</div>';
      return;
    }}
    _pnlState = {{}};
    ctr.innerHTML = '';
    var keys = FUND_ORDER_PNL.filter(function(k){{return funds[k];}})
               .concat(Object.keys(funds).filter(function(k){{return !FUND_ORDER_PNL.includes(k);}}));
    keys.forEach(function(fk) {{ renderPnlFund(ctr, fk, funds[fk]); }});
  }}
  window.initRptPnl = function() {{ renderAll(_RPT_PNL); }};
  window.rptPnlRender = renderAll;
}})();

window.refreshRptPnl = function() {{
  var btn  = document.getElementById('rpt-pnl-refresh-btn');
  var meta = document.getElementById('rpt-pnl-meta');
  if (btn) {{ btn.disabled = true; btn.textContent = '↻ …'; }}
  fetch('http://localhost:5050/api/pnl')
    .then(function(r) {{ if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }})
    .then(function(data) {{
      if (meta) meta.textContent = '— ' + (data.val_date || '') + ' · ao vivo (' + data._source + ')';
      if (window.rptPnlRender) window.rptPnlRender(data);
    }})
    .catch(function(e) {{
      if (meta) meta.textContent = '— erro ao atualizar: ' + e.message;
    }})
    .finally(function() {{
      if (btn) {{ btn.disabled = false; btn.textContent = '↻ Atualizar'; }}
    }});
}};

// ── Peers helpers ─────────────────────────────────────────────────────────────
(function() {{
  var _mode    = 'abs';
  var _sortKey = '12M', _sortDir = 'desc';
  var _rptWin  = '12M';   // period for charts (independent of table sort)
  var _rptView = 'charts'; // 'charts' | 'table'

  function cleanName(n) {{
    if (!n) return '—';
    var tokens = /\\b(FIC|FIF|FIM|FICFIM|FICFIA|FIA|FII|FIP|Feeder|Multimercado|Resp(?:onsabilidade)?\\s*Limitada|R\\.?L\\.?|S\\.?A\\.?|Crédito\\s*Privado|CP|IE|Invest(?:imento)?s?|Fundo\\s*de\\s*Investimento)\\b\\.?/gi;
    return n.replace(tokens,'').replace(/^GALAPAGOS\\s+/i,'GLPG ').replace(/\\s{{2,}}/g,' ').trim() || n;
  }}
  function truncName(s, maxLen) {{
    return s.length <= maxLen ? s : s.slice(0, maxLen-1) + '…';
  }}
  function _isGlpgFundRpt(r, groupFundName) {{
    if (r.is_fund) return true;
    if (groupFundName) return (r.name||'').toUpperCase() === groupFundName.toUpperCase();
    return false;
  }}

  // dv: display value (absolute or alpha-adjusted)
  function dv(row, col, bm) {{
    var raw = row[col];
    if (_mode !== 'alpha' || !bm || bm[col] == null || raw == null) return raw;
    return raw - bm[col];
  }}

  function retCell(row, col, colMax, isSort, bm) {{
    var v  = dv(row, col, bm);
    var sc = isSort ? ' background:rgba(90,163,232,.04)' : '';
    if (v==null||isNaN(v)) return '<td class="mono" style="text-align:right;padding:7px 12px;white-space:nowrap'+sc+'">—</td>';
    var pct  = v*100;
    var w    = Math.min((Math.abs(v)/(colMax||0.001))*50,50);
    var bar  = v>=0 ? 'left:50%;width:'+w+'%' : 'right:50%;width:'+w+'%';
    var col2 = v>=0 ? '#26a65b' : '#e74c3c';
    var lbl  = (v>=0?'+':'')+pct.toFixed(2)+'%';
    return '<td class="ret-cell" style="padding:6px 12px;min-width:110px'+sc+'">' +
      '<div style="position:relative;display:flex;align-items:center;justify-content:flex-end;min-width:80px;height:20px">' +
      '<div style="position:absolute;left:50%;top:0;bottom:0;width:1px;background:var(--line)"></div>' +
      '<div style="position:absolute;top:3px;bottom:3px;border-radius:2px;opacity:.35;background:'+col2+';'+bar+'"></div>' +
      '<span style="position:relative;font-size:11px;font-weight:600;font-family:monospace;color:'+col2+'">'+lbl+'</span>' +
      '</div></td>';
  }}
  function volCell(v, isSort) {{
    var sc = isSort ? ' background:rgba(90,163,232,.04)' : '';
    return '<td class="mono" style="text-align:right;font-size:11px;color:var(--muted);padding:6px 12px'+sc+'">' +
      (v!=null&&!isNaN(v) ? (v*100).toFixed(1)+'%' : '—') + '</td>';
  }}

  function renderTable(tbl, groupKey) {{
    if (!tbl||!_RPT_PEERS||!_RPT_PEERS.groups) return;
    var g = _RPT_PEERS.groups[groupKey];
    if (!g) {{ tbl.innerHTML='<tbody><tr><td style="padding:16px;color:var(--muted)">Grupo não disponível.</td></tr></tbody>'; return; }}
    var retCols    = ['MTD','YTD','12M','24M'].concat(groupKey==='FRONTIER'?['36M']:[]);
    var alphaBmKey = g.alpha_bench || (groupKey==='FRONTIER'?'IBOVESPA':'CDI');
    var bm         = (_RPT_PEERS.benchmarks||{{}})[alphaBmKey];
    var peers      = (g.peers||[]).slice().sort(function(a,b) {{
      function val(r) {{ var v=dv(r,_sortKey,bm); return (v!=null&&!isNaN(v))?v:(_sortDir==='desc'?-Infinity:Infinity); }}
      return _sortDir==='desc' ? val(b)-val(a) : val(a)-val(b);
    }});
    var benches    = (g.benchmarks||['CDI']).map(function(k){{return _RPT_PEERS.benchmarks&&_RPT_PEERS.benchmarks[k];}}).filter(Boolean);
    var colMax     = {{}};
    retCols.forEach(function(c) {{
      var vals = peers.concat(benches).map(function(r){{return dv(r,c,bm);}}).filter(function(v){{return v!=null&&!isNaN(v);}});
      colMax[c] = Math.max.apply(null, vals.map(Math.abs).concat([0.001]));
    }});
    function mkTh(label,key) {{
      var is=key===_sortKey, arrow=is?(_sortDir==='desc'?'▼':'▲'):'';
      return '<th style="background:var(--bg-2);padding:7px 12px;text-align:right;border-bottom:1px solid var(--line);font-size:11px;color:'+(is?'var(--accent)':'var(--muted)')+';font-weight:500;white-space:nowrap;cursor:pointer" data-sort-key="'+key+'">'+label+'<span>'+arrow+'</span></th>';
    }}
    var head = '<thead><tr><th style="background:var(--bg-2);padding:7px 12px;text-align:left;border-bottom:1px solid var(--line);font-size:11px;color:var(--muted);font-weight:500">Fundo</th>' +
      retCols.map(function(c){{return mkTh(c,c);}}).join('') + mkTh('Vol','Vol') + '</tr></thead>';
    var groupFundName = (g.fund_name||'').toUpperCase();
    function mkRow(r,isFund) {{
      var bg = isFund ? 'background:rgba(255,209,102,.06);' : '';
      var bl = isFund ? 'border-left:3px solid rgba(255,193,7,.55);' : '';
      return '<tr style="'+bg+'"><td style="text-align:left;padding:6px 12px;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'+bl+'">'+cleanName(r.name||'—')+'</td>' +
        retCols.map(function(c){{return retCell(r,c,colMax[c],c===_sortKey,bm);}}).join('') + volCell(r.Vol,'Vol'===_sortKey) + '</tr>';
    }}
    var rows = peers.map(function(r){{return mkRow(r,_isGlpgFundRpt(r,groupFundName));}}).join('') +
               benches.map(function(r){{return mkRow(r,false);}}).join('');
    tbl.innerHTML = head + '<tbody>' + rows + '</tbody>';
    tbl.querySelectorAll('th[data-sort-key]').forEach(function(th) {{
      th.addEventListener('click', function() {{
        var k = th.dataset.sortKey;
        if (_sortKey===k) _sortDir=_sortDir==='desc'?'asc':'desc';
        else {{ _sortKey=k; _sortDir='desc'; }}
        renderAllPeersTables();
      }});
    }});
  }}

  function _renderPeersCharts(groupKey) {{
    var wrap = document.getElementById('rpt-peers-charts');
    if (!wrap||!_RPT_PEERS||!_RPT_PEERS.groups) return;
    var g = _RPT_PEERS.groups[groupKey];
    if (!g) {{ wrap.innerHTML=''; return; }}
    var alphaBmKey   = g.alpha_bench || (groupKey==='FRONTIER'?'IBOVESPA':'CDI');
    var bmForAlpha   = (_RPT_PEERS.benchmarks||{{}})[alphaBmKey];
    var groupFundName= (g.fund_name||'').toUpperCase();
    var isAlpha      = _mode === 'alpha';
    var win          = _rptWin;
    var suffix       = '%';
    var benches      = (g.benchmarks||['CDI']).map(function(k){{return _RPT_PEERS.benchmarks&&_RPT_PEERS.benchmarks[k];}}).filter(Boolean).map(function(b){{return Object.assign({{}},b,{{is_bench:true}});}});
    var allRows      = (g.peers||[]).concat(benches);

    function dvChart(row, col) {{
      var raw = row[col];
      if (!isAlpha||!bmForAlpha||bmForAlpha[col]==null||raw==null) return raw;
      return raw - bmForAlpha[col];
    }}

    /* ── Bar chart ── */
    var barRows = allRows.map(function(r){{return Object.assign({{}},r,{{_dv:dvChart(r,win)}});}})
                         .filter(function(r){{return r._dv!=null&&!isNaN(r._dv);}});
    barRows.sort(function(a,b){{return (b._dv??-999)-(a._dv??-999);}});

    var BAR_H=24, BAR_GAP=4, NAME_W=160, BAR_HALF=160, PAD_T=22, PAD_B=10, PAD_R=52;
    var svgH1 = barRows.length*(BAR_H+BAR_GAP)+PAD_T+PAD_B;
    var svgW1 = NAME_W+BAR_HALF*2+PAD_R;
    var zeroX = NAME_W+BAR_HALF;
    var maxAbs= Math.max.apply(null, barRows.map(function(r){{return Math.abs(r._dv);}}).concat([0.001]));
    var scale = BAR_HALF/maxAbs;
    var barTitle= (isAlpha?'Alpha ':'Retorno ')+win;
    var barSvg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 '+svgW1+' '+svgH1+'" width="100%" style="font-family:inherit;overflow:visible;display:block">';
    barSvg += '<text x="'+zeroX+'" y="'+(PAD_T-6)+'" font-size="10" fill="var(--muted)" text-anchor="middle" font-weight="500">'+barTitle+'</text>';
    barSvg += '<line x1="'+zeroX+'" y1="'+PAD_T+'" x2="'+zeroX+'" y2="'+(svgH1-PAD_B)+'" stroke="var(--line-2)" stroke-width="0.6"/>';
    barRows.forEach(function(r,i) {{
      var y       = PAD_T+i*(BAR_H+BAR_GAP);
      var mid     = y+BAR_H/2;
      var isFund  = _isGlpgFundRpt(r, groupFundName);
      var isBench = !!r.is_bench;
      var barFill = isFund ? 'rgba(255,193,7,.82)' : isBench ? 'rgba(150,160,175,.4)' : 'rgba(100,140,210,.55)';
      var nameCol = isFund ? 'var(--accent)' : isBench ? 'var(--muted)' : 'var(--text)';
      var pxW     = Math.abs(r._dv)*scale;
      var barX    = r._dv>=0 ? zeroX : zeroX-pxW;
      var label   = (r._dv>=0?'+':'')+(r._dv*100).toFixed(1)+suffix;
      var labelX  = r._dv>=0 ? barX+pxW+5 : barX-5;
      var anchor  = r._dv>=0 ? 'start' : 'end';
      barSvg += '<text x="'+(NAME_W-4)+'" y="'+(mid+4)+'" font-size="12" fill="'+nameCol+'" text-anchor="end" font-weight="'+(isFund?'600':'400')+'">'+truncName(cleanName(r.name||'—'),28)+'</text>';
      barSvg += '<rect x="'+barX+'" y="'+(y+4)+'" width="'+Math.max(pxW,1.5)+'" height="'+(BAR_H-8)+'" rx="2" fill="'+barFill+'"/>';
      barSvg += '<text x="'+labelX+'" y="'+(mid+4)+'" font-size="10" fill="'+nameCol+'" text-anchor="'+anchor+'">'+label+'</text>';
    }});
    barSvg += '</svg>';

    /* ── Scatter chart ── */
    var scBase = allRows.map(function(r){{return Object.assign({{}},r,{{_dv:dvChart(r,win)}});}})
                        .filter(function(r){{return r.Vol!=null&&!isNaN(r.Vol)&&r._dv!=null&&!isNaN(r._dv);}});
    var scSvg  = '';
    if (scBase.length >= 3) {{
      var SC_SIZE=svgW1, SC_PAD={{l:48,r:16,t:24,b:40}};
      var plotW=SC_SIZE-SC_PAD.l-SC_PAD.r, plotH=SC_SIZE-SC_PAD.t-SC_PAD.b;
      var vols=scBase.map(function(r){{return r.Vol;}}), rets=scBase.map(function(r){{return r._dv;}});
      var minV=Math.min.apply(null,vols), maxV=Math.max.apply(null,vols);
      var minR=Math.min.apply(null,rets), maxR=Math.max.apply(null,rets);
      var vRange=(maxV-minV)||0.01, rRange=(maxR-minR)||0.01;
      var vPad=vRange*0.06, rPad=rRange*0.06;
      function scX(v)  {{ return SC_PAD.l+((v-(minV-vPad))/(vRange+2*vPad))*plotW; }}
      function scY(rv) {{ return SC_PAD.t+plotH-((rv-(minR-rPad))/(rRange+2*rPad))*plotH; }}
      var scTitle=(isAlpha?'Vol vs Alpha ':'Vol vs Retorno ')+win;
      scSvg='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 '+SC_SIZE+' '+SC_SIZE+'" width="100%" style="font-family:inherit;overflow:visible;display:block">';
      scSvg+='<text x="'+(SC_PAD.l+plotW/2)+'" y="16" font-size="10" fill="var(--muted)" text-anchor="middle" font-weight="500">'+scTitle+'</text>';
      scSvg+='<line x1="'+SC_PAD.l+'" y1="'+SC_PAD.t+'" x2="'+SC_PAD.l+'" y2="'+(SC_PAD.t+plotH)+'" stroke="var(--line-2)" stroke-width="0.8"/>';
      scSvg+='<line x1="'+SC_PAD.l+'" y1="'+(SC_PAD.t+plotH)+'" x2="'+(SC_PAD.l+plotW)+'" y2="'+(SC_PAD.t+plotH)+'" stroke="var(--line-2)" stroke-width="0.8"/>';
      scSvg+='<text x="'+(SC_PAD.l+plotW/2)+'" y="'+(SC_SIZE-6)+'" font-size="10" fill="var(--muted)" text-anchor="middle">Volatilidade (anual)</text>';
      scSvg+='<text x="11" y="'+(SC_PAD.t+plotH/2)+'" font-size="10" fill="var(--muted)" text-anchor="middle" transform="rotate(-90,11,'+(SC_PAD.t+plotH/2)+')">'+(isAlpha?'Alpha':'Retorno')+' '+win+'</text>';
      if (minR<0&&maxR>0) {{ var zy=scY(0); scSvg+='<line x1="'+SC_PAD.l+'" y1="'+zy+'" x2="'+(SC_PAD.l+plotW)+'" y2="'+zy+'" stroke="var(--line-2)" stroke-width="0.6" stroke-dasharray="4,3"/>'; }}
      for(var i=0;i<=5;i++) {{
        var rv=minR+rRange*i/5, ty=scY(rv);
        scSvg+='<line x1="'+SC_PAD.l+'" y1="'+ty+'" x2="'+(SC_PAD.l+plotW)+'" y2="'+ty+'" stroke="var(--line-2)" stroke-width="0.3" opacity="0.4"/>';
        scSvg+='<text x="'+(SC_PAD.l-6)+'" y="'+(ty+3)+'" font-size="9" fill="var(--muted)" text-anchor="end">'+(rv>=0?'+':'')+(rv*100).toFixed(0)+suffix+'</text>';
        var xv=minV+vRange*i/5, tx=scX(xv);
        scSvg+='<line x1="'+tx+'" y1="'+SC_PAD.t+'" x2="'+tx+'" y2="'+(SC_PAD.t+plotH)+'" stroke="var(--line-2)" stroke-width="0.3" opacity="0.4"/>';
        scSvg+='<text x="'+tx+'" y="'+(SC_PAD.t+plotH+14)+'" font-size="9" fill="var(--muted)" text-anchor="middle">'+(xv*100).toFixed(0)+'%</text>';
      }}
      [
        scBase.filter(function(r){{return !_isGlpgFundRpt(r,groupFundName)&&!r.is_bench;}}),
        scBase.filter(function(r){{return !!r.is_bench;}}),
        scBase.filter(function(r){{return _isGlpgFundRpt(r,groupFundName);}})
      ].forEach(function(layer) {{
        layer.forEach(function(r) {{
          var cx=scX(r.Vol), cy=scY(r._dv);
          var isFund=_isGlpgFundRpt(r,groupFundName), isBench=!!r.is_bench;
          var radius    = isFund?7:isBench?5:4.5;
          var dotFill   = isFund?'rgba(255,193,7,.9)':isBench?'rgba(150,160,175,.65)':'rgba(100,140,210,.65)';
          var dotStroke = isFund?'#ffc107':isBench?'rgba(180,190,200,.8)':'rgba(120,160,230,.8)';
          var retLbl    = (r._dv>=0?'+':'')+(r._dv*100).toFixed(2)+suffix;
          var cn        = truncName(cleanName(r.name||''),18);
          scSvg+='<circle cx="'+cx+'" cy="'+cy+'" r="'+radius+'" fill="'+dotFill+'" stroke="'+dotStroke+'" stroke-width="1"/>';
          if(isFund||isBench) scSvg+='<text x="'+cx+'" y="'+(cy-radius-4)+'" font-size="'+(isFund?10:9)+'" fill="'+(isFund?'var(--accent)':'var(--muted)')+'" text-anchor="middle" font-weight="'+(isFund?'600':'400')+'">'+cn+'</text>';
        }});
      }});
      scSvg+='</svg>';
    }}

    var chartBox=function(title,svg){{return '<div style="min-width:0"><div style="font-size:10px;color:var(--muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:.05em">'+title+'</div>'+svg+'</div>';}};
    wrap.innerHTML = barRows.length
      ? chartBox(barTitle,barSvg)+(scSvg?chartBox(scTitle||'',scSvg):'')
      : '<div style="color:var(--muted);font-size:12px;padding:16px">Sem dados para '+win+'</div>';
  }}

  /* ── Per-fund peers panel: 2-col layout ──────────────────────────────────
   * LEFT column: 4 thin blue range lines (MTD/YTD × ordinal/returns)
   *   – axis = thin blue line, ticks at P0/P25/P50/P75/P100
   *   – diamond colour codes the temperature (red worst → green best)
   * RIGHT column: 2 scatters (MTD + YTD) — Vol vs Retorno
   * Mode-aware (abs vs alpha). */
  function _renderFundPeersStrips(container, groupKey) {{
    if (!container||!_RPT_PEERS||!_RPT_PEERS.groups) return;
    var g = _RPT_PEERS.groups[groupKey];
    if (!g) {{ container.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:16px">Grupo não disponível.</div>'; return; }}
    var alphaBmKey   = g.alpha_bench || (groupKey==='FRONTIER'?'IBOVESPA':'CDI');
    var bmForAlpha   = (_RPT_PEERS.benchmarks||{{}})[alphaBmKey];
    var groupFundName= (g.fund_name||'').toUpperCase();
    var isAlpha      = _mode === 'alpha';
    var suffix       = '%';
    var benches      = (g.benchmarks||['CDI']).map(function(k){{return _RPT_PEERS.benchmarks&&_RPT_PEERS.benchmarks[k];}}).filter(Boolean).map(function(b){{return Object.assign({{}},b,{{is_bench:true}});}});
    var allRows      = (g.peers||[]).concat(benches);

    function dvWin(row, col) {{
      var raw = row[col];
      if (!isAlpha||!bmForAlpha||bmForAlpha[col]==null||raw==null) return raw;
      return raw - bmForAlpha[col];
    }}

    function fmtRet(v) {{
      if (v==null||isNaN(v)) return '—';
      return (v>=0?'+':'')+(v*100).toFixed(2)+suffix;
    }}

    function diaColors(pc) {{
      // Temperature palette: red worst → green best, picked in 5 bands.
      var fill =
        pc < 20 ? '#C0392B' :
        pc < 40 ? '#E67E22' :
        pc < 60 ? '#F4D03F' :
        pc < 80 ? '#58D68D' :
                  '#1E8C45';
      var stroke =
        pc < 20 ? '#7a1d12' :
        pc < 40 ? '#8a4a12' :
        pc < 60 ? '#8a7a12' :
        pc < 80 ? '#1f7a3a' :
                  '#0d4a22';
      return [fill, stroke];
    }}

    /* Thin blue range line (axis = single blue stroke with end caps and ticks).
     * Diamond colour codes the temperature (red→green) by ourPct. */
    function mkStrip(leftLabel, rightLabel, ourPct, tickLabels, idPrefix, hasFund) {{
      var W = 560, H = 50;
      var L = 92, R = 92, T = 6;
      var lineY  = T + 12;
      var lineL  = L, lineR = W - R;
      var lineW  = lineR - lineL;
      var clamp  = Math.max(0, Math.min(100, ourPct||0));
      var fundX  = lineL + clamp/100 * lineW;
      var BLUE   = '#5aa3e8';
      var BLUE_M = 'rgba(90,163,232,0.35)';

      var s = '<svg viewBox="0 0 '+W+' '+H+'" xmlns="http://www.w3.org/2000/svg" width="100%" style="display:block;overflow:visible">';
      s += '<defs>'+
             '<filter id="'+idPrefix+'-glow" x="-50%" y="-50%" width="200%" height="200%">'+
               '<feGaussianBlur stdDeviation="2" result="b"/>'+
               '<feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>'+
             '</filter>'+
           '</defs>';
      // Thin blue axis line + soft halo behind it
      s += '<line x1="'+lineL+'" y1="'+lineY+'" x2="'+lineR+'" y2="'+lineY+'" '+
           'stroke="'+BLUE_M+'" stroke-width="4" stroke-linecap="round"/>';
      s += '<line x1="'+lineL+'" y1="'+lineY+'" x2="'+lineR+'" y2="'+lineY+'" '+
           'stroke="'+BLUE+'" stroke-width="1.5" stroke-linecap="round"/>';
      // End caps
      s += '<circle cx="'+lineL+'" cy="'+lineY+'" r="2.5" fill="'+BLUE+'"/>';
      s += '<circle cx="'+lineR+'" cy="'+lineY+'" r="2.5" fill="'+BLUE+'"/>';
      // Tick marks at 0/25/50/75/100 (longer at quartiles, short at ends)
      for (var i=0; i<=4; i++) {{
        var tx = lineL + (i/4)*lineW;
        var th = (i===0||i===4) ? 4 : 5;
        s += '<line x1="'+tx+'" y1="'+(lineY-th)+'" x2="'+tx+'" y2="'+(lineY+th)+'" '+
             'stroke="'+BLUE+'" stroke-width="1.1" opacity="0.75"/>';
        if (tickLabels && tickLabels[i]) {{
          s += '<text x="'+tx+'" y="'+(lineY+18)+'" font-size="9.5" '+
               'fill="var(--muted)" text-anchor="middle" font-family="monospace">'+tickLabels[i]+'</text>';
        }}
      }}
      // Diamond marker — temperature-coloured (red worst → green best)
      if (hasFund) {{
        var dr = 7.5;
        var poly = fundX+','+(lineY-dr)+' '+(fundX+dr)+','+lineY+' '+fundX+','+(lineY+dr)+' '+(fundX-dr)+','+lineY;
        var cols = diaColors(clamp);
        // Outer dark halo for contrast against blue line
        s += '<polygon points="'+poly+'" fill="#0b1220" opacity="0.55" filter="url(#'+idPrefix+'-glow)"/>';
        // Temperature diamond
        s += '<polygon points="'+poly+'" fill="'+cols[0]+'" stroke="'+cols[1]+'" '+
             'stroke-width="1.4" stroke-linejoin="round"/>';
        // Inner glassy highlight
        var dr2 = dr*0.42;
        var inner = fundX+','+(lineY-dr2)+' '+(fundX+dr2)+','+lineY+' '+fundX+','+(lineY+dr2)+' '+(fundX-dr2)+','+lineY;
        s += '<polygon points="'+inner+'" fill="rgba(255,255,255,0.55)"/>';
      }}
      // Left label (line title)
      s += '<text x="'+(L-8)+'" y="'+(lineY+4)+'" font-size="11" '+
           'fill="var(--text)" text-anchor="end" font-weight="600">'+leftLabel+'</text>';
      // Right label (our value summary)
      if (rightLabel) {{
        s += '<text x="'+(lineR+8)+'" y="'+(lineY+4)+'" font-size="11" '+
             'fill="var(--text)" text-anchor="start" font-weight="600">'+rightLabel+'</text>';
      }}
      s += '</svg>';
      return s;
    }}

    function buildPair(win, idPrefix) {{
      var rows = allRows.map(function(r){{return Object.assign({{}},r,{{_dv:dvWin(r,win)}});}})
                        .filter(function(r){{return r._dv!=null&&!isNaN(r._dv);}});
      if (!rows.length) return '<div style="color:var(--muted);font-size:11px;padding:6px 12px">Sem dados '+win+'</div>';
      var sorted = rows.slice().sort(function(a,b){{return b._dv-a._dv;}});
      var n = sorted.length;
      var ourIdx = sorted.findIndex(function(r){{return _isGlpgFundRpt(r,groupFundName);}});
      var hasFund = ourIdx >= 0;
      var ordPct = hasFund ? (n>1 ? (n-1-ourIdx)/(n-1)*100 : 50) : 0;
      var ourVal = hasFund ? sorted[ourIdx]._dv : null;
      var asc = rows.map(function(r){{return r._dv;}}).sort(function(a,b){{return a-b;}});
      function q(p) {{
        if (n===1) return asc[0];
        var pos = p*(n-1), lo = Math.floor(pos), hi = Math.ceil(pos);
        return asc[lo] + (asc[hi]-asc[lo])*(pos-lo);
      }}
      var p0=asc[0], p25=q(0.25), p50=q(0.50), p75=q(0.75), p100=asc[n-1];
      var valPct = (hasFund && p100>p0) ? (ourVal-p0)/(p100-p0)*100 : 50;

      var ordTicks = ['P0','P25','P50','P75','P100'];
      var ordRight = hasFund ? ('P'+ordPct.toFixed(0)+' · #'+(ourIdx+1)+'/'+n) : '—';
      var s1 = mkStrip(win+' · ordinal', ordRight, ordPct, ordTicks, idPrefix+'-ord', hasFund);

      var rtTicks = [fmtRet(p0), fmtRet(p25), fmtRet(p50), fmtRet(p75), fmtRet(p100)];
      var rtRight = hasFund ? fmtRet(ourVal) : '—';
      var s2 = mkStrip(win+' · retorno', rtRight, valPct, rtTicks, idPrefix+'-rt', hasFund);

      return s1 + '<div style="height:4px"></div>' + s2;
    }}

    /* Per-window scatter: Vol (12M) on X, return on Y. Adapted from
     * _renderPeersCharts but sized for the right column. */
    function buildScatter(win, idPrefix) {{
      var rows = allRows.map(function(r){{return Object.assign({{}},r,{{_dv:dvWin(r,win)}});}})
                        .filter(function(r){{return r.Vol!=null&&!isNaN(r.Vol)&&r._dv!=null&&!isNaN(r._dv);}});
      if (rows.length < 3) return '<div style="color:var(--muted);font-size:11px;padding:24px;text-align:center">Sem dados suficientes para scatter '+win+'</div>';
      var W = 460, H = 280, P = {{l:48, r:14, t:22, b:34}};
      var plotW = W - P.l - P.r, plotH = H - P.t - P.b;
      var vols = rows.map(function(r){{return r.Vol;}});
      var rets = rows.map(function(r){{return r._dv;}});
      var minV = Math.min.apply(null, vols), maxV = Math.max.apply(null, vols);
      var minR = Math.min.apply(null, rets), maxR = Math.max.apply(null, rets);
      var vRange = (maxV-minV)||0.01, rRange = (maxR-minR)||0.01;
      var vPad = vRange*0.06, rPad = rRange*0.06;
      function sx(v)  {{ return P.l + ((v-(minV-vPad))/(vRange+2*vPad))*plotW; }}
      function sy(rv) {{ return P.t + plotH - ((rv-(minR-rPad))/(rRange+2*rPad))*plotH; }}

      var s = '<svg viewBox="0 0 '+W+' '+H+'" xmlns="http://www.w3.org/2000/svg" width="100%" style="display:block;overflow:visible;font-family:inherit">';
      s += '<text x="'+(P.l+plotW/2)+'" y="14" font-size="10.5" fill="var(--muted)" text-anchor="middle" font-weight="600">'+
           (isAlpha?'Vol vs Alpha ':'Vol vs Retorno ')+win+'</text>';
      // Axes
      s += '<line x1="'+P.l+'" y1="'+P.t+'" x2="'+P.l+'" y2="'+(P.t+plotH)+'" stroke="var(--line-2)" stroke-width="0.8"/>';
      s += '<line x1="'+P.l+'" y1="'+(P.t+plotH)+'" x2="'+(P.l+plotW)+'" y2="'+(P.t+plotH)+'" stroke="var(--line-2)" stroke-width="0.8"/>';
      s += '<text x="'+(P.l+plotW/2)+'" y="'+(H-6)+'" font-size="9.5" fill="var(--muted)" text-anchor="middle">Volatilidade (anual)</text>';
      s += '<text x="11" y="'+(P.t+plotH/2)+'" font-size="9.5" fill="var(--muted)" text-anchor="middle" transform="rotate(-90,11,'+(P.t+plotH/2)+')">'+
           (isAlpha?'Alpha':'Retorno')+' '+win+'</text>';
      // Zero return line if range straddles 0
      if (minR<0 && maxR>0) {{
        var zy = sy(0);
        s += '<line x1="'+P.l+'" y1="'+zy+'" x2="'+(P.l+plotW)+'" y2="'+zy+'" stroke="var(--line-2)" stroke-width="0.6" stroke-dasharray="4,3"/>';
      }}
      // Gridlines + axis labels (5 ticks each)
      for (var i=0; i<=5; i++) {{
        var rv = minR + rRange*i/5, ty = sy(rv);
        s += '<line x1="'+P.l+'" y1="'+ty+'" x2="'+(P.l+plotW)+'" y2="'+ty+'" stroke="var(--line-2)" stroke-width="0.3" opacity="0.4"/>';
        s += '<text x="'+(P.l-6)+'" y="'+(ty+3)+'" font-size="9" fill="var(--muted)" text-anchor="end">'+
             (rv>=0?'+':'')+(rv*100).toFixed(0)+suffix+'</text>';
        var xv = minV + vRange*i/5, tx = sx(xv);
        s += '<line x1="'+tx+'" y1="'+P.t+'" x2="'+tx+'" y2="'+(P.t+plotH)+'" stroke="var(--line-2)" stroke-width="0.3" opacity="0.4"/>';
        s += '<text x="'+tx+'" y="'+(P.t+plotH+13)+'" font-size="9" fill="var(--muted)" text-anchor="middle">'+(xv*100).toFixed(0)+'%</text>';
      }}
      // Layered draw: peers, benches, our fund (drawn on top)
      var layers = [
        rows.filter(function(r){{return !_isGlpgFundRpt(r,groupFundName)&&!r.is_bench;}}),
        rows.filter(function(r){{return !!r.is_bench;}}),
        rows.filter(function(r){{return _isGlpgFundRpt(r,groupFundName);}})
      ];
      function _esc(t) {{ return String(t).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}
      for (var li=0; li<layers.length; li++) {{
        var layer = layers[li];
        for (var i2=0; i2<layer.length; i2++) {{
          var r = layer[i2];
          var cx = sx(r.Vol), cy = sy(r._dv);
          var isFund = _isGlpgFundRpt(r, groupFundName);
          var isBench = !!r.is_bench;
          var radius    = isFund?7:isBench?5:4.5;
          var dotFill   = isFund?'rgba(255,193,7,.9)':isBench?'rgba(150,160,175,.65)':'rgba(100,140,210,.65)';
          var dotStroke = isFund?'#ffc107':isBench?'rgba(180,190,200,.8)':'rgba(120,160,230,.8)';
          var tipName = _esc(cleanName(r.name||'—'));
          var tipRet  = (isAlpha?'Alpha ':'Retorno ')+win+': '+(r._dv>=0?'+':'')+(r._dv*100).toFixed(2)+suffix;
          var tipVol  = 'Vol (12M): '+(r.Vol*100).toFixed(2)+'%';
          s += '<circle cx="'+cx+'" cy="'+cy+'" r="'+radius+'" fill="'+dotFill+'" stroke="'+dotStroke+'" '+
               'stroke-width="1" class="rpt-sc-dot" '+
               'data-tip-name="'+tipName+'" data-tip-ret="'+tipRet+'" data-tip-vol="'+tipVol+'" '+
               'style="cursor:pointer"/>';
          if (isFund || isBench) {{
            var cn = truncName(cleanName(r.name||''),18);
            s += '<text x="'+cx+'" y="'+(cy-radius-4)+'" font-size="'+(isFund?10:9)+'" '+
                 'fill="'+(isFund?'var(--accent)':'var(--muted)')+'" text-anchor="middle" '+
                 'font-weight="'+(isFund?'600':'400')+'" pointer-events="none">'+cn+'</text>';
          }}
        }}
      }}
      s += '</svg>';
      return s;
    }}

    // 2×2 grid: each row pairs the 2 strips of a window with its scatter
    container.innerHTML =
      '<div style="display:grid;grid-template-columns:minmax(0,1.05fr) minmax(0,1fr);'+
                  'grid-auto-rows:auto;gap:18px 18px;align-items:center">' +
        '<div style="min-width:0">' + buildPair('MTD', groupKey+'-mtd') + '</div>' +
        '<div style="min-width:0">' + buildScatter('MTD', groupKey+'-sc-mtd') + '</div>' +
        '<div style="min-width:0">' + buildPair('YTD', groupKey+'-ytd') + '</div>' +
        '<div style="min-width:0">' + buildScatter('YTD', groupKey+'-sc-ytd') + '</div>' +
      '</div>';
  }}

  /* Per-card view toggle (charts ↔ table) */
  window.rptSetFundPeersView = function(pg, view) {{
    document.querySelectorAll('.rpt-fpeers-vw[data-pg="'+pg+'"]').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.fview === view);
    }});
    document.querySelectorAll('.rpt-peers-fund-strips[data-peers-group="'+pg+'"]').forEach(function(el) {{
      el.style.display = view==='charts' ? '' : 'none';
    }});
    document.querySelectorAll('.rpt-peers-fund-tbl-wrap[data-peers-group="'+pg+'"]').forEach(function(el) {{
      el.style.display = view==='table' ? '' : 'none';
    }});
  }};

  function renderAllPeersTables() {{
    renderTable(document.getElementById('rpt-peers-tbl'), _rptPeersGrp);
    document.querySelectorAll('.rpt-peers-fund-tbl').forEach(function(tbl) {{
      renderTable(tbl, tbl.dataset.peersGroup);
    }});
    document.querySelectorAll('.rpt-peers-fund-strips').forEach(function(el) {{
      _renderFundPeersStrips(el, el.dataset.peersGroup);
    }});
    if (_rptView === 'charts') _renderPeersCharts(_rptPeersGrp);
  }}

  var _rptPeersGrp = 'EVOLUTION';
  window._rptPeersGrp = _rptPeersGrp;

  window.rptOnGroupChange = function() {{
    var sel = document.getElementById('rpt-peers-grp-sel');
    if (sel) {{ _rptPeersGrp = sel.value; window._rptPeersGrp = _rptPeersGrp; }}
    renderAllPeersTables();
  }};

  window.rptSetPeersMode = function(mode) {{
    _mode = mode;
    document.getElementById('rpt-btn-abs')  && document.getElementById('rpt-btn-abs').classList.toggle('active',  mode==='abs');
    document.getElementById('rpt-btn-alpha')&& document.getElementById('rpt-btn-alpha').classList.toggle('active', mode==='alpha');
    renderAllPeersTables();
  }};

  // Anchor toggle: 'current' (data do relatório) vs 'eopm' (fim do mês anterior).
  // Troca window._RPT_PEERS pra mudar a fonte que strips/scatters/table consomem.
  window.rptSetPeersAnchor = function(anchor) {{
    var src = (anchor === 'eopm') ? window._RPT_PEERS_EOPM : window._RPT_PEERS_CURRENT;
    if (!src) return;  // snapshot indisponível
    window._RPT_PEERS = src;
    _RPT_PEERS = src;  // local var no closure (mesma referência)
    // Atualiza estado visual dos botões + subtitle de cada card
    document.querySelectorAll('.rpt-peers-anchor').forEach(function(b) {{
      b.classList.toggle('active', b.dataset.anchor === anchor);
    }});
    var vd = src.val_date || '—';
    var stale = !!src._is_stale;
    var anchorTxt = (anchor === 'eopm')
      ? ('Fim do mês anterior · ' + vd + (stale ? ' ⚠' : ''))
      : ('Atual · ' + vd + (stale ? ' ⚠' : ''));
    document.querySelectorAll('[data-peers-sub="1"]').forEach(function(el) {{
      // Mantém o "grupo X" original; substitui só a parte da data
      var grp = (el.textContent.match(/grupo\\s+\\S+/) || [''])[0];
      el.textContent = '— ' + anchorTxt + (grp ? ' · ' + grp : '');
    }});
    // Re-render tudo que depende de _RPT_PEERS
    renderAllPeersTables();
  }};

  window.rptSetPeriod = function(win) {{
    _rptWin  = win;
    _sortKey = win;
    ['mtd','ytd','12m','24m','36m'].forEach(function(k) {{
      var b = document.getElementById('rpt-per-'+k);
      if (b) b.classList.toggle('active', k === win.toLowerCase());
    }});
    renderAllPeersTables();
  }};

  window.rptSetView = function(view) {{
    _rptView = view;
    var chartsWrap = document.getElementById('rpt-peers-charts-wrap');
    var tblWrap    = document.getElementById('rpt-peers-tbl-wrap');
    var btnTbl     = document.getElementById('rpt-view-tbl');
    var btnChrt    = document.getElementById('rpt-view-chrt');
    if (chartsWrap) chartsWrap.style.display = view==='charts' ? '' : 'none';
    if (tblWrap)    tblWrap.style.display    = view==='table'  ? '' : 'none';
    if (btnTbl)  btnTbl.classList.toggle('active',  view==='table');
    if (btnChrt) btnChrt.classList.toggle('active', view==='charts');
    if (view==='charts') _renderPeersCharts(_rptPeersGrp);
  }};

  /* Shared tooltip for scatter dots — single floating div, follows cursor */
  function _ensureRptTip() {{
    var tip = document.getElementById('rpt-sc-tip');
    if (tip) return tip;
    tip = document.createElement('div');
    tip.id = 'rpt-sc-tip';
    tip.style.cssText = 'position:fixed;display:none;pointer-events:none;z-index:9999;'+
      'background:rgba(15,23,42,0.96);color:#e2e8f0;border:1px solid rgba(148,163,184,0.35);'+
      'border-radius:6px;padding:6px 10px;font:12px/1.4 system-ui,sans-serif;'+
      'box-shadow:0 4px 16px rgba(0,0,0,0.45);max-width:280px;white-space:nowrap';
    document.body.appendChild(tip);
    document.addEventListener('mouseover', function(e) {{
      var t = e.target.closest && e.target.closest('.rpt-sc-dot');
      if (!t) return;
      tip.innerHTML =
        '<div style="font-weight:700;color:#fbbf24;margin-bottom:2px">'+t.getAttribute('data-tip-name')+'</div>'+
        '<div style="color:#cbd5e1">'+t.getAttribute('data-tip-ret')+'</div>'+
        '<div style="color:#94a3b8">'+t.getAttribute('data-tip-vol')+'</div>';
      tip.style.display = 'block';
    }});
    document.addEventListener('mousemove', function(e) {{
      if (tip.style.display !== 'block') return;
      var x = e.clientX + 14, y = e.clientY + 14;
      var rb = tip.getBoundingClientRect();
      // Avoid going off the right/bottom edges
      if (x + rb.width > window.innerWidth) x = e.clientX - rb.width - 14;
      if (y + rb.height > window.innerHeight) y = e.clientY - rb.height - 14;
      tip.style.left = x + 'px';
      tip.style.top  = y + 'px';
    }});
    document.addEventListener('mouseout', function(e) {{
      var t = e.target.closest && e.target.closest('.rpt-sc-dot');
      if (!t) return;
      // Hide only if the mouse is leaving for something that's NOT another dot
      var to = e.relatedTarget && e.relatedTarget.closest && e.relatedTarget.closest('.rpt-sc-dot');
      if (!to) tip.style.display = 'none';
    }});
    return tip;
  }}

  window.initRptPeers = function() {{
    _ensureRptTip();
    if (!_RPT_PEERS || !_RPT_PEERS.groups) return;
    var GROUPS_ORDER = ['EVOLUTION','ALBATROZ','FRONTIER','NAZCA','IGUANA','DRAGON','SEA LION','MACRO','GLOBAL','PELICAN'];
    var available = Object.keys(_RPT_PEERS.groups);
    var ordered   = GROUPS_ORDER.filter(function(g){{return available.includes(g);}})
                    .concat(available.filter(function(g){{return !GROUPS_ORDER.includes(g);}}));
    var sel = document.getElementById('rpt-peers-grp-sel');
    if (sel) {{
      sel.innerHTML = '';
      ordered.forEach(function(g) {{
        var opt = document.createElement('option');
        opt.value = g; opt.textContent = g;
        if (g === _rptPeersGrp) opt.selected = true;
        sel.appendChild(opt);
      }});
      if (!ordered.includes(_rptPeersGrp) && ordered.length) {{
        _rptPeersGrp = ordered[0]; window._rptPeersGrp = _rptPeersGrp;
      }}
    }}
    renderAllPeersTables();
  }};
}})();
</script>"""

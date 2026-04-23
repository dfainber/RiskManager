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

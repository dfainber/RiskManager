# Risk Monitor — Code Review (2026-05-01)

**Status: Phase 1 (read-only triage) complete · Phase 2 (execute) partially shipped 2026-05-01.**

Multi-agent review of the Risk Monitor + Credit kit. Four independent audits
ran in read-only mode (Security, Code Quality, Content/Docs, Aesthetics/UX).
Original transcript in [`REVIEW_2026-05-01.txt`](../REVIEW_2026-05-01.txt) at the
repo root — this MD is the curated, prioritized punch list.

---

## 1. Resolved 2026-05-01 (same day as audit)

These IMMEDIATE security and aesthetic findings were addressed in the same
session that ran the audit:

### Security
- **#1 — `glpg_fetch.py:64` bare `except: pass`** → now logs the close error,
  drops `_tl.conn`, and re-raises. No poisoned connection silently reused.
- **#2 — `nav or 1.0` fallback (3 sites)** → replaced with new
  `_require_nav(desk, date_str)` helper in [`db_helpers.py:113`](../db_helpers.py#L113)
  which raises `ValueError` on `None`/non-positive. Applied at the 4 cited
  divisor sites in `data_fetch.py` (`fetch_macro_exposure`, `fetch_quant_var`,
  `fetch_evolution_var`, `fetch_macro_pm_book_var`).
  - `fetch_macro_pm_var_history` keeps the soft path (skip-day instead of
    raising) so a single bad NAV doesn't kill the 121-day chart.
- **#3 — division-by-NAV guards** → upstream fix above means the 2 cited sites
  now consume `_require_nav`, guarded by construction.
- **#6 — HTML/SVG injection in credit report** → added `_h(s)` (wraps
  `html.escape(..., quote=True)` with NaN-aware fallback) in
  [`generate_credit_report.py:25`](../generate_credit_report.py#L25). Applied
  to all DB-string interpolation sites: sanity-check rows, alocação row helper
  (produto / tipo_ativo / indexador / setor / grupo_eff / rating tag),
  concentração table (grupo_economico / limit_text), and SVG chart helpers
  (`_svg_donut` legend + slice tooltip, `_svg_hbar` and `_svg_vbar` text +
  title elements). Bond names containing `<` `>` `&` `"` no longer break
  table layout or allow injected markup in the PDF emailed to 31 recipients.

### Aesthetics
- **D #7 — `daily_monitor.html` brand** → ported the inline `rm-logo` SVG +
  "Risk Monitor" wordmark from `generate_risk_report.py`, plus a "Powered by
  Galápagos" footer. Logo served by `pnl_server.py` via new
  `/assets/galapagos.png` endpoint (decoded once at startup from
  `credit/_galapagos_logo_b64.txt`).
- **D #11/#12 — vardod / pmovers CSV button injection** → added
  `class="modal"` (and `modal-head` / `modal-title`) to both modals in
  [`vardod_renderers.py:51`](../vardod_renderers.py#L51) and
  [`pmovers_renderers.py:191`](../pmovers_renderers.py#L191). `injectCsvButtons`
  now matches them.

### Process
- CLAUDE.md changelog block (~135 lines, ~37 KB) migrated to
  [`docs/CHANGELOG.md`](CHANGELOG.md). CLAUDE.md §7 dropped the stale "wire
  evolution_diversification_card" TODO (verified wired since 2026-04-XX at
  [`generate_risk_report.py:808`](../generate_risk_report.py#L808)).
  `docs/REPORTS.md` line 467 updated to remove the matching stale claim.

---

## 2. Open follow-ups — pick what to fix next

Severity rubric reminder:
- **HIGH** = wrong number visible to PMs OR crash in daily run
- **MEDIUM** = brittle code, breaks on next fund/factor addition
- **LOW** = cosmetic / maintainability

### 2a. Robustness (defense-in-depth, not currently breaking)

- [x] ~~**HIGH** — `evolution_diversification_card.py:169, 226, 333, 448, 496`~~
  → **verified false positive 2026-05-01.** All 5 sites already guarded
  (`if len(sub)`, `if eligible.empty: raise`, `if len(series) < 20: continue`,
  `if not net.empty:`, `if not net_row.empty:`). Auditor saw the iloc[] pattern
  but missed the surrounding guards.
- [ ] **MEDIUM** — `data_fetch.py:441, 541, 1775, 2710, 2716, 2784, 2790,
  2824, 2852, 2858, 2893, 2943` and `expo_renderers.py:680, 1106-1107, 2248,
  2502` and `fund_renderers.py:1851, 1989, 2048, 2181, 2390, 2482, 2488,
  2602, 2634` and `generate_credit_report.py:134-192` and
  `generate_monthly_review.py:194-375` — `.iloc[0]` / `.iloc[-1]` after only
  `.empty` check, no len guard. Realistic risk is low (most sites are
  `LIMIT 1` / `MAX(...)` queries) but fragile around holidays / race
  conditions. Introduce a `_first_or_default(df, default)` helper, use
  everywhere.
- [ ] **MEDIUM** — `data_fetch.py:2040` — empty df returned with shape
  mismatch (`df["var_pct"]=[]`). Return `pd.DataFrame(columns=[...])` with
  proper dtype.
- [ ] **MEDIUM** — `generate_risk_report.py:5477, 5480` and
  `generate_monthly_review.py:1764` — hardcoded
  `F:\Bloomberg\Risk_Manager\Data\Morningcall` mirror path with silent-fail
  try/except. Move to env var `RISK_MIRROR_PATH`; log failures as WARN.
- [ ] **MEDIUM** — `data_fetch.py:1910` — `float(fut["delta"].iloc[0])` after
  `.empty/.notna` guard. Use `pd.to_numeric(..., errors='coerce')` instead.
- [ ] **MEDIUM** — `pnl_server.py:91, 101` — raw exception message in JSON
  error response; quotes/newlines can break JSON encoding. Sanitize.
- [ ] **LOW** — `data_fetch.py:3013` — hardcoded path containing employee
  name. Move to config.
- [ ] **LOW** — `data_fetch.py:2599, 2648` — `map() + .get()` with silent
  None default; new product class added upstream silently dropped from
  report. Add `log.warning`.
- [x] ~~**LOW** — `generate_credit_report.py:1378` — weak CSV escape via
  `replace(chr(34), "&quot;")`.~~ → **fixed 2026-05-01.** The CSV encoding
  itself (`_csv_encode`, line 99) already implements correct CSV escaping
  (doubles `"`, quotes every cell). The actual issue was the HTML-attribute
  escape on the `data-csv="..."` interpolation — only `"` was being escaped,
  so a bond name like "Smith & Co" would emit raw `&` into the attribute.
  Replaced `csv.replace(chr(34), "&quot;")` with `html.escape(csv, quote=True)`
  at all 6 call sites. Validated zero numeric divergence via smoke-test
  snapshot diff (4629 risk_report + 8 vol_card values byte-identical to
  pre-refactor baseline).

### 2b. Code quality / efficiency (use safe-refactor skill — separate sessions)

- [ ] **HIGH** — `generate_risk_report.py:528–4946` — `build_html`
  mega-function (4,419 lines). Extract tab-switching + fund section builders
  to `_build_tab_*` helpers; move CSS/JS blob to `html_assets.py`.
- [ ] **HIGH** — `generate_risk_report.py:4947–5492` — `main()` (546 lines,
  `noqa:C901`). Split into `_fetch_all_data()`, `_build_report_data()`,
  `_write_output()`.
- [ ] **HIGH** — `generate_macro_pa_fx_split.py` + `_evolution_` + `_quant_`
  + `_macroq_` — 4 near-clones, ~2,310 LOC total, ~1,700 shared. Extract
  `_remap_classe` (65 LOC, identical) to `risk_config.py`; move CSS / JS /
  `_bps_cell` / `_bps_color` / `_esc` / tree-cell helpers to
  `html_assets.py` / `pa_renderers.py`.
- [ ] **MEDIUM** — `data_fetch.py:1417` — `df.iterrows()` inside nested loop
  over 4-level grouped data. O(n²); ~100k+ allocations per run.
- [ ] **MEDIUM** — `generate_risk_report.py:713, 1673, 1695` — `iterrows()`
  in tight HTML-row loops. Vectorize.
- [x] ~~**MEDIUM** — `pm_vol_card.py` (1,122 LOC) — confirm whether wired into
  the main HTML or dead.~~ → **verified alive 2026-05-01.** It is a standalone
  diagnostic (file docstring line 1 already says "Standalone card"), not
  imported by any other module (`grep` for `pm_vol_card` returns zero hits in
  source). Run via `run_vol_card.bat` and exercised by `smoke_test.py`
  (`check_vol_card`). Output: `data/morning-calls/pm_vol_card_<DATA>.html`.
  The auditor's claim that it is "imported by generate_risk_report.py via
  evolution_renderers" was incorrect — no such import exists.
- [ ] **LOW** — Inconsistent date / NAV / book-code parsing. Centralize in
  `risk_runtime.py`.
- [ ] **LOW** — `generate_risk_report.py:1–50` — 93 import lines, several
  unused. Run `pylint --unused-imports`.

### 2c. Aesthetics (lower priority post-2026-05-01 fixes)

- [ ] **MEDIUM** — `svg_renderers.py:71, 149, 222` — `#2d2d3d` (sparkline bar
  track) and `#0b1220` (sparkline dot outline) hardcoded, not in
  `_BG_HEX_MAP` of `generate_risk_report_meeting.py`. Verify projector
  contrast; add to remap defensively if unsure.
- [ ] **MEDIUM** — `fund_renderers.py:191, 304-306, 683, 740-745, 798,
  858-860, 1140, 1446, 1554, 1562` — raw f-string numeric formatting
  without `fmt_br_num` wrapping. Most values are <100 so `,` rarely
  appears, but budget/margin can exceed 1000 bps. Wrap with `_fmt_br_num`.

### 2d. Docs cleanup

- [ ] Add `Status: ARCHIVED 2026-04-XX (Camada 2 implemented)` header to
  `docs/CREDITO_TREATMENT.md`.
- [ ] Add `Status: RESOLVED (commit e053a40)` header to
  `docs/IDKA_VAR_EXPLORATION.md` (investigation report dated 2026-04-17,
  bug fix already shipped).
- [ ] Confirm `docs/REPORTS.html` (51 KB, last touched 2026-04-23) has no
  consumer; delete it. `docs/REPORTS.md` is the human-readable source of
  truth.
- [ ] Update `memory/project_status.md` to drop the stale
  `evolution_diversification_card` wire-it TODO (already done in CLAUDE.md
  §7 and `docs/REPORTS.md`).

---

## 3. Coverage and caveats

- Audits ran in read-only mode via Explore agents reading code excerpts.
  Findings are high-confidence but spot-check before acting — the original
  REVIEW_2026-05-01.txt warned "some line numbers may be approximate."
- During same-day verification (2026-05-01) we confirmed that findings
  #1, #2, #3 had already been addressed in working-tree changes (committed
  on the same day by a parallel session) — this MD reflects post-fix state.
- SQL injection: low risk overall — all date / fund values come from
  internal config dicts, not CLI input. No exploitable f-string SQL pattern
  found.

# Risk Monitor — Code & Content Review (2026-05-01, Session 2)

**Replaces / extends** [`docs/CODE_REVIEW_2026-05-01.md`](CODE_REVIEW_2026-05-01.md).
**Inputs:** 3 review agents (Correctness, Content/UX, Commentary) ran in parallel + my own session findings + carryover from the prior morning audit.

This pass adds two lenses the prior review didn't cover:
1. **Content quality of the rendered Morning Call HTML** — what's shown, in what order, with what units, and whether it answers the questions a risk manager actually asks at 8am.
2. **Commentary quality** — comments, docstrings, and skill files as a force multiplier for the next reader.

Findings are spot-verified against the codebase / rendered report; agent hallucinations are excluded.

---

## STATUS (2026-05-02, session 11 — Util Stop column + peers EOPM fix + DQ staleness)

Closes §3.9 (Util Stop column) plus three peers-related fixes (button bug,
selection rule, staleness DQ alert) that surfaced from a user-reported bug
during the session.

### Session 11 additions

| § | Item | Status | File |
|----|------|--------|------|
| 3.9 | Util Stop column added to Status consolidado between Util VaR and Δ VaR D-1; worst-PM utilization for MACRO/ALBATROZ/BALTRA, `—` for funds without a stop mandate. Header tooltip explains the convention. | ✅ DONE | [`summary_renderers.py`](../summary_renderers.py#L744) + [`generate_risk_report.py`](../generate_risk_report.py#L992) |
| user-reported | Peers "Fim Mês Ant." button was a no-op — `_peers_unwrap` always returned `data["latest"]` ignoring `data["month_end"]`. Now mode-aware (key arg). Auto-archive persists both snapshots independently. | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L3433) |
| user-reported | Archive selection rule changed from "latest val_date ≤ anchor" to "max fund coverage among archives ≤ anchor" (CVM quota staggered reporting biases earlier days toward early reporters). | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L3505) |
| user-reported | When EOPM falls back to a network snap newer than the EOPM anchor, sets `_eopm_unavailable=True`. JS handler dims the button + shows a notice on click instead of silently rendering identical data. | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L3520) + [`html_assets.py`](../html_assets.py#L2937) |
| user-reported | Peers staleness alert in Data Quality: flags `stale` when val_date is more than 2 calendar days behind report date (CVM quota delay = 2 trading days; longer = upstream pipeline stalled). | ✅ DONE | [`fund_renderers.py`](../fund_renderers.py#L1962) |

Real upstream pipeline issue surfaced: `peers_data.json` last modified 2026-04-24, val_date 2026-04-20 (12 days stale today). Escalation point.

## STATUS (2026-05-02, session 10 — iterrows vectorization + build_html batch 3)

Closes §2.9/§2.10 (iterrows vectorization) and continues §1.5 with batch 3.
Both pass smoke-test (post-#10 self-vs-self diff = 32 lines, post-#10+§1.5
diff = 32 lines — same magnitude, behavior unchanged).

### Session 10 additions

| § | Item | Status | File |
|----|------|--------|------|
| 2.9 | Nested iterrows in `_regroup_lookthrough` (children dict construction) → `to_dict('records')` with vectorized NaN→None | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L1426) |
| 2.9 | `df.apply(axis=1)` for evo_classify_livro/factor → list comprehension with `zip` | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L2400) |
| 2.10 | Frontier stocks iterrows in `_build_agg_rows` → vectorized arithmetic + boolean mask | ✅ DONE | [`generate_risk_report.py`](../generate_risk_report.py#L1056) |
| 2.10 | Side-table iterrows in `build_single_names_section` → `to_dict('records')` | ✅ DONE | [`fund_renderers.py`](../fund_renderers.py#L162) |
| 1.5 | Master HTML f-string (~85L) extracted to `_render_master_html(...)` with 16 kwargs | ✅ DONE | [`generate_risk_report.py`](../generate_risk_report.py#L1345) |

§1.5 progress: 1214 → 476L (-61% cumulative across batches 1+2+3). Remaining ~476L is harder to extract cleanly — variable unpacking + section-list construction loops + orchestration glue.

## STATUS (2026-05-02, session 9 — Status DIA D-1 fallback)

Closes §1.3 (Status consolidado DIA fallback). When PA hasn't landed for
the requested date, the Status table no longer renders silent `—` cells —
it now shows the last available day's value with an unobtrusive
`(D-1)` muted tag. Bench rows continue to render normal DIA values, so
the table no longer looks broken.

### Session 9 additions

| § | Item | Status | File |
|----|------|--------|------|
| 1.3 | `_sum_bp_cell` accepts optional `lag_tag`; renders the value + `(D-1)` muted suffix when set, plus tooltip "PA pendente para hoje — última obs D-1" on flat values. | ✅ DONE | [`generate_risk_report.py`](../generate_risk_report.py#L844) |
| 1.3 | `_build_summary_rows_html` accepts `df_pa_daily` + `pa_has_today`; falls back to `df_pa_daily` per-fund max-date sum when `pa_has_today` is False. | ✅ DONE | [`generate_risk_report.py`](../generate_risk_report.py#L879) |
| 1.3 | `fetch_pa_daily_per_product` extended to cover BALTRA / IDKAIPCAY3 / IDKAIPCAY10 (was MACRO/QUANT/EVOLUTION/GLOBAL/ALBATROZ/GFA only) so the lag-fallback covers all 9 funds. | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L2712) |

## STATUS (2026-05-02, session 8 — briefing headline + holiday-aware default)

Closes §1.2 (Briefing Executivo headline priority) and the parking-lot
"Holiday-aware default date" item from CLAUDE.md fila #14. Both shipped in a
single commit; smoke test passes on auto-picked default-date run (Labor Day
correctly skipped — Sat 2026-05-02 → Thu 2026-04-30).

### Session 8 additions

| § | Item | Status | File |
|----|------|--------|------|
| 1.2 | Briefing headline now uses `max(margem_inverse, util_VaR, \|Δ VaR\|)` priority. PA hit ≥1% NAV stays as override. Triggers per-kind: util≥85% / \|ΔVaR\|≥10 bps / margem<25 bps. | ✅ DONE | [`fund_renderers.py`](../fund_renderers.py#L2637) |
| 1.2 | "Atenção" relocated above the `brief-grid` (full-width strip when items exist; hidden when empty). | ✅ DONE | [`fund_renderers.py`](../fund_renderers.py#L2876) |
| fila-14 | `risk_runtime._resolve_default_data_date` walks back from today's calendar skipping weekends + `_BR_HOLIDAYS` (hardcoded 2024-2027 set; risk_runtime stays DB-free). | ✅ DONE | [`risk_runtime.py`](../risk_runtime.py#L26) |

## STATUS (2026-05-02, session 7 — build_html extraction batch 2)

Closes §1.5 further. build_html went from **690 → 540 lines** (additional
22% reduction) by extracting 3 more self-contained blocks: per-fund
sections assembly (briefings + peers reorder), VaR DoD commentary
prefetch + top-driver extraction, and the Summary view section-wrap
composition. No behavior change: smoke_test passes (output diff vs
baseline matches the same set/DB drift as a self-vs-self regen), dark
+ meeting reports regenerate clean.

### Session 7 additions

| § | Item | Lines extracted | Helper |
|----|------|----------------|--------|
| 1.5 | Per-fund sections reorder (briefings last + peers tab) | 70 | `_assemble_sections_html` |
| 1.5 | VaR DoD prefetch + top-driver per-fund commentary | 60 | `_build_var_commentary` |
| 1.5 | Summary view section-wrap (EVO C4 headline + cards) | 50 | `_build_summary_view` |

§1.5 is now reclassified from "in progress — ~700 lines remaining" to "in progress — ~540 lines remaining". What's left in build_html is dominated by the master HTML template f-string (~85L), the master `<body>` page composition + tab subtab generation (~150L), and orchestration glue (~300L of variable wiring + section list construction loops that are not cleanly extractable).

## STATUS (2026-05-02, session 6 — build_html extraction batch 1)

Closes §1.5 (partial — biggest open audit item). build_html went from
**1214 → 690 lines** (43% reduction) by extracting 5 self-contained
data-assembly + render blocks to module-level helpers. No behavior change:
smoke_test passes, dark + meeting reports regenerate clean.

### Session 6 additions

| § | Item | Lines extracted | Helper |
|----|------|----------------|--------|
| 1.5 | Cross-fund VaR/BVaR rows for "Risco VaR e BVaR por fundo" card | 52 | `_build_house_rows` |
| 1.5 | Factor × fund risk matrix (10 factors × N funds) + bench matrix | 150 | `_build_factor_matrix` |
| 1.5 | Cross-fund top positions list (Top Posições card source) | 60 | `_build_agg_rows` |
| 1.5 | Status consolidado per-fund row HTML + bench rows | 145 | `_build_summary_rows_html` + `_build_bench_rows_html` |
| 1.5 | PA contribution alerts card (size + fund sort, ≥1% NAV flag) | 135 | `_build_pa_alerts_html` |

## STATUS (2026-05-02, session 5 — main() split + nav_d1 propagation)

Session 5 closes 2 more open items: §1.6 (main() split into 3 helpers) and §2.13f (full nav_d1 call-site propagation through QUANT/EVOLUTION exposure renderers). Plus 2 ad-hoc UI tweaks: removed "Total NAV-pond. (não-div.)" footer row from "Risco VaR e BVaR por fundo" card; Vol Regime card now skips empty rows instead of rendering `—` placeholders. Both dark + meeting reports regenerate clean; smoke test passes.

### Session 5 additions

| § | Item | Status | File |
|----|------|--------|------|
| 1.6 | `main()` 540 lines → 3 helpers (`_fetch_all_data` / `_build_report_data` / `_write_output`). Orchestrator main() now 8 lines. | ✅ DONE | [`generate_risk_report.py`](../generate_risk_report.py#L1763) |
| 2.13f | `nav_d1` plumbed end-to-end: fetched in `_fetch_all_data` (no longer discarded), forwarded through `ReportData` (new `quant_expo_nav_d1` / `evo_expo_nav_d1` fields), wired through `build_quant_exposure_section` + `build_evolution_exposure_section` and into the unified table. D-1 deltas now divided by D-1 NAV under chunky NAV swings. | ✅ DONE | 2 files |
| ad-hoc | Removed "Total NAV-pond. (não-div.)" footer row from Risco VaR e BVaR card | ✅ DONE | [`summary_renderers.py`](../summary_renderers.py) |
| ad-hoc | Vol Regime: skip empty rows instead of rendering `—` placeholders | ✅ DONE | [`fund_renderers.py`](../fund_renderers.py#L267) |

## STATUS (2026-05-02, session 4 — security + centralization batch)

Session 4 closes 5 more open items: §4.3 (security — env-only DB credentials), §2.13h (desk-name centralization), §3.3c (issuer override → JSON), §2.11 (iloc cast hardening), §2.13a (VaR DoD nav_d1 plumbing fully wired). §2.13f infrastructure landed — full call-site propagation deferred. Both dark + meeting reports regenerate clean; smoke test passes.

### Session 4 additions

| § | Item | Status | File |
|----|------|--------|------|
| 4.3 | `glpg_fetch.py` no longer falls back to hardcoded host/user — env-only with fail-fast | ✅ DONE | [`glpg_fetch.py`](../glpg_fetch.py) |
| 2.13h | Desk-name centralization (`_MACRO_DESK`, `_ALBATROZ_DESK`, `_FRONTIER_DESK`, `_EVOLUTION_DESK`, `_BALTRA_DESK` etc. in [`risk_config.py`](../risk_config.py)) — all literal sites in `data_fetch.py`, `db_helpers.py`, `pm_vol_card.py`, `generate_risk_report.py` swept | ✅ DONE | 4 files |
| 3.3c | "Cruz" issuer override → [`credit/issuer_overrides.json`](../credit/issuer_overrides.json), substring-rule loader in [`credit/credit_data.py`](../credit/credit_data.py) | ✅ DONE | new JSON + 1 file |
| 2.11 | `data_fetch.py:1913` `float(iloc[0])` → `pd.to_numeric(...).fillna(0.0).iloc[0]` | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L1913) |
| 2.13a | `_var_dod_rpm` now accepts `nav_d1` and divides D-1 contributions by D-1 NAV (call site plumbed through `_var_dod_dispatch`) | ✅ DONE | [`data_fetch.py`](../data_fetch.py#L1180) |
| 2.13f | `_build_expo_unified_table` accepts `nav_d1` kwarg (defaults to None → falls back to `nav` = legacy behavior). Per-renderer call-site propagation pending | 🟡 PARTIAL | [`expo_renderers.py`](../expo_renderers.py#L41) |

## STATUS (2026-05-01, session 3 — quantity-over-quality pass)

All 6 NEW HIGH correctness items + briefing tightening + skill-refresh + Day-3 hygiene shipped across **9 commits** since this doc was written. Both dark + meeting reports regenerate clean.

| § | Item | Status | Commit |
|----|------|--------|--------|
| 1.0 | `risk_runtime.py` argv-as-date crash | ✅ DONE | `b925bb4` |
| 1.0b | `generate_risk_report.py:5113` `or 1.0` regression | ✅ DONE | `b925bb4` |
| 1.0c | `metrics.py:392` shorts dropped from PA outliers | ✅ DONE | `b925bb4` |
| 1.0d | `expo_renderers.py:1707/1717` Dur double-MD | ✅ DONE | `42ca65f` |
| 1.0e | `credit/credit_data.py:140` multi-book join | ✅ DONE | `c594e77` |
| 1.0f | `generate_monthly_review.py:372` IDKA BVaR sign-flip | ✅ DONE | `b925bb4` |
| 1.1 | Briefing "tranquilo" gate tightening | ✅ DONE | `d78992a` |
| 2.1 | Skill-refresh sprint (5 SKILL.md + 14 `glpg-data-fetch` refs) | ✅ DONE | `9c886f5` |
| 2.2 | Status table "Total (soma)" rename | ✅ DONE | `4fee511` |
| 2.4 | Mudanças Significativas D-1 null guard | ✅ DONE | `4fee511` |
| 2.5 | Risk Budget absolute-margem secondary gate | ✅ DONE | `84f465d` |
| 2.7 | Top Posições "% da posição" rename | ✅ DONE | `4fee511` |
| 2.8 | `Commodities ` trailing-space CLASSE strip | ✅ DONE | `4fee511` |
| 2.13c | `_prev_bday(DATA_STR)` not `str(_prev_bday(DATA))` | ✅ DONE | `4fee511` |
| 2.13d | `generate_credit_report` `nav_at or 0.0` cascading | ✅ DONE | `84f465d` |
| 2.13e | `data_fetch.fetch_book_pnl` `abs(...) or None` guard | ✅ DONE | `4fee511` |
| 2.13g | float-zero anti-pattern (3 sites) | ✅ DONE | `84f465d` |
| 2.13i | PA-FX-split sort_key consistency (abs YTD) | ✅ DONE | `4fee511` |
| 2.13j | m12 SQL window `>` → `>=` (5 files) | ✅ DONE | `4fee511` |
| 2.13k | 2 SyntaxWarnings (Python 3.12-readiness) | ✅ DONE | `777dc94` |
| 3.1 | unused imports cleanup (19 names total) | ✅ DONE | `777dc94` + `4fee511` |
| 3.2 | vacuous comments cleanup (~12 sites) | ✅ DONE | `84f465d` |
| 3.3 | misleading docstrings (compute_pm_hs_var, compute_distribution_stats, REP_RET_CLEAN_BPS, ret_window) | ✅ DONE | `4fee511` |
| 3.3a | NaN-fragile `or "—"` (4 sites) | ✅ DONE | `777dc94` |
| 3.3b | Q1/Q3 quartile bucket drift | ✅ DONE | `4fee511` |
| 3.3d | `read_text()` explicit `encoding="ascii"` | ✅ DONE | `777dc94` |
| 3.3f | `ret_window` None-semantics docstring | ✅ DONE | `4fee511` |
| 4.4 | Dead `DATE_60D` removed | ✅ DONE | `4fee511` |
| 4.5 | svg_renderers `#fb923c` alert color invariant comment | ✅ DONE | `4fee511` |
| 4.6 | PA-FX-split docstring slim-down (4 scripts) | ✅ DONE | `4fee511` |

### Still open (deferred — bigger sessions)
- §1.4 EVOLUTION BRLUSD legacy non-zero (escalation to PA engine owner, not code)
- §1.5 `build_html` extraction — **in progress**, 524L extracted in session 6 + 180L extracted in session 7; ~540L remaining (master HTML template f-string, body composition, orchestration glue)
- §2.3 Breakdown por Fator unit-mixing
- §2.5 Risk Budget thresholding by margem (bps) — full re-thresh + "days to soft breach" projection
- §2.6 Frontier perpetual "—" (TE-based metric needed)
- §2.9 / §2.10 carryover iterrows vectorization
- §2.13b IDKA SHARE flow PnL (cotização axis mismatch)
- §3.5 dado/tomado wording (deferred — design call), §3.6 convention footnote dedup (9×→1), §3.7 `data-no-sort` audit (tedious), §3.8 meeting-port hardcoded hex sweep
- §4.1 Python venv discipline (`from __future__ import annotations`)
- §4.2 CLI entry-point convention

---

## TL;DR — top 5 things to fix

1. **`pnl_server.py --port 5050` crashes immediately** — `risk_runtime` imports `_parse_date_arg(sys.argv[1])` greedily, which reads `'--port'` as a date and exits via `sys.exit(...)`. Verified at [`risk_runtime.py:27`](../risk_runtime.py#L27). Same crash for `generate_credit_report.py --fund SEA_LION` and any future entry-point with non-date argv[1]. *True HIGH — unrelated tools currently broken.*
2. **`generate_risk_report.py:5113` reintroduces `or 1.0` divisor** that the prior audit's #2 fix was supposed to eliminate. Still in the D-1-fallback branch: `macro_aum = (_aum_raw or _latest_nav("Galapagos Macro FIM", d1_str) or 1.0)`. If both lookups fail, every %NAV/bps for MACRO renders ~10⁵× actual. Use `_require_nav` and let it raise.
3. **Briefing copy is vapid.** 8 of 19 fund-card briefings render the boilerplate `"dia tranquilo — sem eventos materiais"` even on funds that materially moved (ALBATROZ −0.53% MTD with VaR ↑7 bps; FRONTIER −10.83% YTD vs IBOV +16.26%). Tighten the rule-based gate (only "tranquilo" when ALL of util_VaR<50% / |dia_alpha|<3 bps / |Δ VaR|<3 bps / |MTD|<25 bps); ship LLM substitution per CLAUDE.md §7 fila #2.
4. **`metrics.py:392` silently drops every short position** from PA outlier detection — the filter `(merged["today_pos"] > 0)` excludes negative positions entirely, so Bracco / Quant_PA shorts and others never produce a z-score even on outlier days. Replace `> 0` with `.abs() > 1e-9`.
5. **Skills (`.claude/skills/**/SKILL.md`) actively mislead.** Five skills claim features are pending that have shipped (`macro-stop-monitor` says carry rule is "em revisão" while `fund_renderers.carry_step` has been live for weeks). All eight reference a non-existent `glpg-data-fetch` skill instead of `glpg_fetch.py`. ~30 min refresh sprint.

---

## 1. HIGH IMPACT — fix soon

### 1.0 [code] `risk_runtime.py:27` — sys.argv[1] parsed as date crashes pnl_server.py and similar

**Where:** `risk_runtime.py:27`
```python
DATA_STR = _parse_date_arg(sys.argv[1]) if len(sys.argv) > 1 else (...)
```
This runs at import time for *every* consumer of `risk_runtime` (which is ~all modules via `data_fetch`).

**What's wrong:** any entry-point script that has its own argparse with non-date argv[1] crashes:
- `pnl_server.py --port 5050` → `risk_runtime` reads `'--port'` as date → `sys.exit("Error: date must be YYYY-MM-DD, got '--port'")`. Verified: `pnl_server.py:151` uses `--port` flag.
- `generate_credit_report.py --fund SEA_LION` — same shape.

**Concrete fix:** only consume `sys.argv[1]` when it matches `^\d{4}-\d{2}-\d{2}$`. Replace:
```python
import re
DATA_STR = (sys.argv[1]
            if len(sys.argv) > 1 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", sys.argv[1])
            else _default_date())
_parse_date_arg(DATA_STR)  # validate once we've decided
```

### 1.0b [code] `generate_risk_report.py:5113` — `or 1.0` NAV regression

**Where:** [`generate_risk_report.py:5113`](../generate_risk_report.py#L5113), inside the D-1-fallback branch when today's exposure missed the lote run:
```python
df_expo, df_var, macro_aum = _expo_d1_raw, _var_d1_raw, (_aum_raw or _latest_nav("Galapagos Macro FIM", d1_str) or 1.0)
```

**What's wrong:** the prior audit's #2 fix replaced `or 1.0` divisor patterns with `_require_nav`. This site survived. If both `_aum_raw` and the D-1 NAV lookup return falsy, `macro_aum = 1.0` and every downstream %NAV / bps calc for MACRO becomes `delta_brl × 100 / 1.0` — values appear ~10⁵× their real magnitude.

**Concrete fix:** replace `or 1.0` with `_require_nav("Galapagos Macro FIM", d1_str)` and let it raise; OR guard the entire fallback branch with an explicit NAV check + skip-with-WARN that keeps the rest of the report readable.

### 1.0c [code] `metrics.py:392-396` — `compute_pa_outliers` silently drops every short position

**Where:** [`metrics.py:392-396`](../metrics.py#L392)
```python
valid = (merged["sigma"] > 1e-9) & (merged["today_pos"] > 0)
merged.loc[valid, "z"] = (
    (merged.loc[valid, "today_bps"] / merged.loc[valid, "today_pos"])
    / merged.loc[valid, "sigma"]
)
```

**What's wrong:** the `(today_pos > 0)` filter excludes every row where today's position summed negative — i.e., short books. z-score is forced to 0 for shorts; the statistical-outlier branch can't trigger for them. The absolute-floor branch still works (catches |bps|≥10 bps), but the σ-test is silently disabled for the entire short side.

**Verified affected positions:** Bracco / Quant_PA shorts (QUANT), USD/X shorts (MACRO_Q), IBOV future shorts (EVOLUTION) — all carry negative position_brl in REPORT_ALPHA_ATRIBUTION. Their daily implied returns never enter the σ estimator (`metrics.py:373` has the same bug on the history side).

**Concrete fix:** replace `> 0` with `.abs() > 1e-9` at lines 373 and 392, and divide by `today_pos.abs()` at 394.

### 1.0d [code] `expo_renderers.py:1707, 1717` — Albatroz/BALTRA "Dur" column double-applies MOD_DURATION

**Where:** [`expo_renderers.py:1707`](../expo_renderers.py#L1707) and 1717, in the Albatroz/BALTRA Exposure RF card.

**What's there now:**
```python
dur_w = (df["delta_brl"].abs() * df["mod_dur"]).sum() / abs_delta
```

**What's wrong:** `delta_brl` is already `POSITION × MOD_DURATION` (per `data_fetch.fetch_albatroz_exposure` docstring + CLAUDE.md §8 "DELTA já é duration-weighted"). Substituting:
```
dur_w = Σ(|POS×MD| × MD) / Σ|POS×MD| = Σ(|POS|×MD²) / Σ(|POS|×MD)
```
That's a duration-weighted-position-weighted MD — biased toward the longest-MD instruments. For a 50/50 split between MD=2y and MD=10y same-position bonds, true weighted-MD = 6y; this formula returns ~8.67y.

**Concrete fix:**
```python
dur_w = df["delta_brl"].abs().sum() / (df["delta_brl"].abs() / df["mod_dur"]).sum()
# since Σ|delta| = Σ(|POS|×MD), Σ(|delta|/MD) = Σ|POS| → ratio = position-weighted MD
```
Visible in the parent "Pré"/"IPCA"/"Outros" rows of the Albatroz/BALTRA Exposure RF card.

### 1.0e [code] `credit/credit_data.py:140-156` — `fetch_price_quality_flags` explodes on multi-book products

**Where:** [`credit/credit_data.py:140-156`](../credit/credit_data.py#L140) — the today/prev CTE LEFT JOIN on PRODUCT alone.

**What's wrong:** if the same PRODUCT appears in N BOOKs today and M BOOKs in prev, the join produces N×M rows. Sea Lion / Iguana hold the same NTN-B 2030 across two books (Caixa + RF Curto) → 2 today rows × 2 prev rows = 4 combined rows; if either price is null, the flag count quadruples.

**Effect:** the price-sanity-check section in the Sea Lion / Iguana / Pelican / Dragon / Barbacena / Nazca PDFs (emailed to 31 recipients) inflates the count of "missing/zero price" warnings.

**Concrete fix:** GROUP BY PRODUCT in both CTEs (sum POSITION, take `MAX(PRICE) FILTER(WHERE PRICE > 0)`), or join on `(PRODUCT, BOOK)`.

### 1.0f [code] `generate_monthly_review.py:372` — IDKA BVaR sign-flip with no abs()

**Where:** [`generate_monthly_review.py:372`](../generate_monthly_review.py#L372)
```python
sub["var_pct"] = -sub["bvar_raw"] * 100  # IDKA path
```

**What's wrong:** for RPM funds (line 311) and RAW funds (344), the convention is `var_abs.abs() / nav * 100`. The IDKA branch relies on `RELATIVE_VAR_PCT` always being negative-signed. If the engine ever emits a positive value (mid-day partial population, sign drift), the resulting `var_pct` is negative and the `max()` call at line 374 silently returns the LEAST-negative — **under-reporting risk**.

**Concrete fix:** `sub["var_pct"] = sub["bvar_raw"].abs() * 100`. Same fix anywhere `RELATIVE_VAR_PCT` is read upstream — audit `data_fetch.py` and `metrics.py`.

### 1.1 Briefing quality (rule-based → LLM)

**Where:** `fund_renderers._build_fund_mini_briefing` (per fund_renderers.py); rendered into `<p class="brief-commentary">` and `<div class="brief-headline">` in the morning HTML.

**What's there now:** Of 19 brief-* card matches in the rendered HTML, 8 contain the literal "dia tranquilo — sem eventos materiais." Verified by direct regex over the latest `2026-04-30_risk_monitor.html`.

**Why it's wrong:**
- ALBATROZ (line ~6371): "dia tranquilo" but VaR ↑7bps, Outlier flagged DI1F31 driver, stop budget at 35% consumido, MTD −0.53%, YTD −6.30%.
- FRONTIER (line ~7612): MTD −0.25%, YTD −10.83%, "dia tranquilo". A fund −10.83% YTD on a small-MTD day is not "tranquilo".
- MACRO (line ~4076): "tranquilo" but elsewhere the report shows MACRO · LF margem de stop = 9 bps (near-breach).

**Concrete fix:** rule-based briefing should ONLY say "tranquilo" when ALL of:
- `util_VaR < 50%`
- `|dia_alpha| < 3 bps`
- `|Δ VaR D-1| < 3 bps`
- `|MTD| < 25 bps`
- `util_stop < 35%` (where applicable)

For everything else, surface the most decision-relevant single fact: "MTD {x}% · stop {y}% consumido — {top_PA_detractor} principal" or "Util VaR {x}% — {bps_to_soft_breach} bps de espaço". The full LLM substitution (Haiku 4.5 per CLAUDE.md §7) is the right long-term path; the rule-tightening is a no-LLM stopgap.

### 1.2 Briefing Executivo headline priority

**Where:** Briefing Executivo card (line ~2516–2540 in HTML; building function in `summary_renderers.py`).

**What's there now:** Headline = "IDKA 10Y VaR subiu 14 bps vs D-1 (agora 0.66%)". Below, under "Atenção", a quieter line reads "MACRO · LF margem de stop em 9 bps".

**Why it's wrong:** IDKA 10Y is at ~66% utilization (green). 14 bps Δ on a benchmarked RF fund where util is comfortable is informational. 9 bps margem on an active PM means a single bad day breaches. The page is pointing the user at the wrong number.

**Concrete fix:** headline priority key = `max(stop_margem_bps_remaining_inverse, util_VaR_pct, abs(Δ_VaR_bps))`. When any PM has < 25 bps margem, headline becomes "MACRO · LF margem 9 bps — risco de breach intramês." Reorder so "Atenção" goes ABOVE "Risco · o que mudou".

### 1.3 Status consolidado — DIA fallback when PA is on D-1

**Where:** Status consolidado card (line ~2545–2570 of rendered HTML; renderer in `summary_renderers.py`).

**What's there now:** All 9 funds show DIA="—" (PA on D-1). Bench rows (IBOV +1.39%, CDI +0.05%) DO show DIA. Status dos Dados card 1,000 lines later explains the lag in yellow but it's far from the silent cells.

**Why it's wrong:** A user can't tell whether DIA="—" means "no move" or "data missing". The bench rows being filled while fund rows are empty makes the table look broken.

**Concrete fix:** when PA is on D-1, render the D-1 value with an explicit "(D-1)" tag, e.g. "+0.04% (D-1)". Add a row-level lag indicator next to fund name: "Macro ⏱D-1". Or render "—" with cell tooltip "PA pendente para 2026-04-30 — última obs D-1".

### 1.4 EVOLUTION's BRLUSD rows haven't fully zeroed (upstream PA-engine issue)

**Where:** `q_models.REPORT_ALPHA_ATRIBUTION` for `FUNDO='EVOLUTION'` — direct DB query.

**What's there now:** On 2026-04-29 (post-cutover), EVOLUTION emits BOTH:
- legacy `CLASSE='BRLUSD'/GRUPO='BRLUSD'/SUBCLASSE='USD Brasil'` = −0.32 bps
- new `CLASSE='FX Carry & Bases Risk'/GRUPO='BRLUSD'/SUBCLASSE='FX Carry & Bases Risk'` = −7.73 bps

For MACRO/QUANT/GLOBAL on the same date, the legacy rows are present but with PnL=0 (cleanly migrated).

**Why it's wrong:** Either (a) EVOLUTION's PA engine is in partial migration, or (b) the legacy USD Brasil row represents a real exposure NOT covered by the new bucket. Without engine-owner confirmation, we can't tell. Today's `_apply_fx_split_remap` correctly sums both, so the bucket TOTAL is right — but the underlying data shape is inconsistent.

**Concrete fix:** ask the PA engine owner:
1. Should EVOLUTION's `BRLUSD/USD Brasil` zero out on 2026-04-24 like it did for the other funds?
2. If yes: it's a transition-lag bug; flag for upstream cleanup.
3. If no: document why; possibly the new `FX Carry & Bases Risk` row isn't a complete replacement and we shouldn't fold them.

### 1.5 `build_html` mega-function (carryover, still HIGH)

**Where:** `generate_risk_report.py:529–4949` — 4,420 lines, single function.

**Action (from prior audit, still open):** extract tab-switching + per-fund section builders into `_build_tab_*` helpers; move CSS/JS blob to `html_assets.py`. Largest single-function refactor in the project. Risk-mitigated by `smoke_test.py --save-snapshot`.

### 1.6 `main()` 540 lines (carryover, still HIGH)

**Where:** `generate_risk_report.py:4949+`.

**Action:** split into `_fetch_all_data()`, `_build_report_data()`, `_write_output()`. C901 noqa is the smell.

---

## 2. MEDIUM IMPACT — this month

### 2.1 Skill files actively mislead (DOCS_DRIFT)

Five `.claude/skills/*/SKILL.md` files claim features pending that have shipped:

- [`macro-stop-monitor/SKILL.md:66-72, 198, 220`](../.claude/skills/macro-stop-monitor/SKILL.md) — claims carry rule is "em revisão" and the auto-calculation "não é executada". **Not true** — `fund_renderers.carry_step` (line ~2888) has been live and is documented in memory `project_rule_macro_carry_step.md`. Reader will avoid touching the carry logic, missing fixes.
  - **Fix:** delete the "regra pendente" flag and placeholder code block; point to `fund_renderers.carry_step` and `docs/CHANGELOG.md`.

- [`performance-attribution/SKILL.md:27-28, 41-47`](../.claude/skills/performance-attribution/SKILL.md) — table marks ALBATROZ and MACRO_Q as "⚠️ a confirmar" when both are wired (CLAUDE.md §6 column "PA" = ✅). BALTRA is missing entirely.
  - **Fix:** mirror CLAUDE.md §6 column "PA" verbatim; drop "Pontos pendentes".

- [`risk-daily-monitor/SKILL.md:13, 20, 42, 56`](../.claude/skills/risk-daily-monitor/SKILL.md) — references "MACRO_TABLES_GRAPHS.py", "SISTEMATICO_TABLES_GRAPHS.py" as canonical sources. They're not — `data_fetch.py` is. Naming "SISTEMATICO" is also stale (canonical is `QUANT`).
  - **Fix:** drop SISTEMATICO refs; point to `generate_risk_report.py` + `data_fetch.py`.

- [`risk-data-collector/SKILL.md`](../.claude/skills/risk-data-collector/SKILL.md) — 149-line spec for a JSON-manifest collector with cron jobs at 03:00/04:00/05:00, plus `rotinas-geradoras.json` + `bases-checklist.md`. **None of those files exist.** Design abandoned; freshness check now lives in `generate_risk_report.py` + `smoke_test.py`.
  - **Fix:** delete the SKILL.md or replace with a 5-line stub.

- 8 files still reference `glpg-data-fetch` as if it were a skill — it's a Python module `glpg_fetch.py`. Fix with a single `rg --replace` across `.claude/skills/`:
  ```
  .claude/skills/evolution-risk-concentration/references/metodologia-percentis.md:144
  .claude/skills/evolution-risk-concentration/references/queries-evolution.md:3
  .claude/skills/evolution-risk-concentration/SKILL.md:24
  .claude/skills/macro-risk-breakdown/references/queries-breakdown.md:140
  .claude/skills/macro-risk-breakdown/SKILL.md:18
  .claude/skills/macro-stop-monitor/SKILL.md:11, 237
  .claude/skills/performance-attribution/SKILL.md:11
  .claude/skills/rf-idka-monitor/references/queries-idka.md:3
  .claude/skills/rf-idka-monitor/SKILL.md:23
  ```

### 2.2 Status table — "Total (soma)" is not actually a sum

**Where:** Risco VaR e BVaR card, footer row (line ~2586).

**What's there now:** "Total (soma) · 815,1M · 0.69% · — · 0.44%". The 0.69% and 0.44% are NAV-weighted means, but the row label says "soma".

**Why it's wrong:** A risk manager reading "Total soma 0.69%" might infer this is undiversified house VaR — it's not.

**Concrete fix:** rename row to "Total NAV-weighted (não-diversificado)". If `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` has a "house" key (or compute it from sum-of-fund-weighted-W), add a second line "HS diversificado: X.XX%" with the diversification benefit `(soma - HS) bps`.

### 2.3 Breakdown por Fator mixes units (yrs and %) in the same column

**Where:** Breakdown por Fator card (line ~2599–2611).

**What's there now:** "Juros Reais (IPCA) Total -0.42 yrs" right above "IPCA Idx Total +4.53%". Same column, mixed units.

**Why it's wrong:** Units aren't comparable; eye-scanning fast a user could add yrs and % mentally. Worse, IPCA Idx +53.44% on BALTRA is an inflation-carry exposure (parking-lot for inflation accrual) that visually competes with directional factors.

**Concrete fix:** split into 2 stacked tables — "Duration factors (yrs)" above, "Notional factors (% NAV)" below. Hide IPCA Idx by default (show via toggle "incluir carry inflação"). Add a unit chip or color-coded left border per row.

### 2.4 "Mudanças Significativas" — D-1 = 0 silent failure

**Where:** Mudanças Significativas card (line ~2700–2750).

**What's there now:** Quantitativo Equity BR D-1=+0.00%, D-0=+4.40%, Δ=+4.40%. Frontier rendering D-1=0.00, D-0=+89%, Δ=+89% — clearly a Frontier `LOTE_PRODUCT_EXPO` D-1 query failed silently.

**Why it's wrong:** When D-1 lookup fails, the renderer shows 0 instead of "n/a", making Δ = D-0. The `|Δ| ≥ 0.30%` filter then promotes pure data-failure rows.

**Concrete fix:** when D-1 is null/missing, render "n/a" not "+0.00%" and gate row out of the Δ filter (don't show row at all if D-1 is missing). The Frontier "Δ +89%" line is pure artifact.

### 2.5 Risk Budget Monitor — MACRO LF "🟢 OK" with margem 9 bps

**Where:** Risk Budget Monitor card (line ~3773–3873).

**What's there now:** CI=+238, LF=+9, JD=+117, RJ=+148. All "Margem (bps)" cells solid blue regardless of value. LF Status icon = 🟢 OK.

**Why it's wrong:** 9 bps of margem on a 63 bps/mês base is ~14% of monthly budget. By Status consolidado convention (verde<70%, amarelo 70–100%), 9 bps margem should already be 🟡 or 🟠. The Briefing's "Atenção: MACRO LF margem 9 bps" contradicts the 🟢 in the same page.

**Concrete fix:** re-thresh by abs margem_bps, not just SEM/ANO percent: margem < 25 bps → 🟡; < 10 bps → 🟠; ≤ 0 → 🔴. Color the "Margem" column by value. Add column "Days to soft breach @ current burn rate" = `remaining_bps / avg_daily_loss_5d`.

### 2.6 Frontier — Util VaR perpetually "—"

**Where:** Status consolidado, row 7 (Frontier).

**What's there now:** "Frontier · VaR 0.86% · Util VaR — · Δ —". The row is permanently grey/inert.

**Why it's wrong:** Frontier moved −10.83% YTD vs IBOV +16.26% — a 27 pp drag — but appears as "all good" every day. For a long-only fund the relevant constraint is tracking error or excess-return budget, not VaR.

**Concrete fix:** for Frontier specifically, replace Util VaR with a TE-based metric ("TE 8.2% / yr · {x}% of soft TE budget") or cumulative ER-vs-IBOV YTD, color-coded.

### 2.7 Top Posições — "% da posição" semantics ambiguous

**Where:** Top Posições card (line ~2756–2778).

**What's there now:** Under DAPK35: "Baltra +60% da posição", "IDKA 10Y +24%", "Albatroz +14%". Sums to 100%.

**Why it's wrong:** "% da posição" reads as "% of the fund's NAV" but means "% of the instrument's total across all funds that hold it". Mixed with fund-level R$ in the same drilldown.

**Concrete fix:** rename Lvl 2 header from "Detalhe" to "Fundo · share do instrumento". Or split: "Σ R$ no fundo" and "% do total instrumento" as two columns.

### 2.8 EVOLUTION — Commodities (with trailing space) is a separate CLASSE

**Where:** DB-side issue in `q_models.REPORT_ALPHA_ATRIBUTION`.

**What's there now:** Verified: `SELECT "FUNDO", "CLASSE", LENGTH("CLASSE")` returns 27 rows for `'Commodities '` (12 chars, with trailing space) across EVOLUTION (12 rows), MACRO (9), QUANT (3), GLOBAL (3) for the past 5 days.

**Why it's wrong:** The `_apply_fx_split_remap` strip already handles `Commodities ` for FX rows but the OUTER PA tree builders group by CLASSE without strip — `Commodities` and `Commodities ` show as twin top-level rows in the FX-split scripts (visible in `2026-04-30_evolution_pa_fx_split.html`).

**Concrete fix:** strip CLASSE in `_apply_fx_split_remap` (currently strips GRUPO only — easy 1-line addition). Better: normalize at the SQL fetch layer in `data_fetch.py` for any function pulling from REPORT_ALPHA_ATRIBUTION — apply `TRIM("CLASSE")` in the SELECT or WHERE the DB writer hygiene ships.

### 2.9 `data_fetch.py:1417` — iterrows in nested loop O(n²) (carryover)
### 2.10 `generate_risk_report.py:713,1673,1695` — iterrows in HTML row loops (carryover)

Both unchanged from prior audit. Vectorize.

### 2.11 `data_fetch.py:1910` — `float(fut["delta"].iloc[0])` after `.empty/.notna` guard (carryover)

Use `pd.to_numeric(..., errors='coerce')` instead.

### 2.12 Mirror path hardcoded (carryover)

`generate_risk_report.py:5477,5480` and `generate_monthly_review.py:1764` — hardcoded `F:\Bloomberg\Risk_Manager\Data\Morningcall`. Move to `RISK_MIRROR_PATH` env var; log failures as WARN.

### 2.13a [code] `data_fetch.py:1199, 1227-1228` — VaR DoD denominators all use today's NAV

The function `_var_dod_rpm` divides D-1 contributions by D-0 NAV. After a chunky subscription/redemption between D-1 and D, the D-1 contribution is mis-scaled by the NAV ratio. Polluting `delta_bps = contrib_d - contrib_d1` and the `pos_effect / vol_effect` decomposition.

**Concrete fix:** add `nav_d1: float` parameter and use it for the D-1 side. `_var_dod_idka` (line 952) and `_explode_albatroz_for_idka` (lines 1097-1098) already do this correctly — propagate the same pattern up.

### 2.13b [code] `metrics.py:115-171` — `compute_idka_bvar_hs` uses fund SHARE that includes flow PnL

Uses `LOTE_TRADING_DESKS_NAV_SHARE.SHARE` and pct_change()s it as the fund return. SHARE is the cota — flow-adjusted, but for IDKAs which have D-2 cotização rules, the SHARE on date D reflects activity from D-2 while the bench INDEX moves on D's calendar. Mismatch produces spurious daily active returns averaging ~0 but with wider σ → BVaR overstated by ~10-30% vs the engine's `RELATIVE_VAR_PCT`.

**Concrete fix:** either fall back to engine-computed `RELATIVE_VAR_PCT`, or shift bench by 2 bdays for IDKAs to align with cotização date axis.

### 2.13c [code] `data_fetch.py:5043` — `_pnl_date = str(_prev_bday(DATA))` passes Timestamp not str

`_prev_bday` is typed `(date_str: str)` but is called with `pd.Timestamp`. `str(Timestamp("2026-05-01"))` = `"2026-05-01 00:00:00"`. PostgreSQL accepts `DATE '2026-05-01 00:00:00'` (truncates), so today the query works — but a future tightening of either path will silently break.

**Concrete fix:** pass `DATA_STR` (already a string) instead of `DATA`, or coerce at the function entry.

### 2.13d [code] `generate_credit_report.py:2066, 1666` — `nav_at = ... or 0.0` cascades to division

Same pattern as the MACRO `or 1.0` — `nav_at = fetch_nav_at(...) or 0.0`, then `pct_pl = pos_brl / nav` produces inf/NaN. % cells then render as "inf%" or NaN-propagate through any `.sum()`.

**Concrete fix:** if NAV missing, early-return (or render explicit "NAV missing" placeholder); don't fall back to 0.0.

### 2.13e [code] `data_fetch.py:2966` — `abs(float(fdf["AMOUNT"].sum())) or None`

If Σ|AMOUNT| = 0 (empty book set, cancelled fund), `fund_nav = None`, then `_bps_pct` returns 0.0 for every position — every PL bps reads as 0 in the daily monitor with no warning.

**Concrete fix:** `if fund_nav is None or fund_nav <= 0: skip + log WARNING`.

### 2.13f [code] `expo_renderers.py:82, 127` — D-1 net_pct divides by today's NAV

Same shape as 2.13a; pass `nav_d1` separately. Or document the "delta-only, ignore NAV swings" convention explicitly in the renderer if intentional.

### 2.13g [code] Float-zero comparison anti-pattern (3 sites)

- `expo_renderers.py:1977` — `np.where(rf_delta != 0, ...)` — exact float comparison, fragile if exposures cancel exactly.
- `generate_credit_report.py:209` — `df["cdi"] != 0` — CDI never exactly 0 in production but ECO_INDEX has occasional zero rows.
- `credit/credit_data.py:160-161` — `df["price_today"] == 0` treats a legitimately-defaulted bond at 0.0 as "missing".

**Concrete fix:** use `.abs() > 1e-9` or `np.isclose(..., 0)` with explicit `atol`. For credit, use `.abs() < 1e-6` for "near-zero" with separate flag for "exactly-zero = defaulted".

### 2.13h [code] `db_helpers.py:73` + `data_fetch.py:2920` + `risk_config.py:24` — desk name "Frontier Ações FIC FI" hardcoded in 3 places, with inconsistent escapes

Today works because Python normalizes — but a `==` byte-comparison from a different DB driver encoding would miss. Same issue for `_ALBATROZ_DESK`, `_MACRO_DESK`, etc.

**Concrete fix:** define `_FRONTIER_DESK = "Frontier Ações FIC FI"` once in `risk_config` and import everywhere.

### 2.13i [code] PA-FX-split sort_key inconsistency

`generate_macro_pa_fx_split.py:108, 130` sorts by signed YTD descending (no abs key). `generate_evolution_pa_fx_split.py:133-135` uses `key=lambda s: s.abs() if s.name == "ytd_bps" else s` — by magnitude. Inconsistent across 4 sister scripts.

**Concrete fix:** pick one convention, apply everywhere. "abs YTD desc" is more useful (shows biggest movers regardless of sign).

### 2.13j [code] `m12_bps` SQL window uses `>` instead of `>=` (data_fetch.py:2543, 2829, all 4 PA-FX-split)

`SUM(CASE WHEN "DATE" > (DATE '{date_str}' - INTERVAL '12 months') ...)` excludes the boundary day. m12 is computed on ~251 trading days, not 252. Inconsistent with MTD/YTD which use `>=`.

**Concrete fix:** change `>` to `>=` for the 12-month boundary OR change MTD/YTD to `>` for consistency. Document the convention.

### 2.13k Two SyntaxWarnings on compile (Python 3.12-readiness)

- `generate_monthly_review.py:1168` — `'\s'` invalid escape (in JS-in-Python f-string per CLAUDE.md §8). Use `\\s` or raw string.
- `pm_vol_card.py:3` — `'\B'` invalid escape (docstring referencing `F:\Bloomberg\...`). Use `r"""..."""`.

Both will become `SyntaxError` in Python 3.12+.

---

## 3. LOW IMPACT — when convenient

### 3.1 33 unused imports across 12 root files

AST-verified scan (4 already removed today via commit 9cfc673):

| File | Unused |
|------|--------|
| `data_fetch.py` | DATA, DATE_60D, _NAV_CACHE, _QUANT_VAR_BOOK_FACTOR, _RATE_PRIM, date |
| `expo_renderers.py` | DATA_STR, FUND_LABELS, compute_distribution_stats, fetch_rf_exposure_map, json, make_sparkline, range_line_svg, stop_bar_svg |
| `generate_risk_report.py` | compute_pa_outliers, field, read_sql |
| `credit_card_renderers.py` | 4 |
| `generate_credit_report.py` | 2 |
| `generate_monthly_review.py` | 2 |
| `pmovers_renderers.py` | 1 |

5-min job: run my AST scan, delete the listed names, ensure smoke_test passes.

### 3.2 13 vacuous / stale comments to delete

Pure subtractive cleanup, ~30 min, no functional risk:

- `generate_risk_report.py:223` — `"# ── Fetch data ──"` followed by no code before next banner
- `generate_risk_report.py:530` — `"# Unpack for readability inside this function — no logic changes below this block."` (self-evident)
- `generate_risk_report.py:1031` — `"# Performance Attribution — one card per fund (MACRO, QUANT, EVOLUTION, MACRO_Q, ALBATROZ)"` — loop iterates `FUND_ORDER` (9 funds, not 5)
- `generate_risk_report.py:1182` — `"# Which fund×report combinations exist — used to enable/disable tabs"` (variable already named `available_pairs`)
- `generate_risk_report.py:1905` — `"# (Análise helpers and per-fund loop were relocated above ..., see earlier block.)"` — refactor leftover
- `generate_risk_report.py:1925` — `"# JS constant: which reports exist per fund (for the jump bar)"` (paraphrases `fund_reports_js`)
- `generate_risk_report.py:1944` — `"# Por Report mode removed — no per-report fund switcher needed"` — STALE; mode still exists at line 1911
- `generate_risk_report.py:5172` — `"# df_pa already resolved above (used to build df_today)"` — stale flow comment
- `expo_renderers.py:1214` — `swap_row = ""  # removido: swap plug não faz sentido ao comparar tudo vs IDKA` — dead variable + tombstone
- `expo_renderers.py:1398` — `"# Legacy names kept for the stat-row / legend blocks below"` (followed by an alias) — drop the comment, ideally rename downstream
- `generate_credit_report.py:1123` — `"# Combine all tenors + rates to set axis bounds"` (next line is min/max calls)
- `generate_monthly_review.py:1666-1667` — `"# Validate format"` followed by `import re` (next call is `re.fullmatch(r"\d{4}-\d{2}", ym)`)
- `data_fetch.py:2391` — `"# Drop excluded rows (Cash/Margin/Provisions) before computing pct_nav"` (the `_EXCL_BOOKS` constant carries the meaning)

### 3.3 4 misleading docstrings

- `metrics.py:23-31` — docstring opens "Parametric 1d VaR per PM from the Historical Simulation series". Technically correct (parametric on top of HS data) but collides with the project's "HS vs realized" rule. Add explicit "Não é o quantil empírico (use compute_pa_outliers para isso)."
- `metrics.py:320` — `"Returns dict with forward-looking stats and (optional) backward comparison"`. "Forward-looking" suggests prediction; it's just empirical W-window stats. Replace with "Stats descritivos da série W (HS portfolio)."
- `fund_renderers.py:1823` — `"# Use the canonical mapping (includes IDKAIPCAY3 / IDKAIPCAY10)."` — IDKA keys in `_FUND_PA_KEY` are `IDKA3Y` / `IDKA10Y` (no IPCA prefix). Mismatch.
- `export_idka_repl_vs_bench.py:124` — `daily["REP_RET_CLEAN_BPS"] = ... # legacy clean-price (with coupon artifact)` — either delete the column (if no consumer) or expand comment to "mantida apenas para diagnóstico do artifact; não consumir em PA."

### 3.3a [code] `generate_evolution_pa_fx_split.py:174` — `lv["LIVRO"] or "—"` is NaN-fragile

`NaN or "—"` returns NaN (NaN is truthy in Python's `or`). Then `(liv).replace(' ', '_')` would raise. Today protected by pandas `dropna=True` default — a future pandas change breaks this. Same in `generate_macro_pa_fx_split.py:152` and `generate_quant_pa_fx_split.py:101`.

**Concrete fix:** `liv = "—" if pd.isna(lv["LIVRO"]) else lv["LIVRO"]`.

### 3.3b [code] `generate_market_review.py:861` — Q1 quartile bucket math drift for tiny n

`q1 = asc[max(0, n // 4 - 1)]` for n=4 returns asc[0] (the worst). For n=8 returns asc[1] (12.5%-ile, not 25%-ile). Box plots biased low by half a bucket on small peer sets.

**Concrete fix:** use `pd.Series(asc).quantile(0.25)` or `np.percentile(asc, 25)`.

### 3.3c [code] `credit/credit_data.py:115-119` — issuer override hardcoded as "Cruz" string

`if has_cruz.any(): df.loc[has_cruz, "issuer_group"] = "Santa Cruz"` — hardcoded business rule in a data layer module. Today: 1 rule. Tomorrow: scattered if-blocks.

**Concrete fix:** move to `credit/issuer_overrides.json` or a `credit.issuer_overrides` DB table.

### 3.3d [code] `credit/credit_config.py:13` — `read_text()` without explicit encoding on Windows

Uses Windows default codepage (cp1252). The b64 file is ASCII so today works. Trivial defense-in-depth: `.read_text(encoding="ascii")`.

### 3.3e [code] `generate_market_review.py:225-262` — `_zscore_summary` "d5" mismatch

`s.iloc[-6]` for `d5` index uses raw series; z-score uses dropna'd series. After dropna, lengths differ — `z.iloc[-6]` is 5 *valid-z* days back, not 5 calendar days back. Fine in steady state with `min_periods=60`; unintuitive at window start.

**Concrete fix:** document in the docstring; or use date-based slicing instead of positional.

### 3.3f [code] `generate_market_review.py:909-936` — `ret_window` returns None for "fund too young"

Same code path returns None for both "data unavailable" and "fund <2 years old". User sees "—" with no explanation.

**Concrete fix:** return sentinel "inception <" when len < window, distinct from data-unavailable None.

### 3.4 Aesthetics carryovers

- `svg_renderers.py:71,149,222` — `#2d2d3d` and `#0b1220` not in `_BG_HEX_MAP`; verify projector contrast.
- `fund_renderers.py` (11 sites) — raw f-string numerics not wrapped in `_fmt_br_num`.

### 3.5 Convention semantics — "dado/tomado" green/red

In Brazilian rates desk parlance "estar dado em juros" can mean "long the rate (taker)" — opposite of the convention in this report. The legend (line ~2538) explains, but the headline "casa está dado em Juros Reais (IPCA) · −345.8M líquido do bench" with a green color is genuinely ambiguous to readers raised on different desk slang.

**Fix:** replace "dado/tomado" with "long bond / short bond" or "long DV01 / short DV01" in the headline. Keep the convention key as glossary.

### 3.6 Convention footnote repeats 9× per page

The same 4-line "dado/tomado" convention block renders inside every per-fund briefing card (lines 4082, 4777, 5707, 5902, 6377, 6716, 7618, 8153, 8688) and at the Briefing Executivo. On the meeting projector port (CLAUDE.md §3a) the bigger fonts will make this dominant.

**Fix:** show convention only in Briefing Executivo + first fund whose briefing actually mentions "dado/tomado". For others, replace with a single-line "ⓘ <a>convenção dado/tomado</a>" tooltip.

### 3.7 65 `data-no-sort="1"` attributes vs §10 convention

CLAUDE.md §10: "toda tabela tem sorting por padrão". The actual practice: 65 opt-outs, with sortability often only on tables given the `summary-table` class. For flat-structure tables (Risco VaR e BVaR, Vol Regime, Mudanças sub-tables), remove the opt-out. For genuinely hierarchical drill-down tables, keep.

### 3.8 Color hierarchy — meeting (light) port hardcodes

Per CLAUDE.md §3a, `_meeting.py` post-processes via `_BG_HEX_MAP` and `_TEXT_COLOR_MAP`. Lines that won't translate cleanly:

- Line ~2589: `★` star color `#facc15` (yellow on dark; legibility on white?).
- Lines ~3793–3873 (Risk Budget SVG bars): `#1e293b`, `#14241a`, `#1e4976`, `#0a0f18` backgrounds; `#f87171`, `#4ade80`, `#facc15` palette.
- Lines ~3216+ (MACRO PM-level drill rows): inline `background:#0a0f18`, `color:#94a3b8`, `color:#475569`.

**Fix:** pre-flight `grep -E 'fill="#0|background:#0|fill="#1' "data/morning-calls/$DATE*.html"` before each meeting export; add any new keys to `_BG_HEX_MAP`.

### 3.9 Cross-fund stop-utilization absent from Status

A PM landing on the page can't answer "of all funds with a stop, which is closest to breach?" without scrolling 4 different sections. Tooltips on row "Macro" mention "LF 54% consumido" but that's invisible until hover.

**Fix:** add "Util Stop" column next to "Util VaR" in Status consolidado. Per fund show worst-PM utilization. Would have surfaced LF=9bps directly at the top of the page.

---

## 4. In-session discoveries

These came up while fixing today's FX-bucket bug and aren't in the agent outputs:

### 4.1 `python` resolves to system Anaconda 3.9, not project venv 3.11

`where python` → `C:\Users\diego.fainberg\Anaconda3\python.exe` first; project's `.venv\Scripts\python.exe` only via explicit path.

Several modules use 3.10+ syntax (`type | None`, `dict[str, str | None]`). On Anaconda 3.9 these fail with `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'` at import.

**Fix:** add `from __future__ import annotations` to every file using 3.10+ syntax (already in `pa_renderers.py` and most modules — audit for the gap). Or guard: `if sys.version_info < (3, 10): sys.exit("Use the venv: .venv/Scripts/python.exe")` in entry-point scripts.

### 4.2 CLI entry-point convention is inconsistent

`risk_runtime.DATA_STR` reads `sys.argv[1]` greedily at module import. The same scripts then call `argparse` with `--date`. argparse sees `2026-04-29` as an unrecognized positional and errors.

Practical impact: only the bare-positional form works (`python script.py 2026-04-29`); the `--date 2026-04-29` form documented in CLI help fails.

**Fix:** centralize on argparse-style; have `risk_runtime` expose a function `parse_date_arg(parser)` that each script calls explicitly. Or document explicitly that positional is canonical.

### 4.3 `glpg_fetch.py:33-37` — hardcoded hostnames/usernames as fallback

Per the user's global rules ("Never hardcode credentials … usernames, hostnames"), these literals shouldn't be fallbacks. Right now they're tolerated. Either delete and require env vars, or at minimum add a comment that this is dev-only.

### 4.4 `risk_runtime.py:32` — variable name lies about its value

`DATE_60D = DATA - pd.Timedelta(days=90)  # ~60 business days buffer`

The constant says 60D but stores DATA−90 days. Reader has to compute 60×7/5=84≈90 to trust this.

**Fix:** rename to `DATE_90D` (or `DATE_60BD`), or expand comment.

### 4.5 `svg_renderers.py:55-60` — hidden alert color invariant

`dot_color = "#fb923c" if above_alert else bar_color` — `#fb923c` is the canonical alert color. A downstream change to the palette will silently break alert signaling. The meeting port's `_TEXT_COLOR_MAP` must mirror it.

**Fix:** add comment: `# fb923c = canonical alert color; mirrored em generate_risk_report_meeting._TEXT_COLOR_MAP`.

### 4.6 Repeated docstrings between the 4 PA-FX-split scripts

Despite today's dedup of `_remap_classe`/`_apply_remap`, the doctring block at the top of `generate_macro_pa_fx_split.py` (and 3 sisters) still repeats the same FX-split rationale. After today's centralization, the docstrings could just point to `pa_renderers.fx_split_classify` for the table.

**Fix:** trim each script's top docstring to: "{Fund} PA — FX-segregated view. See pa_renderers.fx_split_classify for the FX-bucketing rule."

---

## 5. Carryover from prior review (status)

From [`CODE_REVIEW_2026-05-01.md`](CODE_REVIEW_2026-05-01.md), aggregated status after Session 2 fixes:

| Area | Item | Status |
|------|------|--------|
| §2a Robustness | data_fetch.py:2040 empty df shape mismatch | Open |
| §2a | mirror path hardcoded → env var | Open (carried to §2.12 above) |
| §2a | data_fetch.py:1910 float(iloc[0]) | Open (carried to §2.11 above) |
| §2b Code quality | build_html mega-function | Open (HIGH — §1.5) |
| §2b | main() 540 lines | Open (HIGH — §1.6) |
| §2b | **PA-FX-split dedup** | **DONE 2026-05-01 session 2** ✅ |
| §2b | data_fetch.py:1417 nested iterrows | Open (carried to §2.9) |
| §2b | generate_risk_report iterrows | Open (carried to §2.10) |
| §2b | centralize date/NAV parsing | Open |
| §2b | unused imports | Open (carried to §3.1, AST-verified) |
| §2c Aesthetics | svg_renderers hex codes | Open (carried to §3.4) |
| §2c | fund_renderers raw f-string numerics | Open (carried to §3.4) |
| §2d Docs | All 4 items | DONE 2026-05-01 (commit 8c9d8b3) ✅ |

Net carryover: **9 open items** from prior review; **3 closed today**.

---

## 6. Recommended order of execution

### Day 1 — fix the bleeding (highest-impact, ~3 hours total)

1. **`risk_runtime.py:27` argv crash fix** (15 min) — §1.0. Unrelated tools (`pnl_server`) currently broken.
2. **`generate_risk_report.py:5113` `or 1.0` regression** (10 min) — §1.0b. Replace with `_require_nav`.
3. **`metrics.py:392` shorts dropped from PA outliers** (15 min) — §1.0c. Replace `>0` with `.abs() > 1e-9` at lines 373 + 392 + 394.
4. **`expo_renderers.py:1707, 1717` Dur double-MD** (20 min) — §1.0d. Recompute via `Σ|delta|/Σ(|delta|/MD)`. Verify against a known case (50/50 MD=2y/MD=10y bonds → expect 6y not 8.67y).
5. **`generate_monthly_review.py:372` IDKA BVaR sign fix** (10 min) — §1.0f. Add `.abs()`.
6. **`credit/credit_data.py:140-156` price_quality multi-book join** (45 min) — §1.0e. Group by PRODUCT in both CTEs OR join on (PRODUCT, BOOK). Verify count of flagged products matches manual sanity check on Sea Lion.
7. **Skill-refresh sprint** (30 min) — fix the 5 misleading SKILL.md + 8 `glpg-data-fetch` references — §2.1. `rg --replace 'glpg-data-fetch' 'glpg_fetch.py'` across `.claude/skills/`.

### Day 2 — UX (highest user-facing impact, ~3 hours)

8. **Briefing tightening** (45 min) — rule-based "tranquilo" gate (§1.1) + headline priority (§1.2). LLM substitution stays in CLAUDE.md §7 fila #2.
9. **D-1 fallback in Status table** (1 hour) — §1.3.
10. **Risk Budget thresholding by margem (bps)** (45 min) — §2.5. Add "Days to soft breach" projection.
11. **EVOLUTION BRLUSD legacy non-zero rows** (15 min) — §1.4. Send escalation email/Slack to PA engine owner with the verified evidence.

### Day 3 — code hygiene (~2 hours)

12. **Comment cleanup** (30 min) — delete the 13 vacuous/stale comments in §3.2 + fix the 4 misleading docstrings in §3.3.
13. **Unused imports cleanup** (15 min) — §3.1.
14. **PA-FX-split docstring slim-down** (10 min) per §4.6 — natural follow-up to today's dedup.
15. **Status table "soma" rename** (15 min) — §2.2.
16. **Mudanças Significativas D-1 null guard** (30 min) — §2.4.
17. **Top Posições "% da posição" rename** (15 min) — §2.7.
18. **Float-zero comparisons** (15 min) — §2.13g, 3 sites at once.

### Dedicated session (~half day each)

- **`build_html` extraction** (§1.5). Highest-LOC return; snapshot-diff-based safety net.
- **`main()` split** (§1.6).
- **Iterrows vectorization** (§2.9 + §2.10). Likely 5-10× speedup in inner loops.

---

## Appendix — Methodology

For each finding above:
- **WHERE**: file:line (verified to exist as of HEAD; or HTML offset confirmed by direct read)
- **WHAT**: the actual current behavior (quoted from code or rendered HTML)
- **WHY IT'S WRONG**: substantive critique tied to the user (the risk manager) or to a specific bug class
- **CONCRETE FIX**: a one- or two-line action, not "consider improving X"

Findings that did not survive verification are excluded:
- Agent B claimed an `<\b>` HTML typo at line 8683 — **discarded**, the actual line has correct `<b>14 bps</b>` markup.
- Other findings spot-verified via direct DB query (FX cutover, EVOLUTION non-zero rows), HTML re-read (briefing boilerplate count), or AST scan (unused imports).

The 3 review agents that contributed: A=correctness, B=content/UX, C=commentary. All 3 returned full audits. Agent C tried to invoke PowerShell — blocked per security policy; the audit findings themselves were sourced from Read+Grep and remain valid. Agent A's HIGH findings (§1.0 a–f) were spot-verified by re-reading the cited code; all 6 reproduced.

## Counts

- **6 NEW HIGH findings** (correctness): §1.0, 1.0b, 1.0c, 1.0d, 1.0e, 1.0f
- **5 HIGH findings carried/cross-listed**: briefing quality (§1.1), headline (§1.2), D-1 fallback (§1.3), EVO legacy non-zero (§1.4), build_html (§1.5), main() split (§1.6)
- **24 MEDIUM findings**: code (10 new + 3 carryover) + UX (8) + commentary (3)
- **20+ LOW findings**: code (6 new) + commentary (13 vacuous/stale + 4 lying docstrings) + aesthetics carryovers
- **9 carryover items** from prior review still open
- **3 prior-review items closed today** (PA-FX-split dedup + docs §2d)

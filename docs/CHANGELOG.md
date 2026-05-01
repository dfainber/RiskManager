# Risk Monitor — Changelog

Reverse-chronological log of features and fixes shipped, migrated out of CLAUDE.md
on 2026-05-01 (it had grown to ~56% of the doc and was crowding out the actionable
sections). For deep history use `git log`. CLAUDE.md links here for anything past
the latest session.

---

## 2026-05-01 (sessão 3) — Audit follow-ups + safe-refactor Phase 1+2 + housekeeping

Continuation of the audit-driven cleanup. All `STILL OPEN` items in
`REVIEW_2026-05-01.txt` and `docs/CODE_REVIEW_2026-05-01.md` closed
(5 fixed, 4 verified-no-action). Snapshot-diff validation on every commit
(`smoke_test.py --save-snapshot` + diff = 4629 risk_report + 8 vol_card
values byte-identical to baseline through all commits). Branch went from
3 to 10 commits ahead of `origin/master`.

### Code changes
- **`generate_credit_report.py` CSV-attribute escape (commit 32fa6de)** —
  6 sites of `csv.replace(chr(34), "&quot;")` replaced with
  `html.escape(csv, quote=True)`. The CSV encoding itself (`_csv_encode`)
  was already correct. The bug was the HTML-attribute escape on
  `data-csv="..."` only handled `"`; a bond name like "Smith & Co" would
  emit raw `&` into the attribute. `html.escape` is a strict superset
  (handles `&`, `<`, `>`, `"`, `'`) and decodes identically in browsers
  when the JS reads `data-csv`. Closes CODE_REVIEW #17.
- **`evolution_diversification_card.py` + `fund_renderers.py` fmt_br_num
  wrap (commit 825c02b)** — 10 unwrapped `:,.Nf` sites wrapped with
  `_fmt_br_num` (`_fmt_bps` helper, strat rows, soma totals, benefit line,
  per-strat percentile rows; `_bps` helper in fund_renderers). Bps values
  >1000 in tail scenarios were rendering en-US "1,234.5" instead of
  BR-locale "1.234,5".
- **Nested-f-string `SyntaxError` fix at `generate_risk_report.py:722`
  (commit 825c02b)** — pre-existing bug introduced same day in commit
  `f1563913`. Python 3.11 (project venv) does not support f-strings with
  same quote re-used inside (`f"{r["pos_brl"]:,.0f}"`). Extracted to
  local var `pos_brl_str = fmt_br_num(f"{r['pos_brl']:,.0f}")`. Would
  have crashed the next daily run. PEP 701 (Python 3.12+) lifts this
  restriction.
- **`RISK_MIRROR_PATH` env var (commit 825c02b)** — hardcoded
  `F:\Bloomberg\Risk_Manager\Data\Morningcall` now overridable via env
  var; empty disables the mirror; failures log WARN to stderr instead of
  silent try/except. Applied at `generate_risk_report.main` and
  `generate_risk_report_meeting.py`. Note: this change was folded into
  the parallel session's commit `437aedb` since both sessions had the
  files in their staged set.
- **`MARKET_FALLBACK_XLSM` env var (commit 2ea8deb)** — `_EXCEL_MARKET_PATH`
  in `data_fetch.py` (line 3017) now reads from env var with the legacy
  `F:\Macros\Gestao\Miguel Ishimura\Prices_Close_Global - Copy.xlsm`
  path as fallback. Closes CODE_REVIEW #14.
- **WARN on unknown PRODUCT_CLASS (commit 2ea8deb)** — both
  `fetch_fund_position_changes` and `_by_product` now compute the set
  difference between `df["PRODUCT_CLASS"]` and `_PRODCLASS_TO_FACTOR`
  keys before mapping; emit `WARNING [<func>:<short>]: dropped unknown
  PRODUCT_CLASS(es) [...]` to stderr when non-empty. Mapping behavior
  unchanged. Closes CODE_REVIEW #15. `sys` added to data_fetch imports.

### Audit verifications (no-code-change)
- **iloc[] guards review #4 — verified false positive 35/35**
  (commits 0d5eb8c + 2ea8deb): spot-checked all cited sites across
  `evolution_diversification_card.py` (5), `expo_renderers.py` (5),
  `fund_renderers.py` (9), `generate_credit_report.py` (4 in
  `compute_period_returns`), `data_fetch.py` (12). Every site is already
  guarded by `.empty` / `len() >= N` / ternary fallback. Several cited
  line numbers in `data_fetch.py` (2710, 2716, 2784, 2790, 2824, 2852,
  2858, 2893, 2943) point to SQL-content (`BETWEEN`/`INTERVAL`/`CASE`),
  not iloc statements. The auditor consistently missed surrounding
  guards. No `_first_or_default` helper warranted.
- **`pm_vol_card.py` review #6 — verified alive (commit 32fa6de)**: it
  is a standalone diagnostic (file docstring already says so), not
  imported anywhere (`grep` returns zero), runs via `run_vol_card.bat`,
  exercised by `smoke_test.check_vol_card`. Output: `data/morning-calls/
  pm_vol_card_<DATA>.html`. The auditor's claim that it is "imported by
  generate_risk_report.py via evolution_renderers" was incorrect.
- **`pnl_server.py` JSON error review #13 — verified false positive
  (commit 2ea8deb)**: `_json_error` uses `json.dumps({"error": msg})`
  which is safe by construction — `json.dumps` cannot produce invalid
  JSON regardless of input. Localhost-only server so info disclosure
  isn't a concern either.

### Housekeeping
- **CLAUDE.md surgery (commit 87c23b5)** — moved 135-line / ~37 KB
  changelog block from CLAUDE.md to `docs/CHANGELOG.md` (this file).
  CLAUDE.md down from ~360 lines to ~280, all actionable sections.
  §7 fila got pointer to CHANGELOG and CODE_REVIEW; dropped stale
  wire-it TODO for `evolution_diversification_card` (verified wired
  at `generate_risk_report.py:808`).
- **`docs/CODE_REVIEW_2026-05-01.md` created (commit 87c23b5)** —
  curated audit punch list with resolved-vs-open status; folded
  evidence inline as items closed.
- **Archive Status banners (commit 87c23b5)** — added explicit
  `Status: RESOLVED` / `ARCHIVED` headers to
  `docs/IDKA_VAR_EXPLORATION.md` and `docs/CREDITO_TREATMENT.md`.
- **`docs/REPORTS.html` deleted (commit 87c23b5)** — zero consumers in
  source code; `docs/REPORTS.md` is the human-readable source of truth.
- **`.gitignore` extension (commit a627a2a)** — added
  `data/morning-calls-meeting/`, `data/market-review/`,
  `data/monthly-reviews/`, `data/_pptx_inspection/`,
  `data/peers_data.json`, `smoketest_out.txt`,
  `.claude/scheduled_tasks.lock`, `Thumbs.db`. Following the same
  pattern as `data/morning-calls/` (output dirs ignored).
- **Untracked source committed (commit db07e96)** —
  `generate_market_review.py` (1,147 LOC), `generate_monthly_review.py`
  (1,783 LOC, was supposed to be committed 2026-04-26 per CLAUDE.md
  but was left in working tree), `month_bdays.py` (38 LOC helper that
  lists VAL_DATEs with data in LOTE_FUND_STRESS_RPM for a given month),
  `run_monthly_review.bat` (55 LOC), and
  `data/_credit_validation_xlsm_sealion_d2.csv` (6.6 KB validation gold
  std referenced in CLAUDE.md).

## 2026-05-01 (segunda sessão) — Audit-driven hardening

Driven by `REVIEW_2026-05-01.txt` Parts A (security) + D (aesthetics).
CLAUDE.md surgery (changelog → this file, audit follow-ups → CODE_REVIEW_2026-05-01.md)
landed in parallel.

- **`glpg_fetch.read_sql` cleanup widened** — outer `except psycopg2.Error` → `except Exception` (pandas/network errors também devem dropar a conn poisoned). Bare `except: pass` no `conn.close()` agora loga em stderr antes de seguir; `_tl.conn = None` + re-raise mantidos. Sec audit #1.
- **`_require_nav` helper** (`db_helpers.py`) — substitui o pattern `_latest_nav(...) or 1.0` em 4 sites de `data_fetch.py` (`fetch_macro_exposure`, `fetch_quant_var`, `fetch_evolution_var`, `fetch_macro_pm_book_var`). Raise quando NAV é None/0/negativo em vez de default-substitute (silent corruption ~10,000× nos bps). 5º site (`fetch_macro_pm_var_history`, loop por dia) usa pattern `if nav is None or nav <= 0: continue` — skip silencioso de dia ruim sem matar a série de 121d. Sec audit #2/#3.
- **Credit XSS hardening** (`generate_credit_report._h`) — novo helper `_h(s) = html.escape(str(s), quote=True)` aplicado em ~20 sites onde DB-sourced strings (`produto`, `product_class`, labels de donut/hbar/vbar, rating tags) entram em `<td>`/`<text>`/`<title>`. Bond names com `<>"&` não quebram mais layout nem permitem injeção em e-mail (lista BCC = 31). Sec audit #6.
- **Daily monitor brand strip + footer** (`daily_monitor.html` + `pnl_server.py`) — substituído `<span class="site-title">GLPG Monitor</span>` pelo SVG inline `rmGrad` + wordmark "Risk Monitor" do main report. Footer "Powered by Galápagos" servido via novo endpoint `/assets/galapagos.png` (decoda `credit/_galapagos_logo_b64.txt` no startup, serve como `image/png` com `Cache-Control: public, max-age=86400`). Aesthetics audit D #7.
- **VaR DoD + Top Movers modals — CSV button injection** (`vardod_renderers.py`, `pmovers_renderers.py`) — adicionada classe `modal` ao div raiz de cada modal (additivamente, ao lado de `vardod-modal`/`pmovers-modal`), `modal-head` ao head, `modal-title` ao title span. `injectCsvButtons` agora pega ambos modais via `section.card, .modal` selector. Aesthetics audit D #11/#12.
- **Mirror path → env var + WARN log** (`generate_risk_report.main`, `generate_risk_report_meeting.py`) — hardcoded `F:\Bloomberg\Risk_Manager\Data\Morningcall` agora vem de `RISK_MIRROR_PATH` (com fallback pro path antigo). Empty/unset desabilita o mirror inteiro. Erro silencioso virou stderr `WARNING: mirror save failed for {dir}: {e!r}`. Sec audit #9.

## 2026-05-01 — VaR Histórico hydration fix + Nazca wired (hidden) + hygiene + UX

- **VaR Histórico chart hydration fix** (`generate_risk_report.py:723`) — chart era append separado como `("MACRO", "risk-monitor", chart_html)`, criando 2º `<template>` com mesma `(fund, report)` key. Lazy-hydration `querySelector` matchava só o 1º, então o chart nunca chegava ao DOM. Fix: concatenar `chart_html` na entry parent risk-monitor via `sections[-1] = (_f, _r, _h + _chart_html)`.
- **Lazy hydration leak para summary** (`generate_risk_report._hydrateSection`) — `_idleHydrateAll` clonava todos os `<template>`s em background, mas `applyState`'s visibility loop só rodava em mode/sel changes. Sections recém-clonadas herdavam `display:block` default e apareciam no summary view. Fix: hide cada section por default antes de append, depois reveal só se mode/sel da tab ativa fizer match.
- **`SyntaxWarning '\s'`** (`generate_risk_report.py:4229-4230`) — JS regex dentro de Python f-string tinha `\b`, `\s`, `\.` literais. `\s` triggera warning; `\b` era runtime bug (Python convertia pra backspace antes do JS receber). Fix: doublar pra `\\b`, `\\s`, `\\.`. O regex de fund-name token agora realmente funciona.
- **NAZCA wired but hidden** (`risk_config.RF_BENCH_FUNDS` + `data_fetch.fetch_risk_history_rf_bench`) — novo dict `RF_BENCH_FUNDS` para fundos com BVaR computado de retornos ativos realizados (NAV_SHARE pct_change minus benchmark INDEX pct_change, rolling 252d × 1.645). Cobertura: Nazca (IMA-B benchmark, soft 2.0%/hard 3.0% provisional). Stress slot recebe abs VaR de `LOTE_FUND_STRESS` (informativo). Não em `FUND_ORDER` — re-enable adicionando `"NAZCA"`.
- **Purge QM/Macro_QM** (descontinued PM) — `data_fetch.py` (LIVRO IN clauses, BOOK regex, novo PA filter pra rows históricas), `db_helpers.PMS`, `expo_renderers.PM_ORDER_LIST`, `risk_config._PM_LIVRO`/`_PM_LIVRO_ORDER`/`_PA_LIVRO_RENAMES`/`_PA_ORDER_LIVRO`, `pm_vol_card.py` docstring.
- **Memory refs cleanup** — pruned 6 dangling `memory/project_*.md` refs em CLAUDE.md e 3 em `data_fetch.py`. Atualizado BALTRA look-through stale comment pra refletir migração 2026-04-28 pra LOTE_FUND_STRESS_RPM.
- **Sanity Check de Preços extended** (`generate_risk_report.py` Risk Monitor card append) — banner ✓ verde / ⚠ vermelho com tabela de produtos sem PRICE em D ou D-1, em ALBATROZ + BALTRA. Reusa `fetch_price_quality_flags` do `credit/credit_data.py`.
- **Top-issuer tile** (`credit_card_renderers.build_credit_section`) — agrega `pos_brl` por `grupo_economico` (fallback `apelido_emissor`) e adiciona 3º tile no header da Crédito section ao lado de Corp Credit + Carry médio. Aplicado em BALTRA + EVOLUTION.
- **Empty-state mode-aware** (`generate_risk_report.applyState` JS) — quando nada visível: `Nenhum dado disponível para o fundo X` (mode=fund) / `Nenhum fundo tem dados para o report Y` (mode=report) / fallback genérico. Lê label da `.tab.active` da sub-tabs.
- **Subtitle dd/mm/yyyy** (`generate_risk_report.py:4057`) — main report subtitle agora bate com convenção brasileira do credit report (`30/04/2026` vs `2026-04-30`).
- **Meeting (light) variant — color contrast remap** (`generate_risk_report_meeting._TEXT_COLOR_MAP`) — adicionados `#1a8fd1`, `#5aa3e8`, `#22d3ee`, `#a78bfa`, `#f472b6` → versões mais escuras pra leitura no big screen branco. Cobre SVG strokes (multi-line VaR Histórico chart, etc.); PNG sparklines têm cor baked-in e ficam de fora.
- **`run_month.bat` interactive** — prompt com Enter-accepts-default, no dia 1 do mês default = mês anterior. Mantém arg posicional pra Task Scheduler.
- **CLAUDE.md updates** — §6 ganhou tabela do Crédito sub-projeto + linha NAZCA hidden. §9 removeu Crédito de "Fora de escopo" (shipped 2026-04-30). §7 fila atualizada (item 1 MACRO↔QUANT removido — done).

## 2026-04-30 — Drill-down credit + Crédito tab no main report + perf

- **Drill-down Alocação refactor** (`generate_credit_report.render_alocacao_card`) — quatro modos de agrupamento via toggle pills: `Por Tipo` · `Por Grupo` (econômico, alfabético) · `Por Subordinação` · `Por Rating`. Parent rows com caret (▼/▶) + count `(n)`. Default-collapsed via param `default_collapsed=True` (children injetados com classe `alc-hidden` em vez de inline display:none — robusto contra interferência do sort). Botões `Expand all` / `Collapse all` operam só na pane ativa. Dragon default = `subordinacao` (FIDC-heavy); BALTRA/EVO Crédito section default = `grupo`.
- **Subordinação tranche detection** (`_tranche_bucket` + `_TRANCHE_PATTERNS`) — regex no PRODUCT name detecta Senior / Mezanino / Junior. FIDCs sem label mas com `subordinacao` numérico → Senior implícito. Tudo mais (Monocota / Classe Única, Debentures, CRIs, NTN-*) → bucket único `Sem Subordinação`. Coluna `Subord.` na tabela renderiza tag colorido (Senior=verde, Mez=amber, Jr=red, Sem-Sub="—" cinza). Donut "PL por Subordinação" condicional (renderiza só quando há signal não-N/A) com palette alinhado.
- **Soberano bucket** acima de AAA — `_effective_rating` mapeia `grupo_economico=='Tesouro Nacional'` ou `tipo_ativo IN (NTN-B, NTN-C, NTN-F, LFT, LTN)` → `Soberano`. `RATING_ORDER["Soberano"]=0` (sorta acima de AAA=1). Tag style navy fundo + white text + light-blue border. Pinned na 1ª slot do donut Rating pra sempre pegar a cor navy do palette.
- **"Cruz" issuer override** (`credit_data.normalize_issuer_overrides`) — qualquer produto com "Cruz" no nome → emissor/grupo/nome_emissor = "Santa Cruz". Aplicado em `fetch_positions_snapshot` + `data_fetch.fetch_fund_credit_positions` no read time pra não tocar no asset_master. Rolls up Santa Cruz tranches corretamente em concentração.
- **Crédito tab no main risk monitor** (`risk_config.REPORTS` + `credit_card_renderers.build_credit_section`) — nova aba `("credit", "Crédito")` posicionada acima de Peers. Renderiza pra BALTRA + EVOLUTION via look-through `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE`. Conteúdo: 4 header tiles (Crédito Look-through Total · Soberano · Corp Credit · Carry médio corp) + Distribuição (donuts em **2×2 grid**: Tipo, Setores, Rating, Subordinação — denom_nav=True, exclude_sovereign=True; Subordinação usa % crédito no label e % NAV no tooltip) + Alocação (drill-down com defaults `mode="grupo"`, `default_collapsed=True`, `mm_mode=True` → Posição em R$ MM com 2 decimals).
- **Filtros credit-relevantes** (`build_credit_section`) — drop LFT / NTN-C / NTN-F do BALTRA/EVO Crédito section (cash-equivalents / sovereign minor); NTN-B mantém (real-rate/inflation exposure). `Funds BR` filtrado pra só FIDCs (drop vanilla bond funds via tipo_ativo check). Normalização NaN→"" via `astype(object).where(notna, "")` em colunas string-bearing.
- **Performance — sort delegation** (`generate_risk_report.attachUniversalSort` rewrite) — substituído per-table walk + addEventListener × 200 tabelas × 10 ths (~2000 listeners) por **single document-level click+mouseover delegation**. `_initSortableTable` lazy-attacha indicators + dataset.usortColIdx só quando user hover/click pela 1ª vez na tabela. Estado de sort persistido via dataset (sortAttached, usortLast, usortAsc).
- **Performance — lazy hydration** (`build_html` template wrap + `_hydrateFund` JS) — todas as 84 seções `(fund, report)` renderizadas dentro de `<template class="tpl-section">` em vez de `<div class="section-wrap">`. Templates não vão pro DOM vivo até `_hydrateSection` clonar via `tpl.content.cloneNode(true)`. `applyState` chama `_hydrateFund(sel)` em fund mode e `_hydrateReport(sel)` em report mode antes de toggle visibility. **Idle pre-warm** via `requestIdleCallback` hidrata todas as seções no background depois do first paint, então tab switches subsequentes são instantâneos. Re-runs `initAlcSort`/`attachVrCaretToggle`/`injectCsvButtons`/`highlightFundNames` pós-hydrate.
- **`fetch_fund_credit_positions`** (`data_fetch.py`) — nova fetcher pulling credit-relevant rows via `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE = '<desk>'` joined to `credit.asset_master`. DISTINCT ON (PRODUCT, BOOK) deduplica primitives. WHERE filtra credit `PRODUCT_CLASS` set (Debenture, CRI, CRA, FIDC, FIDC NP, NTN-*, LFT, LTN, Funds BR, Nota Comercial). `COALESCE(am.tipo_ativo, e.PRODUCT_CLASS)` no SELECT pra fallback. Aplicado `normalize_issuer_overrides` no return.

## 2026-04-30 — Crédito sub-projeto + brand homogenização

- **NEW: Crédito standalone kit** (`generate_credit_report.py` + `credit/` package) — 6 fundos: Sea Lion, Iguana, Nazca, Pelican, Dragon, Barbacena. Default mode = **consolidado multi-tab** (`{date}_credito_consolidado.html`); single-fund via `--fund SEA_LION`. 8 cards por fundo: Header (9 KPI tiles) · Sanity Check de Preços · Distribuição (Tipo donut + Setores bar + Rating donut) · Concentração (Top emissores/grupos h-bars + limites breach table) · Alocação (full instrument table c/ Carry Anual + sortable c/ rating-quality rank) · Mercado de Crédito (índices BR + curvas ANBIMA AAA/AA/A) · AUM Histórica · Retorno (heatmap mensal merge c/ Total). Posições = `LOTE45.LOTE_BOOK_OVERVIEW` (NÃO `LOTE_PRODUCT_EXPO` — Sea Lion ausente lá). NAV match dentro de R$ 3 vs `LOTE_TRADING_DESKS_NAV_SHARE`. Validation gold std: `data/_credit_validation_xlsm_sealion_d2.csv` (35/35 instrumentos do xlsm green tab presentes no DB).
- **Schema Credit — DDL e ingest**: 2 tabelas novas no schema `credit` (que já existia c/ `MAPS_DEBENTURES`/`PRICES_DEBENTURES`). `credit.asset_master` (533 rows, PK=`nome_lote45`, vem de `Cadastro Ativos` no xlsm) + `credit.issuer_limits` (937 rows long-format, PK=`(emissor,fund_name)`, vem de `Limites_GrupoEconomico`). Ingest XML-direto do .xlsm (260 MB) via `python -m credit.credit_db_helpers all`. CSV/PNG branding extraído do main risk monitor pra reuso em `credit/_galapagos_logo_b64.txt`.
- **Sanity Check de Preços** (`credit_data.fetch_price_quality_flags` + banner card) — pra todo fundo do credit kit, valida que cada security/derivative tenha PRICE não-nulo em D e D-1. Banner verde se OK, vermelho c/ tabela de detalhes se ⚠. Exempções: PRODUCT_CLASS ∈ `{Funds BR, Cash, Margin, Provisions and Costs}`. **Regra de processo**: ao adicionar novo fundo ao kit, REGISTRAR no check (memory `project_rule_price_quality_check_coverage.md`). Cobertura atual: só 6 fundos de crédito; gap = Frontier + MM/RF/Prev/ETF families.
- **Brand homogenização — Risk Monitor + Crédito** (`generate_risk_report.py` + `generate_risk_report_meeting.py` + `generate_credit_report.py`) — substituído o logo PNG legacy do main risk monitor por **chart-icon SVG inline** (gradient azul + linha clara crescente, blue-to-navy `<linearGradient id="rmGrad">`) + wordmark "Risk Monitor" (Gadugi 22px no dark, navy 24px no light). Adicionado **footer "Powered by Galápagos"** em todos os reports (right-aligned), reaproveitando o PNG Galápagos legacy pra wave logo. Meeting (light) version recolore o footer PNG via `filter: hue-rotate(...)`. Credit consolidated adopt the mesmo brand strip + footer pra parity visual.

## 2026-04-29 — UX polish + bug hunting

### Bug fixes
- **Daily P&L bps mismatch (RJ +8bp vs fund +25bp)** — bug **DUPLO**:
  1. **Backend** (`fetch_book_pnl`): somava `PL_PCT` (per-position PL/AMOUNT, não somável). Agora calcula tudo como `PL / fund_NAV` em todos os níveis. Adicionado campo `nav` no payload pra debug. Memory: `project_rule_book_pnl_per_position_pct.md`.
  2. **Frontend** (2 cópias com mesmo bug): group header (RJ/JD/CI/LF) calculava `Σ pl_pct / books.length` — média em vez de soma. Fixado em `daily_monitor.html:402` e `generate_risk_report.py:3938`.
- **Daily P&L sempre colapsado ao abrir**: `bpnlLoadState` agora descarta `expanded` no load + `bpnlSaveState` só persiste `order` (drag-drop de books). `bgroups` default mudou de `true` → `false` em ambas cópias. **Restart manual do `pnl_server.py` necessário** após editar `data_fetch.py` — Python não recarrega módulos importados.
- **BALTRA Exposure RF look-through (gap 5.2 → 6.98 yrs vs Prev.xlsx 6.92)** — descoberta crítica: `fetch_albatroz_exposure` filtrava `TRADING_DESK = '{desk}' AND TRADING_DESK_SHARE_SOURCE = '{desk}'`. Removido o `TRADING_DESK = ...` filter. SHARE_SOURCE sozinho captura look-through nativo (BALTRA agora pega IDKA 10Y -1.28y + IDKA 3Y -0.44y + Albatroz cota -0.03y). ALBATROZ não regrediu (não é cotista). Memory: `project_rule_share_source_lookthrough.md`.

### Features
- **Vol Regime Summary — MACRO_Q + ALBATROZ** (`summary_renderers._FUND_PORTFOLIO_KEY`) — ampliado de 3 → 5 keys. ALBATROZ usa série HS gross própria; MACRO_Q usa SIST_GLOBAL (sub-book Global do QUANT) como proxy (MACRO_Q não tem HS própria upstream).
- **Briefing — bench line ampliada** — IBOV/CDI/USDBRL/DI1F (3y constante, ~Jan/29). Fetchers novos `fetch_usdbrl_returns` (sign-flip BRL convention: USD/BRL ↑ = vermelho, "fortalecimento BRL" = positivo verde) + `fetch_di1_3y_rate` (target 756 BDAYS via `_di` pattern, rola com a data, mostra rate level + bps change).
- **Breakdown por Fator — IGPM + outros** (`summary_renderers._FACTOR_LIST` + `generate_risk_report.py` factor_matrix loop) — ampliado de 3 → 9 fatores: Juros Reais (IPCA), **Juros Reais (IGPM)** (novo), Juros Nominais, IPCA Idx, **IGPM Idx** (novo), Equity BR/DM/EM, FX, Commodities. `_DV01_SIGN_FLIP` ganha `real_igpm: True, igpm_idx: False`. BALTRA aparece em IGPM (-1.02 yrs).
- **Mudanças Significativas + Peers — pp → %** (`summary_renderers._delta_pp` + card-sub + `generate_risk_report.py` 3 sites JS) — substituído sufixo `pp` por `%` em todas as labels do report.
- **VaR Histórico inline MACRO** (novo card, section-id `risk-monitor`) — chart SVG line ~820×320 px abaixo do Risk Monitor card, mostrando Fund total + 4 PMs (CI/LF/RJ/JD) últimos 121d úteis. Fetcher novo `data_fetch.fetch_macro_pm_var_history` (`LOTE_FUND_STRESS_RPM` LEVEL=10 TREE='Main_Macro_Ativos', |Σ signed VaR per PM prefix| × 10000 / NAV). Helper genérico `svg_renderers.multi_line_chart_svg` (multi-série + gridlines + legenda inline). Paleta brand-aligned (gold + 4 azuis/cyan/roxo). Unidade bps. QM excluído (descontinuado). **Section id == "risk-monitor"** (mesma aba do card pai); IDs novos não em `risk_config.REPORTS` ficam escondidos pelo filtro de aba.
- **EVOLUTION PA — default Por Livro** (`pa_renderers.build_pa_section_hier`) — em vez de Por Classe. Strategy/Livro/Produto é mais informativo pra fundo multi-estratégia. Demais fundos mantêm Por Classe.
- **Exposure VaR — delta D-vs-D-1** (`expo_renderers._build_expo_unified_table` + helper novo `_prev_hs_var_bps` em `generate_risk_report.py`) — novas células Δ tanto em "Total não-diversificado" (acumula `tot_var_abs_d1` no loop de fatores via `d1_var_pct`) quanto "Diversificado HS portfolio" (D-1 via `series_map.iloc[-2]`). Param novo `diversified_var_bps_d1` em `_build_expo_unified_table`, propagado via `build_exposure_section`/`build_quant_exposure_section`/`build_evolution_exposure_section`.

## 2026-04-28 — multi-session: FX-split + UX defaults + VaR DoD + BALTRA RPM + IGPM + Top Movers

### Sub-sessão 7 (cleanups)
- **Frontier highlights banner — IBOV fallback + duplicate fix** (`fund_renderers._highlights_div`) — Top 3 banner duplicado removido (criado em `390798c`, era redundante com o "Highlights · α vs <bench> hoje" pré-existente, commit `0a4ae44`). Threshold relaxado de `|val| × 10000 > 0.5 bps` pra `|val| > 0`. Quando coluna bench-relativa é toda zero (caso IBOV upstream sem dado), fallback pra `TOTAL_ATRIBUTION_DAY` (absoluto) com label "(α vs IBOV sem dado upstream)".
- **BALTRA Exposure RF — drop mod_dur≈0 noise** (`expo_renderers.build_albatroz_exposure`) — após filtro CDI, adicionado `df = df[df["mod_dur"] > 0.01]` pra remover Equity / IBOVSPFuture / USDBRLFuture / FIDCs (`Funds BR`) / Corn Futures que não têm rate sensitivity. Outros mantém só CRIs (mod_dur 2-4y, parking lot). ALBATROZ unchanged (sem ruído mod_dur=0 no escopo). Closes session_2026_04_28 TODO #2.
- **Distribuição IDKA — reset Forward ao trocar bench** (`generate_risk_report.setDistBench` JS) — ao trocar entre Benchmark/Replication/Comparação tabs, force `card.dataset.activeMode='forward'` + atualiza visual dos `.dist-btn[data-mode]`. Evita landing em backward+empty quando bench-tab nova não tem realized 252d. Closes session_2026_04_28 TODO #3.
- **BALTRA/ALBATROZ Exposure RF — Net (yrs)** (`expo_renderers.yr_cell`) — novo helper exibe `delta_brl/nav` como `±X.XXyr`. Aplicado em ambas tabelas (Indexador + Top 15) pra consistência parent/child. Headers `Net (%NAV)` → `Net (yrs)`. Possível porque `delta_brl` agora é POSITION × MOD_DURATION (rate primitive) — `delta_brl/nav` dá duração em yrs direto. Closes session_2026_04_28 TODO #1.

### Sub-sessão 6 (VaR DoD into Comments + Top Movers Produto)
- **VaR DoD attribution wired into Comments** (`generate_risk_report._dod_top_driver` + `summary_renderers.build_comments_card`) — replace hand-rolled `_top1_var_delta` (cobertura só MACRO/QUANT/EVO) por unified pull de `fetch_var_dod_decomposition`. Cobertura agora full-suite 9 fundos. Bullet 1-line inclui: `metric_lbl ΔX bps · driver: <leaf> (Δy bps) · [pos +A / marg +B] · ⚠ override`. Threshold default 5 bps; IDKAs 2 bps. Decomp pos/marg só renderiza quando fundo publica per-row pos data. Override flag só dispara quando `|ratio + 1| > 0.05` (correção material). Performance: prefetch único de 9 DoD dataframes compartilhado com modal payload (build_vardod_data_payload aceita `prefetched_dfs` kwarg).
- **VaR DoD driver — exclude bench primitive** (`_dod_top_driver`) — IDKA passivo (`PRIMITIVE='IDKA IPCA 3Y'`/`'IDKA IPCA 10Y'`) é mecanicamente o top |Δ| mas comunicativamente errado (não é decisão do gestor — é tracking 100% NAV com override). Filter: `df[df["label"] != bench_primitive]` antes de selecionar driver. Fallback pro bench se for o único mover. Resolved via `_VAR_DOD_DISPATCH[fund_key][3]` (cfg 4º campo).
- **Top Movers Produto modal** (`pmovers_renderers.py` novo + trigger no PA card head + injection em `build_html`) — popup acionado por botão "Top Movers Produto →" no header de cada PA card. Modal 4 colunas (DIA / MTD / YTD / 12M), cada coluna com 5 PIORES (vermelho) + 5 MELHORES (verde). Source: `df_pa` (REPORT_ALPHA_ATRIBUTION dia/mtd/ytd/m12 _bps). Tag compacto por CLASSE (`[RV BZ]`, `[BRLUSD]`, `[FX Carry]`, `[RF IPCA]`, `[ETF Opt]`). ESC/backdrop fecha. Cobertura: 9 fundos (FRONTIER inclusive via GFA key).
- **Pmovers — consolidação de futuros + filtros** (`pmovers_renderers._consolidate_product` + `_FX_HEDGE_LIVROS`) — futuros consolidados por ativo subjacente (regex Brazilian fut pattern: `<prefix>[FGHJKMNQUVXZ]<2dig>`). Exemplos: `WDOK26 + WDOG26 + WDOM26 → WDO*`; `DI1F33 + DI1F28 → DI1*`; `DAPK35 → DAP*`. Non-futures unchanged (ETFs, options, equities). Filter adicional: drop `PRODUCT='Cash USD'` + LIVROs `{Caixa USD, Cash USD, Caixa USD Futures}` (FX hedge collateral, não alpha). Custos/Caixa/Provisions já filtrados.
- **Mirror save F:\\Bloomberg\\Risk_Manager\\Data\\Morningcall** (`generate_risk_report.main`) — após salvar HTML em `data/morning-calls/`, escreve segunda cópia em `F:\\Bloomberg\\Risk_Manager\\Data\\Morningcall\\` (shared distribution location). `mkdir(parents=True, exist_ok=True)`. Falha no mirror loga warning mas não derruba o save principal.

### Sub-sessão 5 (BALTRA RPM migration + IGPM)
- **BALTRA migration → LOTE_FUND_STRESS_RPM** (`data_fetch._VAR_DOD_DISPATCH["BALTRA"]: lote_fund → rpm_book`) — RPM populado upstream desde 2026-04-07 (LEVEL=10 com look-through nativo). Modal DoD agora compacto (13 BOOK rows, antes 87+ produtos com synthetic ↻ parents). Sem necessidade de cross-join LOTE_PRODUCT_EXPO. Workaround antigo (`_regroup_lookthrough`) ainda existe pra ALBATROZ/MACRO_Q (que continuam em LOTE_FUND_STRESS).
- **BALTRA TREE='Main' filter** (`data_fetch.fetch_risk_history_raw` + `_var_dod_lote_fund`) — BALTRA era único RAW_FUND com 3 TREEs em LOTE_FUND_STRESS (Main / Main_Macro_Gestores / Main_Macro_Ativos), todas com mesmo total — `SUM(PVAR1DAY)` triplicava. Filtro `TREE='Main'` reduz VaR card BALTRA pra valor real (~3× menor). Outros RAW_FUNDS só têm Main → no-op.
- **IGPM exposure — Fase 1 (silent-drop fix)** (`data_fetch.fetch_rf_exposure_map`) — `_RATE_PRIM_BY_CLASS["NTN-C"/"DAC Future"]: "IPCA Coupon" → "IGPM Coupon"`. `keep_mask` aceita `PRIMITIVE_CLASS in ("IPCA","IGPM")`. Override factor: `PRIMITIVE_CLASS='IGPM' → factor='igpm_idx'`. `rate_prims` (sign-flip set) inclui `'IGPM Coupon'`. Antes: NTN-Cs eram silenciosamente dropados do Exposure Map (mismatch entre _RATE_PRIM mapping e PRIMITIVE_CLASS upstream).
- **IGPM — Fase 2 (rendering)** (`risk_config._RF_FACTOR_MAP`, `expo_renderers.{build_idka_exposure_section, build_rf_exposure_map_section}`) — `_RF_FACTOR_MAP["NTN-C"/"DAC Future"] = "real_igpm"` (separado de "real" IPCA). `FACTOR_ORDER`/`FACTOR_LABEL`/`_DUR_FAC`/`_DUR_FACTORS` ganham `real_igpm` + `igpm_idx`. Pivot RF map inclui `real_igpm`. Stat row tem chip "Juros Reais (IGPM)" condicional (só quando |val| > 0.005yr). Position table mostra factor=real_igpm/igpm_idx.
- **`fetch_albatroz_exposure` rewrite** — bug pré-existente de `SUM(DELTA)` sobre todos primitives (spread + face + coupon, sinais inconsistentes → garbage). Fix: WHERE filtra UM rate primitive por PRODUCT_CLASS (NTN-B/DAP→`IPCA Coupon`, NTN-C/DAC→`IGPM Coupon`, DI/NTN-F/LTN→`BRL Rate Curve`). DV01: `DELTA × 0.0001` (era `× MOD_DUR × 0.0001` → squared duration, inflava ~10×). Filtro `MOD_DURATION IS NOT NULL` exclui face values. CRIs/Debentures parked (somam todos primitives — sinal misto).
- **Cobertura IGPM no kit**: BALTRA (NTNC 01/01/2031 book Prev, 51.6M ano_eq, ~1.02yr) + EVOLUTION (via Evo Strategy CI_Macro look-through, 30.7M ano_eq, ~0.11yr). PA `REPORT_ALPHA_ATRIBUTION` já tinha CLASSE='RF BZ IGP-M' separada — PA cards renderizam automaticamente. Para 252d HS / vol regime / replication: tratar IGPM como IPCA proxy (sem vertices upstream).
- **Lista de distribuição daily** (`scripts/send_risk_monitor_email.ps1`) — BCC expandido de 9 → 31 destinatários (lista completa do time).

### Sub-sessão 4 (VaR DoD polish + FRONTIER)
- **VaR DoD modal — relabel "Vol eff" → "Marg eff" + footnote rewrite** (`vardod_renderers.py`) — coluna passou a se chamar "Marg eff", headline mostra "marginal (Δg)" em vez de "vol/marginal", e footnote esclarece que `g = contrib/pos` é a contribuição marginal de VaR por BRL de exposição (absorve mudanças de vol E correlação — engine não isola σ). Dispara dúvida recorrente: "vol_effect" é equívoco; o que medimos é mudança de risco-por-BRL holding pos constante.
- **VaR DoD — FRONTIER coverage** (`data_fetch._var_dod_frontier` + `_VAR_DOD_DISPATCH["FRONTIER"]` source `frontier_hs` + trigger em `expo_renderers.build_frontier_exposure_section`) — fecha cobertura full-suite (9 fundos). Decomposição via component-VaR no q05 worst-day scenario: `component_i = (w_i − w_ibov_i) × r_i_at_q05`, soma exata = -BVaR_pct. Fallback: quando `frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD` upstream não tem D-1 (hoje só tem 1 data populada), reusa pesos de hoje em D-1 — captura só shift de cenário, sem efeito de posição. Caveat surfaced via novo campo `df.attrs["modal_note"]` → payload `modal_note` → warning bar com prefix "ℹ".
- **Modal warning bar — dual-channel** (`vardod_renderers.VARDOD_JS`) — agora suporta `modal_note` (caveat por fundo, prefixo ℹ) E row-level overrides (engine artifact IDKA, prefixo ⚠). Multi-line via `<br>`. Refactor pra desacoplar "info do modal" de "row destacada amarela" — antes setar override em row pra surfaceá uma mensagem genérica pintava o row de amarelo erroneamente.

### Sub-sessão 3 (VaR DoD attribution + IDKA HS replication)
- **IDKA HS replication — engine-style + strict NTN-B coupon TR** (`data_fetch._compute_idka_replication_returns` + cache `data/idka_replication_cache.json`) — substitui o approach asset-based ("hold same NTN-Bs, replay history") por engine-style: **at each historical date `t`, solve a constant-DV-target NTN-B-only portfolio at `t_prev` and earn its 1d total return on `t`**. Replication independente da posição do fundo. Cache JSON imutável (243 dates × 2 targets ≈ 19KB), backfill ~10s on cold start, ~0.3s daily marginal. Plus: strict coupon adjustment via `_get_vna_ntnb` — `r_TR = r_clean + (semi_coupon × VNA(cup)) / P_prev` (era `(1+r)(1+c)−1` que tinha bias ~9 bps em coupon days). VNA puxado de `ECO_INDEX.VNA_NTNB`. Spread mean centrado em ~0 agora (antes -0.14 bps).
- **VaR DoD attribution — Fase 1 (data layer)** (`data_fetch.fetch_var_dod_decomposition`) — função pública que devolve decomposição D-vs-D-1 por leaf factor. Schema 16 colunas (label, group, contrib_d1/d/delta, pos_d1/d/d_pos_pct, vol_d1/d/d_vol, pos_effect/vol_effect, sign, override_note, children). 8 fundos: IDKAs (LOTE_PARAMETRIC_VAR_TABLE per PRIMITIVE) · MACRO/QUANT/EVO (LOTE_FUND_STRESS_RPM LEVEL=10 per BOOK) · ALBATROZ/MACRO_Q/BALTRA (LOTE_FUND_STRESS per PRODUCT). Decomposição "today's pos constant": `pos_effect = (pos_d − pos_d1) × g_d1`, `vol_effect = pos_d × (g_d − g_d1)`, exact sum.
- **VaR DoD attribution — Fase 2 (modal)** (`vardod_renderers.py` novo + 5 triggers em `expo_renderers.py` + injection em `generate_risk_report.build_html`) — popup modal acionado por botão azul outlined "VaR DoD →" no header de cada exposure section. Modal compacto ~820px com tabela ordenada por |Δ| desc, sortável por header. Headline: ΔVaR + breakdown pos/vol. Filter zero-rows (`|contrib_d1| < 0.05 AND |contrib_d| < 0.05`). Linhas com override em destaque amarelo. ESC/backdrop fecha. JSON payload embedded `window.__VAR_DOD_DATA` (~190KB).
- **IDKA bench primitive override — unconditional** (`data_fetch._var_dod_idka`) — força DELTA do passivo (`PRIMITIVE='IDKA IPCA 3Y'`/`'IDKA IPCA 10Y'`) para `-NAV` sempre, escala `contrib`/`vol` proporcionalmente. Resolve bug intermitente do engine que oscila ratio bench/NAV entre -1.00 e -0.62/-0.71 nos 2 IDKAs simultaneamente, gerando ΔBVaR artificial. Audit trail em `override_note` quando correção é material (`|ratio + 1| > 0.05`).
- **Albatroz look-through inline nos modais IDKA** (`data_fetch._explode_albatroz_for_idka` + JS expandable rows) — linha "GALAPAGOS ALBATROZ FIRF LP" na tabela IDKA agora tem caret ▶ que expande pras 4 posições internas do Albatroz (DI1F33, DI1F28, DAPK35, NTNB 15/08/2050) reescaladas pra bps no NAV do IDKA. Position via `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE='IDKA IPCA Xy FIRF'`. VaR via `LOTE_FUND_STRESS` Albatroz × scale = parent_idka / albatroz_total. Soma dos children = parent (exato).
- **BALTRA look-through synthetic parents** (`data_fetch._regroup_lookthrough` + `_fetch_lookthrough_source_funds`) — modal BALTRA agrupa posições look-through sob 3 parent rows: ↻ IDKA 10Y holdings (10 children) · ↻ IDKA 3Y holdings (9) · ↻ Albatroz holdings (22). Direct rows (Prev book, CRIs) ficam standalone. Workaround usando cross-join LOTE_FUND_STRESS × LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE. **Resolvido 2026-04-28** com migração BALTRA → LOTE_FUND_STRESS_RPM (LEVEL=10 nativo) — workaround só ativa pra ALBATROZ/MACRO_Q.
- **Fund switcher scroll-to-top** (`generate_risk_report.selectFund` JS) — clicar em outro fundo no nav agora scrollTo(0) instant. Antes ficava na posição vertical anterior.

### Sub-sessão 2 (Risk Budget + Task Scheduler)
- **Risk Budget — nova regra de carry** (`fund_renderers.carry_step`) — pnl positivo: `next = 63 + 0.5 × pnl` SEMPRE (substitui o reset-para-63 + bônus de crossover YTD); pnl negativo: 3 camadas de penalty (extra `B_t − 63` = 25% · base 63 = 50% · excedente acima de B_t = 100%) + cap em `min(B_t, 63)`. Carry extra não consumido evapora ("use it or lose it").
- **Override LF Apr/26 = 20 bps** (`data/mandatos/risk_budget_overrides.json`) — segundo override ativo (RJ Apr = 63 já existia).
- **STOP → ⚪ FLAT downgrade** (`fund_renderers.build_stop_section` + plumbing em `generate_risk_report.build_html`) — quando PM em STOP territory mas sem exposição viva (`Σ|delta| < 0.05% NAV` em `df_expo`), status passa a FLAT cinza. Não aplica a CI.
- **Stop history modal — drill-down BOOK-level** (`_build_stop_history_modal` + `data_fetch.fetch_pm_book_pnl_history` + handler JS `toggleStopHistRow`) — cada linha-mês ganha caret `▶` clicável; expande mostrando breakdown por BOOK do PnL daquele mês, ordenado por |PnL| desc.
- **MACRO Exposure PM VaR — caret toggle** (`expo_renderers.toggleDrillPM`) — drill de PM (CI/LF/JD/RJ/QM) em PM VaR mode flipa `▶ ↔ ▼` corretamente (antes ficava sempre `▶`).
- **Vol Regime — default expanded** (`fund_renderers.build_vol_regime_section`) — caret das linhas-fund agora `▼` por default, books/PMs visíveis.
- **Paleta de cores — bump `--muted`** (`generate_risk_report.py:1696`) — `#8892a0 → #a8b3c2` (contraste 5.8:1 → 7.8:1, AAA p/ texto pequeno). Adicionados `--muted-strong: #c9d1dd` e `--muted-soft: #6b7480`. Aplicado em `.card-sub` / `.bar-legend` / `.comment-empty` / `.brief-footnote` (com `font-weight:500` em parágrafos pequenos). `@media print` remapeado também (`--muted-strong: #333`, `--muted-soft: #888`).
- **Task Scheduler — daily report automation** (`run_report_auto.bat` + scheduled task `Risk Monitor - Daily Report`) — bat não-interativo com weekend guard via PowerShell `(Get-Date).DayOfWeek`, data via `latest_bday.py` (ANBIMA-aware), sem UI/browser, log em `logs/auto_report.log`. Task registrada Mon-Fri 08:00 logged-on-only via `schtasks /Create`. `logs/` adicionado ao `.gitignore`.

### Sub-sessão 1 (FX-split + UX defaults)
- **EVOLUTION/QUANT/MACRO_Q PA FX-split** (`generate_evolution_pa_fx_split.py`, `generate_quant_pa_fx_split.py`, `generate_macroq_pa_fx_split.py`) — replicam a lógica do MACRO FX-split, ajustando hierarquia por fundo: EVO usa STRATEGY → CLASSE_NEW (FX-split) → GRUPO_NEW → LIVRO → PRODUCT; QUANT/MACRO_Q usam LIVRO → CLASSE_NEW → GRUPO_NEW → PRODUCT. Toolbar Expandir/Colapsar/Reset + sort por header em todos. **MACRO_Q tem toggle extra "FX Detalhado / FX Consolidado"**: a view consolidada lifta todo BRLUSD/FX para uma única linha top-level "≡ FX Basis Risk & Carry" acima de Caixa/Custos. Total preservado em todas as views (verificação numérica embutida).
- **Top Movers — 3º toggle "Por Classe (sem FX)"** (`summary_renderers._movers_rows`) — filtra `CLASSE` em {BRLUSD, FX} antes de agregar; aparece no card consolidado da Summary e nos cards per-fund.
- **Distribuição default expanded + Backward gray-out em 21d** (`fund_renderers._build_*_table` + `generate_risk_report._applyDistVisibility`) — caret das linhas-fund agora `▼` por default, child rows visíveis (sem `display:none`). Quando window=21, botão Backward ganha classe `dist-btn-disabled` (cinza, cursor not-allowed) e clique é bloqueado em `setDistMode` (combo backward+21 não tem overlay realizado).
- **Exposure Map RF Y-axis snap to ±0.5y** (`expo_renderers.build_rf_exposure_map_section`) — quando max abs de all_vals (fund+bench+rel+cumulative) < 0.5 yr, força `y_max=0.5, y_min=-0.5` ao invés do auto-scale com floor 1.0. Resolve o caso EVOLUTION CDI-bench com posição tiny (-0.04yr Total) que renderizava com escala vazia +1.1y.
- **MACRO_Q peers fix** (`risk_config._FUND_PEERS_GROUP`) — `MACRO_Q: "EVOLUTION"` → `"GLOBAL"`. O peer group GLOBAL existe nos dados com fund_name="GALAPAGOS GLOBAL" — antes, o focal dot da scatter no tab MACRO_Q ficava destacado em "GLPG EVOLUTION" porque `g.fund_name` apontava pro grupo EVOLUTION.
- **ALBATROZ/BALTRA Exposure RF — exclui CDI** (`expo_renderers.build_albatroz_exposure`) — adicionou filtro `df = df[df["indexador"] != "CDI"]` no início da função. LFTs (mapeados pra "CDI" via `cls_to_idx`) inflavam Net %NAV / Gross / Total cosmeticamente sem representar risco real (duration ≈ 0). Função generalizada com `fund_label` parameter.
- **BALTRA Exposure RF + Exposure Map RF** — wireup completo. Generalizei `data_fetch.fetch_albatroz_exposure(date_str, desk=...)` para aceitar TRADING_DESK (default ALBATROZ). Em `generate_risk_report`: adicionado `fut_baltra` future + `df_baltra_expo, baltra_nav` no `ReportData`, BALTRA entry em `_RF_MAP_CFG` (bench_dur=0, label="IPCA+"), e seção "BALTRA · exposure" via `build_albatroz_exposure(..., fund_label="BALTRA")`.
- **IDKA 3Y/10Y default = Comparação · 1d · Forward** (`fund_renderers.build_distribution_card`) — quando `has_cmp=True`, bench_toggle marca "Comparação" como ativo e a `<div data-bench-section="comparison">` fica visível por default; benchmark/replication começam ocultas. As 4 _view internas da seção Comparação agora têm `default_active=True` (matching forward+1).
- **`setDistBench` chama `_applyDistVisibility`** (`generate_risk_report.py` JS) — fix de bug: trocar bench tab mostrava o container mas as 4 views internas (bw1/fw1/bw21/fw21) continuavam com display:none, deixando a tabela vazia. Agora após toggle de seção, reaplica visibility de mode/window.
- **IDKA exposure factor breakdown collapsed by default** (`expo_renderers.build_idka_exposure_section`) — caret `▼` → `▶` e `idka-pos-row` ganhou `display:none` inline. Os 4 fatores (Real/Nominal/IPCA Index/RF) começam fechados, click no header expande via `toggleIdkaFac`. Aplica a IDKA 3Y e 10Y.
- **Catálogo de tabelas/defaults** (`docs/REPORT_TABLES_DEFAULTS.txt` + `.md`) — documentação completa de todos os cards do relatório por (mode, fund), com handlers JS de drill/toggle e estado default atual de cada um. Útil pra discutir mudanças sistemáticas de UX.

## 2026-04-27 — Exposure Total + Peers redesign + Distribuição 21d + NTN-B coupon fix + MACRO PA FX-split

- **Exposure Total — 3 métricas** (`_build_expo_unified_table` em `expo_renderers.py`): linha "Total não-diversificado" (Σ |VaR fator|, cinza) + linha "Diversificado HS portfolio" (de `series_map[td]`) + benefício em bps. Aplicado MACRO/QUANT/EVOLUTION via novo helper `_latest_hs_var_bps(short)` em `generate_risk_report.build_html`.
- **Per-fund Peers redesign** — substituiu tabela por 4 strips finos azuis (linha + diamante temperatura red→green) + 2 scatters (MTD/YTD Vol vs Retorno) em grid 2×2 alinhado por janela. Tooltip custom (`<div>` flutuante, `mouseover` delegado em `document`) com nome/retorno/vol em hover. Toggle Gráficos/Tabela por card.
- **PA Reset Sort** — botão `↺ Reset` no toolbar de cada PA card; `window.resetPaSort` restaura ordem default DFS via JSON `byParent`, reseta `userSorted=0` + sortIdx=2 (YTD desc).
- **Markets** — tab renomeado de "Market" → "Markets".
- **Distribuição 1d/21d** — toggle troca universo de retornos: 1d = retornos diários (252 obs); 21d = somas rolantes 21d (≈232 obs). Helper `_to_rolling_sum(w, window)` via cumsum vetorizado. Quando 21d ativo, botão azul "5 piores · 5 melhores" abre modal.
- **Modal Top 21d Janelas** — `compute_top_windows(w, k=5, window_days=21)` em `metrics.py`: greedy não-overlap (sort por soma asc/desc, pula janelas sobrepostas). Modal único com toggle de 3 seções pra IDKA (vs Benchmark / vs Replication / Repl − Bench), 5 PIORES e 5 MELHORES empilhadas. JS `setDistTopSection`/`openDistTop`/`closeDistTop`.
- **NTN-B coupon-date fix** — `_ntnb_total_return_pct_change(prices, maturity=...)` em `data_fetch.py` adiciona semi-coupon ((1.06)^0.5−1 ≈ 2.956%) de volta no pct_change da 1ª BDay ≥ data-cupom. Maturity-aware: cupons derivados do mês de expiração (NTN-B 2030-08-15 → Fev/Ago, não Mai/Nov). Eliminou outliers de -180 a -227 bps no spread Repl−Bench das IDKAs. Aplicado em `fetch_idka_hs_replication_series` e `fetch_idka_hs_spread_series`.
- **`export_idka_repl_vs_bench.py`** — script standalone pra investigação: dump xlsx com colunas `*_RET_CLEAN_BPS` (legado) vs `*_RET_BPS` (TR-adjusted) por NTN-B + summary distribuição. Output em `data/morning-calls/<date>_idka_repl_vs_bench.xlsx`.
- **P95 column** — coluna `a+var95` renomeada pra `p95` na tabela Distribuição (header + legenda).
- **MACRO PA FX-split** (`generate_macro_pa_fx_split.py`) — relatório standalone que reagrupa o PA do MACRO separando efeito-ativo de efeito-FX. CLASSE='BRLUSD' (+ 'FX' cross) → novo bucket "FX Basis Risk & Carry" com sub-grupos: FX em Commodities/RV Intl/RF Intl + FX Spot & Futuros (consolidado: USD Brasil hedge + spot + cross-FX + custos). Demais classes preservadas. Total idêntico ao PA canônico (pura reordenação). Inclui bloco de verificação numérica (✓/⚠ por bucket) e top contribuintes/detratores **excluindo** FX Basis (só efeito-ativo). Output: `data/morning-calls/<date>_macro_pa_fx_split.html`.

## 2026-04-24 — Daily Monitor + Peers charts + bug fix EVO _top1_var_delta

- **Daily Monitor** (`daily_monitor.html` + `pnl_server.py` + `start_monitor.bat`) — live P&L + Peers no browser via localhost:5050; `/api/pnl` busca DB ao vivo; `/api/peers` lê JSON do share.
- **Peers charts** — bar chart (horizontal, sorted desc) + scatter (Vol vs Retorno 12M), SVG puro, portado de `GLPG_Fetch/app.js`; vista padrão = Gráficos; toggles de período MTD/YTD/12M/24M/36M.
- **`risk_config._FUND_PEERS_GROUP`** — dict canônico fundo → peer group; QUANT→MACRO, MACRO_Q→EVOLUTION.
- **VaR/BVaR table rows** — clicáveis (`selectFund`) via `summary_renderers.py`.
- **Market tab parked** — `fetch_market_snapshot()` em `data_fetch.py`, seção HTML pronta; parkeado por 3 bugs de query.
- **Fix `_top1_var_delta` para EVOLUTION** — key corrigida de `"BOOK"` → `"rf"` — `fetch_evolution_var` renomeia a coluna antes de retornar; crash só ocorria quando |ΔVaR EVOLUTION| ≥ 5 bps, por isso não foi capturado no sanity check do wrap-up anterior.

## 2026-04-23 — _liquido_scale + BRL formatter rollout + VaR commentary

- `_liquido_scale` corrigido: `abs(bench)/abs(total)` — IDKA agora mostra exposição ativa correta no toggle Líquido (commit `69bda13`).
- BRL formatter aplicado em IDKA/ALBATROZ/Frontier; return type hints em 5 funções `data_fetch.py` (commit `c385c5d`).
- BRL formatter concluído: `pa_renderers`, `evo_renderers` (7 sites), `evolution_diversification_card`, `fund_renderers` (commit `6994064`).
- VaR commentary nos Comments confirmado implementado (commit `6f463bb`) — dispara quando |ΔVaR| ≥ 5 bps.

# CLAUDE.md — Risk Monitor (GLPG)

Fonte única de verdade sobre o que o projeto é, onde estamos, e quais regras
seguir. Carregado automaticamente em toda sessão.

---

## 1. Propósito

Monitorar o risco de todos os fundos da casa (famílias: **Multimercados, Renda
Fixa, Crédito, Renda Variável**) em base diária para o **Morning Call**:

- Evolução de risco em **VaR** e **Stress**
- Utilização de risco ex-post (orçamento de perda / PnL budget)
- Aderência ao **mandato** por fundo

O produto final é um briefing curto e acionável. Se não leva a uma decisão do
gestor, o briefing está errado.

---

## 2. Arquitetura de skills

```
.claude/skills/
├── risk-monitor/           # skill-mãe (orquestra + mandatos)
│   ├── fundos-canonicos.json
│   └── mandatos/
├── macro-stop-monitor/
├── macro-risk-breakdown/
├── evolution-risk-concentration/
├── rf-idka-monitor/
├── performance-attribution/
├── risk-morning-call/
└── wrap-session/
```

Skills são **complementares, não redundantes**. Não consolidar sem discutir.

---

## 3. Módulos do gerador

`generate_risk_report.py` é o orquestrador (~5050 linhas). Módulos auxiliares:

| Arquivo              | Responsabilidade                                              |
|----------------------|---------------------------------------------------------------|
| `risk_runtime.py`    | `DATA_STR`, `_parse_date_arg`, `fmt_br_num`                  |
| `risk_config.py`     | `FUNDS`, mandatos, PA keys, `_PM_LIVRO`, `_EVO_*`, `_DIST_PORTFOLIOS` |
| `db_helpers.py`      | `_latest_nav`, `_prev_bday`, `fetch_all_latest_navs`         |
| `data_fetch.py`      | Todos os `fetch_*` (~41 funções públicas)                    |
| `vardod_renderers.py`| VaR DoD attribution modal (trigger + scaffold + JS + CSS)    |
| `pmovers_renderers.py`| Top Movers Produto modal (per-fund popup PA card)           |
| `metrics.py`         | `compute_pm_hs_var`, `compute_frontier_bvar_hs`, `compute_pa_outliers`, vol regime |
| `svg_renderers.py`   | `range_bar_svg`, `stop_bar_svg`, `range_line_svg`, sparklines |
| `pa_renderers.py`    | PA tree, lazy-render JSON, section assembler                 |
| `evo_renderers.py`   | 4 camadas Evolution + Camada 4 (Bull Market Alignment)       |
| `expo_renderers.py`  | 8 `build_*_exposure_section`                                 |
| `fund_renderers.py`  | `build_stop_section`, briefings, distribuição, vol regime    |
| `html_assets.py`     | `UEXPO_JS` blob                                              |

### 3a. Variantes de saída — DUAS VERSÕES OBRIGATÓRIAS

O Morning Call é entregue em **duas versões**, cada uma para um público:

| Script                              | Tema | Consumo                         | Output                                                       |
|-------------------------------------|------|---------------------------------|---------------------------------------------------------------|
| `generate_risk_report.py`           | Dark | Desktop individual (analista)   | `data/morning-calls/{DATE}_risk_monitor.html` + mirror        |
| `generate_risk_report_meeting.py`   | Light | Telona da sala de reunião       | `data/morning-calls-meeting/ultimo_morning_call_meeting.html` |

**`_meeting.py` é um POST-PROCESSOR**: lê o HTML produzido pela versão dark e
pós-processa (paleta light, fontes ampliadas, logo escurecido, headers navy
do brand). Não toca em queries, dados, lógica — só apresentação.

**REGRA — ao implementar nova feature**:
1. Implementar normalmente em `generate_risk_report.py` / módulos renderer
2. **Verificar se a feature renderiza bem nas DUAS versões**:
   - Se introduz hex codes hardcoded escuros (`background:#0xxxxx`,
     `fill="#0xxxxx"`) ou claros demais (`color:#94a3b8`, etc), adicionar ao
     `_BG_HEX_MAP` ou `_TEXT_COLOR_MAP` em `generate_risk_report_meeting.py`
   - Se usa CSS variables (`var(--text)`, etc), funciona automático
3. Rodar `generate_risk_report_meeting.py` após o dark e confirmar render
4. Auto-routine (`run_report_auto.bat`) deveria rodar AMBOS sequencialmente
   (dark gera o HTML base, meeting transforma)

---

## 4. Fontes de dados (GLPG-DB01)

Todo acesso via `glpg_fetch.py`. O gerador é **100% DB-sourced** — sem leitura
de Excel, CSV ou JSON em runtime. Mandatos estão hardcoded nos dicts `FUNDS` /
`RAW_FUNDS` / `IDKA_FUNDS` em `risk_config.py`.

| Schema      | Tabela                                      | Uso                                          |
|-------------|---------------------------------------------|----------------------------------------------|
| `LOTE45`    | `LOTE_TRADING_DESKS_NAV_SHARE`              | NAV share por mesa/fundo                     |
| `LOTE45`    | `LOTE_FUND_STRESS_RPM`                      | VaR/Stress fundo — MACRO/QUANT/EVO (LEVEL=2/3) + BALTRA (LEVEL=10, look-through nativo) |
| `LOTE45`    | `LOTE_FUND_STRESS`                          | VaR/Stress produto — ALBATROZ + MACRO_Q (SUM por TRADING_DESK + filtro `TREE='Main'`) |
| `LOTE45`    | `LOTE_PARAMETRIC_VAR_TABLE`                 | BVaR + VaR IDKAs; fração decimal; filtrar `BOOKS::text='{*}'` |
| `LOTE45`    | `LOTE_BOOK_STRESS_RPM`                      | VaR por book/RF (LEVEL=3)                    |
| `LOTE45`    | `LOTE_PRODUCT_EXPO`                         | Exposição/delta — usar `TRADING_DESK_SHARE_SOURCE` |
| `q_models`  | `REPORT_ALPHA_ATRIBUTION`                   | PnL por PM (LIVRO) e instrumento             |
| `q_models`  | `STANDARD_DEVIATION_ASSETS`                 | σ por instrumento, BOOK='MACRO'              |
| `q_models`  | `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION`    | HS retornos (W); keys: MACRO, SIST, EVOLUTION, ALBATROZ, IDKA3Y, IDKA10Y |
| `q_models`  | `RISK_DIRECTION_REPORT`                     | Matriz direcional EVOLUTION (DELTA_SIST × DELTA_DISCR) |
| `q_models`  | `FRONTIER_TARGETS`                          | Posições alvo Frontier                       |
| `q_models`  | `COMPANY_SECTORS`                           | Setor por ação                               |
| `frontier`  | `LONG_ONLY_DAILY_REPORT_MAINBOARD`          | Posições diárias Frontier Long Only          |
| `public`    | `EQUITIES_COMPOSITION`                      | Pesos IBOV / SMLLBV                          |
| `public`    | `EQUITIES_PRICES`                           | Preços históricos (IBOV, ações)              |
| `public`    | `ECO_INDEX`                                 | CDI, IDKA index returns                      |

---

## 5. Mapa canônico de fundos

`risk-monitor/fundos-canonicos.json` é a referência de nomes. Antes de filtrar
por `TRADING_DESK` ou `FUNDO`, consultar o JSON.

Nunca escrever `SISTEMATICO` em query — o nome real no banco é **`QUANT`**.

---

## 6. Estado atual por fundo

| Fundo      | VaR/Stress          | PA       | Exposure          | Dist 252d       | Risk Budget     |
|------------|---------------------|----------|-------------------|-----------------|-----------------|
| MACRO      | ✅ RPM              | ✅       | ✅ por RF factor  | ✅              | ✅ por PM       |
| QUANT      | ✅ RPM              | ✅       | ✅                | ✅              | —               |
| EVOLUTION  | ✅ RPM              | ✅       | ✅ 3-níveis       | ✅              | —               |
| ALBATROZ   | ✅ LOTE_FUND_STRESS | ✅       | ✅ drill DV01     | ✅ HS gross     | ✅ 150bps/mês   |
| BALTRA     | ✅ LOTE_FUND_STRESS_RPM | ✅       | ✅ drill DV01 + Map + look-through (IDKAs/Albatroz cotas) | — (sem HS)      | — (prov. only)  |
| MACRO_Q    | ✅ LOTE_FUND_STRESS | ✅       | —                 | —               | —               |
| FRONTIER   | ✅ BVaR HS          | ✅ (GFA) | ✅ active wt      | ✅ α vs IBOV   | —               |
| IDKA 3Y    | ✅ BVaR param       | ✅       | ✅ 3-vias toggle  | ✅ HS active    | —               |
| IDKA 10Y   | ✅ BVaR param       | ✅       | ✅ 3-vias toggle  | ✅ HS active    | —               |
| NAZCA      | ✅ BVaR realizado (vs IMA-B) — wired but hidden in `FUND_ORDER` | — | — | — | — |

Limites provisórios: ALBATROZ, MACRO_Q, BALTRA, IDKA 3Y (soft 0.40/hard 0.60 daily),
IDKA 10Y (soft 1.00/hard 1.50 daily), NAZCA (soft 2.0/hard 3.0 — hidden). Aguardam mandatos definitivos.

### Crédito sub-projeto (standalone — `generate_credit_report.py`)

| Fundo     | Snapshot | Distribuição | Concentração | Alocação drill | Mercado | AUM | Retorno | Sanity Preços |
|-----------|----------|--------------|--------------|----------------|---------|-----|---------|---------------|
| Sea Lion  | ✅       | ✅           | ✅           | ✅ 4 modos      | ✅      | ✅  | ✅      | ✅            |
| Iguana    | ✅       | ✅           | ✅           | ✅ 4 modos      | ✅      | ✅  | ✅      | ✅            |
| Pelican   | ✅       | ✅           | ✅           | ✅ 4 modos      | ✅      | ✅  | ✅      | ✅            |
| Dragon    | ✅       | ✅           | ✅           | ✅ 4 modos (default subord) | ✅ | ✅ | ✅ | ✅ |
| Barbacena | ✅       | ✅           | ✅           | ✅ 4 modos      | ✅      | ✅  | ✅      | ✅            |
| Nazca     | ✅       | ✅           | ✅           | ✅ 4 modos      | ✅      | ✅  | ✅      | ✅            |

Crédito tab no main report cobre **BALTRA + EVOLUTION** via look-through `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE`. MACRO_Q **não** tem look-through Albatroz — fora do escopo do tab. ALBATROZ tem CRIs/debentures direto (ainda não wired no tab Crédito do main report).

---

## 7. Fase atual e próximas ações

**Fase 4 em andamento** (desde 2026-04-18). Ver `git log` ou
[`docs/CHANGELOG.md`](docs/CHANGELOG.md) para histórico detalhado de features
e fixes (entradas anteriores migradas pra fora do CLAUDE.md em 2026-05-01).

Fila priorizada (fazer nesta ordem):

1. ~~**PA-FX-split dedup**~~ → **DONE 2026-05-01 session 2** (commits 16319e2 + 9cfc673).
2. ~~**6 NEW HIGH correctness fixes**~~ → **DONE 2026-05-01 session 3** (commits b925bb4 + 42ca65f + c594e77).
3. ~~**Briefing tightening (rule-based)**~~ → **DONE 2026-05-01 session 3** (commit d78992a). "tranquilo" gate now util≥70 / |Δ VaR|≥5 / |alpha|≥3 / |MTD|≥25 — count dropped 8→1 of 19 cards.
4. ~~**Skill-refresh sprint**~~ → **DONE 2026-05-01 session 3** (commit 9c886f5). 5 SKILL.md refreshed + 14 `glpg-data-fetch` references corrected.
5. ~~**Day-3 hygiene** (vacuous comments, unused imports, NaN/zero edges, m12 boundary, docstrings, small renames)~~ → **DONE 2026-05-01 session 3** (commits 4fee511 + 84f465d + 777dc94).

Closed na sessão 2026-05-02 (commits 0cd1674 + bb14a7a + dcac9de):

- ~~**§4.3 glpg_fetch.py env-only (security)**~~ — env-only com fail-fast no import; sem fallback de host/user/dbname.
- ~~**§2.13h desk-name centralization**~~ — `_MACRO_DESK` / `_FRONTIER_DESK` / etc. em `risk_config.py`; sweep em `data_fetch.py`, `db_helpers.py`, `pm_vol_card.py`, `generate_risk_report.py`.
- ~~**§3.3c issuer override**~~ — Cruz hardcode → `credit/issuer_overrides.json` com loader genérico.
- ~~**§2.11 iloc cast hardening**~~ — `pd.to_numeric(...).fillna(0.0).iloc[0]` em vez de `float(iloc[0])`.
- ~~**§2.13a VaR DoD nav_d1 (RPM)**~~ — `_var_dod_rpm` divide D-1 contributions por D-1 NAV.
- ~~**§1.6 main() split**~~ — 540 → 8 linhas. Helpers `_fetch_all_data` / `_build_report_data` / `_write_output`.
- ~~**§2.13f nav_d1 propagation**~~ — Plumbed end-to-end: ReportData (novos `quant_expo_nav_d1`/`evo_expo_nav_d1`), `build_quant_exposure_section` + `build_evolution_exposure_section`, `_build_expo_unified_table`.
- ~~**§1.5 build_html extraction batch 1**~~ — 1214 → 690 linhas (-43%). 5 helpers: `_build_pa_alerts_html`, `_build_summary_rows_html`+`_build_bench_rows_html`, `_build_factor_matrix`, `_build_agg_rows`, `_build_house_rows`.
- ~~**§1.5 build_html extraction batch 2**~~ — 690 → 540 linhas (-22%). 3 helpers: `_assemble_sections_html` (per-fund reorder + peers tab), `_build_var_commentary` (DoD prefetch + top-driver), `_build_summary_view` (EVO C4 headline + summary cards). Smoke test passes; output diff vs baseline = same drift como self-vs-self regen.
- ~~**§1.2 Briefing Executivo headline priority**~~ — Headline agora escolhe pela regra `max(margem_inverse, util_VaR, |Δ VaR|)` com triggers individuais (margem<25 / util≥85% / |ΔVaR|≥10 bps). PA hit ≥1% NAV continua override. "Atenção" reposicionada acima do brief-grid (full-width strip — antes ficava em col 2, abaixo de "Risco · o que mudou").
- ~~**§14 Holiday-aware default date**~~ — `risk_runtime._resolve_default_data_date` consulta `LOTE45.LOTE_TRADING_DESKS_NAV_SHARE.MAX(VAL_DATE) < CURRENT_DATE` (DB primary — calendário implícito B3 + awareness de ingestion lag); fallback é o set hardcoded `_BR_HOLIDAYS` (2024-2027) com `BusinessDay(1)` walk-back. DB lookup é lazy — consumers que passam `--date` não pagam custo. Sem CLI arg em pós-feriado, agora pega o último trading day com dados ingeridos (ex: Sat 2026-05-02 → 2026-04-29).
- ~~**§1.3 Status DIA fallback when PA on D-1**~~ — Quando `pa_has_today` é False, a coluna DIA do Status consolidado renderiza o valor do D-1 com tag `(D-1)` em texto pequeno mudo (antes mostrava `—` silente). Backed by `df_pa_daily` (estendido pra incluir BALTRA + IDKAs em `fetch_pa_daily_per_product`). Tooltip "PA pendente para hoje — última obs D-1" no caso flat.

UI tweaks na sessão 2026-05-02:
- Removida footer row "Total NAV-pond. (não-div.)" do "Risco VaR e BVaR por fundo".
- Vol Regime card pula linhas vazias (não renderiza row de "—").

Próximas (ainda abertas):

6. **LLM briefings** — substituir rule-based por Haiku 4.5 em `fund_renderers._build_fund_mini_briefing` (long-term substitution; rule-tightening em #3 é stopgap).
7. **`build_html` extraction batch 3** (§1.5 continued) — ~540L restantes em build_html: master HTML template f-string (~85L), body composition + tab subtab generation (~150L), orchestration glue (~300L de variable wiring + section-list construction loops).
8. **VaR DoD exposure NAV-axis (§2.13b — IDKA cota timing)** — IDKA SHARE pct_change tem cotização axis mismatch (D-2 admin vs D bench); BVaR potencialmente overstated 10-30% vs engine.
9. **Iterrows vectorization** (§2.9 / §2.10) — `data_fetch.py:1417` nested + 3 sites em `generate_risk_report.py`.
10. **Unit tests** para `svg_renderers` + `metrics` (sem DB, ≈ 1 dia).
11. **EVOLUTION BRLUSD legacy non-zero** (§1.4) — escalar pra dono do PA engine; não é bug do kit.

**Backlog primário agora**: `docs/CODE_REVIEW_2026-05-01_session2.md` (com STATUS atualizado no topo na sessão 3). A `docs/CODE_REVIEW_2026-05-01.md` original tem todos itens fechados (PA-FX-split + §2d) ou cross-listados na nova review. Roadmap analítico continua em `memory/project_todo_risk_analytics_roadmap.md`.

---

## 8. Armadilhas conhecidas

- **NAV defasa ~1d vs VaR/Expo** — toda query point-in-time de NAV usa `_latest_nav` / `merge_asof(backward)`.
- **DELTA já é duration-weighted** em `LOTE_PRODUCT_EXPO` (= POSITION × MOD_DURATION). Não multiplicar por MOD_DURATION de novo.
- **`LOTE_PARAMETRIC_VAR_TABLE`** — sempre filtrar `BOOKS::text='{*}'`; sem o filtro, soma triplicada.
- **DV01 sign convention** — `tomado = DV01 > 0` (short bond) vermelho; `dado = DV01 < 0` (long bond) verde.
- **D-1 contábil** — shift de um dia aplica só a fundos com admin externo fora do lote. FICs internos (Evolution) têm look-through direto, sem shift.
- **DB drift** — tabelas PA/NAV são reescritas por batch a cada ~30–60 min. Validação numérica de refactor exige regen back-to-back (< 2 min entre runs).
- **`_liquido_scale` sign mismatch** — `bench_matrix` usa DV01 (long bond = negativo); `agg_rows["brl"]` usa `ano_eq_brl` (long bond = positivo). Em `_liquido_scale` usar `abs(bench)/abs(total)`. Nunca alterar o sinal do `bench_matrix` — quebraria o Factor Breakdown que usa `gross - bench` em DV01.
- **Top Posições double counting** — prevenido por `via == 'direct'` em `agg_rows` (exclui look-through e via_albatroz) e por `fetch_evolution_direct_single_names` (exclui QUANT/Frontier/MACRO da equity Evolution).
- **ANBIMA UNIT_PRICE é clean (ex-coupon)** — `pct_change()` direto em série ANBIMA gera -200 a -300 bps fantasma na data-cupom NTN-B. Sempre usar `_ntnb_total_return_pct_change(prices, maturity=...)` que reinjeta o semi-coupon. Cupons derivam da maturity (`m+6 mod 12`), NÃO são fixos em Mai/Nov.
- **JS strings em Python f-string** — `\n` em string Python vira newline literal no JS de saída → SyntaxError silencioso quebra IIFE inteira. Usar `\\n` ou tooltip via `<div>` custom em vez de `<title>` SVG.
- **`setDistBench` precisa chamar `_applyDistVisibility`** — trocar bench tab (Benchmark/Replication/Comparação) sem reaplicar mode/window faz a tabela aparecer vazia. As 4 views internas (bw1/fw1/bw21/fw21) ficam com display:none até que `_applyDistVisibility(card)` seja chamado.
- **Exposure RF — LFTs (CDI) inflam métricas cosméticas** — `cls_to_idx["LFT"] = "CDI"` em `fetch_albatroz_exposure`. LFTs são floating-rate (mod_dur ≈ 0), e somam em Gross/Net %NAV sem representar risco real. `build_albatroz_exposure` filtra `indexador != "CDI"` no início. Adicionalmente filtra `mod_dur > 0.01` pra remover Equity / IBOVSPFuture / USDBRLFuture / FIDCs / Corn Futures que também não têm rate sensitivity (deixa só CRIs e bonds).
- **Exposure RF — Net (yrs)** — após o fix de `fetch_albatroz_exposure` (filtro de rate primitive), `delta_brl` é POSITION × MOD_DURATION. Logo `delta_brl/nav` dá duração em yrs direto. Coluna "Net" mostra yrs, não %NAV. `yr_cell(v_brl)` em `expo_renderers.py` faz a conversão.
- **`fetch_albatroz_exposure` — primitive filter** — engine decompõe NTN-B/DAP em 3 primitives (sovereign spread + IPCA face + IPCA Coupon rate), com sinais inconsistentes entre primitives. SUM(DELTA) sem filtro = garbage. Função filtra UM rate primitive por PRODUCT_CLASS via WHERE clause: NTN-B/DAP→`IPCA Coupon`, NTN-C/DAC→`IGPM Coupon`, DI/NTN-F/LTN→`BRL Rate Curve`. CRIs ainda somam todos primitives (parking lot).
- **TRADING_DESK_SHARE_SOURCE faz look-through nativo** — em `LOTE_PRODUCT_EXPO`, filtrar **só** `SHARE_SOURCE='X'` captura direto + look-through. Adicionar `AND TRADING_DESK='X'` filtra fora as cotas do fundo (IDKA, Albatroz, Estrat. Prev. CP) e quebra fundos cotistas (BALTRA, IDKAs). Memory: `project_rule_share_source_lookthrough.md`.
- **`LOTE_PRODUCT_BOOK_POSITION_PL.PL_PCT` é per-position** — `PL_PCT = PL/AMOUNT` por linha, NÃO somável em agregação por book/PM. Sempre recomputar bps como `PL / fund_NAV` em qualquer agregação. Frontend group header deve **somar** os pl_pct dos books-filhos (que agora são `PL/NAV`), não calcular média. Memory: `project_rule_book_pnl_per_position_pct.md`.
- **`pnl_server.py` precisa restart manual após editar `data_fetch.py`** — Python importa o módulo uma vez no startup; edits não fazem reload automático. Localizar PID via `wmic process where "CommandLine like '%pnl_server%'" get ProcessId,CommandLine` + `taskkill /PID <pid> /F` + relançar.
- **Section `(fund, report)` keys devem ser únicas** — `sections.append((fund, report, html))` cria `<template class="tpl-section" data-fund=... data-report=...>` e lazy-hydration `_hydrateSection` usa `document.querySelector(...)` (não `querySelectorAll`). Duplicate key = só o primeiro template clonado, conteúdo do segundo desaparece silenciosamente. Para múltiplos cards numa seção, concatenar HTML em uma única entry (ver pattern `sections[-1] = (_f, _r, _h + _extra)`).
- **Lazy hydration + visibility** — `_idleHydrateAll` clona templates em background depois do first paint. `applyState`'s visibility loop NÃO re-roda. Sections recém-clonadas herdam `display:block` (default CSS). Se user está em summary/quality/etc. quando idle-hydration completa, todas as cards de fundo aparecem dumped na view. Fix em `_hydrateSection`: hide cada section antes de append; reveal só se mode/sel da tab ativa fizer match.
- **JS regex em Python f-string** — Python interpreta `\s`, `\b`, `\d` em strings. `\s` emite SyntaxWarning (a partir de 3.12 pode virar SyntaxError); `\b` é convertido pra backspace (0x08) silently → o JS recebe `<BS>` em vez de `\b`. Sempre doublar (`\\s`, `\\b`, `\\.`) ou usar raw f-string `rf"""..."""` pro bloco JS.
- **Nazca wired hidden** — entry em `RF_BENCH_FUNDS` + fetch + label, mas NÃO em `FUND_ORDER`. Re-enable adicionando `"NAZCA"` em `risk_config.FUND_ORDER`. Ver `fetch_risk_history_rf_bench` para extender a outros fundos com bench-relative BVaR computado de retornos realizados.
- **Python 3.11 não suporta same-quote nested f-strings** — venv é 3.11.7; PEP 701 só vem em 3.12. `f"{r["key"]}"` quebra com `SyntaxError: f-string: f-string: unmatched '['`. Extrair pra var local (`v_str = fmt(r["key"])` antes do template) ou usar single quote interno (`f"{r['key']}"`). Caught em compile-sweep 2026-05-01 (commit `f1563913` introduziu — corrigido em `825c02b`).

---

## 9. Gaps deliberados

Fora de escopo até decisão explícita:

- ~~Fundos **BALTRA**~~ — **implementado 2026-04-26, migrado pra LOTE_FUND_STRESS_RPM em 2026-04-28** (commit `51be7a9`): VaR/Stress via RPM (LEVEL=10 nativo) + PA + Exposure RF + Exposure Map + Top Movers Produto. Benchmark = IPCA+ (~3-4 anos duration real), a confirmar. Limites provisórios (soft 1.75%/hard 2.50% VaR; soft 12.6%/hard 18% stress).
- ~~Família **Crédito**~~ — **implementado 2026-04-30** (sub-projeto standalone `generate_credit_report.py` cobre 6 fundos; tab Crédito no main report cobre BALTRA + EVOLUTION via look-through). Mandatos formais aguardam confirmação por fundo.
- Fundos **FMN** (relatório separado via xlwings existe)

---

## 10. Convenções

**Idioma** — código/variáveis/JSON em inglês; documentação e Morning Call em português.

**Git** — commitar após cada sub-feature funcional. Mensagem no imperativo em inglês.

**SQL** — sempre parametrizar datas e fundos. Sem literais de data no WHERE.

**Tabelas HTML** — toda tabela tem sorting (via `attachUniversalSort()`) e CSV export (via `injectCsvButtons()`) por padrão. Exceção: adicionar `data-no-sort="1"`. Pinned rows usam `data-pinned="1"`.

---

## 11. Como começar uma nova sessão

> Leia CLAUDE.md e me diga em qual fase estamos e qual é a próxima ação
> concreta. Antes de rodar qualquer código contra o GLPG, me mostre o plano.

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

`generate_risk_report.py` é o orquestrador (4282 linhas). Módulos auxiliares:

| Arquivo              | Responsabilidade                                              |
|----------------------|---------------------------------------------------------------|
| `risk_runtime.py`    | `DATA_STR`, `_parse_date_arg`, `fmt_br_num`                  |
| `risk_config.py`     | `FUNDS`, mandatos, PA keys, `_PM_LIVRO`, `_EVO_*`, `_DIST_PORTFOLIOS` |
| `db_helpers.py`      | `_latest_nav`, `_prev_bday`, `fetch_all_latest_navs`         |
| `data_fetch.py`      | Todos os `fetch_*` (36 funções públicas)                     |
| `metrics.py`         | `compute_pm_hs_var`, `compute_frontier_bvar_hs`, `compute_pa_outliers`, vol regime |
| `svg_renderers.py`   | `range_bar_svg`, `stop_bar_svg`, `range_line_svg`, sparklines |
| `pa_renderers.py`    | PA tree, lazy-render JSON, section assembler                 |
| `evo_renderers.py`   | 4 camadas Evolution + Camada 4 (Bull Market Alignment)       |
| `expo_renderers.py`  | 8 `build_*_exposure_section`                                 |
| `fund_renderers.py`  | `build_stop_section`, briefings, distribuição, vol regime    |
| `html_assets.py`     | `UEXPO_JS` blob                                              |

---

## 4. Fontes de dados (GLPG-DB01)

Todo acesso via `glpg_fetch.py`. O gerador é **100% DB-sourced** — sem leitura
de Excel, CSV ou JSON em runtime. Mandatos estão hardcoded nos dicts `FUNDS` /
`RAW_FUNDS` / `IDKA_FUNDS` em `risk_config.py`.

| Schema      | Tabela                                      | Uso                                          |
|-------------|---------------------------------------------|----------------------------------------------|
| `LOTE45`    | `LOTE_TRADING_DESKS_NAV_SHARE`              | NAV share por mesa/fundo                     |
| `LOTE45`    | `LOTE_FUND_STRESS_RPM`                      | VaR/Stress fundo (LEVEL=10) — MACRO/QUANT/EVO |
| `LOTE45`    | `LOTE_FUND_STRESS`                          | VaR/Stress produto — ALBATROZ + MACRO_Q (SUM por TRADING_DESK) |
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
| BALTRA     | ✅ LOTE_FUND_STRESS | ✅       | ✅ drill DV01 + Map | — (sem HS)      | — (prov. only)  |
| MACRO_Q    | ✅ LOTE_FUND_STRESS | ✅       | —                 | —               | —               |
| FRONTIER   | ✅ BVaR HS          | ✅ (GFA) | ✅ active wt      | ✅ α vs IBOV   | —               |
| IDKA 3Y    | ✅ BVaR param       | ✅       | ✅ 3-vias toggle  | ✅ HS active    | —               |
| IDKA 10Y   | ✅ BVaR param       | ✅       | ✅ 3-vias toggle  | ✅ HS active    | —               |

Limites provisórios: ALBATROZ, MACRO_Q, BALTRA, IDKA 3Y (soft 0.40/hard 0.60 daily),
IDKA 10Y (soft 1.00/hard 1.50 daily). Aguardam mandatos definitivos.

---

## 7. Fase atual e próximas ações

**Fase 4 em andamento** (desde 2026-04-18). Ver `git log` para histórico detalhado.

Fila priorizada (fazer nesta ordem):

1. **MACRO↔QUANT exposure harmonization** — unificar layout, QUANT herda Δ Expo + σ + VaR signed
2. **LLM briefings** — substituir rule-based por Haiku 4.5 em `fund_renderers._build_fund_mini_briefing`
3. **Wire `evolution_diversification_card.py`** no relatório principal
4. **Unit tests** para `svg_renderers` + `metrics` (sem DB, ≈ 1 dia)

Backlog completo em `memory/project_todo_risk_analytics_roadmap.md`.

**Fixes entregues 2026-04-23:**
- `_liquido_scale` corrigido: `abs(bench)/abs(total)` — IDKA agora mostra exposição ativa correta no toggle Líquido (commit `69bda13`)
- BRL formatter aplicado em IDKA/ALBATROZ/Frontier; return type hints em 5 funções `data_fetch.py` (commit `c385c5d`)
- BRL formatter concluído: `pa_renderers`, `evo_renderers` (7 sites), `evolution_diversification_card`, `fund_renderers` (commit `6994064`)
- VaR commentary nos Comments confirmado implementado (commit `6f463bb`) — dispara quando |ΔVaR| ≥ 5 bps

**Fixes entregues 2026-04-24:**
- `_top1_var_delta` para EVOLUTION: key corrigida de `"BOOK"` → `"rf"` — `fetch_evolution_var` renomeia a coluna antes de retornar; crash só ocorria quando |ΔVaR EVOLUTION| ≥ 5 bps, por isso não foi capturado no sanity check do wrap-up anterior

**Features entregues 2026-04-24 (segunda sessão):**
- **Daily Monitor** (`daily_monitor.html` + `pnl_server.py` + `start_monitor.bat`) — live P&L + Peers no browser via localhost:5050; `/api/pnl` busca DB ao vivo; `/api/peers` lê JSON do share
- **Peers charts** — bar chart (horizontal, sorted desc) + scatter (Vol vs Retorno 12M), SVG puro, portado de `GLPG_Fetch/app.js`; vista padrão = Gráficos; toggles de período MTD/YTD/12M/24M/36M
- **`risk_config._FUND_PEERS_GROUP`** — dict canônico fundo → peer group; QUANT→MACRO, MACRO_Q→EVOLUTION
- **VaR/BVaR table rows** — clicáveis (`selectFund`) via `summary_renderers.py`
- **Market tab parked** — `fetch_market_snapshot()` em `data_fetch.py`, seção HTML pronta; parkeado por 3 bugs de query (ver `memory/project_todo_market_tab_fixes.md`)

**Features entregues 2026-04-27:**
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

**Features entregues 2026-04-28:**
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

**Features entregues 2026-04-28 (segunda sessão — commits `2e948f0`, `c870de1`):**
- **Risk Budget — nova regra de carry** (`fund_renderers.carry_step`) — pnl positivo: `next = 63 + 0.5 × pnl` SEMPRE (substitui o reset-para-63 + bônus de crossover YTD); pnl negativo: 3 camadas de penalty (extra `B_t − 63` = 25% · base 63 = 50% · excedente acima de B_t = 100%) + cap em `min(B_t, 63)`. Carry extra não consumido evapora ("use it or lose it"). Detalhado em `memory/project_rule_macro_carry_step.md`.
- **Override LF Apr/26 = 20 bps** (`data/mandatos/risk_budget_overrides.json`) — segundo override ativo (RJ Apr = 63 já existia).
- **STOP → ⚪ FLAT downgrade** (`fund_renderers.build_stop_section` + plumbing em `generate_risk_report.build_html`) — quando PM em STOP territory mas sem exposição viva (`Σ|delta| < 0.05% NAV` em `df_expo`), status passa a FLAT cinza. Não aplica a CI.
- **Stop history modal — drill-down BOOK-level** (`_build_stop_history_modal` + `data_fetch.fetch_pm_book_pnl_history` + handler JS `toggleStopHistRow`) — cada linha-mês ganha caret `▶` clicável; expande mostrando breakdown por BOOK do PnL daquele mês, ordenado por |PnL| desc.
- **MACRO Exposure PM VaR — caret toggle** (`expo_renderers.toggleDrillPM`) — drill de PM (CI/LF/JD/RJ/QM) em PM VaR mode flipa `▶ ↔ ▼` corretamente (antes ficava sempre `▶`).
- **Vol Regime — default expanded** (`fund_renderers.build_vol_regime_section`) — caret das linhas-fund agora `▼` por default, books/PMs visíveis.
- **Paleta de cores — bump `--muted`** (`generate_risk_report.py:1696`) — `#8892a0 → #a8b3c2` (contraste 5.8:1 → 7.8:1, AAA p/ texto pequeno). Adicionados `--muted-strong: #c9d1dd` e `--muted-soft: #6b7480`. Aplicado em `.card-sub` / `.bar-legend` / `.comment-empty` / `.brief-footnote` (com `font-weight:500` em parágrafos pequenos). `@media print` remapeado também (`--muted-strong: #333`, `--muted-soft: #888`).
- **Task Scheduler — daily report automation** (`run_report_auto.bat` + scheduled task `Risk Monitor - Daily Report`) — bat não-interativo com weekend guard via PowerShell `(Get-Date).DayOfWeek`, data via `latest_bday.py` (ANBIMA-aware), sem UI/browser, log em `logs/auto_report.log`. Task registrada Mon-Fri 08:00 logged-on-only via `schtasks /Create`. `logs/` adicionado ao `.gitignore`.

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
- **Exposure RF — LFTs (CDI) inflam métricas cosméticas** — `cls_to_idx["LFT"] = "CDI"` em `fetch_albatroz_exposure`. LFTs são floating-rate (mod_dur ≈ 0), e somam em Gross/Net %NAV sem representar risco real. `build_albatroz_exposure` filtra `indexador != "CDI"` no início.

---

## 9. Gaps deliberados

Fora de escopo até decisão explícita:

- ~~Fundos **BALTRA**~~ — **implementado 2026-04-26**: VaR/Stress + PA em `RAW_FUNDS`. Benchmark = IPCA+ (~3-4 anos duration real), a confirmar. Limites provisórios (soft 1.5%/hard 2.5% VaR; soft 10%/hard 18% stress).
- Fundos **FMN** (relatório separado via xlwings existe)
- Família **Crédito** (entra só após MM + RF estáveis)

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

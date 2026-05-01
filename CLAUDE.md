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

**Fase 4 em andamento** (desde 2026-04-18). Ver `git log` para histórico detalhado.

Fila priorizada (fazer nesta ordem):

1. **LLM briefings** — substituir rule-based por Haiku 4.5 em `fund_renderers._build_fund_mini_briefing`
2. **Wire `evolution_diversification_card.py`** no relatório principal
3. **Unit tests** para `svg_renderers` + `metrics` (sem DB, ≈ 1 dia)

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
- **Market tab parked** — `fetch_market_snapshot()` em `data_fetch.py`, seção HTML pronta; parkeado por 3 bugs de query.

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
- **Risk Budget — nova regra de carry** (`fund_renderers.carry_step`) — pnl positivo: `next = 63 + 0.5 × pnl` SEMPRE (substitui o reset-para-63 + bônus de crossover YTD); pnl negativo: 3 camadas de penalty (extra `B_t − 63` = 25% · base 63 = 50% · excedente acima de B_t = 100%) + cap em `min(B_t, 63)`. Carry extra não consumido evapora ("use it or lose it").
- **Override LF Apr/26 = 20 bps** (`data/mandatos/risk_budget_overrides.json`) — segundo override ativo (RJ Apr = 63 já existia).
- **STOP → ⚪ FLAT downgrade** (`fund_renderers.build_stop_section` + plumbing em `generate_risk_report.build_html`) — quando PM em STOP territory mas sem exposição viva (`Σ|delta| < 0.05% NAV` em `df_expo`), status passa a FLAT cinza. Não aplica a CI.
- **Stop history modal — drill-down BOOK-level** (`_build_stop_history_modal` + `data_fetch.fetch_pm_book_pnl_history` + handler JS `toggleStopHistRow`) — cada linha-mês ganha caret `▶` clicável; expande mostrando breakdown por BOOK do PnL daquele mês, ordenado por |PnL| desc.
- **MACRO Exposure PM VaR — caret toggle** (`expo_renderers.toggleDrillPM`) — drill de PM (CI/LF/JD/RJ/QM) em PM VaR mode flipa `▶ ↔ ▼` corretamente (antes ficava sempre `▶`).
- **Vol Regime — default expanded** (`fund_renderers.build_vol_regime_section`) — caret das linhas-fund agora `▼` por default, books/PMs visíveis.
- **Paleta de cores — bump `--muted`** (`generate_risk_report.py:1696`) — `#8892a0 → #a8b3c2` (contraste 5.8:1 → 7.8:1, AAA p/ texto pequeno). Adicionados `--muted-strong: #c9d1dd` e `--muted-soft: #6b7480`. Aplicado em `.card-sub` / `.bar-legend` / `.comment-empty` / `.brief-footnote` (com `font-weight:500` em parágrafos pequenos). `@media print` remapeado também (`--muted-strong: #333`, `--muted-soft: #888`).
- **Task Scheduler — daily report automation** (`run_report_auto.bat` + scheduled task `Risk Monitor - Daily Report`) — bat não-interativo com weekend guard via PowerShell `(Get-Date).DayOfWeek`, data via `latest_bday.py` (ANBIMA-aware), sem UI/browser, log em `logs/auto_report.log`. Task registrada Mon-Fri 08:00 logged-on-only via `schtasks /Create`. `logs/` adicionado ao `.gitignore`.

**Features entregues 2026-04-28 (terceira sessão):**
- **IDKA HS replication — engine-style + strict NTN-B coupon TR** (`data_fetch._compute_idka_replication_returns` + cache `data/idka_replication_cache.json`) — substitui o approach asset-based ("hold same NTN-Bs, replay history") por engine-style: **at each historical date `t`, solve a constant-DV-target NTN-B-only portfolio at `t_prev` and earn its 1d total return on `t`**. Replication independente da posição do fundo. Cache JSON imutável (243 dates × 2 targets ≈ 19KB), backfill ~10s on cold start, ~0.3s daily marginal. Plus: strict coupon adjustment via `_get_vna_ntnb` — `r_TR = r_clean + (semi_coupon × VNA(cup)) / P_prev` (era `(1+r)(1+c)−1` que tinha bias ~9 bps em coupon days). VNA puxado de `ECO_INDEX.VNA_NTNB`. Spread mean centrado em ~0 agora (antes -0.14 bps).
- **VaR DoD attribution — Fase 1 (data layer)** (`data_fetch.fetch_var_dod_decomposition`) — função pública que devolve decomposição D-vs-D-1 por leaf factor. Schema 16 colunas (label, group, contrib_d1/d/delta, pos_d1/d/d_pos_pct, vol_d1/d/d_vol, pos_effect/vol_effect, sign, override_note, children). 8 fundos: IDKAs (LOTE_PARAMETRIC_VAR_TABLE per PRIMITIVE) · MACRO/QUANT/EVO (LOTE_FUND_STRESS_RPM LEVEL=10 per BOOK) · ALBATROZ/MACRO_Q/BALTRA (LOTE_FUND_STRESS per PRODUCT). Decomposição "today's pos constant": `pos_effect = (pos_d − pos_d1) × g_d1`, `vol_effect = pos_d × (g_d − g_d1)`, exact sum.
- **VaR DoD attribution — Fase 2 (modal)** (`vardod_renderers.py` novo + 5 triggers em `expo_renderers.py` + injection em `generate_risk_report.build_html`) — popup modal acionado por botão azul outlined "VaR DoD →" no header de cada exposure section. Modal compacto ~820px com tabela ordenada por |Δ| desc, sortável por header. Headline: ΔVaR + breakdown pos/vol. Filter zero-rows (`|contrib_d1| < 0.05 AND |contrib_d| < 0.05`). Linhas com override em destaque amarelo. ESC/backdrop fecha. JSON payload embedded `window.__VAR_DOD_DATA` (~190KB).
- **IDKA bench primitive override — unconditional** (`data_fetch._var_dod_idka`) — força DELTA do passivo (`PRIMITIVE='IDKA IPCA 3Y'`/`'IDKA IPCA 10Y'`) para `-NAV` sempre, escala `contrib`/`vol` proporcionalmente. Resolve bug intermitente do engine que oscila ratio bench/NAV entre -1.00 e -0.62/-0.71 nos 2 IDKAs simultaneamente, gerando ΔBVaR artificial. Audit trail em `override_note` quando correção é material (`|ratio + 1| > 0.05`).
- **Albatroz look-through inline nos modais IDKA** (`data_fetch._explode_albatroz_for_idka` + JS expandable rows) — linha "GALAPAGOS ALBATROZ FIRF LP" na tabela IDKA agora tem caret ▶ que expande pras 4 posições internas do Albatroz (DI1F33, DI1F28, DAPK35, NTNB 15/08/2050) reescaladas pra bps no NAV do IDKA. Position via `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE='IDKA IPCA Xy FIRF'`. VaR via `LOTE_FUND_STRESS` Albatroz × scale = parent_idka / albatroz_total. Soma dos children = parent (exato).
- **BALTRA look-through synthetic parents** (`data_fetch._regroup_lookthrough` + `_fetch_lookthrough_source_funds`) — modal BALTRA agrupa posições look-through sob 3 parent rows: ↻ IDKA 10Y holdings (10 children) · ↻ IDKA 3Y holdings (9) · ↻ Albatroz holdings (22). Direct rows (Prev book, CRIs) ficam standalone. Workaround usando cross-join LOTE_FUND_STRESS × LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE. **Resolvido 2026-04-28** com migração BALTRA → LOTE_FUND_STRESS_RPM (LEVEL=10 nativo) — workaround só ativa pra ALBATROZ/MACRO_Q.
- **Fund switcher scroll-to-top** (`generate_risk_report.selectFund` JS) — clicar em outro fundo no nav agora scrollTo(0) instant. Antes ficava na posição vertical anterior.

**Features entregues 2026-04-28 (quarta sessão):**
- **VaR DoD modal — relabel "Vol eff" → "Marg eff" + footnote rewrite** (`vardod_renderers.py`) — coluna passou a se chamar "Marg eff", headline mostra "marginal (Δg)" em vez de "vol/marginal", e footnote esclarece que `g = contrib/pos` é a contribuição marginal de VaR por BRL de exposição (absorve mudanças de vol E correlação — engine não isola σ). Dispara dúvida recorrente: "vol_effect" é equívoco; o que medimos é mudança de risco-por-BRL holding pos constante.
- **VaR DoD — FRONTIER coverage** (`data_fetch._var_dod_frontier` + `_VAR_DOD_DISPATCH["FRONTIER"]` source `frontier_hs` + trigger em `expo_renderers.build_frontier_exposure_section`) — fecha cobertura full-suite (9 fundos). Decomposição via component-VaR no q05 worst-day scenario: `component_i = (w_i − w_ibov_i) × r_i_at_q05`, soma exata = -BVaR_pct. Fallback: quando `frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD` upstream não tem D-1 (hoje só tem 1 data populada), reusa pesos de hoje em D-1 — captura só shift de cenário, sem efeito de posição. Caveat surfaced via novo campo `df.attrs["modal_note"]` → payload `modal_note` → warning bar com prefix "ℹ".
- **Modal warning bar — dual-channel** (`vardod_renderers.VARDOD_JS`) — agora suporta `modal_note` (caveat por fundo, prefixo ℹ) E row-level overrides (engine artifact IDKA, prefixo ⚠). Multi-line via `<br>`. Refactor pra desacoplar "info do modal" de "row destacada amarela" — antes setar override em row pra surfaceá uma mensagem genérica pintava o row de amarelo erroneamente.

**Features entregues 2026-04-28 (quinta sessão):**
- **BALTRA migration → LOTE_FUND_STRESS_RPM** (`data_fetch._VAR_DOD_DISPATCH["BALTRA"]: lote_fund → rpm_book`) — RPM populado upstream desde 2026-04-07 (LEVEL=10 com look-through nativo). Modal DoD agora compacto (13 BOOK rows, antes 87+ produtos com synthetic ↻ parents). Sem necessidade de cross-join LOTE_PRODUCT_EXPO. Workaround antigo (`_regroup_lookthrough`) ainda existe pra ALBATROZ/MACRO_Q (que continuam em LOTE_FUND_STRESS).
- **BALTRA TREE='Main' filter** (`data_fetch.fetch_risk_history_raw` + `_var_dod_lote_fund`) — BALTRA era único RAW_FUND com 3 TREEs em LOTE_FUND_STRESS (Main / Main_Macro_Gestores / Main_Macro_Ativos), todas com mesmo total — `SUM(PVAR1DAY)` triplicava. Filtro `TREE='Main'` reduz VaR card BALTRA pra valor real (~3× menor). Outros RAW_FUNDS só têm Main → no-op.
- **IGPM exposure — Fase 1 (silent-drop fix)** (`data_fetch.fetch_rf_exposure_map`) — `_RATE_PRIM_BY_CLASS["NTN-C"/"DAC Future"]: "IPCA Coupon" → "IGPM Coupon"`. `keep_mask` aceita `PRIMITIVE_CLASS in ("IPCA","IGPM")`. Override factor: `PRIMITIVE_CLASS='IGPM' → factor='igpm_idx'`. `rate_prims` (sign-flip set) inclui `'IGPM Coupon'`. Antes: NTN-Cs eram silenciosamente dropados do Exposure Map (mismatch entre _RATE_PRIM mapping e PRIMITIVE_CLASS upstream).
- **IGPM — Fase 2 (rendering)** (`risk_config._RF_FACTOR_MAP`, `expo_renderers.{build_idka_exposure_section, build_rf_exposure_map_section}`) — `_RF_FACTOR_MAP["NTN-C"/"DAC Future"] = "real_igpm"` (separado de "real" IPCA). `FACTOR_ORDER`/`FACTOR_LABEL`/`_DUR_FAC`/`_DUR_FACTORS` ganham `real_igpm` + `igpm_idx`. Pivot RF map inclui `real_igpm`. Stat row tem chip "Juros Reais (IGPM)" condicional (só quando |val| > 0.005yr). Position table mostra factor=real_igpm/igpm_idx.
- **`fetch_albatroz_exposure` rewrite** — bug pré-existente de `SUM(DELTA)` sobre todos primitives (spread + face + coupon, sinais inconsistentes → garbage). Fix: WHERE filtra UM rate primitive por PRODUCT_CLASS (NTN-B/DAP→`IPCA Coupon`, NTN-C/DAC→`IGPM Coupon`, DI/NTN-F/LTN→`BRL Rate Curve`). DV01: `DELTA × 0.0001` (era `× MOD_DUR × 0.0001` → squared duration, inflava ~10×). Filtro `MOD_DURATION IS NOT NULL` exclui face values. CRIs/Debentures parked (somam todos primitives — sinal misto).
- **Cobertura IGPM no kit**: BALTRA (NTNC 01/01/2031 book Prev, 51.6M ano_eq, ~1.02yr) + EVOLUTION (via Evo Strategy CI_Macro look-through, 30.7M ano_eq, ~0.11yr). PA `REPORT_ALPHA_ATRIBUTION` já tinha CLASSE='RF BZ IGP-M' separada — PA cards renderizam automaticamente. Para 252d HS / vol regime / replication: tratar IGPM como IPCA proxy (sem vertices upstream).
- **Lista de distribuição daily** (`scripts/send_risk_monitor_email.ps1`) — BCC expandido de 9 → 31 destinatários (lista completa do time).

**Features entregues 2026-04-28 (sexta sessão):**
- **VaR DoD attribution wired into Comments** (`generate_risk_report._dod_top_driver` + `summary_renderers.build_comments_card`) — replace hand-rolled `_top1_var_delta` (cobertura só MACRO/QUANT/EVO) por unified pull de `fetch_var_dod_decomposition`. Cobertura agora full-suite 9 fundos. Bullet 1-line inclui: `metric_lbl ΔX bps · driver: <leaf> (Δy bps) · [pos +A / marg +B] · ⚠ override`. Threshold default 5 bps; IDKAs 2 bps. Decomp pos/marg só renderiza quando fundo publica per-row pos data. Override flag só dispara quando `|ratio + 1| > 0.05` (correção material). Performance: prefetch único de 9 DoD dataframes compartilhado com modal payload (build_vardod_data_payload aceita `prefetched_dfs` kwarg).
- **VaR DoD driver — exclude bench primitive** (`_dod_top_driver`) — IDKA passivo (`PRIMITIVE='IDKA IPCA 3Y'`/`'IDKA IPCA 10Y'`) é mecanicamente o top |Δ| mas comunicativamente errado (não é decisão do gestor — é tracking 100% NAV com override). Filter: `df[df["label"] != bench_primitive]` antes de selecionar driver. Fallback pro bench se for o único mover. Resolved via `_VAR_DOD_DISPATCH[fund_key][3]` (cfg 4º campo).
- **Top Movers Produto modal** (`pmovers_renderers.py` novo + trigger no PA card head + injection em `build_html`) — popup acionado por botão "Top Movers Produto →" no header de cada PA card. Modal 4 colunas (DIA / MTD / YTD / 12M), cada coluna com 5 PIORES (vermelho) + 5 MELHORES (verde). Source: `df_pa` (REPORT_ALPHA_ATRIBUTION dia/mtd/ytd/m12 _bps). Tag compacto por CLASSE (`[RV BZ]`, `[BRLUSD]`, `[FX Carry]`, `[RF IPCA]`, `[ETF Opt]`). ESC/backdrop fecha. Cobertura: 9 fundos (FRONTIER inclusive via GFA key).
- **Pmovers — consolidação de futuros + filtros** (`pmovers_renderers._consolidate_product` + `_FX_HEDGE_LIVROS`) — futuros consolidados por ativo subjacente (regex Brazilian fut pattern: `<prefix>[FGHJKMNQUVXZ]<2dig>`). Exemplos: `WDOK26 + WDOG26 + WDOM26 → WDO*`; `DI1F33 + DI1F28 → DI1*`; `DAPK35 → DAP*`. Non-futures unchanged (ETFs, options, equities). Filter adicional: drop `PRODUCT='Cash USD'` + LIVROs `{Caixa USD, Cash USD, Caixa USD Futures}` (FX hedge collateral, não alpha). Custos/Caixa/Provisions já filtrados.
- **Mirror save F:\Bloomberg\Risk_Manager\Data\Morningcall** (`generate_risk_report.main`) — após salvar HTML em `data/morning-calls/`, escreve segunda cópia em `F:\Bloomberg\Risk_Manager\Data\Morningcall\` (shared distribution location). `mkdir(parents=True, exist_ok=True)`. Falha no mirror loga warning mas não derruba o save principal.

**Features entregues 2026-04-28 (sétima sessão):**
- **Frontier highlights banner — IBOV fallback + duplicate fix** (`fund_renderers._highlights_div`) — Top 3 banner duplicado removido (criado em `390798c`, era redundante com o "Highlights · α vs <bench> hoje" pré-existente, commit `0a4ae44`). Threshold relaxado de `|val| × 10000 > 0.5 bps` pra `|val| > 0`. Quando coluna bench-relativa é toda zero (caso IBOV upstream sem dado), fallback pra `TOTAL_ATRIBUTION_DAY` (absoluto) com label "(α vs IBOV sem dado upstream)".
- **BALTRA Exposure RF — drop mod_dur≈0 noise** (`expo_renderers.build_albatroz_exposure`) — após filtro CDI, adicionado `df = df[df["mod_dur"] > 0.01]` pra remover Equity / IBOVSPFuture / USDBRLFuture / FIDCs (`Funds BR`) / Corn Futures que não têm rate sensitivity. Outros mantém só CRIs (mod_dur 2-4y, parking lot). ALBATROZ unchanged (sem ruído mod_dur=0 no escopo). Closes session_2026_04_28 TODO #2.
- **Distribuição IDKA — reset Forward ao trocar bench** (`generate_risk_report.setDistBench` JS) — ao trocar entre Benchmark/Replication/Comparação tabs, force `card.dataset.activeMode='forward'` + atualiza visual dos `.dist-btn[data-mode]`. Evita landing em backward+empty quando bench-tab nova não tem realized 252d. Closes session_2026_04_28 TODO #3.
- **BALTRA/ALBATROZ Exposure RF — Net (yrs)** (`expo_renderers.yr_cell`) — novo helper exibe `delta_brl/nav` como `±X.XXyr`. Aplicado em ambas tabelas (Indexador + Top 15) pra consistência parent/child. Headers `Net (%NAV)` → `Net (yrs)`. Possível porque `delta_brl` agora é POSITION × MOD_DURATION (rate primitive) — `delta_brl/nav` dá duração em yrs direto. Closes session_2026_04_28 TODO #1.

**Features entregues 2026-04-29:**
- **Vol Regime Summary — MACRO_Q + ALBATROZ** (`summary_renderers._FUND_PORTFOLIO_KEY`) — ampliado de 3 → 5 keys. ALBATROZ usa série HS gross própria; MACRO_Q usa SIST_GLOBAL (sub-book Global do QUANT) como proxy (MACRO_Q não tem HS própria upstream).
- **Briefing — bench line ampliada** — IBOV/CDI/USDBRL/DI1F (3y constante, ~Jan/29). Fetchers novos `fetch_usdbrl_returns` (sign-flip BRL convention: USD/BRL ↑ = vermelho, "fortalecimento BRL" = positivo verde) + `fetch_di1_3y_rate` (target 756 BDAYS via `_di` pattern, rola com a data, mostra rate level + bps change).
- **Breakdown por Fator — IGPM + outros** (`summary_renderers._FACTOR_LIST` + `generate_risk_report.py` factor_matrix loop) — ampliado de 3 → 9 fatores: Juros Reais (IPCA), **Juros Reais (IGPM)** (novo), Juros Nominais, IPCA Idx, **IGPM Idx** (novo), Equity BR/DM/EM, FX, Commodities. `_DV01_SIGN_FLIP` ganha `real_igpm: True, igpm_idx: False`. BALTRA aparece em IGPM (-1.02 yrs).
- **Mudanças Significativas + Peers — pp → %** (`summary_renderers._delta_pp` + card-sub + `generate_risk_report.py` 3 sites JS) — substituído sufixo `pp` por `%` em todas as labels do report.
- **VaR Histórico inline MACRO** (novo card, section-id `risk-monitor`) — chart SVG line ~820×320 px abaixo do Risk Monitor card, mostrando Fund total + 4 PMs (CI/LF/RJ/JD) últimos 121d úteis. Fetcher novo `data_fetch.fetch_macro_pm_var_history` (`LOTE_FUND_STRESS_RPM` LEVEL=10 TREE='Main_Macro_Ativos', |Σ signed VaR per PM prefix| × 10000 / NAV). Helper genérico `svg_renderers.multi_line_chart_svg` (multi-série + gridlines + legenda inline). Paleta brand-aligned (gold + 4 azuis/cyan/roxo). Unidade bps. QM excluído (descontinuado). **Section id == "risk-monitor"** (mesma aba do card pai); IDs novos não em `risk_config.REPORTS` ficam escondidos pelo filtro de aba.
- **EVOLUTION PA — default Por Livro** (`pa_renderers.build_pa_section_hier`) — em vez de Por Classe. Strategy/Livro/Produto é mais informativo pra fundo multi-estratégia. Demais fundos mantêm Por Classe.
- **Exposure VaR — delta D-vs-D-1** (`expo_renderers._build_expo_unified_table` + helper novo `_prev_hs_var_bps` em `generate_risk_report.py`) — novas células Δ tanto em "Total não-diversificado" (acumula `tot_var_abs_d1` no loop de fatores via `d1_var_pct`) quanto "Diversificado HS portfolio" (D-1 via `series_map.iloc[-2]`). Param novo `diversified_var_bps_d1` em `_build_expo_unified_table`, propagado via `build_exposure_section`/`build_quant_exposure_section`/`build_evolution_exposure_section`.

**Bug fixes 2026-04-29:**
- **Daily P&L bps mismatch (RJ +8bp vs fund +25bp)** — bug **DUPLO**:
  1. **Backend** (`fetch_book_pnl`): somava `PL_PCT` (per-position PL/AMOUNT, não somável). Agora calcula tudo como `PL / fund_NAV` em todos os níveis. Adicionado campo `nav` no payload pra debug. Memory: `project_rule_book_pnl_per_position_pct.md`.
  2. **Frontend** (2 cópias com mesmo bug): group header (RJ/JD/CI/LF) calculava `Σ pl_pct / books.length` — média em vez de soma. Fixado em `daily_monitor.html:402` e `generate_risk_report.py:3938`.
- **Daily P&L sempre colapsado ao abrir**: `bpnlLoadState` agora descarta `expanded` no load + `bpnlSaveState` só persiste `order` (drag-drop de books). `bgroups` default mudou de `true` → `false` em ambas cópias. **Restart manual do `pnl_server.py` necessário** após editar `data_fetch.py` — Python não recarrega módulos importados.
- **BALTRA Exposure RF look-through (gap 5.2 → 6.98 yrs vs Prev.xlsx 6.92)** — descoberta crítica: `fetch_albatroz_exposure` filtrava `TRADING_DESK = '{desk}' AND TRADING_DESK_SHARE_SOURCE = '{desk}'`. Removido o `TRADING_DESK = ...` filter. SHARE_SOURCE sozinho captura look-through nativo (BALTRA agora pega IDKA 10Y -1.28y + IDKA 3Y -0.44y + Albatroz cota -0.03y). ALBATROZ não regrediu (não é cotista). Memory: `project_rule_share_source_lookthrough.md`.

**Features entregues 2026-04-30 — Crédito sub-projeto + brand homogenização:**
- **NEW: Crédito standalone kit** (`generate_credit_report.py` + `credit/` package) — 6 fundos: Sea Lion, Iguana, Nazca, Pelican, Dragon, Barbacena. Default mode = **consolidado multi-tab** (`{date}_credito_consolidado.html`); single-fund via `--fund SEA_LION`. 8 cards por fundo: Header (9 KPI tiles) · Sanity Check de Preços · Distribuição (Tipo donut + Setores bar + Rating donut) · Concentração (Top emissores/grupos h-bars + limites breach table) · Alocação (full instrument table c/ Carry Anual + sortable c/ rating-quality rank) · Mercado de Crédito (índices BR + curvas ANBIMA AAA/AA/A) · AUM Histórica · Retorno (heatmap mensal merge c/ Total). Posições = `LOTE45.LOTE_BOOK_OVERVIEW` (NÃO `LOTE_PRODUCT_EXPO` — Sea Lion ausente lá). NAV match dentro de R$ 3 vs `LOTE_TRADING_DESKS_NAV_SHARE`. Validation gold std: `data/_credit_validation_xlsm_sealion_d2.csv` (35/35 instrumentos do xlsm green tab presentes no DB).
- **Schema Credit — DDL e ingest**: 2 tabelas novas no schema `credit` (que já existia c/ `MAPS_DEBENTURES`/`PRICES_DEBENTURES`). `credit.asset_master` (533 rows, PK=`nome_lote45`, vem de `Cadastro Ativos` no xlsm) + `credit.issuer_limits` (937 rows long-format, PK=`(emissor,fund_name)`, vem de `Limites_GrupoEconomico`). Ingest XML-direto do .xlsm (260 MB) via `python -m credit.credit_db_helpers all`. CSV/PNG branding extraído do main risk monitor pra reuso em `credit/_galapagos_logo_b64.txt`.
- **Sanity Check de Preços** (`credit_data.fetch_price_quality_flags` + banner card) — pra todo fundo do credit kit, valida que cada security/derivative tenha PRICE não-nulo em D e D-1. Banner verde se OK, vermelho c/ tabela de detalhes se ⚠. Exempções: PRODUCT_CLASS ∈ `{Funds BR, Cash, Margin, Provisions and Costs}`. **Regra de processo**: ao adicionar novo fundo ao kit, REGISTRAR no check (memory `project_rule_price_quality_check_coverage.md`). Cobertura atual: só 6 fundos de crédito; gap = Frontier + MM/RF/Prev/ETF families.
- **Brand homogenização — Risk Monitor + Crédito** (`generate_risk_report.py` + `generate_risk_report_meeting.py` + `generate_credit_report.py`) — substituído o logo PNG legacy do main risk monitor por **chart-icon SVG inline** (gradient azul + linha clara crescente, blue-to-navy `<linearGradient id="rmGrad">`) + wordmark "Risk Monitor" (Gadugi 22px no dark, navy 24px no light). Adicionado **footer "Powered by Galápagos"** em todos os reports (right-aligned), reaproveitando o PNG Galápagos legacy pra wave logo. Meeting (light) version recolore o footer PNG via `filter: hue-rotate(...)`. Credit consolidated adopt the mesmo brand strip + footer pra parity visual.

**Features entregues 2026-04-30 (segunda sessão) — Drill-down credit + Crédito tab no main report + perf:**
- **Drill-down Alocação refactor** (`generate_credit_report.render_alocacao_card`) — quatro modos de agrupamento via toggle pills: `Por Tipo` · `Por Grupo` (econômico, alfabético) · `Por Subordinação` · `Por Rating`. Parent rows com caret (▼/▶) + count `(n)`. Default-collapsed via param `default_collapsed=True` (children injetados com classe `alc-hidden` em vez de inline display:none — robusto contra interferência do sort). Botões `Expand all` / `Collapse all` operam só na pane ativa. Dragon default = `subordinacao` (FIDC-heavy); BALTRA/EVO Crédito section default = `grupo`.
- **Subordinação tranche detection** (`_tranche_bucket` + `_TRANCHE_PATTERNS`) — regex no PRODUCT name detecta Senior / Mezanino / Junior. FIDCs sem label mas com `subordinacao` numérico → Senior implícito. Tudo mais (Monocota / Classe Única, Debentures, CRIs, NTN-*) → bucket único `Sem Subordinação`. Coluna `Subord.` na tabela renderiza tag colorido (Senior=verde, Mez=amber, Jr=red, Sem-Sub="—" cinza). Donut "PL por Subordinação" condicional (renderiza só quando há signal não-N/A) com palette alinhado.
- **Soberano bucket** acima de AAA — `_effective_rating` mapeia `grupo_economico=='Tesouro Nacional'` ou `tipo_ativo IN (NTN-B, NTN-C, NTN-F, LFT, LTN)` → `Soberano`. `RATING_ORDER["Soberano"]=0` (sorta acima de AAA=1). Tag style navy fundo + white text + light-blue border. Pinned na 1ª slot do donut Rating pra sempre pegar a cor navy do palette.
- **"Cruz" issuer override** (`credit_data.normalize_issuer_overrides`) — qualquer produto com "Cruz" no nome → emissor/grupo/nome_emissor = "Santa Cruz". Aplicado em `fetch_positions_snapshot` + `data_fetch.fetch_fund_credit_positions` no read time pra não tocar no asset_master. Rolls up Santa Cruz tranches corretamente em concentração.
- **Crédito tab no main risk monitor** (`risk_config.REPORTS` + `credit_card_renderers.build_credit_section`) — nova aba `("credit", "Crédito")` posicionada acima de Peers. Renderiza pra BALTRA + EVOLUTION via look-through `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE`. Conteúdo: 4 header tiles (Crédito Look-through Total · Soberano · Corp Credit · Carry médio corp) + Distribuição (donuts em **2×2 grid**: Tipo, Setores, Rating, Subordinação — denom_nav=True, exclude_sovereign=True; Subordinação usa % crédito no label e % NAV no tooltip) + Alocação (drill-down com defaults `mode="grupo"`, `default_collapsed=True`, `mm_mode=True` → Posição em R$ MM com 2 decimals).
- **Filtros credit-relevantes** (`build_credit_section`) — drop LFT / NTN-C / NTN-F do BALTRA/EVO Crédito section (cash-equivalents / sovereign minor); NTN-B mantém (real-rate/inflation exposure). `Funds BR` filtrado pra só FIDCs (drop vanilla bond funds via tipo_ativo check). Normalização NaN→"" via `astype(object).where(notna, "")` em colunas string-bearing.
- **Performance — sort delegation** (`generate_risk_report.attachUniversalSort` rewrite) — substituído per-table walk + addEventListener × 200 tabelas × 10 ths (~2000 listeners) por **single document-level click+mouseover delegation**. `_initSortableTable` lazy-attacha indicators + dataset.usortColIdx só quando user hover/click pela 1ª vez na tabela. Estado de sort persistido via dataset (sortAttached, usortLast, usortAsc).
- **Performance — lazy hydration** (`build_html` template wrap + `_hydrateFund` JS) — todas as 84 seções `(fund, report)` renderizadas dentro de `<template class="tpl-section">` em vez de `<div class="section-wrap">`. Templates não vão pro DOM vivo até `_hydrateSection` clonar via `tpl.content.cloneNode(true)`. `applyState` chama `_hydrateFund(sel)` em fund mode e `_hydrateReport(sel)` em report mode antes de toggle visibility. **Idle pre-warm** via `requestIdleCallback` hidrata todas as seções no background depois do first paint, então tab switches subsequentes são instantâneos. Re-runs `initAlcSort`/`attachVrCaretToggle`/`injectCsvButtons`/`highlightFundNames` pós-hydrate.
- **`fetch_fund_credit_positions`** (`data_fetch.py`) — nova fetcher pulling credit-relevant rows via `LOTE_PRODUCT_EXPO.TRADING_DESK_SHARE_SOURCE = '<desk>'` joined to `credit.asset_master`. DISTINCT ON (PRODUCT, BOOK) deduplica primitives. WHERE filtra credit `PRODUCT_CLASS` set (Debenture, CRI, CRA, FIDC, FIDC NP, NTN-*, LFT, LTN, Funds BR, Nota Comercial). `COALESCE(am.tipo_ativo, e.PRODUCT_CLASS)` no SELECT pra fallback. Aplicado `normalize_issuer_overrides` no return.

**Features entregues 2026-05-01 — VaR Histórico hydration fix + Nazca wired (hidden) + hygiene + UX:**
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

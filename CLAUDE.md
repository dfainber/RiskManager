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
| MACRO_Q    | ✅ LOTE_FUND_STRESS | ✅       | —                 | —               | —               |
| FRONTIER   | ✅ BVaR HS          | ✅ (GFA) | ✅ active wt      | ✅ α vs IBOV   | —               |
| IDKA 3Y    | ✅ BVaR param       | ✅       | ✅ 3-vias toggle  | ✅ HS active    | —               |
| IDKA 10Y   | ✅ BVaR param       | ✅       | ✅ 3-vias toggle  | ✅ HS active    | —               |

Limites provisórios: ALBATROZ, MACRO_Q, IDKA 3Y (soft 0.40/hard 0.60 daily),
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
- VaR commentary nos Comments confirmado implementado (commit `6f463bb`) — dispara quando |ΔVaR| ≥ 5 bps

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

---

## 9. Gaps deliberados

Fora de escopo até decisão explícita:

- Fundos **BALTRA**
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

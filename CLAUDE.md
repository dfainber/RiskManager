 CLAUDE.md — Risk Monitor (GLPG)

Este arquivo é carregado automaticamente em toda sessão do Claude Code dentro
deste repositório. Ele é a **fonte única de verdade** sobre o que o projeto é,
onde estamos, e quais regras devem ser seguidas.

---

## 1. Propósito

Monitorar o risco de todos os fundos da casa (famílias: **Multimercados, Renda
Fixa, Crédito, Renda Variável**) em base diária, para alimentar o **Morning
Call** com uma visão consolidada de:

- Evolução do risco em **VaR** e **Stress**
- Utilização de risco **ex-post** (orçamento de perda / PnL budget)
- Aderência de cada fundo ao seu **mandato** e particularidades

O produto final de cada rodada é um briefing curto, acionável, que leve a uma
decisão do gestor. Se não leva a decisão, o briefing está errado.

---

## 2. Arquitetura de skills

```
.claude/skills/
├── risk-monitor/                       # skill-mãe (orquestra + carrega mandatos)
│   ├── SKILL.md
│   ├── fundos-canonicos.json           # mapa autoritativo de nomes
│   └── mandatos/                       # um JSON por fundo com limites
├── macro-stop-monitor/                 # stops por PM (família Macro)
├── macro-risk-breakdown/               # decomposição de risco MACRO
├── evolution-risk-concentration/       # concentração no fundo Evolution
├── rf-idka-monitor/                    # IDKAs benchmarked (Renda Fixa)
├── performance-attribution/            # PA transversal às famílias
├── risk-morning-call/                  # briefing final consolidado
└── wrap-session/                       # checklist de fim de dia (memory + CLAUDE.md + commit + push)
```

**Regra de ouro:** as sobreposições (ex.: `macro-stop-monitor` vs.
`macro-risk-breakdown`; PA vs. risk-breakdown) são **complementares, não
redundantes**. Cada skill dá uma visão distinta dos mesmos fundos. Não
consolidar à toa.

---

## 3. Fontes de dados (GLPG)

Todo acesso a dados passa pela skill **`glpg-data-fetch`** (já existente). Não
escrever queries ad-hoc espalhadas pelas skills — sempre chamar a camada de
acesso.

Schemas e tabelas principais:

| Schema      | Tabela                            | Uso                          |
|-------------|-----------------------------------|------------------------------|
| `LOTE45`    | `LOTE_TRADING_DESKS_NAV_SHARE`    | NAV share por mesa/fundo     |
| `LOTE45`    | `LOTE_FUND_STRESS_RPM`            | VaR/Stress nível fundo (LEVEL=10) e série histórica — MACRO/QUANT/EVOLUTION |
| `LOTE45`    | `LOTE_FUND_STRESS`                | VaR/Stress nível produto — ALBATROZ + MACRO_Q (SUM por TRADING_DESK) |
| `LOTE45`    | `LOTE_PARAMETRIC_VAR_TABLE`       | BVaR (`RELATIVE_VAR_PCT`) + VaR (`ABSOLUTE_VAR_PCT`) para IDKAs; fração decimal |
| `LOTE45`    | `LOTE_BOOK_STRESS_RPM`            | VaR por book/RF (LEVEL=3), fonte para MACRO via Evolution |
| `LOTE45`    | `LOTE_PRODUCT_EXPO`               | Exposição/delta por produto — usar `TRADING_DESK_SHARE_SOURCE` |
| `q_models`  | `REPORT_ALPHA_ATRIBUTION`         | PnL por PM (LIVRO) e por instrumento |
| `q_models`  | `STANDARD_DEVIATION_ASSETS`       | σ por instrumento, BOOK='MACRO' |
| `q_models`  | `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` | Drawdowns simulados, retornos históricos |

**O generator (`generate_risk_report.py`) é 100% DB-sourced** — não lê
Excel, CSV, JSON ou qualquer arquivo de dados em runtime (verificado
2026-04-19 via grep — zero `read_excel`/`open()`/`read_csv`/`json.load`).

Arquivos externos (Access local, xlsx oficial da Controle, `RELATORIO_POSICOES_*.xlsx`)
aparecem apenas como **referência de validação** durante desenvolvimento
(comparar números do kit com a fonte oficial), não como input. Mandatos
(limites por fundo) estão **hardcoded** nos dicts `FUNDS`/`RAW_FUNDS`/
`IDKA_FUNDS` no topo do código, não lidos de JSONs em runtime.

---

## 4. Mapa canônico de fundos

O arquivo `risk-monitor/fundos-canonicos.json` é a única referência válida para
nomes de fundos e mesas. Regras:

- Antes de filtrar por `TRADING_DESK` ou `FUNDO` em qualquer query, **consultar
  o JSON canônico**.
- Se o nome no banco mudar, atualizar o JSON — nunca hardcode em skill.
- **Correção histórica importante:** onde antes se usava `SISTEMATICO` o nome
  real no banco é **`QUANT`**. Nunca mais escrever `SISTEMATICO` em query.

---

## 5. Onde estamos (fase atual)

| Fase | Duração     | Descrição                                              | Status     |
|------|-------------|--------------------------------------------------------|------------|
| 0    | 1–2 dias    | instalar skills + criar pastas + cadastrar mandatos    | ✅ concluída 2026-04-17 |
| 1    | ½ dia       | auditoria de nomes (4 queries SQL)                     | ✅ concluída 2026-04-17 |
| 2    | 1 semana    | 1ª execução real, validar contra dashboards existentes | ✅ concluída 2026-04-17 |
| 3    | 2–4 semanas | calibrar thresholds com uso real + validar EVOLUTION/QUANT | ✅ 2026-04-18 |
| 4    | 1–3 meses   | UX refactor (tabs, modal, CSV, sort), analytics extras | **em andamento** (2026-04-18) |
| 5    | 3–6+ meses  | state-of-the-art: análise temporal, alertas proativos  | pendente   |

**Fase 0 — entregues:**
- 9 skills instaladas em `.claude/skills/`
- Pastas de dados criadas em `data/` (mandatos, snapshots, morning-calls, macro-stops)
- Mandatos criados: MACRO, QUANT, EVOLUTION, MACRO_Q
- Mandatos pendentes: ALBATROZ, IDKA_3Y, IDKA_10Y (aguardam definição de BVaR)

**Fase 1 — entregues:**
- 4 queries SQL rodadas contra GLPG-DB01 em 2026-04-17
- `fundos-canonicos.json` atualizado com nomes reais (mixed case, não all-caps)
- TREE confirmada como `Main_Macro_Ativos` (era `Main_Macro_Gestores` no design doc — correção crítica)
- PA keys confirmadas; MACRO_Q usa `GLOBAL` como chave PA
- Armadilhas documentadas no JSON canônico

**Fase 2 — entregues (concluída 2026-04-17):**
- `glpg_fetch.py` criado em `Risk_Monitor/` (conexão GLPG-DB01 via `.env`)
- `macro-stop-monitor` executado com dados reais — carrego validado, stops gravados
- `generate_risk_report.py` operacional — HTML diário em `data/morning-calls/`
- Seções: Risk Monitor (VaR/Stress 3 fundos), Risk Budget Monitor (stop por PM), Exposição MACRO
- Exposição MACRO validada: POSIÇÕES (por RF factor) e PM VaR (por gestor), com drill-down
- Colunas: %NAV, σ, ΔExpo, VaR%, ΔVaR, Margem, DIA — sort em todos os níveis
- Fonte de exposição confirmada: `LOTE_PRODUCT_EXPO` com `TRADING_DESK_SHARE_SOURCE`
- σ por instrumento: `q_models.STANDARD_DEVIATION_ASSETS`, BOOK='MACRO'
- DIA por instrumento: `q_models.REPORT_ALPHA_ATRIBUTION` group by LIVRO+PRODUCT
- Date picker com lógica BRT (antes 21h → ontem; depois → hoje com aviso)

**Próxima ação concreta (Fase 3):** usar o relatório diariamente, identificar thresholds
a calibrar (alertas de 80° pct, escala das barras), validar QUANT e EVOLUTION com os
dashboards originais.

**Fase 3 — entregues (concluída 2026-04-18):**
- QUANT validado contra `RELATORIO_POSICOES_2026-04-16.xlsx` — VaR 0.79% bate com DB; stress mapeado para `MACRO_STRESS` (cenário "Quant e Macro V"), gap sistemático de ~1.3 pp vs Asset_RiscoMercado documentado no mandato
- EVOLUTION validado — VaR 0.45% bate com Excel, **shift D-1 era bug** (era contábil, não de risco — engine faz look-through), removido para todos os fundos
- Regra "D-1 contábil só para estruturados fora do lote" salva no memory
- Calibrações de stress documentadas em `_calibracao_stress` nos 3 mandatos

**Fase 4 — entregues até 2026-04-18:**
- Navegação 3-modos (Summary / Por Fundo / Por Report) via URL hash, section registry
- Single-Name inline (QUANT + EVOLUTION com look-through, Bracco/Quant_PA/FMN/FCO/FLO/Frontier/Macro CI_COMMODITIES), com **BOVA11** exploded via lista IBOV, **SMAL11** via SMLLBV, **ADR/ADR Options** mapeados via `PRIMITIVE_NAME` regex `^[A-Z]{4}[0-9]{1,2}$` (exclui BABA→'9988 HK'), Gross absoluto no header, coluna "From Idx" agregando WIN+BOVA+SMAL
- Distribuição 252d com toggle Backward/Forward
- Brand azul Galapagos — logo tortoise, Gadugi→Inter, JetBrains Mono
- CSS variables, cards unificados, regras universais (sort + CSV em todo card)
- **Fund switcher por report** generalizado (.report-fund-switcher) — Por Report > X com ≥2 fundos
- **Performance Attribution hierárquico** (`fetch_pa_leaves` + `build_pa_section_hier`):
  - Toggle **Por Classe** (CLASSE→PRODUCT) / **Por Livro** (LIVRO→PRODUCT). EVO Livro tem 3 níveis: Strategy (Macro/Quant/Equities/Crédito) → Livro → PRODUCT
  - Renames: Macro_JD→JD, Macro_LF→LF, Macro_RJ→RJ, Macro_MD→MD, Macro_QM→QM, Macro_AC/DF/FG idem
  - Ordem default fixa inspirada no Excel PA (`_PA_ORDER_CLASSE`, `_PA_ORDER_LIVRO`, `_PA_ORDER_STRATEGY`) com tiebreak |YTD| desc
  - Pinned-bottom sem sort: Caixa, Caixa USD, Taxas e Custos, Custos
  - Sort clicável nas 4 colunas DIA/MTD/YTD/12M preservando hierarquia
  - Footer 3-linhas: Total Alpha · Benchmark (CDI) · Retorno Nominal (soma)
  - Tudo em % com 2 decimais (era bps)
  - **Heatmap** nas células de métrica (alpha 0.14, sutil)
  - **Lazy render** + **Expand/Collapse All** + sort que preserva pinned
  - HTML 4 MB → 0.9 MB (77% menor)
- **ALBATROZ onboarded** (5º fundo):
  - PA completo (via `FUNDO='ALBATROZ'` em `REPORT_ALPHA_ATRIBUTION`)
  - **Exposure RF** em `build_albatroz_exposure` — resumo por indexador (Pré / IPCA / IGP-M / CDI / Outros) + top 15 posições por |DV01|. DV01 ≈ DELTA × MOD_DURATION × 0.0001
  - **Risk Budget** 150 bps/mês sem carry (`build_albatroz_risk_budget`)
  - Confirmado: ALBATROZ **não tem VaR/Stress** em `LOTE_FUND_STRESS_RPM` — bloqueado até descobrir fonte
- **Summary page** completa (substituiu o placeholder):
  - Status consolidado (grid 5 fundos × DIA/MTD/YTD/12M + VaR util + Stop util + Δ VaR D-1)
  - Alerts (movido do rodapé para Summary)
  - Comments — Outliers do dia (|z| ≥ 2σ vs 90d + |contrib| ≥ 3 bps)
  - Top Movers — DIA com toggle Livro/Classe (Caixa/Custos/Taxas excluídos)
  - Mudanças Significativas D-0 vs D-1 — MACRO em PM×fator, outros em PRODUCT_CLASS → fator agregado (via `fetch_fund_position_changes`)
- **Novo report "Análise"** por fundo (primeiro tab) — replica Outliers + Top Movers + Mudanças Significativas focado em um fundo só
- **Ordem de reports finalizada:** Análise → PA → Risk Monitor → Exposure → Single-Name → Distribuição 252d → Risk Budget
- **PDF export browser-native** — dois botões no header: "⇣ PDF (aba)" imprime a aba atual · "⇣ PDF (completo)" mostra todas seções. `@media print` remapeia CSS vars para paleta clara (fundo branco, verde #0e7a32, vermelho #a8001a), A4 landscape, page-breaks em cards
- **Performance:**
  - 16 fetches paralelos via `ThreadPoolExecutor(max_workers=12)` — total ~5.6s (fetches em ~3.3s)
  - Dust filter em `fetch_pa_leaves` (drop leaves todos zero)
  - Connection reuse por thread em `glpg_fetch.py` (`threading.local`)

**Fase 4 — fixes pós entrega (2026-04-18):**
- `_latest_nav(desk, date_str)` helper + `merge_asof(backward)` em `build_series` — NAV da tabela LOTE_TRADING_DESKS_NAV_SHARE defasa ~1 business day vs. VaR/Expo. Antes: Status consolidado vinha sem VaR no "dia" mais recente e Mudanças Significativas explodia para bilhões de %. Ver `project_rule_nav_lag.md`.
- `compute_pa_outliers` ganha cláusula **OR absoluta**: flaga se `(|z|≥2σ AND |bps|≥3)` OR `|bps|≥10`. Captura losses materiais em nomes historicamente voláteis (AXIA, etc.) onde σ é grande demais para o z-test disparar sozinho. Ver `project_rule_outlier_or_absolute.md`.
- `run_report.bat` — script Windows que pergunta a data e roda o generator (default = hoje no formato YYYY-MM-DD).

**Fase 4 — entregues (sessão pós 2026-04-18):**
- **ALBATROZ + MACRO_Q VaR/Stress** — fonte real descoberta: `LOTE45.LOTE_FUND_STRESS` (product-level, SUM por TRADING_DESK). `PVAR1DAY` = VaR, `SPECIFIC_STRESS` e `MACRO_STRESS` = stress. Ambos agora exibindo no Risk Monitor. Limites provisórios.
- **Frontier Long Only** — 6º fundo (`FRONTIER`) adicionado. Report `frontier-lo` com tabela de posições completa (17 ações), métricas NAV/Gross/Beta pond., atribuição DIA/MTD/YTD vs IBOD e IBOV. Fonte: `frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD`.
- **Frontier Exposure** — report `exposure` para FRONTIER com active weight vs IBOV/IBOD:
  - Toggle Benchmark (IBOV/IBOD) — muda peso bench, active weight E colunas ER simultaneamente via data attributes
  - Toggle Vista (Por Nome / Por Setor) — Por Setor: colapsável por setor, weighted beta por setor, Σ ER D/MTD/YTD por setor, ▼/▶ All
  - Cash allocation exibido no header (ex.: 10.92% caixa em 2026-04-16)
  - TE aproximado via β (provisório — a ser substituído por TE ex-post real)
- **sortTableByCol patcheado globalmente** — respeita `data-pinned="1"` em qualquer tabela. TOTAL e Caixa não se movem no sort.
- Novas fontes canônicas: `public.EQUITIES_COMPOSITION` (pesos IBOV/SMLLBV), `q_models.FRONTIER_TARGETS` + `q_models.COMPANY_SECTORS` (setor por ação).

**Fase 4 — entregues (sessão 2026-04-19):**
- **Vol Regime** — carteira atual × janela HS (`PORTIFOLIO_DAILY_HISTORICAL_SIMULATION.W`):
  - Card no Summary (1 linha por fundo: MACRO, QUANT via `SIST`, EVOLUTION)
  - Report dedicado "Vol Regime" per-fund com drag-down ▶/▼ (default só fund-level; click expande books/PMs — MACRO 4 PMs; QUANT sub-books + Bracco + Quant_PA; EVOLUTION só fund)
  - Métricas: `vol_recent_pct = std(W[-21:]) × √252`, `vol_full_pct = std(W) × √252`, `ratio`, `z-score`, `pct_rank`, `regime` (low/normal/elevated/stressed)
  - Visual **range line SVG** (min→max com dot destacado por tercil, tick cinza na mediana) para vol e z
  - Primary = **pct_rank** (não-paramétrico, imune ao overlap). z-score só informativo (N_ef ~36 em 756d, ~12 em 252d — SE ~15%)
  - Bug resolvido: abordagem inicial usava NAV pct_change mas `NAV_SHARE` inclui flows e dá valores absurdos. Pivot para W-series validou
- **IDKA 3Y + IDKA 10Y onboarded** como 7º e 8º fundos:
  - Dict `IDKA_FUNDS` com `primary: "bvar"`. Limites provisórios: IDKA_3Y soft 1.75%/hard 2.50%; IDKA_10Y soft 3.50%/hard 5.00%
  - Fonte: `LOTE45.LOTE_PARAMETRIC_VAR_TABLE` — `RELATIVE_VAR_PCT` (BVaR, primary), `ABSOLUTE_VAR_PCT` (VaR, reference-only, no limit). Valores em fração decimal
  - `fetch_risk_history_idka()` + `build_series(..., df_risk_idka=...)` — BVaR vai no slot `var_pct`, VaR no slot `stress_pct`
  - Risk Monitor card parametrizado: labels "BVaR 95%" + "VaR 95% (ref)" em vez de "VaR 95% 1d"/"Stress" quando `cfg["primary"] == "bvar"`. Linha VaR sem bar de range nem util %
  - PA: `fetch_pa_leaves` inclui `IDKAIPCAY3` e `IDKAIPCAY10` no filtro. PA keys mapeadas em `_FUND_PA_KEY`
  - Summary grid agora cobre 8 fundos
- **UX navigation polish**:
  - Fund names destacados nos card-subs — classe CSS `.fund-name` (accent azul, chip-style com border + background sutil), JS `highlightFundNames()` wrap automático
  - Fund nav chips — barra `.fund-nav-chips` ("Ir para: Macro | Quantitativo | …") injetada via JS no topo de cada `section-wrap[data-fund]`. Chip ativo destacado, click chama `selectFund()`

**Fase 4 — entregues (sessão 2026-04-19, tarde — cross-fund consolidated views):**
- **Frontier Summary — alpha vs IBOV**: `TOTAL_IBVSP_*` (ER vs IBOV) em vez de `TOTAL_ATRIBUTION_*` (retorno bruto). Apples-to-apples com alpha vs CDI dos outros fundos
- **IBOV + CDI benchmark rows** no rodapé do Status consolidado — `fetch_ibov_returns()` (3y de `EQUITIES_PRICES.CLOSE` para IBOV) + `fetch_cdi_returns()` (ECO_INDEX)
- **Frontier HS BVaR vs IBOV** — `compute_frontier_bvar_hs(df, date)`: pesos atuais × 3y daily retornos − IBOV, 5° pct. Clip |r|>30% p/ corporate actions. Substitui o 2.05% abs por 0.85% BVaR no Summary. Parametric cross-check: 1.645×std(ER) = 0.84%
- **Exposure Map** (IDKA 3Y, IDKA 10Y, Albatroz) em novo report tab `exposure-map`:
  - Bars por bucket (12: 0-6m, 6-12m, 1-2y, 2-3y, 3-4y, ..., 9-10y, 10y+) × fator (Real/Nominal/Bench flush)
  - Toggle Absoluto/Relativo + filtro Ambos/Real/Nominal; bench sempre visível (barra slate)
  - Tabela por bucket + tabela por ativo (ambas collapsíveis)
  - `fetch_rf_exposure_map(desk, date)` com Albatroz look-through explodido (`TRADING_DESK = ALBATROZ` + `TRADING_DESK_SHARE_SOURCE = desk`)
  - Sign convention: `-DELTA` em IPCA Coupon / BRL Rate Curve (recupera exposição de posição), resto = DELTA
  - Unidades normalizadas em anos (não % × 100); labels `+X.Xy` no y-axis
- **Risco Agregado (Main Aggregated Risk)** card no Summary — 8 fundos × NAV / VaR abs (% + R$) / BVaR rel (% + R$) / top-5 🔺 abs + 🔷 rel
- **Breakdown por Fator** card no Summary — matriz fator × fundo (7 fatores: Real/Nominal/IPCA Idx/Equity BR/DM/EM/FX/Commodities)
  - Toggle Bruto / Líquido (default Líquido — abate bench das IDKAs 3y/10y × NAV, Frontier 100% IBOV, CDI funds bench=0)
  - Inclui MACRO equity/FX/commodities/rates do `df_expo`, QUANT + EVOLUTION-direct equity single-names
- **Top Posições — consolidado** (2º da direita-pra-esquerda no Summary) — drill-down Fator → Instrumento → Fundo(s):
  - Level 0: fator header (click expande)
  - Level 1: top 5 instrumentos por fator (click expande)
  - Level 2: cada fundo holder e % do total desse instrumento
  - Toggle Bruto/Líquido (scale por (1 − bench/total_fundo_fator) no Líquido)
  - Evolution-direct (`fetch_evolution_direct_single_names`) para evitar double-count com QUANT/Frontier
  - via_albatroz excluído (contado em ALBATROZ direto)
- **PA Contribuições toggle Por Tamanho / Por Fundo** — grid preserva layout side-by-side em ambos modos (fix: CSS class em vez de inline display)
- **Distribuição 252d default Forward** — toggle Backward continua acessível
- **IDKA HS BVaR stub** (`compute_idka_bvar_hs`) — realized ER via NAV_SHARE.SHARE × IDKA index; não wired ainda (pendente diff vs calculadora Option B)

**Fase 4 — entregues (sessão 2026-04-19 noite → 2026-04-20):**
- **Distribuição 252d — QUANT onboarded** (4º fundo com o card). Entries em `_DIST_PORTFOLIOS`: `SIST` (fund) + 6 sub-books (SIST_RF/FX/COMMO/GLOBAL/Bracco/Quant_PA) com `kind='livro'`. `fetch_pnl_actual_by_cut` expandido pra incluir FUNDO='QUANT' no WHERE de LIVRO — sub-books batem 1:1 como LIVRO em `REPORT_ALPHA_ATRIBUTION`, habilitando Backward completo. Também adicionei `QM` como PM do MACRO (faltava).
- **Risk budget alert no briefing** — VaR (ou BVaR) > 1.5× orçamento MTD remanescente dispara 🚨 headline + bullet vermelho no `_build_fund_mini_briefing`. MACRO usa `sum(max(0, m) for m in pm_margem)`; ALBATROZ usa `max(0, 150 + min(0, mtd_bps))`. Outros fundos sem risk budget explícito ficam silenciosos. Alerta mostra VaR · BVaR separados se os dois existem (IDKAs).
- **IBOV removido da tabela PA dos IDKAs** (bloco Referência) — user pediu; mantidos Retorno Absoluto, IDKA index, CDI.
- **Top Posições — consolidado** (já em prod) **cobre cross-fund overlap instrument-level** — falta só agregação por emissor (VALE3+VALE5+ADR+opções → "Vale"), parkeado.
- **EVOLUTION Risk Concentration — standalone MVP** ([evolution_diversification_card.py](evolution_diversification_card.py)): 3 camadas (utilização por estratégia · diversification benefit · correlação 21d/63d entre PnLs direcionais MACRO/SIST/FRONTIER/CREDITO). Template sozinho em `data/morning-calls/evolution_diversification_<DATA>.html` — ainda não wired no relatório principal. **Tratamento do CREDITO:** o VaR é **winsorizado causalmente** (rolling 63d, mediana ± 3 × 1.4826·MAD, tail superior apenas) para absorver o spike de cotas júnior de dez/2025 sem dropar observações. Ratio principal usa Σ winsorizada; raw continua visível como referência. Share do CREDITO no Σ exibido como semáforo (>40% vermelho, 25-40% amarelo). Substitui o `Ratio_ex_credito` do spec da skill, que era inviável (subtrair `VaR_CREDITO` linearmente de `VaR_real` é matematicamente errado — VaR não é aditivo). Detalhes completos em [docs/CREDITO_TREATMENT.md](docs/CREDITO_TREATMENT.md).

**Fase 4 — entregues (sessão 2026-04-20):**
- **Frontier PA via GFA key** — descoberta a chave `'GFA'` em `REPORT_ALPHA_ATRIBUTION` para Frontier. Wired em `_FUND_PA_KEY`, `fetch_pa_leaves` e `fetch_pa_daily_per_product`. Report PA full para FRONTIER.
- **Frontier Long Only report refatorado** — bench toggle (IBOV/IBOD/CDI), vista toggle (Por Nome / Por Setor com collapse), linha Alienadas (ações vendidas), sub-tab PA hierárquica nested (`.fpa-pa-nested`).
- **Frontier Distribuição 252d (α vs IBOV realized)** — `fetch_frontier_alpha_series` (SUM(TOTAL_IBVSP_DAY) por VAL_DATE). Backward + Forward views.
- **RF Exposure Map para MACRO e EVOLUTION** — `fetch_rf_exposure_map` ganhou param `lookthrough_only` (EVOLUTION=True p/ ver só via-children). Entries em `_RF_MAP_CFG`. 3 fundos não-RF agora no report.
- **ALBATROZ Exposure com drill-down por indexador** — parent row (Pré/IPCA/IGP-M/CDI/Outros) clicável, expande filhos ordenados por |DV01|. Botões ▼ All / ▶ All. `window.albToggleIdx`.
- **Status consolidado (Summary) — limpeza:**
  - Coluna "Util Stop" removida (stop budget vive no Risk Budget card de cada fundo)
  - Bench rows (IBOV, CDI) migradas de `data-no-sort="1"` para `data-pinned="1"` — sort preserva no rodapé
  - Adicionadas bench rows **IDKA 3A** e **IDKA 10A** (vindas de `idka_idx_ret`)
- **Risco VaR e BVaR por fundo (Summary)** — colunas R$ removidas; rank icons (🔺/🔷) movidos pra células %. Legenda ajustada.
- **Breakdown por Fator — conversão pra %NAV:**
  - Cada célula = `v_brl / nav_fundo × 100` (não mais BRL nominal)
  - Total = `Σ v_brl / house_nav_tot × 100`
  - QUANT non-equity factors populados: Juros Nominais (filtrado `BRL Rate Curve`), FX, Commodities a partir de `df_quant_expo`
  - Card-sub + legenda atualizadas
- **MACRO Juros Nominais — fix de double-count (duration²):**
  - Era: `delta_dur = DELTA × MOD_DURATION` no SQL — mas DELTA já = POSITION × MOD_DURATION
  - Virou: filtro `PRIMITIVE_CLASS='BRL Rate Curve'` + `sum(delta)` direto
  - MACRO nominal foi de -2.14 yr (errado) → -1.21 yr (correto) — ver `project_rule_delta_is_duration_weighted.md`
- **Convenção DV01 padronizada — tomado/dado** (briefings + factor_matrix):
  - `tomado = DV01 > 0` (short bond, ganha com alta) · vermelho
  - `dado = DV01 < 0` (long bond, NTN-B comprado) · verde
  - `_DV01_SIGN_FLIP = {"real": True, "nominal": True, "ipca_idx": False}` ao popular factor_matrix de `rf_expo_maps.ano_eq_brl` (negação cancela o flip de gráfico)
  - Footnote visível no card de briefing (per-fund + executive) com a convenção
  - Ver `project_rule_dv01_sign_convention.md`
- **Δ Expo — posição nova tratada como D-1=0:**
  - Bug: `p_d1 = d1_prod.get(key)` retornava None p/ produto novo → `p_dexp = None` → "—" na célula → filhos não somavam ao factor total
  - Fix: se `df_d1` existe globalmente, `p_dexp = p_net - (p_d1 or 0.0)`. Mesma lógica pra Δ VaR.
  - Ver `project_rule_delta_expo_new_position.md`
- **UX polish:**
  - `.kc { text-transform:none !important; }` — wrapping VaR/BVaR em `<span class="kc">` para não virar "VAR" em headers com `text-transform:uppercase`
  - `fund-nav-chips` escondido em "Por Fundo" mode (`body:not([data-mode="report"]) .fund-nav-chips { display:none; }`) — era redundante/confuso fora do report mode
  - `fund_shorts_js` agora inclui `FUND_LABELS.values()` (Macro/Quantitativo/Frontier) + `_EXTRA_FUND_TERMS = ["Evo Strategy", "Evolution FIC", "Evo"]` → highlight cobre labels mixed-case também
  - Briefing de Frontier: equity allocation (gross %) + weighted beta no bullet principal, em vez do "10% short" enganoso
- **Infra setup:**
  - venv em `C:\Users\diego.fainberg\.venvs\risk_monitor\` (Anaconda3 Python 3.11.7)
  - `run_report.bat` aponta pra esse venv
  - `requirements.txt` criado via `pip freeze`
  - `.gitignore` inclui `.venv/` e `venv/`

**Fase 4 — entregues (sessão 2026-04-21):**
- **EVOLUTION Exposure** — novo card com look-through completo, toggle Vista 1 (Strategy → LIVRO → Instrumento, 3 níveis) / Vista 2 (Por Fator reutilizando `_build_expo_unified_table` com taxonomia RF). Classificação via `livros-map.json` + `_EVO_LIVRO_EXTRA_STRATEGY` (Cred_ON, FMN_*, AÇÕES BR LONG, GP_Cred_*, CAIXA USD, Taxas, etc.). Fonte: `LOTE_PRODUCT_EXPO` com `TRADING_DESK_SHARE_SOURCE='Galapagos Evolution FIC FIM CP'`. Sort cascateado entre 3 níveis via `evoExpoSort`.
- **Camada 3 — filtro de significância** — par corr só flagado como "alinhamento relevante" se ambas estratégias ≥ P70 na Camada 1. Pares com corr ≥ P85 mas com uma estratégia ociosa mostram 🟡 "sinal desconsiderado". Estado sem sinal: ✓ verde.
- **Matriz Direcional (EVOLUTION)** — nova camada via `q_models.RISK_DIRECTION_REPORT`. Para cada CATEGORIA aggrega `DELTA_SISTEMATICO × DELTA_DISCRICIONARIO` (usando `Net` quando existe, senão soma non-Gross). Flagra categorias com mesmo sinal com filtros `|delta| ≥ 5 bps` per perna e `|PCT_PL_TOTAL| ≥ 1%` (material).
- **Camada 4 — Bull Market Alignment** — alerta agregado das 5 condições (≥3 buckets ≥ P70 · ≥1 ≥ P95 · Ratio C2 ≥ P80 · ≥1 par corr ≥ P85 filtrado · ≥3 categorias same-sign). Buckets direcionais = {MACRO, SIST, FRONTIER+EVO_STRAT unidos, CREDITO}. FRONTIER+EVO percentile recomputado da soma das séries. Dispara 🚨 quando ≥ 3 acesas, 🟡 parcial quando 1-2, ✓ verde quando 0. Headline no topo do tab Diversificação + Summary.
- **docs/EVOLUTION_DIVERSIFICATION_METHODOLOGY.md** — doc completo (motivação, 4 camadas + Camada 4, thresholds, tratamento CREDITO, caveats, changelog).
- **MACRO Budget vs VaR por PM** — novo card no Risk Budget tab, consolidado no mesmo `sec-MACRO-stop-monitor`. Colunas:
  - **Margem atual** (91/63/124/123 bps — dinâmica por PM, igual Risk Budget Monitor)
  - **VaR paramétrico** (Lote `PARAMETRIC_VAR` LEVEL=10 soma signed por prefixo de PM, magnitude)
  - **VaR hist 21/63/252d** (`1.645 × σ(W)` sobre posição atual × retornos históricos, fonte `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION`)
  - **Worst day pos.** (min da série HS)
  - Cores por linha contra Margem de cada PM: 🟡 ≥ 1σ · 🟠 ≥ VaR · 🔴 ≥ 2σ
  - `compute_pm_hs_var(dist_map)` — ver `project_rule_hs_vs_realized_pnl_for_var.md`
- **Email/mobile compatibility (Fase 1)**:
  - `<noscript>` banner com instruções (Windows Mark-of-the-Web → Properties → Unblock, ou abrir no browser)
  - Meta `X-UA-Compatible` + `viewport`
  - `@media (max-width: 768px)` e `480px`: tabelas `overflow-x:auto` + `white-space:nowrap`, chips flex-wrap, fontes menores, header compacto
  - Fase 2 (CSS-only refactor) **deliberadamente descartada** — ver `project_rule_mark_of_the_web.md`
- **Bug fix — duplicate section IDs**: múltiplos `sections.append((fund, report, html))` com mesma chave criavam DOM duplicados. Consolidação: concatenar HTML em uma entry só (aplicado ao Risk Budget tab juntando stop monitor + Budget vs VaR).

**Fase 4 — entregues (sessão 2026-04-22):**
- **IDKA VaR/BVaR fix — bug de triplicação**: `LOTE_PARAMETRIC_VAR_TABLE` tem múltiplas views por primitivo filtradas por `BOOKS`. `fetch_risk_history_idka` somava todas e triplicava. Fix: `WHERE "BOOKS"::text='{*}'`. Ver [project_rule_lote_parametric_var_table.md](../../../C:/Users/diego.fainberg/.claude/projects/f--Bloomberg-Quant-MODELOS-DFF-Risk-Monitor/memory/project_rule_lote_parametric_var_table.md).
- **IDKA limites corrigidos como daily**: 3Y soft 0.40 / hard 0.60 · 10Y soft 1.00 / hard 1.50 (% daily, 95% 1d). Horizonte Lote confirmado via cross-check HS.
- **IDKA Exposure toggle 3-vias** — `Bruto / Líquido vs Benchmark / Líquido vs Replication`. Replication = DV-match 2 NTN-Bs straddling target (`target_dm = target_anos / (1 + y/100)`), MD ANBIMA = `(DURATION/252) / (1+TIR/100)`. Default = Líquido vs Benchmark. DV01 convention unificada (long bond = negativo).
- **IDKA Distribuição 252d vs Benchmark** — active return (NAV pct_change − IDKA index pct_change via `ECO_INDEX`), injetado em `_DIST_PORTFOLIOS` como `kind='idka_active'`.
- **EVOLUTION Exposure — Por Strategy sem coloridinhos**: `_EVO_STRATEGY_COLOR` removido; 3 níveis (Strategy → LIVRO → Instrumento) com formatação uniforme.
- **Evolution standalone card (`evolution_diversification_card.py`)** — ganhou Camada Direcional (`fetch_direction_report` + `compute_camada_direcional` com filtro P60 histórico de magnitude) + Camada 4 (`compute_camada4`) + filtro P70 na Camada 3. Para 2026-04-17: 1/5 acesas (SIST P96 → C2), sem alerta. Ainda standalone em `data/morning-calls/evolution_diversification_*.html`, não integrado ao principal.
- **docs/IDKA_VAR_EXPLORATION.md** — auditoria do bug, decisão do horizonte (daily), comparativo Lote/HS (ratio 1.6-3× razoável), próximos passos parkeados.

**Fase 4 — entregues (sessão 2026-04-22 tarde):**
- **PM Vol Card standalone** (`pm_vol_card.py` + `run_vol_card.bat`): série vol_30d (realizada) + `vol_estimada` (lote paramétrico VaR anualizado via `LOTE_FUND_STRESS_RPM` LEVEL=10, `|PARAMETRIC_VAR_BRL| / NAV / 1.645 × √252`). Toggles de período no gráfico (Tudo / YTD / 252d / 21d). Análise de quintil migrada de janela 21d cumulativa → próximo dia único (observações independentes, sem solapamento).
- **Distribuição 252d — ALBATROZ onboarded (HS gross)**: confirmado PORTIFOLIO='ALBATROZ' em `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION`. Wired como HS gross (retorno absoluto, sem benchmark). Sub-label "HS · bps de NAV".
- **Distribuição 252d — IDKAs migrados para HS**: PORTIFOLIO keys 'IDKA3Y' / 'IDKA10Y' confirmados no DB. `fetch_idka_hs_active_series` substitui `fetch_idka_active_series` — usa W do motor HS (posição atual × cenários históricos) em vez de NAV pct_change realizado.
- **Toggle 3-vias nas IDKAs (vs Benchmark / vs Replication / Comparação)**:
  - `fetch_idka_hs_replication_series`: W − retorno ponderado das NTN-Bs DV-matched (pesos fixos na data atual, preços históricos ANBIMA)
  - `fetch_idka_hs_spread_series`: `replication_return − benchmark_return` — erro de tracking da réplica vs índice IDKA real (independente do W do fundo)
  - Comparação: tabela com 3 linhas (vs Benchmark · vs Replication · Repl − Bench spread) + footnote explicativo
  - `setDistBench` JS + `.dist-bench-btn` CSS; botões desabilitados quando dado indisponível
  - `setDistMode` corrigido para `[data-mode]` selector (não conflita com bench buttons)

**Fase 4 — entregues (sessão 2026-04-22 noite: code quality):**
- **Auditoria de código** (eficiência, segurança, qualidade, memória) — 22 issues identificados em 4 arquivos (generate_risk_report.py, pm_vol_card.py, evolution_diversification_card.py, glpg_fetch.py)
- **SQL injection fix** — `_parse_date_arg(s)` em `generate_risk_report.py` e `pm_vol_card.py`. Valida `sys.argv[1]` como `YYYY-MM-DD` antes de qualquer SQL. Cobre 80+ interpolações no generator e 10 no vol card.
- **Rolling std O(N²) → O(N)** — `pm_vol_card.py`: loop manual `for i in range(len(pnl))` substituído por `pd.Series.rolling(w, min_periods=w).std()`
- **Quick wins:** iterrows→itertuples no loop quintil, double null-check (`is not None and not pd.isna` → `pd.notna`) em 3 lugares no evolution card, `UTIL_WARN=70.0`/`UTIL_HARD=100.0` como constantes substituindo 5 magic numbers, erros Unicode no console (`→`, `σ`, emoji) corrigidos para ASCII
- **smoke_test.py** — script de regressão novo (11 assertions, ~35s): verifica exit code, tamanho de arquivo, ausência de NaN/None em células, nomes de fundos, seções presentes, VaR em range plausível. **Capturou 2 regressões durante o sweep de iterrows.**
- **iterrows sweep em generate_risk_report.py** — 29 conversões para `itertuples(index=False)`. 13 intencionalmente puladas com comentário inline explicando o motivo.

**Fase 4 — entregues (sessão 2026-04-22 noite tardia: refactor L1+L2 parcial):**
- **Nível 1 — quick wins** — helper `_fmt_br_num(s)` extraído para `risk_runtime.py`, substitui 12 cópias do padrão `.replace(",", "_").replace(".", ",").replace("_", ".")`. `tmp_carry2.py` órfão removido. Smoke test regex (`re.findall(r"VaR.{0,300}", ...)`) ajustado com negative lookbehind `(?<![+\-])` para ignorar `+X.Y% NAV` de exposições (flakiness quando briefing tinha commodity > 15%).
- **Nível 2 — extração de módulos (parcial)** — 3 módulos novos, sem mudança de comportamento:
  - `risk_runtime.py` (40 linhas) — `DATA_STR`, `DATA`, `DATE_1Y`, `DATE_60D`, `OUT_DIR`, `_parse_date_arg`, `fmt_br_num`. Fica abaixo de tudo para quebrar ciclos quando outros módulos importam `DATA_STR` como default arg.
  - `risk_config.py` (108 linhas) — `FUNDS`, `RAW_FUNDS`, `IDKA_FUNDS`, `ALL_FUNDS`, thresholds (`ALERT_THRESHOLD`, `UTIL_WARN`, `UTIL_HARD`), stops (`STOP_BASE`, `STOP_SEM`, `STOP_ANO`, `ALBATROZ_STOP_BPS`), navegação (`REPORTS`, `FUND_ORDER`, `FUND_LABELS`), PA keys (`_FUND_PA_KEY`, `_PA_BENCH_LIVROS`).
  - `html_assets.py` (110 linhas) — blob `UEXPO_JS` (~100 linhas de JS inline) usado pelas seções de exposure.
- **Não extraído (deliberado):** `fetch_*` / `compute_*` / `build_*`. Motivo: acoplamento com helpers internos (`_latest_nav`, `_prev_bday`, `_parse_rf`, `_parse_pm`, `_NAV_CACHE`) e domain dicts (`_PM_LIVRO`, `_PA_*`, `_RF_*`, `_EVO_*`, `_QUANT_*`) que ainda estão inline em `generate_risk_report.py`. Mover sem moves-coordenados desses helpers criaria imports circulares ou ciclos de `from generate_risk_report import …`. Próxima etapa de refactor precisa primeiro decidir o mapa desses helpers (geral/SVG/PA) para ter módulo-destino certo.
- `generate_risk_report.py`: 13071 → 12874 linhas (−197); 3 módulos novos com 258 linhas organizadas. Smoke test verde em todos os 4 commits (`049a888 → 5ceb018 → 0e43c1f → 139e263`).

**Fase 4 — pendente (consolidado e priorizado):**
- **Exposição MACRO ↔ QUANT — harmonização de layout** (user 2026-04-19, noite):
  - Unificar formatação visual: migrar MACRO do layout inline atual pra `.summary-table` (mesmo estilo do QUANT)
  - QUANT herda as colunas de MACRO que hoje não tem: **Δ Expo** (D-0 vs D-1), **σ (bps)**, **VaR (bps)** signed, **Δ VaR** vs D-1
  - QUANT já tem Gross %NAV + Gross BRL — user confirmou que quer manter
  - Renomear "Barra" no MACRO para "σ (bps)" (label não bate com conteúdo — é |std| de fato)
  - Column headers clicáveis para sort (QUANT já tem; MACRO passa a ter), arrow ↓/↑
  - Default sort em ambos: |Net| desc
  - Escopo decidido: "formatação e o gross do QUANT, o resto da info do MACRO"
- **EVOLUTION — direcionalidade (Camada 4 / "bull market alignment")** (pendente após MVP de 2026-04-20):
  - Camadas 1+2+3 entregues (utilização histórica, diversification benefit, correlação 63d)
  - Falta: Camada 4 alerta combinado (≥3 condições → "bull market alignment")
  - Falta: matriz `RISK_DIRECTION_REPORT` (`DELTA_SISTEMATICO` × `DELTA_DISCRICIONARIO`) para smoking gun posição-a-posição
  - Falta: filtro "relevantes ≥ P70" na Camada 3 (matriz 3x3 já OK, mas sem filtro de significância)
  - Falta: exclusão opcional de CREDITO também da Σ da Camada 2 (hoje ratio usa Σ winsorizada; pode ter variante "ex-CREDITO" na Σ também)
- **Distribuição 252d — MACRO_Q**: único fundo ainda sem série HS. Proposta: realized alpha vs CDI via `LOTE_TRADING_DESKS_NAV_SHARE.SHARE` pct_change − ECO_INDEX YIELD.
- **Briefings por fundo via LLM (Claude API)** (user 2026-04-19, noite):
  - Substituir os briefings rule-based atuais por prose gerada por LLM para os 8 fundos
  - Modelo sugerido: **Haiku 4.5** (`claude-haiku-4-5-20251001`) — briefings são descritivos, não precisam raciocínio pesado. ~$0.01–0.02 por rodada total (8 fundos)
  - Paralelizar no `ThreadPoolExecutor` já existente — latência ~2–3s no total
  - **Guardrails contra alucinação:** LLM recebe só JSON com números já computados (DIA/MTD/YTD, VaR util, stop util, top movers, exposições agregadas, outliers). Prompt: *"use exclusivamente os números abaixo; não invente; omita campos ausentes"*. Prose vai no campo `commentary`; números continuam vindo das tabelas da página
  - **Fallback:** se API falhar/timeout → cai no briefing rule-based atual (já existe). Usuário nunca vê erro
  - Independente do MACRO↔QUANT harmonization — dá pra tocar em paralelo
- **Drill-down no Por Bench (PA dos IDKAs)** (user 2026-04-19, noite):
  - Hoje o view "Por Bench" é uma tabela flat (Direct α / Swap / Via Albatroz / Total); user quer poder expandir
  - **Direct α** → árvore de posições diretas do IDKA (df_pa[FUNDO=IDKA_*]) excluindo fatia Albatroz
  - **Via Albatroz α** → árvore PA completa do ALBATROZ (df_pa[FUNDO='ALBATROZ'])
  - Swap leg fica flat (é só CDI − IDKA_index × w_alb, sem detalhe pra expandir)
  - Reusar a infra lazy-render de `_build_pa_view` (`togglePaRow` + `paRenderChildren` + `data-pa-id`)
- **Página de ETFs** — adicionar família ETF como view. Escopo a definir
- **Navigation checklist** — guia de leitura dos relatórios na ordem certa
- **IDKA HS BVaR — current-positions** — aplicar posições atuais (exploded) a 3y de yield moves (`PRICES_ANBIMA_BR_PUBLIC_BONDS` + DI1 yields + IPCA) − retorno do IDKA index. Stub realized-NAV em `76b1080`; wire depois de calibrar
- **IDKA Distribuição 252d — Replication com pesos diários**: hoje os pesos NTN-B (DV-match) são fixos na data atual. Variante mais precisa: resolver DV-match diariamente ao longo dos 252d para capturar drift de duration do índice. Escopo estimado ~2h. Ver `docs/IDKA_VAR_EXPLORATION.md` §4.2.
- **Exposure Map — calibração ANO_EQ vs calculadora existente** — overshoot de ~1yr (IDKA 3Y 3.97 vs ref 2.8; IDKA 10Y 11.32 vs ref 10). Fórmula minha (`-DELTA` p/ IPCA Coupon) == `AMOUNT × DV01 × 10000 / AUM` da calculadora matematicamente. Precisa diff lado-a-lado p/ descobrir diferença (escopo? positions excluded? convention?)
- **QUANT + EVOLUTION equity direct wiring** — Evolution-direct stub pronto; se FMN/FCO/FLO tiverem equity significativo, devem aparecer no Breakdown por Fator
- **IDKA limites definitivos** — provisórios ~80% util; aguarda mandato
- **Setor/Macro na tabela de posições LO** — join já feito, colunas não exibidas (~15 min)
- **TE real** — substituir σ_IBOV=20% por σ(Rp−Rb)×√252 via `EQUITIES_PRICES` (~2h)
- **ALBATROZ: calibrar limites definitivos + clarificar sign convention LFT**
- Main Risks cross-fund por CLASSE (via `df_pa`)
- Backtest de VaR (diagnóstico de calibração)
- Cross-fund overlap **por emissor** (instrument-level já entregue no card "Top Posições — consolidado" do Summary) — agregar VALE3+VALE5+ADR+opções num "issuer"; precisa mapa ticker→emissor
- Scenario library (named shocks)
- Drawdown trajectory (tempo underwater, velocidade)
- Correlation breakdown (diversification benefit ao longo do tempo)
- Style drift (PM vs mandato)
- Filter/search inline no PA (lazy-render-aware)
- Stress column validation guard (sanity query no DQ check)

Ver [memory/project_todo_risk_analytics_roadmap](C:/Users/diego.fainberg/.claude/projects/f--Bloomberg-Quant-MODELOS-DFF-Risk-Monitor/memory/project_todo_risk_analytics_roadmap.md) para o backlog detalhado.

**Code quality — parkado (ver `project_todo_code_quality_2026_04_22.md`):**
- **BRL locale formatter** — `_fmt_brl` helper substituindo `f"{v:,.1f}".replace(...)` em ~L2181 (20 min, LOW risk)
- **Return type hints** em funções `fetch_*` principais (45 min, LOW risk)
- **13 `iterrows` restantes** — requerem refactor das funções render (`make_row`, `_tr`, `fmt_row`) que recebem `r` como Series. Outros motivos: index como chave (L2179, L2357, L2361), `r.get()` (L3561, L4253, L8778), coluna dinâmica (L9527), `"% Cash"` (L10222)
- **`apply()` row-by-row** em `evolution_diversification_card.py` ~L455 (~8k chamadas Python puras)
- **Quebrar funções monolíticas** — `build_macro_exposure_section` (~450 linhas) e `build_evolution_exposure_section` (~250 linhas) — HIGH risk, só tocar com smoke_test expandido

---

## 6. Gaps deliberados (fora do MVP)

Fora de escopo **até a Fase 4 no mínimo**:

- Fundos **BALTRA**
- ~~Fundos **Long Only**~~ → **Frontier Ações FIC incorporado** (Fase 4, sessão pós 2026-04-18)
- Fundos **FMN** (ainda fora de escopo — relatório separado via xlwings/Excel existe)
- Família **Crédito** (só entra após MVP de Macro + RF estar estável)

Isso é escolha, não descuido. Não expandir escopo sem discutir.

---

## 7. Métrica de sucesso

Única métrica que importa:

> **Quantas vezes por semana o Morning Call leva a uma ação concreta do
> gestor.**

Não é cobertura, não é número de skills, não é quantidade de gráficos. Se o
briefing é lido e arquivado sem decisão, o kit está falhando. Iterar até que
isso mude.

---

## 8. Convenções

**Idioma.** Código, nomes de arquivos, variáveis, e chaves de JSON em **inglês**.
Documentação, comentários longos e texto do Morning Call em **português**.

**Git.** Commitar depois de cada sub-skill ficar minimamente funcional. Mensagem
no imperativo, em inglês: `add macro-stop-monitor skill`, `fix QUANT name in
canonical map`, etc.

**Skills novas.** Seguir o padrão do `skill-creator` em `/mnt/skills/examples/`
(ou o equivalente local). Toda skill tem `SKILL.md` com frontmatter `name` e
`description` claros — a `description` é o que faz o Claude saber quando
disparar a skill, então ser específico vale mais que ser curto.

**SQL.** Sempre parametrizar datas e fundos. Nada de literal de data no meio do
WHERE. Usar a camada `glpg-data-fetch`.

**Outputs.** O Morning Call final sai em markdown primeiro (Fase 2–3), depois
migra para HTML (Fase 4). Não perder tempo com estética antes de ter números
confiáveis.

**Tabelas no HTML.** Toda tabela deve, por padrão:
- Ter **sorting** em todas as colunas (click no `th` alterna asc/desc). Injetado
  automaticamente via `attachUniversalSort()` no DOMContentLoaded. Para desligar
  em uma tabela específica (ex: poucas linhas, só 2 rows, conteúdo SVG),
  adicionar `data-no-sort="1"` no `<table>` — exceção deve ser justificada.
- Ter **CSV export** no header do card (botão `⤓ CSV`). Injetado automaticamente
  via `injectCsvButtons()`. Exporta só linhas/tabelas visíveis (respeita toggles
  como Backward/Forward ou POSIÇÕES/PM VaR). Exceções só em cards puramente
  decorativos.

Regras universais do kit. Novas tabelas **não precisam** repetir — o comportamento
é injetado no DOMContentLoaded a partir das classes existentes.

---

## 9. Como começar uma nova sessão

Prompt sugerido para abrir uma sessão produtiva:

> Leia CLAUDE.md e me diga em qual fase estamos e qual é a próxima ação
> concreta. Antes de rodar qualquer código contra o GLPG, me mostre o plano.

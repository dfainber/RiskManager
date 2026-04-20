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

**Fase 4 — pendente (consolidado e priorizado):**
- **EVOLUTION — direcionalidade das estratégias / diversification benefit** (user 2026-04-19, tarde):
  - Medir se as sub-estratégias do Evolution (MACRO, SIST, FRONTIER, CREDITO, etc.) estão tomando risco na mesma direção
  - Uma forma: matriz de correlação realizada dos PnLs por estratégia
  - Outra: para cada fator de risco, quem está de que lado (ex.: MACRO tomado em Juros Nominais + SIST também tomado = alinhamento perigoso)
  - Diversification benefit: VaR aggregate vs soma linear dos VaRs por sub-estratégia
  - Skill `evolution-risk-concentration` já tem alguma infra (diversification benefit + correlação entre MACRO/SIST/FRONTIER)
  - Abordagem ainda não está clara — pendente desenho antes de codar
- **Página de ETFs** — adicionar família ETF como view. Escopo a definir
- **Navigation checklist** — guia de leitura dos relatórios na ordem certa
- **IDKA HS BVaR — current-positions** — aplicar posições atuais (exploded) a 3y de yield moves (`PRICES_ANBIMA_BR_PUBLIC_BONDS` + DI1 yields + IPCA) − retorno do IDKA index. Stub realized-NAV em `76b1080`; wire depois de calibrar
- **Exposure Map — calibração ANO_EQ vs calculadora existente** — overshoot de ~1yr (IDKA 3Y 3.97 vs ref 2.8; IDKA 10Y 11.32 vs ref 10). Fórmula minha (`-DELTA` p/ IPCA Coupon) == `AMOUNT × DV01 × 10000 / AUM` da calculadora matematicamente. Precisa diff lado-a-lado p/ descobrir diferença (escopo? positions excluded? convention?)
- **QUANT + EVOLUTION equity direct wiring** — Evolution-direct stub pronto; se FMN/FCO/FLO tiverem equity significativo, devem aparecer no Breakdown por Fator
- **IDKA limites definitivos** — provisórios ~80% util; aguarda mandato
- **Setor/Macro na tabela de posições LO** — join já feito, colunas não exibidas (~15 min)
- **TE real** — substituir σ_IBOV=20% por σ(Rp−Rb)×√252 via `EQUITIES_PRICES` (~2h)
- **ALBATROZ: calibrar limites definitivos + clarificar sign convention LFT**
- Main Risks cross-fund por CLASSE (via `df_pa`)
- Backtest de VaR (diagnóstico de calibração)
- Cross-fund / firm-level overlap por instrumento/emissor — alta ROI
- Scenario library (named shocks)
- Drawdown trajectory (tempo underwater, velocidade)
- Correlation breakdown (diversification benefit ao longo do tempo)
- Style drift (PM vs mandato)
- Filter/search inline no PA (lazy-render-aware)
- Stress column validation guard (sanity query no DQ check)

Ver [memory/project_todo_risk_analytics_roadmap](C:/Users/diego.fainberg/.claude/projects/f--Bloomberg-Quant-MODELOS-DFF-Risk-Monitor/memory/project_todo_risk_analytics_roadmap.md) para o backlog detalhado.

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

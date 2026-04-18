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
| `LOTE45`    | `LOTE_FUND_STRESS_RPM`            | VaR/Stress nível fundo (LEVEL=10) e série histórica |
| `LOTE45`    | `LOTE_BOOK_STRESS_RPM`            | VaR por book/RF (LEVEL=3), fonte para MACRO via Evolution |
| `LOTE45`    | `LOTE_PRODUCT_EXPO`               | Exposição/delta por produto — usar `TRADING_DESK_SHARE_SOURCE` |
| `q_models`  | `REPORT_ALPHA_ATRIBUTION`         | PnL por PM (LIVRO) e por instrumento |
| `q_models`  | `STANDARD_DEVIATION_ASSETS`       | σ por instrumento, BOOK='MACRO' |
| `q_models`  | `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` | Drawdowns simulados, retornos históricos |

Outras fontes (Access local, Excel diário) são **secundárias** e só entram
quando explicitamente necessárias.

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

**Fase 4 — pendente:**
- **Main Risks cross-fund** (via `df_pa` com CLASSE como fator) — discutido, não implementado
- **(A) Mudanças Significativas** já cobre os 5 fundos; **(B) Main Risks** fica para próxima
- Backtest de VaR (diagnóstico de calibração)
- Cross-fund / firm-level overlap (consolidado por instrumento/emissor)
- Scenario library (named shocks)
- Drawdown trajectory (tempo underwater, velocidade)
- Correlation breakdown (diversification benefit ao longo do tempo)
- Style drift (PM vs mandato)
- ALBATROZ: descobrir fonte VaR/Stress + definir mandato + clarificar sign convention LFT
- Filter/search inline no PA (lazy-render-aware)

Ver [memory/project_todo_risk_analytics_roadmap](C:/Users/diego.fainberg/.claude/projects/f--Bloomberg-Quant-MODELOS-DFF-Risk-Monitor/memory/project_todo_risk_analytics_roadmap.md) e [memory/project_todo_session_2026_04_18](C:/Users/diego.fainberg/.claude/projects/f--Bloomberg-Quant-MODELOS-DFF-Risk-Monitor/memory/project_todo_session_2026_04_18.md) para o backlog detalhado.

---

## 6. Gaps deliberados (fora do MVP)

Fora de escopo **até a Fase 4 no mínimo**:

- Fundos **BALTRA**
- Fundos **Long Only**
- Fundos **FMN**
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

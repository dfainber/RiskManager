# Risk Monitor — Catálogo de Tabelas/Cards × Defaults

> Preencha cada linha com o default desejado:
> `collapse all` · `expand all` · `none` (sem drill-down) · ou descrição custom (ex: "level-0 only", "Resumo aberto", etc).
>
> Card list extraído de `data/morning-calls/2026-04-24_risk_monitor.html` (103 sections, 9 funds + 7 modes).

---

## 1. Modes globais (top nav: `selectMode(...)`)

Os cards abaixo aparecem em modes top-level (Summary, Por Report, Markets, P&L, Peers, Qualidade). Cada um vive num data-mode container — um único card pode aparecer em mais de um mode.

| # | Card | Mode(s) onde aparece | Tem drill / sub-tabs? | Default desejado |
|---|---|---|---|---|
| 1 | **Briefing Executivo** | Summary | seções narrativas | _____ |
| 2 | **Status consolidado** | Summary, Por Report | tabela flat | _____ |
| 3 | **Risco VaR e BVaR por fundo** | Summary, Por Report | linhas clicáveis (`selectFund`), sem expand | _____ |
| 4 | **Breakdown por Fator** | Summary, Por Report | toggle Líquido/Bruto | _____ |
| 5 | **Vol Regime** (consolidado) | Summary, Por Report | drill-down por fundo (`toggleDistChildren`) | _____ |
| 6 | **Comments — Outliers do Dia** | Summary, Por Report | cards por fundo, drop-list de outliers | _____ |
| 7 | **Top Movers — DIA** (consolidado) | Summary, Por Report | toggle: Por Livro / Por Classe / **Por Classe (sem FX)** | _____ |
| 8 | **Mudanças Significativas** (consolidado) | Summary, Por Report | seção MACRO PM×fator + cards | _____ |
| 9 | **Top Posições — consolidado** | Summary, Por Report | drill-down (Factor → Instrument → Fund) `toggleTopPos` | _____ |
| 10 | **Status dos Dados** | Qualidade | tabela flat | _____ |
| 11 | **Qualidade de Dados** | Qualidade | tabela flat | _____ |
| 12 | **Mercado (Markets)** | Markets | sub-tabs: Janelas / Moedas / Commodities (`selectMktTab`) + toggle 1d/5d/1m (`selectMktWin`) | _____ |
| 13 | **Daily P&L** | P&L | tabela flat | _____ |
| 14 | **Peers (consolidado)** | Peers | flat / cards | _____ |

---

## 2. Mode "Por Fundo" — cards por fundo (`selectFund(...)`)

### 2.1 MACRO (12 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | toggle Por Classe / Por Livro · árvore PA hierárquica · search · Expandir/Colapsar/Reset | _____ |
| 2 | **Exposição — MACRO** | tabela por fator × PM com Δ Expo / σ / VaR signed | _____ |
| 3 | **Risk Monitor** | semáforo VaR/Stress + sparklines | _____ |
| 4 | **Outliers do dia** | lista flat de outliers `|z|≥2σ` | _____ |
| 5 | **Top Movers — DIA** | toggle Por Livro/Por Classe (consolidado da casa) | _____ |
| 6 | **Mudanças Significativas** | tabela PM × fator com Δ pp | _____ |
| 7 | **Distribuição 252d** | toggle Backward/Forward × 1d/21d · drill-down por livro/RF (`toggleDistChildren`) · modal Top 21d | _____ |
| 8 | **Vol Regime** | tabela com drill por livro/RF | _____ |
| 9 | **Risk Budget Monitor** | semáforo stop por PM + histórico | _____ |
| 10 | **Budget vs VaR por PM** | tabela com 21d/63d/252d | _____ |
| 11 | **Briefing — Macro** | texto narrativo | _____ |
| 12 | **Peers — Macro** | toggle Gráficos/Tabela · 4 strips + 2 scatters | _____ |

### 2.2 QUANT (11 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | toggle Por Classe / Por Livro · árvore PA · search · Expandir/Colapsar/Reset | _____ |
| 2 | **Exposição — QUANT** | tabela por fator × livro · sort headers (`sortQuantExpoPositions`) | _____ |
| 3 | **Single-Name Exposure** | L/S real pós-decomposição WIN/BOVA · drill | _____ |
| 4 | **Risk Monitor** | semáforo VaR/Stress | _____ |
| 5 | **Outliers do dia** | lista flat | _____ |
| 6 | **Top Movers — DIA** | toggle (consolidado) | _____ |
| 7 | **Mudanças Significativas** | tabela | _____ |
| 8 | **Distribuição 252d** | toggle Backward/Forward × 1d/21d + modal Top 21d | _____ |
| 9 | **Vol Regime** | drill por livro | _____ |
| 10 | **Briefing — Quantitativo** | texto narrativo | _____ |
| 11 | **Peers — Quantitativo** | toggle Gráficos/Tabela | _____ |

### 2.3 EVOLUTION (18 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | toggle Por Classe / Por Livro (Strategy → Livro → Produto) · árvore | _____ |
| 2 | **Exposição — EVOLUTION** | toggle Factor / Strat (`selectEvoExpoView`) · 3 níveis | _____ |
| 3 | **Exposure Map RF** | bucket de maturidade × ANO_EQ | _____ |
| 4 | **Single-Name Exposure** | L/S look-through (QUANT + Evo) | _____ |
| 5 | **Risk Monitor** | semáforo | _____ |
| 6 | **Diversificação — Evolution** | toggle 5 sub-tabs: Resumo / Camada 1 / Camada 2 / Camada 3 / Direcional (`selectEvoDivView`) | _____ |
| 7 | **Camada 4 — Bull market alignment** | painel com 5 condições | _____ |
| 8 | **Camada 1 — Utilização VaR** | tabela percentil 252d por estratégia | _____ |
| 9 | **Camada 2 — Diversification benefit** | série rolling | _____ |
| 10 | **Camada 3 — Correlação realizada** | tabela rolling 63d | _____ |
| 11 | **Matriz Direcional** | matriz SIST × DISC | _____ |
| 12 | **Outliers do dia** | lista flat | _____ |
| 13 | **Top Movers — DIA** | toggle (consolidado) | _____ |
| 14 | **Mudanças Significativas** | tabela | _____ |
| 15 | **Distribuição 252d** | toggle Backward/Forward × 1d/21d + modal Top 21d | _____ |
| 16 | **Vol Regime** | drill por estratégia | _____ |
| 17 | **Briefing — Evolution** | texto narrativo | _____ |
| 18 | **Peers — Evolution** | toggle Gráficos/Tabela | _____ |

### 2.4 MACRO_Q (7 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | toggle Por Classe / Por Livro · árvore PA | _____ |
| 2 | **Risk Monitor** | semáforo VaR/Stress (LOTE_FUND_STRESS SUM) | _____ |
| 3 | **Outliers do dia** | lista flat | _____ |
| 4 | **Top Movers — DIA** | toggle (consolidado) | _____ |
| 5 | **Mudanças Significativas** | tabela | _____ |
| 6 | **Briefing — Macro Q** | texto narrativo | _____ |
| 7 | **Peers — Macro Q** | toggle Gráficos/Tabela | _____ |

### 2.5 ALBATROZ (11 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | árvore | _____ |
| 2 | **Exposure RF** | tabela por instrumento + duration agregada | _____ |
| 3 | **Exposure Map RF** | bucket maturidade × ANO_EQ | _____ |
| 4 | **Risk Monitor** | semáforo | _____ |
| 5 | **Outliers do dia** | lista | _____ |
| 6 | **Top Movers — DIA** | toggle | _____ |
| 7 | **Mudanças Significativas** | tabela | _____ |
| 8 | **Distribuição 252d** | toggle 1d/21d + modal Top 21d (HS gross) | _____ |
| 9 | **Risk Budget** | stop mensal 150 bps | _____ |
| 10 | **Briefing — Albatroz** | texto | _____ |
| 11 | **Peers — Albatroz** | toggle Gráficos/Tabela | _____ |

### 2.6 BALTRA (5 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | árvore | _____ |
| 2 | **Risk Monitor** | semáforo | _____ |
| 3 | **Outliers do dia** | lista | _____ |
| 4 | **Top Movers — DIA** | toggle | _____ |
| 5 | **Briefing — Baltra** | texto | _____ |

### 2.7 FRONTIER (9 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution (Long Only)** | tabela `Frontier Ações` · NAV/Beta/PnL · sub-tab `data-tab=lo` | _____ |
| 2 | **Performance Attribution (PA hier.)** | árvore (sub-tab `data-tab=pa`) | _____ |
| 3 | **Exposição vs Benchmark** | active weight por nome/setor · toggle `data-view=name` / `data-view=sector` | _____ |
| 4 | **Risk Monitor** | semáforo BVaR | _____ |
| 5 | **Outliers do dia** | lista | _____ |
| 6 | **Top Movers — DIA** | toggle | _____ |
| 7 | **Mudanças Significativas** | tabela | _____ |
| 8 | **Briefing — Frontier** | texto | _____ |
| 9 | **Peers — Frontier** | toggle Gráficos/Tabela | _____ |

### 2.8 IDKA_3Y (8 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | toggle Por Classe / Por Livro / **Por Bench** (default = Bench) | _____ |
| 2 | **Exposição — IDKA 3Y** | toggle 3-vias: Bruto / Líq vs Benchmark / Líq vs Replication (`selectIdkaView`) | _____ |
| 3 | **Exposure Map RF** | bucket maturidade × ANO_EQ | _____ |
| 4 | **Risk Monitor** | semáforo BVaR (parametric) | _____ |
| 5 | **Outliers do dia** | lista | _____ |
| 6 | **Top Movers — DIA** | toggle | _____ |
| 7 | **Distribuição 252d** | toggle 4-vias: vs Benchmark / vs Replication / Comparação · Backward/Forward · 1d/21d · modal Top 21d (3 seções) | _____ |
| 8 | **Briefing — IDKA 3Y** | texto | _____ |

### 2.9 IDKA_10Y (8 cards)
| # | Card | Sub-elementos / Drills | Default |
|---|---|---|---|
| 1 | **Performance Attribution** | toggle Por Classe / Por Livro / **Por Bench** | _____ |
| 2 | **Exposição — IDKA 10Y** | toggle 3-vias Bruto / Líq Bench / Líq Repl | _____ |
| 3 | **Exposure Map RF** | bucket maturidade × ANO_EQ | _____ |
| 4 | **Risk Monitor** | semáforo BVaR | _____ |
| 5 | **Outliers do dia** | lista | _____ |
| 6 | **Top Movers — DIA** | toggle | _____ |
| 7 | **Distribuição 252d** | mesmo do IDKA 3Y (vs Benchmark / vs Replication / Comparação) | _____ |
| 8 | **Briefing — IDKA 10Y** | texto | _____ |

---

## 3. Notas técnicas — handlers de drill/expand existentes

| Handler JS | Onde se aplica | Tipo de default |
|---|---|---|
| `paToggle(tr)` | árvore PA (todos os fundos) | level-0 only por default |
| `expandAllPa(btn)` / `collapseAllPa(btn)` | árvore PA — botões na toolbar | manual |
| `resetPaSort(btn)` | árvore PA | reset DFS |
| `toggleTopPos(tr)` | Top Posições — consolidado | level-0 (factor) only por default |
| `toggleDistChildren(tr)` | Distribuição (fund → livro/rf) | level-0 only por default |
| `selectPaView(btn,view)` | PA toggle Por Classe/Por Livro/Por Bench | depende do fundo (IDKA = bench, outros = classe) |
| `selectIdkaView(btn,view)` | Exposição IDKA Bruto/Líq Bench/Líq Repl | atual: Bruto |
| `selectEvoExpoView(btn,view)` | Exposição EVO Factor/Strat | atual: Factor |
| `selectEvoDivView(btn,sec)` | Diversificação 5 sub-tabs | atual: Resumo |
| `selectMoversView(btn,view)` | Top Movers Por Livro/Classe/Classe sem FX | atual: Por Livro |
| `setDistMode(card,mode)` | Distribuição Backward/Forward | atual: Forward |
| `setDistWindow(card,win)` | Distribuição 1d/21d | atual: 1d |
| `setDistBench(card,bench)` | Distribuição IDKA bench/repl/cmp | atual: Benchmark |
| `selectMktTab(tab)` | Markets Janelas/Moedas/Commodities | atual: primeira |
| `selectMktWin(win)` | Markets 1d/5d/1m | atual: primeira |
| `selectRfBrl(btn,mode)` | Breakdown Líquido/Bruto | atual: Líquido |
| `setDistTopSection(modal,sec)` | Modal Top 21d (IDKA: 3 seções) | atual: vs Benchmark |

---

## 4. Próximos passos

1. **Você** preenche cada `_____` com:
   - `collapse all` (todas as linhas hierárquicas começam fechadas)
   - `expand all` (todas abertas)
   - `level-0 only` (default atual de várias árvores)
   - ou descrição custom (ex: "Camada 4 aberto, demais fechadas")

2. **Eu** implemento ajustando os geradores correspondentes (`pa_renderers.py`, `summary_renderers.py`, `fund_renderers.py`, `evo_renderers.py`, `expo_renderers.py`).

3. Validamos rodando o relatório e comparando com a imagem desejada.

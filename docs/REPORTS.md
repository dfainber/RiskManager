# Risk Monitor — Reports, Calculations & System Structure

*Última atualização: 2026-04-28*

Este documento descreve o que cada card do HTML contém, qual é a fórmula por trás, de onde os dados vêm, e como as peças se encaixam. Complementa o `CLAUDE.md` (que é a fonte de verdade sobre escopo e convenções) e `docs/documentacao_tecnica.html` (que tem mais detalhe de SQL).

**Deltas relevantes desde 2026-04-19** (ver CLAUDE.md §7 pra detalhe completo):
- BALTRA adicionado (2026-04-26): VaR/Stress + PA + Exposure RF + Exposure Map RF
- Markets re-enabled (2026-04-26)
- Daily Monitor + Peers redesign (2026-04-24/27)
- Camada 4 EVOLUTION (Bull market alignment) + Matriz Direcional (2026-04-21)
- Distribuição 1d/21d toggle + modal "5 piores · 5 melhores" (2026-04-27)
- IDKA Distribuição: toggle vs Benchmark / vs Replication / Comparação (default = Comparação)
- Top Movers: 3º toggle "Por Classe (sem FX)" (2026-04-28)
- PA FX-split standalone reports: MACRO, EVO, QUANT, MACRO_Q (2026-04-27/28)
- Exposure RF (ALBATROZ + BALTRA): CDI/LFTs excluídos do breakdown (2026-04-28)

---

## 1. Visão geral do sistema

### 1.1 Finalidade

Gerar, em 1 comando, um painel HTML consolidado do risco de todos os fundos da gestora para o **Morning Call**. O output é um único arquivo em `data/morning-calls/{YYYY-MM-DD}_risk_monitor.html` que abre no browser e tem toda a visão da casa.

### 1.2 Famílias de fundos cobertas (9 fundos hoje)

| Short | Fundo | Benchmark | Tipo | Fonte de risco |
|-------|-------|-----------|------|----------------|
| MACRO | Galapagos Macro FIM | CDI | Multimercado | `LOTE_FUND_STRESS_RPM` (LEVEL=2) |
| QUANT | Galapagos Quantitativo FIM | CDI | Multimercado | `LOTE_FUND_STRESS_RPM` (LEVEL=2) |
| EVOLUTION | Galapagos Evolution FIC FIM CP | CDI | Multimercado (multi-estratégia) | `LOTE_FUND_STRESS_RPM` (LEVEL=3) |
| MACRO_Q | Galapagos Global Macro Q | CDI | Multimercado offshore | `LOTE_FUND_STRESS` (product-level SUM) |
| ALBATROZ | GALAPAGOS ALBATROZ FIRF LP | CDI | Renda Fixa | `LOTE_FUND_STRESS` (product-level SUM) |
| BALTRA | Galapagos Baltra Icatu Qualif Prev FIM CP | IPCA+ (~3-4y duration) | Multimercado prev | `LOTE_FUND_STRESS` (product-level SUM) |
| FRONTIER | Frontier Ações FIC FI | IBOV | Long Only equity | `LOTE_FUND_STRESS` + HS BVaR próprio |
| IDKA_3Y | IDKA IPCA 3Y FIRF | IDKA IPCA 3A | RF benchmarked | `LOTE_PARAMETRIC_VAR_TABLE` |
| IDKA_10Y | IDKA IPCA 10Y FIRF | IDKA IPCA 10A | RF benchmarked | `LOTE_PARAMETRIC_VAR_TABLE` |

### 1.3 Fluxo de execução

```
run_report.bat (Windows) ou python generate_risk_report.py YYYY-MM-DD
    ↓
ThreadPoolExecutor (max_workers=12) dispara ~30 queries em paralelo
    ↓
_NAV_CACHE warmup (1 query bulk p/ todas as NAVs de hoje + D-1)
    ↓
build_html(...) monta cada card
    ↓
HTML salvo + abre no browser
```

### 1.4 Estrutura do HTML gerado

O painel tem **7 modos de navegação** (topo direito, via `selectMode(...)`):

- **Summary**: visão da casa — cards agregados cross-fundo
- **Por Fundo**: navegação fundo-a-fundo com múltiplos reports por fundo
- **Por Report**: mesma lista de reports, agrupados por tipo em vez de por fundo
- **Qualidade**: status das bases + drift/staleness por tabela
- **P&L**: Daily P&L D-1 por book/classe/posição
- **Peers**: comparativo consolidado vs pares (gráficos + tabela)
- **Markets**: snapshot cross-asset com sub-tabs (Janelas/Moedas/Commodities) × window (1d/5d/1m)

Default ao abrir = **Summary**.

URLs com hash (`#summary`, `#fund=MACRO`, `#quality`, `#market`, `#pnl`, `#peers`) permitem deep-linking.

---

## 2. Reports da página Summary (visão da casa)

Ordem atual de cima pra baixo:

1. Status consolidado
2. Risco Agregado
3. Breakdown por Fator (com toggle Bruto/Líquido)
4. Vol Regime
5. Alerts (quando houver)
6. Comments — Outliers do dia
7. Top Movers — DIA
8. Mudanças Significativas
9. Top Posições — consolidado (drill-down)
10. Status dos Dados

### 2.1 Status consolidado

**O que mostra:** uma linha por fundo com retorno DIA/MTD/YTD/12M, VaR atual, utilização de VaR e de stop, e Δ VaR D-1. Mais 2 linhas "benchmark" no rodapé (IBOV + CDI) para referência.

**Cálculos:**

- **DIA/MTD/YTD/12M** (alpha vs. CDI): soma de `dia_bps`/`mtd_bps`/`ytd_bps`/`m12_bps` da tabela `q_models.REPORT_ALPHA_ATRIBUTION`, por fundo. Já vem net de CDI pela engine.
  - **Exceção Frontier**: usa `TOTAL_IBVSP_{DAY,MONTH,YEAR}` do mainboard Long Only (ER vs. IBOV, não vs. CDI).
- **VaR atual**: última observação da série. Para MACRO/QUANT/EVO: `PARAMETRIC_VAR / NAV × -100%`. Para ALBATROZ/MACRO_Q: `PVAR1DAY / NAV × -100%`. Para IDKAs: `RELATIVE_VAR_PCT × -100%` (BVaR é a métrica primária). Para FRONTIER: HS BVaR vs IBOV (§ 4.3).
- **Util VaR** = VaR atual / soft limit. Fundos `informative=True` (FRONTIER) não têm util (sem limit).
- **Util Stop** (só MACRO e ALBATROZ hoje): maior utilização de orçamento mensal entre os PMs (MACRO) ou abs MTD / 150 bps (ALBATROZ).
- **Status 🟢/🟡/🔴**: max(util VaR, util stop). Verde <70%, amarelo 70–100%, vermelho ≥100%.
- **Δ VaR D-1** = |VaR hoje| − |VaR D-1|, em bps. "flat" se |Δ| < 0.5 bps.

**Linhas IBOV + CDI:**
- IBOV: retornos compostos de `public.EQUITIES_PRICES.CLOSE` (INSTRUMENT='IBOV') para os 4 windows.
- CDI: `fetch_cdi_returns()` — soma simples de `public.ECO_INDEX` (INSTRUMENT='CDI', FIELD='YIELD').

### 2.2 Risco Agregado (house-wide)

**O que mostra:** uma linha por fundo com NAV, VaR absoluto (% + R$), benchmark do fundo, BVaR relativo (% + R$). Linha total = soma simples (sem benefício de diversificação). Top-5 destacados por 🔺 abs R$ e 🔷 rel R$.

**Cálculos:**

- **VaR abs (%)** = lida do `series_map`, mesma fonte do Status consolidado.
- **VaR abs (R$)** = VaR% / 100 × NAV. NAV vem de `_NAV_CACHE` (warm-up via `fetch_all_latest_navs`).
- **BVaR rel (%)**:
  - FRONTIER: `compute_frontier_bvar_hs` (§ 4.3) — 3y HS vs. IBOV, clip de corp actions.
  - IDKAs: `RELATIVE_VAR_PCT` do engine (paramétrico, vs. IDKA index).
  - CDI-benchmarked (MACRO, QUANT, EVO, MACRO_Q, ALBATROZ): ≈ VaR abs, já que CDI tem vol ≈ 0.
- **Total row** = soma simples. Conservador (assume correlação = 1 entre fundos).

### 2.3 Breakdown por Fator

**O que mostra:** matriz 7 fatores × 8 fundos com R$ de exposição por célula, e total por fator. Toggle **Bruto / Líquido** (default: Líquido).

**Fatores:**

| Fator | Unidade | Fonte |
|-------|---------|-------|
| Real | BRL·ano (ANO_EQ) | `rf_expo_maps` — NTN-B + DAP primitives (`IPCA Coupon`) |
| Nominal | BRL·ano | `rf_expo_maps` — DI1 futures (`BRL Rate Curve`); MACRO via `df_expo.delta_dur` em RF-BZ |
| IPCA Idx | BRL (face) | `rf_expo_maps` — primitive `IPCA` (exposição à inflação) |
| Equity BR | BRL nocional | Frontier NAV × gross; QUANT + EVOLUTION-direct net deltas (com ETF explosion); MACRO rf=RV-BZ |
| Equity DM | BRL nocional | MACRO rf=RV-DM |
| Equity EM | BRL nocional | MACRO rf=RV-EM |
| Commodities | BRL nocional | MACRO rf=COMMODITIES + P-Metals |
| FX | BRL nocional | MACRO rf starts with FX- (omitido da tabela se tudo zero) |

**Bruto vs Líquido:**
- **Bruto**: `sum(ano_eq_brl)` direto da fonte.
- **Líquido**: `bruto − bench_allocation`:
  - Real: IDKA 3Y bench = 3.0 × NAV, IDKA 10Y bench = 10.0 × NAV. Outros bench = 0.
  - IPCA Idx: IDKAs bench = 1.0 × NAV. Outros = 0.
  - Equity BR: Frontier bench = 1.0 × NAV (IBOV 100% long). Outros = 0.
  - Nominal/Commodities/FX/Equity DM/EM: todos bench = 0 (CDI tem zero duration; MACRO vs CDI).

**Por que Líquido importa**: IDKA 10Y tem +997M BRL·ano de exposição real bruta, mas o bench absorve 914M → líquido ~83M. O risco "ativo" do fundo é 8× menor que o bruto sugere.

**Convenção de sinal** (para `rf_expo_maps`):
- Para primitives `IPCA Coupon` e `BRL Rate Curve`, `ano_eq_brl = -DELTA` (o engine guarda DELTA com sinal de hedge-side; negamos para recuperar a exposição da posição).
- Para `IPCA` (inflação carry) e outros primitives, `ano_eq_brl = DELTA` direto.
- `DELTA × MOD_DURATION` no engine é equivalente a `POSITION × MOD_DURATION` para NTN-Bs e DAPs (verificado).

### 2.4 Vol Regime

**O que mostra:** realized 21d vol (annualized) vs. janela full HS, com pct rank do percentil e classificação (low / normal / elevated / stressed).

**Cálculos:**

- Fonte: `q_models.PORTIFOLIO_DAILY_HISTORICAL_SIMULATION.W` — série sintética de P&L simulada diária para cada portfolio (MACRO, SIST, EVOLUTION).
- `vol_recent_pct = std(W[-21:]) × √252 × 100`
- `vol_full_pct = std(W) × √252 × 100`
- `ratio = vol_recent / vol_full`
- `pct_rank` = percentil de `std(W[t-21:t])` entre todas as janelas rolling de 21d (primary signal, não-paramétrico).
- `z-score` = (vol_recent − mean_vol) / sd_vol — informativo, com SE ~15% (N_ef~36 em 756d).
- `regime`: p<20 "low", 20-70 "normal", 70-90 "elevated", ≥90 "stressed".

### 2.5 Comments — Outliers do dia

**O que mostra:** por fundo, lista de produtos com contribuição anormal (|z| ≥ 2σ vs 90d **E** |contrib| ≥ 3 bps) **OU** (|contrib| ≥ 10 bps — cláusula absoluta para capturar losses materiais em nomes voláteis onde σ é grande demais pro z-test disparar).

**Cálculo:**
- Fonte: `q_models.REPORT_ALPHA_ATRIBUTION` (via `fetch_pa_daily_per_product`).
- Para cada `(FUNDO, LIVRO, PRODUCT)`, calcula σ = std(dia_bps) dos últimos 90d.
- `z = dia_bps_today / σ`.
- Flag se `(|z| ≥ 2 AND |bps| ≥ 3)` OR `|bps| ≥ 10`.

### 2.6 Top Movers — DIA

**O que mostra:** top 3 contribuintes e top 3 detratores do dia por fundo. **3 toggles**: **Por Livro** (default) / **Por Classe** / **Por Classe (sem FX)**.

**Cálculo:**
- Soma `dia_bps` por LIVRO (ou CLASSE) dentro do fundo.
- Caixa/Custos/Taxas excluídos (operacionais).
- "Sem FX" também filtra `CLASSE in {BRLUSD, FX}` antes de agregar — isola efeito-ativo.
- Top 3 positivos + top 3 negativos por |sum|.
- Drop abaixo de 0.5 bps (ruído).

### 2.7 Mudanças Significativas D-0 vs D-1

**O que mostra:** exposições que mudaram materialmente entre ontem e hoje. MACRO em PM×fator; outros fundos em fator agregado.

**Cálculo:**
- Fonte: `LOTE_PRODUCT_EXPO` com `TRADING_DESK_SHARE_SOURCE` no source do fundo.
- Agrega `DELTA/NAV × 100` por (PM, rf) para MACRO ou (rf) para outros.
- Δ = today − D-1.
- Flag |Δ| ≥ 0.30 pp.
- Ordenado por |Δ| desc.

### 2.8 Top Posições — consolidado (drill-down)

**O que mostra:** hierarquia clicável Fator → Instrumento → Fundo(s). 3 níveis:

- **Nível 0** (sempre visível): linha por fator com total da casa + nº de instrumentos.
- **Nível 1** (expande ao clicar no fator): top 5 instrumentos por fator, com soma através dos fundos.
- **Nível 2** (expande ao clicar no instrumento): cada fundo holder, valor e % do instrumento.

Toggle **Bruto / Líquido**:
- **Bruto**: exposição total da posição.
- **Líquido**: escalada por `(1 − bench_fundo_fator / total_fundo_fator)` — abate pro-rata a parcela que apenas replica o benchmark.

**Evita double-count:**
- Usa `fetch_evolution_direct_single_names` (desk = source = Evolution) em vez do look-through — sem importar posições que já estão em QUANT/Frontier.
- Exclui via_albatroz (já contado em ALBATROZ direto).

### 2.9 Status dos Dados (DQ compact)

**O que mostra:** um check-list compacto do status das bases que alimentam o relatório. Verde = ok, vermelho = ausente/defasado.

---

## 3. Reports por fundo (tab "Por Fundo")

Cada fundo tem N reports, dependendo do que faz sentido para o fundo. Ordem fixa:

1. **Análise** (por fundo) — outliers + top movers + mudanças filtradas só deste fundo
2. **PA** (Performance Attribution) — hierárquico, com toggle Por Classe / Por Livro (e Por Bench p/ IDKAs, default)
3. **Risk Monitor** — card com VaR/BVaR/Stress, histórico 12m, bar vs. limits
4. **Exposure** — varia por fundo (MACRO/QUANT/EVO/ALBATROZ/BALTRA/IDKAs/FRONTIER têm builders próprios)
5. **Exposure Map** (IDKA_3Y, IDKA_10Y, ALBATROZ, BALTRA, MACRO, EVOLUTION) — § 3.5
6. **Single-Name** (só QUANT, EVOLUTION) — L/S com ETF explosion
7. **Distribuição 252d** (MACRO, QUANT, EVOLUTION, ALBATROZ, IDKA 3Y, IDKA 10Y) — toggle 1d/21d × Backward/Forward; IDKAs ainda têm bench toggle (Comparação default)
8. **Vol Regime** (per fundo MACRO/QUANT/EVOLUTION) — drill-down da Vol Regime do Summary
9. **Risk Budget** (MACRO, ALBATROZ) — stop mensal
10. **Long Only** (só FRONTIER) — tabela completa de posições
11. **Diversificação** (só EVOLUTION) — 5 sub-tabs (Resumo / Camada 1 / 2 / 3 / Direcional)
12. **Briefing** (todos os fundos) — texto narrativo
13. **Peers** (todos os fundos com peer group definido) — strips MTD/YTD/12M/24M + 2 scatters Vol vs Retorno

### 3.1 Análise (por fundo)

3 cards lado-a-lado: Outliers, Top Movers, Mudanças Significativas — mesma lógica do Summary mas filtrada para o fundo específico.

### 3.2 Performance Attribution (PA)

Hierárquico com 2-3 níveis dependendo do fundo:

- **Por Classe**: CLASSE → PRODUCT. Ex: "RF BZ" → "NTNB 2028", "DI1F28", ...
- **Por Livro**: LIVRO → PRODUCT. EVO tem 3 níveis: Strategy (Macro/Quant/Equities/Crédito) → Livro → PRODUCT.

**Ordem default fixa** (`_PA_ORDER_CLASSE`, `_PA_ORDER_LIVRO`, `_PA_ORDER_STRATEGY`) com tiebreak |YTD| desc. Pinned-bottom (sem sort): Caixa, Caixa USD, Taxas e Custos, Custos.

**Colunas**: DIA · MTD · YTD · 12M (em %, 2 decimais; era bps antes). Sort clicável preservando hierarquia.

**Footer 3 linhas**: Total Alpha · Benchmark (CDI) · Retorno Nominal.

**Heatmap** nas células (alpha 0.14). **Lazy render** (nós abrem sob demanda, HTML reduziu 4MB → 0.9MB).

### 3.3 Risk Monitor (per fund)

Card único por fundo com:
- VaR atual · Soft limit · Hard limit · Util%
- Stress atual · Soft · Hard · Util% (não mostrado p/ `informative=True` funds)
- Sparkline 12m de VaR e Stress
- Bar-chart horizontal com range 12m e limits
- Para IDKAs: labels "BVaR 95%" (primary) + "VaR 95% (ref)" em vez de "VaR 95% 1d" / "Stress".

### 3.4 Exposure

Varia por fundo:
- **MACRO**: `build_exposure_section` — POSIÇÕES (por RF factor, expandível por PM × produto) + PM VaR (por gestor, expandível). Colunas: %NAV, σ, ΔExpo, VaR%, ΔVaR, Margem, DIA.
- **QUANT**: `build_quant_exposure_section` — por fator × livro, sort por header.
- **EVOLUTION**: `build_evolution_exposure_section` — toggle Factor / Strat (default = Strat), 3 níveis.
- **ALBATROZ**: `build_albatroz_exposure` — resumo por indexador (Pré/IPCA/IGP-M/Outros — **CDI excluído** desde 2026-04-28: LFTs duration ≈ 0 inflam Net %NAV / Gross sem representar risco) + top 15 posições por |DV01|.
- **BALTRA**: `build_albatroz_exposure(..., fund_label="BALTRA")` — mesma estrutura do ALBATROZ. Generalizado em 2026-04-28.
- **IDKAs**: `build_idka_exposure_section` — toggle 3-vias **Bruto / Líquido vs Benchmark / Líquido vs Replication** (default = Líquido vs Benchmark). Por fator com children collapsed por default (caret ▶), expand via `toggleIdkaFac`.
- **FRONTIER**: `build_frontier_exposure_section` — active weight vs IBOV/IBOD, toggle Por Nome/Por Setor.

### 3.5 Exposure Map (IDKAs, ALBATROZ, BALTRA, MACRO, EVOLUTION)

Card que detalha a exposição de taxas (entregue 2026-04-19; estendido a MACRO/EVO em sessões seguintes; BALTRA adicionado 2026-04-28). **Chart único** com 3 bars por bucket: Fund Real (amber), Fund Nominal (teal), Bench (slate).

- **Eixo X**: 12 buckets de maturidade [0-6m, 6-12m, 1-2y, 2-3y, 3-4y, 4-5y, 5-6y, 6-7y, 7-8y, 8-9y, 9-10y, 10y+].
- **Eixo Y**: ANO_EQ em anos (years equivalent).
- **Cumulative lines**: fund_real_cum, fund_nom_cum, bench_cum (dashed).
- **Toggles**:
  - **Absoluto** (default): mostra fund bars + bench bar side-by-side.
  - **Relativo**: mostra fund − bench per bucket (barras 2-col em vez de 3).
  - **Ambos / Real / Nominal**: filtra qual fator aparece.
- **Stat row**: NAV, Duração Real, Duração Nominal, Total Fund, Bench, CDI %NAV, Gap (Fund − Bench), via Albatroz (quando > 0).
- **Tabelas colapsáveis**:
  - "Mostrar tabela (por bucket)": bucket × Real/Nominal/Fund Total/Bench/Relative.
  - "Mostrar posições (por ativo)": lista completa ordenada por |ANO_EQ|, com Book, Fator, Maturidade, Duration, Position R$, ANO_EQ.
- **Y-axis snap to ±0.5y** (entregue 2026-04-28): quando max abs de all_vals (fund + bench + relative + cumulative) < 0.5 yr, força `y_max=0.5, y_min=-0.5`. Sem isso, fundos pequenos como EVOLUTION (Total ~-0.04yr) renderizavam com auto-scale forçado em ±1.0+ deixando o chart vazio.

**Albatroz look-through explodido:** `fetch_rf_exposure_map(desk)` faz 2 queries unioned:
1. `TRADING_DESK = desk` AND `SHARE_SOURCE = desk` → posições diretas
2. `TRADING_DESK = 'ALBATROZ'` AND `SHARE_SOURCE = desk` → slice de Albatroz que pertence ao desk

Retorna uma coluna `via` = 'direct' | 'via_albatroz'. Stat row mostra "via Albatroz +X.XXyr" quando > 0.

**Convenção ANO_EQ** (item § 4.2).

### 3.6 Single-Name L/S (QUANT, EVOLUTION)

Tabela por ticker com:
- **Direct**: posição direta em BRL.
- **From Idx**: explosion de WIN (IBOV future), BOVA11 (IBOV ETF), SMAL11 (SMLLBV ETF), ADRs (via `PRIMITIVE_NAME` regex `^[A-Z]{4}[0-9]{1,2}$`).
- **Net** = Direct + From Idx.
- **% NAV**.

Gross absoluto no header. Coluna "From Idx" mostra origem (WIN+BOVA+SMAL+ADR). Pinned-bottom por magnitude.

Para EVOLUTION, tem look-through em QUANT (Bracco, Quant_PA), Evo Strategy (FMN_*, FCO, Ações BR Long), Frontier, Macro FIM (CI_COMMODITIES). Para o Summary cross-fund, usa `fetch_evolution_direct_single_names` (só o que Evolution segura direto) p/ evitar double-count.

### 3.7 Distribuição 252d (MACRO, QUANT, EVOLUTION, ALBATROZ, IDKA 3Y, IDKA 10Y)

Toggle **Backward / Forward** (default: Forward) × **1d / 21d** (default: 1d).

- **Backward**: D-1 carteira × 252d históricos + DIA realizado overlayed. Responde: "onde o move de hoje caiu na distribuição histórica da carteira de ontem?"
- **Forward**: D carteira × 252d históricos. Responde: "como a carteira atual se comportaria nos últimos 252d?"
- **1d**: cada obs = retorno HS de 1 dia (~252 obs).
- **21d**: cada obs = soma rolante de 21 retornos consecutivos (~232 obs). Helper `_to_rolling_sum` via cumsum vetorizado. Quando 21d ativo, **botão azul "5 piores · 5 melhores"** abre modal com janelas não-sobrepostas (greedy, sort por soma asc/desc, pula sobreposições). Modal de IDKAs tem 3 sub-seções (vs Benchmark / vs Replication / Repl − Bench).
- **Backward grayed-out em 21d** (entregue 2026-04-28): combo Backward + 21d não tem overlay realizado, então o botão Backward fica cinza/não-clicável quando window=21.

**Toggle adicional para IDKA 3Y / 10Y** (entregue 2026-04-28): **vs Benchmark / vs Replication / Comparação** (default = Comparação · 1d · Forward).
- **vs Benchmark**: HS active return (fundo − índice IDKA).
- **vs Replication**: fundo − réplica NTN-B (DV-match).
- **Comparação**: tabela 3-linhas com as 3 séries lado-a-lado.
- **NTN-B coupon-date fix**: `_ntnb_total_return_pct_change` reinjeta semi-coupon na pct_change de ANBIMA UNIT_PRICE (que é clean / ex-coupon). Eliminou outliers de -200~300 bps fantasmas em datas de cupom.

**Default expanded** (entregue 2026-04-28): linhas filhas do drill-down (livro/RF dentro do fundo) começam visíveis (caret ▼). Antes começavam fechadas.

Fonte: `q_models.PORTIFOLIO_DAILY_HISTORICAL_SIMULATION.W` por portfolio (fund-level ou PM-level). Stats: min, p05, mean, std, p95, max, actual, percentile do actual. Coluna `a+var95` renomeada para **`p95`** em 2026-04-27.

### 3.8 Risk Budget

- **MACRO**: stop por PM (CI/LF/JD/RJ/QM) com carry forward mensal. Base 63 bps/mês, semestral 128 bps, anual 252 bps. Regra de carrego mensal implementada em `carry_step(...)`.
- **ALBATROZ**: 150 bps/mês sem carry.

### 3.9 Long Only (FRONTIER)

Tabela completa de posições:
- % Cash · Delta · Beta · #ADTV · Ret D/MTD/YTD · Attrib D/MTD/YTD · ER IBOD D/MTD/YTD.
- Subtotais por book + Total.
- Fonte: `frontier.LONG_ONLY_DAILY_REPORT_MAINBOARD`.

### 3.10 PA FX-split standalone reports (2026-04-27/28)

Para os 4 fundos com PA em `REPORT_ALPHA_ATRIBUTION` (MACRO, EVOLUTION, QUANT, MACRO_Q), há **scripts standalone** que reagrupam a árvore PA separando efeito-ativo de efeito-FX. Não é card no relatório principal — gera HTML próprio em `data/morning-calls/<date>_<fundo>_pa_fx_split.html`.

**Scripts:**
- `generate_macro_pa_fx_split.py` — CLASSE → GRUPO → LIVRO → PRODUCT
- `generate_evolution_pa_fx_split.py` — STRATEGY → CLASSE_NEW → GRUPO_NEW → LIVRO → PRODUCT
- `generate_quant_pa_fx_split.py` — LIVRO → CLASSE_NEW → GRUPO_NEW → PRODUCT
- `generate_macroq_pa_fx_split.py` — idem do QUANT, **com toggle adicional "FX Detalhado / FX Consolidado"**: a view consolidada lifta todo BRLUSD/FX para uma única linha top-level "≡ FX Basis Risk & Carry" acima de Caixa/Custos.

**Re-mapping** (idêntico nos 4):
- `CLASSE='BRLUSD' AND GRUPO='Commodities'` → "FX Basis Risk & Carry" / "FX em Commodities"
- `CLASSE='BRLUSD' AND GRUPO='RV Intl'` → "FX Basis Risk & Carry" / "FX em RV Intl"
- `CLASSE='BRLUSD' AND GRUPO='RF Intl'` → "FX Basis Risk & Carry" / "FX em RF Intl"
- `CLASSE='BRLUSD' AND GRUPO in {BRLUSD, Custos}` → "FX Basis Risk & Carry" / "FX Spot & Futuros"
- `CLASSE='FX'` → "FX Basis Risk & Carry" / "FX Spot & Futuros"
- Demais → unchanged

**Total preservado**: pura reordenação categórica, sem recálculo. Cada relatório inclui bloco de verificação numérica (✓/⚠) confirmando que Total + buckets preservados (Commodities, RV Intl, RF Intl) batem em DIA/MTD/YTD/12M com diff < 0.05 bps. Top contribuintes/detratores excluem FX Basis (efeito-ativo puro).

UX: toolbar Expandir / Colapsar / Reset + sort por header (DIA/MTD/YTD/12M). Drill-down preservado no tree.

---

## 4. Métricas-chave (detalhadamente)

### 4.1 VaR (absoluto, 95% 1d)

Fórmula: -Paramétrico 95% 1-day / NAV × 100.

Fonte por fundo:
- MACRO / QUANT / EVOLUTION: `LOTE45.LOTE_FUND_STRESS_RPM.PARAMETRIC_VAR` com `LEVEL = cfg["level"]` (2 ou 3).
- ALBATROZ / MACRO_Q: `LOTE45.LOTE_FUND_STRESS.PVAR1DAY`, SUM por TRADING_DESK.
- FRONTIER: mesmo que RAW (PVAR1DAY), mas substituído pelo HS BVaR vs IBOV no Summary (§ 4.3).
- IDKAs: `LOTE45.LOTE_PARAMETRIC_VAR_TABLE.ABSOLUTE_VAR_PCT` × -100 (já em fração decimal).

### 4.2 ANO_EQ (1-year equivalent exposure)

Métrica canônica para exposição de taxas: quantos **BRL de exposição ponderada por duration** (ou equivalentemente: anos-equivalentes por 100% NAV) tem o fundo.

Fórmula canônica (conforme calculadora existente em `RELATORIO_EXPO_PNL_AUTOMATICO_HTML/SHEETS/IDKA_TABLES_GRAPHS.py`):

```
ANO_EQ = AMOUNT × DV01 × 10000 / AUM
       = (AMOUNT × PU) × MOD_DURATION / AUM
       = market_value × MOD_DURATION / AUM
       = position %NAV × MOD_DURATION
```

Nossa implementação em `fetch_rf_exposure_map`:

```python
ano_eq_brl = -DELTA   # para PRIMITIVE_CLASS em {'IPCA Coupon','BRL Rate Curve'}
ano_eq_brl =  DELTA   # para outros primitives
# dividido por NAV depois pra virar "anos"
```

`-DELTA` é equivalente a `POSITION × MOD_DURATION` (verificado empiricamente) porque o engine guarda DELTA com sinal de **hedge-side** (negativo pra long bond, representando o short-DI que hedgearia a posição).

**Buckets de maturidade** (definidos em `_RF_BUCKETS`):
```python
[("0-6m", 0, 0.5), ("6-12m", 0.5, 1), ("1-2y", 1, 2), ("2-3y", 2, 3),
 ("3-4y", 3, 4), ("4-5y", 4, 5), ("5-6y", 5, 6), ("6-7y", 6, 7),
 ("7-8y", 7, 8), ("8-9y", 8, 9), ("9-10y", 9, 10), ("10y+", 10, 99)]
```

Bucket = `DAYS_TO_EXPIRATION / 365.25`. Fallback: usar `MOD_DURATION` se days não disponível.

### 4.3 HS BVaR (Frontier vs. IBOV)

`compute_frontier_bvar_hs(df_frontier, date_str, window_days=756)`:

1. Pega pesos atuais do mainboard (`% Cash` por ticker).
2. Query 3y de CLOSE (756 business days + 120 buffer) para cada ticker + IBOV em `public.EQUITIES_PRICES`.
3. Pivoteia para wide, drop linhas sem IBOV, forward-fill stocks em dias não-trading.
4. `returns = wide.pct_change()`.
5. **Clip de corp actions**: `returns[stock_cols].mask(|returns| > 0.30, 0.0)`. B3 tem circuit breaker individual em ~15%; qualquer coisa > 30% é split/bonificação (AMOB3 teve 5492% num dia, BBAS3 +50% — clipados).
6. `synthetic_fund_return = Σ weight_i × return_i`.
7. `ER = synthetic_fund_return − return_IBOV`.
8. `BVaR_95_1d = -quantile(ER, 0.05) × 100` (em %NAV).

Validação 2026-04-17: std(ER) = 0.51% / dia → parametric 1.645 × std = 0.84% ≈ BVaR 0.85% (distribuição ~normal após clip). TE anualizada 8.1%/yr (plausível p/ LO 17-names).

### 4.4 BVaR paramétrico (IDKAs)

Direto de `LOTE45.LOTE_PARAMETRIC_VAR_TABLE.RELATIVE_VAR_PCT`. É a métrica do engine, vs. o índice IDKA correspondente.

### 4.5 Vol Regime (pct rank)

Ver § 2.4. Primary signal é pct_rank (não-paramétrico, imune ao overlap das janelas). Z-score é secundário.

### 4.6 Stop (MACRO carry forward)

`carry_step(budget_abs, pnl, ytd)`:
- Se PnL ≥ 0: `next_budget = STOP_BASE + pnl × 0.5` (mantém metade do ganho como cushion)
- Se PnL < 0: consome `cushion_extra × 0.25 + base × 0.50 + excesso × 1.0` (camada suave pro cushion, dura pro excesso)
- Se YTD < 0: cap semestral `STOP_SEM - |ytd|` aplicado

Gancho (vermelho) quando `remaining ≤ 0`.

---

## 5. Estrutura de código

### 5.1 Arquivo orquestrador + módulos auxiliares

Orquestrador: `generate_risk_report.py` (~4280 linhas após refactor de 2026-04). Split em módulos auxiliares (ver CLAUDE.md §3 para tabela completa):

| Arquivo | Responsabilidade |
|---|---|
| `risk_runtime.py` | DATA_STR, fmt_br_num, parse_date_arg |
| `risk_config.py` | FUNDS dicts, mandatos, _PM_LIVRO, _FUND_PEERS_GROUP, peer groups |
| `db_helpers.py` | _latest_nav, _prev_bday, fetch_all_latest_navs |
| `data_fetch.py` | Todos os fetch_* (36 funções públicas) |
| `metrics.py` | compute_pm_hs_var, compute_frontier_bvar_hs, compute_pa_outliers, vol regime, compute_top_windows |
| `svg_renderers.py` | range_bar_svg, stop_bar_svg, range_line_svg, sparklines |
| `pa_renderers.py` | PA tree, lazy-render JSON, section assembler |
| `evo_renderers.py` | 4 camadas EVOLUTION + Camada 4 (Bull Market Alignment) |
| `expo_renderers.py` | 8 build_*_exposure_section incl. ALBATROZ + BALTRA generalizados |
| `fund_renderers.py` | build_stop_section, briefings, distribuição (com 1d/21d), peers, vol regime |
| `summary_renderers.py` | summary cards (Status, Top Movers, Outliers, Mudanças, Top Posições) |
| `html_assets.py` | UEXPO_JS blob |
| `evolution_diversification_card.py` | placeholder pra futuro card consolidado (não wireado ainda) |

### 5.2 Layout interno

```
- imports + config
- FUNDS / RAW_FUNDS / IDKA_FUNDS / ALL_FUNDS dicts (mandatos provisórios)
- REPORTS list (registra tipos de report)
- FUND_ORDER, FUND_LABELS, _FUND_PA_KEY

- fetch_* functions (todas as queries)
- compute_* functions (BVaR, vol regime, outliers, etc.)
- build_* functions (montam cada card/section)

- build_html(...) — main orchestrator
  - loop por FUND_ORDER → monta cards por fundo
  - monta cards cross-fund (Risco Agregado, Breakdown, Top Posições, etc.)
  - concatena em section-wrap divs (summary / fund / quality)
  - injeta CSS + JS no <head> + <body>

- main() — CLI entrypoint
  - ThreadPoolExecutor paralelo pra todos os fetches
  - Pré-warmup do _NAV_CACHE
  - Resolve futures sequencialmente com fallback D-1
  - Monta data_manifest
  - Chama build_html
  - Escreve arquivo
```

### 5.3 Dados em cache

- `_NAV_CACHE`: dict `(desk, date_str) → NAV`. Populado em bulk via `fetch_all_latest_navs` antes do `build_html`. `_latest_nav()` consulta cache primeiro, fallback direto no DB se miss.

### 5.4 Skills

10 skills em `.claude/skills/`:
- `risk-manager` — framework de referência
- `risk-daily-monitor` — semáforo diário (MM)
- `macro-stop-monitor` — stops por PM (MACRO)
- `macro-risk-breakdown` — decomposição (MACRO)
- `evolution-risk-concentration` — concentração (EVOLUTION)
- `rf-idka-monitor` — monitor RF benchmarked (IDKAs)
- `performance-attribution` — PA transversal
- `risk-data-collector` — pré-check das bases
- `risk-morning-call` — orquestrador final
- `wrap-session` — ritual de fim de sessão (commit + push + memory update)

### 5.5 Memory (`~/.claude/projects/.../memory/`)

- `MEMORY.md` — índice
- `user_role.md` — role do Diego (risk manager Galapagos)
- `project_status.md` — fase atual
- `feedback_key_corrections.md` — armadilhas críticas
- `project_data_sources.md` — schemas das tabelas
- `project_todo_risk_analytics_roadmap.md` — backlog priorizado

---

## 6. Convenções importantes

### 6.1 Idioma

- Código, variáveis, JSON keys: **inglês**
- UI/texto do Morning Call, comentários longos: **português**

### 6.2 Sinais

- VaR/Stress: armazenado como PnL negativo (-R$), convertido para positivo em %NAV nos outputs.
- DELTA em `LOTE_PRODUCT_EXPO` para primitives de rate (`IPCA Coupon`, `BRL Rate Curve`): sinal hedge-side, negamos para exposição de posição.
- IPCA primitive: sinal direto (long bond = long IPCA face-value).

### 6.3 Unidades

- VaR/Stress: % NAV ou bps de NAV (contexto dita).
- ANO_EQ: anos-equivalentes (1.0 = 100% NAV em bond de 1y duration).
- Retornos PA: % (2 decimais).
- Exposição RF na tabela de Breakdown: BRL·ano para duration-based; BRL nocional para equity/commodity.

### 6.4 NAV lag

NAV lag de ~1 business day vs. VaR/Expo. `_latest_nav` usa `VAL_DATE <= date_str ORDER BY VAL_DATE DESC LIMIT 1`. `merge_asof(backward)` em `build_series` para alinhar NAV aos dias de risco.

### 6.5 D-1 convenção

Não há "shift contábil D-1" — era um bug antigo (engine faz look-through para fundos nested). Todos os fundos usam D direto. Se dados de D faltarem, fallback explícito pra D-1 com banner ⚠ de stale.

---

## 7. Como adicionar um novo fundo

1. **Mandato**: criar `data/mandatos/mandato-{SHORT}.json` com soft/hard limits de VaR e Stress.
2. **Dict**: adicionar entrada em `FUNDS` (se `LEVEL=2/3` em RPM) ou `RAW_FUNDS` (se product-level) ou `IDKA_FUNDS` (se parametric BVaR).
3. **FUND_ORDER**: adicionar o short em ordem desejada no array.
4. **FUND_LABELS**: adicionar display name.
5. **_FUND_PA_KEY**: adicionar pa_key (nome em `REPORT_ALPHA_ATRIBUTION.FUNDO`).
6. **Reports específicos**: se precisar de algo além do default (Risk Monitor, PA, Análise), adicionar builder + registrar em `sections.append((SHORT, "report-id", html))`.

---

## 8. Como adicionar um novo report cross-fund

1. **Fetch**: adicionar `fetch_*` + `ex.submit` no bloco paralelo.
2. **Resolve**: resolver o future com fallback.
3. **Pass**: passar como kwarg para `build_html`.
4. **Build**: criar `build_*` function que gera o HTML do card.
5. **Register**: injetar via f-string no `summary_html`.

---

## 9. Variantes visuais — DUAS versões obrigatórias

O Morning Call é distribuído em **duas versões**. Toda nova feature precisa
ser validada nas duas.

| Script                              | Tema  | Público                       | Output                                                          |
|-------------------------------------|-------|-------------------------------|-----------------------------------------------------------------|
| `generate_risk_report.py`           | Dark  | Desktop individual            | `data/morning-calls/{DATE}_risk_monitor.html` + mirror dated/ultimo |
| `generate_risk_report_meeting.py`   | Light | Telona da sala de Morning Call | `data/morning-calls-meeting/ultimo_morning_call_meeting.html`   |

### 9.1 Como funciona o `_meeting`

É **post-processor**: lê o HTML do dark, aplica:
- Substituição do bloco `:root` (paleta brand light)
- `_BG_HEX_MAP` — troca hex codes escuros (`background:#XXX`, `fill="#XXX"`,
  `stroke="#XXX"`) por equivalentes claros (Análise tiles, expand rows,
  budget bars, etc.)
- `_TEXT_COLOR_MAP` — escurece textos de baixo contraste em fundo branco
  (`#94a3b8` → `#475569`, amarelos `#facc15`/`#fbbf24` → `#a16207`, etc.)
- `_FONT_SIZE_MAP` — bump `~+1.5px` em todas as inline `font-size:NNpx`
- `_MEETING_OVERRIDES` — bloco CSS final (tabelas com header navy, fontes
  ampliadas always-on 17px+, esconde controles secundários, logo em navy
  via filter, bordas grossas)

Não toca em queries, dados, lógica — só apresentação.

### 9.2 Regras ao implementar nova feature

1. Implementar normal em `generate_risk_report.py` / módulos renderer.
2. **Verificar render nas duas versões**:
   - Dark: `python generate_risk_report.py {DATE}`
   - Meeting: `python generate_risk_report_meeting.py {DATE}` (post-processa
     o HTML dark — gera dark se faltar)
3. **Hex codes hardcoded** introduzidos pela feature:
   - Dark backgrounds (`#0xxxxx`–`#3xxxxx` em `background:` ou SVG `fill=`)
     → adicionar ao `_BG_HEX_MAP` em `generate_risk_report_meeting.py`
   - Light text (`color:#94a3b8`, amarelos claros, etc.) → adicionar ao
     `_TEXT_COLOR_MAP`
4. **CSS variables** (`var(--text)`, `var(--muted)`, etc.) — funcionam
   automático nas duas versões.
5. Para automação: `run_report_auto.bat` deveria rodar AMBOS sequenciais
   (dark gera o HTML base, meeting transforma).

### 9.3 Onde encontrar o "ultimo" pra exibição

Telona da sala consome:
`F:\Bloomberg\Risk_Manager\Data\Morningcall\ultimo_morning_call_meeting.html`

Sempre o mais recente (o script sobrescreve). Não há archive datado da
versão meeting — o archive vive na pasta dark.

---

**FIM.** Dúvidas ou inconsistências: falar com Diego (dfainber@gmail.com).

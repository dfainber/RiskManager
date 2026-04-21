# Evolution — Metodologia de Diversificação, Direcionalidade e Risco Agregado

**Fundo:** Galapagos Evolution FIC FIM CP
**Skill:** `evolution-risk-concentration`
**Código:** [`evolution_diversification_card.py`](../evolution_diversification_card.py) · wired em [`generate_risk_report.py`](../generate_risk_report.py) · exibido em **EVOLUTION → Diversificação**
**Versão:** 2026-04-21

---

## 1. Motivação

O Evolution é um fundo **multiestratégia**. Em regime normal, suas estratégias operacionais (MACRO, SIST, FRONTIER, CREDITO, EVO_STRAT) rodam descorrelacionadas, e o VaR agregado do fundo é menor que a soma linear das VaR de cada estratégia — **essa é a justificativa econômica da alocação multi-manager**.

O **risco oculto** aparece em regimes de bull market ou em choques macro direcionais, quando:
- SIST roda risk-on
- MACRO assume posições longs direcionais
- FRONTIER (especialmente FCO, concentrado) amplifica

A descorrelação esperada **desaparece**, e o risco efetivo do fundo fica muito maior do que o VaR linear sugere. Nesse momento, a narrativa "multiestratégia diversificado" para de valer.

Esta metodologia existe para **detectar esse cenário antes que ele se materialize em perda**, em 4 camadas complementares + 1 agregadora.

---

## 2. Buckets direcionais (estratégias)

### 2.1 Taxonomia original (6 estratégias)

Extraída do script `EVOLUTION_TABLES_GRAPHS.py` e congelada em [`assets/livros-map.json`](../.claude/skills/evolution-risk-concentration/assets/livros-map.json):

| Estratégia | Peso-alvo | Descrição |
|---|---|---|
| **MACRO**     | ~48% | CI, Macro_JD, Macro_LF, Macro_RJ, Macro_FG, Macro_QM, Macro_AC, Macro_MD, Giro_Master, LF_RV-BZ_SS |
| **SIST**      | ~17% | Bracco, Quant_PA, SIST_FX, SIST_GLOBAL, SIST_COMMO, SIST_RF |
| **FRONTIER**  | ~5%  | FLO (direcional), FMN (risk neutral), FCO (direcional concentrado) |
| **CREDITO**   | 15–30% | Crédito, GIS FI FUNDO IMOBILIÁRIO, GIS CUSTOS E PROVISÕES |
| **EVO_STRAT** | balanço | Macro_DF, Baltra_DI, FX, AÇÕES BR LONG |
| **CAIXA**     | residual | Caixa, Caixa USD, Taxas e Custos, RV BZ |

### 2.2 Buckets da Camada 4 (4 buckets direcionais)

Para a **Camada 4** (alerta combinado), FRONTIER e EVO_STRAT são **unidos num pacote só** ("tático direcional"). Razão: são estratégias operacionalmente pequenas que se sobrepõem em tese de direcionalidade. Separar gera sinal ruidoso por serem individualmente pequenas demais para carregar significância estatística.

| Bucket | Fonte |
|---|---|
| **MACRO** | VaR contribuído MACRO |
| **SIST** | VaR contribuído SIST |
| **FRONTIER+EVO** | Soma das séries VaR de FRONTIER + EVO_STRAT |
| **CREDITO** | VaR contribuído CREDITO (winsorizado — ver §6) |

CAIXA e OUTROS são **excluídos dos sinais da Camada 4** por natureza não-direcional.

---

## 3. Camada 1 — Utilização histórica por estratégia

### 3.1 Pergunta

*Cada estratégia está carregada (risk-on) comparada ao próprio histórico recente?*

### 3.2 Cálculo

Para cada estratégia `s`:

```
VaR_hoje_s   = VaR contribuído da estratégia hoje (em bps do PL do Evolution)
Série_252d_s = VaR diário dos últimos 252 dias úteis da mesma estratégia
Percentil_s  = rank de VaR_hoje_s dentro da Série_252d_s, em [0, 100]
```

**Fonte:** `LOTE45.LOTE_BOOK_STRESS_RPM`, `LEVEL=2`, `TREE='Main_Macro_Gestores'`. A coluna `BOOK` já traz o nome da estratégia.

### 3.3 Estados

| Percentil hoje | Estado |
|---|---|
| < P50 | 🟢 Tranquilo |
| P50–P70 | 🟢 Normal |
| P70–P85 | 🟡 Elevado |
| P85–P95 | 🔴 Alto |
| > P95 | ⚫ Extremo |

### 3.4 Sinal de alinhamento

Uma estratégia sozinha em P90 pode ser escolha consciente e não é problema. **O sinal relevante é quantas estratégias simultaneamente estão elevadas.**

> **Gatilho Camada 1 (condição 1 da Camada 4):** ≥ 3 de 4 buckets direcionais em ≥ P70

---

## 4. Camada 2 — Diversification Benefit

### 4.1 Pergunta

*As estratégias estão se "ajudando" menos via diversificação do que o histórico?*

### 4.2 Cálculo

```
VaR_soma = Σ_s VaR_hoje_s                 # soma linear (hipótese de corr=1 entre estratégias)
VaR_real = VaR_fundo (LOTE_FUND_STRESS_RPM LEVEL=10, corrs empíricas aplicadas)
Ratio    = VaR_real / VaR_soma            # ∈ (0, 1]
```

Interpretação:

| Ratio | Significa |
|---|---|
| 0.55 | Correlação entre estratégias está reduzindo em 45% a soma linear — ótimo |
| 0.75 | Diversificação mediana |
| 0.90 | Estratégias quase andando juntas — mau sinal |
| 1.00 | Fundo efetivamente "single strategy" do ponto de vista de risco |

O **percentil do Ratio** na janela 252d diz se ele está historicamente baixo (bom) ou alto (ruim).

### 4.3 Tratamento CREDITO (winsorização causal)

**Problema observado em dez/2025:** cotas júnior de crédito tiveram spike de VaR por mark-to-market artificial. A VaR_CREDITO subiu de ~10 bps para ~60 bps num dia, inflando a `VaR_soma` e desfazendo artificialmente o Ratio para baixo (= diversificação "ótima" fantasma).

**Solução:** a série `VaR_CREDITO` é **winsorizada causalmente** usando escala robusta rolling:

```
Para cada dia t:
  med_t   = mediana(VaR_CREDITO[t-63 : t-1])         # 63d passado, exclui t
  mad_t   = mediana(|VaR_CREDITO[t-63 : t-1] − med_t|)
  cap_t   = med_t + 3 × 1.4826 × mad_t                 # σ-equivalente sob normal
  VaR_CREDITO_wins_t = min(VaR_CREDITO_t, cap_t)        # clip só tail superior
```

Propriedades:
- **Causal** (`shift(1)`): a métrica do dia t não usa VaR_t pra construir seu próprio teto — evita "auto-clipagem"
- **Robusta** (MAD): o spike de dezembro não polui o limiar por semanas (std seria envenenado)
- **Tail único** (upper): valores baixos de VaR não são problema; não clipamos embaixo

**Ratio principal usa Σ winsorizada.** O Ratio raw (sem winsorização) é mostrado abaixo como referência para diagnóstico. Ver [`CREDITO_TREATMENT.md`](CREDITO_TREATMENT.md) para detalhes.

### 4.4 Gatilho

> **Condição 3 da Camada 4:** Ratio (winsorizado) ≥ P80 (252d)

---

## 5. Camada 3 — Correlação realizada entre PnLs das estratégias

### 5.1 Pergunta

*As estratégias estão andando juntas em realização de PnL (não só em tese de posição)?*

### 5.2 Cálculo

**Fonte:** `q_models.REPORT_ALPHA_ATRIBUTION` com `FUNDO='EVOLUTION'`, agregado diariamente por LIVRO → estratégia (via [`livros-map.json`](../.claude/skills/evolution-risk-concentration/assets/livros-map.json)).

```
Para cada par (a, b) de estratégias direcionais {MACRO, SIST, FRONTIER}:
  corr_21d = Pearson(PnL_a[last 21d], PnL_b[last 21d])    # sensor rápido
  corr_63d = Pearson(PnL_a[last 63d], PnL_b[last 63d])    # baseline médio-prazo
  Série_pct_63d = histórico de corr_63d em 252d rolling
  c63_pct = rank(corr_63d hoje) na Série_pct_63d
```

CREDITO e EVO_STRAT/CAIXA são excluídos da matriz (CREDITO por distorção do VaR júnior; EVO_STRAT/CAIXA por não serem direcionais).

### 5.3 Filtro de significância

Uma correlação alta entre estratégias pode ser **artefato** quando uma delas está ociosa (PnL próximo de zero, matematicamente instável). Por isso, aplicamos um filtro:

**Um par só é flagado como "alinhamento relevante" quando:**
1. `c63_pct ≥ 85`, **E**
2. **Ambas** as estratégias do par estão em ≥ P70 na Camada 1 (ambas carregadas)

Pares que passam só em (1) são mostrados com badge 🟡 "alta corr mas sinal desconsiderado".

### 5.4 Gatilho

> **Condição 4 da Camada 4:** ≥ 1 par com `c63_pct ≥ 85` **E** filtro de significância aprovado

---

## 6. Camada Direcional — Matriz SIST × DISC por categoria de ativo

### 6.1 Pergunta

*Quando olhamos posição-a-posição, as duas metades do fundo (sistemática vs. discricionária) estão empurrando o mesmo risco na mesma direção?*

É a **smoking gun** do alinhamento: não importa se as estratégias têm correlação baixa no passado — se hoje ambas estão long NTN-B e long Petro ao mesmo tempo, o fundo está direcional mesmo que o histórico diga o contrário.

### 6.2 Cálculo

**Fonte:** `q_models.RISK_DIRECTION_REPORT` — tabela que decompõe o book do Evolution em duas colunas:
- `DELTA_SISTEMATICO` (agregado dos livros sistemáticos)
- `DELTA_DISCRICIONARIO` (MACRO + FRONTIER + EVO_STRAT discricionários)

agrupadas por `CATEGORIA` (~10 classes: Equity Brasil, Equity GLOBAL, Juros BR, BRL (FX), Commodities, P-Metals, Global Rates, Indexador, BRL Corporate Yield, Fundo).

Para cada CATEGORIA:
```
Se existe NOME='Net':      usa os deltas dessa linha
Caso contrário:            soma os deltas de todas as NOMEs exceto 'Gross'
delta_sist_bps = DELTA_SISTEMATICO / NAV × 10000
delta_disc_bps = DELTA_DISCRICIONARIO / NAV × 10000
pct_pl         = PCT_PL_TOTAL (fração do NAV)
```

### 6.3 Classificação de sinal por categoria

| Condição | Estado |
|---|---|
| `|delta_sist_bps| < 5` **e** `|delta_disc_bps| < 5` | **dust** (categoria irrelevante) |
| `|delta_sist_bps| < 5` | **only-disc** (só a parte discricionária tem posição) |
| `|delta_disc_bps| < 5` | **only-sist** |
| sinais opostos | 🟩 **opposite** (estado saudável — metades hedgeiam) |
| sinais iguais | 🟥 **same-sign** (alinhamento — preocupante se repetir em várias categorias) |

**Thresholds (fixos):**
- `min_leg_bps = 5` bps: magnitude mínima por perna para sinal contar (evita ruído)
- `min_cat_pct = 1%` do PL: categoria precisa ser material para ser considerada

### 6.4 Gatilho

> **Condição 5 da Camada 4:** ≥ 3 categorias materiais com `same-sign` status

---

## 7. Camada 4 — Bull Market Alignment (alerta combinado)

### 7.1 Conceito

As 4 camadas anteriores são **complementares**, não redundantes. Cada uma responde a uma pergunta diferente:

| Camada | Pergunta |
|---|---|
| 1 | As estratégias estão carregadas vs. seu próprio histórico? |
| 2 | A diversificação está pior que o normal? |
| 3 | Os PnLs realizados andam juntos? |
| Direcional | As posições ex-ante das duas metades apontam para o mesmo lado? |

**Camada 4** agrega as 4 num **alerta único**: dispara quando **≥ 3 de 5 condições** acendem simultaneamente.

### 7.2 As 5 condições

| # | Condição | Fonte | Threshold |
|---|---|---|---|
| 1 | ≥ 3 de 4 buckets em ≥ P70 | Camada 1 | `pct ≥ 70` |
| 2 | ≥ 1 bucket em ≥ P95 | Camada 1 | `pct ≥ 95` |
| 3 | Ratio C2 (winsorizado) ≥ P80 | Camada 2 | `ratio_wins_pct ≥ 80` |
| 4 | ≥ 1 par corr 63d ≥ P85 **com** filtro de significância | Camada 3 | ver §5.3 |
| 5 | ≥ 3 categorias same-sign | Direcional | ver §6.3 |

### 7.3 Por que ≥ 3?

- **1-2 condições** acesas isoladamente podem ser ruído, escolha consciente de alocação, ou limitação de janela histórica.
- **3+** acesas simultaneamente **raramente** é coincidência — é o regime de "tudo apontando para o mesmo lado" que define bull market alignment.

### 7.4 Estados do alerta

| n_lit | Estado | Visualização |
|---|---|---|
| 0 | ✓ Sem sinais | Caixa verde |
| 1–2 | 🟡 Alinhamento parcial | Caixa amarela |
| ≥ 3 | 🚨 BULL MARKET ALIGNMENT | Caixa vermelha + headline no Summary |

### 7.5 Natureza do alerta

**Não é breach.** Não dispara fluxo do `risk-manager`. É **sinalização** — serve para o gestor revisar no Morning Call se o alinhamento é intencional ou se virou risco involuntário de concentração direcional.

---

## 8. Exibição no relatório

### 8.1 EVOLUTION → Diversificação

Ordem do tab (top-to-bottom):

1. **Camada 4 — alerta combinado** (caixa destacada, cor ~ n_lit)
2. **Camada 2 — Diversification Benefit** (ratio + sparkline 252d)
3. **Camada 1 — Utilização histórica** (tabela × estratégia)
4. **Camada 3 — Correlação realizada** (matriz 3×3 + lista de pares + alerta de filtro)
5. **Matriz Direcional** (tabela CATEGORIA × {Sist, Disc, %PL, Estado})

### 8.2 Summary

Headline no topo do Summary quando n_lit ≥ 1 (amarelo se 1-2, vermelho se ≥ 3), com link direto para o tab da Diversificação.

---

## 9. Limitações e caveats conhecidos

### 9.1 CREDITO — cotas júnior

O VaR do CREDITO pode ser distorcido por cotas júnior de FIDCs (ver [CREDITO_TREATMENT.md](CREDITO_TREATMENT.md)). Mitigação: winsorização causal robusta (§4.3). **Em teoria**, toda análise que envolve `VaR_CREDITO` carrega esse caveat. Em prática, a winsorização é efetiva para os spikes observados.

### 9.2 Históricos curtos

Percentis computados com < 126d de histórico são **provisórios** (ruidosos). Ocorrem em:
- LIVROs/estratégias novos (ex. bucket criado recentemente)
- Retornos da janela 252d que contêm gaps de dados

Quando identificado, o número é exibido mas marcado com flag `(janela curta)`.

### 9.3 Percentis são descritivos, não prescritivos

A metodologia diz "esta é a posição no histórico recente". Não diz "reduza risco". A decisão continua com o gestor — o alerta é input para discussão, não gatilho automático de redução.

### 9.4 Camada 3 — CREDITO e EVO_STRAT excluídos

A correlação realizada só considera MACRO × SIST × FRONTIER. CREDITO tem PnL potencialmente distorcido por cotas júnior; EVO_STRAT/CAIXA não são direcionais. Alinhamento entre essas e as direcionais **não** dispara a condição 4.

### 9.5 Matriz Direcional — categorias

As categorias vêm da tabela `RISK_DIRECTION_REPORT` como estão no banco. Mudanças na taxonomia upstream podem desalinhar os thresholds. Revisitar a cada 6 meses ou quando aparecer CATEGORIA nova.

---

## 10. Referências

- [`evolution_diversification_card.py`](../evolution_diversification_card.py) — fonte única de compute (fetches + lógica)
- [`generate_risk_report.py`](../generate_risk_report.py) — render functions (`_evo_render_*`) + wiring no relatório
- [`.claude/skills/evolution-risk-concentration/SKILL.md`](../.claude/skills/evolution-risk-concentration/SKILL.md) — especificação original
- [`.claude/skills/evolution-risk-concentration/assets/livros-map.json`](../.claude/skills/evolution-risk-concentration/assets/livros-map.json) — taxonomia canônica LIVRO → estratégia
- [`CREDITO_TREATMENT.md`](CREDITO_TREATMENT.md) — justificativa da winsorização
- `q_models.RISK_DIRECTION_REPORT` — fonte da Matriz Direcional
- `LOTE45.LOTE_BOOK_STRESS_RPM` (LEVEL=2, TREE='Main_Macro_Gestores') — VaR por estratégia
- `LOTE45.LOTE_FUND_STRESS_RPM` (LEVEL=10) — VaR agregado do Evolution
- `q_models.REPORT_ALPHA_ATRIBUTION` (FUNDO='EVOLUTION') — PnL diário por LIVRO

---

## 11. Changelog

| Data | Mudança |
|---|---|
| 2026-04-20 | MVP — Camadas 1/2/3 entregues standalone |
| 2026-04-21 | Filtro de significância na Camada 3 (ambas estratégias ≥ P70) |
| 2026-04-21 | Matriz Direcional (RISK_DIRECTION_REPORT) — 5ª camada |
| 2026-04-21 | Camada 4 — alerta combinado bull market alignment + headline no Summary |
| 2026-04-21 | Doc de metodologia inicial |

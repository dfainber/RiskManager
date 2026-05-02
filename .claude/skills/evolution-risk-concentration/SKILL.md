---
name: evolution-risk-concentration
description: Análise de concentração e diversificação do Evolution FIC FIM CP. 3 camadas: (1) utilização histórica de VaR por estratégia em percentis próprios; (2) diversification benefit (correlação entre estratégias reduzindo VaR agregado); (3) correlação realizada rolling entre PnLs das estratégias. Foco em detectar "bull market alignment". Use para pedidos sobre concentração, diversificação, alinhamento direcional, percentil VaR, ou briefing do Evolution.
---

# Evolution Risk Concentration

O Galapagos Evolution é um fundo multiestratégia. Em regime normal, suas cinco estratégias operacionais (MACRO, SIST, FRONTIER, CREDITO, EVO_STRAT) rodam descorrelacionadas e o VaR agregado é menor que a soma das partes — essa é a justificativa da alocação.

O **risco oculto** é o alinhamento direcional em regimes de bull market: SIST fica risk-on, MACRO fica long, FRONTIER (especialmente FCO concentrado) também. A descorrelação esperada desaparece e o fundo fica com risco efetivo muito maior do que parece no VaR linear.

Esta skill existe para **detectar esse cenário antes que ele se realize**. Três camadas de métricas:

| Camada | Pergunta | Métrica |
|--------|----------|---------|
| 1 | Cada estratégia está carregada para seu histórico? | Percentil do VaR em janela de 252d |
| 2 | As estratégias estão se "ajudando" menos via diversificação? | `VaR_real / Σ VaR_estratégias` |
| 3 | As estratégias estão andando juntas? | Correlação rolling 21d/63d de PnL |

**Alerta combinado ("bull market alignment"):** dispara quando ≥ 3 camadas acendem simultaneamente.

**Dependências:**
- `risk-manager` — taxonomia geral
- `glpg_fetch.py` — conexão GLPG-DB01
- Queries base em `risk-daily-monitor/references/queries-mm.md`

---

## ⚠️ Ressalva crítica — VaR de cotas júnior de crédito

**Antes de qualquer análise, ler `references/credito-junior-caveat.md`.**

O fundo tem alocação em estratégia de Crédito Estruturado que pode incluir **cotas júnior** (tranches subordinadas de FIDCs/securitizações). Historicamente, a forma como o Lote45 captura o VaR dessas cotas júnior **já distorceu o VaR agregado do Evolution** em certos momentos — tipicamente subestimando ou superestimando o risco verdadeiro por limitação de marcação a mercado ou de modelagem do instrumento.

**Status atual:** pode ou não ser problema hoje. Nunca assumir que está resolvido.

**Procedimento obrigatório em todo report desta skill:**

1. Verificar se há cotas júnior de crédito na carteira (filtro em `LOTE_BOOK_OVERVIEW` por `PRODUCT_CLASS` relacionado a FIDC/tranche subordinada — critério exato a calibrar quando discutirmos em detalhe)
2. Se houver, reportar no topo do relatório: exposição em %PL, VaR contribuído conforme Lote45
3. **Marcar com flag amarelo de atenção** independentemente do valor. A flag não é alerta de breach, é aviso ao leitor: *"o VaR agregado pode estar distorcido; confirmar com a equipe de risco antes de tirar conclusões finas"*
4. Se a exposição em cotas júnior for material (≥ 5% do PL), **calcular o VaR do fundo excluindo CREDITO** como segunda métrica complementar (ver ajuste nas Camadas 1 e 2 abaixo)

O racional: **melhor reportar um caveat inócuo do que deixar uma distorção passar**. Quando a discussão técnica sobre esse ponto for feita no futuro, atualizar o caveat ou removê-lo.

---

## Composição-alvo do fundo (peso-referência)

| Estratégia | Peso típico | Livros no banco (`LIVRO`) |
|-----------|-------------|---------------------------|
| MACRO | ~48% | CI, Macro_JD, Macro_LF, Macro_RJ, Macro_FG, Macro_AC, Macro_MD, Giro_Master, LF_RV-BZ_SS |
| SIST | ~17% | Bracco, Quant_PA, SIST_FX, SIST_GLOBAL, SIST_COMMO, SIST_RF |
| FRONTIER | ~5% | FLO (direcional), FMN (risk neutral), FCO (direcional concentrado) |
| CREDITO | 15–30% | Crédito, GIS FI FUNDO IMOBILIÁRIO, GIS CUSTOS E PROVISÕES |
| EVO_STRATEGY | balance | Macro_DF, Baltra_DI, FX |
| CAIXA | residual | Caixa, Caixa USD, Taxas e Custos, RV BZ |

Peso sempre medido como **% do PL via VaR contribuído**, não posição bruta.

Lista de livros em `assets/livros-map.json` (extraída do próprio script EVOLUTION). Atualizar quando gestão adicionar novos livros — o script original tem tratamento para `livros novos`, manter consistência.

## Camada 1 — Utilização histórica por estratégia

### Cálculo

Para cada estratégia `s`:

```
VaR_hoje_s = VaR contribuído da estratégia no dia (bps)
Série_252d_s = VaR_s em cada um dos últimos 252 dias úteis
Percentil_s = posição de VaR_hoje_s na Série_252d_s (em %, 0-100)
```

Fonte: `LOTE45.LOTE_BOOK_STRESS_RPM` com `LEVEL=2`, `TREE='Main_Macro_Gestores'`, agregado pela coluna `BOOK` (que já vem com EVO_STRAT, SIST, FRONTIER, MACRO, CREDITO, CAIXA). Query completa em `references/queries-evolution.md`.

**Ajuste por cotas júnior** (quando flag da ressalva estiver ativa): calcular também o percentil da estratégia CREDITO excluindo as posições em cotas júnior, se isolável. Reportar os dois valores lado a lado.

### Apresentação

```
**Camada 1 — Utilização histórica (VaR, janela 252d)**

| Estratégia | VaR hoje (bps) | Percentil 252d | Estado  |
|-----------|----------------|----------------|---------|
| MACRO     |     42.1       |      P78       | 🟡      |
| SIST      |     18.3       |      P85       | 🟡      |
| FRONTIER  |      6.2       |      P65       | 🟢      |
| CREDITO   |     12.0       |      P52       | 🟢 ⚠️   |
| EVO_STRAT |      8.8       |      P70       | 🟢      |
| CAIXA     |      1.1       |      P45       | 🟢      |
| ---       | ---            | ---            | ---     |
| TOTAL     |     88.5       |      P88       | 🟡      |

⚠️ CREDITO: exposição em cotas júnior pode distorcer. Ver caveat.

Simultaneamente acima de P70: MACRO, SIST, EVO_STRAT (3 estratégias)
```

### Estados individuais

| Percentil | Estado |
|-----------|--------|
| < 50 | 🟢 Verde |
| 50-70 | 🟢 Verde (normal) |
| 70-85 | 🟡 Amarelo |
| 85-95 | 🔴 Vermelho |
| > 95 | ⚫ Extremo |

**Camada 1 sozinha não dispara alerta do fundo** — uma estratégia no P90 pode ser escolha consciente. Sinal: **quantas simultaneamente acima de P70**.

## Camada 2 — Diversification benefit

### Cálculo

```
VaR_soma = Σ_s VaR_hoje_s      (soma linear; hipótese de correlação 1)
VaR_real = VaR_fundo            (de LOTE_FUND_STRESS_RPM — inclui correlações)
Ratio = VaR_real / VaR_soma     (entre 0 e 1)
```

Interpretação:
- `Ratio = 0.6` → diversificação reduz 40% do VaR linear (bom)
- `Ratio = 0.9` → estratégias quase andando juntas (ruim)
- `Ratio = 1.0` → como se fossem um fundo único (alarmante)

### Contexto histórico

Série histórica do Ratio (janela 252d):
- Ratio hoje
- Percentil 252d
- Ratio médio histórico
- Mínimo e máximo do ano

### Ajuste por cotas júnior

Se flag ativa e exposição em cotas júnior ≥ 5% do PL:

```
Ratio_ex_credito = VaR_real_sem_CREDITO / (VaR_soma − VaR_CREDITO)
```

Reportar ambos. Se `Ratio_ex_credito` e `Ratio` divergirem muito (ex.: 0.80 vs. 0.55), **a distorção do CREDITO está mascarando o diagnóstico** — priorizar o Ratio ex-CREDITO na leitura.

### Apresentação

```
**Camada 2 — Diversification benefit**

VaR soma (linear):    88.5 bps
VaR real do fundo:    52.3 bps
Ratio (real/soma):    0.59  (P32 na janela 252d)

Excluindo CREDITO:
  VaR soma (ex-CRED):  76.5 bps
  VaR real (ex-CRED):  48.1 bps
  Ratio:               0.63  (P38)

Histórico: ratio médio = 0.58, min = 0.41, max = 0.87.

Interpretação: diversificação saudável; CREDITO não está distorcendo o diagnóstico.
```

### Estados

| Percentil do Ratio | Estado |
|---------------------|--------|
| < P60 | 🟢 Diversificação saudável |
| P60-P80 | 🟡 Diversificação abaixo da média |
| P80-P95 | 🔴 Estratégias alinhadas |
| > P95 | ⚫ Fundo efetivamente sem diversificação |

## Camada 3 — Correlação rolling entre estratégias

### Cálculo

PnL **diário** de cada estratégia vem de `q_models.REPORT_ALPHA_ATRIBUTION`, agregando por `LIVRO → estratégia` via `assets/livros-map.json`.

Correlação de Pearson em janelas:
- **21d** (sensível a mudanças recentes)
- **63d** (baseline de médio prazo)

Foco nos 4 pares direcionais: MACRO, SIST, FRONTIER, CREDITO. Ignorar EVO_STRAT e CAIXA (ruidosas ou sem significado direcional).

### Percentil da correlação

Para cada par:
- Histórico da correlação 63d em janela 252d
- Percentil atual nesse histórico
- Média histórica

### Apresentação

```
**Camada 3 — Correlação realizada (PnL diário)**

Matriz 63d (percentil 252d entre parênteses):

|            | MACRO      | SIST       | FRONTIER   | CREDITO     |
|------------|------------|------------|------------|-------------|
| MACRO      |    —       | 0.65 (P88) | 0.48 (P72) | 0.12 (P45)  |
| SIST       |            |    —       | 0.55 (P80) | 0.08 (P40)  |
| FRONTIER   |            |            |    —       | 0.15 (P50)  |
| CREDITO    |            |            |            |    —        |

⚠️ CREDITO: correlações podem ser distorcidas pelo PnL das cotas júnior (caveat).

Observação: MACRO × SIST em P88 = pouco comum na janela — sinal de 
alinhamento direcional recente das duas estratégias.
```

### Estados por par

| Percentil da correlação 63d | Estado |
|---|---|
| < P70 | 🟢 Normal |
| P70-P85 | 🟡 Correlação elevada |
| > P85 | 🔴 Alinhamento atípico |

## Camada 4 — Alerta combinado "Bull Market Alignment"

Dispara quando **≥ 3 condições** são verdadeiras:

1. **Camada 1:** ≥ 3 estratégias simultaneamente ≥ P70
2. **Camada 1 (extremo):** ≥ 1 estratégia ≥ P95
3. **Camada 2:** Diversification Ratio ≥ P80
4. **Camada 3:** ≥ 1 par com correlação ≥ P85
5. **Direcional:** `RISK_DIRECTION_REPORT` mostra `DELTA_SISTEMATICO` e `DELTA_DISCRICIONARIO` com mesmo sinal em ≥ 3 categorias

Condição 5 é a "smoking gun": as duas metades do fundo apontadas na mesma direção.

**Importante:** se a ressalva de cotas júnior estiver ativa E o CREDITO for o causador principal de algum disparo (ex.: correlação MACRO × CREDITO em P90 é o único sinal), **reduzir severidade** e reportar como "alerta sujeito a caveat". Exceto isso, tratar normalmente.

### Se disparar

```
⚠️ BULL MARKET ALIGNMENT DETECTADO ⚠️

3 de 5 condições acesas:
  ✓ 3 estratégias em P>70 simultaneamente (MACRO, SIST, EVO_STRAT)
  ✓ Diversification Ratio em P84
  ✓ MACRO × SIST em P88 na correlação 63d

Recomendação: revisar no Morning Call se o alinhamento é intencional.
```

Não é breach — é sinalização. Propósito: pôr na mesa do gestor.

## Direção por estratégia (tabela `RISK_DIRECTION_REPORT`)

Query (extraída direto do `EVOLUTION_TABLES_GRAPHS.py`):

```sql
SELECT "TIPO", "CATEGORIA", "NOME",
       "DELTA_SISTEMATICO", "DELTA_DISCRICIONARIO", "PCT_PL_TOTAL"
FROM q_models."RISK_DIRECTION_REPORT"
WHERE "FUNDO" = 'Galapagos Evolution FIC FIM CP'
  AND "VAL_DATE" = '{dia_atual}'
```

Tabela **crítica**: mostra, por ativo/categoria, o Delta do SIST e do DISCRICIONARIO (MACRO + FRONTIER) separados. Se ambos apontam na mesma direção → alinhamento confirmado posição-a-posição.

**Uso:** para cada CATEGORIA (RV-BZ, FX-BRL, RF-BZ, etc.), contar em quantas os dois deltas têm o mesmo sinal. ≥ 3 categorias → condição 5 da Camada 4 acende.

## Estrutura do relatório final

```
# Evolution Risk Concentration — [data-base]

## Ressalva
[Flag amarelo sobre cotas júnior de crédito se exposição > 0]
[Exposição atual em %PL + VaR contribuído + recomendação de leitura]

## Alerta
[Se Camada 4 dispara: box no topo]
[Se não: "✅ Sem sinais de concentração excessiva hoje."]

## Camada 1 — Utilização histórica
[Tabela com VaR, percentil, estado, por estratégia]

## Camada 2 — Diversification benefit
[Ratio hoje + histórico + ajuste ex-CREDITO se aplicável]

## Camada 3 — Correlação entre estratégias
[Matriz 63d e 21d com percentis]

## Camada 4 — Detalhes do alerta combinado
[Condições acesas e apagadas]

## Direção posição-a-posição
[Tabela RISK_DIRECTION_REPORT destacando CATEGORIAs com sinal alinhado]

## Notas
- Janela histórica: 252d úteis
- Cálculos em bps (%PL × 10000)
- AUM base: [valor]
- [Caveat de cotas júnior se aplicável]
```

## Regras de comportamento

- **Nunca substituir o VaR agregado oficial** pela soma linear — a soma (Camada 2) é ferramenta analítica, não a "verdade"
- **Sempre rodar o check de cotas júnior primeiro** — antes de qualquer número, verificar se a ressalva se aplica
- **Percentis são descritivos, não prescritivos** — relatório não diz "reduza risco"; diz "está nesta posição do histórico"
- **Camada 4 = sinalização, não gatilho de breach** — nunca acione fluxo de `risk-manager` a partir daqui
- **Históricos curtos (< 126d) devem ser flag-ados** — percentil calculado com 80d de dados é ruidoso; reportar como "percentil provisório"
- **Três camadas juntas > cada uma sozinha** — não reportar só uma camada isolada a menos que usuário peça explicitamente

## Referências

- `references/credito-junior-caveat.md` — detalhes da ressalva e procedimento de ajuste
- `references/queries-evolution.md` — SQL completas para as três camadas + `RISK_DIRECTION_REPORT`
- `references/metodologia-percentis.md` — como calcular percentis quando o histórico é curto; tratamento de outliers
- `assets/livros-map.json` — mapa `LIVRO → estratégia` (extraído do script EVOLUTION)

## Skills relacionadas

- **`risk-manager`** — taxonomia
- **`risk-daily-monitor`** — monitor agregado do fundo (complementa)
- **`macro-risk-breakdown`** — análise detalhada dentro do MACRO (é uma parte do Evolution)
- **`risk-morning-call`** (a criar) — agrega o output desta skill no briefing

## Roadmap

1. **Agora:** 3 camadas + alerta combinado + caveat de cotas júnior como flag
2. **Próximo:** calibrar percentis de alerta após 1-2 meses de uso real
3. **Quando carrego de MACRO estiver pronto:** integrar utilização de orçamento dos PMs do MACRO na visão do Evolution
4. **Futuro:** fechar o caveat de cotas júnior — discussão técnica pendente sobre como marcar a mercado ou usar proxy de risco
5. **Eventual:** estender abordagem de percentil histórico para outros fundos multiestratégia se surgirem (hoje só Evolution)

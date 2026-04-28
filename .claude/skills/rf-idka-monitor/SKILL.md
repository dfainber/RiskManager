---
name: rf-idka-monitor
description: Monitor de risco dos IDKA IPCA 3Y/10Y FIRF. Cobre BVaR/tracking error, qualidade da réplica (Benchmark_IDKA vs índice), risco ativo via book RF_LF (que pode dirigir Albatroz), perda relativa mensal, critérios de redução. Métrica canônica ANO_EQ; DV01 por instrumento (NTN-B/LFT/DI1). Use para pedidos sobre IDKA, BVaR/tracking error do IDKA, desvio do benchmark, risco ativo, alocação Albatroz, ou réplica.
---

# RF IDKA Monitor

Monitor de risco e performance relativa para os fundos RF benchmarked:

| Fundo | TRADING_DESK | Benchmark |
|-------|--------------|-----------|
| IDKA 3Y | `IDKA IPCA 3Y FIRF` | IDKA IPCA 2A (Anbima) |
| IDKA 10Y | `IDKA IPCA 10Y FIRF` | IDKA IPCA 10A (Anbima) |

O desafio do RF benchmarked é **diferente** do MM: não é limitar perda absoluta, é gerir o **desvio vs. benchmark**. Três camadas de risco devem ser monitoradas separadamente:

1. **Risco total** vs. benchmark (BVaR / tracking error)
2. **Qualidade da réplica** — o quão fiel é o book `Benchmark_IDKA` ao índice oficial
3. **Risco ativo** — apostas no book `RF_LF`, que podem incluir alocação em Albatroz

**Dependências:**
- `risk-manager` — taxonomia, semáforo
- `glpg-data-fetch` — conexão GLPG-DB01
- Biblioteca `GLPG_Public_Bonds` (PB.NTNB, PB.LFT) — cálculo de DV01 por instrumento

---

## Taxonomia: três books, três naturezas

No banco, o IDKA tem três valores de BOOK que importam:

| BOOK | Natureza | O que representa |
|------|----------|------------------|
| `Benchmark_IDKA` | Réplica do índice | Posições que o fundo carrega para replicar o IDKA oficial |
| `RF_LF` | Risco ativo | "Renda Fixa Livre" — apostas ativas (incluindo eventual alocação via Albatroz) |
| `ALL` (`*`) | Agregado | Fundo inteiro (benchmark + ativo) |

Essa separação é **operacional** — os três books existem porque as três métricas precisam ser avaliadas separadamente:

```
Perda efetiva = Perda de réplica (Benchmark_IDKA ≠ índice oficial)
              + Risco ativo (RF_LF apostou e errou)
```

## Camada 1 — Risco Total vs. Benchmark

### BVaR (Benchmark VaR / VaR Relativo)

O BVaR é o **VaR do tracking error**, não o VaR absoluto. Capturado direto na tabela `LOTE45.LOTE_PARAMETRIC_VAR_TABLE`, coluna `RELATIVE_VAR`.

Query:

```sql
SELECT SUM("RELATIVE_VAR") AS "RELATIVE_VAR", "BOOKS", "TRADING_DESK"
FROM "LOTE45"."LOTE_PARAMETRIC_VAR_TABLE"
WHERE "VAL_DATE" = '{dia_atual}'
  AND "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
GROUP BY "BOOKS", "TRADING_DESK"
```

Normalização (conforme script original):

```python
df['BVAR_BPS'] = df['RELATIVE_VAR'] * -10000 / aum_do_fundo
```

**Três valores de BVaR por fundo** (um por BOOK):
- `BVaR_ALL` — BVaR do fundo total (desvio esperado vs. índice)
- `BVaR_Benchmark_IDKA` — BVaR da réplica (idealmente próximo a zero)
- `BVaR_RF_LF` — BVaR do risco ativo (isolado)

### DV01 Ativo vs. Benchmark

Além do BVaR, reportar:

- **DV01 do book `Benchmark_IDKA`** — a sensibilidade "fotocópia" do índice
- **DV01 do book `RF_LF`** — a aposta ativa em duration
- **Diferença** — quanto o fundo está long ou short em duration vs. o índice

Se o book `RF_LF` tem DV01 positivo significativo, o fundo está **longo em duration** vs. benchmark (aposta que taxas caem).

### Apresentação (por fundo)

```
**IDKA IPCA 3Y FIRF — Risco Total**

Data-base: YYYY-MM-DD
AUM: R$ XX MM

BVaR por book (VaR do tracking error, bps):
  ALL (fundo total):          12.3 bps
  Benchmark_IDKA (réplica):    2.1 bps
  RF_LF (risco ativo):        11.8 bps

Interpretação:
  - Desvio esperado 1d do fundo vs. índice: ~12 bps (99% confiança)
  - Réplica do benchmark consome 2 bps — baixo (boa réplica)
  - Risco ativo é a fonte dominante do BVaR (11.8 de 12.3)

DV01 ativo (RF_LF): +R$ XX / bp
  → Fundo está longo em duration vs. índice em Y ano-equivalente
```

---

## Camada 2 — Qualidade da Réplica

O book `Benchmark_IDKA` deveria replicar perfeitamente o índice. Imperfeições vêm de:
- Papéis disponíveis no mercado vs. composição oficial do índice
- Saldo de caixa operacional
- Timing de rebalanceamento

### Métricas

1. **BVaR_Benchmark_IDKA isoladamente** (já calculado acima). Em tese, baixo. Se crescer, réplica está piorando.

2. **Perfil de exposição (ANO_EQ por vértice):**
   - Para cada vértice (BDAYS), comparar `ANO_EQ` do book `Benchmark_IDKA` contra o perfil esperado do índice oficial
   - Calcular DV01 a partir dos instrumentos (NTN-B via `PB.NTNB`, LFT via `PB.LFT`, DI1 manual)

3. **Distância do perfil ideal** (ver nota abaixo sobre "melhor réplica")

### Sobre "melhor réplica possível"

O script atual **compara o book `Benchmark_IDKA` contra ele mesmo** — não tem benchmark externo explícito para medir "desvio da réplica ideal".

**Ação necessária na primeira execução:** decidir a metodologia. Duas opções:

- **Opção A (simples):** assumir que o perfil carregado em `Benchmark_IDKA` *é* a réplica oficial da Anbima. Apenas monitorar se o BVaR dele cresce ao longo do tempo.
- **Opção B (rigorosa):** obter o perfil oficial do índice IDKA (da Anbima) e calcular o gap entre o perfil do book e o perfil oficial. Gap = medida de "slippage da réplica".

**Registrar a decisão em `assets/politica-idka.json`.** Este é um dos pontos pendentes a discutir com a gestão.

### Apresentação

```
**Qualidade da réplica — IDKA 3Y**

BVaR do Benchmark_IDKA: 2.1 bps (stable vs. D-1)
ANO_EQ total: 2.98 (target: 3.00 para IDKA 2A)

Perfil por vértice (ANO_EQ):
| BDAYS  | ANO_EQ | Nota          |
|--------|--------|---------------|
| 252    |  0.45  | curto         |
| 504    |  0.82  |               |
| 756    |  0.95  | principal     |
| 1008   |  0.51  | longo         |
| 1260+  |  0.25  | muito longo   |

⚠️ Metodologia de gap vs. perfil oficial pendente (ver politica-idka.json)
```

---

## Camada 3 — Risco Ativo (RF_LF + Albatroz)

O book `RF_LF` é onde está a "alpha engine" dos IDKAs. Aqui entram:

- Posições direcionais próprias
- Alocação no fundo Albatroz (se houver)
- Arbitragens entre vértices da curva

### Métricas

1. **BVaR_RF_LF** — capturado pela query da Camada 1
2. **DV01 e ANO_EQ do book `RF_LF`** — direção e magnitude da aposta
3. **Composição por instrumento** — quais NTN-Bs, LFTs, DIs estão alocados
4. **Alocação em Albatroz** — se o fundo Albatroz aparece como PRODUCT dentro do book RF_LF

### Identificação da alocação em Albatroz

**A confirmar na primeira execução:** como o Albatroz aparece na tabela `LOTE_PRODUCT_BOOK_POSITION_PL` do IDKA. Hipóteses:

- `PRODUCT_CLASS` = 'FIC' ou 'Multiclass'
- `PRODUCT` contém "Albatroz" ou ticker do fundo
- Tratamento especial: posição em outro fundo tem DV01 calculado diferente (look-through)

Registrar o filtro calibrado em `assets/politica-idka.json`.

### Apresentação

```
**Risco ativo — IDKA 3Y**

BVaR do RF_LF: 11.8 bps (95% do BVaR total)
DV01 do RF_LF: +R$ 42k / bp
ANO_EQ do RF_LF: +0.15 (fundo longo 0.15 ano em duration vs. índice)

Composição:
  - NTN-B 2029: +R$ 4.2M (ANO_EQ +0.08)
  - NTN-B 2032: +R$ 3.1M (ANO_EQ +0.05)
  - Albatroz (look-through): R$ 1.2M (ANO_EQ +0.02)

⚠️ Alocação em Albatroz: 1.2% do PL. Monitorar concentração.
```

---

## Camada 4 — Controle de Perda Relativa (Mensal)

Equivalente ao "stop mensal" do MACRO, mas **em termos relativos ao benchmark**.

### Cálculo

Para cada fundo:

```
PnL_relativo_mes = PnL_fundo_mes - PnL_benchmark_mes   (ambos em bps)
```

Onde:
- `PnL_fundo_mes` = variação da cota no mês (`q_models.REPORT_ALPHA_ATRIBUTION` ou cota direto)
- `PnL_benchmark_mes` = retorno do índice IDKA oficial no mesmo período

**Fonte do benchmark:** Anbima publica retornos diários do índice. A skill precisa acessar essa fonte (via `ECO_INDEX` no banco, se disponível, ou via API Anbima).

### Orçamento de perda relativa

**⚠️ Pendente.** Não existe orçamento formalizado para TE. Até que a gestão defina, a skill reporta o PnL relativo do mês mas não aplica semáforo.

**Propostas iniciais para discussão:**

| Abordagem | Lógica |
|-----------|--------|
| Absoluta (bps) | Stop mensal fixo, ex.: −30 bps para IDKA 3Y, −60 bps para 10Y |
| Múltiplo de BVaR | Stop = k × BVaR (ex.: k=5 → stop = 5 × BVaR diário) |
| Percentual do target | Se target alpha é X bps/ano, stop mensal é Y% de X/12 |

Decidir com a gestão. Até lá, reportar PnL relativo com flag "sem orçamento formal".

### Apresentação

```
**Controle de Perda Relativa — IDKA 3Y**

Mês corrente: 2026-04 (até dia YYYY-MM-DD)

PnL fundo MTD:       +32.1 bps
PnL IDKA 2A MTD:     +28.5 bps
Alpha MTD:           +3.6 bps  🟢

Observação: ⚠️ Sem orçamento formal de perda relativa (pendente de definição pela gestão).
```

---

## Camada 5 — Critérios de Redução de Risco

**Pendentes de definição.** Pontos para discutir com a gestão:

1. **Gatilho absoluto:** se PnL relativo MTD atinge −X bps, reduzir BVaR em Y%
2. **Gatilho relativo:** se BVaR total passa Z bps, revisar apostas ativas
3. **Gatilho qualitativo:** se réplica está deteriorando (BVaR do Benchmark_IDKA sobe), revisar execução

Até a política ser formalizada, a skill apenas **reporta os indicadores** e sinaliza condições que plausivelmente disparariam redução — sem assumir regra específica.

---

## Estrutura do relatório final (por fundo)

```
# IDKA Monitor — [IDKA 3Y / IDKA 10Y] — [data-base]

## Sumário
- BVaR total: X bps
- Alpha MTD: Y bps
- Fontes principais de risco: [book RF_LF dominante? Albatroz? Posição específica?]
- Pontos de atenção: [flags, se houver]

## Camada 1 — Risco Total vs. Benchmark
[Tabela com BVaR por book, DV01 ativo, direção em duration]

## Camada 2 — Qualidade da Réplica
[BVaR do Benchmark_IDKA, perfil ANO_EQ por vértice]
[Se metodologia de gap estiver calibrada: comparação vs. perfil oficial]

## Camada 3 — Risco Ativo (RF_LF + Albatroz)
[BVaR e DV01 do RF_LF, composição, destaque Albatroz se presente]

## Camada 4 — Perda Relativa MTD
[PnL fundo vs. PnL benchmark, alpha MTD, histórico do mês]
[Flag "sem orçamento formal" se pendente]

## Camada 5 — Sinais para possível redução
[Apenas se algum indicador estiver em nível de atenção, sem gatilho automático]

## Pendências da política
- [Se perfil de "melhor réplica" não calibrado: nota]
- [Se filtro de Albatroz não calibrado: nota]
- [Se orçamento de perda relativa não definido: nota]
```

Como são **dois fundos** (3Y e 10Y), a skill produz dois relatórios em sequência. Morning Call final pode mostrar lado a lado.

---

## Regras de comportamento

- **Nunca aplicar orçamento default** — se não há política formal, reportar sem semáforo
- **Sempre separar os três books** (`Benchmark_IDKA`, `RF_LF`, `ALL`) — são naturezas diferentes, não agregar
- **DV01 calculado, não lido** — usar `GLPG_Public_Bonds` (PB.NTNB, PB.LFT) para NTN-B e LFT; cálculo manual para DI1 (conforme script original)
- **Alocação em Albatroz é de segunda ordem** — mencionar quando presente, mas não é o foco da skill. Se Albatroz virar peça central, criar skill dedicada depois
- **Calendário Anbima** em tudo
- **ANO_EQ é a métrica canônica** — sempre reportar exposição em ano-equivalente, não só DV01 em reais

---

## Referências

- `references/queries-idka.md` — SQL completo extraído de `IDKA_TABLES_GRAPHS.py`, com explicações
- `references/ano-eq-methodology.md` — definição formal do ANO_EQ e fórmulas de DV01 por instrumento
- `references/politica-idka-pendente.md` — lista das 3 decisões de política pendentes (réplica, Albatroz, orçamento de perda relativa)

## Assets

- `assets/politica-idka.json` — configurações calibráveis (filtros, thresholds); **placeholder** até política ser definida
- `assets/idka-ticker-map.json` — mapa entre nomes curtos (`IDKAIPCAY3`) e longos (`IDKA IPCA 3Y FIRF`) que aparecem no banco

## Skills relacionadas

- `risk-manager` — framework geral
- `risk-daily-monitor` — monitor agregado (IDKA será integrado quando esta skill estiver estável)
- `rf-albatroz-monitor` (a criar se e quando Albatroz tornar-se material) — análise do fundo Albatroz isoladamente
- `performance-attribution` (próxima a criar) — decomposição dos retornos, relevante para entender fontes do alpha dos IDKAs

## Roadmap

1. **Agora:** cinco camadas operacionais reportando os indicadores
2. **Definir com gestão:** metodologia de "melhor réplica", identificação de Albatroz, orçamento de perda relativa
3. **Após decisões:** aplicar semáforos e gatilhos na Camada 5
4. **Futuro:** se Albatroz ganhar peso, criar skill dedicada com look-through para as posições do próprio Albatroz

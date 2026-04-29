---
name: performance-attribution
description: Atribuição de performance (PA) transversal aos fundos. MACRO, EVOLUTION, ALBATROZ, MACRO_Q, QUANT, BALTRA usam REPORT_ALPHA_ATRIBUTION (decompõe por livro/PM/classe/fator). IDKA 3Y/10Y constrói PA do zero — excess return vs índice IDKA, decomposto entre réplica (Benchmark_IDKA) e risco ativo (RF_LF). Use para pedidos sobre atribuição, alpha, excess return, performance por PM/livro, ou fontes de retorno dos fundos.
---

# Performance Attribution

Análise de atribuição de performance — transversal aos fundos da gestora. Complementa as skills de risco: onde elas respondem "onde está o risco?", esta responde "de onde veio o retorno?".

**Dependências:**
- `glpg-data-fetch` — conexão GLPG-DB01
- `risk-manager` — convenções gerais
- `rf-idka-monitor` — para o módulo IDKA

---

## Escopo: dois regimes de PA

### Regime A — PA existente (consumir)

A tabela `q_models.REPORT_ALPHA_ATRIBUTION` já contém PA oficial para:

| Fundo | Nome oficial | Chave sugerida | Valor `FUNDO` na tabela |
|-------|--------------|----------------|-------------------------|
| MACRO | Galapagos Macro FIM | `MACRO` | ✅ `'MACRO'` confirmado |
| EVOLUTION | Galapagos Evolution FIC FIM CP | `EVOLUTION` | ✅ `'EVOLUTION'` confirmado |
| ALBATROZ | GALAPAGOS ALBATROZ FIRF LP | `GLPG_ALBA` | ⚠️ a confirmar |
| QUANTITATIVO MACRO Q | (a confirmar) | (a confirmar) | ⚠️ a confirmar |

Para esses, a skill **lê, consolida e apresenta**. Não reimplementa.

### Regime B — PA a construir (IDKA 3Y e 10Y)

Os IDKAs são **benchmarked** e não têm PA oficial. A skill **constrói** a atribuição:
- Excess return vs. benchmark oficial (IDKA IPCA 2A / 10A Anbima)
- Decomposição réplica (`Benchmark_IDKA`) vs. risco ativo (`RF_LF`)
- Contribuição por vértice da curva (granularidade futura)

---

## Pontos pendentes antes da operação completa

1. **FUND_KEY_MAP** — usuário tem arquivo central (mencionado na sessão de 13/abr). Quando disponibilizado, integrar em `assets/fund-key-map.json`
2. **Valor exato do campo `FUNDO`** na `REPORT_ALPHA_ATRIBUTION` para ALBATROZ e QUANT MACRO Q
3. **Fonte Anbima do retorno IDKA oficial** — via `ECO_INDEX` ou outra via

Enquanto pendentes, skill opera plenamente para MACRO e EVOLUTION, e marca os demais como "aguardando confirmação".

---

## Regime A — Consumir PA existente

### Query-base

```sql
SELECT "DATE", "LIVRO", "BOOK", "CLASSE", "GRUPO",
       "PRODUCT_CLASS", "PRODUCT", "DIA", "MES"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = '{fundo_chave}'
  AND "DATE" BETWEEN '{data_inicio}' AND '{dia_atual}'
```

Para descobrir os valores exatos de `FUNDO`:

```sql
SELECT DISTINCT "FUNDO", COUNT(*) AS linhas
FROM q_models."REPORT_ALPHA_ATRIBUTION"
GROUP BY "FUNDO"
ORDER BY linhas DESC;
```

Normalização:
```python
df['DIA_BPS'] = df['DIA'] * 10000
df['MES_BPS'] = df['MES'] * 10000
```

### Dimensões de atribuição

**1. Por LIVRO (PM ou estratégia interna)**
```python
pa_por_pm = df.groupby('LIVRO').agg({'DIA_BPS': 'sum', 'MES_BPS': 'sum'})
```

**2. Por BOOK (classe de ativo)**
```python
pa_por_book = df.groupby('BOOK').agg({'DIA_BPS': 'sum', 'MES_BPS': 'sum'})
```

**3. Por FATOR_RISCO**
Aplicar `classificar_fator_risco` (extraída de `EVOLUTION_TABLES_GRAPHS.py`). Mapa em `assets/fator-risco-map.json`.

```python
df['FATOR_RISCO'] = df.apply(classificar_fator_risco, axis=1)
pa_por_fator = df.groupby('FATOR_RISCO').agg({'DIA_BPS': 'sum', 'MES_BPS': 'sum'})
```

### Horizontes padrão

Sempre: **Dia, MTD, QTD, YTD**. SI só quando pedido.

### Destaques (detecção automática)

**Positivos:**
- Contribuição > +5 bps no dia OU > +20 bps MTD
- OU > 30% do PnL total (dominante)

**Negativos:**
- Contribuição < −5 bps no dia OU < −20 bps MTD
- OU > 30% do detrator total

Thresholds configuráveis em `assets/destaque-thresholds.json`.

### Apresentação (exemplo MACRO)

```
**PA — MACRO — 2026-04-16**

Dia:  +18.3 bps   |   MTD: +42.5 bps   |   YTD: +180.2 bps

───── Por LIVRO (PM) ─────
         Dia     MTD    YTD
  CI    +6.1   +15.2  +48.1
  LF    +4.2   +12.8  +52.3
  JD    +3.8    +8.1  +35.0
  RJ    +2.9    +4.3  +22.5

───── Por BOOK ─────
  RF-BZ    +8.2  +22.1  +95.0
  FX-BRL   +4.5  +10.3  +38.2
  COMMOD.  +2.8   +6.5  +22.0
  ...

───── Destaques do dia ─────
  ✓ RF-BZ dominou (+8.2 bps = 45% do PnL); CI principal
  ✓ FX-BRL segundo (+4.5 bps); JD em BRLUSD
  ⚠ RV-EM detrator (−1.2 bps); LF em MXN

───── Destaques MTD ─────
  ✓ RF-BZ sustentando o mês (52% do PnL mensal)
  ✓ CI é o contribuidor dominante (36% MTD)
```

---

## Regime B — Construir PA para IDKA

### Metodologia

Decompor o retorno em quatro componentes:

```
R_fundo = R_benchmark_oficial + ER_replica + ER_ativo + resíduo
```

Onde:
- `R_fundo` — retorno da cota
- `R_benchmark_oficial` — retorno IDKA Anbima (2A ou 10A)
- `ER_replica` — ER gerado por imperfeição da réplica (book `Benchmark_IDKA` ≠ índice)
- `ER_ativo` — ER das apostas ativas (book `RF_LF`)
- `resíduo` — custos, taxas, efeitos não categorizáveis

### Queries

**R_fundo** (cota do IDKA):
```sql
SELECT "VAL_DATE", "TRADING_DESK", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
  AND "VAL_DATE" BETWEEN '{data_inicio}' AND '{dia_atual}'
ORDER BY "TRADING_DESK", "VAL_DATE"
```

Converter para retorno diário via `pct_change()`.

**R_benchmark_oficial** — fonte a confirmar:

```sql
SELECT DISTINCT "INSTRUMENT"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" ILIKE '%IDKA%';
```

**ER_replica e ER_ativo:**

Aproximação inicial:
```python
# Retornos realizados por book (via REPORT_ALPHA_ATRIBUTION ou similar)
ret_book_benchmark = df_pnl_idka[df_pnl_idka['BOOK'] == 'Benchmark_IDKA']['DIA'].sum()
ret_book_rf_lf    = df_pnl_idka[df_pnl_idka['BOOK'] == 'RF_LF']['DIA'].sum()

# ER_replica = o que o book Benchmark_IDKA gerou acima/abaixo do esperado dado o movimento do índice
ret_esperado_benchmark = R_benchmark_oficial * (ano_eq_book / ano_eq_oficial)
ER_replica = ret_book_benchmark - ret_esperado_benchmark

# ER_ativo = tudo que o RF_LF gerou
ER_ativo = ret_book_rf_lf
```

**Limitação:** aproximação assume que `Benchmark_IDKA` pretende replicar exatamente o índice. Se houver desvio intencional de alocação, o cálculo não separa os dois. Ajustar após confirmação metodológica com gestão.

### Apresentação IDKA

```
**PA — IDKA 3Y — 2026-04-16**

R_fundo (dia):           +18.5 bps
R_IDKA 2A Anbima (dia):  +17.2 bps
Excess Return (dia):     +1.3 bps

Decomposição do ER do dia:
  ER Réplica:   +0.2 bps   (Benchmark_IDKA vs. índice)
  ER Ativo:     +1.1 bps   (RF_LF)
    └─ Principal: posição longa em NTN-B 2029

Excess Return MTD:  +4.2 bps
Excess Return YTD:  +18.5 bps

Composição MTD:
  ER Réplica: +0.5 bps (bom, réplica estável)
  ER Ativo:   +3.7 bps (apostas em duration média gerando alpha)
```

---

## Estrutura do relatório final

```
# Performance Attribution — [data-base]

## Sumário
- Fundos cobertos: [lista]
- Destaque do dia: [melhor contribuidor cross-fundos]
- Alerta do dia: [maior detrator, se material]

## Por Fundo (Regime A)
### MACRO
[Tabela por LIVRO, BOOK, FATOR + destaques]

### EVOLUTION
[Idem]

### ALBATROZ (quando confirmado)
### QUANTITATIVO MACRO Q (quando confirmado)

## Por Fundo (Regime B — IDKAs)
### IDKA 3Y
[Retorno, ER decomposto, fontes do ER]

### IDKA 10Y
[Idem]

## Pendências
- [Valores de FUNDO a confirmar]
- [Fonte Anbima a confirmar]
```

---

## Regras de comportamento

- **Nunca inventar atribuição** — zero se não tem dado
- **Destaques são descritivos, não prescritivos** — apontar, não recomendar ação
- **Separar Regime A e B** — não misturar metodologias
- **FUND_KEY_MAP é a fonte de verdade** dos tickers quando disponibilizado
- **Horizontes default:** Dia + MTD + YTD. QTD e SI só sob pedido
- **Em caso de dúvida de FUNDO**, rodar o `SELECT DISTINCT` primeiro

---

## Integração com outras skills

### Com `macro-stop-monitor`
PnL mensal por PM já lido lá. **Não duplicar query** — consumir resultado.

### Com `macro-risk-breakdown`
PA e breakdown usam a mesma fonte (REPORT_ALPHA_ATRIBUTION). Garantir consistência.

### Com `evolution-risk-concentration`
Correlação de PnLs usa mesmo dado. Consistência via `fator-risco-map.json` compartilhado.

### Com `rf-idka-monitor`
Dependência direta — BVaR e perfil já calculados lá; PA adiciona dimensão de retorno realizado.

---

## Referências

- `references/regime-a-queries.md` — queries por fundo do Regime A
- `references/regime-b-metodologia.md` — metodologia IDKA completa
- `references/fator-risco-map.md` — função `classificar_fator_risco`
- `references/destaque-detection.md` — critérios de destaques

## Assets

- `assets/fund-key-map.json` — placeholder até recebermos o oficial
- `assets/fator-risco-map.json` — mapa GRUPO → FATOR_RISCO
- `assets/destaque-thresholds.json` — thresholds editáveis

## Skills relacionadas

- `risk-manager` — framework
- `macro-stop-monitor` — PnL mensal MACRO (fonte compartilhada)
- `macro-risk-breakdown` — breakdown (mesma fonte, lente diferente)
- `evolution-risk-concentration` — PnL por estratégia Evolution
- `rf-idka-monitor` — BVaR e perfil IDKA (base do Regime B)

## Roadmap

1. **MVP:** Regime A para MACRO e EVOLUTION + esqueleto Regime B para IDKA
2. **Confirmação:** valores de `FUNDO` para ALBATROZ e QUANT MACRO Q + FUND_KEY_MAP
3. **Regime B completo:** fonte Anbima confirmada e metodologia validada
4. **Futuro:** contribuição por vértice, Brinson para RV benchmarked, PA intradiário

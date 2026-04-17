# Queries PA Regime B — IDKAs (Construir Atribuição)

Os IDKAs não têm PA na tabela `REPORT_ALPHA_ATRIBUTION`. Construir a atribuição aqui.

## 1. Retorno diário do fundo (cota)

```sql
SELECT "VAL_DATE", "TRADING_DESK", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
  AND "VAL_DATE" BETWEEN '{data_inicio}' AND '{dia_atual}'
ORDER BY "TRADING_DESK", "VAL_DATE"
```

Pós-processamento:
```python
df = df.sort_values(['TRADING_DESK', 'VAL_DATE'])
df['RETURN'] = df.groupby('TRADING_DESK')['NAV'].pct_change()
df['RETURN_BPS'] = df['RETURN'] * 10000
```

## 2. Retorno do benchmark oficial

**Ponto em aberto.** Ver `politica-idka-pendente.md` da skill `rf-idka-monitor`.

Tentativa 1 — ECO_INDEX:
```sql
SELECT DISTINCT "INSTRUMENT"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" ILIKE '%IDKA%'
   OR "INSTRUMENT" ILIKE '%IPCA%2A%'
   OR "INSTRUMENT" ILIKE '%IPCA%10A%'
```

Se encontrar, por exemplo, `IDKA_IPCA_2A`:
```sql
SELECT "DATE", "INSTRUMENT", "VALUE"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" IN ('IDKA_IPCA_2A', 'IDKA_IPCA_10A')
  AND "DATE" BETWEEN '{data_inicio}' AND '{dia_atual}'
ORDER BY "INSTRUMENT", "DATE"
```

Se `VALUE` for o retorno diário → usar direto. Se for o nível do índice → calcular retorno:
```python
df_bench['RETURN'] = df_bench.groupby('INSTRUMENT')['VALUE'].pct_change()
```

## 3. PnL por book (decomposição)

```sql
SELECT "VAL_DATE", "TRADING_DESK", "BOOK",
       SUM("POSITION") AS "POSITION",
       SUM("PL") AS "PL_ACUMULADO"
FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
WHERE "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
  AND "VAL_DATE" BETWEEN '{dia_anterior}' AND '{dia_atual}'
GROUP BY "VAL_DATE", "TRADING_DESK", "BOOK"
```

**PnL do dia = PnL acumulado de hoje − PnL acumulado de ontem** (ajustado por aportes/resgates).

Alternativa — se existir coluna específica:
```sql
-- Verificar se há coluna de PnL diário direto
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'LOTE45' AND table_name = 'LOTE_BOOK_OVERVIEW'
  AND (column_name ILIKE '%day%' OR column_name ILIKE '%dia%' OR column_name ILIKE '%daily%')
```

## 4. Aportes e resgates (ajuste do PnL)

```sql
SELECT "TRADING_DESK", SUM("AMOUNT") AS "AMOUNT"
FROM "LOTE45"."LOTE_APORTES"
WHERE "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
  AND "LAST_UPDATE" BETWEEN '{dia_anterior}' AND '{dia_atual}'
GROUP BY "TRADING_DESK"
```

Para calcular retorno diário corretamente:
```
R_dia = (NAV_hoje - Aportes_dia) / NAV_ontem - 1
```

## Montagem do PA final

Após ter:
- `R_fundo_3y`, `R_fundo_10y` — retornos diários do fundo
- `R_bench_2a`, `R_bench_10a` — retornos diários do benchmark
- `PnL_book_Benchmark_IDKA_3y`, etc. — PnL em bps do book de réplica
- `PnL_book_RF_LF_3y`, etc. — PnL em bps do book ativo

Calcular:
```python
# Excess return
ER = R_fundo - R_bench

# Tracking error da réplica (deveria ser ~zero)
TE_replica = PnL_book_Benchmark_IDKA - R_bench

# Alpha do risco ativo
Alpha = PnL_book_RF_LF

# Validação
residuo = ER - TE_replica - Alpha
if abs(residuo) > 0.5:  # mais de 0.5 bp de diferença
    flag_warning("PA com resíduo significativo: {residuo:.2f} bps")
```

## Fontes de contribuição dentro do RF_LF

Para detalhar "de onde veio o alpha" no risco ativo, ir mais fundo:

```sql
SELECT "VAL_DATE", "PRODUCT", "PRODUCT_CLASS",
       SUM("POSITION") AS "POSITION",
       SUM("PL") AS "PL_ACUMULADO"
FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
WHERE "TRADING_DESK" = 'IDKA IPCA 3Y FIRF'
  AND "BOOK" = 'RF_LF'
  AND "VAL_DATE" BETWEEN '{dia_anterior}' AND '{dia_atual}'
GROUP BY "VAL_DATE", "PRODUCT", "PRODUCT_CLASS"
```

Pivotar e calcular delta:
```python
df_pivot = df.pivot_table(
    index=['PRODUCT', 'PRODUCT_CLASS'],
    columns='VAL_DATE',
    values='PL_ACUMULADO'
).fillna(0)

df_pivot['PNL_DIA'] = df_pivot[dia_atual] - df_pivot[dia_anterior]
df_pivot['PNL_BPS'] = df_pivot['PNL_DIA'] * 10000 / aum
```

Top contribuidores do RF_LF são aqueles com maior `|PNL_BPS|`.

## Pontos a confirmar na primeira execução

1. **Nome exato do benchmark oficial no `ECO_INDEX`** (se presente)
2. **Presença ou não da coluna de PnL diário em `LOTE_BOOK_OVERVIEW`** (evita o cálculo por diferença de acumulado)
3. **Tratamento de aportes/resgates** — o script do IDKA original faz isso; seguir a mesma lógica
4. **Se o IDKA aloca em Albatroz**, o PnL desse "sub-fundo" aparece como um único PRODUCT; avaliar look-through

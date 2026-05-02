# Queries SQL — Evolution Risk Concentration

Todas extraídas do script `EVOLUTION_TABLES_GRAPHS.py`. Padrão de conexão em `glpg_fetch.py`.

Parâmetro principal: `dia_atual` (string `YYYY-MM-DD`).

## 1. AUM do fundo

```sql
SELECT "VAL_DATE", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "VAL_DATE" = DATE '{dia_atual}'
```

Última atualização disponível (fallback):
```sql
SELECT t1.*
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE" t1
INNER JOIN (
    SELECT "TRADING_DESK", MAX("VAL_DATE") AS max_val_date
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
    GROUP BY "TRADING_DESK"
) t2 ON t1."TRADING_DESK" = t2."TRADING_DESK"
     AND t1."VAL_DATE" = t2.max_val_date
```

## 2. VaR por estratégia (LEVEL=2, TREE gestores) — HOJE

Para Camada 1.

```sql
SELECT "BOOK", SUM("PARAMETRIC_VAR") AS "PARAMETRIC_VAR"
FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "LEVEL" = 2
  AND "TREE" = 'Main_Macro_Gestores'
  AND "VAL_DATE" = DATE '{dia_atual}'
GROUP BY "BOOK"
```

Pós-processamento:
```python
# Normalizar para bps
df['VAR_BPS'] = df['PARAMETRIC_VAR'] * -10000 / aum_evo

# Consolidar em categorias esperadas (outros BOOKs viram OUTROS)
categorias_esperadas = ['EVO_STRAT', 'SIST', 'FRONTIER', 'MACRO', 'CREDITO', 'CAIXA']
df.loc[~df['BOOK'].isin(categorias_esperadas), 'BOOK'] = 'OUTROS'
df_consolidado = df.groupby('BOOK')['VAR_BPS'].sum().reset_index()
```

## 3. Série histórica de VaR por estratégia

Para percentil e histórico. Janela ≥ 252 dias úteis.

```sql
SELECT "VAL_DATE", "BOOK", SUM("PARAMETRIC_VAR") AS "PARAMETRIC_VAR"
FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "LEVEL" = 2
  AND "TREE" = 'Main_Macro_Gestores'
  AND "VAL_DATE" >= DATE '{dia_atual}' - INTERVAL '400 days'
  AND "VAL_DATE" <= DATE '{dia_atual}'
GROUP BY "BOOK", "VAL_DATE"
ORDER BY "VAL_DATE"
```

Pós-processamento:
```python
# Normalizar com AUM do dia (precisa da série de AUM)
# Pivotar BOOK x VAL_DATE
pivot = df.pivot(index='VAL_DATE', columns='BOOK', values='PARAMETRIC_VAR')

# Aplicar AUM dia a dia
aum_series = get_aum_series(...)
pivot_bps = pivot.apply(lambda col: col * -10000 / aum_series.loc[col.index])

# Últimos 252 dias úteis
serie_252d = pivot_bps.tail(252)
```

## 4. VaR agregado do fundo (LEVEL=10)

```sql
SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS", "PARAMETRIC_VAR"
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "LEVEL" = 10
  AND "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
```

Pós-processamento:
```python
var_real_total_bps = df['PARAMETRIC_VAR'].sum() * -10000 / aum_evo
```

## 5. Série histórica de VaR agregado do fundo

```sql
SELECT "VAL_DATE", SUM("PARAMETRIC_VAR") AS "PARAMETRIC_VAR"
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "LEVEL" = 10
  AND "VAL_DATE" >= DATE '{dia_atual}' - INTERVAL '400 days'
  AND "VAL_DATE" <= DATE '{dia_atual}'
GROUP BY "VAL_DATE"
ORDER BY "VAL_DATE"
```

Necessária para calcular percentil histórico do **Ratio** (Camada 2) e para confirmar consistência.

## 6. PnL diário por LIVRO

Para Camada 3 (correlações). Janela ≥ 252d.

```sql
SELECT "DATE", "LIVRO", SUM("DIA") AS "PNL_DIA"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'EVOLUTION'
  AND "DATE" >= DATE '{dia_atual}' - INTERVAL '400 days'
  AND "DATE" <= DATE '{dia_atual}'
GROUP BY "DATE", "LIVRO"
ORDER BY "DATE"
```

Pós-processamento:
```python
# Mapear LIVRO → estratégia via assets/livros-map.json
livros_dict = load_json('assets/livros-map.json')
agg_map = {livro: estrategia for estrategia, livros in livros_dict.items() for livro in livros}
df['ESTRATEGIA'] = df['LIVRO'].map(agg_map).fillna('OUTROS')

# Agregar
pnl_por_estrategia = df.groupby(['DATE', 'ESTRATEGIA'])['PNL_DIA'].sum().unstack() * 10000

# Correlação rolling 21d e 63d
corr_21d = pnl_por_estrategia.rolling(window=21).corr(pairwise=True)
corr_63d = pnl_por_estrategia.rolling(window=63).corr(pairwise=True)
```

## 7. Direction report (RISK_DIRECTION_REPORT)

Para Camada 4 condição 5 (smoking gun do alinhamento direcional).

```sql
SELECT "TIPO", "CATEGORIA", "NOME",
       "DELTA_SISTEMATICO", "DELTA_DISCRICIONARIO", "PCT_PL_TOTAL"
FROM q_models."RISK_DIRECTION_REPORT"
WHERE "FUNDO" = 'Galapagos Evolution FIC FIM CP'
  AND "VAL_DATE" = DATE '{dia_atual}'
```

Uso:
```python
# Contar categorias com mesmo sinal nos dois deltas
df['SAME_SIGN'] = (df['DELTA_SISTEMATICO'] * df['DELTA_DISCRICIONARIO']) > 0
categorias_alinhadas = df[df['SAME_SIGN']].groupby('CATEGORIA').size()
condicao_5 = (categorias_alinhadas > 0).sum() >= 3
```

## 8. Identificação de cotas júnior (ressalva)

Na primeira execução da skill, listar produtos do book CREDITO para calibrar o filtro:

```sql
SELECT DISTINCT "PRODUCT", "PRODUCT_CLASS"
FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
WHERE "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "BOOK" LIKE '%Cred%'
  AND "VAL_DATE" = DATE '{dia_atual}'
ORDER BY "PRODUCT_CLASS"
```

Nas execuções seguintes, usar o filtro calibrado e registrado em `assets/cotas-junior-patterns.json`.

## 9. Histórico de livros (para detectar LIVROs novos)

```sql
SELECT DISTINCT "LIVRO"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'EVOLUTION'
  AND "DATE" = DATE '{dia_atual}'
```

Comparar com `assets/livros-map.json`. LIVROs novos (não no map) → flag para atualização manual da taxonomia. Durante execução, vão para categoria `OUTROS`.

## Ordem recomendada de execução

Em uma única conexão:

1. AUM (query 1)
2. Cotas júnior (query 8) → flag da ressalva
3. VaR por estratégia hoje (query 2)
4. VaR agregado fundo hoje (query 4)
5. Série histórica VaR estratégias (query 3)
6. Série histórica VaR agregado (query 5)
7. PnL diário histórico (query 6)
8. Direction report (query 7)
9. Validação de LIVROs (query 9)

Pode ser paralelizado em threads separadas se performance for limitante — mas a dependência entre 1-2 e o resto deve ser respeitada.

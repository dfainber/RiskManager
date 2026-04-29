# Queries SQL — Breakdown Analítico do MACRO

Queries para as 4 camadas do `macro-risk-breakdown`. Complementam `risk-daily-monitor/references/queries-mm.md`.

Conexão padrão igual às outras skills (`GLPG_DBAPI.dbinterface`, GLPG-DB01).

Parâmetros: `dia_atual`, `dia_anterior` (último D.U. anterior).

## 1. Decomposição de VaR por BOOK (classe e gestor)

```sql
SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS", "PARAMETRIC_VAR"
FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "LEVEL" = 3
  AND "BOOK" IN ('RF-BZ','RF-EM','RF-DM','FX-BRL','FX-EM','FX-DM',
                 'RV-BZ','RV-EM','RV-DM','COMMODITIES','P-Metals',
                 'CI','LF','JD','RJ')
```

Pós-processamento (em Python):
```python
# Normalizar VaR para bps positivos
df['VAR_BPS'] = df['PARAMETRIC_VAR'] * -10000 / aum_macro

# Separar as duas visões
classes = ['RF-BZ','RF-EM','RF-DM','FX-BRL','FX-EM','FX-DM',
           'RV-BZ','RV-EM','RV-DM','COMMODITIES','P-Metals']
gestores = ['CI','LF','JD','RJ']

var_por_classe = df[df['BOOK'].isin(classes)].groupby('BOOK')['VAR_BPS'].sum()
var_por_gestor = df[df['BOOK'].isin(gestores)].groupby('BOOK')['VAR_BPS'].sum()

# Validar que somas batem
assert abs(var_por_classe.sum() - var_por_gestor.sum()) < 0.5, "Inconsistência"
```

## 2. Decomposição de Stress (cenário principal do mandato)

Extrair a mesma estrutura, mas com a coluna de stress. **Pendente:** nome exato da coluna de stress por cenário em `LOTE_BOOK_STRESS_RPM` precisa ser confirmado na primeira execução. Os scripts originais focam em `PARAMETRIC_VAR`; verificar colunas adicionais como `STRESS_CRISE_2008`, `STRESS_COVID`, etc.

Esqueleto:
```sql
SELECT "BOOK", "{coluna_stress_do_cenario}"
FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "LEVEL" = 3
  AND "BOOK" IN (...)
```

Se o banco tem uma coluna por cenário, escolher a do `cenario_principal` do mandato. Se só tem `PARAMETRIC_STRESS` (agregado), usar esse.

## 3. Delta (exposição) por BOOK e PRODUCT

Necessário para Camada 2 (atribuição da variação).

```sql
SELECT "VAL_DATE", "BOOK", "PRODUCT", "PRODUCT_CLASS",
       SUM("DELTA") AS "DELTA"
FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
WHERE "VAL_DATE" IN (DATE '{dia_atual}', DATE '{dia_anterior}')
  AND "TRADING_DESK" = 'Galapagos Macro FIM'
GROUP BY "VAL_DATE", "BOOK", "PRODUCT", "PRODUCT_CLASS"
```

Pós-processamento:
```python
# Pivotar para comparar D vs D-1
pivot = df.pivot_table(
    index=['BOOK', 'PRODUCT', 'PRODUCT_CLASS'],
    columns='VAL_DATE', values='DELTA'
).fillna(0)
pivot.columns = ['DELTA_ontem', 'DELTA_hoje']
pivot['DELTA_DIFF'] = pivot['DELTA_hoje'] - pivot['DELTA_ontem']
pivot['DELTA_DIFF_PCT'] = pivot['DELTA_DIFF'] / pivot['DELTA_ontem'].abs().replace(0, 1)
```

Regra de atribuição:
- Se `|DELTA_DIFF_PCT| > 10%` → "posição nova"
- Senão → "mercado"

## 4. Top 10 PRODUCTs do fundo (para Camada 4)

```sql
SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS", "PARAMETRIC_VAR"
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "LEVEL" = 10
  AND "TRADING_DESK" = 'Galapagos Macro FIM'
  AND "TREE" = 'Main_Macro_Ativos'
```

Pós-processamento:
```python
df['VAR_BPS'] = df['PARAMETRIC_VAR'] * -10000 / aum_macro
df['PRODUTO_AJUSTADO'] = df.apply(produto_ajustado, axis=1)  # função do script MACRO

# Agregar por produto ajustado (alguns PRODUCTs representam o mesmo ativo)
por_ativo = df.groupby('PRODUTO_AJUSTADO').agg({
    'VAR_BPS': 'sum',
    'BOOK': 'first',      # classe
    'PRODUCT_CLASS': 'first'
}).sort_values('VAR_BPS', ascending=False)

top10 = por_ativo.head(10)
```

## 5. Série histórica diária de retorno por PRODUCT (para regra de 3)

Necessária para obter o **percentil 1 histórico** de cada ativo dos top 10 — o "movimento implícito no VaR".

```sql
SELECT "PORTIFOLIO", "DATE_SYNTHETIC_POSITION", "W"
FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
WHERE "PORTIFOLIO_DATE" = '{dia_atual}'
  AND "PORTIFOLIO" IN {tuple_dos_top10_ajustados}
```

Pós-processamento:
```python
hist = df.pivot(index='DATE_SYNTHETIC_POSITION',
                columns='PORTIFOLIO', values='W').fillna(0)

# Percentil 1 (1% dos piores retornos) em cada coluna
p1 = hist.quantile(0.01)   # retornos mais negativos = p1
p99 = hist.quantile(0.99)  # retornos mais positivos = p99

# Para cada ativo, pegar o pior dos dois (em magnitude) = movimento implícito no VaR
mov_implicito = {}
for ativo in hist.columns:
    mov_implicito[ativo] = max(abs(p1[ativo]), abs(p99[ativo]))
    # sinal: se delta > 0 na carteira, perda vem de movimento negativo → usar p1
    #        se delta < 0, perda vem de movimento positivo → usar p99
```

**Observação importante:** a coluna `W` desta tabela contém o **retorno simulado do book/portifólio**, não do ativo individual. Se o objetivo é ter o retorno do ativo subjacente (para responder "quanto o petróleo precisa cair?"), precisa de outra fonte:

- Opção A: tabela de preços `PRICES_GLOBAL_EQUITIES` / `FUTURES_PRICES` / `SYNTHETIC_FUTURE` (via `glpg-data-fetch`)
- Opção B: a própria `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` mas em granularidade de ativo, se existir

**A confirmar na primeira execução.** Se nenhuma das duas opções atender, adotar fallback: usar o retorno do BOOK como proxy (menos preciso mas consistente), e documentar a limitação.

## 6. Queries auxiliares

### AUM atual (já existe em queries-mm.md)

```sql
SELECT "NAV" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" = 'Galapagos Macro FIM'
  AND "VAL_DATE" = DATE '{dia_atual}'
```

### PnL mensal por PM (reutiliza da macro-stop-monitor)

```sql
SELECT "LIVRO", SUM("DIA") * 10000 AS "PNL_MES_BPS"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'MACRO'
  AND "DATE" BETWEEN DATE_TRUNC('month', DATE '{dia_atual}')
                 AND DATE '{dia_atual}'
GROUP BY "LIVRO"
```

## Ordem recomendada de execução

Em uma única conexão:
1. AUM (query 6)
2. Decomposição VaR (query 1)
3. Decomposição Stress (query 2) — se coluna confirmada
4. Delta D e D-1 (query 3)
5. Top 10 PRODUCTs (query 4)
6. Histórico de simulação dos top 10 (query 5)
7. PnL mensal por PM (query 6)

Todas são leitura-somente, podem rodar em paralelo se preferível. Manter em uma única conexão para consistência temporal.

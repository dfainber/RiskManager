# Queries SQL — IDKA Monitor

Extraídas de `IDKA_TABLES_GRAPHS.py`. Padrão de conexão em `glpg-data-fetch`.

Parâmetro principal: `dia_atual` (string `YYYY-MM-DD`).

## Fundos-alvo

Dois TRADING_DESKs:
- `IDKA IPCA 3Y FIRF` (ticker interno: IDKA-3Y)
- `IDKA IPCA 10Y FIRF` (ticker interno: IDKA-10Y)

Em algumas tabelas, aparecem como `IDKAIPCAY3` e `IDKAIPCAY10` (versão curta). O mapa está em `assets/idka-ticker-map.json`.

## 1. AUM dos fundos

```sql
SELECT t1.*
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE" t1
INNER JOIN (
    SELECT "TRADING_DESK", MAX("VAL_DATE") AS max_val_date
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
    GROUP BY "TRADING_DESK"
) t2 ON t1."TRADING_DESK" = t2."TRADING_DESK"
     AND t1."VAL_DATE" = t2.max_val_date
```

Pega o último AUM disponível por fundo. Em Python:

```python
aum_3y = df[df['TRADING_DESK'] == 'IDKA IPCA 3Y FIRF']['NAV'].iloc[0]
aum_10y = df[df['TRADING_DESK'] == 'IDKA IPCA 10Y FIRF']['NAV'].iloc[0]
```

## 2. Posições atuais (book, produto, amount, price)

```sql
SELECT "TRADING_DESK", "BOOK", "PRODUCT_CLASS", "PRODUCT", "AMOUNT", "PRICE"
FROM "LOTE45"."LOTE_PRODUCT_BOOK_POSITION_PL"
WHERE (
    ("TRADING_DESK" = 'IDKA IPCA 3Y FIRF' AND "VAL_DATE" > '{max_date_3y}')
    OR ("TRADING_DESK" = 'IDKA IPCA 10Y FIRF' AND "VAL_DATE" > '{max_date_10y}')
)
```

Base para calcular DV01 e ANO_EQ por posição. Também base para identificar se há alocação em Albatroz.

## 3. BVaR (RELATIVE_VAR) por book

Query **crítica** — é o coração do monitoramento:

```sql
SELECT SUM("RELATIVE_VAR") AS "RELATIVE_VAR", "BOOKS", "TRADING_DESK"
FROM "LOTE45"."LOTE_PARAMETRIC_VAR_TABLE"
WHERE "VAL_DATE" = '{dia_atual}'
  AND "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
GROUP BY "BOOKS", "TRADING_DESK"
```

Observações sobre a coluna `BOOKS`:

No script original, `BOOKS` vem como **array** (lista). O script trata:

```python
# Casos principais
df.loc[df['BOOKS'].apply(lambda x: isinstance(x, list) and len(x) == 1 and x[0] == '*'), 'BOOKS'] = 'ALL'
df.loc[df['BOOKS'].apply(lambda x: isinstance(x, list) and len(x) == 1 and x[0] == 'Benchmark_IDKA'), 'BOOKS'] = 'Benchmark_IDKA'
df.loc[df['BOOKS'].apply(lambda x: isinstance(x, list) and len(x) == 1 and x[0] == 'RF_LF'), 'BOOKS'] = 'RF_LF'

# Filtrar só as três visões relevantes
df = df[df['BOOKS'].isin(['ALL', 'Benchmark_IDKA', 'RF_LF'])]
```

Normalização para bps (do script):

```python
df.loc[df['TRADING_DESK'] == 'IDKA IPCA 3Y FIRF', 'BVAR_BPS'] = (
    df.loc[df['TRADING_DESK'] == 'IDKA IPCA 3Y FIRF', 'RELATIVE_VAR'] * -10000 / aum_3y
)
df.loc[df['TRADING_DESK'] == 'IDKA IPCA 10Y FIRF', 'BVAR_BPS'] = (
    df.loc[df['TRADING_DESK'] == 'IDKA IPCA 10Y FIRF', 'RELATIVE_VAR'] * -10000 / aum_10y
)
```

## 4. Cálculo de DV01 (por instrumento)

DV01 não vem do banco — é calculado. O script diferencia 3 famílias:

### NTN-B (DAP) e LFT

```python
from GLPG_Public_Bonds import NTNB, LFT

df_titulos['DV01'] = None
for i in df_titulos.index:
    if df_titulos.loc[i, 'TIPO'] == 'NTN-B':
        df_titulos.loc[i, 'DV01'] = NTNB(
            df_titulos.loc[i, 'EXPIRATION_DATE'], dia_atual
        ).calcular_dv01(df_titulos.loc[i, 'PRICE'], servicos)
    elif df_titulos.loc[i, 'TIPO'] == 'LFT':
        df_titulos.loc[i, 'DV01'] = LFT(
            df_titulos.loc[i, 'EXPIRATION_DATE'], dia_atual
        ).calcular_dv01(df_titulos.loc[i, 'PRICE'], servicos)
```

A instância `servicos` é configurada assim (script original):

```python
from GLPG_Public_Bonds import ServicosFinanceiros
servicos = ServicosFinanceiros(db_config={
    'hostname': 'GLPG-DB01',
    'port': 5432,
    'database': 'DATA_DEV_DB',
    'username': 'svc_automation',
    'password': 'admin',
})
```

### DI1 Futuros

Cálculo manual via BDAYS e PRICE:

```python
import numpy as np

# Para cada DI1:
price = df_di1['PRICE'].values  # taxa
bdays = df_di1['BDAYS'].values  # dias úteis até vencimento
base_value = 100000 / ((1 + price/100) ** (bdays/252))
price_minus_bp = (price - 0.01) / 100  # choque de 1bp
base_value_minus_bp = 100000 / ((1 + price_minus_bp) ** (bdays/252))
df_di1['DV01'] = np.abs(base_value - base_value_minus_bp)

# Para DAP (NTN-B sintética via futuro):
prorata = get_prorata_ipca(dia_atual)
df_dap['DV01'] = np.abs(
    (100000 / ((1 + price_minus_bp) ** (bdays/252))) * prorata * 0.00025
)
```

**`prorata`** é o IPCA prorata do dia — vem de `ECO_INDEX`:

```sql
SELECT "VALUE" FROM "ECO_INDEX"
WHERE "INSTRUMENT" = 'IPCA_PRORATA'
  AND "DATE" = DATE '{dia_atual}'
```

### ANO_EQ (ano-equivalente)

Métrica canônica da gestora. Fórmula (do script):

```python
df['ANO_EQ'] = df['AMOUNT'] * df['DV01'] * 10000 / df['AUM']
```

Interpretação: "quantos anos de exposição IPCA por cota". É o que se lê nos dashboards.

## 5. Separação por book para as 3 camadas

Depois de ter todas as posições com DV01 e ANO_EQ:

```python
# Camada 1 — Total (book = ALL, ou soma de todos)
df_all = df_overview.copy()

# Camada 2 — Qualidade da réplica
df_benchmark = df_overview[df_overview['BOOK'] == 'Benchmark_IDKA']

# Camada 3 — Risco ativo
df_rf_lf = df_overview[df_overview['BOOK'] == 'RF_LF']

# Agregações
ano_eq_total = df_all.groupby('TRADING_DESK')['ANO_EQ'].sum()
ano_eq_benchmark = df_benchmark.groupby('TRADING_DESK')['ANO_EQ'].sum()
ano_eq_rf_lf = df_rf_lf.groupby('TRADING_DESK')['ANO_EQ'].sum()
```

## 6. Perfil de exposição por vértice (para gráfico e gap analysis)

Do script original, gráfico do benchmark 3Y:

```python
# Para cada fundo × book, agregar ANO_EQ por BDAYS
df_profile_3y = df_overview[
    (df_overview['TRADING_DESK'] == 'IDKA IPCA 3Y FIRF') &
    (df_overview['BOOK'] == 'Benchmark_IDKA')
][['BDAYS', 'ANO_EQ']]

df_profile_3y = df_profile_3y.sort_values('BDAYS').groupby('BDAYS')['ANO_EQ'].sum()

# Acumulado (curva):
df_profile_3y_cumulative = df_profile_3y.cumsum()
```

Esse perfil é comparado visualmente no dashboard. Para análise programática de gap vs. índice oficial, precisa da referência externa — ponto em aberto da política.

## 7. PnL mensal (para Camada 4)

```sql
SELECT "DATE", "LIVRO", "DIA", "MES"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" IN ('IDKAIPCAY3', 'IDKAIPCAY10')
  AND "DATE" BETWEEN DATE_TRUNC('month', DATE '{dia_atual}') AND DATE '{dia_atual}'
```

**Atenção:** o valor de `FUNDO` nessa tabela parece ser a versão curta (`IDKAIPCAY3`), não a longa (`IDKA IPCA 3Y FIRF`). Confirmar na primeira execução.

Normalização: `DIA` e `MES` em decimal → multiplicar por 10000 para bps.

## 8. Benchmark oficial (IDKA Anbima)

**Pendente.** O script original não acessa o retorno oficial do benchmark diretamente — o conceito de "réplica" é operacional via book `Benchmark_IDKA`.

Para Camada 4 (alpha MTD), precisamos do retorno do índice IDKA oficial. Opções:
- `ECO_INDEX` com instrument adequado (verificar se existe `IDKA_IPCA_2A`, `IDKA_IPCA_10A`)
- API Anbima externa

Na primeira execução, verificar:

```sql
SELECT DISTINCT "INSTRUMENT"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" ILIKE '%IDKA%' OR "INSTRUMENT" ILIKE '%IMA%'
```

## Ordem recomendada de execução

1. AUM dos fundos (query 1)
2. Posições (query 2)
3. BVaR por book (query 3)
4. Cálculo de DV01 por instrumento (seção 4)
5. Cálculo de ANO_EQ (seção 4)
6. Perfil por vértice (seção 6)
7. PnL mensal (query 7)
8. Retorno do benchmark oficial (query 8)

Rodar tudo em uma conexão. Verificar particularmente as linhas 180-220 do script original para reproduzir exatamente o `merge_overview_futuro` com DV01.

# Queries SQL — Multimercados

Queries extraídas dos scripts `MACRO_TABLES_GRAPHS.py`, `SISTEMATICO_TABLES_GRAPHS.py` e `EVOLUTION_TABLES_GRAPHS.py`, padronizadas para a skill. Todas usam o padrão `glpg-data-fetch`.

## Conexão padrão

```python
import os
import GLPG_DBAPI as SQL

# Credentials must come from environment variables (or .env loaded at startup).
# Never hardcode credentials in source files.
# Canonical env var names match glpg_fetch.py:
#   GLPG_DB_HOST, GLPG_DB_PORT, GLPG_DB_NAME, GLPG_DB_USER, GLPG_DB_PASSWORD

def get_df(query):
    db = SQL.dbinterface()
    db.open(
        hostname=os.environ['GLPG_DB_HOST'],
        port=int(os.environ.get('GLPG_DB_PORT', '5432')),
        database=os.environ['GLPG_DB_NAME'],
        username=os.environ['GLPG_DB_USER'],
        password=os.environ['GLPG_DB_PASSWORD'],
    )
    df = db.read_sql(query)
    db.close()
    return df
```

> **Nota:** Em produção, usar `glpg_fetch.py` (que gerencia pool de conexões e carrega as variáveis de ambiente).

## Parâmetros-chave

- `dia_atual`: data-base (string `YYYY-MM-DD` ou `date`). Default = último dia útil Anbima.
- `trading_desk`: nome do fundo (ver tabela abaixo).
- `tree`: identificador da árvore de agregação (varia por fundo).

| Fundo | TRADING_DESK | TREE (principal) |
|-------|--------------|------------------|
| MACRO | `Galapagos Macro FIM` | `Main_Macro_Ativos` (há também `Main_Macro_Gestores`) |
| SISTEMATICO | `Galapagos Quantitativo FIM` | *(verificar na primeira execução)* |
| EVOLUTION | `Galapagos Evolution FIC FIM CP` | `Main` |

## 1. AUM (NAV) do fundo

```sql
SELECT "VAL_DATE", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" = '{trading_desk}'
  AND "VAL_DATE" = DATE '{dia_atual}'
```

Para obter último AUM disponível (fallback se não houver no dia):
```sql
SELECT t1.*
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE" t1
INNER JOIN (
    SELECT "TRADING_DESK", MAX("VAL_DATE") AS max_val_date
    FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
    WHERE "TRADING_DESK" = '{trading_desk}'
    GROUP BY "TRADING_DESK"
) t2 ON t1."TRADING_DESK" = t2."TRADING_DESK"
     AND t1."VAL_DATE" = t2.max_val_date
```

## 2. Fund VaR (nível fundo)

### MACRO
```sql
SELECT *
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "LEVEL" = 10
  AND "TRADING_DESK" = 'Galapagos Macro FIM'
  AND "TREE" = 'Main_Macro_Ativos'
```

Normalização: `FUND_VAR_PCT = PARAMETRIC_VAR × -10000 / AUM`.

### SISTEMATICO
```sql
SELECT *
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "LEVEL" = 10
  AND "TRADING_DESK" = 'Galapagos Quantitativo FIM'
```
(TREE: verificar — provável `Main` ou similar; olhar o `SISTEMATICO_TABLES_GRAPHS.py` na primeira execução)

### EVOLUTION
```sql
SELECT *
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "LEVEL" = 10
  AND "TRADING_DESK" = 'Galapagos Evolution FIC FIM CP'
  AND "TREE" = 'Main'
```

## 3. Book VaR (nível book dentro do fundo)

Observação importante do script MACRO: o VaR por book é lido do **EVOLUTION**, não do MACRO diretamente — porque MACRO é alocado dentro do Evolution. A query cruza as duas visões.

```sql
SELECT "VAL_DATE", "TRADING_DESK", "BOOK", "PRODUCT", "PRODUCT_CLASS",
       "PARAMETRIC_VAR" AS "BOOK_VAR"
FROM "LOTE45"."LOTE_BOOK_STRESS_RPM"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "TRADING_DESK" = '{trading_desk}'
  AND "LEVEL" = 3
  AND "BOOK" IN {tuple_de_books}
```

`tuple_de_books` para MACRO (quebras ativos): `('RF-BZ','RF-EM','RF-DM','FX-BRL','FX-EM','FX-DM','RV-BZ','RV-EM','RV-DM','COMMODITIES','P-Metals')`.

Para EVOLUTION usar categorias do `classificar_book` — mas a classificação é feita em Python depois de trazer o BOOK cru. Ver `evolution-book-classification.md`.

## 4. Série histórica de VaR

Usado para detecção de tendência e histórico para o Morning Call.

```sql
SELECT "VAL_DATE", "BOOK", SUM("PARAMETRIC_VAR") AS "FUND_VAR"
FROM "LOTE45"."LOTE_FUND_STRESS_RPM"
WHERE "TRADING_DESK" = '{trading_desk}'
  AND ("TREE" = 'Main_Macro_Ativos' OR "TREE" = 'Main_Macro_Gestores')
  AND "LEVEL" IN ('3', '2')
  AND "VAL_DATE" >= '2025-01-01'
  AND "BOOK" IN {tuple_de_books + ('ALL',)}
GROUP BY "VAL_DATE", "BOOK"
ORDER BY "VAL_DATE"
```

Pós-processamento (em Python, conforme scripts originais):
1. Adicionar AUM do dia via `LOTE_TRADING_DESKS_NAV_SHARE`.
2. `FUND_VAR_PCT = FUND_VAR × -10000 / AUM`.
3. Pivot por BOOK para série temporal.

## 5. Portifólio histórico (para drawdowns simulados)

```sql
SELECT "PORTIFOLIO", "DATE_SYNTHETIC_POSITION", "W"
FROM q_models."PORTIFOLIO_DAILY_HISTORICAL_SIMULATION"
WHERE "PORTIFOLIO_DATE" = '{dia_atual}'
  AND "PORTIFOLIO" IN {tuple_de_books + ('MACRO','COMMODITIES',)}
ORDER BY "DATE_SYNTHETIC_POSITION", "PORTIFOLIO"
```

Pós-processamento:
```python
df = df.pivot(index='DATE_SYNTHETIC_POSITION', columns='PORTIFOLIO', values='W').fillna(0)
# Para cada BOOK, calcular max drawdown rolling em janelas [1, 2, 5, 10]
```

Função de drawdown (do script original):
```python
def get_max_drawdown_from_returns(df, col_name, n, is_drawdown):
    growth_factors = 1 + (df[col_name] / 10000.0)
    rolling_compounded = growth_factors.rolling(window=n).apply(np.prod, raw=True) - 1
    result = rolling_compounded.min() if is_drawdown else rolling_compounded.max()
    return result * 10000.0  # bps
```

## 6. Exposições e PnL por BOOK (Overview)

```sql
SELECT *
FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "TRADING_DESK" = '{trading_desk}'
```

Campos relevantes: `BOOK`, `PRODUCT`, `PRODUCT_CLASS`, `POSITION`, `AMOUNT`, `DELTA`, `DIA`, `MES`, etc. Detalhe do significado de cada coluna fica em `LOTE_BOOK_OVERVIEW` (consultar DBA se precisar).

## 7. Benchmark diário (se fundo for benchmarked)

```sql
SELECT *
FROM "LOTE45"."LOTE_DAILY_BENCHMARK"
WHERE "TRADING_DESK" = '{trading_desk}'
  AND "VAL_DATE" = DATE '{dia_atual}'
  AND "VPOS" = FALSE
```

MM geralmente é benchmarked contra CDI. Para VaR/Stress vs. benchmark, o campo `POSITION_EX_EBENCHMARK_PL` tem a atribuição.

## 8. Exposição MACRO por book/produto (LOTE_PRODUCT_EXPO)

**Fonte confirmada em produção (2026-04-17).** Filtros críticos validados:

```sql
SELECT "BOOK", "PRODUCT", "PRODUCT_CLASS", "PRIMITIVE_CLASS",
       SUM("DELTA")                   AS delta,
       SUM("DELTA" * "MOD_DURATION")  AS delta_dur
FROM "LOTE45"."LOTE_PRODUCT_EXPO"
WHERE "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Evolution FIC FIM CP'
  AND "VAL_DATE"                  = DATE '{dia_atual}'
  AND "BOOK" ~* 'CI|JD|LF|QM|RJ'
  AND "BOOK" ~* 'Direcional|Relativo|Hedge|Volatilidade|SS'
GROUP BY "BOOK", "PRODUCT", "PRODUCT_CLASS", "PRIMITIVE_CLASS"
```

**Armadilha crítica:** usar `TRADING_DESK_SHARE_SOURCE`, não `TRADING_DESK`. O MACRO opera dentro do Evolution — cada posição aparece 3 vezes (own / SubA / Evolution feeder). `SHARE_SOURCE = 'Galapagos Evolution FIC FIM CP'` retorna a visão correta (own).

**FX hedge exclusion (em Python):**
```python
# Excluir USDBRL futures em livros não-FX (hedge operacional, não exposição real)
expo = expo[~((expo["PRIMITIVE_CLASS"] == "FX") & (~expo["rf"].str.startswith("FX-", na=False)))]
```

**rf parsing:**
```python
PMS = ("CI","LF","JD","RJ","QM","MD")
def _parse_rf(book):
    parts = book.split("_")
    if len(parts) >= 2 and parts[0] in PMS:
        return parts[1]   # e.g. 'JD_RF-BZ_Direcional' → 'RF-BZ'
    return None

def _parse_pm(book):
    parts = book.split("_")
    if len(parts) >= 1 and parts[0] in PMS:
        return parts[0]
    return "Outros"
```

**Excluir PRIMITIVE_CLASS:** `{"Cash", "Provisions and Costs", "Margin"}`.

## 9. Volatilidade por instrumento (STANDARD_DEVIATION_ASSETS)

**Fonte confirmada em produção (2026-04-17).**

```sql
SELECT "INSTRUMENT", "STANDARD_DEVIATION" AS sigma
FROM q_models."STANDARD_DEVIATION_ASSETS"
WHERE "VAL_DATE" = DATE '{dia_atual}'
  AND "BOOK"     = 'MACRO'
```

Join com `LOTE_PRODUCT_EXPO` em `INSTRUMENT = PRODUCT`. Retorna σ anualizado por instrumento. Para nível PM, calcular média ponderada por `|pct_nav|`.

## 10. PnL diário por instrumento (para drill-down de PM)

```sql
SELECT "LIVRO", "PRODUCT", SUM("DIA") * 10000 AS dia_bps
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'MACRO'
  AND "DATE"  = DATE '{dia_atual}'
  AND "MES"  <> 0
  AND "LIVRO" IN ('CI','Macro_LF','Macro_JD','Macro_RJ','Macro_QM')
GROUP BY "LIVRO", "PRODUCT"
```

**Mapeamento PM → LIVRO (confirmado):**

| PM | LIVRO |
|----|-------|
| CI | CI |
| LF | Macro_LF |
| JD | Macro_JD |
| RJ | Macro_RJ |
| QM | Macro_QM |

Join com `LOTE_PRODUCT_EXPO` em `(LIVRO ↔ PM via mapa) + PRODUCT`.

## Ordem recomendada de execução

Para cada fundo, em um único ciclo de conexão:

1. AUM (query 1)
2. Fund VaR hoje (query 2)
3. Book VaR hoje (query 3)
4. Série histórica VaR (query 4)
5. Portifólio histórico (query 5)
6. Overview (query 6)
7. Exposição MACRO (query 8) — se relatório HTML
8. σ por instrumento (query 9) — se relatório HTML
9. PnL diário por instrumento (query 10) — se relatório HTML

Todas as queries são leitura somente — não há risco de bloqueio.

## Calibração na primeira execução

Confirmados para MACRO em 2026-04-17. Ainda pendentes:
- TREE exato do QUANT (era SISTEMATICO — nome já corrigido)
- Lista completa de BOOKs ativos no QUANT
- BOOKs adicionais no EVOLUTION além dos 5 mapeados pelo `classificar_book`

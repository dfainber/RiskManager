# Queries PA Regime A — Fundos com Atribuição Existente

Fundos cobertos: MACRO, EVOLUTION, QUANTITATIVO, MACRO Q, ALBATROZ.

Todos usam a mesma tabela `q_models.REPORT_ALPHA_ATRIBUTION`, mudando apenas o filtro `FUNDO`.

## Query-base

```sql
SELECT "DATE", "LIVRO", "BOOK", "CLASSE", "GRUPO",
       "PRODUCT_CLASS", "PRODUCT", "DIA", "MES"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = '{fundo_ticker}'
  AND "DATE" BETWEEN '{data_inicio}' AND '{dia_atual}'
```

## Query de descoberta de fundos

Rodar na primeira execução para preencher `assets/fundos-pa-map.json`:

```sql
SELECT DISTINCT "FUNDO",
       MIN("DATE") AS primeira_data,
       MAX("DATE") AS ultima_data,
       COUNT(DISTINCT "DATE") AS num_dias,
       COUNT(DISTINCT "LIVRO") AS num_livros
FROM q_models."REPORT_ALPHA_ATRIBUTION"
GROUP BY "FUNDO"
ORDER BY "FUNDO";
```

**Esperado:** lista contendo algo como `MACRO`, `EVOLUTION`, `QUANTITATIVO`, `MACRO_Q` (ou similar), `ALBATROZ`. Confirmar nomes exatos.

## Query de descoberta de livros por fundo

Para cada fundo, descobrir seus livros ativos:

```sql
SELECT DISTINCT "LIVRO", COUNT(DISTINCT "DATE") AS num_dias_ativo
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = '{fundo_ticker}'
  AND "DATE" >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY "LIVRO"
ORDER BY num_dias_ativo DESC;
```

Isso ajuda a:
- Mapear livros → PMs (MACRO) ou livros → estratégias (EVOLUTION)
- Identificar livros novos que não estão nos mapas existentes
- Calibrar thresholds de destaque por fundo

## Agregações padrão

Após normalizar `DIA_BPS = DIA * 10000` e `MES_BPS = MES * 10000`:

### Dia atual
```python
df_dia = df[df['DATE'] == dia_atual]

# Por LIVRO (PM)
pa_pm = df_dia.groupby('LIVRO')[['DIA_BPS']].sum().sort_values('DIA_BPS', ascending=False)

# Por BOOK (classe)
pa_book = df_dia.groupby('BOOK')[['DIA_BPS']].sum().sort_values('DIA_BPS', ascending=False)

# Top contribuidores / detratores
top5_positivos = pa_book.head(5)
top5_negativos = pa_book.tail(5)
```

### MTD
```python
df_mtd = df[df['DATE'] >= inicio_mes]
pa_pm_mtd = df_mtd.groupby('LIVRO')[['DIA_BPS']].sum()
```

### YTD
```python
df_ytd = df[df['DATE'] >= inicio_ano]
pa_pm_ytd = df_ytd.groupby('LIVRO')[['DIA_BPS']].sum()
```

## Quebra especial para EVOLUTION

Para o EVOLUTION, usar o mapa de livros → estratégias:

```python
import json
with open('../evolution-risk-concentration/assets/livros-map.json') as f:
    livros_map = json.load(f)

# Criar mapa reverso LIVRO -> estrategia
livro_to_estrategia = {}
for estrategia, livros in livros_map.items():
    if estrategia.startswith('_'):
        continue
    for livro in livros:
        livro_to_estrategia[livro] = estrategia

# Aplicar
df['ESTRATEGIA'] = df['LIVRO'].map(livro_to_estrategia).fillna('OUTROS')

# Agregar por estratégia
pa_estrategia = df.groupby('ESTRATEGIA')[['DIA_BPS']].sum()
```

## Validações

- **Soma das contribuições por livro deve fechar com o total do fundo** (tolerância 1 bp por ruído de arredondamento)
- **Livros novos** (não no mapa do EVOLUTION) aparecem como `OUTROS` e devem ser flag-ados no relatório
- **Dias sem dados** (query retorna vazio) — verificar se é feriado, fim de semana ou falha de ETL

## Notas

- A coluna `"DIA"` já contém o retorno do dia em decimal (ex.: 0.0018 = 18 bps). Sempre multiplicar por 10000.
- A coluna `"MES"` é o retorno acumulado do mês até o dia — ao somar valores de dias diferentes do mesmo mês, tomar cuidado para não somar em dobro. Recomendação: usar `DIA` para montar agregações temporais e usar `MES` só como sanity check (último dia do mês = retorno mensal).

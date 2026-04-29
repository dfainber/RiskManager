# Regime A — Queries para PA existente

Uma query-padrão por fundo. Todas usam `q_models.REPORT_ALPHA_ATRIBUTION`.

## Descoberta inicial (uma vez)

Antes de cachear os valores de `FUNDO`:

```sql
SELECT DISTINCT "FUNDO", COUNT(*) AS linhas, MIN("DATE") AS inicio, MAX("DATE") AS fim
FROM q_models."REPORT_ALPHA_ATRIBUTION"
GROUP BY "FUNDO"
ORDER BY linhas DESC;
```

Serve para:
1. Confirmar valores exatos (MACRO, EVOLUTION, IDKAIPCAY3, IDKAIPCAY10, ALBATROZ?, QUANT_MACRO_Q?)
2. Ver período de histórico disponível
3. Detectar fundos novos que foram adicionados sem atualizar o FUND_KEY_MAP

## Query por fundo (padrão)

```sql
SELECT "DATE",
       "LIVRO",
       "BOOK",
       "CLASSE",
       "GRUPO",
       "PRODUCT_CLASS",
       "PRODUCT",
       "DIA",
       "MES"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = '{fundo_chave}'
  AND "DATE" BETWEEN '{data_inicio}' AND '{dia_atual}'
ORDER BY "DATE", "LIVRO", "BOOK";
```

Para o relatório diário típico:
- `data_inicio` = início do ano (para ter Dia, MTD, YTD na mesma query)
- `dia_atual` = último D.U.

## Agregações pós-query (Python)

```python
# Normalizar bps
df['DIA_BPS'] = df['DIA'] * 10000
df['MES_BPS'] = df['MES'] * 10000

# Filtrar por horizonte
today = df[df['DATE'] == dia_atual]
mtd = df[df['DATE'].dt.month == dia_atual.month]
ytd = df  # todo o range

# Agregações para o dia
pa_dia_pm = today.groupby('LIVRO')['DIA_BPS'].sum()
pa_dia_book = today.groupby('BOOK')['DIA_BPS'].sum()

# Para MTD: somar todos os dias do mês
pa_mtd_pm = mtd.groupby('LIVRO')['DIA_BPS'].sum()  # NOTA: somar DIA do mês inteiro
pa_mtd_book = mtd.groupby('BOOK')['DIA_BPS'].sum()

# Para YTD: idem para o ano
pa_ytd_pm = ytd.groupby('LIVRO')['DIA_BPS'].sum()
```

**Atenção com a coluna MES:** `MES` representa o acumulado do mês **até aquela data**, não o PnL daquele dia. Se você pegar `MES` do último dia do mês = PnL MTD. Para consistência, **sempre somar `DIA`** para cálculos de horizonte.

## Filtros específicos por fundo

### MACRO
```sql
WHERE "FUNDO" = 'MACRO'
```

Livros esperados: CI, Macro_JD, Macro_LF, Macro_RJ, Macro_FG, Macro_AC, Macro_MD, Giro_Master, LF_RV-BZ_SS

### EVOLUTION
```sql
WHERE "FUNDO" = 'EVOLUTION'
```

Livros muito mais diversos — usar `livros-map.json` de `evolution-risk-concentration` para mapear em estratégias.

### ALBATROZ (quando confirmado)
```sql
WHERE "FUNDO" = '{valor_a_confirmar}'
```

Consultar `fund-key-map.json` para o valor efetivo.

### QUANTITATIVO MACRO Q (quando confirmado)
Idem.

## Validações

Antes de apresentar o PA, validar:

1. **Soma bate com retorno da cota?**
   ```python
   pnl_total_tabela = df[df['DATE'] == dia_atual]['DIA'].sum()  # decimal
   ret_cota = (nav_hoje / nav_ontem) - 1
   discrepancia = abs(pnl_total_tabela - ret_cota)
   
   if discrepancia > 0.0002:  # 2 bps
       # flag "discrepância grande"
   ```

2. **Todos os livros têm LIVRO preenchido?**
   - Se há linhas com `LIVRO = NULL`, flag e reportar como "não atribuído"

3. **Todas as linhas mapeiam para FATOR_RISCO conhecido?**
   - Linhas que retornam `None` em `classificar_fator_risco` → flag para revisar o mapa

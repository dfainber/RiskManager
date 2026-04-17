# Classificação de Books do EVOLUTION

O EVOLUTION é um fundo guarda-chuva que aloca em 5 categorias de estratégia. A classificação vem de uma função Python em `EVOLUTION_TABLES_GRAPHS.py` que mapeia nomes de BOOK para categorias.

Esta função é a **fonte de verdade** da classificação. A skill deve importá-la ou replicá-la.

## Função original (extraída do script)

```python
def classificar_book(book):
    if not isinstance(book, str):
        return None
    if book == "AÇÕES BR LONG":
        return "FRONTIER"
    elif book == "Backbook":
        return "EVO_STRAT"
    elif "Baltra" in book:
        return "EVO_STRAT"
    elif "Caixa" in book:
        return "CAIXA"
    elif "CAIXA" in book:
        return "CAIXA"
    elif "Carry" in book:
        return "EVO_STRAT"
    elif "CI_Cred" in book:
        return "CREDITO"
    elif "CI_Equities" in book:
        return "EVO_STRAT"
    elif "CI_COMMODITIES" in book:
        return "MACRO"
    elif "CI_FX-" in book:
        return "MACRO"
    elif "CI_P-Metals" in book:
        return "MACRO"
    elif "CI_RF-" in book:
        return "MACRO"
    elif "CI_RV-" in book:
        return "MACRO"
    elif "CI_" in book:
        return "EVO_STRAT"
    elif "AÇÕES BR" in book:
        return "FRONTIER"
    elif "A1" in book:
        return "EVO_STRAT"
    elif "Cred" in book:
        return "CREDITO"
    elif "FB" in book:
        return "EVO_STRAT"
    elif "FCO" in book:
        return "FRONTIER"
    elif "FLO" in book:
        return "FRONTIER"
    elif "FMN" in book:
        return "FRONTIER"
    elif "FS" in book:
        return "EVO_STRAT"
    # [continuação: verificar script completo para os ramos restantes]
    else:
        return None
```

## Categorias (taxonomy)

| Categoria | Significado | Exemplos de books mapeados |
|-----------|-------------|---------------------------|
| `FRONTIER` | Alocação na estratégia equity Long Only (Frontier) | AÇÕES BR LONG, FCO, FLO, FMN |
| `EVO_STRAT` | Estratégia interna do Evolution | Backbook, Baltra, Carry, CI_Equities, FB, FS, A1 |
| `MACRO` | Alocação em macro (commodities, FX, RF, RV global) | CI_COMMODITIES, CI_FX-*, CI_P-Metals, CI_RF-*, CI_RV-* |
| `CREDITO` | Alocação em crédito privado | CI_Cred, Cred* |
| `CAIXA` | Caixa / liquidez | Caixa, CAIXA |

## Uso na skill

```python
# 1. Trazer overview do EVOLUTION com todos os books
overview = get_df(query_overview_evolution)

# 2. Classificar
overview['CATEGORY'] = overview['BOOK'].apply(classificar_book)

# 3. Agregar métricas por CATEGORY
by_category = overview.groupby('CATEGORY').agg({
    'POSITION': 'sum',
    'DIA': 'sum',
    'MES': 'sum',
    # ... outras colunas relevantes
})
```

Fazer o mesmo para o VaR:
```python
book_var['CATEGORY'] = book_var['BOOK'].apply(classificar_book)
var_by_category = book_var.groupby('CATEGORY')['BOOK_VAR'].sum()
```

## Livro-razão do mandato do EVOLUTION

O mandato do EVOLUTION deve ter limites agregados **e** limites por categoria:

```json
{
  "fundo": "EVOLUTION",
  "nome_completo": "Galapagos Evolution FIC FIM CP",
  "familia": "multimercados",
  ...
  "limites": {
    "var_99_1d_total": {"hard": X, "soft": Y, "unidade": "%PL"},
    "exposicao_FRONTIER": {"hard": X, "soft": Y, "unidade": "%PL"},
    "exposicao_MACRO": {"hard": X, "soft": Y, "unidade": "%PL"},
    "exposicao_CREDITO": {"hard": X, "soft": Y, "unidade": "%PL"},
    "exposicao_EVO_STRAT": {"hard": X, "soft": Y, "unidade": "%PL"},
    "exposicao_CAIXA_minimo": {"hard": X, "soft": Y, "unidade": "%PL (piso)"},
    ...
  }
}
```

## Observações

1. **A função original parece incompleta no trecho que vimos** (arquivo original tem mais ramos após `"FS"`). Ao implementar, copiar a função inteira do arquivo real, não a versão aqui.
2. **Books novos não mapeados** retornam `None` — a skill deve flag-ar isso no relatório como "book não classificado", não silenciosamente ignorar.
3. **Alterações na função** precisam ser refletidas na skill. Se o time ajustar o `classificar_book` no script, a skill também precisa ajustar.

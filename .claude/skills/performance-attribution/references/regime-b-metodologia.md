# Regime B — Metodologia PA IDKA

Para os IDKAs, construímos a atribuição do zero porque o banco não tem PA oficial para eles.

## Decomposição canônica

```
R_fundo = R_benchmark_oficial + ER_replica + ER_ativo + resíduo
```

Onde:
- `R_fundo` — retorno realizado da cota
- `R_benchmark_oficial` — retorno IDKA Anbima (2A ou 10A)
- `ER_replica` — ER gerado por imperfeição da réplica
- `ER_ativo` — ER das apostas ativas (book RF_LF)
- `resíduo` — custos, taxas, efeitos não categorizáveis

## Passo a passo

### Passo 1 — R_fundo (retorno da cota)

```sql
SELECT "VAL_DATE", "TRADING_DESK", "NAV"
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" IN ('IDKA IPCA 3Y FIRF', 'IDKA IPCA 10Y FIRF')
ORDER BY "TRADING_DESK", "VAL_DATE"
```

```python
pivot = df.pivot(index='VAL_DATE', columns='TRADING_DESK', values='NAV')
returns = pivot.pct_change().dropna()
# Coluna 'IDKA IPCA 3Y FIRF' → retorno do fundo 3Y
```

### Passo 2 — R_benchmark_oficial (Anbima)

**Fonte a confirmar na primeira execução:**

```sql
SELECT DISTINCT "INSTRUMENT"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" ILIKE '%IDKA%';
```

Valores esperados (hipótese):
- `IDKA_IPCA_2A` — para o IDKA 3Y
- `IDKA_IPCA_10A` — para o IDKA 10Y

Se existem, query de retornos:

```sql
SELECT "DATE", "INSTRUMENT", "VALUE"
FROM "ECO_INDEX"
WHERE "INSTRUMENT" IN ('IDKA_IPCA_2A', 'IDKA_IPCA_10A')
  AND "DATE" BETWEEN '{data_inicio}' AND '{dia_atual}';
```

Se não existem, **a skill não pode calcular o Regime B** até a fonte ser identificada. Reportar PnL apenas (sem decomposição).

### Passo 3 — ER_replica e ER_ativo

**Abordagem simples (MVP):**

```python
# Puxar retorno por BOOK do IDKA (via REPORT_ALPHA_ATRIBUTION ou LOTE_DAILY_BENCHMARK)
ret_benchmark_book = ret_livro[ret_livro['BOOK'] == 'Benchmark_IDKA']['DIA'].sum()
ret_rf_lf_book    = ret_livro[ret_livro['BOOK'] == 'RF_LF']['DIA'].sum()

# ER_ativo: direto do book RF_LF
ER_ativo = ret_rf_lf_book

# ER_replica: diferença entre o que o book Benchmark_IDKA rendeu e o índice oficial,
# ajustado pela magnitude da posição
ano_eq_book_benchmark = ...  # do rf-idka-monitor
ano_eq_indice_oficial = 3.0  # para IDKA 3Y; 10.0 para 10Y

ret_esperado = R_benchmark_oficial * (ano_eq_book_benchmark / ano_eq_indice_oficial)
ER_replica = ret_benchmark_book - ret_esperado
```

### Passo 4 — Resíduo

```python
residuo = R_fundo - (R_benchmark_oficial + ER_replica + ER_ativo)
```

Se o resíduo for **> 2 bps no dia** ou **> 10 bps MTD**, flag — pode indicar:
- Custos/taxas não contabilizados
- Erro de atribuição
- Posição em book não mapeado

## Limitações conhecidas (para MVP)

1. **Aproximação linear** — a decomposição assume relação linear entre ANO_EQ e retorno. Para grandes movimentos de curva, convexidade importa.
2. **Não separa contribuição por vértice** — v1 trata "ER_ativo" como bloco único. Vértice fica para v2.
3. **Albatroz dentro do RF_LF** — se aplica, vai aparecer agregado em ER_ativo. Desagregar quando a identificação for calibrada.
4. **Perfil do índice oficial** — depende de fonte externa. Se não disponível, calcular só ER total (sem decompor em réplica vs. ativo).

## Validações

1. **Resíduo pequeno** (< 2 bps dia, < 10 bps MTD) — se não, investigar
2. **R_fundo bate com cota** — sanity check
3. **R_benchmark_oficial bate com Anbima publicado** — sanity check

## Contribuição por vértice (v2)

Para futura extensão, usar ANO_EQ por bucket de BDAYS (já calculado na `rf-idka-monitor`):

```python
for bucket in buckets_bdays:
    contribuicao = ano_eq_rf_lf_bucket * delta_taxa_bucket
```

Onde `delta_taxa_bucket` é a variação da taxa no vértice no dia. Requer série histórica de taxas da curva IPCA por vértice — possivelmente já em `FUTURES_PRICES` com DAP ou em tabela dedicada.

## Relatório final IDKA

```
**PA — IDKA 3Y — 2026-04-16**

R_fundo:                  +18.5 bps (dia)   +42.1 bps (MTD)   +180.5 bps (YTD)
R_IDKA 2A (Anbima):       +17.2 bps         +37.9 bps         +162.0 bps
Excess Return:            +1.3 bps          +4.2 bps          +18.5 bps

Decomposição do ER MTD:
  ER Réplica:   +0.5 bps  (book Benchmark_IDKA vs. índice oficial)
  ER Ativo:     +3.7 bps  (book RF_LF)
  Resíduo:      0.0 bps   ✓ OK

Contribuidores MTD (book RF_LF):
  Principal: NTN-B 2029 longo
  Secundário: Albatroz (se presente)
```

# Metodologia de Decomposição — ER do IDKA

Formalização da identidade usada no Regime B da `performance-attribution`.

## Identidade central

```
ER = R_fundo − R_benchmark

onde:
  ER           = Excess Return do fundo vs. benchmark oficial
  R_fundo      = retorno diário do fundo (cota)
  R_benchmark  = retorno diário do benchmark oficial (IDKA IPCA 2A ou 10A)
```

Decomposição por books do fundo:

```
ER ≈ TE_replica + Alpha_RF_LF + Residuo

onde:
  TE_replica    = PnL_book_Benchmark_IDKA − R_benchmark
  Alpha_RF_LF   = PnL_book_RF_LF
  Residuo       = diferença residual (outras posições, arredondamentos)
```

## Interpretação de cada componente

### TE_replica

- **Significado:** quanto a réplica operacional se afastou do índice oficial
- **Valor esperado:** próximo de zero
- **Interpretação quando positivo:** réplica ganhou por acaso (papel oscilou mais que o índice)
- **Interpretação quando persistentemente negativo:** slippage sistemático — investigar qualidade da réplica
- **Fonte de slippage típica:** reinvestimento de cupons em papéis diferentes dos oficiais, saldo de caixa operacional, timing de rebalanceamento

### Alpha_RF_LF

- **Significado:** retorno gerado pelas apostas ativas no book `RF_LF`
- **Valor esperado:** pode ser positivo ou negativo, é o "PL ativo" do gestor
- **Este é o número que define se o gestor está agregando ou destruindo valor no mês**

### Residuo

- **Significado:** diferença que não se explica pelos dois books
- **Valor esperado:** pequeno (< 0.5 bps diários)
- **Se alto:** há posições em outros books que precisam ser identificadas, ou há erro de marcação

## Quando o residuo é grande

Se `|Residuo| > 0.5 bps` em um dia, possíveis causas:

1. **Posições em book não esperado.** Rodar query:
   ```sql
   SELECT DISTINCT "BOOK"
   FROM "LOTE45"."LOTE_BOOK_OVERVIEW"
   WHERE "TRADING_DESK" = 'IDKA IPCA 3Y FIRF'
     AND "VAL_DATE" = '{dia_atual}'
   ```
   Se aparecer algo além de `Benchmark_IDKA` e `RF_LF`, incluir na análise.

2. **Aporte/resgate grande não ajustado.** Conferir `LOTE_APORTES` na janela.

3. **Papel em transição de book.** Alguma posição pode ter mudado de classificação entre ontem e hoje.

4. **Erro de dados.** Verificar cota e PnL manualmente no relatório diário oficial.

## Agregação temporal

Para horizontes além do dia (MTD, YTD):

**Opção A — composição de retornos (correto teoricamente):**
```python
ER_MTD = (1 + R_fundo_MTD) / (1 + R_bench_MTD) - 1
```

**Opção B — soma de ERs diários (aproximação, mais intuitiva):**
```python
ER_MTD = soma(ER_diario) ao longo do mês
```

**Convenção da skill:** usar Opção B (soma) para o relatório principal, reportar Opção A como validação quando a diferença for material (> 2 bps).

Motivo: gestores pensam "consumi X bps do budget este mês", que é soma; a composição só importa quando retornos acumulados passam de alguns %.

## Validação contínua

Em cada execução, a skill deve:

1. Calcular `ER_diario` dos últimos N dias (ex.: 5 dias)
2. Reconstruir soma `TE + Alpha` dos mesmos dias
3. Se `|ER − (TE + Alpha)|` > 1 bp em algum dia → flag "divergência histórica"

Isso detecta quebras de consistência na lógica (ex.: se um book novo foi introduzido e a skill não foi atualizada).

## Nota sobre benchmark indisponível

Se o `R_benchmark` não puder ser obtido (fonte oficial não acessível):

- **Não forçar um cálculo com proxy ruim.** Melhor reportar sem ER do que com ER errado.
- Apresentar apenas:
  - `R_fundo` (retorno da cota)
  - `PnL_book_Benchmark_IDKA / AUM` (retorno interno da réplica)
  - `PnL_book_RF_LF / AUM` (retorno interno do ativo)
- Flag claro: "ER não calculado — fonte oficial do benchmark IDKA pendente"

Neste modo degradado, o gestor ainda pode fazer leitura internal (a réplica está estável? o ativo deu alpha?). Falta apenas a referência externa.

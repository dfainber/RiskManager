# Glossário de Métricas — Convenções Internas

Definições precisas. Onde houver múltiplas convenções de mercado, a nossa está explicitada.

## Value at Risk (VaR)

- **Convenção padrão:** VaR paramétrico histórico, janela de 252 dias úteis, intervalo de confiança 99%, horizonte 1 dia útil.
- **Notação:** `VaR 99% 1d` sempre. Se outra convenção for usada em um fundo específico, registrar no mandato.
- **Leitura:** "VaR de 1.5% do PL" = em 1% dos dias, espera-se perda de pelo menos 1.5% do PL.
- **Limitação conhecida:** subestima risco em regime de baixa vol recente; por isso o stress test é complementar, não substituto.

## Stress Test

- Choque instantâneo sobre a carteira atual, sem rebalanceamento.
- Resultado em % do PL, positivo = perda.
- Cenários catalogados em `cenarios-stress.md`.

## Orçamento de Perda (Risk Budget)

- **Anual:** perda máxima tolerada no ano-calendário, % do PL de referência.
- **Mensal:** idem, no mês-calendário.
- **Peak-to-trough:** drawdown máximo tolerado desde o último pico de cota.
- **Utilização:** `perda_acumulada_no_período / orçamento_do_período`. Só conta se negativo (ganhos não reduzem orçamento de meses anteriores — cada mês reinicia).
- **Convenção de "perda":** variação de cota líquida de taxas, bruta de benchmark. Benchmark-relative entra só quando o fundo tem benchmark ativo e o mandato exige.

## DV01

- Sensibilidade a deslocamento de 1 basis point na curva.
- **Convenção:** DV01 expresso em R$ (não % do PL). `DV01 = R$ 100.000` significa que +1bp paralelo gera perda de R$ 100.000.
- DV01 por bucket: curto (até 1y), médio (1–5y), longo (5y+).

## Duration Modificada

- `DMod = -(1/P) × (dP/dy)`, em anos.
- Para carteiras com múltiplos indexadores: duration ponderada por exposição de cada bloco.

## Tracking Error

- **Ex-ante:** desvio-padrão anualizado da diferença esperada de retornos vs. benchmark, estimado via matriz de covariância 252d.
- **Ex-post:** idem, realizado (janela móvel 252d).
- Reportar ambos quando disponíveis; o relevante para limite é ex-ante.

## Beta

- **Ex-ante:** beta da carteira vs. benchmark, calculado bottom-up (β ponderado das posições, com betas individuais 252d).
- **Ex-post:** regressão de retornos do fundo vs. benchmark, janela 63d (3 meses) para sensibilidade a mudanças de posicionamento.

## Exposição Bruta / Líquida

- **Bruta:** soma do valor absoluto de todas as posições, em % do PL. Inclui longs, shorts, derivativos (notional ajustado por delta).
- **Líquida:** longs menos shorts, em % do PL.
- Em Renda Fixa, a exposição relevante costuma ser por indexador (pré, IPCA, CDI, cambial), não bruta/líquida genérica.

## Concentração

- **Top N:** soma das N maiores posições, em % do PL.
- **Maior posição individual:** posição singular com maior peso.
- Em crédito, concentração é sempre por **emissor** (grupo econômico), não por CNPJ ou ISIN. Papéis de CPFL Energia e CPFL Geração contam como um emissor.

## Liquidez

- **Convenção padrão:** % do PL liquidável em D+1 a 20% do ADV (average daily volume) dos últimos 21 dias úteis.
- Para papéis OTC (crédito): estimativa do gestor sobre dias para liquidar a preço justo, registrada manualmente.
- Para fundos com prazo de resgate > D+1, adaptar o horizonte (ex.: D+30 para fundos de crédito com cota mensal).

## Mapa de Rating (escala interna)

Mapeia as três agências para escala numérica comum. `rating_minimo` no mandato sempre em escala S&P.

| S&P / Fitch | Moody's | Escala interna |
|-------------|---------|----------------|
| AAA | Aaa | 1 |
| AA+ / AA / AA− | Aa1 / Aa2 / Aa3 | 2–4 |
| A+ / A / A− | A1 / A2 / A3 | 5–7 |
| BBB+ / BBB / BBB− | Baa1 / Baa2 / Baa3 | 8–10 |
| BB+ / BB / BB− | Ba1 / Ba2 / Ba3 | 11–13 |
| B+ / B / B− | B1 / B2 / B3 | 14–16 |
| CCC+ e abaixo | Caa1 e abaixo | 17+ |

**Investment grade:** escala ≤ 10 (BBB− ou melhor).

## Utilização (convenção universal)

Toda skill de rotina reporta utilização como:

```
utilização = valor_atual / limite_soft
```

NÃO usar limite hard como base — isso mascara a proximidade do alerta interno. Se a métrica for "piso" (liquidez, caixa mínimo), inverter: `utilização = limite_soft / valor_atual`.

Utilização > 1.0 significa soft ultrapassado (amarelo ou pior, depende de quão acima).

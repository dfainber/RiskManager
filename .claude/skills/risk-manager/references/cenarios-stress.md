# Biblioteca de Cenários de Stress

Cenários padronizados usados nos mandatos. Cada fundo elege os aplicáveis a ele no campo `cenarios_stress`.

## Convenções

- Cenários são **choques instantâneos** aplicados sobre a carteira atual (repricing estático, sem rebalanceamento).
- Resultado expresso como % do PL, sempre com sinal (positivo = perda).
- Atualização das definições sob responsabilidade do time de risco; mudanças devem ser comunicadas aos gestores com antecedência.

## Cenários históricos

| Nome | Descrição | Aplicabilidade |
|------|-----------|----------------|
| `crise_2008_set` | Choques de setembro/2008 (Lehman): equities −25%, USDBRL +20%, spread crédito +400bp, curva pré +300bp curta / +150bp longa | Todas as famílias |
| `joesley_mai17` | 18/05/2017: Ibov −9%, USDBRL +8%, pré longa +150bp, pré curta +250bp | Multimercados, Renda Fixa, Renda Variável |
| `covid_mar20` | Mar/2020: equities −35%, USDBRL +20%, crédito +500bp, curva pré +200bp curta | Todas as famílias |
| `eleicao_2022` | Vol eleitoral out/2022: Ibov ±8% (cenário bilateral), USDBRL +10%, pré longa +100bp | Multimercados, Renda Variável |
| `taper_tantrum_13` | Mai/2013: US10y +100bp, EM FX −10%, equities EM −12%, spread crédito +150bp | Multimercados, Renda Fixa, Crédito |

## Cenários paramétricos (curva de juros)

| Nome | Descrição | Aplicabilidade |
|------|-----------|----------------|
| `paralelo_+100bp` | Deslocamento paralelo da curva pré em +100bp | RF, Crédito, MM (exposição a juros) |
| `paralelo_-100bp` | Idem, negativo | RF, Crédito, MM |
| `inclinacao_steepener` | Curta −50bp, 2y flat, 5y +50bp, 10y +100bp | RF, MM |
| `inclinacao_flattener` | Curta +100bp, 2y +50bp, 5y flat, 10y −50bp | RF, MM |

## Cenários de crédito

| Nome | Descrição | Aplicabilidade |
|------|-----------|----------------|
| `spread_+50bp` | Abertura paralela de 50bp em todos os spreads de crédito | Crédito, MM com crédito |
| `spread_+100bp` | Idem, 100bp | Crédito, MM com crédito |
| `downgrade_setorial` | Spread do setor de maior concentração abre 200bp | Crédito |
| `default_maior_emissor` | Zerar recuperação do maior emissor individual (LGD 100%) | Crédito |

## Cenários de equities

| Nome | Descrição | Aplicabilidade |
|------|-----------|----------------|
| `ibov_-10` | Ibovespa −10%, betas aplicados individualmente | RV, MM com equities |
| `ibov_-20` | Idem, −20% | RV, MM com equities |
| `rotacao_value_growth` | Fator value +5%, growth −5% (para carteiras com fator claro) | RV |
| `short_squeeze_top5` | Top 5 shorts sobem +15% (L/S) | RV (L/S) |

## Cenário principal ("worst-case operacional")

Cada mandato deve eleger **um** cenário como principal — ele vira o `stress_cenario_principal` no bloco de limites. Sugestões por família:

- **Multimercados:** `crise_2008_set` ou `covid_mar20`, o mais severo para a composição.
- **Renda Fixa:** `paralelo_+100bp` se pré-fixada, ou combinação via `taper_tantrum_13`.
- **Crédito:** `spread_+100bp` + `default_maior_emissor` combinados.
- **Renda Variável:** `ibov_-20` ou `covid_mar20`.

## Manutenção

- Novos cenários exigem documentação dos choques exatos (não basta o nome).
- Revisão anual dos parâmetros históricos — verificar se a calibração ainda reflete o evento original (ex.: vol de mercado mudou, betas das ações mudaram).

# Metodologia de Percentis — Evolution Risk Concentration

Como calcular percentis históricos de VaR e correlação de forma robusta.

## Percentil do VaR (Camada 1)

### Convenção

Para uma estratégia `s`:
- `VaR_hoje_s`: valor do dia atual (bps)
- `Série_s`: valores dos últimos 252 dias úteis (inclusive hoje)

```python
import numpy as np
from scipy import stats

percentil = stats.percentileofscore(Série_s, VaR_hoje_s, kind='rank')
# Retorna 0-100
```

`kind='rank'` trata ties corretamente (média dos ranks). Alternativas (`strict`, `weak`, `mean`) dão diferenças pequenas; `rank` é o padrão recomendado.

### Janela e tamanho mínimo

- **Padrão:** 252 dias úteis
- **Mínimo operacional:** 126 dias úteis (~6 meses). Abaixo disso, os percentis são ruidosos.
- Se houver **< 126 dias** na janela (ex.: estratégia recém-criada), reportar "percentil provisório" e pedir cautela na leitura.
- Se houver **< 60 dias**, não calcular percentil — reportar só o valor absoluto.

```python
def calcular_percentil(serie, valor_hoje, min_sample=60, provisional_below=126):
    if len(serie) < min_sample:
        return None, "sample_insuficiente"
    
    pct = stats.percentileofscore(serie, valor_hoje, kind='rank')
    
    if len(serie) < provisional_below:
        return pct, "provisional"
    return pct, "ok"
```

## Percentil da correlação (Camada 3)

### Abordagem

Para cada par (A, B):

1. Calcular correlação de Pearson **rolling 63d** dos PnLs diários, gerando uma série temporal de correlações
2. Pegar a correlação de hoje (último valor da série rolling)
3. Calcular o percentil dessa correlação na janela 252d da própria série rolling

```python
import pandas as pd

# pnl_df: DataFrame com colunas = estratégias, index = datas
corr_rolling_63d = (disse VAR?
    pnl_df.rolling(window=63)
          .apply(lambda x: x.corr().loc[A, B] if len(x) >= 63 else np.nan)
)
# Alternativa mais eficiente: pairwise=True com pivot

correlacao_hoje = corr_rolling_63d.iloc[-1]
janela_historica = corr_rolling_63d.tail(252).dropna()
percentil = stats.percentileofscore(janela_historica, correlacao_hoje)
```

### Detalhes importantes

- **Sinal da correlação importa.** Percentil 95 de uma correlação que flutua tipicamente em [−0.3, +0.3] é diferente de percentil 95 de uma que flutua em [+0.4, +0.7]. Reportar sempre o valor da correlação + o percentil, nunca só um dos dois.
- **Ignorar NaNs no cálculo.** Estratégias com dias sem PnL (CREDITO pode ter) vão gerar NaNs na correlação — filtrar com `.dropna()`.
- **PnL zero não é NaN.** Se o book não gerou PnL num dia, mas existia e tinha posição, é zero. Tratar corretamente antes de chegar no cálculo.

## Percentil do Diversification Ratio (Camada 2)

Mesmo princípio da Camada 1, mas sobre a série do Ratio calculada dia a dia:

```python
# Para cada dia do histórico:
ratio_diario = VaR_real_dia / VaR_soma_dia

# Percentil de hoje
pct_ratio_hoje = stats.percentileofscore(ratio_diario.tail(252), ratio_diario.iloc[-1])
```

**Atenção:** o Ratio pode passar de 1 em situações excepcionais (ex.: VaR do fundo maior que a soma das partes — pode acontecer com posições de hedge que escondem risco nas agregações). Isso é sinal de dado ruim, não de diversificação negativa. Flag-ar se ocorrer em > 5% da série.

## Tratamento de outliers

### Spikes no VaR

Fundos de crédito ocasionalmente têm spikes de VaR por reprocessamento de marcação. Se um dia específico tem VaR > 3x a média móvel 21d, considerar:

1. **Não remover** da série — é um dado real
2. Mas **marcar no relatório** quando percentil de hoje é puxado por esses spikes antigos saindo da janela

### Dados faltantes

Se há buracos no histórico (dias úteis sem dado por falha de ETL):

- **Não interpolar** — preencher com NaN e reportar "janela efetiva: X dias com dado"
- Se faltam > 20 dias na janela de 252d, a janela deve ser estendida para trás para completar 252 dias **com dado**, se possível

## Regra de janela "cumulativa" vs. "fixa"

### Janela fixa (default)

Sempre 252 dias úteis antes de hoje. Vai rolando. Comparabilidade entre relatórios diários é boa, mas percentis mudam mesmo sem o valor de hoje mudar (por dados antigos saindo da janela).

### Janela desde início do ano (alternativa)

Percentil calculado apenas sobre dados do ano-calendário corrente. Vantagem: mais interpretável ("P80 da YTD"). Desvantagem: no início do ano tem pouquíssima amostra.

**Decisão:** usar janela fixa como default. Se usuário pedir visão YTD explicitamente, fornecer como camada extra.

## Output recomendado do cálculo

Cada percentil reportado deve vir acompanhado de:

- Valor numérico absoluto (bps, correlação em [-1,1], ou ratio)
- Percentil (0-100)
- Tamanho da amostra efetiva (nem sempre 252)
- Flag de "provisional" se amostra < 126
- Mínimo e máximo da janela (para contexto)
- Mediana da janela (valor "típico")

Exemplo de estrutura:

```json
{
  "valor_hoje": 42.1,
  "percentil": 78,
  "amostra": 252,
  "provisional": false,
  "min_janela": 12.3,
  "max_janela": 67.8,
  "mediana_janela": 35.4
}
```

## Pontos de atenção

- **Mudança de AUM não deve afetar o percentil de VaR em bps** — porque já está normalizado por AUM. Mas pode haver ruído se AUM teve mudança abrupta (aporte grande, resgate grande). Monitorar.
- **Mudança de composição do fundo** — se a gestão muda fundamentalmente a política (ex.: corta CREDITO pela metade), percentis históricos ficam enviesados por meses. Documentar mudanças estruturais na nota do relatório.
- **Calendário de dias úteis** — usar sempre calendário Anbima (`Get_AnbimaCalendar()` do `glpg-data-fetch`), nunca calendário comercial genérico.

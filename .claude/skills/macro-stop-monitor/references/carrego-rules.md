# Regra de Carrego — MACRO PM

**⚠️ STATUS: EM REVISÃO**

Este documento é um **stub** para a mecânica de carrego do stop por PM. A regra está sendo revisada pela gestão. Quando aprovada, preencher este documento com:

1. A fórmula exata de cálculo do stop vigente em função de PnL mês anterior e PnL acumulado
2. Tabela de decisão dos casos (PnL negativo, positivo, zero, sequência de negativos, etc.)
3. Implementação em Python da função `calcular_stop_vigente()` 
4. Testes contra os dois exemplos da planilha `Stop.xlsx`

## O que sabemos hoje (pendente de formalização)

A partir da leitura da planilha `Stop.xlsx`, seção "Regras":

1. **Stop Mês base:** −63 bps
2. **Stop Ano:** −252 bps
3. **Carrego negativo:** 50% do PnL mês anterior, se PnL negativo
4. **Carrego adicional:** 100% sobre o valor que "varar" o stop
5. **Carrego positivo** (stop maior que 63 bps): apenas se PnL acumulado está positivo
6. **Gancho:** se stop projetado para o próximo mês zerar, fica "de gancho" no próximo mês
7. **Base do carrego positivo:** sempre parte de 63
8. **Carrego negativo em sequência:** parte da base anterior se ela já for menor que o stop original (sequência de meses negativos). Mês positivo volta para stop de 63.
9. **First loss sobre base superior ao stop original:** adicional positivo é descontado em 25%; depois desconta no padrão 50% e 100% sobre o que varar o stop.

## Casos-teste obrigatórios (dos exemplos da planilha)

### Exemplo 1 (Stop Semestre = −256 bps)

PM acumulando PnL negativo mês a mês. Stop afunila até zerar, depois se recupera.

| Mês | Stop do mês (bps) | PnL mês (bps) | PnL acumulado (bps) |
|-----|-------------------|---------------|---------------------|
| 1 | −63 | −62 | −62 |
| 2 | −32 | −31 | −93 |
| 3 | −16.5 | −17 | −110 |
| 4 | −7.5 | −7 | −117 |
| 5 | −4 | −4 | −121 |
| 6 | −2 | −2 | −123 |
| 7 | −1 | −1 | −124 |
| 8 | −0.5 | −1 | −125 |
| 9 | 0 | 0 | −125 |
| 10 | −63 | 0 | −125 |
| 11 | −63 | −63 | −188 |
| 12 | −31.5 | −31.5 | −219.5 |

Observação: no mês 10, mesmo após vários meses zerados, o stop **volta a 63** porque o PM não acumulou novas perdas. A implementação precisa reproduzir esta "recuperação".

### Exemplo 2 (Stop Semestre = −128 bps, base 126→160)

PM com PnL positivo acumulado. Stop amplia progressivamente.

| Mês | Stop do mês (bps) | PnL mês (bps) | PnL acumulado (bps) |
|-----|-------------------|---------------|---------------------|
| 1 | −63 | 50 | 50 |
| 2 | −88 | 50 | 100 |
| 3 | −88 | 50 | 150 |
| 4 | −88 | 30 | 180 |
| 5 | −78 | −80 | 100 |
| 6 | −36 | −36 | 64 |
| 7 | −18 | 1 | 65 |
| 8 | −63.5 | 0 | 65 |
| 9 | −63 | −32 | 33 |
| 10 | −47 | −47 | −14 |
| 11 | −23.5 | −23 | −37 |
| 12 | −12 | −15 | −52 |

Observação: stop sai de −63 e abre para −88 quando acumulado positivo suficiente. Nos meses 5-6, PM perde e o mecanismo "engole" o carrego — no mês 7 o stop volta a −18 (muito apertado) refletindo a queda do acumulado.

## Interface da função (quando implementar)

```python
def calcular_stop_vigente(
    pnl_ytd_antes_do_mes_bps: float,    # PnL acumulado até o fim do mês anterior
    pnl_mes_anterior_bps: float,        # PnL do mês anterior isolado
    stop_mes_anterior_bps: float,       # Stop do mês anterior que vigorou
) -> dict:
    """
    Retorna:
    {
        'stop_mes_bps': float,           # Stop vigente para o mês atual
        'carrego_bps': float,            # Componente de carrego (positivo ou negativo)
        'base_carrego_bps': float,       # Base sobre a qual o carrego foi calculado
        'tipo_carrego': str,             # 'positivo' | 'negativo' | 'gancho' | 'nenhum'
    }
    """
    # IMPLEMENTAR QUANDO REGRA FOR APROVADA
    raise NotImplementedError("Regra de carrego ainda em revisão.")
```

## Validação requerida

Antes de ligar o cálculo automático na skill, deve passar em **ambos os testes-exemplo acima** com diferença zero em bps em todos os meses. Sem essa validação, manter `flag_regra_pendente = True` e reportar apenas stop base.

## Responsáveis

- **Aprovação da regra:** a definir
- **Implementação em Python:** a definir
- **Validação numérica:** a definir

# Parâmetros de Stop — MACRO

Valores-base da mecânica. Extraídos do arquivo `Stop.xlsx` mantido pela gestão.

## Tabela-mãe (da planilha)

| Nível | Tgt Alpha (anual) | Meta Mês | Stop1 | Stop2 | Carrego |
|-------|-------------------|----------|-------|-------|---------|
| Comitê | 3.50% | 0.2917% | −1.4583% | −2.3333% | — |
| PM | 2.50% (250 bps) | 0.2083% (20.83 bps) | −0.625% (−62.5 bps) | 0% | 50% mês anterior |

## Constantes operacionais (para a skill)

```python
# Stop base para PM individual
STOP_MES_BASE_PM_BPS = -63      # bps/mês
STOP_ANO_PM_BPS = -252           # bps/ano
META_MES_PM_BPS = 20.83          # bps/mês (target, não stop)
TGT_ALPHA_PM_BPS = 250           # bps/ano (target, não stop)

# Nível Comitê (fundo agregado — não monitorado por esta skill,
# mas documentado aqui para referência futura)
STOP_MES_BASE_COMITE_BPS = -145.83  # ~(−1.4583%)
STOP_ANO_COMITE_BPS = -233.33       # ~(−2.3333%)
TGT_ALPHA_COMITE_BPS = 350          # bps/ano
```

## Interpretação dos níveis

- **Stop Mês (PM = −63 bps):** limite que o PM pode perder no mês. Ultrapassar exige discussão com comitê e pode disparar carrego para o mês seguinte.
- **Stop Ano (PM = −252 bps):** limite acumulado no ano. Ultrapassar → provável reavaliação estrutural do mandato do PM.
- **Stop1 do PM (−62.5 bps):** valor muito próximo ao stop mês (−63). Na planilha aparece como "Stop1" do PM. Pode ser o gatilho soft (alerta), com stop mês sendo o hard. Confirmar.
- **Stop2 do PM (0 bps):** zero. Interpretação provável: "meta mínima" (break-even). Confirmar.
- **Meta Mês (20.83 bps):** target, não stop. Consistente com 250 bps/ano ÷ 12.

**Ponto aberto:** Stop1 e Stop2 na planilha têm relação exata com stop mês/ano? Parecem ser gatilhos diferentes. **Antes de implementar qualquer alerta baseado em Stop1/Stop2, confirmar a semântica com a gestão.**

## Nos dois exemplos da planilha

**Exemplo 1 (colunas C-F):** trajetória de stop afunilando — PM perde, stop vai reduzindo (63→32→16.5→7.5→4→2→1→0.5→0), atinge zero, e **volta para 63** no mês 10.

**Exemplo 2 (colunas I-M):** mostra como o carrego positivo abre o stop — stop sai de 63 e chega a 88 bps (base 160 referenciada no cabeçalho). Mostra que o stop pode **ampliar** quando PnL acumulado é positivo.

Esses exemplos são material de teste para quando a regra de carrego for implementada. A implementação precisa reproduzir exatamente as duas trajetórias.

## Histórico de alterações dos parâmetros

Manter lista aqui com data e motivo de cada mudança.

| Data | Parâmetro | Antes | Depois | Motivo |
|------|-----------|-------|--------|--------|
| 2026-04-16 | — | — | Baseline atual | Criação da skill |

## Pontos de contato

- **Definição da mecânica:** responsável pela gestão do MACRO (a confirmar)
- **Fonte de verdade numérica:** arquivo `Stop.xlsx` (caminho atual: a registrar)
- **Revisão de parâmetros:** tipicamente em revisões semestrais do mandato do PM

# Materialidade — Thresholds e Critérios

Este arquivo documenta a lógica de materialidade usada na Camada 2 (mudanças D vs. D-1). Os valores numéricos ficam em `assets/thresholds.json` — este arquivo é a **justificativa**.

## Critério atual

Uma mudança é material se passar nos **DOIS** critérios abaixo (AND lógico):

**1. Absoluto**
```
|Δ| ≥ 2 bps (em %PL)
```

**2. Relativo** (pelo menos um):
```
|Δ| ≥ 10% do VaR total do fundo
OU
|Δ| ≥ 5% do stop mensal (~3 bps)
```

## Por que interseção (AND) e não união (OR)

**União (OR) gera falsos positivos:**
- Item com VaR de 0.5 bps varia 40% → +0.2 bps. Passa no relativo, falha no absoluto. Não é Morning Call.
- Item com VaR de 50 bps varia 1 bps → 2% relativo. Passa no absoluto, falha no relativo. Ruído de vol histórica.

**Interseção (AND) captura o que importa:**
- Δ de 5 bps num item de 20 bps → passa em absoluto e passa em relativo (10%). Reporta.
- Δ de 2.5 bps num item de 30 bps → passa em absoluto, só 8% relativo... falha. Aceito — é zona de ruído.

## Thresholds atuais

| Parâmetro | Valor | Onde vive |
|-----------|-------|-----------|
| Absoluto mínimo | 2 bps | `thresholds.json → absolute_min_bps` |
| Relativo % VaR fundo | 10% | `thresholds.json → relative_var_pct` |
| Relativo % stop mensal | 5% | `thresholds.json → relative_stop_pct` |

## Histórico de ajustes

| Data | Parâmetro | Antes | Depois | Motivo |
|------|-----------|-------|--------|--------|
| 2026-04-16 | — | — | Baseline inicial | Criação da skill |

Atualizar esta tabela sempre que algum threshold mudar. Manter o histórico ajuda a interpretar relatórios antigos (mudança de threshold pode parecer mudança de risco).

## Calibração prática

Recomendação: começar com os valores acima e ajustar depois de **2-3 semanas de uso**. Sinais para aumentar (endurecer):
- Lista de materiais tem sempre 8+ itens → ruidoso
- Itens aparecendo como materiais que ninguém considera relevantes no call

Sinais para diminuir (afrouxar):
- Lista de materiais frequentemente vazia em dias que "claramente tiveram movimento"
- Gestor menciona no call algo que não apareceu no relatório

## Nota sobre Stress vs. VaR

O critério atual é aplicado sobre ΔVaR. Para ΔStress, a mesma lógica se aplica, mas com escalas típicas diferentes:

- VaR tipicamente em unidades de bps (ex.: 10-50 bps por book)
- Stress tipicamente 5-10x maior em magnitude (ex.: 50-300 bps por book)

Então os thresholds para Stress podem precisar ser **proporcionalmente maiores**. Por ora, usar os mesmos 2 bps / 10% / 5% — se ficar ruidoso no Stress, ajustar independentemente em `thresholds.json`.

## O que NÃO é considerado na materialidade

- **Variação de sinal** (mesmo <2 bps): se um book inverte de long para short, é material **por natureza**, independentemente do tamanho do Δ. Tratar como exceção e reportar com flag especial.
- **Ativos novos no risco**: se um book/PRODUCT aparece hoje e não existia ontem, reportar mesmo se VaR < 2 bps. Flag "posição nova introduzida".
- **Ativos saídos**: se um book/PRODUCT tinha VaR ≥ 5 bps ontem e não existe hoje, reportar. Flag "posição liquidada".

Esses três casos são **sempre** reportados, fora da lógica de threshold.

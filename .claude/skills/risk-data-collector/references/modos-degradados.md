# Modos Degradados — O que fazer quando falta dado

Mapa de comportamento de cada skill quando dependências estão atrasadas ou ausentes. Consumido pela `risk-data-collector` e pelas próprias skills analíticas.

## Princípios

1. **Atraso ≠ falha.** D-1 é degradação, não parada.
2. **Flag > silêncio.** Sempre sinalizar dado degradado no relatório.
3. **Blocker quer dizer blocker.** Se uma fonte `blocker` falha, a skill não roda — não tenta salvar com proxy.

## Matriz de modos degradados

### risk-daily-monitor

| Fonte atrasada | Comportamento |
|----------------|---------------|
| `LOTE_FUND_STRESS_RPM` (blocker) | Skill não roda. Morning Call reporta "monitor não gerado hoje". |
| `LOTE_BOOK_STRESS_RPM` (blocker) | Idem. |
| `LOTE_TRADING_DESKS_NAV_SHARE` (blocker) | Usar último AUM disponível com flag. Skill roda degradada. |
| `LOTE_BOOK_OVERVIEW` (important) | Roda sem decomposição por book. Flag no relatório. |

### macro-stop-monitor

| Fonte atrasada | Comportamento |
|----------------|---------------|
| `REPORT_ALPHA_ATRIBUTION` D-1 | Usar D-1 com flag "PnL do dia não disponível, usando último fechamento". MTD reportado até D-1. |
| `REPORT_ALPHA_ATRIBUTION` D-2+ | Skill não roda — gap excessivo. |

### macro-risk-breakdown

| Fonte atrasada | Comportamento |
|----------------|---------------|
| `LOTE_BOOK_STRESS_RPM` (blocker) | Skill não roda. |
| `LOTE_BOOK_OVERVIEW` (blocker) | Skill não roda — Camada 2 (ΔDelta) fica sem base. |
| `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` (important) | Camada 4 (shock absorption) omitida. Outras camadas rodam. Flag explícito. |

### evolution-risk-concentration

| Fonte atrasada | Comportamento |
|----------------|---------------|
| `LOTE_BOOK_STRESS_RPM` (blocker) | Skill não roda. |
| `LOTE_FUND_STRESS_RPM` (blocker) | Camada 2 (diversification ratio) bloqueada — skill não roda. |
| `REPORT_ALPHA_ATRIBUTION` (important) | Camada 3 (correlação de PnL) omitida. Camadas 1 e 2 rodam. |
| `RISK_DIRECTION_REPORT` (important) | Camada 4 condição 5 (smoking gun) omitida. Alerta combinado ainda pode disparar pelas outras 4. Flag explícito. |

### rf-idka-monitor

| Fonte atrasada | Comportamento |
|----------------|---------------|
| `LOTE_PARAMETRIC_VAR_TABLE` (blocker) | BVaR não disponível — skill não roda. |
| `LOTE_PRODUCT_BOOK_POSITION_PL` (blocker) | DV01/ANO_EQ não calculáveis — skill não roda. |
| `ECO_INDEX` para benchmark IDKA | PA relativa reportada sem excess return. |

### performance-attribution

| Fonte atrasada | Comportamento |
|----------------|---------------|
| `REPORT_ALPHA_ATRIBUTION` D-1 | Regime A (5 fundos) reporta PA de D-1 com flag. |
| `REPORT_ALPHA_ATRIBUTION` D-2+ | Regime A não roda. |
| `LOTE_TRADING_DESKS_NAV_SHARE` | Regime B não calcula retorno do fundo. |
| `ECO_INDEX` IDKA | Regime B não calcula excess return. Reporta só retorno interno. |

## Lógica de fallback implementada em cada skill

Sugestão de padrão em código:

```python
def run_skill(dia_alvo, manifest):
    deps = manifest['skills_afetadas'][skill_name]

    if deps == 'bloqueado':
        return {"status": "not_run", "reason": "Dependências blocker atrasadas"}

    if deps == 'degradado':
        # Rodar com flags
        result = run_with_flags(dia_alvo)
        result['degraded'] = True
        result['missing'] = manifest['fontes_atrasadas_da_skill']
        return result

    # Rodar normalmente
    return run_full(dia_alvo)
```

## Comunicação ao usuário

No Morning Call, skills em modo degradado devem aparecer com:

```
⚠️ macro-stop-monitor: modo degradado
   - PnL usando dados de 2026-04-15 (D-1)
   - Motor de atribuição precisa rodar para dados de hoje
```

E no sumário executivo do topo:

```
## Completude do Relatório
✅ 5 de 7 skills rodaram em modo completo
⚠️ 2 skills em modo degradado: macro-stop-monitor, performance-attribution
❌ Nenhuma skill bloqueada hoje
```

---
name: risk-data-collector
description: Coletor de dados da rotina de madrugada para o Morning Call. Verifica se todas as bases do GLPG-DB01 necessárias para as skills de risco estão atualizadas; se alguma está defasada, tenta disparar a rotina geradora; produz um manifesto JSON de status que é consumido pela risk-morning-call. Não substitui as rotinas existentes — verifica, tenta, e reporta. Use sempre que o usuário pedir "rodar coleta", "verificar dados de risco", "a rotina de hoje", "status das bases", "quais bases estão faltando", "executar coleta noturna".
---

# Risk Data Collector

Coletor/verificador de dados de madrugada. Primeiro elo da cadeia que termina no Morning Call.

**Princípio central:** esta skill **não substitui** as rotinas existentes. Ela **verifica**, **tenta recuperar** bases faltantes, e **reporta** um manifesto.

## Fluxo

1. Verificar bases (data esperada vs. atual)
2. Se alguma defasada → tentar disparar script gerador
3. Nova verificação pós-tentativa
4. Gerar manifesto JSON

## Bases monitoradas

| Base | Skill consumidora | Script gerador |
|------|-------------------|----------------|
| `LOTE45.LOTE_FUND_STRESS_RPM` | risk-daily-monitor, macro-risk-breakdown | rotina interna |
| `LOTE45.LOTE_BOOK_STRESS_RPM` | risk-daily-monitor, evolution-risk-concentration | rotina interna |
| `LOTE45.LOTE_PARAMETRIC_VAR_TABLE` | rf-idka-monitor | rotina interna |
| `LOTE45.LOTE_TRADING_DESKS_NAV_SHARE` | todas | rotina de cotas |
| `LOTE45.LOTE_BOOK_OVERVIEW` | várias | rotina de posições |
| `LOTE45.LOTE_PRODUCT_BOOK_POSITION_PL` | rf-idka-monitor | rotina de posições |
| `LOTE45.LOTE_DAILY_BENCHMARK` | performance-attribution | rotina de benchmark |
| `q_models.REPORT_ALPHA_ATRIBUTION` | performance-attribution, macro-stop-monitor | attribution_report_formulae.py |
| `q_models.PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` | macro-risk-breakdown | rotina de simulação |
| `q_models.RISK_DIRECTION_REPORT` | evolution-risk-concentration | rotina direcional |
| `ECO_INDEX` (IDKA, CDI, IPCA) | rf-idka-monitor, performance-attribution | rotina Anbima |

Lista viva em `assets/bases-monitoradas.json`.

## Verificação de frescor

```sql
SELECT MAX("VAL_DATE") AS ultima_data
FROM "{tabela}";
```

Regra:
- Base precisa ter `VAL_DATE = dia_atual` (ou último D.U. anterior)
- Defasagem 1 D.U. = amarelo; > 1 = vermelho

## Ações quando defasada

### Opção 1 — Disparar script gerador

Se a skill conhece o script (via `rotinas-geradoras.json`), executa:

```python
import subprocess
resultado = subprocess.run(
    ["python", path_script, dia_atual.isoformat()],
    capture_output=True, timeout=600
)
```

**Regras de segurança:**
- Timeout obrigatório (10 min default)
- Só executa scripts em `rotinas-geradoras.json` — **nunca comando arbitrário**
- Captura stdout/stderr
- Falhou → não tenta de novo

### Opção 2 — Seguir com o que tem

Se não puder disparar, marca a base como ausente e segue.

### Opção 3 — Modo interativo

Se usuário chamou manualmente (não madrugada), oferece:
- Aguardar
- Rodar manualmente
- Pular

Em modo batch (madrugada), **nunca trava** — segue com o que tem.

## Manifesto de status

Output: `<pasta-risco>/manifest/YYYY-MM-DD.json`

```json
{
  "data_base": "2026-04-16",
  "gerado_em": "2026-04-16T04:30:15",
  "modo": "automatico|manual",
  "status_geral": "completo|incompleto|falha",
  "bases": {
    "LOTE45.LOTE_FUND_STRESS_RPM": {
      "status": "ok|defasada|ausente|erro",
      "ultima_data_base": "2026-04-16",
      "defasagem_du": 0,
      "tentativa_recuperacao": null
    }
  },
  "skills_afetadas": [
    {"skill": "...", "impacto": "parcial|total", "razao": "..."}
  ],
  "sumario": {
    "total_bases": 11,
    "ok": 9,
    "defasadas": 1,
    "ausentes": 0,
    "erros": 1
  }
}
```

## Regras de comportamento

- **Nunca rodar scripts não-listados** em `rotinas-geradoras.json`
- **Sempre timeouts**
- **Logging persistente** em `<pasta-risco>/logs/collector-YYYY-MM-DD.log`
- **Idempotência** — rodar 2x no mesmo dia deve dar o mesmo resultado
- **Falha não-fatal** — uma base falhar não afeta as outras
- **Read-only no banco** — só grava em filesystem próprio

## Agendamento sugerido

- **03:00** primeira verificação
- **04:00** tentativa de recuperação
- **05:00** verificação final e geração do manifesto
- **06:00+** `risk-morning-call` consome

Agendamento é via cron/Task Scheduler externo, não faz parte da skill.

## Referências

- `references/bases-checklist.md` — queries de verificação por base
- `references/rotinas-geradoras.md` — mapa base → script
- `references/modo-interativo.md` — fluxo manual

## Assets

- `assets/bases-monitoradas.json` — lista viva
- `assets/rotinas-geradoras.json` — mapa script ↔ base

## Skills relacionadas

- `risk-morning-call` — consome o manifesto
- Todas as skills de análise — dependem das bases

## Roadmap

1. **MVP:** verificação + manifesto (sem disparar scripts)
2. **v2:** tentativa de disparo para scripts bem mapeados
3. **v3:** integração com alerta (email/Slack) em falhas

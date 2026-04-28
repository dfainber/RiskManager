---
name: macro-stop-monitor
description: Monitora orçamento de perda (stop) mensal/anual por PM (CI, LF, JD, RJ, QM) no Galapagos Macro FIM. Lê PnL de REPORT_ALPHA_ATRIBUTION, compara com stop (base 63 bps/mês, 252 bps/ano + carrego), emite estado verde/amarelo/vermelho. Faz também fechamento mensal (snapshot JSON p/ carrego). Use para pedidos sobre stop/orçamento dos PMs, carrego, PnL por PM, fechar mês, ou menções aos PMs do MACRO no contexto de stop.
---

# Macro Stop Monitor — Orçamento por PM

Monitoramento diário do **orçamento de perda por gestor (PM)** dentro do `Galapagos Macro FIM`. Cada PM tem um stop individual com mecânica própria (base + carrego), e o objetivo é ver o consumo do stop em tempo útil para discussão no Morning Call.

**Dependência conceitual:** `risk-manager` (semáforo, taxonomia).  
**Dependência técnica:** `glpg-data-fetch` (padrão de conexão ao GLPG-DB01).  
**Escopo:** exclusivamente o MACRO. Outros fundos não usam esta mecânica.

## PMs ativos

| Inicial | Filtro no banco | Papel |
|---------|-----------------|-------|
| CI | `LIVRO` contém "CI" | Gestor |
| LF | `LIVRO` contém "LF" | Gestor |
| JD | `LIVRO` contém "JD" | Gestor |
| RJ | `LIVRO` contém "RJ" | Gestor |
| QM | `LIVRO` contém "QM" | Gestor |

Lista mantida em `pms-registry.json`. Atualizar quando PM entra/sai.

## Dois modos de operação

Esta skill tem **duas rotinas** distintas:

### Modo 1 — Monitor diário (leitura)
Executa várias vezes ao longo do mês. Lê PnL por PM direto do banco, calcula consumo do stop vigente, emite semáforo. Não altera estado persistente.

**Gatilhos:** *"como está o stop dos PMs"*, *"rodar monitor MACRO"*, *"como estão os gestores hoje"*.

### Modo 2 — Fechamento mensal (escrita)
Executa **uma vez por mês**, manualmente disparado pelo usuário após o último dia útil. Consolida o PnL do mês por PM, grava snapshot em JSON, atualiza o índice. Esse snapshot é a **base de cálculo do carrego do mês seguinte** — sem ele, a regra de carrego não pode ser aplicada.

**Gatilhos:** *"fechar mês"*, *"fechar março/2026"*, *"rodar snapshot mensal"*, *"gerar fechamento de <mês>"*.

**Passo a passo do fechamento em `references/monthly-snapshot.md`.** Nunca gravar sem pedir confirmação ao usuário.

---

## Parâmetros da mecânica de stop

Valores-base, extraídos do arquivo `Stop.xlsx` (planilha fonte da gestão):

| Parâmetro | PM | Comitê |
|-----------|-----|--------|
| Tgt Alpha anual | 250 bps (2.50%) | 350 bps (3.50%) |
| Meta Mês | 20.83 bps | 2.92 bps |
| Stop1 | −62.5 bps | −145.8 bps |
| Stop2 | 0 bps | −233.3 bps |
| Carrego | 50% do mês anterior | — |

**Regras operacionais (conforme planilha):**
- **Stop Mês (base):** −63 bps
- **Stop Ano:** −252 bps
- **Carrego negativo:** 50% do PnL mês anterior (se negativo)
- **Carrego adicional:** 100% sobre o valor que "varar" o stop
- **Carrego positivo** (stop > 63 bps): só se PnL acumulado positivo
- **Gancho:** se stop projetado para próximo mês zerar → próximo mês fica "de gancho"
- **Base de carrego positivo:** sempre parte de 63
- **Carrego negativo em sequência:** parte da base anterior se já menor que stop original; mês positivo volta para 63
- **First loss sobre base superior ao stop original:** adicional positivo desconta 25%; depois padrão 50% e 100% sobre o que varar

**⚠️ A regra de carrego está em revisão.** Esta skill documenta a regra conforme a planilha atual, mas a lógica de cálculo automático do carrego **não é executada** até que a regra seja finalizada. Enquanto isso, a skill reporta:
- PnL do mês e do ano por PM
- Stop base vigente (63/252)
- Utilização do stop base (sem carrego aplicado)
- Flag "regra de carrego pendente" em todos os cálculos sujeitos

Quando a regra for travada, implementar em `references/carrego-rules.md` e na função `calcular_stop_vigente()`.

## Workflow

### Passo 1 — Setup

1. Determinar `dia_atual` (último dia útil se não informado).
2. Conectar ao GLPG-DB01.
3. Carregar `pms-registry.json` e parâmetros de stop.

### Passo 2 — Extrair PnL por PM

Query principal (extraída direto do `MACRO_TABLES_GRAPHS.py`):

```sql
SELECT "BOOK", "DATE", "GRUPO", "LIVRO", "CLASSE" AS "CATEGORIA",
       "PRODUCT_CLASS", "PRODUCT", "DIA", "MES"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'MACRO'
  AND "DATE" = DATE '{dia_atual}'
  AND "MES" <> 0
```

**Normalização:**
- `DIA` e `MES` vêm como decimal → multiplicar por 10000 para ter em bps
- Agregar por PM: filtrar `LIVRO` contains inicial, somar `DIA` e `MES`

```python
pnl_por_pm = {}
for pm in ['CI', 'LF', 'JD', 'RJ', 'QM']:
    df_pm = df_pnl[df_pnl['LIVRO'].str.contains(pm, case=False)]
    pnl_por_pm[pm] = {
        'dia_bps': df_pm['DIA'].sum() * 10000,
        'mes_bps': df_pm['MES'].sum() * 10000,
    }
```

### Passo 3 — PnL acumulado do ano (para stop anual)

Query auxiliar para agregar PnL mês a mês no ano-calendário:

```sql
SELECT DATE_TRUNC('month', "DATE") AS "MES_REF",
       "LIVRO",
       SUM("DIA") * 10000 AS "PNL_MES_BPS"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'MACRO'
  AND "DATE" >= DATE_TRUNC('year', DATE '{dia_atual}')
  AND "DATE" <= DATE '{dia_atual}'
GROUP BY DATE_TRUNC('month', "DATE"), "LIVRO"
ORDER BY "MES_REF"
```

Pós-processamento:
- Filtrar por PM (via `LIVRO contains`)
- PnL YTD = soma dos meses
- Utilização stop anual = |PnL YTD negativo| / 252 bps

### Passo 4 — Cálculo do stop vigente por PM

**Enquanto a regra de carrego não é definitiva**, usar stop base:

```python
def calcular_stop_vigente(pm, mes_atual, pnl_mes_anterior_bps, pnl_ytd_bps):
    """
    PLACEHOLDER. Implementação simplificada (só stop base, sem carrego).
    
    Quando a regra de carrego for definida, substituir pela lógica completa
    descrita em references/carrego-rules.md.
    """
    STOP_MES_BASE = -63  # bps
    STOP_ANO = -252      # bps
    
    return {
        'stop_mes_bps': STOP_MES_BASE,
        'stop_ano_bps': STOP_ANO,
        'carrego_aplicado': None,
        'flag_regra_pendente': True,
    }
```

### Passo 5 — Aplicar semáforo

**Por PM, para PnL mês:**

| Estado | Critério | |
|--------|----------|---|
| 🟢 Verde | PnL_mês ≥ 0, ou PnL_mês > 70% × stop_mes_base | Positivo ou consumo < 70% |
| 🟡 Amarelo | PnL_mês entre 70% e 100% do stop_mes_base (i.e. −44 a −63 bps) | Consumo relevante |
| 🔴 Vermelho | PnL_mês entre 100% e 150% do stop_mes_base (−63 a −94 bps) | Estourou stop base |
| ⚫ Breach | PnL_mês abaixo de 150% do stop_mes_base (< −94 bps) | Escalation |

Para stop anual, mesma lógica usando 252 bps como base.

### Passo 6 — Consolidação

```
# Stop Monitor MACRO — [data-base]

## Resumo
- PMs ativos: CI, LF, JD, RJ, QM
- ⚠️ Regra de carrego em revisão — cálculos usam stop base (63/252)

## PnL do Dia
| PM | PnL Dia (bps) |
|----|---------------|
| CI | +X.X |
| LF | −X.X |
| ...

## PnL Mês vs. Stop Mês (−63 bps base)
| PM | PnL Mês (bps) | Utilização | Estado |
|----|---------------|------------|--------|
| CI | −45 | 71% | 🟡 |
| ...

## PnL YTD vs. Stop Ano (−252 bps)
| PM | PnL YTD (bps) | Utilização | Estado |
|----|----------------|------------|--------|
| ...

## Exceções (amarelo ou pior)
- [PM] PnL mês −XX bps (YY% do stop) → atenção
- ...

## Avisos
- ⚠️ Carrego do mês ainda não calculado: regra em definição
- [Outros avisos]
```

### Passo 7 — Snapshot

Salvar em `snapshots/<dia>_macro_stops.json`:
```json
{
  "data_base": "YYYY-MM-DD",
  "pms": {
    "CI": {"pnl_dia_bps": X, "pnl_mes_bps": Y, "pnl_ytd_bps": Z, "estado_mes": "🟢"},
    ...
  },
  "flags": ["carrego_pendente"]
}
```

Consumido pela `risk-morning-call` no bloco de MACRO.

## Regras de comportamento

- **Jamais calcular carrego** até a regra ser aprovada. Reportar só o stop base.
- **Jamais confundir Comitê e PM.** São parâmetros distintos. Esta skill monitora PM; Comitê é nível de fundo (vai pra outro lugar).
- **Sempre mostrar os 5 PMs**, mesmo que um esteja sem posição no mês. Linha com zeros é informação útil.
- **PnL realizado + não realizado** — confirmar que a coluna `DIA`/`MES` da tabela `REPORT_ALPHA_ATRIBUTION` considera ambos (é o padrão esperado, mas vale sanity-check na primeira execução).

## Referências

- `references/monthly-snapshot.md` — **workflow de fechamento mensal.** Como gravar e recuperar o snapshot de PnL por PM (fim do mês = base do carrego do mês seguinte).
- `references/carrego-rules.md` — **stub**. Documentar aqui a regra final do carrego quando aprovada, com exemplos numéricos.
- `references/stop-parameters.md` — parâmetros fixos (stop base, meta, tgt alpha) e histórico de alterações.
- `assets/pms-registry.json` — lista de PMs ativos.
- `assets/stop-example.xlsx` — planilha original `Stop.xlsx` como referência (copiada aqui para rastreabilidade).

## Skills relacionadas

- **`risk-manager`** — taxonomia geral.
- **`risk-daily-monitor`** — monitora fundo agregado; esta skill é complementar, específica para o MACRO em nível PM.
- **`glpg-data-fetch`** — padrão de conexão ao banco.
- **`risk-morning-call`** (a criar) — agrega o output desta skill no briefing do dia.

## Roadmap

1. **Agora (Modo 1):** reportar stop base + PnL sem carrego. Flag de regra pendente em toda saída.
2. **Agora (Modo 2):** fechamento mensal manual ativo desde já. Começar a acumular snapshots mesmo antes da regra de carrego ser implementada — quando a regra entrar em vigor, já teremos histórico para aplicar retroativamente.
3. **Quando carrego for definido:** implementar `calcular_stop_vigente()` completo, lendo o snapshot do mês anterior. Rodar contra a planilha `Stop.xlsx` (os dois exemplos de trajetórias devem reproduzir exatamente).
4. **Futuro:** histórico de carrego acumulado por PM (para detectar padrões de uso e ajustes necessários no modelo). Auditoria automatizada dos snapshots contra dados vivos do banco.

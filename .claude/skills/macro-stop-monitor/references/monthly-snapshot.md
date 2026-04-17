# Snapshot Mensal — Fechamento por PM

Para aplicar a regra de carrego, o **stop do mês N+1 depende do PnL do mês N**. Portanto, ao final de cada mês, a skill precisa **gravar e persistir** o PnL fechado de cada PM. Sem isso, nunca conseguiremos calcular o stop vigente automaticamente.

## Estrutura de arquivos

Um arquivo JSON por mês × PM. Pasta dedicada:

```
<pasta-risco>/macro-stops/
├── snapshots/
│   ├── 2026-01/
│   │   ├── CI.json
│   │   ├── LF.json
│   │   ├── JD.json
│   │   ├── RJ.json
│   │   └── QM.json
│   ├── 2026-02/
│   │   └── ...
│   └── ...
└── _index.json
```

O `_index.json` lista todos os snapshots fechados, para a skill saber rapidamente qual o mês fechado mais recente:

```json
{
  "ultimo_mes_fechado": "2026-03",
  "snapshots": [
    {"mes": "2026-01", "pms": ["CI","LF","JD","RJ","QM"], "fechado_em": "2026-02-03T09:15:22", "fechado_por": "bruno"},
    {"mes": "2026-02", "pms": ["CI","LF","JD","RJ","QM"], "fechado_em": "2026-03-04T08:40:11", "fechado_por": "bruno"},
    {"mes": "2026-03", "pms": ["CI","LF","JD","RJ","QM"], "fechado_em": "2026-04-02T09:02:55", "fechado_por": "bruno"}
  ]
}
```

## Schema do snapshot (`<YYYY-MM>/<PM>.json`)

**Conteúdo mínimo** (decisão atual: só PnL do mês + YTD):

```json
{
  "pm": "CI",
  "fundo": "MACRO",
  "mes_ref": "2026-03",
  "pnl_mes_bps": -45.3,
  "pnl_ytd_bps": -12.7,
  "data_base_ultimo_dia_util": "2026-03-31",
  "fechado_em": "2026-04-02T09:02:55",
  "fechado_por": "bruno",
  "fonte_query": "q_models.REPORT_ALPHA_ATRIBUTION"
}
```

**Convenções:**
- `pnl_mes_bps` e `pnl_ytd_bps` em **bps com sinal** (negativo = perda).
- `mes_ref` no formato `YYYY-MM` (sem dia).
- `data_base_ultimo_dia_util` é o último dia útil do mês — usado para auditar contra o banco se necessário.
- `fechado_por` é quem rodou o comando — sempre preencher.

**Por que tão enxuto:** a decisão foi guardar só o essencial. Se no futuro precisar de mais (stop que vigorou, PnL diário do mês, etc.), estender o schema sem quebrar compatibilidade (campos novos são opcionais).

## Workflow do fechamento manual

Quando o usuário invocar "fechar mês" (frases-gatilho: *"rodar fechamento de março"*, *"fechar mês passado"*, *"gerar snapshot mensal"*, *"fechar stop dos PMs"*):

### Passo 1 — Determinar o mês a fechar

1. Se usuário especifica mês (ex.: "fechar março/2026") → usar esse.
2. Se não especifica → assumir **mês anterior ao atual** (último mês completo).
3. Validar: se o mês já tem snapshots em `snapshots/<YYYY-MM>/`, avisar e perguntar se é pra sobrescrever.

### Passo 2 — Determinar a data-base

Último dia útil do mês alvo, usando calendário Anbima (mesma função `offset_BDays_from` dos scripts).

```python
from GLPG_BDays import offset_BDays_from, Get_AnbimaCalendar
import datetime as dt

hol = Get_AnbimaCalendar()["DATE"]
primeiro_dia_prox_mes = dt.date(ano, mes, 1) + dt.timedelta(days=32)
primeiro_dia_prox_mes = primeiro_dia_prox_mes.replace(day=1)
ultimo_du_mes = offset_BDays_from(primeiro_dia_prox_mes, -1, hol, Roll="backward")[0]
```

### Passo 3 — Buscar PnL do mês e YTD por PM

Query principal:

```sql
SELECT "LIVRO",
       SUM("DIA") * 10000 AS "PNL_MES_BPS"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'MACRO'
  AND "DATE" BETWEEN DATE_TRUNC('month', DATE '{ultimo_du_mes}')
                 AND DATE '{ultimo_du_mes}'
GROUP BY "LIVRO"
```

Query YTD:

```sql
SELECT "LIVRO",
       SUM("DIA") * 10000 AS "PNL_YTD_BPS"
FROM q_models."REPORT_ALPHA_ATRIBUTION"
WHERE "FUNDO" = 'MACRO'
  AND "DATE" BETWEEN DATE_TRUNC('year', DATE '{ultimo_du_mes}')
                 AND DATE '{ultimo_du_mes}'
GROUP BY "LIVRO"
```

Agregar por PM (somando todos os livros que contêm a inicial):

```python
def agregar_por_pm(df, pm):
    return df[df['LIVRO'].str.contains(pm, case=False)]['PNL_MES_BPS'].sum()
```

### Passo 4 — Preview antes de gravar

**Nunca gravar direto.** Mostrar ao usuário a tabela resultante e pedir confirmação:

```
Mês a fechar: 2026-03
Data-base (último D.U.): 2026-03-31

PM | PnL Mês (bps) | PnL YTD (bps)
---|---------------|---------------
CI |  +12.3        |  +45.1
LF |  -45.3        |  -12.7
JD |  -8.1         |  +23.4
RJ |  +5.5         |  +31.2
QM |  -22.0        |  -5.8

Confirmar fechamento? [s/n]
```

Se o usuário confirmar → prosseguir. Se não → abortar sem gravar.

### Passo 5 — Gravar os snapshots

Para cada PM, escrever `snapshots/<YYYY-MM>/<PM>.json` com o schema acima.

Atualizar `_index.json`:
- Adicionar entry para o mês fechado
- Atualizar `ultimo_mes_fechado`

**Atomicidade:** gravar todos os 5 PMs antes de atualizar o índice. Se falhar no meio, deixar o índice inalterado (estado "meio-fechado" não deve aparecer no índice).

### Passo 6 — Retornar resumo

```
✅ Fechamento de 2026-03 concluído.
Arquivos gravados em <pasta>/snapshots/2026-03/:
  - CI.json
  - LF.json
  - JD.json
  - RJ.json
  - QM.json

PnL consolidado do mês:
  Agregado MACRO: −57.6 bps
  Em utilização de stop base (−63 bps): 91% → 🟡
```

## Retificação

Se o usuário precisar **corrigir** um snapshot já fechado (ex.: PnL foi revisado após ajuste de cota):

- Frase-gatilho: *"refazer fechamento de março"*, *"corrigir snapshot do PM X em março"*.
- A skill deve:
  1. Ler o snapshot atual e mostrar os valores vigentes.
  2. Fazer nova query (mesmos SQL do Passo 3) e mostrar os novos valores.
  3. Pedir confirmação explícita com diff.
  4. Se confirmado: sobrescrever o JSON e adicionar entry no histórico de revisões (dentro do próprio JSON):

```json
{
  "pm": "CI",
  "mes_ref": "2026-03",
  "pnl_mes_bps": -44.0,
  "pnl_ytd_bps": -11.4,
  "data_base_ultimo_dia_util": "2026-03-31",
  "fechado_em": "2026-04-02T09:02:55",
  "fechado_por": "bruno",
  "historico_revisoes": [
    {
      "em": "2026-04-15T14:23:11",
      "por": "bruno",
      "motivo": "Ajuste de cota retroativo",
      "valores_anteriores": {"pnl_mes_bps": -45.3, "pnl_ytd_bps": -12.7}
    }
  ]
}
```

## Leitura por outras skills

Quando `macro-stop-monitor` for calcular o stop vigente do mês atual (com a regra de carrego, quando implementada):

```python
def get_last_closed_month_pnl(pm: str, current_month: str) -> dict | None:
    """
    Retorna {pnl_mes_bps, pnl_ytd_bps} do mês fechado mais recente antes do current_month.
    Retorna None se não houver snapshot (ex.: primeiro mês de operação).
    """
    # 1. Ler _index.json
    # 2. Encontrar o mês imediatamente anterior a current_month que tenha o PM
    # 3. Ler snapshots/<mes>/<PM>.json e retornar os campos
```

**Comportamento quando não há snapshot:**
- Primeira execução histórica (nenhum mês fechado) → reportar "sem histórico, usando stop base −63" e pedir pro usuário fechar o mês anterior quando possível.
- Snapshot faltando no meio → flag vermelho no relatório: "snapshot de <mês> não encontrado para <PM>".

## Ponto aberto

**Auditoria contra dados vivos:** quando o mês N+1 está em curso, os dados do mês N no banco podem ainda mudar (raras retificações). A skill poderia periodicamente **comparar** o snapshot gravado com o banco atual e alertar se divergirem. Fica como melhoria futura; por ora, rastreia-se via `historico_revisoes` quando o usuário explicitamente corrigir.

---
name: risk-daily-monitor
description: Rotina diária de monitoramento de risco para os fundos Multimercados da Galapagos (Galapagos Macro FIM, Galapagos Quantitativo FIM, Galapagos Evolution FIC FIM CP). Lê dados direto do banco GLPG-DB01 (tabelas LOTE_FUND_STRESS_RPM, LOTE_BOOK_STRESS_RPM, PORTIFOLIO_DAILY_HISTORICAL_SIMULATION, LOTE_TRADING_DESKS_NAV_SHARE), cruza com mandatos e emite estado verde/amarelo/vermelho por fundo × métrica. Use sempre que o usuário pedir "rodar o monitor", "checar o risco hoje", "como está o MACRO/QUANT/EVOLUTION", "varredura diária", "VaR dos MM", "preparação Morning Call" ou mencionar qualquer fundo MM no contexto de risco diário. Suporte para outras famílias (RF, Crédito, RV) será adicionado incrementalmente.
---

# Risk Daily Monitor — Multimercados

Rotina diária de monitoramento para os três fundos MM ativos:

| Subfamília | TRADING_DESK | Descrição |
|-----------|--------------|-----------|
| MACRO | `Galapagos Macro FIM` | Multimercado macro discricionário |
| QUANT | `Galapagos Quantitativo FIM` | Multimercado sistemático/quantitativo (era `SISTEMATICO` no design original — renomeado p/ bater com o nome do banco) |
| EVOLUTION | `Galapagos Evolution FIC FIM CP` | Guarda-chuva multi-estratégia (FRONTIER, MACRO, CREDITO, CAIXA) |

**Dependência conceitual:** `risk-manager` (taxonomia, semáforo, schema de mandato). **Dependência técnica:** `glpg-data-fetch` (conexão ao GLPG-DB01 via `GLPG_DBAPI`).

## Princípio: ler do banco, não do HTML

Os scripts existentes em `F:\Bloomberg\Quant\Rotinas\RELATORIO_EXPO_PNL_AUTOMATICO_HTML\` (`MACRO_TABLES_GRAPHS.py`, `SISTEMATICO_TABLES_GRAPHS.py`, `EVOLUTION_TABLES_GRAPHS.py`) geram dashboards HTML via Panel, mas **as queries SQL por trás deles são a fonte canônica**. Esta skill replica aquelas queries, não parseia o HTML.

Vantagens:
- Não depende de o dashboard ter sido gerado no dia
- Valores numéricos sem perda de precisão de formatação
- Pode olhar qualquer data histórica, não só hoje
- Rápido (1 conexão ao banco vs. inspeção de DOM)

Os HTMLs continuam sendo úteis como **referência visual** para o usuário, mas esta skill não depende deles.

## Workflow

### Passo 1 — Setup e data-base

1. Determinar `dia_atual` — se não informado, usar último dia útil (calendário Anbima).
2. Abrir conexão ao GLPG-DB01 via `GLPG_DBAPI.dbinterface` (credenciais em `references/queries-mm.md`).
3. Carregar o registro de fundos ativos (`fund-registry.json`) e os mandatos correspondentes (`mandato-<fundo>.json`).

### Passo 2 — Extração de métricas por fundo

Para cada fundo do registro, executar o bloco de queries específico. Ver queries completas em `references/queries-mm.md`. Resumo:

**Métricas universais (MACRO, SISTEMATICO, EVOLUTION):**

| Métrica | Fonte | Query-chave |
|---------|-------|-------------|
| AUM | `LOTE_TRADING_DESKS_NAV_SHARE` | `NAV` por `TRADING_DESK` na data |
| Fund VaR (%PL) | `LOTE_FUND_STRESS_RPM` | `PARAMETRIC_VAR` × −10000 / AUM |
| Book VaR (%PL) | `LOTE_BOOK_STRESS_RPM` | Idem, por BOOK, LEVEL=3 |
| VaR histórico (série) | `LOTE_FUND_STRESS_RPM` | Janela ≥ 2025-01-01 |
| Returns & drawdowns | `q_models.PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` | Max DD em 1/2/5/10 dias |
| Exposições por BOOK | `LOTE_BOOK_OVERVIEW` | Soma posições |

**Campos `BOOK` conhecidos:**

- **MACRO:** `RF-BZ, RF-EM, RF-DM, FX-BRL, FX-EM, FX-DM, RV-BZ, RV-EM, RV-DM, COMMODITIES, P-Metals, ALL` (ativos); `CI, LF, JD, RJ, QM, ALL` (gestores)
- **SISTEMATICO:** (a confirmar na primeira execução — provavelmente similar a MACRO)
- **EVOLUTION:** `FRONTIER, EVO_STRAT, MACRO, CREDITO, CAIXA` (categorias de book via função `classificar_book`)

### Passo 3 — Normalização

- VaR sempre em **%PL positivo** (perda esperada). Fórmula padrão dos scripts: `PARAMETRIC_VAR × −10000 / AUM`.
- PnL em **bps** (basis points), horizontes: DIA, MES.
- Exposições em `%PL`.

### Passo 4 — Cruzamento com mandato

Para cada fundo × métrica extraída:

1. Buscar `mandato-<fundo>.json` (schema em `risk-manager`).
2. Se existe: calcular `utilização = valor / soft_limit` (inverso para pisos).
3. Aplicar semáforo (🟢 < 70%, 🟡 70–100%, 🔴 > 100% soft, ⚫ > 100% hard).
4. Se não existe: reportar valor absoluto + flag "sem mandato". **Jamais aplicar default.**

### Passo 5 — Detecção de tendência

Com histórico da `var_historico_query` (≥ 2025-01-01), calcular:
- Variação 1d, 5d, 21d do Fund VaR
- VaR subindo > 20% em 5d úteis → amarelo mesmo se nível absoluto verde
- Máximos de drawdown 1/2/5/10 dias (já vem da query `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION`)

### Passo 6 — Consolidação

Estrutura fixa:

```
# Monitor de Risco MM — [data-base]

## Resumo Executivo
- 3 fundos MM monitorados
- N exceções (P amarelos, Q vermelhos)
- Destaque: [1-2 linhas]

## Exceções (breach → vermelho → amarelo)
- [Fundo] | [métrica] | [valor] | [utilização %] | [causa provável]

## Detalhamento por Fundo

### Galapagos Macro FIM
- Fund VaR: X.XX %PL (Y% do soft) 🟢
- Stress: ...
- Exposição por book (%PL): RF-BZ X | FX-BRL Y | ...
- Top 3 books em VaR: ...
- Drawdowns históricos (1/2/5/10d): ...
- PnL Dia / MTD / YTD: ...

### Galapagos Quantitativo FIM
[mesma estrutura]

### Galapagos Evolution FIC FIM CP
- **Agregado:** Fund VaR / Stress / PnL
- **Por estratégia (book category):**
  - FRONTIER: VaR X, PnL dia Y
  - MACRO: VaR X, PnL dia Y
  - CREDITO: ...
  - CAIXA: ...

## Tendência (vs. D-1, D-5, D-21)
| Fundo | VaR D | Δ 1d | Δ 5d | Δ 21d |

## Avisos
- Mandatos pendentes de cadastro: [lista]
- Dados ausentes: [fundo → métrica]
- Datas defasadas: [se houver]
```

### Passo 7 — Snapshot (opcional)

Salvar o JSON estruturado em `snapshots/YYYY-MM-DD_mm.json` para:
- Histórico de tendência offline
- Alimentar `risk-morning-call`
- Auditoria

## Tratamento específico do EVOLUTION

EVOLUTION é diferente: é um **guarda-chuva** que aloca em 5 categorias via a função `classificar_book`. Para ele:

1. Calcular VaR e PnL agregados (como qualquer fundo).
2. Adicionalmente, **quebrar por book category** (FRONTIER, EVO_STRAT, MACRO, CREDITO, CAIXA).
3. O mandato do EVOLUTION deve ter limites agregados **e** por estratégia.
4. A função `classificar_book` do script `EVOLUTION_TABLES_GRAPHS.py` é a fonte de verdade da classificação — replicar no código da skill ou importá-la.

Ignorar `Galapagos Evo Strategy FIM CP` e book `FMN` mesmo que apareçam como BOOKs internos — escopo atual exclui esses.

## Arquivos de configuração

- **`fund-registry.json`** — lista de fundos MM ativos com metadata (TRADING_DESK, TREE, mapa de book-categories). Criar a primeira vez, atualizar quando um fundo entrar ou sair.
- **`mandato-<fundo>.json`** — seguir schema do `risk-manager`. Até ser cadastrado, monitor reporta valores absolutos sem semáforo.

Templates desses arquivos em `references/config-templates.md`.

## Regras de comportamento

- **Nunca inventar valor.** Query retornou vazio → reportar ausente.
- **Nunca aplicar limite default.** Sem mandato → reporta cru.
- **Sempre citar a data-base** no cabeçalho.
- **Exceções primeiro, detalhamento depois.**
- Em caso de erro na conexão ao banco → parar e reportar o erro; não tentar fallback para HTML (fonte inferior).

## Referências

- `references/queries-mm.md` — queries SQL completas extraídas dos scripts originais, com comentários sobre o que cada uma produz.
- `references/config-templates.md` — templates de `fund-registry.json` e mandato inicial para os 3 MM.
- `references/evolution-book-classification.md` — função `classificar_book` do EVOLUTION portada para uso direto.

## Extensão para outras famílias (futuro)

Quando forem adicionadas, cada família vai ganhar seu próprio arquivo em `references/queries-<familia>.md` e entries no `fund-registry.json`. Estrutura a adicionar:

- `queries-rf.md`: IDKA 3Y e 10Y (queries de `IDKA_TABLES_GRAPHS.py`) + Albatroz
- `queries-credito.md`: Sea Lion, Dragon, Nazca, Iguana (a mapear)
- `queries-rv.md`: Long Only (XLSX do `daily_report_generator_def.py`) + FMN

A lógica core da skill não muda — só entra novo material em referências e novo entry no registro.

## Skills relacionadas

- **`risk-manager`** (mãe) — taxonomia, semáforo, schema de mandato.
- **`glpg-data-fetch`** — padrão de conexão ao GLPG-DB01.
- **`risk-morning-call`** (a criar) — consome snapshot desta skill para gerar dashboard visual + resumo executivo.

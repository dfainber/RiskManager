# Infraestrutura GLPG — Referência Autoritativa

Fonte: `GLPG_System_Guide.docx` (Abril 2026). Este arquivo sintetiza a informação relevante para as skills de risco.

## Pipeline de dados paralelo existente

Já existe uma **infraestrutura de coleta rodando em produção**:

```
PostgreSQL → glpg_market_data.py → data_refresh.py → market_data.json → front-end
```

**Frequência:** `data_refresh.py` roda **a cada 1 minuto** (seg–sex 07:00–22:00) via Task Scheduler Windows.

**Output consolidado:** `\\fs02\FS_GALAPAGOS\Bloomberg\Quant\Claude_GLPG_Fetch\market_data.json`

**Adicional:** `book_pnl.json` é atualizado a cada 5 minutos com PnL por livro/fundo.

### Implicação para a `risk-data-collector`

Em vez de conectar direto no banco para verificar frescor, a coletor pode:

1. **Preferência:** ler o `market_data.json` atualizado (1 minuto de latência máxima)
2. **Fallback:** se JSON antigo ou ausente, conectar ao banco

Verificação de frescor do JSON:

```powershell
Get-Item "\\fs02\FS_GALAPAGOS\Bloomberg\Quant\Claude_GLPG_Fetch\market_data.json" | Select-Object LastWriteTime
```

Se `LastWriteTime` > 5 minutos atrás → sinalizar que o pipeline pode estar parado.

### Status do Task Scheduler

Tasks relevantes em `\GLPG\`:
- `\GLPG\DataRefresh` — roda `data_refresh.py` a cada 1 min
- `\GLPG\Watchdog` — mata processos Python travados a cada 10 min

Verificar status:
```powershell
Get-ScheduledTask -TaskPath "\GLPG\" | Select-Object TaskName, State, LastRunTime, LastTaskResult
```

Reiniciar manualmente se necessário:
```powershell
Start-ScheduledTask -TaskPath "\GLPG\" -TaskName "DataRefresh"
```

## FUND_KEY_MAP

Mapa definido em `glpg_market_data.py`:

```python
FUND_KEY_MAP = {
    "GALAPAGOS ALBATROZ FIRF LP": "GLPG_ALBA",
    "GALAPAGOS MACRO FIC FIM":    "GLPG_MACRO",
    "GALAPAGOS QUANT FIC FIM":    "GLPG_QUANT",
    "GALAPAGOS EVOLUTION FIC FIM": "GLPG_EVO",
    "GALAPAGOS BALTRA FIC FIM":   "GLPG_BALT",
    # ... pode ter outros
}
```

**⚠️ Variações de nome entre tabelas:** Os scripts analíticos (`MACRO_TABLES_GRAPHS.py`, etc.) usam nomes ligeiramente diferentes:

| Nome no guia (DB canônico) | Nome nos scripts |
|---|---|
| `GALAPAGOS MACRO FIC FIM` | `Galapagos Macro FIM` (sem FIC) |
| `GALAPAGOS QUANT FIC FIM` | `Galapagos Quantitativo FIM` |
| `GALAPAGOS EVOLUTION FIC FIM` | `Galapagos Evolution FIC FIM CP` (com CP) |

**Ação:** executar query de auditoria para entender qual nome está em cada tabela:

```sql
-- Em LOTE_TRADING_DESKS_NAV_SHARE (fonte de AUM)
SELECT DISTINCT "TRADING_DESK" FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE" ORDER BY 1;

-- Em LOTE_FUND_STRESS_RPM (fonte de VaR)
SELECT DISTINCT "TRADING_DESK" FROM "LOTE45"."LOTE_FUND_STRESS_RPM" ORDER BY 1;

-- Em REPORT_ALPHA_ATRIBUTION (fonte de PA)
SELECT DISTINCT "FUNDO" FROM q_models."REPORT_ALPHA_ATRIBUTION" ORDER BY 1;
```

Atualizar `fundos-canonicos.json` com os valores reais encontrados.

## EVOLUTION tem 1 dia de delay

**Citação direta do guia:** "GALAPAGOS EVOLUTION FIC FIM / Fundo Evolution (1 dia delay no DB)"

**Implicação:** skills que processam EVOLUTION devem tratar D-1 como "dia atual" do fundo. O `risk-data-collector` precisa refletir isso no manifesto — esperar EVOLUTION em D-1 é normal, não é atraso.

Atualizar `data-dependencies.json` para marcar este caso como comportamento esperado.

## Tabelas do banco (inventário atualizado)

### Schema `LOTE45` (fundos)

| Tabela | Colunas-chave | Uso nas skills |
|--------|---------------|----------------|
| `LOTE_TRADING_DESKS_NAV_SHARE` | TRADING_DESK, VAL_DATE, NAV, NAV_SHARE | AUM — todas as skills |
| `LOTE_FUND_STRESS_RPM` | VAL_DATE, TRADING_DESK, TREE, LEVEL, PARAMETRIC_VAR | VaR fundo |
| `LOTE_BOOK_STRESS_RPM` | VAL_DATE, TRADING_DESK, BOOK, LEVEL, PARAMETRIC_VAR | VaR book |
| `LOTE_BOOK_OVERVIEW` | VAL_DATE, TRADING_DESK, BOOK, PRODUCT, POSITION, DELTA, DIA, MES | Exposição + PnL |
| `LOTE_PARAMETRIC_VAR_TABLE` | VAL_DATE, TRADING_DESK, BOOKS, RELATIVE_VAR | BVaR (IDKAs) |
| `LOTE_PRODUCT_BOOK_POSITION_PL` | VAL_DATE, TRADING_DESK, BOOK, PRODUCT_CLASS, PRODUCT, AMOUNT, PRICE | Posições para DV01 |
| `LOTE_APORTES` | TRADING_DESK, LAST_UPDATE, AMOUNT | Aportes/resgates |
| `LOTE_DAILY_BENCHMARK` | TRADING_DESK, BOOK, TREE, BENCHMARK, PRODUCT, PRODUCT_CLASS | Benchmarks operacionais |

### Schema `q_models` (modelos)

| Tabela | Colunas-chave | Uso nas skills |
|--------|---------------|----------------|
| `REPORT_ALPHA_ATRIBUTION` | DATE, FUNDO, LIVRO, BOOK, CLASSE, DIA, MES | PA + stops PM |
| `RISK_DIRECTION_REPORT` | VAL_DATE, FUNDO, DELTA_SISTEMATICO, DELTA_DISCRICIONARIO | Alinhamento direcional |
| `PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` | PORTIFOLIO_DATE, PORTIFOLIO, DATE_SYNTHETIC_POSITION, W | Drawdowns histórica |

### Schema `public` (dados de mercado)

| Tabela | Colunas-chave | Uso |
|--------|---------------|-----|
| `SYNTHETIC_FUTURE` | TICKER, DATE, PX_LAST, FLAG | Futuros BR — **FLAG=1 (int) = front** |
| `DI1_JANUARY` | TICKER, DATE, PX_LAST, EXPIRY | Futuros DI1 |
| `NTN_BONDS` | TICKER, DATE, PX_MID, **EXPIRATION_DATE** | NTN-B — coluna é EXPIRATION_DATE, não MATURITY |
| `ECO_INDEX` | TICKER, DATE, VALUE, FIELD | CDI, SELIC, IPCA — **filtrar FIELD='YIELD' para CDI** |
| `BR_EQUITY` | TICKER, DATE, PX_LAST | Ações B3 |
| `GLOBAL_EQUITY` | TICKER, DATE, PX_LAST | Ações globais / ETFs |
| `FX_SPOT` | PAIR, DATE, PX_LAST | Câmbio |

## Armadilhas documentadas

Lista consolidada — reforçar em todas as skills que fizerem queries:

1. **ECO_INDEX sem FIELD filtro** → valores errados para CDI
2. **NTN_BONDS usa EXPIRATION_DATE** → não MATURITY_DATE
3. **SYNTHETIC_FUTURE.FLAG é integer 1** → não string 'L'
4. **TRADING_DESK em MAIÚSCULAS** → preferir `=` sobre `ILIKE`
5. **EVOLUTION tem 1 dia de delay** → D-1 é "hoje" para esse fundo
6. **Variações de nome entre tabelas** → auditar antes de assumir

## Diagnóstico rápido (quando algo dá errado)

### JSON não atualiza
```powershell
# 1. Verificar última escrita
Get-Item "\\fs02\FS_GALAPAGOS\Bloomberg\Quant\Claude_GLPG_Fetch\market_data.json" | Select-Object LastWriteTime

# 2. Verificar se o Task Scheduler está ativo
Get-ScheduledTask -TaskPath "\GLPG\" | Select-Object TaskName, State, LastRunTime, LastTaskResult

# 3. Reiniciar
Start-ScheduledTask -TaskPath "\GLPG\" -TaskName "DataRefresh"
```

### Fundo específico sem dado
```sql
SELECT MAX("VAL_DATE"), COUNT(*)
FROM "LOTE45"."LOTE_TRADING_DESKS_NAV_SHARE"
WHERE "TRADING_DESK" = 'GALAPAGOS ALBATROZ FIRF LP';
```

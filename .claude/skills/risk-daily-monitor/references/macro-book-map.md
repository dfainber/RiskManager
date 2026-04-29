# MACRO Book Map — LOTE_PRODUCT_EXPO

## Dedup rule
Always filter: `"TRADING_DESK_SHARE_SOURCE" = 'Galapagos Macro FIM'`
Three rows per position = own / SubA / Evolution feeder. Own = correct view.

## Book naming pattern
`{PM}_{RiskFactor}_{Type}`

### PMs
| Prefix | Name |
|--------|------|
| CI | Comitê (all managers) |
| LF | Luiz Felipe |
| JD | Joca Dib |
| RJ | Rodrigo Jafet |
| MD | Macro_MD (trader book, no limit) |

### Risk factors in BOOK name
| Code | Asset class | PRIMITIVE_CLASS examples |
|------|-------------|--------------------------|
| RF-BZ | BRL interest rates | BRL Rate Curve (DI1, LFT) |
| RF-DM | Developed market rates | — |
| RF-EM | EM rates | — |
| FX-BRL | BRL FX (USDBRL) | FX (USDBRLFuture) |
| FX-DM | DM FX | — |
| FX-EM | EM FX | — |
| RV-BZ | Brazilian equities | Equity |
| RV-DM | DM equities | Equity (S&P500 Futures - BMF) |
| RV-EM | EM equities | Equity |
| COMMODITIES | Commodities + FX overlay | FX (USDBRLFuture) |
| P-Metals | Precious metals | — |

### Book types
| Suffix | Meaning |
|--------|---------|
| Direcional | Directional position |
| Relativo | Relative value |
| Hedge | Hedge |
| SS | Short-selling |
| (none) | Structural / cash books |

## Structural books (non-PM)
| Book | Description |
|------|-------------|
| Caixa | Cash (LFT float) |
| CAIXA USD | USD cash hedge (USDBRLFuture) |
| Default | Residual / uncategorized |
| Giro_Master | Master fund liquidity |
| RF_LF | Fixed income long/float |
| CUSTOS E PROVISÕES | Costs and provisions |

## Active positions on 2026-04-16 (by DELTA magnitude)
| Book | Product | DELTA (BRL) |
|------|---------|-------------|
| Caixa | LFT | −371M |
| LF_RF-BZ_Direcional | DI1Future | −126M |
| RJ_COMMODITIES_Direcional | USDBRLFuture | −68M |
| CAIXA USD | USDBRLFuture | −45M |
| RJ_RV-DM_Direcional | S&P500 Futures | −17M |
| JD_RV-DM_Direcional | USDBRLFuture + S&P500 | −27M |
| JD_COMMODITIES_Direcional | USDBRLFuture | −12M |
| CI_COMMODITIES_Direcional | USDBRLFuture + Equity | −9M |

## Interest rate note
For rate exposure, use `MOD_DURATION` (already in LOTE_PRODUCT_EXPO).
`DELTA * MOD_DURATION / AUM * 100` = % NAV DV01-equivalent.
LFT duration ≈ 0 (floating). DI1 duration varies by contract (e.g. DI1F31 ≈ 4.1 years mod).

## SQL template
```sql
SELECT "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS",
       SUM("DELTA") AS total_delta,
       SUM("DELTA" * "MOD_DURATION") AS delta_duration
FROM "LOTE45"."LOTE_PRODUCT_EXPO"
WHERE "TRADING_DESK" = 'Galapagos Macro FIM'
  AND "TRADING_DESK_SHARE_SOURCE" = 'Galapagos Macro FIM'
  AND "VAL_DATE" = '{date}'
GROUP BY "BOOK", "PRODUCT_CLASS", "PRIMITIVE_CLASS"
```

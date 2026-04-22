# IDKA — Exploração de VaR e BVaR

**Data-base:** 2026-04-17
**Escopo:** auditoria do parametric da Lote + construção de HS do zero + comparação
**Status:** exploratório — não refletido no relatório ainda

---

## 1. Parametric da Lote (fonte atual)

### Estrutura
Fonte: `LOTE45.LOTE_PARAMETRIC_VAR_TABLE`, agregada por `SUM` sobre os fatores primitivos.

- 203 linhas/dia para IDKA 3Y, ~55 não-zero (os fatores que realmente contribuem)
- Cada linha = contribuição de 1 primitivo ao VaR total
- Cada primitivo aparece **3 vezes** por dia (3 snapshots — provavelmente 3 cortes, `SUM` é idempotente)
- Colunas-chave:
  - `ABSOLUTE_VAR_PCT`: contribuição ao VaR absoluto (negativo = perda) em fração decimal
  - `RELATIVE_VAR_PCT`: contribuição ao BVaR (vs benchmark)
  - `CLASS_VAR_PCT`: agregação no nível de classe
  - `RELATIVE_VOL_PCT`: contribuição à vol relativa
  - `DELTA`: BRL nominal que gerou a contribuição
- Engine rodada por `luca.esposito` (upstream, fora do escopo do kit)

### Top contribuidores (IDKA 3Y, 2026-04-17)
| PRIMITIVE | ABSOLUTE_VAR_PCT | RELATIVE_VAR_PCT |
|---|---:|---:|
| IPCA_COUPON_01080 (~3y NTN-B bucket) | -0.331% × 3 = -0.99% | -0.30% × 3 = -0.91% |
| SPREAD DAP NTN-B | -0.092% × 3 = -0.28% | -0.003% × 3 = -0.010% |
| IDKA IPCA 3Y (bench synthetic) | -0.29% × 3 = -0.86% | +0.26% × 3 = +0.78% |
| GALAPAGOS ALBATROZ FIRF LP | -0.088% × 3 = -0.26% | -0.075% × 3 = -0.22% |
| IPCA_COUPON_01440 (~4y) | -0.10% × 3 = -0.30% | -0.09% × 3 = -0.28% |
| IPCA PRT (IPCA index carry) | -0.076% × 3 = -0.23% | +0.006% × 3 = +0.019% |
| LFT Premium | -0.000007 × 3 | -0.000001 × 3 |

### Valores finais (2026-04-17)
- **IDKA 3Y**: BVaR **1.40%** · VaR absoluto **3.17%**
- **IDKA 10Y**: BVaR **2.92%** · VaR absoluto **6.33%**

### Série histórica (IDKA 3Y, ~114d desde 2025-10-29)
- BVaR: min 0.00 · mean 0.99 · max 2.18 · median 0.82
- VaR: min 0.00 · mean 2.27 · max 4.74 · median 1.94

> **Observação**: mean VaR 2.27% — coerente com **anualizado** (ver §3).

---

## 2. HS (Historical Simulation) construído do zero

### 2.1 HS empírico — realized NAV_share vs IDKA index

**Metodologia:**
- `fund_return[t] = SHARE[t] / SHARE[t-1] − 1` (de `LOTE_TRADING_DESKS_NAV_SHARE`)
- `bench_return[t] = IDKA_index[t] / IDKA_index[t-1] − 1` (de `ECO_INDEX`, instrumentos `IDKA_IPCA_3A`, `IDKA_IPCA_10A`)
- `active_return[t] = fund − bench`
- `BVaR_HS = −quantile(active, 0.05)` → magnitude

**Limitação de histórico**: IDKA 3Y só tem NAV-share desde 2025-08-20 (168d), IDKA 10Y desde 2025-08 (160d).
Janela 252d pega tudo disponível (que é menos que 252d).

### 2.2 HS posicional — posições atuais × histórico ANBIMA

**Metodologia:**
- Cada NTN-B da carteira atual → série de `UNIT_PRICE` em `PRICES_ANBIMA_BR_PUBLIC_BONDS` (mapeamento via `MAPS_ANBIMA_BR_PUBLIC_BONDS`, key diferente de `MAPS_BR_PUBLIC_BONDS`)
- `pnl_bond[t] = face_BRL × (P[t]/P[t-1] − 1)`
- `fund_pnl[t] = Σ pnl_bond[t] / NAV`

**Limitação**: implementação atual só cobre NTN-Bs diretos (2 posições no IDKA 3Y).
Não inclui DI1/DAP Futures, LFT, via_albatroz holdings. O VaR absoluto fica muito baixo (0.05%) porque NTN-B é só 24% do NAV — falta o resto do portfólio.

---

## 3. Comparativo

### IDKA 3Y (2026-04-17)

| Métrica | Lote parametric | HS empírico 252d (daily) | HS empírico 252d (anualizado) |
|---|---:|---:|---:|
| **BVaR** | 1.40% | 0.17% | **2.70%** |
| **VaR absoluto** | 3.17% | 0.35% | **5.48%** |

### IDKA 10Y (2026-04-17)

| Métrica | Lote parametric | HS empírico 252d (daily) | HS empírico 252d (anualizado) |
|---|---:|---:|---:|
| **BVaR** | 2.92% | 0.40% | **6.34%** |
| **VaR absoluto** | 6.33% | 0.88% | **13.89%** |

### Interpretação: Lote é anualizado
Comparando magnitudes:
- Lote VaR médio histórico IDKA 3Y = 2.27%. Consistente com **anualizado** (daily 0.20% × √252 = 3.17%).
- Limites de mandato (IDKA 3Y soft 1.75%, hard 2.50%) são **tracking error anualizado**, convenção padrão para fundos RF benchmarked. Reforça que Lote é anualizado.

### Gap estrutural: HS é ~2× Lote
Comparando anualizado vs anualizado:
- IDKA 3Y BVaR: Lote 1.40% vs HS 2.70% → **HS 1.9×**
- IDKA 3Y VaR: Lote 3.17% vs HS 5.48% → **HS 1.7×**
- IDKA 10Y BVaR: Lote 2.92% vs HS 6.34% → **HS 2.2×**
- IDKA 10Y VaR: Lote 6.33% vs HS 13.89% → **HS 2.2×**

**Gap consistente ~2x**. Hipóteses (não resolvido):

1. **EWMA vs equal-weight**: Lote provavelmente usa decaimento (λ≈0.94, effective window ~33d), favorecendo regime recente calmo. HS equal-weight 252d captura volatilidade histórica maior (inclui stress de 2025).
2. **Confidence level diferente**: se Lote usa 99% em vez de 95% seria HS < Lote, não o contrário. Descartado.
3. **Horizonte temporal**: anualização pode usar √252 vs √365 (factor 0.83). Descartado — gap 2x é maior.
4. **Covariância regularizada (shrinkage)**: Lote pode aplicar shrinkage para reduzir cross-factor correlation — reduz VaR agregado. Plausível.
5. **Janela curta do fund data**: IDKA 3Y só tem 168d de NAV-share (fund novo). HS de 252d pega tudo disponível — se o regime da amostra é mais volátil que o de 252d cheios, HS fica inflado.

---

## 4. Próximos passos (se quiser aprofundar)

### 4.1 Validar hipótese "Lote anualizado"
- Contatar upstream (`luca.esposito`) para confirmar horizonte e confiança
- Verificar se existe coluna/parametro que indica window

### 4.2 HS posicional completo (wire depois)
- Incluir todas as classes: NTN-B + DI1 Future + DAP Future + LFT + ALBATROZ look-through
- DI1: curva DI via `ECO_INDEX` FIELD=YIELD; convert to price move via duration
- LFT: CDI-linked, ≈ 0 duration
- ALBATROZ look-through: explodir posições do ALBATROZ × IDKA's share

Stub atual em `compute_idka_bvar_hs` — não wired.

### 4.3 Backtest de hit rate
- Para cada dia de histórico, compute Lote BVaR(t)
- Verificar quantas vezes |active_return(t+1)| > Lote_BVaR(t) / √252
- Esperado ~5% em 95% confidence. Se << 5%, Lote é conservador demais em excesso; se >> 5%, risco subestimado.

### 4.4 Decisão de metodologia
Escolher entre:
- **Opção A**: manter Lote no relatório (conservador, comparável aos outros fundos via engine comum), apenas validar via HS periodicamente
- **Opção B**: migrar pra HS empírico NAV-share (252d) — mais realista mas depende de histórico de fund (problema pra funds novos)
- **Opção C**: dual-display (Lote + HS side-by-side) para transparência

---

## 5. Changelog

| Data | Mudança |
|---|---|
| 2026-04-22 | Exploração inicial. Identificou gap 2x Lote vs HS empirical. Lote = anualizado. |

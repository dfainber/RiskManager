# IDKA — Exploração de VaR e BVaR

> **Status: RESOLVED** — investigação de 2026-04-17, bug estrutural corrigido em
> commit `e053a40`. Mantido aqui como histórico/referência do raciocínio. Não é
> mais um documento ativo.

**Data-base:** 2026-04-17
**Escopo:** auditoria do parametric da Lote + construção de HS do zero + comparação
**Resolução:** bug estrutural encontrado e corrigido (fix em commit `e053a40`). Gap residual ~1.5-3x é esperado (parametric vs HS).

---

## 0. Resumo executivo

- **Bug crítico**: `fetch_risk_history_idka` somava todas as views de `LOTE_PARAMETRIC_VAR_TABLE` sem filtrar `BOOKS`, **triplicando** o VaR/BVaR. Valores reportados nos IDKAs estavam ~3× inflados.
- **Fix**: filtrar `BOOKS::text = '{*}'` (view wildcard = agregado do fundo). Commit `e053a40`.
- **Impacto 2026-04-17**:
  - IDKA 3Y: BVaR 1.40% → **0.28%** · VaR 3.17% → **1.05%**
  - IDKA 10Y: BVaR 2.92% → **0.66%** · VaR 6.33% → **2.10%**
- **Horizonte do Lote**: **DAILY** (não anualizado). Valores diretamente comparáveis com HS diário.
- **Gap Lote/HS (daily)**: 1.64–3.05x — Lote parametric é consistentemente mais conservador que HS realizado, plausível por EWMA/factor model vs equal-weight.

---

## 1. Parametric da Lote (fonte atual)

### 1.1 Estrutura da tabela
Fonte: `LOTE45.LOTE_PARAMETRIC_VAR_TABLE`.

- ~203 linhas/dia para IDKA 3Y
- Cada linha = contribuição de 1 PRIMITIVO × BOOK filter ao VaR total
- Colunas-chave:
  - `ABSOLUTE_VAR_PCT`: contribuição ao VaR absoluto (negativo = perda) em fração decimal
  - `RELATIVE_VAR_PCT`: contribuição ao BVaR (vs benchmark)
  - `CLASS_VAR_PCT`: agregação no nível de classe
  - `BOOKS`: **ARRAY** com a lista de books considerados nessa view (crítico!)
  - `DELTA`: BRL nominal da posição
- Engine rodada por `luca.esposito` (upstream, fora do escopo do kit)

### 1.2 Views redundantes por BOOKS (bug identificado)

Cada primitivo aparece **múltiplas vezes** por dia, cada linha refletindo um filtro diferente de `BOOKS`:

IDKA 3Y em 2026-04-17, primitivo `IPCA_COUPON_01080`:

| BOOKS filter | ABSOLUTE_VAR_PCT | RELATIVE_VAR_PCT |
|---|---:|---:|
| `{*}` (wildcard — agregado do fundo) | -0.003306 | -0.003009 |
| `{Default,Caixa,CUSTOS,RF_LF,Benchmark_IDKA}` (explicit all) | -0.003306 | -0.003009 |
| `{Benchmark_IDKA}` (só esse book) | -0.003307 | -0.003211 |

As três views representam **o mesmo risco sob diferentes agregações**, não fragmentos aditivos. Somando-as triplicava o valor.

Por fundo inteiro (IDKA 3Y 2026-04-17):

| BOOKS filter | VaR | BVaR |
|---|---:|---:|
| `{*}` (fund total) | **1.054%** | **0.282%** |
| Explicit all-books | 1.054% | 0.282% |
| `{Benchmark_IDKA}` only | 0.688% | 0.476% |
| `{Caixa}` only | 0.286% | 0.274% |
| `{RF_LF}` only | 0.083% | 0.083% |
| Outros | ~0 | ~0 |
| **SUM de TUDO (bug)** | **3.165%** 🔴 | **1.398%** 🔴 |

### 1.3 Valores corretos (após fix)
- **IDKA 3Y (2026-04-17)**: BVaR **0.282%** · VaR **1.054%**
- **IDKA 10Y (2026-04-17)**: BVaR **0.656%** · VaR **2.104%**

---

## 2. HS (Historical Simulation) do zero

### 2.1 HS empírico — realized NAV_share vs IDKA index

**Metodologia:**
- `fund_return[t] = SHARE[t] / SHARE[t-1] − 1` (de `LOTE_TRADING_DESKS_NAV_SHARE`)
- `bench_return[t] = IDKA_index[t] / IDKA_index[t-1] − 1` (de `public.ECO_INDEX`, instrumentos `IDKA_IPCA_3A`, `IDKA_IPCA_10A`)
- `active_return[t] = fund − bench`
- `BVaR_HS = −quantile(active, 0.05)` · `VaR_HS = −quantile(fund, 0.05)` → magnitude

**Limitação**: IDKA 3Y só tem NAV-share desde 2025-08-20 (168d); IDKA 10Y 160d. Janela "252d" usa tudo disponível.

### 2.2 HS posicional — posições atuais × histórico ANBIMA

**Metodologia:**
- Cada NTN-B → série de `UNIT_PRICE` em `public.PRICES_ANBIMA_BR_PUBLIC_BONDS` (mapping via `MAPS_ANBIMA_BR_PUBLIC_BONDS`, key diferente de `MAPS_BR_PUBLIC_BONDS`)
- `pnl_bond[t] = face_BRL × (P[t]/P[t-1] − 1)`
- `fund_pnl[t] = Σ pnl_bond[t] / NAV`

**Limitação**: só cobre NTN-Bs diretos (2 posições no IDKA 3Y, 24% do NAV). Não inclui DI1/DAP Futures, LFT, via_albatroz. VaR absoluto fica muito baixo por falta de cobertura. Não é conclusivo.

---

## 3. Comparativo (daily)

### 3.1 IDKA 3Y

| Métrica | Lote (daily) | HS 252d (daily) | Ratio Lote/HS |
|---|---:|---:|---:|
| BVaR | **0.282%** | 0.170% | **1.66x** |
| VaR | **1.054%** | 0.345% | **3.05x** |

### 3.2 IDKA 10Y

| Métrica | Lote (daily) | HS 252d (daily) | Ratio Lote/HS |
|---|---:|---:|---:|
| BVaR | **0.656%** | 0.399% | **1.64x** |
| VaR | **2.104%** | 0.875% | **2.41x** |

### 3.3 Interpretação: horizonte do Lote é DAILY

Testei hipóteses:
- **Daily**: ratios Lote/HS 1.6-3x — razoável (parametric conservador vs HS realizado)
- **Anualizado**: ratios Lote/HS 0.10-0.19x (Lote seria 5-10x menor que HS) — absurdo, descartado

Lote é **VaR 95% 1-day**, consistente com convenção de risco de mercado (Basel).

Limites do mandato (IDKA 3Y soft 1.75% / hard 2.50%) em **% daily** implicam tolerar tracking error diário de ~1.75% — o que equivale a anualizado ~27.8% (× √252). Alto mas possível se for o "hard" da auditoria.

Alternativa: limites são em anualizado e a comparação com Lote-daily precisa × √252. Nesse caso:
- IDKA 3Y BVaR annualized = 0.282% × √252 = **4.48%** vs soft 1.75% = **256% utilização** 🔴 (fora do limite)
- IDKA 10Y BVaR annualized = 0.656% × √252 = **10.42%** vs soft 3.50% = **298% utilização** 🔴

**Resolvido 2026-04-22**: limites confirmados como **daily** — 3Y soft 0.40% / hard 0.60%, 10Y soft 1.00% / hard 1.50%. Os limites anteriores (1.75/2.50) eram provisórios incorretos. Com os novos limites: utilização ~70-80%, dentro do soft. Ver CLAUDE.md Fase 4 sessão 2026-04-22.

### 3.4 Gap 1.6-3x Lote vs HS — causas prováveis

Consistente em 3Y e 10Y, BVaR e VaR:
1. **EWMA vs equal-weight**: Lote provavelmente usa decaimento (λ≈0.94, effective ~33d), favorecendo regime calmo recente. HS equal-weight 252d captura mais vol histórica (regime de stress de 2025).
2. **Covariância regularizada (shrinkage)**: Lote pode aplicar shrinkage para reduzir cross-factor correlation.
3. **Short history do fund**: IDKA 3Y só 168d de NAV-share — HS fica puxado por poucos dias.

---

## 4. Próximos passos (parkeado)

### 4.1 ~~Validar convenção dos limites~~ — **fechado 2026-04-22**
Limites confirmados como **daily**: 3Y soft 0.40% / hard 0.60% · 10Y soft 1.00% / hard 1.50%.
Utilização atual ~70-80% — dentro do soft. Limites provisórios anteriores (1.75/2.50) eram incorretos.

### 4.2 HS posicional completo
- Incluir DI1 Future + DAP Future + LFT + ALBATROZ look-through
- Stub existe em `compute_idka_bvar_hs`, não wired

### 4.3 Backtest de hit rate do Lote
- Para cada dia de histórico, Lote BVaR(t) vs |active_return(t+1)|
- Esperado ~5% violations em 95% confidence
- Confirma/refuta a calibração

### 4.4 Contatar upstream (luca.esposito)
- Confirmar horizonte (presumivelmente 1-day)
- Confirmar confidence level (95%)
- Confirmar método (parametric factor model + EWMA?)

---

## 5. Changelog

| Data | Mudança |
|---|---|
| 2026-04-22 | Exploração inicial. Identificou bug de triplicação no SUM. |
| 2026-04-22 | Fix aplicado: filtro `BOOKS='{*}'`. Valores IDKA 3Y/10Y caem ~3x. |
| 2026-04-22 | Análise corrigida: Lote é DAILY (não annualized). Gap 1.6-3x vs HS é razoável. Dúvida aberta: convenção dos limites (daily vs annualized). |
| 2026-04-22 | Limites confirmados como daily: 3Y 0.40/0.60%, 10Y 1.00/1.50%. Dúvida fechada. IDKAs migrados para HS engine em Distribuição 252d com toggle Benchmark/Replication/Comparação. |

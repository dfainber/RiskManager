# Tratamento do CREDITO no card Evolution — Diversification Benefit

**Data:** 2026-04-20
**Código:** [`evolution_diversification_card.py`](../evolution_diversification_card.py)
**Relacionado:** skill [`evolution-risk-concentration`](../.claude/skills/evolution-risk-concentration/SKILL.md)

---

## 1. Problema

A estratégia **CREDITO** do Galapagos Evolution FIC FIM CP contém
posições em **cotas júnior de FIDCs / tranches subordinadas de
securitizações**. A forma como o Lote45 captura o VaR desses
instrumentos periodicamente distorce o número — tipicamente por
**limitação de marcação a mercado** ou de **modelagem de spread**.

**Incidente concreto — dezembro/2025:** o VaR do CREDITO apresentou um
spike pontual que dominou o `Σ VaR_estratégias` por várias semanas,
inflando artificialmente o denominador do diversification ratio e
mascarando o sinal real de alinhamento direcional entre MACRO, SIST e
FRONTIER.

---

## 2. O que **não** funciona

### Opção A — Subtrair `VaR_CREDITO` linearmente do `VaR_real`
```
VaR_real_ex ≈ VaR_real − VaR_CREDITO     ❌
```
VaR **não é aditivo** (`VaR(A+B) ≠ VaR(A) + VaR(B)`). Essa subtração
descarta o benefício de correlação entre CREDITO e as demais
estratégias — geralmente negativo, às vezes significativo. Rejeitado.

### Opção B — Recomputar o VaR do fundo excluindo posições de CREDITO
Precisaria repricificação via engine completa do Lote45. Inviável em
runtime do kit; dependência pesada. Parkado.

---

## 3. Tratamento adotado (primeiro modelo — 2026-04-20)

Duas camadas de mitigação, **cumulativas e auditáveis**:

### 3.1 — Winsorização causal do `VaR_CREDITO` histórico

Para cada data `t` da série de `VaR_CREDITO` (em bps):

```
janela       = series[t-63 : t-1]          # 63 dias úteis, causal (sem ver t)
med          = median(janela)
mad          = median(|janela − med|)
scale_robust = 1.4826 × mad                 # MAD → σ-equivalente sob normal
upper_cap    = med + 3 × scale_robust       # 3σ robusto, tail superior

VaR_CREDITO_clipped[t] = min(VaR_CREDITO[t], upper_cap)
```

**Decisões de desenho:**

| Escolha | Motivo |
|---------|--------|
| **MAD** e não std | std é poluída pelo próprio spike por semanas após o evento. MAD é robusto — o spike não desloca a escala do estimador |
| **Janela 63d** | Balanceia estabilidade (estimador confiável) e adaptabilidade (segue mudança de regime lenta) |
| **Causal (`shift(1)`)** | A mediana rolling de tamanho 63 que *inclui* `t` tende a cobrir o próprio ponto, reduzindo força da clipagem. Usa-se só passado estrito |
| **Tail superior apenas** | Dias de VaR baixo **não são o problema**. Clipagem bilateral descartaria janelas calmas legítimas |
| **3σ robusto** | No spike de dez/2025, 3σ MAD clipa; dias voláteis porém normais ficam |

Implementação: `_winsorize_causal()` em `evolution_diversification_card.py`.

### 3.2 — Share do CREDITO no Σ como métrica visível

Em paralelo ao ratio, o card exibe:

```
Share CREDITO_raw     = VaR_CREDITO_raw     / Σ VaR_raw
Share CREDITO_wins    = VaR_CREDITO_clipped / Σ VaR_wins
```

**Uso:** semáforo visual.
- Share > 40% → vermelho (dependência extrema, ratio é dominado por CREDITO)
- Share 25–40% → amarelo (CREDITO já pesa no Σ; ratio deve ser lido com cautela)
- Share < 25% → cinza (CREDITO é um componente normal, ratio é defensável)

Este número **sempre** aparece, mesmo quando a winsorização não ativou
em nenhuma data da janela. É o caveat estrutural.

### 3.3 — Transparência: lista de datas winsorizadas

O card mostra:
- Total de dias clipados na janela 252d
- Os 3 mais recentes, por data

Permite reconciliação manual: "o dia X foi clipado?". Auditoria tem
que ser trivial, senão o tratamento é uma caixa-preta.

---

## 4. O que **não** é alterado

- **`VaR_real` do fundo** (LEVEL=10 de `LOTE_FUND_STRESS_RPM`) — sempre
  o número cheio, inclui CREDITO. Não é cosmético; é o oficial
- **`VaR_CREDITO` mostrado na tabela por estratégia** — raw; quando
  difere da versão winsorizada, uma nota ao lado indica o clip
- **Camada 1 percentil do CREDITO** — computado na série raw; o sinal
  "quantas estratégias ≥ P70 simultaneamente" pode incluir CREDITO, mas
  a interpretação final fica para o leitor (com o caveat visível)
- **Camada 3 correlações de PnL** — fonte diferente
  (`REPORT_ALPHA_ATRIBUTION`), não sofre o mesmo bug de marcação de VaR

---

## 5. O que esperamos ver

Se o tratamento está funcionando:

1. Em dias **normais**: `ratio_wins ≈ ratio_raw` (winsorização inerte)
2. Durante/depois de um spike de CREDITO:
   `ratio_wins > ratio_raw` (denominador menor → ratio mais honesto)
3. O percentil de `ratio_wins` é mais **estável** dia-a-dia; `ratio_raw`
   pode mergulhar artificialmente quando CREDITO explode

---

## 6. Pendências / extensões

1. **Overlay manual de datas** — adicionar config `CREDITO_EXCLUDE_DATES`
   para incidentes específicos conhecidos não capturados pela winsorização
   (ex.: spike lento ao longo de 2 semanas que desloca a mediana junto)
2. **Segregação de cotas júnior** — quando `LOTE_BOOK_OVERVIEW` permitir
   identificar cotas júnior por `PRODUCT_CLASS`, aplicar winsorização só
   à fatia júnior; senior credit fica intocado
3. **Calibração dos thresholds** — `n_sigma=3` é defensivo; pode ser
   afrouxado para `2.5` após 6 meses de uso se falsos positivos forem raros
4. **Propagação às Camadas 1/3** — Camada 1 usa série raw hoje; revisar
   se CREDITO deve ter percentil calculado na série clipada também

---

## 7. Ligação com a skill

Ver seção **"⚠️ Ressalva crítica — VaR de cotas júnior de crédito"** no
[SKILL.md](../.claude/skills/evolution-risk-concentration/SKILL.md). A
skill prevê o problema e pede `Ratio_ex_credito` como métrica paralela;
por inviabilidade do ex-real (VaR não-aditivo), substituído pela
winsorização + share visível. A intenção — mostrar quando o CREDITO
está distorcendo o ratio — é a mesma.

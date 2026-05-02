---
name: macro-risk-breakdown
description: Relatório analítico de risco do Galapagos Macro FIM. Decompõe VaR/Stress por classe de ativo e por PM (CI, LF, JD, RJ), mudanças D-vs-D-1, traça cada PM contra orçamento, calcula "shock absorption" (quanto top-10 ativos precisam mexer pra consumir frações do orçamento). Use para pedidos sobre breakdown do MACRO, decomposição de risco, fontes de risco, shock absorption, ou análise analítica detalhada do MACRO além do semáforo da risk-daily-monitor.
---

# Macro Risk Breakdown

Relatório analítico do `Galapagos Macro FIM` para o Morning Call. Quatro camadas:

1. **Onde está o risco hoje?** — decomposição de VaR e Stress por classe e por PM
2. **O que mudou?** — variação D vs. D-1, com filtro de materialidade
3. **Como está cada PM vs. seu orçamento?** — tracking individual
4. **Quanto de choque o fundo aguenta?** — shock absorption nos top 10 ativos

**Dependências:**
- `risk-manager` — taxonomia, convenções
- `macro-stop-monitor` — orçamento por PM
- `glpg_fetch.py` — padrão de conexão GLPG-DB01
- Queries canônicas em `risk-daily-monitor/references/queries-mm.md`

**Não é monitor.** Esta skill não emite breach nem aciona semáforo para gestão. É leitura analítica. O semáforo vive na `risk-daily-monitor`.

## Camada 1 — Decomposição do risco (ex-ante)

### Duas visões complementares

Mesma carteira, agregada de duas formas:

**Por classe de ativo** (books-ativos):
`RF-BZ, RF-EM, RF-DM, FX-BRL, FX-EM, FX-DM, RV-BZ, RV-EM, RV-DM, COMMODITIES, P-Metals`

**Por gestor** (books-gestores):
`CI, LF, JD, RJ`

Para cada visão, calcular por book:
- **VaR contribuído (bps)** — de `LOTE_BOOK_STRESS_RPM` com `LEVEL=3`
- **VaR % do total** — `(book_var / fund_var) × 100`
- **Stress contribuído (bps)** — mesmo princípio, cenário principal do mandato

Query em `references/queries-breakdown.md` (item 1).

**Consistência de agregação:** as duas visões devem somar ao mesmo total (VaR fundo). Se divergirem, flag "discrepância" e investigar `LEVEL`/`TREE`.

### Apresentação

Para cada visão, formato fixo:

```
**Decomposição por classe de ativo (VaR)**

Top 5:
| Classe      | VaR (bps) | % total | Stress (bps) |
|-------------|-----------|---------|--------------|
| RF-BZ       |   28.4    |   32%   |    −45.0     |
| FX-BRL      |   18.2    |   21%   |    −28.1     |
| RV-BZ       |   14.0    |   16%   |    −52.3     |
| COMMODITIES |    9.1    |   10%   |    −18.5     |
| FX-EM       |    6.5    |    7%   |    −10.2     |

Adicionais materiais (≥ 2 bps e ≥ 5% do total):
| Classe      | VaR (bps) | % total |
|-------------|-----------|---------|
| P-Metals    |    3.5    |    4%   |

Outros (soma agregada): 8.3 bps (10%)
Total fundo: 88.0 bps
```

**Regras:**
- Top 5 sempre aparece, mesmo que alguns sejam < 2 bps — dá comparabilidade entre dias
- "Adicionais materiais" só aparece se houver item abaixo do top 5 que seja material
- Linha "outros" fecha a conta para 100%
- VaR e Stress lado a lado — não em tabelas separadas

### Detalhamento de COMMODITIES e P-Metals

**Commodities não é bloco uniforme.** Se a classe aparece no top 5 (ou é material), **sempre abrir por PRODUCT**:

```
COMMODITIES (9.1 bps total)
  ├─ Petróleo   : 6.8 bps (75%)
  ├─ Milho      : 1.5 bps (16%)
  └─ Soja       : 0.8 bps ( 9%)
```

Mesma abertura para P-Metals. Outras classes não precisam da abertura aqui — o detalhamento por ativo vive na Camada 4.

## Camada 2 — Mudança marginal D vs. D-1

Para cada classe e para cada PM, comparar com D-1:
- ΔVaR (bps)
- ΔStress (bps)
- Δ% do total

### Definição de "material"

**Passa nos DOIS critérios (AND):**

1. **Absoluto:** |Δ| ≥ 2 bps
2. **Relativo:** |Δ| ≥ 10% do VaR total do fundo **OU** |Δ| ≥ 5% do stop mensal (≈ 3 bps)

Interseção evita ruído:
- Δ de 30% num item minúsculo (0.5 bps) → falha no absoluto → ignorar
- Δ de 5 bps num item dominante (50% do VaR) → passa nos dois → reportar

Thresholds configuráveis em `assets/thresholds.json`.

### Atribuição da variação

Quando possível, classificar em:
- **Posição nova** — ΔDelta significativo (`|ΔDELTA / DELTA_ontem| > 10%`)
- **Mercado** — Delta estável, VaR/Stress variou. Vol mudou
- **Rolagem de janela** — ΔVaR grande sem ΔDelta grande e sem movimento claro do ativo. Marcar como "possível"

Apresentação (só materiais):

```
**Mudanças materiais D vs. D-1**

| Item         | ΔVaR (bps) | Δ%   | Causa provável            |
|--------------|------------|------|---------------------------|
| RF-BZ (CI)   |   +4.2     | +18% | Posição nova (ΔDelta +25%)|
| FX-BRL (LF)  |   −3.1     | −14% | Mercado (vol caiu)        |
| Petróleo (JD)|   +2.5     | +37% | Posição nova              |

Nada material em: RF-EM, FX-EM, FX-DM, RV-EM, RV-DM, P-Metals, PM RJ
```

A linha final ajuda o leitor a confirmar que o silêncio é informativo, não omissão.

## Camada 3 — PM vs. orçamento individual

Tracking interno, não gestão ativa. Formato enxuto:

| PM | VaR (bps) | Stress (bps) | Stop restante (bps) | VaR / Stop |
|----|-----------|--------------|---------------------|------------|
| CI | 18.4      | −35.0        | 51 (de 63)          | 36%        |
| LF | 12.2      | −22.1        | 48                  | 25%        |
| JD |  8.5      | −14.3        | 60                  | 14%        |
| RJ |  5.1      | −10.8        | 63                  |  8%        |

Stop restante = 63 − |PnL_mês_corrente| (se PnL negativo; se positivo, volta a 63). PnL_mês vem de `q_models.REPORT_ALPHA_ATRIBUTION`, filtrando `LIVRO contains <inicial>`, como na `macro-stop-monitor`.

**VaR / Stop restante** é o KPI: razão entre risco 1d e o que ainda pode ser perdido no mês. Alto = PM carregando muito risco vs. margem disponível.

## Camada 4 — Shock absorption (regra de 3)

### Abordagem

Para cada ativo, o VaR já é uma medida de "quanto se perde com movimento típico adverso". A sensibilidade pode ser **inferida do próprio VaR** sem calibração manual:

```
Sensibilidade = VaR_atual_bps / Movimento_implícito_no_VaR
```

E então:

```
Movimento_para_consumir_Z_bps = Z × (Movimento_implícito / VaR_atual_bps)
```

O "movimento implícito" é extraído do próprio motor de VaR histórico (99%, 1d), que tem um cenário pior do ativo em 252 dias de janela. Esse cenário pior é, efetivamente, o choque que gera o VaR.

### Operacionalização

1. Identificar os **top 10 PRODUCTs** por VaR contribuído (query item 4 em `references/queries-breakdown.md`)
2. Aplicar `produto_ajustado` para ter nome curto do ativo
3. Para cada um, buscar na tabela de simulação histórica (`PORTIFOLIO_DAILY_HISTORICAL_SIMULATION` ou equivalente em nível PRODUCT) o **percentil 1 histórico do retorno** do ativo. Esse é o "movimento implícito no VaR".
4. Regra de 3 para calcular os choques que consomem 25%, 50%, 100% do stop mensal restante

### Apresentação

```
**Shock absorption — Top 10 ativos**

Stop mensal restante do fundo: 51 bps

| # | Ativo       | Classe | VaR hoje | Δp1 histórico | →25% stop | →50% stop | →100% stop |
|---|-------------|--------|----------|---------------|-----------|-----------|------------|
| 1 | DI1F30      | RF-BZ  | 12.3 bps |   +65 bp      |   +67 bp  |  +134 bp  |  +268 bp   |
| 2 | BRLUSD      | FX-BRL |  8.1 bps |   +2.1%       |   +3.3%   |   +6.6%   |  +13.2%    |
| 3 | WIN (Ibov)  | RV-BZ  |  6.5 bps |   −3.8%       |   −7.5%   |  −14.9%   |  −29.9%    |
| 4 | Petróleo    | COMM   |  5.8 bps |   −5.4%       |  −11.9%   |  −23.8%   |  −47.6%    |
| 5 | T10         | RF-DM  |  4.2 bps |   +18 bp      |   +55 bp  |  +109 bp  |  +219 bp   |
| 6 | EUP         | FX-DM  |  3.0 bps |   +2.6%       |   +11.0%  |   +22.1%  |  +44.2%    |
| 7 | Milho       | COMM   |  2.5 bps |   −8.1%       |   −41.3%  |   −82.7%  |  (>100%)   |
|...|

Leitura: "DI1F30 precisa abrir +134 bp para consumir 50% do stop mensal restante.
         Movimento histórico pior em 1 ano foi +65 bp — precisaria de 2x esse evento."
```

Notas de apresentação:
- **Δp1 histórico** = pior retorno em 252d úteis (proxy do cenário que gera o VaR 99%)
- **Valores >100%** (ativo teria que mexer além de qualquer choque histórico): mostrar como "(>100%)" para sinalizar
- **Direção do sinal**: positiva para ativos onde a perda vem de movimento para cima (short ou inverse), negativa onde vem do movimento para baixo (long). Extrair do sinal do DELTA

### Calibração progressiva

A regra de 3 assume linearidade da sensibilidade. Para a maioria dos ativos isso é OK, mas para **opções e curvas com convexidade** pode subestimar. Começamos com a versão simples; se algum ativo mostrar inconsistência clara (ex.: cenário 100% do stop resulta em choque menor que Δp1 histórico), flag-ar e revisar caso a caso.

**Você (usuário) vai ajudar a calibrar** — ao longo do uso, se alguma regra de 3 parecer errada (ex.: "DI longo aguentaria 270 bp? Isso não faz sentido"), a gente ajusta. Por ora, regra de 3 simples, transparente, sem fator mágico.

## Estrutura do relatório final

Ordem e seções fixas. Apresentar no Morning Call nesta sequência:

```
# Macro Risk Breakdown — [data-base]

## 1. Onde está o risco (ex-ante)
[Camada 1 — duas tabelas: por classe e por PM, com top 5 + materiais + outros]
[Abertura de COMMODITIES/P-Metals se entraram]

## 2. O que mudou (D vs. D-1)
[Camada 2 — só movimentos materiais, com causa provável]

## 3. PM vs. orçamento
[Camada 3 — tabela enxuta dos 5 PMs]

## 4. Shock absorption
[Camada 4 — top 10 ativos com choques para 25/50/100% do stop restante]

## Notas
- Data-base dos dados: [DATA]
- Stop mensal restante (fundo): [X] bps
- VaR total ex-ante: [Y] bps
- Cenário principal de stress: [nome do cenário]
```

## Regras de comportamento

- **Sempre lado a lado VaR e Stress** — são lentes complementares, nunca mostrar só uma
- **Materialidade é filtro, não censura** — linha "nada material em: X, Y, Z" é parte do relatório
- **Regra de 3 é default, não dogma** — se a sensibilidade parecer errada, perguntar ao usuário antes de reportar
- **Top N sempre fixo** — top 5 nas decomposições, top 10 em shock absorption — para manter comparabilidade entre dias
- **Stop restante vem de `macro-stop-monitor`** — ler snapshot mensal mais recente + PnL do mês corrente

## Referências

- `references/queries-breakdown.md` — SQL para decomposição por book, por PRODUCT, e série histórica de cada PRODUCT
- `references/materialidade.md` — threshold atuais e histórico de ajustes
- `assets/thresholds.json` — constantes de materialidade (editáveis sem mudar código)

## Skills relacionadas

- **`risk-manager`** — taxonomia
- **`risk-daily-monitor`** — semáforo de limites agregado
- **`macro-stop-monitor`** — orçamento por PM (consumido aqui para calcular "stop restante")
- **`risk-morning-call`** (a criar) — agrega outputs de todas as skills num briefing final

## Roadmap

1. **Agora:** quatro camadas operacionais com regra de 3 simples na Camada 4
2. **Após primeiras execuções:** calibrar thresholds de materialidade e revisar se regra de 3 precisa de fatores de convexidade em algum ativo específico
3. **Depois:** incorporar análise de correlação (quando uma classe reduz, outra aumenta por quê?)

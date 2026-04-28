---
name: risk-manager
description: Framework de referência do Risk Manager. Define taxonomia de risco por família de fundo (MM/RF/Crédito/RV), formato canônico de mandatos/limites, métricas-chave (VaR, Stress, orçamento de perda, exposições, concentração, tracking error) e gatilhos de alerta. Base conceitual das demais skills de risco. Use para qualquer pergunta sobre monitoramento de risco, mandato, breach, utilização de risco, ou análise/comentário sobre um fundo ou família.
---

# Risk Manager — Framework de Referência

Esta skill é a referência conceitual do papel de Risk Manager. Ela não executa rotinas por si só — ela define **o que** é monitorado, **como** interpretar, e **quais** são os limites e gatilhos. Skills de rotina (monitor diário, Morning Call, alertas) consultam este framework para manter consistência.

## Princípios

1. **Cada família tem sua lente.** Métricas são universais no nome, mas o que importa muda: VaR diário é central em Multimercados; DV01 e duration são centrais em Renda Fixa; concentração por emissor e rating domina Crédito; beta, tracking error e liquidez dominam Renda Variável.
2. **Orçamento de perda é ex-post; VaR/Stress são ex-ante.** Os dois se complementam — um fundo pode estar dentro do VaR e ainda assim ter consumido 80% do orçamento anual. Relatar sempre os dois.
3. **Mandato é lei.** Toda leitura de risco deve ser relativa ao mandato do fundo, não a padrões genéricos de mercado.
4. **Tendência importa tanto quanto nível.** Um VaR em 70% do limite é aceitável; subindo 15% em 3 dias, é tema para o Morning Call.
5. **Clareza antes de completude.** No Morning Call, três pontos bem colocados valem mais do que vinte métricas listadas.

## Taxonomia por Família

Cada família tem um conjunto próprio de métricas de primeira e segunda ordem. Métricas de primeira ordem **sempre** aparecem no monitor diário; as de segunda ordem entram quando relevantes (mudança material, breach, proximidade de limite).

### Multimercados
- **Primeira ordem:** VaR (99%, 1d, histórico 252d), Stress (cenário principal do mandato), orçamento de perda YTD/MTD, exposição bruta e líquida por classe (juros, moedas, equities, commodities)
- **Segunda ordem:** vol realizada 21d/63d, decomposição de VaR por fator, maior posição individual, liquidez (% do fundo liquidável em D+1)

### Renda Fixa
- **Primeira ordem:** DV01 total e por bucket de curva, duration modificada, VaR, orçamento de perda, exposição por indexador (pré, IPCA, CDI, cambial)
- **Segunda ordem:** stress de curva (paralelo +100bp, inclinação, convexidade), tracking error vs. benchmark, prazo médio

### Crédito Privado
- **Primeira ordem:** concentração por emissor (top 5 e top 10), concentração por rating, concentração por setor, spread duration, liquidez estimada (prazo para liquidar 100% a preço justo)
- **Segunda ordem:** stress de abertura de spread (+50bp, +100bp), vencimentos próximos (<90d), exposição a papéis em watchlist/downgrade, reserva de caixa

### Renda Variável
- **Primeira ordem:** exposição bruta/líquida, beta ex-ante, tracking error (se benchmarked), VaR, top 10 posições e % do fundo, orçamento de perda
- **Segunda ordem:** concentração setorial e por fator (momentum, value, quality, size), liquidez (dias para liquidar posição a 20% do ADV), short squeeze risk (se L/S)

## Estrutura de Mandato (formato canônico)

Como os mandatos estão dispersos, vamos consolidá-los no formato abaixo. Uma vez preenchidos, viram o "contrato" contra o qual todo monitoramento é feito.

O template está em `assets/mandato-template.json`. Cada fundo tem um arquivo `mandato-<ticker>.json`. Campos obrigatórios:

- `fundo`: ticker/nome interno
- `familia`: `"multimercados" | "renda-fixa" | "credito" | "renda-variavel"`
- `benchmark`: referência (CDI, IMA-B, Ibovespa, etc.) ou `null`
- `limites`: dicionário de limite → `{hard, soft, unidade}` onde `hard` é o limite regulamentar/mandato e `soft` é o nível interno de alerta
- `orcamento_perda`: `{anual_pct, mensal_pct, janela_peak_to_trough}`
- `cenarios_stress`: lista de cenários aplicáveis (nomes e definições vivem em `references/cenarios-stress.md`)
- `observacoes`: peculiaridades relevantes (ex.: "não pode posicionar em moedas exóticas", "concentração setorial máx 25%")

Ver `references/mandato-estrutura.md` para o schema completo e exemplos por família.

## Semáforo de Alertas

Padrão usado em monitor diário, Morning Call, e qualquer report. **Aplicar por métrica, não pelo fundo como um todo** — um fundo pode estar verde em VaR e vermelho em concentração.

| Estado | Critério | Ação |
|--------|----------|------|
| 🟢 Verde | Utilização < 70% do limite soft | Reportar no sumário agregado apenas |
| 🟡 Amarelo | 70%–100% do soft, ou tendência de alta >10% em 5d úteis | Mencionar no Morning Call, flag para gestor |
| 🔴 Vermelho | Acima do soft, ou qualquer hard sendo tocado | Destaque no topo do Morning Call, comunicação imediata ao gestor e CIO |
| ⚫ Breach | Hard rompido | Escalation: gestor, CIO, compliance no mesmo dia |

"Utilização" sempre como `métrica_atual / limite`. Para orçamento de perda, é `perda_acumulada / orçamento`.

## Interpretação de Variações

Ao comentar movimentação de risco (dia a dia), sempre distinguir:

1. **Aumento por posição nova** — gestor adicionou risco. Pergunta: é consistente com a tese?
2. **Aumento por movimento de mercado** — vol subiu, VaR sobe mecanicamente. Pergunta: é persistente ou ruído?
3. **Aumento por rolagem de janela** — VaR histórico pode saltar quando um dia calmo sai da janela e um dia ruim entra. Pergunta: é artefato?
4. **Redução por marcação** — posições cairam de valor, reduzindo exposição nominal. Não é "redução de risco" no sentido ativo.

Sempre que possível, atribuir a variação a uma dessas quatro causas antes de comentar.

## O que **não** fazer

- Não dar leitura de risco sem contextualizar com o mandato do fundo específico.
- Não agregar métricas entre famílias diferentes (somar VaR de Multimercado e Renda Fixa não tem significado útil).
- Não reportar métricas sem a utilização relativa ao limite — "VaR de R$ 2mm" é menos informativo que "VaR em 68% do limite soft".
- Não apresentar o Morning Call como listagem de todos os fundos em ordem. Agrupar por família e começar por exceções (amarelos e vermelhos).

## Referências

- `references/mandato-estrutura.md` — Schema completo do mandato por família, com campos específicos de cada uma.
- `references/cenarios-stress.md` — Biblioteca de cenários de stress (Crise 2008, Joesley, COVID-Março-20, eleição 2022, etc.) e sua aplicabilidade por família.
- `references/glossario.md` — Definições precisas de cada métrica (VaR, DV01, tracking error, etc.) com a convenção usada internamente.
- `references/infra-glpg.md` — **Infraestrutura GLPG autoritativa**: pipeline de coleta existente (market_data.json atualizado a cada 1 min), tabelas do banco, armadilhas de queries documentadas. Fonte: GLPG_System_Guide.docx.
- `assets/mandato-template.json` — Template em branco para cadastrar um novo fundo.
- `assets/fundos-canonicos.json` — **Mapa AUTORITATIVO** dos nomes de fundos (TRADING_DESK, chaves JSON, variações entre tabelas). Sempre consultar antes de escrever query.

## Skills filhas (a criar)

Esta skill-mãe será consultada pelas seguintes skills de rotina, nessa ordem sugerida de criação:

1. `risk-daily-monitor` — pull dos dados do GLPG, cálculo de utilizações, identificação de flags amarelos/vermelhos.
2. `risk-morning-call` — consolida o monitor num briefing executivo + dashboard visual para o Morning Call.
3. `risk-breach-alert` — triage e comunicação quando um limite é rompido.
4. `risk-mandate-onboarding` — assistente para cadastrar um novo fundo no formato canônico.

# Política do IDKA — Pontos Pendentes

Três decisões de política precisam ser tomadas com a gestão antes da skill operar com semáforo completo. Este arquivo é o **registro institucional** das pendências.

---

## Pendência 1: Metodologia de "Melhor Réplica"

### O problema

O book `Benchmark_IDKA` deveria ser uma réplica perfeita do índice IDKA oficial. Na prática, haverá desvios por:

- Papéis disponíveis no mercado podem divergir da composição oficial
- Reinvestimento de cupons em momentos diferentes dos oficiais
- Timing de rebalanceamento
- Restrições operacionais (tamanho mínimo de lote, liquidez)

A pergunta é: **como medir o quão boa é a réplica?**

### Opções

**Opção A — Autoconsistência (mais simples)**
- Assumir que o perfil do book `Benchmark_IDKA` *é* a réplica oficial do dia
- Monitorar apenas o BVaR_Benchmark_IDKA ao longo do tempo
- Se crescer → réplica está se degradando
- Vantagem: não depende de fonte externa
- Desvantagem: não detecta viés sistemático vs. índice oficial

**Opção B — Comparação vs. Anbima (rigorosa)**
- Obter composição oficial do índice IDKA (2A ou 10A) da Anbima
- Calcular ANO_EQ por vértice do índice oficial
- Calcular o **gap** = diferença absoluta entre perfil carregado e perfil oficial
- Vantagem: mensura fielmente a qualidade da réplica
- Desvantagem: requer fonte externa (API Anbima ou planilha diária)

**Opção C — Híbrida**
- Autoconsistência no dia-a-dia (Opção A)
- Validação mensal contra Anbima (Opção B)

### O que precisa ser decidido

- Qual opção adotar
- Se B ou C: onde obter o perfil oficial e com que frequência
- Tolerância: qual gap é aceitável antes de acionar alerta

### Status

**Não decidido.** Skill opera hoje com Opção A implícita: reporta BVaR_Benchmark_IDKA como proxy de qualidade da réplica.

---

## Pendência 2: Identificação da Alocação em Albatroz

### O problema

O book `RF_LF` (risco ativo) pode conter alocação no fundo Albatroz — um fundo de RF com mais liberdade. Essa alocação aparece como uma posição em outro fundo, não como papel individual.

Três questões práticas:

1. **Como identificar?** Qual é o `PRODUCT` ou `PRODUCT_CLASS` correspondente ao Albatroz no banco?
2. **Como calcular DV01?** A posição em outro fundo não tem DV01 nativo — precisa de look-through
3. **Como reportar?** Tratar como peça separada dentro do RF_LF ou agregar silenciosamente?

### Opções de identificação

Hipóteses a confirmar na primeira execução:

- `PRODUCT_CLASS` contendo "FIC", "FIM", "Multiclass"
- `PRODUCT` contendo "Albatroz"
- Coluna dedicada de tipo de ativo (pouco provável)
- Tabela de cadastro de fundos investidos

**Ação:** rodar a query 2 de `queries-idka.md` (posições do IDKA), filtrar por `BOOK = 'RF_LF'`, inspecionar os PRODUCTs disponíveis, identificar manualmente.

### Opções de DV01 em posição em outro fundo

**Opção A — Look-through puro**
- Obter posições internas do Albatroz
- Calcular DV01 ponderado por participação
- Ideal mas requer acesso às posições do Albatroz
- Só faz sentido se a alocação for material

**Opção B — Proxy estático**
- Assumir DV01 médio histórico do Albatroz
- Revisar periodicamente
- Simples, mas impreciso

**Opção C — DV01 zero (ignorar)**
- Tratar posição como caixa para fins de DV01
- **Subestima risco real** — não recomendado

### Status

**Não decidido.** Skill hoje não aplica nenhum dos três — marca posições em outros fundos apenas se identificáveis, sem DV01 look-through.

---

## Pendência 3: Orçamento de Perda Relativa ao Benchmark

### O problema

Equivalente ao "stop mensal" do MACRO, mas relativo. Precisamos definir:

> Quanto o IDKA pode perder vs. o benchmark ao longo do mês antes de acionar redução de risco?

Isso **não existe formalmente**. A gestão opera de forma discricionária.

### Propostas para discussão

**Proposta A — Stop absoluto em bps**
- IDKA 3Y: stop mensal = −X bps vs. benchmark (ex.: −30 bps)
- IDKA 10Y: stop mensal = −Y bps (ex.: −60 bps, maior por ter mais duration)
- Vantagem: simples e direto
- Desvantagem: não se ajusta ao nível de risco ativo vigente

**Proposta B — Múltiplo de BVaR**
- Stop mensal = k × BVaR diário médio do fundo
- k pode ser 5, 8, 10 — a definir
- Vantagem: auto-ajustável ao risco que o fundo está carregando
- Desvantagem: requer escolha de k e tratamento de mês parcial

**Proposta C — Percentual do target alpha**
- Se target alpha anual é X bps (digamos, +150 bps para IDKA 3Y)
- Stop mensal = −Y% de (X/12)
- Exemplo: −150% do target mensal de 12.5 bps = −19 bps
- Vantagem: ancora no objetivo de performance
- Desvantagem: depende de target alpha formalizado (que talvez também não exista)

**Proposta D — Histórico empírico**
- Analisar trajetória histórica de alpha MTD dos fundos
- Definir stop como percentil 5 da pior perda mensal em N meses
- Vantagem: ancorado em dados
- Desvantagem: se histórico tem regime diferente, enviesa

### Decisões adicionais necessárias

Mesmo escolhida a metodologia:

- **Janela:** mês-calendário ou rolling 21 dias úteis?
- **Granularidade:** hard stop ou gatilhos graduais (ex.: 50% = amarelo, 100% = vermelho, 150% = ação)?
- **Reset:** após atingir o stop, o que acontece no mês seguinte? Volta ao cheio? Meio?
- **Diferenciação 3Y vs 10Y:** parâmetros iguais ou diferentes?

### Status

**Não decidido.** Skill reporta PnL relativo MTD mas não aplica semáforo nem gatilho.

---

## Pendência 4 (bônus): Critérios de Redução de Risco

Relacionado à pendência 3, mas separado porque endereça a **ação**, não o gatilho.

Quando a perda relativa se acumula, o que fazer?

- Reduzir DV01 do book `RF_LF`?
- Reduzir alocação no Albatroz?
- Fechar apostas específicas?
- Comunicar ao comitê?
- Tudo isso em qual ordem?

**Até que essa política exista,** a skill apenas **sinaliza** condições que plausivelmente disparariam ação — mas sem prescrever qual ação.

---

## Como proceder

1. **Discussão com a gestão** nos pontos 1-4
2. **Documentar decisões** neste arquivo (atualizar a seção "Status" de cada pendência)
3. **Implementar** em `assets/politica-idka.json` as configurações resultantes
4. **Atualizar a skill** para remover flags "pendente" e aplicar semáforos

## Gatilhos de revisão

Quando revisar/renegociar a política:

- Mudança material no AUM dos fundos (>30% de variação)
- Mudança no target alpha
- Mudança na estratégia (introdução ou retirada de alocação no Albatroz)
- Performance sistematicamente fora do esperado (bom ou ruim) por 2+ trimestres
- A cada 12 meses, por padrão

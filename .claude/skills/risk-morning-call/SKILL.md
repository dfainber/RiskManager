---
name: risk-morning-call
description: Orquestrador final do briefing diário de risco e performance. Consome manifesto do risk-data-collector + outputs das 7 skills de análise, consolida em painel HTML com sumário executivo, visão horizontal (cross-fund) e vertical (por fundo). Marca dados ausentes/degradados. Use para pedidos sobre Morning Call, briefing diário, painel de risco, ou resumo consolidado dos fundos.
---

# Risk Morning Call

Orquestrador final. Consome tudo que as outras skills produzem e gera **um briefing único** para o Morning Call da gestora.

**Dependências (todas as outras skills do kit):**
- `risk-data-collector` — manifesto de disponibilidade
- `risk-daily-monitor` — monitor agregado
- `macro-stop-monitor` — stops por PM do MACRO
- `macro-risk-breakdown` — análise detalhada do MACRO
- `evolution-risk-concentration` — diversificação Evolution
- `rf-idka-monitor` — RF benchmarked
- `performance-attribution` — atribuição de performance
- `risk-manager` — convenções gerais

---

## Fluxo geral

```
1. Coleta  ─► Rotina de madrugada já rodou o risk-data-collector
             ↓
2. Manifesto ─► Morning Call lê snapshots/<data>_manifest.json
             ↓
3. Execução ─► Para cada skill não-bloqueada, roda e coleta output
             (skills atrasadas rodam em modo degradado com flag)
             ↓
4. Síntese  ─► Sumário executivo automático
             ↓
5. Painéis  ─► Gera HTML com visão horizontal + vertical
             ↓
6. Entrega  ─► HTML salvo + email (opcional)
```

---

## Estrutura do briefing

### Seção 0 — Cabeçalho e Completude

```
# Morning Call — [data-base] — gerado em [timestamp]

## Status do Relatório
✅ 5 de 7 skills completas
⚠️ 2 em modo degradado: macro-stop-monitor, performance-attribution (dados D-1)
❌ Nenhuma bloqueada

[Link para manifesto detalhado]
```

### Seção 1 — Sumário Executivo

Essa é a parte mais importante. Estrutura:

```
## Sumário Executivo

### Performance do dia
| Fundo           | PnL Dia | PnL MTD | vs. Benchmark MTD | Destaque |
|-----------------|---------|---------|-------------------|----------|
| MACRO           | +18 bps | +42 bps | —                 | RF-BZ    |
| QUANTITATIVO    | +12 bps | +28 bps | —                 |          |
| MACRO Q         |  +4 bps | +15 bps | —                 |          |
| EVOLUTION       | +14 bps | +35 bps | —                 |          |
| ALBATROZ        |  +3 bps | +12 bps | —                 |          |
| IDKA 3Y         |  +3 bps | +28 bps | +3.3 bps (alpha)  |          |
| IDKA 10Y        |  +5 bps | +45 bps | −1.2 bps          | ⚠️ neg   |

### Principais Riscos por Produto
- MACRO: VaR em 42 bps (P78 hist.). Concentração em RF-BZ (32%)
- EVOLUTION: diversification ratio saudável (P32). Sem alinhamento.
- IDKA 10Y: risco ativo elevado em curto prazo da curva
- QUANTITATIVO: posição longa concentrada em USDBRL
- [outros conforme relevância]

### Pontos de Atenção
1. IDKA 10Y: alpha MTD negativo (−1.2 bps) — monitorar consumo do orçamento relativo
2. MACRO (PM LF): −44 bps MTD, 69% do stop — atenção se continuar
3. CREDITO dentro do Evolution: flag do caveat de cotas júnior acende (ver detalhe)

### Sugestões de Aprofundamento
- Investigar detrator em IDKA 10Y (book RF_LF, seção detalhada)
- Avaliar se ajuste no LF do MACRO é pertinente
- Validar marcação das cotas júnior do Evolution com equipe de crédito
```

### Seção 2 — Visão Horizontal (comparativo)

Todos os fundos lado a lado, uma linha cada, métricas comparáveis:

```
## Visão Horizontal

### Risco (hoje)
| Fundo      | VaR (bps) | Stress bps (cenário) | Utilização vs. limite |
|------------|-----------|----------------------|------------------------|
| MACRO      |   42.1    | −180 (Crise 2008)    | 66% soft               |
| QUANTITATIVO | 25.3    | −95                  | 51% soft               |
...

### Orçamento Disponível
| Fundo      | Stop mensal | Consumido MTD | Restante | % rest |
|------------|-------------|----------------|----------|--------|
| MACRO (CI) |  −63 bps    |  −12 bps       |  51 bps  | 81%    |
| MACRO (LF) |  −63 bps    |  −44 bps       |  19 bps  | 30%    |
...

### PA do Dia (top 3 contribuições por fundo)
| Fundo      | Top+ #1        | Top+ #2      | Top+ #3      |
|------------|----------------|--------------|--------------|
| MACRO      | RF-BZ (+8.2)   | FX-BRL (+4.5)| COMMO (+2.8) |
...
```

### Seção 3 — Visão Vertical (por produto, A-Z)

Para cada fundo, uma seção completa integrando risco + PA + observações:

```
## [MACRO] — Galapagos Macro FIM

### Estado de risco
- VaR 42 bps (P78 histórico) — 🟡 elevado mas não em breach
- Stress principal: −180 bps no cenário Crise 2008 (50% do stop semestral)
- Concentração: RF-BZ é 32% do VaR (top contribuidor)
- Mudança D-1: +3.2 bps (crescimento gradual)

### Estado dos PMs
| PM  | PnL Dia | PnL MTD | Stop rest. | Estado |
|-----|---------|---------|------------|--------|
| CI  | +6.1    | +18.2   | 51 bps (81%) | 🟢   |
| LF  | +4.2    | −44.0   | 19 bps (30%) | 🟡   |
| ...

### PA do dia
- Contribuidor dominante: RF-BZ (+8.2 bps, 45% do PnL do dia)
- Detrator: RV-EM (−1.2 bps, LF)

### Breakdown analítico detalhado
[link para seção expandida com 4 camadas do macro-risk-breakdown]

### Sinais para o dia
- LF deve ter atenção — próximo do soft do stop mensal
- Apostas concentradas em RF-BZ estão rendendo mas aumentam sensibilidade a curva
```

Seguir o mesmo padrão para QUANTITATIVO, MACRO Q, EVOLUTION, ALBATROZ, IDKA 3Y, IDKA 10Y.

---

## Geração de HTML

Output principal: `data/morning-calls/<YYYY-MM-DD>.html`

**Template base:** `assets/template-base.html` — arquivo completo com design system,
estrutura de abas e todos os placeholders `{{VARIAVEL}}` documentados.

### Design System

O Morning Call usa **exatamente o mesmo design system** do `Cross-Asset Monitor`
(`F:\Bloomberg\Quant\Claude_GLPG_Fetch\index.html`). Nunca inventar estilos novos —
reusar as variáveis CSS e os componentes abaixo.

**Fontes:** `Manrope` (prosa) + `JetBrains Mono` (números, tickers, bps).

**Paleta (CSS variables):**
```css
--bg:#0b0d10        /* fundo geral */
--panel:#14181d     /* cards, tabelas */
--panel-2:#181d24   /* header de card, thead */
--line:#232a33      /* bordas */
--text:#e7ecf2      /* texto principal */
--muted:#8892a0     /* labels, subtítulos */
--accent:#0071BB    /* azul GLPG */
--up:#26d07c        /* verde (positivo) */
--down:#ff5a6a      /* vermelho (negativo) */
--warn:#f5a623      /* amarelo (atenção) */
```

**Componentes reutilizáveis (documentados no template):**

| Componente CSS        | Uso no Morning Call                             |
|-----------------------|-------------------------------------------------|
| `.kpi-card`           | VaR casa / Stop consumido / PnL dia / ER MTD    |
| `.fund-card`          | Card por fundo com semáforo + 4 métricas-chave  |
| `.sem.green/yellow/red/black` | Semáforo de risco — mapeamento abaixo  |
| `.util-row` + `.util-bar` | Barra de utilização stop por PM (0→100%)   |
| `.ret` centro-ancorado | PnL dia/MTD em bps (igual às tabelas de mercado) |
| `.atencao-item.warn/crit` | Pontos de atenção automáticos                |
| `.alert.bma`          | Bull Market Alignment (Evolution) — sempre topo |
| `.degraded-banner`    | Aviso de dados parciais                         |
| `thead/tbody` padrão  | Todas as tabelas de breakdown                   |

**Mapeamento semáforo → CSS:**
- `sem green`  ← < 70% do limite soft
- `sem yellow` ← 70–100% do soft
- `sem red`    ← > 100% soft (hard respeitado)
- `sem black`  ← hard rompido (breach)
- `sem na`     ← sem mandato / dado indisponível

### Estrutura de abas do HTML

```
Sumário | MACRO | QUANT | Evolution | RF / IDKA | PA
```

- **Sumário:** KPI strip + fund cards + pontos de atenção + tabela horizontal
- **MACRO:** stop bars por PM + breakdown VaR por classe + VaR por PM + shock absorption
- **QUANT:** breakdown por book/classe
- **Evolution:** VaR por estratégia (barras) + Diversification Ratio + correlações + caveat crédito
- **RF / IDKA:** BVaR por book + ANO_EQ por vértice + ER MTD streak
- **PA:** Regime A (5 fundos) + Regime B (IDKAs) + destaques automáticos

### Placeholders do template

Todos os `{{PLACEHOLDER}}` no template são substituídos em Python via `str.replace()` ou Jinja2.
Blocos condicionais usam `{{IF_X_START}}...{{IF_X_END}}` — remover o bloco inteiro quando condição falsa.

Exemplos principais:
- `{{DATE}}` — data-base do relatório (YYYY-MM-DD)
- `{{GENERATED_AT}}` — timestamp de geração
- `{{SKILLS_OK}}/{{SKILLS_TOTAL}}` — completude
- `{{STATUS_CLASS}}` — `ok` | `warn` | `error`
- `{{MACRO_DIA_BPS}}`, `{{MACRO_DIA_CLASS}}` — valor + classe CSS (`up`/`down`)
- `{{MACRO_SEM_CLASS}}`, `{{MACRO_SEM_LABEL}}` — semáforo
- `{{HORIZONTAL_ROWS}}` — HTML das `<tr>` da tabela horizontal
- `{{MACRO_STOP_ROWS}}` — HTML das `.util-row` de stops por PM
- `{{ATENCAO_ITEMS}}` — HTML dos `<li class="atencao-item">` gerados pelas heurísticas

### Tabelas interativas

- Ordenação por coluna
- Links internos entre abas (card do fundo no Sumário → aba do fundo)
- Cores consistentes com o semáforo
- Suporte a print CSS (todos os estilos @media print documentados no template)

## Análise automática do sumário executivo

O sumário não é uma lista estática — é **análise**. A skill precisa:

### Identificação de destaques

**Por fundo, identificar e descrever:**

1. **Performance absoluta:** fundo com PnL no top/bottom decil dos últimos 60 dias úteis?
2. **Performance relativa:** fundo benchmarked com ER acima/abaixo do histórico?
3. **Fonte do retorno:** qual foi a contribuição dominante (via PA)?
4. **Risco elevado:** VaR em percentil alto, orçamento consumido > 50%?

### Geração de texto narrativo

Exemplo de lógica:

```python
def gerar_narrativa_fundo(fundo_data, pa_data, risco_data, hist_data):
    partes = []
    
    # Performance
    if fundo_data['pnl_dia_bps'] > hist_data['p90_dia']:
        partes.append(f"{fundo} teve dia excepcional (+{fundo_data['pnl_dia_bps']:.0f} bps).")
    
    # Fonte do retorno
    top_pa = pa_data['top_contribuidor_dia']
    partes.append(f"Principal contribuição: {top_pa['book']} (+{top_pa['bps']:.1f} bps).")
    
    # Risco
    if risco_data['pct_stop_mensal_consumido'] > 50:
        partes.append(f"⚠️ Atenção: {risco_data['pct_stop_mensal_consumido']:.0f}% do stop mensal consumido.")
    
    return " ".join(partes)
```

### Sugestões de aprofundamento

Heurísticas:

- Se um PM tem PnL < −30 bps MTD → sugerir investigação
- Se ER de fundo benchmarked está consistentemente negativo (>3 dias) → sugerir revisão da réplica ou do risco ativo
- Se camada 4 do Evolution (Bull Market Alignment) disparar → sempre destacar
- Se caveat das cotas júnior está ativo e CREDITO está no top 3 de contribuição → sugerir validação

## Execução

### Modo normal

```
# Assumindo que risk-data-collector já rodou
# e que o manifesto existe em snapshots/<data>_manifest.json

risk-morning-call --date auto --output /path/to/morning-calls/
```

### Modo interativo

Também utilizável sob demanda durante o dia:

> "Claude, monta o Morning Call pra hoje"

Skill lê o manifesto mais recente e gera o HTML. Se manifesto não existe, invoca o coletor primeiro.

### Modo parcial

Para gerar o briefing mesmo com dados incompletos:

```
risk-morning-call --allow-degraded --date 2026-04-16
```

Cada seção fica com um aviso visual de dados ausentes.

## Regras de comportamento

- **O briefing é ponto de partida para conversas, não conclusão** — destaques apontam para aprofundamento
- **Nunca esconder problemas de completude** — se dados estão faltando, aparece no topo
- **Ordem das seções é fixa** — sumário → horizontal → vertical. Usuário deve saber onde procurar
- **Narrativa automática é sucinta** — 1–3 frases por fundo no sumário. Detalhes ficam nas seções verticais
- **Destaques automáticos são sugestão** — sempre com linguagem "considere investigar", nunca prescritiva
- **Histórico do Morning Call fica acessível** — `morning-calls/` guarda HTMLs anteriores para comparação ao longo do tempo

## Referências

- `references/estrutura-html.md` — template e padrões visuais
- `references/geracao-narrativa.md` — heurísticas para os textos automáticos do sumário
- `references/integracao-skills.md` — como consumir output de cada skill

## Assets

- `assets/template-base.html` — skeleton HTML com CSS
- `assets/heuristicas-destaque.json` — parâmetros para detecção de destaques
- `morning-calls/` — HTMLs gerados (criados em runtime)

## Skills relacionadas

Todas as outras skills do kit alimentam esta.

## Roadmap

1. **Agora:** estrutura completa com HTML estático + tabelas + narrativa automática
2. **MVP:** rodar manual, 1 vez ao dia, validar qualidade do briefing
3. **Após MVP:** agendamento automático, envio por email, histórico consultável
4. **Futuro:** painel interativo (Dash/Streamlit), alertas em tempo real, comparação entre dias

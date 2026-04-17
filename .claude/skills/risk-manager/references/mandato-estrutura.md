# Estrutura de Mandato — Schema por Família

Todos os mandatos seguem um esqueleto comum + campos específicos da família. Arquivo por fundo: `mandato-<ticker>.json`.

## Esqueleto comum (todos os fundos)

```json
{
  "fundo": "string (ticker interno)",
  "nome_completo": "string",
  "familia": "multimercados | renda-fixa | credito | renda-variavel",
  "benchmark": "CDI | IMA-B | Ibovespa | IPCA+X | null",
  "pl_referencia": "number (PL em R$ para cálculo de % — atualizar mensalmente)",
  "orcamento_perda": {
    "anual_pct": "number (ex: 4.0 para 4% ao ano)",
    "mensal_pct": "number | null",
    "janela_peak_to_trough": "number | null (drawdown máximo tolerado em %)"
  },
  "cenarios_stress": ["lista de nomes de cenários — ver cenarios-stress.md"],
  "observacoes": "string (peculiaridades relevantes em prosa livre)",
  "gestor_responsavel": "string",
  "ultima_atualizacao": "YYYY-MM-DD"
}
```

## Bloco `limites` — específico por família

Cada limite é `{hard, soft, unidade}`. `hard` é o teto regulamentar/mandato (romper = breach). `soft` é o gatilho interno de alerta (tipicamente 80–90% do hard).

### Multimercados

```json
"limites": {
  "var_99_1d": {"hard": 2.0, "soft": 1.6, "unidade": "%PL"},
  "stress_cenario_principal": {"hard": 8.0, "soft": 6.5, "unidade": "%PL"},
  "exposicao_bruta": {"hard": 400, "soft": 350, "unidade": "%PL"},
  "exposicao_liquida_equities": {"hard": 50, "soft": 40, "unidade": "%PL"},
  "concentracao_maior_posicao": {"hard": 15, "soft": 12, "unidade": "%PL"},
  "liquidez_d1": {"hard": 70, "soft": 80, "unidade": "%PL (mínimo liquidável em D+1)"}
}
```

Nota: para `liquidez_d1`, hard é o **piso** (abaixo = breach), não teto. Sinalizar no campo via comentário no JSON.

### Renda Fixa

```json
"limites": {
  "dv01_total": {"hard": 150000, "soft": 120000, "unidade": "R$/bp"},
  "duration_modificada": {"hard": 5.5, "soft": 4.8, "unidade": "anos"},
  "var_99_1d": {"hard": 0.8, "soft": 0.65, "unidade": "%PL"},
  "stress_paralelo_100bp": {"hard": 4.0, "soft": 3.2, "unidade": "%PL"},
  "tracking_error_ex_ante": {"hard": 1.5, "soft": 1.2, "unidade": "% a.a."},
  "exposicao_indexador_cambial": {"hard": 0, "soft": 0, "unidade": "%PL (proibido)"}
}
```

Incluir apenas os indexadores permitidos pelo mandato. Proibições explícitas (ex.: câmbio) entram com hard=0.

### Crédito Privado

```json
"limites": {
  "concentracao_top5_emissores": {"hard": 30, "soft": 25, "unidade": "%PL"},
  "concentracao_maior_emissor": {"hard": 8, "soft": 6, "unidade": "%PL"},
  "concentracao_setor": {"hard": 25, "soft": 20, "unidade": "%PL (por setor)"},
  "rating_minimo": {"hard": "BB+", "soft": "BBB-", "unidade": "rating (qualquer papel abaixo é breach)"},
  "% abaixo_investment_grade": {"hard": 20, "soft": 15, "unidade": "%PL"},
  "spread_duration": {"hard": 3.0, "soft": 2.5, "unidade": "anos"},
  "stress_abertura_spread_100bp": {"hard": 3.0, "soft": 2.4, "unidade": "%PL"},
  "caixa_minimo": {"hard": 5, "soft": 8, "unidade": "%PL (piso)"}
}
```

Ratings comparados pela escala interna (Moody's/S&P/Fitch → mapa em `glossario.md`).

### Renda Variável

```json
"limites": {
  "exposicao_bruta": {"hard": 150, "soft": 130, "unidade": "%PL"},
  "exposicao_liquida": {"hard": 100, "soft": 95, "unidade": "%PL (para L/S: min e max)"},
  "beta_ex_ante": {"hard": 1.2, "soft": 1.1, "unidade": "vs benchmark"},
  "tracking_error": {"hard": 6.0, "soft": 5.0, "unidade": "% a.a."},
  "top10_posicoes": {"hard": 45, "soft": 40, "unidade": "%PL"},
  "concentracao_maior_posicao": {"hard": 8, "soft": 6, "unidade": "%PL"},
  "concentracao_setor": {"hard": 30, "soft": 25, "unidade": "%PL"},
  "liquidez_5d_20pct_adv": {"hard": 80, "soft": 90, "unidade": "%PL liquidável (piso)"},
  "var_99_1d": {"hard": 3.0, "soft": 2.5, "unidade": "%PL"}
}
```

Para L/S puros, `exposicao_liquida` costuma ser intervalo (ex.: `{hard_min: -20, hard_max: 20}`). Adaptar schema se necessário.

## Convenções de preenchimento

- **Percentuais** sempre em pontos percentuais (5.0 = 5%), nunca decimal (0.05).
- **Moeda** sempre R$ explícito na unidade.
- **Rating** como string com a escala de referência (ex.: "BBB-" escala S&P).
- **Stress** sempre com sinal — perda expressa como número positivo (ex.: 8.0 significa perda de 8% do PL no cenário).
- **Cenário "principal"** de stress é o que o mandato elege como worst-case operacional. Outros cenários ficam na lista `cenarios_stress` para monitoramento, mas não geram breach.

## Quando criar/revisar o mandato

- Criação: todo fundo novo antes da primeira operação.
- Revisão obrigatória: mudança de gestor, de benchmark, de política de investimento, ou alteração regulamentar.
- Revisão recomendada: a cada 12 meses, mesmo sem mudança formal, para reconfirmar que os soft limits ainda refletem o apetite atual.

## Armazenamento

Convenção sugerida: `<pasta-risco>/mandatos/mandato-<ticker>.json`. Um índice `mandatos/_index.json` lista todos os fundos ativos com path, família e status (ativo/em liquidação/suspenso).

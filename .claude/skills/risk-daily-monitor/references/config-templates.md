# Templates de Configuração

Dois arquivos de configuração suportam a skill: um registro de fundos e os mandatos individuais.

## `fund-registry.json`

Lista central de fundos monitorados. A primeira versão (MM apenas):

```json
{
  "fundos_ativos": [
    {
      "ticker": "MACRO",
      "nome_completo": "Galapagos Macro FIM",
      "familia": "multimercados",
      "subfamilia": "macro-discricionario",
      "trading_desk": "Galapagos Macro FIM",
      "tree_principal": "Main_Macro_Ativos",
      "tree_secundario": "Main_Macro_Gestores",
      "books_ativos": [
        "RF-BZ", "RF-EM", "RF-DM",
        "FX-BRL", "FX-EM", "FX-DM",
        "RV-BZ", "RV-EM", "RV-DM",
        "COMMODITIES", "P-Metals"
      ],
      "books_gestores": ["CI", "LF", "JD", "RJ"],
      "mandato_path": "mandato-MACRO.json",
      "html_path": "F:\\Bloomberg\\Quant\\Rotinas\\RELATORIO_EXPO_PNL_AUTOMATICO_HTML\\dashboard_macro_YYYY-MM-DD.html",
      "status": "ativo"
    },
    {
      "ticker": "SISTEMATICO",
      "nome_completo": "Galapagos Quantitativo FIM",
      "familia": "multimercados",
      "subfamilia": "sistematico",
      "trading_desk": "Galapagos Quantitativo FIM",
      "tree_principal": "PREENCHER_NA_PRIMEIRA_EXECUCAO",
      "books_ativos": ["PREENCHER_NA_PRIMEIRA_EXECUCAO"],
      "mandato_path": "mandato-SISTEMATICO.json",
      "html_path": "F:\\Bloomberg\\Quant\\Rotinas\\RELATORIO_EXPO_PNL_AUTOMATICO_HTML\\dashboard_sistematico_YYYY-MM-DD.html",
      "status": "ativo"
    },
    {
      "ticker": "EVOLUTION",
      "nome_completo": "Galapagos Evolution FIC FIM CP",
      "familia": "multimercados",
      "subfamilia": "guarda-chuva",
      "trading_desk": "Galapagos Evolution FIC FIM CP",
      "tree_principal": "Main",
      "books_ativos": null,
      "categorias": ["FRONTIER", "EVO_STRAT", "MACRO", "CREDITO", "CAIXA"],
      "classificador": "classificar_book",
      "mandato_path": "mandato-EVOLUTION.json",
      "html_path": "F:\\Bloomberg\\Quant\\Rotinas\\RELATORIO_EXPO_PNL_AUTOMATICO_HTML\\dashboard_evolution_YYYY-MM-DD.html",
      "status": "ativo"
    }
  ],
  "ignorar": [
    "Galapagos Evo Strategy FIM CP"
  ],
  "ultima_atualizacao": "2026-04-16"
}
```

**Observações:**
- `tree_principal` / `tree_secundario` são strings que vão em `WHERE "TREE" = ...` nas queries.
- `books_ativos: null` para EVOLUTION porque a classificação é dinâmica (via `classificar_book`).
- `ignorar` lista fundos que explicitamente saem do escopo.

## `mandato-<fundo>.json` — schema

Ver `risk-manager/references/mandato-estrutura.md` para o schema completo. Abaixo, esqueletos iniciais prontos para preencher para os três MM.

### `mandato-MACRO.json` (esqueleto)

```json
{
  "fundo": "MACRO",
  "nome_completo": "Galapagos Macro FIM",
  "familia": "multimercados",
  "subfamilia": "macro-discricionario",
  "benchmark": "CDI",
  "pl_referencia": 0,
  "gestor_responsavel": "PREENCHER",
  "ultima_atualizacao": "PREENCHER",

  "orcamento_perda": {
    "anual_pct": 0.0,
    "mensal_pct": null,
    "janela_peak_to_trough": null
  },

  "limites": {
    "var_99_1d_fundo": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"},
    "stress_cenario_principal": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"},
    "exposicao_book_RF-BZ": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"},
    "exposicao_book_FX-BRL": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"},
    "exposicao_book_RV-BZ": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
  },

  "cenarios_stress": [],
  "cenario_principal": null,
  "observacoes": ""
}
```

### `mandato-SISTEMATICO.json` (esqueleto)

```json
{
  "fundo": "SISTEMATICO",
  "nome_completo": "Galapagos Quantitativo FIM",
  "familia": "multimercados",
  "subfamilia": "sistematico",
  "benchmark": "CDI",
  "pl_referencia": 0,
  "gestor_responsavel": "PREENCHER",
  "ultima_atualizacao": "PREENCHER",

  "orcamento_perda": {
    "anual_pct": 0.0,
    "mensal_pct": null,
    "janela_peak_to_trough": null
  },

  "limites": {
    "var_99_1d_fundo": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"},
    "stress_cenario_principal": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
  },

  "cenarios_stress": [],
  "cenario_principal": null,
  "observacoes": "Sistemático: limites podem ter natureza diferente (alocação modelo, risco de modelo)"
}
```

### `mandato-EVOLUTION.json` (esqueleto) — tem limites por categoria

```json
{
  "fundo": "EVOLUTION",
  "nome_completo": "Galapagos Evolution FIC FIM CP",
  "familia": "multimercados",
  "subfamilia": "guarda-chuva",
  "benchmark": "CDI",
  "pl_referencia": 0,
  "gestor_responsavel": "PREENCHER",
  "ultima_atualizacao": "PREENCHER",

  "orcamento_perda": {
    "anual_pct": 0.0,
    "mensal_pct": null,
    "janela_peak_to_trough": null
  },

  "limites_agregado": {
    "var_99_1d_fundo": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"},
    "stress_cenario_principal": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
  },

  "limites_por_categoria": {
    "FRONTIER": {
      "exposicao": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
    },
    "EVO_STRAT": {
      "exposicao": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
    },
    "MACRO": {
      "exposicao": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
    },
    "CREDITO": {
      "exposicao": {"hard": 0.0, "soft": 0.0, "unidade": "%PL"}
    },
    "CAIXA": {
      "exposicao_minima": {"hard": 0.0, "soft": 0.0, "unidade": "%PL (piso)"}
    }
  },

  "cenarios_stress": [],
  "cenario_principal": null,
  "observacoes": "Fundo guarda-chuva: agregado de FRONTIER + EVO_STRAT + MACRO + CREDITO + CAIXA via função classificar_book."
}
```

## Localização sugerida

Todos os arquivos de configuração em uma pasta dedicada, fora do path da skill:

```
<pasta-risco>/
├── fund-registry.json
├── mandatos/
│   ├── mandato-MACRO.json
│   ├── mandato-SISTEMATICO.json
│   └── mandato-EVOLUTION.json
└── snapshots/
    └── YYYY-MM-DD_mm.json
```

A primeira vez que a skill rodar, se não achar esses arquivos, deve perguntar ao usuário onde criá-los (ou usar um default tipo `F:\Bloomberg\Quant\Rotinas\Risco\`).

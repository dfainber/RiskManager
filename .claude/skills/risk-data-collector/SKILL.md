---
name: risk-data-collector
description: Coletor de dados da rotina de madrugada para Morning Call. Verifica se bases GLPG-DB01 (necessárias para skills de risco) estão atualizadas; se defasada, tenta disparar a rotina geradora; produz manifesto JSON consumido pela risk-morning-call. Não substitui rotinas — verifica, tenta, reporta. Use para pedidos sobre rodar coleta, verificar dados, status das bases, ou executar coleta noturna.
---

# risk-data-collector

> **Status (2026-05-01): design abandonado.**
>
> A coleta JSON-manifest descrita no design original nunca foi implementada.
> O check de freshness das bases GLPG-DB01 vive hoje dentro do próprio
> `generate_risk_report.py` (card "Status dos Dados") e em `smoke_test.py`.
>
> Nenhum dos artefatos referenciados no design original existe:
> `rotinas-geradoras.json`, `bases-checklist.md`, cron jobs em 03:00/04:00/05:00.
> Não há orchestrator separado.
>
> Esta skill é mantida como entrada nominal apenas porque outras skills ainda
> referenciam pelo nome. Se aparecer em busca, redirecionar para:
>
> - `generate_risk_report.py` (card "Status dos Dados") — freshness check ao vivo
> - `smoke_test.py` — validação local pré-commit
> - `docs/REPORTS.md` §"Status dos Dados" — especificação atual

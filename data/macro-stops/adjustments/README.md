# Ajustes Gerenciais de Stop — MACRO

Um arquivo por mês: `adjustments-YYYY-MM.json`

Preencher no início de cada mês após revisão do comitê de risco.
Se um PM não tiver ajuste, omitir a entrada (o monitor usa o stop calculado pela fórmula).

Campos por PM:
- `stop_mensal_override_bps`: substitui o stop calculado pela fórmula para este mês
- `motivo`: justificativa obrigatória
- `aprovado_por`: quem aprovou o ajuste
- `data_aprovacao`: data da decisão (YYYY-MM-DD)

Se o arquivo do mês não existir, o monitor usa os stops calculados automaticamente.

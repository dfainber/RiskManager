---
name: wrap-session
description: Ritual de fim de sessão do Risk Monitor kit. Identifica o que foi feito, atualiza memory + CLAUDE.md, roda sanity check (regen do relatório), commit + push para origin/master. Use para "wrap", "wrap up", "fechar/finalizar/encerrar sessão", "fim de sessão", "fechar o dia". NÃO usar para commits avulsos no meio da sessão.
---

# Wrap Session — Fim de dia de desenvolvimento

Esta skill formaliza o ritual de encerramento de sessão. Garante que nada do trabalho do dia se perca entre memory, documentação e repositório.

## Quando disparar
Usuário encerrando a sessão. Frases gatilho: "wrap up", "fechar", "finalizar", "encerrar", "fim de sessão", "fechar o dia", "hoje acabou", "ok guarda tudo".

## Quando NÃO disparar
- Commits pontuais no meio da sessão ("commita só esse fix") — rodar `git add/commit/push` direto
- Quando o usuário explicitamente pede só commit sem tocar em memory

## Checklist

Executar NA ORDEM:

### 1. Inspecionar estado atual
```bash
git status -s
git diff --stat
git log --oneline -10
```
Identificar:
- Arquivos modificados mas não commitados
- Commits já feitos na sessão (pelo hash + mensagem)
- Gaps entre o que o código tem e o que está documentado

### 2. Recapitular a sessão
Ler a conversa. Listar mentalmente:
- **Features entregues** (novos cards, novas queries, nova UX)
- **Bugs encontrados e corrigidos** (com root cause)
- **Regras/convenções descobertas** (ex: NAV lag, sign conventions)
- **TODOs que surgiram** (coisas pendentes, próximos passos)
- **Decisões de escopo** (o que foi deliberadamente deixado de fora)

### 3. Atualizar memory
Arquivos em `C:\Users\diego.fainberg\.claude\projects\f--Bloomberg-Quant-MODELOS-DFF-Risk-Monitor\memory\`.

**Se surgiu uma regra nova** (algo não óbvio pelo código, algo que você vai precisar lembrar em sessões futuras):
- Criar `project_rule_<slug>.md` com frontmatter + seções (O gatilho · O impacto · A regra · Why saved · How to apply)
- Adicionar linha no `MEMORY.md` apontando pro arquivo

**Se completou TODOs do backlog**:
- Editar `project_todo_session_YYYY_MM_DD.md` (o mais recente) movendo items de "Pendente" para "Fechados nesta sessão"
- Se a sessão gerou TODOs novos, adicionar em "Pendente"

**Se o estado do projeto mudou** (nova fase, nova skill, novo fundo coberto):
- Atualizar `project_kit_design.md` com o delta

### 4. Atualizar CLAUDE.md
Seção "§5 Onde estamos":
- Adicionar bullets na fase atual de "entregues"
- Mover items "fazidos" de "pendente" para "entregues"
- Atualizar a tabela de fases se aplicável
- Se aplicável, criar subseção "Fixes pós entrega" para consertos que não são features principais

Adicionar na seção §3 (Fontes de dados) se uma nova tabela/coluna virou fonte canônica.

### 5. Sanity check
Rodar o relatório com a data mais recente útil para confirmar que nada quebrou:
```bash
python generate_risk_report.py
```
Se falhar, **parar o wrap** e mostrar o erro ao usuário. Não commita código quebrado.

### 6. Stage, commit, push
```bash
git add CLAUDE.md generate_risk_report.py glpg_fetch.py <outros>
git commit -m "<mensagem descritiva multi-linha>"
git push origin master
```

**Mensagem de commit**: título curto + bullets das principais mudanças + co-author Claude. Exemplo:
```
Fase 4 wrap: Summary, PA hierárquico, ALBATROZ, PDF export, perf

Summary:
- Status consolidado grid 5 fundos
- Outliers, Top Movers, Mudanças Significativas

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

### 7. Reportar ao usuário
Resumo curto:
- Hash do(s) commit(s) feito(s) hoje
- Arquivos de memory criados/atualizados
- Items de TODO que foram fechados
- O que sobrou como pendente (de olho na próxima sessão)

## Princípios

- **Segurança primeiro**: NUNCA push se `python generate_risk_report.py` falhar. NUNCA commita secrets (checar se algo em `.env` ou `credentials` apareceu em `git status`).
- **Idempotente**: rodar de novo em seguida não deve criar entradas duplicadas. Ler memory antes de escrever.
- **Conciso**: prefira editar rules/TODOs existentes em vez de criar muitos arquivos pequenos. 1 arquivo = 1 conceito coeso.
- **Preservar contexto histórico**: itens movidos de "pendente" pra "fechados" mantém o lookup funcional se alguém pesquisar o arquivo depois.

## Critérios de sucesso
Ao fim do wrap:
- `git status` limpo (nada uncommitted, nada untracked relevante)
- `git log --oneline -1` mostra o commit de encerramento
- `git ls-remote origin master` confirma que push chegou
- MEMORY.md index reflete os novos arquivos de memory
- CLAUDE.md reflete o novo estado da Fase
- O relatório do dia renderiza sem erros

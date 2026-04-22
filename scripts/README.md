# Risk Monitor — Scheduled Task Scripts

Automação diária: gera o HTML e envia por email via Outlook COM, 03:00 todos os dias.

## Arquivos

- **`send_risk_monitor_email.ps1`** — script que efetivamente roda. Gera o relatório,
  anexa ao email e envia via Outlook. Loga em `data/email-logs/<data>.log`.
- **`install_scheduled_task.ps1`** — registra a tarefa no Windows Task Scheduler.
  Pode ser rodado várias vezes (substitui a task anterior).

## Instalação (primeira vez)

1. Garantir que o Outlook Desktop está instalado e logado com sua conta.
2. Abrir PowerShell (não precisa admin) e rodar:

   ```powershell
   cd F:\Bloomberg\Quant\MODELOS\DFF\Risk_Monitor\scripts
   powershell -ExecutionPolicy Bypass -File .\install_scheduled_task.ps1
   ```

3. Testar manualmente:

   ```powershell
   Start-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily'
   ```

   Deve aparecer a caixa de diálogo do Outlook no tray e chegar um email em ~15s.

## Operação

| Ação | Comando |
|---|---|
| Rodar agora (teste) | `Start-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily'` |
| Ver histórico | `Get-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily' \| Get-ScheduledTaskInfo` |
| Ver último log | `Get-Content F:\Bloomberg\Quant\MODELOS\DFF\Risk_Monitor\data\email-logs\*.log \| Select-Object -Last 50` |
| Desabilitar | `Disable-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily'` |
| Reabilitar | `Enable-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily'` |
| Remover | `Unregister-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily' -Confirm:$false` |

## Comportamento

- **Horário**: 03:00 BRT todo dia.
- **Data-alvo**: último dia útil (hoje − 1, pulando sábado/domingo).
  - Terça 03:00 → gera relatório de segunda.
  - Segunda 03:00 → gera de sexta.
- **Email**:
  - Sucesso: subject `Risk Monitor — YYYY-MM-DD`, corpo curto, HTML em anexo.
  - Falha: subject `Risk Monitor — FALHA YYYY-MM-DD`, corpo com exit code + últimas 80 linhas do log, log completo em anexo.

## Limitações conhecidas

1. **Sessão Windows precisa estar ativa** (logado, mesmo bloqueado). Se você deslogar,
   a task não roda até o próximo login — e não recupera o dia perdido.
2. **Outlook precisa estar acessível** — se não estiver rodando, o COM sobe ele.
   Se o perfil não carregar ou a conta não estiver logada, o envio falha.
3. **Máquina off às 3am** — a task tenta rodar quando a máquina liga (graças ao
   `StartWhenAvailable`). Se a máquina ligar muitas horas depois, o relatório
   ainda é do dia-alvo esperado (não do dia corrente).
4. **Gmail 25 MB** — o HTML tá em ~2 MB hoje, então folga grande.

## Troubleshooting

**Não recebi o email às 3am**
1. Checar o log: `Get-Content data\email-logs\<data>.log`
2. Checar estado da task: `Get-ScheduledTask ... | Get-ScheduledTaskInfo` — campos `LastRunTime`, `LastTaskResult`, `NumberOfMissedRuns`
3. `LastTaskResult = 0` → task rodou OK, problema no Outlook → ver log
4. `LastTaskResult != 0` → task falhou → abrir Task Scheduler GUI e ver o erro no History

**Outlook abre janela de confirmação pedindo permissão de envio programático**
- Isso é um safeguard antigo do Outlook. Ir em *Trust Center → Programmatic Access* e
  configurar como "Never warn me" se a máquina tem antivírus, OU clicar Allow toda vez.

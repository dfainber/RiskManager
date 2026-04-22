# ============================================================================
# install_scheduled_task.ps1
# Registra (ou substitui) a Scheduled Task "Galapagos-RiskMonitor-Daily"
# que chama send_risk_monitor_email.ps1 todo dia as 3h da manha.
#
# Uso:
#     powershell -ExecutionPolicy Bypass -File .\install_scheduled_task.ps1
#
# Desinstalar:
#     Unregister-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily' -Confirm:$false
#
# Inspecionar:
#     Get-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily' | Get-ScheduledTaskInfo
#
# Rodar manualmente (teste):
#     Start-ScheduledTask -TaskName 'Galapagos-RiskMonitor-Daily'
# ============================================================================

$TaskName = 'Galapagos-RiskMonitor-Daily'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PsScript  = Join-Path $ScriptDir 'send_risk_monitor_email.ps1'

if (-not (Test-Path $PsScript)) {
    Write-Error "Script nao encontrado: $PsScript"
    exit 1
}

# Action: roda PowerShell com o script, ocultando a janela
$Action = New-ScheduledTaskAction `
    -Execute 'powershell.exe' `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$PsScript`""

# Trigger: todo dia as 3am
$Trigger = New-ScheduledTaskTrigger -Daily -At 3am

# Settings
#  - AllowStartIfOnBatteries + DontStopIfGoingOnBatteries: laptop friendly
#  - StartWhenAvailable: se a maquina estava off as 3am, roda assim que ligar
#  - RestartCount/Interval: retry simples se travar
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 15) `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

# Principal: roda sob o usuario atual, modo Interactive
# (Interactive e NECESSARIO para o Outlook COM funcionar - SYSTEM nao da)
$Principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName   $TaskName `
    -Action     $Action `
    -Trigger    $Trigger `
    -Settings   $Settings `
    -Principal  $Principal `
    -Description 'Galapagos Risk Monitor: gera o HTML diario do ultimo dia util e envia por email via Outlook COM.' `
    -Force

Write-Host ""
Write-Host "[OK] Task '$TaskName' registrada com sucesso." -ForegroundColor Green
Write-Host "  Trigger:   diario as 03:00"
Write-Host "  Script:    $PsScript"
Write-Host "  Destino:   dfainber@gmail.com"
Write-Host ""
Write-Host "Proximos passos:"
Write-Host "  1. Testar manualmente agora:   Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "  2. Inspecionar ultima rodada:  Get-ScheduledTask -TaskName '$TaskName' | Get-ScheduledTaskInfo"
Write-Host "  3. Remover (se quiser):        Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
Write-Host ""
Write-Host "IMPORTANTE: a task so dispara quando sua sessao Windows esta ATIVA"
Write-Host "(logado, mesmo que bloqueado). Se fizer logout, nao roda."

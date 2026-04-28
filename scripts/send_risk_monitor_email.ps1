# ============================================================================
# send_risk_monitor_email.ps1
# Gera o Risk Monitor do dia e envia por email via Outlook COM.
# Chamado diariamente pela Scheduled Task "Galapagos-RiskMonitor-Daily".
#
# Comportamento:
#   - Calcula data-alvo = ultimo dia util (hoje - 1, skipping fim-de-semana)
#   - Roda python generate_risk_report.py <data>
#   - Se sucesso: email com HTML anexo, subject "Risk Monitor - YYYY-MM-DD"
#   - Se falha: email "Risk Monitor - FALHA YYYY-MM-DD" + log anexo
#   - Log completo em data/email-logs/<YYYY-MM-DD>.log
# ============================================================================

$ErrorActionPreference = "Continue"

# Resolve paths - script vive em <repo>/scripts/, repo root e o parent
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoDir   = Split-Path -Parent $ScriptDir
$Python    = "C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe"
$Script    = Join-Path $RepoDir "generate_risk_report.py"
$MailTo    = "diego.fainberg@galapagoscapital.com"
$MailBcc   = "bruno.stein@galapagoscapital.com; bruno.tuma@galapagoscapital.com; claudio.ferraz@galapagoscapital.com; danilo.franco@galapagoscapital.com; eduardo.cotrim@galapagoscapital.com; enzo.campos@galapagoscapital.com; erik.freundt@galapagoscapital.com; fabio.guarda@galapagoscapital.com; guilherme.quintero@galapagoscapital.com; guilherme.ramos@galapagoscapital.com; jorge.dib@galapagoscapital.com; luca.esposito@galapagoscapital.com; lucas.lage@galapagoscapital.com; luiz.laudari@galapagoscapital.com; marcos.colombo@galapagoscapital.com; maria.mathias@galapagoscapital.com; mariam.dayoub@galapagoscapital.com; marina.benvenuto@galapagoscapital.com; mateus.tonello@galapagoscapital.com; miguel.ishimura@galapagoscapital.com; paulo.gitz@galapagoscapital.us; pedro.alexandre@galapagoscapital.com; pedro.borges@galapagoscapital.com; rafael.vianna@galapagoscapital.com; rodrigo.fonseca@galapagoscapital.com; rodrigo.jafet@galapagoscapital.com; rodrigo.mota@galapagoscapital.com; svc_automation@galapagoscapital.com; thais.groberman@galapagoscapital.com; valentina.guida@galapagoscapital.com; vitor.batista@galapagoscapital.com"

# Data-alvo: ultimo dia util
$d = (Get-Date).AddDays(-1)
while ($d.DayOfWeek -eq 'Saturday' -or $d.DayOfWeek -eq 'Sunday') {
    $d = $d.AddDays(-1)
}
$DataStr = $d.ToString('yyyy-MM-dd')

# Log
$LogDir = Join-Path $RepoDir "data\email-logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }
$LogFile  = Join-Path $LogDir "${DataStr}.log"
$HtmlFile = Join-Path $RepoDir "data\morning-calls\${DataStr}_risk_monitor.html"

"=== Run started $(Get-Date -Format o) - target date $DataStr ===" |
    Out-File -FilePath $LogFile -Encoding utf8

# 1) Gerar o relatorio
Push-Location $RepoDir
try {
    & $Python $Script $DataStr 2>&1 | Tee-Object -FilePath $LogFile -Append
    $ExitCode = $LASTEXITCODE
} catch {
    "Exception: $_" | Out-File -FilePath $LogFile -Append
    $ExitCode = 1
}
Pop-Location

$Success = ($ExitCode -eq 0) -and (Test-Path $HtmlFile)
"=== Generation $(if ($Success) { 'OK' } else { 'FAILED' }) (exit=$ExitCode) ===" |
    Out-File -FilePath $LogFile -Append

# 2) Enviar via Outlook COM
try {
    $Outlook = New-Object -ComObject Outlook.Application
    $Mail    = $Outlook.CreateItem(0)   # 0 = olMailItem
    $Mail.To  = $MailTo
    $Mail.BCC = $MailBcc

    if ($Success) {
        $HtmlSizeKB = [math]::Round((Get-Item $HtmlFile).Length / 1KB, 0)
        $Mail.Subject = "Risk Monitor - $DataStr"
        $Mail.Body = @"
Risk Monitor diario - data base $DataStr.

Relatorio HTML em anexo ($HtmlSizeKB KB). Abra no Chrome ou Edge.

Se o Windows bloquear os scripts (navegacao por abas nao funciona):
  botao direito no arquivo > Propriedades > marque "Desbloquear" > OK.

Gerado automaticamente em $(Get-Date -Format 'yyyy-MM-dd HH:mm') via Scheduled Task.
"@
        $null = $Mail.Attachments.Add($HtmlFile)
    } else {
        $LogTail = if (Test-Path $LogFile) {
            (Get-Content $LogFile -Tail 80 -ErrorAction SilentlyContinue | Out-String)
        } else { "(log vazio)" }
        $Mail.Subject = "Risk Monitor - FALHA $DataStr"
        $Mail.Body = @"
Geracao automatica do Risk Monitor FALHOU.

Data-alvo: $DataStr
Horario:   $(Get-Date -Format 'yyyy-MM-dd HH:mm')
Exit code: $ExitCode

Ultimas linhas do log:

$LogTail

Log completo em anexo.
"@
        if (Test-Path $LogFile) { $null = $Mail.Attachments.Add($LogFile) }
    }

    $Mail.Send()
    "=== Email sent to $MailTo ===" | Out-File -FilePath $LogFile -Append
    $EmailOk = $true
} catch {
    "Outlook send failed: $_" | Out-File -FilePath $LogFile -Append
    $EmailOk = $false
}

# Exit code: 0 se tudo OK, 1 se relatorio falhou mas email foi, 2 se email tambem falhou
if (-not $EmailOk) { exit 2 }
if (-not $Success) { exit 1 }
exit 0

@echo off
REM ============================================================================
REM run_report.bat — gera o Risk Monitor HTML para uma data (prompt interativo)
REM   - Enter direto => usa data de hoje (YYYY-MM-DD)
REM   - Data em qualquer formato aceito por pd.Timestamp (ex: 2026-04-17)
REM   - Abre o HTML gerado no browser padrao ao final
REM ============================================================================
setlocal ENABLEDELAYEDEXPANSION

pushd "%~dp0"

REM Data de hoje em YYYY-MM-DD (fallback se usuario apertar Enter)
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "$d=(Get-Date).AddDays(-1);while($d.DayOfWeek -eq 'Saturday' -or $d.DayOfWeek -eq 'Sunday'){$d=$d.AddDays(-1)};$d.ToString('yyyy-MM-dd')"`) do set TODAY=%%a

echo.
echo Risk Monitor — Gerador de relatorio
echo ========================================
echo Data padrao: %TODAY%
set /p DATA_INPUT=Data (YYYY-MM-DD) [Enter = %TODAY%]:

if "!DATA_INPUT!"=="" (
    set DATA=%TODAY%
) else (
    set DATA=!DATA_INPUT!
)

echo.
echo ^> Gerando relatorio para %DATA%...
echo.

C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe generate_risk_report.py %DATA%
if errorlevel 1 (
    echo.
    echo *** ERRO ao gerar o relatorio.
    pause
    exit /b 1
)

set HTML_PATH=data\morning-calls\%DATA%_risk_monitor.html
set HTML_FULL=%~dp0%HTML_PATH%
if not exist "%HTML_FULL%" (
    echo *** Arquivo nao encontrado: %HTML_FULL%
    pause
    exit /b 1
)

echo.
echo ^> Abrindo %HTML_FULL% no browser...
REM Fallback chain: default app -> explorer -> chrome/edge hardcoded
start "" "%HTML_FULL%" || explorer.exe "%HTML_FULL%" || (
    if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" (
        start "" "%ProgramFiles%\Google\Chrome\Application\chrome.exe" "%HTML_FULL%"
    ) else if exist "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" (
        start "" "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" "%HTML_FULL%"
    )
)

popd
endlocal

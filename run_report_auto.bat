@echo off
REM ============================================================================
REM run_report_auto.bat
REM   Versao nao-interativa do run_report.bat para Task Scheduler.
REM   - Skip automatico em sabado/domingo (defesa em dupla com trigger weekly)
REM   - Data = latest business day com dado em GLPG-DB01 (cobre feriados BR)
REM   - Sem UI, sem browser; output em logs\auto_report.log
REM ============================================================================

setlocal ENABLEDELAYEDEXPANSION
pushd "%~dp0"

if not exist logs mkdir logs

REM --- Weekend guard ---
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "(Get-Date).DayOfWeek.ToString()"`) do set DOW=%%a
if /i "%DOW%"=="Saturday" (
    echo [%DATE% %TIME%] Sabado - skip >> logs\auto_report.log
    popd & endlocal & exit /b 0
)
if /i "%DOW%"=="Sunday" (
    echo [%DATE% %TIME%] Domingo - skip >> logs\auto_report.log
    popd & endlocal & exit /b 0
)

REM --- Date = latest business day com dado em DB (ANBIMA-aware) ---
for /f "usebackq tokens=*" %%a in (`C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe latest_bday.py 2^>nul`) do set DATA=%%a
if "%DATA%"=="" (
    echo [%DATE% %TIME%] latest_bday.py falhou >> logs\auto_report.log
    popd & endlocal & exit /b 1
)

echo. >> logs\auto_report.log
echo ============================================================ >> logs\auto_report.log
echo [%DATE% %TIME%] Generating report for %DATA% >> logs\auto_report.log

C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe generate_risk_report.py %DATA% >> logs\auto_report.log 2>&1

if errorlevel 1 (
    echo [%DATE% %TIME%] *** ERROR exit code %ERRORLEVEL% >> logs\auto_report.log
    popd & endlocal & exit /b 1
)

echo [%DATE% %TIME%] Done. >> logs\auto_report.log
popd
endlocal
exit /b 0

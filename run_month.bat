@echo off
REM ============================================================================
REM run_month.bat — gera Risk Monitor HTML para todos os dias uteis do mes
REM   Usa VAL_DATEs reais do DB (ANBIMA calendar, sem feriados)
REM   Uso: run_month.bat [YYYY-MM]
REM     - sem argumento: prompt interativo (Enter = default)
REM     - default = mes atual; no dia 1 do mes, default = mes anterior
REM ============================================================================
setlocal ENABLEDELAYEDEXPANSION

pushd "%~dp0"

set PYTHON=C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe

REM ── Mes: parametro, prompt interativo, ou default (no dia 1, mes anterior) ──
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "$d=Get-Date; if ($d.Day -eq 1) {$d=$d.AddMonths(-1)}; $d.ToString('yyyy-MM')"`) do set DEFAULT_MONTH=%%a

if not "%~1"=="" (
    set ARG=%~1
    goto :have_arg
)

echo.
echo Risk Monitor — Geracao em lote
echo ========================================
echo Mes padrao: %DEFAULT_MONTH%
set /p MONTH_INPUT=Mes YYYY-MM [Enter = %DEFAULT_MONTH%]:

if "%MONTH_INPUT%"=="" (
    set ARG=%DEFAULT_MONTH%
) else (
    set ARG=%MONTH_INPUT%
)

:have_arg
echo.
echo Mes: %ARG%
echo.

REM ── Busca dias uteis reais do DB ─────────────────────────────────────────────
set TMPFILE=%TEMP%\rm_bdays_%RANDOM%.txt
%PYTHON% month_bdays.py %ARG% > "%TMPFILE%" 2>nul
if errorlevel 1 (
    echo *** Falha ao buscar dias uteis do DB.
    del "%TMPFILE%" 2>nul
    pause
    exit /b 1
)

REM ── Conta linhas ─────────────────────────────────────────────────────────────
set COUNT=0
for /f %%x in ("%TMPFILE%") do set /a COUNT+=1
for /f "usebackq" %%x in ("%TMPFILE%") do set /a COUNT+=1
REM simpler count via PowerShell
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "(Get-Content '%TMPFILE%').Count"`) do set TOTAL=%%a
echo %TOTAL% dias uteis encontrados.
echo.

set OK=0
set SKIP=0
set N=0

for /f "usebackq tokens=*" %%D in ("%TMPFILE%") do (
    set DATA=%%D
    set /a N+=1
    echo [!N!/%TOTAL%] !DATA!...

    %PYTHON% generate_risk_report.py !DATA! > nul 2>&1
    if errorlevel 1 (
        echo        [SKIP] sem dados
        set /a SKIP+=1
    ) else (
        if exist "data\morning-calls\!DATA!_risk_monitor.html" (
            echo        [OK]
            set /a OK+=1
        ) else (
            echo        [SKIP] arquivo nao gerado
            set /a SKIP+=1
        )
    )
)

del "%TMPFILE%" 2>nul

echo.
echo ========================================
echo Concluido: %OK% gerados, %SKIP% ignorados
echo Arquivos em: %~dp0data\morning-calls\
echo.
pause

popd
endlocal

@echo off
REM ============================================================================
REM run_monthly_review.bat — gera o Monthly Review HTML + PPTX para um mês
REM   - Enter direto => usa o mês anterior (default seguro)
REM   - Ex: 2026-04
REM ============================================================================
setlocal ENABLEDELAYEDEXPANSION

pushd "%~dp0"

set PYTHON=C:\Users\diego.fainberg\.venvs\risk_monitor\Scripts\python.exe

for /f "usebackq tokens=*" %%a in (`%PYTHON% -c "import pandas as pd; print((pd.Timestamp('today') - pd.offsets.MonthBegin(1)).strftime('%%Y-%%m'))" 2^>nul`) do set DEFAULT_MONTH=%%a
if "%DEFAULT_MONTH%"=="" set DEFAULT_MONTH=2026-04

echo.
echo Galapagos Capital — Monthly Review
echo ============================================
echo Mes padrao: %DEFAULT_MONTH%
set /p MONTH_INPUT=Mes (YYYY-MM) [Enter = %DEFAULT_MONTH%]:

if "!MONTH_INPUT!"=="" (
    set MONTH=%DEFAULT_MONTH%
) else (
    set MONTH=!MONTH_INPUT!
)

echo.
echo ^> Gerando Monthly Review para %MONTH%...
echo.

%PYTHON% generate_monthly_review.py --month %MONTH%
if errorlevel 1 (
    echo.
    echo *** ERRO ao gerar o relatorio.
    pause
    exit /b 1
)

set HTML_PATH=data\monthly-reviews\%MONTH%_monthly_review.html
set PPTX_PATH=data\monthly-reviews\%MONTH%_monthly_review.pptx
set HTML_FULL=%~dp0%HTML_PATH%
set PPTX_FULL=%~dp0%PPTX_PATH%

if exist "%HTML_FULL%" (
    echo ^> Abrindo HTML no browser...
    start "" "%HTML_FULL%"
)
if exist "%PPTX_FULL%" (
    echo ^> Abrindo PPTX...
    start "" "%PPTX_FULL%"
)

popd
endlocal

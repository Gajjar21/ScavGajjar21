@echo off
setlocal
title GJ21 AWB Pipeline Launcher

cd /d "%~dp0"

if exist ".venv_gui\Scripts\python.exe" (
    call ".venv_gui\Scripts\activate.bat"
) else if exist ".venv\Scripts\python.exe" (
    call ".venv\Scripts\activate.bat"
) else (
    py -3.11 -m venv .venv
    if errorlevel 1 (
        python -m venv .venv
    )
    call ".venv\Scripts\activate.bat"
)

python -m pip install --upgrade pip >nul 2>&1
python -m pip install -r requirements.txt >nul 2>&1
python -m V3.launcher

endlocal

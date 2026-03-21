@echo off
setlocal enabledelayedexpansion
title AWB Pipeline - Windows Installer
color 0A

echo.
echo ======================================================
echo          AWB Pipeline -- Windows Installer
echo ======================================================
echo.

:: Move to the folder where this .bat lives (the project root)
cd /d "%~dp0"

:: ── 1. Python ──────────────────────────────────────────────
echo [1/6] Checking Python 3.11+...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo   [X] Python not found.
    echo   Opening Python download page...
    start https://www.python.org/downloads/
    echo.
    echo   Install Python 3.11+, tick "Add Python to PATH",
    echo   then double-click this installer again.
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo   [OK] Python %PYVER% found

:: Check version is 3.11+
python -c "import sys; exit(0 if (sys.version_info>=(3,11)) else 1)" >nul 2>&1
if %errorlevel% neq 0 (
    echo   [!] Python %PYVER% is below 3.11. Please install Python 3.11+.
    start https://www.python.org/downloads/
    pause
    exit /b 1
)

:: ── 2. Virtual environment ─────────────────────────────────
echo.
echo [2/6] Setting up virtual environment...
if not exist ".venv" (
    python -m venv .venv
    echo   [OK] Virtual environment created
) else (
    echo   [OK] Virtual environment already exists
)
call .venv\Scripts\activate.bat

:: ── 3. Dependencies ────────────────────────────────────────
echo.
echo [3/6] Installing Python dependencies...
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
if %errorlevel% neq 0 (
    echo   [X] Dependency install failed. Check requirements.txt and internet connection.
    pause
    exit /b 1
)
echo   [OK] All dependencies installed

:: ── 4. Tesseract OCR ───────────────────────────────────────
echo.
echo [4/6] Checking Tesseract OCR...
set TESS_PATH=

:: Check if already on PATH
tesseract --version >nul 2>&1
if %errorlevel% equ 0 (
    for /f "delims=" %%p in ('where tesseract 2^>nul') do (
        set TESS_PATH=%%p
        goto :tess_found
    )
)

:: Check common install locations
if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" (
    set TESS_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe
    goto :tess_found
)
if exist "C:\Program Files (x86)\Tesseract-OCR\tesseract.exe" (
    set TESS_PATH=C:\Program Files (x86)\Tesseract-OCR\tesseract.exe
    goto :tess_found
)

:: Not found — download and install
echo   [!] Tesseract not found. Downloading installer...
set TESS_INSTALLER=%TEMP%\tesseract-installer.exe
curl -L --progress-bar -o "%TESS_INSTALLER%" "https://digi.bib.uni-mannheim.de/tesseract/tesseract-ocr-w64-setup-5.3.3.20231005.exe"
if %errorlevel% neq 0 (
    echo   [X] Download failed. Install Tesseract manually:
    echo       https://github.com/UB-Mannheim/tesseract/wiki
    start https://github.com/UB-Mannheim/tesseract/wiki
    echo   Then re-run this installer.
    pause
    exit /b 1
)
echo   Running Tesseract installer (follow the prompts, use default install path)...
"%TESS_INSTALLER%"
set TESS_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe

:tess_found
echo   [OK] Tesseract: !TESS_PATH!

:: ── 5. Configure .env ──────────────────────────────────────
echo.
echo [5/6] Configuring .env...

if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
    ) else (
        type nul > ".env"
    )
)

:: Use Python to safely set keys in .env (handles paths with backslashes)
python -c "
import re, sys

env_path = '.env'
base_dir = sys.argv[1]
tess_path = sys.argv[2]

try:
    content = open(env_path, encoding='utf-8').read()
except FileNotFoundError:
    content = ''

def set_var(text, key, val):
    pattern = rf'^{re.escape(key)}=.*$'
    line = f'{key}={val}'
    if re.search(pattern, text, re.MULTILINE):
        return re.sub(pattern, line, text, flags=re.MULTILINE)
    return text.rstrip() + '\n' + line + '\n'

content = set_var(content, 'PIPELINE_BASE_DIR', base_dir)
content = set_var(content, 'TESSERACT_PATH', tess_path)
open(env_path, 'w', encoding='utf-8').write(content)
print('  [OK] .env configured (PIPELINE_BASE_DIR + TESSERACT_PATH set)')
" "%CD%" "!TESS_PATH!"

:: ── 6. Desktop launcher ────────────────────────────────────
echo.
echo [6/6] Creating desktop launcher...
set LAUNCHER=%USERPROFILE%\Desktop\AWB Pipeline.bat

(
    echo @echo off
    echo title AWB Pipeline
    echo cd /d "%CD%"
    echo call .venv\Scripts\activate.bat
    echo python -m V3.app
) > "%LAUNCHER%"

echo   [OK] Desktop launcher: %LAUNCHER%

:: ── Verify setup ───────────────────────────────────────────
echo.
echo ------------------------------------------------------
echo   Verifying configuration...
echo ------------------------------------------------------
python -m V3.config
set CONFIG_OK=%errorlevel%

echo.
echo ======================================================
if %CONFIG_OK% equ 0 (
    echo   Installation complete!
    echo.
    echo   Next step:
    echo     Open .env and paste your FedEx EDM token
    echo     into the EDM_TOKEN line.
    echo.
    echo   Then double-click 'AWB Pipeline' on your Desktop
    echo   to launch the pipeline.
) else (
    echo   Installed -- but config check reported warnings.
    echo   Review the output above, fix .env if needed,
    echo   and re-run this installer.
)
echo ======================================================
echo.
pause

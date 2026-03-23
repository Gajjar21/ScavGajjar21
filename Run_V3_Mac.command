#!/bin/bash
# AWB Pipeline V3 - macOS launcher that bypasses pyenv shims.
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

PYTHON_BIN=""
if [ -x "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3" ]; then
  PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
elif [ -x "/Library/Frameworks/Python.framework/Versions/3.11/bin/python3" ]; then
  PYTHON_BIN="/Library/Frameworks/Python.framework/Versions/3.11/bin/python3"
else
  PYTHON_BIN="python3"
fi

VENV_DIR="$PROJECT_DIR/.venv_gui"
if [ ! -x "$VENV_DIR/bin/python" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip >/dev/null
python -m pip install -r "$PROJECT_DIR/requirements.txt" >/dev/null

export TK_SILENCE_DEPRECATION=1
python -m V3.app

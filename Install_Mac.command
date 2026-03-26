#!/bin/bash
# =============================================================
# AWB Pipeline — Mac Installer
# Double-click this file in Finder to install.
# First time only: right-click → Open if macOS blocks it.
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}⚠${NC}  $1"; }
err()  { echo -e "  ${RED}✗${NC} $1"; }
step() { echo -e "\n${BOLD}${BLUE}[$1]${NC} $2"; }

echo ""
echo "======================================================"
echo "         AWB Pipeline — Mac Installer"
echo "======================================================"

# ── 1. Python ─────────────────────────────────────────────
step "1/6" "Checking Python 3.11+..."
PYTHON=""
for cmd in python3.13 python3.12 python3.11 python3; do
    if command -v "$cmd" &>/dev/null; then
        MAJOR=$("$cmd" -c "import sys; print(sys.version_info.major)")
        MINOR=$("$cmd" -c "import sys; print(sys.version_info.minor)")
        if [ "$MAJOR" -ge 3 ] && [ "$MINOR" -ge 11 ]; then
            PYTHON="$cmd"
            ok "Found $("$cmd" --version)"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    err "Python 3.11+ not found."
    warn "Opening Python download page..."
    open "https://www.python.org/downloads/"
    echo ""
    echo "  Install Python 3.11+, then double-click this installer again."
    echo ""
    read -n 1 -s -r -p "Press any key to close..."
    exit 1
fi

# ── 2. Virtual environment ────────────────────────────────
step "2/6" "Setting up virtual environment..."
if [ ! -d ".venv" ]; then
    "$PYTHON" -m venv .venv
    ok "Virtual environment created"
else
    ok "Virtual environment already exists"
fi
source .venv/bin/activate

# ── 3. Dependencies ───────────────────────────────────────
step "3/6" "Installing Python dependencies..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
ok "All dependencies installed"

# ── 4. Tesseract OCR ──────────────────────────────────────
step "4/6" "Checking Tesseract OCR..."
TESS_PATH=""

# Common install locations
for candidate in \
    "$(which tesseract 2>/dev/null)" \
    "/usr/local/bin/tesseract" \
    "/opt/homebrew/bin/tesseract"; do
    if [ -x "$candidate" ]; then
        TESS_PATH="$candidate"
        break
    fi
done

if [ -n "$TESS_PATH" ]; then
    ok "Tesseract found: $TESS_PATH"
else
    warn "Tesseract not found. Installing via Homebrew..."
    if ! command -v brew &>/dev/null; then
        warn "Homebrew not found. Installing Homebrew first..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        # Add brew to PATH for Apple Silicon
        eval "$(/opt/homebrew/bin/brew shellenv 2>/dev/null || true)"
    fi
    brew install tesseract
    TESS_PATH="$(which tesseract)"
    ok "Tesseract installed: $TESS_PATH"
fi

# ── 5. Configure .env ─────────────────────────────────────
step "5/6" "Configuring .env..."

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    cp ".env.example" ".env"
fi

# Create .env if missing entirely
if [ ! -f ".env" ]; then
    touch ".env"
fi

# Helper: set or add a key in .env
set_env_var() {
    local KEY="$1"
    local VAL="$2"
    if grep -q "^${KEY}=" .env; then
        sed -i '' "s|^${KEY}=.*|${KEY}=${VAL}|" .env
    else
        echo "${KEY}=${VAL}" >> .env
    fi
}

set_env_var "PIPELINE_BASE_DIR" "$SCRIPT_DIR"
set_env_var "TESSERACT_PATH"    "$TESS_PATH"
ok ".env configured (PIPELINE_BASE_DIR + TESSERACT_PATH set)"

# ── 6. Desktop launcher ───────────────────────────────────
step "6/6" "Creating desktop launcher..."
LAUNCHER="$HOME/Desktop/AWB Pipeline.command"
cat > "$LAUNCHER" << LAUNCHER_EOF
#!/bin/bash
cd "$SCRIPT_DIR"
source .venv/bin/activate
python -m V3.app
LAUNCHER_EOF
chmod +x "$LAUNCHER"
ok "Desktop launcher created: ~/Desktop/AWB Pipeline.command"

# ── Verify setup ──────────────────────────────────────────
echo ""
echo "------------------------------------------------------"
echo "  Verifying configuration..."
echo "------------------------------------------------------"
set +e
python -m V3.config
CONFIG_OK=$?
set -e

echo ""
echo "======================================================"
if [ $CONFIG_OK -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}Installation complete!${NC}"
    echo ""
    echo "  Next step:"
    echo "    → Open .env and paste your FedEx EDM token"
    echo "      into the EDM_TOKEN line."
    echo ""
    echo "  Then double-click  'AWB Pipeline'  on your Desktop"
    echo "  to launch the pipeline."
else
    echo -e "  ${YELLOW}${BOLD}Installed — but config check reported warnings above.${NC}"
    echo "  Review the output, fix .env if needed, and re-run."
fi
echo "======================================================"
echo ""
read -n 1 -s -r -p "Press any key to close..."
echo ""

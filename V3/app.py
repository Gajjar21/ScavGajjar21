# V3/app.py
# Entry point for AWB Pipeline V3 UI.
#
# Usage:
#   python -m V3.app

import os
import sys
from pathlib import Path

if sys.platform == "darwin":
    # Suppress macOS system-Tk deprecation warning noise.
    os.environ.setdefault("TK_SILENCE_DEPRECATION", "1")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from V3.ui.app_window import App


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()

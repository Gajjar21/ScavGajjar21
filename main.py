"""Legacy compatibility entrypoint.

AWB Pipeline now runs from the V3 package. This file is intentionally kept so
older launchers that still call `python main.py` continue to work.
"""

from V3.app import main as v3_main


def main() -> None:
    print("[DEPRECATED] main.py is now a compatibility launcher for V3.")
    print("[DEPRECATED] Use: python -m V3.app")
    v3_main()


if __name__ == "__main__":
    main()

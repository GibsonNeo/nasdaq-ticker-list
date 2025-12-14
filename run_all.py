#!/usr/bin/env python3
"""Run the three scripts in nasdaq-ticker-list sequentially.

This script runs the scripts using the same Python interpreter so virtualenvs
and installed packages are respected.
"""
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PY = sys.executable
# Schwab data already has clean names and sector info, so just run clean
SCRIPTS = ["clean_tickers.py"]


def run_script(script: str) -> None:
    path = SCRIPT_DIR / script
    if not path.exists():
        raise SystemExit(f"Missing script: {path}")
    print(f"--- Running {script} ---")
    subprocess.run([PY, str(path)], check=True)


def main() -> None:
    for s in SCRIPTS:
        run_script(s)
    # clean_tickers.py now outputs directly to final-tickers.csv
    final = SCRIPT_DIR / "final-tickers.csv"

    if final.exists():
        print(f"Final output: {final}")
    else:
        print(f"Warning: expected final file not found: {final}")

    print("All scripts finished successfully.")


if __name__ == "__main__":
    main()

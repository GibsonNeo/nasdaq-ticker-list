#!/usr/bin/env python3
"""Run the three scripts in nasdaq-ticker-list sequentially.

This script runs the scripts using the same Python interpreter so virtualenvs
and installed packages are respected.
"""
import subprocess
import sys
import shutil
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PY = sys.executable
# run clean -> enrich -> shorten (shorten expects enriched file)
SCRIPTS = ["clean_tickers.py", "enrich_sectors.py", "shorten_name.py"]


def run_script(script: str) -> None:
    path = SCRIPT_DIR / script
    if not path.exists():
        raise SystemExit(f"Missing script: {path}")
    print(f"--- Running {script} ---")
    subprocess.run([PY, str(path)], check=True)


def main() -> None:
    for s in SCRIPTS:
        run_script(s)
    # After the pipeline, the last script writes
    # `clean_tickers_enriched_shortnames.csv` by default. Copy it to
    # `final-tickers.csv` so there's a consistent final filename.
    last = SCRIPT_DIR / "clean_tickers_enriched_shortnames.csv"
    final = SCRIPT_DIR / "final-tickers.csv"

    if last.exists():
        shutil.copy2(last, final)
        print(f"Final output copied to: {final}")
    else:
        print(f"Warning: expected final file not found: {last}")

    print("All scripts finished successfully.")


if __name__ == "__main__":
    main()

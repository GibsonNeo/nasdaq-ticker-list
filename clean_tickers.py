import pandas as pd
import re

INPUT_FILES = [
    "Schwab-Tickers-ComServices-Financials.csv",
    "Schwab-Tickers-Health-Materials.csv",
    "Schwab-Tickers-RealEstate-Utilities.csv",
]
OUTPUT_FILE = "final-tickers.csv"

MIN_DOLLAR_VOLUME = 1_000_000  # Price times Average Volume must be at least this

def parse_money(x) -> float:
    """
    Converts strings like "$2,055.52", " 37.21 ", None to float.
    Returns 0.0 if it cannot parse.
    """
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace("$", "").replace(",", "")
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    try:
        return float(s)
    except ValueError:
        return 0.0

def parse_int_like(x) -> float:
    """
    Converts strings like "8470801", "8,470,801", None to float.
    Returns 0.0 if it cannot parse.
    """
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace(",", "")
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    try:
        return float(s)
    except ValueError:
        return 0.0

# Load and concatenate all input CSVs
dfs = [pd.read_csv(f, dtype=str) for f in INPUT_FILES]
df = pd.concat(dfs, ignore_index=True)

original_count = len(df)

# -------------------------
# Phase 1: Remove low liquidity by dollar volume
# DollarVolume = Price times Average Volume (10 Day), must be at least 1 million
# -------------------------
required_cols = ["Price", "Average Volume (10 Day)"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise SystemExit(f"Missing required columns: {missing}. Available columns: {list(df.columns)}")

df["_price_num"] = df["Price"].apply(parse_money)
df["_volume_num"] = df["Average Volume (10 Day)"].apply(parse_int_like)

df["_dollar_volume"] = df["_price_num"] * df["_volume_num"]

df = df[df["_dollar_volume"] >= MIN_DOLLAR_VOLUME]

# -------------------------
# Cleanup and write output
# -------------------------
df = df.drop(columns=[
    "_price_num",
    "_volume_num",
    "_dollar_volume",
])

df.to_csv(OUTPUT_FILE, index=False)

print(f"Original rows: {original_count}")
print(f"Cleaned rows:  {len(df)}")
print(f"Output written to: {OUTPUT_FILE}")
import pandas as pd
import re

INPUT_FILE = "tickers.csv"
OUTPUT_FILE = "tickers_clean.csv"

MIN_DOLLAR_VOLUME = 1_000_000  # Last Sale times Volume must be at least this

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

# Load CSV
df = pd.read_csv(INPUT_FILE, dtype=str)

original_count = len(df)

# Normalize Name for matching
df["name_norm"] = df["Name"].fillna("").astype(str).str.lower()

# -------------------------
# Phase 1: Remove non equity instruments
# -------------------------
exclude_keywords = [
    "depositary",
    "adr",
    "preferred",
    "unit",
    "warrant",
    "right",
    "notes",
    "trust",
    "etf",
    "fund",
]

exclude_pattern = "|".join(exclude_keywords)
df = df[~df["name_norm"].str.contains(exclude_pattern, regex=True, na=False)]

# -------------------------
# Phase 2: Keep only common equity
# -------------------------
include_pattern = "common stock|common shares|ordinary shares"
df = df[df["name_norm"].str.contains(include_pattern, regex=True, na=False)]

# -------------------------
# Phase 3: Deduplicate share classes
# -------------------------
def normalize_company(name: str) -> str:
    name = re.sub(r"class\s+[a-z]", "", name)
    name = name.replace("common stock", "")
    name = name.replace("common shares", "")
    name = name.replace("ordinary shares", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip()

df["company_key"] = df["name_norm"].apply(normalize_company)

def class_rank(name: str) -> int:
    if "class a" in name:
        return 0
    if "class b" in name:
        return 1
    return 2

df["class_rank"] = df["name_norm"].apply(class_rank)

df = (
    df.sort_values("class_rank")
      .drop_duplicates(subset="company_key", keep="first")
)

# -------------------------
# Phase 4: Remove low liquidity by dollar volume
# DollarVolume = Last Sale times Volume, must be at least 1 million
# -------------------------
required_cols = ["Last Sale", "Volume"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise SystemExit(f"Missing required columns: {missing}. Available columns: {list(df.columns)}")

df["_last_sale_num"] = df["Last Sale"].apply(parse_money)
df["_volume_num"] = df["Volume"].apply(parse_int_like)

df["_dollar_volume"] = df["_last_sale_num"] * df["_volume_num"]

df = df[df["_dollar_volume"] >= MIN_DOLLAR_VOLUME]

# -------------------------
# Cleanup and write output
# -------------------------
df = df.drop(columns=[
    "name_norm",
    "company_key",
    "class_rank",
    "_last_sale_num",
    "_volume_num",
    "_dollar_volume",
])

df.to_csv(OUTPUT_FILE, index=False)

print(f"Original rows: {original_count}")
print(f"Cleaned rows:  {len(df)}")
print(f"Output written to: {OUTPUT_FILE}")
import pandas as pd
import re

INPUT_FILE = "tickers.csv"
OUTPUT_FILE = "tickers_clean.csv"

# Load CSV
df = pd.read_csv(INPUT_FILE)

# Normalize Name for matching
df["name_norm"] = df["Name"].str.lower()

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
    "fund"
]

exclude_pattern = "|".join(exclude_keywords)

df = df[~df["name_norm"].str.contains(exclude_pattern, regex=True)]

# -------------------------
# Phase 2: Keep only common equity
# -------------------------
include_pattern = "common stock|common shares|ordinary shares"

df = df[df["name_norm"].str.contains(include_pattern, regex=True)]

# -------------------------
# Phase 3: Deduplicate share classes
# -------------------------
def normalize_company(name):
    name = re.sub(r"class\s+[a-z]", "", name)
    name = name.replace("common stock", "")
    name = name.replace("common shares", "")
    name = name.replace("ordinary shares", "")
    name = re.sub(r"\s+", " ", name)
    return name.strip()

df["company_key"] = df["name_norm"].apply(normalize_company)

def class_rank(name):
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
# Cleanup and write output
# -------------------------
df = df.drop(columns=["name_norm", "company_key", "class_rank"])

df.to_csv(OUTPUT_FILE, index=False)

print(f"Original rows: {len(pd.read_csv(INPUT_FILE))}")
print(f"Cleaned rows:  {len(df)}")
print(f"Output written to: {OUTPUT_FILE}")
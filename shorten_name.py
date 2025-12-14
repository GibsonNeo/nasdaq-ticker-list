# =========================
# CONFIG
# =========================

INPUT_CSV = "clean_tickers_enriched.csv"
OUTPUT_CSV = "clean_tickers_enriched_shortnames.csv"

# If NAME_COLUMN is None, the script will try to auto detect a name column.
NAME_COLUMN = None  # examples: "Name", "Security Name", "Company Name"

# Where to write the shortened name
OUTPUT_COLUMN = "ShortName"

# If True, replace the original name column instead of creating OUTPUT_COLUMN
OVERWRITE_NAME_COLUMN = False

# =========================
# CODE
# =========================

from pathlib import Path
import re
import pandas as pd


def read_csv_flex(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=True, na_values=["", "NA", "NaN"])


def norm(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip()


def pick_name_column(df: pd.DataFrame, preferred: str | None) -> str:
    if preferred and preferred in df.columns:
        return preferred

    candidates = [
        "Security Name",
        "SecurityName",
        "Name",
        "Company Name",
        "Company",
        "Issuer",
        "Description",
        "description",
    ]
    for c in candidates:
        if c in df.columns:
            return c

    raise SystemExit(
        "Could not find a name column. Set NAME_COLUMN at the top to the correct CSV column name."
    )


_PAREN_NOISE_EXACT = {
    "the",
    "de",
    "new",
    "holding company",
    "holdings",
    "ireland",
    "canada",
    "hc",
    "nv",
    "tx",
    "fl",
    "nj",
    "pa",
    "ca",
    "uk",
    "marshall islands",
}


_TRAILING_PATTERNS = [
    r"\bcommon stock\b",
    r"\bnew common stock\b",
    r"\bcommon shares\b",
    r"\bordinary shares\b",
    r"\bvoting common stock\b",
    r"\bclass\s+[a-z]\b",
    r"\bclass\s+[a-z]\s+common stock\b",
    r"\bclass\s+[a-z]\s+ordinary shares\b",
    r"\bseries\s+[a-z]\b",
    r"\bseries\s+[a-z]\s+common stock\b",
    r"\bnonvoting\b",
    r"\bnon voting\b",
    r"\bdepositary shares\b",
    r"\badr\b",
    r"\bads\b",
    r"\breit\b",
    r"\bbeneficial interest\b",
    r"\bpar value\b.*$",
    r"\b\$[0-9.]+\s+par value\b.*$",
    r"\bwhen issued\b.*$",
    r"\bunit\b.*$",
    r"\bwarrants?\b.*$",
    r"\bordinary shares\s*\(.*\)\b",
]


def strip_trailing_security_descriptors(name: str) -> str:
    s = norm(name)

    # Normalize whitespace and weird quotes
    s = s.replace("\u00a0", " ")
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    s = re.sub(r"\s+", " ", s).strip()

    # Remove duplicated company name like "FormFactor Inc. FormFactor Inc. Common Stock"
    # If the first half repeats exactly, collapse it.
    parts = s.split(" ")
    if len(parts) > 6:
        half = len(parts) // 2
        left = " ".join(parts[:half]).strip()
        right = " ".join(parts[half:]).strip()
        if left and right.lower().startswith(left.lower()):
            s = right

    # Remove common noise in parentheses anywhere, but keep meaningful ones like (MindMed)
    def paren_repl(m):
        inner = m.group(1).strip()
        inner_norm = inner.lower()
        inner_norm = re.sub(r"\s+", " ", inner_norm).strip()
        inner_norm = inner_norm.strip(".,")
        if inner_norm in _PAREN_NOISE_EXACT:
            return ""
        if re.fullmatch(r"[a-z]{2}", inner_norm):
            return ""
        if re.fullmatch(r"[a-z]{2,3}", inner_norm) and inner_norm in _PAREN_NOISE_EXACT:
            return ""
        return f"({inner})"

    s = re.sub(r"\(([^)]*)\)", paren_repl, s)
    s = re.sub(r"\s+", " ", s).strip()

    # Remove trailing descriptors, repeatedly, since some names have multiple phrases
    changed = True
    while changed:
        before = s

        # Remove trailing commas and punctuation
        s = s.rstrip(" ,.;:")

        # Remove any trailing descriptor patterns
        for pat in _TRAILING_PATTERNS:
            s = re.sub(rf"(?:,?\s+){pat}\s*$", "", s, flags=re.IGNORECASE)

        # If we ended with leftover empty parentheses, drop them
        s = re.sub(r"\s*\(\s*\)\s*$", "", s).strip()
        s = re.sub(r"\s+", " ", s).strip()

        changed = s != before

    # Final cleanup for stray punctuation
    s = s.strip(" ,.;:")
    s = re.sub(r"\s+", " ", s).strip()

    return s


def main():
    in_path = Path(INPUT_CSV)
    out_path = Path(OUTPUT_CSV)

    df = read_csv_flex(in_path)

    name_col = pick_name_column(df, NAME_COLUMN)
    if name_col not in df.columns:
        raise SystemExit(f"Name column not found: {name_col}")

    short = df[name_col].apply(strip_trailing_security_descriptors)

    if OVERWRITE_NAME_COLUMN:
        df[name_col] = short
    else:
        df[OUTPUT_COLUMN] = short

    df.to_csv(out_path, index=False)
    print(f"Input: {in_path}")
    print(f"Name column: {name_col}")
    print(f"Wrote: {out_path}")
    print("Sample:")
    for i in range(min(10, len(df))):
        original = norm(df.loc[i, name_col])
        cleaned = norm(short.iloc[i])
        print(f"  {original}  ->  {cleaned}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

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

# If True, also remove anything after common entity suffixes like Inc, Corporation, Company, Co, etc
# Example: "Foo Inc. Class A Common Stock" -> "Foo Inc"
# Example: "Foo Inc. (DE) Common Stock" -> "Foo Inc"
REMOVE_AFTER_ENTITY_SUFFIX = True

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
    "de",
    "delaware",
    "usa",
    "u.s.",
    "us",
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
    r"\bclass\s+[0-9]+\b",
    r"\bclass\s+[0-9]+\s+common stock\b",
    r"\bclass\s+[0-9]+\s+ordinary shares\b",
    r"\bseries\s+[a-z]\b",
    r"\bseries\s+[a-z]\s+common stock\b",
    r"\bseries\s+[0-9]+\b",
    r"\bseries\s+[0-9]+\s+common stock\b",
    r"\bnonvoting\b",
    r"\bnon voting\b",
    r"\bdepositary shares\b",
    r"\bamerican depositary shares\b",
    r"\bdepositary shares\b",
    r"\badr\b",
    r"\bads\b",
    r"\breit\b",
    r"\bbeneficial interest\b",
    r"\bsub\.?\s*vot\.?\b",
    r"\bsub\.?\s*voting\b",
    r"\bpar value\b.*$",
    r"\b\$[0-9.]+\s+par value\b.*$",
    r"\bwhen issued\b.*$",
    r"\bunit\b.*$",
    r"\bunits\b.*$",
    r"\bwarrants?\b.*$",
    r"\bright(s)?\b.*$",
    r"\bnotes?\b.*$",
    r"\bsenior notes?\b.*$",
    r"\bconvertible notes?\b.*$",
    r"\bordinary shares\s*\(.*\)\b",
]


_ENTITY_SUFFIXES = [
    "inc",
    "inc.",
    "incorporated",
    "corp",
    "corp.",
    "corporation",
    "co",
    "co.",
    "company",
    "ltd",
    "ltd.",
    "limited",
    "llc",
    "plc",
    "lp",
    "l.p.",
    "sa",
    "s.a.",
    "ag",
    "se",
    "nv",
    "n.v.",
    "bv",
    "b.v.",
    "gmbh",
    "group",
    "holdings",
    "holding",
]


_ENTITY_SUFFIX_RE = re.compile(
    r"^\s*(?P<base>.*?)\b(?P<suffix>("
    + "|".join(re.escape(x) for x in _ENTITY_SUFFIXES)
    + r"))\b(?P<tail>\s+.+)?\s*$",
    re.IGNORECASE,
)


def trim_after_entity_suffix(name: str) -> str:
    s = norm(name)
    m = _ENTITY_SUFFIX_RE.match(s)
    if not m:
        return s

    tail = (m.group("tail") or "").strip()
    if not tail:
        return s

    base = (m.group("base") or "").strip()
    suffix = (m.group("suffix") or "").strip()
    kept = f"{base} {suffix}".strip()
    kept = re.sub(r"\s+", " ", kept).strip(" ,.;:")
    return kept


def strip_trailing_security_descriptors(name: str) -> str:
    s = norm(name)

    # Normalize whitespace and weird quotes
    s = s.replace("\u00a0", " ")
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    s = re.sub(r"\s+", " ", s).strip()

    # Remove duplicated company name like "FormFactor Inc. FormFactor Inc. Common Stock"
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
        inner_norm = re.sub(r"\s+", " ", inner_norm).strip().strip(".,")
        if inner_norm in _PAREN_NOISE_EXACT:
            return ""
        if re.fullmatch(r"[a-z]{2}", inner_norm):
            return ""
        return f"({inner})"

    s = re.sub(r"\(([^)]*)\)", paren_repl, s)
    s = re.sub(r"\s+", " ", s).strip()

    # Remove trailing descriptors, repeatedly, since some names have multiple phrases
    changed = True
    while changed:
        before = s

        s = s.rstrip(" ,.;:")

        for pat in _TRAILING_PATTERNS:
            s = re.sub(rf"(?:,?\s+){pat}\s*$", "", s, flags=re.IGNORECASE)

        s = re.sub(r"\s*\(\s*\)\s*$", "", s).strip()
        s = re.sub(r"\s+", " ", s).strip()

        changed = s != before

    # Optional final trim after entity suffix
    if REMOVE_AFTER_ENTITY_SUFFIX:
        s2 = trim_after_entity_suffix(s)
        if s2:
            s = s2

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

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
OVERWRITE_NAME_COLUMN = True

# Phase 2 option, truncate anything after a legal suffix location, example "Inc of Louisiana" becomes "Inc"
TRUNCATE_AFTER_LEGAL_SUFFIX = True

# Phase 3 option, remove trailing legal suffix tokens, example "Acme Inc" becomes "Acme"
REMOVE_TRAILING_LEGAL_SUFFIX_TOKENS = True

# Words you want to keep even if they look like suffixes
KEEP_SUFFIX_WORDS = {
    "company",
    "group",
    "holdings",
}

# If True, drop parentheses that look like short all caps codes, example (CDA)
DROP_SHORT_ALLCAPS_PARENS = True
DROP_SHORT_ALLCAPS_PARENS_MAXLEN = 4

# Parentheses to keep even if they are short all caps
KEEP_PAREN_TICKERS = {
    "mindmed",
}

# Final phase, enforce max length
MAX_NAME_LEN = 50  # includes spaces
ENFORCE_MAX_NAME_LEN = True

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
    "s.c",
    "s.c.",
    "sc",
}

_TRAILING_PATTERNS = [
    r"\bcommon stock\b",
    r"\bnew common stock\b",
    r"\bcommon shares\b",
    r"\bcommon shares?\s+of\b.*$",
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
    r"\bbeneficial ownership\b",
    r"\bshares?\s+of\s+beneficial\s+ownership\b",
    r"\bpar value\b.*$",
    r"\b\$[0-9.]+\s+par value\b.*$",
    r"\bwhen issued\b.*$",
    r"\bunit\b.*$",
    r"\bwarrants?\b.*$",
    r"\bordinary shares\s*\(.*\)\b",
    r"\$\s*\d+(?:\.\d+)?\s*$",
    r"\$\d+(?:\.\d+)?\s*$",
]

_LEGAL_SUFFIX_REGEX = re.compile(
    r"""
    \b(
        incorporated|inc\.?|corp\.?|corporation|
        company|co\.?|
        ltd\.?|limited|
        plc|
        llc|l\.l\.c\.|
        lp|l\.p\.|
        llp|l\.l\.p\.|
        ag|
        nv|n\.v\.|
        sa|s\.a\.|
        se|
        spa|s\.p\.a\.|
        bancorp
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Tokens we remove only at the end of the final cleaned name
_TRAILING_SUFFIX_TOKENS = {
    "inc",
    "inc.",
    "corp",
    "corp.",
    "corporation",
    "co",
    "co.",
    "incorporated",
    "ltd",
    "ltd.",
    "limited",
    "llc",
    "l.l.c.",
    "plc",
    "lp",
    "l.p.",
    "llp",
    "l.l.p.",
    "ag",
    "nv",
    "n.v.",
    "sa",
    "s.a.",
    "se",
    "spa",
    "s.p.a.",
}


def _normalize_whitespace(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_trailing_security_descriptors(name: str) -> str:
    s = norm(name)
    s = _normalize_whitespace(s)

    # Remove duplicated company name like "X Inc. X Inc. Common Stock"
    parts = s.split(" ")
    if len(parts) > 6:
        half = len(parts) // 2
        left = " ".join(parts[:half]).strip()
        right = " ".join(parts[half:]).strip()
        if left and right.lower().startswith(left.lower()):
            s = right

    def paren_repl(m):
        inner = m.group(1).strip()
        inner_norm = re.sub(r"\s+", " ", inner).strip().strip(".,")
        inner_l = inner_norm.lower()

        if inner_l in _PAREN_NOISE_EXACT:
            return ""

        if DROP_SHORT_ALLCAPS_PARENS:
            if re.fullmatch(r"[A-Z]{2,%d}" % DROP_SHORT_ALLCAPS_PARENS_MAXLEN, inner_norm):
                if inner_l not in KEEP_PAREN_TICKERS:
                    return ""

        if re.fullmatch(r"[a-z]{2}", inner_l):
            return ""

        return f"({inner})"

    s = re.sub(r"\(([^)]*)\)", paren_repl, s)
    s = _normalize_whitespace(s)

    changed = True
    while changed:
        before = s

        s = s.rstrip(" ,.;:")

        for pat in _TRAILING_PATTERNS:
            s = re.sub(rf"(?:,?\s+){pat}\s*$", "", s, flags=re.IGNORECASE)

        s = re.sub(r"\s*\(\s*\)\s*$", "", s).strip()
        s = _normalize_whitespace(s)

        changed = s != before

    s = s.strip(" ,.;:")
    s = _normalize_whitespace(s)
    return s


def truncate_after_legal_suffix(name: str) -> str:
    s = norm(name)
    s = _normalize_whitespace(s)

    m = _LEGAL_SUFFIX_REGEX.search(s)
    if not m:
        return s

    cut = m.end()
    out = s[:cut].rstrip(" ,.;:")
    out = _normalize_whitespace(out)
    return out


def remove_trailing_legal_suffix_tokens(name: str) -> str:
    s = norm(name)
    s = _normalize_whitespace(s)
    if not s:
        return s

    keep = {w.lower() for w in KEEP_SUFFIX_WORDS}

    changed = True
    while changed and s:
        before = s

        s = s.rstrip(" ,.;:")
        tokens = s.split(" ")
        if not tokens:
            break

        last = tokens[-1].strip(" ,.;:").lower()

        if last in keep:
            break

        if last in _TRAILING_SUFFIX_TOKENS:
            tokens = tokens[:-1]
            s = " ".join(tokens).strip()
            s = _normalize_whitespace(s)

        changed = s != before

    return s


def enforce_max_len(name: str, max_len: int) -> str:
    s = norm(name)
    s = _normalize_whitespace(s)

    if len(s) <= max_len:
        return s

    s = s[:max_len]
    s = s.rstrip(" ,.;:")
    s = _normalize_whitespace(s)
    return s


def make_short_name(original: str) -> str:
    s = strip_trailing_security_descriptors(original)

    if TRUNCATE_AFTER_LEGAL_SUFFIX:
        s = truncate_after_legal_suffix(s)

    if REMOVE_TRAILING_LEGAL_SUFFIX_TOKENS:
        s = remove_trailing_legal_suffix_tokens(s)

    if ENFORCE_MAX_NAME_LEN and MAX_NAME_LEN and MAX_NAME_LEN > 0:
        s = enforce_max_len(s, MAX_NAME_LEN)

    # Remove trailing single-character symbol tokens left after cleaning,
    # e.g. "Acme &" -> "Acme". Only remove if the final token is a
    # single non-alphanumeric character.
    def _strip_trailing_symbol(name: str) -> str:
        t = norm(name)
        t = _normalize_whitespace(t)
        if not t:
            return t
        parts = t.split()
        if parts:
            last = parts[-1]
            if len(last) == 1 and not last.isalnum():
                parts = parts[:-1]
                t = " ".join(parts).strip()
                t = _normalize_whitespace(t)
        return t

    s = _strip_trailing_symbol(s)

    # Remove trailing dotted short tokens like "S.A" or "S.A." which
    # often appear as country/legal suffixes. Match tokens composed of single
    # letters separated by dots (2-4 groups), e.g. S.A, U.K., S.A.
    def _strip_trailing_dotted(name: str) -> str:
        t = norm(name)
        t = _normalize_whitespace(t)
        if not t:
            return t
        parts = t.split()
        if not parts:
            return t
        last = parts[-1]
        # pattern: single-letter groups separated by dots, optional trailing dot
        import re as _re

        if _re.fullmatch(r"^(?:[A-Za-z]\.){1,3}[A-Za-z]\.?$|^(?:[A-Za-z]\.){1,3}$", last):
            parts = parts[:-1]
            t = " ".join(parts).strip()
            t = _normalize_whitespace(t)
        return t

    s = _strip_trailing_dotted(s)

    return s


def main():
    in_path = Path(INPUT_CSV)
    out_path = Path(OUTPUT_CSV)

    df = read_csv_flex(in_path)

    name_col = pick_name_column(df, NAME_COLUMN)
    if name_col not in df.columns:
        raise SystemExit(f"Name column not found: {name_col}")

    short = df[name_col].apply(make_short_name)

    if OVERWRITE_NAME_COLUMN:
        df[name_col] = short
    else:
        df[OUTPUT_COLUMN] = short

    df.to_csv(out_path, index=False)
    print(f"Input: {in_path}")
    print(f"Name column: {name_col}")
    print(f"Wrote: {out_path}")
    print("Sample:")
    for i in range(min(15, len(df))):
        original = norm(df.loc[i, name_col])
        cleaned = norm(short.iloc[i])
        print(f"  {original}  ->  {cleaned}")


if __name__ == "__main__":
    main()
